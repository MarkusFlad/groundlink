//! The SLE user actor for RAF and FCLTU: the initiating side of an
//! association, on top of a [`TmlClient`] and an initiating
//! [`TmlLinkActor`].

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use kameo::actor::{Actor, ActorRef, Recipient};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use kameo::reply::{DelegatedReply, ReplySender};
use tokio::time::Instant;
use tokio_util::sync::DropGuard;
use tracing::{debug, info, warn};

use super::DEFAULT_SLE_TICK;
use super::fcltu::Cltu;
use crate::actor::batch::Downstream;
use crate::actor::tcp::{Close, Connect, TmlClient};
use crate::actor::tml::{SleDisconnect, SlePeerAbort, Tick, TmlLinkActor, TmlLinkEvent, start_ticker};
use crate::impl_batch_message;
use crate::protocol::sle::common::{
    BindInvocation, ConditionalTime, Credentials, Diagnostic, InvokeId, PduError, ServiceInstanceId, StopInvocation,
    UnbindInvocation, peer_abort, service_type, unbind_reason,
};
use crate::protocol::sle::fcltu::{
    CltuId, CltuStartInvocation, CltuTransferDataInvocation, CltuTransferDataReturn, FcltuProviderPdu, FcltuUserPdu,
};
use crate::protocol::sle::raf::{RafFrame, RafProviderPdu, RafStartInvocation, RafUserPdu, RequestedFrameQuality};
use crate::protocol::tml::SlePdu;

/// Configuration of a [`SleUserActor`]: what it sends in the BIND
/// invocation, and how long it waits for returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SleUserConfig {
    /// The initiator identifier of the user.
    pub initiator_id: String,
    /// The logical name of the provider port.
    pub responder_port: String,
    /// The service instance to bind to.
    pub service_instance: ServiceInstanceId,
    /// The version of the service.
    pub version: u16,
    /// How long to wait for the return of an invocation before aborting
    /// the association with PEER-ABORT `returnTimeout`.
    pub return_timeout: Duration,
}

impl SleUserConfig {
    /// Version 5, return timeout 30 s.
    pub fn new(
        initiator_id: impl Into<String>,
        responder_port: impl Into<String>,
        service_instance: ServiceInstanceId,
    ) -> Self {
        SleUserConfig {
            initiator_id: initiator_id.into(),
            responder_port: responder_port.into(),
            service_instance,
            version: 5,
            return_timeout: Duration::from_secs(30),
        }
    }
}

/// Why an operation of a [`SleUserActor`] failed. Returned to callers that
/// `ask`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SleUserError {
    /// The operation is not allowed in the current state of the
    /// association, e.g. START before BIND; nothing was sent.
    InvalidState(&'static str),
    /// The operation is not part of the service of this actor, e.g.
    /// [`CltuStart`] on a RAF user.
    WrongService,
    /// The provider rejected the BIND with this diagnostic, see
    /// [`bind_diagnostic`](crate::protocol::sle::common::bind_diagnostic).
    BindRejected(i64),
    /// The provider rejected the operation with this diagnostic.
    Rejected(Diagnostic),
    /// The association was aborted before the return arrived; the text
    /// says why (link lost, return timeout, PEER-ABORT, ...).
    Aborted(String),
}

impl fmt::Display for SleUserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SleUserError::InvalidState(what) => write!(f, "not allowed now: {what}"),
            SleUserError::WrongService => f.write_str("operation not part of this service"),
            SleUserError::BindRejected(diagnostic) => write!(f, "BIND rejected with diagnostic {diagnostic}"),
            SleUserError::Rejected(diagnostic) => write!(f, "rejected with {diagnostic:?}"),
            SleUserError::Aborted(reason) => write!(f, "association aborted: {reason}"),
        }
    }
}

impl std::error::Error for SleUserError {}

/// What happened to the association of a [`SleUserActor`] (see
/// [`SleUserActor::with_events`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SleUserEvent {
    /// The provider accepted the BIND.
    Bound,
    /// The provider accepted START.
    Started,
    /// The provider accepted STOP.
    Stopped,
    /// The association ended with UNBIND.
    Unbound,
    /// The association ended abnormally.
    Aborted {
        /// What happened.
        reason: String,
    },
}

/// Connects to the provider and binds. Replies when the BIND return has
/// arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SleBind;

/// UNBIND with reason `end`. Replies when the UNBIND return has arrived;
/// the connection is then closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SleUnbind;

/// RAF-START: online delivery of the frames of `requested_quality`.
/// Replies when the START return has arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RafStart {
    /// Which frames to deliver.
    pub requested_quality: RequestedFrameQuality,
}

/// CLTU-START, announcing `first_cltu_id` as the identification of the
/// first CLTU. Replies when the START return has arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CltuStart {
    /// The identification of the first CLTU.
    pub first_cltu_id: CltuId,
}

/// STOP, for RAF and FCLTU. Replies when the STOP return has arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SleStop;

/// Sent to the actor when the connection could not be opened.
struct ConnectFailed {
    connection: u64,
    reason: String,
}

type UnitReply = Option<ReplySender<Result<(), SleUserError>>>;
type TransferReply = Option<ReplySender<Result<CltuTransferDataReturn, SleUserError>>>;

enum Service {
    Raf { frames: Downstream<RafFrame> },
    Fcltu,
}

enum UserState {
    /// No association.
    Idle,
    /// BIND requested, waiting for the TML link.
    Connecting { reply: UnitReply },
    /// BIND sent.
    Binding { reply: UnitReply, deadline: Instant },
    /// Bound, not started.
    Ready,
    /// START sent.
    Starting {
        invoke_id: InvokeId,
        reply: UnitReply,
        deadline: Instant,
    },
    /// Started.
    Active,
    /// STOP sent.
    Stopping {
        invoke_id: InvokeId,
        reply: UnitReply,
        deadline: Instant,
    },
    /// UNBIND sent.
    Unbinding { reply: UnitReply, deadline: Instant },
}

impl UserState {
    fn name(&self) -> &'static str {
        match self {
            UserState::Idle => "not bound",
            UserState::Connecting { .. } | UserState::Binding { .. } => "BIND in progress",
            UserState::Ready => "bound, not started",
            UserState::Starting { .. } => "START in progress",
            UserState::Active => "started",
            UserState::Stopping { .. } => "STOP in progress",
            UserState::Unbinding { .. } => "UNBIND in progress",
        }
    }

    fn deadline(&self) -> Option<Instant> {
        match self {
            UserState::Binding { deadline, .. }
            | UserState::Starting { deadline, .. }
            | UserState::Stopping { deadline, .. }
            | UserState::Unbinding { deadline, .. } => Some(*deadline),
            _ => None,
        }
    }

    /// The caller waiting for the return of the current operation.
    fn take_reply(&mut self) -> UnitReply {
        match self {
            UserState::Connecting { reply }
            | UserState::Binding { reply, .. }
            | UserState::Starting { reply, .. }
            | UserState::Stopping { reply, .. }
            | UserState::Unbinding { reply, .. } => reply.take(),
            _ => None,
        }
    }
}

struct PendingTransfer {
    deadline: Instant,
    reply: TransferReply,
}

/// Reason to end an association with PEER-ABORT.
struct Abort {
    diagnostic: u8,
    reason: String,
}

fn abort(diagnostic: u8, reason: impl Into<String>) -> Abort {
    Abort {
        diagnostic,
        reason: reason.into(),
    }
}

fn decode_error(err: PduError) -> Abort {
    match err {
        PduError::Unsupported(tag) => abort(peer_abort::OTHER_REASON, format!("unsupported operation {tag}")),
        PduError::Ber(err) => abort(peer_abort::ENCODING_ERROR, err.to_string()),
    }
}

fn unexpected(what: &str, state: &UserState) -> Abort {
    abort(
        peer_abort::PROTOCOL_ERROR,
        format!("{what} not expected while {}", state.name()),
    )
}

fn reply<T: Send + 'static>(reply: Option<ReplySender<Result<T, SleUserError>>>, result: Result<T, SleUserError>) {
    if let Some(reply) = reply {
        reply.send(result);
    }
}

/// Actor for the user side of an SLE association, RAF or FCLTU, on top of
/// a [`TmlClient`] and an [initiating](TmlLinkActor::initiator)
/// [`TmlLinkActor`].
///
/// The application drives the association with messages, best sent with
/// `ask`: each reply comes when the return of the provider has arrived.
///
/// | Message | Service | Invocation | Reply |
/// |---|---|---|---|
/// | [`SleBind`] | both | connects, then BIND | `()` |
/// | [`RafStart`] | RAF | RAF-START | `()` |
/// | [`CltuStart`] | FCLTU | CLTU-START | `()` |
/// | [`Cltu`] | FCLTU | CLTU-TRANSFER-DATA | [`CltuTransferDataReturn`] |
/// | [`SleStop`] | both | STOP | `()` |
/// | [`SleUnbind`] | both | UNBIND, then closes the connection | `()` |
/// | [`SlePeerAbort`] | both | PEER-ABORT | none |
///
/// A negative return gives [`SleUserError::BindRejected`] or
/// [`SleUserError::Rejected`], except for CLTU-TRANSFER-DATA, whose return
/// is passed on as it is, with the identification of the CLTU expected
/// next and the free buffer space. A [`Cltu`] is sent with its own
/// [`id`](Cltu::id), and several can be on their way at once. An operation
/// that is not allowed in the current state is refused with
/// [`SleUserError::InvalidState`] without sending anything.
///
/// The frames of RAF-TRANSFER-BUFFERs go to the `frames` downstream given
/// to [`raf`](Self::raf), one [`Batch`](crate::Batch) per transfer buffer
/// to a batched recipient.
///
/// The association is aborted, and every caller still waiting gets
/// [`SleUserError::Aborted`], when the TML link is lost (which includes a
/// PEER-ABORT of the provider), when a return does not arrive within
/// [`SleUserConfig::return_timeout`] (PEER-ABORT `returnTimeout`), and when
/// the provider sends a PDU that cannot be decoded (`encodingError`) or is
/// not expected (`protocolError`, `unsolicitedInvokeId`).
///
/// Like the [`SleAssociationActor`](super::SleAssociationActor), spawn it
/// with an unbounded mailbox, since it and the link actor send to each
/// other:
///
/// ```no_run
/// use kameo::actor::Spawn;
/// use kameo::mailbox;
/// use groundlink::{
///     Batch, ConnectionEvent, RafFrame, RafStart, RequestedFrameQuality, SleBind, SlePdu, SleUserActor,
///     SleUserConfig, TcpClientArgs, TestActor, TmlClient, TmlCodec, TmlContext, TmlLinkActor, TmlMessage,
/// };
///
/// # #[tokio::main]
/// # async fn main() {
/// let frames = TestActor::<RafFrame>::spawn(TestActor::new());
/// let config = SleUserConfig::new("MOC1", "RAF_PORT", "sagr=1.spack=VST-PASS0001.rsl-fg=1.raf=onlt1".parse().unwrap());
///
/// let user = SleUserActor::prepare_with_mailbox(mailbox::unbounded());
/// let link = TmlLinkActor::prepare();
/// let client = TmlClient::spawn(
///     TcpClientArgs::new("127.0.0.1:5100".parse().unwrap(), link.actor_ref().clone().recipient(), TmlCodec::default())
///         .with_observer(link.actor_ref().clone().reply_recipient::<ConnectionEvent>()),
/// );
/// let link_ref = link.actor_ref().clone();
/// link.spawn(
///     TmlLinkActor::initiator(
///         client.clone().recipient::<TmlMessage>(),
///         client.clone().recipient(),
///         user.actor_ref().clone().recipient::<Batch<SlePdu>>(),
///         TmlContext::new(30, 3),
///     )
///     .with_events(user.actor_ref().clone().recipient()),
/// );
/// let user_ref = user.actor_ref().clone();
/// user.spawn(SleUserActor::raf(client, link_ref, config, frames.recipient::<Batch<RafFrame>>()));
///
/// user_ref.ask(SleBind).await.unwrap();
/// user_ref.ask(RafStart { requested_quality: RequestedFrameQuality::AllFrames }).await.unwrap();
/// # }
/// ```
pub struct SleUserActor {
    service: Service,
    client: ActorRef<TmlClient>,
    link: Downstream<SlePdu>,
    peer_abort: Recipient<SlePeerAbort>,
    disconnect: Recipient<SleDisconnect>,
    config: SleUserConfig,
    events: Option<Recipient<SleUserEvent>>,
    tick: Duration,
    state: UserState,
    transfers: HashMap<InvokeId, PendingTransfer>,
    next_invoke_id: InvokeId,
    /// Counts the connection attempts, to recognise stale failures.
    connection: u64,
    /// Checks the return timeout while an association exists.
    ticker: Option<DropGuard>,
}

impl SleUserActor {
    /// A RAF user that sends the received frames to `frames`.
    pub fn raf(
        client: ActorRef<TmlClient>,
        link: ActorRef<TmlLinkActor>,
        config: SleUserConfig,
        frames: impl Into<Downstream<RafFrame>>,
    ) -> Self {
        let service = Service::Raf { frames: frames.into() };
        Self::new(service, client, link, config)
    }

    /// An FCLTU user.
    pub fn fcltu(client: ActorRef<TmlClient>, link: ActorRef<TmlLinkActor>, config: SleUserConfig) -> Self {
        Self::new(Service::Fcltu, client, link, config)
    }

    fn new(service: Service, client: ActorRef<TmlClient>, link: ActorRef<TmlLinkActor>, config: SleUserConfig) -> Self {
        SleUserActor {
            service,
            client,
            link: link.clone().recipient::<SlePdu>().into(),
            peer_abort: link.clone().recipient(),
            disconnect: link.recipient(),
            config,
            events: None,
            tick: DEFAULT_SLE_TICK,
            state: UserState::Idle,
            transfers: HashMap::new(),
            next_invoke_id: 0,
            connection: 0,
            ticker: None,
        }
    }

    /// Sends the [`SleUserEvent`]s to `events`.
    pub fn with_events(mut self, events: Recipient<SleUserEvent>) -> Self {
        self.events = Some(events);
        self
    }

    /// Checks the return timeout every `tick` instead of every
    /// [`DEFAULT_SLE_TICK`].
    ///
    /// # Panics
    ///
    /// Panics if `tick` is zero.
    pub fn with_tick(mut self, tick: Duration) -> Self {
        assert!(!tick.is_zero(), "tick must not be zero");
        self.tick = tick;
        self
    }

    fn is_raf(&self) -> bool {
        matches!(self.service, Service::Raf { .. })
    }

    fn invoke_id(&mut self) -> InvokeId {
        let id = self.next_invoke_id;
        self.next_invoke_id = self.next_invoke_id.wrapping_add(1);
        id
    }

    fn deadline(&self) -> Instant {
        Instant::now() + self.config.return_timeout
    }

    async fn emit(&self, event: SleUserEvent) {
        if let Some(events) = &self.events
            && let Err(err) = events.tell(event).await
        {
            warn!("SleUserActor: could not send user event: {err}");
        }
    }

    async fn send(&mut self, pdu: Bytes) -> Result<(), Abort> {
        self.link.send(SlePdu(pdu)).await.map_err(|err| {
            abort(
                peer_abort::COMMUNICATIONS_FAILURE,
                format!("TML link not reachable: {err}"),
            )
        })
    }

    /// Ends the association: answers every waiting caller with `error` and
    /// goes idle.
    fn end(&mut self, error: SleUserError) {
        reply(self.state.take_reply(), Err(error.clone()));
        for (_, transfer) in self.transfers.drain() {
            reply(transfer.reply, Err(error.clone()));
        }
        self.state = UserState::Idle;
        self.ticker = None;
    }

    /// Ends the association with PEER-ABORT.
    async fn abort(&mut self, abort: Abort) {
        warn!(
            "SleUserActor: PEER-ABORT with diagnostic {}, {}",
            abort.diagnostic, abort.reason
        );
        if let Err(err) = self
            .peer_abort
            .tell(SlePeerAbort {
                diagnostic: abort.diagnostic,
            })
            .await
        {
            warn!("SleUserActor: could not abort the TML link: {err}");
        }
        self.end(SleUserError::Aborted(abort.reason.clone()));
        self.emit(SleUserEvent::Aborted { reason: abort.reason }).await;
    }

    /// Closes the connection after the PDUs sent so far.
    async fn disconnect(&mut self) {
        if let Err(err) = self.disconnect.tell(SleDisconnect).await {
            warn!("SleUserActor: could not close the TML link: {err}");
        }
    }

    async fn send_bind(&mut self) -> Result<(), Abort> {
        let config = &self.config;
        let service_type = if self.is_raf() {
            service_type::RTN_ALL_FRAMES
        } else {
            service_type::FWD_CLTU
        };
        let bind = BindInvocation {
            credentials: Credentials::Unused,
            initiator_id: config.initiator_id.clone(),
            responder_port: config.responder_port.clone(),
            service_type,
            version: config.version,
            service_instance: config.service_instance.clone(),
        };
        self.send(bind.encode_pdu()).await
    }

    /// Handles a BIND return: decoded the same for both services.
    async fn on_bind_return(&mut self, result: Result<u16, i64>, reply_to: UnitReply) -> Result<(), Abort> {
        match result {
            Ok(version) => {
                info!(
                    "SleUserActor: bound to {}, version {version}",
                    self.config.service_instance
                );
                self.state = UserState::Ready;
                reply(reply_to, Ok(()));
                self.emit(SleUserEvent::Bound).await;
            }
            Err(diagnostic) => {
                info!("SleUserActor: BIND rejected with diagnostic {diagnostic}");
                self.end(SleUserError::Aborted("BIND rejected".to_string()));
                reply(reply_to, Err(SleUserError::BindRejected(diagnostic)));
                self.disconnect().await;
            }
        }
        Ok(())
    }

    /// Handles a START or STOP return for `invoke_id`.
    async fn on_start_stop_return(
        &mut self,
        is_start: bool,
        invoke_id: InvokeId,
        result: Result<(), Diagnostic>,
    ) -> Result<(), Abort> {
        let state = std::mem::replace(&mut self.state, UserState::Idle);
        let (pending_id, reply_to, on_ok, on_err, event) = match state {
            UserState::Starting { invoke_id, reply, .. } if is_start => (
                invoke_id,
                reply,
                UserState::Active,
                UserState::Ready,
                SleUserEvent::Started,
            ),
            UserState::Stopping { invoke_id, reply, .. } if !is_start => (
                invoke_id,
                reply,
                UserState::Ready,
                UserState::Active,
                SleUserEvent::Stopped,
            ),
            state => {
                let what = if is_start { "START return" } else { "STOP return" };
                let abort = unexpected(what, &state);
                self.state = state;
                return Err(abort);
            }
        };
        if pending_id != invoke_id {
            return Err(abort(
                peer_abort::UNSOLICITED_INVOKE_ID,
                format!("return with invoke ID {invoke_id}, expected {pending_id}"),
            ));
        }
        match result {
            Ok(()) => {
                self.state = on_ok;
                reply(reply_to, Ok(()));
                self.emit(event).await;
            }
            Err(diagnostic) => {
                self.state = on_err;
                reply(reply_to, Err(SleUserError::Rejected(diagnostic)));
            }
        }
        Ok(())
    }

    async fn on_unbind_return(&mut self) -> Result<(), Abort> {
        let UserState::Unbinding { reply: reply_to, .. } = std::mem::replace(&mut self.state, UserState::Idle) else {
            return Err(abort(peer_abort::PROTOCOL_ERROR, "UNBIND return not expected"));
        };
        self.end(SleUserError::Aborted("unbound".to_string()));
        reply(reply_to, Ok(()));
        self.disconnect().await;
        self.emit(SleUserEvent::Unbound).await;
        Ok(())
    }

    async fn on_raf_pdu(&mut self, pdu: Bytes) -> Result<(), Abort> {
        match RafProviderPdu::decode(pdu).map_err(decode_error)? {
            RafProviderPdu::BindReturn(ret) => match &mut self.state {
                UserState::Binding { reply, .. } => {
                    let reply = reply.take();
                    self.on_bind_return(ret.result, reply).await
                }
                state => Err(unexpected("BIND return", state)),
            },
            RafProviderPdu::StartReturn(ret) => self.on_start_stop_return(true, ret.invoke_id, ret.result).await,
            RafProviderPdu::StopReturn(ret) => {
                let result = ret.result.map_err(Diagnostic::Common);
                self.on_start_stop_return(false, ret.invoke_id, result).await
            }
            RafProviderPdu::UnbindReturn(_) => self.on_unbind_return().await,
            RafProviderPdu::StatusReport(report) => {
                debug!("SleUserActor: RAF status report {report:?}");
                Ok(())
            }
            RafProviderPdu::ScheduleStatusReportReturn(_) => {
                Err(unexpected("SCHEDULE-STATUS-REPORT return", &self.state))
            }
            RafProviderPdu::TransferBuffer(frames) => {
                // Frames may still arrive until the STOP return.
                if !matches!(self.state, UserState::Active | UserState::Stopping { .. }) {
                    return Err(unexpected("transfer buffer", &self.state));
                }
                if let Service::Raf { frames: downstream } = &mut self.service
                    && let Err(err) = downstream.send_all(frames).await
                {
                    warn!("SleUserActor: frames dropped, could not pass them on: {err}");
                }
                Ok(())
            }
        }
    }

    async fn on_fcltu_pdu(&mut self, pdu: Bytes) -> Result<(), Abort> {
        match FcltuProviderPdu::decode(pdu).map_err(decode_error)? {
            FcltuProviderPdu::BindReturn(ret) => match &mut self.state {
                UserState::Binding { reply, .. } => {
                    let reply = reply.take();
                    self.on_bind_return(ret.result, reply).await
                }
                state => Err(unexpected("BIND return", state)),
            },
            FcltuProviderPdu::StartReturn(ret) => {
                let result = ret.result.map(|_| ());
                self.on_start_stop_return(true, ret.invoke_id, result).await
            }
            FcltuProviderPdu::StopReturn(ret) => {
                let result = ret.result.map_err(Diagnostic::Common);
                self.on_start_stop_return(false, ret.invoke_id, result).await
            }
            FcltuProviderPdu::UnbindReturn(_) => self.on_unbind_return().await,
            FcltuProviderPdu::StatusReport(report) => {
                debug!("SleUserActor: CLTU status report {report:?}");
                Ok(())
            }
            FcltuProviderPdu::AsyncNotify(notify) => {
                debug!("SleUserActor: CLTU notification {notify:?}");
                Ok(())
            }
            FcltuProviderPdu::ScheduleStatusReportReturn(_) => {
                Err(unexpected("SCHEDULE-STATUS-REPORT return", &self.state))
            }
            FcltuProviderPdu::GetParameterReturn(_) => Err(unexpected("GET-PARAMETER return", &self.state)),
            FcltuProviderPdu::TransferDataReturn(ret) => {
                let Some(transfer) = self.transfers.remove(&ret.invoke_id) else {
                    return Err(abort(
                        peer_abort::UNSOLICITED_INVOKE_ID,
                        format!("TRANSFER-DATA return with unknown invoke ID {}", ret.invoke_id),
                    ));
                };
                if transfer.reply.is_none()
                    && let Err(diagnostic) = ret.result
                {
                    warn!(
                        "SleUserActor: CLTU rejected with {diagnostic:?}, next expected {}",
                        ret.cltu_id
                    );
                }
                reply(transfer.reply, Ok(ret));
                Ok(())
            }
        }
    }
}

impl Actor for SleUserActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<SleBind> for SleUserActor {
    type Reply = DelegatedReply<Result<(), SleUserError>>;

    async fn handle(&mut self, _msg: SleBind, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if !matches!(self.state, UserState::Idle) {
            return ctx.reply(Err(SleUserError::InvalidState(self.state.name())));
        }
        let (delegated, reply_sender) = ctx.reply_sender();
        self.state = UserState::Connecting { reply: reply_sender };
        self.connection += 1;
        self.ticker = Some(start_ticker(ctx.actor_ref().clone(), self.tick));
        // Waits until a previous connection is closed completely, then
        // connects. The link reports a failed connect as `Released`.
        let (client, actor_ref, connection) = (self.client.clone(), ctx.actor_ref().clone(), self.connection);
        tokio::spawn(async move {
            let _ = client.ask(Close).await;
            if let Err(err) = client.ask(Connect).await {
                let reason = err.to_string();
                let _ = actor_ref.tell(ConnectFailed { connection, reason }).await;
            }
        });
        delegated
    }
}

impl Message<RafStart> for SleUserActor {
    type Reply = DelegatedReply<Result<(), SleUserError>>;

    async fn handle(&mut self, msg: RafStart, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if !self.is_raf() {
            return ctx.reply(Err(SleUserError::WrongService));
        }
        if !matches!(self.state, UserState::Ready) {
            return ctx.reply(Err(SleUserError::InvalidState(self.state.name())));
        }
        let invoke_id = self.invoke_id();
        let start = RafUserPdu::Start(RafStartInvocation {
            credentials: Credentials::Unused,
            invoke_id,
            start_time: ConditionalTime::Undefined,
            stop_time: ConditionalTime::Undefined,
            requested_quality: msg.requested_quality,
        });
        let (delegated, reply_sender) = ctx.reply_sender();
        self.state = UserState::Starting {
            invoke_id,
            reply: reply_sender,
            deadline: self.deadline(),
        };
        if let Err(abort) = self.send(start.encode()).await {
            self.abort(abort).await;
        }
        delegated
    }
}

impl Message<CltuStart> for SleUserActor {
    type Reply = DelegatedReply<Result<(), SleUserError>>;

    async fn handle(&mut self, msg: CltuStart, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if self.is_raf() {
            return ctx.reply(Err(SleUserError::WrongService));
        }
        if !matches!(self.state, UserState::Ready) {
            return ctx.reply(Err(SleUserError::InvalidState(self.state.name())));
        }
        let invoke_id = self.invoke_id();
        let start = FcltuUserPdu::Start(CltuStartInvocation {
            credentials: Credentials::Unused,
            invoke_id,
            first_cltu_id: msg.first_cltu_id,
        });
        let (delegated, reply_sender) = ctx.reply_sender();
        self.state = UserState::Starting {
            invoke_id,
            reply: reply_sender,
            deadline: self.deadline(),
        };
        if let Err(abort) = self.send(start.encode()).await {
            self.abort(abort).await;
        }
        delegated
    }
}

/// CLTU-TRANSFER-DATA with the CLTU's own identification.
impl Message<Cltu> for SleUserActor {
    type Reply = DelegatedReply<Result<CltuTransferDataReturn, SleUserError>>;

    async fn handle(&mut self, cltu: Cltu, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if self.is_raf() {
            return ctx.reply(Err(SleUserError::WrongService));
        }
        if !matches!(self.state, UserState::Active) {
            return ctx.reply(Err(SleUserError::InvalidState(self.state.name())));
        }
        let invoke_id = self.invoke_id();
        let transfer = FcltuUserPdu::TransferData(CltuTransferDataInvocation {
            credentials: Credentials::Unused,
            invoke_id,
            cltu_id: cltu.id,
            earliest_transmission_time: cltu.earliest_transmission_time,
            latest_transmission_time: cltu.latest_transmission_time,
            delay_time_us: cltu.delay_time_us,
            produce_notification: false,
            data: cltu.data,
        });
        let (delegated, reply_sender) = ctx.reply_sender();
        let pending = PendingTransfer {
            deadline: self.deadline(),
            reply: reply_sender,
        };
        self.transfers.insert(invoke_id, pending);
        if let Err(abort) = self.send(transfer.encode()).await {
            self.abort(abort).await;
        }
        delegated
    }
}

impl Message<SleStop> for SleUserActor {
    type Reply = DelegatedReply<Result<(), SleUserError>>;

    async fn handle(&mut self, _msg: SleStop, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if !matches!(self.state, UserState::Active) {
            return ctx.reply(Err(SleUserError::InvalidState(self.state.name())));
        }
        let invoke_id = self.invoke_id();
        let stop = StopInvocation {
            credentials: Credentials::Unused,
            invoke_id,
        };
        let pdu = if self.is_raf() {
            RafUserPdu::Stop(stop).encode()
        } else {
            FcltuUserPdu::Stop(stop).encode()
        };
        let (delegated, reply_sender) = ctx.reply_sender();
        self.state = UserState::Stopping {
            invoke_id,
            reply: reply_sender,
            deadline: self.deadline(),
        };
        if let Err(abort) = self.send(pdu).await {
            self.abort(abort).await;
        }
        delegated
    }
}

impl Message<SleUnbind> for SleUserActor {
    type Reply = DelegatedReply<Result<(), SleUserError>>;

    async fn handle(&mut self, _msg: SleUnbind, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if !matches!(self.state, UserState::Ready) {
            return ctx.reply(Err(SleUserError::InvalidState(self.state.name())));
        }
        let unbind = UnbindInvocation {
            credentials: Credentials::Unused,
            reason: unbind_reason::END,
        };
        let pdu = if self.is_raf() {
            RafUserPdu::Unbind(unbind).encode()
        } else {
            FcltuUserPdu::Unbind(unbind).encode()
        };
        let (delegated, reply_sender) = ctx.reply_sender();
        self.state = UserState::Unbinding {
            reply: reply_sender,
            deadline: self.deadline(),
        };
        if let Err(abort) = self.send(pdu).await {
            self.abort(abort).await;
        }
        delegated
    }
}

/// PEER-ABORT requested by the application.
impl Message<SlePeerAbort> for SleUserActor {
    type Reply = ();

    async fn handle(&mut self, msg: SlePeerAbort, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if matches!(self.state, UserState::Idle) {
            return;
        }
        let reason = format!("PEER-ABORT by the user, diagnostic {}", msg.diagnostic);
        self.abort(abort(msg.diagnostic, reason)).await;
    }
}

/// Sends the BIND once the link is established, and aborts the
/// association when the link is released.
impl Message<TmlLinkEvent> for SleUserActor {
    type Reply = ();

    async fn handle(&mut self, event: TmlLinkEvent, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        match event {
            TmlLinkEvent::Established(_) => {
                let UserState::Connecting { reply } = &mut self.state else {
                    warn!("SleUserActor: TML link established without a BIND request");
                    return;
                };
                let reply = reply.take();
                self.state = UserState::Binding {
                    reply,
                    deadline: self.deadline(),
                };
                if let Err(abort) = self.send_bind().await {
                    self.abort(abort).await;
                }
            }
            TmlLinkEvent::Released(reason) => {
                if matches!(self.state, UserState::Idle) {
                    return;
                }
                let reason = reason.to_string();
                warn!("SleUserActor: association aborted, {reason}");
                self.end(SleUserError::Aborted(reason.clone()));
                self.emit(SleUserEvent::Aborted { reason }).await;
            }
        }
    }
}

impl Message<ConnectFailed> for SleUserActor {
    type Reply = ();

    async fn handle(&mut self, msg: ConnectFailed, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if msg.connection == self.connection && matches!(self.state, UserState::Connecting { .. }) {
            let reason = format!("cannot connect: {}", msg.reason);
            warn!("SleUserActor: {reason}");
            self.end(SleUserError::Aborted(reason.clone()));
            self.emit(SleUserEvent::Aborted { reason }).await;
        }
    }
}

/// An SLE PDU of the provider.
impl Message<SlePdu> for SleUserActor {
    type Reply = ();

    async fn handle(&mut self, SlePdu(pdu): SlePdu, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if matches!(self.state, UserState::Idle | UserState::Connecting { .. }) {
            warn!("SleUserActor: SLE PDU dropped, no association");
            return;
        }
        let result = if self.is_raf() {
            self.on_raf_pdu(pdu).await
        } else {
            self.on_fcltu_pdu(pdu).await
        };
        if let Err(abort) = result {
            self.abort(abort).await;
        }
    }
}

impl_batch_message!(SleUserActor, SlePdu);

/// Aborts the association when a return is overdue.
impl Message<Tick> for SleUserActor {
    type Reply = ();

    async fn handle(&mut self, _tick: Tick, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let now = Instant::now();
        let overdue = self.state.deadline().is_some_and(|deadline| now >= deadline)
            || self.transfers.values().any(|transfer| now >= transfer.deadline);
        if overdue {
            let reason = abort(peer_abort::RETURN_TIMEOUT, "no return within the return timeout");
            self.abort(reason).await;
        }
    }
}
