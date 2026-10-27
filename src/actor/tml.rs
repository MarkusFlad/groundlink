//! The SLE Transport Mapping Layer (TML, CCSDS 913.1-B-2) as an actor: the
//! [`TmlLinkActor`] sits between a [`TmlServer`](crate::TmlServer) or
//! [`TmlClient`](crate::TmlClient) and the SLE layer, and exchanges the SLE
//! PDUs with it as [`SlePdu`] messages.
//!
//! ```text
//!              TmlMessage                  SlePdu
//! TmlServer ───────────────▶ TmlLinkActor ────────▶ SLE layer (e.g. a provider actor)
//!     ▲     ConnectionEvent       │   ▲   TmlLinkEvent ─▶
//!     └───── TmlMessage, Close ───┘   └──── SlePdu, SlePeerAbort, SleDisconnect
//! ```
//!
//! The SLE provider actors in [`actor::sle`](crate::actor::sle) are such an
//! SLE layer.
//!
//! The server and the link actor each need a reference to the other, so
//! the link actor is prepared first:
//!
//! ```no_run
//! use kameo::actor::Spawn;
//! use groundlink::{
//!     ConnectionEvent, SlePdu, TcpServerArgs, TestActor, TmlCodec, TmlLinkActor, TmlLinkEvent,
//!     TmlMessage, TmlResponderConfig, TmlServer,
//! };
//!
//! # #[tokio::main]
//! # async fn main() {
//! // The SLE layer; here TestActors.
//! let pdus = TestActor::<SlePdu>::spawn(TestActor::new());
//! let events = TestActor::<TmlLinkEvent>::spawn(TestActor::new());
//!
//! let link = TmlLinkActor::prepare();
//! let server = TmlServer::spawn(
//!     TcpServerArgs::new("0.0.0.0:5100".parse().unwrap(), link.actor_ref().clone().recipient(), TmlCodec::default())
//!         .with_observer(link.actor_ref().clone().reply_recipient::<ConnectionEvent>()),
//! );
//! link.spawn(
//!     TmlLinkActor::responder(
//!         server.clone().recipient::<TmlMessage>(),
//!         server.clone().recipient(),
//!         pdus.recipient::<SlePdu>(),
//!         TmlResponderConfig::default(),
//!     )
//!     .with_events(events.recipient()),
//! );
//! # }
//! ```

use std::fmt;
use std::ops::RangeInclusive;
use std::time::Duration;

use kameo::actor::{Actor, ActorRef, Recipient};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{info, warn};

use crate::actor::batch::Downstream;
use crate::actor::tcp::{Close, ConnectionEvent, ConnectionEventKind};
use crate::impl_batch_message;
use crate::protocol::tml::{SlePdu, TmlContext, TmlMessage};

/// Default for how often a [`TmlLinkActor`] checks its timers; see
/// [`TmlLinkActor::with_tick`].
pub const DEFAULT_TML_TICK: Duration = Duration::from_millis(250);

/// What a responding [`TmlLinkActor`] (the SLE provider side) accepts from
/// the initiator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TmlResponderConfig {
    /// Time allowed between the TCP connect and the context message.
    pub startup_timeout: Duration,
    /// Whether the initiator may switch heartbeats off (heartbeat
    /// interval 0).
    pub allow_no_heartbeat: bool,
    /// Accepted heartbeat intervals in seconds, if not 0.
    pub heartbeat_interval: RangeInclusive<u16>,
    /// Accepted dead factors, if the heartbeat interval is not 0.
    pub dead_factor: RangeInclusive<u16>,
}

impl Default for TmlResponderConfig {
    /// 60 s start-up timeout, heartbeats may be switched off, heartbeat
    /// interval 1..=3600 s, dead factor 2..=60.
    fn default() -> Self {
        TmlResponderConfig {
            startup_timeout: Duration::from_secs(60),
            allow_no_heartbeat: true,
            heartbeat_interval: 1..=3600,
            dead_factor: 2..=60,
        }
    }
}

impl TmlResponderConfig {
    fn check(&self, context: TmlContext) -> Result<(), TmlReleaseReason> {
        let rejected = |what: &str| Err(TmlReleaseReason::ContextRejected(what.to_string()));
        if context.heartbeat_interval == 0 {
            return if self.allow_no_heartbeat {
                Ok(())
            } else {
                rejected("heartbeat interval 0 not allowed")
            };
        }
        if !self.heartbeat_interval.contains(&context.heartbeat_interval) {
            return rejected(&format!("heartbeat interval {} s", context.heartbeat_interval));
        }
        if !self.dead_factor.contains(&context.dead_factor) {
            return rejected(&format!("dead factor {}", context.dead_factor));
        }
        Ok(())
    }
}

/// The side of the TML connection a [`TmlLinkActor`] is on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TmlRole {
    /// The side that opens the connection (normally the SLE user): sends
    /// this context message when connected.
    Initiator(TmlContext),
    /// The side that accepts the connection (normally the SLE provider):
    /// waits for the context message and checks it.
    Responder(TmlResponderConfig),
}

/// Why a TML link ended; see [`TmlLinkEvent::Released`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TmlReleaseReason {
    /// The TCP connection was closed or could not be opened. The text
    /// describes what happened.
    ConnectionClosed(String),
    /// No context message arrived within
    /// [`TmlResponderConfig::startup_timeout`].
    StartupTimeout,
    /// The context message of the initiator was not accepted.
    ContextRejected(String),
    /// A TML message that is not allowed at this point, e.g. an SLE PDU
    /// before the context message.
    ProtocolError(String),
    /// Nothing arrived for `heartbeat interval × dead factor` seconds.
    PeerDead,
}

impl fmt::Display for TmlReleaseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TmlReleaseReason::ConnectionClosed(what) => write!(f, "connection closed: {what}"),
            TmlReleaseReason::StartupTimeout => f.write_str("no TML context message within the start-up timeout"),
            TmlReleaseReason::ContextRejected(what) => write!(f, "TML context message rejected: {what}"),
            TmlReleaseReason::ProtocolError(what) => write!(f, "TML protocol error: {what}"),
            TmlReleaseReason::PeerDead => f.write_str("peer dead: TML receive timer expired"),
        }
    }
}

/// State change of a TML link, sent by the [`TmlLinkActor`] to the SLE
/// layer (see [`TmlLinkActor::with_events`]).
///
/// For every connection, `Established` comes before the first [`SlePdu`]
/// of the connection, and `Released` once after the last one. A connection
/// that ends before the context exchange is reported as `Released` only.
/// A connection closed because of [`SlePeerAbort`] is not reported, since
/// the SLE layer asked for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TmlLinkEvent {
    /// The context exchange is done; SLE PDUs can flow. Carries the
    /// heartbeat parameters in force.
    Established(TmlContext),
    /// The link has ended.
    Released(TmlReleaseReason),
}

/// SLE PEER-ABORT: asks the [`TmlLinkActor`] to abort the current
/// connection. `diagnostic` is the PEER-ABORT diagnostic (CCSDS 913.1).
///
/// ISP1 sends the diagnostic as one byte of TCP urgent data before closing.
/// The generic TCP writer has no urgent data, so the diagnostic is only
/// logged and the connection is closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlePeerAbort {
    /// The PEER-ABORT diagnostic.
    pub diagnostic: u8,
}

/// SLE layer: asks the [`TmlLinkActor`] to close the current connection
/// in an orderly way, e.g. after a negative BIND return. The SLE PDUs sent
/// before are still written.
///
/// Unlike [`SlePeerAbort`], this is not an abort. Like it, the end of the
/// connection is not reported as [`TmlLinkEvent::Released`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SleDisconnect;

/// Sent to an actor by its ticker task (see [`start_ticker`]) to check its
/// timers.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Tick;

enum LinkState {
    /// No connection, or the connection is being closed.
    Idle,
    /// Responder: connected, waiting for the context message.
    AwaitingContext { deadline: Instant, _ticker: DropGuard },
    /// The context exchange is done.
    Established {
        context: TmlContext,
        last_received: Instant,
        last_sent: Instant,
        _ticker: DropGuard,
    },
}

/// Actor that runs the TML rules on top of a [`TmlServer`](crate::TmlServer)
/// or [`TmlClient`](crate::TmlClient) and exchanges the SLE PDUs with the
/// SLE layer as [`SlePdu`] messages.
///
/// It must receive the [`TmlMessage`]s read from the connection (as the
/// `downstream` of the server or client, single or batched) and its
/// [`ConnectionEvent`]s (as its observer). Per connection it
///
/// - as [responder](Self::responder), waits for the context message, checks
///   it against its [`TmlResponderConfig`] and closes the connection if it
///   is missing, late or not accepted;
/// - as [initiator](Self::initiator), sends its context message as soon as
///   the connection is up;
/// - passes every received SLE PDU on as [`SlePdu`] to `upstream`, and
///   writes every [`SlePdu`] it receives to the connection;
/// - sends a heartbeat when nothing was sent for the heartbeat interval,
///   and closes the connection when nothing was received for
///   `heartbeat interval × dead factor` (both off with interval 0);
/// - closes the connection on a TML message that is not allowed (an SLE
///   PDU before the context message, a second context message), and on
///   [`SlePeerAbort`] and [`SleDisconnect`];
/// - reports [`TmlLinkEvent`]s to the events recipient, if set.
///
/// Malformed TML messages are rejected by the [`TmlCodec`](crate::TmlCodec)
/// already: the reader then closes the connection, which is reported as
/// [`TmlReleaseReason::ConnectionClosed`].
///
/// [`SlePdu`]s that arrive while no link is established are dropped with a
/// warning. Handles [`Batch<TmlMessage>`](crate::Batch) and
/// [`Batch<SlePdu>`](crate::Batch) too, and passes the PDUs of a batch on
/// as one batch to a batched recipient.
pub struct TmlLinkActor {
    role: TmlRole,
    connection: Downstream<TmlMessage>,
    close: Recipient<Close>,
    upstream: Downstream<SlePdu>,
    events: Option<Recipient<TmlLinkEvent>>,
    tick: Duration,
    state: LinkState,
}

impl TmlLinkActor {
    /// A link actor for the responding side (normally the SLE provider).
    ///
    /// - `connection`: where SLE PDUs and heartbeats are written, the
    ///   `Recipient<TmlMessage>` (or `Recipient<Batch<TmlMessage>>`) of the
    ///   server;
    /// - `close`: the `Recipient<Close>` of the same server;
    /// - `upstream`: receives the SLE PDUs, a `Recipient<SlePdu>` or
    ///   `Recipient<Batch<SlePdu>>`.
    pub fn responder(
        connection: impl Into<Downstream<TmlMessage>>,
        close: Recipient<Close>,
        upstream: impl Into<Downstream<SlePdu>>,
        config: TmlResponderConfig,
    ) -> Self {
        Self::new(TmlRole::Responder(config), connection.into(), close, upstream.into())
    }

    /// A link actor for the initiating side (normally the SLE user), which
    /// sends `context` when connected. The arguments are those of
    /// [`responder`](Self::responder), for a [`TmlClient`](crate::TmlClient).
    pub fn initiator(
        connection: impl Into<Downstream<TmlMessage>>,
        close: Recipient<Close>,
        upstream: impl Into<Downstream<SlePdu>>,
        context: TmlContext,
    ) -> Self {
        Self::new(TmlRole::Initiator(context), connection.into(), close, upstream.into())
    }

    fn new(
        role: TmlRole,
        connection: Downstream<TmlMessage>,
        close: Recipient<Close>,
        upstream: Downstream<SlePdu>,
    ) -> Self {
        TmlLinkActor {
            role,
            connection,
            close,
            upstream,
            events: None,
            tick: DEFAULT_TML_TICK,
            state: LinkState::Idle,
        }
    }

    /// Sends the [`TmlLinkEvent`]s to `events`.
    pub fn with_events(mut self, events: Recipient<TmlLinkEvent>) -> Self {
        self.events = Some(events);
        self
    }

    /// Checks the timers (heartbeat, dead timer, start-up timeout) every
    /// `tick` instead of every [`DEFAULT_TML_TICK`]. Timers fire up to one
    /// tick late.
    ///
    /// # Panics
    ///
    /// Panics if `tick` is zero.
    pub fn with_tick(mut self, tick: Duration) -> Self {
        assert!(!tick.is_zero(), "tick must not be zero");
        self.tick = tick;
        self
    }

    async fn emit(&self, event: TmlLinkEvent) {
        if let Some(events) = &self.events
            && let Err(err) = events.tell(event).await
        {
            warn!("TmlLinkActor: could not send link event: {err}");
        }
    }

    async fn send_to_connection(&mut self, message: TmlMessage) {
        if let Err(err) = self.connection.send(message).await {
            warn!("TmlLinkActor: could not send TML message to the connection: {err}");
        }
    }

    /// Ends the link because of `reason`: closes the connection and
    /// reports `Released`.
    async fn fail(&mut self, reason: TmlReleaseReason) {
        warn!("TmlLinkActor: closing the connection, {reason}");
        self.state = LinkState::Idle;
        if let Err(err) = self.close.tell(Close).await {
            warn!("TmlLinkActor: could not close the connection: {err}");
        }
        // Inside a batch, the PDUs before the error must reach the SLE layer
        // before `Released`.
        self.upstream.release().await;
        self.emit(TmlLinkEvent::Released(reason)).await;
    }

    async fn establish(&mut self, context: TmlContext, ticker: DropGuard) {
        let now = Instant::now();
        self.state = LinkState::Established {
            context,
            last_received: now,
            last_sent: now,
            _ticker: ticker,
        };
        info!(
            "TmlLinkActor: link established, heartbeat interval {} s, dead factor {}",
            context.heartbeat_interval, context.dead_factor
        );
        self.emit(TmlLinkEvent::Established(context)).await;
    }

    async fn on_connected(&mut self, actor_ref: ActorRef<Self>) {
        let ticker = start_ticker(actor_ref, self.tick);
        match self.role.clone() {
            TmlRole::Responder(config) => {
                self.state = LinkState::AwaitingContext {
                    deadline: Instant::now() + config.startup_timeout,
                    _ticker: ticker,
                };
            }
            TmlRole::Initiator(context) => {
                self.send_to_connection(TmlMessage::Context(context)).await;
                self.establish(context, ticker).await;
            }
        }
    }
}

/// Starts a task that sends [`Tick`] to `actor_ref` every `period`, until
/// the returned guard is dropped or the actor has stopped.
pub(crate) fn start_ticker<A>(actor_ref: ActorRef<A>, period: Duration) -> DropGuard
where
    A: Actor + Message<Tick>,
{
    let token = CancellationToken::new();
    let cancelled = token.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = cancelled.cancelled() => break,
                _ = interval.tick() => {
                    if actor_ref.tell(Tick).await.is_err() {
                        break;
                    }
                }
            }
        }
    });
    token.drop_guard()
}

impl Actor for TmlLinkActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// Starts a link on `Connected` and ends it when the connection is gone.
impl Message<ConnectionEvent> for TmlLinkActor {
    type Reply = ();

    async fn handle(&mut self, event: ConnectionEvent, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let (reason, connect_failed) = match &event.kind {
            ConnectionEventKind::Connected => {
                self.on_connected(ctx.actor_ref().clone()).await;
                return;
            }
            ConnectionEventKind::HalfClosed { .. } => return,
            ConnectionEventKind::Disconnected => (format!("{} disconnected", event.peer_addr), false),
            ConnectionEventKind::ConnectFailed { reason } => {
                (format!("cannot connect to {}: {reason}", event.peer_addr), true)
            }
        };
        let was_idle = matches!(self.state, LinkState::Idle);
        self.state = LinkState::Idle;
        // After `fail` or a PEER-ABORT, the link is idle already and the end
        // has been reported (or was requested by the SLE layer).
        if !was_idle || connect_failed {
            self.emit(TmlLinkEvent::Released(TmlReleaseReason::ConnectionClosed(reason)))
                .await;
        }
    }
}

/// A TML message read from the connection.
impl Message<TmlMessage> for TmlLinkActor {
    type Reply = ();

    async fn handle(&mut self, message: TmlMessage, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        match &mut self.state {
            LinkState::Idle => warn!("TmlLinkActor: TML message dropped, no link"),
            LinkState::AwaitingContext { .. } => {
                let TmlRole::Responder(config) = &self.role else {
                    unreachable!("only a responder waits for the context message");
                };
                let result = match message {
                    TmlMessage::Context(context) => config.check(context).map(|()| context),
                    _ => Err(TmlReleaseReason::ProtocolError(
                        "the first TML message is not a context message".to_string(),
                    )),
                };
                match result {
                    Ok(context) => {
                        let LinkState::AwaitingContext { _ticker: ticker, .. } =
                            std::mem::replace(&mut self.state, LinkState::Idle)
                        else {
                            unreachable!();
                        };
                        self.establish(context, ticker).await;
                    }
                    Err(reason) => self.fail(reason).await,
                }
            }
            LinkState::Established { last_received, .. } => {
                *last_received = Instant::now();
                match message {
                    TmlMessage::SlePdu(pdu) => {
                        if let Err(err) = self.upstream.send(SlePdu(pdu)).await {
                            warn!("TmlLinkActor: could not pass SLE PDU on: {err}");
                        }
                    }
                    TmlMessage::Heartbeat => {}
                    TmlMessage::Context(_) => {
                        let reason =
                            TmlReleaseReason::ProtocolError("context message on an established link".to_string());
                        self.fail(reason).await;
                    }
                }
            }
        }
    }
}

// The SLE PDUs of a batch of TML messages go on as one batch.
impl_batch_message!(TmlLinkActor, TmlMessage, outputs = [upstream]);

/// An SLE PDU from the SLE layer, written to the connection.
impl Message<SlePdu> for TmlLinkActor {
    type Reply = ();

    async fn handle(&mut self, SlePdu(pdu): SlePdu, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let LinkState::Established { last_sent, .. } = &mut self.state else {
            warn!("TmlLinkActor: SLE PDU dropped, no link established");
            return;
        };
        *last_sent = Instant::now();
        self.send_to_connection(TmlMessage::SlePdu(pdu)).await;
    }
}

// The SLE PDUs of a batch go to the connection as one batch.
impl_batch_message!(TmlLinkActor, SlePdu, outputs = [connection]);

/// Aborts the current connection.
impl Message<SlePeerAbort> for TmlLinkActor {
    type Reply = ();

    async fn handle(&mut self, abort: SlePeerAbort, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if matches!(self.state, LinkState::Idle) {
            return;
        }
        info!(
            "TmlLinkActor: PEER-ABORT with diagnostic {}, closing the connection",
            abort.diagnostic
        );
        self.state = LinkState::Idle;
        if let Err(err) = self.close.tell(Close).await {
            warn!("TmlLinkActor: could not close the connection: {err}");
        }
    }
}

/// Closes the current connection after the pending SLE PDUs.
impl Message<SleDisconnect> for TmlLinkActor {
    type Reply = ();

    async fn handle(&mut self, _msg: SleDisconnect, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if matches!(self.state, LinkState::Idle) {
            return;
        }
        info!("TmlLinkActor: SLE layer releases the connection, closing it");
        self.state = LinkState::Idle;
        if let Err(err) = self.close.tell(Close).await {
            warn!("TmlLinkActor: could not close the connection: {err}");
        }
    }
}

/// Checks the start-up timeout, the dead timer and whether a heartbeat is
/// due.
impl Message<Tick> for TmlLinkActor {
    type Reply = ();

    async fn handle(&mut self, _tick: Tick, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let now = Instant::now();
        let reason = match &mut self.state {
            LinkState::Idle => return,
            LinkState::AwaitingContext { deadline, .. } => {
                if now < *deadline {
                    return;
                }
                TmlReleaseReason::StartupTimeout
            }
            LinkState::Established {
                context,
                last_received,
                last_sent,
                ..
            } => {
                if context.heartbeat_interval == 0 {
                    return;
                }
                let interval = Duration::from_secs(u64::from(context.heartbeat_interval));
                if now.duration_since(*last_received) >= interval * u32::from(context.dead_factor) {
                    TmlReleaseReason::PeerDead
                } else {
                    if now.duration_since(*last_sent) >= interval {
                        *last_sent = now;
                        self.send_to_connection(TmlMessage::Heartbeat).await;
                    }
                    return;
                }
            }
        };
        self.fail(reason).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn responder_config_checks_the_context() {
        let config = TmlResponderConfig {
            allow_no_heartbeat: false,
            ..TmlResponderConfig::default()
        };
        assert!(config.check(TmlContext::new(30, 3)).is_ok());
        for context in [
            TmlContext::new(0, 0),
            TmlContext::new(3601, 3),
            TmlContext::new(30, 1),
            TmlContext::new(30, 61),
        ] {
            assert!(
                matches!(config.check(context), Err(TmlReleaseReason::ContextRejected(_))),
                "{context:?}"
            );
        }
        assert!(TmlResponderConfig::default().check(TmlContext::new(0, 0)).is_ok());
    }
}
