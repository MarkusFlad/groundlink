//! The association actor of an SLE provider port, the registry of the
//! service instances and the provider events.

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use kameo::actor::{Actor, ActorId, ActorRef, Recipient};
use kameo::error::{Infallible, SendError};
use kameo::message::{Context, Message};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{info, warn};

use super::fcltu::{
    CltuNotice, ExpectedCltuIdRequest, FcltuServiceInstance, FcltuStartRequest, FcltuStatusRequest, FcltuStopRequest,
    FcltuTransferRequest,
};
use super::raf::{RafServiceInstance, RafStartRequest, RafStatusRequest, RafStopRequest};
use super::{BindRequest, ReleaseRequest};
use crate::actor::batch::Downstream;
use crate::actor::tml::{SleDisconnect, SlePeerAbort, Tick, TmlLinkActor, TmlLinkEvent, start_ticker};
use crate::impl_batch_message;
use crate::protocol::sle::ber::{BerReader, Tag};
use crate::protocol::sle::common::{
    Acknowledgement, BindInvocation, BindReturn, ConditionalTime, Credentials, Diagnostic, PduError, ReportRequestType,
    ScheduleStatusReportInvocation, ScheduleStatusReportReturn, ServiceInstanceId, Time, UnbindReturn, bind_diagnostic,
    peer_abort, schedule_status_report_diagnostic,
};
use crate::protocol::sle::fcltu::{
    CltuGetParameterInvocation, CltuGetParameterReturn, CltuParameter, CltuStartPositive, CltuStartReturn,
    CltuTransferDataReturn, FcltuProviderPdu, FcltuUserPdu, get_parameter_diagnostic, parameter_name,
};
use crate::protocol::sle::raf::{RafProviderPdu, RafStartReturn, RafUserPdu};
use crate::protocol::tml::SlePdu;

/// Configuration of a [`SleAssociationActor`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SleAssociationConfig {
    /// Responder identifier returned in every BIND return.
    pub responder_id: String,
    /// Time allowed between the start of the TML link and the BIND
    /// invocation.
    pub bind_timeout: Duration,
}

impl Default for SleAssociationConfig {
    /// Responder identifier `PROVIDER`, BIND timeout 60 s.
    fn default() -> Self {
        SleAssociationConfig {
            responder_id: "PROVIDER".to_string(),
            bind_timeout: Duration::from_secs(60),
        }
    }
}

/// A service instance actor, as held by a [`SleServiceRegistry`].
#[derive(Clone)]
pub enum ServiceInstanceRef {
    /// A RAF service instance.
    Raf(ActorRef<RafServiceInstance>),
    /// An FCLTU service instance.
    Fcltu(ActorRef<FcltuServiceInstance>),
}

impl ServiceInstanceRef {
    /// Asks the instance to accept the BIND; `Err` carries the BIND
    /// diagnostic.
    async fn bind(&self, request: BindRequest) -> Result<(), i64> {
        let result = match self {
            ServiceInstanceRef::Raf(instance) => instance.ask(request).await,
            ServiceInstanceRef::Fcltu(instance) => instance.ask(request).await,
        };
        result.map_err(|err| match err {
            SendError::HandlerError(diagnostic) => diagnostic,
            err => {
                warn!("SleAssociationActor: service instance not reachable: {err:?}");
                bind_diagnostic::OTHER_REASON
            }
        })
    }

    async fn release(&self, owner: ActorId) {
        let request = ReleaseRequest { owner };
        let result = match self {
            ServiceInstanceRef::Raf(instance) => instance.tell(request).await.map_err(|_| ()),
            ServiceInstanceRef::Fcltu(instance) => instance.tell(request).await.map_err(|_| ()),
        };
        if result.is_err() {
            warn!("SleAssociationActor: could not release the service instance, it has stopped");
        }
    }
}

/// The service instances a [`SleAssociationActor`] can bind to, by their
/// identifier. Cheap to clone, so several ports can share one.
#[derive(Clone, Default)]
pub struct SleServiceRegistry {
    instances: HashMap<ServiceInstanceId, ServiceInstanceRef>,
}

impl SleServiceRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a RAF service instance.
    pub fn with_raf(mut self, service_instance: ServiceInstanceId, instance: ActorRef<RafServiceInstance>) -> Self {
        self.instances
            .insert(service_instance, ServiceInstanceRef::Raf(instance));
        self
    }

    /// Adds an FCLTU service instance.
    pub fn with_fcltu(mut self, service_instance: ServiceInstanceId, instance: ActorRef<FcltuServiceInstance>) -> Self {
        self.instances
            .insert(service_instance, ServiceInstanceRef::Fcltu(instance));
        self
    }

    /// The instance with the identifier `service_instance`.
    pub fn get(&self, service_instance: &ServiceInstanceId) -> Option<&ServiceInstanceRef> {
        self.instances.get(service_instance)
    }
}

/// What happened to an association, sent by the [`SleAssociationActor`]
/// (see [`SleAssociationActor::with_events`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SleProviderEvent {
    /// A BIND was accepted.
    Bound {
        /// The service instance bound to.
        service_instance: ServiceInstanceId,
        /// The initiator identifier of the user.
        initiator: String,
        /// The version of the service.
        version: u16,
    },
    /// A BIND was rejected; the connection is closed.
    BindRejected {
        /// The service instance asked for.
        service_instance: ServiceInstanceId,
        /// The initiator identifier of the user.
        initiator: String,
        /// The diagnostic of the negative BIND return, see
        /// [`bind_diagnostic`].
        diagnostic: i64,
    },
    /// START was accepted.
    Started {
        /// The service instance.
        service_instance: ServiceInstanceId,
    },
    /// STOP was accepted.
    Stopped {
        /// The service instance.
        service_instance: ServiceInstanceId,
    },
    /// The user unbound; the connection is closed.
    Unbound {
        /// The service instance.
        service_instance: ServiceInstanceId,
    },
    /// The association or connection ended abnormally: by a PEER-ABORT of
    /// the provider, or because the TML link was lost.
    Aborted {
        /// The service instance, `None` if the association ended before a
        /// BIND was accepted.
        service_instance: Option<ServiceInstanceId>,
        /// What happened.
        reason: String,
    },
}

/// Sent to the actor by its timer task when the BIND timeout is up.
struct BindTimeout {
    connection: u64,
}

enum AssociationState {
    /// No TML link, or the association has ended and the link is being
    /// closed.
    Idle,
    /// The TML link is up, the BIND has not arrived yet.
    AwaitingBind { _timer: DropGuard },
    /// Bound to `instance`; `active` after START.
    Bound {
        service_instance: ServiceInstanceId,
        instance: ServiceInstanceRef,
        active: bool,
        /// The version of the service agreed in BIND.
        version: u16,
        /// Sends a [`Tick`] for every periodic status report.
        reports: Option<DropGuard>,
    },
}

/// What the PDU handlers of a bound association need.
struct BoundContext {
    owner: ActorId,
    service_instance: ServiceInstanceId,
    active: bool,
    version: u16,
    me: ActorRef<SleAssociationActor>,
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

fn protocol_error(operation: &str) -> Abort {
    abort(
        peer_abort::PROTOCOL_ERROR,
        format!("{operation} not allowed in current state"),
    )
}

fn instance_stopped(err: impl fmt::Debug) -> Abort {
    abort(
        peer_abort::OTHER_REASON,
        format!("service instance not reachable: {err:?}"),
    )
}

/// The error for a first PDU that is not a valid BIND invocation: a
/// protocol error for another operation of RAF or FCLTU, an encoding error
/// otherwise.
fn unbound_pdu_error(pdu: &Bytes, err: PduError) -> Abort {
    let operation = match BerReader::new(pdu.clone()).peek_tag() {
        Ok(tag) if tag == Tag::ctx_cons(0) => "START",
        Ok(tag) if tag == Tag::ctx_cons(2) => "STOP",
        Ok(tag) if tag == Tag::ctx_cons(10) => "TRANSFER-DATA",
        Ok(tag) if tag == Tag::ctx_cons(102) => "UNBIND",
        Ok(tag) if [4, 6, 8].map(Tag::ctx_cons).contains(&tag) => return decode_error(PduError::Unsupported(tag)),
        _ => return decode_error(err),
    };
    protocol_error(operation)
}

/// Actor that runs the SLE associations of one provider port: one
/// association after the other on the connections of one
/// [`TmlLinkActor`].
///
/// It receives the [`SlePdu`]s (single or as a
/// [`Batch<SlePdu>`](crate::Batch)) and the [`TmlLinkEvent`]s of the link
/// actor, and per association
///
/// - waits for the BIND invocation, at most
///   [`SleAssociationConfig::bind_timeout`] after the link is established,
///   and checks it: an unknown service instance is rejected with
///   `noSuchServiceInstance`; the instance then checks the service type
///   (`inconsistentServiceType`), the version (`versionNotSupported`), the
///   initiator (`accessDenied`) and whether it is bound already
///   (`alreadyBound`). After a negative BIND return the connection is
///   closed;
/// - goes from bound to active on START and back on STOP, and ends the
///   association on UNBIND, after which the connection is closed;
/// - answers every invocation with the return encoded for the service of
///   the instance; for RAF, the instance sends the transfer buffers, and
///   the START return before them;
/// - while bound, sends status reports on SCHEDULE-STATUS-REPORT: one at
///   once, or every 2 to 600 s with the first at once; until `stop` or the
///   end of the association;
/// - for FCLTU, sends the CLTU-ASYNC-NOTIFY of the instance (after the
///   return of the CLTU it reports on) and answers CLTU-GET-PARAMETER for
///   the expected CLTU and event identifications, encoded for the version
///   agreed in BIND; other parameters get `unknownParameter`.
///
/// A PDU that is not allowed in the current state, e.g. STOP before START,
/// aborts the association with PEER-ABORT `protocolError`, one that cannot
/// be decoded with `encodingError`, and RAF-GET-PARAMETER and
/// CLTU-THROW-EVENT with `otherReason`. When the
/// link is released, the association is aborted. Either way the service
/// instance is released for the next BIND.
///
/// Reports [`SleProviderEvent`]s to the events recipient, if set. See the
/// [module documentation](super) for how to wire it up; spawn it with an
/// unbounded mailbox.
pub struct SleAssociationActor {
    link: Downstream<SlePdu>,
    peer_abort: Recipient<SlePeerAbort>,
    disconnect: Recipient<SleDisconnect>,
    registry: SleServiceRegistry,
    config: SleAssociationConfig,
    events: Option<Recipient<SleProviderEvent>>,
    state: AssociationState,
    /// Counts the established links, to recognise stale BIND timeouts.
    connection: u64,
}

impl SleAssociationActor {
    /// An association actor for the provider port of `link`, binding to
    /// the service instances of `registry`.
    pub fn new(link: ActorRef<TmlLinkActor>, registry: SleServiceRegistry, config: SleAssociationConfig) -> Self {
        SleAssociationActor {
            link: link.clone().recipient::<SlePdu>().into(),
            peer_abort: link.clone().recipient(),
            disconnect: link.recipient(),
            registry,
            config,
            events: None,
            state: AssociationState::Idle,
            connection: 0,
        }
    }

    /// Sends the [`SleProviderEvent`]s to `events`.
    pub fn with_events(mut self, events: Recipient<SleProviderEvent>) -> Self {
        self.events = Some(events);
        self
    }

    async fn emit(&self, event: SleProviderEvent) {
        if let Some(events) = &self.events
            && let Err(err) = events.tell(event).await
        {
            warn!("SleAssociationActor: could not send provider event: {err}");
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

    /// Ends the association with PEER-ABORT and releases the instance.
    async fn abort(&mut self, abort: Abort, owner: ActorId) {
        warn!(
            "SleAssociationActor: PEER-ABORT with diagnostic {}, {}",
            abort.diagnostic, abort.reason
        );
        if let Err(err) = self
            .peer_abort
            .tell(SlePeerAbort {
                diagnostic: abort.diagnostic,
            })
            .await
        {
            warn!("SleAssociationActor: could not abort the TML link: {err}");
        }
        let service_instance = self.end(owner).await;
        self.emit(SleProviderEvent::Aborted {
            service_instance,
            reason: abort.reason,
        })
        .await;
    }

    /// Goes idle and releases the instance, if bound; returns its
    /// identifier.
    async fn end(&mut self, owner: ActorId) -> Option<ServiceInstanceId> {
        match std::mem::replace(&mut self.state, AssociationState::Idle) {
            AssociationState::Bound {
                service_instance,
                instance,
                ..
            } => {
                instance.release(owner).await;
                Some(service_instance)
            }
            _ => None,
        }
    }

    /// Closes the connection after the PDUs sent so far.
    async fn disconnect(&mut self) {
        if let Err(err) = self.disconnect.tell(SleDisconnect).await {
            warn!("SleAssociationActor: could not close the TML link: {err}");
        }
    }

    fn set_active(&mut self, now_active: bool) {
        if let AssociationState::Bound { active, .. } = &mut self.state {
            *active = now_active;
        }
    }

    async fn reject(&mut self, bind: &BindInvocation, diagnostic: i64) -> Result<(), Abort> {
        info!(
            "SleAssociationActor: BIND of {} to {} rejected with diagnostic {diagnostic}",
            bind.initiator_id, bind.service_instance
        );
        let ret = BindReturn {
            credentials: Credentials::Unused,
            responder_id: self.config.responder_id.clone(),
            result: Err(diagnostic),
        };
        self.state = AssociationState::Idle;
        self.send(ret.encode_pdu()).await?;
        self.disconnect().await;
        self.emit(SleProviderEvent::BindRejected {
            service_instance: bind.service_instance.clone(),
            initiator: bind.initiator_id.clone(),
            diagnostic,
        })
        .await;
        Ok(())
    }

    async fn on_bind(&mut self, pdu: Bytes, owner: ActorId) -> Result<(), Abort> {
        let bind = BindInvocation::decode_pdu(pdu.clone()).map_err(|err| unbound_pdu_error(&pdu, err))?;
        let Some(instance) = self.registry.get(&bind.service_instance).cloned() else {
            return self.reject(&bind, bind_diagnostic::NO_SUCH_SERVICE_INSTANCE).await;
        };
        let request = BindRequest {
            owner,
            bind: bind.clone(),
        };
        if let Err(diagnostic) = instance.bind(request).await {
            return self.reject(&bind, diagnostic).await;
        }
        info!(
            "SleAssociationActor: {} bound to {}, version {}",
            bind.initiator_id, bind.service_instance, bind.version
        );
        self.state = AssociationState::Bound {
            service_instance: bind.service_instance.clone(),
            instance,
            active: false,
            version: bind.version,
            reports: None,
        };
        let ret = BindReturn {
            credentials: Credentials::Unused,
            responder_id: self.config.responder_id.clone(),
            result: Ok(bind.version),
        };
        self.send(ret.encode_pdu()).await?;
        self.emit(SleProviderEvent::Bound {
            service_instance: bind.service_instance,
            initiator: bind.initiator_id,
            version: bind.version,
        })
        .await;
        Ok(())
    }

    /// UNBIND: releases the instance, answers and closes the connection.
    async fn unbind(&mut self, owner: ActorId, unbind_return: Bytes) -> Result<(), Abort> {
        let service_instance = self.end(owner).await;
        self.send(unbind_return).await?;
        self.disconnect().await;
        if let Some(service_instance) = service_instance {
            self.emit(SleProviderEvent::Unbound { service_instance }).await;
        }
        Ok(())
    }

    async fn on_raf_pdu(
        &mut self,
        pdu: Bytes,
        owner: ActorId,
        service_instance: ServiceInstanceId,
        instance: ActorRef<RafServiceInstance>,
        active: bool,
        me: ActorRef<Self>,
    ) -> Result<(), Abort> {
        match RafUserPdu::decode(pdu).map_err(decode_error)? {
            RafUserPdu::ScheduleStatusReport(invocation) => {
                self.schedule_status_report(invocation, owner, &me).await?;
            }
            RafUserPdu::Start(invocation) if !active => {
                let ret = RafStartReturn {
                    credentials: Credentials::Unused,
                    invoke_id: invocation.invoke_id,
                    result: Ok(()),
                };
                // The instance sends the START return, before its first
                // transfer buffer.
                let request = RafStartRequest {
                    owner,
                    quality: invocation.requested_quality,
                    link: self.link.clone(),
                    start_return: SlePdu(RafProviderPdu::StartReturn(ret).encode()),
                };
                instance.ask(request).await.map_err(instance_stopped)?;
                self.set_active(true);
                self.emit(SleProviderEvent::Started { service_instance }).await;
            }
            RafUserPdu::Stop(invocation) if active => {
                // The instance sends the collected frames before it replies.
                instance.ask(RafStopRequest { owner }).await.map_err(instance_stopped)?;
                let ret = RafProviderPdu::StopReturn(Acknowledgement::positive(invocation.invoke_id));
                self.send(ret.encode()).await?;
                self.set_active(false);
                self.emit(SleProviderEvent::Stopped { service_instance }).await;
            }
            RafUserPdu::Unbind(_) if !active => {
                let ret = RafProviderPdu::UnbindReturn(UnbindReturn::default());
                self.unbind(owner, ret.encode()).await?;
            }
            RafUserPdu::Start(_) => return Err(protocol_error("START")),
            RafUserPdu::Stop(_) => return Err(protocol_error("STOP")),
            RafUserPdu::Unbind(_) => return Err(protocol_error("UNBIND")),
            RafUserPdu::Bind(_) => return Err(protocol_error("BIND")),
        }
        Ok(())
    }

    async fn on_fcltu_pdu(
        &mut self,
        pdu: Bytes,
        bound: BoundContext,
        instance: ActorRef<FcltuServiceInstance>,
    ) -> Result<(), Abort> {
        let BoundContext {
            owner,
            service_instance,
            active,
            version,
            me,
        } = bound;
        match FcltuUserPdu::decode(pdu).map_err(decode_error)? {
            FcltuUserPdu::Start(invocation) if !active => {
                let request = FcltuStartRequest {
                    owner,
                    first_cltu_id: invocation.first_cltu_id,
                    notices: me.recipient::<CltuNotice>(),
                };
                instance.ask(request).await.map_err(instance_stopped)?;
                let ret = CltuStartReturn {
                    credentials: Credentials::Unused,
                    invoke_id: invocation.invoke_id,
                    result: Ok(CltuStartPositive {
                        start_radiation_time: Time::now(),
                        stop_radiation_time: ConditionalTime::Undefined,
                    }),
                };
                self.send(FcltuProviderPdu::StartReturn(ret).encode()).await?;
                self.set_active(true);
                self.emit(SleProviderEvent::Started { service_instance }).await;
            }
            FcltuUserPdu::TransferData(invocation) if active => {
                let invoke_id = invocation.invoke_id;
                let outcome = instance
                    .ask(FcltuTransferRequest { owner, invocation })
                    .await
                    .map_err(instance_stopped)?;
                let ret = CltuTransferDataReturn {
                    credentials: Credentials::Unused,
                    invoke_id,
                    cltu_id: outcome.next_cltu_id,
                    buffer_available: outcome.buffer_available,
                    result: outcome.result,
                };
                self.send(FcltuProviderPdu::TransferDataReturn(ret).encode()).await?;
            }
            FcltuUserPdu::Stop(invocation) if active => {
                instance
                    .ask(FcltuStopRequest { owner })
                    .await
                    .map_err(instance_stopped)?;
                let ret = FcltuProviderPdu::StopReturn(Acknowledgement::positive(invocation.invoke_id));
                self.send(ret.encode()).await?;
                self.set_active(false);
                self.emit(SleProviderEvent::Stopped { service_instance }).await;
            }
            FcltuUserPdu::Unbind(_) if !active => {
                let ret = FcltuProviderPdu::UnbindReturn(UnbindReturn::default());
                self.unbind(owner, ret.encode()).await?;
            }
            FcltuUserPdu::ScheduleStatusReport(invocation) => {
                self.schedule_status_report(invocation, owner, &me).await?;
            }
            FcltuUserPdu::GetParameter(invocation) => {
                self.get_parameter(invocation, owner, &instance, version).await?;
            }
            FcltuUserPdu::Start(_) => return Err(protocol_error("START")),
            FcltuUserPdu::TransferData(_) => return Err(protocol_error("TRANSFER-DATA")),
            FcltuUserPdu::Stop(_) => return Err(protocol_error("STOP")),
            FcltuUserPdu::Unbind(_) => return Err(protocol_error("UNBIND")),
            FcltuUserPdu::Bind(_) => return Err(protocol_error("BIND")),
        }
        Ok(())
    }

    fn bound_instance(&self) -> Option<ServiceInstanceRef> {
        match &self.state {
            AssociationState::Bound { instance, .. } => Some(instance.clone()),
            _ => None,
        }
    }

    /// SCHEDULE-STATUS-REPORT: answers, then reports at once or starts or
    /// stops the periodic reports. The first periodic report follows at
    /// once, so the user learns the state without waiting a cycle.
    async fn schedule_status_report(
        &mut self,
        invocation: ScheduleStatusReportInvocation,
        owner: ActorId,
        me: &ActorRef<Self>,
    ) -> Result<(), Abort> {
        let reporting = matches!(self.state, AssociationState::Bound { reports: Some(_), .. });
        let result = match invocation.request {
            ReportRequestType::Periodically(cycle) if !(2..=600).contains(&cycle) => Err(Diagnostic::Specific(
                schedule_status_report_diagnostic::INVALID_REPORTING_CYCLE,
            )),
            ReportRequestType::Stop if !reporting => {
                Err(Diagnostic::Specific(schedule_status_report_diagnostic::ALREADY_STOPPED))
            }
            _ => Ok(()),
        };
        let ret = ScheduleStatusReportReturn {
            credentials: Credentials::Unused,
            invoke_id: invocation.invoke_id,
            result,
        };
        let pdu = match self.bound_instance() {
            Some(ServiceInstanceRef::Raf(_)) => RafProviderPdu::ScheduleStatusReportReturn(ret).encode(),
            Some(ServiceInstanceRef::Fcltu(_)) => FcltuProviderPdu::ScheduleStatusReportReturn(ret).encode(),
            None => return Ok(()),
        };
        self.send(pdu).await?;
        if result.is_err() {
            return Ok(());
        }
        let ticker = match invocation.request {
            ReportRequestType::Immediately => return self.send_status_report(owner).await,
            ReportRequestType::Periodically(cycle) => {
                Some(start_ticker(me.clone(), Duration::from_secs(u64::from(cycle))))
            }
            ReportRequestType::Stop => None,
        };
        if let AssociationState::Bound { reports, .. } = &mut self.state {
            *reports = ticker;
        }
        Ok(())
    }

    /// Sends a RAF- or CLTU-STATUS-REPORT with the state of the instance.
    async fn send_status_report(&mut self, owner: ActorId) -> Result<(), Abort> {
        let pdu = match self.bound_instance() {
            Some(ServiceInstanceRef::Raf(instance)) => {
                let report = instance
                    .ask(RafStatusRequest { owner })
                    .await
                    .map_err(instance_stopped)?;
                RafProviderPdu::StatusReport(report).encode()
            }
            Some(ServiceInstanceRef::Fcltu(instance)) => {
                let report = instance.ask(FcltuStatusRequest).await.map_err(instance_stopped)?;
                FcltuProviderPdu::StatusReport(report).encode()
            }
            None => return Ok(()),
        };
        self.send(pdu).await
    }

    /// CLTU-GET-PARAMETER for the expected CLTU and event identifications;
    /// other parameters get `unknownParameter`.
    async fn get_parameter(
        &mut self,
        invocation: CltuGetParameterInvocation,
        owner: ActorId,
        instance: &ActorRef<FcltuServiceInstance>,
        version: u16,
    ) -> Result<(), Abort> {
        let result = match invocation.parameter {
            parameter_name::EXPECTED_SLDU_IDENTIFICATION => {
                let expected = instance
                    .ask(ExpectedCltuIdRequest { owner })
                    .await
                    .map_err(instance_stopped)?;
                Ok(CltuParameter::ExpectedSlduIdentification(expected))
            }
            // THROW-EVENT is not supported, so no event has been invoked.
            parameter_name::EXPECTED_EVENT_INVOCATION_IDENTIFICATION => {
                Ok(CltuParameter::ExpectedEventInvocationIdentification(0))
            }
            _ => Err(Diagnostic::Specific(get_parameter_diagnostic::UNKNOWN_PARAMETER)),
        };
        let ret = FcltuProviderPdu::GetParameterReturn(CltuGetParameterReturn {
            credentials: Credentials::Unused,
            invoke_id: invocation.invoke_id,
            result,
        });
        self.send(ret.encode_version(version)).await
    }
}

/// Starts a task that sends [`BindTimeout`] to `actor_ref` after `timeout`,
/// unless the returned guard is dropped before.
fn start_bind_timer(actor_ref: ActorRef<SleAssociationActor>, connection: u64, timeout: Duration) -> DropGuard {
    let token = CancellationToken::new();
    let cancelled = token.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = cancelled.cancelled() => {}
            _ = tokio::time::sleep(timeout) => {
                let _ = actor_ref.tell(BindTimeout { connection }).await;
            }
        }
    });
    token.drop_guard()
}

impl Actor for SleAssociationActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// Starts waiting for the BIND when the link is established, and aborts
/// the association when the link is released.
impl Message<TmlLinkEvent> for SleAssociationActor {
    type Reply = ();

    async fn handle(&mut self, event: TmlLinkEvent, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let owner = ctx.actor_ref().id();
        match event {
            TmlLinkEvent::Established(_) => {
                if self.end(owner).await.is_some() {
                    warn!("SleAssociationActor: new TML link while bound, association dropped");
                }
                self.connection += 1;
                let timer = start_bind_timer(ctx.actor_ref().clone(), self.connection, self.config.bind_timeout);
                self.state = AssociationState::AwaitingBind { _timer: timer };
            }
            TmlLinkEvent::Released(reason) => {
                if matches!(self.state, AssociationState::Idle) {
                    return;
                }
                let service_instance = self.end(owner).await;
                warn!("SleAssociationActor: association aborted, {reason}");
                self.emit(SleProviderEvent::Aborted {
                    service_instance,
                    reason: reason.to_string(),
                })
                .await;
            }
        }
    }
}

/// An SLE PDU of the user.
impl Message<SlePdu> for SleAssociationActor {
    type Reply = ();

    async fn handle(&mut self, SlePdu(pdu): SlePdu, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let owner = ctx.actor_ref().id();
        let result = match &self.state {
            AssociationState::Idle => {
                warn!("SleAssociationActor: SLE PDU dropped, no association");
                return;
            }
            AssociationState::AwaitingBind { .. } => self.on_bind(pdu, owner).await,
            AssociationState::Bound {
                service_instance,
                instance,
                active,
                version,
                ..
            } => {
                let (service_instance, active, version) = (service_instance.clone(), *active, *version);
                let me = ctx.actor_ref().clone();
                match instance.clone() {
                    ServiceInstanceRef::Raf(instance) => {
                        self.on_raf_pdu(pdu, owner, service_instance, instance, active, me)
                            .await
                    }
                    ServiceInstanceRef::Fcltu(instance) => {
                        let bound = BoundContext {
                            owner,
                            service_instance,
                            active,
                            version,
                            me,
                        };
                        self.on_fcltu_pdu(pdu, bound, instance).await
                    }
                }
            }
        };
        if let Err(abort) = result {
            self.abort(abort, owner).await;
        }
    }
}

impl_batch_message!(SleAssociationActor, SlePdu);

/// Sends a periodic status report.
impl Message<Tick> for SleAssociationActor {
    type Reply = ();

    async fn handle(&mut self, _tick: Tick, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let owner = ctx.actor_ref().id();
        if matches!(self.state, AssociationState::Bound { reports: Some(_), .. })
            && let Err(abort) = self.send_status_report(owner).await
        {
            self.abort(abort, owner).await;
        }
    }
}

/// Sends a CLTU-ASYNC-NOTIFY reported by the FCLTU instance.
impl Message<CltuNotice> for SleAssociationActor {
    type Reply = ();

    async fn handle(&mut self, CltuNotice(notify): CltuNotice, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if !matches!(
            self.state,
            AssociationState::Bound {
                instance: ServiceInstanceRef::Fcltu(_),
                ..
            }
        ) {
            return;
        }
        if let Err(abort) = self.send(FcltuProviderPdu::AsyncNotify(notify).encode()).await {
            self.abort(abort, ctx.actor_ref().id()).await;
        }
    }
}

/// Aborts the association if the BIND has not arrived in time.
impl Message<BindTimeout> for SleAssociationActor {
    type Reply = ();

    async fn handle(&mut self, timeout: BindTimeout, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if timeout.connection == self.connection && matches!(self.state, AssociationState::AwaitingBind { .. }) {
            let reason = abort(peer_abort::OTHER_REASON, "no BIND invocation received");
            self.abort(reason, ctx.actor_ref().id()).await;
        }
    }
}
