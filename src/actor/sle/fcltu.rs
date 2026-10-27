//! The FCLTU service instance actor (Forward CLTU, CCSDS 912.1-B-4).

use std::ops::RangeInclusive;

use bytes::Bytes;
use kameo::actor::{Actor, ActorId, ActorRef, Recipient};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::{info, warn};

use super::{BindRequest, ReleaseRequest, check_bind};
use crate::protocol::sle::common::{
    ConditionalTime, Credentials, Diagnostic, ServiceInstanceId, Time, bind_diagnostic, service_type,
};
use crate::protocol::sle::fcltu::{
    CltuAsyncNotify, CltuId, CltuLastOk, CltuLastProcessed, CltuNotification, CltuProductionStatus, CltuStatus,
    CltuStatusReport, CltuTransferDataInvocation, UplinkStatus, transfer_diagnostic,
};

/// Configuration of an [`FcltuServiceInstance`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FcltuConfig {
    /// The service instance identifier users bind to.
    pub service_instance: ServiceInstanceId,
    /// Initiator identifiers allowed to bind; `None` allows everyone.
    pub allowed_initiators: Option<Vec<String>>,
    /// Accepted versions of the service.
    pub versions: RangeInclusive<u16>,
    /// Largest accepted CLTU in octets.
    pub max_cltu_len: usize,
    /// CLTUs the instance buffers on their way to the radiation side.
    pub buffer_capacity: usize,
}

impl FcltuConfig {
    /// Everyone may bind with versions 2 to 5; CLTUs up to 4096 octets, a
    /// buffer of 64 CLTUs.
    pub fn new(service_instance: ServiceInstanceId) -> Self {
        FcltuConfig {
            service_instance,
            allowed_initiators: None,
            versions: 2..=5,
            max_cltu_len: 4096,
            buffer_capacity: 64,
        }
    }
}

/// A CLTU accepted by CLTU-TRANSFER-DATA, to be radiated; sent by the
/// [`FcltuServiceInstance`] to the radiation side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cltu {
    /// The identification of the CLTU.
    pub id: CltuId,
    /// Do not radiate before this time.
    pub earliest_transmission_time: ConditionalTime,
    /// Do not radiate after this time.
    pub latest_transmission_time: ConditionalTime,
    /// Minimum delay after the previous CLTU, in microseconds.
    pub delay_time_us: u64,
    /// The CLTU.
    pub data: Bytes,
}

/// Asks the instance to accept CLTUs, starting with `first_cltu_id`, and
/// to send its CLTU-ASYNC-NOTIFY content to `notices`.
pub(crate) struct FcltuStartRequest {
    pub(crate) owner: ActorId,
    pub(crate) first_cltu_id: CltuId,
    pub(crate) notices: Recipient<CltuNotice>,
}

/// Asks the instance to check and buffer the CLTU of a CLTU-TRANSFER-DATA
/// invocation.
pub(crate) struct FcltuTransferRequest {
    pub(crate) owner: ActorId,
    pub(crate) invocation: CltuTransferDataInvocation,
}

/// What the return of a CLTU-TRANSFER-DATA invocation reports.
pub(crate) struct TransferOutcome {
    pub(crate) result: Result<(), Diagnostic>,
    /// The identification of the CLTU expected next.
    pub(crate) next_cltu_id: CltuId,
    /// Free buffer space in octets.
    pub(crate) buffer_available: u64,
}

/// Asks the instance to stop accepting CLTUs.
pub(crate) struct FcltuStopRequest {
    pub(crate) owner: ActorId,
}

/// Asks the instance for the content of a CLTU-STATUS-REPORT.
pub(crate) struct FcltuStatusRequest;

/// Asks the instance for the identification of the CLTU it expects next,
/// for CLTU-GET-PARAMETER.
pub(crate) struct ExpectedCltuIdRequest {
    pub(crate) owner: ActorId,
}

/// The content of a CLTU-ASYNC-NOTIFY, sent by the instance to the
/// association that started it.
pub(crate) struct CltuNotice(pub(crate) CltuAsyncNotify);

/// Sent by the forwarding task when the radiation side has taken a CLTU.
struct Radiated {
    id: CltuId,
    start: Time,
    stop: Time,
    notify: bool,
}

/// Sent by the forwarding task when the radiation side has stopped.
struct RadiationFailed {
    id: CltuId,
}

/// A CLTU in the buffer and whether the user wants to hear about its
/// radiation.
struct Buffered {
    cltu: Cltu,
    notify: bool,
}

struct Binding {
    owner: ActorId,
    /// The identification of the next CLTU, while the association is
    /// active (after CLTU-START).
    expected: Option<CltuId>,
    /// The identification of the next CLTU, also after STOP.
    last_expected: CltuId,
    notices: Option<Recipient<CltuNotice>>,
}

/// Actor for one FCLTU service instance.
///
/// It lives as long as the application and is found by the
/// [`SleAssociationActor`](super::SleAssociationActor) through a
/// [`SleServiceRegistry`](super::SleServiceRegistry). It accepts one
/// binding at a time; a second BIND gets `alreadyBound`.
///
/// Every CLTU of a CLTU-TRANSFER-DATA invocation is checked, in this
/// order, for its identification (`outOfSequence`), its length
/// (`cltuError`), earliest after latest transmission time
/// (`inconsistentTimeRange`), a latest transmission time in the past
/// (`lateSldu`) and room in the buffer (`unableToStore`). An accepted CLTU
/// goes to the buffer of [`FcltuConfig::buffer_capacity`] CLTUs and from
/// there to the radiation side as a [`Cltu`], in order. The return of
/// each invocation reports the identification of the CLTU expected next and
/// the free buffer space, counted as free slots × [`FcltuConfig::max_cltu_len`].
///
/// A CLTU counts as radiated as soon as the radiation side has taken it
/// into its mailbox; its radiation starts when it leaves the buffer. If the
/// user asked for it, the association then sends a CLTU-ASYNC-NOTIFY
/// `cltuRadiated`. Status reports count the CLTUs received, processed and
/// radiated since CLTU-START. Production is `operational` and the uplink
/// `nominal` until the radiation side stops; production is then `halted`.
pub struct FcltuServiceInstance {
    config: FcltuConfig,
    target: Recipient<Cltu>,
    buffer: mpsc::Sender<Buffered>,
    /// Taken by the forwarding task in `on_start`.
    buffer_rx: Option<mpsc::Receiver<Buffered>>,
    binding: Option<Binding>,
    production_status: CltuProductionStatus,
    received: u32,
    processed: u32,
    radiated: u32,
    last_processed: CltuLastProcessed,
    last_ok: CltuLastOk,
}

impl FcltuServiceInstance {
    /// An instance with `config` that sends the accepted CLTUs to `target`.
    pub fn new(config: FcltuConfig, target: Recipient<Cltu>) -> Self {
        let (buffer, buffer_rx) = mpsc::channel(config.buffer_capacity.max(1));
        FcltuServiceInstance {
            config,
            target,
            buffer,
            buffer_rx: Some(buffer_rx),
            binding: None,
            production_status: CltuProductionStatus::Operational,
            received: 0,
            processed: 0,
            radiated: 0,
            last_processed: CltuLastProcessed::None,
            last_ok: CltuLastOk::None,
        }
    }

    fn binding_of(&mut self, owner: ActorId) -> Option<&mut Binding> {
        self.binding.as_mut().filter(|binding| binding.owner == owner)
    }

    fn buffer_available(&self) -> u64 {
        self.buffer.capacity() as u64 * self.config.max_cltu_len as u64
    }

    fn uplink_status(&self) -> UplinkStatus {
        match self.production_status {
            CltuProductionStatus::Operational => UplinkStatus::Nominal,
            _ => UplinkStatus::NotAvailable,
        }
    }

    fn check_and_buffer(&self, invocation: &CltuTransferDataInvocation, expected: CltuId) -> Result<(), Diagnostic> {
        let reject = |diagnostic| Err(Diagnostic::Specific(diagnostic));
        if invocation.cltu_id != expected {
            return reject(transfer_diagnostic::OUT_OF_SEQUENCE);
        }
        if invocation.data.is_empty() || invocation.data.len() > self.config.max_cltu_len {
            return reject(transfer_diagnostic::CLTU_ERROR);
        }
        if let (ConditionalTime::Known(earliest), ConditionalTime::Known(latest)) = (
            invocation.earliest_transmission_time,
            invocation.latest_transmission_time,
        ) && earliest > latest
        {
            return reject(transfer_diagnostic::INCONSISTENT_TIME_RANGE);
        }
        if let ConditionalTime::Known(latest) = invocation.latest_transmission_time
            && latest < Time::now()
        {
            return reject(transfer_diagnostic::LATE_SLDU);
        }
        let cltu = Cltu {
            id: invocation.cltu_id,
            earliest_transmission_time: invocation.earliest_transmission_time,
            latest_transmission_time: invocation.latest_transmission_time,
            delay_time_us: invocation.delay_time_us,
            data: invocation.data.clone(),
        };
        let buffered = Buffered {
            cltu,
            notify: invocation.produce_notification,
        };
        match self.buffer.try_send(buffered) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => reject(transfer_diagnostic::UNABLE_TO_STORE),
            Err(TrySendError::Closed(_)) => reject(transfer_diagnostic::UNABLE_TO_PROCESS),
        }
    }
}

impl Actor for FcltuServiceInstance {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(mut state: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        if let Some(mut buffer_rx) = state.buffer_rx.take() {
            let target = state.target.clone();
            let service_instance = state.config.service_instance.clone();
            let instance = actor_ref.downgrade();
            // Ends when the instance has stopped (the sender is dropped) or the
            // target has; the buffer then reports `unableToProcess`.
            tokio::spawn(async move {
                while let Some(Buffered { cltu, notify }) = buffer_rx.recv().await {
                    let id = cltu.id;
                    let start = Time::now();
                    let result = target.tell(cltu).await;
                    let Some(instance) = instance.upgrade() else {
                        break;
                    };
                    if let Err(err) = result {
                        warn!("FcltuServiceInstance {service_instance}: could not pass CLTU on: {err}");
                        let _ = instance.tell(RadiationFailed { id }).await;
                        break;
                    }
                    let stop = Time::now();
                    let _ = instance
                        .tell(Radiated {
                            id,
                            start,
                            stop,
                            notify,
                        })
                        .await;
                }
            });
        }
        Ok(state)
    }
}

impl Message<BindRequest> for FcltuServiceInstance {
    type Reply = Result<(), i64>;

    async fn handle(&mut self, request: BindRequest, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let config = &self.config;
        check_bind(
            &request.bind,
            service_type::FWD_CLTU,
            &config.versions,
            &config.allowed_initiators,
        )?;
        if self.binding.is_some() {
            return Err(bind_diagnostic::ALREADY_BOUND);
        }
        info!(
            "FcltuServiceInstance {}: bound by {}",
            config.service_instance, request.bind.initiator_id
        );
        self.binding = Some(Binding {
            owner: request.owner,
            expected: None,
            last_expected: 0,
            notices: None,
        });
        Ok(())
    }
}

impl Message<ReleaseRequest> for FcltuServiceInstance {
    type Reply = ();

    async fn handle(&mut self, request: ReleaseRequest, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if self.binding_of(request.owner).is_some() {
            info!("FcltuServiceInstance {}: released", self.config.service_instance);
            self.binding = None;
        }
    }
}

impl Message<FcltuStartRequest> for FcltuServiceInstance {
    type Reply = ();

    async fn handle(&mut self, request: FcltuStartRequest, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let Some(binding) = self.binding_of(request.owner) else {
            warn!("FcltuServiceInstance: START from an association that is not bound");
            return;
        };
        binding.expected = Some(request.first_cltu_id);
        binding.last_expected = request.first_cltu_id;
        binding.notices = Some(request.notices);
        self.received = 0;
        self.processed = 0;
        self.radiated = 0;
    }
}

impl Message<FcltuTransferRequest> for FcltuServiceInstance {
    type Reply = Result<TransferOutcome, Infallible>;

    async fn handle(&mut self, request: FcltuTransferRequest, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let expected = self.binding_of(request.owner).and_then(|binding| binding.expected);
        let Some(expected) = expected else {
            warn!("FcltuServiceInstance: TRANSFER-DATA from an association that is not active");
            return Ok(TransferOutcome {
                result: Err(Diagnostic::Specific(transfer_diagnostic::UNABLE_TO_PROCESS)),
                next_cltu_id: request.invocation.cltu_id,
                buffer_available: self.buffer_available(),
            });
        };
        let result = self.check_and_buffer(&request.invocation, expected);
        let next_cltu_id = if result.is_ok() {
            self.received = self.received.wrapping_add(1);
            expected.wrapping_add(1)
        } else {
            expected
        };
        if let Some(binding) = self.binding_of(request.owner) {
            binding.expected = Some(next_cltu_id);
            binding.last_expected = next_cltu_id;
        }
        Ok(TransferOutcome {
            result,
            next_cltu_id,
            buffer_available: self.buffer_available(),
        })
    }
}

impl Message<FcltuStopRequest> for FcltuServiceInstance {
    type Reply = ();

    async fn handle(&mut self, request: FcltuStopRequest, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if let Some(binding) = self.binding_of(request.owner) {
            binding.expected = None;
        }
    }
}

impl Message<FcltuStatusRequest> for FcltuServiceInstance {
    type Reply = Result<CltuStatusReport, Infallible>;

    async fn handle(&mut self, _request: FcltuStatusRequest, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        Ok(CltuStatusReport {
            credentials: Credentials::Unused,
            last_processed: self.last_processed,
            last_ok: self.last_ok,
            production_status: self.production_status,
            uplink_status: self.uplink_status(),
            cltus_received: self.received,
            cltus_processed: self.processed,
            cltus_radiated: self.radiated,
            buffer_available: self.buffer_available(),
        })
    }
}

impl Message<ExpectedCltuIdRequest> for FcltuServiceInstance {
    type Reply = Result<CltuId, Infallible>;

    async fn handle(&mut self, request: ExpectedCltuIdRequest, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        Ok(self
            .binding_of(request.owner)
            .map(|binding| binding.last_expected)
            .unwrap_or(0))
    }
}

impl Message<Radiated> for FcltuServiceInstance {
    type Reply = ();

    async fn handle(&mut self, msg: Radiated, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.processed = self.processed.wrapping_add(1);
        self.radiated = self.radiated.wrapping_add(1);
        self.last_processed = CltuLastProcessed::Processed {
            cltu_id: msg.id,
            start_radiation_time: ConditionalTime::Known(msg.start),
            status: CltuStatus::Radiated,
        };
        self.last_ok = CltuLastOk::Ok {
            cltu_id: msg.id,
            stop_radiation_time: msg.stop,
        };
        let notify = CltuAsyncNotify {
            credentials: Credentials::Unused,
            notification: CltuNotification::CltuRadiated,
            last_processed: self.last_processed,
            last_ok: self.last_ok,
            production_status: self.production_status,
            uplink_status: self.uplink_status(),
        };
        if msg.notify
            && let Some(notices) = self.binding.as_ref().and_then(|binding| binding.notices.as_ref())
            && let Err(err) = notices.tell(CltuNotice(notify)).await
        {
            warn!(
                "FcltuServiceInstance: could not report the radiation of CLTU {}: {err}",
                msg.id
            );
        }
    }
}

impl Message<RadiationFailed> for FcltuServiceInstance {
    type Reply = ();

    async fn handle(&mut self, msg: RadiationFailed, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.processed = self.processed.wrapping_add(1);
        self.last_processed = CltuLastProcessed::Processed {
            cltu_id: msg.id,
            start_radiation_time: ConditionalTime::Undefined,
            status: CltuStatus::Interrupted,
        };
        self.production_status = CltuProductionStatus::Halted;
        let notify = CltuAsyncNotify {
            credentials: Credentials::Unused,
            notification: CltuNotification::ProductionHalted,
            last_processed: self.last_processed,
            last_ok: self.last_ok,
            production_status: self.production_status,
            uplink_status: self.uplink_status(),
        };
        if let Some(notices) = self.binding.as_ref().and_then(|binding| binding.notices.as_ref()) {
            let _ = notices.tell(CltuNotice(notify)).await;
        }
    }
}
