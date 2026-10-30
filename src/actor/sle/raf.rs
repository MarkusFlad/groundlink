//! The RAF service instance actor (Return All Frames, CCSDS 911.1-B-4,
//! online delivery).

use std::ops::RangeInclusive;
use std::time::Duration;

use kameo::actor::{Actor, ActorId, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use tokio::time::Instant;
use tokio_util::sync::DropGuard;
use tracing::{info, warn};

use super::{BindRequest, DEFAULT_SLE_TICK, ReleaseRequest, check_bind};
use crate::actor::batch::Downstream;
use crate::actor::tml::{Tick, start_ticker};
use crate::impl_batch_message;
use crate::protocol::sle::common::{Credentials, ServiceInstanceId, bind_diagnostic, service_type};
use crate::protocol::sle::raf::{
    FrameQuality, LockStatus, RafFrame, RafProductionStatus, RafStatusReport, RequestedFrameQuality,
    encode_transfer_buffer,
};
use crate::protocol::tml::SlePdu;

/// Configuration of a [`RafServiceInstance`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RafConfig {
    /// The service instance identifier users bind to.
    pub service_instance: ServiceInstanceId,
    /// Initiator identifiers allowed to bind; `None` allows everyone.
    pub allowed_initiators: Option<Vec<String>>,
    /// Accepted versions of the service.
    pub versions: RangeInclusive<u16>,
    /// Frames per RAF-TRANSFER-BUFFER.
    pub transfer_buffer_size: usize,
    /// Maximum time a frame waits in a partly filled transfer buffer.
    pub latency_limit: Duration,
}

impl RafConfig {
    /// Everyone may bind with versions 2 to 5; 16 frames per transfer
    /// buffer, latency limit 500 ms.
    pub fn new(service_instance: ServiceInstanceId) -> Self {
        RafConfig {
            service_instance,
            allowed_initiators: None,
            versions: 2..=5,
            transfer_buffer_size: 16,
            latency_limit: Duration::from_millis(500),
        }
    }

    /// Allows only the initiators in `initiators` to bind.
    pub fn with_allowed_initiators(mut self, initiators: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.allowed_initiators = Some(initiators.into_iter().map(Into::into).collect());
        self
    }

    /// Accepts the versions of the service in `versions`.
    pub fn with_versions(mut self, versions: RangeInclusive<u16>) -> Self {
        self.versions = versions;
        self
    }

    /// Sends `transfer_buffer_size` frames per RAF-TRANSFER-BUFFER.
    pub fn with_transfer_buffer_size(mut self, transfer_buffer_size: usize) -> Self {
        self.transfer_buffer_size = transfer_buffer_size;
        self
    }

    /// Lets a frame wait at most `latency_limit` in a partly filled
    /// transfer buffer.
    pub fn with_latency_limit(mut self, latency_limit: Duration) -> Self {
        self.latency_limit = latency_limit;
        self
    }
}

/// Asks the instance to send `start_return` to `link` and then to deliver
/// the frames of `quality` there. Sending the return here puts it before
/// the first transfer buffer, and the frames after it are delivered.
pub(crate) struct RafStartRequest {
    pub(crate) owner: ActorId,
    pub(crate) quality: RequestedFrameQuality,
    pub(crate) link: Downstream<SlePdu>,
    pub(crate) start_return: SlePdu,
}

/// Asks the instance for the content of a RAF-STATUS-REPORT.
pub(crate) struct RafStatusRequest {
    pub(crate) owner: ActorId,
}

/// Asks the instance to stop delivering frames. The frames in the transfer
/// buffer are sent before the reply.
pub(crate) struct RafStopRequest {
    pub(crate) owner: ActorId,
}

struct Binding {
    owner: ActorId,
    /// Error-free frames received since BIND.
    error_free_frames: u32,
    /// Frames delivered since BIND.
    delivered_frames: u32,
    /// `Some` while the association is active (after RAF-START).
    delivery: Option<Delivery>,
}

struct Delivery {
    link: Downstream<SlePdu>,
    quality: RequestedFrameQuality,
    first_after_start: bool,
    buffer: Vec<RafFrame>,
    /// When the oldest frame in `buffer` has to go out.
    deadline: Instant,
    _ticker: DropGuard,
}

impl Delivery {
    async fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        let pdu = encode_transfer_buffer(&self.buffer);
        let len = self.buffer.len();
        self.buffer.clear();
        if let Err(err) = self.link.send(SlePdu(pdu)).await {
            warn!("RafServiceInstance: {len} frame(s) dropped, could not send the transfer buffer: {err}");
        }
    }
}

/// Actor for one RAF service instance, online delivery.
///
/// It lives as long as the application and is found by the
/// [`SleAssociationActor`](super::SleAssociationActor) through a
/// [`SleServiceRegistry`](super::SleServiceRegistry). It accepts one
/// binding at a time; a second BIND gets `alreadyBound`.
///
/// The receiver chain sends it every frame as a [`RafFrame`], single or as
/// a [`Batch<RafFrame>`](crate::Batch). Frames that arrive while no
/// association is active (after RAF-START) are discarded. While active,
/// frames of the requested quality are collected into RAF-TRANSFER-BUFFERs
/// of [`RafConfig::transfer_buffer_size`] frames, which go to the
/// [`TmlLinkActor`](crate::TmlLinkActor) when full or when the oldest frame
/// has waited for [`RafConfig::latency_limit`]. The first frame after
/// RAF-START has a data link continuity of -1. RAF-STOP sends the frames
/// collected so far before the STOP return.
///
/// Sending a transfer buffer waits while the link's mailbox is full, so a
/// slow user slows the frame source down.
pub struct RafServiceInstance {
    config: RafConfig,
    tick: Duration,
    binding: Option<Binding>,
}

impl RafServiceInstance {
    /// An instance with `config`.
    pub fn new(config: RafConfig) -> Self {
        RafServiceInstance {
            config,
            tick: DEFAULT_SLE_TICK,
            binding: None,
        }
    }

    /// Checks the latency limit every `tick` instead of every
    /// [`DEFAULT_SLE_TICK`]. A partly filled transfer buffer goes out up to
    /// one tick after the latency limit.
    ///
    /// # Panics
    ///
    /// Panics if `tick` is zero.
    pub fn with_tick(mut self, tick: Duration) -> Self {
        assert!(!tick.is_zero(), "tick must not be zero");
        self.tick = tick;
        self
    }

    fn binding_of(&mut self, owner: ActorId) -> Option<&mut Binding> {
        self.binding.as_mut().filter(|binding| binding.owner == owner)
    }
}

impl Actor for RafServiceInstance {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<BindRequest> for RafServiceInstance {
    type Reply = Result<(), i64>;

    async fn handle(&mut self, request: BindRequest, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let config = &self.config;
        check_bind(
            &request.bind,
            service_type::RTN_ALL_FRAMES,
            &config.versions,
            &config.allowed_initiators,
        )?;
        if self.binding.is_some() {
            return Err(bind_diagnostic::ALREADY_BOUND);
        }
        info!(
            "RafServiceInstance {}: bound by {}",
            config.service_instance, request.bind.initiator_id
        );
        self.binding = Some(Binding {
            owner: request.owner,
            error_free_frames: 0,
            delivered_frames: 0,
            delivery: None,
        });
        Ok(())
    }
}

impl Message<ReleaseRequest> for RafServiceInstance {
    type Reply = ();

    async fn handle(&mut self, request: ReleaseRequest, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if self.binding_of(request.owner).is_some() {
            info!("RafServiceInstance {}: released", self.config.service_instance);
            self.binding = None;
        }
    }
}

impl Message<RafStartRequest> for RafServiceInstance {
    type Reply = ();

    async fn handle(&mut self, request: RafStartRequest, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let tick = self.tick;
        let capacity = self.config.transfer_buffer_size.max(1);
        let Some(binding) = self.binding_of(request.owner) else {
            warn!("RafServiceInstance: START from an association that is not bound");
            return;
        };
        let mut link = request.link;
        if let Err(err) = link.send(request.start_return).await {
            warn!("RafServiceInstance: could not send the START return: {err}");
        }
        // Online delivery: frames from before START are not delivered. None
        // are held here, since frames are discarded while not active.
        binding.delivery = Some(Delivery {
            link,
            quality: request.quality,
            first_after_start: true,
            buffer: Vec::with_capacity(capacity),
            deadline: Instant::now(),
            _ticker: start_ticker(ctx.actor_ref().clone(), tick),
        });
    }
}

impl Message<RafStopRequest> for RafServiceInstance {
    type Reply = ();

    async fn handle(&mut self, request: RafStopRequest, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let Some(binding) = self.binding_of(request.owner) else {
            return;
        };
        if let Some(mut delivery) = binding.delivery.take() {
            delivery.flush().await;
        }
    }
}

/// A frame from the receiver chain.
impl Message<RafFrame> for RafServiceInstance {
    type Reply = ();

    async fn handle(&mut self, mut frame: RafFrame, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let buffer_size = self.config.transfer_buffer_size.max(1);
        let latency_limit = self.config.latency_limit;
        let Some(binding) = self.binding.as_mut() else {
            return;
        };
        if frame.quality == FrameQuality::Good {
            binding.error_free_frames = binding.error_free_frames.wrapping_add(1);
        }
        let Some(delivery) = binding.delivery.as_mut() else {
            return;
        };
        if !delivery.quality.accepts(frame.quality) {
            return;
        }
        binding.delivered_frames = binding.delivered_frames.wrapping_add(1);
        if delivery.first_after_start {
            frame.data_link_continuity = -1;
            delivery.first_after_start = false;
        }
        if delivery.buffer.is_empty() {
            delivery.deadline = Instant::now() + latency_limit;
        }
        delivery.buffer.push(frame);
        if delivery.buffer.len() >= buffer_size {
            delivery.flush().await;
        }
    }
}

impl_batch_message!(RafServiceInstance, RafFrame);

/// The receiver is simulated: always in lock, production running.
impl Message<RafStatusRequest> for RafServiceInstance {
    type Reply = Result<RafStatusReport, Infallible>;

    async fn handle(&mut self, request: RafStatusRequest, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let (error_free, delivered) = self
            .binding_of(request.owner)
            .map(|binding| (binding.error_free_frames, binding.delivered_frames))
            .unwrap_or_default();
        Ok(RafStatusReport {
            credentials: Credentials::Unused,
            error_free_frame_number: error_free,
            delivered_frame_number: delivered,
            frame_sync_lock_status: LockStatus::InLock,
            symbol_sync_lock_status: LockStatus::InLock,
            subcarrier_lock_status: LockStatus::InLock,
            carrier_lock_status: LockStatus::InLock,
            production_status: RafProductionStatus::Running,
        })
    }
}

/// Sends a partly filled transfer buffer when the latency limit is up.
impl Message<Tick> for RafServiceInstance {
    type Reply = ();

    async fn handle(&mut self, _tick: Tick, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if let Some(delivery) = self.binding.as_mut().and_then(|binding| binding.delivery.as_mut())
            && !delivery.buffer.is_empty()
            && Instant::now() >= delivery.deadline
        {
            delivery.flush().await;
        }
    }
}
