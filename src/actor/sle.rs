//! SLE provider actors for the RAF and FCLTU transfer services (CCSDS
//! 911.1-B-4, 912.1-B-4) on top of the [`TmlLinkActor`](crate::TmlLinkActor).
//!
//! ```text
//!                 SlePdu, TmlLinkEvent                  BIND, START, STOP, ... (ask)
//! TmlLinkActor ─────────────────────────▶ SleAssociationActor ───────────────▶ RafServiceInstance
//!      ▲  ▲   SlePdu, SlePeerAbort,             │   via SleServiceRegistry      FcltuServiceInstance
//!      │  └─────── SleDisconnect ───────────────┘                                  │        │
//!      └──────────────── SlePdu (RAF-TRANSFER-BUFFER) ─────────────────────────────┘        ▼
//!                                                                         RafFrame ─▶     Cltu ─▶
//! ```
//!
//! - A [`SleAssociationActor`] per [`TmlLinkActor`](crate::TmlLinkActor), that is per provider
//!   port, runs the association: it decodes the PDUs of the user, keeps the
//!   state (unbound, bound, active), sends the returns and aborts the
//!   association on errors.
//! - A [`RafServiceInstance`] or [`FcltuServiceInstance`] per service
//!   instance lives as long as the application: it holds the
//!   configuration, allows one binding at a time and moves the data:
//!   frames from the receiver chain into RAF-TRANSFER-BUFFERs, accepted
//!   CLTUs on to the radiation side as [`Cltu`] messages.
//! - A [`SleServiceRegistry`] maps the service instance identifiers to the
//!   instance actors, so the association actor finds the instance at BIND.
//!
//! The other side of the association is the [`SleUserActor`], on top of a
//! [`TmlClient`](crate::TmlClient) and an initiating link actor.
//!
//! Covered are BIND, UNBIND, START, STOP, TRANSFER-DATA, PEER-ABORT and
//! SCHEDULE-STATUS-REPORT with the status reports of both services; for
//! FCLTU also CLTU-ASYNC-NOTIFY (`cltuRadiated`, `productionHalted`) and
//! CLTU-GET-PARAMETER for the expected CLTU and event identifications.
//! Credentials are accepted but not checked. A user invoking RAF-GET-PARAMETER
//! or CLTU-THROW-EVENT is peer-aborted with `otherReason`. Each port serves
//! one user at a time.
//!
//! # Wiring
//!
//! The server, the link actor and the association actor refer to each
//! other, so the link and the association are prepared first. The
//! association actor is spawned with an unbounded mailbox: the link actor
//! and the association send to each other, and with two bounded mailboxes
//! a user that sends faster than it reads could block both.
//!
//! ```no_run
//! use kameo::actor::Spawn;
//! use kameo::mailbox;
//! use groundlink::{
//!     Batch, ConnectionEvent, RafConfig, RafServiceInstance, SleAssociationActor, SleAssociationConfig, SlePdu,
//!     SleServiceRegistry, TcpServerArgs, TmlCodec, TmlLinkActor, TmlMessage, TmlResponderConfig, TmlServer,
//! };
//!
//! # #[tokio::main]
//! # async fn main() {
//! let config = RafConfig::new("sagr=1.spack=VST-PASS0001.rsl-fg=1.raf=onlt1".parse().unwrap());
//! let si = config.service_instance.clone();
//! // The receiver chain sends `RafFrame`s to `raf`.
//! let raf = RafServiceInstance::spawn(RafServiceInstance::new(config));
//! let registry = SleServiceRegistry::new().with_raf(si, raf.clone());
//!
//! let association = SleAssociationActor::prepare_with_mailbox(mailbox::unbounded());
//! let link = TmlLinkActor::prepare();
//! let server = TmlServer::spawn(
//!     TcpServerArgs::new("0.0.0.0:5100".parse().unwrap(), link.actor_ref().clone().recipient(), TmlCodec::default())
//!         .with_observer(link.actor_ref().clone().reply_recipient::<ConnectionEvent>()),
//! );
//! let link_ref = link.actor_ref().clone();
//! link.spawn(
//!     TmlLinkActor::responder(
//!         server.clone().recipient::<TmlMessage>(),
//!         server.clone().recipient(),
//!         association.actor_ref().clone().recipient::<Batch<SlePdu>>(),
//!         TmlResponderConfig::default(),
//!     )
//!     .with_events(association.actor_ref().clone().recipient()),
//! );
//! association.spawn(SleAssociationActor::new(link_ref, registry, SleAssociationConfig::default()));
//! # }
//! ```

use std::ops::RangeInclusive;
use std::time::Duration;

use kameo::actor::ActorId;

use crate::protocol::sle::common::{BindInvocation, bind_diagnostic};

pub mod association;
pub mod fcltu;
pub mod raf;
pub mod user;

pub use association::{SleAssociationActor, SleAssociationConfig, SleProviderEvent, SleServiceRegistry};
pub use fcltu::{Cltu, FcltuConfig, FcltuServiceInstance};
pub use raf::{RafConfig, RafServiceInstance};
pub use user::{
    CltuStart, RafStart, SleBind, SleStop, SleUnbind, SleUserActor, SleUserConfig, SleUserError, SleUserEvent,
};

/// Default for how often a [`RafServiceInstance`] checks the latency limit
/// of its transfer buffer; see [`RafServiceInstance::with_tick`].
pub const DEFAULT_SLE_TICK: Duration = Duration::from_millis(100);

/// Asks a service instance to accept the BIND of the association `owner`.
/// Replies `Err(bind diagnostic)` if it does not.
pub(crate) struct BindRequest {
    pub(crate) owner: ActorId,
    pub(crate) bind: BindInvocation,
}

/// Ends the binding of the association `owner`, if it is bound. The
/// instance discards what it still holds for it.
pub(crate) struct ReleaseRequest {
    pub(crate) owner: ActorId,
}

/// The BIND checks of a service instance after the service instance
/// identifier, in the order of CCSDS 911.1/912.1: service type, version,
/// initiator.
pub(crate) fn check_bind(
    bind: &BindInvocation,
    service_type: i64,
    versions: &RangeInclusive<u16>,
    allowed_initiators: &Option<Vec<String>>,
) -> Result<(), i64> {
    if bind.service_type != service_type {
        return Err(bind_diagnostic::INCONSISTENT_SERVICE_TYPE);
    }
    if !versions.contains(&bind.version) {
        return Err(bind_diagnostic::VERSION_NOT_SUPPORTED);
    }
    if let Some(allowed) = allowed_initiators
        && !allowed.contains(&bind.initiator_id)
    {
        return Err(bind_diagnostic::ACCESS_DENIED);
    }
    Ok(())
}
