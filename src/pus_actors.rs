//! Actors that process PUS packets for an application process.
//!
//! Typical chain for an application with a fixed APID:
//!
//! ```text
//! PusListener ──PusPacket──▶ PusTcAcceptor ──TM(1,1)──▶ …
//!                                 │
//!                                 └─PusTc (Service 17)──▶ PusTestServiceActor ──TM(17,2), TM(1,7)──▶ …
//! ```

use std::collections::HashMap;
use std::fmt::Display;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use kameo::actor::{Actor, ActorRef, Recipient};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use tracing::{error, warn};

use crate::ccsds::SEQUENCE_COUNT_MAX;
use crate::cuc::{CucFormat, CucTime};
use crate::pus::service1::{FailureCode, RequestId, VerificationKind, VerificationReport};
use crate::pus::service17::{AreYouAliveReport, AreYouAliveRequest};
use crate::pus::{PusPacket, PusTc};

/// Packet sequence count of an APID that several actors can share, so that
/// all TM packets of that APID are numbered consecutively.
///
/// Clones share the same counter. It wraps around to 0 after
/// [`SEQUENCE_COUNT_MAX`].
///
/// ```
/// use kameo_tcp_example::SequenceCounter;
///
/// let counter = SequenceCounter::new();
/// let shared = counter.clone();
/// assert_eq!(counter.next(), 0);
/// assert_eq!(shared.next(), 1);
/// ```
#[derive(Debug, Clone, Default)]
pub struct SequenceCounter(Arc<AtomicU16>);

impl SequenceCounter {
    /// Creates a counter starting at 0.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the current value and advances the counter.
    pub fn next(&self) -> u16 {
        self.0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                Some(if count >= SEQUENCE_COUNT_MAX { 0 } else { count + 1 })
            })
            .expect("update function always returns Some")
    }
}

/// Message type counters per (service type, subtype, destination ID), as
/// PUS-C specifies.
#[derive(Debug, Default)]
struct MessageTypeCounters(HashMap<(u8, u8, u16), u16>);

impl MessageTypeCounters {
    fn next(&mut self, service_type: u8, subtype: u8, destination_id: u16) -> u16 {
        let counter = self.0.entry((service_type, subtype, destination_id)).or_insert(0);
        let value = *counter;
        *counter = counter.wrapping_add(1);
        value
    }
}

fn now(actor: &str, apid: u16, format: CucFormat) -> Option<CucTime> {
    CucTime::now(format)
        .inspect_err(|err| error!(actor, apid, error = %err, "cannot create time stamp"))
        .ok()
}

/// Actor for an application with a fixed APID: performs the acceptance
/// check for every PUS telecommand addressed to its APID, reports the
/// result via service 1 to `tm_recipient`, and forwards accepted TCs to the
/// downstream actor registered for their service type (see
/// [`with_service_handler`](Self::with_service_handler)).
///
/// For each TC:
/// 1. No handler for the service type → TM(1,2) with
///    [`FailureCode::UnsupportedService`]; subtype not supported → TM(1,2)
///    with [`FailureCode::UnsupportedSubtype`]. The TC is dropped.
/// 2. Otherwise TM(1,1), if the TC has the acknowledgement flag
///    `acceptance` set.
/// 3. The TC is forwarded to the handler. If that fails (the handler actor
///    has stopped), TM(1,10) with [`FailureCode::RoutingFailed`] follows.
///
/// As PUS-C specifies, failure reports are generated regardless of the
/// acknowledgement flags; their failure data contain the service type and
/// subtype of the TC. TCs for other APIDs and telemetry packets are
/// ignored.
///
/// The reports carry the actor's own APID, the packet sequence count from
/// [`sequence_counter`](Self::sequence_counter), a message type counter per
/// (service type, subtype, destination ID), the source ID of the TC as
/// destination ID, and the current time as [`CucTime`].
///
/// Handles both [`PusPacket`] (e.g. directly as the `downstream` of a
/// [`PusListener`](crate::PusListener)) and [`PusTc`]. See
/// [`PusTestServiceActor`] for a complete example.
pub struct PusTcAcceptor {
    apid: u16,
    tm_recipient: Recipient<VerificationReport>,
    service_handlers: HashMap<u8, ServiceHandler>,
    time_format: CucFormat,
    sequence_counter: SequenceCounter,
    message_type_counters: MessageTypeCounters,
}

struct ServiceHandler {
    subtypes: Vec<u8>,
    recipient: Recipient<PusTc>,
}

impl PusTcAcceptor {
    /// Creates the actor for the application `apid`. The verification
    /// reports go to `tm_recipient`, with time stamps in
    /// [`CucFormat::default`].
    pub fn new(apid: u16, tm_recipient: Recipient<VerificationReport>) -> Self {
        PusTcAcceptor {
            apid,
            tm_recipient,
            service_handlers: HashMap::new(),
            time_format: CucFormat::default(),
            sequence_counter: SequenceCounter::new(),
            message_type_counters: MessageTypeCounters::default(),
        }
    }

    /// Accepts TCs with service type `service_type` and one of `subtypes`
    /// and forwards them to `handler`. TCs of other services or subtypes are
    /// rejected with TM(1,2).
    ///
    /// Registering the same service type again replaces the previous
    /// handler.
    pub fn with_service_handler(mut self, service_type: u8, subtypes: &[u8], handler: Recipient<PusTc>) -> Self {
        self.service_handlers
            .insert(service_type, ServiceHandler { subtypes: subtypes.to_vec(), recipient: handler });
        self
    }

    /// Uses `format` for the time stamps of the reports.
    pub fn with_time_format(mut self, format: CucFormat) -> Self {
        self.time_format = format;
        self
    }

    /// Uses `counter` as packet sequence count, e.g. to share it with other
    /// actors of the same APID.
    pub fn with_sequence_counter(mut self, counter: SequenceCounter) -> Self {
        self.sequence_counter = counter;
        self
    }

    /// The APID this actor is responsible for.
    pub fn apid(&self) -> u16 {
        self.apid
    }

    /// The packet sequence count of this actor, for sharing with downstream
    /// actors of the same APID.
    pub fn sequence_counter(&self) -> SequenceCounter {
        self.sequence_counter.clone()
    }

    /// Creates a verification report for `tc` and sends it to
    /// `tm_recipient`.
    async fn report(&mut self, tc: &PusTc, kind: VerificationKind) {
        let Some(time) = now("PusTcAcceptor", self.apid, self.time_format) else {
            return;
        };
        let subtype = kind.subtype();
        let mut report = VerificationReport::for_tc(self.apid, self.sequence_counter.next(), time, tc, kind);
        report.message_type_counter =
            self.message_type_counters.next(crate::pus::service1::SERVICE_TYPE, subtype, report.destination_id);
        if let Err(err) = self.tm_recipient.tell(report).await {
            warn!(apid = self.apid, error = %err, "could not send TM(1,{subtype})");
        }
    }

    async fn handle_tc(&mut self, tc: PusTc) {
        if tc.header.apid != self.apid {
            return;
        }
        let (service_type, subtype) = (tc.secondary_header.service_type, tc.secondary_header.message_subtype);
        let failure_data = [service_type, subtype];

        let rejection = match self.service_handlers.get(&service_type) {
            None => Some(FailureCode::UnsupportedService),
            Some(handler) if !handler.subtypes.contains(&subtype) => Some(FailureCode::UnsupportedSubtype),
            Some(_) => None,
        };
        if let Some(code) = rejection {
            let kind = VerificationKind::AcceptanceFailure(code.notice(failure_data.to_vec()));
            self.report(&tc, kind).await;
            return;
        }

        if tc.secondary_header.ack_flags.acceptance {
            self.report(&tc, VerificationKind::AcceptanceSuccess).await;
        }

        let request = tc.clone();
        let handler = &self.service_handlers[&service_type].recipient;
        if handler.tell(tc).await.is_err() {
            let kind = VerificationKind::RoutingFailure(FailureCode::RoutingFailed.notice(failure_data.to_vec()));
            self.report(&request, kind).await;
        }
    }
}

impl Actor for PusTcAcceptor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<PusTc> for PusTcAcceptor {
    type Reply = ();

    async fn handle(&mut self, tc: PusTc, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.handle_tc(tc).await;
    }
}

impl Message<PusPacket> for PusTcAcceptor {
    type Reply = ();

    async fn handle(&mut self, packet: PusPacket, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if let PusPacket::Tc(tc) = packet {
            self.handle_tc(tc).await;
        }
    }
}

/// Actor for PUS service 17 "Test": answers every TC(17,1) "Are-You-Alive"
/// first with TM(17,2) "Are-You-Alive Connection Report" and then, if the
/// TC has the acknowledgement flag `completion` set, with TM(1,7)
/// "Successful Completion of Execution Verification Report". Both packets
/// go to the same `tm_recipient` so that their order is preserved.
///
/// Intended as a downstream actor of a [`PusTcAcceptor`]:
///
/// ```
/// # use std::time::Duration;
/// # use kameo::actor::Spawn;
/// # use kameo_tcp_example::pus::service17;
/// # use kameo_tcp_example::{
/// #     AreYouAliveRequest, PusPacket, PusPacketAdapter, PusTc, PusTcAcceptor,
/// #     PusTestServiceActor, TestActor, VerificationReport,
/// # };
/// # #[tokio::main]
/// # async fn main() {
/// let apid = 0x042;
/// // The TM packets would typically go to a `PusWriter`; here a `TestActor`.
/// let tms = TestActor::<PusPacket>::spawn(TestActor::new());
/// let reports = PusPacketAdapter::<VerificationReport>::spawn(PusPacketAdapter::new(tms.clone().recipient()));
///
/// let acceptor = PusTcAcceptor::new(apid, reports.recipient());
/// let test_service = PusTestServiceActor::spawn(
///     PusTestServiceActor::new(apid, tms.clone().recipient())
///         .with_sequence_counter(acceptor.sequence_counter()),
/// );
/// let acceptor = PusTcAcceptor::spawn(
///     acceptor.with_service_handler(service17::SERVICE_TYPE, &[1], test_service.recipient::<PusTc>()),
/// );
///
/// acceptor.tell(PusTc::from(AreYouAliveRequest::new(apid, 0))).await.unwrap();
///
/// // TM(1,1) from the acceptor, TM(17,2) and TM(1,7) from the test service.
/// let received = TestActor::assert_received(&tms, 3, Duration::from_secs(1)).await;
/// let mut types: Vec<_> = received.iter().map(|p| (p.service_type(), p.message_subtype())).collect();
/// types.sort();
/// assert_eq!(types, vec![(1, 1), (1, 7), (17, 2)]);
/// # }
/// ```
///
/// Handles [`AreYouAliveRequest`] and [`PusTc`]. A TC that is not a valid
/// TC(17,1) is answered with TM(1,4) "Failed Start of Execution"
/// ([`FailureCode::UnsupportedSubtype`] or
/// [`FailureCode::InvalidApplicationData`]; failure data: service type and
/// subtype). It is TM(1,4) rather than TM(1,2) because the upstream
/// [`PusTcAcceptor`] has already accepted the TC.
pub struct PusTestServiceActor {
    apid: u16,
    tm_recipient: Recipient<PusPacket>,
    time_format: CucFormat,
    sequence_counter: SequenceCounter,
    message_type_counters: MessageTypeCounters,
}

impl PusTestServiceActor {
    /// Creates the actor for the application `apid`. The TM packets go to
    /// `tm_recipient`, with time stamps in [`CucFormat::default`].
    pub fn new(apid: u16, tm_recipient: Recipient<PusPacket>) -> Self {
        PusTestServiceActor {
            apid,
            tm_recipient,
            time_format: CucFormat::default(),
            sequence_counter: SequenceCounter::new(),
            message_type_counters: MessageTypeCounters::default(),
        }
    }

    /// Uses `format` for the time stamps of the reports.
    pub fn with_time_format(mut self, format: CucFormat) -> Self {
        self.time_format = format;
        self
    }

    /// Uses `counter` as packet sequence count, typically the one of the
    /// upstream [`PusTcAcceptor`] (see
    /// [`PusTcAcceptor::sequence_counter`]).
    pub fn with_sequence_counter(mut self, counter: SequenceCounter) -> Self {
        self.sequence_counter = counter;
        self
    }

    async fn send(&self, packet: PusPacket, name: &str) {
        if let Err(err) = self.tm_recipient.tell(packet).await {
            warn!(apid = self.apid, error = %err, "could not send {name}");
        }
    }

    /// Creates a verification report for the TC `request_id` and sends it
    /// to `tm_recipient`.
    async fn report(&mut self, request_id: RequestId, destination_id: u16, time: CucTime, kind: VerificationKind) {
        let subtype = kind.subtype();
        let mut report = VerificationReport::new(self.apid, self.sequence_counter.next(), time, request_id, kind);
        report.destination_id = destination_id;
        report.message_type_counter =
            self.message_type_counters.next(crate::pus::service1::SERVICE_TYPE, subtype, destination_id);
        match PusPacket::try_from(report) {
            Ok(packet) => self.send(packet, &format!("TM(1,{subtype})")).await,
            Err(err) => error!(apid = self.apid, error = %err, "cannot encode TM(1,{subtype})"),
        }
    }

    async fn handle_request(&mut self, request: AreYouAliveRequest) {
        let Some(time) = now("PusTestServiceActor", self.apid, self.time_format) else {
            return;
        };

        let mut alive = AreYouAliveReport::for_request(self.apid, self.sequence_counter.next(), time, &request);
        alive.message_type_counter = self.message_type_counters.next(
            crate::pus::service17::SERVICE_TYPE,
            crate::pus::service17::ARE_YOU_ALIVE_REPORT_SUBTYPE,
            alive.destination_id,
        );
        self.send(alive.into(), "TM(17,2)").await;

        if request.ack_flags.completion {
            let request_id = RequestId::from_header(&request.header);
            self.report(request_id, request.source_id, time, VerificationKind::CompletionSuccess).await;
        }
    }

    async fn handle_tc(&mut self, tc: PusTc) {
        let request_id = RequestId::from(&tc);
        let destination_id = tc.secondary_header.source_id;
        let (service_type, subtype) = (tc.secondary_header.service_type, tc.secondary_header.message_subtype);

        if let Ok(request) = AreYouAliveRequest::try_from(tc) {
            return self.handle_request(request).await;
        }

        let code = if (service_type, subtype)
            == (crate::pus::service17::SERVICE_TYPE, crate::pus::service17::ARE_YOU_ALIVE_REQUEST_SUBTYPE)
        {
            FailureCode::InvalidApplicationData
        } else {
            FailureCode::UnsupportedSubtype
        };
        let Some(time) = now("PusTestServiceActor", self.apid, self.time_format) else {
            return;
        };
        let kind = VerificationKind::StartFailure(code.notice(vec![service_type, subtype]));
        self.report(request_id, destination_id, time, kind).await;
    }
}

impl Actor for PusTestServiceActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<AreYouAliveRequest> for PusTestServiceActor {
    type Reply = ();

    async fn handle(&mut self, request: AreYouAliveRequest, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.handle_request(request).await;
    }
}

impl Message<PusTc> for PusTestServiceActor {
    type Reply = ();

    async fn handle(&mut self, tc: PusTc, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.handle_tc(tc).await;
    }
}

/// Adapter actor: converts typed PUS messages `T` (e.g.
/// [`VerificationReport`]) into a [`PusPacket`] via `TryFrom` and forwards
/// it to `target`, typically a [`PusWriter`](crate::PusWriter) that sends
/// it over TCP.
///
/// ```
/// # use std::time::Duration;
/// # use kameo::actor::Spawn;
/// # use kameo_tcp_example::{
/// #     PusPacket, PusPacketAdapter, PusTc, RequestId, TestActor, VerificationKind,
/// #     VerificationReport,
/// # };
/// # #[tokio::main]
/// # async fn main() {
/// // The target would typically be a `PusWriter`; here a `TestActor`.
/// let target = TestActor::<PusPacket>::spawn(TestActor::new());
/// let adapter = PusPacketAdapter::<VerificationReport>::spawn(
///     PusPacketAdapter::new(target.clone().recipient::<PusPacket>()),
/// );
///
/// let tc = PusTc::new(0x042, 0, 17, 1, bytes::Bytes::new());
/// let report = VerificationReport::for_tc(0x042, 0, vec![0u8; 7], &tc, VerificationKind::AcceptanceSuccess);
/// adapter.tell(report.clone()).await.unwrap();
///
/// let received = TestActor::assert_received(&target, 1, Duration::from_secs(1)).await;
/// assert_eq!(received, vec![PusPacket::try_from(report).unwrap()]);
/// # }
/// ```
///
/// Messages that cannot be converted are dropped and logged as an error.
pub struct PusPacketAdapter<T> {
    target: Recipient<PusPacket>,
    _marker: PhantomData<fn(T)>,
}

impl<T> PusPacketAdapter<T> {
    /// Creates an adapter that forwards to `target`.
    pub fn new(target: Recipient<PusPacket>) -> Self {
        PusPacketAdapter { target, _marker: PhantomData }
    }
}

impl<T> Actor for PusPacketAdapter<T>
where
    T: Send + 'static,
{
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl<T> Message<T> for PusPacketAdapter<T>
where
    T: Send + 'static,
    PusPacket: TryFrom<T>,
    <PusPacket as TryFrom<T>>::Error: Display,
{
    type Reply = ();

    async fn handle(&mut self, msg: T, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let packet = match PusPacket::try_from(msg) {
            Ok(packet) => packet,
            Err(err) => {
                error!(error = %err, "message cannot be converted into a PUS packet");
                return;
            }
        };
        if let Err(err) = self.target.tell(packet).await {
            warn!(error = %err, "could not forward PUS packet");
        }
    }
}
