//! Actors that process PUS packets for an application process.
//!
//! Typical chain for an application with a fixed APID:
//!
//! ```text
//! SpacePacketServer ──SpacePacket──▶ PusTcAcceptor ──TM(1,1), TM(1,2)──────────────▶ PusTmStamper ──PusPacket──▶ PusServer
//!                                         │                                               ▲
//!                                         └─PusTc (Service 17)──▶ PusTestServiceActor ──TM(1,3), TM(17,2), TM(1,7)
//! ```
//!
//! The service actors send unstamped [`PusTm`]s. The [`PusTmStamper`] of
//! the APID assigns the packet sequence count, the message type counter
//! and the time stamp, in the order in which the packets arrive, so that
//! the telemetry of the APID is numbered consecutively on the wire.

use std::collections::HashMap;
use std::fmt::Display;
use std::marker::PhantomData;

use kameo::actor::{Actor, ActorRef, Recipient};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use tracing::{error, warn};

use bytes::Bytes;

use crate::ccsds::{PacketType, SpacePacket, SEQUENCE_COUNT_MAX};
use crate::cuc::{CucFormat, CucTime};
use crate::pus::service1::{FailureCode, RequestId, VerificationKind, VerificationReport};
use crate::pus::service17::{AreYouAliveReport, AreYouAliveRequest};
use crate::pus::{PusConfig, PusPacket, PusTc, PusTm};

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

/// Sends an unstamped verification report for the TC `request_id` to
/// `tm_recipient`.
async fn send_report(
    apid: u16,
    tm_recipient: &Recipient<PusTm>,
    request_id: RequestId,
    destination_id: u16,
    kind: VerificationKind,
) {
    let subtype = kind.subtype();
    let mut report = VerificationReport::new(apid, 0, Bytes::new(), request_id, kind);
    report.destination_id = destination_id;
    let tm = match PusTm::try_from(report) {
        Ok(tm) => tm,
        Err(err) => {
            error!(apid, error = %err, "cannot encode TM(1,{subtype})");
            return;
        }
    };
    if let Err(err) = tm_recipient.tell(tm).await {
        warn!(apid, error = %err, "could not send TM(1,{subtype})");
    }
}

/// Actor that stamps the telemetry of one APID and forwards it as
/// [`PusPacket`]s to `target`, e.g. a [`PusServer`](crate::PusServer).
///
/// For every [`PusTm`] it receives, in the order of arrival, it sets
/// - the APID to its own APID,
/// - the packet sequence count (consecutive, wrapping around to 0 after
///   [`SEQUENCE_COUNT_MAX`]),
/// - the message type counter per (service type, subtype, destination
///   ID), as PUS-C specifies, and
/// - the time stamp to the current time as [`CucTime`] (default format:
///   [`CucFormat::default`]).
///
/// All actors that produce telemetry for the APID (e.g. [`PusTcAcceptor`]
/// and [`PusTestServiceActor`]) should send it to the same stamper. The
/// counters then have no gaps or duplicates, and the sequence counts
/// increase in the order in which the packets are sent. If no time stamp
/// can be created, the packet is dropped and logged as an error.
///
/// ```
/// # use std::time::Duration;
/// # use kameo::actor::Spawn;
/// # use groundlink::{PusPacket, PusTm, PusTmStamper, TestActor};
/// # #[tokio::main]
/// # async fn main() {
/// // The packets would typically go to a `PusServer`; here a `TestActor`.
/// let target = TestActor::<PusPacket>::spawn(TestActor::new());
/// let stamper = PusTmStamper::spawn(PusTmStamper::new(0x042, target.clone().recipient::<PusPacket>()));
///
/// for _ in 0..2 {
///     stamper.tell(PusTm::new(0x042, 0, 17, 2, bytes::Bytes::new(), bytes::Bytes::new())).await.unwrap();
/// }
///
/// let received = TestActor::assert_received(&target, 2, Duration::from_secs(1)).await;
/// let PusPacket::Tm(second) = &received[1] else { unreachable!() };
/// assert_eq!(second.header.sequence_count, 1);
/// assert_eq!(second.secondary_header.message_type_counter, 1);
/// # }
/// ```
pub struct PusTmStamper {
    apid: u16,
    target: Recipient<PusPacket>,
    time_format: CucFormat,
    sequence_count: u16,
    message_type_counters: MessageTypeCounters,
}

impl PusTmStamper {
    /// Creates the stamper for the application `apid`. The stamped packets
    /// go to `target`.
    pub fn new(apid: u16, target: Recipient<PusPacket>) -> Self {
        PusTmStamper {
            apid,
            target,
            time_format: CucFormat::default(),
            sequence_count: 0,
            message_type_counters: MessageTypeCounters::default(),
        }
    }

    /// Uses `format` for the time stamps. Its length must equal
    /// [`PusConfig::tm_time_len`] of the [`PusCodec`](crate::PusCodec) that
    /// encodes the packets, otherwise encoding fails.
    pub fn with_time_format(mut self, format: CucFormat) -> Self {
        self.time_format = format;
        self
    }

    fn next_sequence_count(&mut self) -> u16 {
        let count = self.sequence_count;
        self.sequence_count = if count >= SEQUENCE_COUNT_MAX { 0 } else { count + 1 };
        count
    }
}

impl Actor for PusTmStamper {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<PusTm> for PusTmStamper {
    type Reply = ();

    async fn handle(&mut self, mut tm: PusTm, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let Some(time) = now("PusTmStamper", self.apid, self.time_format) else {
            return;
        };
        let header = &mut tm.secondary_header;
        let (service_type, subtype) = (header.service_type, header.message_subtype);
        header.message_type_counter = self.message_type_counters.next(service_type, subtype, header.destination_id);
        header.time = time.into();
        tm.header.apid = self.apid;
        tm.header.sequence_count = self.next_sequence_count();

        if let Err(err) = self.target.tell(PusPacket::Tm(tm)).await {
            warn!(apid = self.apid, error = %err, "could not send TM({service_type},{subtype})");
        }
    }
}

/// Actor for an application with a fixed APID: performs the acceptance
/// check for every PUS telecommand addressed to its APID, reports the
/// result via service 1 to `tm_recipient` (typically a [`PusTmStamper`]),
/// and forwards accepted TCs to the
/// downstream actor registered for their service type (see
/// [`with_service_handler`](Self::with_service_handler)).
///
/// For each TC:
/// 1. Only for a [`SpacePacket`]: if it is not a valid PUS packet (see
///    [`PusPacket::from_space_packet`]), TM(1,2) with the
///    [`FailureCode`] for the [`PusDecodeError`](crate::PusDecodeError)
///    (e.g. [`FailureCode::ChecksumError`]) and empty failure data. The
///    request ID comes from the primary header; the destination ID is 0,
///    because the source ID of the TC is unknown. The TC is dropped.
/// 2. No handler for the service type → TM(1,2) with
///    [`FailureCode::UnsupportedService`]; subtype not supported → TM(1,2)
///    with [`FailureCode::UnsupportedSubtype`]. The TC is dropped.
/// 3. Otherwise TM(1,1), if the TC has the acknowledgement flag
///    `acceptance` set.
/// 4. The TC is forwarded to the handler. If that fails (the handler actor
///    has stopped), TM(1,10) with [`FailureCode::RoutingFailed`] follows.
///
/// As PUS-C specifies, failure reports are generated regardless of the
/// acknowledgement flags; except for step 1, their failure data contain the
/// service type and subtype of the TC. TCs for other APIDs and telemetry
/// packets are ignored.
///
/// The reports are unstamped [`PusTm`]s: they carry the actor's own APID
/// and the source ID of the TC as destination ID; the packet sequence
/// count, the message type counter and the time stamp are left for the
/// [`PusTmStamper`].
///
/// If the service handlers send their telemetry to the same
/// [`PusTmStamper`], TM(1,1) is guaranteed to arrive before the handlers'
/// telemetry for the same TC, because it is queued before the TC is
/// forwarded.
///
/// Handles [`SpacePacket`] (e.g. as the `downstream` of a
/// [`SpacePacketServer`](crate::SpacePacketServer)), [`PusPacket`] and
/// [`PusTc`]. Prefer [`SpacePacket`] for TCs received from outside, so
/// that invalid packets are answered with TM(1,2) instead of being dropped
/// by the codec. See [`PusTestServiceActor`] for a complete example.
pub struct PusTcAcceptor {
    apid: u16,
    tm_recipient: Recipient<PusTm>,
    service_handlers: HashMap<u8, ServiceHandler>,
    pus_config: PusConfig,
}

struct ServiceHandler {
    subtypes: Vec<u8>,
    recipient: Recipient<PusTc>,
}

impl PusTcAcceptor {
    /// Creates the actor for the application `apid`. The verification
    /// reports go to `tm_recipient` as unstamped [`PusTm`]s, typically to
    /// a [`PusTmStamper`].
    pub fn new(apid: u16, tm_recipient: Recipient<PusTm>) -> Self {
        PusTcAcceptor { apid, tm_recipient, service_handlers: HashMap::new(), pus_config: PusConfig::default() }
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

    /// Uses `config` to interpret received [`SpacePacket`]s (default:
    /// [`PusConfig::default`]). Use the same configuration as the
    /// [`PusCodec`](crate::PusCodec) of the peer.
    pub fn with_pus_config(mut self, config: PusConfig) -> Self {
        self.pus_config = config;
        self
    }

    /// The APID this actor is responsible for.
    pub fn apid(&self) -> u16 {
        self.apid
    }

    /// Creates a verification report for `tc` and sends it to
    /// `tm_recipient`.
    async fn report(&mut self, tc: &PusTc, kind: VerificationKind) {
        self.send_report(RequestId::from(tc), tc.secondary_header.source_id, kind).await;
    }

    /// Creates a verification report for the TC identified by `request_id`
    /// and sends it to `tm_recipient`.
    async fn send_report(&mut self, request_id: RequestId, destination_id: u16, kind: VerificationKind) {
        send_report(self.apid, &self.tm_recipient, request_id, destination_id, kind).await;
    }

    async fn handle_space_packet(&mut self, packet: SpacePacket) {
        let header = packet.header;
        if header.packet_type != PacketType::Telecommand || header.apid != self.apid {
            return;
        }
        match PusPacket::from_space_packet(packet, &self.pus_config) {
            Ok(PusPacket::Tc(tc)) => self.handle_tc(tc).await,
            Ok(PusPacket::Tm(_)) => {}
            Err(err) => {
                warn!(apid = self.apid, sequence_count = header.sequence_count, error = %err, "rejecting invalid TC");
                let kind = VerificationKind::AcceptanceFailure(FailureCode::from(&err).notice(Bytes::new()));
                self.send_report(RequestId::from_header(&header), 0, kind).await;
            }
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

impl Message<SpacePacket> for PusTcAcceptor {
    type Reply = ();

    async fn handle(&mut self, packet: SpacePacket, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.handle_space_packet(packet).await;
    }
}

/// Actor for PUS service 17 "Test": answers every TC(17,1) "Are-You-Alive"
/// with, in this order,
/// 1. TM(1,3) "Successful Start of Execution Verification Report", if the
///    TC has the acknowledgement flag `start` set,
/// 2. TM(17,2) "Are-You-Alive Connection Report", and
/// 3. TM(1,7) "Successful Completion of Execution Verification Report", if
///    the TC has the acknowledgement flag `completion` set.
///
/// TC(17,1) has no execution steps, so no progress reports (TM(1,5)) are
/// generated. All packets go as unstamped [`PusTm`]s to the same
/// `tm_recipient`, typically the [`PusTmStamper`] of the APID, so that
/// their order is preserved.
///
/// Intended as a downstream actor of a [`PusTcAcceptor`]:
///
/// ```
/// # use std::time::Duration;
/// # use kameo::actor::Spawn;
/// # use groundlink::pus::service17;
/// # use groundlink::{
/// #     AreYouAliveRequest, PusPacket, PusTc, PusTcAcceptor, PusTestServiceActor, PusTm,
/// #     PusTmStamper, TestActor,
/// # };
/// # #[tokio::main]
/// # async fn main() {
/// let apid = 0x042;
/// // The TM packets would typically go to a `PusServer`; here a `TestActor`.
/// let tms = TestActor::<PusPacket>::spawn(TestActor::new());
/// let stamper = PusTmStamper::spawn(PusTmStamper::new(apid, tms.clone().recipient::<PusPacket>()));
///
/// let test_service = PusTestServiceActor::spawn(PusTestServiceActor::new(apid, stamper.clone().recipient::<PusTm>()));
/// let acceptor = PusTcAcceptor::spawn(
///     PusTcAcceptor::new(apid, stamper.recipient::<PusTm>())
///         .with_service_handler(service17::SERVICE_TYPE, &[1], test_service.recipient::<PusTc>()),
/// );
///
/// acceptor.tell(PusTc::from(AreYouAliveRequest::new(apid, 0))).await.unwrap();
///
/// // TM(1,1) from the acceptor, then TM(1,3), TM(17,2) and TM(1,7) from
/// // the test service, numbered consecutively by the stamper.
/// let received = TestActor::assert_received(&tms, 4, Duration::from_secs(1)).await;
/// let types: Vec<_> = received.iter().map(|p| (p.service_type(), p.message_subtype())).collect();
/// assert_eq!(types, vec![(1, 1), (1, 3), (17, 2), (1, 7)]);
/// let counts: Vec<_> = received.iter().map(|p| p.header().sequence_count).collect();
/// assert_eq!(counts, vec![0, 1, 2, 3]);
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
    tm_recipient: Recipient<PusTm>,
}

impl PusTestServiceActor {
    /// Creates the actor for the application `apid`. The TM packets go to
    /// `tm_recipient` as unstamped [`PusTm`]s, typically to a
    /// [`PusTmStamper`].
    pub fn new(apid: u16, tm_recipient: Recipient<PusTm>) -> Self {
        PusTestServiceActor { apid, tm_recipient }
    }

    async fn handle_request(&mut self, request: AreYouAliveRequest) {
        let request_id = RequestId::from_header(&request.header);
        if request.ack_flags.start {
            send_report(self.apid, &self.tm_recipient, request_id, request.source_id, VerificationKind::StartSuccess)
                .await;
        }

        let alive = AreYouAliveReport::for_request(self.apid, 0, Bytes::new(), &request);
        if let Err(err) = self.tm_recipient.tell(PusTm::from(alive)).await {
            warn!(apid = self.apid, error = %err, "could not send TM(17,2)");
        }

        if request.ack_flags.completion {
            send_report(self.apid, &self.tm_recipient, request_id, request.source_id, VerificationKind::CompletionSuccess)
                .await;
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
        let kind = VerificationKind::StartFailure(code.notice(vec![service_type, subtype]));
        send_report(self.apid, &self.tm_recipient, request_id, destination_id, kind).await;
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
/// it to `target`, e.g. a [`PusServer`](crate::PusServer) that sends it
/// over TCP.
///
/// ```
/// # use std::time::Duration;
/// # use kameo::actor::Spawn;
/// # use groundlink::{
/// #     PusPacket, PusPacketAdapter, PusTc, RequestId, TestActor, VerificationKind,
/// #     VerificationReport,
/// # };
/// # #[tokio::main]
/// # async fn main() {
/// // The target would typically be a `PusServer`; here a `TestActor`.
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
