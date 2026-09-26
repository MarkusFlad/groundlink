//! Actors, die PUS-Pakete fachlich verarbeiten.
//!
//! Typische Kette für eine Applikation mit fester APID:
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

use crate::ccsds::SEQUENCE_COUNT_MAX;
use crate::cuc::{CucFormat, CucTime};
use crate::pus::service1::{RequestId, VerificationKind, VerificationReport};
use crate::pus::service17::{AreYouAliveReport, AreYouAliveRequest};
use crate::pus::{PusPacket, PusTc};

/// Packet Sequence Count einer APID, den sich mehrere Actors teilen
/// können, damit alle TM-Pakete dieser APID fortlaufend nummeriert sind.
/// Läuft nach [`SEQUENCE_COUNT_MAX`] auf 0 über.
#[derive(Debug, Clone, Default)]
pub struct SequenceCounter(Arc<AtomicU16>);

impl SequenceCounter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Liefert den aktuellen Wert und zählt weiter.
    pub fn next(&self) -> u16 {
        self.0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                Some(if count >= SEQUENCE_COUNT_MAX { 0 } else { count + 1 })
            })
            .expect("Update-Funktion liefert immer Some")
    }
}

/// Message Type Counter je (Service Type, Subtype, Destination ID), wie
/// von PUS-C vorgesehen.
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
        .inspect_err(|err| eprintln!("{actor} (APID {apid}): Zeitstempel nicht erzeugbar: {err}"))
        .ok()
}

/// Actor für eine Applikation mit fester APID: nimmt PUS-Telekommandos
/// entgegen, bestätigt jedes an seine APID adressierte TC mit einem
/// TM(1,1) "Successful Acceptance Verification Report" an
/// `tm_recipient` und leitet es danach an den für seinen Service Type
/// registrierten, nachgelagerten Actor weiter (siehe
/// [`with_service_handler`](Self::with_service_handler)).
///
/// - TCs an andere APIDs und Telemetriepakete werden ignoriert.
/// - Wie von PUS vorgeschrieben, wird TM(1,1) nur erzeugt, wenn im TC das
///   Acknowledgement Flag `acceptance` gesetzt ist. Weitergeleitet wird
///   das TC unabhängig davon.
/// - Die Berichte tragen die eigene APID, den Packet Sequence Count aus
///   [`sequence_counter`](Self::sequence_counter), einen Message Type
///   Counter je Destination ID, die Source ID des TC als Destination ID
///   und die aktuelle Zeit als [`CucTime`].
///
/// Verarbeitet sowohl [`PusPacket`] (z. B. direkt als `downstream` eines
/// [`crate::PusListener`]) als auch [`PusTc`].
pub struct PusTcAcceptor {
    apid: u16,
    tm_recipient: Recipient<VerificationReport>,
    service_handlers: HashMap<u8, Recipient<PusTc>>,
    time_format: CucFormat,
    sequence_counter: SequenceCounter,
    message_type_counters: MessageTypeCounters,
}

impl PusTcAcceptor {
    /// Erstellt den Actor für die Applikation `apid`; die TM(1,1)-Berichte
    /// gehen an `tm_recipient`. Zeitstempel im [`CucFormat::default`].
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

    /// Leitet angenommene TCs mit Service Type `service_type` an `handler`
    /// weiter. TCs ohne registrierten Handler werden nur bestätigt.
    pub fn with_service_handler(mut self, service_type: u8, handler: Recipient<PusTc>) -> Self {
        self.service_handlers.insert(service_type, handler);
        self
    }

    /// Verwendet `format` für die Zeitstempel der Berichte.
    pub fn with_time_format(mut self, format: CucFormat) -> Self {
        self.time_format = format;
        self
    }

    /// Verwendet `counter` als Packet Sequence Count, z. B. um ihn mit
    /// anderen Actors derselben APID zu teilen.
    pub fn with_sequence_counter(mut self, counter: SequenceCounter) -> Self {
        self.sequence_counter = counter;
        self
    }

    pub fn apid(&self) -> u16 {
        self.apid
    }

    /// Der Packet Sequence Count dieses Actors, zum Teilen mit
    /// nachgelagerten Actors derselben APID.
    pub fn sequence_counter(&self) -> SequenceCounter {
        self.sequence_counter.clone()
    }

    /// Erzeugt den TM(1,1)-Bericht zu `tc` und zählt die Zähler weiter,
    /// oder `None`, wenn kein Bericht angefordert ist.
    fn acceptance_report(&mut self, tc: &PusTc) -> Option<VerificationReport> {
        if !tc.secondary_header.ack_flags.acceptance {
            return None;
        }
        let time = now("PusTcAcceptor", self.apid, self.time_format)?;
        let kind = VerificationKind::AcceptanceSuccess;
        let subtype = kind.subtype();
        let mut report = VerificationReport::for_tc(self.apid, self.sequence_counter.next(), time, tc, kind);
        report.message_type_counter =
            self.message_type_counters.next(crate::pus::service1::SERVICE_TYPE, subtype, report.destination_id);
        Some(report)
    }

    async fn handle_tc(&mut self, tc: PusTc) {
        if tc.header.apid != self.apid {
            return;
        }
        if let Some(report) = self.acceptance_report(&tc) {
            if let Err(err) = self.tm_recipient.tell(report).await {
                eprintln!("PusTcAcceptor (APID {}): Konnte TM(1,1) nicht senden: {err}", self.apid);
            }
        }
        let service_type = tc.secondary_header.service_type;
        if let Some(handler) = self.service_handlers.get(&service_type) {
            if let Err(err) = handler.tell(tc).await {
                eprintln!(
                    "PusTcAcceptor (APID {}): Konnte TC an Service-{service_type}-Handler nicht weiterleiten: {err}",
                    self.apid
                );
            }
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

/// Actor für PUS Service 17 "Test": beantwortet jedes TC(17,1)
/// "Are-You-Alive" zuerst mit TM(17,2) "Are-You-Alive Connection Report"
/// und danach – falls im TC das Acknowledgement Flag `completion` gesetzt
/// ist – mit TM(1,7) "Successful Completion of Execution Verification
/// Report". Beide Pakete gehen an denselben `tm_recipient`, damit ihre
/// Reihenfolge erhalten bleibt.
///
/// Gedacht als nachgelagerter Actor eines [`PusTcAcceptor`]:
///
/// ```ignore
/// let acceptor = PusTcAcceptor::new(apid, reports_recipient);
/// let test_service = PusTestServiceActor::spawn(
///     PusTestServiceActor::new(apid, writer.recipient::<PusPacket>())
///         .with_sequence_counter(acceptor.sequence_counter()),
/// );
/// let acceptor = PusTcAcceptor::spawn(
///     acceptor.with_service_handler(service17::SERVICE_TYPE, test_service.recipient::<PusTc>()),
/// );
/// ```
///
/// Verarbeitet [`AreYouAliveRequest`] und [`PusTc`]; andere TCs als (17,1)
/// werden mit einer Fehlermeldung verworfen.
pub struct PusTestServiceActor {
    apid: u16,
    tm_recipient: Recipient<PusPacket>,
    time_format: CucFormat,
    sequence_counter: SequenceCounter,
    message_type_counters: MessageTypeCounters,
}

impl PusTestServiceActor {
    /// Erstellt den Actor für die Applikation `apid`; die TM-Pakete gehen
    /// an `tm_recipient`. Zeitstempel im [`CucFormat::default`].
    pub fn new(apid: u16, tm_recipient: Recipient<PusPacket>) -> Self {
        PusTestServiceActor {
            apid,
            tm_recipient,
            time_format: CucFormat::default(),
            sequence_counter: SequenceCounter::new(),
            message_type_counters: MessageTypeCounters::default(),
        }
    }

    /// Verwendet `format` für die Zeitstempel der Berichte.
    pub fn with_time_format(mut self, format: CucFormat) -> Self {
        self.time_format = format;
        self
    }

    /// Verwendet `counter` als Packet Sequence Count – typischerweise den
    /// des vorgelagerten [`PusTcAcceptor`] (siehe
    /// [`PusTcAcceptor::sequence_counter`]).
    pub fn with_sequence_counter(mut self, counter: SequenceCounter) -> Self {
        self.sequence_counter = counter;
        self
    }

    async fn send(&self, packet: PusPacket, name: &str) {
        if let Err(err) = self.tm_recipient.tell(packet).await {
            eprintln!("PusTestServiceActor (APID {}): Konnte {name} nicht senden: {err}", self.apid);
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

        if !request.ack_flags.completion {
            return;
        }
        let kind = VerificationKind::CompletionSuccess;
        let subtype = kind.subtype();
        let mut completion = VerificationReport::new(
            self.apid,
            self.sequence_counter.next(),
            time,
            RequestId::from_header(&request.header),
            kind,
        );
        completion.destination_id = request.source_id;
        completion.message_type_counter =
            self.message_type_counters.next(crate::pus::service1::SERVICE_TYPE, subtype, completion.destination_id);
        match PusPacket::try_from(completion) {
            Ok(packet) => self.send(packet, "TM(1,7)").await,
            Err(err) => eprintln!("PusTestServiceActor (APID {}): TM(1,7) nicht kodierbar: {err}", self.apid),
        }
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
        match AreYouAliveRequest::try_from(tc) {
            Ok(request) => self.handle_request(request).await,
            Err(err) => eprintln!("PusTestServiceActor (APID {}): TC verworfen: {err}", self.apid),
        }
    }
}

/// Adapter-Actor: wandelt typisierte PUS-Nachrichten `T` (z. B.
/// [`VerificationReport`]) per `TryFrom` in ein [`PusPacket`] um und
/// leitet es an `target` weiter – typischerweise einen
/// [`crate::PusWriter`], der es über TCP verschickt.
///
/// ```ignore
/// let writer = PusWriter::spawn(writer_args);
/// let adapter = PusPacketAdapter::<VerificationReport>::spawn(
///     PusPacketAdapter::new(writer.recipient::<PusPacket>()),
/// );
/// let acceptor = PusTcAcceptor::spawn(PusTcAcceptor::new(apid, adapter.recipient()));
/// ```
///
/// Nachrichten, die sich nicht umwandeln lassen, werden mit einer
/// Fehlermeldung verworfen.
pub struct PusPacketAdapter<T> {
    target: Recipient<PusPacket>,
    _marker: PhantomData<fn(T)>,
}

impl<T> PusPacketAdapter<T> {
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
                eprintln!("PusPacketAdapter: Nachricht nicht als PUS-Paket darstellbar: {err}");
                return;
            }
        };
        if let Err(err) = self.target.tell(packet).await {
            eprintln!("PusPacketAdapter: Konnte PUS-Paket nicht weiterleiten: {err}");
        }
    }
}
