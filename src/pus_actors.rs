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
use crate::pus::service1::{FailureCode, RequestId, VerificationKind, VerificationReport};
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

/// Actor für eine Applikation mit fester APID: prüft jedes an seine APID
/// adressierte PUS-Telekommando auf Annahme, meldet das Ergebnis per
/// Service 1 an `tm_recipient` und leitet angenommene TCs an den für ihren
/// Service Type registrierten, nachgelagerten Actor weiter (siehe
/// [`with_service_handler`](Self::with_service_handler)).
///
/// Ablauf je TC:
/// 1. Kein Handler für den Service Type → TM(1,2) mit
///    [`FailureCode::UnsupportedService`]; Subtype nicht unterstützt →
///    TM(1,2) mit [`FailureCode::UnsupportedSubtype`]. Das TC wird
///    verworfen.
/// 2. Sonst TM(1,1), sofern im TC das Acknowledgement Flag `acceptance`
///    gesetzt ist.
/// 3. Weiterleitung an den Handler; scheitert sie (Handler-Actor beendet),
///    folgt TM(1,10) mit [`FailureCode::RoutingFailed`].
///
/// Fehlerberichte werden wie von PUS-C vorgesehen unabhängig von den
/// Acknowledgement Flags erzeugt; ihre Failure Data enthalten Service
/// Type und Subtype des TC. TCs an andere APIDs und Telemetriepakete
/// werden ignoriert.
///
/// Die Berichte tragen die eigene APID, den Packet Sequence Count aus
///   [`sequence_counter`](Self::sequence_counter), einen Message Type
///   Counter je Destination ID, die Source ID des TC als Destination ID
///   und die aktuelle Zeit als [`CucTime`].
///
/// Verarbeitet sowohl [`PusPacket`] (z. B. direkt als `downstream` eines
/// [`crate::PusListener`]) als auch [`PusTc`].
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

    /// Nimmt TCs mit Service Type `service_type` und einem der
    /// `subtypes` an und leitet sie an `handler` weiter. TCs anderer
    /// Services oder Subtypes werden mit TM(1,2) abgelehnt.
    pub fn with_service_handler(mut self, service_type: u8, subtypes: &[u8], handler: Recipient<PusTc>) -> Self {
        self.service_handlers
            .insert(service_type, ServiceHandler { subtypes: subtypes.to_vec(), recipient: handler });
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

    /// Erzeugt einen Verifikationsbericht zu `tc` und schickt ihn an
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
            eprintln!("PusTcAcceptor (APID {}): Konnte TM(1,{subtype}) nicht senden: {err}", self.apid);
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
///     acceptor.with_service_handler(service17::SERVICE_TYPE, &[1], test_service.recipient::<PusTc>()),
/// );
/// ```
///
/// Verarbeitet [`AreYouAliveRequest`] und [`PusTc`]. Ein TC, das kein
/// gültiges TC(17,1) ist, wird mit TM(1,4) "Failed Start of Execution"
/// beantwortet ([`FailureCode::UnsupportedSubtype`] bzw.
/// [`FailureCode::InvalidApplicationData`], Failure Data: Service Type und
/// Subtype) – TM(1,4) statt TM(1,2), weil der vorgelagerte
/// [`PusTcAcceptor`] das TC bereits angenommen hat.
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

    /// Erzeugt einen Verifikationsbericht zum TC `request_id` und schickt
    /// ihn an `tm_recipient`.
    async fn report(&mut self, request_id: RequestId, destination_id: u16, time: CucTime, kind: VerificationKind) {
        let subtype = kind.subtype();
        let mut report = VerificationReport::new(self.apid, self.sequence_counter.next(), time, request_id, kind);
        report.destination_id = destination_id;
        report.message_type_counter =
            self.message_type_counters.next(crate::pus::service1::SERVICE_TYPE, subtype, destination_id);
        match PusPacket::try_from(report) {
            Ok(packet) => self.send(packet, &format!("TM(1,{subtype})")).await,
            Err(err) => eprintln!("PusTestServiceActor (APID {}): TM(1,{subtype}) nicht kodierbar: {err}", self.apid),
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
