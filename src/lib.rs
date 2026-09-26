//! Generische TCP-Actors + zwei Protokolle:
//!
//! - [`TcpListenerActor<M, C>`] akzeptiert Verbindungen und spawnt pro
//!   Verbindung je einen [`TcpConnectionActor<M, C>`] (liest) und
//!   [`TcpWriterActor<M, C>`] (schreibt). `M` ist der Nachrichtentyp
//!   (z. B. [`SimpleString`] oder [`SpacePacket`]), `C` der zugehörige
//!   [`MessageCodec<M>`] (z. B. [`SimpleStringCodec`] oder
//!   [`SpacePacketCodec`]). Schließt eine Hälfte (Fehler oder graceful),
//!   beendet sich der jeweilige Actor und meldet dies per
//!   [`ConnectionHalfClosed`] an den `TcpListenerActor`. Erst wenn read UND
//!   write half einer Verbindung als geschlossen gemeldet wurden,
//!   akzeptiert der `TcpListenerActor` die nächste Verbindung.
//! - [`TcpClientActor<M, C>`] ist das Gegenstück für ausgehende
//!   Verbindungen: hält `remote_addr`, baut bei [`Connect`] die Verbindung
//!   auf (spawnt dieselben `TcpConnectionActor`/`TcpWriterActor`) und
//!   schließt bei [`Close`]/[`CloseRead`]/[`CloseWrite`] geordnet. Über
//!   [`PeerHalfClosed`] kann eine gekoppelte Gegenseite (z. B. eine
//!   `TcpListenerActor`-Verbindung) das Schließen der entsprechenden
//!   eigenen Hälfte anstoßen. Nachrichten werden über [`Relay<M>`] bzw.
//!   den Hilfs-Actor [`RelayAdapter<M>`] gesendet.
//! - Bequeme Typ-Aliase für die beiden mitgelieferten Protokolle:
//!   [`SimpleStringListener`]/[`SimpleStringConnection`]/[`SimpleStringWriter`]/[`SimpleStringClient`]
//!   und [`SpacePacketListener`]/[`SpacePacketConnection`]/[`SpacePacketWriter`]/[`SpacePacketClient`].
//! - [`TestActor`]: generischer Actor für Tests, der beliebige Kameo-Nachrichten
//!   vom Typ `M` speichert und über [`GetMessages`] eine Kopie des
//!   aufgezeichneten Vektors zurückgibt, bzw. per [`TestActor::assert_received`]
//!   wartet, bis eine bestimmte Anzahl eingetroffen ist.
//! - [`ccsds`]: CCSDS Space Packet Typ (`SpacePacket`) und Codec
//!   (`SpacePacketCodec`), unabhängig von den TCP-Actors oben nutzbar.

pub mod ccsds;
pub use ccsds::{PacketType, SequenceFlags, SpacePacket, SpacePacketCodec, SpacePacketHeader};

use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::{stream, SinkExt, StreamExt};
use kameo::actor::{Actor, ActorRef, Recipient, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message, StreamMessage};
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite, LengthDelimitedCodec};

/// Die "SimpleString"-Nachricht: ein einzelner String, wie er vom
/// [`SimpleStringCodec`] (16-Bit-Längenfeld + ASCII/UTF-8-Nutzdaten)
/// gelesen bzw. geschrieben wird.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleString(pub String);

/// Codec für [`SimpleString`]: 16-Bit-Längenfeld (Big-Endian) +
/// ASCII/UTF-8-Nutzdaten. Implementiert sowohl [`Decoder`] als auch
/// [`Encoder<SimpleString>`] und ist damit ein [`MessageCodec<SimpleString>`]
/// – nutzbar mit den generischen TCP-Actors (siehe [`SimpleStringListener`]
/// & Co.) oder direkt mit `tokio_util::codec::Framed`.
#[derive(Debug, Clone)]
pub struct SimpleStringCodec {
    inner: LengthDelimitedCodec,
}

impl Default for SimpleStringCodec {
    fn default() -> Self {
        SimpleStringCodec {
            inner: LengthDelimitedCodec::builder()
                .length_field_length(2)
                .big_endian()
                .new_codec(),
        }
    }
}

impl Decoder for SimpleStringCodec {
    type Item = SimpleString;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        match self.inner.decode(src)? {
            Some(bytes) => match std::str::from_utf8(&bytes) {
                Ok(s) => Ok(Some(SimpleString(s.to_owned()))),
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "ungültige ASCII/UTF-8 Nutzdaten empfangen",
                )),
            },
            None => Ok(None),
        }
    }
}

impl Encoder<SimpleString> for SimpleStringCodec {
    type Error = io::Error;

    fn encode(&mut self, SimpleString(payload): SimpleString, dst: &mut BytesMut) -> io::Result<()> {
        self.inner.encode(Bytes::from(payload.into_bytes()), dst)
    }
}

/// Kodiert einen String als Frame mit 16-Bit-Längenfeld (Big-Endian) +
/// ASCII-Payload, passend zum [`SimpleStringCodec`]-Format. Praktisch für
/// Tests und einfache Clients, die keinen eigenen `Framed`-Stream
/// aufbauen wollen.
pub fn encode_frame(payload: &str) -> Vec<u8> {
    let mut codec = SimpleStringCodec::default();
    let mut buf = BytesMut::new();
    codec
        .encode(SimpleString(payload.to_string()), &mut buf)
        .expect("encode_frame: Kodierung fehlgeschlagen");
    buf.to_vec()
}

// ---------------------------------------------------------------------
// TCP-Zustand (Verbindungsabbau)
// ---------------------------------------------------------------------

/// Grund, warum eine Verbindungs-Hälfte (Lesen oder Schreiben) beendet
/// wurde.
#[derive(Debug, Clone, PartialEq)]
pub enum CloseReason {
    /// Sauberes Ende: EOF beim Lesen bzw. die Verbindung wurde ordentlich
    /// geschlossen.
    Graceful,
    /// Ein I/O- oder Protokollfehler (z. B. ungültige Frame-Daten, Broken
    /// Pipe, Connection Reset, ...). Der Text ist die `Display`-Ausgabe
    /// des zugrunde liegenden Fehlers.
    Error(String),
}

/// Welche Hälfte der Verbindung betroffen ist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionHalf {
    Read,
    Write,
}

/// Nachricht an den `TcpListenerActor`: eine Verbindungs-Hälfte wurde
/// beendet. Wird sowohl vom `TcpConnectionActor` (Read) als auch vom
/// `TcpWriterActor` (Write) gesendet.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionHalfClosed {
    pub peer_addr: SocketAddr,
    pub half: ConnectionHalf,
    pub reason: CloseReason,
}

// ---------------------------------------------------------------------
// MessageCodec: Anforderung an einen Codec, den die generischen TCP-Actors
// für einen Nachrichtentyp M nutzen können.
// ---------------------------------------------------------------------

/// Ein Codec, der Nachrichten vom Typ `M` sowohl dekodieren (Lesen) als
/// auch kodieren (Schreiben) kann – die Voraussetzung, damit
/// [`TcpConnectionActor<M, C>`] und [`TcpWriterActor<M, C>`] ihn nutzen
/// können. Blanket-implementiert für jeden Typ, der die entsprechenden
/// `tokio_util`-Traits erfüllt; i. d. R. muss man diesen Trait nicht
/// selbst implementieren.
pub trait MessageCodec<M>:
    Decoder<Item = M, Error = io::Error> + Encoder<M, Error = io::Error> + Default + Unpin + Send + 'static
{
}

impl<M, C> MessageCodec<M> for C where
    C: Decoder<Item = M, Error = io::Error>
        + Encoder<M, Error = io::Error>
        + Default
        + Unpin
        + Send
        + 'static
{
}

// ---------------------------------------------------------------------
// TcpListenerActor<M, C>
// ---------------------------------------------------------------------

/// Argumente zum Starten des `TcpListenerActor<M, C>`.
pub struct TcpListenerArgs<M: Send + 'static> {
    pub bind_addr: SocketAddr,
    /// Ziel-Actor, an den alle empfangenen Nachrichten weitergeleitet werden.
    pub downstream: Recipient<M>,
}

/// Actor, der einen TCP-Port bindet und für jede eingehende Verbindung
/// einen `TcpConnectionActor<M, C>` (Lesen) und einen `TcpWriterActor<M, C>`
/// (Schreiben) erzeugt. `M` ist der Nachrichtentyp, `C` der zugehörige
/// [`MessageCodec<M>`]. Nimmt erst dann die nächste Verbindung an, wenn
/// beide Hälften der aktuellen Verbindung als geschlossen gemeldet wurden.
pub struct TcpListenerActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    local_addr: SocketAddr,
    /// Signalisiert der Accept-Loop-Task, dass die aktuelle Verbindung
    /// vollständig geschlossen ist und die nächste Verbindung angenommen
    /// werden darf.
    resume_tx: mpsc::Sender<()>,
    /// (peer_addr, read_closed, write_closed) der aktuell offenen
    /// Verbindung, falls vorhanden.
    current: Option<(SocketAddr, bool, bool)>,
    _types: PhantomData<fn() -> (M, C)>,
}

/// Fragt die tatsächlich gebundene lokale Adresse ab. Nützlich, wenn mit
/// Port 0 gebunden wurde (z. B. in Tests), um den vom OS vergebenen Port
/// zu erfahren.
#[derive(Debug)]
pub struct GetLocalAddr;

impl<M, C> Actor for TcpListenerActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Args = TcpListenerArgs<M>;
    type Error = io::Error;

    async fn on_start(args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        let listener = TcpListener::bind(args.bind_addr).await?;
        let local_addr = listener.local_addr()?;
        println!(
            "TcpListenerActor<{}>: lausche auf {local_addr}",
            std::any::type_name::<M>()
        );

        let listener_recipient = actor_ref.recipient::<ConnectionHalfClosed>();

        // Kapazität 1 reicht: es ist immer höchstens eine ausstehende
        // "beide Hälften zu" - Benachrichtigung relevant.
        let (resume_tx, resume_rx) = mpsc::channel(1);

        // Tokio-Task, die im Kontext dieses Actors (in on_start) gescheduled
        // wird und in einer Schleife auf eingehende Verbindungen wartet.
        tokio::spawn(accept_loop::<M, C>(
            listener,
            args.downstream,
            listener_recipient,
            resume_rx,
        ));

        Ok(TcpListenerActor {
            local_addr,
            resume_tx,
            current: None,
            _types: PhantomData,
        })
    }
}

impl<M, C> Message<GetLocalAddr> for TcpListenerActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = Result<SocketAddr, Infallible>;

    async fn handle(
        &mut self,
        _msg: GetLocalAddr,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Ok(self.local_addr)
    }
}

impl<M, C> Message<ConnectionHalfClosed> for TcpListenerActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(
        &mut self,
        msg: ConnectionHalfClosed,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!(
            "TCP-Zustand: {} Hälfte {:?} geschlossen ({:?})",
            msg.peer_addr, msg.half, msg.reason
        );

        let (peer, mut read_closed, mut write_closed) = self
            .current
            .filter(|(peer, _, _)| *peer == msg.peer_addr)
            .unwrap_or((msg.peer_addr, false, false));

        match msg.half {
            ConnectionHalf::Read => read_closed = true,
            ConnectionHalf::Write => write_closed = true,
        }

        if read_closed && write_closed {
            self.current = None;
            // Verbindung ist vollständig abgebaut -> Accept-Loop darf die
            // nächste Verbindung annehmen.
            let _ = self.resume_tx.send(()).await;
        } else {
            self.current = Some((peer, read_closed, write_closed));
        }
    }
}

async fn accept_loop<M, C>(
    listener: TcpListener,
    downstream: Recipient<M>,
    listener_recipient: Recipient<ConnectionHalfClosed>,
    mut resume_rx: mpsc::Receiver<()>,
) where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                println!("Neue Verbindung von {peer_addr}");

                let (read_half, write_half) = stream.into_split();

                // Writer-Actor: bekommt die write half und schreibt jede
                // eingehende Nachricht (Typ M) über den Codec C auf den
                // Socket.
                let writer_ref = TcpWriterActor::<M, C>::spawn(TcpWriterArgs {
                    write_half,
                    peer_addr,
                    listener: listener_recipient.clone(),
                });
                let writer_shutdown = writer_ref.recipient::<Shutdown<M>>();

                // Connection-Actor: bekommt die read half, liest darüber
                // Nachrichten (Typ M) via Codec C und leitet sie an den
                // Downstream-Actor weiter. Schließt seine read half, wird
                // der Writer-Actor per Shutdown-Nachricht angewiesen,
                // ebenfalls zu schließen.
                let _conn_ref = TcpConnectionActor::<M, C>::spawn(TcpConnectionArgs {
                    read_half,
                    peer_addr,
                    downstream: downstream.clone(),
                    listener: listener_recipient.clone(),
                    writer_shutdown,
                });

                // Erst die nächste Verbindung annehmen, wenn read UND write
                // half dieser Verbindung als geschlossen gemeldet wurden.
                resume_rx.recv().await;
            }
            Err(err) => {
                eprintln!("Fehler bei accept(): {err}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

// ---------------------------------------------------------------------
// TcpConnectionActor<M, C>
// ---------------------------------------------------------------------

/// Argumente zum Starten eines `TcpConnectionActor<M, C>`.
pub struct TcpConnectionArgs<M: Send + 'static> {
    pub read_half: OwnedReadHalf,
    pub peer_addr: SocketAddr,
    pub downstream: Recipient<M>,
    /// Actor (i. d. R. der `TcpListenerActor`), der über das Schließen der
    /// read half informiert wird.
    pub listener: Recipient<ConnectionHalfClosed>,
    /// Referenz auf den zugehörigen `TcpWriterActor<M, C>`. Wird
    /// angewiesen, sich ebenfalls zu beenden (per [`Shutdown`]), sobald
    /// die read half geschlossen wird.
    pub writer_shutdown: Recipient<Shutdown<M>>,
}

/// Actor, der die Lese-Hälfte (`OwnedReadHalf`) einer TCP-Verbindung hält
/// und über den Codec `C` Nachrichten vom Typ `M` liest. Er kennt vom
/// Ziel-Actor nur eine `Recipient<M>` – also nur, dass dieser `M`
/// verarbeiten kann, nicht aber seinen konkreten Typ. Die Schreib-Hälfte
/// wird separat von `TcpWriterActor<M, C>` gehalten.
pub struct TcpConnectionActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    peer_addr: SocketAddr,
    downstream: Recipient<M>,
    listener: Recipient<ConnectionHalfClosed>,
    writer_shutdown: Recipient<Shutdown<M>>,
    _codec: PhantomData<fn() -> C>,
}

/// Internes Element des über den Codec `C` dekodierten Streams: entweder
/// eine erfolgreich dekodierte Nachricht, oder – als jeweils letztes
/// Element vor dem Stream-Ende – der Grund, warum die read half
/// geschlossen wurde. So kann der (bei `attach_stream` statisch
/// festgelegte) `Finished`-Wert umgangen werden, der den tatsächlichen
/// Grund nicht dynamisch transportieren könnte.
enum ReadOutcome<M> {
    Item(M),
    Closed(CloseReason),
}

impl<M, C> Actor for TcpConnectionActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Args = TcpConnectionArgs<M>;
    type Error = io::Error;

    async fn on_start(args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        let framed: FramedRead<_, C> = FramedRead::new(args.read_half, C::default());

        // EOF, I/O-Fehler oder ungültige Nutzdaten (vom Codec als Err
        // gemeldet) führen zu einem abschließenden ReadOutcome::Closed-
        // Element, danach endet der Stream.
        let item_stream = stream::unfold(Some(framed), |state| async move {
            let mut framed = state?;
            let outcome = match framed.next().await {
                Some(Ok(item)) => return Some((ReadOutcome::Item(item), Some(framed))),
                Some(Err(err)) => ReadOutcome::Closed(CloseReason::Error(err.to_string())),
                None => ReadOutcome::Closed(CloseReason::Graceful),
            };
            Some((outcome, None))
        })
        .boxed();

        actor_ref.attach_stream(item_stream, (), ());

        Ok(TcpConnectionActor {
            peer_addr: args.peer_addr,
            downstream: args.downstream,
            listener: args.listener,
            writer_shutdown: args.writer_shutdown,
            _codec: PhantomData,
        })
    }
}

impl<M, C> Message<StreamMessage<ReadOutcome<M>, (), ()>> for TcpConnectionActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(
        &mut self,
        msg: StreamMessage<ReadOutcome<M>, (), ()>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match msg {
            StreamMessage::Started(()) => {
                println!("Stream für {} angehängt", self.peer_addr);
            }
            StreamMessage::Next(ReadOutcome::Item(item)) => {
                if let Err(err) = self.downstream.tell(item).await {
                    eprintln!("Konnte Nachricht nicht an Downstream-Actor senden: {err}");
                }
            }
            StreamMessage::Next(ReadOutcome::Closed(reason)) => {
                println!("Read-Half von {} geschlossen: {reason:?}", self.peer_addr);
                if let Err(err) = self
                    .listener
                    .tell(ConnectionHalfClosed {
                        peer_addr: self.peer_addr,
                        half: ConnectionHalf::Read,
                        reason,
                    })
                    .await
                {
                    eprintln!("Konnte Read-Half-Closed nicht an Listener senden: {err}");
                }

                // Read half ist zu -> den zugehörigen Writer-Actor anweisen,
                // die write half ebenfalls geordnet zu schließen. Ohne das
                // würde "beide Hälften geschlossen" nie erreicht, solange
                // nie (mehr) geschrieben wird.
                if let Err(err) = self.writer_shutdown.tell(Shutdown::new()).await {
                    eprintln!("Konnte Shutdown nicht an Writer-Actor senden: {err}");
                }

                ctx.stop();
            }
            StreamMessage::Finished(()) => {
                // Wird im Normalfall nicht mehr erreicht, da
                // ReadOutcome::Closed bereits als letztes Element vor
                // Stream-Ende gesendet wurde. Sicherheitsnetz.
                ctx.stop();
            }
        }
    }
}

/// Erlaubt es, die read half *aktiv* (unabhängig von einem tatsächlichen
/// EOF/Fehler auf der Leitung) zu schließen – z. B. vom `TcpClientActor`
/// bei einer `Close`- oder `CloseRead`-Nachricht. Meldet
/// `ConnectionHalfClosed{ half: Read, reason: Graceful }` an `listener`
/// und beendet den Actor; der noch laufende `attach_stream`-Hintergrund-
/// Task erkennt das Stoppen über `ActorRef::wait_for_shutdown` und gibt
/// die read half danach frei.
impl<M, C> Message<Shutdown<M>> for TcpConnectionActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: Shutdown<M>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!("Read-Half für {} wird auf Anforderung geschlossen", self.peer_addr);

        if let Err(err) = self
            .listener
            .tell(ConnectionHalfClosed {
                peer_addr: self.peer_addr,
                half: ConnectionHalf::Read,
                reason: CloseReason::Graceful,
            })
            .await
        {
            eprintln!("Konnte Read-Half-Closed nicht an Listener senden: {err}");
        }

        ctx.stop();
    }
}

// ---------------------------------------------------------------------
// TcpWriterActor<M, C>
// ---------------------------------------------------------------------

/// Argumente zum Starten eines `TcpWriterActor<M, C>`. Hängt selbst nicht
/// von `M`/`C` ab (der Codec wird per `C::default()` erzeugt).
pub struct TcpWriterArgs {
    pub write_half: OwnedWriteHalf,
    pub peer_addr: SocketAddr,
    /// Actor (i. d. R. der `TcpListenerActor`), der über das Schließen der
    /// write half informiert wird.
    pub listener: Recipient<ConnectionHalfClosed>,
}

/// Nachricht: fordert den zu `M` gehörenden `TcpWriterActor<M, C>` auf,
/// die write half geordnet zu schließen und sich danach zu beenden. Wird
/// vom zugehörigen `TcpConnectionActor<M, C>` gesendet, sobald dessen read
/// half geschlossen wurde.
///
/// Ist über `PhantomData<fn() -> M>` an den Nachrichtentyp `M` gebunden.
/// Das ist notwendig, damit diese Nachricht nicht mit der generischen
/// `impl<M, C> Message<M> for TcpWriterActor<M, C>` kollidiert (Rusts
/// Kohärenz-/Overlap-Regeln): eine nicht an `M` gebundene `Shutdown`-
/// Nachricht wäre für M = Shutdown gleichzeitig von beiden
/// Implementierungen abgedeckt. Durch die Bindung an `M` müsste dafür
/// M = Shutdown<M> gelten, was für kein konkretes M erfüllbar ist –
/// derselbe Trick wie bei [`GetMessages`].
pub struct Shutdown<M>(PhantomData<fn() -> M>);

impl<M> Shutdown<M> {
    pub fn new() -> Self {
        Shutdown(PhantomData)
    }
}

impl<M> Default for Shutdown<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M> Clone for Shutdown<M> {
    fn clone(&self) -> Self {
        Shutdown(PhantomData)
    }
}

impl<M> Copy for Shutdown<M> {}

impl<M> std::fmt::Debug for Shutdown<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Shutdown").finish()
    }
}

/// Actor, der die Schreib-Hälfte (`OwnedWriteHalf`) einer TCP-Verbindung
/// hält und über den Codec `C` Nachrichten vom Typ `M` schreibt. Für jede
/// per `tell` empfangene Nachricht `M` wird sie über `C` kodiert und auf
/// die Verbindung geschrieben.
///
/// Schlägt ein Schreibversuch fehl (Broken Pipe, Connection Reset, ...),
/// wird dies als `CloseReason::Error` an `listener` gemeldet und der Actor
/// beendet sich. Ein geordnetes ("graceful") Schließen der write half
/// erfolgt, wenn der Actor eine [`Shutdown<M>`]-Nachricht erhält
/// (typischerweise vom zugehörigen `TcpConnectionActor<M, C>`, nachdem
/// dessen read half geschlossen wurde).
pub struct TcpWriterActor<M, C> {
    framed: FramedWrite<OwnedWriteHalf, C>,
    peer_addr: SocketAddr,
    listener: Recipient<ConnectionHalfClosed>,
    _msg: PhantomData<fn() -> M>,
}

impl<M, C> Actor for TcpWriterActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Args = TcpWriterArgs;
    type Error = io::Error;

    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(TcpWriterActor {
            framed: FramedWrite::new(args.write_half, C::default()),
            peer_addr: args.peer_addr,
            listener: args.listener,
            _msg: PhantomData,
        })
    }
}

impl<M, C> Message<Shutdown<M>> for TcpWriterActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: Shutdown<M>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Zunächst über `get_mut()` direkt auf der zugrundeliegenden write
        // half `shutdown()` aufrufen (sendet FIN). `FramedWrite` puffert
        // hier nichts Unversandtes: `SinkExt::send()` (in der Message<M>-
        // Implementierung unten) flusht nach jedem Element vollständig,
        // daher ist ein direkter Zugriff auf die write half sicher – und
        // vermeidet, dass der Item-Typ von `Sink::close()` in diesem
        // generischen Kontext für den Compiler mehrdeutig aufgelöst werden
        // müsste.
        if let Err(err) = self.framed.get_mut().shutdown().await {
            eprintln!(
                "Fehler beim geordneten Schließen der write half an {}: {err}",
                self.peer_addr
            );
        }

        if let Err(send_err) = self
            .listener
            .tell(ConnectionHalfClosed {
                peer_addr: self.peer_addr,
                half: ConnectionHalf::Write,
                reason: CloseReason::Graceful,
            })
            .await
        {
            eprintln!("Konnte Write-Half-Closed nicht an Listener senden: {send_err}");
        }

        ctx.stop();
    }
}

impl<M, C> Message<M> for TcpWriterActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, item: M, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if let Err(err) = self.framed.send(item).await {
            eprintln!("Fehler beim Schreiben an {}: {err}", self.peer_addr);

            if let Err(send_err) = self
                .listener
                .tell(ConnectionHalfClosed {
                    peer_addr: self.peer_addr,
                    half: ConnectionHalf::Write,
                    reason: CloseReason::Error(err.to_string()),
                })
                .await
            {
                eprintln!("Konnte Write-Half-Closed nicht an Listener senden: {send_err}");
            }

            ctx.stop();
        }
    }
}

// ---------------------------------------------------------------------
// TcpClientActor<M, C>
// ---------------------------------------------------------------------

/// Argumente zum Starten eines `TcpClientActor<M, C>`.
pub struct TcpClientArgs<M: Send + 'static> {
    /// Adresse des entfernten Servers, zu dem bei [`Connect`] eine
    /// Verbindung aufgebaut wird.
    pub remote_addr: SocketAddr,
    /// Ziel-Actor, an den über die Verbindung empfangene Nachrichten
    /// weitergeleitet werden.
    pub downstream: Recipient<M>,
    /// Optionaler Beobachter, der über das Schließen einzelner Hälften
    /// der eigenen Verbindung informiert wird (z. B. um das Schließen an
    /// eine andere, gekoppelte Verbindung weiterzureichen – siehe
    /// [`PeerHalfClosed`]).
    pub on_half_closed: Option<Recipient<ConnectionHalfClosed>>,
}

/// Baut bei [`Connect`] eine ausgehende TCP-Verbindung zu `remote_addr`
/// auf und spawnt dafür – genau wie `TcpListenerActor` nach einem
/// `accept()` – einen [`TcpConnectionActor<M, C>`] (liest) und
/// [`TcpWriterActor<M, C>`] (schreibt). Bei [`Close`] werden beide
/// Hälften geordnet geschlossen; [`CloseRead`]/[`CloseWrite`] schließen
/// nur die jeweilige Hälfte.
///
/// Da `TcpClientActor` mehrere eigene, konkrete Steuer-Nachrichten
/// (`Connect`, `Close`, `ConnectionHalfClosed`, ...) implementiert, kann
/// er aus Kohärenzgründen (siehe [`Shutdown`]) nicht zugleich generisch
/// `Message<M>` implementieren. Zum Versenden über die aktuelle
/// Verbindung dient daher [`Relay<M>`]; für die typische Verwendung als
/// `downstream` eines anderen Actors (der nur ein rohes `Recipient<M>`
/// erwartet) gibt es den Hilfs-Actor [`RelayAdapter<M>`].
pub struct TcpClientActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    remote_addr: SocketAddr,
    downstream: Recipient<M>,
    on_half_closed: Option<Recipient<ConnectionHalfClosed>>,
    connection: Option<ClientConnection<M, C>>,
}

struct ClientConnection<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    peer_addr: SocketAddr,
    reader_ref: ActorRef<TcpConnectionActor<M, C>>,
    writer_ref: ActorRef<TcpWriterActor<M, C>>,
    read_closed: bool,
    write_closed: bool,
}

/// Fordert den `TcpClientActor` auf, die Verbindung zu `remote_addr`
/// aufzubauen. Antwortet mit der tatsächlichen Peer-Adresse bei Erfolg
/// bzw. dem aufgetretenen I/O-Fehler.
#[derive(Debug)]
pub struct Connect;

/// Schließt – falls verbunden – beide Hälften der aktuellen Verbindung
/// geordnet (per [`Shutdown<M>`] an Reader und Writer).
#[derive(Debug)]
pub struct Close;

/// Schließt nur die read half der aktuellen Verbindung.
#[derive(Debug)]
pub struct CloseRead;

/// Schließt nur die write half der aktuellen Verbindung.
#[derive(Debug)]
pub struct CloseWrite;

/// Benachrichtigung: die "andere Seite" einer Relais-Kopplung (z. B. die
/// Serververbindung, wenn dieser Actor die Clientseite ist) hat eine
/// Hälfte geschlossen. Der Empfänger reagiert, indem er die
/// *entgegengesetzte* eigene Hälfte schließt:
///
/// - Read auf der anderen Seite geschlossen -> eigene **write** half
///   schließen (von dort kommt nichts mehr, das weitergeleitet werden
///   müsste).
/// - Write auf der anderen Seite geschlossen -> eigene **read** half
///   schließen (Ergebnisse könnten ohnehin nicht mehr zurückgesendet
///   werden).
///
/// Dieselbe Regel gilt symmetrisch in beide Richtungen (Server->Client
/// und Client->Server), weshalb ein und derselbe Handler für beide Fälle
/// passt.
#[derive(Debug)]
pub struct PeerHalfClosed(pub ConnectionHalfClosed);

/// Nachricht: die enthaltene Nachricht `M` soll über die aktuelle
/// Verbindung des `TcpClientActor<M, C>` gesendet werden.
///
/// Bewusst in einen eigenen Typ verpackt statt `M` direkt entgegen-
/// zunehmen: `TcpClientActor` muss daneben mehrere konkrete, nicht an `M`
/// gebundene Steuer-Nachrichten implementieren (`Connect`, `Close`,
/// `ConnectionHalfClosed`, ...). Eine zusätzliche generische
/// `impl<M, C> Message<M> for TcpClientActor<M, C>` würde für M =
/// `ConnectionHalfClosed` (oder `Connect`, ...) mit diesen kollidieren
/// (Rusts Kohärenzregeln) – siehe [`Shutdown`] für dieselbe Problematik.
/// `Relay<M>` selbst kollidiert mit keiner dieser konkreten Nachrichten,
/// da es ein eigenständiger, generischer Typ ist.
pub struct Relay<M>(pub M);

impl<M, C> Actor for TcpClientActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Args = TcpClientArgs<M>;
    type Error = Infallible;

    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(TcpClientActor {
            remote_addr: args.remote_addr,
            downstream: args.downstream,
            on_half_closed: args.on_half_closed,
            connection: None,
        })
    }
}

impl<M, C> Message<Connect> for TcpClientActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = io::Result<SocketAddr>;

    async fn handle(
        &mut self,
        _msg: Connect,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.connection.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "TcpClientActor ist bereits verbunden",
            ));
        }

        let stream = TcpStream::connect(self.remote_addr).await?;
        let peer_addr = stream.peer_addr()?;
        let (read_half, write_half) = stream.into_split();

        println!(
            "TcpClientActor<{}>: verbunden mit {peer_addr}",
            std::any::type_name::<M>()
        );

        let listener_recipient = ctx.actor_ref().clone().recipient::<ConnectionHalfClosed>();

        let writer_ref = TcpWriterActor::<M, C>::spawn(TcpWriterArgs {
            write_half,
            peer_addr,
            listener: listener_recipient.clone(),
        });
        let writer_shutdown = writer_ref.clone().recipient::<Shutdown<M>>();

        let reader_ref = TcpConnectionActor::<M, C>::spawn(TcpConnectionArgs {
            read_half,
            peer_addr,
            downstream: self.downstream.clone(),
            listener: listener_recipient,
            writer_shutdown,
        });

        self.connection = Some(ClientConnection {
            peer_addr,
            reader_ref,
            writer_ref,
            read_closed: false,
            write_closed: false,
        });

        Ok(peer_addr)
    }
}

impl<M, C> Message<Close> for TcpClientActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, _msg: Close, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let Some(conn) = &self.connection else {
            println!("TcpClientActor: Close ignoriert, da aktuell keine Verbindung besteht");
            return;
        };

        if let Err(err) = conn.reader_ref.tell(Shutdown::<M>::new()).await {
            eprintln!("TcpClientActor: Konnte Shutdown nicht an Reader senden: {err}");
        }
        if let Err(err) = conn.writer_ref.tell(Shutdown::<M>::new()).await {
            eprintln!("TcpClientActor: Konnte Shutdown nicht an Writer senden: {err}");
        }
    }
}

impl<M, C> Message<CloseRead> for TcpClientActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, _msg: CloseRead, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if let Some(conn) = &self.connection {
            if let Err(err) = conn.reader_ref.tell(Shutdown::<M>::new()).await {
                eprintln!("TcpClientActor: Konnte Shutdown nicht an Reader senden: {err}");
            }
        }
    }
}

impl<M, C> Message<CloseWrite> for TcpClientActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, _msg: CloseWrite, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if let Some(conn) = &self.connection {
            if let Err(err) = conn.writer_ref.tell(Shutdown::<M>::new()).await {
                eprintln!("TcpClientActor: Konnte Shutdown nicht an Writer senden: {err}");
            }
        }
    }
}

/// Interne Zustandsverfolgung (analog zu `TcpListenerActor`): merkt sich,
/// welche Hälften der aktuellen Verbindung schon geschlossen sind, und
/// leitet jede Meldung optional an `on_half_closed` weiter.
impl<M, C> Message<ConnectionHalfClosed> for TcpClientActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(
        &mut self,
        msg: ConnectionHalfClosed,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        println!(
            "TcpClientActor: Hälfte {:?} der Verbindung zu {} geschlossen ({:?})",
            msg.half, msg.peer_addr, msg.reason
        );

        if let Some(conn) = &mut self.connection {
            match msg.half {
                ConnectionHalf::Read => conn.read_closed = true,
                ConnectionHalf::Write => conn.write_closed = true,
            }
            if conn.read_closed && conn.write_closed {
                println!("TcpClientActor: Verbindung zu {} vollständig geschlossen", conn.peer_addr);
                self.connection = None;
            }
        }

        if let Some(observer) = &self.on_half_closed {
            if let Err(err) = observer.tell(msg).await {
                eprintln!("TcpClientActor: Konnte ConnectionHalfClosed nicht weiterleiten: {err}");
            }
        }
    }
}

impl<M, C> Message<PeerHalfClosed> for TcpClientActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(
        &mut self,
        PeerHalfClosed(msg): PeerHalfClosed,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let Some(conn) = &self.connection else {
            return;
        };

        // Vertauschte Zuordnung, siehe Doku von `PeerHalfClosed`.
        match msg.half {
            ConnectionHalf::Read => {
                if let Err(err) = conn.writer_ref.tell(Shutdown::<M>::new()).await {
                    eprintln!(
                        "TcpClientActor: Konnte Shutdown (via PeerHalfClosed) nicht an Writer senden: {err}"
                    );
                }
            }
            ConnectionHalf::Write => {
                if let Err(err) = conn.reader_ref.tell(Shutdown::<M>::new()).await {
                    eprintln!(
                        "TcpClientActor: Konnte Shutdown (via PeerHalfClosed) nicht an Reader senden: {err}"
                    );
                }
            }
        }
    }
}

impl<M, C> Message<Relay<M>> for TcpClientActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(
        &mut self,
        Relay(item): Relay<M>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match &self.connection {
            Some(conn) => {
                if let Err(err) = conn.writer_ref.tell(item).await {
                    eprintln!("TcpClientActor: Konnte Nachricht nicht an Writer senden: {err}");
                }
            }
            None => {
                eprintln!("TcpClientActor: Nachricht verworfen, da (noch) nicht verbunden");
            }
        }
    }
}

// ---------------------------------------------------------------------
// RelayAdapter<M>: macht einen Recipient<Relay<M>> (z. B. einen
// TcpClientActor) als gewöhnliches Recipient<M> nutzbar.
// ---------------------------------------------------------------------

/// Argumente zum Starten eines `RelayAdapter<M>`.
pub struct RelayAdapterArgs<M: Send + 'static> {
    pub target: Recipient<Relay<M>>,
}

/// Winziger Adapter-Actor: nimmt rohe Nachrichten vom Typ `M` entgegen
/// (z. B. als `downstream` eines `TcpConnectionActor<M, C>`) und leitet
/// sie als [`Relay<M>`] an `target` weiter – typischerweise einen
/// `TcpClientActor<M, C>`, der aus Kohärenzgründen kein rohes
/// `Message<M>` implementieren kann (siehe [`Relay`]).
///
/// ```no_run
/// # use kameo::actor::Spawn;
/// # use kameo_tcp_example::{
/// #     Relay, RelayAdapter, RelayAdapterArgs, SpacePacket, SpacePacketClient, TcpClientArgs,
/// #     TestActor,
/// # };
/// # #[tokio::main]
/// # async fn main() {
/// # let downstream = TestActor::<SpacePacket>::spawn(TestActor::new()).recipient::<SpacePacket>();
/// let client_ref = SpacePacketClient::spawn(TcpClientArgs {
///     remote_addr: "127.0.0.1:9000".parse().unwrap(),
///     downstream,
///     on_half_closed: None,
/// });
/// let adapter_ref = RelayAdapter::spawn(RelayAdapterArgs {
///     target: client_ref.recipient::<Relay<SpacePacket>>(),
/// });
/// // adapter_ref.recipient::<SpacePacket>() kann jetzt überall dort
/// // verwendet werden, wo ein gewöhnliches Recipient<SpacePacket>
/// // erwartet wird, z. B. als `downstream` eines Server-seitigen
/// // TcpConnectionActor<SpacePacket, SpacePacketCodec>.
/// let _recipient = adapter_ref.recipient::<SpacePacket>();
/// # }
/// ```
pub struct RelayAdapter<M: Send + 'static> {
    target: Recipient<Relay<M>>,
}

impl<M> Actor for RelayAdapter<M>
where
    M: Send + 'static,
{
    type Args = RelayAdapterArgs<M>;
    type Error = Infallible;

    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(RelayAdapter {
            target: args.target,
        })
    }
}

impl<M> Message<M> for RelayAdapter<M>
where
    M: Send + 'static,
{
    type Reply = ();

    async fn handle(&mut self, item: M, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if let Err(err) = self.target.tell(Relay(item)).await {
            eprintln!("RelayAdapter: Konnte Nachricht nicht weiterleiten: {err}");
        }
    }
}

// ---------------------------------------------------------------------
// Bequeme Typ-Aliase für die beiden mitgelieferten Protokolle
// ---------------------------------------------------------------------

/// `TcpListenerActor` für das `SimpleString`-Protokoll (16-Bit-Längenfeld +
/// ASCII-Text). Kann direkt mit `TcpListenerArgs<SimpleString>` gespawnt
/// werden, ohne die Codec-Typparameter angeben zu müssen.
pub type SimpleStringListener = TcpListenerActor<SimpleString, SimpleStringCodec>;
/// `TcpConnectionActor` für das `SimpleString`-Protokoll.
pub type SimpleStringConnection = TcpConnectionActor<SimpleString, SimpleStringCodec>;
/// `TcpWriterActor` für das `SimpleString`-Protokoll.
pub type SimpleStringWriter = TcpWriterActor<SimpleString, SimpleStringCodec>;
/// `TcpClientActor` für das `SimpleString`-Protokoll.
pub type SimpleStringClient = TcpClientActor<SimpleString, SimpleStringCodec>;

/// `TcpListenerActor` für CCSDS Space Packets. Kann direkt mit
/// `TcpListenerArgs<SpacePacket>` gespawnt werden, ohne die Codec-
/// Typparameter angeben zu müssen.
pub type SpacePacketListener = TcpListenerActor<SpacePacket, SpacePacketCodec>;
/// `TcpConnectionActor` für CCSDS Space Packets.
pub type SpacePacketConnection = TcpConnectionActor<SpacePacket, SpacePacketCodec>;
/// `TcpWriterActor` für CCSDS Space Packets.
pub type SpacePacketWriter = TcpWriterActor<SpacePacket, SpacePacketCodec>;
/// `TcpClientActor` für CCSDS Space Packets.
pub type SpacePacketClient = TcpClientActor<SpacePacket, SpacePacketCodec>;

// ---------------------------------------------------------------------
// TestActor<M>: generischer Actor für Tests
// ---------------------------------------------------------------------

/// Actor, der beliebige Kameo-Nachrichten vom Typ `M` entgegennimmt, im
/// internen Vektor speichert und per [`GetMessages`] eine Kopie dieses
/// Vektors zurückgeben kann.
///
/// Welcher Nachrichtentyp gehandhabt wird, legt der generische Parameter
/// `M` fest, z. B. `TestActor<SimpleString>`. `TestActor<M>` implementiert
/// `Message<M>` einmalig und generisch – es ist also kein weiterer
/// Boilerplate-Code pro Nachrichtentyp nötig.
pub struct TestActor<M> {
    received: Vec<M>,
}

impl<M> TestActor<M> {
    pub fn new() -> Self {
        Self {
            received: Vec::new(),
        }
    }
}

impl<M> Default for TestActor<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M> Actor for TestActor<M>
where
    M: Send + 'static,
{
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// Speichert jede eingehende Nachricht vom Typ `M` im internen Vektor.
impl<M> Message<M> for TestActor<M>
where
    M: Send + 'static,
{
    type Reply = ();

    async fn handle(&mut self, msg: M, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.received.push(msg);
    }
}

/// Query-Nachricht: liefert per `ask` eine Kopie aller bisher empfangenen
/// Nachrichten. Über `PhantomData<fn() -> M>` an `M` gebunden – aus
/// denselben Kohärenzgründen wie bei [`Shutdown`].
pub struct GetMessages<M>(PhantomData<fn() -> M>);

impl<M> GetMessages<M> {
    pub fn new() -> Self {
        GetMessages(PhantomData)
    }
}

impl<M> Default for GetMessages<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M> Message<GetMessages<M>> for TestActor<M>
where
    M: Clone + Send + 'static,
{
    type Reply = Vec<M>;

    async fn handle(
        &mut self,
        _msg: GetMessages<M>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.received.clone()
    }
}

impl<M> TestActor<M>
where
    M: Clone + std::fmt::Debug + Send + 'static,
{
    /// Wartet, bis der `TestActor` mindestens `expected_count` Nachrichten
    /// empfangen hat, und gibt dann eine Kopie des bisher aufgezeichneten
    /// Vektors zurück, damit der Aufrufer weitere Prüfungen (Inhalt,
    /// Reihenfolge, ...) darauf durchführen kann.
    ///
    /// Schlägt mit `panic!` fehl (wie ein `assert!`), wenn `timeout`
    /// überschritten wird, bevor genügend Nachrichten eingetroffen sind.
    ///
    /// Da der Zustand des Actors nur über Nachrichten abgefragt werden kann
    /// (kein direkter Feldzugriff von außen möglich), wird dafür in
    /// kurzen Intervallen per [`GetMessages`] nachgefragt (Polling), statt
    /// blockierend *innerhalb* des Actors zu warten – ein blockierendes
    /// Warten im `handle`-Aufruf des Actors selbst würde die Verarbeitung
    /// genau der Nachrichten verhindern, auf die gewartet wird, da der
    /// Actor seine Mailbox sequenziell abarbeitet.
    pub async fn assert_received(
        actor_ref: &ActorRef<Self>,
        expected_count: usize,
        timeout: Duration,
    ) -> Vec<M> {
        const POLL_INTERVAL: Duration = Duration::from_millis(10);
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            let received = actor_ref
                .ask(GetMessages::<M>::new())
                .await
                .expect("TestActor nicht erreichbar (bereits gestoppt?)");

            if received.len() >= expected_count {
                return received;
            }

            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "TestActor: erwartete mindestens {expected_count} Nachricht(en) \
                     innerhalb von {timeout:?}, aber nur {} empfangen: {received:?}",
                    received.len()
                );
            }

            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}
