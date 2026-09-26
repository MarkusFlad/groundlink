use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::time::Duration;

use futures::{stream, SinkExt, StreamExt};
use kameo::actor::{Actor, ActorRef, Recipient, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message, StreamMessage};
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::ccsds::{SpacePacket, SpacePacketCodec};
use crate::messages::{
    Close, CloseRead, CloseReason, CloseWrite, Connect, ConnectionHalf, ConnectionHalfClosed,
    GetLocalAddr, MessageCodec, PeerHalfClosed, Relay, Shutdown,
};
use crate::simple_string::{SimpleString, SimpleStringCodec};

/// Argumente zum Starten des `TcpListenerActor<M, C>`.
pub struct TcpListenerArgs<M: Send + 'static> {
    pub bind_addr: SocketAddr,
    /// Ziel-Actor, an den alle empfangenen Nachrichten weitergeleitet werden.
    pub downstream: Recipient<M>,
}

/// Actor, der einen TCP-Port bindet und für jede eingehende Verbindung
/// einen `TcpConnectionActor<M, C>` (Lesen) und einen `TcpWriterActor<M, C>`
/// (Schreiben) erzeugt. `M` ist der Nachrichtentyp, `C` der zugehörige
/// [`MessageCodec<M>`].
pub struct TcpListenerActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    local_addr: SocketAddr,
    resume_tx: mpsc::Sender<()>,
    current: Option<(SocketAddr, bool, bool)>,
    _types: PhantomData<fn() -> (M, C)>,
}

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
        let (resume_tx, resume_rx) = mpsc::channel(1);

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

                let writer_ref = TcpWriterActor::<M, C>::spawn(TcpWriterArgs {
                    write_half,
                    peer_addr,
                    listener: listener_recipient.clone(),
                });
                let writer_shutdown = writer_ref.recipient::<Shutdown<M>>();

                let _conn_ref = TcpConnectionActor::<M, C>::spawn(TcpConnectionArgs {
                    read_half,
                    peer_addr,
                    downstream: downstream.clone(),
                    listener: listener_recipient.clone(),
                    writer_shutdown,
                });

                resume_rx.recv().await;
            }
            Err(err) => {
                eprintln!("Fehler bei accept(): {err}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Argumente zum Starten eines `TcpConnectionActor<M, C>`.
pub struct TcpConnectionArgs<M: Send + 'static> {
    pub read_half: OwnedReadHalf,
    pub peer_addr: SocketAddr,
    pub downstream: Recipient<M>,
    pub listener: Recipient<ConnectionHalfClosed>,
    pub writer_shutdown: Recipient<Shutdown<M>>,
}

/// Actor, der die Lese-Hälfte (`OwnedReadHalf`) einer TCP-Verbindung hält
/// und über den Codec `C` Nachrichten vom Typ `M` liest.
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

                if let Err(err) = self.writer_shutdown.tell(Shutdown::new()).await {
                    eprintln!("Konnte Shutdown nicht an Writer-Actor senden: {err}");
                }

                ctx.stop();
            }
            StreamMessage::Finished(()) => {
                ctx.stop();
            }
        }
    }
}

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

/// Argumente zum Starten eines `TcpWriterActor<M, C>`.
pub struct TcpWriterArgs {
    pub write_half: OwnedWriteHalf,
    pub peer_addr: SocketAddr,
    pub listener: Recipient<ConnectionHalfClosed>,
}

/// Actor, der die Schreib-Hälfte (`OwnedWriteHalf`) einer TCP-Verbindung
/// hält und über den Codec `C` Nachrichten vom Typ `M` schreibt.
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

/// Argumente zum Starten eines `TcpClientActor<M, C>`.
pub struct TcpClientArgs<M: Send + 'static> {
    pub remote_addr: SocketAddr,
    pub downstream: Recipient<M>,
    pub on_half_closed: Option<Recipient<ConnectionHalfClosed>>,
}

/// Baut bei [`Connect`] eine ausgehende TCP-Verbindung zu `remote_addr`
/// auf und spawnt dafür – genau wie `TcpListenerActor` nach einem
/// `accept()` – einen [`TcpConnectionActor<M, C>`] (liest) und
/// [`TcpWriterActor<M, C>`] (schreibt).
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

/// Argumente zum Starten eines `RelayAdapter<M>`.
pub struct RelayAdapterArgs<M: Send + 'static> {
    pub target: Recipient<Relay<M>>,
}

/// Winziger Adapter-Actor: nimmt rohe Nachrichten vom Typ `M` entgegen
/// und leitet sie als [`Relay<M>`] an `target` weiter.
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

/// `TcpListenerActor` für das `SimpleString`-Protokoll.
pub type SimpleStringListener = TcpListenerActor<SimpleString, SimpleStringCodec>;
/// `TcpConnectionActor` für das `SimpleString`-Protokoll.
pub type SimpleStringConnection = TcpConnectionActor<SimpleString, SimpleStringCodec>;
/// `TcpWriterActor` für das `SimpleString`-Protokoll.
pub type SimpleStringWriter = TcpWriterActor<SimpleString, SimpleStringCodec>;
/// `TcpClientActor` für das `SimpleString`-Protokoll.
pub type SimpleStringClient = TcpClientActor<SimpleString, SimpleStringCodec>;

/// `TcpListenerActor` für CCSDS Space Packets.
pub type SpacePacketListener = TcpListenerActor<SpacePacket, SpacePacketCodec>;
/// `TcpConnectionActor` für CCSDS Space Packets.
pub type SpacePacketConnection = TcpConnectionActor<SpacePacket, SpacePacketCodec>;
/// `TcpWriterActor` für CCSDS Space Packets.
pub type SpacePacketWriter = TcpWriterActor<SpacePacket, SpacePacketCodec>;
/// `TcpClientActor` für CCSDS Space Packets.
pub type SpacePacketClient = TcpClientActor<SpacePacket, SpacePacketCodec>;
