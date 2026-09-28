//! Generic TCP actors, parameterised by a message type `M` and a codec `C`
//! implementing [`MessageCodec<M>`].
//!
//! - [`TcpServerActor`] accepts incoming connections.
//! - [`TcpClientActor`] opens outgoing connections.
//! - Each connection is served by a [`TcpReaderActor`] (read half,
//!   forwards decoded messages to a `downstream` recipient) and a
//!   [`TcpWriterActor`] (write half, encodes and sends every `M` it
//!   receives).
//!
//! Both [`TcpServerActor`] and [`TcpClientActor`] write every `M` they
//! receive to their current connection, so the same code can send to an
//! accepted or to an opened connection. This requires `M` to implement
//! [`WireMessage`].
//!
//! The protocols of this crate get type aliases, e.g. [`PusServer`] for
//! `TcpServerActor<PusPacket, PusCodec>`.
//!
//! Closing is tracked per connection half: the reader and the writer report
//! [`ConnectionHalfClosed`] to their server or client. When the read half
//! ends, the reader also asks the writer to shut down.

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
use tracing::{debug, error, info, warn};

use crate::ccsds::{SpacePacket, SpacePacketCodec};
use crate::pus::{PusCodec, PusPacket};
use crate::messages::{
    Close, CloseRead, CloseReason, CloseWrite, Connect, ConnectionHalf, ConnectionHalfClosed,
    GetLocalAddr, MessageCodec, PeerHalfClosed, Shutdown, WireMessage,
};
use crate::simple_string::{SimpleString, SimpleStringCodec};

/// TCP keepalive settings that detect a peer that has failed or become
/// unreachable without closing the connection.
///
/// Without keepalive, a connection to such a peer stays open until the
/// operating system gives up, which by default takes about two hours on an
/// idle connection. With these settings, the operating system sends a
/// probe after `idle` without traffic and then every `interval`; after
/// `retries` unanswered probes, reading from the connection fails and the
/// actors close it. The default detects a failed peer after about 25
/// seconds (10 s + 3 × 5 s).
///
/// On Linux and Android, the same total time is also set as
/// `TCP_USER_TIMEOUT`. This covers the case keepalive does not: data sent
/// to the peer that is never acknowledged, which would otherwise be
/// retransmitted for about 15 minutes.
///
/// `interval` and `retries` are applied on Linux, Android, macOS, iOS,
/// FreeBSD, NetBSD and Windows; on other systems only `idle` is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeepAlive {
    /// Time without traffic before the first probe.
    pub idle: Duration,
    /// Time between two probes.
    pub interval: Duration,
    /// Number of unanswered probes after which the connection fails.
    pub retries: u32,
}

impl KeepAlive {
    /// The time after which a failed peer is detected on an idle
    /// connection: `idle + retries × interval`.
    pub fn timeout(&self) -> Duration {
        self.idle + self.interval * self.retries
    }
}

impl Default for KeepAlive {
    /// 10 s idle, 5 s interval, 3 retries.
    fn default() -> Self {
        KeepAlive { idle: Duration::from_secs(10), interval: Duration::from_secs(5), retries: 3 }
    }
}

/// Applies `keepalive` to `stream`; `None` leaves the system defaults.
pub(crate) fn configure_keepalive(stream: &TcpStream, keepalive: Option<KeepAlive>) -> io::Result<()> {
    let Some(keepalive) = keepalive else {
        return Ok(());
    };
    let params = socket2::TcpKeepalive::new().with_time(keepalive.idle);
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "netbsd",
        windows
    ))]
    let params = params.with_interval(keepalive.interval).with_retries(keepalive.retries);

    let socket = socket2::SockRef::from(stream);
    socket.set_tcp_keepalive(&params)?;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    socket.set_tcp_user_timeout(Some(keepalive.timeout()))?;
    Ok(())
}

/// Arguments for spawning a [`TcpServerActor<M, C>`].
pub struct TcpServerArgs<M: Send + 'static> {
    /// Address to bind to. Use port 0 to let the operating system choose a
    /// port and query it with [`GetLocalAddr`].
    pub bind_addr: SocketAddr,
    /// Actor that receives every message read from any connection.
    pub downstream: Recipient<M>,
    /// Keepalive settings for accepted connections; `None` keeps the
    /// system defaults (usually no keepalive).
    pub keepalive: Option<KeepAlive>,
}

/// Actor that binds a TCP port and spawns a [`TcpReaderActor<M, C>`]
/// (reading) and a [`TcpWriterActor<M, C>`] (writing) for each incoming
/// connection. `M` is the message type, `C` its [`MessageCodec<M>`].
///
/// Connections are served one at a time: the next connection is accepted
/// only after both halves of the current one have been closed.
///
/// An `M` sent to the server is written to the peer of the current
/// connection, which lets other actors send to whichever client is
/// connected. Without a connection the message is dropped with a warning.
/// This requires `M: WireMessage`.
///
/// Spawning fails with the I/O error of binding the address. Also handles
/// [`GetLocalAddr`] and [`ConnectionHalfClosed`].
pub struct TcpServerActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    local_addr: SocketAddr,
    resume_tx: mpsc::Sender<()>,
    current: Option<(SocketAddr, bool, bool)>,
    writer: Option<(SocketAddr, ActorRef<TcpWriterActor<M, C>>)>,
}

/// Sent by the accept loop to its server when the writer of a new
/// connection has been spawned.
struct WriterSpawned<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    peer_addr: SocketAddr,
    writer: ActorRef<TcpWriterActor<M, C>>,
}

impl<M, C> Actor for TcpServerActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Args = TcpServerArgs<M>;
    type Error = io::Error;

    async fn on_start(args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        let listener = TcpListener::bind(args.bind_addr).await?;
        let local_addr = listener.local_addr()?;
        info!(
            "TcpServerActor<{}>: listening on {local_addr}",
            std::any::type_name::<M>()
        );

        let (resume_tx, resume_rx) = mpsc::channel(1);

        tokio::spawn(accept_loop::<M, C>(listener, args.downstream, args.keepalive, actor_ref, resume_rx));

        Ok(TcpServerActor {
            local_addr,
            resume_tx,
            current: None,
            writer: None,
        })
    }
}

impl<M, C> Message<GetLocalAddr> for TcpServerActor<M, C>
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

impl<M, C> Message<ConnectionHalfClosed> for TcpServerActor<M, C>
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
        info!(
            "TCP state: {} half {:?} closed ({:?})",
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

        if write_closed && self.writer.as_ref().is_some_and(|(peer, _)| *peer == msg.peer_addr) {
            self.writer = None;
        }

        if read_closed && write_closed {
            self.current = None;
            let _ = self.resume_tx.send(()).await;
        } else {
            self.current = Some((peer, read_closed, write_closed));
        }
    }
}

impl<M, C> Message<WriterSpawned<M, C>> for TcpServerActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, msg: WriterSpawned<M, C>, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.writer = Some((msg.peer_addr, msg.writer));
    }
}

/// Sends the message to the peer of the current connection.
impl<M, C> Message<M> for TcpServerActor<M, C>
where
    M: WireMessage + Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, item: M, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let Some((peer_addr, writer)) = &self.writer else {
            warn!("TcpServerActor: message dropped, no connection");
            return;
        };
        if let Err(err) = writer.tell(item).await {
            warn!("TcpServerActor: message to {peer_addr} dropped, connection closed: {err}");
            self.writer = None;
        }
    }
}

/// Accepts connections and spawns reader and writer actors for each, one
/// connection at a time (waits on `resume_rx` until it has been closed).
async fn accept_loop<M, C>(
    listener: TcpListener,
    downstream: Recipient<M>,
    keepalive: Option<KeepAlive>,
    listener_ref: ActorRef<TcpServerActor<M, C>>,
    mut resume_rx: mpsc::Receiver<()>,
) where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    let listener_recipient = listener_ref.clone().recipient::<ConnectionHalfClosed>();
    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                info!("new connection from {peer_addr}");
                if let Err(err) = configure_keepalive(&stream, keepalive) {
                    warn!("cannot set keepalive for {peer_addr}: {err}");
                }

                let (read_half, write_half) = stream.into_split();

                let writer_ref = TcpWriterActor::<M, C>::spawn(TcpWriterArgs {
                    write_half,
                    peer_addr,
                    listener: listener_recipient.clone(),
                });
                let writer_shutdown = writer_ref.clone().recipient::<Shutdown<M>>();

                // Register the writer before the reader starts, so the
                // server knows it before any close event of the connection.
                if let Err(err) = listener_ref.tell(WriterSpawned { peer_addr, writer: writer_ref }).await {
                    warn!("could not register writer with listener: {err}");
                }

                let _reader_ref = TcpReaderActor::<M, C>::spawn(TcpReaderArgs {
                    read_half,
                    peer_addr,
                    downstream: downstream.clone(),
                    listener: listener_recipient.clone(),
                    writer_shutdown,
                });

                resume_rx.recv().await;
            }
            Err(err) => {
                error!("accept() failed: {err}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Arguments for spawning a [`TcpReaderActor<M, C>`].
pub struct TcpReaderArgs<M: Send + 'static> {
    /// Read half of the connection.
    pub read_half: OwnedReadHalf,
    /// Address of the remote peer.
    pub peer_addr: SocketAddr,
    /// Actor that receives every decoded message.
    pub downstream: Recipient<M>,
    /// Receives [`ConnectionHalfClosed`] when the read half ends.
    pub listener: Recipient<ConnectionHalfClosed>,
    /// Writer of the same connection; asked to shut down when the read half
    /// ends.
    pub writer_shutdown: Recipient<Shutdown<M>>,
}

/// Actor that owns the read half ([`OwnedReadHalf`]) of a TCP connection and
/// reads messages of type `M` with the codec `C`.
///
/// Every decoded message is forwarded to `downstream`. When the peer closes
/// the connection or a read or decode error occurs, the actor reports
/// [`ConnectionHalfClosed`] with [`ConnectionHalf::Read`] to its `listener`,
/// sends [`Shutdown<M>`] to the writer and stops. [`Shutdown<M>`] sent to
/// this actor closes the read half on request.
pub struct TcpReaderActor<M, C>
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

/// Item of the read stream attached to a [`TcpReaderActor`].
enum ReadOutcome<M> {
    Item(M),
    Closed(CloseReason),
}

impl<M, C> Actor for TcpReaderActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Args = TcpReaderArgs<M>;
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

        Ok(TcpReaderActor {
            peer_addr: args.peer_addr,
            downstream: args.downstream,
            listener: args.listener,
            writer_shutdown: args.writer_shutdown,
            _codec: PhantomData,
        })
    }
}

impl<M, C> Message<StreamMessage<ReadOutcome<M>, (), ()>> for TcpReaderActor<M, C>
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
                debug!("stream attached for {}", self.peer_addr);
            }
            StreamMessage::Next(ReadOutcome::Item(item)) => {
                if let Err(err) = self.downstream.tell(item).await {
                    warn!("could not send message to downstream actor: {err}");
                }
            }
            StreamMessage::Next(ReadOutcome::Closed(reason)) => {
                info!("read half of {} closed: {reason:?}", self.peer_addr);
                if let Err(err) = self
                    .listener
                    .tell(ConnectionHalfClosed {
                        peer_addr: self.peer_addr,
                        half: ConnectionHalf::Read,
                        reason,
                    })
                    .await
                {
                    warn!("could not send read-half-closed to listener: {err}");
                }

                if let Err(err) = self.writer_shutdown.tell(Shutdown::new()).await {
                    warn!("could not send shutdown to writer actor: {err}");
                }

                ctx.stop();
            }
            StreamMessage::Finished(()) => {
                ctx.stop();
            }
        }
    }
}

impl<M, C> Message<Shutdown<M>> for TcpReaderActor<M, C>
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
        info!("closing read half of {} on request", self.peer_addr);

        if let Err(err) = self
            .listener
            .tell(ConnectionHalfClosed {
                peer_addr: self.peer_addr,
                half: ConnectionHalf::Read,
                reason: CloseReason::Graceful,
            })
            .await
        {
            warn!("could not send read-half-closed to listener: {err}");
        }

        ctx.stop();
    }
}

/// Arguments for spawning a [`TcpWriterActor<M, C>`].
pub struct TcpWriterArgs {
    /// Write half of the connection.
    pub write_half: OwnedWriteHalf,
    /// Address of the remote peer.
    pub peer_addr: SocketAddr,
    /// Receives [`ConnectionHalfClosed`] when the write half is closed.
    pub listener: Recipient<ConnectionHalfClosed>,
}

/// Actor that owns the write half ([`OwnedWriteHalf`]) of a TCP connection
/// and writes messages of type `M` with the codec `C`.
///
/// Every `M` sent to this actor is encoded and written. If writing fails,
/// the actor reports [`ConnectionHalfClosed`] with
/// [`CloseReason::Error`] and stops. [`Shutdown<M>`] shuts the write half
/// down in an orderly way, reports it as [`CloseReason::Graceful`] and
/// stops the actor.
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
            warn!(
                "error while shutting down the write half to {}: {err}",
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
            warn!("could not send write-half-closed to listener: {send_err}");
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
            warn!("error while writing to {}: {err}", self.peer_addr);

            if let Err(send_err) = self
                .listener
                .tell(ConnectionHalfClosed {
                    peer_addr: self.peer_addr,
                    half: ConnectionHalf::Write,
                    reason: CloseReason::Error(err.to_string()),
                })
                .await
            {
                warn!("could not send write-half-closed to listener: {send_err}");
            }

            ctx.stop();
        }
    }
}

/// Arguments for spawning a [`TcpClientActor<M, C>`].
pub struct TcpClientArgs<M: Send + 'static> {
    /// Address to connect to on [`Connect`].
    pub remote_addr: SocketAddr,
    /// Actor that receives every message read from the connection.
    pub downstream: Recipient<M>,
    /// Optional observer that is notified of every [`ConnectionHalfClosed`].
    pub on_half_closed: Option<Recipient<ConnectionHalfClosed>>,
    /// Keepalive settings for the connection; `None` keeps the system
    /// defaults (usually no keepalive).
    pub keepalive: Option<KeepAlive>,
}

/// Actor that opens an outgoing TCP connection to `remote_addr` on
/// [`Connect`] and, just like [`TcpServerActor`] after an `accept()`,
/// spawns a [`TcpReaderActor<M, C>`] (reading) and a
/// [`TcpWriterActor<M, C>`] (writing) for it.
///
/// Messages:
/// - [`Connect`]: connects; replies with the peer address, or an error of
///   kind [`io::ErrorKind::AlreadyExists`] if already connected.
/// - `M`: sends the message over the connection (dropped with a warning
///   if not connected; requires `M: WireMessage`).
/// - [`Close`], [`CloseRead`], [`CloseWrite`]: close both or one half.
/// - [`PeerHalfClosed`]: mirrors a half-close of a coupled connection.
/// - [`ConnectionHalfClosed`]: sent by the reader and writer; once both
///   halves are closed, the actor can connect again.
pub struct TcpClientActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    remote_addr: SocketAddr,
    downstream: Recipient<M>,
    on_half_closed: Option<Recipient<ConnectionHalfClosed>>,
    keepalive: Option<KeepAlive>,
    connection: Option<ClientConnection<M, C>>,
}

/// The reader and writer of the current connection and which halves are
/// closed.
struct ClientConnection<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    peer_addr: SocketAddr,
    reader_ref: ActorRef<TcpReaderActor<M, C>>,
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
            keepalive: args.keepalive,
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
                "TcpClientActor is already connected",
            ));
        }

        let stream = TcpStream::connect(self.remote_addr).await?;
        let peer_addr = stream.peer_addr()?;
        if let Err(err) = configure_keepalive(&stream, self.keepalive) {
            warn!("cannot set keepalive for {peer_addr}: {err}");
        }
        let (read_half, write_half) = stream.into_split();

        info!(
            "TcpClientActor<{}>: connected to {peer_addr}",
            std::any::type_name::<M>()
        );

        let listener_recipient = ctx.actor_ref().clone().recipient::<ConnectionHalfClosed>();

        let writer_ref = TcpWriterActor::<M, C>::spawn(TcpWriterArgs {
            write_half,
            peer_addr,
            listener: listener_recipient.clone(),
        });
        let writer_shutdown = writer_ref.clone().recipient::<Shutdown<M>>();

        let reader_ref = TcpReaderActor::<M, C>::spawn(TcpReaderArgs {
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
            debug!("TcpClientActor: Close ignored, not connected");
            return;
        };

        if let Err(err) = conn.reader_ref.tell(Shutdown::<M>::new()).await {
            warn!("TcpClientActor: could not send shutdown to reader: {err}");
        }
        if let Err(err) = conn.writer_ref.tell(Shutdown::<M>::new()).await {
            warn!("TcpClientActor: could not send shutdown to writer: {err}");
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
                warn!("TcpClientActor: could not send shutdown to reader: {err}");
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
                warn!("TcpClientActor: could not send shutdown to writer: {err}");
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
        info!(
            "TcpClientActor: half {:?} of the connection to {} closed ({:?})",
            msg.half, msg.peer_addr, msg.reason
        );

        if let Some(conn) = &mut self.connection {
            match msg.half {
                ConnectionHalf::Read => conn.read_closed = true,
                ConnectionHalf::Write => conn.write_closed = true,
            }
            if conn.read_closed && conn.write_closed {
                info!("TcpClientActor: connection to {} fully closed", conn.peer_addr);
                self.connection = None;
            }
        }

        if let Some(observer) = &self.on_half_closed {
            if let Err(err) = observer.tell(msg).await {
                warn!("TcpClientActor: could not forward ConnectionHalfClosed: {err}");
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
                    warn!(
                        "TcpClientActor: could not send shutdown (via PeerHalfClosed) to writer: {err}"
                    );
                }
            }
            ConnectionHalf::Write => {
                if let Err(err) = conn.reader_ref.tell(Shutdown::<M>::new()).await {
                    warn!(
                        "TcpClientActor: could not send shutdown (via PeerHalfClosed) to reader: {err}"
                    );
                }
            }
        }
    }
}

impl<M, C> Message<M> for TcpClientActor<M, C>
where
    M: WireMessage + Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(
        &mut self,
        item: M,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match &self.connection {
            Some(conn) => {
                if let Err(err) = conn.writer_ref.tell(item).await {
                    warn!("TcpClientActor: could not send message to writer: {err}");
                }
            }
            None => {
                warn!("TcpClientActor: message dropped, not connected (yet)");
            }
        }
    }
}

/// [`TcpServerActor`] for the [`SimpleString`] protocol.
pub type SimpleStringServer = TcpServerActor<SimpleString, SimpleStringCodec>;
/// [`TcpReaderActor`] for the [`SimpleString`] protocol.
pub type SimpleStringReader = TcpReaderActor<SimpleString, SimpleStringCodec>;
/// [`TcpWriterActor`] for the [`SimpleString`] protocol.
pub type SimpleStringWriter = TcpWriterActor<SimpleString, SimpleStringCodec>;
/// [`TcpClientActor`] for the [`SimpleString`] protocol.
pub type SimpleStringClient = TcpClientActor<SimpleString, SimpleStringCodec>;

/// [`TcpServerActor`] for CCSDS [`SpacePacket`]s.
pub type SpacePacketServer = TcpServerActor<SpacePacket, SpacePacketCodec>;
/// [`TcpReaderActor`] for CCSDS [`SpacePacket`]s.
pub type SpacePacketReader = TcpReaderActor<SpacePacket, SpacePacketCodec>;
/// [`TcpWriterActor`] for CCSDS [`SpacePacket`]s.
pub type SpacePacketWriter = TcpWriterActor<SpacePacket, SpacePacketCodec>;
/// [`TcpClientActor`] for CCSDS [`SpacePacket`]s.
pub type SpacePacketClient = TcpClientActor<SpacePacket, SpacePacketCodec>;

/// [`TcpServerActor`] for ECSS PUS-C packets ([`PusPacket`]).
pub type PusServer = TcpServerActor<PusPacket, PusCodec>;
/// [`TcpReaderActor`] for ECSS PUS-C packets ([`PusPacket`]).
pub type PusReader = TcpReaderActor<PusPacket, PusCodec>;
/// [`TcpWriterActor`] for ECSS PUS-C packets ([`PusPacket`]).
pub type PusWriter = TcpWriterActor<PusPacket, PusCodec>;
/// [`TcpClientActor`] for ECSS PUS-C packets ([`PusPacket`]).
pub type PusClient = TcpClientActor<PusPacket, PusCodec>;

#[cfg(test)]
mod tests {
    use super::*;

    async fn connected_stream() -> TcpStream {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (client, _server) = tokio::join!(TcpStream::connect(addr), listener.accept());
        client.unwrap()
    }

    #[test]
    fn default_keepalive_detects_failed_peer_after_25_seconds() {
        assert_eq!(KeepAlive::default().timeout(), Duration::from_secs(25));
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[tokio::test]
    async fn keepalive_is_applied_to_the_socket() {
        let stream = connected_stream().await;
        let keepalive = KeepAlive { idle: Duration::from_secs(7), interval: Duration::from_secs(2), retries: 4 };

        configure_keepalive(&stream, Some(keepalive)).unwrap();

        let socket = socket2::SockRef::from(&stream);
        assert!(socket.keepalive().unwrap());
        assert_eq!(socket.tcp_keepalive_time().unwrap(), keepalive.idle);
        assert_eq!(socket.tcp_keepalive_interval().unwrap(), keepalive.interval);
        assert_eq!(socket.tcp_keepalive_retries().unwrap(), keepalive.retries);
        assert_eq!(socket.tcp_user_timeout().unwrap(), Some(Duration::from_secs(15)));
    }

    #[tokio::test]
    async fn no_keepalive_leaves_the_socket_unchanged() {
        let stream = connected_stream().await;

        configure_keepalive(&stream, None).unwrap();

        assert!(!socket2::SockRef::from(&stream).keepalive().unwrap());
    }
}
