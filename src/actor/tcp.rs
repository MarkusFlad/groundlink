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
//! All four actors also handle [`Batch<M>`], several messages as one kameo
//! message. A server or client created with `batched` forwards the
//! messages that arrive together as one batch, which raises the throughput
//! of small messages several times; see the [`batch`](crate::actor::batch)
//! module.
//!
//! The protocols of this crate get type aliases, e.g. [`PusServer`] for
//! `TcpServerActor<PusPacket, PusCodec>`.
//!
//! Closing is tracked per connection half: the reader and the writer report
//! [`ConnectionHalfClosed`] to their server or client. When the read half
//! ends, the reader also asks the writer to shut down.

use std::collections::VecDeque;
use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::BytesMut;
use futures::{StreamExt, stream};
use kameo::actor::{Actor, ActorRef, Recipient, ReplyRecipient, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message, StreamMessage};
use kameo::reply::{DelegatedReply, ReplySender};
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::codec::FramedRead;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::actor::batch::{Batch, DEFAULT_MAX_BATCH_LEN, Downstream};
use crate::protocol::ccsds::{SpacePacket, SpacePacketCodec};
use crate::protocol::pus::{PusCodec, PusPacket};
use crate::protocol::simple_string::{SimpleString, SimpleStringCodec};
use crate::protocol::{MessageCodec, WireMessage};

mod message;

pub use message::{
    Close, CloseRead, CloseReason, CloseWrite, Connect, ConnectionEvent, ConnectionEventKind, ConnectionHalf,
    ConnectionHalfClosed, EventSource, GetLocalAddr, Shutdown,
};

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
        KeepAlive {
            idle: Duration::from_secs(10),
            interval: Duration::from_secs(5),
            retries: 3,
        }
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

/// What a [`TcpServerActor`] does when a new client connects while a
/// connection is active.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConnectionPolicy {
    /// The new client is accepted only after both halves of the current
    /// connection have been closed. Until then, the operating system keeps
    /// the new connection in the listen backlog: the client's connect
    /// succeeds, but nothing it sends is read yet.
    #[default]
    WaitForClose,
    /// The current connection is closed and the new client is served
    /// instead. The new connection starts only after both halves of the
    /// old one have been closed, so messages of the two connections never
    /// interleave.
    ReplaceCurrent,
}

/// Observer of the connection events of a [`TcpServerActor`] or
/// [`TcpClientActor`]: any actor that handles [`ConnectionEvent`] with
/// `Reply = ()`, obtained with `actor_ref.reply_recipient::<ConnectionEvent>()`.
pub type ConnectionObserver = ReplyRecipient<ConnectionEvent, ()>;

/// Arguments for spawning a [`TcpServerActor<M, C>`].
pub struct TcpServerArgs<M: Send + 'static, C> {
    /// Address to bind to. Use port 0 to let the operating system choose a
    /// port and query it with [`GetLocalAddr`].
    pub bind_addr: SocketAddr,
    /// Receives every message read from any connection, single or in
    /// batches.
    pub downstream: Downstream<M>,
    /// Keepalive settings for accepted connections; `None` keeps the
    /// system defaults (usually no keepalive).
    pub keepalive: Option<KeepAlive>,
    /// What happens when a new client connects while a connection is
    /// active.
    pub connection_policy: ConnectionPolicy,
    /// Codec for the connections, e.g. `PusCodec::new(config)`; every
    /// connection uses a clone of it.
    pub codec: C,
    /// Receives the [`ConnectionEvent`]s of the accepted connections.
    pub observer: Option<ConnectionObserver>,
    /// How long a write to a client may make no progress before the
    /// connection counts as failed; see [`TcpWriterArgs::write_timeout`].
    pub write_timeout: Option<Duration>,
    /// How many messages the mailbox of each reader and writer actor
    /// spawned for a connection holds before senders have to wait. Larger
    /// mailboxes let the actors work in longer stretches, which raises the
    /// throughput of small messages, but more messages queue up before
    /// backpressure reaches the sender.
    pub mailbox_capacity: usize,
    /// How many messages that one read from the socket has delivered are
    /// forwarded to `downstream` at once at most. Larger batches raise the
    /// throughput of small messages; smaller ones limit how many messages
    /// queue up before backpressure reaches the peer, since a mailbox
    /// holds [`mailbox_capacity`](Self::mailbox_capacity) batches. 1
    /// forwards every message on its own.
    pub max_batch_len: usize,
}

impl<M: Send + 'static, C> TcpServerArgs<M, C> {
    /// Arguments with [`KeepAlive::default`],
    /// [`ConnectionPolicy::WaitForClose`], no observer,
    /// [`DEFAULT_WRITE_TIMEOUT`], [`DEFAULT_MAILBOX_CAPACITY`] and
    /// [`DEFAULT_MAX_BATCH_LEN`]; change them with
    /// [`with_keepalive`](Self::with_keepalive),
    /// [`with_connection_policy`](Self::with_connection_policy),
    /// [`with_observer`](Self::with_observer),
    /// [`with_write_timeout`](Self::with_write_timeout),
    /// [`with_mailbox_capacity`](Self::with_mailbox_capacity) and
    /// [`with_max_batch_len`](Self::with_max_batch_len). `downstream`
    /// receives every message on its own; see [`batched`](Self::batched).
    pub fn new(bind_addr: SocketAddr, downstream: Recipient<M>, codec: C) -> Self {
        Self::with_downstream(bind_addr, downstream.into(), codec)
    }

    /// Like [`new`](Self::new), but `downstream` receives the messages in
    /// batches: what one read from the socket has delivered arrives as one
    /// [`Batch<M>`], which costs far less per message than sending each
    /// one on its own. See [`impl_batch_message!`](crate::impl_batch_message)
    /// for how an actor handles batches.
    pub fn batched(bind_addr: SocketAddr, downstream: Recipient<Batch<M>>, codec: C) -> Self {
        Self::with_downstream(bind_addr, downstream.into(), codec)
    }

    fn with_downstream(bind_addr: SocketAddr, downstream: Downstream<M>, codec: C) -> Self {
        TcpServerArgs {
            bind_addr,
            downstream,
            keepalive: Some(KeepAlive::default()),
            connection_policy: ConnectionPolicy::WaitForClose,
            codec,
            observer: None,
            write_timeout: Some(DEFAULT_WRITE_TIMEOUT),
            mailbox_capacity: DEFAULT_MAILBOX_CAPACITY,
            max_batch_len: DEFAULT_MAX_BATCH_LEN,
        }
    }

    /// Uses `keepalive` for accepted connections; `None` keeps the system
    /// defaults.
    pub fn with_keepalive(mut self, keepalive: Option<KeepAlive>) -> Self {
        self.keepalive = keepalive;
        self
    }

    /// Uses `policy` when a new client connects while a connection is
    /// active.
    pub fn with_connection_policy(mut self, policy: ConnectionPolicy) -> Self {
        self.connection_policy = policy;
        self
    }

    /// Sends the [`ConnectionEvent`]s of the accepted connections to
    /// `observer`.
    pub fn with_observer(mut self, observer: ConnectionObserver) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Uses `write_timeout` for the accepted connections; `None` waits
    /// forever.
    pub fn with_write_timeout(mut self, write_timeout: Option<Duration>) -> Self {
        self.write_timeout = write_timeout;
        self
    }

    /// Uses `capacity` for the mailboxes of the reader and writer actors of
    /// each connection. The mailbox of the server actor itself is set
    /// when spawning it, e.g. with
    /// `spawn_with_mailbox(args, kameo::mailbox::bounded(capacity))`.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is 0.
    pub fn with_mailbox_capacity(mut self, capacity: usize) -> Self {
        assert!(capacity > 0, "mailbox capacity must be at least 1");
        self.mailbox_capacity = capacity;
        self
    }

    /// Forwards at most `max_batch_len` messages to `downstream` at once.
    ///
    /// # Panics
    ///
    /// Panics if `max_batch_len` is 0.
    pub fn with_max_batch_len(mut self, max_batch_len: usize) -> Self {
        assert!(max_batch_len > 0, "max batch length must be at least 1");
        self.max_batch_len = max_batch_len;
        self
    }
}

/// Actor that binds a TCP port and spawns a [`TcpReaderActor<M, C>`]
/// (reading) and a [`TcpWriterActor<M, C>`] (writing) for each incoming
/// connection. `M` is the message type, `C` its [`MessageCodec<M>`].
///
/// Connections are served one at a time. With
/// [`ConnectionPolicy::WaitForClose`], the next connection is accepted only
/// after both halves of the current one have been closed; with
/// [`ConnectionPolicy::ReplaceCurrent`], a new client closes the current
/// connection and takes its place.
///
/// An `M` sent to the server is written to the peer of the current
/// connection, which lets other actors send to whichever client is
/// connected. Without a connection the message is dropped with a warning.
/// This requires `M: WireMessage`. [`Close`] closes the current connection.
///
/// If an observer is set (see [`TcpServerArgs::with_observer`]), it
/// receives the [`ConnectionEvent`]s of every connection. The server sends
/// [`ConnectionEventKind::Connected`] with `ask` and starts reading from
/// the connection only after the observer has handled it, so the observer
/// can prepare before the first message goes downstream. The server also
/// handles the [`ConnectionEvent`]s of a coupled client; see
/// [`TcpClientActor`] for coupling a server and a client.
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
    observer: Option<ConnectionObserver>,
    connection: Option<ServerConnection<M, C>>,
}

/// The connection a [`TcpServerActor`] is serving and which halves are
/// closed.
struct ServerConnection<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    id: u64,
    peer_addr: SocketAddr,
    /// `None` once the write half is closed.
    writer: Option<WriterHandle<M, C>>,
    reader_shutdown: Recipient<Shutdown<M>>,
    read_closed: bool,
    write_closed: bool,
}

/// Sent by the accept loop to its server when the reader and writer of a
/// new connection have been created, before the reader starts.
struct ConnectionSpawned<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    id: u64,
    peer_addr: SocketAddr,
    writer: WriterHandle<M, C>,
    reader_shutdown: Recipient<Shutdown<M>>,
}

impl<M, C> Actor for TcpServerActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Args = TcpServerArgs<M, C>;
    type Error = io::Error;

    async fn on_start(args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        let listener = TcpListener::bind(args.bind_addr).await?;
        let local_addr = listener.local_addr()?;
        info!(
            "TcpServerActor<{}>: listening on {local_addr}",
            std::any::type_name::<M>()
        );

        let (resume_tx, resume_rx) = mpsc::channel(1);

        tokio::spawn(accept_loop::<M, C>(
            listener,
            AcceptConfig {
                downstream: args.downstream,
                max_batch_len: args.max_batch_len,
                keepalive: args.keepalive,
                policy: args.connection_policy,
                codec: args.codec,
                observer: args.observer.clone(),
                write_timeout: args.write_timeout,
                mailbox_capacity: args.mailbox_capacity,
            },
            actor_ref,
            resume_rx,
        ));

        Ok(TcpServerActor {
            local_addr,
            resume_tx,
            observer: args.observer,
            connection: None,
        })
    }
}

/// Sends `event` to `observer`, if any, with `tell`.
async fn notify(observer: &Option<ConnectionObserver>, event: ConnectionEvent) {
    if let Some(observer) = observer
        && let Err(err) = observer.tell(event).await
    {
        warn!("could not send connection event: {err}");
    }
}

/// A connection event reported by a server.
fn server_event(connection_id: u64, peer_addr: SocketAddr, kind: ConnectionEventKind) -> ConnectionEvent {
    ConnectionEvent {
        source: EventSource::Server,
        connection_id,
        peer_connection_id: None,
        peer_addr,
        kind,
    }
}

impl<M, C> Message<GetLocalAddr> for TcpServerActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = Result<SocketAddr, Infallible>;

    async fn handle(&mut self, _msg: GetLocalAddr, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        Ok(self.local_addr)
    }
}

impl<M, C> Message<ConnectionSpawned<M, C>> for TcpServerActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, msg: ConnectionSpawned<M, C>, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.connection = Some(ServerConnection {
            id: msg.id,
            peer_addr: msg.peer_addr,
            writer: Some(msg.writer),
            reader_shutdown: msg.reader_shutdown,
            read_closed: false,
            write_closed: false,
        });
    }
}

impl<M, C> Message<ConnectionHalfClosed> for TcpServerActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, msg: ConnectionHalfClosed, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        info!(
            "TCP state: {} half {:?} closed ({:?})",
            msg.peer_addr, msg.half, msg.reason
        );

        let Some(conn) = self.connection.as_mut().filter(|conn| conn.peer_addr == msg.peer_addr) else {
            warn!("TcpServerActor: close event for unknown connection {}", msg.peer_addr);
            return;
        };
        match msg.half {
            ConnectionHalf::Read => conn.read_closed = true,
            ConnectionHalf::Write => {
                conn.write_closed = true;
                conn.writer = None;
            }
        }
        let (id, disconnected) = (conn.id, conn.read_closed && conn.write_closed);

        let kind = ConnectionEventKind::HalfClosed {
            half: msg.half,
            reason: msg.reason,
        };
        notify(&self.observer, server_event(id, msg.peer_addr, kind)).await;
        if disconnected {
            self.connection = None;
            // Before resuming the accept loop, so that the observer learns
            // about the end of this connection before the next one starts.
            notify(
                &self.observer,
                server_event(id, msg.peer_addr, ConnectionEventKind::Disconnected),
            )
            .await;
            let _ = self.resume_tx.send(()).await;
        }
    }
}

/// Closes both halves of the current connection.
impl<M, C> Message<Close> for TcpServerActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, _msg: Close, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.close_current().await;
    }
}

/// Closes the current connection when the client connection that belongs
/// to it ends or cannot be opened; see [`TcpClientActor`] for how to couple
/// a server and a client. Events of servers and of other client
/// connections are ignored.
impl<M, C> Message<ConnectionEvent> for TcpServerActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, event: ConnectionEvent, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let current = self.connection.as_ref().map(|conn| conn.id);
        if event.source != EventSource::Client
            || event.peer_connection_id.is_none()
            || event.peer_connection_id != current
        {
            return;
        }
        match event.kind {
            ConnectionEventKind::Disconnected | ConnectionEventKind::ConnectFailed { .. } => {
                info!("TcpServerActor: the coupled connection to {} ended", event.peer_addr);
                self.close_current().await;
            }
            ConnectionEventKind::Connected | ConnectionEventKind::HalfClosed { .. } => {}
        }
    }
}

impl<M, C> TcpServerActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    /// Asks the reader and writer of the current connection to close.
    async fn close_current(&self) {
        let Some(conn) = &self.connection else {
            debug!("TcpServerActor: Close ignored, no connection");
            return;
        };
        info!("TcpServerActor: closing connection to {}", conn.peer_addr);
        // Either actor may have stopped already; it has then reported its
        // half as closed.
        let _ = conn.reader_shutdown.tell(Shutdown::new()).await;
        if let Some(writer) = &conn.writer {
            writer.shut_down();
        }
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
        let Some((peer_addr, writer)) = self
            .connection
            .as_ref()
            .and_then(|conn| conn.writer.as_ref().map(|writer| (conn.peer_addr, writer)))
        else {
            warn!("TcpServerActor: message dropped, no connection");
            return;
        };
        // Waits while the writer's mailbox is full; the write timeout
        // bounds that wait if the client stops reading.
        if let Err(err) = writer.actor_ref.tell(item).await {
            warn!("TcpServerActor: message to {peer_addr} dropped, connection closed: {err}");
        }
    }
}

impl<M, C> Message<Batch<M>> for TcpServerActor<M, C>
where
    M: WireMessage + Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, batch: Batch<M>, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let Some((peer_addr, writer)) = self
            .connection
            .as_ref()
            .and_then(|conn| conn.writer.as_ref().map(|writer| (conn.peer_addr, writer)))
        else {
            warn!("TcpServerActor: messages dropped, no connection");
            return;
        };
        if let Err(err) = writer.actor_ref.tell(batch).await {
            warn!("TcpServerActor: messages to {peer_addr} dropped, connection closed: {err}");
        }
    }
}

/// Settings of an [`accept_loop`], taken from the [`TcpServerArgs`].
struct AcceptConfig<M: Send + 'static, C> {
    downstream: Downstream<M>,
    max_batch_len: usize,
    keepalive: Option<KeepAlive>,
    policy: ConnectionPolicy,
    codec: C,
    observer: Option<ConnectionObserver>,
    write_timeout: Option<Duration>,
    mailbox_capacity: usize,
}

/// Accepts connections and spawns reader and writer actors for each, one
/// connection at a time. `resume_rx` signals that both halves of the
/// current connection have been closed. With
/// [`ConnectionPolicy::ReplaceCurrent`], a new client closes the current
/// connection first.
async fn accept_loop<M, C>(
    listener: TcpListener,
    config: AcceptConfig<M, C>,
    server_ref: ActorRef<TcpServerActor<M, C>>,
    mut resume_rx: mpsc::Receiver<()>,
) where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    let listener_recipient = server_ref.clone().recipient::<ConnectionHalfClosed>();
    let mut active = false;
    let mut last_id = 0;
    loop {
        let accept_now = !active || config.policy == ConnectionPolicy::ReplaceCurrent;
        let accepted = tokio::select! {
            _ = resume_rx.recv(), if active => {
                active = false;
                continue;
            }
            accepted = listener.accept(), if accept_now => accepted,
        };
        match accepted {
            Ok((stream, peer_addr)) => {
                info!("new connection from {peer_addr}");
                if active {
                    info!("closing the current connection for new client {peer_addr}");
                    let _ = server_ref.tell(Close).await;
                    resume_rx.recv().await;
                }
                if let Err(err) = configure_keepalive(&stream, config.keepalive) {
                    warn!("cannot set keepalive for {peer_addr}: {err}");
                }

                let (read_half, write_half) = stream.into_split();

                let writer = WriterHandle::spawn(
                    TcpWriterArgs::new(write_half, peer_addr, listener_recipient.clone(), config.codec.clone())
                        .with_write_timeout(config.write_timeout),
                    config.mailbox_capacity,
                );
                let writer_shutdown = writer.actor_ref.clone().recipient::<Shutdown<M>>();
                // The reader gets its reference now but starts only below.
                let reader =
                    TcpReaderActor::<M, C>::prepare_with_mailbox(kameo::mailbox::bounded(config.mailbox_capacity));

                // Register the connection before the reader starts, so the
                // server knows it before any close event of the connection.
                last_id += 1;
                let spawned = ConnectionSpawned {
                    id: last_id,
                    peer_addr,
                    writer,
                    reader_shutdown: reader.actor_ref().clone().recipient::<Shutdown<M>>(),
                };
                if let Err(err) = server_ref.tell(spawned).await {
                    warn!("could not register connection with server: {err}");
                }

                // Let the observer prepare before the first message is read.
                if let Some(observer) = &config.observer {
                    let event = server_event(last_id, peer_addr, ConnectionEventKind::Connected);
                    if let Err(err) = observer.ask(event).await {
                        warn!("could not send connection event: {err}");
                    }
                }

                reader.spawn(TcpReaderArgs {
                    read_half,
                    peer_addr,
                    downstream: config.downstream.clone(),
                    max_batch_len: config.max_batch_len,
                    listener: listener_recipient.clone(),
                    writer_shutdown,
                    codec: config.codec.clone(),
                });
                active = true;
            }
            Err(err) => {
                error!("accept() failed: {err}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Initial size of a [`TcpReaderActor`]'s read buffer, and thus about how
/// many bytes one read can fetch. Larger than tokio-util's default of
/// 8 KiB, so that one system call reads many small messages.
const READ_BUFFER_LEN: usize = 64 * 1024;

/// Arguments for spawning a [`TcpReaderActor<M, C>`].
pub struct TcpReaderArgs<M: Send + 'static, C> {
    /// Read half of the connection.
    pub read_half: OwnedReadHalf,
    /// Address of the remote peer.
    pub peer_addr: SocketAddr,
    /// Receives every decoded message, single or in batches.
    pub downstream: Downstream<M>,
    /// Receives [`ConnectionHalfClosed`] when the read half ends.
    pub listener: Recipient<ConnectionHalfClosed>,
    /// Writer of the same connection; asked to shut down when the read half
    /// ends.
    pub writer_shutdown: Recipient<Shutdown<M>>,
    /// Codec that decodes the messages.
    pub codec: C,
    /// How many messages that have arrived together are forwarded at once
    /// at most; see [`TcpServerArgs::max_batch_len`].
    pub max_batch_len: usize,
}

impl<M: Send + 'static, C> TcpReaderArgs<M, C> {
    /// Arguments with [`DEFAULT_MAX_BATCH_LEN`]; change it with
    /// [`with_max_batch_len`](Self::with_max_batch_len). `downstream` is a
    /// `Recipient<M>` or, to forward batches, a `Recipient<Batch<M>>`.
    pub fn new(
        read_half: OwnedReadHalf,
        peer_addr: SocketAddr,
        downstream: impl Into<Downstream<M>>,
        listener: Recipient<ConnectionHalfClosed>,
        writer_shutdown: Recipient<Shutdown<M>>,
        codec: C,
    ) -> Self {
        TcpReaderArgs {
            read_half,
            peer_addr,
            downstream: downstream.into(),
            listener,
            writer_shutdown,
            codec,
            max_batch_len: DEFAULT_MAX_BATCH_LEN,
        }
    }

    /// Forwards at most `max_batch_len` messages at once.
    ///
    /// # Panics
    ///
    /// Panics if `max_batch_len` is 0.
    pub fn with_max_batch_len(mut self, max_batch_len: usize) -> Self {
        assert!(max_batch_len > 0, "max batch length must be at least 1");
        self.max_batch_len = max_batch_len;
        self
    }
}

/// Actor that owns the read half ([`OwnedReadHalf`]) of a TCP connection and
/// reads messages of type `M` with the codec `C`.
///
/// Every decoded message is forwarded to `downstream`. Messages that one
/// read from the socket has delivered together are forwarded together, up
/// to `max_batch_len` at once: as one [`Batch<M>`] if `downstream` takes
/// batches, otherwise one after the other. The actor never waits for more
/// messages to fill a batch.
///
/// When the peer closes
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
    downstream: Downstream<M>,
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
    type Args = TcpReaderArgs<M, C>;
    type Error = io::Error;

    async fn on_start(args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        let framed: FramedRead<_, C> = FramedRead::with_capacity(args.read_half, args.codec, READ_BUFFER_LEN);

        let item_stream = stream::unfold(Some(framed), |state| async move {
            let mut framed = state?;
            let outcome = match framed.next().await {
                Some(Ok(item)) => return Some((ReadOutcome::Item(item), Some(framed))),
                Some(Err(err)) => ReadOutcome::Closed(CloseReason::Error(err.to_string())),
                None => ReadOutcome::Closed(CloseReason::Graceful),
            };
            Some((outcome, None))
        })
        // Whatever is decoded already, without waiting for more.
        .ready_chunks(args.max_batch_len)
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

impl<M, C> Message<StreamMessage<Vec<ReadOutcome<M>>, (), ()>> for TcpReaderActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(
        &mut self,
        msg: StreamMessage<Vec<ReadOutcome<M>>, (), ()>,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let outcomes = match msg {
            StreamMessage::Started(()) => {
                debug!("stream attached for {}", self.peer_addr);
                return;
            }
            StreamMessage::Next(outcomes) => outcomes,
            StreamMessage::Finished(()) => {
                ctx.stop();
                return;
            }
        };
        let mut closed = None;
        let mut items = Vec::with_capacity(outcomes.len());
        for outcome in outcomes {
            match outcome {
                ReadOutcome::Item(item) => items.push(item),
                ReadOutcome::Closed(reason) => closed = Some(reason),
            }
        }
        if let Err(err) = self.downstream.send_all(items).await {
            warn!("could not send message to downstream actor: {err}");
        }
        match closed {
            None => {}
            Some(reason) => {
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
        }
    }
}

impl<M, C> Message<Shutdown<M>> for TcpReaderActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, _msg: Shutdown<M>, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
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

/// Default capacity of the mailboxes of the reader and writer actors
/// spawned for a connection; see [`TcpServerArgs::mailbox_capacity`].
/// kameo's default.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 64;

/// Default for how long a write to the peer may make no progress before
/// it counts as failed; see [`TcpWriterArgs::write_timeout`].
pub const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(25);

/// Arguments for spawning a [`TcpWriterActor<M, C>`].
pub struct TcpWriterArgs<C> {
    /// Write half of the connection.
    pub write_half: OwnedWriteHalf,
    /// Address of the remote peer.
    pub peer_addr: SocketAddr,
    /// Receives [`ConnectionHalfClosed`] when the write half is closed.
    pub listener: Recipient<ConnectionHalfClosed>,
    /// Codec that encodes the messages.
    pub codec: C,
    /// How long a write may make no progress before it fails, e.g.
    /// because the peer is alive but no longer reads. `None` waits
    /// forever.
    pub write_timeout: Option<Duration>,
    /// Cancelling this token shuts the write half down like
    /// [`Shutdown<M>`], but also while a write is blocked and without
    /// waiting for the actor's mailbox.
    pub shutdown: CancellationToken,
}

impl<C> TcpWriterArgs<C> {
    /// Arguments with [`DEFAULT_WRITE_TIMEOUT`] and a new shutdown token;
    /// change the timeout with [`with_write_timeout`](Self::with_write_timeout).
    pub fn new(
        write_half: OwnedWriteHalf,
        peer_addr: SocketAddr,
        listener: Recipient<ConnectionHalfClosed>,
        codec: C,
    ) -> Self {
        TcpWriterArgs {
            write_half,
            peer_addr,
            listener,
            codec,
            write_timeout: Some(DEFAULT_WRITE_TIMEOUT),
            shutdown: CancellationToken::new(),
        }
    }

    /// Uses `write_timeout`; `None` waits forever.
    pub fn with_write_timeout(mut self, write_timeout: Option<Duration>) -> Self {
        self.write_timeout = write_timeout;
        self
    }
}

/// Encoded bytes from which a [`TcpWriterActor`] writes at once instead of
/// waiting for more messages.
const WRITE_BATCH_LEN: usize = 64 * 1024;

/// Sent by a [`TcpWriterActor`] to itself: everything queued before it
/// has been encoded, so the buffer can be written. Generic over the message
/// type like [`Shutdown<M>`], so that it cannot be `M` itself.
struct Flush<M>(PhantomData<fn() -> M>);

/// Actor that owns the write half ([`OwnedWriteHalf`]) of a TCP connection
/// and writes messages of type `M` with the codec `C`.
///
/// Every `M` sent to this actor is encoded and written. Messages that are
/// queued together are written together, with one system call where
/// possible: the actor encodes into a buffer and writes it once no more
/// messages are waiting or the buffer holds 64 KiB. A single message is
/// thus written right after it has been handled.
///
/// A message that the codec cannot encode is dropped with a warning; the
/// connection stays open, and no partially encoded bytes reach the peer.
/// If writing fails or makes no progress for the write timeout, the actor
/// reports [`ConnectionHalfClosed`] with [`CloseReason::Error`] and stops.
/// [`Shutdown<M>`] writes what is still buffered, shuts the write half
/// down in an orderly way, reports it as [`CloseReason::Graceful`] and
/// stops the actor. Cancelling the shutdown token does the same without
/// writing the buffer and also interrupts a blocked write; the peer then
/// receives a partial frame.
pub struct TcpWriterActor<M, C> {
    write_half: OwnedWriteHalf,
    codec: C,
    /// Encoded messages not yet written; reused between writes.
    buf: BytesMut,
    /// Whether a [`Flush`] is queued in the mailbox.
    flush_queued: bool,
    peer_addr: SocketAddr,
    listener: Recipient<ConnectionHalfClosed>,
    write_timeout: Option<Duration>,
    shutdown: CancellationToken,
    _msg: PhantomData<fn() -> M>,
}

impl<M, C> Actor for TcpWriterActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Args = TcpWriterArgs<C>;
    type Error = io::Error;

    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(TcpWriterActor {
            write_half: args.write_half,
            codec: args.codec,
            buf: BytesMut::new(),
            flush_queued: false,
            peer_addr: args.peer_addr,
            listener: args.listener,
            write_timeout: args.write_timeout,
            shutdown: args.shutdown,
            _msg: PhantomData,
        })
    }
}

impl<M, C> TcpWriterActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    /// Writes `buf`; fails if a single write makes no progress for the
    /// write timeout.
    async fn write_buf(&mut self) -> io::Result<()> {
        let mut written = 0;
        while written < self.buf.len() {
            let write = self.write_half.write(&self.buf[written..]);
            let n = match self.write_timeout {
                Some(limit) => tokio::time::timeout(limit, write).await.map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, format!("peer accepted no data for {limit:?}"))
                })??,
                None => write.await?,
            };
            if n == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            written += n;
        }
        self.buf.clear();
        Ok(())
    }

    /// Writes the buffered messages, unless the shutdown token interrupts
    /// it. Returns `false` if the actor has been closed instead.
    async fn flush(&mut self, ctx: &mut Context<Self, ()>) -> bool {
        if self.buf.is_empty() {
            return true;
        }
        let shutdown = self.shutdown.clone();
        let result = tokio::select! {
            result = self.write_buf() => Some(result),
            () = shutdown.cancelled() => None,
        };
        match result {
            Some(Ok(())) => true,
            Some(Err(err)) => {
                warn!("error while writing to {}: {err}", self.peer_addr);
                self.close(CloseReason::Error(err.to_string()), ctx).await;
                false
            }
            None => {
                info!("write to {} interrupted by shutdown", self.peer_addr);
                self.close(CloseReason::Graceful, ctx).await;
                false
            }
        }
    }

    /// Shuts the write half down if `reason` is graceful, reports the
    /// closed half and stops the actor.
    async fn close(&mut self, reason: CloseReason, ctx: &mut Context<Self, ()>) {
        if reason == CloseReason::Graceful
            && let Err(err) = self.write_half.shutdown().await
        {
            warn!("error while shutting down the write half to {}: {err}", self.peer_addr);
        }

        // Not awaited here: the listener may itself be waiting for room in
        // this actor's mailbox, which is released only once it stops.
        let listener = self.listener.clone();
        let closed = ConnectionHalfClosed {
            peer_addr: self.peer_addr,
            half: ConnectionHalf::Write,
            reason,
        };
        tokio::spawn(async move {
            if let Err(send_err) = listener.tell(closed).await {
                warn!("could not send write-half-closed to listener: {send_err}");
            }
        });

        ctx.stop();
    }
}

impl<M, C> Message<Shutdown<M>> for TcpWriterActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, _msg: Shutdown<M>, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if self.flush(ctx).await {
            self.close(CloseReason::Graceful, ctx).await;
        }
    }
}

impl<M, C> Message<Flush<M>> for TcpWriterActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, _msg: Flush<M>, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.flush_queued = false;
        self.flush(ctx).await;
    }
}

impl<M, C> Message<M> for TcpWriterActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, item: M, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        // After the grace period of a shutdown, queued messages are dropped.
        if self.shutdown.is_cancelled() {
            self.close(CloseReason::Graceful, ctx).await;
            return;
        }

        // A message the codec rejects neither closes the connection nor
        // leaves a partial frame in the buffer.
        let len = self.buf.len();
        if let Err(err) = self.codec.encode(item, &mut self.buf) {
            self.buf.truncate(len);
            warn!("message to {} dropped, cannot be encoded: {err}", self.peer_addr);
            return;
        }

        if self.buf.len() >= WRITE_BATCH_LEN {
            self.flush(ctx).await;
        } else if !self.flush_queued {
            // Queued behind the messages already waiting, which are thus
            // written together. If the mailbox is full, a later message
            // tries again.
            self.flush_queued = ctx.actor_ref().tell(Flush(PhantomData)).try_send().is_ok();
        }
    }
}

impl<M, C> Message<Batch<M>> for TcpWriterActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, batch: Batch<M>, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if self.shutdown.is_cancelled() {
            self.close(CloseReason::Graceful, ctx).await;
            return;
        }
        for item in batch {
            let len = self.buf.len();
            if let Err(err) = self.codec.encode(item, &mut self.buf) {
                self.buf.truncate(len);
                warn!("message to {} dropped, cannot be encoded: {err}", self.peer_addr);
                continue;
            }
            if self.buf.len() >= WRITE_BATCH_LEN && !self.flush(ctx).await {
                return;
            }
        }
        if !self.buf.is_empty() && !self.flush_queued {
            self.flush_queued = ctx.actor_ref().tell(Flush(PhantomData)).try_send().is_ok();
        }
    }
}

/// The writer of a connection, as seen by its server or client.
struct WriterHandle<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    actor_ref: ActorRef<TcpWriterActor<M, C>>,
    shutdown: CancellationToken,
    /// How long [`shut_down`](Self::shut_down) lets the writer finish the
    /// queued messages; `None` waits forever.
    grace_period: Option<Duration>,
}

impl<M, C> WriterHandle<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    /// Spawns a writer for `args` with a mailbox of `mailbox_capacity`;
    /// the write timeout is also the grace period.
    fn spawn(args: TcpWriterArgs<C>, mailbox_capacity: usize) -> Self {
        let shutdown = args.shutdown.clone();
        let grace_period = args.write_timeout;
        WriterHandle {
            actor_ref: TcpWriterActor::<M, C>::spawn_with_mailbox(args, kameo::mailbox::bounded(mailbox_capacity)),
            shutdown,
            grace_period,
        }
    }

    /// Asks the writer to shut down without waiting for it. The
    /// [`Shutdown<M>`] is queued behind the pending messages, so they are
    /// still written. If the writer has not stopped after the grace period,
    /// e.g. because the peer reads only a trickle, the token closes it at
    /// once and the remaining messages are dropped.
    fn shut_down(&self) {
        let actor_ref = self.actor_ref.clone();
        let shutdown = self.shutdown.clone();
        let grace_period = self.grace_period;
        tokio::spawn(async move {
            let stopped = async {
                let _ = actor_ref.tell(Shutdown::<M>::new()).await;
                actor_ref.wait_for_shutdown().await;
            };
            let Some(grace_period) = grace_period else {
                return stopped.await;
            };
            if tokio::time::timeout(grace_period, stopped).await.is_err() {
                warn!("writer did not stop within {grace_period:?}, closing it now");
                shutdown.cancel();
            }
        });
    }
}

/// Arguments for spawning a [`TcpClientActor<M, C>`].
pub struct TcpClientArgs<M: Send + 'static, C> {
    /// Address to connect to.
    pub remote_addr: SocketAddr,
    /// Receives every message read from the connection, single or in
    /// batches.
    pub downstream: Downstream<M>,
    /// Keepalive settings for the connection; `None` keeps the system
    /// defaults (usually no keepalive).
    pub keepalive: Option<KeepAlive>,
    /// Codec for the connection, e.g. `PusCodec::new(config)`; every
    /// connection uses a clone of it.
    pub codec: C,
    /// Receives the [`ConnectionEvent`]s of the connections.
    pub observer: Option<ConnectionObserver>,
    /// How long a write to the peer may make no progress before the
    /// connection counts as failed; see [`TcpWriterArgs::write_timeout`].
    pub write_timeout: Option<Duration>,
    /// How many messages the mailbox of each reader and writer actor
    /// spawned for a connection holds before senders have to wait. Larger
    /// mailboxes let the actors work in longer stretches, which raises the
    /// throughput of small messages, but more messages queue up before
    /// backpressure reaches the sender.
    pub mailbox_capacity: usize,
    /// How many messages that one read from the socket has delivered are
    /// forwarded to `downstream` at once at most. Larger batches raise the
    /// throughput of small messages; smaller ones limit how many messages
    /// queue up before backpressure reaches the peer, since a mailbox
    /// holds [`mailbox_capacity`](Self::mailbox_capacity) batches. 1
    /// forwards every message on its own.
    pub max_batch_len: usize,
}

impl<M: Send + 'static, C> TcpClientArgs<M, C> {
    /// Arguments with [`KeepAlive::default`], no observer,
    /// [`DEFAULT_WRITE_TIMEOUT`], [`DEFAULT_MAILBOX_CAPACITY`] and
    /// [`DEFAULT_MAX_BATCH_LEN`]; change them with
    /// [`with_keepalive`](Self::with_keepalive),
    /// [`with_observer`](Self::with_observer),
    /// [`with_write_timeout`](Self::with_write_timeout),
    /// [`with_mailbox_capacity`](Self::with_mailbox_capacity) and
    /// [`with_max_batch_len`](Self::with_max_batch_len). `downstream`
    /// receives every message on its own; see [`batched`](Self::batched).
    pub fn new(remote_addr: SocketAddr, downstream: Recipient<M>, codec: C) -> Self {
        Self::with_downstream(remote_addr, downstream.into(), codec)
    }

    /// Like [`new`](Self::new), but `downstream` receives the messages in
    /// batches; see [`TcpServerArgs::batched`].
    pub fn batched(remote_addr: SocketAddr, downstream: Recipient<Batch<M>>, codec: C) -> Self {
        Self::with_downstream(remote_addr, downstream.into(), codec)
    }

    fn with_downstream(remote_addr: SocketAddr, downstream: Downstream<M>, codec: C) -> Self {
        TcpClientArgs {
            remote_addr,
            downstream,
            keepalive: Some(KeepAlive::default()),
            codec,
            observer: None,
            write_timeout: Some(DEFAULT_WRITE_TIMEOUT),
            mailbox_capacity: DEFAULT_MAILBOX_CAPACITY,
            max_batch_len: DEFAULT_MAX_BATCH_LEN,
        }
    }

    /// Uses `keepalive` for the connection; `None` keeps the system
    /// defaults.
    pub fn with_keepalive(mut self, keepalive: Option<KeepAlive>) -> Self {
        self.keepalive = keepalive;
        self
    }

    /// Sends the [`ConnectionEvent`]s of the connections to `observer`.
    pub fn with_observer(mut self, observer: ConnectionObserver) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Uses `write_timeout` for the connection; `None` waits forever.
    pub fn with_write_timeout(mut self, write_timeout: Option<Duration>) -> Self {
        self.write_timeout = write_timeout;
        self
    }

    /// Uses `capacity` for the mailboxes of the reader and writer actors of
    /// each connection. The mailbox of the client actor itself is set
    /// when spawning it, e.g. with
    /// `spawn_with_mailbox(args, kameo::mailbox::bounded(capacity))`.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is 0.
    pub fn with_mailbox_capacity(mut self, capacity: usize) -> Self {
        assert!(capacity > 0, "mailbox capacity must be at least 1");
        self.mailbox_capacity = capacity;
        self
    }

    /// Forwards at most `max_batch_len` messages to `downstream` at once.
    ///
    /// # Panics
    ///
    /// Panics if `max_batch_len` is 0.
    pub fn with_max_batch_len(mut self, max_batch_len: usize) -> Self {
        assert!(max_batch_len > 0, "max batch length must be at least 1");
        self.max_batch_len = max_batch_len;
        self
    }
}

/// Actor that opens an outgoing TCP connection to `remote_addr` and, just
/// like [`TcpServerActor`] after an `accept()`, spawns a
/// [`TcpReaderActor<M, C>`] (reading) and a [`TcpWriterActor<M, C>`]
/// (writing) for it.
///
/// Messages:
/// - [`Connect`]: connects; replies with the peer address, or an error of
///   kind [`io::ErrorKind::AlreadyExists`] if already connected.
/// - `M`: sends the message over the connection (dropped with a warning
///   if not connected; requires `M: WireMessage`).
/// - [`Close`], [`CloseRead`], [`CloseWrite`]: close both or one half.
/// - [`ConnectionEvent`] from a server (see below).
/// - [`ConnectionHalfClosed`]: sent by the reader and writer; once both
///   halves are closed, the actor can connect again.
///
/// If an observer is set (see [`TcpClientArgs::with_observer`]), it
/// receives the [`ConnectionEvent`]s of every connection, all sent with
/// `tell`. [`ConnectionEventKind::Connected`] arrives before the first
/// message of the connection goes downstream.
///
/// # Coupling with a server
///
/// A client can open and close its connection together with the connection
/// of a [`TcpServerActor`], e.g. to forward everything a peer sends to the
/// server on to another peer:
///
/// ```text
/// peer A ──▶ server ──M, events──▶ [actor] ──M, events──▶ client ──▶ peer B
///        ◀──        ◀──M, events── [actor] ◀──M, events──        ◀──
/// ```
///
/// Each actor sends its messages and its events to the same actor: the
/// server's downstream and observer is the actor on the way to the client,
/// and vice versa. Actors in between forward both messages and events. The
/// events then travel in the same mailboxes as the messages and keep their
/// order: `Connected` arrives before the first message of a connection and
/// `Disconnected` after its last one, so no message is lost or overtaken.
///
/// The client reacts to the server's events:
/// - `Connected`: it connects to `remote_addr`. If its previous connection
///   is still closing, it connects once that is closed and keeps the
///   messages it receives in the meantime. If connecting fails, it reports
///   [`ConnectionEventKind::ConnectFailed`].
/// - `Disconnected`: it closes the connection it opened for that server
///   connection.
///
/// Its events carry the server's connection ID as
/// [`peer_connection_id`](ConnectionEvent::peer_connection_id), and the
/// server closes its connection on `Disconnected` or `ConnectFailed` of the
/// matching client connection. Connections are always closed completely.
///
/// Without actors in between, the server's downstream and observer is the
/// client and vice versa.
pub struct TcpClientActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    remote_addr: SocketAddr,
    downstream: Downstream<M>,
    max_batch_len: usize,
    observer: Option<ConnectionObserver>,
    keepalive: Option<KeepAlive>,
    write_timeout: Option<Duration>,
    mailbox_capacity: usize,
    codec: C,
    last_id: u64,
    connection: Option<ClientConnection<M, C>>,
    /// A connection to open once the current one is closed.
    pending: Option<PendingConnect<M>>,
}

/// The reader and writer of the current connection and which halves are
/// closed.
struct ClientConnection<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    id: u64,
    peer_connection_id: Option<u64>,
    peer_addr: SocketAddr,
    reader_ref: ActorRef<TcpReaderActor<M, C>>,
    writer: WriterHandle<M, C>,
    read_closed: bool,
    write_closed: bool,
    /// Whether [`Close`] was requested; messages are no longer written.
    closing: bool,
    /// Callers of [`Close`] with `ask`, answered once both halves are closed.
    close_replies: Vec<ReplySender<()>>,
}

/// A connection requested by a server's `Connected` event while the
/// previous connection was still closing, and the messages for it.
struct PendingConnect<M> {
    peer_connection_id: u64,
    messages: VecDeque<M>,
}

impl<M, C> Actor for TcpClientActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Args = TcpClientArgs<M, C>;
    type Error = Infallible;

    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(TcpClientActor {
            remote_addr: args.remote_addr,
            downstream: args.downstream,
            max_batch_len: args.max_batch_len,
            observer: args.observer,
            keepalive: args.keepalive,
            write_timeout: args.write_timeout,
            mailbox_capacity: args.mailbox_capacity,
            codec: args.codec,
            last_id: 0,
            connection: None,
            pending: None,
        })
    }
}

impl<M, C> TcpClientActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    fn event(
        &self,
        id: u64,
        peer_connection_id: Option<u64>,
        peer_addr: SocketAddr,
        kind: ConnectionEventKind,
    ) -> ConnectionEvent {
        ConnectionEvent {
            source: EventSource::Client,
            connection_id: id,
            peer_connection_id,
            peer_addr,
            kind,
        }
    }

    /// Connects to `remote_addr` and spawns the reader and writer; reports
    /// `Connected` or `ConnectFailed`.
    async fn open(&mut self, actor_ref: ActorRef<Self>, peer_connection_id: Option<u64>) -> io::Result<SocketAddr> {
        self.last_id += 1;
        let id = self.last_id;
        let stream = match TcpStream::connect(self.remote_addr).await {
            Ok(stream) => stream,
            Err(err) => {
                warn!("TcpClientActor: cannot connect to {}: {err}", self.remote_addr);
                let kind = ConnectionEventKind::ConnectFailed {
                    reason: err.to_string(),
                };
                notify(
                    &self.observer,
                    self.event(id, peer_connection_id, self.remote_addr, kind),
                )
                .await;
                return Err(err);
            }
        };
        let peer_addr = stream.peer_addr()?;
        if let Err(err) = configure_keepalive(&stream, self.keepalive) {
            warn!("cannot set keepalive for {peer_addr}: {err}");
        }
        let (read_half, write_half) = stream.into_split();

        info!(
            "TcpClientActor<{}>: connected to {peer_addr}",
            std::any::type_name::<M>()
        );

        let listener_recipient = actor_ref.recipient::<ConnectionHalfClosed>();

        let writer = WriterHandle::spawn(
            TcpWriterArgs::new(write_half, peer_addr, listener_recipient.clone(), self.codec.clone())
                .with_write_timeout(self.write_timeout),
            self.mailbox_capacity,
        );
        let writer_shutdown = writer.actor_ref.clone().recipient::<Shutdown<M>>();

        let connected = self.event(id, peer_connection_id, peer_addr, ConnectionEventKind::Connected);
        notify(&self.observer, connected).await;

        let reader_ref = TcpReaderActor::<M, C>::spawn_with_mailbox(
            TcpReaderArgs {
                read_half,
                peer_addr,
                downstream: self.downstream.clone(),
                max_batch_len: self.max_batch_len,
                listener: listener_recipient,
                writer_shutdown,
                codec: self.codec.clone(),
            },
            kameo::mailbox::bounded(self.mailbox_capacity),
        );

        self.connection = Some(ClientConnection {
            id,
            peer_connection_id,
            peer_addr,
            reader_ref,
            writer,
            read_closed: false,
            write_closed: false,
            closing: false,
            close_replies: Vec::new(),
        });
        Ok(peer_addr)
    }

    /// Opens the pending connection, if any, and writes the messages kept
    /// for it.
    async fn open_pending(&mut self, actor_ref: ActorRef<Self>) {
        let Some(pending) = self.pending.take() else {
            return;
        };
        if self.open(actor_ref, Some(pending.peer_connection_id)).await.is_err() {
            if !pending.messages.is_empty() {
                warn!(
                    "TcpClientActor: {} message(s) dropped, cannot connect",
                    pending.messages.len()
                );
            }
            return;
        }
        if let Some(conn) = &self.connection {
            for item in pending.messages {
                if let Err(err) = conn.writer.actor_ref.tell(item).await {
                    warn!("TcpClientActor: could not send message to writer: {err}");
                }
            }
        }
    }

    /// Asks the reader and writer of the current connection to close.
    async fn close_current(&mut self) {
        let Some(conn) = &mut self.connection else {
            return;
        };
        conn.closing = true;
        // Either actor may have stopped already; it has then reported its
        // half as closed.
        if !conn.read_closed {
            let _ = conn.reader_ref.tell(Shutdown::<M>::new()).await;
        }
        if !conn.write_closed {
            conn.writer.shut_down();
        }
    }
}

impl<M, C> Message<Connect> for TcpClientActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = io::Result<SocketAddr>;

    async fn handle(&mut self, _msg: Connect, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if self.connection.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "TcpClientActor is already connected",
            ));
        }
        self.open(ctx.actor_ref().clone(), None).await
    }
}

impl<M, C> Message<Close> for TcpClientActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = DelegatedReply<()>;

    async fn handle(&mut self, _msg: Close, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.pending = None;
        if self.connection.is_none() {
            debug!("TcpClientActor: Close ignored, not connected");
            return ctx.reply(());
        }
        self.close_current().await;
        let (delegated, reply_sender) = ctx.reply_sender();
        if let Some(conn) = &mut self.connection {
            conn.close_replies.extend(reply_sender);
        }
        delegated
    }
}

impl<M, C> Message<CloseRead> for TcpClientActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, _msg: CloseRead, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if let Some(conn) = &self.connection
            && let Err(err) = conn.reader_ref.tell(Shutdown::<M>::new()).await
        {
            warn!("TcpClientActor: could not send shutdown to reader: {err}");
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
            conn.writer.shut_down();
        }
    }
}

/// Reacts to the events of a server's connection; see "Coupling with a
/// server" above. Events of clients are ignored.
impl<M, C> Message<ConnectionEvent> for TcpClientActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, event: ConnectionEvent, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if event.source != EventSource::Server {
            return;
        }
        match event.kind {
            ConnectionEventKind::Connected => {
                let pending = PendingConnect {
                    peer_connection_id: event.connection_id,
                    messages: VecDeque::new(),
                };
                match &self.connection {
                    None => {
                        self.pending = Some(pending);
                        self.open_pending(ctx.actor_ref().clone()).await;
                    }
                    Some(conn) => {
                        debug!(
                            "TcpClientActor: connecting once the connection to {} is closed",
                            conn.peer_addr
                        );
                        self.pending = Some(pending);
                        self.close_current().await;
                    }
                }
            }
            ConnectionEventKind::Disconnected => {
                if self
                    .pending
                    .as_ref()
                    .is_some_and(|p| p.peer_connection_id == event.connection_id)
                {
                    self.pending = None;
                } else if self
                    .connection
                    .as_ref()
                    .is_some_and(|c| c.peer_connection_id == Some(event.connection_id))
                {
                    self.close_current().await;
                }
            }
            ConnectionEventKind::HalfClosed { .. } | ConnectionEventKind::ConnectFailed { .. } => {}
        }
    }
}

impl<M, C> Message<ConnectionHalfClosed> for TcpClientActor<M, C>
where
    M: Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, msg: ConnectionHalfClosed, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        info!(
            "TcpClientActor: half {:?} of the connection to {} closed ({:?})",
            msg.half, msg.peer_addr, msg.reason
        );

        let Some(conn) = self.connection.as_mut().filter(|conn| conn.peer_addr == msg.peer_addr) else {
            warn!("TcpClientActor: close event for unknown connection {}", msg.peer_addr);
            return;
        };
        match msg.half {
            ConnectionHalf::Read => conn.read_closed = true,
            ConnectionHalf::Write => conn.write_closed = true,
        }
        let (id, peer_id, disconnected) = (conn.id, conn.peer_connection_id, conn.read_closed && conn.write_closed);

        let kind = ConnectionEventKind::HalfClosed {
            half: msg.half,
            reason: msg.reason,
        };
        notify(&self.observer, self.event(id, peer_id, msg.peer_addr, kind)).await;
        if disconnected {
            info!("TcpClientActor: connection to {} fully closed", msg.peer_addr);
            let close_replies = self
                .connection
                .take()
                .map(|conn| conn.close_replies)
                .unwrap_or_default();
            notify(
                &self.observer,
                self.event(id, peer_id, msg.peer_addr, ConnectionEventKind::Disconnected),
            )
            .await;
            for reply in close_replies {
                reply.send(());
            }
            self.open_pending(ctx.actor_ref().clone()).await;
        }
    }
}

impl<M, C> Message<M> for TcpClientActor<M, C>
where
    M: WireMessage + Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, item: M, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if let Some(pending) = &mut self.pending {
            pending.messages.push_back(item);
            return;
        }
        match &self.connection {
            Some(conn) if !conn.closing => {
                // Waits while the writer's mailbox is full; the write
                // timeout bounds that wait if the peer stops reading.
                if let Err(err) = conn.writer.actor_ref.tell(item).await {
                    warn!("TcpClientActor: could not send message to writer: {err}");
                }
            }
            _ => warn!("TcpClientActor: message dropped, not connected"),
        }
    }
}

impl<M, C> Message<Batch<M>> for TcpClientActor<M, C>
where
    M: WireMessage + Send + 'static,
    C: MessageCodec<M>,
{
    type Reply = ();

    async fn handle(&mut self, batch: Batch<M>, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if let Some(pending) = &mut self.pending {
            pending.messages.extend(batch);
            return;
        }
        match &self.connection {
            Some(conn) if !conn.closing => {
                if let Err(err) = conn.writer.actor_ref.tell(batch).await {
                    warn!("TcpClientActor: could not send messages to writer: {err}");
                }
            }
            _ => warn!("TcpClientActor: messages dropped, not connected"),
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
        let keepalive = KeepAlive {
            idle: Duration::from_secs(7),
            interval: Duration::from_secs(2),
            retries: 4,
        };

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
