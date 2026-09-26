//! Messages exchanged with the generic TCP actors in [`crate::actors`] and
//! the [`MessageCodec`] trait that ties a message type to its codec.

use std::io;
use std::net::SocketAddr;

use kameo::actor::Recipient;
use tokio_util::codec::{Decoder, Encoder};

/// Why one half of a connection (reading or writing) was closed.
#[derive(Debug, Clone, PartialEq)]
pub enum CloseReason {
    /// Clean end: EOF while reading, or the connection was shut down in an
    /// orderly way.
    Graceful,
    /// An I/O or protocol error (e.g. invalid frame data, broken pipe,
    /// connection reset, …). The text is the `Display` output of the
    /// underlying error.
    Error(String),
}

/// Which half of a connection is affected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionHalf {
    /// The read half (receiving direction).
    Read,
    /// The write half (sending direction).
    Write,
}

/// Notification that one half of a connection was closed.
///
/// Sent to the [`TcpServerActor`](crate::actors::TcpServerActor) (or
/// [`TcpClientActor`](crate::actors::TcpClientActor)) by the
/// [`TcpReaderActor`](crate::actors::TcpReaderActor) for the read
/// half and by the [`TcpWriterActor`](crate::actors::TcpWriterActor) for
/// the write half.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionHalfClosed {
    /// Address of the remote peer of the connection.
    pub peer_addr: SocketAddr,
    /// The half that was closed.
    pub half: ConnectionHalf,
    /// Why it was closed.
    pub reason: CloseReason,
}

/// A codec that can both decode (read) and encode (write) messages of type
/// `M`.
///
/// This is the requirement for using `M` with
/// [`TcpReaderActor<M, C>`](crate::actors::TcpReaderActor) and
/// [`TcpWriterActor<M, C>`](crate::actors::TcpWriterActor). The trait has
/// a blanket implementation, so any codec satisfying the bounds implements
/// it automatically; the actors create their codec via [`Default`].
pub trait MessageCodec<M>:
    Decoder<Item = M, Error = io::Error>
        + Encoder<M, Error = io::Error>
        + Default
        + Unpin
        + Send
        + 'static
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

/// Asks a [`TcpServerActor`](crate::actors::TcpServerActor) for the local
/// address it is actually bound to.
///
/// Useful after binding to port 0 (e.g. in tests) to learn the port chosen
/// by the operating system.
#[derive(Debug)]
pub struct GetLocalAddr;

/// Asks the [`TcpClientActor`](crate::actors::TcpClientActor) to connect
/// to its configured remote address.
///
/// Replies with the actual peer address on success, or the I/O error that
/// occurred.
#[derive(Debug)]
pub struct Connect;

/// Closes both halves of the current connection in an orderly way (if
/// connected), by sending [`Shutdown<M>`] to the reader and the writer.
#[derive(Debug)]
pub struct Close;

/// Closes only the read half of the current connection.
#[derive(Debug)]
pub struct CloseRead;

/// Closes only the write half of the current connection.
#[derive(Debug)]
pub struct CloseWrite;

/// Notification that the other side of a relay coupling (e.g. the server
/// connection when this actor is the client side) closed one of its
/// halves.
#[derive(Debug)]
pub struct PeerHalfClosed(pub ConnectionHalfClosed);

/// Asks a [`TcpClientActor<M, C>`](crate::actors::TcpClientActor) or
/// [`TcpServerActor<M, C>`](crate::actors::TcpServerActor) to send the
/// contained message over its current connection.
///
/// Without a connection the message is dropped with a warning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relay<M>(pub M);

/// Where an actor sends messages of type `M`: either directly to an actor
/// that handles `M`, or wrapped in [`Relay<M>`] to a
/// [`TcpServerActor`](crate::actors::TcpServerActor) or
/// [`TcpClientActor`](crate::actors::TcpClientActor), which writes them to
/// its current connection.
///
/// Actors that produce messages (e.g. the PUS actors in
/// [`crate::pus_actors`]) take an `impl Into<MessageSink<M>>`, so they
/// accept both a `Recipient<M>` and a `Recipient<Relay<M>>` without an
/// adapter actor in between.
///
/// ```
/// use kameo::actor::Spawn;
/// use kameo_tcp_example::{GetMessages, MessageSink, Relay, TestActor};
///
/// # #[tokio::main]
/// # async fn main() {
/// let direct = TestActor::<u32>::spawn(TestActor::new());
/// let relayed = TestActor::<Relay<u32>>::spawn(TestActor::new());
///
/// MessageSink::from(direct.clone().recipient::<u32>()).tell(1).await.unwrap();
/// MessageSink::from(relayed.clone().recipient::<Relay<u32>>()).tell(2).await.unwrap();
///
/// assert_eq!(direct.ask(GetMessages::new()).await.unwrap(), vec![1]);
/// assert_eq!(relayed.ask(GetMessages::new()).await.unwrap(), vec![Relay(2)]);
/// # }
/// ```
pub enum MessageSink<M: Send + 'static> {
    /// Send `M` as is.
    Direct(Recipient<M>),
    /// Send `M` wrapped in [`Relay<M>`].
    Relay(Recipient<Relay<M>>),
}

impl<M: Send + 'static> MessageSink<M> {
    /// Sends `msg` to the target, wrapping it in [`Relay`] if needed.
    ///
    /// # Errors
    ///
    /// Fails if the target actor is not running.
    pub async fn tell(&self, msg: M) -> Result<(), SinkError> {
        match self {
            MessageSink::Direct(recipient) => recipient.tell(msg).await.map_err(|err| SinkError(err.to_string())),
            MessageSink::Relay(recipient) => {
                recipient.tell(Relay(msg)).await.map_err(|err| SinkError(err.to_string()))
            }
        }
    }
}

impl<M: Send + 'static> Clone for MessageSink<M> {
    fn clone(&self) -> Self {
        match self {
            MessageSink::Direct(recipient) => MessageSink::Direct(recipient.clone()),
            MessageSink::Relay(recipient) => MessageSink::Relay(recipient.clone()),
        }
    }
}

impl<M: Send + 'static> std::fmt::Debug for MessageSink<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageSink::Direct(_) => f.write_str("MessageSink::Direct"),
            MessageSink::Relay(_) => f.write_str("MessageSink::Relay"),
        }
    }
}

impl<M: Send + 'static> From<Recipient<M>> for MessageSink<M> {
    fn from(recipient: Recipient<M>) -> Self {
        MessageSink::Direct(recipient)
    }
}

impl<M: Send + 'static> From<Recipient<Relay<M>>> for MessageSink<M> {
    fn from(recipient: Recipient<Relay<M>>) -> Self {
        MessageSink::Relay(recipient)
    }
}

/// Error of [`MessageSink::tell`]: the message could not be delivered
/// because the target actor is not running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkError(String);

impl std::fmt::Display for SinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SinkError {}

/// Asks the reader or writer actor for message type `M` to close its half
/// of the connection in an orderly way and then stop.
///
/// The type parameter only selects the actor; it is carried as
/// `PhantomData<fn() -> M>` so that `Shutdown<M>` is `Send`, `Sync` and
/// `Copy` for every `M`.
pub struct Shutdown<M>(std::marker::PhantomData<fn() -> M>);

impl<M> Shutdown<M> {
    /// Creates the message.
    pub fn new() -> Self {
        Shutdown(std::marker::PhantomData)
    }
}

impl<M> Default for Shutdown<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M> Clone for Shutdown<M> {
    fn clone(&self) -> Self {
        Shutdown(std::marker::PhantomData)
    }
}

impl<M> Copy for Shutdown<M> {}

impl<M> std::fmt::Debug for Shutdown<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Shutdown").finish()
    }
}
