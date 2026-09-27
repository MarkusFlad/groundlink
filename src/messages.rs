//! Messages exchanged with the generic TCP actors in [`crate::actors`] and
//! the [`MessageCodec`] trait that ties a message type to its codec.

use std::io;
use std::net::SocketAddr;

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

/// Marker for message types that the generic TCP actors send and receive.
///
/// A [`TcpServerActor<M, C>`](crate::actors::TcpServerActor) or
/// [`TcpClientActor<M, C>`](crate::actors::TcpClientActor) handles an `M`
/// by writing it to its current connection, which requires
/// `M: WireMessage`. Implement it for the message type of your own
/// protocol:
///
/// ```
/// use groundlink::WireMessage;
///
/// struct MyMessage(Vec<u8>);
///
/// impl WireMessage for MyMessage {}
/// ```
///
/// The control messages of this crate (e.g. [`GetLocalAddr`] or
/// [`Connect`]) never implement it. This lets the actors implement both
/// `Message<M>` and `Message<GetLocalAddr>` without the implementations
/// overlapping.
pub trait WireMessage {}

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
