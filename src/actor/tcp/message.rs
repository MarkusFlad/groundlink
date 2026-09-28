//! Control messages of the generic TCP actors: requests such as
//! [`Connect`] or [`GetLocalAddr`] and notifications such as
//! [`ConnectionHalfClosed`].

use std::net::SocketAddr;

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
/// Sent to the [`TcpServerActor`](crate::actor::tcp::TcpServerActor) (or
/// [`TcpClientActor`](crate::actor::tcp::TcpClientActor)) by the
/// [`TcpReaderActor`](crate::actor::tcp::TcpReaderActor) for the read
/// half and by the [`TcpWriterActor`](crate::actor::tcp::TcpWriterActor) for
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

/// Asks a [`TcpServerActor`](crate::actor::tcp::TcpServerActor) for the local
/// address it is actually bound to.
///
/// Useful after binding to port 0 (e.g. in tests) to learn the port chosen
/// by the operating system.
#[derive(Debug)]
pub struct GetLocalAddr;

/// Asks the [`TcpClientActor`](crate::actor::tcp::TcpClientActor) to connect
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
