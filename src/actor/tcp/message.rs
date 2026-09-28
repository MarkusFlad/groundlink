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

/// Closes both halves of the current connection of a
/// [`TcpServerActor`](crate::actor::tcp::TcpServerActor) or
/// [`TcpClientActor`](crate::actor::tcp::TcpClientActor) in an orderly way
/// (if connected), by sending [`Shutdown<M>`] to the reader and the writer.
///
/// Sent to a `TcpClientActor` with `ask`, the reply comes once both halves
/// are closed, so a following [`Connect`] cannot fail because the old
/// connection still exists.
#[derive(Debug)]
pub struct Close;

/// Closes only the read half of the current connection of a
/// [`TcpClientActor`](crate::actor::tcp::TcpClientActor).
#[derive(Debug)]
pub struct CloseRead;

/// Closes only the write half of the current connection of a
/// [`TcpClientActor`](crate::actor::tcp::TcpClientActor).
#[derive(Debug)]
pub struct CloseWrite;

/// Which kind of actor reported a [`ConnectionEvent`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSource {
    /// A [`TcpServerActor`](crate::actor::tcp::TcpServerActor), about a
    /// connection it accepted.
    Server,
    /// A [`TcpClientActor`](crate::actor::tcp::TcpClientActor), about a
    /// connection it opened.
    Client,
}

/// What happened to a connection; see [`ConnectionEvent`].
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionEventKind {
    /// The connection has been set up.
    Connected,
    /// One half of the connection was closed.
    HalfClosed {
        /// The half that was closed.
        half: ConnectionHalf,
        /// Why it was closed.
        reason: CloseReason,
    },
    /// Both halves of the connection are closed.
    Disconnected,
    /// A [`TcpClientActor`](crate::actor::tcp::TcpClientActor) could not
    /// connect; `peer_addr` is the address it tried to connect to.
    ConnectFailed {
        /// The `Display` output of the I/O error.
        reason: String,
    },
}

/// Lifecycle event of a connection, sent to the observer of a
/// [`TcpServerActor`](crate::actor::tcp::TcpServerActor) or
/// [`TcpClientActor`](crate::actor::tcp::TcpClientActor) (see
/// [`TcpServerArgs::with_observer`](crate::actor::tcp::TcpServerArgs::with_observer)).
///
/// For every connection, the observer receives `Connected`, any
/// `HalfClosed` events and finally `Disconnected`, in this order; a failed
/// connection attempt of a client is reported as `ConnectFailed` instead.
///
/// Servers and clients also handle the events of the other side, which
/// couples two connections: a client connects when it receives `Connected`
/// from a server and closes on `Disconnected`, and a server closes when it
/// receives `Disconnected` or `ConnectFailed` from the client connection
/// that belongs to its current connection. See
/// [`TcpClientActor`](crate::actor::tcp::TcpClientActor) for how to wire
/// them.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionEvent {
    /// Which kind of actor reported the event.
    pub source: EventSource,
    /// Number of the connection, counting from 1 for each actor. It tells
    /// apart the events of consecutive connections to the same peer.
    pub connection_id: u64,
    /// For a client connection opened because of a server's `Connected`
    /// event: the `connection_id` of that server connection. `None` for
    /// server connections and for client connections opened with
    /// [`Connect`].
    pub peer_connection_id: Option<u64>,
    /// Address of the remote peer of the connection.
    pub peer_addr: SocketAddr,
    /// What happened.
    pub kind: ConnectionEventKind,
}

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
