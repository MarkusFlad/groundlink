//! Protocols that plug into the generic TCP actors, and the traits that
//! connect a protocol to them.
//!
//! - [`ccsds`]: CCSDS Space Packet types and codec (CCSDS 133.0-B-2).
//! - [`cuc`]: CCSDS Unsegmented Time Code with conversion from and to UTC.
//! - [`pus`]: ECSS PUS-C telecommand and telemetry packets on top of Space
//!   Packets, their codec and typed service messages.
//! - [`simple_string`]: a simple length-prefixed string protocol.
//!
//! A protocol consists of a message type implementing [`WireMessage`] and a
//! codec implementing [`MessageCodec`].

use std::io;

use tokio_util::codec::{Decoder, Encoder};

pub mod ccsds;
pub mod cuc;
pub mod pus;
pub mod simple_string;

/// A codec that can both decode (read) and encode (write) messages of type
/// `M`.
///
/// This is the requirement for using `M` with
/// [`TcpReaderActor<M, C>`](crate::actor::tcp::TcpReaderActor) and
/// [`TcpWriterActor<M, C>`](crate::actor::tcp::TcpWriterActor). The trait has
/// a blanket implementation, so any codec satisfying the bounds implements
/// it automatically. The codec is passed to the actors as a value (e.g. in
/// [`TcpServerArgs::codec`](crate::actor::tcp::TcpServerArgs::codec)); every
/// connection uses a clone of it.
pub trait MessageCodec<M>:
    Decoder<Item = M, Error = io::Error> + Encoder<M, Error = io::Error> + Clone + Unpin + Send + 'static
{
}

impl<M, C> MessageCodec<M> for C where
    C: Decoder<Item = M, Error = io::Error> + Encoder<M, Error = io::Error> + Clone + Unpin + Send + 'static
{
}

/// Marker for message types that the generic TCP actors send and receive.
///
/// A [`TcpServerActor<M, C>`](crate::actor::tcp::TcpServerActor) or
/// [`TcpClientActor<M, C>`](crate::actor::tcp::TcpClientActor) handles an `M`
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
/// The control messages of this crate (e.g. [`GetLocalAddr`](crate::GetLocalAddr) or
/// [`Connect`](crate::Connect)) never implement it. This lets the actors implement both
/// `Message<M>` and `Message<GetLocalAddr>` without the implementations
/// overlapping.
pub trait WireMessage {}
