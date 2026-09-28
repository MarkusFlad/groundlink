//! Generic TCP actors built on [`kameo`], plus three protocols that plug
//! into them: a simple length-prefixed string protocol, CCSDS Space
//! Packets and ECSS PUS-C packets.
//!
//! # Overview
//!
//! The TCP actors in [`actor::tcp`] are generic over a message type `M` and
//! a codec `C` implementing [`MessageCodec<M>`]. Every protocol therefore gets
//! the same set of actors through type aliases:
//!
//! | Protocol | Message | Codec | Actors |
//! |---|---|---|---|
//! | [`simple_string`](protocol::simple_string) | [`SimpleString`] | [`SimpleStringCodec`] | [`SimpleStringServer`], [`SimpleStringClient`], … |
//! | [`ccsds`](protocol::ccsds) | [`SpacePacket`] | [`SpacePacketCodec`] | [`SpacePacketServer`], [`SpacePacketClient`], … |
//! | [`pus`](protocol::pus) | [`PusPacket`] | [`PusCodec`] | [`PusServer`], [`PusClient`], … |
//!
//! On top of the PUS packets, [`actor::pus`] provides actors that handle
//! telecommands for an application process: [`PusTcAcceptor`] performs
//! the acceptance check (including the PUS format, when it receives
//! [`SpacePacket`]s) and reports it via PUS service 1,
//! [`PusTestServiceActor`] implements PUS service 17, and
//! [`PusTmStamper`] assigns sequence counts, message type counters and time
//! stamps to the telemetry of an APID.
//!
//! # Modules
//!
//! - [`protocol`]: the protocols and the traits [`MessageCodec`] and
//!   [`WireMessage`] that connect them to the TCP actors.
//!   - [`protocol::ccsds`]: CCSDS Space Packet types and codec (CCSDS
//!     133.0-B-2).
//!   - [`protocol::cuc`]: CCSDS Unsegmented Time Code with conversion from
//!     and to UTC.
//!   - [`protocol::pus`]: ECSS PUS-C telecommand and telemetry packets on
//!     top of Space Packets, their codec and typed service messages.
//!   - [`protocol::simple_string`]: the simple string protocol.
//! - [`actor`]: the actors.
//!   - [`actor::tcp`]: generic TCP server, client, reader and writer actors,
//!     their control messages and the per-protocol type aliases.
//!   - [`actor::pus`]: actors that process PUS packets.
//!   - [`actor::test`]: a generic actor that records messages, for
//!     assertions in tests.
//!
//! The most important items are re-exported at the crate root.
//!
//! # Example programs
//!
//! - `simple_string_server`: a [`SimpleStringServer`] that records what
//!   it receives.
//! - `pus_server <tc_port> <tm_port>`: receives telecommands on one port
//!   with the actor chain [`SpacePacketServer`] → [`PusTcAcceptor`] →
//!   [`PusTestServiceActor`] and sends all telemetry through a
//!   [`PusTmStamper`] to a second [`PusServer`], which writes it to the
//!   client connected to the second port.
//! - `pus_client <address> <tc_port> <tm_port>`: an interactive client with
//!   two [`PusClient`]s that sends TC(17,1) and prints the received
//!   telemetry.
//!
//! Run them with e.g. `cargo run --example pus_server -- 9000 9001` and
//! `cargo run --example pus_client -- 127.0.0.1 9000 9001`.
//!
//! # Logging
//!
//! The crate logs through [`tracing`]. It does not install a subscriber;
//! the application decides whether and how messages are emitted, e.g. with
//! `tracing_subscriber::fmt`.
//!
//! # Example
//!
//! A server that forwards every received [`SimpleString`] to a
//! [`TestActor`]:
//!
//! ```
//! use std::time::Duration;
//!
//! use futures::SinkExt;
//! use kameo::actor::Spawn;
//! use groundlink::{
//!     GetLocalAddr, SimpleString, SimpleStringCodec, SimpleStringServer, TcpServerArgs,
//!     TestActor,
//! };
//! use tokio_util::codec::Framed;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let received = TestActor::<SimpleString>::spawn(TestActor::new());
//! let server = SimpleStringServer::spawn(TcpServerArgs::new(
//!     "127.0.0.1:0".parse().unwrap(),
//!     received.clone().recipient(),
//!     SimpleStringCodec::default(),
//! ));
//! let addr = server.ask(GetLocalAddr).await.unwrap();
//!
//! let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
//! let mut client = Framed::new(stream, SimpleStringCodec::default());
//! client.send(SimpleString("hello".into())).await.unwrap();
//!
//! let messages = TestActor::assert_received(&received, 1, Duration::from_secs(1)).await;
//! assert_eq!(messages, vec![SimpleString("hello".into())]);
//! # }
//! ```

#![warn(missing_docs)]

pub mod actor;
pub mod protocol;

pub use actor::pus::{PusPacketAdapter, PusTcAcceptor, PusTestServiceActor, PusTmStamper};
pub use actor::tcp::{
    Close, CloseRead, CloseReason, CloseWrite, Connect, ConnectionHalf, ConnectionHalfClosed, ConnectionPolicy,
    GetLocalAddr, KeepAlive, PeerHalfClosed, PusClient, PusReader, PusServer, PusWriter, Shutdown, SimpleStringClient,
    SimpleStringReader, SimpleStringServer, SimpleStringWriter, SpacePacketClient, SpacePacketReader,
    SpacePacketServer, SpacePacketWriter, TcpClientActor, TcpClientArgs, TcpReaderActor, TcpReaderArgs, TcpServerActor,
    TcpServerArgs, TcpWriterActor, TcpWriterArgs,
};
pub use actor::test::{GetMessages, TestActor};
pub use protocol::ccsds::{PacketType, SequenceFlags, SpacePacket, SpacePacketCodec, SpacePacketHeader};
pub use protocol::cuc::{CucEpoch, CucFormat, CucTime};
pub use protocol::pus::service1::{FailureCode, FailureNotice, RequestId, VerificationKind, VerificationReport};
pub use protocol::pus::service17::{AreYouAliveReport, AreYouAliveRequest};
pub use protocol::pus::{
    AckFlags, PusCodec, PusConfig, PusDecodeError, PusPacket, PusTc, PusTcSecondaryHeader, PusTm, PusTmSecondaryHeader,
};
pub use protocol::simple_string::{SimpleString, SimpleStringCodec, encode_frame};
pub use protocol::{MessageCodec, WireMessage};
