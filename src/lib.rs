//! Generic TCP actors built on [`kameo`], plus three protocols that plug
//! into them: a simple length-prefixed string protocol, CCSDS Space
//! Packets and ECSS PUS-C packets.
//!
//! # Overview
//!
//! The TCP actors in [`actors`] are generic over a message type `M` and a
//! codec `C` implementing [`MessageCodec<M>`]. Every protocol therefore gets
//! the same set of actors through type aliases:
//!
//! | Protocol | Message | Codec | Actors |
//! |---|---|---|---|
//! | [`simple_string`] | [`SimpleString`] | [`SimpleStringCodec`] | [`SimpleStringServer`], [`SimpleStringClient`], … |
//! | [`ccsds`] | [`SpacePacket`] | [`SpacePacketCodec`] | [`SpacePacketServer`], [`SpacePacketClient`], … |
//! | [`pus`] | [`PusPacket`] | [`PusCodec`] | [`PusServer`], [`PusClient`], … |
//!
//! On top of the PUS packets, [`pus_actors`] provides actors that handle
//! telecommands for an application process: [`PusTcAcceptor`] performs
//! the acceptance check (including the PUS format, when it receives
//! [`SpacePacket`]s) and reports it via PUS service 1,
//! [`PusTestServiceActor`] implements PUS service 17, and
//! [`PusTmStamper`] assigns sequence counts, message type counters and time
//! stamps to the telemetry of an APID.
//!
//! # Modules
//!
//! - [`actors`]: generic TCP server, client, reader and writer actors
//!   and the per-protocol type aliases.
//! - [`ccsds`]: CCSDS Space Packet types and codec (CCSDS 133.0-B-2).
//! - [`cuc`]: CCSDS Unsegmented Time Code with conversion from and to UTC.
//! - [`messages`]: messages exchanged with the TCP actors and the
//!   [`MessageCodec`] trait.
//! - [`pus`]: ECSS PUS-C telecommand and telemetry packets on top of Space
//!   Packets, their codec and typed service messages.
//! - [`pus_actors`]: actors that process PUS packets.
//! - [`simple_string`]: the simple string protocol.
//! - [`test`](mod@test): a generic actor that records messages, for
//!   assertions in tests.
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
//! let server = SimpleStringServer::spawn(TcpServerArgs {
//!     bind_addr: "127.0.0.1:0".parse().unwrap(),
//!     downstream: received.clone().recipient(),
//! });
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

pub mod actors;
pub mod ccsds;
pub mod cuc;
pub mod messages;
pub mod pus;
pub mod pus_actors;
pub mod simple_string;
pub mod test;

pub use actors::{
    PusClient, PusReader, PusServer, PusWriter, SimpleStringClient, SimpleStringReader,
    SimpleStringServer, SimpleStringWriter, SpacePacketClient, SpacePacketReader,
    SpacePacketServer, SpacePacketWriter, TcpClientActor, TcpClientArgs, TcpReaderActor,
    TcpReaderArgs, TcpServerActor, TcpServerArgs, TcpWriterActor, TcpWriterArgs,
};
pub use ccsds::{PacketType, SequenceFlags, SpacePacket, SpacePacketCodec, SpacePacketHeader};
pub use cuc::{CucEpoch, CucFormat, CucTime};
pub use messages::{
    Close, CloseRead, CloseReason, CloseWrite, Connect, ConnectionHalf, ConnectionHalfClosed,
    GetLocalAddr, MessageCodec, PeerHalfClosed, Shutdown, WireMessage,
};
pub use pus::{
    AckFlags, PusCodec, PusConfig, PusDecodeError, PusPacket, PusTc, PusTcSecondaryHeader, PusTm,
    PusTmSecondaryHeader,
};
pub use pus::service1::{FailureCode, FailureNotice, RequestId, VerificationKind, VerificationReport};
pub use pus::service17::{AreYouAliveReport, AreYouAliveRequest};
pub use pus_actors::{PusPacketAdapter, PusTcAcceptor, PusTestServiceActor, PusTmStamper};
pub use simple_string::{encode_frame, SimpleString, SimpleStringCodec};
pub use test::{GetMessages, TestActor};
