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
//! | [`tml`](protocol::tml) | [`TmlMessage`] | [`TmlCodec`] | [`TmlServer`], [`TmlClient`], … |
//!
//! On top of the PUS packets, [`actor::pus`] provides actors that handle
//! telecommands for an application process: [`PusTcAcceptor`] performs
//! the acceptance check (including the PUS format, when it receives
//! [`SpacePacket`]s) and reports it via PUS service 1,
//! [`PusTestServiceActor`] implements PUS service 17, and
//! [`PusTmStamper`] assigns sequence counts, message type counters and time
//! stamps to the telemetry of an APID.
//!
//! For SLE, the [`TmlLinkActor`] in [`actor::tml`] runs the rules of the
//! SLE Transport Mapping Layer (context message, heartbeats, dead timer) on
//! top of a [`TmlServer`] or [`TmlClient`] and exchanges the BER-encoded SLE
//! PDUs with the SLE layer as [`SlePdu`] messages. [`protocol::sle`]
//! encodes and decodes these PDUs, e.g. as [`RafUserPdu`] or
//! [`FcltuProviderPdu`]. The provider actors in [`actor::sle`] run the RAF
//! and FCLTU services on top: a [`SleAssociationActor`] per port and a
//! [`RafServiceInstance`] or [`FcltuServiceInstance`] per service instance.
//! The [`SleUserActor`] is the user side, on top of a [`TmlClient`].
//!
//! # Modules
//!
//! - [`protocol`]: the protocols and the traits [`MessageCodec`] and
//!   [`WireMessage`] that connect them to the TCP actors.
//!   - [`protocol::ccsds`]: CCSDS Space Packet types and codec (CCSDS
//!     133.0-B-2).
//!   - [`protocol::cuc`]: CCSDS Unsegmented Time Code with conversion from
//!     and to UTC.
//!   - [`protocol::datalink`]: CLTUs, TC and TM Transfer Frames (CCSDS
//!     231.0, 232.0, 132.0).
//!   - [`protocol::pus`]: ECSS PUS-C telecommand and telemetry packets on
//!     top of Space Packets, their codec and typed service messages.
//!   - [`protocol::simple_string`]: the simple string protocol.
//!   - [`protocol::sle`]: BER encoding and the SLE PDUs of RAF (CCSDS
//!     911.1-B-4) and FCLTU (CCSDS 912.1-B-4).
//!   - [`protocol::tml`]: SLE Transport Mapping Layer messages (CCSDS
//!     913.1-B-2) and their codec.
//! - [`actor`]: the actors.
//!   - [`actor::tcp`]: generic TCP server, client, reader and writer actors,
//!     their control messages and the per-protocol type aliases.
//!   - [`actor::pus`]: actors that process PUS packets.
//!   - [`actor::datalink`]: actors between Space Packets and CLTUs or TM
//!     Transfer Frames.
//!   - [`actor::tml`]: the SLE Transport Mapping Layer actor.
//!   - [`actor::sle`]: SLE provider actors for RAF and FCLTU.
//!   - `actor::test`: a generic actor that records messages, for
//!     assertions in tests (feature `test-util`).
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
//! - `sle_provider <raf_port> <fcltu_port>`: an SLE provider with a
//!   [`RafServiceInstance`] fed with generated frames and an
//!   [`FcltuServiceInstance`] that prints the accepted CLTUs, each on its
//!   own port.
//! - `sle_user <address> <raf_port> <fcltu_port>`: two [`SleUserActor`]s
//!   that receive RAF frames from `sle_provider` and send it CLTUs.
//! - `sle_spacecraft <raf_port> <fcltu_port>`: a simulated spacecraft
//!   behind an SLE provider: TCs arrive in CLTUs over FCLTU, go through
//!   [`TcFrameDecoder`] to the PUS actors, and their TM leaves through
//!   [`TmFrameEncoder`] over RAF.
//! - `sle_ground <address> <raf_port> <fcltu_port>`: an interactive ground
//!   segment for `sle_spacecraft` that sends TC(17,1) and prints the TM.
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
//! `TestActor` (feature `test-util`):
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

pub use actor::batch::{Batch, DEFAULT_MAX_BATCH_LEN, Downstream, DownstreamError};
pub use actor::datalink::{TcFrameDecoder, TcFrameEncoder, TmFrameDecoder, TmFrameEncoder};
pub use actor::pus::{PusPacketAdapter, PusTcAcceptor, PusTestServiceActor, PusTmStamper};
pub use actor::sle::{
    Cltu, CltuStart, DEFAULT_SLE_TICK, FcltuConfig, FcltuServiceInstance, RafConfig, RafServiceInstance, RafStart,
    SleAssociationActor, SleAssociationConfig, SleBind, SleProviderEvent, SleServiceRegistry, SleStop, SleUnbind,
    SleUserActor, SleUserConfig, SleUserError, SleUserEvent,
};
pub use actor::tcp::{
    Close, CloseRead, CloseReason, CloseWrite, Connect, ConnectionEvent, ConnectionEventKind, ConnectionHalf,
    ConnectionObserver, ConnectionPolicy, DEFAULT_MAILBOX_CAPACITY, DEFAULT_WRITE_TIMEOUT, EventSource, GetLocalAddr,
    KeepAlive, PusClient, PusServer, SimpleStringClient, SimpleStringServer, SpacePacketClient, SpacePacketServer,
    TcpClientActor, TcpClientArgs, TcpServerActor, TcpServerArgs, TmlClient, TmlServer,
};
#[cfg(any(test, feature = "test-util"))]
pub use actor::test::{GetMessages, TestActor};
pub use actor::tml::{
    DEFAULT_TML_TICK, SleDisconnect, SlePeerAbort, TmlLinkActor, TmlLinkEvent, TmlReleaseReason, TmlResponderConfig,
    TmlRole,
};
pub use protocol::ccsds::{PacketType, SequenceFlags, SpacePacket, SpacePacketCodec, SpacePacketHeader};
pub use protocol::cuc::{CucEpoch, CucFormat, CucTime};
pub use protocol::datalink::FrameError;
pub use protocol::datalink::tc::{TcFrameConfig, TcTransferFrame};
pub use protocol::datalink::tm::{TmFrameConfig, TmTransferFrame};
pub use protocol::pus::service1::{FailureCode, FailureNotice, RequestId, VerificationKind, VerificationReport};
pub use protocol::pus::service17::{AreYouAliveReport, AreYouAliveRequest};
pub use protocol::pus::{
    AckFlags, PusCodec, PusConfig, PusDecodeError, PusPacket, PusTc, PusTcSecondaryHeader, PusTm, PusTmSecondaryHeader,
};
pub use protocol::simple_string::{SimpleString, SimpleStringCodec, encode_frame};
pub use protocol::sle::ber::BerError;
pub use protocol::sle::common::{
    Acknowledgement, BindInvocation, BindReturn, PduError, ServiceInstanceId, StopInvocation, UnbindInvocation,
    UnbindReturn,
};
pub use protocol::sle::fcltu::{
    CltuId, CltuStartInvocation, CltuStartReturn, CltuTransferDataInvocation, CltuTransferDataReturn, FcltuProviderPdu,
    FcltuUserPdu,
};
pub use protocol::sle::raf::{
    RafFrame, RafProviderPdu, RafStartInvocation, RafStartReturn, RafUserPdu, RequestedFrameQuality,
};
pub use protocol::tml::{SlePdu, TmlCodec, TmlContext, TmlMessage};
pub use protocol::{MessageCodec, WireMessage};
