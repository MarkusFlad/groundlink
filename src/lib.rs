//! Generische TCP-Actors + drei Protokolle.
//!
//! Die Bibliothek ist in thematische Module aufgeteilt:
//! - [`ccsds`] enthält CCSDS-Space-Packet-Typen und den zugehörigen Codec.
//! - [`cuc`] enthält die CCSDS-CUC-Zeit inkl. Umrechnung von/nach UTC.
//! - [`pus`] enthält ECSS-PUS-C-Pakete (TC/TM) auf Basis der CCSDS Space
//!   Packets und den zugehörigen Codec.
//! - [`pus_actors`] enthält Actors, die PUS-Pakete fachlich verarbeiten
//!   (z. B. [`PusTcAcceptor`]).
//! - [`simple_string`] enthält das einfache String-Protokoll.
//! - [`tcp`] enthält alle generischen TCP-Actor-Implementierungen,
//!   Zustands- und Verbindungslogik sowie die Convenience-Typen.
//! - [`test`] enthält den generischen Test-Actor für Assertions in Tests.
//!
//! Die öffentliche API bleibt kompatibel: Die wichtigsten Symbole werden hier
//! erneut exportiert, damit bestehender Code unverändert weiterarbeitet.

pub mod actors;
pub mod ccsds;
pub mod cuc;
pub mod messages;
pub mod pus;
pub mod pus_actors;
pub mod simple_string;
pub mod test;

pub use actors::{
    PusClient, PusConnection, PusListener, PusWriter, RelayAdapter, RelayAdapterArgs,
    SimpleStringClient, SimpleStringConnection,
    SimpleStringListener, SimpleStringWriter, SpacePacketClient, SpacePacketConnection,
    SpacePacketListener, SpacePacketWriter, TcpClientActor, TcpClientArgs, TcpConnectionActor,
    TcpConnectionArgs, TcpListenerActor, TcpListenerArgs, TcpWriterActor, TcpWriterArgs,
};
pub use ccsds::{PacketType, SequenceFlags, SpacePacket, SpacePacketCodec, SpacePacketHeader};
pub use cuc::{CucEpoch, CucFormat, CucTime};
pub use messages::{
    Close, CloseRead, CloseReason, CloseWrite, Connect, ConnectionHalf, ConnectionHalfClosed,
    GetLocalAddr, MessageCodec, PeerHalfClosed, Relay, Shutdown,
};
pub use pus::{
    AckFlags, PusCodec, PusConfig, PusPacket, PusTc, PusTcSecondaryHeader, PusTm,
    PusTmSecondaryHeader,
};
pub use pus::service1::{FailureCode, FailureNotice, RequestId, VerificationKind, VerificationReport};
pub use pus::service17::{AreYouAliveReport, AreYouAliveRequest};
pub use pus_actors::{PusPacketAdapter, PusTcAcceptor, PusTestServiceActor, SequenceCounter};
pub use simple_string::{encode_frame, SimpleString, SimpleStringCodec};
pub use test::{GetMessages, TestActor};
