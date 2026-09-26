//! Generische TCP-Actors + zwei Protokolle.
//!
//! Die Bibliothek ist in thematische Module aufgeteilt:
//! - [`ccsds`] enthält CCSDS-Space-Packet-Typen und den zugehörigen Codec.
//! - [`simple_string`] enthält das einfache String-Protokoll.
//! - [`tcp`] enthält alle generischen TCP-Actor-Implementierungen,
//!   Zustands- und Verbindungslogik sowie die Convenience-Typen.
//! - [`test`] enthält den generischen Test-Actor für Assertions in Tests.
//!
//! Die öffentliche API bleibt kompatibel: Die wichtigsten Symbole werden hier
//! erneut exportiert, damit bestehender Code unverändert weiterarbeitet.

pub mod actors;
pub mod ccsds;
pub mod messages;
pub mod simple_string;
pub mod test;

pub use actors::{
    RelayAdapter, RelayAdapterArgs, SimpleStringClient, SimpleStringConnection,
    SimpleStringListener, SimpleStringWriter, SpacePacketClient, SpacePacketConnection,
    SpacePacketListener, SpacePacketWriter, TcpClientActor, TcpClientArgs, TcpConnectionActor,
    TcpConnectionArgs, TcpListenerActor, TcpListenerArgs, TcpWriterActor, TcpWriterArgs,
};
pub use ccsds::{PacketType, SequenceFlags, SpacePacket, SpacePacketCodec, SpacePacketHeader};
pub use messages::{
    Close, CloseRead, CloseReason, CloseWrite, Connect, ConnectionHalf, ConnectionHalfClosed,
    GetLocalAddr, MessageCodec, PeerHalfClosed, Relay, Shutdown,
};
pub use simple_string::{encode_frame, SimpleString, SimpleStringCodec};
pub use test::{GetMessages, TestActor};
