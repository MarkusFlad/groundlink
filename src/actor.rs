//! Actors: the generic TCP actors and the actors that process PUS packets.
//!
//! - [`tcp`]: generic TCP server, client, reader and writer actors, their
//!   control messages and the per-protocol type aliases.
//! - [`pus`]: actors that process PUS packets for an application process.
//! - [`test`](mod@test): a generic actor that records messages, for
//!   assertions in tests.

pub mod pus;
pub mod tcp;
pub mod test;
