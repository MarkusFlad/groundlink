//! Actors: the generic TCP actors and the actors that process PUS packets.
//!
//! - [`tcp`]: generic TCP server and client actors, their control
//!   messages and the per-protocol type aliases.
//! - [`batch`]: several messages passed between actors as one kameo
//!   message, for a high throughput of small messages.
//! - [`pus`]: actors that process PUS packets for an application process.
//! - [`datalink`]: actors that put Space Packets into CLTUs and TM
//!   Transfer Frames and take them out again.
//! - [`tml`]: the SLE Transport Mapping Layer actor, which exchanges SLE
//!   PDUs with the SLE layer as kameo messages.
//! - [`sle`]: SLE provider actors for RAF and FCLTU on top of the TML
//!   actor.
//! - `test`: a generic actor that records messages, for assertions in
//!   tests. Only with the feature `test-util`.

pub mod batch;
pub mod datalink;
pub mod pus;
pub mod sle;
pub mod tcp;
#[cfg(any(test, feature = "test-util"))]
pub mod test;
pub mod tml;
