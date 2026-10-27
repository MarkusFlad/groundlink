//! CCSDS space data link protocols between the SLE services and the Space
//! Packets: CLTUs and TC Transfer Frames on the forward link, TM Transfer
//! Frames on the return link.
//!
//! - [`cltu`]: CLTUs with the (63,56) BCH code (CCSDS 231.0-B-4).
//! - [`tc`]: TC Transfer Frames (CCSDS 232.0-B-4), carrying Space Packets.
//! - [`tm`]: TM Transfer Frames (CCSDS 132.0-B-3), and packing Space
//!   Packets into frames and extracting them again.
//!
//! On the forward link, FCLTU carries CLTUs; each holds one TC Transfer
//! Frame with one or more Space Packets. On the return link, RAF carries
//! TM Transfer Frames as they come out of the ground station's decoder:
//! synchronisation marker, derandomisation and Reed-Solomon decoding are
//! done by then, so there are no CADUs to handle here. Space Packets are
//! packed into the TM frames back to back and may span frames.
//!
//! ```
//! use bytes::Bytes;
//! use groundlink::protocol::datalink::{cltu, tc::TcTransferFrame};
//!
//! let frame = TcTransferFrame::new(0x2a, 1, 0, Bytes::from_static(b"packet"));
//! let encoded = frame.encode(true).unwrap();
//! let cltu = cltu::encode(&encoded);
//! assert_eq!(&cltu[..2], &cltu::START_SEQUENCE);
//!
//! let decoded = TcTransferFrame::decode(&cltu::decode(&cltu).unwrap(), true).unwrap();
//! assert_eq!(decoded, frame);
//! ```

use std::fmt;

pub mod cltu;
pub mod tc;
pub mod tm;

/// Why a CLTU or a transfer frame could not be encoded or decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    /// The CLTU does not start with the start sequence.
    NoStartSequence,
    /// The CLTU contains no valid codeblock.
    NoCodeblock,
    /// The data are shorter than the frame header or the frame length
    /// says.
    Truncated,
    /// The frame would exceed the maximum length, or does not have the
    /// configured length.
    InvalidLength(usize),
    /// The transfer frame version number is not 0.
    InvalidVersion(u8),
    /// The frame error control field does not match.
    CrcMismatch,
    /// A field exceeds its bit width; the text says which.
    InvalidField(&'static str),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::NoStartSequence => f.write_str("CLTU without start sequence"),
            FrameError::NoCodeblock => f.write_str("CLTU without a valid codeblock"),
            FrameError::Truncated => f.write_str("frame truncated"),
            FrameError::InvalidLength(len) => write!(f, "invalid frame length {len}"),
            FrameError::InvalidVersion(version) => write!(f, "invalid transfer frame version {version}"),
            FrameError::CrcMismatch => f.write_str("frame error control field mismatch"),
            FrameError::InvalidField(what) => write!(f, "{what} out of range"),
        }
    }
}

impl std::error::Error for FrameError {}
