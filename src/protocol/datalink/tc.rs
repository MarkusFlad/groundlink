//! TC Transfer Frames, CCSDS 232.0-B-4.
//!
//! ```text
//! | version (2) | bypass (1) | control command (1) | spare (2) | spacecraft ID (10) |
//! | virtual channel ID (6) | frame length - 1 (10) | frame sequence number (8) |
//! | data field (up to 1019 octets) | frame error control field (16, optional) |
//! ```
//!
//! The frame error control field is a CRC-16 (CCITT, initial value
//! `0xFFFF`) over the whole frame. Whether it is present is a mission
//! parameter, see [`TcFrameConfig::fecf`].

use bytes::{BufMut, Bytes, BytesMut};

use super::FrameError;
use crate::protocol::pus::crc16_ccitt;

/// Length of the primary header in octets.
pub const PRIMARY_HEADER_LEN: usize = 5;
/// Length of the frame error control field in octets.
pub const FECF_LEN: usize = 2;
/// Maximum length of a TC Transfer Frame in octets.
pub const MAX_FRAME_LEN: usize = 1024;

/// The parameters of a TC virtual channel that sender and receiver agree
/// on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct TcFrameConfig {
    /// The spacecraft identifier, 10 bits.
    pub spacecraft_id: u16,
    /// The virtual channel identifier, 6 bits.
    pub virtual_channel_id: u8,
    /// With a segment header (MAP service), the MAP identifier, 6 bits;
    /// the packets are then sent unsegmented with sequence flags `11`.
    pub map_id: Option<u8>,
    /// Whether the frames carry a frame error control field.
    pub fecf: bool,
}

impl TcFrameConfig {
    /// Frames of `spacecraft_id` on `virtual_channel_id`, without segment
    /// header, with frame error control field.
    pub fn new(spacecraft_id: u16, virtual_channel_id: u8) -> Self {
        TcFrameConfig {
            spacecraft_id,
            virtual_channel_id,
            map_id: None,
            fecf: true,
        }
    }

    /// Sends the packets with a segment header with the MAP identifier
    /// `map_id`.
    pub fn with_map_id(mut self, map_id: u8) -> Self {
        self.map_id = Some(map_id);
        self
    }

    /// Sets whether the frames carry a frame error control field.
    pub fn with_fecf(mut self, fecf: bool) -> Self {
        self.fecf = fecf;
        self
    }
}

/// A TC Transfer Frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcTransferFrame {
    /// Type-B (bypass) frame: accepted without the sequence check of the
    /// FARM.
    pub bypass: bool,
    /// Whether the data field carries control commands for the FARM
    /// instead of user data.
    pub control_command: bool,
    /// The spacecraft identifier, 10 bits.
    pub spacecraft_id: u16,
    /// The virtual channel identifier, 6 bits.
    pub virtual_channel_id: u8,
    /// The frame sequence number N(S).
    pub sequence_number: u8,
    /// The frame data field.
    pub data: Bytes,
}

impl TcTransferFrame {
    /// A type-BD frame (bypass, user data).
    pub fn new(spacecraft_id: u16, virtual_channel_id: u8, sequence_number: u8, data: impl Into<Bytes>) -> Self {
        TcTransferFrame {
            bypass: true,
            control_command: false,
            spacecraft_id,
            virtual_channel_id,
            sequence_number,
            data: data.into(),
        }
    }

    /// Encodes the frame, with a frame error control field if `fecf`.
    pub fn encode(&self, fecf: bool) -> Result<Bytes, FrameError> {
        if self.spacecraft_id > 0x3ff {
            return Err(FrameError::InvalidField("spacecraft ID"));
        }
        if self.virtual_channel_id > 0x3f {
            return Err(FrameError::InvalidField("virtual channel ID"));
        }
        let len = PRIMARY_HEADER_LEN + self.data.len() + if fecf { FECF_LEN } else { 0 };
        if self.data.is_empty() || len > MAX_FRAME_LEN {
            return Err(FrameError::InvalidLength(len));
        }
        let mut frame = BytesMut::with_capacity(len);
        frame.put_u16((u16::from(self.bypass) << 13) | (u16::from(self.control_command) << 12) | self.spacecraft_id);
        frame.put_u16((u16::from(self.virtual_channel_id) << 10) | (len - 1) as u16);
        frame.put_u8(self.sequence_number);
        frame.put_slice(&self.data);
        if fecf {
            let crc = crc16_ccitt(&frame);
            frame.put_u16(crc);
        }
        Ok(frame.freeze())
    }

    /// Decodes a frame from `data`, which may carry fill after it (as
    /// decoded from a CLTU). Checks the frame error control field if
    /// `fecf`.
    pub fn decode(data: &[u8], fecf: bool) -> Result<Self, FrameError> {
        if data.len() < PRIMARY_HEADER_LEN {
            return Err(FrameError::Truncated);
        }
        let word0 = u16::from_be_bytes([data[0], data[1]]);
        let word1 = u16::from_be_bytes([data[2], data[3]]);
        let version = (word0 >> 14) as u8;
        if version != 0 {
            return Err(FrameError::InvalidVersion(version));
        }
        let len = usize::from(word1 & 0x3ff) + 1;
        let trailer = if fecf { FECF_LEN } else { 0 };
        if len <= PRIMARY_HEADER_LEN + trailer {
            return Err(FrameError::InvalidLength(len));
        }
        if data.len() < len {
            return Err(FrameError::Truncated);
        }
        let frame = &data[..len];
        if fecf && crc16_ccitt(&frame[..len - FECF_LEN]) != u16::from_be_bytes([frame[len - 2], frame[len - 1]]) {
            return Err(FrameError::CrcMismatch);
        }
        Ok(TcTransferFrame {
            bypass: word0 & (1 << 13) != 0,
            control_command: word0 & (1 << 12) != 0,
            spacecraft_id: word0 & 0x3ff,
            virtual_channel_id: (word1 >> 10) as u8,
            sequence_number: frame[4],
            data: Bytes::copy_from_slice(&frame[PRIMARY_HEADER_LEN..len - trailer]),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_format() {
        let frame = TcTransferFrame::new(0x2a, 1, 7, Bytes::from_static(&[0xde, 0xad]));
        let encoded = frame.encode(true).unwrap();
        assert_eq!(&encoded[..5], &[0x20, 0x2a, 0x04, 0x08, 0x07]);
        assert_eq!(encoded.len(), 9);
        assert_eq!(crc16_ccitt(&encoded), 0, "CRC over frame and FECF is zero");
        assert_eq!(TcTransferFrame::decode(&encoded, true).unwrap(), frame);
    }

    #[test]
    fn decoding_ignores_fill_and_checks_the_crc() {
        let frame = TcTransferFrame::new(0x3ff, 63, 255, Bytes::from_static(b"abc"));
        let mut encoded = frame.encode(true).unwrap().to_vec();
        encoded.extend_from_slice(&[0x55; 4]);
        assert_eq!(TcTransferFrame::decode(&encoded, true).unwrap(), frame);
        encoded[6] ^= 1;
        assert_eq!(TcTransferFrame::decode(&encoded, true), Err(FrameError::CrcMismatch));
        assert_eq!(TcTransferFrame::decode(&encoded[..7], true), Err(FrameError::Truncated));
    }

    #[test]
    fn invalid_frames_are_not_encoded() {
        let too_long = TcTransferFrame::new(1, 0, 0, vec![0; MAX_FRAME_LEN]);
        assert!(matches!(too_long.encode(true), Err(FrameError::InvalidLength(_))));
        let bad_vc = TcTransferFrame::new(1, 64, 0, Bytes::from_static(b"x"));
        assert!(matches!(bad_vc.encode(true), Err(FrameError::InvalidField(_))));
    }
}
