//! CCSDS Space Packet Protocol (CCSDS 133.0-B-2).
//!
//! Contains the Space Packet type (primary header + packet data field) and
//! [`SpacePacketCodec`], which implements both [`Decoder`] and
//! [`Encoder<SpacePacket>`] and can therefore be used directly with
//! [`tokio_util::codec::Framed`] to read and write concurrently on an
//! `AsyncRead + AsyncWrite` stream such as a `TcpStream`.
//!
//! The 6-byte primary header is fully defined by the standard. The packet
//! data field may start with a mission-specific secondary header; its
//! format is not part of CCSDS 133.0 and is deliberately not interpreted
//! here. [`SpacePacket::data`] therefore holds the complete packet data
//! field as raw bytes. See [`crate::protocol::pus`] for PUS packets, which define
//! such a secondary header.
//!
//! ```
//! use bytes::BytesMut;
//! use groundlink::{PacketType, SpacePacket, SpacePacketCodec};
//! use tokio_util::codec::{Decoder, Encoder};
//!
//! let packet = SpacePacket::new(PacketType::Telemetry, 42, 7, &b"payload"[..]);
//!
//! let mut buf = BytesMut::new();
//! SpacePacketCodec.encode(packet.clone(), &mut buf).unwrap();
//! assert_eq!(buf.len(), 6 + 7);
//!
//! assert_eq!(SpacePacketCodec.decode(&mut buf).unwrap(), Some(packet));
//! ```

use bytes::{Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// Length of the primary header in bytes (CCSDS 133.0-B-2, section 4.1.2).
pub const PRIMARY_HEADER_LEN: usize = 6;
/// Packet version number of Space Packets (CCSDS 133.0-B-2, section
/// 4.1.3.2): the top 3 bits of the primary header, always `000`.
pub const PACKET_VERSION_NUMBER: u8 = 0;
/// Largest value of [`SpacePacketHeader::apid`] (11 bits).
pub const APID_MAX: u16 = 0x07FF;
/// Largest value of [`SpacePacketHeader::sequence_count`] (14 bits).
pub const SEQUENCE_COUNT_MAX: u16 = 0x3FFF;
/// Largest length of the packet data field in bytes. The 16-bit length
/// field in the header encodes `length - 1`, so `u16::MAX + 1` is
/// reachable.
pub const MAX_PACKET_DATA_LEN: usize = u16::MAX as usize + 1;

/// CCSDS "Packet Type" field (CCSDS 133.0-B-2, section 4.1.3.3):
/// distinguishes telemetry from telecommand packets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    /// Telemetry (TM), bit value 0.
    Telemetry,
    /// Telecommand (TC), bit value 1.
    Telecommand,
}

impl PacketType {
    pub(crate) fn from_bit(bit: bool) -> Self {
        if bit {
            PacketType::Telecommand
        } else {
            PacketType::Telemetry
        }
    }

    pub(crate) fn to_bit(self) -> bool {
        matches!(self, PacketType::Telecommand)
    }
}

/// CCSDS "Sequence Flags" field (CCSDS 133.0-B-2, section 4.1.3.4):
/// tells whether a packet is part of a segmented sequence of packets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceFlags {
    /// Continuation segment (bit value `0b00`).
    Continuation,
    /// First segment (bit value `0b01`).
    FirstSegment,
    /// Last segment (bit value `0b10`).
    LastSegment,
    /// Unsegmented, stand-alone packet (bit value `0b11`); the usual case
    /// when segmentation is not used.
    Unsegmented,
}

impl SequenceFlags {
    pub(crate) fn from_bits(bits: u8) -> Self {
        match bits & 0b11 {
            0b00 => SequenceFlags::Continuation,
            0b01 => SequenceFlags::FirstSegment,
            0b10 => SequenceFlags::LastSegment,
            _ => SequenceFlags::Unsegmented,
        }
    }

    pub(crate) fn to_bits(self) -> u8 {
        match self {
            SequenceFlags::Continuation => 0b00,
            SequenceFlags::FirstSegment => 0b01,
            SequenceFlags::LastSegment => 0b10,
            SequenceFlags::Unsegmented => 0b11,
        }
    }
}

/// The 6-byte primary header of a CCSDS Space Packet (CCSDS 133.0-B-2,
/// section 4.1).
///
/// The packet version number is currently always `0b000` according to the
/// standard. It is therefore not stored as a field; it is written as 0 when
/// encoding and ignored when decoding. The packet data length is derived
/// from the data when encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpacePacketHeader {
    /// Packet type: telemetry or telecommand.
    pub packet_type: PacketType,
    /// Whether the packet data field starts with a secondary header (not
    /// interpreted by this module).
    pub secondary_header_flag: bool,
    /// Application process ID, 11 bits (`0..=`[`APID_MAX`]).
    pub apid: u16,
    /// Segmentation information.
    pub sequence_flags: SequenceFlags,
    /// Packet sequence count or packet name, 14 bits
    /// (`0..=`[`SEQUENCE_COUNT_MAX`]).
    pub sequence_count: u16,
}

impl SpacePacketHeader {
    /// Appends the header (6 bytes, big-endian) for a packet data field of
    /// length `data_len` to `dst`.
    ///
    /// Fails with [`io::ErrorKind::InvalidInput`] if a field exceeds its bit
    /// width or `data_len` is outside `1..=`[`MAX_PACKET_DATA_LEN`].
    pub(crate) fn encode(&self, data_len: usize, dst: &mut BytesMut) -> io::Result<()> {
        if self.apid > APID_MAX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("APID {} exceeds the 11-bit range (max {APID_MAX})", self.apid),
            ));
        }
        if self.sequence_count > SEQUENCE_COUNT_MAX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "sequence_count {} exceeds the 14-bit range (max {SEQUENCE_COUNT_MAX})",
                    self.sequence_count
                ),
            ));
        }
        if data_len == 0 || data_len > MAX_PACKET_DATA_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("packet data field length {data_len} outside the valid range (1..={MAX_PACKET_DATA_LEN})"),
            ));
        }

        dst.extend_from_slice(&self.to_bytes(data_len));
        Ok(())
    }

    /// The header (6 bytes, big-endian) for a packet data field of length
    /// `data_len`, without validation: fields that exceed their bit width
    /// are truncated.
    pub(crate) fn to_bytes(self, data_len: usize) -> [u8; PRIMARY_HEADER_LEN] {
        // Version number (3 bits, always 0) | type (1 bit) |
        // secondary header flag (1 bit) | APID (11 bits)
        let word0: u16 = ((self.packet_type.to_bit() as u16) << 12)
            | ((self.secondary_header_flag as u16) << 11)
            | (self.apid & APID_MAX);
        // Sequence flags (2 bits) | sequence count (14 bits)
        let word1: u16 = ((self.sequence_flags.to_bits() as u16) << 14) | (self.sequence_count & SEQUENCE_COUNT_MAX);
        // Packet data length = actual length - 1
        let word2: u16 = data_len.wrapping_sub(1) as u16;

        let mut bytes = [0; PRIMARY_HEADER_LEN];
        bytes[0..2].copy_from_slice(&word0.to_be_bytes());
        bytes[2..4].copy_from_slice(&word1.to_be_bytes());
        bytes[4..6].copy_from_slice(&word2.to_be_bytes());
        bytes
    }

    /// Decodes a 6-byte header and also returns the length of the packet
    /// data field that follows it.
    fn decode(src: &[u8]) -> (Self, usize) {
        debug_assert_eq!(src.len(), PRIMARY_HEADER_LEN);
        let word0 = u16::from_be_bytes([src[0], src[1]]);
        let word1 = u16::from_be_bytes([src[2], src[3]]);
        let word2 = u16::from_be_bytes([src[4], src[5]]);

        let header = SpacePacketHeader {
            packet_type: PacketType::from_bit((word0 >> 12) & 0b1 != 0),
            secondary_header_flag: (word0 >> 11) & 0b1 != 0,
            apid: word0 & APID_MAX,
            sequence_flags: SequenceFlags::from_bits((word1 >> 14) as u8),
            sequence_count: word1 & SEQUENCE_COUNT_MAX,
        };
        let data_len = word2 as usize + 1;

        (header, data_len)
    }
}

/// A complete CCSDS Space Packet: primary header + packet data field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpacePacket {
    /// The primary header.
    pub header: SpacePacketHeader,
    /// The complete packet data field (including a mission-specific
    /// secondary header, if any, followed by the user data), as raw bytes.
    pub data: Bytes,
}

impl crate::WireMessage for SpacePacket {}

impl SpacePacket {
    /// Creates an unsegmented Space Packet without secondary header,
    /// carrying `data`.
    ///
    /// `data` must not be empty (the standard requires at least one octet in
    /// the packet data field) and must not exceed [`MAX_PACKET_DATA_LEN`]
    /// bytes. This is only checked when the packet is actually encoded,
    /// e.g. by [`SpacePacketCodec`].
    pub fn new(packet_type: PacketType, apid: u16, sequence_count: u16, data: impl Into<Bytes>) -> Self {
        SpacePacket {
            header: SpacePacketHeader {
                packet_type,
                secondary_header_flag: false,
                apid,
                sequence_flags: SequenceFlags::Unsegmented,
                sequence_count,
            },
            data: data.into(),
        }
    }
}

/// Codec for CCSDS Space Packets.
///
/// Usable with [`tokio_util::codec::Framed`] to read and write concurrently
/// on an `AsyncRead + AsyncWrite` stream:
///
/// ```no_run
/// # use futures::StreamExt;
/// # use groundlink::SpacePacketCodec;
/// # #[tokio::main]
/// # async fn main() -> std::io::Result<()> {
/// # let tcp_stream = tokio::net::TcpStream::connect("127.0.0.1:9000").await?;
/// let framed = tokio_util::codec::Framed::new(tcp_stream, SpacePacketCodec::default());
/// let (mut sink, mut stream) = framed.split();
/// // stream: Stream<Item = io::Result<SpacePacket>>
/// // sink:   Sink<SpacePacket, Error = io::Error>
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Encoding fails with [`io::ErrorKind::InvalidInput`] if a header field
/// exceeds its bit width or the packet data field is empty or longer than
/// [`MAX_PACKET_DATA_LEN`].
///
/// Decoding fails with [`io::ErrorKind::InvalidData`] if the packet version
/// number is not [`PACKET_VERSION_NUMBER`]. The data are then not a Space
/// Packet, or the stream is out of sync; because Space Packets carry no
/// sync marker, decoding cannot resume reliably, so the generic TCP actors
/// close the connection. The check happens on the first byte, before the
/// codec waits for a length taken from invalid data. Incomplete packets
/// stay in the buffer until the rest arrives.
#[derive(Debug, Clone, Copy, Default)]
pub struct SpacePacketCodec;

impl Decoder for SpacePacketCodec {
    type Item = SpacePacket;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        if let Some(&first) = src.first() {
            let version = first >> 5;
            if version != PACKET_VERSION_NUMBER {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("packet version number {version} is not a Space Packet (expected {PACKET_VERSION_NUMBER})"),
                ));
            }
        }
        if src.len() < PRIMARY_HEADER_LEN {
            return Ok(None);
        }

        // Only peek at the header (do not remove it from the buffer) until
        // the complete packet data field has arrived as well.
        let (_, data_len) = SpacePacketHeader::decode(&src[..PRIMARY_HEADER_LEN]);
        let total_len = PRIMARY_HEADER_LEN + data_len;

        if src.len() < total_len {
            src.reserve(total_len - src.len());
            return Ok(None);
        }

        let mut packet_bytes = src.split_to(total_len);
        let (header, _) = SpacePacketHeader::decode(&packet_bytes[..PRIMARY_HEADER_LEN]);
        let data = packet_bytes.split_off(PRIMARY_HEADER_LEN).freeze();

        Ok(Some(SpacePacket { header, data }))
    }
}

impl Encoder<SpacePacket> for SpacePacketCodec {
    type Error = io::Error;

    fn encode(&mut self, packet: SpacePacket, dst: &mut BytesMut) -> io::Result<()> {
        dst.reserve(PRIMARY_HEADER_LEN + packet.data.len());
        packet.header.encode(packet.data.len(), dst)?;
        dst.extend_from_slice(&packet.data);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_rejects_wrong_packet_version_on_the_first_byte() {
        let mut buf = BytesMut::from(&[0b0010_0000][..]);
        let err = SpacePacketCodec.decode(&mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn decode_accepts_version_zero_and_waits_for_the_header() {
        let mut buf = BytesMut::from(&[0b0001_1111][..]);
        assert_eq!(SpacePacketCodec.decode(&mut buf).unwrap(), None);
    }

    #[test]
    fn encode_produces_exact_expected_bytes() {
        let packet = SpacePacket {
            header: SpacePacketHeader {
                packet_type: PacketType::Telecommand,
                secondary_header_flag: false,
                apid: 0x0AB,
                sequence_flags: SequenceFlags::Unsegmented,
                sequence_count: 0x0001,
            },
            data: Bytes::from_static(b"hi"),
        };

        let mut codec = SpacePacketCodec;
        let mut buf = BytesMut::new();
        codec.encode(packet, &mut buf).unwrap();

        // word0 = (1<<12) | (0<<11) | 0x0AB          = 0x10AB
        // word1 = (0b11<<14) | 0x0001                 = 0xC001
        // word2 = data_len(2) - 1                      = 0x0001
        assert_eq!(&buf[..], &[0x10, 0xAB, 0xC0, 0x01, 0x00, 0x01, b'h', b'i']);
    }

    #[test]
    fn encode_decode_roundtrip() {
        let original = SpacePacket::new(
            PacketType::Telemetry,
            APID_MAX,
            SEQUENCE_COUNT_MAX,
            &b"hello spacecraft"[..],
        );

        let mut codec = SpacePacketCodec;
        let mut buf = BytesMut::new();
        codec.encode(original.clone(), &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().expect("should decode completely");

        assert_eq!(decoded, original);
        assert!(buf.is_empty(), "buffer should be empty after decoding completely");
    }

    #[test]
    fn decode_waits_for_complete_header() {
        let mut codec = SpacePacketCodec;
        let mut buf = BytesMut::from(&[0x10, 0xAB, 0xC0][..]); // only 3 of 6 header bytes

        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert_eq!(buf.len(), 3, "incomplete bytes must not be consumed");
    }

    #[test]
    fn decode_waits_for_complete_data_field() {
        let packet = SpacePacket::new(PacketType::Telemetry, 1, 1, &b"1234567890"[..]);

        let mut codec = SpacePacketCodec;
        let mut full = BytesMut::new();
        codec.encode(packet.clone(), &mut full).unwrap();

        // Simulate header + half of the data.
        let mut partial = BytesMut::from(&full[..PRIMARY_HEADER_LEN + 5]);
        assert_eq!(codec.decode(&mut partial).unwrap(), None);
        assert_eq!(
            partial.len(),
            PRIMARY_HEADER_LEN + 5,
            "incomplete data field must not be consumed"
        );

        // Deliver the remaining bytes.
        partial.extend_from_slice(&full[PRIMARY_HEADER_LEN + 5..]);
        let decoded = codec.decode(&mut partial).unwrap().expect("complete now");
        assert_eq!(decoded, packet);
        assert!(partial.is_empty());
    }

    #[test]
    fn encode_rejects_empty_data_field() {
        let packet = SpacePacket::new(PacketType::Telemetry, 1, 1, Bytes::new());
        let mut codec = SpacePacketCodec;
        let mut buf = BytesMut::new();
        assert!(codec.encode(packet, &mut buf).is_err());
    }

    #[test]
    fn encode_rejects_out_of_range_fields() {
        let mut header = SpacePacketHeader {
            packet_type: PacketType::Telemetry,
            secondary_header_flag: false,
            apid: APID_MAX + 1,
            sequence_flags: SequenceFlags::Unsegmented,
            sequence_count: 0,
        };
        let mut buf = BytesMut::new();
        assert!(header.encode(1, &mut buf).is_err());

        header.apid = APID_MAX;
        header.sequence_count = SEQUENCE_COUNT_MAX + 1;
        assert!(header.encode(1, &mut buf).is_err());
    }
}
