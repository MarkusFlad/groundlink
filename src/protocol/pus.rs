//! ECSS Packet Utilisation Standard (PUS-C, ECSS-E-ST-70-41C).
//!
//! PUS packets are CCSDS Space Packets (see [`crate::protocol::ccsds`]) whose packet
//! data field contains a standardised secondary header and, optionally, a
//! packet error control field (CRC-16):
//!
//! ```text
//! +------------------+----------------------+---------------------+-----------+
//! | Primary header   | PUS secondary header | Application data /  | PEC       |
//! | (CCSDS, 6 bytes) | (TC: 5, TM: 7+time)  | source data         | (2 bytes) |
//! +------------------+----------------------+---------------------+-----------+
//! ```
//!
//! The Space Packet is included by composition: [`PusTc`] and [`PusTm`]
//! contain a [`SpacePacketHeader`] and convert to and from a
//! [`SpacePacket`] with [`PusPacket::to_space_packet`] and
//! [`PusPacket::from_space_packet`]. [`PusCodec`] delegates the framing to
//! [`SpacePacketCodec`].
//!
//! Mission-specific parts are set with [`PusConfig`]: the length of the
//! time stamp in the TM secondary header and whether a packet error
//! control field is present. Optional spare fields in the secondary header
//! are not supported.
//!
//! Typed messages of individual PUS services live in submodules:
//! [`service1`] (request verification) and [`service17`] (test).
//!
//! ```
//! use bytes::BytesMut;
//! use groundlink::{PusCodec, PusPacket, PusTc};
//! use tokio_util::codec::{Decoder, Encoder};
//!
//! let tc = PusPacket::from(PusTc::new(0x042, 1, 17, 1, &b""[..]));
//!
//! let mut codec = PusCodec::default();
//! let mut buf = BytesMut::new();
//! codec.encode(tc.clone(), &mut buf).unwrap();
//! assert_eq!(buf.len(), 6 + 5 + 2); // primary header + secondary header + CRC
//!
//! let decoded = codec.decode(&mut buf).unwrap().unwrap();
//! assert_eq!((decoded.service_type(), decoded.message_subtype()), (17, 1));
//! assert_eq!(decoded, tc);
//! ```

use bytes::{Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};
use tracing::warn;

use crate::protocol::ccsds::{PacketType, SequenceFlags, SpacePacket, SpacePacketCodec, SpacePacketHeader};
use crate::protocol::cuc::{CucFormat, CucTime};

pub mod service1;
pub mod service17;

/// PUS version number of PUS-C (ECSS-E-ST-70-41C).
pub const PUS_VERSION: u8 = 2;
/// Length of the TC secondary header in bytes.
pub const TC_SECONDARY_HEADER_LEN: usize = 5;
/// Length of the TM secondary header in bytes, *without* the time stamp.
pub const TM_SECONDARY_HEADER_LEN_WITHOUT_TIME: usize = 7;
/// Length of the packet error control field (CRC-16) in bytes.
pub const PEC_LEN: usize = 2;
/// Default length of the TM time stamp: CUC 4+2 with P-field, matching
/// [`CucFormat::default`].
pub const DEFAULT_TM_TIME_LEN: usize = 7;

/// Computes the CRC-16 that ECSS prescribes for the packet error control
/// field (CCITT: polynomial `0x1021`, initial value `0xFFFF`, no reflection,
/// no final XOR).
///
/// ```
/// use groundlink::protocol::pus::crc16_ccitt;
///
/// assert_eq!(crc16_ccitt(b"123456789"), 0x29B1);
/// ```
pub fn crc16_ccitt(data: &[u8]) -> u16 {
    let mut crc: u16 = 0xFFFF;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

fn invalid_input(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg)
}

/// Why a Space Packet is not a valid PUS packet; see
/// [`PusPacket::from_space_packet`].
///
/// Converts into an [`io::Error`] of kind [`io::ErrorKind::InvalidData`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PusDecodeError {
    /// The secondary header flag of the primary header is not set.
    MissingSecondaryHeader,
    /// The packet data field is too short for the packet error control
    /// field or the PUS secondary header.
    PacketTooShort {
        /// Length of the packet data field in bytes (without the packet
        /// error control field, if that has already been checked).
        len: usize,
        /// Minimum length in bytes.
        min_len: usize,
    },
    /// The CRC of the packet error control field is wrong.
    ChecksumError,
    /// The PUS version in the secondary header is not [`PUS_VERSION`].
    UnsupportedPusVersion(u8),
}

impl std::fmt::Display for PusDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PusDecodeError::MissingSecondaryHeader => {
                f.write_str("Space Packet without secondary header is not a PUS packet")
            }
            PusDecodeError::PacketTooShort { len, min_len } => {
                write!(
                    f,
                    "packet data field ({len} bytes) too short (at least {min_len} bytes expected)"
                )
            }
            PusDecodeError::ChecksumError => f.write_str("CRC error in packet error control field"),
            PusDecodeError::UnsupportedPusVersion(version) => {
                write!(f, "PUS version {version} not supported (expected {PUS_VERSION}, PUS-C)")
            }
        }
    }
}

impl std::error::Error for PusDecodeError {}

impl From<PusDecodeError> for io::Error {
    fn from(err: PusDecodeError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, err)
    }
}

/// Acknowledgement flags of a telecommand: which successful verification
/// reports (service 1) the receiver shall generate.
///
/// Failure reports are always generated, regardless of these flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AckFlags {
    /// Report successful acceptance, TM(1,1) (bit `0b0001`).
    pub acceptance: bool,
    /// Report successful start of execution, TM(1,3) (bit `0b0010`).
    pub start: bool,
    /// Report progress of execution, TM(1,5) (bit `0b0100`).
    pub progress: bool,
    /// Report successful completion of execution, TM(1,7) (bit `0b1000`).
    pub completion: bool,
}

impl AckFlags {
    /// All verification reports requested.
    pub const ALL: AckFlags = AckFlags {
        acceptance: true,
        start: true,
        progress: true,
        completion: true,
    };
    /// No verification reports requested.
    pub const NONE: AckFlags = AckFlags {
        acceptance: false,
        start: false,
        progress: false,
        completion: false,
    };

    fn from_bits(bits: u8) -> Self {
        AckFlags {
            acceptance: bits & 0b0001 != 0,
            start: bits & 0b0010 != 0,
            progress: bits & 0b0100 != 0,
            completion: bits & 0b1000 != 0,
        }
    }

    fn to_bits(self) -> u8 {
        (self.acceptance as u8) | (self.start as u8) << 1 | (self.progress as u8) << 2 | (self.completion as u8) << 3
    }
}

/// Secondary header of a PUS-C telecommand (5 bytes).
///
/// The PUS version number is not stored; it is always
/// [`PUS_VERSION`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PusTcSecondaryHeader {
    /// Which successful verification reports are requested.
    pub ack_flags: AckFlags,
    /// Service type (e.g. 17 = test).
    pub service_type: u8,
    /// Message subtype (e.g. 1 = are-you-alive request).
    pub message_subtype: u8,
    /// Source ID: identifies the sending application.
    pub source_id: u16,
}

/// Secondary header of a PUS-C telemetry packet (7 bytes + time stamp).
///
/// The PUS version number is not stored; it is always
/// [`PUS_VERSION`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PusTmSecondaryHeader {
    /// Spacecraft time reference status, 4 bits (`0..=15`).
    pub time_reference_status: u8,
    /// Service type.
    pub service_type: u8,
    /// Message subtype.
    pub message_subtype: u8,
    /// Message type counter: counts per service type, subtype and
    /// destination.
    pub message_type_counter: u16,
    /// Destination ID: identifies the receiving application.
    pub destination_id: u16,
    /// Time stamp in a mission-specific format (e.g. CUC or CDS), as raw
    /// bytes. Its length must equal [`PusConfig::tm_time_len`]; see
    /// [`cuc_time`](Self::cuc_time) for CUC.
    pub time: Bytes,
}

impl PusTmSecondaryHeader {
    /// Interprets the time stamp as a CUC time in the given format.
    ///
    /// # Errors
    ///
    /// See [`CucTime::from_bytes`].
    pub fn cuc_time(&self, format: CucFormat) -> io::Result<CucTime> {
        CucTime::from_bytes(&self.time, format)
    }
}

/// A PUS-C telecommand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PusTc {
    /// Primary header of the underlying Space Packet. `packet_type` and
    /// `secondary_header_flag` are always set to `Telecommand` and `true`
    /// when encoding.
    pub header: SpacePacketHeader,
    /// The PUS secondary header.
    pub secondary_header: PusTcSecondaryHeader,
    /// Application data (may be empty).
    pub app_data: Bytes,
}

impl PusTc {
    /// Creates an unsegmented telecommand with all acknowledgement flags set
    /// and source ID 0.
    pub fn new(
        apid: u16,
        sequence_count: u16,
        service_type: u8,
        message_subtype: u8,
        app_data: impl Into<Bytes>,
    ) -> Self {
        PusTc {
            header: SpacePacketHeader {
                packet_type: PacketType::Telecommand,
                secondary_header_flag: true,
                apid,
                sequence_flags: SequenceFlags::Unsegmented,
                sequence_count,
            },
            secondary_header: PusTcSecondaryHeader {
                ack_flags: AckFlags::ALL,
                service_type,
                message_subtype,
                source_id: 0,
            },
            app_data: app_data.into(),
        }
    }
}

/// A PUS-C telemetry packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PusTm {
    /// Primary header of the underlying Space Packet. `packet_type` and
    /// `secondary_header_flag` are always set to `Telemetry` and `true`
    /// when encoding.
    pub header: SpacePacketHeader,
    /// The PUS secondary header.
    pub secondary_header: PusTmSecondaryHeader,
    /// Source data (may be empty).
    pub source_data: Bytes,
}

impl PusTm {
    /// Creates an unsegmented telemetry packet with time reference status 0,
    /// message type counter 0 and destination ID 0.
    ///
    /// `time` can be a [`CucTime`] or any raw time stamp.
    pub fn new(
        apid: u16,
        sequence_count: u16,
        service_type: u8,
        message_subtype: u8,
        time: impl Into<Bytes>,
        source_data: impl Into<Bytes>,
    ) -> Self {
        PusTm {
            header: SpacePacketHeader {
                packet_type: PacketType::Telemetry,
                secondary_header_flag: true,
                apid,
                sequence_flags: SequenceFlags::Unsegmented,
                sequence_count,
            },
            secondary_header: PusTmSecondaryHeader {
                time_reference_status: 0,
                service_type,
                message_subtype,
                message_type_counter: 0,
                destination_id: 0,
                time: time.into(),
            },
            source_data: source_data.into(),
        }
    }
}

/// A PUS packet: telecommand or telemetry. When decoding, the packet type
/// bit of the primary header selects the variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PusPacket {
    /// A telecommand.
    Tc(PusTc),
    /// A telemetry packet.
    Tm(PusTm),
}

impl crate::WireMessage for PusPacket {}

impl From<PusTc> for PusPacket {
    fn from(tc: PusTc) -> Self {
        PusPacket::Tc(tc)
    }
}

impl From<PusTm> for PusPacket {
    fn from(tm: PusTm) -> Self {
        PusPacket::Tm(tm)
    }
}

impl PusPacket {
    /// Primary header of the underlying Space Packet.
    pub fn header(&self) -> &SpacePacketHeader {
        match self {
            PusPacket::Tc(tc) => &tc.header,
            PusPacket::Tm(tm) => &tm.header,
        }
    }

    /// Service type from the secondary header.
    pub fn service_type(&self) -> u8 {
        match self {
            PusPacket::Tc(tc) => tc.secondary_header.service_type,
            PusPacket::Tm(tm) => tm.secondary_header.service_type,
        }
    }

    /// Message subtype from the secondary header.
    pub fn message_subtype(&self) -> u8 {
        match self {
            PusPacket::Tc(tc) => tc.secondary_header.message_subtype,
            PusPacket::Tm(tm) => tm.secondary_header.message_subtype,
        }
    }

    /// User data: application data (TC) or source data (TM).
    pub fn user_data(&self) -> &Bytes {
        match self {
            PusPacket::Tc(tc) => &tc.app_data,
            PusPacket::Tm(tm) => &tm.source_data,
        }
    }

    /// Converts the PUS packet into the underlying Space Packet.
    ///
    /// The packet data field contains the secondary header, the user data
    /// and, if enabled in `config`, the packet error control field.
    ///
    /// # Errors
    ///
    /// Fails with [`io::ErrorKind::InvalidInput`] if the TM time stamp does
    /// not have [`PusConfig::tm_time_len`] bytes, the time reference status
    /// exceeds 4 bits, or (with packet error control) a primary header
    /// field exceeds its bit width.
    pub fn to_space_packet(&self, config: &PusConfig) -> io::Result<SpacePacket> {
        let mut data = BytesMut::new();
        let mut header = *self.header();
        header.secondary_header_flag = true;

        match self {
            PusPacket::Tc(tc) => {
                header.packet_type = PacketType::Telecommand;
                let sec = &tc.secondary_header;
                data.reserve(TC_SECONDARY_HEADER_LEN + tc.app_data.len() + PEC_LEN);
                data.extend_from_slice(&[
                    PUS_VERSION << 4 | sec.ack_flags.to_bits(),
                    sec.service_type,
                    sec.message_subtype,
                ]);
                data.extend_from_slice(&sec.source_id.to_be_bytes());
                data.extend_from_slice(&tc.app_data);
            }
            PusPacket::Tm(tm) => {
                header.packet_type = PacketType::Telemetry;
                let sec = &tm.secondary_header;
                if sec.time_reference_status > 0x0F {
                    return Err(invalid_input(format!(
                        "time_reference_status {} exceeds the 4-bit range",
                        sec.time_reference_status
                    )));
                }
                if sec.time.len() != config.tm_time_len {
                    return Err(invalid_input(format!(
                        "time stamp has {} bytes, expected {} bytes",
                        sec.time.len(),
                        config.tm_time_len
                    )));
                }
                data.reserve(TM_SECONDARY_HEADER_LEN_WITHOUT_TIME + sec.time.len() + tm.source_data.len() + PEC_LEN);
                data.extend_from_slice(&[
                    PUS_VERSION << 4 | sec.time_reference_status,
                    sec.service_type,
                    sec.message_subtype,
                ]);
                data.extend_from_slice(&sec.message_type_counter.to_be_bytes());
                data.extend_from_slice(&sec.destination_id.to_be_bytes());
                data.extend_from_slice(&sec.time);
                data.extend_from_slice(&tm.source_data);
            }
        }

        if config.packet_error_control {
            // The CRC covers the whole packet, including the primary header.
            let mut crc_input =
                BytesMut::with_capacity(crate::protocol::ccsds::PRIMARY_HEADER_LEN + data.len() + PEC_LEN);
            header.encode(data.len() + PEC_LEN, &mut crc_input)?;
            crc_input.extend_from_slice(&data);
            data.extend_from_slice(&crc16_ccitt(&crc_input).to_be_bytes());
        }

        Ok(SpacePacket {
            header,
            data: data.freeze(),
        })
    }

    /// Interprets a Space Packet as a PUS packet.
    ///
    /// # Errors
    ///
    /// Fails with a [`PusDecodeError`] if the secondary header flag is not
    /// set, the packet data field is too short, the PUS version is not
    /// [`PUS_VERSION`], or (if enabled in `config`) the CRC of the packet
    /// error control field is wrong.
    pub fn from_space_packet(packet: SpacePacket, config: &PusConfig) -> Result<Self, PusDecodeError> {
        let SpacePacket { header, mut data } = packet;

        if !header.secondary_header_flag {
            return Err(PusDecodeError::MissingSecondaryHeader);
        }

        if config.packet_error_control {
            if data.len() < PEC_LEN {
                return Err(PusDecodeError::PacketTooShort {
                    len: data.len(),
                    min_len: PEC_LEN,
                });
            }
            let mut crc_input = BytesMut::with_capacity(crate::protocol::ccsds::PRIMARY_HEADER_LEN + data.len());
            crc_input.extend_from_slice(&header.to_bytes(data.len()));
            crc_input.extend_from_slice(&data);
            // The CRC over data + appended CRC is 0 if it is correct.
            if crc16_ccitt(&crc_input) != 0 {
                return Err(PusDecodeError::ChecksumError);
            }
            data.truncate(data.len() - PEC_LEN);
        }

        let min_len = match header.packet_type {
            PacketType::Telecommand => TC_SECONDARY_HEADER_LEN,
            PacketType::Telemetry => TM_SECONDARY_HEADER_LEN_WITHOUT_TIME + config.tm_time_len,
        };
        if data.len() < min_len {
            return Err(PusDecodeError::PacketTooShort {
                len: data.len(),
                min_len,
            });
        }

        let version = data[0] >> 4;
        if version != PUS_VERSION {
            return Err(PusDecodeError::UnsupportedPusVersion(version));
        }

        let low_nibble = data[0] & 0x0F;
        let service_type = data[1];
        let message_subtype = data[2];

        Ok(match header.packet_type {
            PacketType::Telecommand => PusPacket::Tc(PusTc {
                header,
                secondary_header: PusTcSecondaryHeader {
                    ack_flags: AckFlags::from_bits(low_nibble),
                    service_type,
                    message_subtype,
                    source_id: u16::from_be_bytes([data[3], data[4]]),
                },
                app_data: data.split_off(TC_SECONDARY_HEADER_LEN),
            }),
            PacketType::Telemetry => {
                let time_end = TM_SECONDARY_HEADER_LEN_WITHOUT_TIME + config.tm_time_len;
                PusPacket::Tm(PusTm {
                    header,
                    secondary_header: PusTmSecondaryHeader {
                        time_reference_status: low_nibble,
                        service_type,
                        message_subtype,
                        message_type_counter: u16::from_be_bytes([data[3], data[4]]),
                        destination_id: u16::from_be_bytes([data[5], data[6]]),
                        time: data.slice(TM_SECONDARY_HEADER_LEN_WITHOUT_TIME..time_end),
                    },
                    source_data: data.split_off(time_end),
                })
            }
        })
    }
}

/// Mission-specific parameters of the PUS format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PusConfig {
    /// Length of the time stamp in the TM secondary header in bytes.
    pub tm_time_len: usize,
    /// Whether TC and TM packets carry a packet error control field
    /// (CRC-16).
    pub packet_error_control: bool,
}

impl Default for PusConfig {
    /// Time stamp of [`DEFAULT_TM_TIME_LEN`] bytes, with packet error control.
    fn default() -> Self {
        PusConfig {
            tm_time_len: DEFAULT_TM_TIME_LEN,
            packet_error_control: true,
        }
    }
}

/// Codec for PUS-C packets, usable with [`tokio_util::codec::Framed`] and
/// the generic TCP actors (see [`PusServer`](crate::PusServer) and
/// friends).
///
/// [`SpacePacketCodec`] does the framing; this codec only converts between
/// [`SpacePacket`] and [`PusPacket`].
///
/// [`PusCodec::default`] uses [`PusConfig::default`]. For other
/// parameters, create the codec with [`PusCodec::new`] and pass it to the
/// TCP actors (e.g. as [`TcpServerArgs::codec`](crate::TcpServerArgs::codec))
/// or use it directly with `Framed`.
///
/// When decoding, a complete Space Packet that is not a valid PUS packet
/// (see [`PusPacket::from_space_packet`]) is dropped with a warning and
/// decoding continues with the next packet. The framing stays intact, so a
/// single invalid packet does not close the connection.
///
/// # Errors
///
/// Encoding fails as described at [`PusPacket::to_space_packet`] and
/// [`SpacePacketCodec`]. Decoding fails only if the framing fails, as
/// described at [`SpacePacketCodec`] (wrong packet version number).
#[derive(Debug, Clone, Copy, Default)]
pub struct PusCodec {
    config: PusConfig,
    inner: SpacePacketCodec,
}

impl PusCodec {
    /// Creates a codec with the given mission parameters.
    pub fn new(config: PusConfig) -> Self {
        PusCodec {
            config,
            inner: SpacePacketCodec,
        }
    }

    /// The mission parameters of this codec.
    pub fn config(&self) -> &PusConfig {
        &self.config
    }
}

impl Decoder for PusCodec {
    type Item = PusPacket;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        // Loop so that a valid packet buffered behind an invalid one is
        // returned right away instead of waiting for more input.
        while let Some(space_packet) = self.inner.decode(src)? {
            let header = space_packet.header;
            match PusPacket::from_space_packet(space_packet, &self.config) {
                Ok(packet) => return Ok(Some(packet)),
                Err(err) => warn!(
                    apid = header.apid,
                    sequence_count = header.sequence_count,
                    error = %err,
                    "dropping invalid PUS packet"
                ),
            }
        }
        Ok(None)
    }
}

impl Encoder<PusPacket> for PusCodec {
    type Error = io::Error;

    fn encode(&mut self, packet: PusPacket, dst: &mut BytesMut) -> io::Result<()> {
        let space_packet = packet.to_space_packet(&self.config)?;
        self.inner.encode(space_packet, dst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(codec: &mut PusCodec, packet: PusPacket) -> BytesMut {
        let mut buf = BytesMut::new();
        codec.encode(packet, &mut buf).unwrap();
        buf
    }

    #[test]
    fn default_time_len_matches_default_cuc_format() {
        assert_eq!(DEFAULT_TM_TIME_LEN, CucFormat::default().len());
    }

    #[test]
    fn tm_with_cuc_time_stamp_roundtrip() {
        let time: CucTime = "2026-09-26T12:00:00.5Z".parse().unwrap();
        let original = PusPacket::Tm(PusTm::new(1, 0, 17, 2, time, Bytes::new()));

        let mut codec = PusCodec::default();
        let mut buf = encode(&mut codec, original);
        let Some(PusPacket::Tm(tm)) = codec.decode(&mut buf).unwrap() else {
            panic!("expected TM")
        };

        assert_eq!(tm.secondary_header.cuc_time(CucFormat::default()).unwrap(), time);
    }

    #[test]
    fn crc16_ccitt_reference_value() {
        assert_eq!(crc16_ccitt(b"123456789"), 0x29B1);
    }

    #[test]
    fn tc_encode_produces_exact_expected_bytes() {
        let mut tc = PusTc::new(0x0AB, 1, 17, 1, &b"hi"[..]);
        tc.secondary_header.source_id = 0x1234;
        let mut codec = PusCodec::default();
        let buf = encode(&mut codec, tc.into());

        // Primary header: type TC + secondary header flag -> 0x18AB,
        // data length = 5 (sec. header) + 2 (data) + 2 (CRC) - 1 = 8
        assert_eq!(&buf[..6], &[0x18, 0xAB, 0xC0, 0x01, 0x00, 0x08]);
        // Secondary header: version 2 | ack 0b1111, service 17, subtype 1, source ID
        assert_eq!(&buf[6..11], &[0x2F, 17, 1, 0x12, 0x34]);
        assert_eq!(&buf[11..13], b"hi");
        let crc = crc16_ccitt(&buf[..13]);
        assert_eq!(&buf[13..], &crc.to_be_bytes());
    }

    #[test]
    fn tc_roundtrip() {
        let mut tc = PusTc::new(42, 7, 8, 1, &b"command"[..]);
        tc.secondary_header.ack_flags = AckFlags {
            acceptance: true,
            completion: true,
            ..AckFlags::NONE
        };
        let original = PusPacket::Tc(tc);

        let mut codec = PusCodec::default();
        let mut buf = encode(&mut codec, original.clone());
        let decoded = codec.decode(&mut buf).unwrap().expect("complete");

        assert_eq!(decoded, original);
        assert!(buf.is_empty());
    }

    #[test]
    fn tm_roundtrip_with_custom_config() {
        let config = PusConfig {
            tm_time_len: 4,
            packet_error_control: false,
        };
        let mut tm = PusTm::new(3, 99, 3, 25, &[1u8, 2, 3, 4][..], &b"housekeeping"[..]);
        tm.secondary_header.time_reference_status = 0x5;
        tm.secondary_header.message_type_counter = 0xBEEF;
        tm.secondary_header.destination_id = 0x0102;
        let original = PusPacket::Tm(tm);

        let mut codec = PusCodec::new(config);
        let mut buf = encode(&mut codec, original.clone());
        assert_eq!(buf.len(), 6 + 7 + 4 + 12, "no CRC appended without PEC");

        let decoded = codec.decode(&mut buf).unwrap().expect("complete");
        assert_eq!(decoded, original);
    }

    #[test]
    fn tm_without_source_data_is_valid() {
        let original = PusPacket::Tm(PusTm::new(1, 0, 17, 2, vec![0u8; DEFAULT_TM_TIME_LEN], Bytes::new()));
        let mut codec = PusCodec::default();
        let mut buf = encode(&mut codec, original.clone());
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(original));
    }

    #[test]
    fn decode_drops_packet_with_crc_error() {
        let mut codec = PusCodec::default();
        let mut buf = encode(&mut codec, PusTc::new(1, 1, 17, 1, &b"x"[..]).into());
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;

        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert!(buf.is_empty());
    }

    #[test]
    fn decode_continues_after_invalid_packet() {
        let mut codec = PusCodec::default();
        let mut buf = encode(&mut codec, PusTc::new(1, 1, 17, 1, &b"bad"[..]).into());
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;
        let valid: PusPacket = PusTc::new(1, 2, 17, 1, &b"good"[..]).into();
        buf.extend_from_slice(&encode(&mut codec, valid.clone()));

        assert_eq!(codec.decode(&mut buf).unwrap(), Some(valid));
        assert!(buf.is_empty());
    }

    #[test]
    fn decode_waits_for_complete_packet() {
        let mut codec = PusCodec::default();
        let full = encode(&mut codec, PusTc::new(1, 1, 17, 1, &b"1234"[..]).into());

        let mut partial = BytesMut::from(&full[..full.len() - 1]);
        assert_eq!(codec.decode(&mut partial).unwrap(), None);
        partial.extend_from_slice(&full[full.len() - 1..]);
        assert!(codec.decode(&mut partial).unwrap().is_some());
    }

    #[test]
    fn decode_rejects_space_packet_without_secondary_header() {
        let mut buf = BytesMut::new();
        SpacePacketCodec
            .encode(
                SpacePacket::new(PacketType::Telecommand, 1, 1, &b"0123456789"[..]),
                &mut buf,
            )
            .unwrap();
        assert_eq!(PusCodec::default().decode(&mut buf).unwrap(), None);
        assert!(buf.is_empty());
    }

    #[test]
    fn decode_rejects_wrong_pus_version() {
        let config = PusConfig {
            packet_error_control: false,
            ..PusConfig::default()
        };
        let mut space_packet = PusPacket::from(PusTc::new(1, 1, 17, 1, Bytes::new()))
            .to_space_packet(&config)
            .unwrap();
        let mut data = BytesMut::from(&space_packet.data[..]);
        data[0] = 0x1F; // PUS-A
        space_packet.data = data.freeze();

        let err = PusPacket::from_space_packet(space_packet, &config).unwrap_err();
        assert_eq!(err, PusDecodeError::UnsupportedPusVersion(1));
    }

    #[test]
    fn encode_rejects_wrong_time_len() {
        let tm = PusTm::new(1, 1, 3, 25, &[0u8; 3][..], Bytes::new());
        let mut buf = BytesMut::new();
        assert!(PusCodec::default().encode(tm.into(), &mut buf).is_err());
    }
}
