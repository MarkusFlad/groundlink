//! PUS service 17 "Test" (ECSS-E-ST-70-41C, section 6.17).
//!
//! Contains typed messages that convert losslessly to and from the generic
//! [`PusTc`]/[`PusTm`]/[`PusPacket`] types, so they can be sent with
//! [`PusCodec`](crate::PusCodec) and the PUS actors.
//!
//! ```
//! use groundlink::{AreYouAliveReport, AreYouAliveRequest, PusPacket};
//!
//! let request = AreYouAliveRequest::new(0x042, 1);
//! let packet = PusPacket::from(request.clone());
//! assert_eq!(AreYouAliveRequest::try_from(packet).unwrap(), request);
//!
//! let report = AreYouAliveReport::for_request(0x042, 0, vec![0u8; 7], &request);
//! let packet = PusPacket::from(report);
//! assert_eq!((packet.service_type(), packet.message_subtype()), (17, 2));
//! ```

use bytes::Bytes;
use std::io;

use super::{AckFlags, PusPacket, PusTc, PusTcSecondaryHeader, PusTm, PusTmSecondaryHeader};
use crate::ccsds::{PacketType, SequenceFlags, SpacePacketHeader};

/// Service type of the test service.
pub const SERVICE_TYPE: u8 = 17;
/// Message subtype of TC(17,1) "Perform an Are-You-Alive Connection Test".
pub const ARE_YOU_ALIVE_REQUEST_SUBTYPE: u8 = 1;
/// Message subtype of TM(17,2) "Are-You-Alive Connection Report".
pub const ARE_YOU_ALIVE_REPORT_SUBTYPE: u8 = 2;

/// TC(17,1) "Perform an Are-You-Alive Connection Test": asks the receiver
/// to answer with TM(17,2). The telecommand carries no application data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AreYouAliveRequest {
    /// Primary header of the underlying Space Packet. `packet_type` and
    /// `secondary_header_flag` are always set to `Telecommand` and `true`
    /// when encoding.
    pub header: SpacePacketHeader,
    /// Which successful verification reports are requested.
    pub ack_flags: AckFlags,
    /// Source ID: identifies the sending application.
    pub source_id: u16,
}

impl AreYouAliveRequest {
    /// Creates an unsegmented TC(17,1) with all acknowledgement flags set
    /// and source ID 0.
    pub fn new(apid: u16, sequence_count: u16) -> Self {
        AreYouAliveRequest {
            header: SpacePacketHeader {
                packet_type: PacketType::Telecommand,
                secondary_header_flag: true,
                apid,
                sequence_flags: SequenceFlags::Unsegmented,
                sequence_count,
            },
            ack_flags: AckFlags::ALL,
            source_id: 0,
        }
    }
}

impl From<AreYouAliveRequest> for PusTc {
    fn from(request: AreYouAliveRequest) -> Self {
        PusTc {
            header: request.header,
            secondary_header: PusTcSecondaryHeader {
                ack_flags: request.ack_flags,
                service_type: SERVICE_TYPE,
                message_subtype: ARE_YOU_ALIVE_REQUEST_SUBTYPE,
                source_id: request.source_id,
            },
            app_data: Bytes::new(),
        }
    }
}

impl From<AreYouAliveRequest> for PusPacket {
    fn from(request: AreYouAliveRequest) -> Self {
        PusPacket::Tc(request.into())
    }
}

impl TryFrom<PusTc> for AreYouAliveRequest {
    type Error = io::Error;

    /// Fails if the telecommand is not TC(17,1) or carries application
    /// data.
    fn try_from(tc: PusTc) -> io::Result<Self> {
        let sec = &tc.secondary_header;
        if (sec.service_type, sec.message_subtype) != (SERVICE_TYPE, ARE_YOU_ALIVE_REQUEST_SUBTYPE) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "TC({},{}) is not an are-you-alive request TC({SERVICE_TYPE},{ARE_YOU_ALIVE_REQUEST_SUBTYPE})",
                    sec.service_type, sec.message_subtype
                ),
            ));
        }
        if !tc.app_data.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "TC(17,1) must not contain application data (received {} bytes)",
                    tc.app_data.len()
                ),
            ));
        }
        Ok(AreYouAliveRequest {
            header: tc.header,
            ack_flags: sec.ack_flags,
            source_id: sec.source_id,
        })
    }
}

impl TryFrom<PusPacket> for AreYouAliveRequest {
    type Error = io::Error;

    fn try_from(packet: PusPacket) -> io::Result<Self> {
        match packet {
            PusPacket::Tc(tc) => tc.try_into(),
            PusPacket::Tm(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "telemetry packet is not an are-you-alive request",
            )),
        }
    }
}

/// TM(17,2) "Are-You-Alive Connection Report": the answer to TC(17,1).
/// The report carries no source data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AreYouAliveReport {
    /// Primary header of the underlying Space Packet. `packet_type` and
    /// `secondary_header_flag` are always set to `Telemetry` and `true`
    /// when encoding.
    pub header: SpacePacketHeader,
    /// Spacecraft time reference status, 4 bits.
    pub time_reference_status: u8,
    /// Message type counter of the TM secondary header.
    pub message_type_counter: u16,
    /// Destination ID: receiver of the report, usually the source ID of the
    /// TC(17,1).
    pub destination_id: u16,
    /// Time stamp as raw bytes (e.g. a [`CucTime`](crate::CucTime)).
    pub time: Bytes,
}

impl AreYouAliveReport {
    /// Creates an unsegmented report with time reference status 0, message
    /// type counter 0 and destination ID 0.
    pub fn new(apid: u16, sequence_count: u16, time: impl Into<Bytes>) -> Self {
        AreYouAliveReport {
            header: SpacePacketHeader {
                packet_type: PacketType::Telemetry,
                secondary_header_flag: true,
                apid,
                sequence_flags: SequenceFlags::Unsegmented,
                sequence_count,
            },
            time_reference_status: 0,
            message_type_counter: 0,
            destination_id: 0,
            time: time.into(),
        }
    }

    /// Creates the answer to a TC(17,1): the destination ID is the
    /// request's source ID.
    pub fn for_request(apid: u16, sequence_count: u16, time: impl Into<Bytes>, request: &AreYouAliveRequest) -> Self {
        let mut report = Self::new(apid, sequence_count, time);
        report.destination_id = request.source_id;
        report
    }
}

impl From<AreYouAliveReport> for PusTm {
    fn from(report: AreYouAliveReport) -> Self {
        PusTm {
            header: report.header,
            secondary_header: PusTmSecondaryHeader {
                time_reference_status: report.time_reference_status,
                service_type: SERVICE_TYPE,
                message_subtype: ARE_YOU_ALIVE_REPORT_SUBTYPE,
                message_type_counter: report.message_type_counter,
                destination_id: report.destination_id,
                time: report.time,
            },
            source_data: Bytes::new(),
        }
    }
}

impl From<AreYouAliveReport> for PusPacket {
    fn from(report: AreYouAliveReport) -> Self {
        PusPacket::Tm(report.into())
    }
}

impl TryFrom<PusTm> for AreYouAliveReport {
    type Error = io::Error;

    /// Fails if the packet is not TM(17,2) or carries source data.
    fn try_from(tm: PusTm) -> io::Result<Self> {
        let sec = tm.secondary_header;
        if (sec.service_type, sec.message_subtype) != (SERVICE_TYPE, ARE_YOU_ALIVE_REPORT_SUBTYPE) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "TM({},{}) is not an are-you-alive report TM({SERVICE_TYPE},{ARE_YOU_ALIVE_REPORT_SUBTYPE})",
                    sec.service_type, sec.message_subtype
                ),
            ));
        }
        if !tm.source_data.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "TM(17,2) must not contain source data (received {} bytes)",
                    tm.source_data.len()
                ),
            ));
        }
        Ok(AreYouAliveReport {
            header: tm.header,
            time_reference_status: sec.time_reference_status,
            message_type_counter: sec.message_type_counter,
            destination_id: sec.destination_id,
            time: sec.time,
        })
    }
}

impl TryFrom<PusPacket> for AreYouAliveReport {
    type Error = io::Error;

    fn try_from(packet: PusPacket) -> io::Result<Self> {
        match packet {
            PusPacket::Tm(tm) => tm.try_into(),
            PusPacket::Tc(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "telecommand is not an are-you-alive report",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cuc::{CucFormat, CucTime};
    use crate::pus::PusCodec;
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    #[test]
    fn encode_produces_exact_expected_bytes() {
        let mut request = AreYouAliveRequest::new(0x0AB, 1);
        request.source_id = 0x1234;

        let mut buf = BytesMut::new();
        PusCodec::default().encode(request.into(), &mut buf).unwrap();

        // Data length = 5 (sec. header) + 0 (data) + 2 (CRC) - 1 = 6
        assert_eq!(&buf[..6], &[0x18, 0xAB, 0xC0, 0x01, 0x00, 0x06]);
        assert_eq!(&buf[6..11], &[0x2F, 17, 1, 0x12, 0x34]);
        assert_eq!(buf.len(), 13, "only the CRC follows");
    }

    #[test]
    fn roundtrip_through_pus_codec() {
        let mut original = AreYouAliveRequest::new(42, 7);
        original.ack_flags = AckFlags {
            acceptance: true,
            ..AckFlags::NONE
        };
        original.source_id = 5;

        let mut codec = PusCodec::default();
        let mut buf = BytesMut::new();
        codec.encode(original.clone().into(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().expect("complete");

        assert_eq!(AreYouAliveRequest::try_from(decoded).unwrap(), original);
    }

    #[test]
    fn rejects_other_service() {
        let tc = PusTc::new(1, 1, 17, 3, Bytes::new());
        assert!(AreYouAliveRequest::try_from(tc).is_err());
    }

    #[test]
    fn rejects_application_data() {
        let tc = PusTc::new(1, 1, 17, 1, &b"x"[..]);
        assert!(AreYouAliveRequest::try_from(tc).is_err());
    }

    #[test]
    fn rejects_telemetry() {
        let tm = PusTm::new(1, 1, 17, 1, vec![0u8; 7], Bytes::new());
        assert!(AreYouAliveRequest::try_from(PusPacket::Tm(tm)).is_err());
    }

    fn time() -> CucTime {
        "2026-09-26T12:00:00Z".parse().unwrap()
    }

    #[test]
    fn report_encode_produces_exact_expected_bytes() {
        let mut report = AreYouAliveReport::new(0x0AB, 2, time());
        report.message_type_counter = 0x0102;
        report.destination_id = 0x1234;

        let mut buf = BytesMut::new();
        PusCodec::default().encode(report.into(), &mut buf).unwrap();

        // Data length = 7 (sec. header) + 7 (CUC) + 0 (data) + 2 (CRC) - 1 = 15
        assert_eq!(&buf[..6], &[0x08, 0xAB, 0xC0, 0x02, 0x00, 15]);
        assert_eq!(&buf[6..13], &[0x20, 17, 2, 0x01, 0x02, 0x12, 0x34]);
        assert_eq!(buf.len(), 22, "only time stamp and CRC follow");
    }

    #[test]
    fn report_roundtrip_through_pus_codec() {
        let mut request = AreYouAliveRequest::new(1, 1);
        request.source_id = 0x0815;
        let mut original = AreYouAliveReport::for_request(42, 7, time(), &request);
        original.message_type_counter = 3;
        assert_eq!(original.destination_id, 0x0815);

        let mut codec = PusCodec::default();
        let mut buf = BytesMut::new();
        codec.encode(original.clone().into(), &mut buf).unwrap();
        let decoded = AreYouAliveReport::try_from(codec.decode(&mut buf).unwrap().unwrap()).unwrap();

        assert_eq!(decoded, original);
        assert_eq!(
            CucTime::from_bytes(&decoded.time, CucFormat::default()).unwrap(),
            time()
        );
    }

    #[test]
    fn report_rejects_invalid_packets() {
        let tm = |subtype, data: &[u8]| PusTm::new(1, 1, 17, subtype, time(), data.to_vec());
        assert!(AreYouAliveReport::try_from(tm(1, &[])).is_err());
        assert!(AreYouAliveReport::try_from(tm(2, &[0])).is_err());
        assert!(AreYouAliveReport::try_from(PusTm::new(1, 1, 1, 2, time(), Bytes::new())).is_err());
        let tc = PusTc::new(1, 1, 17, 2, Bytes::new());
        assert!(AreYouAliveReport::try_from(PusPacket::Tc(tc)).is_err());
    }
}
