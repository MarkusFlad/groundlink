//! PUS service 1 "Request Verification" (ECSS-E-ST-70-41C, section 6.1).
//!
//! All verification reports TM(1,1) to TM(1,10) are represented by the
//! common type [`VerificationReport`]; [`VerificationKind`] tells which
//! report it is. It converts to and from the generic
//! [`PusTm`]/[`PusPacket`] types, so it can be sent with
//! [`PusCodec`](crate::PusCodec) and the PUS actors.
//!
//! Mission-specific field widths: the step ID and the failure code are 2
//! bytes (big-endian) each; the failure data take up all remaining bytes
//! of the source data.
//!
//! | Subtype | [`VerificationKind`] | Source data after the request ID |
//! |---|---|---|
//! | 1 / 2 | `AcceptanceSuccess` / `AcceptanceFailure` | – / failure notice |
//! | 3 / 4 | `StartSuccess` / `StartFailure` | – / failure notice |
//! | 5 / 6 | `ProgressSuccess` / `ProgressFailure` | step ID / step ID + failure notice |
//! | 7 / 8 | `CompletionSuccess` / `CompletionFailure` | – / failure notice |
//! | 10 | `RoutingFailure` | failure notice |
//!
//! ```
//! use groundlink::{
//!     FailureCode, PusPacket, PusTc, VerificationKind, VerificationReport,
//! };
//!
//! let tc = PusTc::new(0x042, 7, 17, 1, &b""[..]);
//! let kind = VerificationKind::StartFailure(FailureCode::InvalidApplicationData.notice(vec![17, 1]));
//! let report = VerificationReport::for_tc(0x042, 0, vec![0u8; 7], &tc, kind);
//!
//! let packet = PusPacket::try_from(report.clone()).unwrap();
//! assert_eq!((packet.service_type(), packet.message_subtype()), (1, 4));
//! assert_eq!(VerificationReport::try_from(packet).unwrap(), report);
//! ```

use bytes::{BufMut, Bytes, BytesMut};
use std::io;

use super::{PusDecodeError, PusPacket, PusTc, PusTm, PusTmSecondaryHeader};
use crate::protocol::ccsds::{APID_MAX, PacketType, SEQUENCE_COUNT_MAX, SequenceFlags, SpacePacketHeader};

/// Service type of the request verification service.
pub const SERVICE_TYPE: u8 = 1;
/// Length of the request ID in bytes.
pub const REQUEST_ID_LEN: usize = 4;
/// Length of the step ID in bytes.
pub const STEP_ID_LEN: usize = 2;
/// Length of the failure code in bytes.
pub const FAILURE_CODE_LEN: usize = 2;

fn invalid_data(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

fn invalid_input(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg)
}

/// Request ID: identifies the telecommand a verification report refers
/// to. It equals the first 4 bytes of that telecommand's primary header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestId {
    /// Packet version number, 3 bits (always 0 for CCSDS Space Packets).
    pub packet_version: u8,
    /// Packet type of the telecommand.
    pub packet_type: PacketType,
    /// Secondary header flag of the telecommand.
    pub secondary_header_flag: bool,
    /// APID, 11 bits.
    pub apid: u16,
    /// Sequence flags of the telecommand.
    pub sequence_flags: SequenceFlags,
    /// Packet sequence count, 14 bits.
    pub sequence_count: u16,
}

impl RequestId {
    /// The request ID for a primary header.
    pub fn from_header(header: &SpacePacketHeader) -> Self {
        RequestId {
            packet_version: 0,
            packet_type: header.packet_type,
            secondary_header_flag: header.secondary_header_flag,
            apid: header.apid,
            sequence_flags: header.sequence_flags,
            sequence_count: header.sequence_count,
        }
    }

    /// Appends the request ID (4 bytes, big-endian) to `dst`.
    ///
    /// # Errors
    ///
    /// Fails with [`io::ErrorKind::InvalidInput`] if a field exceeds its bit
    /// width.
    pub fn encode(&self, dst: &mut BytesMut) -> io::Result<()> {
        if self.packet_version > 0b111 || self.apid > APID_MAX || self.sequence_count > SEQUENCE_COUNT_MAX {
            return Err(invalid_input(format!(
                "request ID with values exceeding their bit widths: {self:?}"
            )));
        }
        dst.put_u16(
            (self.packet_version as u16) << 13
                | (self.packet_type.to_bit() as u16) << 12
                | (self.secondary_header_flag as u16) << 11
                | self.apid,
        );
        dst.put_u16((self.sequence_flags.to_bits() as u16) << 14 | self.sequence_count);
        Ok(())
    }

    /// Decodes a 4-byte request ID.
    ///
    /// # Errors
    ///
    /// Fails with [`io::ErrorKind::InvalidData`] if `src` is not exactly
    /// [`REQUEST_ID_LEN`] bytes long.
    pub fn decode(src: &[u8]) -> io::Result<Self> {
        let bytes: [u8; REQUEST_ID_LEN] = src
            .try_into()
            .map_err(|_| invalid_data(format!("request ID has {} bytes, expected {REQUEST_ID_LEN}", src.len())))?;
        let word0 = u16::from_be_bytes([bytes[0], bytes[1]]);
        let word1 = u16::from_be_bytes([bytes[2], bytes[3]]);
        Ok(RequestId {
            packet_version: (word0 >> 13) as u8,
            packet_type: PacketType::from_bit(word0 >> 12 & 1 != 0),
            secondary_header_flag: word0 >> 11 & 1 != 0,
            apid: word0 & APID_MAX,
            sequence_flags: SequenceFlags::from_bits((word1 >> 14) as u8),
            sequence_count: word1 & SEQUENCE_COUNT_MAX,
        })
    }
}

impl From<&PusTc> for RequestId {
    fn from(tc: &PusTc) -> Self {
        RequestId::from_header(&tc.header)
    }
}

/// Failure notice of a failure report: a mission-specific failure code and
/// related data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureNotice {
    /// Failure code; see [`FailureCode`] for the codes used by this crate.
    pub code: u16,
    /// Additional data about the failure (may be empty).
    pub data: Bytes,
}

impl FailureNotice {
    /// Creates a failure notice.
    pub fn new(code: u16, data: impl Into<Bytes>) -> Self {
        FailureNotice {
            code,
            data: data.into(),
        }
    }
}

/// Failure codes used by the actors of this crate.
///
/// The values are mission-specific; adapt them here if needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum FailureCode {
    /// No handler is registered for the service type of the TC.
    UnsupportedService = 1,
    /// The service does not support the message subtype.
    UnsupportedSubtype = 2,
    /// The application data do not match the telecommand.
    InvalidApplicationData = 3,
    /// The TC could not be forwarded to the service handler.
    RoutingFailed = 4,
    /// The TC is not a PUS packet: its secondary header flag is not set.
    MissingSecondaryHeader = 5,
    /// The packet data field of the TC is too short.
    PacketTooShort = 6,
    /// The CRC of the TC's packet error control field is wrong.
    ChecksumError = 7,
    /// The TC has an unsupported PUS version.
    UnsupportedPusVersion = 8,
}

impl FailureCode {
    /// The failure code for a numeric value, if known.
    pub fn from_code(code: u16) -> Option<Self> {
        [
            FailureCode::UnsupportedService,
            FailureCode::UnsupportedSubtype,
            FailureCode::InvalidApplicationData,
            FailureCode::RoutingFailed,
            FailureCode::MissingSecondaryHeader,
            FailureCode::PacketTooShort,
            FailureCode::ChecksumError,
            FailureCode::UnsupportedPusVersion,
        ]
        .into_iter()
        .find(|c| *c as u16 == code)
    }

    /// A failure notice with this code and the given data.
    pub fn notice(self, data: impl Into<Bytes>) -> FailureNotice {
        FailureNotice::new(self as u16, data)
    }
}

impl From<&PusDecodeError> for FailureCode {
    fn from(err: &PusDecodeError) -> Self {
        match err {
            PusDecodeError::MissingSecondaryHeader => FailureCode::MissingSecondaryHeader,
            PusDecodeError::PacketTooShort { .. } => FailureCode::PacketTooShort,
            PusDecodeError::ChecksumError => FailureCode::ChecksumError,
            PusDecodeError::UnsupportedPusVersion(_) => FailureCode::UnsupportedPusVersion,
        }
    }
}

impl From<FailureCode> for u16 {
    fn from(code: FailureCode) -> Self {
        code as u16
    }
}

/// Kind of verification report, including its subtype-specific data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerificationKind {
    /// TM(1,1) Successful Acceptance Verification Report.
    AcceptanceSuccess,
    /// TM(1,2) Failed Acceptance Verification Report.
    AcceptanceFailure(FailureNotice),
    /// TM(1,3) Successful Start of Execution Verification Report.
    StartSuccess,
    /// TM(1,4) Failed Start of Execution Verification Report.
    StartFailure(FailureNotice),
    /// TM(1,5) Successful Progress of Execution Verification Report.
    ProgressSuccess {
        /// The step that was completed.
        step_id: u16,
    },
    /// TM(1,6) Failed Progress of Execution Verification Report.
    ProgressFailure {
        /// The step that failed.
        step_id: u16,
        /// Why it failed.
        failure: FailureNotice,
    },
    /// TM(1,7) Successful Completion of Execution Verification Report.
    CompletionSuccess,
    /// TM(1,8) Failed Completion of Execution Verification Report.
    CompletionFailure(FailureNotice),
    /// TM(1,10) Failed Routing Verification Report.
    RoutingFailure(FailureNotice),
}

impl VerificationKind {
    /// The message subtype of this kind.
    pub fn subtype(&self) -> u8 {
        match self {
            VerificationKind::AcceptanceSuccess => 1,
            VerificationKind::AcceptanceFailure(_) => 2,
            VerificationKind::StartSuccess => 3,
            VerificationKind::StartFailure(_) => 4,
            VerificationKind::ProgressSuccess { .. } => 5,
            VerificationKind::ProgressFailure { .. } => 6,
            VerificationKind::CompletionSuccess => 7,
            VerificationKind::CompletionFailure(_) => 8,
            VerificationKind::RoutingFailure(_) => 10,
        }
    }

    /// Whether this is a failure report.
    pub fn is_failure(&self) -> bool {
        self.failure().is_some()
    }

    /// The failure notice, if this is a failure report.
    pub fn failure(&self) -> Option<&FailureNotice> {
        match self {
            VerificationKind::AcceptanceFailure(f)
            | VerificationKind::StartFailure(f)
            | VerificationKind::ProgressFailure { failure: f, .. }
            | VerificationKind::CompletionFailure(f)
            | VerificationKind::RoutingFailure(f) => Some(f),
            _ => None,
        }
    }

    /// Encodes the data after the request ID (step ID, failure notice).
    fn encode(&self, dst: &mut BytesMut) {
        if let VerificationKind::ProgressSuccess { step_id } | VerificationKind::ProgressFailure { step_id, .. } = self
        {
            dst.put_u16(*step_id);
        }
        if let Some(failure) = self.failure() {
            dst.put_u16(failure.code);
            dst.extend_from_slice(&failure.data);
        }
    }

    /// Decodes the data after the request ID according to the subtype.
    fn decode(subtype: u8, mut rest: Bytes) -> io::Result<Self> {
        let too_short = |needed: usize, rest: &Bytes| {
            invalid_data(format!(
                "TM(1,{subtype}): {} bytes after the request ID, expected at least {needed}",
                rest.len()
            ))
        };
        let take_u16 = |rest: &mut Bytes| {
            let value = u16::from_be_bytes([rest[0], rest[1]]);
            *rest = rest.slice(2..);
            value
        };
        let failure = |mut rest: Bytes| {
            if rest.len() < FAILURE_CODE_LEN {
                return Err(too_short(FAILURE_CODE_LEN, &rest));
            }
            let code = take_u16(&mut rest);
            Ok(FailureNotice { code, data: rest })
        };

        // Success reports carry no data after the request ID or step ID.
        let success = |kind: VerificationKind, rest: &Bytes| {
            if rest.is_empty() {
                Ok(kind)
            } else {
                Err(invalid_data(format!(
                    "TM(1,{subtype}) contains {} unexpected extra bytes",
                    rest.len()
                )))
            }
        };

        match subtype {
            1 => success(VerificationKind::AcceptanceSuccess, &rest),
            2 => Ok(VerificationKind::AcceptanceFailure(failure(rest)?)),
            3 => success(VerificationKind::StartSuccess, &rest),
            4 => Ok(VerificationKind::StartFailure(failure(rest)?)),
            5 | 6 => {
                if rest.len() < STEP_ID_LEN {
                    return Err(too_short(STEP_ID_LEN, &rest));
                }
                let step_id = take_u16(&mut rest);
                if subtype == 5 {
                    success(VerificationKind::ProgressSuccess { step_id }, &rest)
                } else {
                    Ok(VerificationKind::ProgressFailure {
                        step_id,
                        failure: failure(rest)?,
                    })
                }
            }
            7 => success(VerificationKind::CompletionSuccess, &rest),
            8 => Ok(VerificationKind::CompletionFailure(failure(rest)?)),
            10 => Ok(VerificationKind::RoutingFailure(failure(rest)?)),
            _ => Err(invalid_data(format!("TM(1,{subtype}) is not a verification report"))),
        }
    }
}

/// A verification report TM(1,x): reports success or failure of one stage
/// in processing the telecommand identified by
/// [`request_id`](Self::request_id).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationReport {
    /// Primary header of the underlying Space Packet. `packet_type` and
    /// `secondary_header_flag` are always set to `Telemetry` and `true`
    /// when encoding.
    pub header: SpacePacketHeader,
    /// Spacecraft time reference status, 4 bits.
    pub time_reference_status: u8,
    /// Message type counter of the TM secondary header.
    pub message_type_counter: u16,
    /// Destination ID: receiver of the report, usually the source ID of the
    /// verified telecommand.
    pub destination_id: u16,
    /// Time stamp as raw bytes (e.g. a [`CucTime`](crate::CucTime)).
    pub time: Bytes,
    /// The verified telecommand.
    pub request_id: RequestId,
    /// Which report this is, with its subtype-specific data.
    pub kind: VerificationKind,
}

impl VerificationReport {
    /// Creates an unsegmented report with time reference status 0, message
    /// type counter 0 and destination ID 0.
    pub fn new(
        apid: u16,
        sequence_count: u16,
        time: impl Into<Bytes>,
        request_id: RequestId,
        kind: VerificationKind,
    ) -> Self {
        VerificationReport {
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
            request_id,
            kind,
        }
    }

    /// Creates the report for a received telecommand: the request ID is
    /// taken from its header and the destination ID is its source ID.
    pub fn for_tc(apid: u16, sequence_count: u16, time: impl Into<Bytes>, tc: &PusTc, kind: VerificationKind) -> Self {
        let mut report = Self::new(apid, sequence_count, time, RequestId::from(tc), kind);
        report.destination_id = tc.secondary_header.source_id;
        report
    }
}

impl TryFrom<VerificationReport> for PusTm {
    type Error = io::Error;

    /// Fails if a field of the request ID exceeds its bit width.
    fn try_from(report: VerificationReport) -> io::Result<Self> {
        let mut source_data = BytesMut::new();
        report.request_id.encode(&mut source_data)?;
        report.kind.encode(&mut source_data);
        Ok(PusTm {
            header: report.header,
            secondary_header: PusTmSecondaryHeader {
                time_reference_status: report.time_reference_status,
                service_type: SERVICE_TYPE,
                message_subtype: report.kind.subtype(),
                message_type_counter: report.message_type_counter,
                destination_id: report.destination_id,
                time: report.time,
            },
            source_data: source_data.freeze(),
        })
    }
}

impl TryFrom<VerificationReport> for PusPacket {
    type Error = io::Error;

    fn try_from(report: VerificationReport) -> io::Result<Self> {
        Ok(PusPacket::Tm(report.try_into()?))
    }
}

impl TryFrom<PusTm> for VerificationReport {
    type Error = io::Error;

    /// Fails if the packet is not a verification report or its source data
    /// do not match the subtype.
    fn try_from(tm: PusTm) -> io::Result<Self> {
        let sec = tm.secondary_header;
        if sec.service_type != SERVICE_TYPE {
            return Err(invalid_data(format!(
                "TM({},{}) is not a verification report (service {SERVICE_TYPE})",
                sec.service_type, sec.message_subtype
            )));
        }
        if tm.source_data.len() < REQUEST_ID_LEN {
            return Err(invalid_data(format!(
                "TM(1,{}): source data ({} bytes) too short for the request ID",
                sec.message_subtype,
                tm.source_data.len()
            )));
        }
        let request_id = RequestId::decode(&tm.source_data[..REQUEST_ID_LEN])?;
        let kind = VerificationKind::decode(sec.message_subtype, tm.source_data.slice(REQUEST_ID_LEN..))?;
        Ok(VerificationReport {
            header: tm.header,
            time_reference_status: sec.time_reference_status,
            message_type_counter: sec.message_type_counter,
            destination_id: sec.destination_id,
            time: sec.time,
            request_id,
            kind,
        })
    }
}

impl TryFrom<PusPacket> for VerificationReport {
    type Error = io::Error;

    fn try_from(packet: PusPacket) -> io::Result<Self> {
        match packet {
            PusPacket::Tm(tm) => tm.try_into(),
            PusPacket::Tc(_) => Err(invalid_data("telecommand is not a verification report".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::cuc::{CucFormat, CucTime};
    use crate::protocol::pus::PusCodec;
    use tokio_util::codec::{Decoder, Encoder};

    fn time() -> CucTime {
        "2026-09-26T12:00:00Z".parse().unwrap()
    }

    fn tc() -> PusTc {
        PusTc::new(0x0AB, 1, 17, 1, Bytes::new())
    }

    fn all_kinds() -> Vec<VerificationKind> {
        let failure = || FailureNotice::new(0xBEEF, &b"details"[..]);
        vec![
            VerificationKind::AcceptanceSuccess,
            VerificationKind::AcceptanceFailure(failure()),
            VerificationKind::StartSuccess,
            VerificationKind::StartFailure(FailureNotice::new(1, Bytes::new())),
            VerificationKind::ProgressSuccess { step_id: 3 },
            VerificationKind::ProgressFailure {
                step_id: 4,
                failure: failure(),
            },
            VerificationKind::CompletionSuccess,
            VerificationKind::CompletionFailure(failure()),
            VerificationKind::RoutingFailure(failure()),
        ]
    }

    fn source_data(kind: VerificationKind) -> Bytes {
        let report = VerificationReport::new(1, 0, time(), RequestId::from(&tc()), kind);
        PusTm::try_from(report).unwrap().source_data
    }

    #[test]
    fn request_id_equals_first_4_header_bytes_of_tc() {
        let tc = PusTc::new(0x0AB, 0x1234, 17, 1, Bytes::new());
        let mut tc_bytes = BytesMut::new();
        PusCodec::default().encode(tc.clone().into(), &mut tc_bytes).unwrap();

        let mut id_bytes = BytesMut::new();
        RequestId::from(&tc).encode(&mut id_bytes).unwrap();

        assert_eq!(&id_bytes[..], &tc_bytes[..REQUEST_ID_LEN]);
        assert_eq!(RequestId::decode(&id_bytes).unwrap(), RequestId::from(&tc));
    }

    #[test]
    fn tm_1_1_produces_exact_expected_bytes() {
        let report = VerificationReport::new(
            0x010,
            5,
            time(),
            RequestId::from(&tc()),
            VerificationKind::AcceptanceSuccess,
        );

        let mut buf = BytesMut::new();
        PusCodec::default()
            .encode(report.try_into().unwrap(), &mut buf)
            .unwrap();

        // Data length = 7 (sec. header) + 7 (CUC) + 4 (request ID) + 2 (CRC) - 1 = 19
        assert_eq!(&buf[..6], &[0x08, 0x10, 0xC0, 0x05, 0x00, 19]);
        assert_eq!(&buf[6..13], &[0x20, 1, 1, 0, 0, 0, 0]);
        assert_eq!(&buf[20..24], &[0x18, 0xAB, 0xC0, 0x01]);
        assert_eq!(buf.len(), 26);
    }

    #[test]
    fn source_data_layout_per_subtype() {
        let id = [0x18, 0xAB, 0xC0, 0x01];
        assert_eq!(&source_data(VerificationKind::CompletionSuccess)[..], &id);
        assert_eq!(
            &source_data(VerificationKind::ProgressSuccess { step_id: 0x0102 })[..],
            &[&id[..], &[1, 2]].concat()[..]
        );
        assert_eq!(
            &source_data(VerificationKind::StartFailure(FailureNotice::new(0xBEEF, &[9u8][..])))[..],
            &[&id[..], &[0xBE, 0xEF, 9]].concat()[..]
        );
        assert_eq!(
            &source_data(VerificationKind::ProgressFailure {
                step_id: 7,
                failure: FailureNotice::new(0x0A0B, Bytes::new())
            })[..],
            &[&id[..], &[0, 7, 0x0A, 0x0B]].concat()[..]
        );
    }

    #[test]
    fn roundtrip_of_all_reports_through_pus_codec() {
        let mut tc = tc();
        tc.secondary_header.source_id = 0x0815;
        let mut codec = PusCodec::default();

        for kind in all_kinds() {
            let mut original = VerificationReport::for_tc(1, 99, time(), &tc, kind);
            original.message_type_counter = 3;
            assert_eq!(original.destination_id, 0x0815);

            let mut buf = BytesMut::new();
            codec.encode(original.clone().try_into().unwrap(), &mut buf).unwrap();
            let packet = codec.decode(&mut buf).unwrap().unwrap();
            assert_eq!(packet.message_subtype(), original.kind.subtype());

            let decoded = VerificationReport::try_from(packet).unwrap();
            assert_eq!(decoded, original);
            assert_eq!(
                CucTime::from_bytes(&decoded.time, CucFormat::default()).unwrap(),
                time()
            );
        }
    }

    #[test]
    fn rejects_invalid_packets() {
        let id = [0x18, 0xAB, 0xC0, 0x01];
        let tm = |service, subtype, data: &[u8]| PusTm::new(1, 1, service, subtype, time(), data.to_vec());

        for (service, subtype, data) in [
            (17, 1, &id[..]),                         // wrong service
            (1, 9, &id[..]),                          // TM(1,9) does not exist
            (1, 1, &id[..3]),                         // request ID too short
            (1, 1, &[&id[..], &[0]].concat()[..]),    // extra bytes
            (1, 2, &id[..]),                          // failure code missing
            (1, 5, &[&id[..], &[0]].concat()[..]),    // step ID too short
            (1, 6, &[&id[..], &[0, 1]].concat()[..]), // failure code missing
        ] {
            assert!(
                VerificationReport::try_from(tm(service, subtype, data)).is_err(),
                "TM({service},{subtype}) {data:?}"
            );
        }
        assert!(VerificationReport::try_from(PusPacket::Tc(tc())).is_err());
    }

    #[test]
    fn rejects_invalid_request_id() {
        let mut request_id = RequestId::from(&tc());
        request_id.apid = APID_MAX + 1;
        let report = VerificationReport::new(1, 1, time(), request_id, VerificationKind::AcceptanceSuccess);
        assert!(PusPacket::try_from(report).is_err());
    }

    #[test]
    fn failure_code_roundtrip() {
        for code in [
            FailureCode::UnsupportedService,
            FailureCode::UnsupportedSubtype,
            FailureCode::InvalidApplicationData,
            FailureCode::RoutingFailed,
            FailureCode::MissingSecondaryHeader,
            FailureCode::PacketTooShort,
            FailureCode::ChecksumError,
            FailureCode::UnsupportedPusVersion,
        ] {
            assert_eq!(FailureCode::from_code(code.into()), Some(code));
            assert_eq!(code.notice(Bytes::new()).code, u16::from(code));
        }
        assert_eq!(FailureCode::from_code(0), None);
    }

    #[test]
    fn failure_helpers() {
        for kind in all_kinds() {
            assert_eq!(kind.is_failure(), kind.subtype() % 2 == 0, "{kind:?}");
        }
    }
}
