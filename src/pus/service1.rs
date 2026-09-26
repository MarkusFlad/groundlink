//! PUS Service 1 "Request Verification" (ECSS-E-ST-70-41C, Abschnitt 6.1).
//!
//! Alle Verifikationsberichte TM(1,1) bis TM(1,10) werden durch den
//! gemeinsamen Typ [`VerificationReport`] abgebildet; welcher Bericht
//! vorliegt, bestimmt [`VerificationKind`]. Er lässt sich in die
//! generischen [`PusTm`]/[`PusPacket`]-Typen und zurück wandeln, sodass er
//! mit dem [`crate::PusCodec`] und den PUS-Actors übertragen werden kann.
//!
//! Missionsspezifische Feldbreiten: Step ID und Failure Code sind hier je
//! 2 Byte (Big-Endian) lang; die Failure Data umfassen alle restlichen
//! Byte der Source Data.

use bytes::{BufMut, Bytes, BytesMut};
use std::io;

use super::{PusPacket, PusTc, PusTm, PusTmSecondaryHeader};
use crate::ccsds::{PacketType, SequenceFlags, SpacePacketHeader, APID_MAX, SEQUENCE_COUNT_MAX};

/// Service Type des Request-Verification-Service.
pub const SERVICE_TYPE: u8 = 1;
/// Länge der Request ID in Byte.
pub const REQUEST_ID_LEN: usize = 4;
/// Länge der Step ID in Byte.
pub const STEP_ID_LEN: usize = 2;
/// Länge des Failure Code in Byte.
pub const FAILURE_CODE_LEN: usize = 2;

fn invalid_data(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

fn invalid_input(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg)
}

/// Request ID: identifiziert das Telekommando, auf das sich ein
/// Verifikationsbericht bezieht. Entspricht den ersten 4 Byte des
/// Primary Headers dieses Telekommandos.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestId {
    /// Packet Version Number, 3 Bit (für CCSDS Space Packets immer 0).
    pub packet_version: u8,
    pub packet_type: PacketType,
    pub secondary_header_flag: bool,
    /// APID, 11 Bit.
    pub apid: u16,
    pub sequence_flags: SequenceFlags,
    /// Packet Sequence Count, 14 Bit.
    pub sequence_count: u16,
}

impl RequestId {
    /// Die Request ID zu einem Primary Header.
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

    /// Kodiert die Request ID (4 Byte, Big-Endian) an `dst`.
    pub fn encode(&self, dst: &mut BytesMut) -> io::Result<()> {
        if self.packet_version > 0b111 || self.apid > APID_MAX || self.sequence_count > SEQUENCE_COUNT_MAX {
            return Err(invalid_input(format!("Request ID mit Werten außerhalb der Bitbreiten: {self:?}")));
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

    /// Dekodiert eine 4 Byte lange Request ID.
    pub fn decode(src: &[u8]) -> io::Result<Self> {
        let bytes: [u8; REQUEST_ID_LEN] = src.try_into().map_err(|_| {
            invalid_data(format!("Request ID hat {} Byte, erwartet werden {REQUEST_ID_LEN}", src.len()))
        })?;
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

/// Failure Notice eines Fehlerberichts: missionsspezifischer Fehlercode
/// und zugehörige Zusatzdaten.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureNotice {
    pub code: u16,
    /// Zusatzdaten zum Fehler (dürfen leer sein).
    pub data: Bytes,
}

impl FailureNotice {
    pub fn new(code: u16, data: impl Into<Bytes>) -> Self {
        FailureNotice { code, data: data.into() }
    }
}

/// Art des Verifikationsberichts inkl. der subtype-spezifischen Daten.
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
    ProgressSuccess { step_id: u16 },
    /// TM(1,6) Failed Progress of Execution Verification Report.
    ProgressFailure { step_id: u16, failure: FailureNotice },
    /// TM(1,7) Successful Completion of Execution Verification Report.
    CompletionSuccess,
    /// TM(1,8) Failed Completion of Execution Verification Report.
    CompletionFailure(FailureNotice),
    /// TM(1,10) Failed Routing Verification Report.
    RoutingFailure(FailureNotice),
}

impl VerificationKind {
    /// Der zugehörige Message Subtype.
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

    /// Ob es sich um einen Fehlerbericht handelt.
    pub fn is_failure(&self) -> bool {
        self.failure().is_some()
    }

    /// Die Failure Notice, falls es sich um einen Fehlerbericht handelt.
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

    /// Kodiert die Daten nach der Request ID (Step ID, Failure Notice).
    fn encode(&self, dst: &mut BytesMut) {
        if let VerificationKind::ProgressSuccess { step_id } | VerificationKind::ProgressFailure { step_id, .. } = self {
            dst.put_u16(*step_id);
        }
        if let Some(failure) = self.failure() {
            dst.put_u16(failure.code);
            dst.extend_from_slice(&failure.data);
        }
    }

    /// Dekodiert die Daten nach der Request ID anhand des Subtypes.
    fn decode(subtype: u8, mut rest: Bytes) -> io::Result<Self> {
        let too_short = |needed: usize, rest: &Bytes| {
            invalid_data(format!(
                "TM(1,{subtype}): {} Byte nach der Request ID, mindestens {needed} erwartet",
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

        // Erfolgsberichte tragen nach Request ID bzw. Step ID keine Daten mehr.
        let success = |kind: VerificationKind, rest: &Bytes| {
            if rest.is_empty() {
                Ok(kind)
            } else {
                Err(invalid_data(format!("TM(1,{subtype}) enthält {} unerwartete zusätzliche Byte", rest.len())))
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
                    Ok(VerificationKind::ProgressFailure { step_id, failure: failure(rest)? })
                }
            }
            7 => success(VerificationKind::CompletionSuccess, &rest),
            8 => Ok(VerificationKind::CompletionFailure(failure(rest)?)),
            10 => Ok(VerificationKind::RoutingFailure(failure(rest)?)),
            _ => Err(invalid_data(format!("TM(1,{subtype}) ist kein Verifikationsbericht"))),
        }
    }
}

/// Ein Verifikationsbericht TM(1,x): meldet Erfolg oder Fehlschlag einer
/// Verarbeitungsstufe des durch [`request_id`](Self::request_id)
/// bezeichneten Telekommandos.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationReport {
    /// Primary Header des zugrunde liegenden Space Packets.
    /// `packet_type` und `secondary_header_flag` werden beim Kodieren
    /// stets auf `Telemetry` bzw. `true` gesetzt.
    pub header: SpacePacketHeader,
    /// Spacecraft Time Reference Status, 4 Bit.
    pub time_reference_status: u8,
    pub message_type_counter: u16,
    /// Destination ID: Empfänger des Berichts, üblicherweise die Source ID
    /// des verifizierten Telekommandos.
    pub destination_id: u16,
    /// Zeitstempel, roh (z. B. eine [`crate::CucTime`]).
    pub time: Bytes,
    pub request_id: RequestId,
    pub kind: VerificationKind,
}

impl VerificationReport {
    /// Erstellt einen unsegmentierten Bericht mit Time Reference Status 0,
    /// Message Type Counter 0 und Destination ID 0.
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

    /// Erstellt den Bericht zu einem empfangenen Telekommando: Request ID
    /// aus dessen Header, Destination ID = dessen Source ID.
    pub fn for_tc(
        apid: u16,
        sequence_count: u16,
        time: impl Into<Bytes>,
        tc: &PusTc,
        kind: VerificationKind,
    ) -> Self {
        let mut report = Self::new(apid, sequence_count, time, RequestId::from(tc), kind);
        report.destination_id = tc.secondary_header.source_id;
        report
    }
}

impl TryFrom<VerificationReport> for PusTm {
    type Error = io::Error;

    /// Schlägt fehl, wenn die Request ID Werte außerhalb ihrer Bitbreiten
    /// enthält.
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

    /// Schlägt fehl, wenn das Paket kein Verifikationsbericht ist oder die
    /// Source Data nicht zum Subtype passen.
    fn try_from(tm: PusTm) -> io::Result<Self> {
        let sec = tm.secondary_header;
        if sec.service_type != SERVICE_TYPE {
            return Err(invalid_data(format!(
                "TM({},{}) ist kein Verifikationsbericht (Service {SERVICE_TYPE})",
                sec.service_type, sec.message_subtype
            )));
        }
        if tm.source_data.len() < REQUEST_ID_LEN {
            return Err(invalid_data(format!(
                "TM(1,{}): Source Data ({} Byte) zu kurz für die Request ID",
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
            PusPacket::Tc(_) => Err(invalid_data("Telekommando ist kein Verifikationsbericht".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cuc::{CucFormat, CucTime};
    use crate::pus::PusCodec;
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
            VerificationKind::ProgressFailure { step_id: 4, failure: failure() },
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
    fn request_id_entspricht_den_ersten_4_header_bytes_des_tc() {
        let tc = PusTc::new(0x0AB, 0x1234, 17, 1, Bytes::new());
        let mut tc_bytes = BytesMut::new();
        PusCodec::default().encode(tc.clone().into(), &mut tc_bytes).unwrap();

        let mut id_bytes = BytesMut::new();
        RequestId::from(&tc).encode(&mut id_bytes).unwrap();

        assert_eq!(&id_bytes[..], &tc_bytes[..REQUEST_ID_LEN]);
        assert_eq!(RequestId::decode(&id_bytes).unwrap(), RequestId::from(&tc));
    }

    #[test]
    fn tm_1_1_erzeugt_die_exakten_erwarteten_bytes() {
        let report = VerificationReport::new(0x010, 5, time(), RequestId::from(&tc()), VerificationKind::AcceptanceSuccess);

        let mut buf = BytesMut::new();
        PusCodec::default().encode(report.try_into().unwrap(), &mut buf).unwrap();

        // Data Length = 7 (Sec. Header) + 7 (CUC) + 4 (Request ID) + 2 (CRC) - 1 = 19
        assert_eq!(&buf[..6], &[0x08, 0x10, 0xC0, 0x05, 0x00, 19]);
        assert_eq!(&buf[6..13], &[0x20, 1, 1, 0, 0, 0, 0]);
        assert_eq!(&buf[20..24], &[0x18, 0xAB, 0xC0, 0x01]);
        assert_eq!(buf.len(), 26);
    }

    #[test]
    fn source_data_layout_je_subtype() {
        let id = [0x18, 0xAB, 0xC0, 0x01];
        assert_eq!(&source_data(VerificationKind::CompletionSuccess)[..], &id);
        assert_eq!(&source_data(VerificationKind::ProgressSuccess { step_id: 0x0102 })[..], &[&id[..], &[1, 2]].concat()[..]);
        assert_eq!(
            &source_data(VerificationKind::StartFailure(FailureNotice::new(0xBEEF, &[9u8][..])))[..],
            &[&id[..], &[0xBE, 0xEF, 9]].concat()[..]
        );
        assert_eq!(
            &source_data(VerificationKind::ProgressFailure { step_id: 7, failure: FailureNotice::new(0x0A0B, Bytes::new()) })[..],
            &[&id[..], &[0, 7, 0x0A, 0x0B]].concat()[..]
        );
    }

    #[test]
    fn roundtrip_aller_berichte_ueber_pus_codec() {
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
            assert_eq!(CucTime::from_bytes(&decoded.time, CucFormat::default()).unwrap(), time());
        }
    }

    #[test]
    fn lehnt_ungueltige_pakete_ab() {
        let id = [0x18, 0xAB, 0xC0, 0x01];
        let tm = |service, subtype, data: &[u8]| PusTm::new(1, 1, service, subtype, time(), data.to_vec());

        for (service, subtype, data) in [
            (17, 1, &id[..]),              // falscher Service
            (1, 9, &id[..]),               // TM(1,9) gibt es nicht
            (1, 1, &id[..3]),              // Request ID zu kurz
            (1, 1, &[&id[..], &[0]].concat()[..]), // zusätzliche Byte
            (1, 2, &id[..]),               // Failure Code fehlt
            (1, 5, &[&id[..], &[0]].concat()[..]), // Step ID zu kurz
            (1, 6, &[&id[..], &[0, 1]].concat()[..]), // Failure Code fehlt
        ] {
            assert!(VerificationReport::try_from(tm(service, subtype, data)).is_err(), "TM({service},{subtype}) {data:?}");
        }
        assert!(VerificationReport::try_from(PusPacket::Tc(tc())).is_err());
    }

    #[test]
    fn lehnt_ungueltige_request_id_ab() {
        let mut request_id = RequestId::from(&tc());
        request_id.apid = APID_MAX + 1;
        let report = VerificationReport::new(1, 1, time(), request_id, VerificationKind::AcceptanceSuccess);
        assert!(PusPacket::try_from(report).is_err());
    }

    #[test]
    fn failure_hilfsmethoden() {
        for kind in all_kinds() {
            assert_eq!(kind.is_failure(), kind.subtype() % 2 == 0, "{kind:?}");
        }
    }
}
