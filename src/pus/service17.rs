//! PUS Service 17 "Test" (ECSS-E-ST-70-41C, Abschnitt 6.17).
//!
//! Enthält typisierte Nachrichten, die sich verlustfrei in die
//! generischen [`PusTc`]/[`PusPacket`]-Typen und zurück wandeln lassen,
//! sodass sie mit dem [`crate::PusCodec`] und den PUS-Actors übertragen
//! werden können.

use bytes::Bytes;
use std::io;

use super::{AckFlags, PusPacket, PusTc, PusTcSecondaryHeader, PusTm, PusTmSecondaryHeader};
use crate::ccsds::{PacketType, SequenceFlags, SpacePacketHeader};

/// Service Type des Test-Service.
pub const SERVICE_TYPE: u8 = 17;
/// Message Subtype von TC(17,1) "Are-You-Alive Connection Test".
pub const ARE_YOU_ALIVE_REQUEST_SUBTYPE: u8 = 1;
/// Message Subtype von TM(17,2) "Are-You-Alive Connection Report".
pub const ARE_YOU_ALIVE_REPORT_SUBTYPE: u8 = 2;

/// TC(17,1) "Perform an Are-You-Alive Connection Test": fordert den
/// Empfänger auf, mit TM(17,2) zu antworten. Das Telekommando trägt keine
/// Application Data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AreYouAliveRequest {
    /// Primary Header des zugrunde liegenden Space Packets.
    /// `packet_type` und `secondary_header_flag` werden beim Kodieren
    /// stets auf `Telecommand` bzw. `true` gesetzt.
    pub header: SpacePacketHeader,
    pub ack_flags: AckFlags,
    /// Source ID: Kennung der sendenden Applikation.
    pub source_id: u16,
}

impl AreYouAliveRequest {
    /// Erstellt ein unsegmentiertes TC(17,1) mit allen Acknowledgement
    /// Flags gesetzt und Source ID 0.
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

    /// Schlägt fehl, wenn das Telekommando nicht TC(17,1) ist oder
    /// Application Data enthält.
    fn try_from(tc: PusTc) -> io::Result<Self> {
        let sec = &tc.secondary_header;
        if (sec.service_type, sec.message_subtype) != (SERVICE_TYPE, ARE_YOU_ALIVE_REQUEST_SUBTYPE) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "TC({},{}) ist kein Are-You-Alive-Request TC({SERVICE_TYPE},{ARE_YOU_ALIVE_REQUEST_SUBTYPE})",
                    sec.service_type, sec.message_subtype
                ),
            ));
        }
        if !tc.app_data.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("TC(17,1) darf keine Application Data enthalten ({} Byte empfangen)", tc.app_data.len()),
            ));
        }
        Ok(AreYouAliveRequest { header: tc.header, ack_flags: sec.ack_flags, source_id: sec.source_id })
    }
}

impl TryFrom<PusPacket> for AreYouAliveRequest {
    type Error = io::Error;

    fn try_from(packet: PusPacket) -> io::Result<Self> {
        match packet {
            PusPacket::Tc(tc) => tc.try_into(),
            PusPacket::Tm(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Telemetriepaket ist kein Are-You-Alive-Request",
            )),
        }
    }
}

/// TM(17,2) "Are-You-Alive Connection Report": Antwort auf TC(17,1).
/// Der Bericht trägt keine Source Data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AreYouAliveReport {
    /// Primary Header des zugrunde liegenden Space Packets.
    /// `packet_type` und `secondary_header_flag` werden beim Kodieren
    /// stets auf `Telemetry` bzw. `true` gesetzt.
    pub header: SpacePacketHeader,
    /// Spacecraft Time Reference Status, 4 Bit.
    pub time_reference_status: u8,
    pub message_type_counter: u16,
    /// Destination ID: Empfänger des Berichts, üblicherweise die Source ID
    /// des TC(17,1).
    pub destination_id: u16,
    /// Zeitstempel, roh (z. B. eine [`crate::CucTime`]).
    pub time: Bytes,
}

impl AreYouAliveReport {
    /// Erstellt einen unsegmentierten Bericht mit Time Reference Status 0,
    /// Message Type Counter 0 und Destination ID 0.
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

    /// Erstellt die Antwort auf ein TC(17,1): Destination ID = dessen
    /// Source ID.
    pub fn for_request(
        apid: u16,
        sequence_count: u16,
        time: impl Into<Bytes>,
        request: &AreYouAliveRequest,
    ) -> Self {
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

    /// Schlägt fehl, wenn das Paket nicht TM(17,2) ist oder Source Data
    /// enthält.
    fn try_from(tm: PusTm) -> io::Result<Self> {
        let sec = tm.secondary_header;
        if (sec.service_type, sec.message_subtype) != (SERVICE_TYPE, ARE_YOU_ALIVE_REPORT_SUBTYPE) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "TM({},{}) ist kein Are-You-Alive-Report TM({SERVICE_TYPE},{ARE_YOU_ALIVE_REPORT_SUBTYPE})",
                    sec.service_type, sec.message_subtype
                ),
            ));
        }
        if !tm.source_data.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("TM(17,2) darf keine Source Data enthalten ({} Byte empfangen)", tm.source_data.len()),
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
                "Telekommando ist kein Are-You-Alive-Report",
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
    fn encode_erzeugt_die_exakten_erwarteten_bytes() {
        let mut request = AreYouAliveRequest::new(0x0AB, 1);
        request.source_id = 0x1234;

        let mut buf = BytesMut::new();
        PusCodec::default().encode(request.into(), &mut buf).unwrap();

        // Data Length = 5 (Sec. Header) + 0 (Daten) + 2 (CRC) - 1 = 6
        assert_eq!(&buf[..6], &[0x18, 0xAB, 0xC0, 0x01, 0x00, 0x06]);
        assert_eq!(&buf[6..11], &[0x2F, 17, 1, 0x12, 0x34]);
        assert_eq!(buf.len(), 13, "nur noch die CRC folgt");
    }

    #[test]
    fn roundtrip_ueber_pus_codec() {
        let mut original = AreYouAliveRequest::new(42, 7);
        original.ack_flags = AckFlags { acceptance: true, ..AckFlags::NONE };
        original.source_id = 5;

        let mut codec = PusCodec::default();
        let mut buf = BytesMut::new();
        codec.encode(original.clone().into(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().expect("vollständig");

        assert_eq!(AreYouAliveRequest::try_from(decoded).unwrap(), original);
    }

    #[test]
    fn lehnt_anderen_service_ab() {
        let tc = PusTc::new(1, 1, 17, 3, Bytes::new());
        assert!(AreYouAliveRequest::try_from(tc).is_err());
    }

    #[test]
    fn lehnt_application_data_ab() {
        let tc = PusTc::new(1, 1, 17, 1, &b"x"[..]);
        assert!(AreYouAliveRequest::try_from(tc).is_err());
    }

    #[test]
    fn lehnt_telemetrie_ab() {
        let tm = PusTm::new(1, 1, 17, 1, vec![0u8; 7], Bytes::new());
        assert!(AreYouAliveRequest::try_from(PusPacket::Tm(tm)).is_err());
    }

    fn time() -> CucTime {
        "2026-09-26T12:00:00Z".parse().unwrap()
    }

    #[test]
    fn report_encode_erzeugt_die_exakten_erwarteten_bytes() {
        let mut report = AreYouAliveReport::new(0x0AB, 2, time());
        report.message_type_counter = 0x0102;
        report.destination_id = 0x1234;

        let mut buf = BytesMut::new();
        PusCodec::default().encode(report.into(), &mut buf).unwrap();

        // Data Length = 7 (Sec. Header) + 7 (CUC) + 0 (Daten) + 2 (CRC) - 1 = 15
        assert_eq!(&buf[..6], &[0x08, 0xAB, 0xC0, 0x02, 0x00, 15]);
        assert_eq!(&buf[6..13], &[0x20, 17, 2, 0x01, 0x02, 0x12, 0x34]);
        assert_eq!(buf.len(), 22, "nur noch Zeitstempel und CRC folgen");
    }

    #[test]
    fn report_roundtrip_ueber_pus_codec() {
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
        assert_eq!(CucTime::from_bytes(&decoded.time, CucFormat::default()).unwrap(), time());
    }

    #[test]
    fn report_lehnt_ungueltige_pakete_ab() {
        let tm = |subtype, data: &[u8]| PusTm::new(1, 1, 17, subtype, time(), data.to_vec());
        assert!(AreYouAliveReport::try_from(tm(1, &[])).is_err());
        assert!(AreYouAliveReport::try_from(tm(2, &[0])).is_err());
        assert!(AreYouAliveReport::try_from(PusTm::new(1, 1, 1, 2, time(), Bytes::new())).is_err());
        let tc = PusTc::new(1, 1, 17, 2, Bytes::new());
        assert!(AreYouAliveReport::try_from(PusPacket::Tc(tc)).is_err());
    }
}
