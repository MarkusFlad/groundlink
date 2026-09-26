//! ECSS Packet Utilisation Standard (PUS-C, ECSS-E-ST-70-41C).
//!
//! PUS-Pakete sind CCSDS Space Packets (siehe [`crate::ccsds`]), deren
//! Packet Data Field einen standardisierten Secondary Header und
//! optional ein Packet Error Control Field (CRC-16) enthält:
//!
//! ```text
//! +------------------+----------------------+---------------------+-----------+
//! | Primary Header   | PUS Secondary Header | Application Data /  | PEC       |
//! | (CCSDS, 6 Byte)  | (TC: 5, TM: 7+Zeit)  | Source Data         | (2 Byte)  |
//! +------------------+----------------------+---------------------+-----------+
//! ```
//!
//! Rust kennt keine Vererbung; die "Basisklasse" Space Packet wird daher
//! per Komposition eingebunden: [`PusTc`] und [`PusTm`] enthalten einen
//! [`SpacePacketHeader`], lassen sich über [`PusPacket::to_space_packet`]
//! bzw. [`PusPacket::from_space_packet`] in ein [`SpacePacket`] und zurück
//! wandeln, und der [`PusCodec`] delegiert das Framing an den
//! [`SpacePacketCodec`].
//!
//! Missionsspezifische Teile werden über [`PusConfig`] festgelegt:
//! die Länge des Zeitstempels im TM Secondary Header und ob ein Packet
//! Error Control Field vorhanden ist. Optionale Spare-Felder im Secondary
//! Header werden nicht unterstützt.
//!
//! Typisierte Nachrichten einzelner PUS-Services liegen in Untermodulen,
//! z. B. [`service1`] und [`service17`].

use bytes::{Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

use crate::ccsds::{PacketType, SequenceFlags, SpacePacket, SpacePacketCodec, SpacePacketHeader};
use crate::cuc::{CucFormat, CucTime};

pub mod service1;
pub mod service17;

/// PUS Version Number für PUS-C (ECSS-E-ST-70-41C).
pub const PUS_VERSION: u8 = 2;
/// Länge des TC Secondary Headers in Byte.
pub const TC_SECONDARY_HEADER_LEN: usize = 5;
/// Länge des TM Secondary Headers in Byte *ohne* Zeitstempel.
pub const TM_SECONDARY_HEADER_LEN_WITHOUT_TIME: usize = 7;
/// Länge des Packet Error Control Field (CRC-16) in Byte.
pub const PEC_LEN: usize = 2;
/// Standardlänge des TM-Zeitstempels: CUC 4+2 mit P-Field, entspricht
/// [`CucFormat::default`].
pub const DEFAULT_TM_TIME_LEN: usize = 7;

/// Berechnet die von ECSS für das Packet Error Control Field
/// vorgeschriebene CRC-16 (CCITT: Polynom `0x1021`, Startwert `0xFFFF`,
/// ohne Reflexion und ohne abschließendes XOR).
pub fn crc16_ccitt(data: &[u8]) -> u16 {
    let mut crc: u16 = 0xFFFF;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 { (crc << 1) ^ 0x1021 } else { crc << 1 };
        }
    }
    crc
}

fn invalid_input(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg)
}

fn invalid_data(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

/// Acknowledgement Flags eines Telekommandos: welche Verifikationsberichte
/// (Service 1) der Empfänger erzeugen soll.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AckFlags {
    /// Bericht über erfolgreiche Annahme (Bit `0b0001`).
    pub acceptance: bool,
    /// Bericht über erfolgreichen Ausführungsstart (Bit `0b0010`).
    pub start: bool,
    /// Berichte über den Ausführungsfortschritt (Bit `0b0100`).
    pub progress: bool,
    /// Bericht über erfolgreichen Ausführungsabschluss (Bit `0b1000`).
    pub completion: bool,
}

impl AckFlags {
    /// Alle Verifikationsberichte angefordert.
    pub const ALL: AckFlags = AckFlags { acceptance: true, start: true, progress: true, completion: true };
    /// Keine Verifikationsberichte angefordert.
    pub const NONE: AckFlags = AckFlags { acceptance: false, start: false, progress: false, completion: false };

    fn from_bits(bits: u8) -> Self {
        AckFlags {
            acceptance: bits & 0b0001 != 0,
            start: bits & 0b0010 != 0,
            progress: bits & 0b0100 != 0,
            completion: bits & 0b1000 != 0,
        }
    }

    fn to_bits(self) -> u8 {
        (self.acceptance as u8)
            | (self.start as u8) << 1
            | (self.progress as u8) << 2
            | (self.completion as u8) << 3
    }
}

/// Secondary Header eines PUS-C-Telekommandos (5 Byte).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PusTcSecondaryHeader {
    pub ack_flags: AckFlags,
    /// Service Type (z. B. 17 = Test).
    pub service_type: u8,
    /// Message Subtype (z. B. 1 = Are-You-Alive-Request).
    pub message_subtype: u8,
    /// Source ID: Kennung der sendenden Applikation.
    pub source_id: u16,
}

/// Secondary Header eines PUS-C-Telemetriepakets (7 Byte + Zeitstempel).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PusTmSecondaryHeader {
    /// Spacecraft Time Reference Status, 4 Bit (`0..=15`).
    pub time_reference_status: u8,
    /// Service Type.
    pub service_type: u8,
    /// Message Subtype.
    pub message_subtype: u8,
    /// Message Type Counter: Zähler je Service Type/Subtype und Ziel.
    pub message_type_counter: u16,
    /// Destination ID: Kennung der empfangenden Applikation.
    pub destination_id: u16,
    /// Zeitstempel (missionsspezifisches Format, z. B. CUC oder CDS),
    /// roh. Die Länge muss [`PusConfig::tm_time_len`] entsprechen.
    pub time: Bytes,
}

impl PusTmSecondaryHeader {
    /// Interpretiert den Zeitstempel als CUC-Zeit im gegebenen Format.
    pub fn cuc_time(&self, format: CucFormat) -> io::Result<CucTime> {
        CucTime::from_bytes(&self.time, format)
    }
}

/// Ein PUS-C-Telekommando.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PusTc {
    /// Primary Header des zugrunde liegenden Space Packets.
    /// `packet_type` und `secondary_header_flag` werden beim Kodieren
    /// stets auf `Telecommand` bzw. `true` gesetzt.
    pub header: SpacePacketHeader,
    pub secondary_header: PusTcSecondaryHeader,
    /// Application Data (darf leer sein).
    pub app_data: Bytes,
}

impl PusTc {
    /// Erstellt ein unsegmentiertes Telekommando mit allen Acknowledgement
    /// Flags gesetzt und Source ID 0.
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

/// Ein PUS-C-Telemetriepaket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PusTm {
    /// Primary Header des zugrunde liegenden Space Packets.
    /// `packet_type` und `secondary_header_flag` werden beim Kodieren
    /// stets auf `Telemetry` bzw. `true` gesetzt.
    pub header: SpacePacketHeader,
    pub secondary_header: PusTmSecondaryHeader,
    /// Source Data (darf leer sein).
    pub source_data: Bytes,
}

impl PusTm {
    /// Erstellt ein unsegmentiertes Telemetriepaket mit Time Reference
    /// Status 0, Message Type Counter 0 und Destination ID 0.
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

/// Ein PUS-Paket: Telekommando oder Telemetrie. Welche Variante vorliegt,
/// bestimmt beim Dekodieren das Packet-Type-Bit des Primary Headers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PusPacket {
    Tc(PusTc),
    Tm(PusTm),
}

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
    /// Primary Header des zugrunde liegenden Space Packets.
    pub fn header(&self) -> &SpacePacketHeader {
        match self {
            PusPacket::Tc(tc) => &tc.header,
            PusPacket::Tm(tm) => &tm.header,
        }
    }

    /// Service Type aus dem Secondary Header.
    pub fn service_type(&self) -> u8 {
        match self {
            PusPacket::Tc(tc) => tc.secondary_header.service_type,
            PusPacket::Tm(tm) => tm.secondary_header.service_type,
        }
    }

    /// Message Subtype aus dem Secondary Header.
    pub fn message_subtype(&self) -> u8 {
        match self {
            PusPacket::Tc(tc) => tc.secondary_header.message_subtype,
            PusPacket::Tm(tm) => tm.secondary_header.message_subtype,
        }
    }

    /// Nutzdaten: Application Data (TC) bzw. Source Data (TM).
    pub fn user_data(&self) -> &Bytes {
        match self {
            PusPacket::Tc(tc) => &tc.app_data,
            PusPacket::Tm(tm) => &tm.source_data,
        }
    }

    /// Wandelt das PUS-Paket in das zugrunde liegende Space Packet um.
    /// Das Packet Data Field enthält Secondary Header, Nutzdaten und –
    /// falls in `config` aktiviert – das Packet Error Control Field.
    pub fn to_space_packet(&self, config: &PusConfig) -> io::Result<SpacePacket> {
        let mut data = BytesMut::new();
        let mut header = *self.header();
        header.secondary_header_flag = true;

        match self {
            PusPacket::Tc(tc) => {
                header.packet_type = PacketType::Telecommand;
                let sec = &tc.secondary_header;
                data.reserve(TC_SECONDARY_HEADER_LEN + tc.app_data.len() + PEC_LEN);
                data.extend_from_slice(&[PUS_VERSION << 4 | sec.ack_flags.to_bits(), sec.service_type, sec.message_subtype]);
                data.extend_from_slice(&sec.source_id.to_be_bytes());
                data.extend_from_slice(&tc.app_data);
            }
            PusPacket::Tm(tm) => {
                header.packet_type = PacketType::Telemetry;
                let sec = &tm.secondary_header;
                if sec.time_reference_status > 0x0F {
                    return Err(invalid_input(format!(
                        "time_reference_status {} überschreitet den 4-Bit-Wertebereich",
                        sec.time_reference_status
                    )));
                }
                if sec.time.len() != config.tm_time_len {
                    return Err(invalid_input(format!(
                        "Zeitstempel hat {} Byte, erwartet werden {} Byte",
                        sec.time.len(),
                        config.tm_time_len
                    )));
                }
                data.reserve(TM_SECONDARY_HEADER_LEN_WITHOUT_TIME + sec.time.len() + tm.source_data.len() + PEC_LEN);
                data.extend_from_slice(&[PUS_VERSION << 4 | sec.time_reference_status, sec.service_type, sec.message_subtype]);
                data.extend_from_slice(&sec.message_type_counter.to_be_bytes());
                data.extend_from_slice(&sec.destination_id.to_be_bytes());
                data.extend_from_slice(&sec.time);
                data.extend_from_slice(&tm.source_data);
            }
        }

        if config.packet_error_control {
            // Die CRC deckt das gesamte Paket inkl. Primary Header ab.
            let mut crc_input = BytesMut::with_capacity(crate::ccsds::PRIMARY_HEADER_LEN + data.len() + PEC_LEN);
            header.encode(data.len() + PEC_LEN, &mut crc_input)?;
            crc_input.extend_from_slice(&data);
            data.extend_from_slice(&crc16_ccitt(&crc_input).to_be_bytes());
        }

        Ok(SpacePacket { header, data: data.freeze() })
    }

    /// Interpretiert ein Space Packet als PUS-Paket. Prüft Secondary Header
    /// Flag, PUS-Version, Mindestlänge und – falls in `config` aktiviert –
    /// die CRC des Packet Error Control Field.
    pub fn from_space_packet(packet: SpacePacket, config: &PusConfig) -> io::Result<Self> {
        let SpacePacket { header, mut data } = packet;

        if !header.secondary_header_flag {
            return Err(invalid_data("Space Packet ohne Secondary Header ist kein PUS-Paket".into()));
        }

        if config.packet_error_control {
            if data.len() < PEC_LEN {
                return Err(invalid_data("Packet Data Field zu kurz für das Packet Error Control Field".into()));
            }
            let mut crc_input = BytesMut::with_capacity(crate::ccsds::PRIMARY_HEADER_LEN + data.len());
            header.encode(data.len(), &mut crc_input)?;
            crc_input.extend_from_slice(&data);
            // Die CRC über Daten + angehängte CRC ergibt 0, wenn sie stimmt.
            if crc16_ccitt(&crc_input) != 0 {
                return Err(invalid_data("CRC-Fehler im Packet Error Control Field".into()));
            }
            data.truncate(data.len() - PEC_LEN);
        }

        let min_len = match header.packet_type {
            PacketType::Telecommand => TC_SECONDARY_HEADER_LEN,
            PacketType::Telemetry => TM_SECONDARY_HEADER_LEN_WITHOUT_TIME + config.tm_time_len,
        };
        if data.len() < min_len {
            return Err(invalid_data(format!(
                "Packet Data Field ({} Byte) zu kurz für den PUS Secondary Header ({min_len} Byte)",
                data.len()
            )));
        }

        let version = data[0] >> 4;
        if version != PUS_VERSION {
            return Err(invalid_data(format!(
                "PUS-Version {version} wird nicht unterstützt (erwartet {PUS_VERSION}, PUS-C)"
            )));
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

/// Missionsspezifische Parameter des PUS-Formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PusConfig {
    /// Länge des Zeitstempels im TM Secondary Header in Byte.
    pub tm_time_len: usize,
    /// Ob TC- und TM-Pakete ein Packet Error Control Field (CRC-16)
    /// enthalten.
    pub packet_error_control: bool,
}

impl Default for PusConfig {
    /// [`DEFAULT_TM_TIME_LEN`] Byte Zeitstempel, mit Packet Error Control.
    fn default() -> Self {
        PusConfig { tm_time_len: DEFAULT_TM_TIME_LEN, packet_error_control: true }
    }
}

/// Codec für PUS-C-Pakete, nutzbar mit [`tokio_util::codec::Framed`] und
/// den generischen TCP-Actors (siehe [`crate::PusListener`] & Co.). Das
/// Framing übernimmt der [`SpacePacketCodec`]; dieser Codec wandelt nur
/// zwischen [`SpacePacket`] und [`PusPacket`] um.
///
/// Die TCP-Actors erzeugen ihren Codec per [`Default`], verwenden also
/// [`PusConfig::default`]. Für andere Parameter kann der Codec über
/// [`PusCodec::new`] direkt mit `Framed` genutzt werden.
#[derive(Debug, Clone, Copy, Default)]
pub struct PusCodec {
    config: PusConfig,
    inner: SpacePacketCodec,
}

impl PusCodec {
    pub fn new(config: PusConfig) -> Self {
        PusCodec { config, inner: SpacePacketCodec }
    }

    pub fn config(&self) -> &PusConfig {
        &self.config
    }
}

impl Decoder for PusCodec {
    type Item = PusPacket;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        match self.inner.decode(src)? {
            Some(space_packet) => PusPacket::from_space_packet(space_packet, &self.config).map(Some),
            None => Ok(None),
        }
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
    fn default_zeitstempellaenge_passt_zum_cuc_standardformat() {
        assert_eq!(DEFAULT_TM_TIME_LEN, CucFormat::default().len());
    }

    #[test]
    fn tm_mit_cuc_zeitstempel_roundtrip() {
        let time: CucTime = "2026-09-26T12:00:00.5Z".parse().unwrap();
        let original = PusPacket::Tm(PusTm::new(1, 0, 17, 2, time, Bytes::new()));

        let mut codec = PusCodec::default();
        let mut buf = encode(&mut codec, original);
        let Some(PusPacket::Tm(tm)) = codec.decode(&mut buf).unwrap() else { panic!("TM erwartet") };

        assert_eq!(tm.secondary_header.cuc_time(CucFormat::default()).unwrap(), time);
    }

    #[test]
    fn crc16_ccitt_referenzwert() {
        assert_eq!(crc16_ccitt(b"123456789"), 0x29B1);
    }

    #[test]
    fn tc_encode_erzeugt_die_exakten_erwarteten_bytes() {
        let mut tc = PusTc::new(0x0AB, 1, 17, 1, &b"hi"[..]);
        tc.secondary_header.source_id = 0x1234;
        let mut codec = PusCodec::default();
        let buf = encode(&mut codec, tc.into());

        // Primary Header: Typ TC + Secondary Header Flag -> 0x18AB,
        // Data Length = 5 (Sec. Header) + 2 (Daten) + 2 (CRC) - 1 = 8
        assert_eq!(&buf[..6], &[0x18, 0xAB, 0xC0, 0x01, 0x00, 0x08]);
        // Secondary Header: Version 2 | Ack 0b1111, Service 17, Subtype 1, Source ID
        assert_eq!(&buf[6..11], &[0x2F, 17, 1, 0x12, 0x34]);
        assert_eq!(&buf[11..13], b"hi");
        let crc = crc16_ccitt(&buf[..13]);
        assert_eq!(&buf[13..], &crc.to_be_bytes());
    }

    #[test]
    fn tc_roundtrip() {
        let mut tc = PusTc::new(42, 7, 8, 1, &b"kommando"[..]);
        tc.secondary_header.ack_flags = AckFlags { acceptance: true, completion: true, ..AckFlags::NONE };
        let original = PusPacket::Tc(tc);

        let mut codec = PusCodec::default();
        let mut buf = encode(&mut codec, original.clone());
        let decoded = codec.decode(&mut buf).unwrap().expect("vollständig");

        assert_eq!(decoded, original);
        assert!(buf.is_empty());
    }

    #[test]
    fn tm_roundtrip_mit_eigener_konfiguration() {
        let config = PusConfig { tm_time_len: 4, packet_error_control: false };
        let mut tm = PusTm::new(3, 99, 3, 25, &[1u8, 2, 3, 4][..], &b"housekeeping"[..]);
        tm.secondary_header.time_reference_status = 0x5;
        tm.secondary_header.message_type_counter = 0xBEEF;
        tm.secondary_header.destination_id = 0x0102;
        let original = PusPacket::Tm(tm);

        let mut codec = PusCodec::new(config);
        let mut buf = encode(&mut codec, original.clone());
        assert_eq!(buf.len(), 6 + 7 + 4 + 12, "ohne PEC kein CRC-Anhang");

        let decoded = codec.decode(&mut buf).unwrap().expect("vollständig");
        assert_eq!(decoded, original);
    }

    #[test]
    fn tm_ohne_nutzdaten_ist_gueltig() {
        let original = PusPacket::Tm(PusTm::new(1, 0, 17, 2, vec![0u8; DEFAULT_TM_TIME_LEN], Bytes::new()));
        let mut codec = PusCodec::default();
        let mut buf = encode(&mut codec, original.clone());
        assert_eq!(codec.decode(&mut buf).unwrap(), Some(original));
    }

    #[test]
    fn decode_erkennt_crc_fehler() {
        let mut codec = PusCodec::default();
        let mut buf = encode(&mut codec, PusTc::new(1, 1, 17, 1, &b"x"[..]).into());
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;

        let err = codec.decode(&mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn decode_wartet_auf_vollstaendiges_paket() {
        let mut codec = PusCodec::default();
        let full = encode(&mut codec, PusTc::new(1, 1, 17, 1, &b"1234"[..]).into());

        let mut partial = BytesMut::from(&full[..full.len() - 1]);
        assert_eq!(codec.decode(&mut partial).unwrap(), None);
        partial.extend_from_slice(&full[full.len() - 1..]);
        assert!(codec.decode(&mut partial).unwrap().is_some());
    }

    #[test]
    fn decode_lehnt_space_packet_ohne_secondary_header_ab() {
        let mut buf = BytesMut::new();
        SpacePacketCodec
            .encode(SpacePacket::new(PacketType::Telecommand, 1, 1, &b"0123456789"[..]), &mut buf)
            .unwrap();
        assert!(PusCodec::default().decode(&mut buf).is_err());
    }

    #[test]
    fn decode_lehnt_falsche_pus_version_ab() {
        let config = PusConfig { packet_error_control: false, ..PusConfig::default() };
        let mut space_packet = PusPacket::from(PusTc::new(1, 1, 17, 1, Bytes::new()))
            .to_space_packet(&config)
            .unwrap();
        let mut data = BytesMut::from(&space_packet.data[..]);
        data[0] = 0x1F; // PUS-A
        space_packet.data = data.freeze();

        let err = PusPacket::from_space_packet(space_packet, &config).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn encode_lehnt_falsche_zeitstempellaenge_ab() {
        let tm = PusTm::new(1, 1, 3, 25, &[0u8; 3][..], Bytes::new());
        let mut buf = BytesMut::new();
        assert!(PusCodec::default().encode(tm.into(), &mut buf).is_err());
    }
}
