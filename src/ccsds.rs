//! CCSDS Space Packet Protocol (CCSDS 133.0-B-2).
//!
//! Enthält den Space-Packet-Typ (Primary Header + Packet Data Field) sowie
//! einen [`SpacePacketCodec`], der sowohl [`Decoder`] als auch
//! [`Encoder<SpacePacket>`] implementiert und sich damit direkt mit
//! [`tokio_util::codec::Framed`] verwenden lässt (gleichzeitiges Lesen und
//! Schreiben auf einem `AsyncRead + AsyncWrite`-Stream, z. B. einem
//! `TcpStream`).
//!
//! Der 6 Byte lange Primary Header ist vollständig durch den Standard
//! festgelegt. Das Packet Data Field (Nutzdaten) kann laut Standard einen
//! missionsspezifischen Secondary Header enthalten – dessen Format ist
//! nicht Teil von CCSDS 133.0 und wird hier bewusst nicht interpretiert;
//! [`SpacePacket::data`] enthält daher das komplette Packet Data Field als
//! Rohdaten.

use bytes::{Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// Größe des Primary Headers in Byte (CCSDS 133.0-B-2, Abschnitt 4.1.2).
pub const PRIMARY_HEADER_LEN: usize = 6;
/// Maximaler Wert für [`SpacePacketHeader::apid`] (11 Bit).
pub const APID_MAX: u16 = 0x07FF;
/// Maximaler Wert für [`SpacePacketHeader::sequence_count`] (14 Bit).
pub const SEQUENCE_COUNT_MAX: u16 = 0x3FFF;
/// Maximale Länge des Packet Data Field in Byte. Das 16-Bit-Längenfeld im
/// Header kodiert `Länge - 1`, daher ist `u16::MAX + 1` erreichbar.
pub const MAX_PACKET_DATA_LEN: usize = u16::MAX as usize + 1;

/// CCSDS "Packet Type" Feld (CCSDS 133.0-B-2, Abschnitt 4.1.3.3):
/// unterscheidet Telemetrie- von Telekommando-Paketen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    /// Telemetrie (TM), Bitwert 0.
    Telemetry,
    /// Telekommando (TC), Bitwert 1.
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

/// CCSDS "Sequence Flags" Feld (CCSDS 133.0-B-2, Abschnitt 4.1.3.4):
/// beschreibt, ob ein Paket Teil einer segmentierten Folge von Paketen mit
/// gemeinsamem `sequence_count` ist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceFlags {
    /// Fortsetzungssegment (Bitwert `0b00`).
    Continuation,
    /// Erstes Segment (Bitwert `0b01`).
    FirstSegment,
    /// Letztes Segment (Bitwert `0b10`).
    LastSegment,
    /// Unsegmentiertes, eigenständiges Paket (Bitwert `0b11`) – der
    /// Regelfall, wenn keine Segmentierung verwendet wird.
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

/// Der 6 Byte lange Primary Header eines CCSDS Space Packets (CCSDS
/// 133.0-B-2, Abschnitt 4.1). Die Packet Version Number ist laut Standard
/// aktuell immer `0b000` und wird daher nicht als Feld geführt, sondern
/// beim Kodieren fest auf 0 gesetzt bzw. beim Dekodieren ignoriert.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpacePacketHeader {
    /// Packet Type: Telemetrie oder Telekommando.
    pub packet_type: PacketType,
    /// Ob im Packet Data Field ein (hier nicht interpretierter)
    /// Secondary Header vorangestellt ist.
    pub secondary_header_flag: bool,
    /// Application Process ID, 11 Bit (`0..=`[`APID_MAX`]).
    pub apid: u16,
    /// Segmentierungsinformation.
    pub sequence_flags: SequenceFlags,
    /// Packet Sequence Count bzw. Packet Name, 14 Bit
    /// (`0..=`[`SEQUENCE_COUNT_MAX`]).
    pub sequence_count: u16,
}

impl SpacePacketHeader {
    /// Kodiert den Header (6 Byte, Big-Endian) für ein Packet Data Field
    /// der Länge `data_len` an `dst` an.
    pub(crate) fn encode(&self, data_len: usize, dst: &mut BytesMut) -> io::Result<()> {
        if self.apid > APID_MAX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("APID {} überschreitet den 11-Bit-Wertebereich (max {APID_MAX})", self.apid),
            ));
        }
        if self.sequence_count > SEQUENCE_COUNT_MAX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "sequence_count {} überschreitet den 14-Bit-Wertebereich (max {SEQUENCE_COUNT_MAX})",
                    self.sequence_count
                ),
            ));
        }
        if data_len == 0 || data_len > MAX_PACKET_DATA_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Packet-Data-Field-Länge {data_len} außerhalb des gültigen Bereichs (1..={MAX_PACKET_DATA_LEN})"
                ),
            ));
        }

        // Version Number (3 Bit, immer 0) | Type (1 Bit) |
        // Secondary Header Flag (1 Bit) | APID (11 Bit)
        let word0: u16 = ((self.packet_type.to_bit() as u16) << 12)
            | ((self.secondary_header_flag as u16) << 11)
            | (self.apid & APID_MAX);
        // Sequence Flags (2 Bit) | Sequence Count (14 Bit)
        let word1: u16 =
            ((self.sequence_flags.to_bits() as u16) << 14) | (self.sequence_count & SEQUENCE_COUNT_MAX);
        // Packet Data Length = tatsächliche Länge - 1
        let word2: u16 = (data_len - 1) as u16;

        dst.extend_from_slice(&word0.to_be_bytes());
        dst.extend_from_slice(&word1.to_be_bytes());
        dst.extend_from_slice(&word2.to_be_bytes());
        Ok(())
    }

    /// Dekodiert einen 6 Byte langen Header und liefert zusätzlich die
    /// daraus abgeleitete Länge des nachfolgenden Packet Data Field.
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

/// Ein vollständiges CCSDS Space Packet: Primary Header + Packet Data
/// Field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpacePacket {
    pub header: SpacePacketHeader,
    /// Das komplette Packet Data Field (ggf. inkl. missionsspezifischem
    /// Secondary Header + eigentlichen Nutzdaten), unverändert/roh.
    pub data: Bytes,
}

impl SpacePacket {
    /// Erstellt ein neues, unsegmentiertes Space Packet (ohne Secondary
    /// Header) mit den gegebenen Nutzdaten.
    ///
    /// `data` darf nicht leer sein (das Packet Data Field muss laut
    /// Standard mindestens 1 Oktett umfassen) und maximal
    /// [`MAX_PACKET_DATA_LEN`] Byte lang sein – dies wird erst beim
    /// tatsächlichen Kodieren (z. B. über [`SpacePacketCodec`]) geprüft.
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

/// Codec für CCSDS Space Packets, nutzbar mit
/// [`tokio_util::codec::Framed`] für gleichzeitiges Lesen und Schreiben auf
/// einem `AsyncRead + AsyncWrite`-Stream:
///
/// ```no_run
/// # use futures::StreamExt;
/// # use kameo_tcp_example::SpacePacketCodec;
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
#[derive(Debug, Clone, Copy, Default)]
pub struct SpacePacketCodec;

impl Decoder for SpacePacketCodec {
    type Item = SpacePacket;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        if src.len() < PRIMARY_HEADER_LEN {
            return Ok(None);
        }

        // Header nur "vorab lesen" (nicht aus dem Puffer entfernen), bis
        // auch das vollständige Packet Data Field eingetroffen ist.
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
    fn encode_erzeugt_die_exakten_erwarteten_bytes() {
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
        assert_eq!(
            &buf[..],
            &[0x10, 0xAB, 0xC0, 0x01, 0x00, 0x01, b'h', b'i']
        );
    }

    #[test]
    fn encode_decode_roundtrip() {
        let original = SpacePacket::new(PacketType::Telemetry, APID_MAX, SEQUENCE_COUNT_MAX, &b"hallo raumschiff"[..]);

        let mut codec = SpacePacketCodec;
        let mut buf = BytesMut::new();
        codec.encode(original.clone(), &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().expect("sollte vollständig dekodierbar sein");

        assert_eq!(decoded, original);
        assert!(buf.is_empty(), "Puffer sollte nach vollständigem Dekodieren leer sein");
    }

    #[test]
    fn decode_wartet_auf_vollstaendigen_header() {
        let mut codec = SpacePacketCodec;
        let mut buf = BytesMut::from(&[0x10, 0xAB, 0xC0][..]); // nur 3 von 6 Header-Byte

        assert_eq!(codec.decode(&mut buf).unwrap(), None);
        assert_eq!(buf.len(), 3, "unvollständige Bytes dürfen nicht konsumiert werden");
    }

    #[test]
    fn decode_wartet_auf_vollstaendiges_data_field() {
        let packet = SpacePacket::new(PacketType::Telemetry, 1, 1, &b"1234567890"[..]);

        let mut codec = SpacePacketCodec;
        let mut full = BytesMut::new();
        codec.encode(packet.clone(), &mut full).unwrap();

        // Nur Header + halbe Nutzdaten simulieren.
        let mut partial = BytesMut::from(&full[..PRIMARY_HEADER_LEN + 5]);
        assert_eq!(codec.decode(&mut partial).unwrap(), None);
        assert_eq!(
            partial.len(),
            PRIMARY_HEADER_LEN + 5,
            "unvollständiges Data Field darf nicht konsumiert werden"
        );

        // Restliche Bytes "nachliefern".
        partial.extend_from_slice(&full[PRIMARY_HEADER_LEN + 5..]);
        let decoded = codec.decode(&mut partial).unwrap().expect("jetzt vollständig");
        assert_eq!(decoded, packet);
        assert!(partial.is_empty());
    }

    #[test]
    fn encode_lehnt_leeres_data_field_ab() {
        let packet = SpacePacket::new(PacketType::Telemetry, 1, 1, Bytes::new());
        let mut codec = SpacePacketCodec;
        let mut buf = BytesMut::new();
        assert!(codec.encode(packet, &mut buf).is_err());
    }

    #[test]
    fn encode_lehnt_zu_grossen_apid_ab() {
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
