//! TM Transfer Frames, CCSDS 132.0-B-3, and Space Packets in them.
//!
//! ```text
//! | version (2) | spacecraft ID (10) | virtual channel ID (3) | OCF flag (1) |
//! | master channel frame count (8) | virtual channel frame count (8) |
//! | secondary header (1) | sync (1) | packet order (1) | segment length ID (2) | first header pointer (11) |
//! | data field | operational control field (32, if OCF flag) | frame error control field (16, optional) |
//! ```
//!
//! All frames of a mission have the same length. The Space Packets are
//! packed into the data fields back to back and may span frames; the first
//! header pointer gives the offset of the first packet that starts in a
//! frame. [`TmFramePacker`] packs packets into frames, completing a frame
//! with an idle packet when there is nothing more to send, and
//! [`TmPacketExtractor`] takes them out again.

use bytes::{BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};
use tracing::warn;

use super::FrameError;
use crate::protocol::ccsds::{PacketType, SpacePacket, SpacePacketCodec};
use crate::protocol::pus::crc16_ccitt;

/// Length of the primary header in octets.
pub const PRIMARY_HEADER_LEN: usize = 6;
/// Length of the operational control field in octets.
pub const OCF_LEN: usize = 4;
/// Length of the frame error control field in octets.
pub const FECF_LEN: usize = 2;
/// First header pointer: no packet starts in this frame.
pub const FHP_NO_PACKET_START: u16 = 0x7ff;
/// First header pointer: the frame carries idle data only.
pub const FHP_IDLE_DATA: u16 = 0x7fe;
/// The APID of idle packets.
pub const IDLE_APID: u16 = 0x7ff;

const SPACE_PACKET_HEADER_LEN: usize = 6;
/// The shortest Space Packet: header and one octet of data.
const MIN_PACKET_LEN: usize = SPACE_PACKET_HEADER_LEN + 1;

/// The parameters of a TM virtual channel that sender and receiver agree
/// on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct TmFrameConfig {
    /// The spacecraft identifier, 10 bits.
    pub spacecraft_id: u16,
    /// The virtual channel identifier, 3 bits.
    pub virtual_channel_id: u8,
    /// The length of every frame in octets.
    pub frame_len: usize,
    /// Whether the frames carry a frame error control field.
    pub fecf: bool,
}

impl TmFrameConfig {
    /// Frames of `spacecraft_id` on `virtual_channel_id`, 1115 octets long
    /// (the length for Reed-Solomon interleaving depth 5), with frame
    /// error control field.
    pub fn new(spacecraft_id: u16, virtual_channel_id: u8) -> Self {
        TmFrameConfig {
            spacecraft_id,
            virtual_channel_id,
            frame_len: 1115,
            fecf: true,
        }
    }

    /// The length of the data field of a frame without operational control
    /// field.
    pub fn data_field_len(&self) -> usize {
        self.frame_len - PRIMARY_HEADER_LEN - if self.fecf { FECF_LEN } else { 0 }
    }

    /// Uses frames of `frame_len` octets.
    pub fn with_frame_len(mut self, frame_len: usize) -> Self {
        self.frame_len = frame_len;
        self
    }

    /// Sets whether the frames carry a frame error control field.
    pub fn with_fecf(mut self, fecf: bool) -> Self {
        self.fecf = fecf;
        self
    }
}

/// A TM Transfer Frame without secondary header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TmTransferFrame {
    /// The spacecraft identifier, 10 bits.
    pub spacecraft_id: u16,
    /// The virtual channel identifier, 3 bits.
    pub virtual_channel_id: u8,
    /// Counts all frames of the spacecraft, modulo 256.
    pub master_channel_frame_count: u8,
    /// Counts the frames of the virtual channel, modulo 256.
    pub virtual_channel_frame_count: u8,
    /// Offset of the first packet header in the data field, or
    /// [`FHP_NO_PACKET_START`] or [`FHP_IDLE_DATA`].
    pub first_header_pointer: u16,
    /// The operational control field (e.g. the CLCW), if present.
    pub ocf: Option<[u8; 4]>,
    /// The data field.
    pub data: Bytes,
}

impl TmTransferFrame {
    /// Encodes the frame, with a frame error control field if `fecf`.
    pub fn encode(&self, fecf: bool) -> Result<Bytes, FrameError> {
        if self.spacecraft_id > 0x3ff {
            return Err(FrameError::InvalidField("spacecraft ID"));
        }
        if self.virtual_channel_id > 7 {
            return Err(FrameError::InvalidField("virtual channel ID"));
        }
        if self.first_header_pointer > 0x7ff {
            return Err(FrameError::InvalidField("first header pointer"));
        }
        let len = PRIMARY_HEADER_LEN
            + self.data.len()
            + if self.ocf.is_some() { OCF_LEN } else { 0 }
            + if fecf { FECF_LEN } else { 0 };
        let mut frame = BytesMut::with_capacity(len);
        frame.put_u16(
            (self.spacecraft_id << 4) | (u16::from(self.virtual_channel_id) << 1) | u16::from(self.ocf.is_some()),
        );
        frame.put_u8(self.master_channel_frame_count);
        frame.put_u8(self.virtual_channel_frame_count);
        // No secondary header, sync flag 0, packet order 0, segment length
        // ID 11.
        frame.put_u16((0b11 << 11) | self.first_header_pointer);
        frame.put_slice(&self.data);
        if let Some(ocf) = &self.ocf {
            frame.put_slice(ocf);
        }
        if fecf {
            let crc = crc16_ccitt(&frame);
            frame.put_u16(crc);
        }
        Ok(frame.freeze())
    }

    /// Decodes a frame. Checks the frame error control field if `fecf`.
    pub fn decode(data: &[u8], fecf: bool) -> Result<Self, FrameError> {
        let trailer = if fecf { FECF_LEN } else { 0 };
        if data.len() <= PRIMARY_HEADER_LEN + trailer {
            return Err(FrameError::Truncated);
        }
        let word0 = u16::from_be_bytes([data[0], data[1]]);
        let version = (word0 >> 14) as u8;
        if version != 0 {
            return Err(FrameError::InvalidVersion(version));
        }
        let len = data.len();
        if fecf && crc16_ccitt(&data[..len - FECF_LEN]) != u16::from_be_bytes([data[len - 2], data[len - 1]]) {
            return Err(FrameError::CrcMismatch);
        }
        let has_ocf = word0 & 1 != 0;
        let data_end = len - trailer - if has_ocf { OCF_LEN } else { 0 };
        if data_end < PRIMARY_HEADER_LEN {
            return Err(FrameError::Truncated);
        }
        let ocf = has_ocf.then(|| {
            let mut ocf = [0; 4];
            ocf.copy_from_slice(&data[data_end..data_end + OCF_LEN]);
            ocf
        });
        Ok(TmTransferFrame {
            spacecraft_id: (word0 >> 4) & 0x3ff,
            virtual_channel_id: ((word0 >> 1) & 0x7) as u8,
            master_channel_frame_count: data[2],
            virtual_channel_frame_count: data[3],
            first_header_pointer: u16::from_be_bytes([data[4], data[5]]) & 0x7ff,
            ocf,
            data: Bytes::copy_from_slice(&data[PRIMARY_HEADER_LEN..data_end]),
        })
    }
}

/// Packs Space Packets into the TM Transfer Frames of one virtual channel.
///
/// ```
/// use groundlink::protocol::datalink::tm::{TmFrameConfig, TmFramePacker};
/// use groundlink::protocol::ccsds::PacketType;
/// use groundlink::SpacePacket;
///
/// let mut packer = TmFramePacker::new(TmFrameConfig::new(0x2a, 0));
/// let mut frames = packer.push(&SpacePacket::new(PacketType::Telemetry, 0x42, 0, vec![1, 2, 3])).unwrap();
/// assert!(frames.is_empty(), "the packet waits for more");
/// frames.extend(packer.flush());
/// assert_eq!(frames.len(), 1);
/// assert_eq!(frames[0].first_header_pointer, 0);
/// ```
#[derive(Debug)]
pub struct TmFramePacker {
    config: TmFrameConfig,
    master_channel_frame_count: u8,
    virtual_channel_frame_count: u8,
    /// The data field of the frame being filled.
    data: BytesMut,
    first_header_pointer: Option<u16>,
}

impl TmFramePacker {
    /// A packer for frames with `config`.
    ///
    /// # Panics
    ///
    /// Panics if the frames of `config` have no room for a packet header.
    pub fn new(config: TmFrameConfig) -> Self {
        assert!(
            config.data_field_len() >= MIN_PACKET_LEN,
            "TM frames of {} octets are too short",
            config.frame_len
        );
        TmFramePacker {
            config,
            master_channel_frame_count: 0,
            virtual_channel_frame_count: 0,
            data: BytesMut::with_capacity(config.data_field_len()),
            first_header_pointer: None,
        }
    }

    /// The frame configuration.
    pub fn config(&self) -> &TmFrameConfig {
        &self.config
    }

    /// Appends `packet`; returns the frames it has filled.
    pub fn push(&mut self, packet: &SpacePacket) -> std::io::Result<Vec<TmTransferFrame>> {
        let mut bytes = BytesMut::new();
        SpacePacketCodec.encode(packet.clone(), &mut bytes)?;
        Ok(self.push_bytes(&bytes))
    }

    fn push_bytes(&mut self, mut packet: &[u8]) -> Vec<TmTransferFrame> {
        let data_field_len = self.config.data_field_len();
        let mut frames = Vec::new();
        if self.first_header_pointer.is_none() {
            self.first_header_pointer = Some(self.data.len() as u16);
        }
        while !packet.is_empty() {
            let n = (data_field_len - self.data.len()).min(packet.len());
            self.data.put_slice(&packet[..n]);
            packet = &packet[n..];
            if self.data.len() == data_field_len {
                frames.push(self.complete());
            }
        }
        frames
    }

    /// Completes the frame being filled, if any, with an idle packet;
    /// returns the frames this has filled (one, or two if the room left
    /// was too small for an idle packet).
    pub fn flush(&mut self) -> Vec<TmTransferFrame> {
        if self.data.is_empty() {
            return Vec::new();
        }
        let room = self.config.data_field_len() - self.data.len();
        let len = if room >= MIN_PACKET_LEN {
            room
        } else {
            room + self.config.data_field_len()
        };
        let mut idle = BytesMut::with_capacity(len);
        let idle_packet = SpacePacket::new(
            PacketType::Telemetry,
            IDLE_APID,
            0,
            vec![0x55; len - SPACE_PACKET_HEADER_LEN],
        );
        SpacePacketCodec
            .encode(idle_packet, &mut idle)
            .expect("idle packets are valid");
        self.push_bytes(&idle)
    }

    fn complete(&mut self) -> TmTransferFrame {
        let frame = TmTransferFrame {
            spacecraft_id: self.config.spacecraft_id,
            virtual_channel_id: self.config.virtual_channel_id,
            master_channel_frame_count: self.master_channel_frame_count,
            virtual_channel_frame_count: self.virtual_channel_frame_count,
            first_header_pointer: self.first_header_pointer.take().unwrap_or(FHP_NO_PACKET_START),
            ocf: None,
            data: self.data.split().freeze(),
        };
        self.master_channel_frame_count = self.master_channel_frame_count.wrapping_add(1);
        self.virtual_channel_frame_count = self.virtual_channel_frame_count.wrapping_add(1);
        frame
    }
}

/// Takes the Space Packets out of the TM Transfer Frames of one virtual
/// channel, also packets that span frames. Drops idle packets.
///
/// After a gap in the virtual channel frame count, or at the start, it
/// waits for a frame in which a packet starts and continues there; the
/// packet cut by the gap is lost.
#[derive(Debug, Default)]
pub struct TmPacketExtractor {
    /// The start of a packet that continues in the next frame.
    partial: BytesMut,
    /// Whether `partial` is in step with the frames.
    synced: bool,
    last_count: Option<u8>,
}

impl TmPacketExtractor {
    /// An extractor that waits for the first packet start.
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes the packets that `frame` completes.
    pub fn push(&mut self, frame: &TmTransferFrame) -> Vec<SpacePacket> {
        let count = frame.virtual_channel_frame_count;
        if let Some(last) = self.last_count
            && count != last.wrapping_add(1)
        {
            warn!(
                "TM virtual channel {}: frames lost before count {count}",
                frame.virtual_channel_id
            );
            self.resync();
        }
        self.last_count = Some(count);

        let fhp = frame.first_header_pointer;
        let mut packets = Vec::new();
        if fhp == FHP_IDLE_DATA {
            return packets;
        }
        if fhp == FHP_NO_PACKET_START {
            if self.synced {
                self.partial.put_slice(&frame.data);
                self.take_packets(&mut packets);
            }
            return packets;
        }
        let start = usize::from(fhp);
        if start >= frame.data.len() {
            warn!(
                "TM virtual channel {}: first header pointer {fhp} out of range",
                frame.virtual_channel_id
            );
            self.resync();
            return packets;
        }
        if self.synced {
            self.partial.put_slice(&frame.data[..start]);
            self.take_packets(&mut packets);
            if !self.partial.is_empty() {
                warn!(
                    "TM virtual channel {}: packet does not end at the first header pointer",
                    frame.virtual_channel_id
                );
            }
        }
        self.partial.clear();
        self.synced = true;
        self.partial.put_slice(&frame.data[start..]);
        self.take_packets(&mut packets);
        packets
    }

    fn resync(&mut self) {
        self.partial.clear();
        self.synced = false;
    }

    fn take_packets(&mut self, packets: &mut Vec<SpacePacket>) {
        loop {
            match SpacePacketCodec.decode(&mut self.partial) {
                Ok(Some(packet)) => {
                    if packet.header.apid != IDLE_APID {
                        packets.push(packet);
                    }
                }
                Ok(None) => return,
                Err(err) => {
                    warn!("TM packet extraction out of step: {err}");
                    self.resync();
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> TmFrameConfig {
        TmFrameConfig {
            frame_len: 64,
            ..TmFrameConfig::new(0x2a, 1)
        }
    }

    fn packet(n: u16, len: usize) -> SpacePacket {
        SpacePacket::new(PacketType::Telemetry, 0x42, n, vec![n as u8; len])
    }

    #[test]
    fn wire_format() {
        let frame = TmTransferFrame {
            spacecraft_id: 0x2a,
            virtual_channel_id: 1,
            master_channel_frame_count: 5,
            virtual_channel_frame_count: 3,
            first_header_pointer: 0,
            ocf: Some([1, 2, 3, 4]),
            data: Bytes::from_static(&[0xaa; 8]),
        };
        let encoded = frame.encode(true).unwrap();
        assert_eq!(&encoded[..6], &[0x02, 0xa3, 0x05, 0x03, 0x18, 0x00]);
        assert_eq!(encoded.len(), 6 + 8 + 4 + 2);
        assert_eq!(crc16_ccitt(&encoded), 0);
        assert_eq!(TmTransferFrame::decode(&encoded, true).unwrap(), frame);

        let mut corrupted = encoded.to_vec();
        corrupted[7] ^= 0x80;
        assert_eq!(TmTransferFrame::decode(&corrupted, true), Err(FrameError::CrcMismatch));
    }

    #[test]
    fn packets_span_frames_and_come_out_again() {
        let mut packer = TmFramePacker::new(config());
        let packets = [packet(1, 10), packet(2, 100), packet(3, 1), packet(4, 40)];
        let mut frames = Vec::new();
        for p in &packets {
            frames.extend(packer.push(p).unwrap());
        }
        frames.extend(packer.flush());
        for frame in &frames {
            assert_eq!(frame.encode(true).unwrap().len(), 64);
        }
        // 175 octets of packets in data fields of 56 octets.
        assert_eq!(frames.len(), 4);
        assert_eq!(frames[0].first_header_pointer, 0);
        assert_eq!(frames[1].first_header_pointer, FHP_NO_PACKET_START);
        assert_eq!(frames[2].first_header_pointer, 16 + 106 - 112);

        let mut extractor = TmPacketExtractor::new();
        let got: Vec<_> = frames.iter().flat_map(|f| extractor.push(f)).collect();
        assert_eq!(got, packets);
    }

    #[test]
    fn a_small_rest_is_filled_with_an_idle_packet_into_the_next_frame() {
        let mut packer = TmFramePacker::new(config());
        // 52 of 56 octets used: 4 octets left, too few for an idle packet.
        assert!(packer.push(&packet(1, 46)).unwrap().is_empty());
        let frames = packer.flush();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[1].first_header_pointer, FHP_NO_PACKET_START);
        let mut extractor = TmPacketExtractor::new();
        let got: Vec<_> = frames.iter().flat_map(|f| extractor.push(f)).collect();
        assert_eq!(got, [packet(1, 46)]);
    }

    #[test]
    fn the_extractor_resyncs_after_a_lost_frame() {
        let mut packer = TmFramePacker::new(config());
        let mut frames = Vec::new();
        for n in 1..=6 {
            frames.extend(packer.push(&packet(n, 30)).unwrap());
        }
        frames.extend(packer.flush());
        let mut extractor = TmPacketExtractor::new();
        // 36-octet packets in 56-octet data fields. Frame 1 is lost, and
        // with it packet 2 (cut), 3 and 4 (both start in frame 1).
        let got: Vec<_> = [&frames[0], &frames[2], &frames[3]]
            .into_iter()
            .flat_map(|f| extractor.push(f))
            .collect();
        let ids: Vec<u16> = got.iter().map(|p| p.header.sequence_count).collect();
        assert_eq!(ids, [1, 5, 6]);
    }
}
