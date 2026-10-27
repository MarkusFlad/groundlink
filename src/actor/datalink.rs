//! Actors between Space Packets and the SLE services: they put packets into
//! CLTUs and TM Transfer Frames and take them out again.
//!
//! ```text
//! ground                                                     spacecraft (simulated)
//! SpacePacket/PusPacket ─▶ TcFrameEncoder ─Cltu─▶ SleUserActor ═FCLTU═▶ FcltuServiceInstance ─Cltu─▶ TcFrameDecoder ─SpacePacket─▶
//! ◀─SpacePacket─ TmFrameDecoder ◀─RafFrame─ SleUserActor ◀═RAF═ RafServiceInstance ◀─RafFrame─ TmFrameEncoder ◀─SpacePacket/PusPacket─
//! ```
//!
//! - [`TcFrameEncoder`]: packet → TC Transfer Frame → CLTU, numbered for
//!   CLTU-TRANSFER-DATA.
//! - [`TcFrameDecoder`]: CLTU → TC Transfer Frame → packets.
//! - [`TmFrameEncoder`]: packets → TM Transfer Frames → [`RafFrame`]s, as
//!   the ground station would receive them.
//! - [`TmFrameDecoder`]: [`RafFrame`]s → TM Transfer Frames → packets.
//!
//! The encoders take [`SpacePacket`]s and, converted with their
//! [`PusConfig`], [`PusPacket`]s. Each sends one CLTU per packet or one
//! frame as soon as a packet is complete; there is no COP-1 (frames are
//! sent and accepted as type-BD) and no segmentation of packets that do not
//! fit into one TC frame. All handle batches and pass the results of a
//! batch on as one batch.

use std::collections::HashMap;

use bytes::{BufMut, Bytes, BytesMut};
use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use tokio_util::codec::{Decoder, Encoder};
use tracing::warn;

use crate::actor::batch::Downstream;
use crate::actor::sle::Cltu;
use crate::impl_batch_message;
use crate::protocol::ccsds::{SpacePacket, SpacePacketCodec};
use crate::protocol::datalink::cltu;
use crate::protocol::datalink::tc::{TcFrameConfig, TcTransferFrame};
use crate::protocol::datalink::tm::{TmFrameConfig, TmFramePacker, TmPacketExtractor, TmTransferFrame};
use crate::protocol::pus::{PusConfig, PusPacket};
use crate::protocol::sle::common::{ConditionalTime, Time};
use crate::protocol::sle::fcltu::CltuId;
use crate::protocol::sle::raf::{AntennaId, FrameQuality, RafFrame};

/// Segment header sequence flags for an unsegmented packet.
const UNSEGMENTED: u8 = 0b11 << 6;

/// Actor that puts each packet into a TC Transfer Frame and the frame into
/// a [`Cltu`] (ground side).
///
/// The frames are type-BD frames of the virtual channel of its
/// [`TcFrameConfig`], numbered with a frame sequence number from 0; the
/// CLTUs are numbered from `first_cltu_id`, which is the identification
/// announced with CLTU-START. Send the CLTUs to a
/// [`SleUserActor`](crate::SleUserActor), which forwards them with
/// CLTU-TRANSFER-DATA. Packets that do not fit into one frame are dropped
/// with a warning.
pub struct TcFrameEncoder {
    config: TcFrameConfig,
    pus_config: PusConfig,
    sequence_number: u8,
    next_cltu_id: CltuId,
    downstream: Downstream<Cltu>,
}

impl TcFrameEncoder {
    /// An encoder that sends the CLTUs, numbered from `first_cltu_id`, to
    /// `downstream`.
    pub fn new(config: TcFrameConfig, first_cltu_id: CltuId, downstream: impl Into<Downstream<Cltu>>) -> Self {
        TcFrameEncoder {
            config,
            pus_config: PusConfig::default(),
            sequence_number: 0,
            next_cltu_id: first_cltu_id,
            downstream: downstream.into(),
        }
    }

    /// Converts [`PusPacket`]s with `pus_config` instead of
    /// [`PusConfig::default`].
    pub fn with_pus_config(mut self, pus_config: PusConfig) -> Self {
        self.pus_config = pus_config;
        self
    }

    async fn send(&mut self, packet: SpacePacket) {
        let mut data = BytesMut::new();
        if let Some(map_id) = self.config.map_id {
            data.put_u8(UNSEGMENTED | (map_id & 0x3f));
        }
        if let Err(err) = SpacePacketCodec.encode(packet, &mut data) {
            warn!("TcFrameEncoder: packet dropped: {err}");
            return;
        }
        let frame = TcTransferFrame::new(
            self.config.spacecraft_id,
            self.config.virtual_channel_id,
            self.sequence_number,
            data.freeze(),
        );
        let encoded = match frame.encode(self.config.fecf) {
            Ok(encoded) => encoded,
            Err(err) => {
                warn!("TcFrameEncoder: packet dropped, no TC frame for it: {err}");
                return;
            }
        };
        self.sequence_number = self.sequence_number.wrapping_add(1);
        let cltu = Cltu {
            id: self.next_cltu_id,
            earliest_transmission_time: ConditionalTime::Undefined,
            latest_transmission_time: ConditionalTime::Undefined,
            delay_time_us: 0,
            data: cltu::encode(&encoded),
        };
        self.next_cltu_id = self.next_cltu_id.wrapping_add(1);
        if let Err(err) = self.downstream.send(cltu).await {
            warn!("TcFrameEncoder: could not pass CLTU on: {err}");
        }
    }
}

impl Actor for TcFrameEncoder {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<SpacePacket> for TcFrameEncoder {
    type Reply = ();

    async fn handle(&mut self, packet: SpacePacket, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.send(packet).await;
    }
}

impl Message<PusPacket> for TcFrameEncoder {
    type Reply = ();

    async fn handle(&mut self, packet: PusPacket, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        match packet.to_space_packet(&self.pus_config) {
            Ok(packet) => self.send(packet).await,
            Err(err) => warn!("TcFrameEncoder: PUS packet dropped: {err}"),
        }
    }
}

impl_batch_message!(TcFrameEncoder, SpacePacket, outputs = [downstream]);
impl_batch_message!(TcFrameEncoder, PusPacket, outputs = [downstream]);

/// Actor that decodes each [`Cltu`] to a TC Transfer Frame and passes the
/// packets in it on (spacecraft side).
///
/// Accepts the frames of the spacecraft and virtual channel of its
/// [`TcFrameConfig`], type-AD and type-BD alike (there is no FARM), and
/// drops all others with a warning, as well as CLTUs and frames that cannot
/// be decoded. Control command frames are dropped.
pub struct TcFrameDecoder {
    config: TcFrameConfig,
    downstream: Downstream<SpacePacket>,
}

impl TcFrameDecoder {
    /// A decoder that sends the packets to `downstream`.
    pub fn new(config: TcFrameConfig, downstream: impl Into<Downstream<SpacePacket>>) -> Self {
        TcFrameDecoder {
            config,
            downstream: downstream.into(),
        }
    }

    fn packets(&self, cltu: &Cltu) -> Result<Vec<SpacePacket>, String> {
        let data = cltu::decode(&cltu.data).map_err(|err| err.to_string())?;
        let frame = TcTransferFrame::decode(&data, self.config.fecf).map_err(|err| err.to_string())?;
        if frame.spacecraft_id != self.config.spacecraft_id
            || frame.virtual_channel_id != self.config.virtual_channel_id
        {
            return Err(format!(
                "frame of spacecraft {} virtual channel {}",
                frame.spacecraft_id, frame.virtual_channel_id
            ));
        }
        if frame.control_command {
            return Err("control command frame".to_string());
        }
        let mut data = BytesMut::from(&frame.data[..]);
        if let Some(map_id) = self.config.map_id {
            if data.is_empty() || data[0] != UNSEGMENTED | (map_id & 0x3f) {
                return Err("segment header of another MAP or of a segment".to_string());
            }
            let _ = data.split_to(1);
        }
        let mut packets = Vec::new();
        while !data.is_empty() {
            match SpacePacketCodec.decode(&mut data) {
                Ok(Some(packet)) => packets.push(packet),
                Ok(None) => return Err("frame ends inside a packet".to_string()),
                Err(err) => return Err(err.to_string()),
            }
        }
        Ok(packets)
    }
}

impl Actor for TcFrameDecoder {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<Cltu> for TcFrameDecoder {
    type Reply = ();

    async fn handle(&mut self, cltu: Cltu, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        match self.packets(&cltu) {
            Ok(packets) => {
                if let Err(err) = self.downstream.send_all(packets).await {
                    warn!("TcFrameDecoder: could not pass packets on: {err}");
                }
            }
            Err(reason) => warn!("TcFrameDecoder: CLTU {} dropped: {reason}", cltu.id),
        }
    }
}

impl_batch_message!(TcFrameDecoder, Cltu, outputs = [downstream]);

/// Actor that packs packets into TM Transfer Frames and sends each frame as
/// a [`RafFrame`] (spacecraft and ground station side, simulated).
///
/// Packets are packed back to back and may span frames. After each packet
/// (or each batch of packets), the frame being filled is completed with an
/// idle packet and sent, so nothing waits for more packets. The
/// [`RafFrame`]s carry the current time as earth receive time, the antenna
/// of [`with_antenna`](Self::with_antenna) (default `ANT1`) and quality
/// good. Send them to a [`RafServiceInstance`](crate::RafServiceInstance).
pub struct TmFrameEncoder {
    packer: TmFramePacker,
    pus_config: PusConfig,
    antenna_id: AntennaId,
    downstream: Downstream<RafFrame>,
    /// Whether a batch is being handled; the frame is completed after it.
    in_batch: bool,
}

impl TmFrameEncoder {
    /// An encoder for frames with `config` that sends them to
    /// `downstream`.
    ///
    /// # Panics
    ///
    /// Panics if the frames of `config` are too short for a packet.
    pub fn new(config: TmFrameConfig, downstream: impl Into<Downstream<RafFrame>>) -> Self {
        TmFrameEncoder {
            packer: TmFramePacker::new(config),
            pus_config: PusConfig::default(),
            antenna_id: AntennaId::Local(Bytes::from_static(b"ANT1")),
            downstream: downstream.into(),
            in_batch: false,
        }
    }

    /// Converts [`PusPacket`]s with `pus_config` instead of
    /// [`PusConfig::default`].
    pub fn with_pus_config(mut self, pus_config: PusConfig) -> Self {
        self.pus_config = pus_config;
        self
    }

    /// Reports `antenna_id` as the receiving antenna.
    pub fn with_antenna(mut self, antenna_id: AntennaId) -> Self {
        self.antenna_id = antenna_id;
        self
    }

    async fn send_frames(&mut self, frames: Vec<TmTransferFrame>) {
        let fecf = self.packer.config().fecf;
        let mut raf_frames = Vec::with_capacity(frames.len());
        for frame in frames {
            match frame.encode(fecf) {
                Ok(data) => raf_frames.push(RafFrame {
                    earth_receive_time: Time::now(),
                    antenna_id: self.antenna_id.clone(),
                    data_link_continuity: 0,
                    quality: FrameQuality::Good,
                    private_annotation: None,
                    data,
                }),
                Err(err) => warn!("TmFrameEncoder: frame dropped: {err}"),
            }
        }
        if let Err(err) = self.downstream.send_all(raf_frames).await {
            warn!("TmFrameEncoder: could not pass frames on: {err}");
        }
    }

    async fn push(&mut self, packet: SpacePacket) {
        let mut frames = match self.packer.push(&packet) {
            Ok(frames) => frames,
            Err(err) => {
                warn!("TmFrameEncoder: packet dropped: {err}");
                return;
            }
        };
        if !self.in_batch {
            frames.extend(self.packer.flush());
        }
        self.send_frames(frames).await;
    }
}

impl Actor for TmFrameEncoder {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<SpacePacket> for TmFrameEncoder {
    type Reply = ();

    async fn handle(&mut self, packet: SpacePacket, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.push(packet).await;
    }
}

impl Message<PusPacket> for TmFrameEncoder {
    type Reply = ();

    async fn handle(&mut self, packet: PusPacket, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        match packet.to_space_packet(&self.pus_config) {
            Ok(packet) => self.push(packet).await,
            Err(err) => warn!("TmFrameEncoder: PUS packet dropped: {err}"),
        }
    }
}

/// The packets of a batch are packed together; the last frame is completed
/// after the batch.
macro_rules! tm_encoder_batch {
    ($message:ty) => {
        impl Message<crate::Batch<$message>> for TmFrameEncoder {
            type Reply = ();

            async fn handle(
                &mut self,
                batch: crate::Batch<$message>,
                ctx: &mut Context<Self, Self::Reply>,
            ) -> Self::Reply {
                self.downstream.hold();
                self.in_batch = true;
                for message in batch {
                    <Self as Message<$message>>::handle(self, message, ctx).await;
                }
                self.in_batch = false;
                let frames = self.packer.flush();
                self.send_frames(frames).await;
                self.downstream.release().await;
            }
        }
    };
}

tm_encoder_batch!(SpacePacket);
tm_encoder_batch!(PusPacket);

/// Actor that decodes the TM Transfer Frames of [`RafFrame`]s and passes
/// the packets in them on (ground side).
///
/// Takes the frames of the spacecraft of its [`TmFrameConfig`], on every
/// virtual channel, and extracts the packets per virtual channel, also
/// packets that span frames; idle packets and idle frames are dropped.
/// Frames of the wrong length, of other spacecraft or with a frame error
/// control field that does not match are dropped with a warning.
pub struct TmFrameDecoder {
    config: TmFrameConfig,
    extractors: HashMap<u8, TmPacketExtractor>,
    downstream: Downstream<SpacePacket>,
}

impl TmFrameDecoder {
    /// A decoder for frames with `config` that sends the packets to
    /// `downstream`.
    pub fn new(config: TmFrameConfig, downstream: impl Into<Downstream<SpacePacket>>) -> Self {
        TmFrameDecoder {
            config,
            extractors: HashMap::new(),
            downstream: downstream.into(),
        }
    }
}

impl Actor for TmFrameDecoder {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<RafFrame> for TmFrameDecoder {
    type Reply = ();

    async fn handle(&mut self, raf_frame: RafFrame, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if raf_frame.data.len() != self.config.frame_len {
            warn!("TmFrameDecoder: frame of {} octets dropped", raf_frame.data.len());
            return;
        }
        let frame = match TmTransferFrame::decode(&raf_frame.data, self.config.fecf) {
            Ok(frame) => frame,
            Err(err) => {
                warn!("TmFrameDecoder: frame dropped: {err}");
                return;
            }
        };
        if frame.spacecraft_id != self.config.spacecraft_id {
            warn!("TmFrameDecoder: frame of spacecraft {} dropped", frame.spacecraft_id);
            return;
        }
        let packets = self
            .extractors
            .entry(frame.virtual_channel_id)
            .or_default()
            .push(&frame);
        if let Err(err) = self.downstream.send_all(packets).await {
            warn!("TmFrameDecoder: could not pass packets on: {err}");
        }
    }
}

impl_batch_message!(TmFrameDecoder, RafFrame, outputs = [downstream]);
