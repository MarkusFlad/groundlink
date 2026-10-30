//! Tests for the data link actors, encoder straight to decoder.

use std::time::Duration;

use bytes::Bytes;
use groundlink::{
    Batch, Cltu, PacketType, RafFrame, SpacePacket, TcFrameConfig, TcFrameDecoder, TcFrameEncoder, TestActor,
    TmFrameConfig, TmFrameDecoder, TmFrameEncoder,
};
use kameo::actor::Spawn;

const TIMEOUT: Duration = Duration::from_secs(5);

fn packet(packet_type: PacketType, n: u16, len: usize) -> SpacePacket {
    SpacePacket::new(packet_type, 0x42, n, vec![n as u8; len])
}

#[tokio::test]
async fn tc_packets_go_through_cltus_with_a_segment_header() {
    let mut config = TcFrameConfig::new(0x2a, 1);
    config.map_id = Some(3);
    let cltus = TestActor::<Cltu>::spawn(TestActor::new());
    let encoder = TcFrameEncoder::spawn(TcFrameEncoder::new(config, 100, cltus.clone().recipient::<Cltu>()));
    for n in 0..3 {
        encoder.tell(packet(PacketType::Telecommand, n, 50)).await.unwrap();
    }
    let cltus = TestActor::assert_received(&cltus, 3, TIMEOUT).await;
    assert_eq!(cltus.iter().map(|c| c.id).collect::<Vec<_>>(), [100, 101, 102]);

    let packets = TestActor::<SpacePacket>::spawn(TestActor::new());
    let decoder = TcFrameDecoder::spawn(TcFrameDecoder::new(config, packets.clone().recipient::<SpacePacket>()));
    // A CLTU for another spacecraft is dropped.
    let other = TestActor::<Cltu>::spawn(TestActor::new());
    let mut other_config = config;
    other_config.spacecraft_id = 0x2b;
    let other_encoder = TcFrameEncoder::spawn(TcFrameEncoder::new(other_config, 0, other.clone().recipient::<Cltu>()));
    other_encoder.tell(packet(PacketType::Telecommand, 9, 5)).await.unwrap();
    let foreign = TestActor::assert_received(&other, 1, TIMEOUT).await;

    decoder.tell(foreign[0].clone()).await.unwrap();
    decoder.tell(Batch::from(cltus)).await.unwrap();
    let received = TestActor::assert_received(&packets, 3, TIMEOUT).await;
    assert_eq!(
        received,
        (0..3)
            .map(|n| packet(PacketType::Telecommand, n, 50))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_corrupted_cltu_is_dropped() {
    let config = TcFrameConfig::new(0x2a, 0);
    let cltus = TestActor::<Cltu>::spawn(TestActor::new());
    let encoder = TcFrameEncoder::spawn(TcFrameEncoder::new(config, 0, cltus.clone().recipient::<Cltu>()));
    encoder.tell(packet(PacketType::Telecommand, 1, 20)).await.unwrap();
    encoder.tell(packet(PacketType::Telecommand, 2, 20)).await.unwrap();
    let mut cltus = TestActor::assert_received(&cltus, 2, TIMEOUT).await;
    let mut data = cltus[0].data.to_vec();
    data[2 + 8 + 1] ^= 0x04;
    cltus[0].data = Bytes::from(data);

    let packets = TestActor::<SpacePacket>::spawn(TestActor::new());
    let decoder = TcFrameDecoder::spawn(TcFrameDecoder::new(config, packets.clone().recipient::<SpacePacket>()));
    decoder.tell(Batch::from(cltus)).await.unwrap();
    let received = TestActor::assert_received(&packets, 1, TIMEOUT).await;
    assert_eq!(received, [packet(PacketType::Telecommand, 2, 20)]);
}

#[tokio::test]
async fn tm_packets_span_frames_and_batches_are_packed_together() {
    let config = TmFrameConfig::new(0x2a, 2);
    let frames = TestActor::<RafFrame>::spawn(TestActor::new());
    let encoder = TmFrameEncoder::spawn(TmFrameEncoder::new(config, frames.clone().recipient::<RafFrame>()));

    // 3000 octets need 3 frames of 1107 data octets.
    encoder.tell(packet(PacketType::Telemetry, 1, 2994)).await.unwrap();
    let after_big = TestActor::assert_received(&frames, 3, TIMEOUT).await.len();
    // Ten small packets in a batch share one frame.
    let small: Vec<_> = (2..12).map(|n| packet(PacketType::Telemetry, n, 20)).collect();
    encoder.tell(Batch::from(small.clone())).await.unwrap();
    let all = TestActor::assert_received(&frames, after_big + 1, TIMEOUT).await;
    assert_eq!((after_big, all.len()), (3, 4));
    assert!(all.iter().all(|f| f.data.len() == 1115));

    let packets = TestActor::<SpacePacket>::spawn(TestActor::new());
    let decoder = TmFrameDecoder::spawn(TmFrameDecoder::new(
        config,
        packets.clone().recipient::<Batch<SpacePacket>>(),
    ));
    decoder.tell(Batch::from(all)).await.unwrap();
    let received = TestActor::assert_received(&packets, 11, TIMEOUT).await;
    assert_eq!(received[0], packet(PacketType::Telemetry, 1, 2994));
    assert_eq!(&received[1..], &small[..]);
}
