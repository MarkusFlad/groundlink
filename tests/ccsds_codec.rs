//! Integration test for `SpacePacketCodec`: verifies its use with
//! `tokio_util::codec::Framed` on a real TCP connection, reading and
//! writing in both directions.

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use groundlink::{PacketType, SequenceFlags, SpacePacket, SpacePacketCodec, SpacePacketHeader};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

#[tokio::test]
async fn framed_sends_and_receives_space_packets_both_ways() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let client_task = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });

    let (server_stream, _peer_addr) = listener.accept().await.unwrap();
    let client_stream = client_task.await.unwrap();

    let mut server_framed = Framed::new(server_stream, SpacePacketCodec);
    let mut client_framed = Framed::new(client_stream, SpacePacketCodec);

    // Server -> Client
    let tm_packet = SpacePacket::new(PacketType::Telemetry, 42, 7, &b"hello client"[..]);
    server_framed.send(tm_packet.clone()).await.unwrap();

    let received = client_framed.next().await.unwrap().unwrap();
    assert_eq!(received, tm_packet);

    // Client -> server, with the secondary header flag and segmentation
    // info set, to check these fields over the wire as well.
    let tc_packet = SpacePacket {
        header: SpacePacketHeader {
            packet_type: PacketType::Telecommand,
            secondary_header_flag: true,
            apid: 100,
            sequence_flags: SequenceFlags::FirstSegment,
            sequence_count: 555,
        },
        data: Bytes::from_static(b"command"),
    };
    client_framed.send(tc_packet.clone()).await.unwrap();

    let received = server_framed.next().await.unwrap().unwrap();
    assert_eq!(received, tc_packet);
}

#[tokio::test]
async fn framed_handles_several_packets_sent_back_to_back() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let client_task = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });

    let (server_stream, _peer_addr) = listener.accept().await.unwrap();
    let client_stream = client_task.await.unwrap();

    let mut server_framed = Framed::new(server_stream, SpacePacketCodec);
    let mut client_framed = Framed::new(client_stream, SpacePacketCodec);

    let packets: Vec<SpacePacket> = (0..5)
        .map(|i| SpacePacket::new(PacketType::Telemetry, 1, i, format!("packet {i}").into_bytes()))
        .collect();

    for packet in &packets {
        server_framed.send(packet.clone()).await.unwrap();
    }

    for expected in &packets {
        let received = client_framed.next().await.unwrap().unwrap();
        assert_eq!(&received, expected);
    }
}
