//! Shows that the same generic TCP actors (`TcpServerActor<M, C>`,
//! `TcpReaderActor<M, C>`, `TcpWriterActor<M, C>`) that are used with
//! `SimpleString`/`SimpleStringCodec` also work unchanged for CCSDS Space
//! Packets, here through the aliases `SpacePacketServer` and
//! `SpacePacketWriter` (`TcpServerActor<SpacePacket, SpacePacketCodec>`
//! and `TcpWriterActor<SpacePacket, SpacePacketCodec>`).

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use kameo::actor::{Recipient, Spawn};
use groundlink::{
    ConnectionHalfClosed, GetLocalAddr, PacketType, SpacePacket, SpacePacketCodec,
    SpacePacketServer, SpacePacketWriter, TcpServerArgs, TcpWriterArgs, TestActor,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

#[tokio::test]
async fn space_packets_are_forwarded_to_downstream_actor() {
    let test_actor_ref = TestActor::<SpacePacket>::spawn(TestActor::new());
    let downstream: Recipient<SpacePacket> = test_actor_ref.clone().recipient::<SpacePacket>();

    let listener_ref = SpacePacketServer::spawn(TcpServerArgs::new(
        "127.0.0.1:0".parse().unwrap(),
        downstream,
        SpacePacketCodec::default(),
    ));
    let local_addr = listener_ref.ask(GetLocalAddr).await.unwrap();

    let client_stream = TcpStream::connect(local_addr).await.unwrap();
    let mut client_framed = Framed::new(client_stream, SpacePacketCodec);

    let packet_a = SpacePacket::new(PacketType::Telemetry, 42, 1, &b"hello server"[..]);
    let packet_b = SpacePacket::new(PacketType::Telecommand, 100, 2, &b"do something"[..]);

    client_framed.send(packet_a.clone()).await.unwrap();
    client_framed.send(packet_b.clone()).await.unwrap();

    let received = TestActor::assert_received(&test_actor_ref, 2, Duration::from_secs(1)).await;

    assert_eq!(received, vec![packet_a, packet_b]);
}

#[tokio::test]
async fn space_packet_writer_writes_packet_readable_with_framed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let client_task = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });

    let (server_stream, peer_addr) = listener.accept().await.unwrap();
    let client_stream = client_task.await.unwrap();

    let (_server_read, server_write) = server_stream.into_split();
    let mut client_framed = Framed::new(client_stream, SpacePacketCodec);

    let listener_ref = TestActor::<ConnectionHalfClosed>::spawn(TestActor::new());
    let listener_recipient = listener_ref.clone().recipient::<ConnectionHalfClosed>();

    let writer_ref = SpacePacketWriter::spawn(TcpWriterArgs {
        write_half: server_write,
        peer_addr,
        listener: listener_recipient,
        codec: SpacePacketCodec::default(),
    });

    let packet = SpacePacket::new(PacketType::Telemetry, 7, 99, &b"status ok"[..]);
    writer_ref.tell(packet.clone()).await.unwrap();

    let received = client_framed.next().await.unwrap().unwrap();
    assert_eq!(received, packet);
}

#[tokio::test]
async fn data_with_wrong_packet_version_closes_the_connection() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let test_actor_ref = TestActor::<SpacePacket>::spawn(TestActor::new());
    let listener_ref = SpacePacketServer::spawn(TcpServerArgs::new(
        "127.0.0.1:0".parse().unwrap(),
        test_actor_ref.clone().recipient::<SpacePacket>(),
        SpacePacketCodec::default(),
    ));
    let local_addr = listener_ref.ask(GetLocalAddr).await.unwrap();
    let mut stream = TcpStream::connect(local_addr).await.unwrap();

    // Not a Space Packet: the version bits of the first byte are 0b111.
    stream.write_all(b"\xFFgarbage").await.unwrap();

    // The server closes the connection instead of waiting for more data.
    let mut buf = [0u8; 16];
    let read = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut buf))
        .await
        .expect("connection should be closed");
    assert!(matches!(read, Ok(0) | Err(_)), "expected EOF or reset, got {read:?}");
    assert!(test_actor_ref.ask(groundlink::GetMessages::new()).await.unwrap().is_empty());
}
