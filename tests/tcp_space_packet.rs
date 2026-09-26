//! Zeigt, dass dieselben generischen TCP-Actors (`TcpListenerActor<M, C>`,
//! `TcpConnectionActor<M, C>`, `TcpWriterActor<M, C>`), die bisher mit
//! `SimpleString`/`SimpleStringCodec` genutzt wurden, ohne Änderungen auch
//! für CCSDS Space Packets funktionieren – hier über die Aliase
//! `SpacePacketListener`/`SpacePacketWriter`
//! (`TcpListenerActor<SpacePacket, SpacePacketCodec>` bzw.
//! `TcpWriterActor<SpacePacket, SpacePacketCodec>`).

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use kameo::actor::{Recipient, Spawn};
use kameo_tcp_example::{
    ConnectionHalfClosed, GetLocalAddr, PacketType, SpacePacket, SpacePacketCodec,
    SpacePacketListener, SpacePacketWriter, TcpListenerArgs, TcpWriterArgs, TestActor,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

#[tokio::test]
async fn space_packets_werden_an_downstream_actor_weitergeleitet() {
    let test_actor_ref = TestActor::<SpacePacket>::spawn(TestActor::new());
    let downstream: Recipient<SpacePacket> = test_actor_ref.clone().recipient::<SpacePacket>();

    let listener_ref = SpacePacketListener::spawn(TcpListenerArgs {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        downstream,
    });
    let local_addr = listener_ref.ask(GetLocalAddr).await.unwrap();

    let client_stream = TcpStream::connect(local_addr).await.unwrap();
    let mut client_framed = Framed::new(client_stream, SpacePacketCodec);

    let packet_a = SpacePacket::new(PacketType::Telemetry, 42, 1, &b"hallo server"[..]);
    let packet_b = SpacePacket::new(PacketType::Telecommand, 100, 2, &b"tue etwas"[..]);

    client_framed.send(packet_a.clone()).await.unwrap();
    client_framed.send(packet_b.clone()).await.unwrap();

    let received = TestActor::assert_received(&test_actor_ref, 2, Duration::from_secs(1)).await;

    assert_eq!(received, vec![packet_a, packet_b]);
}

#[tokio::test]
async fn space_packet_writer_actor_schreibt_ein_ueber_framed_lesbares_paket() {
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
    });

    let packet = SpacePacket::new(PacketType::Telemetry, 7, 99, &b"status ok"[..]);
    writer_ref.tell(packet.clone()).await.unwrap();

    let received = client_framed.next().await.unwrap().unwrap();
    assert_eq!(received, packet);
}
