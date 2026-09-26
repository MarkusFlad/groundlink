//! Integrationstest für `SpacePacketCodec`: Verifiziert die Nutzung mit
//! `tokio_util::codec::Framed` auf einer echten TCP-Verbindung – sowohl
//! lesend als auch schreibend, in beide Richtungen.

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use kameo_tcp_example::{PacketType, SequenceFlags, SpacePacket, SpacePacketCodec, SpacePacketHeader};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

#[tokio::test]
async fn framed_sendet_und_empfaengt_space_packets_in_beide_richtungen() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let client_task = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });

    let (server_stream, _peer_addr) = listener.accept().await.unwrap();
    let client_stream = client_task.await.unwrap();

    let mut server_framed = Framed::new(server_stream, SpacePacketCodec);
    let mut client_framed = Framed::new(client_stream, SpacePacketCodec);

    // Server -> Client
    let tm_packet = SpacePacket::new(PacketType::Telemetry, 42, 7, &b"hallo client"[..]);
    server_framed.send(tm_packet.clone()).await.unwrap();

    let received = client_framed.next().await.unwrap().unwrap();
    assert_eq!(received, tm_packet);

    // Client -> Server, inkl. gesetztem Secondary-Header-Flag und
    // Segmentierungsinfo, um auch diese Felder über die Leitung zu prüfen.
    let tc_packet = SpacePacket {
        header: SpacePacketHeader {
            packet_type: PacketType::Telecommand,
            secondary_header_flag: true,
            apid: 100,
            sequence_flags: SequenceFlags::FirstSegment,
            sequence_count: 555,
        },
        data: Bytes::from_static(b"kommando"),
    };
    client_framed.send(tc_packet.clone()).await.unwrap();

    let received = server_framed.next().await.unwrap().unwrap();
    assert_eq!(received, tc_packet);
}

#[tokio::test]
async fn framed_verarbeitet_mehrere_hintereinander_gesendete_pakete() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let client_task = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });

    let (server_stream, _peer_addr) = listener.accept().await.unwrap();
    let client_stream = client_task.await.unwrap();

    let mut server_framed = Framed::new(server_stream, SpacePacketCodec);
    let mut client_framed = Framed::new(client_stream, SpacePacketCodec);

    let packets: Vec<SpacePacket> = (0..5)
        .map(|i| SpacePacket::new(PacketType::Telemetry, 1, i, format!("paket {i}").into_bytes()))
        .collect();

    for packet in &packets {
        server_framed.send(packet.clone()).await.unwrap();
    }

    for expected in &packets {
        let received = client_framed.next().await.unwrap().unwrap();
        assert_eq!(&received, expected);
    }
}
