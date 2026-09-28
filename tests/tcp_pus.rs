//! Shows that the generic TCP actors also work with ECSS PUS-C packets,
//! here through the alias `PusServer`
//! (`TcpServerActor<PusPacket, PusCodec>`).

use std::time::Duration;

use bytes::BytesMut;
use futures::SinkExt;
use kameo::actor::{Recipient, Spawn};
use groundlink::{
    ConnectionPolicy, GetLocalAddr, KeepAlive, PusCodec, PusServer, PusPacket, PusTc, PusTm,
    TcpServerArgs, TestActor,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_util::codec::{Encoder, Framed};

#[tokio::test]
async fn pus_packets_are_forwarded_to_downstream_actor() {
    let test_actor_ref = TestActor::<PusPacket>::spawn(TestActor::new());
    let downstream: Recipient<PusPacket> = test_actor_ref.clone().recipient::<PusPacket>();

    let listener_ref = PusServer::spawn(TcpServerArgs {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        downstream,
        keepalive: Some(KeepAlive::default()),
        connection_policy: ConnectionPolicy::WaitForClose,
    });
    let local_addr = listener_ref.ask(GetLocalAddr).await.unwrap();

    let client_stream = TcpStream::connect(local_addr).await.unwrap();
    let mut client_framed = Framed::new(client_stream, PusCodec::default());

    let tc: PusPacket = PusTc::new(42, 1, 17, 1, &b"ping"[..]).into();
    let tm: PusPacket = PusTm::new(42, 2, 17, 2, vec![0u8; 7], &b"pong"[..]).into();

    client_framed.send(tc.clone()).await.unwrap();
    client_framed.send(tm.clone()).await.unwrap();

    let received = TestActor::assert_received(&test_actor_ref, 2, Duration::from_secs(1)).await;

    assert_eq!(received, vec![tc, tm]);
}

fn encode(packet: PusPacket) -> BytesMut {
    let mut buf = BytesMut::new();
    PusCodec::default().encode(packet, &mut buf).unwrap();
    buf
}

#[tokio::test]
async fn invalid_pus_packet_is_dropped_and_connection_stays_open() {
    let test_actor_ref = TestActor::<PusPacket>::spawn(TestActor::new());
    let listener_ref = PusServer::spawn(TcpServerArgs {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        downstream: test_actor_ref.clone().recipient::<PusPacket>(),
        keepalive: Some(KeepAlive::default()),
        connection_policy: ConnectionPolicy::WaitForClose,
    });
    let local_addr = listener_ref.ask(GetLocalAddr).await.unwrap();
    let mut stream = TcpStream::connect(local_addr).await.unwrap();

    // A valid Space Packet with a broken CRC, followed by a valid packet.
    let mut invalid = encode(PusTc::new(42, 1, 17, 1, &b"bad"[..]).into());
    let last = invalid.len() - 1;
    invalid[last] ^= 0xFF;
    let first: PusPacket = PusTc::new(42, 2, 17, 1, &b"first"[..]).into();
    invalid.extend_from_slice(&encode(first.clone()));
    stream.write_all(&invalid).await.unwrap();

    TestActor::assert_received(&test_actor_ref, 1, Duration::from_secs(1)).await;

    // The connection is still open.
    let second: PusPacket = PusTc::new(42, 3, 17, 1, &b"second"[..]).into();
    stream.write_all(&encode(second.clone())).await.unwrap();

    let received = TestActor::assert_received(&test_actor_ref, 2, Duration::from_secs(1)).await;
    assert_eq!(received, vec![first, second]);
}
