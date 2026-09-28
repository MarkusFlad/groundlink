//! Shows that the generic TCP actors also work with ECSS PUS-C packets,
//! here through the alias `PusServer`
//! (`TcpServerActor<PusPacket, PusCodec>`).

use std::time::Duration;

use bytes::BytesMut;
use futures::SinkExt;
use kameo::actor::{Recipient, Spawn};
use groundlink::{
    GetLocalAddr, PusClient, PusCodec, PusConfig, PusServer, PusPacket, PusTc, PusTm, TcpClientArgs,
    TcpServerArgs, TestActor,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_util::codec::{Encoder, Framed};

#[tokio::test]
async fn pus_packets_are_forwarded_to_downstream_actor() {
    let test_actor_ref = TestActor::<PusPacket>::spawn(TestActor::new());
    let downstream: Recipient<PusPacket> = test_actor_ref.clone().recipient::<PusPacket>();

    let listener_ref = PusServer::spawn(TcpServerArgs::new(
        "127.0.0.1:0".parse().unwrap(),
        downstream,
        PusCodec::default(),
    ));
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
    let listener_ref = PusServer::spawn(TcpServerArgs::new(
        "127.0.0.1:0".parse().unwrap(),
        test_actor_ref.clone().recipient::<PusPacket>(),
        PusCodec::default(),
    ));
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

#[tokio::test]
async fn pus_config_of_the_codec_reaches_server_and_client() {
    use groundlink::Connect;

    // No packet error control and a 4-byte time stamp, unlike the default.
    let config = PusConfig { tm_time_len: 4, packet_error_control: false };
    let codec = PusCodec::new(config);

    let at_server = TestActor::<PusPacket>::spawn(TestActor::new());
    let server = PusServer::spawn(TcpServerArgs::new(
        "127.0.0.1:0".parse().unwrap(),
        at_server.clone().recipient::<PusPacket>(),
        codec,
    ));
    let addr = server.ask(GetLocalAddr).await.unwrap();

    let at_client = TestActor::<PusPacket>::spawn(TestActor::new());
    let client = PusClient::spawn(TcpClientArgs::new(
        addr,
        at_client.clone().recipient::<PusPacket>(),
        codec,
    ));
    client.ask(Connect).await.unwrap();

    // Client -> server: a TM with a 4-byte time stamp would be rejected by
    // the default configuration.
    let tm: PusPacket = PusTm::new(42, 1, 17, 2, vec![1u8, 2, 3, 4], &b"up"[..]).into();
    client.tell(tm.clone()).await.unwrap();
    assert_eq!(TestActor::assert_received(&at_server, 1, Duration::from_secs(1)).await, vec![tm]);

    // Server -> client: if the server appended a CRC, the client (without
    // packet error control) would decode it as part of the application data.
    let tc: PusPacket = PusTc::new(42, 2, 17, 1, &b"down"[..]).into();
    server.tell(tc.clone()).await.unwrap();
    assert_eq!(TestActor::assert_received(&at_client, 1, Duration::from_secs(1)).await, vec![tc]);
}

#[tokio::test]
async fn packet_that_cannot_be_encoded_does_not_close_the_connection() {
    use futures::StreamExt;

    let received = TestActor::<PusPacket>::spawn(TestActor::new());
    let server = PusServer::spawn(TcpServerArgs::new(
        "127.0.0.1:0".parse().unwrap(),
        received.clone().recipient::<PusPacket>(),
        PusCodec::default(),
    ));
    let addr = server.ask(GetLocalAddr).await.unwrap();
    let mut client = Framed::new(TcpStream::connect(addr).await.unwrap(), PusCodec::default());

    // Wait until the server serves the connection.
    let hello: PusPacket = PusTc::new(42, 0, 17, 1, &b""[..]).into();
    client.send(hello).await.unwrap();
    TestActor::assert_received(&received, 1, Duration::from_secs(1)).await;

    // A 3-byte time stamp cannot be encoded with the default 7-byte config.
    let invalid: PusPacket = PusTm::new(42, 0, 17, 2, vec![0u8; 3], &b""[..]).into();
    let valid: PusPacket = PusTm::new(42, 1, 17, 2, vec![0u8; 7], &b""[..]).into();
    server.tell(invalid).await.unwrap();
    server.tell(valid.clone()).await.unwrap();

    let next = tokio::time::timeout(Duration::from_secs(1), client.next())
        .await
        .expect("the valid packet should arrive")
        .expect("connection should stay open")
        .unwrap();
    assert_eq!(next, valid);
}
