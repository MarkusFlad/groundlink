//! Shows that the generic TCP actors also work with ECSS PUS-C packets,
//! here through the alias `PusListener`
//! (`TcpListenerActor<PusPacket, PusCodec>`).

use std::time::Duration;

use futures::SinkExt;
use kameo::actor::{Recipient, Spawn};
use kameo_tcp_example::{
    GetLocalAddr, PusCodec, PusListener, PusPacket, PusTc, PusTm, TcpListenerArgs, TestActor,
};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

#[tokio::test]
async fn pus_packets_are_forwarded_to_downstream_actor() {
    let test_actor_ref = TestActor::<PusPacket>::spawn(TestActor::new());
    let downstream: Recipient<PusPacket> = test_actor_ref.clone().recipient::<PusPacket>();

    let listener_ref = PusListener::spawn(TcpListenerArgs {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        downstream,
        on_connect: None,
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
