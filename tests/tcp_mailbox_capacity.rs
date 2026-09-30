//! Tests for the mailbox capacity of the reader and writer actors
//! (`with_mailbox_capacity`):
//! 1. Even with the smallest mailbox, a server coupled with a client
//!    passes every message on, in order. The writer can then rarely queue
//!    its own flush and depends on later messages to retry.
//! 2. A capacity of 0 is rejected.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use groundlink::{
    ConnectionEvent, GetLocalAddr, SimpleString, SimpleStringClient, SimpleStringCodec, SimpleStringServer,
    TcpClientArgs, TcpServerArgs,
};
use kameo::actor::{Recipient, Spawn};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

#[tokio::test]
async fn smallest_mailbox_passes_all_messages_in_order() {
    let peer_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let codec = SimpleStringCodec::default();

    let client = SimpleStringClient::prepare_with_mailbox(kameo::mailbox::bounded(1));
    let client_messages: Recipient<SimpleString> = client.actor_ref().clone().recipient();
    let server = SimpleStringServer::spawn(
        TcpServerArgs::new("127.0.0.1:0".parse().unwrap(), client_messages, codec.clone())
            .with_observer(client.actor_ref().clone().reply_recipient::<ConnectionEvent>())
            .with_mailbox_capacity(1),
    );
    client.spawn(
        TcpClientArgs::new(peer_b.local_addr().unwrap(), server.clone().recipient(), codec)
            .with_observer(server.clone().reply_recipient::<ConnectionEvent>())
            .with_mailbox_capacity(1),
    );

    let addr = server.ask(GetLocalAddr).await.unwrap();
    let mut a = Framed::new(TcpStream::connect(addr).await.unwrap(), SimpleStringCodec::default());
    let (b_stream, _) = tokio::time::timeout(Duration::from_secs(2), peer_b.accept())
        .await
        .unwrap()
        .unwrap();
    let mut b = Framed::new(b_stream, SimpleStringCodec::default());

    let sent: Vec<SimpleString> = (0..2000).map(|i| SimpleString(format!("message {i}"))).collect();
    let sending = {
        let sent = sent.clone();
        tokio::spawn(async move {
            for message in sent {
                a.feed(message).await.unwrap();
            }
            a.flush().await.unwrap();
            a
        })
    };

    let mut received = Vec::new();
    while received.len() < sent.len() {
        let message = tokio::time::timeout(Duration::from_secs(5), b.next())
            .await
            .expect("timed out")
            .expect("connection closed")
            .unwrap();
        received.push(message);
    }
    drop(sending.await.unwrap());
    assert_eq!(received, sent);
}

#[tokio::test]
#[should_panic(expected = "mailbox capacity must be at least 1")]
async fn zero_mailbox_capacity_is_rejected() {
    let downstream = groundlink::TestActor::<SimpleString>::spawn(groundlink::TestActor::new());
    let _ = TcpServerArgs::new(
        "127.0.0.1:0".parse().unwrap(),
        downstream.recipient::<SimpleString>(),
        SimpleStringCodec::default(),
    )
    .with_mailbox_capacity(0);
}
