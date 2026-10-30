//! Tests for a peer that stays connected but no longer reads, e.g. a frozen
//! GUI. Writing then blocks as soon as the TCP buffers are full:
//! 1. A server with `ConnectionPolicy::ReplaceCurrent` still serves a new
//!    client.
//! 2. `Close` to a client is still answered.

use std::time::Duration;

use groundlink::{
    Close, ConnectionPolicy, GetLocalAddr, SimpleString, SimpleStringClient, SimpleStringCodec, SimpleStringServer,
    TcpClientArgs, TcpServerArgs, TestActor,
};
use kameo::actor::{Recipient, Spawn};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

const SHORT_TIMEOUT: Duration = Duration::from_millis(300);

fn text(s: &str) -> SimpleString {
    SimpleString(s.to_string())
}

/// Sends large messages to `target` until aborted; the peer never reads
/// them, so the TCP buffers fill up quickly.
fn flood(target: Recipient<SimpleString>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let _ = target.tell(SimpleString("x".repeat(60_000))).await;
        }
    })
}

#[tokio::test]
async fn replace_current_serves_a_new_client_while_the_old_one_stops_reading() {
    let messages = TestActor::<SimpleString>::spawn(TestActor::new());
    let server = SimpleStringServer::spawn(
        TcpServerArgs::new(
            "127.0.0.1:0".parse().unwrap(),
            messages.clone().recipient(),
            SimpleStringCodec::default(),
        )
        .with_connection_policy(ConnectionPolicy::ReplaceCurrent)
        .with_keepalive(None)
        .with_write_timeout(Some(SHORT_TIMEOUT)),
    );
    let addr = server.ask(GetLocalAddr).await.unwrap();

    // The first client sends one message and then never reads.
    let mut first = TcpStream::connect(addr).await.unwrap();
    first.write_all(&groundlink::encode_frame("first")).await.unwrap();
    TestActor::assert_received(&messages, 1, Duration::from_secs(1)).await;
    let flooding = flood(server.clone().recipient());
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut second = TcpStream::connect(addr).await.unwrap();
    second.write_all(&groundlink::encode_frame("second")).await.unwrap();
    let all = TestActor::assert_received(&messages, 2, Duration::from_secs(5)).await;
    flooding.abort();

    assert_eq!(all, vec![text("first"), text("second")]);
}

#[tokio::test]
async fn close_is_answered_while_the_peer_stops_reading() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let downstream = TestActor::<SimpleString>::spawn(TestActor::new());
    let client = SimpleStringClient::spawn(
        TcpClientArgs::new(addr, downstream.recipient(), SimpleStringCodec::default())
            .with_keepalive(None)
            .with_write_timeout(Some(SHORT_TIMEOUT)),
    );
    client.ask(groundlink::Connect).await.unwrap();
    // Accepted, but never read.
    let (_peer, _) = listener.accept().await.unwrap();

    let flooding = flood(client.clone().recipient());
    tokio::time::sleep(Duration::from_millis(100)).await;

    tokio::time::timeout(Duration::from_secs(5), client.ask(Close))
        .await
        .expect("Close should be answered")
        .unwrap();
    flooding.abort();
}
