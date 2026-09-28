//! Tests for `ConnectionPolicy`: what a `TcpServerActor` does when a new
//! client connects while a connection is active.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use kameo::actor::{ActorRef, Spawn};
use groundlink::{
    ConnectionPolicy, GetLocalAddr, GetMessages, KeepAlive, SimpleString, SimpleStringCodec,
    SimpleStringServer, TcpServerArgs, TestActor,
};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

type Client = Framed<TcpStream, SimpleStringCodec>;

fn text(s: &str) -> SimpleString {
    SimpleString(s.to_string())
}

/// Spawns a server with `policy` whose received messages go to the
/// returned test actor.
fn server(policy: ConnectionPolicy) -> (ActorRef<SimpleStringServer>, ActorRef<TestActor<SimpleString>>) {
    let received = TestActor::<SimpleString>::spawn(TestActor::new());
    let server = SimpleStringServer::spawn(TcpServerArgs {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        downstream: received.clone().recipient(),
        keepalive: Some(KeepAlive::default()),
        connection_policy: policy,
        codec: SimpleStringCodec::default(),
    });
    (server, received)
}

async fn client(server: &ActorRef<SimpleStringServer>) -> Client {
    let addr = server.ask(GetLocalAddr).await.unwrap();
    Framed::new(TcpStream::connect(addr).await.unwrap(), SimpleStringCodec::default())
}

async fn received(actor: &ActorRef<TestActor<SimpleString>>) -> Vec<SimpleString> {
    actor.ask(GetMessages::new()).await.unwrap()
}

#[tokio::test]
async fn wait_for_close_serves_the_new_client_only_after_the_old_one_is_gone() {
    let (server, messages) = server(ConnectionPolicy::WaitForClose);

    let mut first = client(&server).await;
    first.send(text("first")).await.unwrap();
    TestActor::assert_received(&messages, 1, Duration::from_secs(1)).await;

    // The second client can connect, but is not read while the first one
    // is connected.
    let mut second = client(&server).await;
    second.send(text("second")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(received(&messages).await, vec![text("first")]);

    drop(first);
    let all = TestActor::assert_received(&messages, 2, Duration::from_secs(1)).await;
    assert_eq!(all, vec![text("first"), text("second")]);
}

#[tokio::test]
async fn replace_current_closes_the_old_connection_for_a_new_client() {
    let (server, messages) = server(ConnectionPolicy::ReplaceCurrent);

    let mut first = client(&server).await;
    first.send(text("first")).await.unwrap();
    TestActor::assert_received(&messages, 1, Duration::from_secs(1)).await;

    let mut second = client(&server).await;
    second.send(text("second")).await.unwrap();
    let all = TestActor::assert_received(&messages, 2, Duration::from_secs(1)).await;
    assert_eq!(all, vec![text("first"), text("second")]);

    // The server has closed the first connection.
    let end = tokio::time::timeout(Duration::from_secs(1), first.next())
        .await
        .expect("first connection should be closed");
    assert!(matches!(end, None | Some(Err(_))), "expected end of stream, got {end:?}");

    // Messages sent through the server go to the new client.
    server.tell(text("to second")).await.unwrap();
    let next = tokio::time::timeout(Duration::from_secs(1), second.next())
        .await
        .expect("message should arrive")
        .unwrap()
        .unwrap();
    assert_eq!(next, text("to second"));
}

#[tokio::test]
async fn replace_current_works_repeatedly_and_after_a_client_left() {
    let (server, messages) = server(ConnectionPolicy::ReplaceCurrent);

    // A client that left on its own does not block the next one.
    let mut gone = client(&server).await;
    gone.send(text("0")).await.unwrap();
    TestActor::assert_received(&messages, 1, Duration::from_secs(1)).await;
    drop(gone);

    let mut clients = Vec::new();
    for i in 1..=3 {
        let mut next = client(&server).await;
        next.send(text(&i.to_string())).await.unwrap();
        TestActor::assert_received(&messages, i + 1, Duration::from_secs(1)).await;
        clients.push(next);
    }

    assert_eq!(received(&messages).await, ["0", "1", "2", "3"].map(text).to_vec());
}
