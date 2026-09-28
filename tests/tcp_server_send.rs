//! Tests for sending through a `TcpServerActor`: a message sent to the
//! server is written to the client of its current connection.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use groundlink::{GetLocalAddr, SimpleString, SimpleStringCodec, SimpleStringServer, TcpServerArgs, TestActor};
use kameo::actor::{ActorRef, Spawn};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

type Client = Framed<TcpStream, SimpleStringCodec>;

fn text(s: &str) -> SimpleString {
    SimpleString(s.to_string())
}

/// Spawns a server whose received messages go to the returned test actor.
async fn server() -> (ActorRef<SimpleStringServer>, ActorRef<TestActor<SimpleString>>) {
    let received = TestActor::<SimpleString>::spawn(TestActor::new());
    let server = SimpleStringServer::spawn(TcpServerArgs::new(
        "127.0.0.1:0".parse().unwrap(),
        received.clone().recipient(),
        SimpleStringCodec::default(),
    ));
    (server, received)
}

/// Connects a client and waits until the server has set up the
/// connection: the reader only starts after the writer is registered, so
/// once a message from the client arrives downstream, sending works.
async fn connect(
    server: &ActorRef<SimpleStringServer>,
    received: &ActorRef<TestActor<SimpleString>>,
    expected_count: usize,
) -> Client {
    let addr = server.ask(GetLocalAddr).await.unwrap();
    let mut client = Framed::new(TcpStream::connect(addr).await.unwrap(), SimpleStringCodec::default());
    client.send(text("hello")).await.unwrap();
    TestActor::assert_received(received, expected_count, Duration::from_secs(1)).await;
    client
}

async fn next(client: &mut Client) -> SimpleString {
    tokio::time::timeout(Duration::from_secs(1), client.next())
        .await
        .expect("message should arrive")
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn message_is_written_to_the_connected_client() {
    let (server, received) = server().await;
    let mut client = connect(&server, &received, 1).await;

    server.tell(text("one")).await.unwrap();
    server.tell(text("two")).await.unwrap();

    assert_eq!(next(&mut client).await, text("one"));
    assert_eq!(next(&mut client).await, text("two"));
}

#[tokio::test]
async fn message_without_connection_is_dropped() {
    let (server, received) = server().await;

    server.ask(text("lost")).await.unwrap();
    let mut client = connect(&server, &received, 1).await;
    server.tell(text("delivered")).await.unwrap();

    assert_eq!(next(&mut client).await, text("delivered"));
}

#[tokio::test]
async fn message_goes_to_the_next_client_after_reconnect() {
    let (server, received) = server().await;

    let first = connect(&server, &received, 1).await;
    drop(first);
    // The server accepts the next client once the first one is closed.
    let mut second = connect(&server, &received, 2).await;

    server.tell(text("to second")).await.unwrap();
    assert_eq!(next(&mut second).await, text("to second"));
}
