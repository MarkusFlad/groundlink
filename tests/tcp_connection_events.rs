//! Tests for the `ConnectionEvent`s of `TcpServerActor` and
//! `TcpClientActor`, for `Close` on the server and for closing single
//! halves of a client connection with `CloseRead` and `CloseWrite`.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use groundlink::{
    Close, CloseRead, CloseWrite, Connect, ConnectionEvent, ConnectionEventKind, ConnectionHalf, EventSource,
    GetLocalAddr, GetMessages, SimpleString, SimpleStringClient, SimpleStringCodec, SimpleStringServer, TcpClientArgs,
    TcpServerArgs, TestActor,
};
use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

type Framed16 = Framed<TcpStream, SimpleStringCodec>;

fn text(s: &str) -> SimpleString {
    SimpleString(s.to_string())
}

fn kinds(events: &[ConnectionEvent]) -> Vec<(u64, ConnectionEventKind)> {
    events.iter().map(|e| (e.connection_id, e.kind.clone())).collect()
}

fn half_closed(half: ConnectionHalf) -> impl Fn(&ConnectionEventKind) -> bool {
    move |kind| matches!(kind, ConnectionEventKind::HalfClosed { half: h, .. } if *h == half)
}

async fn next(framed: &mut Framed16) -> Option<SimpleString> {
    tokio::time::timeout(Duration::from_secs(1), framed.next())
        .await
        .expect("timed out")
        .map(Result::unwrap)
}

/// Spawns a server whose messages go to the first test actor and whose
/// events go to the second.
fn server() -> (
    ActorRef<SimpleStringServer>,
    ActorRef<TestActor<SimpleString>>,
    ActorRef<TestActor<ConnectionEvent>>,
) {
    let messages = TestActor::<SimpleString>::spawn(TestActor::new());
    let events = TestActor::<ConnectionEvent>::spawn(TestActor::new());
    let server = SimpleStringServer::spawn(
        TcpServerArgs::new(
            "127.0.0.1:0".parse().unwrap(),
            messages.clone().recipient(),
            SimpleStringCodec::default(),
        )
        .with_observer(events.clone().reply_recipient::<ConnectionEvent>()),
    );
    (server, messages, events)
}

async fn connect_to(server: &ActorRef<SimpleStringServer>) -> Framed16 {
    let addr = server.ask(GetLocalAddr).await.unwrap();
    Framed::new(TcpStream::connect(addr).await.unwrap(), SimpleStringCodec::default())
}

#[tokio::test]
async fn server_reports_the_lifecycle_of_each_connection() {
    let (server, _messages, events) = server();

    for id in 1..=2 {
        let client = connect_to(&server).await;
        TestActor::assert_received(&events, 4 * (id as usize - 1) + 1, Duration::from_secs(1)).await;
        drop(client);
        TestActor::assert_received(&events, 4 * id as usize, Duration::from_secs(1)).await;
    }

    let received = events.ask(GetMessages::new()).await.unwrap();
    assert!(received.iter().all(|e| e.source == EventSource::Server));
    for (id, chunk) in (1..).zip(kinds(&received).chunks(4)) {
        assert!(chunk.iter().all(|(event_id, _)| *event_id == id), "{chunk:?}");
        assert_eq!(chunk[0].1, ConnectionEventKind::Connected);
        let halves = &chunk[1..3];
        assert!(
            halves.iter().any(|(_, kind)| half_closed(ConnectionHalf::Read)(kind)),
            "{chunk:?}"
        );
        assert!(
            halves.iter().any(|(_, kind)| half_closed(ConnectionHalf::Write)(kind)),
            "{chunk:?}"
        );
        assert_eq!(chunk[3].1, ConnectionEventKind::Disconnected);
    }
}

/// Observer that takes its time with `Connected` and records whether the
/// downstream actor had received a message by then.
struct SlowObserver {
    messages: ActorRef<TestActor<SimpleString>>,
    seen_before_ready: Option<usize>,
}

impl Actor for SlowObserver {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<ConnectionEvent> for SlowObserver {
    type Reply = ();

    async fn handle(&mut self, event: ConnectionEvent, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if event.kind == ConnectionEventKind::Connected {
            tokio::time::sleep(Duration::from_millis(200)).await;
            self.seen_before_ready = Some(self.messages.ask(GetMessages::new()).await.unwrap().len());
        }
    }
}

struct SeenBeforeReady;

impl Message<SeenBeforeReady> for SlowObserver {
    type Reply = Option<usize>;

    async fn handle(&mut self, _msg: SeenBeforeReady, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.seen_before_ready
    }
}

#[tokio::test]
async fn server_reads_only_after_the_observer_handled_connected() {
    let messages = TestActor::<SimpleString>::spawn(TestActor::new());
    let observer = SlowObserver::spawn(SlowObserver {
        messages: messages.clone(),
        seen_before_ready: None,
    });
    let server = SimpleStringServer::spawn(
        TcpServerArgs::new(
            "127.0.0.1:0".parse().unwrap(),
            messages.clone().recipient(),
            SimpleStringCodec::default(),
        )
        .with_observer(observer.clone().reply_recipient::<ConnectionEvent>()),
    );

    // The client sends right after connecting.
    let mut client = connect_to(&server).await;
    client.send(text("early")).await.unwrap();

    TestActor::assert_received(&messages, 1, Duration::from_secs(2)).await;
    assert_eq!(observer.ask(SeenBeforeReady).await.unwrap(), Some(0));
}

#[tokio::test]
async fn close_on_the_server_ends_the_current_connection() {
    let (server, messages, events) = server();

    let mut client = connect_to(&server).await;
    client.send(text("hello")).await.unwrap();
    TestActor::assert_received(&messages, 1, Duration::from_secs(1)).await;

    server.tell(Close).await.unwrap();

    assert_eq!(
        next(&mut client).await,
        None,
        "the client should see the end of the connection"
    );
    let received = TestActor::assert_received(&events, 4, Duration::from_secs(1)).await;
    assert_eq!(received.last().unwrap().kind, ConnectionEventKind::Disconnected);
}

/// A listener and a client connected to it; returns the client, the
/// accepted stream, the client's messages and events, and the listener.
async fn client_pair() -> (
    ActorRef<SimpleStringClient>,
    Framed16,
    ActorRef<TestActor<SimpleString>>,
    ActorRef<TestActor<ConnectionEvent>>,
    TcpListener,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let messages = TestActor::<SimpleString>::spawn(TestActor::new());
    let events = TestActor::<ConnectionEvent>::spawn(TestActor::new());
    let client = SimpleStringClient::spawn(
        TcpClientArgs::new(
            listener.local_addr().unwrap(),
            messages.clone().recipient(),
            SimpleStringCodec::default(),
        )
        .with_observer(events.clone().reply_recipient::<ConnectionEvent>()),
    );
    client.ask(Connect).await.unwrap();
    let (stream, _) = listener.accept().await.unwrap();
    (
        client,
        Framed::new(stream, SimpleStringCodec::default()),
        messages,
        events,
        listener,
    )
}

#[tokio::test]
async fn client_reports_events_and_close_with_ask_waits_for_the_end() {
    let (client, _remote, _messages, events, listener) = client_pair().await;
    let connected = TestActor::assert_received(&events, 1, Duration::from_secs(1)).await;
    assert_eq!(connected[0].source, EventSource::Client);
    assert_eq!(
        (connected[0].connection_id, connected[0].kind.clone()),
        (1, ConnectionEventKind::Connected)
    );

    client.ask(Close).await.unwrap();

    // The old connection is gone when `ask` returns, so connecting again
    // right away succeeds instead of failing with `AlreadyExists`.
    client.ask(Connect).await.unwrap();
    let _second = listener.accept().await.unwrap();

    let received = TestActor::assert_received(&events, 5, Duration::from_secs(1)).await;
    assert_eq!(
        (received[3].connection_id, received[3].kind.clone()),
        (1, ConnectionEventKind::Disconnected)
    );
    assert_eq!(
        (received[4].connection_id, received[4].kind.clone()),
        (2, ConnectionEventKind::Connected)
    );
}

#[tokio::test]
async fn close_write_ends_the_sending_direction_only() {
    let (client, mut remote, messages, events, _listener) = client_pair().await;

    client.tell(CloseWrite).await.unwrap();

    assert_eq!(
        next(&mut remote).await,
        None,
        "the remote should see the end of the client's data"
    );
    let received = TestActor::assert_received(&events, 2, Duration::from_secs(1)).await;
    assert!(half_closed(ConnectionHalf::Write)(&received[1].kind), "{received:?}");

    // The client still receives.
    remote.send(text("still open")).await.unwrap();
    assert_eq!(
        TestActor::assert_received(&messages, 1, Duration::from_secs(1)).await,
        vec![text("still open")]
    );
}

#[tokio::test]
async fn close_read_ends_the_receiving_direction_only() {
    let (client, mut remote, _messages, events, _listener) = client_pair().await;

    client.tell(CloseRead).await.unwrap();
    let received = TestActor::assert_received(&events, 2, Duration::from_secs(1)).await;
    assert!(half_closed(ConnectionHalf::Read)(&received[1].kind), "{received:?}");

    // The client still sends.
    client.tell(text("still open")).await.unwrap();
    assert_eq!(next(&mut remote).await, Some(text("still open")));
}
