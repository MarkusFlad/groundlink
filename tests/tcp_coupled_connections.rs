//! Tests for coupling a `TcpServerActor` and a `TcpClientActor`: a peer A
//! connects to the server, the client connects to peer B, and messages and
//! connection events flow between them, through actors in between or
//! directly.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use groundlink::{
    ConnectionEvent, GetLocalAddr, SimpleString, SimpleStringClient, SimpleStringCodec, SimpleStringServer,
    TcpClientArgs, TcpServerArgs,
};
use kameo::actor::{Actor, ActorRef, Recipient, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

type Framed16 = Framed<TcpStream, SimpleStringCodec>;

fn text(s: &str) -> SimpleString {
    SimpleString(s.to_string())
}

/// Actor in between: forwards messages (optionally in upper case and after
/// a delay, to simulate processing) and connection events to the next
/// actor.
struct Forwarder {
    messages: Recipient<SimpleString>,
    events: Recipient<ConnectionEvent>,
    delay: Duration,
    upper_case: bool,
}

impl Actor for Forwarder {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<SimpleString> for Forwarder {
    type Reply = ();

    async fn handle(&mut self, SimpleString(s): SimpleString, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        tokio::time::sleep(self.delay).await;
        let s = if self.upper_case { s.to_uppercase() } else { s };
        self.messages.tell(SimpleString(s)).await.unwrap();
    }
}

impl Message<ConnectionEvent> for Forwarder {
    type Reply = ();

    async fn handle(&mut self, event: ConnectionEvent, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.events.tell(event).await.unwrap();
    }
}

/// Peer B listening, and a server coupled with a client that connects to
/// B, with a `Forwarder` in each direction.
async fn coupled_via_forwarders(delay: Duration) -> (ActorRef<SimpleStringServer>, TcpListener) {
    let peer_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let codec = SimpleStringCodec::default();

    // Server and client each send to the other, so the client gets its
    // reference before it starts.
    let client = SimpleStringClient::prepare();
    let to_b = Forwarder::spawn(Forwarder {
        messages: client.actor_ref().clone().recipient(),
        events: client.actor_ref().clone().recipient(),
        delay,
        upper_case: true,
    });
    let server = SimpleStringServer::spawn(
        TcpServerArgs::new("127.0.0.1:0".parse().unwrap(), to_b.clone().recipient(), codec.clone())
            .with_observer(to_b.reply_recipient::<ConnectionEvent>()),
    );
    let to_a = Forwarder::spawn(Forwarder {
        messages: server.clone().recipient(),
        events: server.clone().recipient(),
        delay,
        upper_case: false,
    });
    client.spawn(
        TcpClientArgs::new(peer_b.local_addr().unwrap(), to_a.clone().recipient(), codec)
            .with_observer(to_a.reply_recipient::<ConnectionEvent>()),
    );
    (server, peer_b)
}

/// Like `coupled_via_forwarders`, but server and client send to each other
/// directly.
async fn coupled_directly() -> (ActorRef<SimpleStringServer>, TcpListener) {
    let peer_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let codec = SimpleStringCodec::default();

    let client = SimpleStringClient::prepare();
    let server = SimpleStringServer::spawn(
        TcpServerArgs::new(
            "127.0.0.1:0".parse().unwrap(),
            client.actor_ref().clone().recipient(),
            codec.clone(),
        )
        .with_observer(client.actor_ref().clone().reply_recipient::<ConnectionEvent>()),
    );
    client.spawn(
        TcpClientArgs::new(peer_b.local_addr().unwrap(), server.clone().recipient(), codec)
            .with_observer(server.clone().reply_recipient::<ConnectionEvent>()),
    );
    (server, peer_b)
}

async fn next(framed: &mut Framed16) -> Option<SimpleString> {
    tokio::time::timeout(Duration::from_secs(2), framed.next())
        .await
        .expect("timed out")
        .map(Result::unwrap)
}

async fn connect_a(server: &ActorRef<SimpleStringServer>) -> Framed16 {
    let addr = server.ask(GetLocalAddr).await.unwrap();
    Framed::new(TcpStream::connect(addr).await.unwrap(), SimpleStringCodec::default())
}

async fn accept_b(peer_b: &TcpListener) -> Framed16 {
    let (stream, _) = tokio::time::timeout(Duration::from_secs(2), peer_b.accept())
        .await
        .expect("the client should connect to peer B")
        .unwrap();
    Framed::new(stream, SimpleStringCodec::default())
}

#[tokio::test]
async fn b_is_connected_when_a_connects_and_messages_flow_through_the_actors_in_between() {
    let (server, peer_b) = coupled_via_forwarders(Duration::ZERO).await;

    // A sends right after connecting; the message must not be lost while
    // the client connects to B.
    let mut a = connect_a(&server).await;
    a.send(text("ping")).await.unwrap();
    let mut b = accept_b(&peer_b).await;
    assert_eq!(next(&mut b).await, Some(text("PING")));

    b.send(text("pong")).await.unwrap();
    assert_eq!(next(&mut a).await, Some(text("pong")));
}

#[tokio::test]
async fn messages_sent_just_before_a_leaves_reach_b() {
    let (server, peer_b) = coupled_via_forwarders(Duration::from_millis(5)).await;
    let mut a = connect_a(&server).await;
    let mut b = accept_b(&peer_b).await;

    for i in 0..20 {
        a.send(text(&format!("m{i}"))).await.unwrap();
    }
    drop(a);

    // The forwarder is slow, but the end of the connection travels behind
    // the messages.
    for i in 0..20 {
        assert_eq!(next(&mut b).await, Some(text(&format!("M{i}"))));
    }
    assert_eq!(next(&mut b).await, None, "B should see the end of the connection");
}

#[tokio::test]
async fn messages_sent_just_before_b_leaves_reach_a() {
    let (server, peer_b) = coupled_via_forwarders(Duration::from_millis(5)).await;
    let mut a = connect_a(&server).await;
    let mut b = accept_b(&peer_b).await;

    for i in 0..20 {
        b.send(text(&format!("m{i}"))).await.unwrap();
    }
    drop(b);

    for i in 0..20 {
        assert_eq!(next(&mut a).await, Some(text(&format!("m{i}"))));
    }
    assert_eq!(next(&mut a).await, None, "A should see the end of the connection");
}

#[tokio::test]
async fn a_is_disconnected_if_b_is_unreachable() {
    let (server, peer_b) = coupled_via_forwarders(Duration::ZERO).await;
    drop(peer_b);

    let mut a = connect_a(&server).await;

    assert_eq!(next(&mut a).await, None, "A should see the end of the connection");
}

#[tokio::test]
async fn the_next_connection_of_a_is_coupled_again_after_b_left() {
    let (server, peer_b) = coupled_via_forwarders(Duration::ZERO).await;
    let mut a = connect_a(&server).await;
    let b = accept_b(&peer_b).await;

    drop(b);
    assert_eq!(next(&mut a).await, None, "A should see the end of the connection");

    let mut a2 = connect_a(&server).await;
    a2.send(text("again")).await.unwrap();
    let mut b2 = accept_b(&peer_b).await;
    assert_eq!(next(&mut b2).await, Some(text("AGAIN")));
}

#[tokio::test]
async fn quick_reconnects_are_coupled_independently() {
    let (server, peer_b) = coupled_via_forwarders(Duration::from_millis(1)).await;
    let mut a = connect_a(&server).await;
    let mut b = accept_b(&peer_b).await;

    for i in 0..10 {
        // A sends, leaves and the next A connects and sends immediately; the
        // end of the old connection must neither drop the old message nor
        // close the new connection.
        a.send(text(&format!("last {i}"))).await.unwrap();
        drop(a);
        a = connect_a(&server).await;
        a.send(text(&format!("first {i}"))).await.unwrap();

        assert_eq!(next(&mut b).await, Some(text(&format!("LAST {i}"))));
        assert_eq!(next(&mut b).await, None, "B should see the end of the old connection");
        b = accept_b(&peer_b).await;
        assert_eq!(next(&mut b).await, Some(text(&format!("FIRST {i}"))));
    }

    b.send(text("still open")).await.unwrap();
    assert_eq!(next(&mut a).await, Some(text("still open")));
}

#[tokio::test]
async fn client_keeps_messages_until_its_previous_connection_is_closed() {
    use groundlink::{ConnectionEventKind, EventSource, TestActor};

    let peer_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let from_b = TestActor::<SimpleString>::spawn(TestActor::new());
    let client = SimpleStringClient::spawn(TcpClientArgs::new(
        peer_b.local_addr().unwrap(),
        from_b.recipient(),
        SimpleStringCodec::default(),
    ));
    let server_event = |connection_id, kind| ConnectionEvent {
        source: EventSource::Server,
        connection_id,
        peer_connection_id: None,
        peer_addr: "127.0.0.1:1".parse().unwrap(),
        kind,
    };

    client
        .tell(server_event(1, ConnectionEventKind::Connected))
        .await
        .unwrap();
    let mut b1 = accept_b(&peer_b).await;

    // Server connection 1 ends and 2 starts, all before the client has
    // closed its first connection.
    client
        .tell(server_event(1, ConnectionEventKind::Disconnected))
        .await
        .unwrap();
    client
        .tell(server_event(2, ConnectionEventKind::Connected))
        .await
        .unwrap();
    client.tell(text("for 2")).await.unwrap();

    assert_eq!(next(&mut b1).await, None, "the first connection should be closed");
    let mut b2 = accept_b(&peer_b).await;
    assert_eq!(next(&mut b2).await, Some(text("for 2")));
}

#[tokio::test]
async fn server_and_client_can_be_coupled_directly() {
    let (server, peer_b) = coupled_directly().await;

    let mut a = connect_a(&server).await;
    a.send(text("ping")).await.unwrap();
    let mut b = accept_b(&peer_b).await;
    assert_eq!(next(&mut b).await, Some(text("ping")));
    b.send(text("pong")).await.unwrap();
    assert_eq!(next(&mut a).await, Some(text("pong")));

    drop(a);
    assert_eq!(next(&mut b).await, None, "B should see the end of the connection");
}
