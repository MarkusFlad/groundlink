//! Tests for the `TmlLinkActor` on top of a `TmlServer` (responder) and a
//! `TmlClient` (initiator). The peer is a plain `Framed` TCP stream with
//! the `TmlCodec`.

use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use groundlink::{
    Connect, ConnectionEvent, GetLocalAddr, SlePdu, TcpClientArgs, TcpServerArgs, TestActor, TmlClient, TmlCodec,
    TmlContext, TmlLinkActor, TmlLinkEvent, TmlMessage, TmlReleaseReason, TmlResponderConfig, TmlServer,
};
use kameo::actor::{ActorRef, Spawn};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

type Peer = Framed<TcpStream, TmlCodec>;

const TICK: Duration = Duration::from_millis(50);

struct Responder {
    server: ActorRef<TmlServer>,
    link: ActorRef<TmlLinkActor>,
    pdus: ActorRef<TestActor<SlePdu>>,
    events: ActorRef<TestActor<TmlLinkEvent>>,
}

fn spawn_responder(config: TmlResponderConfig) -> Responder {
    let pdus = TestActor::<SlePdu>::spawn(TestActor::new());
    let events = TestActor::<TmlLinkEvent>::spawn(TestActor::new());

    let link = TmlLinkActor::prepare();
    let server = TmlServer::spawn(
        TcpServerArgs::new(
            "127.0.0.1:0".parse().unwrap(),
            link.actor_ref().clone().recipient(),
            TmlCodec::default(),
        )
        .with_observer(link.actor_ref().clone().reply_recipient::<ConnectionEvent>()),
    );
    let link_ref = link.actor_ref().clone();
    link.spawn(
        TmlLinkActor::responder(
            server.clone().recipient::<TmlMessage>(),
            server.clone().recipient(),
            pdus.clone().recipient::<SlePdu>(),
            config,
        )
        .with_events(events.clone().recipient())
        .with_tick(TICK),
    );
    Responder {
        server,
        link: link_ref,
        pdus,
        events,
    }
}

async fn connect(responder: &Responder) -> Peer {
    let addr = responder.server.ask(GetLocalAddr).await.unwrap();
    Framed::new(TcpStream::connect(addr).await.unwrap(), TmlCodec::default())
}

/// The next message from the link, skipping heartbeats; `None` when the
/// connection was closed.
async fn next_non_heartbeat(peer: &mut Peer) -> Option<TmlMessage> {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(5), peer.next())
            .await
            .expect("timed out")
            .and_then(Result::ok);
        if message != Some(TmlMessage::Heartbeat) {
            return message;
        }
    }
}

async fn events(responder: &Responder, count: usize) -> Vec<TmlLinkEvent> {
    TestActor::assert_received(&responder.events, count, Duration::from_secs(5)).await
}

fn pdu(bytes: &'static [u8]) -> Bytes {
    Bytes::from_static(bytes)
}

#[tokio::test]
async fn sle_pdus_flow_in_both_directions_after_the_context_message() {
    let responder = spawn_responder(TmlResponderConfig::default());
    let mut peer = connect(&responder).await;

    let context = TmlContext::new(30, 3);
    peer.send(TmlMessage::Context(context)).await.unwrap();
    peer.send(TmlMessage::SlePdu(pdu(&[0xbf, 0x64, 0x01]))).await.unwrap();
    peer.send(TmlMessage::Heartbeat).await.unwrap();
    peer.send(TmlMessage::SlePdu(pdu(&[0xa0, 0x00]))).await.unwrap();

    let received = TestActor::assert_received(&responder.pdus, 2, Duration::from_secs(5)).await;
    assert_eq!(
        received,
        vec![SlePdu(pdu(&[0xbf, 0x64, 0x01])), SlePdu(pdu(&[0xa0, 0x00]))]
    );
    assert_eq!(events(&responder, 1).await, vec![TmlLinkEvent::Established(context)]);

    responder.link.tell(SlePdu(pdu(&[0xbf, 0x65, 0x00]))).await.unwrap();
    assert_eq!(
        next_non_heartbeat(&mut peer).await,
        Some(TmlMessage::SlePdu(pdu(&[0xbf, 0x65, 0x00])))
    );

    drop(peer);
    let released = events(&responder, 2).await;
    assert!(matches!(
        &released[1],
        TmlLinkEvent::Released(TmlReleaseReason::ConnectionClosed(_))
    ));
}

#[tokio::test]
async fn an_sle_pdu_before_the_context_message_closes_the_connection() {
    let responder = spawn_responder(TmlResponderConfig::default());
    let mut peer = connect(&responder).await;

    peer.send(TmlMessage::SlePdu(pdu(&[0xbf, 0x64, 0x00]))).await.unwrap();

    assert_eq!(next_non_heartbeat(&mut peer).await, None);
    let events = events(&responder, 1).await;
    assert!(matches!(
        &events[0],
        TmlLinkEvent::Released(TmlReleaseReason::ProtocolError(_))
    ));
    let pdus = TestActor::assert_received(&responder.pdus, 0, Duration::from_secs(1)).await;
    assert!(pdus.is_empty());
}

#[tokio::test]
async fn a_rejected_context_closes_the_connection() {
    let responder = spawn_responder(TmlResponderConfig {
        allow_no_heartbeat: false,
        ..TmlResponderConfig::default()
    });
    let mut peer = connect(&responder).await;

    peer.send(TmlMessage::Context(TmlContext::new(0, 0))).await.unwrap();

    assert_eq!(next_non_heartbeat(&mut peer).await, None);
    let events = events(&responder, 1).await;
    assert!(matches!(
        &events[0],
        TmlLinkEvent::Released(TmlReleaseReason::ContextRejected(_))
    ));
}

#[tokio::test]
async fn a_missing_context_message_times_out() {
    let responder = spawn_responder(TmlResponderConfig {
        startup_timeout: Duration::from_millis(300),
        ..TmlResponderConfig::default()
    });
    let mut peer = connect(&responder).await;

    assert_eq!(next_non_heartbeat(&mut peer).await, None);
    assert_eq!(
        events(&responder, 1).await,
        vec![TmlLinkEvent::Released(TmlReleaseReason::StartupTimeout)]
    );
}

#[tokio::test]
async fn heartbeats_keep_the_link_alive_and_silence_kills_it() {
    let responder = spawn_responder(TmlResponderConfig::default());
    let mut peer = connect(&responder).await;
    // Heartbeat interval 1 s, dead factor 2: the link dies after 2 s of silence.
    peer.send(TmlMessage::Context(TmlContext::new(1, 2))).await.unwrap();
    events(&responder, 1).await;

    // The link sends a heartbeat when it has sent nothing for 1 s.
    let first = tokio::time::timeout(Duration::from_secs(2), peer.next()).await.unwrap();
    assert_eq!(first.unwrap().unwrap(), TmlMessage::Heartbeat);

    // Our heartbeats keep the link alive for longer than the dead time.
    for _ in 0..6 {
        peer.send(TmlMessage::Heartbeat).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let events_so_far = TestActor::assert_received(&responder.events, 1, Duration::from_secs(1)).await;
    assert_eq!(events_so_far.len(), 1, "{events_so_far:?}");

    // Silence: the link counts the peer as dead and closes the connection.
    let released = events(&responder, 2).await;
    assert_eq!(released[1], TmlLinkEvent::Released(TmlReleaseReason::PeerDead));
    assert_eq!(next_non_heartbeat(&mut peer).await, None);
}

#[tokio::test]
async fn sle_pdus_without_a_link_are_dropped() {
    let responder = spawn_responder(TmlResponderConfig::default());
    responder.link.tell(SlePdu(pdu(&[0x01]))).await.unwrap();

    // A peer that connects afterwards does not get the PDU.
    let mut peer = connect(&responder).await;
    peer.send(TmlMessage::Context(TmlContext::new(0, 0))).await.unwrap();
    events(&responder, 1).await;
    responder.link.tell(SlePdu(pdu(&[0x02]))).await.unwrap();
    assert_eq!(
        next_non_heartbeat(&mut peer).await,
        Some(TmlMessage::SlePdu(pdu(&[0x02])))
    );
}

#[tokio::test]
async fn initiator_sends_the_context_message_first() {
    let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let pdus = TestActor::<SlePdu>::spawn(TestActor::new());
    let events = TestActor::<TmlLinkEvent>::spawn(TestActor::new());

    let context = TmlContext::new(25, 5);
    let link = TmlLinkActor::prepare();
    let client = TmlClient::spawn(
        TcpClientArgs::new(
            provider.local_addr().unwrap(),
            link.actor_ref().clone().recipient(),
            TmlCodec::default(),
        )
        .with_observer(link.actor_ref().clone().reply_recipient::<ConnectionEvent>()),
    );
    let link_ref = link.actor_ref().clone();
    link.spawn(
        TmlLinkActor::initiator(
            client.clone().recipient::<TmlMessage>(),
            client.clone().recipient(),
            pdus.clone().recipient::<SlePdu>(),
            context,
        )
        .with_events(events.clone().recipient())
        .with_tick(TICK),
    );

    client.ask(Connect).await.unwrap();
    let (stream, _) = provider.accept().await.unwrap();
    let mut peer = Framed::new(stream, TmlCodec::default());

    assert_eq!(next_non_heartbeat(&mut peer).await, Some(TmlMessage::Context(context)));
    assert_eq!(
        TestActor::assert_received(&events, 1, Duration::from_secs(5)).await,
        vec![TmlLinkEvent::Established(context)]
    );

    link_ref.tell(SlePdu(pdu(&[0xbf, 0x64, 0x00]))).await.unwrap();
    assert_eq!(
        next_non_heartbeat(&mut peer).await,
        Some(TmlMessage::SlePdu(pdu(&[0xbf, 0x64, 0x00])))
    );

    peer.send(TmlMessage::SlePdu(pdu(&[0xbf, 0x65, 0x00]))).await.unwrap();
    assert_eq!(
        TestActor::assert_received(&pdus, 1, Duration::from_secs(5)).await,
        vec![SlePdu(pdu(&[0xbf, 0x65, 0x00]))]
    );
}
