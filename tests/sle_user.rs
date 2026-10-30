//! Tests for the `SleUserActor` on top of a `TmlClient` and an initiating
//! `TmlLinkActor`: against the provider actors of this crate, and against
//! a fake provider (a plain `TcpListener` with the `TmlCodec`) for the
//! error cases.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use groundlink::protocol::sle::common::{ConditionalTime, Diagnostic, Time, bind_diagnostic};
use groundlink::protocol::sle::fcltu::transfer_diagnostic;
use groundlink::protocol::sle::raf::{AntennaId, FrameQuality};
use groundlink::{
    Batch, Cltu, CltuStart, ConnectionEvent, FcltuConfig, FcltuServiceInstance, GetLocalAddr, RafConfig, RafFrame,
    RafServiceInstance, RafStart, RequestedFrameQuality, SleAssociationActor, SleAssociationConfig, SleBind, SlePdu,
    SleServiceRegistry, SleStop, SleUnbind, SleUserActor, SleUserConfig, SleUserError, SleUserEvent, TcpClientArgs,
    TcpServerArgs, TestActor, TmlClient, TmlCodec, TmlContext, TmlLinkActor, TmlMessage, TmlResponderConfig, TmlServer,
};
use kameo::actor::{ActorRef, Spawn};
use kameo::error::SendError;
use kameo::mailbox;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

const RAF_SI: &str = "sagr=1.spack=VST-PASS0001.rsl-fg=1.raf=onlt1";
const CLTU_SI: &str = "sagr=1.spack=VST-PASS0001.fsl-fg=1.cltu=cltu1";
const TIMEOUT: Duration = Duration::from_secs(5);

struct Provider {
    addr: SocketAddr,
    raf: ActorRef<RafServiceInstance>,
    cltus: ActorRef<TestActor<Cltu>>,
}

/// A provider port with a RAF instance (2 frames per buffer) and an FCLTU
/// instance whose CLTUs go to a `TestActor`.
async fn spawn_provider() -> Provider {
    let mut raf_config = RafConfig::new(RAF_SI.parse().unwrap());
    raf_config.transfer_buffer_size = 2;
    raf_config.latency_limit = Duration::from_millis(50);
    let raf = RafServiceInstance::spawn(RafServiceInstance::new(raf_config).with_tick(Duration::from_millis(10)));
    let cltus = TestActor::<Cltu>::spawn(TestActor::new());
    let fcltu = FcltuServiceInstance::spawn(FcltuServiceInstance::new(
        FcltuConfig::new(CLTU_SI.parse().unwrap()),
        cltus.clone().recipient(),
    ));
    let registry = SleServiceRegistry::new()
        .with_raf(RAF_SI.parse().unwrap(), raf.clone())
        .with_fcltu(CLTU_SI.parse().unwrap(), fcltu);

    let association = SleAssociationActor::prepare_with_mailbox(mailbox::unbounded());
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
            association.actor_ref().clone().recipient::<Batch<SlePdu>>(),
            TmlResponderConfig::default(),
        )
        .with_events(association.actor_ref().clone().recipient()),
    );
    association.spawn(SleAssociationActor::new(
        link_ref,
        registry,
        SleAssociationConfig::default(),
    ));
    Provider {
        addr: server.ask(GetLocalAddr).await.unwrap(),
        raf,
        cltus,
    }
}

struct User {
    user: ActorRef<SleUserActor>,
    frames: ActorRef<TestActor<RafFrame>>,
    events: ActorRef<TestActor<SleUserEvent>>,
}

/// A user connecting to `addr`; `raf` selects the service.
fn spawn_user(addr: SocketAddr, raf: bool, config: SleUserConfig) -> User {
    let frames = TestActor::<RafFrame>::spawn(TestActor::new());
    let events = TestActor::<SleUserEvent>::spawn(TestActor::new());

    let user = SleUserActor::prepare_with_mailbox(mailbox::unbounded());
    let link = TmlLinkActor::prepare();
    let client = TmlClient::spawn(
        TcpClientArgs::new(addr, link.actor_ref().clone().recipient(), TmlCodec::default())
            .with_observer(link.actor_ref().clone().reply_recipient::<ConnectionEvent>()),
    );
    let link_ref = link.actor_ref().clone();
    link.spawn(
        TmlLinkActor::initiator(
            client.clone().recipient::<TmlMessage>(),
            client.clone().recipient(),
            user.actor_ref().clone().recipient::<Batch<SlePdu>>(),
            TmlContext::new(30, 3),
        )
        .with_events(user.actor_ref().clone().recipient()),
    );
    let user_ref = user.actor_ref().clone();
    let actor = if raf {
        SleUserActor::raf(client, link_ref, config, frames.clone().recipient::<Batch<RafFrame>>())
    } else {
        SleUserActor::fcltu(client, link_ref, config)
    };
    user.spawn(
        actor
            .with_events(events.clone().recipient())
            .with_tick(Duration::from_millis(20)),
    );
    User {
        user: user_ref,
        frames,
        events,
    }
}

fn config(si: &str) -> SleUserConfig {
    SleUserConfig::new("MOC1", "PORT1", si.parse().unwrap())
}

/// The error a request was answered with.
fn user_error<M>(result: Result<impl std::fmt::Debug, SendError<M, SleUserError>>) -> SleUserError {
    match result {
        Err(SendError::HandlerError(err)) => err,
        other => panic!("expected an SleUserError, got {:?}", other.map_err(|_| "send error")),
    }
}

fn frame(n: u8) -> RafFrame {
    RafFrame {
        earth_receive_time: Time::now(),
        antenna_id: AntennaId::Local(Bytes::from_static(b"ANT1")),
        data_link_continuity: 0,
        quality: FrameQuality::Good,
        private_annotation: None,
        data: Bytes::from(vec![n; 100]),
    }
}

fn cltu(id: u32) -> Cltu {
    Cltu {
        id,
        earliest_transmission_time: ConditionalTime::Undefined,
        latest_transmission_time: ConditionalTime::Undefined,
        delay_time_us: 0,
        data: Bytes::from(vec![0xeb, 0x90, id as u8, 0xc5]),
    }
}

#[tokio::test]
async fn raf_user_receives_frames_from_the_provider() {
    let provider = spawn_provider().await;
    let user = spawn_user(provider.addr, true, config(RAF_SI));

    user.user.ask(SleBind).await.unwrap();
    let start = RafStart {
        requested_quality: RequestedFrameQuality::AllFrames,
    };
    user.user.ask(start).await.unwrap();
    for n in 1..=3 {
        provider.raf.tell(frame(n)).await.unwrap();
    }

    let frames = TestActor::assert_received(&user.frames, 3, TIMEOUT).await;
    assert_eq!(frames.iter().map(|f| f.data[0]).collect::<Vec<_>>(), [1, 2, 3]);
    assert_eq!(frames[0].data_link_continuity, -1);

    user.user.ask(SleStop).await.unwrap();
    user.user.ask(SleUnbind).await.unwrap();
    assert_eq!(
        TestActor::assert_received(&user.events, 4, TIMEOUT).await,
        [
            SleUserEvent::Bound,
            SleUserEvent::Started,
            SleUserEvent::Stopped,
            SleUserEvent::Unbound
        ]
    );

    // The same actor binds again on a new connection.
    user.user.ask(SleBind).await.unwrap();
    user.user.ask(start).await.unwrap();
}

#[tokio::test]
async fn fcltu_user_sends_cltus_to_the_provider() {
    let provider = spawn_provider().await;
    let user = spawn_user(provider.addr, false, config(CLTU_SI));

    user.user.ask(SleBind).await.unwrap();
    user.user.ask(CltuStart { first_cltu_id: 10 }).await.unwrap();

    // Several CLTUs on their way at once.
    let (a, b) = tokio::join!(user.user.ask(cltu(10)), user.user.ask(cltu(11)));
    assert_eq!((a.as_ref().unwrap().result, a.unwrap().cltu_id), (Ok(()), 11));
    assert_eq!((b.as_ref().unwrap().result, b.unwrap().cltu_id), (Ok(()), 12));

    // A rejected CLTU is a return like any other.
    let ret = user.user.ask(cltu(20)).await.unwrap();
    assert_eq!(
        (ret.result, ret.cltu_id),
        (Err(Diagnostic::Specific(transfer_diagnostic::OUT_OF_SEQUENCE)), 12)
    );

    let radiated = TestActor::assert_received(&provider.cltus, 2, TIMEOUT).await;
    assert_eq!(radiated.iter().map(|c| c.id).collect::<Vec<_>>(), [10, 11]);
    assert_eq!(&radiated[0].data[..], &[0xeb, 0x90, 10, 0xc5]);

    user.user.ask(SleStop).await.unwrap();
    user.user.ask(SleUnbind).await.unwrap();
}

#[tokio::test]
async fn a_rejected_bind_is_reported_and_the_user_can_bind_again() {
    let provider = spawn_provider().await;
    let user = spawn_user(provider.addr, true, config("sagr=1.spack=X.rsl-fg=1.raf=nope"));
    assert_eq!(
        user_error(user.user.ask(SleBind).await),
        SleUserError::BindRejected(bind_diagnostic::NO_SUCH_SERVICE_INSTANCE)
    );
    assert_eq!(
        user_error(user.user.ask(SleBind).await),
        SleUserError::BindRejected(bind_diagnostic::NO_SUCH_SERVICE_INSTANCE)
    );

    let user = spawn_user(provider.addr, true, {
        let mut config = config(RAF_SI);
        config.version = 1;
        config
    });
    assert_eq!(
        user_error(user.user.ask(SleBind).await),
        SleUserError::BindRejected(bind_diagnostic::VERSION_NOT_SUPPORTED)
    );
}

#[tokio::test]
async fn operations_out_of_order_are_refused_locally() {
    let provider = spawn_provider().await;
    let user = spawn_user(provider.addr, true, config(RAF_SI));

    let start = RafStart {
        requested_quality: RequestedFrameQuality::AllFrames,
    };
    assert!(matches!(
        user_error(user.user.ask(start).await),
        SleUserError::InvalidState(_)
    ));
    assert_eq!(
        user_error(user.user.ask(CltuStart { first_cltu_id: 0 }).await),
        SleUserError::WrongService
    );

    user.user.ask(SleBind).await.unwrap();
    assert!(matches!(
        user_error(user.user.ask(SleBind).await),
        SleUserError::InvalidState(_)
    ));
    assert!(matches!(
        user_error(user.user.ask(SleStop).await),
        SleUserError::InvalidState(_)
    ));
    // The association is still fine.
    user.user.ask(start).await.unwrap();
}

#[tokio::test]
async fn a_missing_return_aborts_the_association() {
    // A provider that accepts the connection but never answers.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let user = spawn_user(listener.local_addr().unwrap(), true, {
        let mut config = config(RAF_SI);
        config.return_timeout = Duration::from_millis(200);
        config
    });
    let bind = tokio::spawn({
        let user = user.user.clone();
        async move { user.ask(SleBind).await }
    });

    let (stream, _) = listener.accept().await.unwrap();
    let mut provider = Framed::new(stream, TmlCodec::default());
    assert!(matches!(
        provider.next().await.unwrap().unwrap(),
        TmlMessage::Context(_)
    ));
    assert!(matches!(provider.next().await.unwrap().unwrap(), TmlMessage::SlePdu(_)));

    let err = user_error(tokio::time::timeout(TIMEOUT, bind).await.unwrap().unwrap());
    assert!(
        matches!(&err, SleUserError::Aborted(reason) if reason.contains("return timeout")),
        "{err:?}"
    );
    // The user has closed the connection.
    let end = tokio::time::timeout(TIMEOUT, provider.next()).await.unwrap();
    assert!(!matches!(end, Some(Ok(TmlMessage::SlePdu(_)))));
}

#[tokio::test]
async fn losing_the_link_fails_the_waiting_requests() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let user = spawn_user(listener.local_addr().unwrap(), true, config(RAF_SI));
    let bind = tokio::spawn({
        let user = user.user.clone();
        async move { user.ask(SleBind).await }
    });

    // The provider goes away after the BIND invocation.
    let (stream, _) = listener.accept().await.unwrap();
    let mut provider = Framed::new(stream, TmlCodec::default());
    provider.next().await.unwrap().unwrap();
    provider.next().await.unwrap().unwrap();
    drop(provider);

    let err = user_error(tokio::time::timeout(TIMEOUT, bind).await.unwrap().unwrap());
    assert!(
        matches!(&err, SleUserError::Aborted(reason) if reason.contains("connection closed")),
        "{err:?}"
    );
    let events = TestActor::assert_received(&user.events, 1, TIMEOUT).await;
    assert!(matches!(&events[0], SleUserEvent::Aborted { .. }), "{events:?}");
}

#[tokio::test]
async fn a_failed_connect_fails_the_bind() {
    // Nothing listens on this port any more.
    let addr = TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap();
    let user = spawn_user(addr, true, config(RAF_SI));
    let err = user_error(tokio::time::timeout(TIMEOUT, user.user.ask(SleBind)).await.unwrap());
    assert!(
        matches!(&err, SleUserError::Aborted(reason) if reason.contains("connect")),
        "{err:?}"
    );
}
