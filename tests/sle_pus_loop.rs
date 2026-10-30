//! End to end: a PUS telecommand goes from the ground over SLE FCLTU to a
//! simulated spacecraft, and its telemetry comes back over SLE RAF.
//!
//! ```text
//! ground:     PusTc ─▶ TcFrameEncoder ─Cltu─▶ SleUserActor (FCLTU) ══TCP══▶ FCLTU port
//! spacecraft: FcltuServiceInstance ─Cltu─▶ TcFrameDecoder ─SpacePacket─▶ PusTcAcceptor ─▶ PusTestServiceActor
//!             PusTcAcceptor, PusTestServiceActor ─PusTm─▶ PusTmStamper ─PusPacket─▶ TmFrameEncoder ─RafFrame─▶ RafServiceInstance
//! ground:     RAF port ══TCP══▶ SleUserActor (RAF) ─RafFrame─▶ TmFrameDecoder ─SpacePacket─▶ TestActor
//! ```

use std::time::Duration;

use bytes::Bytes;
use groundlink::actor::sle::{Cltu, CltuStart, RafStart, SleBind};
use groundlink::actor::tcp::GetLocalAddr;
use groundlink::protocol::pus::service1::{RequestId, VerificationKind, VerificationReport};
use groundlink::protocol::pus::{service1, service17};
use groundlink::protocol::sle::raf::{RafFrame, RequestedFrameQuality};
use groundlink::protocol::tml::TmlContext;
use groundlink::{
    Batch, ConnectionEvent, FcltuConfig, FcltuServiceInstance, PusConfig, PusPacket, PusTc, PusTcAcceptor,
    PusTestServiceActor, PusTm, PusTmStamper, RafConfig, RafServiceInstance, SleAssociationActor, SleAssociationConfig,
    SlePdu, SleServiceRegistry, SleUserActor, SleUserConfig, SpacePacket, TcFrameConfig, TcFrameDecoder,
    TcFrameEncoder, TcpClientArgs, TcpServerArgs, TestActor, TmFrameConfig, TmFrameDecoder, TmFrameEncoder, TmlClient,
    TmlCodec, TmlLinkActor, TmlMessage, TmlResponderConfig, TmlServer,
};
use kameo::actor::{ActorRef, Spawn};
use kameo::mailbox;
use std::net::SocketAddr;

const RAF_SI: &str = "sagr=1.spack=VST-PASS0001.rsl-fg=1.raf=onlt1";
const CLTU_SI: &str = "sagr=1.spack=VST-PASS0001.fsl-fg=1.cltu=cltu1";
const SPACECRAFT_ID: u16 = 0x2a;
const APID: u16 = 0x042;
const TIMEOUT: Duration = Duration::from_secs(5);

/// A provider port with the instances of `registry`.
async fn spawn_port(registry: SleServiceRegistry) -> SocketAddr {
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
    server.ask(GetLocalAddr).await.unwrap()
}

/// The simulated spacecraft behind a provider with a RAF and an FCLTU port.
async fn spawn_spacecraft() -> (SocketAddr, SocketAddr) {
    let mut raf_config = RafConfig::new(RAF_SI.parse().unwrap());
    raf_config.latency_limit = Duration::from_millis(50);
    let raf = RafServiceInstance::spawn(RafServiceInstance::new(raf_config).with_tick(Duration::from_millis(10)));

    // Telemetry: stamped, packed into TM frames, received by the ground
    // station as RAF frames.
    let tm_encoder = TmFrameEncoder::spawn(TmFrameEncoder::new(
        TmFrameConfig::new(SPACECRAFT_ID, 0),
        raf.clone().recipient::<Batch<RafFrame>>(),
    ));
    let stamper = PusTmStamper::spawn(PusTmStamper::new(APID, tm_encoder.recipient::<Batch<PusPacket>>()));
    let telemetry = stamper.recipient::<PusTm>();

    // Telecommands: CLTU decoding, acceptance check, service 17.
    let test_service = PusTestServiceActor::spawn(PusTestServiceActor::new(APID, telemetry.clone()));
    let acceptor = PusTcAcceptor::spawn(PusTcAcceptor::new(APID, telemetry).with_service_handler(
        service17::SERVICE_TYPE,
        &[service17::ARE_YOU_ALIVE_REQUEST_SUBTYPE],
        test_service.recipient::<PusTc>(),
    ));
    let tc_decoder = TcFrameDecoder::spawn(TcFrameDecoder::new(
        TcFrameConfig::new(SPACECRAFT_ID, 0),
        acceptor.recipient::<SpacePacket>(),
    ));
    let fcltu = FcltuServiceInstance::spawn(FcltuServiceInstance::new(
        FcltuConfig::new(CLTU_SI.parse().unwrap()),
        tc_decoder.recipient::<Cltu>(),
    ));

    let registry = SleServiceRegistry::new()
        .with_raf(RAF_SI.parse().unwrap(), raf)
        .with_fcltu(CLTU_SI.parse().unwrap(), fcltu);
    (spawn_port(registry.clone()).await, spawn_port(registry).await)
}

/// A user on `addr`: client, initiating link actor and user actor.
fn spawn_user(addr: SocketAddr, si: &str, frames: Option<ActorRef<TmFrameDecoder>>) -> ActorRef<SleUserActor> {
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
    let config = SleUserConfig::new("MOC1", "PORT1", si.parse().unwrap());
    user.spawn(match frames {
        Some(frames) => SleUserActor::raf(client, link_ref, config, frames.recipient::<Batch<RafFrame>>()),
        None => SleUserActor::fcltu(client, link_ref, config),
    });
    user_ref
}

struct Ground {
    telecommands: ActorRef<TcFrameEncoder>,
    telemetry: ActorRef<TestActor<SpacePacket>>,
}

/// Binds and starts both services from the ground.
async fn connect_ground(raf_addr: SocketAddr, fcltu_addr: SocketAddr) -> Ground {
    let telemetry = TestActor::<SpacePacket>::spawn(TestActor::new());
    let tm_decoder = TmFrameDecoder::spawn(TmFrameDecoder::new(
        TmFrameConfig::new(SPACECRAFT_ID, 0),
        telemetry.clone().recipient::<Batch<SpacePacket>>(),
    ));
    let raf_user = spawn_user(raf_addr, RAF_SI, Some(tm_decoder));
    raf_user.ask(SleBind).await.unwrap();
    raf_user
        .ask(RafStart {
            requested_quality: RequestedFrameQuality::GoodFramesOnly,
        })
        .await
        .unwrap();

    let fcltu_user = spawn_user(fcltu_addr, CLTU_SI, None);
    fcltu_user.ask(SleBind).await.unwrap();
    fcltu_user.ask(CltuStart { first_cltu_id: 0 }).await.unwrap();
    let telecommands = TcFrameEncoder::spawn(TcFrameEncoder::new(
        TcFrameConfig::new(SPACECRAFT_ID, 0),
        0,
        fcltu_user.recipient::<Cltu>(),
    ));
    Ground {
        telecommands,
        telemetry,
    }
}

/// The telemetry received so far as PUS TM packets.
async fn received_tm(ground: &Ground, count: usize) -> Vec<PusTm> {
    TestActor::assert_received(&ground.telemetry, count, TIMEOUT)
        .await
        .into_iter()
        .map(
            |packet| match PusPacket::from_space_packet(packet, &PusConfig::default()).unwrap() {
                PusPacket::Tm(tm) => tm,
                other => panic!("expected TM, got {other:?}"),
            },
        )
        .collect()
}

fn service(tm: &PusTm) -> (u8, u8) {
    (tm.secondary_header.service_type, tm.secondary_header.message_subtype)
}

#[tokio::test]
async fn are_you_alive_over_sle() {
    let (raf_addr, fcltu_addr) = spawn_spacecraft().await;
    let ground = connect_ground(raf_addr, fcltu_addr).await;

    let tc = PusTc::new(
        APID,
        1,
        service17::SERVICE_TYPE,
        service17::ARE_YOU_ALIVE_REQUEST_SUBTYPE,
        Bytes::new(),
    );
    ground.telecommands.tell(PusPacket::from(tc.clone())).await.unwrap();

    let tm = received_tm(&ground, 4).await;
    assert_eq!(
        tm.iter().map(service).collect::<Vec<_>>(),
        [(1, 1), (1, 3), (17, 2), (1, 7)],
        "acceptance, start, Are-You-Alive report, completion"
    );
    for (n, packet) in tm.iter().enumerate() {
        assert_eq!(packet.header.apid, APID);
        assert_eq!(packet.header.sequence_count, n as u16, "consecutive sequence counts");
    }
    for report in [&tm[0], &tm[1], &tm[3]] {
        let report = VerificationReport::try_from(report.clone()).unwrap();
        assert_eq!(report.request_id, RequestId::from(&tc), "the reports refer to the TC");
    }

    // A second TC goes through as well; the frames and CLTUs are numbered on.
    let tc = PusTc::new(APID, 2, 17, 1, Bytes::new());
    ground.telecommands.tell(PusPacket::from(tc)).await.unwrap();
    let tm = received_tm(&ground, 8).await;
    assert_eq!(tm[6].secondary_header.service_type, service17::SERVICE_TYPE);
    assert_eq!(tm[7].header.sequence_count, 7);
}

#[tokio::test]
async fn an_unsupported_tc_is_rejected_over_sle() {
    let (raf_addr, fcltu_addr) = spawn_spacecraft().await;
    let ground = connect_ground(raf_addr, fcltu_addr).await;

    // Service 17 has no subtype 99.
    let tc = PusTc::new(APID, 5, service17::SERVICE_TYPE, 99, Bytes::new());
    ground.telecommands.tell(PusPacket::from(tc.clone())).await.unwrap();

    let tm = received_tm(&ground, 1).await;
    assert_eq!(service(&tm[0]), (service1::SERVICE_TYPE, 2), "acceptance failure");
    let report = VerificationReport::try_from(tm[0].clone()).unwrap();
    assert_eq!(report.request_id, RequestId::from(&tc));
    assert!(matches!(report.kind, VerificationKind::AcceptanceFailure(_)));
}
