//! Tests for the SLE provider actors on top of a real `TmlServer` and
//! `TmlLinkActor`. The SLE user is a plain `Framed` TCP stream with the
//! `TmlCodec` that encodes the PDUs with `groundlink::protocol::sle`.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use groundlink::actor::sle::Cltu;
use groundlink::actor::tcp::GetLocalAddr;
use groundlink::protocol::sle::common::{
    Acknowledgement, BindInvocation, BindReturn, StopInvocation, UnbindInvocation,
};
use groundlink::protocol::sle::common::{
    ConditionalTime, Credentials, Diagnostic, Time, bind_diagnostic, service_type, unbind_reason,
};
use groundlink::protocol::sle::common::{
    ReportRequestType, ScheduleStatusReportInvocation, ScheduleStatusReportReturn, schedule_status_report_diagnostic,
};
use groundlink::protocol::sle::fcltu::{
    CltuGetParameterInvocation, CltuLastOk, CltuNotification, CltuParameter, CltuProductionStatus, UplinkStatus,
    parameter_name, transfer_diagnostic,
};
use groundlink::protocol::sle::fcltu::{
    CltuStartInvocation, CltuStartReturn, CltuTransferDataInvocation, CltuTransferDataReturn, FcltuProviderPdu,
    FcltuUserPdu,
};
use groundlink::protocol::sle::raf::{AntennaId, FrameQuality};
use groundlink::protocol::sle::raf::{LockStatus, RafProductionStatus};
use groundlink::protocol::sle::raf::{
    RafFrame, RafProviderPdu, RafStartInvocation, RafStartReturn, RafUserPdu, RequestedFrameQuality,
};
use groundlink::protocol::tml::TmlContext;
use groundlink::{
    Batch, ConnectionEvent, FcltuConfig, FcltuServiceInstance, RafConfig, RafServiceInstance, SleAssociationActor,
    SleAssociationConfig, SlePdu, SleProviderEvent, SleServiceRegistry, TcpServerArgs, TestActor, TmlCodec,
    TmlLinkActor, TmlMessage, TmlResponderConfig, TmlServer,
};
use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::mailbox;
use kameo::message::{Context, Message};
use tokio::net::TcpStream;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::codec::Framed;

type User = Framed<TcpStream, TmlCodec>;

const RAF_SI: &str = "sagr=1.spack=VST-PASS0001.rsl-fg=1.raf=onlt1";
const CLTU_SI: &str = "sagr=1.spack=VST-PASS0001.fsl-fg=1.cltu=cltu1";
const TIMEOUT: Duration = Duration::from_secs(5);

/// The radiation side: takes a CLTU only when the test opens the gate.
struct Radiator {
    gate: Arc<Semaphore>,
    radiated: mpsc::UnboundedSender<Cltu>,
}

impl Actor for Radiator {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<Cltu> for Radiator {
    type Reply = ();

    async fn handle(&mut self, cltu: Cltu, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.gate.acquire().await.unwrap().forget();
        let _ = self.radiated.send(cltu);
    }
}

struct Provider {
    registry: SleServiceRegistry,
    raf: ActorRef<RafServiceInstance>,
    gate: Arc<Semaphore>,
    radiated: mpsc::UnboundedReceiver<Cltu>,
    events: ActorRef<TestActor<SleProviderEvent>>,
}

/// A RAF instance (4 frames per buffer, 50 ms latency, only `MOC1` may
/// bind) and an FCLTU instance (buffer of 2 CLTUs, gated radiator).
fn spawn_instances() -> Provider {
    let raf_config = RafConfig::new(RAF_SI.parse().unwrap())
        .with_transfer_buffer_size(4)
        .with_latency_limit(Duration::from_millis(50))
        .with_allowed_initiators(["MOC1"]);
    let raf = RafServiceInstance::spawn(RafServiceInstance::new(raf_config).with_tick(Duration::from_millis(10)));

    let gate = Arc::new(Semaphore::new(0));
    let (radiated_tx, radiated) = mpsc::unbounded_channel();
    let radiator = Radiator::spawn_with_mailbox(
        Radiator {
            gate: gate.clone(),
            radiated: radiated_tx,
        },
        mailbox::bounded(1),
    );
    let cltu_config = FcltuConfig::new(CLTU_SI.parse().unwrap()).with_buffer_capacity(2);
    let fcltu = FcltuServiceInstance::spawn(FcltuServiceInstance::new(cltu_config, radiator.recipient()));

    let registry = SleServiceRegistry::new()
        .with_raf(RAF_SI.parse().unwrap(), raf.clone())
        .with_fcltu(CLTU_SI.parse().unwrap(), fcltu);
    Provider {
        registry,
        raf,
        gate,
        radiated,
        events: TestActor::spawn(TestActor::new()),
    }
}

/// A provider port: server, link actor and association actor.
fn spawn_port(provider: &Provider) -> ActorRef<TmlServer> {
    spawn_port_with_bind_timeout(provider, Duration::from_secs(60))
}

fn spawn_port_with_bind_timeout(provider: &Provider, bind_timeout: Duration) -> ActorRef<TmlServer> {
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
        .with_events(association.actor_ref().clone().recipient())
        .with_tick(Duration::from_millis(50)),
    );
    let config = SleAssociationConfig::default()
        .with_responder_id("PROV")
        .with_bind_timeout(bind_timeout);
    association.spawn(
        SleAssociationActor::new(link_ref, provider.registry.clone(), config)
            .with_events(provider.events.clone().recipient()),
    );
    server
}

/// Connects and sends the TML context message.
async fn connect(server: &ActorRef<TmlServer>) -> User {
    let addr = server.ask(GetLocalAddr).await.unwrap();
    let mut user = Framed::new(TcpStream::connect(addr).await.unwrap(), TmlCodec::default());
    user.send(TmlMessage::Context(TmlContext::new(0, 0))).await.unwrap();
    user
}

async fn send(user: &mut User, pdu: Bytes) {
    user.send(TmlMessage::SlePdu(pdu)).await.unwrap();
}

/// The next SLE PDU; `None` when the provider closed the connection.
async fn recv(user: &mut User) -> Option<Bytes> {
    loop {
        match tokio::time::timeout(TIMEOUT, user.next()).await.expect("timed out") {
            Some(Ok(TmlMessage::SlePdu(pdu))) => return Some(pdu),
            Some(Ok(_)) => continue,
            Some(Err(_)) | None => return None,
        }
    }
}

async fn recv_raf(user: &mut User) -> RafProviderPdu {
    RafProviderPdu::decode(recv(user).await.expect("connection closed")).unwrap()
}

async fn recv_fcltu(user: &mut User) -> FcltuProviderPdu {
    FcltuProviderPdu::decode(recv(user).await.expect("connection closed")).unwrap()
}

/// Waits until the provider has closed the connection.
async fn assert_closed(user: &mut User) {
    assert_eq!(recv(user).await, None);
}

fn bind(si: &str, service_type: i64, version: u16) -> BindInvocation {
    BindInvocation {
        credentials: Credentials::Unused,
        initiator_id: "MOC1".into(),
        responder_port: "PORT1".into(),
        service_type,
        version,
        service_instance: si.parse().unwrap(),
    }
}

async fn bind_raf(server: &ActorRef<TmlServer>) -> User {
    let mut user = connect(server).await;
    send(&mut user, bind(RAF_SI, service_type::RTN_ALL_FRAMES, 5).encode_pdu()).await;
    let RafProviderPdu::BindReturn(ret) = recv_raf(&mut user).await else {
        panic!("expected BIND return")
    };
    assert_eq!(ret.result, Ok(5));
    assert_eq!(ret.responder_id, "PROV");
    user
}

async fn start_raf(user: &mut User, invoke_id: u16, requested_quality: RequestedFrameQuality) {
    let start = RafUserPdu::Start(RafStartInvocation {
        credentials: Credentials::Unused,
        invoke_id,
        start_time: ConditionalTime::Undefined,
        stop_time: ConditionalTime::Undefined,
        requested_quality,
    });
    send(user, start.encode()).await;
    assert_eq!(
        recv_raf(user).await,
        RafProviderPdu::StartReturn(RafStartReturn {
            credentials: Credentials::Unused,
            invoke_id,
            result: Ok(()),
        })
    );
}

fn stop(invoke_id: u16) -> StopInvocation {
    StopInvocation {
        credentials: Credentials::Unused,
        invoke_id,
    }
}

fn unbind() -> UnbindInvocation {
    UnbindInvocation {
        credentials: Credentials::Unused,
        reason: unbind_reason::END,
    }
}

fn frame(n: u8, quality: FrameQuality) -> RafFrame {
    RafFrame {
        earth_receive_time: Time::now(),
        antenna_id: AntennaId::Local(Bytes::from_static(b"ANT1")),
        data_link_continuity: 0,
        quality,
        private_annotation: None,
        data: Bytes::from(vec![n; 1115]),
    }
}

/// Waits for `count` events, then returns all received so far.
async fn events(provider: &Provider, count: usize) -> Vec<SleProviderEvent> {
    TestActor::assert_received(&provider.events, count, TIMEOUT).await
}

/// Waits for the first `Aborted` event and returns its fields.
async fn aborted(provider: &Provider) -> (Option<String>, String) {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    loop {
        let events = TestActor::assert_received(&provider.events, 0, TIMEOUT).await;
        if let Some(SleProviderEvent::Aborted {
            service_instance,
            reason,
        }) = events
            .into_iter()
            .find(|e| matches!(e, SleProviderEvent::Aborted { .. }))
        {
            return (service_instance.map(|si| si.to_string()), reason);
        }
        assert!(tokio::time::Instant::now() < deadline, "no Aborted event");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn raf_bind_start_transfer_stop_unbind() {
    let provider = spawn_instances();
    let port = spawn_port(&provider);
    let mut user = bind_raf(&port).await;

    // Frames before START are discarded.
    provider.raf.tell(frame(0, FrameQuality::Good)).await.unwrap();
    start_raf(&mut user, 1, RequestedFrameQuality::GoodFramesOnly).await;

    // 5 good and 1 erred frame: one full buffer of 4, the 5th after the
    // latency limit. The erred frame is filtered out.
    for n in 1..=3 {
        provider.raf.tell(frame(n, FrameQuality::Good)).await.unwrap();
    }
    provider
        .raf
        .tell(Batch::from(vec![
            frame(99, FrameQuality::Erred),
            frame(4, FrameQuality::Good),
            frame(5, FrameQuality::Good),
        ]))
        .await
        .unwrap();

    let mut buffers = Vec::new();
    let mut got = Vec::new();
    while got.len() < 5 {
        match recv_raf(&mut user).await {
            RafProviderPdu::TransferBuffer(frames) => {
                buffers.push(frames.len());
                got.extend(frames);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(buffers, [4, 1]);
    let ids: Vec<u8> = got.iter().map(|f| f.data[0]).collect();
    assert_eq!(ids, [1, 2, 3, 4, 5]);
    assert_eq!(got[0].data_link_continuity, -1);
    assert_eq!(got[1].data_link_continuity, 0);

    send(&mut user, RafUserPdu::Stop(stop(2)).encode()).await;
    assert_eq!(
        recv_raf(&mut user).await,
        RafProviderPdu::StopReturn(Acknowledgement::positive(2))
    );
    // Not active any more: frames are discarded.
    provider.raf.tell(frame(6, FrameQuality::Good)).await.unwrap();

    send(&mut user, RafUserPdu::Unbind(unbind()).encode()).await;
    assert!(matches!(recv_raf(&mut user).await, RafProviderPdu::UnbindReturn(_)));
    assert_closed(&mut user).await;

    let si = RAF_SI.parse().unwrap();
    assert_eq!(
        events(&provider, 4).await,
        vec![
            SleProviderEvent::Bound {
                service_instance: RAF_SI.parse().unwrap(),
                initiator: "MOC1".to_string(),
                version: 5,
            },
            SleProviderEvent::Started {
                service_instance: RAF_SI.parse().unwrap(),
            },
            SleProviderEvent::Stopped {
                service_instance: RAF_SI.parse().unwrap(),
            },
            SleProviderEvent::Unbound { service_instance: si },
        ]
    );
}

#[tokio::test]
async fn raf_stop_sends_the_collected_frames_first() {
    let provider = spawn_instances();
    let port = spawn_port(&provider);
    let mut user = bind_raf(&port).await;
    start_raf(&mut user, 1, RequestedFrameQuality::AllFrames).await;

    // Fewer frames than a buffer holds, and STOP before the latency limit.
    provider.raf.tell(frame(1, FrameQuality::Good)).await.unwrap();
    provider.raf.tell(frame(2, FrameQuality::Erred)).await.unwrap();
    send(&mut user, RafUserPdu::Stop(stop(2)).encode()).await;

    let RafProviderPdu::TransferBuffer(frames) = recv_raf(&mut user).await else {
        panic!("expected the transfer buffer before the STOP return")
    };
    assert_eq!(frames.iter().map(|f| f.data[0]).collect::<Vec<_>>(), [1, 2]);
    assert_eq!(
        recv_raf(&mut user).await,
        RafProviderPdu::StopReturn(Acknowledgement::positive(2))
    );
}

#[tokio::test]
async fn bind_rejections_close_the_connection() {
    let provider = spawn_instances();
    let port = spawn_port(&provider);
    let intruder = BindInvocation {
        initiator_id: "INTRUDER".into(),
        ..bind(RAF_SI, service_type::RTN_ALL_FRAMES, 5)
    };
    let cases = [
        (
            bind("sagr=1.spack=X.rsl-fg=1.raf=nope", service_type::RTN_ALL_FRAMES, 5),
            bind_diagnostic::NO_SUCH_SERVICE_INSTANCE,
        ),
        (
            bind(RAF_SI, service_type::FWD_CLTU, 5),
            bind_diagnostic::INCONSISTENT_SERVICE_TYPE,
        ),
        (
            bind(RAF_SI, service_type::RTN_ALL_FRAMES, 1),
            bind_diagnostic::VERSION_NOT_SUPPORTED,
        ),
        (intruder, bind_diagnostic::ACCESS_DENIED),
    ];
    for (i, (bind, diagnostic)) in cases.into_iter().enumerate() {
        let mut user = connect(&port).await;
        send(&mut user, bind.encode_pdu()).await;
        match recv_raf(&mut user).await {
            RafProviderPdu::BindReturn(ret) => assert_eq!(ret.result, Err(diagnostic), "{bind:?}"),
            other => panic!("unexpected {other:?}"),
        }
        assert_closed(&mut user).await;
        let events = events(&provider, i + 1).await;
        assert_eq!(
            events[i],
            SleProviderEvent::BindRejected {
                service_instance: bind.service_instance.clone(),
                initiator: bind.initiator_id.clone(),
                diagnostic,
            }
        );
    }

    // None of them has bound the instance.
    let mut user = bind_raf(&port).await;
    send(&mut user, RafUserPdu::Unbind(unbind()).encode()).await;
    assert!(matches!(recv_raf(&mut user).await, RafProviderPdu::UnbindReturn(_)));
}

#[tokio::test]
async fn a_second_bind_gets_already_bound() {
    let provider = spawn_instances();
    let first_port = spawn_port(&provider);
    let second_port = spawn_port(&provider);
    let mut first = bind_raf(&first_port).await;

    let mut second = connect(&second_port).await;
    send(&mut second, bind(RAF_SI, service_type::RTN_ALL_FRAMES, 5).encode_pdu()).await;
    match recv_raf(&mut second).await {
        RafProviderPdu::BindReturn(ret) => assert_eq!(ret.result, Err(bind_diagnostic::ALREADY_BOUND)),
        other => panic!("unexpected {other:?}"),
    }
    assert_closed(&mut second).await;

    // Once the first user has unbound, the second can bind.
    send(&mut first, RafUserPdu::Unbind(unbind()).encode()).await;
    assert!(matches!(recv_raf(&mut first).await, RafProviderPdu::UnbindReturn(_)));
    bind_raf(&second_port).await;
}

#[tokio::test]
async fn a_protocol_error_aborts_the_association() {
    let provider = spawn_instances();
    let port = spawn_port(&provider);
    let mut user = bind_raf(&port).await;

    // STOP without START.
    send(&mut user, RafUserPdu::Stop(stop(1)).encode()).await;
    assert_closed(&mut user).await;
    let (service_instance, reason) = aborted(&provider).await;
    assert_eq!(service_instance.as_deref(), Some(RAF_SI));
    assert!(reason.contains("STOP"), "{reason}");

    // The abort has released the instance.
    bind_raf(&port).await;
}

#[tokio::test]
async fn a_pdu_before_bind_aborts_with_a_protocol_error() {
    let provider = spawn_instances();
    let port = spawn_port(&provider);
    let mut user = connect(&port).await;

    send(&mut user, RafUserPdu::Stop(stop(1)).encode()).await;
    assert_closed(&mut user).await;
    let (service_instance, reason) = aborted(&provider).await;
    assert_eq!(service_instance, None);
    assert!(reason.contains("STOP"), "{reason}");
}

#[tokio::test]
async fn a_missing_bind_times_out() {
    let provider = spawn_instances();
    let port = spawn_port_with_bind_timeout(&provider, Duration::from_millis(200));
    let mut user = connect(&port).await;

    assert_closed(&mut user).await;
    let (service_instance, reason) = aborted(&provider).await;
    assert_eq!(service_instance, None);
    assert!(reason.contains("no BIND"), "{reason}");

    // A user that binds in time is not affected by the earlier timer.
    let mut user = bind_raf(&port).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    start_raf(&mut user, 1, RequestedFrameQuality::AllFrames).await;
}

#[tokio::test]
async fn malformed_and_unsupported_pdus_abort_the_association() {
    let provider = spawn_instances();
    let port = spawn_port(&provider);

    let mut user = bind_raf(&port).await;
    send(&mut user, Bytes::from_static(&[0xa0, 0x05, 0x80])).await;
    assert_closed(&mut user).await;
    let (_, reason) = aborted(&provider).await;
    assert!(reason.contains("truncated"), "{reason}");

    // GET-PARAMETER is not implemented.
    let mut user = bind_raf(&port).await;
    send(&mut user, Bytes::from_static(&[0xa6, 0x02, 0x80, 0x00])).await;
    assert_closed(&mut user).await;
    let events = TestActor::assert_received(&provider.events, 4, TIMEOUT).await;
    let SleProviderEvent::Aborted { reason, .. } = &events[3] else {
        panic!("expected Aborted, got {events:?}")
    };
    assert!(reason.contains("unsupported"), "{reason}");
}

#[tokio::test]
async fn losing_the_link_aborts_the_association() {
    let provider = spawn_instances();
    let port = spawn_port(&provider);
    let mut user = bind_raf(&port).await;
    start_raf(&mut user, 1, RequestedFrameQuality::AllFrames).await;

    drop(user);
    let (service_instance, reason) = aborted(&provider).await;
    assert_eq!(service_instance.as_deref(), Some(RAF_SI));
    assert!(reason.contains("connection closed"), "{reason}");

    // The instance is free for the next user, and inactive until START.
    let mut user = bind_raf(&port).await;
    provider.raf.tell(frame(1, FrameQuality::Good)).await.unwrap();
    start_raf(&mut user, 2, RequestedFrameQuality::AllFrames).await;
    provider.raf.tell(frame(2, FrameQuality::Good)).await.unwrap();
    let RafProviderPdu::TransferBuffer(frames) = recv_raf(&mut user).await else {
        panic!("expected a transfer buffer")
    };
    assert_eq!(frames[0].data[0], 2);
}

async fn bind_and_start_fcltu(server: &ActorRef<TmlServer>, first_cltu_id: u32) -> User {
    let mut user = connect(server).await;
    send(
        &mut user,
        FcltuUserPdu::Bind(bind(CLTU_SI, service_type::FWD_CLTU, 5)).encode(),
    )
    .await;
    assert!(matches!(
        recv_fcltu(&mut user).await,
        FcltuProviderPdu::BindReturn(BindReturn { result: Ok(5), .. })
    ));
    let start = FcltuUserPdu::Start(CltuStartInvocation {
        credentials: Credentials::Unused,
        invoke_id: 1,
        first_cltu_id,
    });
    send(&mut user, start.encode()).await;
    assert!(matches!(
        recv_fcltu(&mut user).await,
        FcltuProviderPdu::StartReturn(CltuStartReturn {
            invoke_id: 1,
            result: Ok(_),
            ..
        })
    ));
    user
}

fn cltu(invoke_id: u16, cltu_id: u32) -> CltuTransferDataInvocation {
    CltuTransferDataInvocation {
        credentials: Credentials::Unused,
        invoke_id,
        cltu_id,
        earliest_transmission_time: ConditionalTime::Undefined,
        latest_transmission_time: ConditionalTime::Undefined,
        delay_time_us: 0,
        produce_notification: false,
        data: Bytes::from(vec![0xeb, 0x90, cltu_id as u8, 0xc5]),
    }
}

async fn transfer(user: &mut User, invocation: CltuTransferDataInvocation) -> CltuTransferDataReturn {
    send(user, FcltuUserPdu::TransferData(invocation).encode()).await;
    match recv_fcltu(user).await {
        FcltuProviderPdu::TransferDataReturn(ret) => ret,
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn fcltu_checks_the_sequence_and_reports_a_full_buffer() {
    let mut provider = spawn_instances();
    let port = spawn_port(&provider);
    let mut user = bind_and_start_fcltu(&port, 10).await;

    let ret = transfer(&mut user, cltu(2, 10)).await;
    assert_eq!((ret.invoke_id, ret.result, ret.cltu_id), (2, Ok(()), 11));
    // Out of sequence: the provider still expects 11.
    let ret = transfer(&mut user, cltu(3, 13)).await;
    assert_eq!(
        (ret.result, ret.cltu_id),
        (Err(Diagnostic::Specific(transfer_diagnostic::OUT_OF_SEQUENCE)), 11)
    );

    // Nothing is radiated while the gate is closed, so the buffer fills
    // up: 2 CLTUs in the buffer plus the few on their way to the radiator.
    let mut next = 11;
    let full = loop {
        let ret = transfer(&mut user, cltu(next as u16, next)).await;
        if ret.result.is_err() {
            break ret;
        }
        assert_eq!(ret.cltu_id, next + 1);
        next += 1;
        assert!(next < 20, "the buffer never got full");
    };
    assert!(next >= 13, "only {} CLTU(s) accepted", next - 10);
    assert_eq!(
        full.result,
        Err(Diagnostic::Specific(transfer_diagnostic::UNABLE_TO_STORE))
    );
    assert_eq!((full.cltu_id, full.buffer_available), (next, 0));

    // Opening the gate radiates the accepted CLTUs in order.
    provider.gate.add_permits(100);
    for id in 10..next {
        let radiated = tokio::time::timeout(TIMEOUT, provider.radiated.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (radiated.id, &radiated.data[..]),
            (id, &[0xeb, 0x90, id as u8, 0xc5][..])
        );
    }

    send(&mut user, FcltuUserPdu::Stop(stop(50)).encode()).await;
    assert_eq!(
        recv_fcltu(&mut user).await,
        FcltuProviderPdu::StopReturn(Acknowledgement::positive(50))
    );
    send(&mut user, FcltuUserPdu::Unbind(unbind()).encode()).await;
    assert!(matches!(recv_fcltu(&mut user).await, FcltuProviderPdu::UnbindReturn(_)));
    assert_closed(&mut user).await;
}

#[tokio::test]
async fn fcltu_rejects_invalid_cltus() {
    let provider = spawn_instances();
    let port = spawn_port(&provider);
    let mut user = bind_and_start_fcltu(&port, 0).await;
    let specific = |d| Err(Diagnostic::Specific(d));

    let empty = CltuTransferDataInvocation {
        data: Bytes::new(),
        ..cltu(1, 0)
    };
    assert_eq!(
        transfer(&mut user, empty).await.result,
        specific(transfer_diagnostic::CLTU_ERROR)
    );

    let later = Time::from_system_time(std::time::SystemTime::now() + Duration::from_secs(3600));
    let earlier = Time::from_system_time(std::time::SystemTime::now() + Duration::from_secs(60));
    let inconsistent = CltuTransferDataInvocation {
        earliest_transmission_time: ConditionalTime::Known(later),
        latest_transmission_time: ConditionalTime::Known(earlier),
        ..cltu(2, 0)
    };
    assert_eq!(
        transfer(&mut user, inconsistent).await.result,
        specific(transfer_diagnostic::INCONSISTENT_TIME_RANGE)
    );

    let late = CltuTransferDataInvocation {
        latest_transmission_time: ConditionalTime::Known(Time::Ccsds([0; 8])),
        ..cltu(3, 0)
    };
    assert_eq!(
        transfer(&mut user, late).await.result,
        specific(transfer_diagnostic::LATE_SLDU)
    );

    // After all that, CLTU 0 is still the one expected.
    let ret = transfer(&mut user, cltu(4, 0)).await;
    assert_eq!((ret.result, ret.cltu_id), (Ok(()), 1));

    // TRANSFER-DATA after STOP is a protocol error.
    send(&mut user, FcltuUserPdu::Stop(stop(5)).encode()).await;
    recv_fcltu(&mut user).await;
    send(&mut user, FcltuUserPdu::TransferData(cltu(6, 1)).encode()).await;
    assert_closed(&mut user).await;
    let (service_instance, reason) = aborted(&provider).await;
    assert_eq!(service_instance.as_deref(), Some(CLTU_SI));
    assert!(reason.contains("TRANSFER-DATA"), "{reason}");
}

/// The next FCLTU PDU that is not a periodic status report.
async fn recv_fcltu_reply(user: &mut User) -> FcltuProviderPdu {
    loop {
        match recv_fcltu(user).await {
            FcltuProviderPdu::StatusReport(_) => continue,
            pdu => return pdu,
        }
    }
}

fn schedule_status_report(invoke_id: u16, request: ReportRequestType) -> ScheduleStatusReportInvocation {
    ScheduleStatusReportInvocation {
        credentials: Credentials::Unused,
        invoke_id,
        request,
    }
}

/// The user side of the FCLTU service as ReatMetric drives it: periodic
/// status reports for the buffer size, radiation notifications to free it,
/// GET-PARAMETER for the expected CLTU.
#[tokio::test]
async fn fcltu_reports_status_notifies_radiation_and_answers_get_parameter() {
    let provider = spawn_instances();
    let port = spawn_port(&provider);
    let mut user = bind_and_start_fcltu(&port, 20).await;

    let ssr = FcltuUserPdu::ScheduleStatusReport(schedule_status_report(2, ReportRequestType::Periodically(2)));
    send(&mut user, ssr.encode()).await;
    assert_eq!(
        recv_fcltu(&mut user).await,
        FcltuProviderPdu::ScheduleStatusReportReturn(ScheduleStatusReportReturn {
            credentials: Credentials::Unused,
            invoke_id: 2,
            result: Ok(()),
        })
    );
    // The first report comes at once and tells the buffer size.
    let FcltuProviderPdu::StatusReport(report) = recv_fcltu(&mut user).await else {
        panic!("expected a CLTU-STATUS-REPORT")
    };
    assert_eq!(report.buffer_available, 2 * 4096);
    assert_eq!(report.production_status, CltuProductionStatus::Operational);
    assert_eq!(report.uplink_status, UplinkStatus::Nominal);
    assert_eq!(report.last_ok, CltuLastOk::None);

    // With notification: the return, then CLTU-ASYNC-NOTIFY once the
    // radiator has taken the CLTU.
    let with_notification = CltuTransferDataInvocation {
        produce_notification: true,
        ..cltu(3, 20)
    };
    send(&mut user, FcltuUserPdu::TransferData(with_notification).encode()).await;
    let FcltuProviderPdu::TransferDataReturn(ret) = recv_fcltu_reply(&mut user).await else {
        panic!("expected the TRANSFER-DATA return before the notification")
    };
    assert_eq!((ret.invoke_id, ret.result, ret.cltu_id), (3, Ok(()), 21));
    let FcltuProviderPdu::AsyncNotify(notify) = recv_fcltu_reply(&mut user).await else {
        panic!("expected CLTU-ASYNC-NOTIFY")
    };
    assert_eq!(notify.notification, CltuNotification::CltuRadiated);
    assert!(matches!(notify.last_ok, CltuLastOk::Ok { cltu_id: 20, .. }));

    // Without notification: only the return.
    send(&mut user, FcltuUserPdu::TransferData(cltu(4, 21)).encode()).await;
    assert!(matches!(
        recv_fcltu_reply(&mut user).await,
        FcltuProviderPdu::TransferDataReturn(CltuTransferDataReturn { invoke_id: 4, .. })
    ));

    let get = |invoke_id, parameter| {
        FcltuUserPdu::GetParameter(CltuGetParameterInvocation {
            credentials: Credentials::Unused,
            invoke_id,
            parameter,
        })
        .encode()
    };
    send(&mut user, get(5, parameter_name::EXPECTED_SLDU_IDENTIFICATION)).await;
    let FcltuProviderPdu::GetParameterReturn(ret) = recv_fcltu_reply(&mut user).await else {
        panic!("expected the GET-PARAMETER return")
    };
    assert_eq!(ret.result, Ok(CltuParameter::ExpectedSlduIdentification(22)));
    send(&mut user, get(6, 999)).await;
    let FcltuProviderPdu::GetParameterReturn(ret) = recv_fcltu_reply(&mut user).await else {
        panic!("expected the GET-PARAMETER return")
    };
    assert_eq!(ret.result, Err(Diagnostic::Specific(0)), "unknownParameter");

    // The next periodic report counts the CLTUs. Notifications and
    // reports may come in any order, so skip to the report.
    let report = loop {
        match recv_fcltu(&mut user).await {
            FcltuProviderPdu::StatusReport(report) => break report,
            FcltuProviderPdu::AsyncNotify(_) => continue,
            other => panic!("unexpected {other:?}"),
        }
    };
    assert_eq!(report.cltus_received, 2);
    assert!(report.cltus_radiated >= 1, "{report:?}");

    let stop_reports = FcltuUserPdu::ScheduleStatusReport(schedule_status_report(7, ReportRequestType::Stop));
    send(&mut user, stop_reports.encode()).await;
    let result = loop {
        match recv_fcltu(&mut user).await {
            FcltuProviderPdu::ScheduleStatusReportReturn(ret) => break ret.result,
            FcltuProviderPdu::StatusReport(_) | FcltuProviderPdu::AsyncNotify(_) => continue,
            other => panic!("unexpected {other:?}"),
        }
    };
    assert_eq!(result, Ok(()));
    send(&mut user, stop_reports.encode()).await;
    let FcltuProviderPdu::ScheduleStatusReportReturn(ret) = recv_fcltu(&mut user).await else {
        panic!("expected the SCHEDULE-STATUS-REPORT return")
    };
    assert_eq!(
        ret.result,
        Err(Diagnostic::Specific(schedule_status_report_diagnostic::ALREADY_STOPPED))
    );

    let too_fast = FcltuUserPdu::ScheduleStatusReport(schedule_status_report(8, ReportRequestType::Periodically(1)));
    send(&mut user, too_fast.encode()).await;
    let FcltuProviderPdu::ScheduleStatusReportReturn(ret) = recv_fcltu(&mut user).await else {
        panic!("expected the SCHEDULE-STATUS-REPORT return")
    };
    assert_eq!(
        ret.result,
        Err(Diagnostic::Specific(
            schedule_status_report_diagnostic::INVALID_REPORTING_CYCLE
        ))
    );
    drop(provider);
}

#[tokio::test]
async fn raf_reports_its_status_immediately() {
    let provider = spawn_instances();
    let port = spawn_port(&provider);
    let mut user = bind_raf(&port).await;
    start_raf(&mut user, 1, RequestedFrameQuality::GoodFramesOnly).await;
    provider.raf.tell(frame(1, FrameQuality::Good)).await.unwrap();
    provider.raf.tell(frame(2, FrameQuality::Erred)).await.unwrap();
    // The frame goes out after the latency limit.
    assert!(matches!(recv_raf(&mut user).await, RafProviderPdu::TransferBuffer(_)));

    let ssr = RafUserPdu::ScheduleStatusReport(schedule_status_report(2, ReportRequestType::Immediately));
    send(&mut user, ssr.encode()).await;
    assert!(matches!(
        recv_raf(&mut user).await,
        RafProviderPdu::ScheduleStatusReportReturn(ScheduleStatusReportReturn { result: Ok(()), .. })
    ));
    let RafProviderPdu::StatusReport(report) = recv_raf(&mut user).await else {
        panic!("expected a RAF-STATUS-REPORT")
    };
    assert_eq!((report.error_free_frame_number, report.delivered_frame_number), (1, 1));
    assert_eq!(report.frame_sync_lock_status, LockStatus::InLock);
    assert_eq!(report.production_status, RafProductionStatus::Running);
}
