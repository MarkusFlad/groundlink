//! Tests for `PusTcAcceptor`: acknowledges TCs for its APID with TM(1,1),
//! rejects unsupported ones with TM(1,2) and reports failed forwarding
//! with TM(1,10).

use std::time::Duration;

use bytes::Bytes;
use futures::SinkExt;
use kameo::actor::{ActorRef, Recipient, Spawn};
use kameo_tcp_example::{
    AckFlags, CucFormat, CucTime, FailureCode, GetLocalAddr, GetMessages, PusCodec, PusServer,
    PusPacket, PusTc, PusTcAcceptor, PusTm, RequestId, TcpServerArgs, TestActor,
    VerificationKind, VerificationReport,
};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

const APID: u16 = 0x042;

/// Acceptor that accepts TC(17,1) and forwards it to the returned test
/// handler.
fn acceptor(reports: Recipient<VerificationReport>) -> (PusTcAcceptor, ActorRef<TestActor<PusTc>>) {
    let handler = TestActor::<PusTc>::spawn(TestActor::new());
    let acceptor = PusTcAcceptor::new(APID, reports).with_service_handler(17, &[1], handler.clone().recipient());
    (acceptor, handler)
}

#[tokio::test]
async fn tc_for_own_apid_is_acknowledged_with_tm_1_1() {
    let reports = TestActor::<VerificationReport>::spawn(TestActor::new());
    let (acceptor, _handler) = acceptor(reports.clone().recipient());
    let acceptor = PusTcAcceptor::spawn(acceptor);

    let mut tc = PusTc::new(APID, 17, 17, 1, Bytes::new());
    tc.secondary_header.source_id = 0x0815;
    acceptor.ask(PusPacket::from(tc.clone())).await.unwrap();

    let received = reports.ask(GetMessages::new()).await.unwrap();
    assert_eq!(received.len(), 1);
    let report = &received[0];
    assert_eq!(report.kind, VerificationKind::AcceptanceSuccess);
    assert_eq!(report.request_id, RequestId::from(&tc));
    assert_eq!(report.header.apid, APID);
    assert_eq!(report.destination_id, 0x0815);

    let time = CucTime::from_bytes(&report.time, CucFormat::default()).unwrap();
    let age = chrono::Utc::now() - time.to_utc().unwrap();
    assert!(age < chrono::Duration::seconds(1), "time stamp should be current: {time}");

    // The report can be encoded as a PUS packet.
    PusPacket::try_from(report.clone()).unwrap();
}

#[tokio::test]
async fn ignores_other_apid_telemetry_and_tc_without_acceptance_flag() {
    let reports = TestActor::<VerificationReport>::spawn(TestActor::new());
    let (acceptor, _handler) = acceptor(reports.clone().recipient());
    let acceptor = PusTcAcceptor::spawn(acceptor);

    acceptor.ask(PusPacket::from(PusTc::new(APID + 1, 0, 17, 1, Bytes::new()))).await.unwrap();
    acceptor
        .ask(PusPacket::from(PusTm::new(APID, 0, 17, 2, vec![0u8; 7], Bytes::new())))
        .await
        .unwrap();
    let mut no_ack = PusTc::new(APID, 0, 17, 1, Bytes::new());
    no_ack.secondary_header.ack_flags = AckFlags { acceptance: false, ..AckFlags::ALL };
    acceptor.ask(no_ack).await.unwrap();

    assert!(reports.ask(GetMessages::new()).await.unwrap().is_empty());
}

#[tokio::test]
async fn counters_advance() {
    let reports = TestActor::<VerificationReport>::spawn(TestActor::new());
    let (acceptor, _handler) = acceptor(reports.clone().recipient());
    let acceptor = PusTcAcceptor::spawn(acceptor);

    for (seq, source_id) in [(0, 1), (1, 1), (2, 2)] {
        let mut tc = PusTc::new(APID, seq, 17, 1, Bytes::new());
        tc.secondary_header.source_id = source_id;
        acceptor.ask(tc).await.unwrap();
    }

    let received = reports.ask(GetMessages::new()).await.unwrap();
    let counters: Vec<_> = received
        .iter()
        .map(|r| (r.header.sequence_count, r.destination_id, r.message_type_counter))
        .collect();
    // Sequence count is global, message type counter is per destination ID.
    assert_eq!(counters, vec![(0, 1, 0), (1, 1, 1), (2, 2, 0)]);
}

#[tokio::test]
async fn end_to_end_over_tcp() {
    let reports = TestActor::<VerificationReport>::spawn(TestActor::new());
    let (acceptor, _handler) = acceptor(reports.clone().recipient());
    let acceptor = PusTcAcceptor::spawn(acceptor);

    let server = PusServer::spawn(TcpServerArgs {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        downstream: acceptor.recipient::<PusPacket>(),
    });
    let addr = server.ask(GetLocalAddr).await.unwrap();

    let mut client = Framed::new(TcpStream::connect(addr).await.unwrap(), PusCodec::default());
    client.send(PusTc::new(APID, 5, 17, 1, Bytes::new()).into()).await.unwrap();
    client.send(PusTc::new(APID + 1, 6, 17, 1, Bytes::new()).into()).await.unwrap();
    client.send(PusTc::new(APID, 7, 17, 1, Bytes::new()).into()).await.unwrap();

    let received = TestActor::assert_received(&reports, 2, Duration::from_secs(1)).await;
    let acked: Vec<_> = received.iter().map(|r| r.request_id.sequence_count).collect();
    assert_eq!(acked, vec![5, 7]);
}

#[tokio::test]
async fn tm_1_1_is_sent_back_over_tcp_via_adapter_and_pus_writer() {
    use futures::StreamExt;
    use kameo_tcp_example::{ConnectionHalfClosed, PusPacketAdapter, PusWriter, TcpWriterArgs};
    use tokio::net::TcpListener;

    // Set up a TCP connection: the server side writes, the client reads.
    let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = tcp_listener.local_addr().unwrap();
    let client_task = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
    let (server_stream, peer_addr) = tcp_listener.accept().await.unwrap();
    let mut client = Framed::new(client_task.await.unwrap(), PusCodec::default());
    let (_server_read, server_write) = server_stream.into_split();

    let closed = TestActor::<ConnectionHalfClosed>::spawn(TestActor::new());
    let writer = PusWriter::spawn(TcpWriterArgs {
        write_half: server_write,
        peer_addr,
        listener: closed.recipient(),
    });
    let adapter = PusPacketAdapter::<VerificationReport>::spawn(PusPacketAdapter::new(
        writer.recipient::<PusPacket>(),
    ));
    let (acceptor, _handler) = acceptor(adapter.recipient());
    let acceptor = PusTcAcceptor::spawn(acceptor);

    let tc = PusTc::new(APID, 11, 17, 1, Bytes::new());
    acceptor.tell(tc.clone()).await.unwrap();

    let packet = tokio::time::timeout(Duration::from_secs(1), client.next())
        .await
        .expect("TM(1,1) should arrive")
        .unwrap()
        .unwrap();
    let report = VerificationReport::try_from(packet).unwrap();
    assert_eq!(report.kind, VerificationKind::AcceptanceSuccess);
    assert_eq!(report.request_id, RequestId::from(&tc));
}

#[tokio::test]
async fn adapter_accepts_infallible_conversions() {
    use kameo_tcp_example::{AreYouAliveReport, PusPacketAdapter};

    let packets = TestActor::<PusPacket>::spawn(TestActor::new());
    let adapter = PusPacketAdapter::<AreYouAliveReport>::spawn(PusPacketAdapter::new(packets.clone().recipient()));

    let report = AreYouAliveReport::new(APID, 0, vec![0u8; 7]);
    adapter.ask(report.clone()).await.unwrap();

    let received = packets.ask(GetMessages::new()).await.unwrap();
    assert_eq!(received, vec![PusPacket::from(report)]);
}

#[tokio::test]
async fn accepted_tc_is_forwarded_to_service_handler() {
    let reports = TestActor::<VerificationReport>::spawn(TestActor::new());
    let (acceptor, handler) = acceptor(reports.clone().recipient());
    let acceptor = PusTcAcceptor::spawn(acceptor);

    let tc = PusTc::new(APID, 1, 17, 1, Bytes::new());
    acceptor.ask(tc.clone()).await.unwrap();

    let forwarded = TestActor::assert_received(&handler, 1, Duration::from_secs(1)).await;
    assert_eq!(forwarded, vec![tc]);
}

#[tokio::test]
async fn unsupported_services_and_subtypes_are_rejected_with_tm_1_2() {
    let reports = TestActor::<VerificationReport>::spawn(TestActor::new());
    let (acceptor, handler) = acceptor(reports.clone().recipient());
    let acceptor = PusTcAcceptor::spawn(acceptor);

    // Failure reports are sent even without acknowledgement flags set.
    let mut unknown_service = PusTc::new(APID, 1, 3, 1, Bytes::new());
    unknown_service.secondary_header.ack_flags = AckFlags::NONE;
    let unknown_subtype = PusTc::new(APID, 2, 17, 5, Bytes::new());
    acceptor.ask(unknown_service.clone()).await.unwrap();
    acceptor.ask(unknown_subtype.clone()).await.unwrap();

    let received = reports.ask(GetMessages::new()).await.unwrap();
    let expected = [
        (&unknown_service, FailureCode::UnsupportedService, [3u8, 1]),
        (&unknown_subtype, FailureCode::UnsupportedSubtype, [17, 5]),
    ];
    assert_eq!(received.len(), expected.len());
    for (report, (tc, code, data)) in received.iter().zip(expected) {
        let VerificationKind::AcceptanceFailure(failure) = &report.kind else {
            panic!("expected TM(1,2), got: {:?}", report.kind);
        };
        assert_eq!(report.request_id, RequestId::from(tc));
        assert_eq!(FailureCode::from_code(failure.code), Some(code));
        assert_eq!(&failure.data[..], &data);
    }

    assert!(handler.ask(GetMessages::new()).await.unwrap().is_empty(), "rejected TCs are not forwarded");
}

#[tokio::test]
async fn failed_forwarding_is_reported_with_tm_1_10() {
    let reports = TestActor::<VerificationReport>::spawn(TestActor::new());
    let (acceptor, handler) = acceptor(reports.clone().recipient());
    let acceptor = PusTcAcceptor::spawn(acceptor);

    handler.kill();
    handler.wait_for_shutdown().await;

    let tc = PusTc::new(APID, 1, 17, 1, Bytes::new());
    acceptor.ask(tc.clone()).await.unwrap();

    let received = reports.ask(GetMessages::new()).await.unwrap();
    let kinds: Vec<_> = received.iter().map(|r| r.kind.subtype()).collect();
    assert_eq!(kinds, vec![1, 10], "TM(1,1) first, then TM(1,10)");

    let VerificationKind::RoutingFailure(failure) = &received[1].kind else { unreachable!() };
    assert_eq!(received[1].request_id, RequestId::from(&tc));
    assert_eq!(FailureCode::from_code(failure.code), Some(FailureCode::RoutingFailed));
}
