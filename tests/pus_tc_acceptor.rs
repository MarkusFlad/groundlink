//! Tests for `PusTcAcceptor`: acknowledges TCs for its APID with TM(1,1),
//! rejects unsupported and invalid ones with TM(1,2) and reports failed
//! forwarding with TM(1,10).

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::SinkExt;
use kameo::actor::{ActorRef, Recipient, Spawn};
use groundlink::{
    AckFlags, ConnectionPolicy, FailureCode, GetLocalAddr, GetMessages, KeepAlive, PacketType,
    PusCodec, PusConfig, PusServer, PusPacket, PusTc, PusTcAcceptor, PusTm, RequestId, SpacePacket,
    SpacePacketCodec, SpacePacketHeader, SpacePacketServer, TcpServerArgs, TestActor,
    VerificationKind, VerificationReport,
};
use tokio::net::TcpStream;
use tokio_util::codec::{Encoder, Framed};

const APID: u16 = 0x042;

/// Acceptor that accepts TC(17,1) and forwards it to the returned test
/// handler.
fn acceptor(reports: Recipient<PusTm>) -> (PusTcAcceptor, ActorRef<TestActor<PusTc>>) {
    let handler = TestActor::<PusTc>::spawn(TestActor::new());
    let acceptor = PusTcAcceptor::new(APID, reports).with_service_handler(17, &[1], handler.clone().recipient());
    (acceptor, handler)
}

/// The verification reports received so far.
async fn received_reports(reports: &ActorRef<TestActor<PusTm>>) -> Vec<VerificationReport> {
    let tms = reports.ask(GetMessages::new()).await.unwrap();
    tms.into_iter().map(|tm| VerificationReport::try_from(tm).unwrap()).collect()
}

/// Waits for `count` verification reports.
async fn wait_for_reports(reports: &ActorRef<TestActor<PusTm>>, count: usize) -> Vec<VerificationReport> {
    let tms = TestActor::assert_received(reports, count, Duration::from_secs(1)).await;
    tms.into_iter().map(|tm| VerificationReport::try_from(tm).unwrap()).collect()
}

#[tokio::test]
async fn tc_for_own_apid_is_acknowledged_with_tm_1_1() {
    let reports = TestActor::<PusTm>::spawn(TestActor::new());
    let (acceptor, _handler) = acceptor(reports.clone().recipient());
    let acceptor = PusTcAcceptor::spawn(acceptor);

    let mut tc = PusTc::new(APID, 17, 17, 1, Bytes::new());
    tc.secondary_header.source_id = 0x0815;
    acceptor.ask(PusPacket::from(tc.clone())).await.unwrap();

    let received = received_reports(&reports).await;
    assert_eq!(received.len(), 1);
    let report = &received[0];
    assert_eq!(report.kind, VerificationKind::AcceptanceSuccess);
    assert_eq!(report.request_id, RequestId::from(&tc));
    assert_eq!(report.header.apid, APID);
    assert_eq!(report.destination_id, 0x0815);
}

#[tokio::test]
async fn ignores_other_apid_telemetry_and_tc_without_acceptance_flag() {
    let reports = TestActor::<PusTm>::spawn(TestActor::new());
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

    assert!(received_reports(&reports).await.is_empty());
}

#[tokio::test]
async fn end_to_end_over_tcp() {
    let reports = TestActor::<PusTm>::spawn(TestActor::new());
    let (acceptor, _handler) = acceptor(reports.clone().recipient());
    let acceptor = PusTcAcceptor::spawn(acceptor);

    let server = PusServer::spawn(TcpServerArgs {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        downstream: acceptor.recipient::<PusPacket>(),
        keepalive: Some(KeepAlive::default()),
        connection_policy: ConnectionPolicy::WaitForClose,
    });
    let addr = server.ask(GetLocalAddr).await.unwrap();

    let mut client = Framed::new(TcpStream::connect(addr).await.unwrap(), PusCodec::default());
    client.send(PusTc::new(APID, 5, 17, 1, Bytes::new()).into()).await.unwrap();
    client.send(PusTc::new(APID + 1, 6, 17, 1, Bytes::new()).into()).await.unwrap();
    client.send(PusTc::new(APID, 7, 17, 1, Bytes::new()).into()).await.unwrap();

    let received = wait_for_reports(&reports, 2).await;
    let acked: Vec<_> = received.iter().map(|r| r.request_id.sequence_count).collect();
    assert_eq!(acked, vec![5, 7]);
}

#[tokio::test]
async fn tm_1_1_is_sent_back_over_tcp_via_stamper_and_pus_writer() {
    use futures::StreamExt;
    use groundlink::{ConnectionHalfClosed, PusTmStamper, PusWriter, TcpWriterArgs};
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
    let stamper = PusTmStamper::spawn(PusTmStamper::new(APID, writer.recipient::<PusPacket>()));
    let (acceptor, _handler) = acceptor(stamper.recipient());
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
    use groundlink::{AreYouAliveReport, PusPacketAdapter};

    let packets = TestActor::<PusPacket>::spawn(TestActor::new());
    let adapter = PusPacketAdapter::<AreYouAliveReport>::spawn(PusPacketAdapter::new(packets.clone().recipient::<PusPacket>()));

    let report = AreYouAliveReport::new(APID, 0, vec![0u8; 7]);
    adapter.ask(report.clone()).await.unwrap();

    let received = packets.ask(GetMessages::new()).await.unwrap();
    assert_eq!(received, vec![PusPacket::from(report)]);
}

#[tokio::test]
async fn accepted_tc_is_forwarded_to_service_handler() {
    let reports = TestActor::<PusTm>::spawn(TestActor::new());
    let (acceptor, handler) = acceptor(reports.clone().recipient());
    let acceptor = PusTcAcceptor::spawn(acceptor);

    let tc = PusTc::new(APID, 1, 17, 1, Bytes::new());
    acceptor.ask(tc.clone()).await.unwrap();

    let forwarded = TestActor::assert_received(&handler, 1, Duration::from_secs(1)).await;
    assert_eq!(forwarded, vec![tc]);
}

#[tokio::test]
async fn unsupported_services_and_subtypes_are_rejected_with_tm_1_2() {
    let reports = TestActor::<PusTm>::spawn(TestActor::new());
    let (acceptor, handler) = acceptor(reports.clone().recipient());
    let acceptor = PusTcAcceptor::spawn(acceptor);

    // Failure reports are sent even without acknowledgement flags set.
    let mut unknown_service = PusTc::new(APID, 1, 3, 1, Bytes::new());
    unknown_service.secondary_header.ack_flags = AckFlags::NONE;
    let unknown_subtype = PusTc::new(APID, 2, 17, 5, Bytes::new());
    acceptor.ask(unknown_service.clone()).await.unwrap();
    acceptor.ask(unknown_subtype.clone()).await.unwrap();

    let received = received_reports(&reports).await;
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
    let reports = TestActor::<PusTm>::spawn(TestActor::new());
    let (acceptor, handler) = acceptor(reports.clone().recipient());
    let acceptor = PusTcAcceptor::spawn(acceptor);

    handler.kill();
    handler.wait_for_shutdown().await;

    let tc = PusTc::new(APID, 1, 17, 1, Bytes::new());
    acceptor.ask(tc.clone()).await.unwrap();

    let received = received_reports(&reports).await;
    let kinds: Vec<_> = received.iter().map(|r| r.kind.subtype()).collect();
    assert_eq!(kinds, vec![1, 10], "TM(1,1) first, then TM(1,10)");

    let VerificationKind::RoutingFailure(failure) = &received[1].kind else { unreachable!() };
    assert_eq!(received[1].request_id, RequestId::from(&tc));
    assert_eq!(FailureCode::from_code(failure.code), Some(FailureCode::RoutingFailed));
}

/// Encodes `packet` as a Space Packet with the default PUS configuration.
fn space_packet(packet: impl Into<PusPacket>) -> SpacePacket {
    packet.into().to_space_packet(&PusConfig::default()).unwrap()
}

/// Space Packet of `tc` with a broken CRC.
fn with_crc_error(tc: PusTc) -> SpacePacket {
    let mut packet = space_packet(tc);
    let mut data = BytesMut::from(&packet.data[..]);
    let last = data.len() - 1;
    data[last] ^= 0xFF;
    packet.data = data.freeze();
    packet
}

#[tokio::test]
async fn valid_space_packet_is_accepted_and_forwarded() {
    let reports = TestActor::<PusTm>::spawn(TestActor::new());
    let (acceptor, handler) = acceptor(reports.clone().recipient());
    let acceptor = PusTcAcceptor::spawn(acceptor);

    let tc = PusTc::new(APID, 3, 17, 1, Bytes::new());
    acceptor.ask(space_packet(tc.clone())).await.unwrap();

    let received = received_reports(&reports).await;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].kind, VerificationKind::AcceptanceSuccess);
    assert_eq!(TestActor::assert_received(&handler, 1, Duration::from_secs(1)).await, vec![tc]);
}

#[tokio::test]
async fn invalid_space_packets_are_rejected_with_tm_1_2() {
    let reports = TestActor::<PusTm>::spawn(TestActor::new());
    let (acceptor, handler) = acceptor(reports.clone().recipient());
    let acceptor = PusTcAcceptor::spawn(acceptor);

    let crc_error = with_crc_error(PusTc::new(APID, 1, 17, 1, Bytes::new()));
    let no_secondary_header = SpacePacket::new(PacketType::Telecommand, APID, 2, &b"0123456789"[..]);
    let too_short = SpacePacket {
        header: SpacePacketHeader { secondary_header_flag: true, ..no_secondary_header.header },
        data: Bytes::from_static(&[0]),
    };
    for packet in [&crc_error, &no_secondary_header, &too_short] {
        acceptor.ask(packet.clone()).await.unwrap();
    }

    let received = received_reports(&reports).await;
    let expected = [
        (&crc_error, FailureCode::ChecksumError),
        (&no_secondary_header, FailureCode::MissingSecondaryHeader),
        (&too_short, FailureCode::PacketTooShort),
    ];
    assert_eq!(received.len(), expected.len());
    for (report, (packet, code)) in received.iter().zip(expected) {
        let VerificationKind::AcceptanceFailure(failure) = &report.kind else {
            panic!("expected TM(1,2), got: {:?}", report.kind);
        };
        assert_eq!(report.request_id, RequestId::from_header(&packet.header));
        assert_eq!(report.destination_id, 0);
        assert_eq!(FailureCode::from_code(failure.code), Some(code));
        assert!(failure.data.is_empty());
    }

    assert!(handler.ask(GetMessages::new()).await.unwrap().is_empty(), "invalid TCs are not forwarded");
}

#[tokio::test]
async fn invalid_space_packets_for_other_apids_or_telemetry_are_ignored() {
    let reports = TestActor::<PusTm>::spawn(TestActor::new());
    let (acceptor, _handler) = acceptor(reports.clone().recipient());
    let acceptor = PusTcAcceptor::spawn(acceptor);

    acceptor.ask(with_crc_error(PusTc::new(APID + 1, 0, 17, 1, Bytes::new()))).await.unwrap();
    acceptor.ask(SpacePacket::new(PacketType::Telemetry, APID, 0, &b"0123456789"[..])).await.unwrap();

    assert!(received_reports(&reports).await.is_empty());
}

#[tokio::test]
async fn tc_with_crc_error_over_tcp_is_rejected_and_connection_stays_open() {
    use tokio::io::AsyncWriteExt;

    let reports = TestActor::<PusTm>::spawn(TestActor::new());
    let (acceptor, _handler) = acceptor(reports.clone().recipient());
    let acceptor = PusTcAcceptor::spawn(acceptor);

    let server = SpacePacketServer::spawn(TcpServerArgs {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        downstream: acceptor.recipient::<SpacePacket>(),
        keepalive: Some(KeepAlive::default()),
        connection_policy: ConnectionPolicy::WaitForClose,
    });
    let addr = server.ask(GetLocalAddr).await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let mut bytes = BytesMut::new();
    for packet in [
        with_crc_error(PusTc::new(APID, 8, 17, 1, Bytes::new())),
        space_packet(PusTc::new(APID, 9, 17, 1, Bytes::new())),
    ] {
        SpacePacketCodec.encode(packet, &mut bytes).unwrap();
    }
    stream.write_all(&bytes).await.unwrap();

    let received = wait_for_reports(&reports, 2).await;
    let kinds: Vec<_> = received
        .iter()
        .map(|r| (r.request_id.sequence_count, r.kind.subtype()))
        .collect();
    assert_eq!(kinds, vec![(8, 2), (9, 1)]);
}
