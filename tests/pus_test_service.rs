//! Tests for `PusTestServiceActor` (service 17), on its own and as the
//! downstream actor of a `PusTcAcceptor`.

use std::time::Duration;

use bytes::Bytes;
use kameo::actor::Spawn;
use kameo_tcp_example::{
    AckFlags, AreYouAliveReport, AreYouAliveRequest, FailureCode, GetMessages, PusPacket, PusTc,
    PusTcAcceptor, PusTestServiceActor, RequestId, TestActor, VerificationKind, VerificationReport,
};

const APID: u16 = 0x042;

fn request(sequence_count: u16) -> AreYouAliveRequest {
    let mut request = AreYouAliveRequest::new(APID, sequence_count);
    request.source_id = 0x0815;
    request
}

#[tokio::test]
async fn answers_tc_17_1_with_tm_17_2_then_tm_1_7() {
    let tms = TestActor::<PusPacket>::spawn(TestActor::new());
    let service = PusTestServiceActor::spawn(PusTestServiceActor::new(APID, tms.clone().recipient()));

    let request = request(9);
    service.ask(request.clone()).await.unwrap();

    let received = tms.ask(GetMessages::new()).await.unwrap();
    assert_eq!(received.len(), 2);

    let alive = AreYouAliveReport::try_from(received[0].clone()).expect("TM(17,2) first");
    assert_eq!(alive.header.apid, APID);
    assert_eq!(alive.destination_id, 0x0815);

    let completion = VerificationReport::try_from(received[1].clone()).expect("then TM(1,7)");
    assert_eq!(completion.kind, VerificationKind::CompletionSuccess);
    assert_eq!(completion.request_id, RequestId::from_header(&request.header));
    assert_eq!(completion.destination_id, 0x0815);

    assert_eq!((alive.header.sequence_count, completion.header.sequence_count), (0, 1));
}

#[tokio::test]
async fn without_completion_flag_only_tm_17_2() {
    let tms = TestActor::<PusPacket>::spawn(TestActor::new());
    let service = PusTestServiceActor::spawn(PusTestServiceActor::new(APID, tms.clone().recipient()));

    let mut request = request(1);
    request.ack_flags = AckFlags { completion: false, ..AckFlags::ALL };
    service.ask(request).await.unwrap();

    let received = tms.ask(GetMessages::new()).await.unwrap();
    assert_eq!(received.len(), 1);
    assert!(AreYouAliveReport::try_from(received[0].clone()).is_ok());
}

#[tokio::test]
async fn invalid_tcs_are_answered_with_tm_1_4() {
    let tms = TestActor::<PusPacket>::spawn(TestActor::new());
    let service = PusTestServiceActor::spawn(PusTestServiceActor::new(APID, tms.clone().recipient()));

    let unknown_subtype = PusTc::new(APID, 0, 17, 3, Bytes::new());
    let with_app_data = PusTc::new(APID, 1, 17, 1, &b"x"[..]);
    service.ask(unknown_subtype.clone()).await.unwrap();
    service.ask(with_app_data.clone()).await.unwrap();

    let received = tms.ask(GetMessages::new()).await.unwrap();
    let expected = [
        (&unknown_subtype, FailureCode::UnsupportedSubtype, [17u8, 3]),
        (&with_app_data, FailureCode::InvalidApplicationData, [17, 1]),
    ];
    assert_eq!(received.len(), expected.len());
    for (packet, (tc, code, data)) in received.iter().zip(expected) {
        let report = VerificationReport::try_from(packet.clone()).unwrap();
        let VerificationKind::StartFailure(failure) = &report.kind else {
            panic!("expected TM(1,4), got: {:?}", report.kind);
        };
        assert_eq!(report.request_id, RequestId::from(tc));
        assert_eq!(FailureCode::from_code(failure.code), Some(code));
        assert_eq!(&failure.data[..], &data);
    }
}

#[tokio::test]
async fn downstream_of_acceptor_with_shared_sequence_counter() {
    let acceptances = TestActor::<VerificationReport>::spawn(TestActor::new());
    let tms = TestActor::<PusPacket>::spawn(TestActor::new());

    let acceptor = PusTcAcceptor::new(APID, acceptances.clone().recipient());
    let service = PusTestServiceActor::spawn(
        PusTestServiceActor::new(APID, tms.clone().recipient())
            .with_sequence_counter(acceptor.sequence_counter()),
    );
    let acceptor = PusTcAcceptor::spawn(
        acceptor.with_service_handler(17, &[1], service.recipient::<PusTc>()),
    );

    // TC(17,1) is acknowledged and passed on to the test service.
    let tc: PusTc = request(3).into();
    acceptor.ask(PusPacket::from(tc.clone())).await.unwrap();
    // A TC of a service without handler is rejected with TM(1,2).
    acceptor.ask(PusPacket::from(PusTc::new(APID, 4, 3, 1, Bytes::new()))).await.unwrap();
    // A TC for another APID is neither acknowledged nor passed on.
    acceptor.ask(PusPacket::from(PusTc::new(APID + 1, 5, 17, 1, Bytes::new()))).await.unwrap();

    let accepted = TestActor::assert_received(&acceptances, 2, Duration::from_secs(1)).await;
    let service_tms = TestActor::assert_received(&tms, 2, Duration::from_secs(1)).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(tms.ask(GetMessages::new()).await.unwrap().len(), 2, "no further TMs");

    assert_eq!(accepted.iter().map(|r| r.request_id.sequence_count).collect::<Vec<_>>(), vec![3, 4]);
    assert_eq!(accepted[0].kind, VerificationKind::AcceptanceSuccess);
    assert!(matches!(accepted[1].kind, VerificationKind::AcceptanceFailure(_)));

    let alive = AreYouAliveReport::try_from(service_tms[0].clone()).unwrap();
    let completion = VerificationReport::try_from(service_tms[1].clone()).unwrap();
    assert_eq!(completion.kind, VerificationKind::CompletionSuccess);
    assert_eq!(completion.request_id, RequestId::from(&tc));

    // All TMs of the APID are numbered consecutively: TM(1,1) for the first
    // TC, then TM(17,2) and TM(1,7), with TM(1,2) for the second TC in
    // between.
    let mut counts = vec![
        accepted[0].header.sequence_count,
        alive.header.sequence_count,
        completion.header.sequence_count,
        accepted[1].header.sequence_count,
    ];
    counts.sort();
    assert_eq!(counts, vec![0, 1, 2, 3]);
    assert_eq!(accepted[0].header.sequence_count, 0);
    assert!(alive.header.sequence_count < completion.header.sequence_count);
}
