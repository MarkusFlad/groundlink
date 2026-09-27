//! Tests for `PusTestServiceActor` (service 17), on its own and together
//! with a `PusTcAcceptor` and a `PusTmStamper`.

use std::time::Duration;

use bytes::Bytes;
use kameo::actor::Spawn;
use groundlink::{
    AckFlags, AreYouAliveReport, AreYouAliveRequest, FailureCode, GetMessages, PusPacket, PusTc,
    PusTcAcceptor, PusTestServiceActor, PusTm, PusTmStamper, RequestId, TestActor,
    VerificationKind, VerificationReport,
};

const APID: u16 = 0x042;

fn request(sequence_count: u16) -> AreYouAliveRequest {
    let mut request = AreYouAliveRequest::new(APID, sequence_count);
    request.source_id = 0x0815;
    request
}

#[tokio::test]
async fn answers_tc_17_1_with_tm_17_2_then_tm_1_7() {
    let tms = TestActor::<PusTm>::spawn(TestActor::new());
    let service = PusTestServiceActor::spawn(PusTestServiceActor::new(APID, tms.clone().recipient::<PusTm>()));

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
}

#[tokio::test]
async fn without_completion_flag_only_tm_17_2() {
    let tms = TestActor::<PusTm>::spawn(TestActor::new());
    let service = PusTestServiceActor::spawn(PusTestServiceActor::new(APID, tms.clone().recipient::<PusTm>()));

    let mut request = request(1);
    request.ack_flags = AckFlags { completion: false, ..AckFlags::ALL };
    service.ask(request).await.unwrap();

    let received = tms.ask(GetMessages::new()).await.unwrap();
    assert_eq!(received.len(), 1);
    assert!(AreYouAliveReport::try_from(received[0].clone()).is_ok());
}

#[tokio::test]
async fn invalid_tcs_are_answered_with_tm_1_4() {
    let tms = TestActor::<PusTm>::spawn(TestActor::new());
    let service = PusTestServiceActor::spawn(PusTestServiceActor::new(APID, tms.clone().recipient::<PusTm>()));

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
    for (tm, (tc, code, data)) in received.iter().zip(expected) {
        let report = VerificationReport::try_from(tm.clone()).unwrap();
        let VerificationKind::StartFailure(failure) = &report.kind else {
            panic!("expected TM(1,4), got: {:?}", report.kind);
        };
        assert_eq!(report.request_id, RequestId::from(tc));
        assert_eq!(FailureCode::from_code(failure.code), Some(code));
        assert_eq!(&failure.data[..], &data);
    }
}

#[tokio::test]
async fn acceptor_and_service_share_one_stamper() {
    let packets = TestActor::<PusPacket>::spawn(TestActor::new());
    let stamper = PusTmStamper::spawn(PusTmStamper::new(APID, packets.clone().recipient::<PusPacket>()));

    let service = PusTestServiceActor::spawn(PusTestServiceActor::new(APID, stamper.clone().recipient::<PusTm>()));
    let acceptor = PusTcAcceptor::spawn(
        PusTcAcceptor::new(APID, stamper.recipient::<PusTm>()).with_service_handler(
            17,
            &[1],
            service.recipient::<PusTc>(),
        ),
    );

    // TC(17,1) is acknowledged and passed on to the test service.
    let tc: PusTc = request(3).into();
    acceptor.ask(PusPacket::from(tc.clone())).await.unwrap();
    // A TC for another APID is neither acknowledged nor passed on.
    acceptor.ask(PusPacket::from(PusTc::new(APID + 1, 5, 17, 1, Bytes::new()))).await.unwrap();

    let received = TestActor::assert_received(&packets, 3, Duration::from_secs(1)).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(packets.ask(GetMessages::new()).await.unwrap().len(), 3, "no further TMs");

    let types: Vec<_> = received.iter().map(|p| (p.service_type(), p.message_subtype())).collect();
    assert_eq!(types, vec![(1, 1), (17, 2), (1, 7)]);
    let completion = VerificationReport::try_from(received[2].clone()).unwrap();
    assert_eq!(completion.request_id, RequestId::from(&tc));

    let counts: Vec<_> = received.iter().map(|p| p.header().sequence_count).collect();
    assert_eq!(counts, vec![0, 1, 2]);
}

#[tokio::test]
async fn sequence_counts_follow_the_order_on_the_wire() {
    let packets = TestActor::<PusPacket>::spawn(TestActor::new());
    let stamper = PusTmStamper::spawn(PusTmStamper::new(APID, packets.clone().recipient::<PusPacket>()));

    let service = PusTestServiceActor::spawn(PusTestServiceActor::new(APID, stamper.clone().recipient::<PusTm>()));
    let acceptor = PusTcAcceptor::spawn(
        PusTcAcceptor::new(APID, stamper.recipient::<PusTm>()).with_service_handler(
            17,
            &[1],
            service.recipient::<PusTc>(),
        ),
    );

    for sequence_count in 0..20 {
        acceptor.tell(PusTc::from(request(sequence_count))).await.unwrap();
    }

    let received = TestActor::assert_received(&packets, 60, Duration::from_secs(2)).await;
    let positions = |service, subtype| -> Vec<usize> {
        received
            .iter()
            .enumerate()
            .filter(|(_, p)| (p.service_type(), p.message_subtype()) == (service, subtype))
            .map(|(i, _)| i)
            .collect()
    };
    let (accepted, alive, completed) = (positions(1, 1), positions(17, 2), positions(1, 7));
    assert_eq!((accepted.len(), alive.len(), completed.len()), (20, 20, 20));

    // For each TC: TM(1,1) before TM(17,2) before TM(1,7). Both actors handle
    // the TCs in order, so the k-th report of each kind belongs to the k-th TC.
    for k in 0..20 {
        assert!(accepted[k] < alive[k] && alive[k] < completed[k], "TC {k}: {received:?}");
        let request_id = |i: usize| VerificationReport::try_from(received[i].clone()).unwrap().request_id;
        assert_eq!(request_id(accepted[k]).sequence_count, k as u16);
        assert_eq!(request_id(completed[k]).sequence_count, k as u16);
    }

    // The stamper numbers the packets in the order in which it forwards them.
    let counts: Vec<_> = received.iter().map(|p| p.header().sequence_count).collect();
    assert_eq!(counts, (0..60).collect::<Vec<u16>>());
}
