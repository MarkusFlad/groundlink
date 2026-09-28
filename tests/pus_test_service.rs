//! Tests for `PusTestServiceActor` (service 17), on its own and together
//! with a `PusTcAcceptor` and a `PusTmStamper`.

use std::time::Duration;

use bytes::Bytes;
use groundlink::{
    AckFlags, AreYouAliveReport, AreYouAliveRequest, FailureCode, GetMessages, PusPacket, PusTc, PusTcAcceptor,
    PusTestServiceActor, PusTm, PusTmStamper, RequestId, TestActor, VerificationKind, VerificationReport,
};
use kameo::actor::Spawn;

const APID: u16 = 0x042;

fn request(sequence_count: u16) -> AreYouAliveRequest {
    let mut request = AreYouAliveRequest::new(APID, sequence_count);
    request.source_id = 0x0815;
    request
}

#[tokio::test]
async fn answers_tc_17_1_with_tm_1_3_then_tm_17_2_then_tm_1_7() {
    let tms = TestActor::<PusTm>::spawn(TestActor::new());
    let service = PusTestServiceActor::spawn(PusTestServiceActor::new(APID, tms.clone().recipient::<PusTm>()));

    let request = request(9);
    service.ask(request.clone()).await.unwrap();

    let received = tms.ask(GetMessages::new()).await.unwrap();
    assert_eq!(received.len(), 3);

    let start = VerificationReport::try_from(received[0].clone()).expect("TM(1,3) first");
    assert_eq!(start.kind, VerificationKind::StartSuccess);
    assert_eq!(start.request_id, RequestId::from_header(&request.header));
    assert_eq!(start.destination_id, 0x0815);

    let alive = AreYouAliveReport::try_from(received[1].clone()).expect("then TM(17,2)");
    assert_eq!(alive.header.apid, APID);
    assert_eq!(alive.destination_id, 0x0815);

    let completion = VerificationReport::try_from(received[2].clone()).expect("then TM(1,7)");
    assert_eq!(completion.kind, VerificationKind::CompletionSuccess);
    assert_eq!(completion.request_id, RequestId::from_header(&request.header));
    assert_eq!(completion.destination_id, 0x0815);
}

#[tokio::test]
async fn ack_flags_select_the_verification_reports() {
    let cases = [
        (
            AckFlags {
                start: false,
                completion: false,
                ..AckFlags::ALL
            },
            vec![(17, 2)],
        ),
        (
            AckFlags {
                start: true,
                completion: false,
                ..AckFlags::NONE
            },
            vec![(1, 3), (17, 2)],
        ),
        (
            AckFlags {
                start: false,
                completion: true,
                ..AckFlags::NONE
            },
            vec![(17, 2), (1, 7)],
        ),
    ];
    for (ack_flags, expected) in cases {
        let tms = TestActor::<PusTm>::spawn(TestActor::new());
        let service = PusTestServiceActor::spawn(PusTestServiceActor::new(APID, tms.clone().recipient::<PusTm>()));

        let mut request = request(1);
        request.ack_flags = ack_flags;
        service.ask(request).await.unwrap();

        let received = tms.ask(GetMessages::new()).await.unwrap();
        let types: Vec<_> = received
            .iter()
            .map(|tm| (tm.secondary_header.service_type, tm.secondary_header.message_subtype))
            .collect();
        assert_eq!(types, expected, "{ack_flags:?}");
    }
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
    acceptor
        .ask(PusPacket::from(PusTc::new(APID + 1, 5, 17, 1, Bytes::new())))
        .await
        .unwrap();

    let received = TestActor::assert_received(&packets, 4, Duration::from_secs(1)).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        packets.ask(GetMessages::new()).await.unwrap().len(),
        4,
        "no further TMs"
    );

    let types: Vec<_> = received
        .iter()
        .map(|p| (p.service_type(), p.message_subtype()))
        .collect();
    assert_eq!(types, vec![(1, 1), (1, 3), (17, 2), (1, 7)]);
    let completion = VerificationReport::try_from(received[3].clone()).unwrap();
    assert_eq!(completion.request_id, RequestId::from(&tc));

    let counts: Vec<_> = received.iter().map(|p| p.header().sequence_count).collect();
    assert_eq!(counts, vec![0, 1, 2, 3]);
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

    let received = TestActor::assert_received(&packets, 80, Duration::from_secs(2)).await;
    let positions = |service, subtype| -> Vec<usize> {
        received
            .iter()
            .enumerate()
            .filter(|(_, p)| (p.service_type(), p.message_subtype()) == (service, subtype))
            .map(|(i, _)| i)
            .collect()
    };
    let (accepted, started, alive, completed) = (positions(1, 1), positions(1, 3), positions(17, 2), positions(1, 7));
    assert_eq!(
        (accepted.len(), started.len(), alive.len(), completed.len()),
        (20, 20, 20, 20)
    );

    // For each TC: TM(1,1), TM(1,3), TM(17,2), TM(1,7) in this order. Both
    // actors handle the TCs in order, so the k-th report of each kind belongs
    // to the k-th TC.
    for k in 0..20 {
        assert!(
            accepted[k] < started[k] && started[k] < alive[k] && alive[k] < completed[k],
            "TC {k}: {received:?}"
        );
        let request_id = |i: usize| VerificationReport::try_from(received[i].clone()).unwrap().request_id;
        for i in [accepted[k], started[k], completed[k]] {
            assert_eq!(request_id(i).sequence_count, k as u16);
        }
    }

    // The stamper numbers the packets in the order in which it forwards them.
    let counts: Vec<_> = received.iter().map(|p| p.header().sequence_count).collect();
    assert_eq!(counts, (0..80).collect::<Vec<u16>>());
}
