//! Tests für den `PusTestServiceActor` (Service 17) – allein und als
//! nachgelagerter Actor des `PusTcAcceptor`.

use std::time::Duration;

use bytes::Bytes;
use kameo::actor::Spawn;
use kameo_tcp_example::{
    AckFlags, AreYouAliveReport, AreYouAliveRequest, GetMessages, PusPacket, PusTc, PusTcAcceptor,
    PusTestServiceActor, RequestId, TestActor, VerificationKind, VerificationReport,
};

const APID: u16 = 0x042;

fn request(sequence_count: u16) -> AreYouAliveRequest {
    let mut request = AreYouAliveRequest::new(APID, sequence_count);
    request.source_id = 0x0815;
    request
}

#[tokio::test]
async fn beantwortet_tc_17_1_mit_tm_17_2_und_danach_tm_1_7() {
    let tms = TestActor::<PusPacket>::spawn(TestActor::new());
    let service = PusTestServiceActor::spawn(PusTestServiceActor::new(APID, tms.clone().recipient()));

    let request = request(9);
    service.ask(request.clone()).await.unwrap();

    let received = tms.ask(GetMessages::new()).await.unwrap();
    assert_eq!(received.len(), 2);

    let alive = AreYouAliveReport::try_from(received[0].clone()).expect("zuerst TM(17,2)");
    assert_eq!(alive.header.apid, APID);
    assert_eq!(alive.destination_id, 0x0815);

    let completion = VerificationReport::try_from(received[1].clone()).expect("danach TM(1,7)");
    assert_eq!(completion.kind, VerificationKind::CompletionSuccess);
    assert_eq!(completion.request_id, RequestId::from_header(&request.header));
    assert_eq!(completion.destination_id, 0x0815);

    assert_eq!((alive.header.sequence_count, completion.header.sequence_count), (0, 1));
}

#[tokio::test]
async fn ohne_completion_flag_nur_tm_17_2() {
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
async fn verwirft_andere_tcs() {
    let tms = TestActor::<PusPacket>::spawn(TestActor::new());
    let service = PusTestServiceActor::spawn(PusTestServiceActor::new(APID, tms.clone().recipient()));

    service.ask(PusTc::new(APID, 0, 17, 3, Bytes::new())).await.unwrap();

    assert!(tms.ask(GetMessages::new()).await.unwrap().is_empty());
}

#[tokio::test]
async fn nachgelagert_zum_acceptor_mit_gemeinsamem_sequence_counter() {
    let acceptances = TestActor::<VerificationReport>::spawn(TestActor::new());
    let tms = TestActor::<PusPacket>::spawn(TestActor::new());

    let acceptor = PusTcAcceptor::new(APID, acceptances.clone().recipient());
    let service = PusTestServiceActor::spawn(
        PusTestServiceActor::new(APID, tms.clone().recipient())
            .with_sequence_counter(acceptor.sequence_counter()),
    );
    let acceptor = PusTcAcceptor::spawn(
        acceptor.with_service_handler(17, service.recipient::<PusTc>()),
    );

    // TC(17,1) wird bestätigt und an den Test-Service weitergereicht.
    let tc: PusTc = request(3).into();
    acceptor.ask(PusPacket::from(tc.clone())).await.unwrap();
    // TC eines Services ohne Handler wird nur bestätigt.
    acceptor.ask(PusPacket::from(PusTc::new(APID, 4, 3, 1, Bytes::new()))).await.unwrap();
    // TC an fremde APID wird weder bestätigt noch weitergereicht.
    acceptor.ask(PusPacket::from(PusTc::new(APID + 1, 5, 17, 1, Bytes::new()))).await.unwrap();

    let accepted = TestActor::assert_received(&acceptances, 2, Duration::from_secs(1)).await;
    let service_tms = TestActor::assert_received(&tms, 2, Duration::from_secs(1)).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(tms.ask(GetMessages::new()).await.unwrap().len(), 2, "keine weiteren TMs");

    assert_eq!(accepted.iter().map(|r| r.request_id.sequence_count).collect::<Vec<_>>(), vec![3, 4]);
    assert!(accepted.iter().all(|r| r.kind == VerificationKind::AcceptanceSuccess));

    let alive = AreYouAliveReport::try_from(service_tms[0].clone()).unwrap();
    let completion = VerificationReport::try_from(service_tms[1].clone()).unwrap();
    assert_eq!(completion.kind, VerificationKind::CompletionSuccess);
    assert_eq!(completion.request_id, RequestId::from(&tc));

    // Alle TMs der APID sind fortlaufend nummeriert: TM(1,1) für das erste
    // TC, dann TM(17,2) und TM(1,7), dann TM(1,1) für das zweite TC.
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
