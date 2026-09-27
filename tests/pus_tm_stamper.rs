//! Tests for `PusTmStamper`: assigns APID, packet sequence count, message
//! type counter and time stamp to the telemetry of one APID.

use std::time::Duration;

use bytes::Bytes;
use kameo::actor::{ActorRef, Spawn};
use groundlink::{CucFormat, CucTime, PusPacket, PusTm, PusTmStamper, TestActor};

const APID: u16 = 0x042;

fn stamper() -> (ActorRef<PusTmStamper>, ActorRef<TestActor<PusPacket>>) {
    let packets = TestActor::<PusPacket>::spawn(TestActor::new());
    let stamper = PusTmStamper::spawn(PusTmStamper::new(APID, packets.clone().recipient::<PusPacket>()));
    (stamper, packets)
}

fn tm(service_type: u8, subtype: u8, destination_id: u16) -> PusTm {
    let mut tm = PusTm::new(0, 0, service_type, subtype, Bytes::new(), Bytes::new());
    tm.secondary_header.destination_id = destination_id;
    tm
}

fn unwrap_tm(packet: &PusPacket) -> &PusTm {
    let PusPacket::Tm(tm) = packet else { panic!("expected TM, got {packet:?}") };
    tm
}

#[tokio::test]
async fn sets_apid_and_current_time() {
    let (stamper, packets) = stamper();

    stamper.tell(tm(17, 2, 0)).await.unwrap();

    let received = TestActor::assert_received(&packets, 1, Duration::from_secs(1)).await;
    let tm = unwrap_tm(&received[0]);
    assert_eq!(tm.header.apid, APID);

    let time = tm.secondary_header.cuc_time(CucFormat::default()).unwrap();
    let age = chrono::Utc::now() - time.to_utc().unwrap();
    assert!(age < chrono::Duration::seconds(1), "time stamp should be current: {time}");

    // The packet can be encoded with the default configuration.
    PusPacket::Tm(tm.clone()).to_space_packet(&Default::default()).unwrap();
}

#[tokio::test]
async fn counters_advance() {
    let (stamper, packets) = stamper();

    for (service_type, subtype, destination_id) in [(1, 1, 1), (1, 1, 1), (1, 1, 2), (1, 7, 1), (17, 2, 1)] {
        stamper.tell(tm(service_type, subtype, destination_id)).await.unwrap();
    }

    let received = TestActor::assert_received(&packets, 5, Duration::from_secs(1)).await;
    let counters: Vec<_> = received
        .iter()
        .map(unwrap_tm)
        .map(|tm| (tm.header.sequence_count, tm.secondary_header.message_type_counter))
        .collect();
    // The sequence count is shared by all packets; the message type counter
    // counts per (service type, subtype, destination ID).
    assert_eq!(counters, vec![(0, 0), (1, 1), (2, 0), (3, 0), (4, 0)]);
}

#[tokio::test]
async fn uses_the_configured_time_format() {
    let packets = TestActor::<PusPacket>::spawn(TestActor::new());
    let format = CucFormat { fine_len: 1, ..CucFormat::default() };
    let stamper = PusTmStamper::spawn(
        PusTmStamper::new(APID, packets.clone().recipient::<PusPacket>()).with_time_format(format),
    );

    stamper.tell(tm(17, 2, 0)).await.unwrap();

    let received = TestActor::assert_received(&packets, 1, Duration::from_secs(1)).await;
    let time = &unwrap_tm(&received[0]).secondary_header.time;
    assert_eq!(time.len(), format.len());
    CucTime::from_bytes(time, format).unwrap();
}
