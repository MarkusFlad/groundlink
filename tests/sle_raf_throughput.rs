//! Throughput of the RAF return link of an SLE provider, for frames of
//! different sizes and transfer buffer sizes:
//!
//! ```text
//! feeder ─Batch<RafFrame>─▶ RafServiceInstance ─RAF-TRANSFER-BUFFER─▶ TmlLinkActor ─▶ TmlServer ─TCP─▶ user
//! ```
//!
//! The feeder sends the frames in batches of `FEED_BATCH_LEN`, as a TM
//! frame decoder behind a batched reader would. The frames share their
//! data, so creating them is not measured. The user is a plain `Framed`
//! stream with the `TmlCodec`; it decodes every RAF-TRANSFER-BUFFER to
//! count the frames, which is part of the measurement. The rate counts
//! the frame data only, not the SLE and TML overhead.
//!
//! Ignored by default; run in release mode:
//!
//! ```text
//! cargo test --release --test sle_raf_throughput -- --ignored --nocapture
//! ```

use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use groundlink::actor::tcp::GetLocalAddr;
use groundlink::protocol::sle::common::{BindInvocation, ConditionalTime, Credentials, Time, service_type};
use groundlink::protocol::sle::raf::{
    AntennaId, FrameQuality, RafFrame, RafProviderPdu, RafStartInvocation, RafUserPdu, RequestedFrameQuality,
};
use groundlink::protocol::tml::TmlContext;
use groundlink::{
    Batch, ConnectionEvent, RafConfig, RafServiceInstance, SleAssociationActor, SleAssociationConfig, SlePdu,
    SleServiceRegistry, TcpServerArgs, TmlCodec, TmlLinkActor, TmlMessage, TmlResponderConfig, TmlServer,
};
use kameo::actor::{ActorRef, Spawn};
use kameo::mailbox;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

type User = Framed<TcpStream, TmlCodec>;

const RAF_SI: &str = "sagr=1.spack=VST-PASS0001.rsl-fg=1.raf=onlt1";
/// Lengths of the frames.
const FRAME_LENS: [usize; 4] = [256, 1115, 2048, 8920];
/// Frames per RAF-TRANSFER-BUFFER; 16 is the default of `RafConfig`.
const TRANSFER_BUFFER_SIZES: [usize; 2] = [16, 64];
/// Frames per batch from the feeder.
const FEED_BATCH_LEN: usize = 64;
/// Frame data sent per measurement, unless limited by `MAX_FRAMES`.
const MAX_BYTES: usize = 512 << 20;
/// Frames sent per measurement at most.
const MAX_FRAMES: usize = 2_000_000;
const TIMEOUT: Duration = Duration::from_secs(10);

/// A RAF instance with a provider port in front of it; returns the
/// instance and the port's server.
fn spawn_provider(transfer_buffer_size: usize) -> (ActorRef<RafServiceInstance>, ActorRef<TmlServer>) {
    let config = RafConfig::new(RAF_SI.parse().unwrap()).with_transfer_buffer_size(transfer_buffer_size);
    let raf = RafServiceInstance::spawn(RafServiceInstance::new(config));
    let registry = SleServiceRegistry::new().with_raf(RAF_SI.parse().unwrap(), raf.clone());

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
        SleAssociationConfig::default().with_responder_id("PROV"),
    ));
    (raf, server)
}

/// The next SLE PDU from the provider.
async fn recv(user: &mut User) -> Bytes {
    loop {
        match tokio::time::timeout(TIMEOUT, user.next()).await.expect("timed out") {
            Some(Ok(TmlMessage::SlePdu(pdu))) => return pdu,
            Some(Ok(_)) => continue,
            Some(Err(err)) => panic!("connection failed: {err}"),
            None => panic!("connection closed"),
        }
    }
}

/// Connects a user, binds and starts RAF.
async fn bind_and_start(server: &ActorRef<TmlServer>) -> User {
    let addr = server.ask(GetLocalAddr).await.unwrap();
    let mut user = Framed::new(TcpStream::connect(addr).await.unwrap(), TmlCodec::default());
    user.send(TmlMessage::Context(TmlContext::new(0, 0))).await.unwrap();

    let bind = BindInvocation {
        credentials: Credentials::Unused,
        initiator_id: "MOC1".into(),
        responder_port: "PORT1".into(),
        service_type: service_type::RTN_ALL_FRAMES,
        version: 5,
        service_instance: RAF_SI.parse().unwrap(),
    };
    user.send(TmlMessage::SlePdu(bind.encode_pdu())).await.unwrap();
    let RafProviderPdu::BindReturn(ret) = RafProviderPdu::decode(recv(&mut user).await).unwrap() else {
        panic!("expected BIND return")
    };
    assert_eq!(ret.result, Ok(5));

    let start = RafUserPdu::Start(RafStartInvocation {
        credentials: Credentials::Unused,
        invoke_id: 1,
        start_time: ConditionalTime::Undefined,
        stop_time: ConditionalTime::Undefined,
        requested_quality: RequestedFrameQuality::AllFrames,
    });
    user.send(TmlMessage::SlePdu(start.encode())).await.unwrap();
    let RafProviderPdu::StartReturn(ret) = RafProviderPdu::decode(recv(&mut user).await).unwrap() else {
        panic!("expected START return")
    };
    assert_eq!(ret.result, Ok(()));
    user
}

/// Feeds `batches` batches of frames of `frame_len` octets to `raf` and
/// receives them at `user`; returns the time until the last frame has
/// arrived.
async fn transfer(raf: ActorRef<RafServiceInstance>, mut user: User, frame_len: usize, batches: usize) -> Duration {
    let data = Bytes::from(vec![0xA5; frame_len]);
    let batch: Vec<RafFrame> = (0..FEED_BATCH_LEN)
        .map(|_| RafFrame {
            earth_receive_time: Time::now(),
            antenna_id: AntennaId::Local(Bytes::from_static(b"ANT1")),
            data_link_continuity: 0,
            quality: FrameQuality::Good,
            private_annotation: None,
            data: data.clone(),
        })
        .collect();
    let total = batches * FEED_BATCH_LEN;

    let start = Instant::now();
    let feeding = tokio::spawn(async move {
        for _ in 0..batches {
            raf.tell(Batch::from(batch.clone())).await.unwrap();
        }
        raf
    });

    let mut received = 0;
    while received < total {
        match RafProviderPdu::decode(recv(&mut user).await).unwrap() {
            RafProviderPdu::TransferBuffer(frames) => received += frames.len(),
            other => panic!("unexpected {other:?}"),
        }
    }
    let elapsed = start.elapsed();
    assert_eq!(received, total);
    drop(feeding.await.unwrap());
    elapsed
}

fn rate(bytes: usize, elapsed: Duration) -> String {
    let bits_per_second = bytes as f64 * 8.0 / elapsed.as_secs_f64();
    if bits_per_second >= 1e9 {
        format!("{:7.2} Gbit/s", bits_per_second / 1e9)
    } else {
        format!("{:7.1} Mbit/s", bits_per_second / 1e6)
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "performance test; run with --release -- --ignored --nocapture"]
async fn raf_throughput_through_the_provider() {
    if cfg!(debug_assertions) {
        println!("note: debug build, the numbers are not representative; use --release");
    }
    println!(
        "{:>10} {:>10} {:>10} {:>10} {:>16} {:>12}",
        "frame len", "buffer", "frames", "MB data", "throughput", "frames/s"
    );

    for frame_len in FRAME_LENS {
        for transfer_buffer_size in TRANSFER_BUFFER_SIZES {
            let frames = (MAX_BYTES / frame_len).min(MAX_FRAMES);
            let batches = frames.div_ceil(FEED_BATCH_LEN);
            let frames = batches * FEED_BATCH_LEN;
            let bytes = frames * frame_len;

            let (raf, server) = spawn_provider(transfer_buffer_size);
            let user = bind_and_start(&server).await;
            let elapsed = transfer(raf, user, frame_len, batches).await;

            println!(
                "{:>10} {:>10} {:>10} {:>10.1} {:>16} {:>12.0}",
                frame_len,
                transfer_buffer_size,
                frames,
                bytes as f64 / 1e6,
                rate(bytes, elapsed),
                frames as f64 / elapsed.as_secs_f64(),
            );
        }
    }
}
