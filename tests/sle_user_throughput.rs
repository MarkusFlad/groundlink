//! Throughput of the RAF return link at an SLE user, for frames of
//! different sizes and transfer buffer sizes:
//!
//! ```text
//! provider ─TCP─▶ TmlClient ─▶ TmlLinkActor ─▶ SleUserActor ─RafFrame─▶ sink
//! ```
//!
//! The provider is a plain TCP connection: it answers BIND and RAF-START
//! and then writes pre-encoded RAF-TRANSFER-BUFFERs, so encoding them is
//! not measured. The sink is an actor that counts the frames. The rate
//! counts the frame data only, not the SLE and TML overhead.
//!
//! The actors are measured twice: passing every message on its own
//! (TML messages, SLE PDUs and frames), and passing batches (the messages
//! of one read, and the frames of one transfer buffer). For comparison,
//! the same data is also received by a plain `Framed` stream with the
//! `TmlCodec` that decodes every transfer buffer, without actors.
//!
//! Ignored by default; run in release mode:
//!
//! ```text
//! cargo test --release --test sle_user_throughput -- --ignored --nocapture
//! ```

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use groundlink::actor::sle::{RafStart, SleBind};
use groundlink::protocol::sle::common::{BindInvocation, BindReturn, ConditionalTime, Credentials, Time, service_type};
use groundlink::protocol::sle::raf::{
    AntennaId, FrameQuality, RafFrame, RafProviderPdu, RafStartInvocation, RafStartReturn, RafUserPdu,
    RequestedFrameQuality, encode_transfer_buffer,
};
use groundlink::protocol::tml::TmlContext;
use groundlink::{
    Batch, ConnectionEvent, SlePdu, SleUserActor, SleUserConfig, TcpClientArgs, TmlClient, TmlCodec, TmlLinkActor,
    TmlMessage, impl_batch_message,
};
use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::mailbox;
use kameo::message::{Context, Message};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::codec::{Encoder, Framed};

const RAF_SI: &str = "sagr=1.spack=VST-PASS0001.rsl-fg=1.raf=onlt1";
/// Lengths of the frames.
const FRAME_LENS: [usize; 4] = [256, 1115, 2048, 8920];
/// Frames per RAF-TRANSFER-BUFFER; 16 is the default of `RafConfig`.
const TRANSFER_BUFFER_SIZES: [usize; 2] = [16, 64];
/// Frames per pre-encoded chunk that the provider writes repeatedly.
const CHUNK_FRAMES: usize = 256;
/// Frame data sent per measurement, unless limited by `MAX_FRAMES`.
const MAX_BYTES: usize = 512 << 20;
/// Frames sent per measurement at most.
const MAX_FRAMES: usize = 2_000_000;
const TIMEOUT: Duration = Duration::from_secs(60);

/// `CHUNK_FRAMES` frames of `frame_len` octets in RAF-TRANSFER-BUFFERs of
/// `transfer_buffer_size` frames, as TML messages.
fn encoded_chunk(frame_len: usize, transfer_buffer_size: usize) -> Bytes {
    let data = Bytes::from(vec![0xA5; frame_len]);
    let frames: Vec<RafFrame> = (0..transfer_buffer_size)
        .map(|_| RafFrame {
            earth_receive_time: Time::now(),
            antenna_id: AntennaId::Local(Bytes::from_static(b"ANT1")),
            data_link_continuity: 0,
            quality: FrameQuality::Good,
            private_annotation: None,
            data: data.clone(),
        })
        .collect();
    let pdu = encode_transfer_buffer(&frames);
    let mut codec = TmlCodec::default();
    let mut chunk = BytesMut::new();
    for _ in 0..CHUNK_FRAMES / transfer_buffer_size {
        codec.encode(TmlMessage::SlePdu(pdu.clone()), &mut chunk).unwrap();
    }
    chunk.freeze()
}

/// The next SLE PDU from the peer.
async fn recv(peer: &mut Framed<TcpStream, TmlCodec>) -> Bytes {
    loop {
        match tokio::time::timeout(TIMEOUT, peer.next()).await.expect("timed out") {
            Some(Ok(TmlMessage::SlePdu(pdu))) => return pdu,
            Some(Ok(_)) => continue,
            Some(Err(err)) => panic!("connection failed: {err}"),
            None => panic!("connection closed"),
        }
    }
}

/// A provider that accepts one connection, answers BIND and RAF-START
/// and, once told to through the returned sender, writes `chunk`
/// `repeats` times. The task returns the connection, to keep it open
/// until everything has arrived.
async fn spawn_provider(
    chunk: Bytes,
    repeats: usize,
) -> (SocketAddr, oneshot::Sender<()>, JoinHandle<Framed<TcpStream, TmlCodec>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (go, started) = oneshot::channel();
    let provider = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut user = Framed::new(stream, TmlCodec::default());

        let bind = BindInvocation::decode_pdu(recv(&mut user).await).unwrap();
        let bind_return = BindReturn {
            credentials: Credentials::Unused,
            responder_id: "PROV".into(),
            result: Ok(bind.version),
        };
        user.send(TmlMessage::SlePdu(bind_return.encode_pdu())).await.unwrap();

        let RafUserPdu::Start(start) = RafUserPdu::decode(recv(&mut user).await).unwrap() else {
            panic!("expected RAF-START")
        };
        let start_return = RafProviderPdu::StartReturn(RafStartReturn {
            credentials: Credentials::Unused,
            invoke_id: start.invoke_id,
            result: Ok(()),
        });
        user.send(TmlMessage::SlePdu(start_return.encode())).await.unwrap();

        started.await.unwrap();
        for _ in 0..repeats {
            user.get_mut().write_all(&chunk).await.unwrap();
        }
        user
    });
    (addr, go, provider)
}

/// Counts the frames and tells `done` the octets of frame data once
/// `expected` frames have arrived.
struct FrameSink {
    expected: usize,
    frames: usize,
    bytes: usize,
    done: Option<oneshot::Sender<usize>>,
}

impl Actor for FrameSink {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<RafFrame> for FrameSink {
    type Reply = ();

    async fn handle(&mut self, frame: RafFrame, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.frames += 1;
        self.bytes += frame.data.len();
        if self.frames == self.expected
            && let Some(done) = self.done.take()
        {
            let _ = done.send(self.bytes);
        }
    }
}

impl_batch_message!(FrameSink, RafFrame);

/// Receives `frames` frames of `chunk` through the user actors and
/// returns the time from the provider's first write until the last frame
/// has arrived at the sink. With `batched`, the actors pass batches on.
async fn through_user(chunk: Bytes, frames: usize, frame_len: usize, batched: bool) -> Duration {
    let (addr, go, provider) = spawn_provider(chunk, frames / CHUNK_FRAMES).await;
    let (done, finished) = oneshot::channel();
    let sink = FrameSink::spawn(FrameSink {
        expected: frames,
        frames: 0,
        bytes: 0,
        done: Some(done),
    });

    let user = SleUserActor::prepare_with_mailbox(mailbox::unbounded());
    let link = TmlLinkActor::prepare();
    let client_args = if batched {
        TcpClientArgs::batched(addr, link.actor_ref().clone().recipient(), TmlCodec::default())
    } else {
        TcpClientArgs::new(addr, link.actor_ref().clone().recipient(), TmlCodec::default())
    };
    let client =
        TmlClient::spawn(client_args.with_observer(link.actor_ref().clone().reply_recipient::<ConnectionEvent>()));
    let link_ref = link.actor_ref().clone();
    let user_ref = user.actor_ref().clone();
    let context = TmlContext::new(30, 3);
    let initiator = if batched {
        TmlLinkActor::initiator(
            client.clone().recipient::<Batch<TmlMessage>>(),
            client.clone().recipient(),
            user_ref.clone().recipient::<Batch<SlePdu>>(),
            context,
        )
    } else {
        TmlLinkActor::initiator(
            client.clone().recipient::<TmlMessage>(),
            client.clone().recipient(),
            user_ref.clone().recipient::<SlePdu>(),
            context,
        )
    };
    link.spawn(initiator.with_events(user_ref.clone().recipient()));
    let config = SleUserConfig::new("MOC1", "PORT1", RAF_SI.parse().unwrap());
    user.spawn(if batched {
        SleUserActor::raf(client, link_ref, config, sink.recipient::<Batch<RafFrame>>())
    } else {
        SleUserActor::raf(client, link_ref, config, sink.recipient::<RafFrame>())
    });

    user_ref.ask(SleBind).await.unwrap();
    let start = RafStart {
        requested_quality: RequestedFrameQuality::AllFrames,
    };
    user_ref.ask(start).await.unwrap();

    let start = Instant::now();
    go.send(()).unwrap();
    let bytes = tokio::time::timeout(TIMEOUT, finished)
        .await
        .expect("timed out")
        .unwrap();
    let elapsed = start.elapsed();
    assert_eq!(bytes, frames * frame_len);
    drop(provider.await.unwrap());
    elapsed
}

/// Like [`through_user`], but receives with a plain `Framed` stream that
/// decodes every transfer buffer.
async fn plain(chunk: Bytes, frames: usize, frame_len: usize) -> Duration {
    let (addr, go, provider) = spawn_provider(chunk, frames / CHUNK_FRAMES).await;
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
    let RafProviderPdu::BindReturn(_) = RafProviderPdu::decode(recv(&mut user).await).unwrap() else {
        panic!("expected BIND return")
    };
    let start = RafUserPdu::Start(RafStartInvocation {
        credentials: Credentials::Unused,
        invoke_id: 1,
        start_time: ConditionalTime::Undefined,
        stop_time: ConditionalTime::Undefined,
        requested_quality: RequestedFrameQuality::AllFrames,
    });
    user.send(TmlMessage::SlePdu(start.encode())).await.unwrap();
    let RafProviderPdu::StartReturn(_) = RafProviderPdu::decode(recv(&mut user).await).unwrap() else {
        panic!("expected START return")
    };

    let start = Instant::now();
    go.send(()).unwrap();
    let mut received = 0;
    let mut bytes = 0;
    while received < frames {
        match RafProviderPdu::decode(recv(&mut user).await).unwrap() {
            RafProviderPdu::TransferBuffer(buffer) => {
                received += buffer.len();
                bytes += buffer.iter().map(|frame| frame.data.len()).sum::<usize>();
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    let elapsed = start.elapsed();
    assert_eq!(bytes, frames * frame_len);
    drop(provider.await.unwrap());
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
async fn raf_throughput_through_the_user() {
    if cfg!(debug_assertions) {
        println!("note: debug build, the numbers are not representative; use --release");
    }
    println!(
        "{:>10} {:>8} {:>10} {:>10} {:>16} {:>16} {:>16} {:>12}",
        "frame len", "buffer", "frames", "MB data", "single", "batched", "plain", "frames/s"
    );

    for frame_len in FRAME_LENS {
        for transfer_buffer_size in TRANSFER_BUFFER_SIZES {
            let frames = (MAX_BYTES / frame_len).min(MAX_FRAMES).div_ceil(CHUNK_FRAMES) * CHUNK_FRAMES;
            let bytes = frames * frame_len;
            let chunk = encoded_chunk(frame_len, transfer_buffer_size);

            let single = through_user(chunk.clone(), frames, frame_len, false).await;
            let batched = through_user(chunk.clone(), frames, frame_len, true).await;
            let plain = plain(chunk, frames, frame_len).await;

            println!(
                "{:>10} {:>8} {:>10} {:>10.1} {:>16} {:>16} {:>16} {:>12.0}",
                frame_len,
                transfer_buffer_size,
                frames,
                bytes as f64 / 1e6,
                rate(bytes, single),
                rate(bytes, batched),
                rate(bytes, plain),
                frames as f64 / batched.as_secs_f64(),
            );
        }
    }
}
