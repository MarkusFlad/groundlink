//! Time per frame of encoding and decoding RAF-TRANSFER-BUFFERs, for
//! different numbers of frames per buffer and frame lengths.
//!
//! Encoding is `encode_transfer_buffer`, decoding is
//! `RafProviderPdu::decode`, both without actors or sockets. Each value
//! is the best of seven runs of about 300 ms.
//!
//! Ignored by default; run in release mode:
//!
//! ```text
//! cargo test --release --test sle_raf_codec -- --ignored --nocapture
//! ```

use std::hint::black_box;
use std::time::Instant;

use bytes::Bytes;
use groundlink::protocol::sle::common::Time;
use groundlink::protocol::sle::raf::{AntennaId, FrameQuality, RafFrame, RafProviderPdu, encode_transfer_buffer};

/// (frames per buffer, frame length) of the measurements.
const CASES: [(usize, usize); 7] = [
    (1, 1115),
    (16, 256),
    (16, 1115),
    (64, 1115),
    (16, 2048),
    (16, 8920),
    (64, 8920),
];

fn frames(count: usize, len: usize) -> Vec<RafFrame> {
    let data = Bytes::from(vec![0xA5; len]);
    (0..count)
        .map(|_| RafFrame {
            earth_receive_time: Time::now(),
            antenna_id: AntennaId::Local(Bytes::from_static(b"ANT1")),
            data_link_continuity: 0,
            quality: FrameQuality::Good,
            private_annotation: None,
            data: data.clone(),
        })
        .collect()
}

/// The best time per frame of seven runs of `f`, which handles `frames`
/// frames, in nanoseconds.
fn ns_per_frame(frames: usize, mut f: impl FnMut()) -> f64 {
    // Find the number of iterations that takes about 300 ms.
    let mut iterations = 1;
    loop {
        let start = Instant::now();
        for _ in 0..iterations {
            f();
        }
        if start.elapsed().as_millis() >= 50 {
            break;
        }
        iterations *= 2;
    }
    iterations *= 6;

    (0..7)
        .map(|_| {
            let start = Instant::now();
            for _ in 0..iterations {
                f();
            }
            start.elapsed().as_nanos() as f64 / (iterations * frames) as f64
        })
        .fold(f64::MAX, f64::min)
}

#[test]
#[ignore = "performance test; run with --release -- --ignored --nocapture"]
fn raf_transfer_buffer_codec() {
    if cfg!(debug_assertions) {
        println!("note: debug build, the numbers are not representative; use --release");
    }
    println!(
        "{:>10} {:>10} {:>16} {:>16}",
        "frames", "frame len", "encode ns/frame", "decode ns/frame"
    );
    for (count, len) in CASES {
        let frames = frames(count, len);
        let encode = ns_per_frame(count, || {
            black_box(encode_transfer_buffer(black_box(&frames)));
        });
        let pdu = encode_transfer_buffer(&frames);
        let decode = ns_per_frame(count, || {
            black_box(RafProviderPdu::decode(black_box(pdu.clone())).unwrap());
        });
        println!("{count:>10} {len:>10} {encode:>16.0} {decode:>16.0}");
    }
}
