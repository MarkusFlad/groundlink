//! Throughput of a server coupled with a client, for Space Packets of
//! different sizes:
//!
//! ```text
//! sender ──▶ SpacePacketServer ──▶ SpacePacketClient ──▶ receiver
//! ```
//!
//! The sender writes pre-encoded packets, so encoding them is not measured;
//! the receiver only counts bytes. The actors are measured with the
//! default mailbox capacity and with `TUNED_MAILBOX_CAPACITY`. For
//! comparison, the same data is also sent over a plain loopback connection
//! without actors in between.
//!
//! Ignored by default; run in release mode:
//!
//! ```text
//! cargo test --release --test tcp_throughput -- --ignored --nocapture
//! ```

use std::time::{Duration, Instant};

use bytes::BytesMut;
use groundlink::{
    ConnectionEvent, DEFAULT_MAILBOX_CAPACITY, GetLocalAddr, PacketType, SpacePacket, SpacePacketClient,
    SpacePacketCodec, SpacePacketServer, TcpClientArgs, TcpServerArgs,
};
use kameo::actor::Spawn;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Encoder;

/// Sizes of the packet data field.
const DATA_LENS: [usize; 7] = [16, 64, 256, 1024, 4096, 16384, 65536];
/// Bytes sent per measurement, unless limited by `MAX_PACKETS`.
const MAX_BYTES: usize = 256 << 20;
/// Packets sent per measurement at most, so small packets do not take
/// too long.
const MAX_PACKETS: usize = 2_000_000;
/// Mailbox capacity of the reader, writer and client in the second
/// measurement through the actors.
const TUNED_MAILBOX_CAPACITY: usize = 256;
/// Packets per pre-encoded chunk that the sender writes repeatedly.
const CHUNK_PACKETS: usize = 256;

/// `packets` encoded Space Packets with `data_len` bytes of data each.
fn encoded_chunk(data_len: usize, packets: usize) -> BytesMut {
    let mut codec = SpacePacketCodec;
    let mut buf = BytesMut::new();
    for count in 0..packets {
        let packet = SpacePacket::new(PacketType::Telemetry, 42, count as u16, vec![0xA5; data_len]);
        codec.encode(packet, &mut buf).unwrap();
    }
    buf
}

/// Writes `chunk` `repeats` times to `sender` and reads everything at
/// `receiver`; returns the time until the last byte has arrived.
async fn transfer(mut sender: TcpStream, mut receiver: TcpStream, chunk: BytesMut, repeats: usize) -> Duration {
    let total = chunk.len() * repeats;
    let start = Instant::now();
    let writing = tokio::spawn(async move {
        for _ in 0..repeats {
            sender.write_all(&chunk).await.unwrap();
        }
        sender
    });

    let mut buf = vec![0u8; 1 << 20];
    let mut received = 0;
    while received < total {
        let n = receiver.read(&mut buf).await.unwrap();
        assert!(n > 0, "connection closed after {received} of {total} bytes");
        received += n;
    }
    let elapsed = start.elapsed();
    assert_eq!(received, total);
    // Keep the sender open until everything has arrived.
    drop(writing.await.unwrap());
    elapsed
}

/// A sender connected to a server that is coupled with a client, and the
/// receiver the client has connected to. The reader, writer and client
/// have mailboxes of `mailbox_capacity`.
async fn through_actors(mailbox_capacity: usize) -> (TcpStream, TcpStream) {
    let receiver_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let client = SpacePacketClient::prepare_with_mailbox(kameo::mailbox::bounded(mailbox_capacity));
    let server = SpacePacketServer::spawn(
        TcpServerArgs::new(
            "127.0.0.1:0".parse().unwrap(),
            client.actor_ref().clone().recipient(),
            SpacePacketCodec,
        )
        .with_observer(client.actor_ref().clone().reply_recipient::<ConnectionEvent>())
        .with_mailbox_capacity(mailbox_capacity),
    );
    client.spawn(
        TcpClientArgs::new(
            receiver_listener.local_addr().unwrap(),
            server.clone().recipient(),
            SpacePacketCodec,
        )
        .with_observer(server.clone().reply_recipient::<ConnectionEvent>())
        .with_mailbox_capacity(mailbox_capacity),
    );

    let sender = TcpStream::connect(server.ask(GetLocalAddr).await.unwrap())
        .await
        .unwrap();
    // The client connects when the server reports the sender's connection.
    let (receiver, _) = receiver_listener.accept().await.unwrap();
    (sender, receiver)
}

/// A sender connected directly to the receiver.
async fn direct() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (sender, accepted) = tokio::join!(TcpStream::connect(listener.local_addr().unwrap()), listener.accept());
    (sender.unwrap(), accepted.unwrap().0)
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
async fn space_packet_throughput_through_server_and_client() {
    if cfg!(debug_assertions) {
        println!("note: debug build, the numbers are not representative; use --release");
    }
    let tuned = format!("mailbox {TUNED_MAILBOX_CAPACITY}");
    println!(
        "{:>10} {:>10} {:>10} {:>16} {:>16} {:>16} {:>12}",
        "data len", "packets", "MB sent", "actors", tuned, "direct", "packets/s"
    );

    for data_len in DATA_LENS {
        let chunk = encoded_chunk(data_len, CHUNK_PACKETS);
        let packet_len = chunk.len() / CHUNK_PACKETS;
        let packets = (MAX_BYTES / packet_len).min(MAX_PACKETS);
        let repeats = packets.div_ceil(CHUNK_PACKETS);
        let (packets, bytes) = (repeats * CHUNK_PACKETS, repeats * chunk.len());

        let (sender, receiver) = through_actors(DEFAULT_MAILBOX_CAPACITY).await;
        let actors = transfer(sender, receiver, chunk.clone(), repeats).await;
        let (sender, receiver) = through_actors(TUNED_MAILBOX_CAPACITY).await;
        let actors_tuned = transfer(sender, receiver, chunk.clone(), repeats).await;
        let (sender, receiver) = direct().await;
        let plain = transfer(sender, receiver, chunk, repeats).await;

        println!(
            "{:>10} {:>10} {:>10.1} {:>16} {:>16} {:>16} {:>12.0}",
            data_len,
            packets,
            bytes as f64 / 1e6,
            rate(bytes, actors),
            rate(bytes, actors_tuned),
            rate(bytes, plain),
            packets as f64 / actors.as_secs_f64(),
        );
    }
}
