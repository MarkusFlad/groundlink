//! Tests for `TcpWriterActor<M, C>` (here for the `SimpleString` protocol
//! through the alias `SimpleStringWriter`):
//! 1. A normal write produces the expected frame at the client.
//! 2. If a write fails (the client closed the connection), the actor
//!    reports `ConnectionHalfClosed { half: Write }` to the given `listener`
//!    and stops.
//! 3. A `Shutdown<M>` message closes the write half in an orderly way (the
//!    client sees EOF), reports
//!    `ConnectionHalfClosed { half: Write, reason: Graceful }` and stops the
//!    actor.

use std::time::Duration;

use kameo::actor::Spawn;
use groundlink::{
    CloseReason, ConnectionHalf, ConnectionHalfClosed, Shutdown, SimpleString, SimpleStringWriter,
    TcpWriterArgs, TestActor,
};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn writer_actor_writes_length_prefixed_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let client_task = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });

    let (server_stream, peer_addr) = listener.accept().await.unwrap();
    let client_stream = client_task.await.unwrap();

    let (_server_read, server_write) = server_stream.into_split();
    let (mut client_read, _client_write) = client_stream.into_split();

    let listener_ref = TestActor::<ConnectionHalfClosed>::spawn(TestActor::new());
    let listener_recipient = listener_ref.clone().recipient::<ConnectionHalfClosed>();

    let writer_ref = SimpleStringWriter::spawn(TcpWriterArgs {
        write_half: server_write,
        peer_addr,
        listener: listener_recipient,
    });

    writer_ref
        .tell(SimpleString("hello world".to_string()))
        .await
        .unwrap();

    // Decode the frame by hand: 16-bit length field (big-endian) + payload.
    let mut len_buf = [0u8; 2];
    client_read.read_exact(&mut len_buf).await.unwrap();
    let len = u16::from_be_bytes(len_buf) as usize;

    let mut payload_buf = vec![0u8; len];
    client_read.read_exact(&mut payload_buf).await.unwrap();

    assert_eq!(String::from_utf8(payload_buf).unwrap(), "hello world");
}

#[tokio::test]
async fn writer_actor_reports_write_half_closed_on_write_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let client_task = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });

    let (server_stream, peer_addr) = listener.accept().await.unwrap();
    let client_stream = client_task.await.unwrap();

    let (_server_read, server_write) = server_stream.into_split();

    // Close the client connection completely so that a write on the server
    // side fails.
    drop(client_stream);

    let listener_ref = TestActor::<ConnectionHalfClosed>::spawn(TestActor::new());
    let listener_recipient = listener_ref.clone().recipient::<ConnectionHalfClosed>();

    let writer_ref = SimpleStringWriter::spawn(TcpWriterArgs {
        write_half: server_write,
        peer_addr,
        listener: listener_recipient,
    });

    // A single write does not always fail right after the connection is
    // gone (the first frame may still land in the TCP send buffer before
    // the reset comes back), so send several times with a short pause.
    for _ in 0..10 {
        let _ = writer_ref.tell(SimpleString("ping".to_string())).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let received =
        TestActor::assert_received(&listener_ref, 1, Duration::from_secs(3)).await;

    assert_eq!(received[0].peer_addr, peer_addr);
    assert_eq!(received[0].half, ConnectionHalf::Write);
}

#[tokio::test]
async fn writer_actor_closes_gracefully_on_shutdown() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let client_task = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });

    let (server_stream, peer_addr) = listener.accept().await.unwrap();
    let client_stream = client_task.await.unwrap();

    let (_server_read, server_write) = server_stream.into_split();
    let (mut client_read, _client_write) = client_stream.into_split();

    let listener_ref = TestActor::<ConnectionHalfClosed>::spawn(TestActor::new());
    let listener_recipient = listener_ref.clone().recipient::<ConnectionHalfClosed>();

    let writer_ref = SimpleStringWriter::spawn(TcpWriterArgs {
        write_half: server_write,
        peer_addr,
        listener: listener_recipient,
    });

    writer_ref.tell(Shutdown::<SimpleString>::new()).await.unwrap();

    // After the write half has been shut down, the client must see EOF
    // (read returns 0).
    let mut buf = [0u8; 1];
    let n = client_read.read(&mut buf).await.unwrap();
    assert_eq!(n, 0, "client should have seen EOF");

    let received =
        TestActor::assert_received(&listener_ref, 1, Duration::from_secs(1)).await;

    assert_eq!(received[0].peer_addr, peer_addr);
    assert_eq!(received[0].half, ConnectionHalf::Write);
    assert_eq!(received[0].reason, CloseReason::Graceful);
}
