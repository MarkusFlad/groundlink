//! Unit tests of the TCP actors, including the reader and writer actors,
//! which are not part of the public API.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::Framed;

use super::*;
use crate::actor::test::{GetMessages, TestActor};
use crate::protocol::simple_string::encode_frame;

type Reader = TcpReaderActor<SimpleString, SimpleStringCodec>;
type Writer = TcpWriterActor<SimpleString, SimpleStringCodec>;

fn text(s: impl Into<String>) -> SimpleString {
    SimpleString(s.into())
}

async fn connected_stream() -> TcpStream {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (client, _server) = tokio::join!(TcpStream::connect(addr), listener.accept());
    client.unwrap()
}

#[test]
fn default_keepalive_detects_failed_peer_after_25_seconds() {
    assert_eq!(KeepAlive::default().timeout(), Duration::from_secs(25));
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[tokio::test]
async fn keepalive_is_applied_to_the_socket() {
    let stream = connected_stream().await;
    let keepalive = KeepAlive {
        idle: Duration::from_secs(7),
        interval: Duration::from_secs(2),
        retries: 4,
    };

    configure_keepalive(&stream, Some(keepalive)).unwrap();

    let socket = socket2::SockRef::from(&stream);
    assert!(socket.keepalive().unwrap());
    assert_eq!(socket.tcp_keepalive_time().unwrap(), keepalive.idle);
    assert_eq!(socket.tcp_keepalive_interval().unwrap(), keepalive.interval);
    assert_eq!(socket.tcp_keepalive_retries().unwrap(), keepalive.retries);
    assert_eq!(socket.tcp_user_timeout().unwrap(), Some(Duration::from_secs(15)));
}

#[tokio::test]
async fn no_keepalive_leaves_the_socket_unchanged() {
    let stream = connected_stream().await;

    configure_keepalive(&stream, None).unwrap();

    assert!(!socket2::SockRef::from(&stream).keepalive().unwrap());
}

/// A connected pair: the server side split into halves, and the client
/// stream.
async fn connection() -> (OwnedReadHalf, OwnedWriteHalf, SocketAddr, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (client, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
    let (server_stream, peer_addr) = accepted.unwrap();
    let (read_half, write_half) = server_stream.into_split();
    (read_half, write_half, peer_addr, client.unwrap())
}

/// Spawns a reader on `read_half` that forwards to `downstream` and reports
/// to `listener`; `writer` stands in for the writer of the connection.
fn spawn_reader(
    read_half: OwnedReadHalf,
    peer_addr: SocketAddr,
    downstream: &ActorRef<TestActor<SimpleString>>,
    listener: &ActorRef<TestActor<ConnectionHalfClosed>>,
    writer: &ActorRef<TestActor<Shutdown<SimpleString>>>,
) -> ActorRef<Reader> {
    Reader::spawn(TcpReaderArgs {
        read_half,
        peer_addr,
        downstream: downstream.clone().recipient::<SimpleString>().into(),
        listener: listener.clone().recipient(),
        writer_shutdown: writer.clone().recipient(),
        codec: SimpleStringCodec::default(),
        max_batch_len: DEFAULT_MAX_BATCH_LEN,
    })
}

fn spawn_writer(
    write_half: OwnedWriteHalf,
    peer_addr: SocketAddr,
    listener: &ActorRef<TestActor<ConnectionHalfClosed>>,
) -> ActorRef<Writer> {
    Writer::spawn(TcpWriterArgs::new(
        write_half,
        peer_addr,
        listener.clone().recipient(),
        SimpleStringCodec::default(),
    ))
}

#[tokio::test]
async fn reader_reports_read_half_closed_and_shuts_down_writer() {
    let (read_half, _write_half, peer_addr, client) = connection().await;
    let downstream = TestActor::<SimpleString>::spawn(TestActor::new());
    let listener = TestActor::<ConnectionHalfClosed>::spawn(TestActor::new());
    let writer = TestActor::<Shutdown<SimpleString>>::spawn(TestActor::new());
    let _reader = spawn_reader(read_half, peer_addr, &downstream, &listener, &writer);

    // The client closes the connection -> the reader reads EOF.
    drop(client);

    let closed = TestActor::assert_received(&listener, 1, Duration::from_secs(1)).await;
    assert_eq!(
        closed,
        vec![ConnectionHalfClosed {
            peer_addr,
            half: ConnectionHalf::Read,
            reason: CloseReason::Graceful,
        }]
    );
    TestActor::assert_received(&writer, 1, Duration::from_secs(1)).await;
}

#[tokio::test]
async fn reader_shutdown_reports_read_half_closed_and_stops_forwarding() {
    let (read_half, _write_half, peer_addr, mut client) = connection().await;
    let downstream = TestActor::<SimpleString>::spawn(TestActor::new());
    let listener = TestActor::<ConnectionHalfClosed>::spawn(TestActor::new());
    let writer = TestActor::<Shutdown<SimpleString>>::spawn(TestActor::new());
    let reader = spawn_reader(read_half, peer_addr, &downstream, &listener, &writer);

    client.write_all(&encode_frame("before")).await.unwrap();
    TestActor::assert_received(&downstream, 1, Duration::from_secs(1)).await;

    reader.tell(Shutdown::<SimpleString>::new()).await.unwrap();
    let closed = TestActor::assert_received(&listener, 1, Duration::from_secs(1)).await;
    assert_eq!(closed[0].half, ConnectionHalf::Read);
    assert_eq!(closed[0].reason, CloseReason::Graceful);
    reader.wait_for_shutdown().await;

    // Nothing is read after the shutdown, and the writer is left alone:
    // the server or client that sent the shutdown closes it itself.
    let _ = client.write_all(&encode_frame("after")).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(downstream.ask(GetMessages::new()).await.unwrap(), vec![text("before")]);
    assert!(writer.ask(GetMessages::new()).await.unwrap().is_empty());
}

#[tokio::test]
async fn writer_writes_length_prefixed_frame() {
    let (_read_half, write_half, peer_addr, mut client) = connection().await;
    let listener = TestActor::<ConnectionHalfClosed>::spawn(TestActor::new());
    let writer = spawn_writer(write_half, peer_addr, &listener);

    writer.tell(text("hello world")).await.unwrap();

    // 16-bit length field (big-endian) + payload.
    let mut frame = [0u8; 2 + 11];
    client.read_exact(&mut frame).await.unwrap();
    assert_eq!(&frame, b"\x00\x0bhello world");
}

#[tokio::test]
async fn writer_reports_write_half_closed_on_write_error() {
    let (_read_half, write_half, peer_addr, client) = connection().await;
    // Close the client connection completely so that writing fails.
    drop(client);
    let listener = TestActor::<ConnectionHalfClosed>::spawn(TestActor::new());
    let writer = spawn_writer(write_half, peer_addr, &listener);

    // A single write does not always fail right after the connection is
    // gone (the first frame may still land in the TCP send buffer before
    // the reset comes back), so send several times with a short pause.
    for _ in 0..10 {
        let _ = writer.tell(text("ping")).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let closed = TestActor::assert_received(&listener, 1, Duration::from_secs(3)).await;
    assert_eq!(closed[0].peer_addr, peer_addr);
    assert_eq!(closed[0].half, ConnectionHalf::Write);
}

#[tokio::test]
async fn writer_closes_gracefully_on_shutdown() {
    let (_read_half, write_half, peer_addr, mut client) = connection().await;
    let listener = TestActor::<ConnectionHalfClosed>::spawn(TestActor::new());
    let writer = spawn_writer(write_half, peer_addr, &listener);

    writer.tell(Shutdown::<SimpleString>::new()).await.unwrap();

    let mut buf = [0u8; 1];
    assert_eq!(client.read(&mut buf).await.unwrap(), 0, "client should have seen EOF");
    let closed = TestActor::assert_received(&listener, 1, Duration::from_secs(1)).await;
    assert_eq!(
        closed,
        vec![ConnectionHalfClosed {
            peer_addr,
            half: ConnectionHalf::Write,
            reason: CloseReason::Graceful,
        }]
    );
}

#[tokio::test]
async fn writer_drops_message_that_cannot_be_encoded_and_stays_open() {
    let (_read_half, write_half, peer_addr, mut client) = connection().await;
    let listener = TestActor::<ConnectionHalfClosed>::spawn(TestActor::new());
    let writer = spawn_writer(write_half, peer_addr, &listener);

    // Too long for the 16-bit length field.
    writer.tell(text("x".repeat(70_000))).await.unwrap();
    writer.tell(text("after")).await.unwrap();

    let mut frame = [0u8; 2 + 5];
    tokio::time::timeout(Duration::from_secs(1), client.read_exact(&mut frame))
        .await
        .expect("the valid message should arrive")
        .unwrap();
    assert_eq!(&frame, b"\x00\x05after");
    assert!(writer.is_alive());
    assert!(listener.ask(GetMessages::new()).await.unwrap().is_empty());
}

#[tokio::test]
async fn writer_drops_only_the_message_of_a_batch_that_cannot_be_encoded() {
    let (_read_half, write_half, peer_addr, mut client) = connection().await;
    let listener = TestActor::<ConnectionHalfClosed>::spawn(TestActor::new());
    let writer = spawn_writer(write_half, peer_addr, &listener);

    // The second message is too long for the 16-bit length field.
    let batch = Batch::from(vec![text("before"), text("x".repeat(70_000)), text("after")]);
    writer.tell(batch).await.unwrap();

    let mut frames = [0u8; 2 + 6 + 2 + 5];
    tokio::time::timeout(Duration::from_secs(1), client.read_exact(&mut frames))
        .await
        .expect("both valid messages should arrive")
        .unwrap();
    assert_eq!(&frames, b"\x00\x06before\x00\x05after");
    assert!(writer.is_alive());
    assert!(listener.ask(GetMessages::new()).await.unwrap().is_empty());
}

#[tokio::test]
async fn writer_writes_messages_queued_together_complete_and_in_order() {
    let (_read_half, write_half, peer_addr, client) = connection().await;
    let listener = TestActor::<ConnectionHalfClosed>::spawn(TestActor::new());
    let writer = spawn_writer(write_half, peer_addr, &listener);

    // Messages of different sizes, so that batches exceed the batch
    // length at varying points; then a shutdown behind them.
    let sent: Vec<String> = (0..2000)
        .map(|i| format!("{i}:{}", "x".repeat(i * 37 % 3000)))
        .collect();
    // Read concurrently: the messages do not fit into the TCP buffers.
    let reading = tokio::spawn(
        FramedRead::new(client, SimpleStringCodec::default())
            .map(|item| item.unwrap().0)
            .collect::<Vec<String>>(),
    );
    for s in &sent {
        writer.tell(text(s.clone())).await.unwrap();
    }
    writer.tell(Shutdown::<SimpleString>::new()).await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(5), reading)
        .await
        .expect("the writer should shut down after the last message")
        .unwrap();
    assert_eq!(received, sent);
}

/// Sends large messages to `writer` until aborted; the peer never reads
/// them, so the TCP buffers fill up quickly.
fn flood(writer: ActorRef<Writer>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let _ = writer.tell(text("x".repeat(60_000))).await;
        }
    })
}

#[tokio::test]
async fn writer_write_timeout_reports_an_error_when_the_peer_stops_reading() {
    let (_read_half, write_half, peer_addr, _client) = connection().await;
    let listener = TestActor::<ConnectionHalfClosed>::spawn(TestActor::new());
    let writer = Writer::spawn(
        TcpWriterArgs::new(
            write_half,
            peer_addr,
            listener.clone().recipient(),
            SimpleStringCodec::default(),
        )
        .with_write_timeout(Some(Duration::from_millis(300))),
    );

    let flooding = flood(writer);
    let closed = TestActor::assert_received(&listener, 1, Duration::from_secs(10)).await;
    flooding.abort();

    assert_eq!(closed[0].half, ConnectionHalf::Write);
    assert!(
        matches!(closed[0].reason, CloseReason::Error(_)),
        "expected an error, got {:?}",
        closed[0].reason
    );
}

#[tokio::test]
async fn writer_shutdown_token_interrupts_a_blocked_write() {
    let (_read_half, write_half, peer_addr, _client) = connection().await;
    let listener = TestActor::<ConnectionHalfClosed>::spawn(TestActor::new());
    let args = TcpWriterArgs::new(
        write_half,
        peer_addr,
        listener.clone().recipient(),
        SimpleStringCodec::default(),
    )
    .with_write_timeout(None);
    let shutdown = args.shutdown.clone();
    let writer = Writer::spawn(args);

    let flooding = flood(writer.clone());
    // Long enough for the buffers to fill and the writer to block.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(writer.is_alive());

    shutdown.cancel();
    let closed = TestActor::assert_received(&listener, 1, Duration::from_secs(1)).await;
    flooding.abort();

    assert_eq!(closed[0].half, ConnectionHalf::Write);
    assert_eq!(closed[0].reason, CloseReason::Graceful);
}

/// The events are made up here; outside the crate they can only come from
/// a server.
#[tokio::test]
async fn client_keeps_messages_until_its_previous_connection_is_closed() {
    async fn accept(peer: &TcpListener) -> Framed<TcpStream, SimpleStringCodec> {
        let (stream, _) = tokio::time::timeout(Duration::from_secs(2), peer.accept())
            .await
            .expect("the client should connect")
            .unwrap();
        Framed::new(stream, SimpleStringCodec::default())
    }
    async fn next(framed: &mut Framed<TcpStream, SimpleStringCodec>) -> Option<SimpleString> {
        tokio::time::timeout(Duration::from_secs(2), framed.next())
            .await
            .expect("timed out")
            .map(Result::unwrap)
    }
    let server_event = |connection_id, kind| ConnectionEvent {
        source: EventSource::Server,
        connection_id,
        peer_connection_id: None,
        peer_addr: "127.0.0.1:1".parse().unwrap(),
        kind,
    };

    let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let downstream = TestActor::<SimpleString>::spawn(TestActor::new());
    let client = SimpleStringClient::spawn(TcpClientArgs::new(
        peer.local_addr().unwrap(),
        downstream.recipient(),
        SimpleStringCodec::default(),
    ));

    client
        .tell(server_event(1, ConnectionEventKind::Connected))
        .await
        .unwrap();
    let mut first = accept(&peer).await;

    // Server connection 1 ends and 2 starts, all before the client has
    // closed its first connection.
    client
        .tell(server_event(1, ConnectionEventKind::Disconnected))
        .await
        .unwrap();
    client
        .tell(server_event(2, ConnectionEventKind::Connected))
        .await
        .unwrap();
    client.tell(text("for 2")).await.unwrap();

    assert_eq!(next(&mut first).await, None, "the first connection should be closed");
    let mut second = accept(&peer).await;
    assert_eq!(next(&mut second).await, Some(text("for 2")));
}
