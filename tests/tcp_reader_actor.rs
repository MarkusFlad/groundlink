//! Tests for `TcpReaderActor<M, C>` (here for the `SimpleString`
//! protocol through the alias `SimpleStringReader`): when the client
//! closes the connection (EOF), the actor reports
//! `ConnectionHalfClosed { half: Read, reason: Graceful }` to the given
//! `listener` and also sends a `Shutdown<M>` message to its writer actor
//! (replaced here by a `TestActor<Shutdown<SimpleString>>`) before it
//! stops.

use std::time::Duration;

use kameo::actor::Spawn;
use groundlink::{
    CloseReason, ConnectionHalf, ConnectionHalfClosed, Shutdown, SimpleString,
    SimpleStringReader, TcpReaderArgs, TestActor,
};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn reader_actor_reports_read_half_closed_and_shuts_down_writer() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let client_task = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });

    let (server_stream, peer_addr) = listener.accept().await.unwrap();
    let client_stream = client_task.await.unwrap();

    let (server_read, _server_write) = server_stream.into_split();

    let downstream_ref = TestActor::<SimpleString>::spawn(TestActor::new());
    let downstream = downstream_ref.recipient::<SimpleString>();

    let listener_ref = TestActor::<ConnectionHalfClosed>::spawn(TestActor::new());
    let listener_recipient = listener_ref.clone().recipient::<ConnectionHalfClosed>();

    // Stands in for the TcpWriterActor<SimpleString, _>: we only check that
    // the reader actor sends it a Shutdown message.
    let writer_stub_ref = TestActor::<Shutdown<SimpleString>>::spawn(TestActor::new());
    let writer_shutdown = writer_stub_ref.clone().recipient::<Shutdown<SimpleString>>();

    let _reader_ref = SimpleStringReader::spawn(TcpReaderArgs {
        read_half: server_read,
        peer_addr,
        downstream,
        listener: listener_recipient,
        writer_shutdown,
    });

    // The client closes the connection -> the server reads EOF on the read half.
    drop(client_stream);

    let received_closed =
        TestActor::assert_received(&listener_ref, 1, Duration::from_secs(1)).await;

    assert_eq!(received_closed.len(), 1);
    assert_eq!(received_closed[0].peer_addr, peer_addr);
    assert_eq!(received_closed[0].half, ConnectionHalf::Read);
    assert_eq!(received_closed[0].reason, CloseReason::Graceful);

    // The reader actor must also have sent a Shutdown message to the
    // writer actor.
    let received_shutdowns =
        TestActor::assert_received(&writer_stub_ref, 1, Duration::from_secs(1)).await;
    assert_eq!(received_shutdowns.len(), 1);
}
