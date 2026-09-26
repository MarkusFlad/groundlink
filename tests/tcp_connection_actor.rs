//! Tests für `TcpConnectionActor<M, C>` (hier konkret für das
//! `SimpleString`-Protokoll über den Alias `SimpleStringConnection`):
//! Schließt der Client die Verbindung (EOF), meldet der Actor
//! `ConnectionHalfClosed{ half: Read, reason: Graceful }` an den
//! übergebenen `listener` und schickt zusätzlich eine `Shutdown<M>`-
//! Nachricht an den (hier durch einen `TestActor<Shutdown<SimpleString>>`
//! ersetzten) zugehörigen Writer-Actor, bevor er sich selbst beendet.

use std::time::Duration;

use kameo::actor::Spawn;
use kameo_tcp_example::{
    CloseReason, ConnectionHalf, ConnectionHalfClosed, Shutdown, SimpleString,
    SimpleStringConnection, TcpConnectionArgs, TestActor,
};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn connection_actor_meldet_read_half_closed_und_schickt_shutdown_an_writer() {
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

    // Steht hier stellvertretend für den TcpWriterActor<SimpleString, _>:
    // wir prüfen nur, dass der Connection-Actor ihm eine Shutdown-Nachricht
    // schickt.
    let writer_stub_ref = TestActor::<Shutdown<SimpleString>>::spawn(TestActor::new());
    let writer_shutdown = writer_stub_ref.clone().recipient::<Shutdown<SimpleString>>();

    let _conn_ref = SimpleStringConnection::spawn(TcpConnectionArgs {
        read_half: server_read,
        peer_addr,
        downstream,
        listener: listener_recipient,
        writer_shutdown,
    });

    // Client schließt die Verbindung -> Server liest EOF auf der read half.
    drop(client_stream);

    let received_closed =
        TestActor::assert_received(&listener_ref, 1, Duration::from_secs(1)).await;

    assert_eq!(received_closed.len(), 1);
    assert_eq!(received_closed[0].peer_addr, peer_addr);
    assert_eq!(received_closed[0].half, ConnectionHalf::Read);
    assert_eq!(received_closed[0].reason, CloseReason::Graceful);

    // Der Connection-Actor muss dem Writer-Actor ebenfalls eine
    // Shutdown-Nachricht geschickt haben.
    let received_shutdowns =
        TestActor::assert_received(&writer_stub_ref, 1, Duration::from_secs(1)).await;
    assert_eq!(received_shutdowns.len(), 1);
}
