//! Tests für `TcpWriterActor<M, C>` (hier konkret für das
//! `SimpleString`-Protokoll über den Alias `SimpleStringWriter`):
//! 1. Ein normaler Schreibvorgang erzeugt das erwartete Frame beim Client.
//! 2. Schlägt ein Schreibversuch fehl (Client hat die Verbindung
//!    geschlossen), meldet der Actor `ConnectionHalfClosed{ half: Write }`
//!    an den übergebenen `listener` und beendet sich.
//! 3. Eine `Shutdown<M>`-Nachricht schließt die write half geordnet
//!    (Client sieht EOF), meldet `ConnectionHalfClosed{ half: Write,
//!    reason: Graceful }` und beendet den Actor.

use std::time::Duration;

use kameo::actor::Spawn;
use kameo_tcp_example::{
    CloseReason, ConnectionHalf, ConnectionHalfClosed, Shutdown, SimpleString, SimpleStringWriter,
    TcpWriterArgs, TestActor,
};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn writer_actor_schreibt_laengenpraefixierten_frame() {
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
        .tell(SimpleString("hallo welt".to_string()))
        .await
        .unwrap();

    // Frame manuell dekodieren: 16-Bit-Längenfeld (Big-Endian) + Nutzdaten.
    let mut len_buf = [0u8; 2];
    client_read.read_exact(&mut len_buf).await.unwrap();
    let len = u16::from_be_bytes(len_buf) as usize;

    let mut payload_buf = vec![0u8; len];
    client_read.read_exact(&mut payload_buf).await.unwrap();

    assert_eq!(String::from_utf8(payload_buf).unwrap(), "hallo welt");
}

#[tokio::test]
async fn writer_actor_meldet_write_half_closed_bei_schreibfehler() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let client_task = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });

    let (server_stream, peer_addr) = listener.accept().await.unwrap();
    let client_stream = client_task.await.unwrap();

    let (_server_read, server_write) = server_stream.into_split();

    // Client-Verbindung vollständig schließen, damit ein Schreibversuch
    // serverseitig fehlschlägt.
    drop(client_stream);

    let listener_ref = TestActor::<ConnectionHalfClosed>::spawn(TestActor::new());
    let listener_recipient = listener_ref.clone().recipient::<ConnectionHalfClosed>();

    let writer_ref = SimpleStringWriter::spawn(TcpWriterArgs {
        write_half: server_write,
        peer_addr,
        listener: listener_recipient,
    });

    // Ein einzelner Schreibversuch schlägt nach einem Verbindungsabbau nicht
    // immer sofort fehl (das erste Frame landet teils noch im TCP-Sendepuffer,
    // bevor der Reset zurückkommt) - daher mehrfach mit kurzer Pause senden.
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
async fn writer_actor_schliesst_geordnet_bei_shutdown_nachricht() {
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

    // Nach dem geordneten Schließen der write half muss der Client EOF sehen
    // (read gibt 0 zurück).
    let mut buf = [0u8; 1];
    let n = client_read.read(&mut buf).await.unwrap();
    assert_eq!(n, 0, "Client hätte EOF sehen müssen");

    let received =
        TestActor::assert_received(&listener_ref, 1, Duration::from_secs(1)).await;

    assert_eq!(received[0].peer_addr, peer_addr);
    assert_eq!(received[0].half, ConnectionHalf::Write);
    assert_eq!(received[0].reason, CloseReason::Graceful);
}
