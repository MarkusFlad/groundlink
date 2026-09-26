//! Integrationstest: SimpleStringListener (= TcpListenerActor<SimpleString,
//! SimpleStringCodec>) + TcpConnectionActor mit TestActor<SimpleString>
//! als Downstream. Es wird eine echte TCP-Verbindung aufgebaut,
//! längenpräfixierte ASCII-Strings gesendet und anschließend über
//! `TestActor::assert_received` gewartet und geprüft, dass der TestActor
//! die erwarteten SimpleString-Nachrichten aufgezeichnet hat.

use std::time::Duration;

use futures::FutureExt;
use kameo::actor::{Recipient, Spawn};
use kameo_tcp_example::{
    encode_frame, GetLocalAddr, SimpleString, SimpleStringListener, TcpListenerArgs, TestActor,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[tokio::test]
async fn simple_strings_werden_an_downstream_actor_weitergeleitet() {
    // TestActor als Downstream für SimpleString-Nachrichten.
    let test_actor_ref = TestActor::<SimpleString>::spawn(TestActor::new());
    let downstream: Recipient<SimpleString> = test_actor_ref.clone().recipient::<SimpleString>();

    // Port 0 -> OS wählt einen freien Port; über GetLocalAddr erfragen wir ihn.
    let listener_ref = SimpleStringListener::spawn(TcpListenerArgs {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        downstream,
    });
    let local_addr = listener_ref.ask(GetLocalAddr).await.unwrap();

    // Client-Verbindung aufbauen und zwei Frames senden.
    let mut client = TcpStream::connect(local_addr).await.unwrap();
    client.write_all(&encode_frame("hello")).await.unwrap();
    client.write_all(&encode_frame("world")).await.unwrap();
    client.flush().await.unwrap();

    // Wartet, bis mindestens 2 Nachrichten beim TestActor eingetroffen sind,
    // oder schlägt nach 1s fehl. Liefert bei Erfolg eine Kopie des Vektors.
    let received = TestActor::assert_received(&test_actor_ref, 2, Duration::from_secs(1)).await;

    assert_eq!(
        received,
        vec![
            SimpleString("hello".to_string()),
            SimpleString("world".to_string()),
        ]
    );
}

#[tokio::test]
async fn assert_received_schlaegt_bei_timeout_fehl() {
    let test_actor_ref = TestActor::<SimpleString>::spawn(TestActor::new());
    let downstream: Recipient<SimpleString> = test_actor_ref.clone().recipient::<SimpleString>();

    let listener_ref = SimpleStringListener::spawn(TcpListenerArgs {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        downstream,
    });
    let local_addr = listener_ref.ask(GetLocalAddr).await.unwrap();

    let mut client = TcpStream::connect(local_addr).await.unwrap();
    client.write_all(&encode_frame("hello")).await.unwrap();
    client.flush().await.unwrap();

    // Es wird nur 1 Nachricht gesendet, wir warten aber auf 5 -> muss
    // innerhalb kurzer Zeit mit panic! fehlschlagen.
    let result = std::panic::AssertUnwindSafe(TestActor::assert_received(
        &test_actor_ref,
        5,
        Duration::from_millis(200),
    ))
    .catch_unwind()
    .await;

    assert!(
        result.is_err(),
        "assert_received hätte wegen Timeout fehlschlagen müssen"
    );
}
