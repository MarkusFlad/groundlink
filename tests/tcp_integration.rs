//! Integration test: SimpleStringServer (= TcpServerActor<SimpleString,
//! SimpleStringCodec>) + TcpReaderActor with a TestActor<SimpleString>
//! as downstream. A real TCP connection is opened, length-prefixed ASCII
//! strings are sent, and `TestActor::assert_received` then waits for and
//! checks the SimpleString messages recorded by the TestActor.

use std::time::Duration;

use futures::FutureExt;
use kameo::actor::{Recipient, Spawn};
use groundlink::{
    KeepAlive, encode_frame, GetLocalAddr, SimpleString, SimpleStringServer, TcpServerArgs, TestActor,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[tokio::test]
async fn simple_strings_are_forwarded_to_downstream_actor() {
    // TestActor as downstream for SimpleString messages.
    let test_actor_ref = TestActor::<SimpleString>::spawn(TestActor::new());
    let downstream: Recipient<SimpleString> = test_actor_ref.clone().recipient::<SimpleString>();

    // Port 0 -> the OS picks a free port; GetLocalAddr tells us which.
    let listener_ref = SimpleStringServer::spawn(TcpServerArgs {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        downstream,
        keepalive: Some(KeepAlive::default()),
    });
    let local_addr = listener_ref.ask(GetLocalAddr).await.unwrap();

    // Open a client connection and send two frames.
    let mut client = TcpStream::connect(local_addr).await.unwrap();
    client.write_all(&encode_frame("hello")).await.unwrap();
    client.write_all(&encode_frame("world")).await.unwrap();
    client.flush().await.unwrap();

    // Waits until at least 2 messages have reached the TestActor, or fails
    // after 1 s. On success returns a copy of the recorded messages.
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
async fn assert_received_fails_on_timeout() {
    let test_actor_ref = TestActor::<SimpleString>::spawn(TestActor::new());
    let downstream: Recipient<SimpleString> = test_actor_ref.clone().recipient::<SimpleString>();

    let listener_ref = SimpleStringServer::spawn(TcpServerArgs {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        downstream,
        keepalive: Some(KeepAlive::default()),
    });
    let local_addr = listener_ref.ask(GetLocalAddr).await.unwrap();

    let mut client = TcpStream::connect(local_addr).await.unwrap();
    client.write_all(&encode_frame("hello")).await.unwrap();
    client.flush().await.unwrap();

    // Only 1 message is sent but we wait for 5 -> must fail with panic!
    // shortly.
    let result = std::panic::AssertUnwindSafe(TestActor::assert_received(
        &test_actor_ref,
        5,
        Duration::from_millis(200),
    ))
    .catch_unwind()
    .await;

    assert!(
        result.is_err(),
        "assert_received should have failed because of the timeout"
    );
}
