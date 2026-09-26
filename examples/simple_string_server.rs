//! Starts the TCP server from the library with a `TestActor<SimpleString>`
//! as downstream actor, to show that any actor implementing
//! `Message<SimpleString>` (here generically through `TestActor<M>`) can
//! be the target.
//!
//! `SimpleStringListener` is a type alias for
//! `TcpListenerActor<SimpleString, SimpleStringCodec>`; the CCSDS Space
//! Packet protocol has `SpacePacketListener`
//! (`TcpListenerActor<SpacePacket, SpacePacketCodec>`) and PUS has
//! `PusListener`.
//!
//! The library logs through `tracing`; set the log level with `RUST_LOG`,
//! e.g. `RUST_LOG=debug cargo run --example simple_string_server`.

use kameo::actor::{Recipient, Spawn};
use kameo_tcp_example::{
    GetMessages, SimpleString, SimpleStringListener, TcpListenerArgs, TestActor,
};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let test_actor_ref = TestActor::<SimpleString>::spawn(TestActor::new());
    let downstream: Recipient<SimpleString> = test_actor_ref.clone().recipient::<SimpleString>();

    let _listener_ref = SimpleStringListener::spawn(TcpListenerArgs {
        bind_addr: "127.0.0.1:9000".parse()?,
        downstream,
        on_connect: None,
    });

    println!("Server running on 127.0.0.1:9000 - press Ctrl+C to stop");

    // Print what the TestActor has recorded so far, every 5 seconds.
    let poll_ref = test_actor_ref.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            if let Ok(received) = poll_ref.ask(GetMessages::<SimpleString>::new()).await {
                println!("Received so far: {received:?}");
            }
        }
    });

    tokio::signal::ctrl_c().await?;
    Ok(())
}
