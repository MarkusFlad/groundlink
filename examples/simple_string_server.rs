//! Startet den TCP-Server aus der Library und nutzt `TestActor<SimpleString>`
//! als Downstream-Actor, um zu demonstrieren, dass beliebige Actors, die
//! nur `Message<SimpleString>` implementieren (bzw. hier: generisch über
//! `TestActor<M>`), als Ziel dienen können.
//!
//! `SimpleStringListener` ist ein Typ-Alias für
//! `TcpListenerActor<SimpleString, SimpleStringCodec>` – für das
//! CCSDS-Space-Packet-Protokoll gibt es analog `SpacePacketListener`
//! (`TcpListenerActor<SpacePacket, SpacePacketCodec>`).
//!
//! Die Library loggt über `tracing`; das Log-Level lässt sich per
//! `RUST_LOG` einstellen, z. B. `RUST_LOG=debug cargo run --example simple_string_server`.

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
    });

    println!("Server läuft auf 127.0.0.1:9000 – Strg+C zum Beenden");

    // Alle 5 Sekunden ausgeben, was der TestActor bisher aufgezeichnet hat.
    let poll_ref = test_actor_ref.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            if let Ok(received) = poll_ref.ask(GetMessages::<SimpleString>::new()).await {
                println!("Bisher empfangen: {received:?}");
            }
        }
    });

    tokio::signal::ctrl_c().await?;
    Ok(())
}
