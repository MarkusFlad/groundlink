//! PUS server with separate TCP ports for telecommands and telemetry.
//!
//! ```text
//! TC port: PusServer ──PusPacket──▶ PusTcAcceptor ────────TM(1,x)───────────┐
//!                                        │                                  │
//!                                        └──TC(17,1)──▶ PusTestServiceActor ┤ Relay<PusPacket>
//!                                                                           ▼
//! TM port:                                                    TM client ◀── PusServer
//! ```
//!
//! Telecommands are received on the TC port. The application process has
//! APID 0x042 and supports TC(17,1). All telemetry (TM(1,1), TM(17,2),
//! TM(1,7)) is sent to the client connected to the TM port, in that order.
//! Telemetry produced while no client is connected to the TM port is
//! dropped. Each port serves one client at a time.
//!
//! Usage: `cargo run --example pus_server -- <tc_port> <tm_port>`
//!
//! Set the log level with `RUST_LOG`, e.g. `RUST_LOG=debug`.

use std::net::SocketAddr;

use anyhow::{anyhow, Context as _};
use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use groundlink::pus::service17;
use groundlink::{
    GetLocalAddr, PusServer, PusPacket, PusTc, PusTcAcceptor, PusTestServiceActor, Relay,
    TcpServerArgs,
};
use tracing::warn;
use tracing_subscriber::EnvFilter;

/// APID of the application process served by this example.
const APID: u16 = 0x042;

/// Downstream of the TM listener: clients are not expected to send
/// anything on the telemetry connection.
struct IgnoreIncoming;

impl Actor for IgnoreIncoming {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<PusPacket> for IgnoreIncoming {
    type Reply = ();

    async fn handle(&mut self, packet: PusPacket, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        warn!(?packet, "ignoring packet received on the TM connection");
    }
}

fn parse_port(arg: Option<String>, usage: &str) -> anyhow::Result<u16> {
    let arg = arg.context(usage.to_owned())?;
    arg.parse().with_context(|| format!("invalid port '{arg}'"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let usage = "usage: pus_server <tc_port> <tm_port>";
    let mut args = std::env::args().skip(1);
    let tc_port = parse_port(args.next(), usage)?;
    let tm_port = parse_port(args.next(), usage)?;

    // Telemetry: the TM server writes every `Relay<PusPacket>` to its
    // connected client.
    let tm_server = PusServer::spawn(TcpServerArgs {
        bind_addr: SocketAddr::from(([0, 0, 0, 0], tm_port)),
        downstream: IgnoreIncoming::spawn(IgnoreIncoming).recipient::<PusPacket>(),
    });
    let telemetry = tm_server.clone().recipient::<Relay<PusPacket>>();

    // Telecommands: acceptance check, then service 17.
    let acceptor = PusTcAcceptor::for_packets(APID, telemetry.clone());
    let test_service = PusTestServiceActor::spawn(
        PusTestServiceActor::new(APID, telemetry)
            .with_sequence_counter(acceptor.sequence_counter()),
    );
    let acceptor = PusTcAcceptor::spawn(acceptor.with_service_handler(
        service17::SERVICE_TYPE,
        &[service17::ARE_YOU_ALIVE_REQUEST_SUBTYPE],
        test_service.recipient::<PusTc>(),
    ));
    let tc_server = PusServer::spawn(TcpServerArgs {
        bind_addr: SocketAddr::from(([0, 0, 0, 0], tc_port)),
        downstream: acceptor.recipient::<PusPacket>(),
    });

    let tc_addr = tc_server
        .ask(GetLocalAddr)
        .await
        .map_err(|err| anyhow!("cannot listen on TC port {tc_port}: {err}"))?;
    let tm_addr = tm_server
        .ask(GetLocalAddr)
        .await
        .map_err(|err| anyhow!("cannot listen on TM port {tm_port}: {err}"))?;

    println!(
        "PUS server (APID {APID:#05x}): telecommands on {tc_addr}, telemetry on {tm_addr} - press Ctrl+C to stop"
    );
    tokio::signal::ctrl_c().await?;
    Ok(())
}
