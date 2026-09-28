//! Interactive PUS client for the `pus_server` example, with separate TCP
//! connections for telecommands and telemetry, each handled by a
//! `PusClient` (`TcpClientActor<PusPacket, PusCodec>`).
//!
//! Reads commands from stdin (case-insensitive):
//! - `Open`: opens both connections (telemetry first, so no reply is lost)
//! - `Close`: closes both connections
//! - `TC17_1`: sends a TC(17,1) "Are-You-Alive" to APID 0x042 on the TC
//!   connection
//! - `Quit`: exits
//!
//! Every packet received on either connection is printed to the console.
//! Against the `pus_server` example, a TC(17,1) is answered on the TM
//! connection with TM(1,1), TM(1,3), TM(17,2) and TM(1,7).
//!
//! Usage: `cargo run --example pus_client -- <address> <tc_port> <tm_port>`

use std::net::SocketAddr;

use anyhow::Context as _;
use groundlink::ccsds::SEQUENCE_COUNT_MAX;
use groundlink::{
    AreYouAliveReport, AreYouAliveRequest, Close, Connect, ConnectionHalfClosed, CucFormat, PusClient, PusCodec,
    PusPacket, TcpClientArgs, VerificationReport,
};
use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing_subscriber::EnvFilter;

/// APID of the application process on the server.
const APID: u16 = 0x042;

const COMMANDS: &str = "Open, Close, TC17_1, Quit";

/// Prints the packets and connection state changes of one connection.
struct PacketPrinter {
    /// Name of the connection, e.g. "TM".
    connection: &'static str,
}

impl Actor for PacketPrinter {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<PusPacket> for PacketPrinter {
    type Reply = ();

    async fn handle(&mut self, packet: PusPacket, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        println!("[{}] < {}", self.connection, describe(&packet));
    }
}

impl Message<ConnectionHalfClosed> for PacketPrinter {
    type Reply = ();

    async fn handle(&mut self, msg: ConnectionHalfClosed, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        println!(
            "[{}] {:?} half of the connection to {} closed ({:?})",
            self.connection, msg.half, msg.peer_addr, msg.reason
        );
    }
}

/// One-line description of a packet.
fn describe(packet: &PusPacket) -> String {
    let (service, subtype) = (packet.service_type(), packet.message_subtype());
    let header = packet.header();
    let prefix = match packet {
        PusPacket::Tc(_) => format!("TC({service},{subtype})"),
        PusPacket::Tm(_) => format!("TM({service},{subtype})"),
    };
    let mut text = format!("{prefix} apid={:#05x} seq={}", header.apid, header.sequence_count);

    if let PusPacket::Tm(tm) = packet {
        match tm.secondary_header.cuc_time(CucFormat::default()) {
            Ok(time) => text += &format!(" time={time}"),
            Err(_) => text += " time=?",
        }
    }

    if let Ok(report) = VerificationReport::try_from(packet.clone()) {
        text += &format!(" {:?} for TC seq={}", report.kind, report.request_id.sequence_count);
    } else if AreYouAliveReport::try_from(packet.clone()).is_ok() {
        text += " are-you-alive report";
    } else if !packet.user_data().is_empty() {
        text += &format!(" data={:02x?}", &packet.user_data()[..]);
    }
    text
}

/// Spawns a client for `remote_addr` whose packets and state changes are
/// printed under the name `connection`.
fn spawn_client(connection: &'static str, remote_addr: SocketAddr) -> ActorRef<PusClient> {
    let printer = PacketPrinter::spawn(PacketPrinter { connection });
    let args = TcpClientArgs::new(
        remote_addr,
        printer.clone().recipient::<PusPacket>(),
        PusCodec::default(),
    )
    .with_on_half_closed(printer.recipient::<ConnectionHalfClosed>());
    PusClient::spawn(args)
}

async fn open(connection: &str, client: &ActorRef<PusClient>) {
    match client.ask(Connect).await {
        Ok(peer) => println!("[{connection}] connected to {peer}"),
        Err(err) => println!("[{connection}] connect failed: {err}"),
    }
}

async fn close(connection: &str, client: &ActorRef<PusClient>) {
    if let Err(err) = client.tell(Close).await {
        println!("[{connection}] close failed: {err}");
    }
}

fn parse_port(arg: Option<String>, usage: &str) -> anyhow::Result<u16> {
    let arg = arg.context(usage.to_owned())?;
    arg.parse().with_context(|| format!("invalid port '{arg}'"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Warnings of the library, e.g. a message dropped while not connected.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")))
        .init();

    let usage = "usage: pus_client <address> <tc_port> <tm_port>";
    let mut args = std::env::args().skip(1);
    let address = args.next().context(usage)?;
    let tc_port = parse_port(args.next(), usage)?;
    let tm_port = parse_port(args.next(), usage)?;
    let resolve = |port: u16| {
        let address = address.clone();
        async move {
            tokio::net::lookup_host((address.as_str(), port))
                .await
                .with_context(|| format!("cannot resolve {address}"))?
                .next()
                .with_context(|| format!("no address for {address}"))
        }
    };
    let tc_addr = resolve(tc_port).await?;
    let tm_addr = resolve(tm_port).await?;

    let tc_client = spawn_client("TC", tc_addr);
    let tm_client = spawn_client("TM", tm_addr);

    println!("PUS client: telecommands to {tc_addr}, telemetry from {tm_addr}. Commands: {COMMANDS}");
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut sequence_count: u16 = 0;

    while let Some(line) = lines.next_line().await? {
        match line.trim().to_ascii_lowercase().as_str() {
            "open" => {
                open("TM", &tm_client).await;
                open("TC", &tc_client).await;
            }
            "close" => {
                close("TC", &tc_client).await;
                close("TM", &tm_client).await;
            }
            "tc17_1" => {
                let request = AreYouAliveRequest::new(APID, sequence_count);
                sequence_count = if sequence_count >= SEQUENCE_COUNT_MAX {
                    0
                } else {
                    sequence_count + 1
                };
                let packet = PusPacket::from(request);
                println!("[TC] > {}", describe(&packet));
                if let Err(err) = tc_client.tell(packet).await {
                    println!("[TC] sending failed: {err}");
                }
            }
            "quit" | "exit" => break,
            "" => {}
            other => println!("unknown command '{other}'. Commands: {COMMANDS}"),
        }
    }
    Ok(())
}
