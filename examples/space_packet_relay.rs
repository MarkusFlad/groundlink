//! Relay for CCSDS Space Packets that passes them between its actors in
//! batches, for a high throughput of small packets.
//!
//! ```text
//! peer A ──▶ SpacePacketServer ──Batch──▶ PacketCounter ──Batch──▶ SpacePacketClient ──▶ peer B
//!        ◀──                   ◀──Batch── PacketCounter ◀──Batch──                   ◀──
//! ```
//!
//! When a peer connects to the listen port, the relay connects to the
//! target address; everything one of them sends is forwarded to the other.
//! When either connection ends, the other one is closed.
//!
//! Three things make the packets travel in batches:
//! - The server and the client are created with `batched`. Their readers
//!   then forward the packets that one read from the socket has delivered
//!   as one `Batch<SpacePacket>`. They never wait for more packets, so a
//!   single packet is forwarded at once, as a batch of one.
//! - The server and the client write a `Batch<SpacePacket>` they receive to
//!   their connection.
//! - The `PacketCounter` in between handles one packet at a time;
//!   `impl_batch_message!` adds the handling of a batch. Because it sends
//!   through a `Downstream` named as an output, the packets of a batch
//!   leave it as one batch again.
//!
//! The connection events take the same way as the packets, through the
//! counters, so that they stay in order with them.
//!
//! Usage: `cargo run --release --example space_packet_relay -- <listen_port> <target_address> <target_port>`
//!
//! The relay prints the forwarded packets and bytes every five seconds.
//! Set the log level with `RUST_LOG`, e.g. `RUST_LOG=debug`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context as _, anyhow};
use groundlink::protocol::ccsds::PRIMARY_HEADER_LEN;
use groundlink::{
    Batch, ConnectionEvent, Downstream, GetLocalAddr, SpacePacket, SpacePacketClient, SpacePacketCodec,
    SpacePacketServer, TcpClientArgs, TcpServerArgs, impl_batch_message,
};
use kameo::actor::{Actor, ActorRef, Recipient, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use tracing::warn;
use tracing_subscriber::EnvFilter;

/// Packets and bytes forwarded in one direction.
#[derive(Default)]
struct Counters {
    packets: AtomicU64,
    bytes: AtomicU64,
}

/// Counts the packets of one direction and forwards them, together with
/// the connection events of the actor they come from.
struct PacketCounter {
    counters: Arc<Counters>,
    /// The server or client that writes the packets to its peer.
    packets: Downstream<SpacePacket>,
    events: Recipient<ConnectionEvent>,
}

impl Actor for PacketCounter {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// The handler for a single packet; this is all the logic of the actor.
impl Message<SpacePacket> for PacketCounter {
    type Reply = ();

    async fn handle(&mut self, packet: SpacePacket, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let len = PRIMARY_HEADER_LEN + packet.data.len();
        self.counters.packets.fetch_add(1, Ordering::Relaxed);
        self.counters.bytes.fetch_add(len as u64, Ordering::Relaxed);

        if let Err(err) = self.packets.send(packet).await {
            warn!("packet dropped: {err}");
        }
    }
}

// Handles `Batch<SpacePacket>` by calling the handler above for each
// packet. What the handler sends to `packets` meanwhile is collected and
// sent on as one batch when the batch is done.
impl_batch_message!(PacketCounter, SpacePacket, outputs = [packets]);

impl Message<ConnectionEvent> for PacketCounter {
    type Reply = ();

    async fn handle(&mut self, event: ConnectionEvent, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if let Err(err) = self.events.tell(event).await {
            warn!("connection event dropped: {err}");
        }
    }
}

fn parse_port(arg: Option<String>, usage: &str) -> anyhow::Result<u16> {
    let arg = arg.context(usage.to_owned())?;
    arg.parse().with_context(|| format!("invalid port '{arg}'"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let usage = "usage: space_packet_relay <listen_port> <target_address> <target_port>";
    let mut args = std::env::args().skip(1);
    let listen_port = parse_port(args.next(), usage)?;
    let target_address = args.next().context(usage)?;
    let target_port = parse_port(args.next(), usage)?;
    let target_addr: SocketAddr = format!("{target_address}:{target_port}")
        .parse()
        .with_context(|| format!("invalid target address '{target_address}'"))?;

    let (to_target, to_listener) = (Arc::new(Counters::default()), Arc::new(Counters::default()));

    // The server and the client each send to the counter on the way to the
    // other, so the client gets its reference before it starts.
    let client = SpacePacketClient::prepare();
    let counter_to_target = PacketCounter::spawn(PacketCounter {
        counters: to_target.clone(),
        packets: client.actor_ref().clone().recipient::<Batch<SpacePacket>>().into(),
        events: client.actor_ref().clone().recipient(),
    });
    let server = SpacePacketServer::spawn(
        TcpServerArgs::batched(
            SocketAddr::from(([0, 0, 0, 0], listen_port)),
            counter_to_target.clone().recipient(),
            SpacePacketCodec,
        )
        .with_observer(counter_to_target.reply_recipient::<ConnectionEvent>()),
    );
    let counter_to_listener = PacketCounter::spawn(PacketCounter {
        counters: to_listener.clone(),
        packets: server.clone().recipient::<Batch<SpacePacket>>().into(),
        events: server.clone().recipient(),
    });
    client.spawn(
        TcpClientArgs::batched(target_addr, counter_to_listener.clone().recipient(), SpacePacketCodec)
            .with_observer(counter_to_listener.reply_recipient::<ConnectionEvent>()),
    );

    let listen_addr = server
        .ask(GetLocalAddr)
        .await
        .map_err(|err| anyhow!("cannot listen on port {listen_port}: {err}"))?;
    println!("Space Packet relay: {listen_addr} <-> {target_addr} - press Ctrl+C to stop");

    // Print what has been forwarded, every 5 seconds.
    tokio::spawn(async move {
        let period = Duration::from_secs(5);
        let (mut last_to_target, mut last_to_listener) = (0, 0);
        loop {
            tokio::time::sleep(period).await;
            for (name, counters, last) in [
                ("to target", &to_target, &mut last_to_target),
                ("to listener", &to_listener, &mut last_to_listener),
            ] {
                let packets = counters.packets.load(Ordering::Relaxed);
                let bytes = counters.bytes.load(Ordering::Relaxed);
                let rate = (bytes - *last) as f64 / period.as_secs_f64() / 1e6;
                *last = bytes;
                println!("{name}: {packets} packets, {bytes} bytes in total, {rate:.1} MB/s");
            }
        }
    });

    tokio::signal::ctrl_c().await?;
    Ok(())
}
