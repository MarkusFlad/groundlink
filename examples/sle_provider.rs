//! SLE provider with a RAF port and an FCLTU port.
//!
//! ```text
//! RAF port:   user ◀──▶ TmlServer ◀──▶ TmlLinkActor ◀──▶ SleAssociationActor
//!                                          ▲                     │ ask
//!                                          │ transfer buffers    ▼
//!                                          └─────────── RafServiceInstance ◀──RafFrame── frame generator
//!
//! FCLTU port: user ◀──▶ TmlServer ◀──▶ TmlLinkActor ◀──▶ SleAssociationActor
//!                                                                │ ask
//!                                                                ▼
//!                                                       FcltuServiceInstance ──Cltu──▶ Radiator
//! ```
//!
//! The RAF service instance `sagr=1.spack=VST-PASS0001.rsl-fg=1.raf=onlt1`
//! gets a generated 1115-octet frame every 100 ms, and delivers them to a
//! bound user after RAF-START. The FCLTU service instance
//! `sagr=1.spack=VST-PASS0001.fsl-fg=1.cltu=cltu1` passes the accepted
//! CLTUs on to a `Radiator`, which prints them. Both accept versions 2 to 5
//! from any initiator. The provider events are printed too. Each port
//! serves one user at a time.
//!
//! Usage: `cargo run --example sle_provider -- <raf_port> <fcltu_port>`
//!
//! Set the log level with `RUST_LOG`, e.g. `RUST_LOG=debug`.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context as _, anyhow};
use bytes::Bytes;
use groundlink::actor::sle::Cltu;
use groundlink::actor::tcp::GetLocalAddr;
use groundlink::protocol::sle::common::Time;
use groundlink::protocol::sle::raf::RafFrame;
use groundlink::protocol::sle::raf::{AntennaId, FrameQuality};
use groundlink::{
    Batch, ConnectionEvent, FcltuConfig, FcltuServiceInstance, RafConfig, RafServiceInstance, SleAssociationActor,
    SleAssociationConfig, SlePdu, SleProviderEvent, SleServiceRegistry, TcpServerArgs, TmlCodec, TmlLinkActor,
    TmlMessage, TmlResponderConfig, TmlServer,
};
use kameo::actor::{Actor, ActorRef, Recipient, Spawn};
use kameo::error::Infallible;
use kameo::mailbox;
use kameo::message::{Context, Message};
use tracing_subscriber::EnvFilter;

const RAF_SI: &str = "sagr=1.spack=VST-PASS0001.rsl-fg=1.raf=onlt1";
const CLTU_SI: &str = "sagr=1.spack=VST-PASS0001.fsl-fg=1.cltu=cltu1";

/// The radiation side: prints every CLTU.
struct Radiator;

impl Actor for Radiator {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<Cltu> for Radiator {
    type Reply = ();

    async fn handle(&mut self, cltu: Cltu, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        println!("radiating CLTU {} ({} octets)", cltu.id, cltu.data.len());
    }
}

/// Prints the provider events.
struct EventPrinter;

impl Actor for EventPrinter {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<SleProviderEvent> for EventPrinter {
    type Reply = ();

    async fn handle(&mut self, event: SleProviderEvent, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        println!("{event:?}");
    }
}

/// Spawns a provider port on `port`: server, TML link actor and association
/// actor, wired to each other.
async fn spawn_port(
    port: u16,
    registry: SleServiceRegistry,
    events: Recipient<SleProviderEvent>,
) -> anyhow::Result<SocketAddr> {
    let association = SleAssociationActor::prepare_with_mailbox(mailbox::unbounded());
    let link = TmlLinkActor::prepare();
    let server = TmlServer::spawn(
        TcpServerArgs::new(
            SocketAddr::from(([0, 0, 0, 0], port)),
            link.actor_ref().clone().recipient(),
            TmlCodec::default(),
        )
        .with_observer(link.actor_ref().clone().reply_recipient::<ConnectionEvent>()),
    );
    let link_ref = link.actor_ref().clone();
    link.spawn(
        TmlLinkActor::responder(
            server.clone().recipient::<TmlMessage>(),
            server.clone().recipient(),
            association.actor_ref().clone().recipient::<Batch<SlePdu>>(),
            TmlResponderConfig::default(),
        )
        .with_events(association.actor_ref().clone().recipient()),
    );
    association
        .spawn(SleAssociationActor::new(link_ref, registry, SleAssociationConfig::default()).with_events(events));
    server
        .ask(GetLocalAddr)
        .await
        .map_err(|err| anyhow!("cannot listen on port {port}: {err}"))
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

    let usage = "usage: sle_provider <raf_port> <fcltu_port>";
    let mut args = std::env::args().skip(1);
    let raf_port = parse_port(args.next(), usage)?;
    let fcltu_port = parse_port(args.next(), usage)?;

    let raf = RafServiceInstance::spawn(RafServiceInstance::new(RafConfig::new(RAF_SI.parse()?)));
    let radiator = Radiator::spawn(Radiator);
    let fcltu = FcltuServiceInstance::spawn(FcltuServiceInstance::new(
        FcltuConfig::new(CLTU_SI.parse()?),
        radiator.recipient(),
    ));
    let registry = SleServiceRegistry::new()
        .with_raf(RAF_SI.parse()?, raf.clone())
        .with_fcltu(CLTU_SI.parse()?, fcltu);
    let events = EventPrinter::spawn(EventPrinter).recipient::<SleProviderEvent>();

    let raf_addr = spawn_port(raf_port, registry.clone(), events.clone()).await?;
    let fcltu_addr = spawn_port(fcltu_port, registry, events).await?;

    // The receiver chain: a generated frame every 100 ms, counting up.
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        for n in 0u32.. {
            interval.tick().await;
            let mut data = vec![0u8; 1115];
            data[..4].copy_from_slice(&n.to_be_bytes());
            let frame = RafFrame {
                earth_receive_time: Time::now(),
                antenna_id: AntennaId::Local(Bytes::from_static(b"ANT1")),
                data_link_continuity: 0,
                quality: FrameQuality::Good,
                private_annotation: None,
                data: Bytes::from(data),
            };
            if raf.tell(frame).await.is_err() {
                break;
            }
        }
    });

    println!("SLE provider: RAF {RAF_SI} on {raf_addr}, FCLTU {CLTU_SI} on {fcltu_addr} - press Ctrl+C to stop");
    tokio::signal::ctrl_c().await?;
    Ok(())
}
