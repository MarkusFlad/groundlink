//! SLE user for the `sle_provider` example: receives RAF frames for a few
//! seconds and sends some CLTUs.
//!
//! ```text
//! RAF:   SleUserActor ◀──▶ TmlLinkActor ◀──▶ TmlClient ◀──▶ provider RAF port
//!             │ Batch<RafFrame>
//!             ▼
//!        FramePrinter
//!
//! FCLTU: SleUserActor ◀──▶ TmlLinkActor ◀──▶ TmlClient ◀──▶ provider FCLTU port
//! ```
//!
//! Binds to the RAF service instance, starts it, prints the received frames
//! for five seconds, stops and unbinds. Then binds to the FCLTU service
//! instance, sends five CLTUs, prints the returns, stops and unbinds.
//!
//! Usage: `cargo run --example sle_user -- <address> <raf_port> <fcltu_port>`,
//! e.g. with `cargo run --example sle_provider -- 5100 5101` running:
//! `cargo run --example sle_user -- 127.0.0.1 5100 5101`
//!
//! Set the log level with `RUST_LOG`, e.g. `RUST_LOG=debug`.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use anyhow::{Context as _, anyhow};
use bytes::Bytes;
use groundlink::protocol::sle::common::ConditionalTime;
use groundlink::{
    Batch, Cltu, CltuStart, ConnectionEvent, RafFrame, RafStart, RequestedFrameQuality, SleBind, SlePdu, SleStop,
    SleUnbind, SleUserActor, SleUserConfig, TcpClientArgs, TmlClient, TmlCodec, TmlContext, TmlLinkActor, TmlMessage,
};
use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::mailbox;
use kameo::message::{Context, Message};
use tracing_subscriber::EnvFilter;

const RAF_SI: &str = "sagr=1.spack=VST-PASS0001.rsl-fg=1.raf=onlt1";
const CLTU_SI: &str = "sagr=1.spack=VST-PASS0001.fsl-fg=1.cltu=cltu1";

/// Prints every transfer buffer.
struct FramePrinter;

impl Actor for FramePrinter {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<Batch<RafFrame>> for FramePrinter {
    type Reply = ();

    async fn handle(&mut self, frames: Batch<RafFrame>, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        for frame in frames {
            let number = u32::from_be_bytes(frame.data[..4].try_into().unwrap_or_default());
            println!(
                "frame {number}: {} octets, continuity {}",
                frame.data.len(),
                frame.data_link_continuity
            );
        }
    }
}

/// Spawns a user for `addr`: client, initiating TML link actor and user
/// actor, wired to each other. `frames` selects RAF.
fn spawn_user(
    addr: SocketAddr,
    config: SleUserConfig,
    frames: Option<ActorRef<FramePrinter>>,
) -> ActorRef<SleUserActor> {
    let user = SleUserActor::prepare_with_mailbox(mailbox::unbounded());
    let link = TmlLinkActor::prepare();
    let client = TmlClient::spawn(
        TcpClientArgs::new(addr, link.actor_ref().clone().recipient(), TmlCodec::default())
            .with_observer(link.actor_ref().clone().reply_recipient::<ConnectionEvent>()),
    );
    let link_ref = link.actor_ref().clone();
    link.spawn(
        TmlLinkActor::initiator(
            client.clone().recipient::<TmlMessage>(),
            client.clone().recipient(),
            user.actor_ref().clone().recipient::<Batch<SlePdu>>(),
            TmlContext::new(30, 3),
        )
        .with_events(user.actor_ref().clone().recipient()),
    );
    let user_ref = user.actor_ref().clone();
    user.spawn(match frames {
        Some(frames) => SleUserActor::raf(client, link_ref, config, frames.recipient::<Batch<RafFrame>>()),
        None => SleUserActor::fcltu(client, link_ref, config),
    });
    user_ref
}

/// Turns the error of a request into an `anyhow::Error` naming `what`.
fn failed<E: std::fmt::Debug>(what: &'static str) -> impl FnOnce(E) -> anyhow::Error {
    move |err| anyhow!("{what} failed: {err:?}")
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

    let usage = "usage: sle_user <address> <raf_port> <fcltu_port>";
    let mut args = std::env::args().skip(1);
    let address: IpAddr = args.next().context(usage)?.parse().context("invalid address")?;
    let raf_port = parse_port(args.next(), usage)?;
    let fcltu_port = parse_port(args.next(), usage)?;

    let raf = spawn_user(
        SocketAddr::new(address, raf_port),
        SleUserConfig::new("MOC1", "RAF_PORT", RAF_SI.parse()?),
        Some(FramePrinter::spawn(FramePrinter)),
    );
    raf.ask(SleBind).await.map_err(failed("RAF-BIND"))?;
    let start = RafStart {
        requested_quality: RequestedFrameQuality::AllFrames,
    };
    raf.ask(start).await.map_err(failed("RAF-START"))?;
    tokio::time::sleep(Duration::from_secs(5)).await;
    raf.ask(SleStop).await.map_err(failed("RAF-STOP"))?;
    raf.ask(SleUnbind).await.map_err(failed("RAF-UNBIND"))?;
    println!("RAF done");

    let fcltu = spawn_user(
        SocketAddr::new(address, fcltu_port),
        SleUserConfig::new("MOC1", "CLTU_PORT", CLTU_SI.parse()?),
        None,
    );
    fcltu.ask(SleBind).await.map_err(failed("CLTU-BIND"))?;
    fcltu
        .ask(CltuStart { first_cltu_id: 1 })
        .await
        .map_err(failed("CLTU-START"))?;
    for id in 1..=5 {
        let cltu = Cltu {
            id,
            earliest_transmission_time: ConditionalTime::Undefined,
            latest_transmission_time: ConditionalTime::Undefined,
            delay_time_us: 0,
            data: Bytes::from(vec![0xeb, 0x90, id as u8, 0, 0, 0, 0, 0, 0, 0xc5]),
        };
        let ret = fcltu.ask(cltu).await.map_err(failed("CLTU-TRANSFER-DATA"))?;
        println!(
            "CLTU {id}: {:?}, next expected {}, buffer available {} octets",
            ret.result, ret.cltu_id, ret.buffer_available
        );
    }
    fcltu.ask(SleStop).await.map_err(failed("CLTU-STOP"))?;
    fcltu.ask(SleUnbind).await.map_err(failed("CLTU-UNBIND"))?;
    println!("FCLTU done");
    Ok(())
}
