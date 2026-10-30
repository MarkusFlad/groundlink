//! A simulated spacecraft behind an SLE provider: telecommands arrive as
//! CLTUs over FCLTU, telemetry leaves as TM Transfer Frames over RAF.
//!
//! ```text
//! FCLTU port ─▶ FcltuServiceInstance ─Cltu─▶ TcFrameDecoder ─SpacePacket─▶ PusTcAcceptor ─TC(17,1)─▶ PusTestServiceActor
//!                                                                               │ TM(1,x)               │ TM(17,2)
//!                                                                               ▼                       ▼
//! RAF port   ◀─ RafServiceInstance ◀─RafFrame─ TmFrameEncoder ◀─PusPacket─ PusTmStamper ◀──────────────┘
//! ```
//!
//! Spacecraft ID 0x2a, TC and TM on virtual channel 0, TM frames of 1115
//! octets; both with frame error control field. The application process
//! has APID 0x042 and supports TC(17,1), answered with TM(1,1), TM(1,3),
//! TM(17,2) and TM(1,7). Telecommands may also carry PUS version 1, as
//! ReatMetric sends them. The service instances are
//! `sagr=1.spack=VST-PASS0001.rsl-fg=1.raf=onlt1` (RAF) and
//! `sagr=1.spack=VST-PASS0001.fsl-fg=1.cltu=cltu1` (FCLTU). Telemetry is
//! only delivered while a RAF user has started the service; the rest is
//! discarded, as on a real online RAF.
//!
//! Usage: `cargo run --example sle_spacecraft -- <raf_port> <fcltu_port>`,
//! then `cargo run --example sle_ground -- 127.0.0.1 <raf_port> <fcltu_port>`.
//!
//! Set the log level with `RUST_LOG`, e.g. `RUST_LOG=debug`.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context as _, anyhow};
use groundlink::protocol::pus::service17;
use groundlink::{
    Batch, Cltu, ConnectionEvent, FcltuConfig, FcltuServiceInstance, GetLocalAddr, PusConfig, PusPacket, PusTc,
    PusTcAcceptor, PusTestServiceActor, PusTm, PusTmStamper, RafConfig, RafFrame, RafServiceInstance,
    SleAssociationActor, SleAssociationConfig, SlePdu, SleProviderEvent, SleServiceRegistry, SpacePacket,
    TcFrameConfig, TcFrameDecoder, TcpServerArgs, TmFrameConfig, TmFrameEncoder, TmlCodec, TmlLinkActor, TmlMessage,
    TmlResponderConfig, TmlServer,
};
use kameo::actor::{Actor, ActorRef, Recipient, Spawn};
use kameo::error::Infallible;
use kameo::mailbox;
use kameo::message::{Context, Message};
use tracing_subscriber::EnvFilter;

const RAF_SI: &str = "sagr=1.spack=VST-PASS0001.rsl-fg=1.raf=onlt1";
const CLTU_SI: &str = "sagr=1.spack=VST-PASS0001.fsl-fg=1.cltu=cltu1";
const SPACECRAFT_ID: u16 = 0x2a;
const APID: u16 = 0x042;

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
        println!("SLE: {event:?}");
    }
}

/// Spawns a provider port on `port` for the instances of `registry`.
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

    let usage = "usage: sle_spacecraft <raf_port> <fcltu_port>";
    let mut args = std::env::args().skip(1);
    let raf_port = parse_port(args.next(), usage)?;
    let fcltu_port = parse_port(args.next(), usage)?;

    // Telemetry: numbered and time-stamped, packed into TM frames that the
    // ground station receives and offers over RAF.
    let mut raf_config = RafConfig::new(RAF_SI.parse()?);
    raf_config.latency_limit = Duration::from_millis(100);
    let raf = RafServiceInstance::spawn(RafServiceInstance::new(raf_config));
    let tm_encoder = TmFrameEncoder::spawn(TmFrameEncoder::new(
        TmFrameConfig::new(SPACECRAFT_ID, 0),
        raf.clone().recipient::<Batch<RafFrame>>(),
    ));
    let stamper = PusTmStamper::spawn(PusTmStamper::new(APID, tm_encoder.recipient::<Batch<PusPacket>>()));
    let telemetry = stamper.recipient::<PusTm>();

    // Telecommands: CLTU and frame decoding, acceptance check, service 17.
    let test_service = PusTestServiceActor::spawn(PusTestServiceActor::new(APID, telemetry.clone()));
    // Also PUS-A/B telecommands (version 1), as ReatMetric sends them.
    let pus_config = PusConfig {
        accept_tc_version_1: true,
        ..PusConfig::default()
    };
    let acceptor = PusTcAcceptor::spawn(
        PusTcAcceptor::new(APID, telemetry)
            .with_pus_config(pus_config)
            .with_service_handler(
                service17::SERVICE_TYPE,
                &[service17::ARE_YOU_ALIVE_REQUEST_SUBTYPE],
                test_service.recipient::<PusTc>(),
            ),
    );
    let tc_decoder = TcFrameDecoder::spawn(TcFrameDecoder::new(
        TcFrameConfig::new(SPACECRAFT_ID, 0),
        acceptor.recipient::<SpacePacket>(),
    ));
    let fcltu = FcltuServiceInstance::spawn(FcltuServiceInstance::new(
        FcltuConfig::new(CLTU_SI.parse()?),
        tc_decoder.recipient::<Cltu>(),
    ));

    let registry = SleServiceRegistry::new()
        .with_raf(RAF_SI.parse()?, raf)
        .with_fcltu(CLTU_SI.parse()?, fcltu);
    let events = EventPrinter::spawn(EventPrinter).recipient::<SleProviderEvent>();
    let raf_addr = spawn_port(raf_port, registry.clone(), events.clone()).await?;
    let fcltu_addr = spawn_port(fcltu_port, registry, events).await?;

    println!(
        "Spacecraft {SPACECRAFT_ID:#x} (APID {APID:#05x}): RAF {RAF_SI} on {raf_addr}, FCLTU {CLTU_SI} on {fcltu_addr} \
         - press Ctrl+C to stop"
    );
    tokio::signal::ctrl_c().await?;
    Ok(())
}
