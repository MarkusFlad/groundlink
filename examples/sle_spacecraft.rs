//! A simulated spacecraft behind an SLE provider: telecommands arrive as
//! CLTUs over FCLTU, telemetry leaves as TM Transfer Frames over RAF.
//!
//! ```text
//! FCLTU port ─▶ FcltuServiceInstance ─Cltu─▶ TcFrameDecoder ─SpacePacket─▶ TcLog ─▶ PusTcAcceptor ─TC(17,1)─▶ PusTestServiceActor
//!                                                                                        │ TM(1,x)               │ TM(17,2)
//!                                                                                        ▼                       ▼
//! RAF port   ◀─ RafServiceInstance ◀─RafFrame─ TmFrameEncoder ◀─PusPacket─ TmLog ◀─ PusTmStamper ◀──────────────┘
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
//! Usage: `cargo run --example sle_spacecraft -- [--log LAYERS] [--hex] <raf_port> <fcltu_port>`,
//! then `cargo run --example sle_ground -- 127.0.0.1 <raf_port> <fcltu_port>`.
//!
//! `--log` selects, comma-separated, which data units are logged on their
//! way through the spacecraft, in the order they pass:
//!
//! | Layer | Logged |
//! |---|---|
//! | `cltu` | every CLTU received: identification, length |
//! | `tc-frame` | every TC Transfer Frame: spacecraft, virtual channel, sequence number, type |
//! | `tc` | every telecommand: PUS service, APID, sequence count, ack flags; also one that is not valid PUS |
//! | `tm` | every telemetry packet: PUS service, APID, sequence count, the TC a report refers to |
//! | `tm-frame` | every TM Transfer Frame: virtual channel, frame counts, first header pointer |
//!
//! `all` and `none` select all or no layers; the default is `tc,tm`. With
//! `--hex`, the selected data units are also dumped in hex. There are no
//! CADUs: RAF carries the TM Transfer Frames as the ground station's
//! decoder delivers them.
//!
//! `RUST_LOG` sets the levels of everything else, e.g. `RUST_LOG=debug`.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context as _, anyhow};
use bytes::BytesMut;
use groundlink::actor::datalink::{LOG_TARGET_CLTU, LOG_TARGET_TC_FRAME, LOG_TARGET_TM_FRAME};
use groundlink::actor::sle::Cltu;
use groundlink::actor::tcp::GetLocalAddr;
use groundlink::protocol::pus::service1::VerificationReport;
use groundlink::protocol::pus::service17;
use groundlink::protocol::pus::service17::AreYouAliveReport;
use groundlink::protocol::sle::raf::RafFrame;
use groundlink::{
    Batch, ConnectionEvent, Downstream, FcltuConfig, FcltuServiceInstance, PusConfig, PusPacket, PusTc, PusTcAcceptor,
    PusTestServiceActor, PusTm, PusTmStamper, RafConfig, RafServiceInstance, SleAssociationActor, SleAssociationConfig,
    SlePdu, SleProviderEvent, SleServiceRegistry, SpacePacket, SpacePacketCodec, TcFrameConfig, TcFrameDecoder,
    TcpServerArgs, TmFrameConfig, TmFrameEncoder, TmlCodec, TmlLinkActor, TmlMessage, TmlResponderConfig, TmlServer,
};
use kameo::actor::{Actor, ActorRef, Recipient, Spawn};
use kameo::error::Infallible;
use kameo::mailbox;
use kameo::message::{Context, Message};
use tokio_util::codec::Encoder;
use tracing::{info, trace, warn};
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

/// Log target of the telecommands (`--log tc`).
const LOG_TARGET_TC: &str = "sle_spacecraft::tc";
/// Log target of the telemetry packets (`--log tm`).
const LOG_TARGET_TM: &str = "sle_spacecraft::tm";

/// The layers `--log` can select, with their log targets and the levels
/// for a one-line summary and for a hex dump.
const LAYERS: [(&str, &str, &str, &str); 5] = [
    ("cltu", LOG_TARGET_CLTU, "debug", "trace"),
    ("tc-frame", LOG_TARGET_TC_FRAME, "debug", "trace"),
    ("tc", LOG_TARGET_TC, "info", "trace"),
    ("tm", LOG_TARGET_TM, "info", "trace"),
    ("tm-frame", LOG_TARGET_TM_FRAME, "debug", "trace"),
];

/// The whole Space Packet, primary header included, in hex.
fn packet_hex(packet: &SpacePacket) -> String {
    let mut bytes = BytesMut::new();
    match SpacePacketCodec.encode(packet.clone(), &mut bytes) {
        Ok(()) => hex(&bytes),
        Err(err) => format!(" (not encodable: {err})"),
    }
}

/// Octets in hex, 32 per line.
fn hex(data: &[u8]) -> String {
    let mut text = String::new();
    for (i, chunk) in data.chunks(32).enumerate() {
        text.push_str(&format!("\n  {:04x}:", i * 32));
        for byte in chunk {
            text.push_str(&format!(" {byte:02x}"));
        }
    }
    text
}

/// The command line: the selected layers, hex dumps, the ports.
struct Options {
    layers: Vec<&'static str>,
    hex: bool,
    raf_port: u16,
    fcltu_port: u16,
}

const USAGE: &str = "usage: sle_spacecraft [--log LAYERS] [--hex] <raf_port> <fcltu_port>
  LAYERS: comma-separated cltu, tc-frame, tc, tm, tm-frame, all or none (default: tc,tm)";

fn parse_options() -> anyhow::Result<Options> {
    let mut layers = vec!["tc", "tm"];
    let mut hex = false;
    let mut ports = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let value = match arg.as_str() {
            "--hex" => {
                hex = true;
                continue;
            }
            "--log" => args.next().context(USAGE)?,
            _ => match arg.strip_prefix("--log=") {
                Some(value) => value.to_string(),
                None if arg.starts_with('-') => return Err(anyhow!("unknown option '{arg}'\n{USAGE}")),
                None => {
                    ports.push(
                        arg.parse::<u16>()
                            .with_context(|| format!("invalid port '{arg}'\n{USAGE}"))?,
                    );
                    continue;
                }
            },
        };
        layers.clear();
        for name in value.split(',').map(str::trim).filter(|name| !name.is_empty()) {
            match name {
                "all" => layers.extend(LAYERS.iter().map(|layer| layer.0)),
                "none" => layers.clear(),
                _ => match LAYERS.iter().find(|layer| layer.0 == name) {
                    Some(layer) => layers.push(layer.0),
                    None => return Err(anyhow!("unknown layer '{name}'\n{USAGE}")),
                },
            }
        }
    }
    let [raf_port, fcltu_port] = ports[..] else {
        return Err(anyhow!("{USAGE}"));
    };
    Ok(Options {
        layers,
        hex,
        raf_port,
        fcltu_port,
    })
}

/// `RUST_LOG` (default `info`), with the selected layers on and the others
/// off.
fn log_filter(options: &Options) -> anyhow::Result<EnvFilter> {
    let mut filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    for (name, target, summary, dump) in LAYERS {
        let level = match (options.layers.contains(&name), options.hex) {
            (false, _) => "off",
            (true, false) => summary,
            (true, true) => dump,
        };
        filter = filter.add_directive(format!("{target}={level}").parse()?);
    }
    Ok(filter)
}

/// Logs every telecommand on its way to the acceptance check.
struct TcLog {
    pus_config: PusConfig,
    acceptor: Downstream<SpacePacket>,
}

impl Actor for TcLog {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<SpacePacket> for TcLog {
    type Reply = ();

    async fn handle(&mut self, packet: SpacePacket, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let header = packet.header;
        match PusPacket::from_space_packet(packet.clone(), &self.pus_config) {
            Ok(PusPacket::Tc(tc)) => {
                let sec = &tc.secondary_header;
                let ack = sec.ack_flags;
                let flag = |set: bool, c: char| if set { c } else { '-' };
                info!(
                    target: LOG_TARGET_TC,
                    "TC received: TC({},{}) apid={:#05x} seq={} source={} ack={}{}{}{} app_data={} octets",
                    sec.service_type,
                    sec.message_subtype,
                    header.apid,
                    header.sequence_count,
                    sec.source_id,
                    flag(ack.acceptance, 'A'),
                    flag(ack.start, 'S'),
                    flag(ack.progress, 'P'),
                    flag(ack.completion, 'C'),
                    tc.app_data.len()
                );
            }
            Ok(PusPacket::Tm(_)) => warn!(
                target: LOG_TARGET_TC,
                "TC received: telemetry packet apid={:#05x} seq={} on the TC link",
                header.apid, header.sequence_count
            ),
            Err(err) => warn!(
                target: LOG_TARGET_TC,
                "TC received: space packet apid={:#05x} seq={} that is not a valid PUS TC: {err}",
                header.apid, header.sequence_count
            ),
        }
        trace!(target: LOG_TARGET_TC, "TC seq={}:{}", header.sequence_count, packet_hex(&packet));
        if let Err(err) = self.acceptor.send(packet).await {
            warn!("TC dropped, could not pass it on: {err}");
        }
    }
}

groundlink::impl_batch_message!(TcLog, SpacePacket, outputs = [acceptor]);

/// Logs every telemetry packet on its way into the TM frames.
struct TmLog {
    encoder: Downstream<PusPacket>,
}

impl Actor for TmLog {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<PusPacket> for TmLog {
    type Reply = ();

    async fn handle(&mut self, packet: PusPacket, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        let header = packet.header();
        let mut text = format!(
            "TM sent: TM({},{}) apid={:#05x} seq={}",
            packet.service_type(),
            packet.message_subtype(),
            header.apid,
            header.sequence_count
        );
        if let Ok(report) = VerificationReport::try_from(packet.clone()) {
            text += &format!(" {:?} for TC seq={}", report.kind, report.request_id.sequence_count);
        } else if AreYouAliveReport::try_from(packet.clone()).is_ok() {
            text += " are-you-alive report";
        }
        info!(target: LOG_TARGET_TM, "{text}");
        if tracing::enabled!(target: LOG_TARGET_TM, tracing::Level::TRACE)
            && let Ok(space_packet) = packet.to_space_packet(&PusConfig::default())
        {
            trace!(target: LOG_TARGET_TM, "TM seq={}:{}", header.sequence_count, packet_hex(&space_packet));
        }
        if let Err(err) = self.encoder.send(packet).await {
            warn!("TM dropped, could not pass it on: {err}");
        }
    }
}

groundlink::impl_batch_message!(TmLog, PusPacket, outputs = [encoder]);

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let options = parse_options()?;
    tracing_subscriber::fmt().with_env_filter(log_filter(&options)?).init();
    let (raf_port, fcltu_port) = (options.raf_port, options.fcltu_port);
    info!(
        "logging {}{}",
        if options.layers.is_empty() {
            "no data units".to_string()
        } else {
            options.layers.join(", ")
        },
        if options.hex && !options.layers.is_empty() {
            " with hex dumps"
        } else {
            ""
        }
    );

    // Telemetry: numbered and time-stamped, packed into TM frames that the
    // ground station receives and offers over RAF.
    let mut raf_config = RafConfig::new(RAF_SI.parse()?);
    raf_config.latency_limit = Duration::from_millis(100);
    let raf = RafServiceInstance::spawn(RafServiceInstance::new(raf_config));
    let tm_encoder = TmFrameEncoder::spawn(TmFrameEncoder::new(
        TmFrameConfig::new(SPACECRAFT_ID, 0),
        raf.clone().recipient::<Batch<RafFrame>>(),
    ));
    let tm_log = TmLog::spawn(TmLog {
        encoder: tm_encoder.recipient::<Batch<PusPacket>>().into(),
    });
    let stamper = PusTmStamper::spawn(PusTmStamper::new(APID, tm_log.recipient::<Batch<PusPacket>>()));
    let telemetry = stamper.recipient::<PusTm>();

    // Telecommands: CLTU and frame decoding, acceptance check, service 17.
    let test_service = PusTestServiceActor::spawn(PusTestServiceActor::new(APID, telemetry.clone()));
    // Also PUS-A/B telecommands (version 1), as ReatMetric sends them.
    let mut pus_config = PusConfig::default();
    pus_config.accept_tc_version_1 = true;
    let acceptor = PusTcAcceptor::spawn(
        PusTcAcceptor::new(APID, telemetry)
            .with_pus_config(pus_config)
            .with_service_handler(
                service17::SERVICE_TYPE,
                &[service17::ARE_YOU_ALIVE_REQUEST_SUBTYPE],
                test_service.recipient::<PusTc>(),
            ),
    );
    let tc_log = TcLog::spawn(TcLog {
        pus_config,
        acceptor: acceptor.recipient::<Batch<SpacePacket>>().into(),
    });
    let tc_decoder = TcFrameDecoder::spawn(TcFrameDecoder::new(
        TcFrameConfig::new(SPACECRAFT_ID, 0),
        tc_log.recipient::<Batch<SpacePacket>>(),
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
