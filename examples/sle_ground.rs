//! Interactive ground segment for the `sle_spacecraft` example: sends PUS
//! telecommands over SLE FCLTU and prints the PUS telemetry received over
//! SLE RAF.
//!
//! ```text
//! stdin ─TC(17,1)─▶ TcFrameEncoder ─Cltu─▶ SleUserActor (FCLTU) ══▶ FCLTU port
//! RAF port ══▶ SleUserActor (RAF) ─RafFrame─▶ TmFrameDecoder ─SpacePacket─▶ TmPrinter ─▶ stdout
//! ```
//!
//! Reads commands from stdin (case-insensitive):
//! - `Open`: binds and starts RAF (first, so no telemetry is lost) and FCLTU
//! - `Close`: stops and unbinds FCLTU and RAF
//! - `TC17_1`: sends a TC(17,1) "Are-You-Alive" to APID 0x042
//! - `Quit`: exits
//!
//! Against `sle_spacecraft`, a TC(17,1) is answered with TM(1,1), TM(1,3),
//! TM(17,2) and TM(1,7).
//!
//! Usage: `cargo run --example sle_ground -- <address> <raf_port> <fcltu_port>`

use std::net::SocketAddr;

use anyhow::Context as _;
use groundlink::protocol::ccsds::SEQUENCE_COUNT_MAX;
use groundlink::{
    AreYouAliveReport, AreYouAliveRequest, Batch, Cltu, CltuStart, ConnectionEvent, CucFormat, PusConfig, PusPacket,
    RafFrame, RafStart, RequestedFrameQuality, SleBind, SlePdu, SleStop, SleUnbind, SleUserActor, SleUserConfig,
    SleUserEvent, SpacePacket, TcFrameConfig, TcFrameEncoder, TcpClientArgs, TmFrameConfig, TmFrameDecoder, TmlClient,
    TmlCodec, TmlContext, TmlLinkActor, TmlMessage, VerificationReport,
};
use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::mailbox;
use kameo::message::{Context, Message};
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing_subscriber::EnvFilter;

const RAF_SI: &str = "sagr=1.spack=VST-PASS0001.rsl-fg=1.raf=onlt1";
const CLTU_SI: &str = "sagr=1.spack=VST-PASS0001.fsl-fg=1.cltu=cltu1";
const SPACECRAFT_ID: u16 = 0x2a;
const APID: u16 = 0x042;
const COMMANDS: &str = "Open, Close, TC17_1, Quit";

/// Prints the received telemetry packets.
struct TmPrinter;

impl Actor for TmPrinter {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<SpacePacket> for TmPrinter {
    type Reply = ();

    async fn handle(&mut self, packet: SpacePacket, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        match PusPacket::from_space_packet(packet, &PusConfig::default()) {
            Ok(packet) => println!("[RAF] < {}", describe(&packet)),
            Err(err) => println!("[RAF] < packet that is not PUS: {err}"),
        }
    }
}

groundlink::impl_batch_message!(TmPrinter, SpacePacket);

/// Prints the events of a user, e.g. that the provider aborted.
struct EventPrinter {
    service: &'static str,
}

impl Actor for EventPrinter {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<SleUserEvent> for EventPrinter {
    type Reply = ();

    async fn handle(&mut self, event: SleUserEvent, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        println!("[{}] {event:?}", self.service);
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
    if let PusPacket::Tm(tm) = packet
        && let Ok(time) = tm.secondary_header.cuc_time(CucFormat::default())
    {
        text += &format!(" time={time}");
    }
    if let Ok(report) = VerificationReport::try_from(packet.clone()) {
        text += &format!(" {:?} for TC seq={}", report.kind, report.request_id.sequence_count);
    } else if AreYouAliveReport::try_from(packet.clone()).is_ok() {
        text += " are-you-alive report";
    }
    text
}

/// Spawns a user for `addr`: client, initiating link actor and user actor.
/// With `frames`, a RAF user that passes the frames on to it.
fn spawn_user(
    service: &'static str,
    addr: SocketAddr,
    config: SleUserConfig,
    frames: Option<ActorRef<TmFrameDecoder>>,
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
    let actor = match frames {
        Some(frames) => SleUserActor::raf(client, link_ref, config, frames.recipient::<Batch<RafFrame>>()),
        None => SleUserActor::fcltu(client, link_ref, config),
    };
    let events = EventPrinter::spawn(EventPrinter { service }).recipient();
    user.spawn(actor.with_events(events));
    user_ref
}

/// Sends `msg` with `ask` and prints the outcome.
async fn request<M>(service: &str, what: &str, user: &ActorRef<SleUserActor>, msg: M) -> bool
where
    SleUserActor: Message<M, Reply = kameo::reply::DelegatedReply<Result<(), groundlink::SleUserError>>>,
    M: std::fmt::Debug + Send + 'static,
{
    match user.ask(msg).await {
        Ok(()) => {
            println!("[{service}] {what} done");
            true
        }
        Err(err) => {
            println!("[{service}] {what} failed: {err:?}");
            false
        }
    }
}

fn parse_port(arg: Option<String>, usage: &str) -> anyhow::Result<u16> {
    let arg = arg.context(usage.to_owned())?;
    arg.parse().with_context(|| format!("invalid port '{arg}'"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Warnings of the library, e.g. a rejected CLTU.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")))
        .init();

    let usage = "usage: sle_ground <address> <raf_port> <fcltu_port>";
    let mut args = std::env::args().skip(1);
    let address = args.next().context(usage)?;
    let raf_port = parse_port(args.next(), usage)?;
    let fcltu_port = parse_port(args.next(), usage)?;
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
    let raf_addr = resolve(raf_port).await?;
    let fcltu_addr = resolve(fcltu_port).await?;

    let tm_decoder = TmFrameDecoder::spawn(TmFrameDecoder::new(
        TmFrameConfig::new(SPACECRAFT_ID, 0),
        TmPrinter::spawn(TmPrinter).recipient::<Batch<SpacePacket>>(),
    ));
    let raf = spawn_user(
        "RAF",
        raf_addr,
        SleUserConfig::new("MOC1", "RAF_PORT", RAF_SI.parse()?),
        Some(tm_decoder),
    );
    let fcltu = spawn_user(
        "FCLTU",
        fcltu_addr,
        SleUserConfig::new("MOC1", "CLTU_PORT", CLTU_SI.parse()?),
        None,
    );
    // Created anew on every `Open`, so CLTUs and frames are numbered from 0.
    let mut tc_encoder: Option<ActorRef<TcFrameEncoder>> = None;

    println!("SLE ground: RAF from {raf_addr}, FCLTU to {fcltu_addr}. Commands: {COMMANDS}");
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut sequence_count: u16 = 0;

    while let Some(line) = lines.next_line().await? {
        match line.trim().to_ascii_lowercase().as_str() {
            "open" => {
                let start = RafStart {
                    requested_quality: RequestedFrameQuality::GoodFramesOnly,
                };
                if request("RAF", "BIND", &raf, SleBind).await {
                    request("RAF", "START", &raf, start).await;
                }
                if request("FCLTU", "BIND", &fcltu, SleBind).await
                    && request("FCLTU", "START", &fcltu, CltuStart { first_cltu_id: 0 }).await
                {
                    tc_encoder = Some(TcFrameEncoder::spawn(TcFrameEncoder::new(
                        TcFrameConfig::new(SPACECRAFT_ID, 0),
                        0,
                        fcltu.clone().recipient::<Cltu>(),
                    )));
                }
            }
            "close" => {
                tc_encoder = None;
                if request("FCLTU", "STOP", &fcltu, SleStop).await {
                    request("FCLTU", "UNBIND", &fcltu, SleUnbind).await;
                }
                if request("RAF", "STOP", &raf, SleStop).await {
                    request("RAF", "UNBIND", &raf, SleUnbind).await;
                }
            }
            "tc17_1" => {
                let Some(encoder) = &tc_encoder else {
                    println!("[FCLTU] not started, use Open first");
                    continue;
                };
                let packet = PusPacket::from(AreYouAliveRequest::new(APID, sequence_count));
                sequence_count = if sequence_count >= SEQUENCE_COUNT_MAX {
                    0
                } else {
                    sequence_count + 1
                };
                println!("[FCLTU] > {}", describe(&packet));
                if let Err(err) = encoder.tell(packet).await {
                    println!("[FCLTU] sending failed: {err}");
                }
            }
            "quit" | "exit" => break,
            "" => {}
            other => println!("unknown command '{other}'. Commands: {COMMANDS}"),
        }
    }
    Ok(())
}
