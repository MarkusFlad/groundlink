# groundlink

Async Rust library for CCSDS Space Packets and ECSS PUS-C telecommands and
telemetry over TCP, built on [kameo](https://crates.io/crates/kameo) actors
and [tokio](https://tokio.rs).

## Features

- **Generic TCP actors**: server, client, reader and writer actors that are
  generic over a message type and a codec. Any protocol that implements
  `MessageCodec<M>` gets the full set of actors. Optional TCP keepalive
  (`KeepAlive`) detects a failed peer within seconds instead of hours. A
  server serves one client at a time; `ConnectionPolicy` selects whether a
  new client waits for the current one to leave or replaces it.
- **Connection events and coupling**: servers and clients report
  `ConnectionEvent`s (connected, half closed, disconnected, connect failed)
  to an optional observer. A server and a client can be coupled, directly or
  through actors in between: when a peer connects to the server, the client
  connects to its remote address, and when either connection ends, the other
  one is closed. The events travel with the messages, so none is lost or
  overtaken.
- **Batches**: a server or client created with `batched` forwards the
  messages that arrive together as one `Batch<M>`, which raises the
  throughput of small messages several times without adding latency.
  Actors still handle one message at a time; `impl_batch_message!` adds
  the handling of batches.
- **CCSDS Space Packets** (CCSDS 133.0-B-2): packet types and a codec.
- **CCSDS Unsegmented Time Code (CUC)**: conversion from and to UTC.
- **ECSS PUS-C**: telecommand and telemetry packets on top of Space Packets,
  with typed messages for
  - service 1 (request verification) and
  - service 17 (test, "Are-You-Alive").
- **PUS actors**: `PusTcAcceptor` performs the acceptance check (packet
  format, CRC, APID, service and subtype) and reports it via service 1;
  `PusTestServiceActor` implements service 17; `PusTmStamper` numbers the
  telemetry of an APID consecutively and adds the time stamps.
- **SLE Transport Mapping Layer** (ISP1, CCSDS 913.1-B-2): TML messages and
  a codec, and `TmlLinkActor`, which runs the TML rules (context message,
  heartbeats, dead timer) on top of a `TmlServer` or `TmlClient` and
  exchanges the BER-encoded SLE PDUs with the SLE layer as `SlePdu`
  messages.
- **SLE PDUs and provider**: BER encoding and the PDUs of Return All Frames
  (CCSDS 911.1-B-4) and Forward CLTU (CCSDS 912.1-B-4), with BIND, UNBIND,
  START, STOP, TRANSFER-DATA and SCHEDULE-STATUS-REPORT, for FCLTU also
  ASYNC-NOTIFY and GET-PARAMETER (as needed by users such as ReatMetric);
  provider actors on top of the
  `TmlLinkActor`: a `SleAssociationActor` per port and a
  `RafServiceInstance` or `FcltuServiceInstance` per service instance.
  On the user side, `SleUserActor` binds, starts and stops RAF and FCLTU
  on top of a `TmlClient`; each operation is a message answered when the
  provider's return has arrived.
- **Space data link**: CLTUs with the BCH code (CCSDS 231.0-B-4), TC
  Transfer Frames (CCSDS 232.0-B-4) and TM Transfer Frames (CCSDS
  132.0-B-3), and the actors `TcFrameEncoder`/`TcFrameDecoder` and
  `TmFrameEncoder`/`TmFrameDecoder` between Space Packets and the SLE
  services. A PUS TC(17,1) can thus go from the ground over FCLTU to a
  simulated spacecraft, and its TM(1,1), TM(1,3), TM(17,2) and TM(1,7)
  come back over RAF (see `tests/sle_pus_loop.rs`).
- **Simple string protocol**: a length-prefixed string protocol, useful for
  getting started and for tests.
- **Test helpers**: `TestActor` records messages for assertions in tests
  (feature `test-util`).

The crate logs through [`tracing`](https://crates.io/crates/tracing) and
does not install a subscriber itself.

## Protocols and actors

| Protocol | Message | Codec | Actors |
|---|---|---|---|
| `simple_string` | `SimpleString` | `SimpleStringCodec` | `SimpleStringServer`, `SimpleStringClient`, … |
| `ccsds` | `SpacePacket` | `SpacePacketCodec` | `SpacePacketServer`, `SpacePacketClient`, … |
| `pus` | `PusPacket` | `PusCodec` | `PusServer`, `PusClient`, … |
| `tml` | `TmlMessage` | `TmlCodec` | `TmlServer`, `TmlClient`, … |

## Usage

The crate is not on crates.io yet. Add it as a git dependency:

```toml
[dependencies]
groundlink = { git = "https://github.com/MarkusFlad/groundlink" }
kameo = "0.22"
tokio = { version = "1", features = ["full"] }
```

A server that forwards every received `SimpleString` to a `TestActor`
(which requires the feature `test-util`):

```rust
use std::time::Duration;

use futures::SinkExt;
use kameo::actor::Spawn;
use groundlink::actor::tcp::GetLocalAddr;
use groundlink::{SimpleString, SimpleStringCodec, SimpleStringServer, TcpServerArgs, TestActor};
use tokio_util::codec::Framed;

#[tokio::main]
async fn main() {
    let received = TestActor::<SimpleString>::spawn(TestActor::new());
    let server = SimpleStringServer::spawn(TcpServerArgs::new(
        "127.0.0.1:0".parse().unwrap(),
        received.clone().recipient(),
        SimpleStringCodec::default(),
    ));
    let addr = server.ask(GetLocalAddr).await.unwrap();

    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut client = Framed::new(stream, SimpleStringCodec::default());
    client.send(SimpleString("hello".into())).await.unwrap();

    let messages = TestActor::assert_received(&received, 1, Duration::from_secs(1)).await;
    assert_eq!(messages, vec![SimpleString("hello".into())]);
}
```

## Examples

### PUS server and client

The PUS examples use separate TCP connections for telecommands and
telemetry:

```text
TC port: SpacePacketServer ──SpacePacket──▶ PusTcAcceptor ────────TM(1,x)───────────┐
                                                 │                                  │ PusTm
                                                 └──TC(17,1)──▶ PusTestServiceActor ┤
                                                                                    ▼
                                                                              PusTmStamper
                                                                                    │ PusPacket
                                                                                    ▼
TM port:                                                              TM client ◀── PusServer
```

Start the server with a TC port and a TM port:

```sh
cargo run --example pus_server -- 9000 9001
```

In a second terminal, start the interactive client:

```sh
cargo run --example pus_client -- 127.0.0.1 9000 9001
```

The client reads these commands from stdin:

| Command | Action |
|---|---|
| `Open` | Opens both connections (telemetry first) |
| `TC17_1` | Sends a TC(17,1) "Are-You-Alive" to APID 0x042 |
| `Close` | Closes both connections |
| `Quit` | Exits |

The server answers a TC(17,1) on the TM connection with TM(1,1) (acceptance),
TM(1,3) (start of execution), TM(17,2) (Are-You-Alive report) and TM(1,7)
(completion). The TC side
receives plain Space Packets, so that the `PusTcAcceptor` can answer an
invalid PUS packet (for example one with a CRC error) with TM(1,2) instead
of dropping it.

### SLE provider

An SLE provider with a RAF port and an FCLTU port:

```sh
cargo run --example sle_provider -- 5100 5101
```

The RAF service instance `sagr=1.spack=VST-PASS0001.rsl-fg=1.raf=onlt1`
on port 5100 delivers a generated frame every 100 ms to a user that has
bound and started it. The FCLTU service instance
`sagr=1.spack=VST-PASS0001.fsl-fg=1.cltu=cltu1` on port 5101 prints the
CLTUs it accepts. Both ports speak ISP1, so any SLE user (for example one
built on ESA's SLE API) can connect. The provider events (bound, started,
aborted, ...) are printed as well.

In a second terminal, the matching SLE user receives RAF frames for five
seconds and then sends five CLTUs:

```sh
cargo run --example sle_user -- 127.0.0.1 5100 5101
```

### PUS over SLE

A simulated spacecraft behind an SLE provider, and an interactive ground
segment that talks to it over RAF and FCLTU:

```text
sle_ground:     TC(17,1) ─▶ TcFrameEncoder ─CLTU─▶ SleUserActor ══FCLTU══╗
                                                                         ▼
sle_spacecraft: FcltuServiceInstance ─▶ TcFrameDecoder ─▶ PusTcAcceptor ─▶ PusTestServiceActor
                RafServiceInstance ◀─ TmFrameEncoder ◀─ PusTmStamper ◀─ TM(1,x), TM(17,2)
                       ║
sle_ground:            ╚══RAF══▶ SleUserActor ─▶ TmFrameDecoder ─▶ printed TM
```

```sh
cargo run --example sle_spacecraft -- 5200 5201
```

In a second terminal:

```sh
cargo run --example sle_ground -- 127.0.0.1 5200 5201
```

The ground segment reads the commands `Open` (binds and starts RAF and
FCLTU), `TC17_1`, `Close` (stops and unbinds) and `Quit` from stdin. Each
TC(17,1) goes as a CLTU with a TC Transfer Frame to the spacecraft, whose
TM(1,1), TM(1,3), TM(17,2) and TM(1,7) come back in TM Transfer Frames
and are printed. `sle_spacecraft` logs every TC it receives and every TM
it sends; `--log all --hex` adds the CLTUs, TC and TM Transfer Frames,
with hex dumps.

`sle_spacecraft` can also be commanded by
[ReatMetric](https://github.com/dariol83/reatmetric), an independent SLE
user: `tools/reatmetric/setup.sh` installs and configures it, see
[Testing the SLE provider with ReatMetric](docs/reatmetric.md).

### Space Packet relay with batches

A relay that forwards Space Packets between a peer that connects to it
and a target it connects to, and passes them between its actors in
batches:

```text
peer A ──▶ SpacePacketServer ──Batch──▶ PacketCounter ──Batch──▶ SpacePacketClient ──▶ peer B
       ◀──                   ◀──Batch── PacketCounter ◀──Batch──                   ◀──
```

```sh
cargo run --release --example space_packet_relay -- 9000 127.0.0.1 9001
```

This listens on port 9000 and, when a peer connects, connects to
`127.0.0.1:9001`. It prints the forwarded packets and bytes every five
seconds.

The example shows the three parts of the batched path: a server and a
client created with `batched`, which forward what arrives together as one
`Batch<SpacePacket>`; an actor in between, `PacketCounter`, whose handler
takes one packet at a time and gets the handling of batches from
`impl_batch_message!`; and its `Downstream`, through which the packets of
a batch leave it as one batch again. See
[Performance](docs/performance.md) for what batches gain.

### Simple string server

```sh
cargo run --example simple_string_server
```

Listens on `127.0.0.1:9000` and prints the received messages every five
seconds.

### Logging

Set the log level with `RUST_LOG`, for example:

```sh
RUST_LOG=debug cargo run --example pus_server -- 9000 9001
```

## Documentation

- [API documentation](https://markusflad.github.io/groundlink/) of the
  `main` branch; `cargo doc --open` builds it locally for the checked-out
  version.
- [WireMessage](docs/wire-message.md): how servers and clients send
  messages, and why message types implement the `WireMessage` marker
  trait.
- [Performance](docs/performance.md): what limits the throughput of
  the TCP actors, the optimizations made so far (among them batches
  between actors), the settings an application can tune (batch length,
  mailbox capacity, worker threads), what was tried and not adopted, and
  throughput measurements for each.
- [Testing the SLE provider with ReatMetric](docs/reatmetric.md): setting
  up ReatMetric as an independent SLE user for `sle_spacecraft`, with the
  setup script, what is configured and why, and the logs.

## Development

```sh
cargo build
cargo test
cargo doc --open
```

### Performance test

`tests/tcp_throughput.rs` measures how fast Space Packets pass through a
server coupled with a client:

```text
sender ──▶ SpacePacketServer ──▶ SpacePacketClient ──▶ receiver
```

For each size of the packet data field (16 bytes to 64 KiB), the sender
writes up to 256 MiB (at most 2 million packets) of pre-encoded packets,
and the receiver counts the bytes until all have arrived. Encoding and
decoding at the two ends are therefore not measured. The actors are
measured twice: with the server forwarding every packet on its own, and
forwarding batches (`TcpServerArgs::batched`). The same data is also sent
over a plain loopback connection without actors in between, as a
baseline.

The test is ignored by default and only meaningful in release mode:

```sh
cargo test --release --test tcp_throughput -- --ignored --nocapture
```

It prints one line per packet size: the throughput through the actors
with single packets and with batches, the throughput of the direct
connection, and the packets per second with batches. Single small
packets are limited by the packet rate, since each one passes three
mailboxes (reader, client, writer); batches pass them together. Large
packets are limited by copying the data. See
[Performance](docs/performance.md) for measured results.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this crate by you, as defined in the Apache-2.0
license, shall be licensed as above, without any additional terms or
conditions.
