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
- **Simple string protocol**: a length-prefixed string protocol, useful for
  getting started and for tests.
- **Test helpers**: `TestActor` records messages for assertions in tests.

The crate logs through [`tracing`](https://crates.io/crates/tracing) and
does not install a subscriber itself.

## Protocols and actors

| Protocol | Message | Codec | Actors |
|---|---|---|---|
| `simple_string` | `SimpleString` | `SimpleStringCodec` | `SimpleStringServer`, `SimpleStringClient`, … |
| `ccsds` | `SpacePacket` | `SpacePacketCodec` | `SpacePacketServer`, `SpacePacketClient`, … |
| `pus` | `PusPacket` | `PusCodec` | `PusServer`, `PusClient`, … |

## Usage

The crate is not on crates.io yet. Add it as a git dependency:

```toml
[dependencies]
groundlink = { git = "https://github.com/MarkusFlad/groundlink" }
kameo = "0.22"
tokio = { version = "1", features = ["full"] }
```

A server that forwards every received `SimpleString` to a `TestActor`:

```rust
use std::time::Duration;

use futures::SinkExt;
use kameo::actor::Spawn;
use groundlink::{
    GetLocalAddr, SimpleString, SimpleStringCodec, SimpleStringServer, TcpServerArgs,
    TestActor,
};
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
