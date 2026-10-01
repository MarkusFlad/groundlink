# Performance

This document explains what limits the throughput of the TCP actors, the
optimizations made so far, the settings an application can tune, what was
tried and not adopted, and the measured throughput for each.

Every message that goes over a connection is its own kameo message: one
`SpacePacket`, one `PusPacket`, one `SimpleString`. That keeps the actors
simple, because any actor can hand a single message to a server, a client
or a writer. The optimizations below keep it that way; they only change
how the actors move these messages to and from the socket and between
each other.

## The path of a packet

In a server coupled with a client, a packet passes these steps:

```text
socket ─read─▶ stream task ─▶ [reader mailbox] ─▶ reader ─▶ [client mailbox] ─▶ client
       ─▶ [writer mailbox] ─▶ writer ─write─▶ socket
```

Each hop between tasks costs a few microseconds: the message is queued,
and the receiving task may have to be woken up, often on another thread.
Each system call costs a few microseconds as well, no matter how many
bytes it transfers. For small packets these fixed costs dominate, so the
limit is the number of packets per second rather than the number of
bytes.

## Where the time goes

A CPU profile (`perf`) of 1 KiB packets through a coupled server and
client, after optimizations 1 and 2, showed this distribution:

| Share | What |
|---:|---|
| ~37% | Passing messages between actors: tokio channels, semaphores and wakers, kameo's sending, receiving and dispatching |
| ~15% | System calls of the actors: sending (writer) and receiving (reader) |
| ~11% | The test's own sender and receiver |
| ~7–8% | Worker threads going to sleep and being woken (futex, epoll) |
| 7% | Memory allocation |
| 5% | Copying packet data in the encoder |
| 3.4% | Tracing spans created by kameo for every message |
| 3% | Codec |
| 3% | tokio scheduler |

The hops between actors dominate. A CPU profile shows where time is
spent, though, not which stage of the pipeline sets its pace: making one
stage cheaper helps only if that stage is the bottleneck (see
[delayed writes](#delayed-writes)).

## Optimization 1: write batching

Originally the writer made one `write` system call per packet. It still
receives one kameo message per packet, but it no longer writes each one
immediately. Instead:

1. It **encodes** each message into its buffer, behind the messages
   already there.
2. After the first message into an empty buffer, it sends itself a
   **`Flush`** message. That message lands in the mailbox *behind* all
   messages already waiting, so by the time it is handled, all of them
   have been encoded.
3. On `Flush`, it **writes the whole buffer** with as few system calls as
   possible.
4. If the buffer reaches `WRITE_BATCH_LEN` (64 KiB) before that, it
   writes right away, so the buffer stays bounded.

```text
mailbox:  [P1] [P2] [P3] ... [Pn] [Flush]
             │    │    │        │     │
             ▼    ▼    ▼        ▼     ▼
buffer:   P1 + P2 + P3 + ... + Pn ──write──▶ socket
```

No timer is involved. When the writer is idle, a single message waits
only for its own `Flush` to pass through the mailbox and is then written.
Under load, many messages are waiting, and each `Flush` writes all of
them together. The writer thus adapts to the load by itself: low latency
when idle, large writes when busy.

Details:

- **Only one `Flush` at a time.** A flag records whether a `Flush` is
  already queued, so the mailbox holds at most one.
- **Full mailbox.** The writer queues its `Flush` with `try_send`. If the
  mailbox is full, that fails; but then more messages are waiting behind
  the current one, and the next of them tries again. The buffer is
  therefore always written eventually. Data messages are still sent to
  the writer with a waiting `tell`, so backpressure works as before.
- **Buffered data.** The buffer holds up to 64 KiB (or one larger
  message) that has left the mailbox but not yet been written. Senders
  therefore start to wait a little later than before, by at most that
  amount per connection.
- **Encoding errors.** If the codec rejects a message, the buffer is cut
  back to its length before that message. No partial frame reaches the
  peer, and the other buffered messages are unaffected.
- **Shutdown.** `Shutdown<M>` first writes what is still buffered, then
  shuts the write half down. Queued messages are not lost. The shutdown
  token, which ends a writer that is stuck after the grace period, still
  drops the buffer and closes at once.
- **Write errors.** A failed write, or one that makes no progress for the
  write timeout, closes the connection. It now affects the whole batch
  rather than a single message.

## Optimization 2: a larger read buffer

The reader decodes with tokio-util's `FramedRead`, which reads into a
buffer of 8 KiB by default. One `read` system call can therefore fetch at
most about 8 KiB, i.e. about eight 1 KiB packets. The reader now creates
the `FramedRead` with a buffer of `READ_BUFFER_LEN` (64 KiB), so one
system call can fetch up to 64 KiB of packets that have already arrived.
This is the counterpart of write batching on the read side.

It is a one-line change without effect on the behavior: the reader still
forwards one message per packet, in order. The buffer takes up to 64 KiB
per connection. 128 and 256 KiB were measured as well and brought no
further gain.

## Optimization 3: kameo without tracing

kameo's default feature `tracing` records the current tracing span with
every message sent and runs every message handler inside a span, even
when no tracing subscriber is installed. groundlink depends on kameo with
`default-features = false`, which removes this cost from every hop.
groundlink's own logging uses the `tracing` crate directly and is not
affected.

An application that wants kameo's spans, e.g. to trace messages with
OpenTelemetry, can enable the feature in its own `Cargo.toml`; Cargo
merges the features of all dependents:

```toml
kameo = { version = "0.22", features = ["tracing"] }
```

## Tuning in the application

### Mailbox capacity

The mailboxes of the reader and writer of each connection hold
`DEFAULT_MAILBOX_CAPACITY` (64, kameo's default) messages. With small
mailboxes, a sending stage often has to wait for the receiving one and
wakes it up for a few messages only. With larger mailboxes, both work in
longer stretches.

This is a trade-off with backpressure: a larger mailbox lets more
messages queue up before the sender has to wait. The default therefore
stays at 64, and an application can choose a larger capacity:

```rust
let server = PusServer::spawn(
    TcpServerArgs::new(addr, downstream, PusCodec::default()).with_mailbox_capacity(256),
);
```

`with_mailbox_capacity` sets the mailboxes of the reader and writer. A
client or server that is itself on the path of the packets, like the
client coupled with a server, gets its mailbox when it is spawned:

```rust
let client = PusClient::prepare_with_mailbox(kameo::mailbox::bounded(256));
```

A capacity of 256 raised the throughput of 1 KiB packets by about
10–15%; 1024 brought no further gain.

### Worker threads

tokio's multi-threaded runtime starts one worker thread per CPU thread by
default. On the test machine, 2 cores with hyperthreading, 2 worker
threads were about 13% faster than the default 4: the stages of the
pipeline disturb each other less. A single thread (`current_thread`
runtime or 1 worker) was about 27% slower, so the stages do benefit from
running in parallel.

How many worker threads are best depends on the machine and on what else
the application runs. It is worth measuring on the target system:

```rust
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() { /* ... */ }
```

## Tried and not adopted

### Reading without the reader's mailbox

The reader attaches its socket to itself as a stream (`attach_stream`).
kameo runs such a stream in a task of its own, which sends every packet
into the reader's mailbox; the reader then forwards it to `downstream`.
The packet thus passes the reader's mailbox without need.

As an experiment, the reader was changed to spawn its own read task that
sent each packet directly to `downstream`, with the reader actor handling
only the end of the reading and `Shutdown<M>`. To keep the guarantees
(the closed read half reported exactly once, no packet after the report),
the actor had to wait for the read task on `Shutdown<M>` and receive an
internal message when reading ended.

Measured on top of write batching, with the default read buffer, it
raised the throughput by about 20% for 16-byte packets, by about 5–10%
for 64 to 1024 bytes, and not measurably above that. For the main use
case of about 1 KiB packets, the gain did not justify the more complex
reader, so the change was not adopted. It remains an option if very
small packets matter.

### Delayed writes

The profile showed that the writer spent almost twice as much time in
`send` system calls as the test's sender for the same amount of data:
its batches were much smaller than 64 KiB, because it writes as soon as
its mailbox is empty. As an experiment, the writer waited 1 ms or 5 ms
before writing, so that more packets could collect.

The throughput of 1 KiB packets changed by at most 3–5%, within the
variation between runs. The writer was not the bottleneck: it was waiting
for the client that feeds it, so saving time in the writer did not speed
up the pipeline. Since the delay also adds latency, it was not adopted.

## Measurements

The [throughput test](../tests/tcp_throughput.rs) sends pre-encoded Space
Packets through a `SpacePacketServer` coupled with a `SpacePacketClient`:

```text
sender ──▶ SpacePacketServer ──▶ SpacePacketClient ──▶ receiver
```

It measures the actors with the default mailbox capacity and with a
capacity of 256, and for comparison sends the same data over a plain
loopback connection without actors in between ("direct"). Run it with:

```sh
cargo test --release --test tcp_throughput -- --ignored --nocapture
```

Setup: Intel Core i5-3210M (2 cores, 4 threads), Linux 6.8, Rust 1.89,
default release profile, loopback. Ranges show the lowest and highest
value of several runs. Differences of about 10% can be variation between
runs: the direct connection alone varied between 4.8 and 28.8 Gbit/s.

### The optimizations

With the default settings (4 worker threads, mailbox capacity 64). The
original version was measured once, the others two or three times.

Throughput through the actors:

| Data length (bytes) | Original | 1: write batching | 2: + read buffer | 3: + no tracing (current) |
|---:|---:|---:|---:|---:|
| 16 | 32.0 Mbit/s | 85.6–88.0 Mbit/s | 89.6 Mbit/s | 99.2–100.0 Mbit/s |
| 64 | 117.6 Mbit/s | 270.4–279.2 Mbit/s | 284.0–288.8 Mbit/s | 312.8–316.0 Mbit/s |
| 256 | 453.6 Mbit/s | 976–1008 Mbit/s | 1000–1024 Mbit/s | 1056–1160 Mbit/s |
| 1024 | 1.44 Gbit/s | 2.98–3.14 Gbit/s | 3.30–3.34 Gbit/s | 3.38–3.62 Gbit/s |
| 4096 | 3.34 Gbit/s | 4.13–4.53 Gbit/s | 6.74–6.86 Gbit/s | 6.63–7.07 Gbit/s |
| 16384 | 6.62 Gbit/s | 7.36–7.94 Gbit/s | 7.15–8.16 Gbit/s | 7.84–8.48 Gbit/s |
| 65536 | 8.96 Gbit/s | 8.56–9.20 Gbit/s | 7.84–7.92 Gbit/s | 7.60–8.24 Gbit/s |

Packets per second through the actors:

| Data length (bytes) | Original | 1: write batching | 2: + read buffer | 3: + no tracing (current) |
|---:|---:|---:|---:|---:|
| 16 | 180,000 | 486,000–500,000 | 508,000–509,000 | 563,000–568,000 |
| 64 | 211,000 | 482,000–499,000 | 507,000–516,000 | 559,000–564,000 |
| 256 | 216,000 | 467,000–482,000 | 477,000–488,000 | 504,000–553,000 |
| 1024 | 175,000 | 362,000–382,000 | 401,000–406,000 | 410,000–439,000 |
| 4096 | 102,000 | 126,000–138,000 | 205,000–209,000 | 202,000–215,000 |
| 16384 | 50,000 | 56,000–60,000 | 55,000–62,000 | 60,000–64,000 |
| 65536 | 17,000 | 16,000–17,500 | 15,000 | 14,600–15,700 |

### The tuning options

Measured in one session, three runs each, starting from optimizations 1
and 2 with kameo's tracing still enabled. The mailbox capacity applies to
the reader, the client and the writer.

| Configuration | 64 B | 256 B | 1024 B | 4096 B |
|---|---:|---:|---:|---:|
| Default: 4 workers, mailbox 64 | 280 Mbit/s | 1008–1032 Mbit/s | 3.31–3.33 Gbit/s | 6.34–6.77 Gbit/s |
| `current_thread` runtime | 224 Mbit/s | 784–816 Mbit/s | 2.38–2.51 Gbit/s | 4.62–5.10 Gbit/s |
| 1 worker | 224–232 Mbit/s | 784–792 Mbit/s | 2.40–2.43 Gbit/s | 4.74–5.10 Gbit/s |
| 2 workers | 352–360 Mbit/s | 1176–1224 Mbit/s | 3.70–3.81 Gbit/s | 5.54–7.34 Gbit/s |
| Mailbox 256 | 328 Mbit/s | 1128–1168 Mbit/s | 3.64–3.68 Gbit/s | 6.26–6.92 Gbit/s |
| Mailbox 1024 | 320–352 Mbit/s | 1152–1184 Mbit/s | 3.78–3.83 Gbit/s | 6.08–6.86 Gbit/s |
| No tracing | 312–320 Mbit/s | 1088–1136 Mbit/s | 3.42–3.57 Gbit/s | 6.77–7.22 Gbit/s |
| No tracing, 2 workers | 368–384 Mbit/s | 1320–1336 Mbit/s | 3.81–4.03 Gbit/s | 6.35–8.16 Gbit/s |
| No tracing, mailbox 256 | 360 Mbit/s | 1240–1280 Mbit/s | 3.91–3.99 Gbit/s | 6.90–7.32 Gbit/s |
| No tracing, 2 workers, mailbox 256 | 416–424 Mbit/s | 1392–1448 Mbit/s | 3.90–4.30 Gbit/s | 7.74–7.94 Gbit/s |
| No tracing, 2 workers, mailbox 1024 | 400–432 Mbit/s | 1344–1480 Mbit/s | 3.90–4.09 Gbit/s | 7.15–7.81 Gbit/s |

With the current version, the throughput test's own comparison of the
mailbox capacities gave, over three runs:

| Data length (bytes) | Mailbox 64 (default) | Mailbox 256 |
|---:|---:|---:|
| 16 | 99.2–100.0 Mbit/s | 112.8–115.2 Mbit/s |
| 64 | 312.8–316.0 Mbit/s | 282.4–363.2 Mbit/s |
| 256 | 1056–1160 Mbit/s | 1152–1208 Mbit/s |
| 1024 | 3.38–3.62 Gbit/s | 3.53–3.93 Gbit/s |
| 4096 | 6.63–7.07 Gbit/s | 6.11–7.24 Gbit/s |
| 16384 | 7.84–8.48 Gbit/s | 6.96–8.32 Gbit/s |
| 65536 | 7.60–8.24 Gbit/s | 9.12–9.52 Gbit/s |

### Reading the results

- **Write batching** raised the throughput of packets up to 1 KiB by a
  factor of 2.1 to 2.5. This is where system calls dominated, and
  batching removes most of them.
- **The larger read buffer** adds about 8–10% for 1 KiB packets and about
  55% for 4 KiB packets: with the default buffer, one read fetched only
  about two 4 KiB packets. For packets of 256 bytes and less, it makes no
  difference beyond the variation; there, the hops between tasks
  dominate.
- **Without kameo's tracing**, every hop gets cheaper: about 3–7% for
  1 KiB packets and 10–15% for the smallest ones.
- **A mailbox capacity of 256** adds about 5–15% for small and medium
  packets, and **2 worker threads** instead of 4 about 13% on the test
  machine. Together with the optimizations, 1 KiB packets reach about
  3.9–4.3 Gbit/s, close to three times the original.
- **Large packets (16 KiB and more)** varied between about 7 and 10
  Gbit/s over all versions and settings, without a clear trend. One such
  packet already fills a read and a write, and the cost is copying the
  data.

## Possible next steps

- **Reading without the reader's mailbox**, as described above, if very
  small packets become important.
- **Avoid the copy for large packets.** The encoder copies the packet
  data into the buffer. Writing large packets with vectored writes
  (header and data separately) would skip that copy.
- **A different allocator** in the application, such as mimalloc or
  jemalloc: allocation took about 7% of the time in the profile, mostly
  for the messages passed between actors.
