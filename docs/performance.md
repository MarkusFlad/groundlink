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
runs: the direct connection alone varied between 0.6 and 3.6 GB/s.

### The optimizations

With the default settings (4 worker threads, mailbox capacity 64). The
original version was measured once, the others two or three times.

Throughput through the actors:

| Data length (bytes) | Original | 1: write batching | 2: + read buffer | 3: + no tracing (current) |
|---:|---:|---:|---:|---:|
| 16 | 4.0 MB/s | 10.7–11.0 MB/s | 11.2 MB/s | 12.4–12.5 MB/s |
| 64 | 14.7 MB/s | 33.8–34.9 MB/s | 35.5–36.1 MB/s | 39.1–39.5 MB/s |
| 256 | 56.7 MB/s | 122–126 MB/s | 125–128 MB/s | 132–145 MB/s |
| 1024 | 180 MB/s | 373–393 MB/s | 413–418 MB/s | 422–452 MB/s |
| 4096 | 417 MB/s | 516–566 MB/s | 842–857 MB/s | 829–884 MB/s |
| 16384 | 827 MB/s | 920–992 MB/s | 894–1020 MB/s | 0.98–1.06 GB/s |
| 65536 | 1.12 GB/s | 1.07–1.15 GB/s | 0.98–0.99 GB/s | 0.95–1.03 GB/s |

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
| Default: 4 workers, mailbox 64 | 35 MB/s | 126–129 MB/s | 414–416 MB/s | 793–846 MB/s |
| `current_thread` runtime | 28 MB/s | 98–102 MB/s | 298–314 MB/s | 577–637 MB/s |
| 1 worker | 28–29 MB/s | 98–99 MB/s | 300–304 MB/s | 593–637 MB/s |
| 2 workers | 44–45 MB/s | 147–153 MB/s | 463–476 MB/s | 692–917 MB/s |
| Mailbox 256 | 41 MB/s | 141–146 MB/s | 455–460 MB/s | 783–865 MB/s |
| Mailbox 1024 | 40–44 MB/s | 144–148 MB/s | 472–479 MB/s | 760–857 MB/s |
| No tracing | 39–40 MB/s | 136–142 MB/s | 428–446 MB/s | 846–903 MB/s |
| No tracing, 2 workers | 46–48 MB/s | 165–167 MB/s | 476–504 MB/s | 794–1020 MB/s |
| No tracing, mailbox 256 | 45 MB/s | 155–160 MB/s | 489–499 MB/s | 862–915 MB/s |
| No tracing, 2 workers, mailbox 256 | 52–53 MB/s | 174–181 MB/s | 487–538 MB/s | 968–992 MB/s |
| No tracing, 2 workers, mailbox 1024 | 50–54 MB/s | 168–185 MB/s | 487–511 MB/s | 894–976 MB/s |

With the current version, the throughput test's own comparison of the
mailbox capacities gave, over three runs:

| Data length (bytes) | Mailbox 64 (default) | Mailbox 256 |
|---:|---:|---:|
| 16 | 12.4–12.5 MB/s | 14.1–14.4 MB/s |
| 64 | 39.1–39.5 MB/s | 35.3–45.4 MB/s |
| 256 | 132–145 MB/s | 144–151 MB/s |
| 1024 | 422–452 MB/s | 441–491 MB/s |
| 4096 | 829–884 MB/s | 764–905 MB/s |
| 16384 | 0.98–1.06 GB/s | 0.87–1.04 GB/s |
| 65536 | 0.95–1.03 GB/s | 1.14–1.19 GB/s |

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
  490–540 MB/s, close to three times the original.
- **Large packets (16 KiB and more)** varied between about 0.9 and 1.2
  GB/s over all versions and settings, without a clear trend. One such
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
