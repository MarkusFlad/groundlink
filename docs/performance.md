# Performance

This document explains what limits the throughput of the TCP actors, the
optimizations made so far, one that was tried and not adopted, and the
measured throughput for each.

Every message that goes over a connection is its own kameo message: one
`SpacePacket`, one `PusPacket`, one `SimpleString`. That keeps the actors
simple, because any actor can hand a single message to a server, a client
or a writer. The optimizations below keep it that way; they only change
how the reader and the writer move these messages to and from the socket.

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
bytes. Both optimizations therefore reduce the number of system calls per
packet.

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

## Tried and not adopted: reading without the reader's mailbox

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
for 64 to 1024 bytes, and not measurably above that (see
[Measurements](#measurements)). For the main use case of about 1 KiB
packets, the gain did not justify the more complex reader, so the change
was not adopted. It remains an option if very small packets matter.

## Measurements

The [throughput test](../tests/tcp_throughput.rs) sends pre-encoded Space
Packets through a `SpacePacketServer` coupled with a `SpacePacketClient`:

```text
sender ──▶ SpacePacketServer ──▶ SpacePacketClient ──▶ receiver
```

For comparison it sends the same data over a plain loopback connection
without actors in between ("direct"). Run it with:

```sh
cargo test --release --test tcp_throughput -- --ignored --nocapture
```

Setup: Intel Core i5-3210M (2 cores, 4 threads), Linux 6.8, Rust 1.89,
default release profile, loopback. The original version was measured
once, write batching three times, the other versions twice; ranges show
the lowest and highest value. Differences of about 10% can be variation
between runs: the direct connection alone varied between 0.6 and 3.6
GB/s.

Throughput through the actors:

| Data length (bytes) | Original | Write batching | + read buffer (adopted) | + direct reading (not adopted) |
|---:|---:|---:|---:|---:|
| 16 | 4.0 MB/s | 10.7–11.0 MB/s | 11.2 MB/s | 13.0–13.8 MB/s |
| 64 | 14.7 MB/s | 33.8–34.9 MB/s | 35.5–36.1 MB/s | 38.2–40.6 MB/s |
| 256 | 56.7 MB/s | 122–126 MB/s | 125–128 MB/s | 131–133 MB/s |
| 1024 | 180 MB/s | 373–393 MB/s | 413–418 MB/s | 398–412 MB/s |
| 4096 | 417 MB/s | 516–566 MB/s | 842–857 MB/s | 559–560 MB/s |
| 16384 | 827 MB/s | 920–992 MB/s | 894–1020 MB/s | 869–880 MB/s |
| 65536 | 1.12 GB/s | 1.07–1.15 GB/s | 0.98–0.99 GB/s | 1.14–1.16 GB/s |

Packets per second through the actors:

| Data length (bytes) | Original | Write batching | + read buffer (adopted) | + direct reading (not adopted) |
|---:|---:|---:|---:|---:|
| 16 | 180,000 | 486,000–500,000 | 508,000–509,000 | 593,000–628,000 |
| 64 | 211,000 | 482,000–499,000 | 507,000–516,000 | 546,000–580,000 |
| 256 | 216,000 | 467,000–482,000 | 477,000–488,000 | 501,000–508,000 |
| 1024 | 175,000 | 362,000–382,000 | 401,000–406,000 | 386,000–400,000 |
| 4096 | 102,000 | 126,000–138,000 | 205,000–209,000 | 136,000 |
| 16384 | 50,000 | 56,000–60,000 | 55,000–62,000 | 53,000–54,000 |
| 65536 | 17,000 | 16,000–17,500 | 15,000 | 17,500–17,700 |

"+ read buffer" and "+ direct reading" are each measured on top of write
batching, not on top of each other.

### Reading the results

- **Write batching** raised the throughput of packets up to 1 KiB by a
  factor of 2.1 to 2.5. This is where system calls dominated, and
  batching removes most of them.
- **The larger read buffer** adds about 8–10% for 1 KiB packets and about
  55% for 4 KiB packets: with the default buffer, one read fetched only
  about two 4 KiB packets. For packets of 256 bytes and less, it makes no
  difference beyond the variation; there, the hops between tasks
  dominate.
- **64 KiB packets** varied between 0.98 and 1.15 GB/s over all versions
  and read buffer sizes, without a clear trend. One such packet already
  fills a read and a write, and the cost is copying the data.
- **For 1 KiB packets**, the throughput is now about 415 MB/s, or about
  400,000 packets per second, 2.3 times the original.

## Possible next steps

- **Reading without the reader's mailbox**, as described above, if very
  small packets become important.
- **Larger mailboxes.** With kameo's default of 64 slots, senders and
  receivers often wake each other up. A larger capacity lets both work in
  longer stretches, but it also lets more data queue up before senders
  have to wait.
- **Avoid the copy for large packets.** The encoder copies the packet
  data into the buffer. Writing large packets with vectored writes
  (header and data separately) would skip that copy.
