# Write batching

This document explains how `TcpWriterActor` writes the messages it
receives, why it combines them into larger writes, and what that changed
in the measured throughput.

## The problem: one system call per message

Every message that goes out over a connection is its own kameo message:
one `SpacePacket`, one `PusPacket`, one `SimpleString`. That keeps the
actors simple, because any actor can hand a single message to a server,
a client or a writer.

Originally the writer also *wrote* each message on its own: encode it,
then `write` it to the socket. A system call costs a few microseconds,
no matter whether it writes 22 bytes or 64 KiB. For small packets, the
writer spent most of its time on system calls rather than on the data.
In a server coupled with a client, all packets passing through ended up
in one writer, so that writer set the limit of about 200,000 packets per
second.

## The solution: write what is queued together

The writer still receives one kameo message per packet, but it no longer
writes each one immediately. Instead:

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

### Details

- **Only one `Flush` at a time.** A flag records whether a `Flush` is
  already queued, so the mailbox holds at most one.
- **Full mailbox.** The writer queues its `Flush` with `try_send`. If the
  mailbox is full, that fails; but then more messages are waiting behind
  the current one, and the next of them tries again. The buffer is
  therefore always written eventually.
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
default release profile, loopback. Each value is from a single run, so
differences of about 10% are within the variation between runs; the
direct connection alone varied between 1.6 and 3.0 GB/s.

| Data length (bytes) | Before | After | Direct | Packets/s before | Packets/s after |
|---:|---:|---:|---:|---:|---:|
| 16 | 4.0 MB/s | 11.0 MB/s | 1.6–2.2 GB/s | 180,000 | 500,000 |
| 64 | 14.7 MB/s | 34.9 MB/s | 2.0–2.4 GB/s | 211,000 | 499,000 |
| 256 | 56.7 MB/s | 125 MB/s | 1.8–2.7 GB/s | 216,000 | 477,000 |
| 1024 | 180 MB/s | 393 MB/s | 2.4–3.0 GB/s | 175,000 | 382,000 |
| 4096 | 417 MB/s | 516 MB/s | 2.3–2.4 GB/s | 102,000 | 126,000 |
| 16384 | 827 MB/s | 920 MB/s | 2.7–2.9 GB/s | 50,000 | 56,000 |
| 65536 | 1.12 GB/s | 1.07 GB/s | 2.5–2.8 GB/s | 17,000 | 16,000 |

### Reading the results

- **Small packets (up to 1 KiB)** gain a factor of 2.2 to 2.8. This is
  where the system calls dominated, and batching removes most of them.
- **Large packets (16 KiB and more)** hardly change. A single packet
  already fills a large part of a write, so there was little to combine;
  the cost is copying the data.
- **The new limit** is about 500,000 packets per second. Each packet
  still passes three mailboxes (reader, client, writer), and the handoffs
  between them now take most of the time.

## Possible next steps

- **Remove the reader's extra hop.** The reader's read loop runs in a
  task that kameo's `attach_stream` spawns, which sends each packet into
  the reader's mailbox before the reader forwards it. Sending straight
  from the read loop to `downstream` would save one handoff per packet.
- **Larger mailboxes.** With kameo's default of 64 slots, senders and
  receivers often wake each other up. A larger capacity lets both work in
  longer stretches.
- **Avoid the copy for large packets.** The encoder copies the packet
  data into the buffer. Writing large packets with vectored writes
  (header and data separately) would skip that copy.
