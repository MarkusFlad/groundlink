# WireMessage

This document explains how `TcpServerActor` and `TcpClientActor` send
messages, and why the message types of a protocol implement the
`WireMessage` marker trait.

## How messages flow through a server

A `TcpServerActor<M, C>` (for example `PusServer`) is not on the path of
incoming messages. For each accepted connection, it spawns a
`TcpReaderActor` and a `TcpWriterActor`:

- The **reader** decodes messages from the socket and sends them directly
  to the `downstream: Recipient<M>` that was passed when the server was
  spawned.
- The **writer** encodes every `M` it receives onto the socket.

Besides its control messages (`GetLocalAddr`, `ConnectionHalfClosed`, and
an internal notification when a writer has been spawned), the server
handles `M` itself: it forwards the message to the writer of its current
connection. Without a connection, the message is dropped with a warning.

The `TcpClientActor` works the same way, with its own control messages
(`Connect`, `Close`, `CloseRead`, `CloseWrite`, …).

Because server and client both handle `M`, any actor that produces
messages only needs a `Recipient<M>`. It doesn't matter whether that
recipient is a server, a client or another processing actor. For example,
in the [`pus_server`](../examples/pus_server.rs) example the
`PusTmStamper` sends the telemetry straight to the TM server:

```rust
// The TM server writes every `PusPacket` it receives to its connected client.
let tm_server = PusServer::spawn(TcpServerArgs { /* ... */ });
let stamper = PusTmStamper::spawn(PusTmStamper::new(APID, tm_server.recipient::<PusPacket>()));
```

In a unit test, you can pass a `Recipient<PusPacket>` of a `TestActor`
instead.

## Why `M` must implement `WireMessage`

Without a bound on `M`, the server's implementations would look like this:

```rust
impl<M, C> Message<GetLocalAddr> for TcpServerActor<M, C> { /* ... */ }
impl<M, C> Message<M> for TcpServerActor<M, C> { /* ... */ }
```

This does not compile:

```text
error[E0119]: conflicting implementations of trait `Message<GetLocalAddr>`
              for type `TcpServerActor<GetLocalAddr, _>`
```

Rust's coherence rules reject any two implementations that *could* apply
to the same type. If `M` were `GetLocalAddr`, both implementations would
provide `Message<GetLocalAddr>` for the same actor. It does not matter that
nobody ever instantiates a server with that message type.

The fix is a marker trait, defined in
[`src/messages.rs`](../src/messages.rs):

```rust
pub trait WireMessage {}

impl<M, C> Message<M> for TcpServerActor<M, C>
where
    M: WireMessage + Send + 'static,
    C: MessageCodec<M>,
{ /* ... */ }
```

`WireMessage` and all control messages are defined in groundlink, and
groundlink never implements `WireMessage` for a control message. Because of
Rust's orphan rules, no other crate can add such an implementation either.
The compiler can therefore prove that `GetLocalAddr: WireMessage` never
holds, and the two implementations can't overlap.

The existing bound `C: MessageCodec<M>` is not enough for this proof:
another crate could implement `MessageCodec<GetLocalAddr>` for its own
codec type.

## Using your own message type

To use the generic TCP actors with your own protocol, implement
`WireMessage` for its message type, next to the codec:

```rust
use groundlink::WireMessage;

struct MyMessage(Vec<u8>);

impl WireMessage for MyMessage {}
```

The protocols of this crate already do this for `SimpleString`,
`SpacePacket` and `PusPacket`.

## Summary

- Incoming messages go from the reader directly to `downstream`.
- Servers and clients handle `M` by writing it to their current
  connection, so producers only need a `Recipient<M>`.
- `M: WireMessage` keeps this `Message<M>` implementation from
  conflicting with the actors' control message implementations.
