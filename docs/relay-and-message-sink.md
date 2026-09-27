# Relay and MessageSink

This document explains why groundlink wraps outgoing messages in `Relay<M>`
and how `MessageSink<M>` lets message-producing actors send either to a
processing actor or to a TCP connection.

## The problem: one message type, two directions

A `TcpServerActor<M, C>` (for example `PusServer`) deals with messages of
type `M` in two directions:

1. **Incoming:** it reads `M` from the TCP socket and forwards it to its
   `downstream: Recipient<M>`.
2. **Outgoing:** other actors want it to *write* an `M` to the connected
   peer.

In kameo, handling a message type means implementing `Message<M>`. If the
server implemented `Message<M>` for the outgoing direction, a
`Recipient<M>` would mean different things depending on the actor behind
it:

- for a `PusTcAcceptor`: "process this packet"
- for a `PusServer`: "send this packet over the network"

The type system could not tell these apart. You could, for example, pass a
server as the `downstream` of another server, or even as its own
`downstream`. This would compile, and every received packet would be
written straight back to the network.

## The solution: `Relay<M>`

`Relay<M>` (defined in [`src/messages.rs`](../src/messages.rs)) is a plain
newtype:

```rust
pub struct Relay<M>(pub M);
```

It means "write `M` to your current connection". This puts the direction
into the type:

| Type | Meaning |
|---|---|
| `Recipient<PusPacket>` | an actor that **processes** packets |
| `Recipient<Relay<PusPacket>>` | a server or client that **sends** packets over TCP |

Both `TcpServerActor` and `TcpClientActor` implement `Message<Relay<M>>`
(see [`src/actors.rs`](../src/actors.rs)). The server writes the message to
the peer of its current connection, and the client writes it to the
server it is connected to. Without a connection, the message is dropped
with a warning.

`Relay` does not change the content of a message. Converting a specific
message into a packet is done separately, through `From`/`TryFrom`, for
example `PusPacket::from(AreYouAliveRequest::new(apid, seq))`.

### Further benefits

- **Server and client are interchangeable for senders.** Code that sends
  `Relay<PusPacket>` works the same whether the target is a `PusServer` or
  a `PusClient`.
- **Tests are unambiguous.** A `TestActor<Relay<PusPacket>>` stands for
  "this would have gone out over the network", while a
  `TestActor<PusPacket>` stands for "this would have been processed".

## `MessageSink<M>`: sending to either kind of target

Actors that produce messages, such as `PusTcAcceptor` and
`PusTestServiceActor`, should not have to know whether their output goes
to another processing actor or out over a TCP connection. They therefore
take an `impl Into<MessageSink<M>>`:

```rust
pub enum MessageSink<M: Send + 'static> {
    /// Send `M` as is.
    Direct(Recipient<M>),
    /// Send `M` wrapped in `Relay<M>`.
    Relay(Recipient<Relay<M>>),
}
```

Both `Recipient<M>` and `Recipient<Relay<M>>` convert into a
`MessageSink<M>`. When the producing actor calls `MessageSink::tell`, the
sink wraps the message in `Relay` if needed. You don't need an adapter
actor between the producer and the server.

### Example

In the [`pus_server`](../examples/pus_server.rs) example, the service
actors send their telemetry straight to the TM server:

```rust
// The TM server writes every `Relay<PusPacket>` to its connected client.
let tm_server = PusServer::spawn(TcpServerArgs { /* ... */ });
let telemetry = tm_server.clone().recipient::<Relay<PusPacket>>();

// Both actors accept the `Recipient<Relay<PusPacket>>` as their sink.
let acceptor = PusTcAcceptor::for_packets(APID, telemetry.clone());
let test_service = PusTestServiceActor::spawn(
    PusTestServiceActor::new(APID, telemetry)
        .with_sequence_counter(acceptor.sequence_counter()),
);
```

In a unit test, you can pass a `Recipient<PusPacket>` of a `TestActor`
instead, and the same actors send the unwrapped packets to it.

## Summary

- `Recipient<M>` means "process `M`". `Recipient<Relay<M>>` means "send `M`
  over TCP".
- `TcpServerActor` and `TcpClientActor` handle `Relay<M>`, never `M`
  itself. This keeps a server from being wired up as a processing actor
  by accident.
- Actors that produce messages take a `MessageSink<M>`, so they can target
  either kind of recipient without an adapter.
