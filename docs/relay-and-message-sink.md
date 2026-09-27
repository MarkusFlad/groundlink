# Relay and MessageSink

This document explains why groundlink wraps outgoing messages in `Relay<M>`
and how `MessageSink<M>` lets message-producing actors send either to a
processing actor or to a TCP connection.

## How messages flow through a server

A `TcpServerActor<M, C>` (for example `PusServer`) is not on the path of
incoming messages. For each accepted connection, it spawns a
`TcpReaderActor` and a `TcpWriterActor`:

- The **reader** decodes messages from the socket and sends them directly
  to the `downstream: Recipient<M>` that was passed when the server was
  spawned.
- The **writer** implements `Message<M>` and encodes every `M` it receives
  onto the socket.

The server itself only handles control messages: `GetLocalAddr`,
`ConnectionHalfClosed`, an internal notification when a writer has been
spawned, and the request to send an `M` to the connected peer. The
`TcpClientActor` works the same way, with its own control messages
(`Connect`, `Close`, `CloseRead`, `CloseWrite`, …).

## Why `Relay<M>` is needed

The natural way to send an `M` through a server would be to implement
`Message<M>` on it, just like the writer does. That does not compile:

```rust
impl<M, C> Message<GetLocalAddr> for TcpServerActor<M, C> { /* ... */ }
impl<M, C> Message<M> for TcpServerActor<M, C> { /* ... */ }
```

```text
error[E0119]: conflicting implementations of trait `Message<GetLocalAddr>`
              for type `TcpServerActor<GetLocalAddr, _>`
```

Rust's coherence rules reject any two implementations that *could* apply
to the same type. If `M` were `GetLocalAddr`, both implementations would
provide `Message<GetLocalAddr>` for the same actor. It does not matter that
nobody ever instantiates a server with that message type. The same
conflict would occur for every other control message of the server and
the client.

The writer does not have this problem: besides `Message<M>`, it only
implements `Message<Shutdown<M>>`, and `M` can never be `Shutdown<M>`.

`Relay<M>` (defined in [`src/messages.rs`](../src/messages.rs)) solves the
conflict with a plain newtype:

```rust
pub struct Relay<M>(pub M);
```

It means "write `M` to your current connection". Because `Relay<M>` is a
distinct type, `Message<Relay<M>>` can never overlap with the control
messages. Both `TcpServerActor` and `TcpClientActor` implement it (see
[`src/actors.rs`](../src/actors.rs)): the server writes the message to the
peer of its current connection, and the client writes it to the server it
is connected to. Without a connection, the message is dropped with a
warning.

`Relay` does not change the content of a message. Converting a specific
message into a packet is done separately, through `From`/`TryFrom`, for
example `PusPacket::from(AreYouAliveRequest::new(apid, seq))`.

## `MessageSink<M>`: sending to either kind of target

Because of `Relay`, a server or client is a `Recipient<Relay<M>>`, while a
processing actor (or a `TcpWriterActor`) is a `Recipient<M>`. Actors that
produce messages, such as `PusTcAcceptor` and `PusTestServiceActor`,
should not have to care about this difference. They therefore take an
`impl Into<MessageSink<M>>`:

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

- Incoming messages go from the reader directly to `downstream`. The
  server and client only handle control messages and outgoing messages.
- Outgoing messages are wrapped in `Relay<M>` because a generic
  `Message<M>` implementation would conflict with the control message
  implementations.
- Actors that produce messages take a `MessageSink<M>`, so they can send
  to a `Recipient<M>` or a `Recipient<Relay<M>>` without an adapter.
