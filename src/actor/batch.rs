//! Batches: several messages sent between actors as one kameo message.
//!
//! Passing a message from one actor to the next costs about the same
//! whether it is a single packet or a thousand. For small packets at a high
//! rate, that cost dominates. A [`Batch<M>`] carries several messages
//! through a mailbox at once, while the actors still handle them one by
//! one.
//!
//! Batches form only where messages arrive together anyway: a
//! [`TcpReaderActor`](crate::TcpReaderActor) forwards what one read from
//! the socket has delivered as one batch. It never waits for more, so
//! batching adds no latency, and nothing can be left behind in a buffer.
//!
//! # Receiving batches
//!
//! An actor that handles `M` handles `Batch<M>` too after
//! [`impl_batch_message!`](crate::impl_batch_message), which calls the
//! handler for `M` for each message of the batch:
//!
//! ```
//! use groundlink::{Batch, SimpleString, impl_batch_message};
//! use kameo::actor::{Actor, ActorRef, Spawn};
//! use kameo::error::Infallible;
//! use kameo::message::{Context, Message};
//!
//! struct Counter(usize);
//!
//! impl Actor for Counter {
//!     type Args = Self;
//!     type Error = Infallible;
//!
//!     async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
//!         Ok(state)
//!     }
//! }
//!
//! impl Message<SimpleString> for Counter {
//!     type Reply = ();
//!
//!     async fn handle(&mut self, _msg: SimpleString, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
//!         self.0 += 1;
//!     }
//! }
//!
//! impl_batch_message!(Counter, SimpleString);
//!
//! # #[tokio::main]
//! # async fn main() {
//! let counter = Counter::spawn(Counter(0));
//! // The recipient to pass as a batched downstream, e.g. to
//! // `TcpServerArgs::batched`.
//! let batched = counter.recipient::<Batch<SimpleString>>();
//! batched.tell(Batch::from(vec![SimpleString("a".into()), SimpleString("b".into())])).await.unwrap();
//! # }
//! ```
//!
//! # Passing batches on
//!
//! An actor between a reader and a writer keeps the batches together by
//! sending through a [`Downstream<M>`] and naming it in the macro:
//! `impl_batch_message!(MyActor, In, outputs = [target])`. What the
//! handler sends to `self.target` while a batch is handled is collected
//! and sent as one batch when the batch is done. A single message is sent
//! on at once, as before.

use std::fmt;

use kameo::actor::Recipient;
use tracing::warn;

/// Default for how many messages a reader forwards as one batch at most;
/// see [`TcpServerArgs::max_batch_len`](crate::TcpServerArgs::max_batch_len).
pub const DEFAULT_MAX_BATCH_LEN: usize = 64;

/// Several messages of type `M`, sent as one kameo message; see the
/// [module documentation](self).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch<M>(Vec<M>);

impl<M> Batch<M> {
    /// The number of messages.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the batch holds no messages.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The messages, in their order.
    pub fn into_vec(self) -> Vec<M> {
        self.0
    }
}

impl<M> From<Vec<M>> for Batch<M> {
    fn from(messages: Vec<M>) -> Self {
        Batch(messages)
    }
}

impl<M> IntoIterator for Batch<M> {
    type Item = M;
    type IntoIter = std::vec::IntoIter<M>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// A [`Downstream`] could not deliver a message because its recipient has
/// stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownstreamError(String);

impl fmt::Display for DownstreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for DownstreamError {}

/// Where an actor sends its messages of type `M`: to a recipient of single
/// messages (`Recipient<M>`) or of batches (`Recipient<Batch<M>>`). Create
/// it from either with `into()`.
///
/// While the actor handles a batch, between [`hold`](Self::hold) and
/// [`release`](Self::release), the messages are collected and then sent on
/// together: as one batch to a batch recipient, one after the other to a
/// recipient of single messages.
/// [`impl_batch_message!`](crate::impl_batch_message) calls both for the
/// fields named as `outputs`.
pub struct Downstream<M: Send + 'static> {
    target: Target<M>,
    /// The messages collected since [`hold`](Self::hold).
    held: Option<Vec<M>>,
}

enum Target<M: Send + 'static> {
    Each(Recipient<M>),
    Batched(Recipient<Batch<M>>),
}

impl<M: Send + 'static> Downstream<M> {
    /// Whether the recipient takes batches.
    pub fn is_batched(&self) -> bool {
        matches!(self.target, Target::Batched(_))
    }

    /// Sends `message`, or collects it while messages are held.
    pub async fn send(&mut self, message: M) -> Result<(), DownstreamError> {
        match &mut self.held {
            Some(held) => {
                held.push(message);
                Ok(())
            }
            None => match &self.target {
                Target::Each(recipient) => recipient.tell(message).await.map_err(closed),
                Target::Batched(recipient) => recipient.tell(Batch(vec![message])).await.map_err(closed),
            },
        }
    }

    /// Sends `messages`, or collects them while messages are held. If the
    /// recipient has stopped, the remaining messages are dropped.
    pub async fn send_all(&mut self, messages: Vec<M>) -> Result<(), DownstreamError> {
        match &mut self.held {
            Some(held) => {
                held.extend(messages);
                Ok(())
            }
            None => self.deliver(messages).await,
        }
    }

    /// Collects the messages sent from now on until
    /// [`release`](Self::release).
    pub fn hold(&mut self) {
        self.held.get_or_insert_with(Vec::new);
    }

    /// Sends the messages collected since [`hold`](Self::hold). If the
    /// recipient has stopped, they are dropped with a warning.
    pub async fn release(&mut self) {
        let Some(messages) = self.held.take() else {
            return;
        };
        let len = messages.len();
        if let Err(err) = self.deliver(messages).await {
            warn!("{len} message(s) dropped, could not send them downstream: {err}");
        }
    }

    async fn deliver(&mut self, messages: Vec<M>) -> Result<(), DownstreamError> {
        if messages.is_empty() {
            return Ok(());
        }
        match &self.target {
            Target::Each(recipient) => {
                for message in messages {
                    recipient.tell(message).await.map_err(closed)?;
                }
                Ok(())
            }
            Target::Batched(recipient) => recipient.tell(Batch(messages)).await.map_err(closed),
        }
    }
}

fn closed(err: impl fmt::Display) -> DownstreamError {
    DownstreamError(err.to_string())
}

/// A downstream to the same recipient that holds no messages.
impl<M: Send + 'static> Clone for Downstream<M> {
    fn clone(&self) -> Self {
        let target = match &self.target {
            Target::Each(recipient) => Target::Each(recipient.clone()),
            Target::Batched(recipient) => Target::Batched(recipient.clone()),
        };
        Downstream { target, held: None }
    }
}

impl<M: Send + 'static> From<Recipient<M>> for Downstream<M> {
    fn from(recipient: Recipient<M>) -> Self {
        Downstream {
            target: Target::Each(recipient),
            held: None,
        }
    }
}

impl<M: Send + 'static> From<Recipient<Batch<M>>> for Downstream<M> {
    fn from(recipient: Recipient<Batch<M>>) -> Self {
        Downstream {
            target: Target::Batched(recipient),
            held: None,
        }
    }
}

/// Makes an actor that handles `$message` with `Reply = ()` also handle
/// [`Batch<$message>`](Batch), by calling its handler for each message of
/// the batch in order.
///
/// With `outputs = [field, ...]`, naming fields of type [`Downstream`],
/// the messages the handler sends to them during a batch are sent on
/// together when the batch is done; see the [module documentation](self).
/// Name an output only if the order between it and the actor's other
/// outputs does not matter, since held messages are sent later than those
/// sent at once.
///
/// The actor's crate must depend on `kameo`.
#[macro_export]
macro_rules! impl_batch_message {
    ($actor:ty, $message:ty) => {
        $crate::impl_batch_message!($actor, $message, outputs = []);
    };
    ($actor:ty, $message:ty, outputs = [$($output:ident),* $(,)?]) => {
        impl ::kameo::message::Message<$crate::Batch<$message>> for $actor {
            type Reply = ();

            async fn handle(
                &mut self,
                batch: $crate::Batch<$message>,
                ctx: &mut ::kameo::message::Context<Self, Self::Reply>,
            ) -> Self::Reply {
                $( self.$output.hold(); )*
                for message in batch {
                    <Self as ::kameo::message::Message<$message>>::handle(self, message, ctx).await;
                }
                $( self.$output.release().await; )*
            }
        }
    };
}
