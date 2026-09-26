//! A generic actor that records every message it receives, for assertions
//! in tests.

use std::time::Duration;

use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};

/// Actor that accepts messages of type `M`, stores them in an internal
/// vector and returns a copy of that vector on [`GetMessages`].
///
/// The generic parameter selects the message type, e.g.
/// `TestActor<SimpleString>`. `TestActor<M>` implements `Message<M>` once,
/// generically, so no boilerplate is needed per message type.
///
/// ```
/// use kameo::actor::Spawn;
/// use kameo_tcp_example::{GetMessages, TestActor};
///
/// # #[tokio::main]
/// # async fn main() {
/// let actor = TestActor::<u32>::spawn(TestActor::new());
/// actor.tell(42).await.unwrap();
/// assert_eq!(actor.ask(GetMessages::new()).await.unwrap(), vec![42]);
/// # }
/// ```
pub struct TestActor<M> {
    received: Vec<M>,
}

impl<M> TestActor<M> {
    /// Creates an actor that has not received any messages yet.
    pub fn new() -> Self {
        Self {
            received: Vec::new(),
        }
    }
}

impl<M> Default for TestActor<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M> Actor for TestActor<M>
where
    M: Send + 'static,
{
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

/// Stores every incoming message of type `M` in the internal vector.
impl<M> Message<M> for TestActor<M>
where
    M: Send + 'static,
{
    type Reply = ();

    async fn handle(&mut self, msg: M, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.received.push(msg);
    }
}

/// Query message: replies (via `ask`) with a copy of all messages received
/// so far.
///
/// Tied to `M` through `PhantomData<fn() -> M>`, for the same coherence
/// reasons as [`Shutdown`](crate::Shutdown).
pub struct GetMessages<M>(std::marker::PhantomData<fn() -> M>);

impl<M> GetMessages<M> {
    /// Creates the query.
    pub fn new() -> Self {
        GetMessages(std::marker::PhantomData)
    }
}

impl<M> Default for GetMessages<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M> Message<GetMessages<M>> for TestActor<M>
where
    M: Clone + Send + 'static,
{
    type Reply = Vec<M>;

    async fn handle(
        &mut self,
        _msg: GetMessages<M>,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.received.clone()
    }
}

impl<M> TestActor<M>
where
    M: Clone + std::fmt::Debug + Send + 'static,
{
    /// Waits until the actor has received at least `expected_count`
    /// messages and returns a copy of everything recorded so far, so the
    /// caller can check contents, order, etc.
    ///
    /// Polls the actor every 10 ms.
    ///
    /// # Panics
    ///
    /// Panics (like an `assert!`) if `timeout` elapses before enough
    /// messages have arrived, or if the actor is no longer running.
    pub async fn assert_received(
        actor_ref: &ActorRef<Self>,
        expected_count: usize,
        timeout: Duration,
    ) -> Vec<M> {
        const POLL_INTERVAL: Duration = Duration::from_millis(10);
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            let received = actor_ref
                .ask(GetMessages::<M>::new())
                .await
                .expect("TestActor not reachable (already stopped?)");

            if received.len() >= expected_count {
                return received;
            }

            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "TestActor: expected at least {expected_count} message(s) \
                     within {timeout:?}, but received only {}: {received:?}",
                    received.len()
                );
            }

            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}
