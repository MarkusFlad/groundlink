use std::time::Duration;

use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};

/// Actor, der beliebige Kameo-Nachrichten vom Typ `M` entgegennimmt, im
/// internen Vektor speichert und per [`GetMessages`] eine Kopie dieses
/// Vektors zurückgeben kann.
///
/// Welcher Nachrichtentyp gehandhabt wird, legt der generische Parameter
/// `M` fest, z. B. `TestActor<SimpleString>`. `TestActor<M>` implementiert
/// `Message<M>` einmalig und generisch – es ist also kein weiterer
/// Boilerplate-Code pro Nachrichtentyp nötig.
pub struct TestActor<M> {
    received: Vec<M>,
}

impl<M> TestActor<M> {
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

/// Speichert jede eingehende Nachricht vom Typ `M` im internen Vektor.
impl<M> Message<M> for TestActor<M>
where
    M: Send + 'static,
{
    type Reply = ();

    async fn handle(&mut self, msg: M, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.received.push(msg);
    }
}

/// Query-Nachricht: liefert per `ask` eine Kopie aller bisher empfangenen
/// Nachrichten. Über `PhantomData<fn() -> M>` an `M` gebunden – aus
/// denselben Kohärenzgründen wie bei [`crate::Shutdown`].
pub struct GetMessages<M>(std::marker::PhantomData<fn() -> M>);

impl<M> GetMessages<M> {
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
    /// Wartet, bis der `TestActor` mindestens `expected_count` Nachrichten
    /// empfangen hat, und gibt dann eine Kopie des bisher aufgezeichneten
    /// Vektors zurück, damit der Aufrufer weitere Prüfungen (Inhalt,
    /// Reihenfolge, ...) darauf durchführen kann.
    ///
    /// Schlägt mit `panic!` fehl (wie ein `assert!`), wenn `timeout`
    /// überschritten wird, bevor genügend Nachrichten eingetroffen sind.
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
                .expect("TestActor nicht erreichbar (bereits gestoppt?)");

            if received.len() >= expected_count {
                return received;
            }

            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "TestActor: erwartete mindestens {expected_count} Nachricht(en) \
                     innerhalb von {timeout:?}, aber nur {} empfangen: {received:?}",
                    received.len()
                );
            }

            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}
