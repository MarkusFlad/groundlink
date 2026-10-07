//! Tests for batches (`Batch<M>`): several messages passed between actors
//! as one kameo message.
//! 1. A server created with `TcpServerArgs::batched` forwards what arrives
//!    together as batches of at most `max_batch_len`, complete and in
//!    order.
//! 2. A server coupled with a client passes batches through; all messages
//!    arrive in order, and the connection closes after the last one.
//! 3. A writer drops only the message of a batch that cannot be encoded.
//! 4. `impl_batch_message!` calls the handler for each message; the
//!    messages sent to an output during a batch are passed on together.
//! 5. `PusTmStamper` stamps a batch in order and passes it on as one batch.
//! 6. A maximum batch length of 0 is rejected.

use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use groundlink::{
    Batch, ConnectionEvent, ConnectionHalfClosed, Downstream, GetLocalAddr, GetMessages, PusPacket, PusTm,
    PusTmStamper, SimpleString, SimpleStringClient, SimpleStringCodec, SimpleStringServer, SimpleStringWriter,
    TcpClientArgs, TcpServerArgs, TcpWriterArgs, TestActor, encode_frame, impl_batch_message,
};
use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;

fn text(s: impl Into<String>) -> SimpleString {
    SimpleString(s.into())
}

/// Records the batches it receives, so tests can check how messages were
/// grouped.
struct BatchRecorder<M>(Vec<Vec<M>>);

impl<M: Send + 'static> Actor for BatchRecorder<M> {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl<M: Send + 'static> Message<Batch<M>> for BatchRecorder<M> {
    type Reply = ();

    async fn handle(&mut self, batch: Batch<M>, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.0.push(batch.into_vec());
    }
}

struct GetBatches;

impl<M: Clone + Send + 'static> Message<GetBatches> for BatchRecorder<M> {
    type Reply = Vec<Vec<M>>;

    async fn handle(&mut self, _msg: GetBatches, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.0.clone()
    }
}

/// Waits until `recorder` has received `count` messages in total and
/// returns its batches.
async fn batches<M: Clone + Send + 'static>(recorder: &ActorRef<BatchRecorder<M>>, count: usize) -> Vec<Vec<M>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let batches = recorder.ask(GetBatches).await.unwrap();
        let received: usize = batches.iter().map(Vec::len).sum();
        if received >= count {
            return batches;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected {count} messages, got only {received}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn batched_server_forwards_messages_in_batches() {
    let recorder = BatchRecorder::spawn(BatchRecorder::<SimpleString>(Vec::new()));
    let server = SimpleStringServer::spawn(
        TcpServerArgs::batched(
            "127.0.0.1:0".parse().unwrap(),
            recorder.clone().recipient(),
            SimpleStringCodec::default(),
        )
        .with_max_batch_len(8),
    );
    let mut client = TcpStream::connect(server.ask(GetLocalAddr).await.unwrap())
        .await
        .unwrap();

    // All frames in one write, so that the server reads many at once.
    let sent: Vec<SimpleString> = (0..1000).map(|i| text(format!("message {i}"))).collect();
    let frames: Vec<u8> = sent.iter().flat_map(|message| encode_frame(&message.0)).collect();
    client.write_all(&frames).await.unwrap();

    let batches = batches(&recorder, sent.len()).await;
    assert!(batches.iter().all(|batch| !batch.is_empty() && batch.len() <= 8));
    assert!(
        batches.len() < sent.len(),
        "messages that arrive together should be forwarded together"
    );
    assert_eq!(batches.concat(), sent);
}

#[tokio::test]
async fn coupled_server_and_client_pass_batches_through() {
    let peer_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let codec = SimpleStringCodec::default();

    let client = SimpleStringClient::prepare();
    let server = SimpleStringServer::spawn(
        TcpServerArgs::batched(
            "127.0.0.1:0".parse().unwrap(),
            client.actor_ref().clone().recipient(),
            codec.clone(),
        )
        .with_observer(client.actor_ref().clone().reply_recipient::<ConnectionEvent>()),
    );
    client.spawn(
        TcpClientArgs::batched(peer_b.local_addr().unwrap(), server.clone().recipient(), codec)
            .with_observer(server.clone().reply_recipient::<ConnectionEvent>()),
    );

    let addr = server.ask(GetLocalAddr).await.unwrap();
    let mut a = Framed::new(TcpStream::connect(addr).await.unwrap(), SimpleStringCodec::default());
    let (b_stream, _) = tokio::time::timeout(Duration::from_secs(2), peer_b.accept())
        .await
        .unwrap()
        .unwrap();
    let mut b = Framed::new(b_stream, SimpleStringCodec::default());

    // A sends everything and closes right away.
    let sent: Vec<SimpleString> = (0..3000).map(|i| text(format!("message {i}"))).collect();
    let sending = {
        let sent = sent.clone();
        tokio::spawn(async move {
            for message in sent {
                a.feed(message).await.unwrap();
            }
            a.flush().await.unwrap();
        })
    };

    // B receives every message and then the end of the connection.
    let mut received = Vec::new();
    while let Some(message) = tokio::time::timeout(Duration::from_secs(5), b.next())
        .await
        .expect("timed out")
    {
        received.push(message.unwrap());
    }
    sending.await.unwrap();
    assert_eq!(received, sent);
}

#[tokio::test]
async fn writer_drops_only_the_message_of_a_batch_that_cannot_be_encoded() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (client_stream, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
    let mut client_stream = client_stream.unwrap();
    let (server_stream, peer_addr) = accepted.unwrap();
    let (_server_read, server_write) = server_stream.into_split();

    let closed = TestActor::<ConnectionHalfClosed>::spawn(TestActor::new());
    let writer = SimpleStringWriter::spawn(TcpWriterArgs::new(
        server_write,
        peer_addr,
        closed.clone().recipient(),
        SimpleStringCodec::default(),
    ));

    // The second message is too long for the 16-bit length field.
    let batch = Batch::from(vec![text("before"), text("x".repeat(70_000)), text("after")]);
    writer.tell(batch).await.unwrap();

    let mut frames = [0u8; 2 + 6 + 2 + 5];
    tokio::time::timeout(Duration::from_secs(1), client_stream.read_exact(&mut frames))
        .await
        .expect("both valid messages should arrive")
        .unwrap();
    assert_eq!(&frames, b"\x00\x06before\x00\x05after");
    assert!(writer.is_alive());
    assert!(closed.ask(GetMessages::new()).await.unwrap().is_empty());
}

/// Sends every message on twice.
struct Doubler {
    out: Downstream<SimpleString>,
}

impl Actor for Doubler {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(state: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<SimpleString> for Doubler {
    type Reply = ();

    async fn handle(&mut self, message: SimpleString, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.out.send(message.clone()).await.unwrap();
        self.out.send(message).await.unwrap();
    }
}

impl_batch_message!(Doubler, SimpleString, outputs = [out]);

#[tokio::test]
async fn messages_sent_during_a_batch_are_passed_on_as_one_batch() {
    let recorder = BatchRecorder::spawn(BatchRecorder::<SimpleString>(Vec::new()));
    let doubler = Doubler::spawn(Doubler {
        out: recorder.clone().recipient::<Batch<SimpleString>>().into(),
    });

    doubler
        .tell(Batch::from(vec![text("a"), text("b"), text("c")]))
        .await
        .unwrap();
    // A single message is not held back.
    doubler.tell(text("d")).await.unwrap();

    let batches = batches(&recorder, 8).await;
    assert_eq!(
        batches,
        vec![
            vec![text("a"), text("a"), text("b"), text("b"), text("c"), text("c")],
            vec![text("d")],
            vec![text("d")],
        ]
    );
}

#[tokio::test]
async fn messages_sent_during_a_batch_reach_a_recipient_of_single_messages_in_order() {
    let received = TestActor::<SimpleString>::spawn(TestActor::new());
    let doubler = Doubler::spawn(Doubler {
        out: received.clone().recipient::<SimpleString>().into(),
    });

    doubler.tell(Batch::from(vec![text("a"), text("b")])).await.unwrap();

    let messages = TestActor::assert_received(&received, 4, Duration::from_secs(1)).await;
    assert_eq!(messages, vec![text("a"), text("a"), text("b"), text("b")]);
}

#[tokio::test]
async fn stamper_stamps_a_batch_in_order_and_passes_it_on_as_one_batch() {
    let recorder = BatchRecorder::spawn(BatchRecorder::<PusPacket>(Vec::new()));
    let stamper = PusTmStamper::spawn(PusTmStamper::new(
        0x042,
        recorder.clone().recipient::<Batch<PusPacket>>(),
    ));

    let tms: Vec<PusTm> = (0..3)
        .map(|_| PusTm::new(0, 0, 17, 2, Bytes::new(), Bytes::new()))
        .collect();
    stamper.tell(Batch::from(tms)).await.unwrap();

    let batches = batches(&recorder, 3).await;
    assert_eq!(batches.len(), 1, "the batch should stay together");
    let counts: Vec<u16> = batches[0]
        .iter()
        .map(|packet| match packet {
            PusPacket::Tm(tm) => tm.header.sequence_count,
            PusPacket::Tc(_) => panic!("expected TM"),
        })
        .collect();
    assert_eq!(counts, vec![0, 1, 2]);
}

#[tokio::test]
#[should_panic(expected = "max batch length must be at least 1")]
async fn zero_max_batch_len_is_rejected() {
    let downstream = TestActor::<SimpleString>::spawn(TestActor::new());
    let _ = TcpServerArgs::new(
        "127.0.0.1:0".parse().unwrap(),
        downstream.recipient::<SimpleString>(),
        SimpleStringCodec::default(),
    )
    .with_max_batch_len(0);
}
