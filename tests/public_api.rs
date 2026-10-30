//! Tests of the public API, as a user of the crate sees it: the entry
//! points re-exported at the crate root and the items used through their
//! modules are reachable, and the protocols plug into the TCP actors.

use std::any::type_name;

use groundlink::{
    Batch, ConnectionEvent, Downstream, FcltuConfig, FcltuServiceInstance, GetMessages, MessageCodec, PusClient,
    PusCodec, PusConfig, PusPacket, PusPacketAdapter, PusServer, PusTc, PusTcAcceptor, PusTestServiceActor, PusTm,
    PusTmStamper, RafConfig, RafServiceInstance, SimpleString, SimpleStringClient, SimpleStringCodec,
    SimpleStringServer, SleAssociationActor, SleAssociationConfig, SlePdu, SleProviderEvent, SleServiceRegistry,
    SleUserActor, SleUserConfig, SleUserEvent, SpacePacket, SpacePacketClient, SpacePacketCodec, SpacePacketServer,
    TcFrameConfig, TcFrameDecoder, TcFrameEncoder, TcpClientActor, TcpClientArgs, TcpServerActor, TcpServerArgs,
    TestActor, TmFrameConfig, TmFrameDecoder, TmFrameEncoder, TmlClient, TmlCodec, TmlLinkActor, TmlLinkEvent,
    TmlMessage, TmlResponderConfig, TmlServer, WireMessage,
};

fn assert_protocol<M: WireMessage, C: MessageCodec<M>>() {}

#[test]
fn entry_points_are_reachable_from_the_root() {
    let names = [
        // Actors, by layer.
        type_name::<TcpServerActor<SimpleString, SimpleStringCodec>>(),
        type_name::<TcpClientActor<SimpleString, SimpleStringCodec>>(),
        type_name::<SimpleStringServer>(),
        type_name::<SimpleStringClient>(),
        type_name::<SpacePacketServer>(),
        type_name::<SpacePacketClient>(),
        type_name::<PusServer>(),
        type_name::<PusClient>(),
        type_name::<TmlServer>(),
        type_name::<TmlClient>(),
        type_name::<PusTcAcceptor>(),
        type_name::<PusTestServiceActor>(),
        type_name::<PusTmStamper>(),
        type_name::<PusPacketAdapter<PusTm>>(),
        type_name::<TcFrameEncoder>(),
        type_name::<TcFrameDecoder>(),
        type_name::<TmFrameEncoder>(),
        type_name::<TmFrameDecoder>(),
        type_name::<TmlLinkActor>(),
        type_name::<SleAssociationActor>(),
        type_name::<RafServiceInstance>(),
        type_name::<FcltuServiceInstance>(),
        type_name::<SleUserActor>(),
        type_name::<TestActor<SimpleString>>(),
        // What it takes to set them up, and what they report.
        type_name::<TcpServerArgs<SimpleString, SimpleStringCodec>>(),
        type_name::<TcpClientArgs<SimpleString, SimpleStringCodec>>(),
        type_name::<ConnectionEvent>(),
        type_name::<PusConfig>(),
        type_name::<TcFrameConfig>(),
        type_name::<TmFrameConfig>(),
        type_name::<TmlResponderConfig>(),
        type_name::<TmlLinkEvent>(),
        type_name::<SleAssociationConfig>(),
        type_name::<SleServiceRegistry>(),
        type_name::<SleProviderEvent>(),
        type_name::<RafConfig>(),
        type_name::<FcltuConfig>(),
        type_name::<SleUserConfig>(),
        type_name::<SleUserEvent>(),
        type_name::<Batch<SimpleString>>(),
        type_name::<Downstream<SimpleString>>(),
        type_name::<GetMessages<SimpleString>>(),
        // Messages.
        type_name::<PusTc>(),
        type_name::<PusTm>(),
        type_name::<SlePdu>(),
    ];
    for name in names {
        assert!(name.starts_with("groundlink::"), "{name} is not an item of groundlink");
    }

    assert_protocol::<SimpleString, SimpleStringCodec>();
    assert_protocol::<SpacePacket, SpacePacketCodec>();
    assert_protocol::<PusPacket, PusCodec>();
    assert_protocol::<TmlMessage, TmlCodec>();
}

#[test]
fn details_are_reachable_through_their_modules() {
    use groundlink::actor::sle::{Cltu, CltuStart, RafStart, SleBind, SleStop, SleUnbind, SleUserError};
    use groundlink::actor::tcp::{
        Close, CloseRead, CloseReason, CloseWrite, Connect, ConnectionEventKind, ConnectionHalf, ConnectionPolicy,
        EventSource, GetLocalAddr, KeepAlive,
    };
    use groundlink::actor::tml::{SleDisconnect, SlePeerAbort, TmlReleaseReason, TmlRole};
    use groundlink::protocol::ccsds::PacketType;
    use groundlink::protocol::cuc::CucTime;
    use groundlink::protocol::pus::service1::VerificationReport;
    use groundlink::protocol::pus::service17::AreYouAliveRequest;
    use groundlink::protocol::simple_string::encode_frame;
    use groundlink::protocol::sle::fcltu::FcltuUserPdu;
    use groundlink::protocol::sle::raf::{RafFrame, RafUserPdu};

    let names = [
        type_name::<Cltu>(),
        type_name::<CltuStart>(),
        type_name::<RafStart>(),
        type_name::<SleBind>(),
        type_name::<SleStop>(),
        type_name::<SleUnbind>(),
        type_name::<SleUserError>(),
        type_name::<Close>(),
        type_name::<CloseRead>(),
        type_name::<CloseReason>(),
        type_name::<CloseWrite>(),
        type_name::<Connect>(),
        type_name::<ConnectionEventKind>(),
        type_name::<ConnectionHalf>(),
        type_name::<ConnectionPolicy>(),
        type_name::<EventSource>(),
        type_name::<GetLocalAddr>(),
        type_name::<KeepAlive>(),
        type_name::<SleDisconnect>(),
        type_name::<SlePeerAbort>(),
        type_name::<TmlReleaseReason>(),
        type_name::<TmlRole>(),
        type_name::<PacketType>(),
        type_name::<CucTime>(),
        type_name::<VerificationReport>(),
        type_name::<AreYouAliveRequest>(),
        type_name::<FcltuUserPdu>(),
        type_name::<RafFrame>(),
        type_name::<RafUserPdu>(),
    ];
    for name in names {
        assert!(name.starts_with("groundlink::"), "{name} is not an item of groundlink");
    }
    assert_eq!(&encode_frame("a")[..], b"\x00\x01a");
}
