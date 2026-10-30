// Events can get new variants later, so a match needs a wildcard.
use groundlink::actor::tcp::ConnectionEventKind;

fn describe(kind: ConnectionEventKind) -> &'static str {
    match kind {
        ConnectionEventKind::Connected => "connected",
        ConnectionEventKind::HalfClosed { .. } => "half closed",
        ConnectionEventKind::Disconnected => "disconnected",
        ConnectionEventKind::ConnectFailed { .. } => "connect failed",
    }
}

fn main() {
    let _ = describe(ConnectionEventKind::Connected);
}
