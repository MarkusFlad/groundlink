use std::io;
use std::net::SocketAddr;

use tokio_util::codec::{Decoder, Encoder};

/// Grund, warum eine Verbindungs-Hälfte (Lesen oder Schreiben) beendet
/// wurde.
#[derive(Debug, Clone, PartialEq)]
pub enum CloseReason {
    /// Sauberes Ende: EOF beim Lesen bzw. die Verbindung wurde ordentlich
    /// geschlossen.
    Graceful,
    /// Ein I/O- oder Protokollfehler (z. B. ungültige Frame-Daten, Broken
    /// Pipe, Connection Reset, ...). Der Text ist die `Display`-Ausgabe
    /// des zugrunde liegenden Fehlers.
    Error(String),
}

/// Welche Hälfte der Verbindung betroffen ist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionHalf {
    Read,
    Write,
}

/// Nachricht an den `TcpListenerActor`: eine Verbindungs-Hälfte wurde
/// beendet. Wird sowohl vom `TcpConnectionActor` (Read) als auch vom
/// `TcpWriterActor` (Write) gesendet.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionHalfClosed {
    pub peer_addr: SocketAddr,
    pub half: ConnectionHalf,
    pub reason: CloseReason,
}

/// Ein Codec, der Nachrichten vom Typ `M` sowohl dekodieren (Lesen) als
/// auch kodieren (Schreiben) kann – die Voraussetzung, damit
/// [`crate::actors::TcpConnectionActor<M, C>`] und
/// [`crate::actors::TcpWriterActor<M, C>`] ihn nutzen können.
pub trait MessageCodec<M>:
    Decoder<Item = M, Error = io::Error>
        + Encoder<M, Error = io::Error>
        + Default
        + Unpin
        + Send
        + 'static
{
}

impl<M, C> MessageCodec<M> for C where
    C: Decoder<Item = M, Error = io::Error>
        + Encoder<M, Error = io::Error>
        + Default
        + Unpin
        + Send
        + 'static
{
}

/// Fragt die tatsächlich gebundene lokale Adresse ab. Nützlich, wenn mit
/// Port 0 gebunden wurde (z. B. in Tests), um den vom OS vergebenen Port
/// zu erfahren.
#[derive(Debug)]
pub struct GetLocalAddr;

/// Fordert den `TcpClientActor` auf, die Verbindung zu `remote_addr`
/// aufzubauen. Antwortet mit der tatsächlichen Peer-Adresse bei Erfolg
/// bzw. dem aufgetretenen I/O-Fehler.
#[derive(Debug)]
pub struct Connect;

/// Schließt – falls verbunden – beide Hälften der aktuellen Verbindung
/// geordnet (per [`Shutdown<M>`] an Reader und Writer).
#[derive(Debug)]
pub struct Close;

/// Schließt nur die read half der aktuellen Verbindung.
#[derive(Debug)]
pub struct CloseRead;

/// Schließt nur die write half der aktuellen Verbindung.
#[derive(Debug)]
pub struct CloseWrite;

/// Benachrichtigung: die "andere Seite" einer Relais-Kopplung (z. B. die
/// Serververbindung, wenn dieser Actor die Clientseite ist) hat eine
/// Hälfte geschlossen.
#[derive(Debug)]
pub struct PeerHalfClosed(pub ConnectionHalfClosed);

/// Nachricht: die enthaltene Nachricht `M` soll über die aktuelle
/// Verbindung des `TcpClientActor<M, C>` gesendet werden.
pub struct Relay<M>(pub M);

/// Nachricht: fordert den zu `M` gehörenden `TcpWriterActor<M, C>` auf,
/// die write half geordnet zu schließen und sich danach zu beenden.
pub struct Shutdown<M>(std::marker::PhantomData<fn() -> M>);

impl<M> Shutdown<M> {
    pub fn new() -> Self {
        Shutdown(std::marker::PhantomData)
    }
}

impl<M> Default for Shutdown<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M> Clone for Shutdown<M> {
    fn clone(&self) -> Self {
        Shutdown(std::marker::PhantomData)
    }
}

impl<M> Copy for Shutdown<M> {}

impl<M> std::fmt::Debug for Shutdown<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Shutdown").finish()
    }
}
