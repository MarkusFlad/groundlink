use std::io;

use bytes::{Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

/// Die "SimpleString"-Nachricht: ein einzelner String, wie er vom
/// [`SimpleStringCodec`] (16-Bit-Längenfeld + ASCII/UTF-8-Nutzdaten)
/// gelesen bzw. geschrieben wird.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleString(pub String);

/// Codec für [`SimpleString`]: 16-Bit-Längenfeld (Big-Endian) +
/// ASCII/UTF-8-Nutzdaten. Implementiert sowohl [`Decoder`] als auch
/// [`Encoder<SimpleString>`] und ist damit ein [`crate::MessageCodec<SimpleString>`]
/// – nutzbar mit den generischen TCP-Actors (siehe [`crate::SimpleStringListener`]
/// & Co.) oder direkt mit `tokio_util::codec::Framed`.
#[derive(Debug, Clone)]
pub struct SimpleStringCodec {
    inner: LengthDelimitedCodec,
}

impl Default for SimpleStringCodec {
    fn default() -> Self {
        SimpleStringCodec {
            inner: LengthDelimitedCodec::builder()
                .length_field_length(2)
                .big_endian()
                .new_codec(),
        }
    }
}

impl Decoder for SimpleStringCodec {
    type Item = SimpleString;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        match self.inner.decode(src)? {
            Some(bytes) => match std::str::from_utf8(&bytes) {
                Ok(s) => Ok(Some(SimpleString(s.to_owned()))),
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "ungültige ASCII/UTF-8 Nutzdaten empfangen",
                )),
            },
            None => Ok(None),
        }
    }
}

impl Encoder<SimpleString> for SimpleStringCodec {
    type Error = io::Error;

    fn encode(&mut self, SimpleString(payload): SimpleString, dst: &mut BytesMut) -> io::Result<()> {
        self.inner.encode(Bytes::from(payload.into_bytes()), dst)
    }
}

/// Kodiert einen String als Frame mit 16-Bit-Längenfeld (Big-Endian) +
/// ASCII-Payload, passend zum [`SimpleStringCodec`]-Format. Praktisch für
/// Tests und einfache Clients, die keinen eigenen `Framed`-Stream
/// aufbauen wollen.
pub fn encode_frame(payload: &str) -> Vec<u8> {
    let mut codec = SimpleStringCodec::default();
    let mut buf = BytesMut::new();
    codec
        .encode(SimpleString(payload.to_string()), &mut buf)
        .expect("encode_frame: Kodierung fehlgeschlagen");
    buf.to_vec()
}
