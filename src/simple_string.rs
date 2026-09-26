//! A simple string protocol: each frame is a 16-bit big-endian length
//! field followed by that many bytes of UTF-8 text.
//!
//! ```
//! use bytes::BytesMut;
//! use kameo_tcp_example::{encode_frame, SimpleString, SimpleStringCodec};
//! use tokio_util::codec::Decoder;
//!
//! assert_eq!(encode_frame("hi"), vec![0x00, 0x02, b'h', b'i']);
//!
//! let mut buf = BytesMut::from(&encode_frame("hi")[..]);
//! let decoded = SimpleStringCodec::default().decode(&mut buf).unwrap();
//! assert_eq!(decoded, Some(SimpleString("hi".into())));
//! ```

use std::io;

use bytes::{Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

/// A single string message as read and written by [`SimpleStringCodec`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleString(pub String);

/// Codec for [`SimpleString`]: 16-bit big-endian length field followed by
/// UTF-8 payload.
///
/// Implements both [`Decoder`] and [`Encoder<SimpleString>`], so it is a
/// [`MessageCodec<SimpleString>`](crate::MessageCodec) usable with the
/// generic TCP actors (see [`SimpleStringServer`](crate::SimpleStringServer)
/// and friends) or directly with [`tokio_util::codec::Framed`].
///
/// Decoding fails with [`io::ErrorKind::InvalidData`] if a payload is not
/// valid UTF-8.
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
                    "received invalid ASCII/UTF-8 payload",
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

/// Encodes a string as a frame in the [`SimpleStringCodec`] format.
///
/// Handy for tests and simple clients that do not want to set up a
/// `Framed` stream.
///
/// # Panics
///
/// Panics if `payload` is longer than 65535 bytes.
pub fn encode_frame(payload: &str) -> Vec<u8> {
    let mut codec = SimpleStringCodec::default();
    let mut buf = BytesMut::new();
    codec
        .encode(SimpleString(payload.to_string()), &mut buf)
        .expect("encode_frame: encoding failed");
    buf.to_vec()
}
