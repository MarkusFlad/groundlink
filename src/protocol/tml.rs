//! SLE Transport Mapping Layer (TML) messages of ISP1 over TCP (CCSDS
//! 913.1-B-2), and [`TmlCodec`], which frames them on a TCP connection.
//!
//! Every TML message starts with an 8-byte header:
//!
//! ```text
//!  0        1        2        3        4                                7
//! +--------+--------+--------+--------+--------+--------+--------+--------+
//! |  type  |    0   |    0   |    0   |       body length (u32, BE)       |
//! +--------+--------+--------+--------+--------+--------+--------+--------+
//! ```
//!
//! | Type | Message | Body |
//! |---|---|---|
//! | `0x01` | [`TmlMessage::SlePdu`] | BER-encoded SLE PDU |
//! | `0x02` | [`TmlMessage::Context`] | `"ISP1"`, 3 zero bytes, version 1, heartbeat interval (u16), dead factor (u16) |
//! | `0x03` | [`TmlMessage::Heartbeat`] | empty |
//!
//! The codec only frames the messages. The rules of a TML connection (the
//! initiator sends the context message first, heartbeats, the dead timer)
//! are implemented by [`TmlLinkActor`](crate::TmlLinkActor), which passes
//! the SLE PDUs on as [`SlePdu`] messages.
//!
//! ```
//! use bytes::{Bytes, BytesMut};
//! use groundlink::protocol::tml::TmlContext;
//! use groundlink::{TmlCodec, TmlMessage};
//! use tokio_util::codec::{Decoder, Encoder};
//!
//! let mut codec = TmlCodec::default();
//! let mut buf = BytesMut::new();
//! codec.encode(TmlMessage::Context(TmlContext::new(30, 3)), &mut buf).unwrap();
//! codec.encode(TmlMessage::SlePdu(Bytes::from_static(&[0xbf, 0x64, 0x00])), &mut buf).unwrap();
//! assert_eq!(buf.len(), 8 + 12 + 8 + 3);
//!
//! assert_eq!(codec.decode(&mut buf).unwrap(), Some(TmlMessage::Context(TmlContext::new(30, 3))));
//! assert!(matches!(codec.decode(&mut buf).unwrap(), Some(TmlMessage::SlePdu(_))));
//! ```

use std::io;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::protocol::WireMessage;

/// Length of the TML message header in bytes.
pub const TML_HEADER_LEN: usize = 8;
/// Length of the body of a context message in bytes.
pub const TML_CONTEXT_BODY_LEN: usize = 12;
/// Protocol identifier in the context message.
pub const TML_PROTOCOL_ID: [u8; 4] = *b"ISP1";
/// TML version in the context message.
pub const TML_VERSION: u8 = 1;
/// Default for the largest accepted SLE PDU, see [`TmlCodec::with_max_pdu_len`].
pub const DEFAULT_MAX_SLE_PDU_LEN: usize = 1024 * 1024;

const TYPE_SLE_PDU: u8 = 0x01;
const TYPE_CONTEXT: u8 = 0x02;
const TYPE_HEARTBEAT: u8 = 0x03;

/// Body of a TML context message: the heartbeat parameters the initiator
/// proposes for the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TmlContext {
    /// Heartbeat interval in seconds; 0 switches heartbeats and the dead
    /// timer off.
    pub heartbeat_interval: u16,
    /// The peer counts as dead when nothing has arrived for
    /// `heartbeat_interval × dead_factor` seconds.
    pub dead_factor: u16,
}

impl TmlContext {
    /// Creates a context with the given heartbeat interval (seconds) and
    /// dead factor.
    pub fn new(heartbeat_interval: u16, dead_factor: u16) -> Self {
        TmlContext {
            heartbeat_interval,
            dead_factor,
        }
    }
}

/// A TML message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TmlMessage {
    /// A BER-encoded SLE PDU, a slice of the receive buffer without copy.
    SlePdu(Bytes),
    /// The context message the initiator sends first.
    Context(TmlContext),
    /// A heartbeat, sent when nothing else was sent for the heartbeat
    /// interval.
    Heartbeat,
}

impl WireMessage for TmlMessage {}

/// A BER-encoded SLE PDU, as the [`TmlLinkActor`](crate::TmlLinkActor)
/// exchanges it with the SLE layer (e.g. an SLE provider actor).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlePdu(pub Bytes);

/// Codec for [`TmlMessage`]s.
///
/// Decoding fails with [`io::ErrorKind::InvalidData`] on a TML protocol
/// error: an unknown message type, non-zero reserved bytes, a heartbeat or
/// context message of the wrong length, an invalid context message, or an
/// SLE PDU longer than [`max_pdu_len`](Self::max_pdu_len). The header is
/// checked before the body arrives, so a bogus length never makes the
/// reader wait for or buffer a huge body. Encoding fails with
/// [`io::ErrorKind::InvalidInput`] for an SLE PDU longer than the limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TmlCodec {
    max_pdu_len: usize,
}

impl Default for TmlCodec {
    /// A codec that accepts SLE PDUs of up to [`DEFAULT_MAX_SLE_PDU_LEN`]
    /// bytes.
    fn default() -> Self {
        TmlCodec {
            max_pdu_len: DEFAULT_MAX_SLE_PDU_LEN,
        }
    }
}

impl TmlCodec {
    /// A codec that accepts SLE PDUs of up to `max_pdu_len` bytes, in both
    /// directions.
    pub fn with_max_pdu_len(max_pdu_len: usize) -> Self {
        TmlCodec { max_pdu_len }
    }

    /// The largest accepted SLE PDU in bytes.
    pub fn max_pdu_len(&self) -> usize {
        self.max_pdu_len
    }
}

fn invalid_data(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

fn put_header(dst: &mut BytesMut, msg_type: u8, body_len: u32) {
    dst.put_u8(msg_type);
    dst.put_slice(&[0, 0, 0]);
    dst.put_u32(body_len);
}

impl Decoder for TmlCodec {
    type Item = TmlMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<TmlMessage>> {
        if src.len() < TML_HEADER_LEN {
            src.reserve(TML_HEADER_LEN - src.len());
            return Ok(None);
        }

        let msg_type = src[0];
        let reserved = [src[1], src[2], src[3]];
        let body_len = u32::from_be_bytes([src[4], src[5], src[6], src[7]]) as usize;

        if reserved != [0, 0, 0] {
            return Err(invalid_data(format!(
                "TML header bytes 1..=3 must be zero, got {reserved:02x?}"
            )));
        }
        match msg_type {
            TYPE_SLE_PDU if body_len > self.max_pdu_len => {
                return Err(invalid_data(format!(
                    "SLE PDU of {body_len} bytes exceeds the limit of {} bytes",
                    self.max_pdu_len
                )));
            }
            TYPE_CONTEXT if body_len != TML_CONTEXT_BODY_LEN => {
                return Err(invalid_data(format!(
                    "TML context message must have length {TML_CONTEXT_BODY_LEN}, got {body_len}"
                )));
            }
            TYPE_HEARTBEAT if body_len != 0 => {
                return Err(invalid_data(format!(
                    "TML heartbeat message must have length 0, got {body_len}"
                )));
            }
            TYPE_SLE_PDU | TYPE_CONTEXT | TYPE_HEARTBEAT => {}
            other => return Err(invalid_data(format!("unknown TML message type 0x{other:02x}"))),
        }

        let frame_len = TML_HEADER_LEN + body_len;
        if src.len() < frame_len {
            src.reserve(frame_len - src.len());
            return Ok(None);
        }

        src.advance(TML_HEADER_LEN);
        let message = match msg_type {
            TYPE_SLE_PDU => TmlMessage::SlePdu(src.split_to(body_len).freeze()),
            TYPE_HEARTBEAT => TmlMessage::Heartbeat,
            _ => {
                let body = src.split_to(TML_CONTEXT_BODY_LEN);
                if body[0..4] != TML_PROTOCOL_ID {
                    return Err(invalid_data(format!(
                        "invalid TML protocol ID {:02x?}, expected \"ISP1\"",
                        &body[0..4]
                    )));
                }
                if body[4..7] != [0, 0, 0] {
                    return Err(invalid_data(format!(
                        "TML context message reserved bytes must be zero, got {:02x?}",
                        &body[4..7]
                    )));
                }
                if body[7] != TML_VERSION {
                    return Err(invalid_data(format!("unsupported TML version {}", body[7])));
                }
                TmlMessage::Context(TmlContext {
                    heartbeat_interval: u16::from_be_bytes([body[8], body[9]]),
                    dead_factor: u16::from_be_bytes([body[10], body[11]]),
                })
            }
        };
        Ok(Some(message))
    }
}

impl Encoder<TmlMessage> for TmlCodec {
    type Error = io::Error;

    fn encode(&mut self, message: TmlMessage, dst: &mut BytesMut) -> io::Result<()> {
        match message {
            TmlMessage::SlePdu(pdu) => {
                if pdu.len() > self.max_pdu_len || pdu.len() > u32::MAX as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "SLE PDU of {} bytes exceeds the limit of {} bytes",
                            pdu.len(),
                            self.max_pdu_len
                        ),
                    ));
                }
                dst.reserve(TML_HEADER_LEN + pdu.len());
                put_header(dst, TYPE_SLE_PDU, pdu.len() as u32);
                dst.put_slice(&pdu);
            }
            TmlMessage::Context(context) => {
                dst.reserve(TML_HEADER_LEN + TML_CONTEXT_BODY_LEN);
                put_header(dst, TYPE_CONTEXT, TML_CONTEXT_BODY_LEN as u32);
                dst.put_slice(&TML_PROTOCOL_ID);
                dst.put_slice(&[0, 0, 0]);
                dst.put_u8(TML_VERSION);
                dst.put_u16(context.heartbeat_interval);
                dst.put_u16(context.dead_factor);
            }
            TmlMessage::Heartbeat => {
                dst.reserve(TML_HEADER_LEN);
                put_header(dst, TYPE_HEARTBEAT, 0);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(message: TmlMessage) -> BytesMut {
        let mut buf = BytesMut::new();
        TmlCodec::default().encode(message, &mut buf).unwrap();
        buf
    }

    fn decode_err(wire: &[u8]) -> io::Error {
        let mut buf = BytesMut::from(wire);
        TmlCodec::with_max_pdu_len(1024).decode(&mut buf).unwrap_err()
    }

    #[test]
    fn context_wire_format() {
        let buf = encode(TmlMessage::Context(TmlContext::new(30, 3)));
        assert_eq!(
            &buf[..],
            &[
                0x02, 0, 0, 0, 0, 0, 0, 12, // header
                b'I', b'S', b'P', b'1', 0, 0, 0, 1, // protocol ID, reserved, version
                0, 30, 0, 3, // heartbeat interval, dead factor
            ]
        );
    }

    #[test]
    fn heartbeat_and_pdu_wire_format() {
        assert_eq!(&encode(TmlMessage::Heartbeat)[..], &[0x03, 0, 0, 0, 0, 0, 0, 0]);
        let buf = encode(TmlMessage::SlePdu(Bytes::from_static(&[0xbf, 0x64, 0x00])));
        assert_eq!(&buf[..], &[0x01, 0, 0, 0, 0, 0, 0, 3, 0xbf, 0x64, 0x00]);
    }

    #[test]
    fn decodes_a_stream_byte_by_byte() {
        let messages = [
            TmlMessage::Context(TmlContext::new(60, 4)),
            TmlMessage::SlePdu(Bytes::from(vec![0xaa; 300])),
            TmlMessage::Heartbeat,
            TmlMessage::SlePdu(Bytes::new()),
        ];
        let mut codec = TmlCodec::default();
        let mut wire = BytesMut::new();
        for m in &messages {
            codec.encode(m.clone(), &mut wire).unwrap();
        }
        let mut buf = BytesMut::new();
        let mut decoded = Vec::new();
        for byte in wire.iter() {
            buf.put_u8(*byte);
            while let Some(m) = codec.decode(&mut buf).unwrap() {
                decoded.push(m);
            }
        }
        assert_eq!(decoded, messages);
        assert!(buf.is_empty());
    }

    #[test]
    fn rejects_protocol_errors() {
        for wire in [
            &[0x04, 0, 0, 0, 0, 0, 0, 0][..],       // unknown type
            &[0x01, 0, 1, 0, 0, 0, 0, 0][..],       // reserved byte set
            &[0x03, 0, 0, 0, 0, 0, 0, 1][..],       // heartbeat with body
            &[0x02, 0, 0, 0, 0, 0, 0, 11][..],      // short context
            &[0x01, 0, 0, 0, 0, 0, 0x04, 0x01][..], // PDU of 1025 bytes, header only
        ] {
            assert_eq!(decode_err(wire).kind(), io::ErrorKind::InvalidData, "{wire:02x?}");
        }
        let context = |id: &[u8; 4], reserved: u8, version: u8| {
            let mut v = vec![0x02, 0, 0, 0, 0, 0, 0, 12];
            v.extend_from_slice(id);
            v.extend_from_slice(&[0, reserved, 0, version, 0, 30, 0, 3]);
            v
        };
        for wire in [context(b"ISP2", 0, 1), context(b"ISP1", 7, 1), context(b"ISP1", 0, 2)] {
            assert_eq!(decode_err(&wire).kind(), io::ErrorKind::InvalidData, "{wire:02x?}");
        }
    }

    #[test]
    fn encoder_rejects_oversized_pdu() {
        let mut buf = BytesMut::new();
        let err = TmlCodec::with_max_pdu_len(4)
            .encode(TmlMessage::SlePdu(Bytes::from_static(&[0; 5])), &mut buf)
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(buf.is_empty());
    }
}
