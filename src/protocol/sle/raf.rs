//! Return All Frames (RAF) PDUs, CCSDS 911.1-B-4 (subset: BIND, UNBIND,
//! START, STOP, TRANSFER-DATA).
//!
//! The user sends [`RafUserPdu`]s, the provider [`RafProviderPdu`]s. The
//! frames reach the user in RAF-TRANSFER-BUFFERs, each holding one or more
//! [`RafFrame`]s (RAF-TRANSFER-DATA invocations).
//!
//! ```
//! use bytes::Bytes;
//! use groundlink::protocol::sle::common::Time;
//! use groundlink::protocol::sle::raf::{AntennaId, FrameQuality};
//! use groundlink::{RafFrame, RafProviderPdu};
//!
//! let frame = RafFrame {
//!     earth_receive_time: Time::now(),
//!     antenna_id: AntennaId::Local(Bytes::from_static(b"ANT1")),
//!     data_link_continuity: -1,
//!     quality: FrameQuality::Good,
//!     private_annotation: None,
//!     data: Bytes::from_static(&[0x12; 64]),
//! };
//! let pdu = RafProviderPdu::TransferBuffer(vec![frame.clone()]).encode();
//! assert_eq!(RafProviderPdu::decode(pdu).unwrap(), RafProviderPdu::TransferBuffer(vec![frame]));
//! ```

use bytes::Bytes;

use super::ber::{BerError, BerReader, BerResult, BerWriter, Tag};
use super::common::*;

const TAG_START_INVOCATION: Tag = Tag::ctx_cons(0);
const TAG_START_RETURN: Tag = Tag::ctx_cons(1);
const TAG_STOP_INVOCATION: Tag = Tag::ctx_cons(2);
const TAG_STOP_RETURN: Tag = Tag::ctx_cons(3);
const TAG_SCHEDULE_STATUS_REPORT: Tag = Tag::ctx_cons(4);
const TAG_GET_PARAMETER: Tag = Tag::ctx_cons(6);
const TAG_TRANSFER_BUFFER: Tag = Tag::ctx_cons(8);
const TAG_ANNOTATED_FRAME: Tag = Tag::ctx_cons(0);
const TAG_SYNC_NOTIFICATION: Tag = Tag::ctx_cons(1);

/// RAF-START specific diagnostics.
pub mod start_diagnostic {
    /// The service instance is out of service.
    pub const OUT_OF_SERVICE: i64 = 0;
    /// The provider cannot comply with the request.
    pub const UNABLE_TO_COMPLY: i64 = 1;
    /// The start time is invalid.
    pub const INVALID_START_TIME: i64 = 2;
    /// The stop time is invalid.
    pub const INVALID_STOP_TIME: i64 = 3;
    /// A start or stop time is required but missing.
    pub const MISSING_TIME_VALUE: i64 = 4;
}

/// The frames a user asks for in RAF-START.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestedFrameQuality {
    /// Only frames that passed the frame check.
    GoodFramesOnly = 0,
    /// Only frames that failed the frame check.
    ErredFramesOnly = 1,
    /// All frames.
    AllFrames = 2,
}

impl RequestedFrameQuality {
    fn from_i64(v: i64) -> BerResult<Self> {
        match v {
            0 => Ok(Self::GoodFramesOnly),
            1 => Ok(Self::ErredFramesOnly),
            2 => Ok(Self::AllFrames),
            _ => Err(BerError::InvalidValue("requested frame quality")),
        }
    }

    /// Whether a frame of `quality` is delivered.
    pub fn accepts(self, quality: FrameQuality) -> bool {
        match self {
            Self::GoodFramesOnly => quality == FrameQuality::Good,
            Self::ErredFramesOnly => quality == FrameQuality::Erred,
            Self::AllFrames => true,
        }
    }
}

/// The quality of a received frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameQuality {
    /// The frame passed the frame check.
    Good = 0,
    /// The frame failed the frame check.
    Erred = 1,
    /// The frame has no frame check.
    Undetermined = 2,
}

impl FrameQuality {
    fn from_i64(v: i64) -> BerResult<Self> {
        match v {
            0 => Ok(Self::Good),
            1 => Ok(Self::Erred),
            2 => Ok(Self::Undetermined),
            _ => Err(BerError::InvalidValue("frame quality")),
        }
    }
}

/// The antenna that received a frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AntennaId {
    /// Globally unique, an OBJECT IDENTIFIER (its arcs).
    Global(Vec<u32>),
    /// Unique within the ground station, 1 to 16 octets.
    Local(Bytes),
}

impl AntennaId {
    fn write(&self, w: &mut BerWriter) {
        match self {
            AntennaId::Global(oid) => w.oid(Tag::ctx(0), oid),
            AntennaId::Local(b) => w.octets(Tag::ctx(1), b),
        }
    }

    fn read(r: &mut BerReader) -> BerResult<Self> {
        match r.peek_tag()? {
            t if t == Tag::ctx(0) => r.oid(t).map(AntennaId::Global),
            t if t == Tag::ctx(1) => r.octets(t).map(AntennaId::Local),
            t => Err(BerError::UnknownChoice(t)),
        }
    }
}

/// RAF-START invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RafStartInvocation {
    /// The credentials of the user.
    pub credentials: Credentials,
    /// The invoke identifier.
    pub invoke_id: InvokeId,
    /// Start of the requested period (offline delivery only).
    pub start_time: ConditionalTime,
    /// End of the requested period (offline delivery only).
    pub stop_time: ConditionalTime,
    /// Which frames to deliver.
    pub requested_quality: RequestedFrameQuality,
}

impl RafStartInvocation {
    fn write(&self, w: &mut BerWriter) {
        self.credentials.write(w);
        w.integer(Tag::INTEGER, i64::from(self.invoke_id));
        self.start_time.write(w);
        self.stop_time.write(w);
        w.integer(Tag::INTEGER, self.requested_quality as i64);
    }

    fn read(r: &mut BerReader) -> BerResult<Self> {
        Ok(Self {
            credentials: Credentials::read(r)?,
            invoke_id: r.integer_as(Tag::INTEGER)?,
            start_time: ConditionalTime::read(r)?,
            stop_time: ConditionalTime::read(r)?,
            requested_quality: RequestedFrameQuality::from_i64(r.integer(Tag::INTEGER)?)?,
        })
    }
}

/// RAF-START return.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RafStartReturn {
    /// The credentials of the provider.
    pub credentials: Credentials,
    /// The invoke identifier of the invocation.
    pub invoke_id: InvokeId,
    /// `Err` carries a common diagnostic or one of [`start_diagnostic`].
    pub result: Result<(), Diagnostic>,
}

impl RafStartReturn {
    fn write(&self, w: &mut BerWriter) {
        self.credentials.write(w);
        w.integer(Tag::INTEGER, i64::from(self.invoke_id));
        match &self.result {
            Ok(()) => w.null(Tag::ctx(0)),
            Err(d) => w.constructed(Tag::ctx_cons(1), |w| d.write(w)),
        }
    }

    fn read(r: &mut BerReader) -> BerResult<Self> {
        let credentials = Credentials::read(r)?;
        let invoke_id = r.integer_as(Tag::INTEGER)?;
        let result = match r.peek_tag()? {
            t if t == Tag::ctx(0) => Ok(r.null(t)?),
            t if t == Tag::ctx_cons(1) => Err(r.sequence(t, Diagnostic::read)?),
            t => return Err(BerError::UnknownChoice(t)),
        };
        Ok(Self {
            credentials,
            invoke_id,
            result,
        })
    }
}

/// One annotated frame of a RAF-TRANSFER-DATA invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RafFrame {
    /// When the frame was received.
    pub earth_receive_time: Time,
    /// The antenna that received the frame.
    pub antenna_id: AntennaId,
    /// Frames missing since the previous delivered frame; -1 = unknown
    /// (the provider sets -1 for the first frame after START).
    pub data_link_continuity: i32,
    /// The quality of the frame.
    pub quality: FrameQuality,
    /// Annotation of the ground station, if any.
    pub private_annotation: Option<Bytes>,
    /// The frame.
    pub data: Bytes,
}

impl RafFrame {
    fn write(&self, w: &mut BerWriter) {
        Credentials::Unused.write(w);
        self.earth_receive_time.write(w);
        self.antenna_id.write(w);
        w.integer(Tag::INTEGER, i64::from(self.data_link_continuity));
        w.integer(Tag::INTEGER, self.quality as i64);
        match &self.private_annotation {
            None => w.null(Tag::ctx(0)),
            Some(b) => w.octets(Tag::ctx(1), b),
        }
        w.octets(Tag::OCTET_STRING, &self.data);
    }

    fn read(r: &mut BerReader) -> BerResult<Self> {
        let _credentials = Credentials::read(r)?;
        let earth_receive_time = Time::read(r)?;
        let antenna_id = AntennaId::read(r)?;
        let data_link_continuity = r.integer_as(Tag::INTEGER)?;
        let quality = FrameQuality::from_i64(r.integer(Tag::INTEGER)?)?;
        let private_annotation = match r.peek_tag()? {
            t if t == Tag::ctx(0) => {
                r.null(t)?;
                None
            }
            t if t == Tag::ctx(1) => Some(r.octets(t)?),
            t => return Err(BerError::UnknownChoice(t)),
        };
        let data = r.octets(Tag::OCTET_STRING)?;
        Ok(Self {
            earth_receive_time,
            antenna_id,
            data_link_continuity,
            quality,
            private_annotation,
            data,
        })
    }
}

/// Encodes a RAF-TRANSFER-BUFFER PDU holding `frames`; the same as
/// [`RafProviderPdu::TransferBuffer`], without the copy into a `Vec`.
pub fn encode_transfer_buffer(frames: &[RafFrame]) -> Bytes {
    let capacity = frames.iter().map(|f| f.data.len() + 48).sum::<usize>() + 8;
    let mut w = BerWriter::with_capacity(capacity);
    w.constructed(TAG_TRANSFER_BUFFER, |w| {
        for f in frames {
            w.constructed(TAG_ANNOTATED_FRAME, |w| f.write(w));
        }
    });
    w.into_bytes()
}

/// PDUs sent by the RAF user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RafUserPdu {
    /// RAF-BIND invocation.
    Bind(BindInvocation),
    /// RAF-UNBIND invocation.
    Unbind(UnbindInvocation),
    /// RAF-START invocation.
    Start(RafStartInvocation),
    /// RAF-STOP invocation.
    Stop(StopInvocation),
}

impl RafUserPdu {
    /// Encodes the PDU.
    pub fn encode(&self) -> Bytes {
        let mut w = BerWriter::new();
        match self {
            Self::Bind(v) => w.constructed(TAG_BIND_INVOCATION, |w| v.write(w)),
            Self::Unbind(v) => w.constructed(TAG_UNBIND_INVOCATION, |w| v.write(w)),
            Self::Start(v) => w.constructed(TAG_START_INVOCATION, |w| v.write(w)),
            Self::Stop(v) => w.constructed(TAG_STOP_INVOCATION, |w| v.write(w)),
        }
        w.into_bytes()
    }

    /// Decodes a PDU. GET-PARAMETER and SCHEDULE-STATUS-REPORT give
    /// [`PduError::Unsupported`].
    pub fn decode(pdu: Bytes) -> Result<Self, PduError> {
        let (tag, mut r) = open_pdu(pdu)?;
        let pdu = match tag {
            TAG_BIND_INVOCATION => Self::Bind(BindInvocation::read(&mut r)?),
            TAG_UNBIND_INVOCATION => Self::Unbind(UnbindInvocation::read(&mut r)?),
            TAG_START_INVOCATION => Self::Start(RafStartInvocation::read(&mut r)?),
            TAG_STOP_INVOCATION => Self::Stop(StopInvocation::read(&mut r)?),
            TAG_SCHEDULE_STATUS_REPORT | TAG_GET_PARAMETER => return Err(PduError::Unsupported(tag)),
            other => return Err(BerError::UnknownChoice(other).into()),
        };
        finish(r, pdu)
    }
}

/// PDUs sent by the RAF provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RafProviderPdu {
    /// RAF-BIND return.
    BindReturn(BindReturn),
    /// RAF-UNBIND return.
    UnbindReturn(UnbindReturn),
    /// RAF-START return.
    StartReturn(RafStartReturn),
    /// RAF-STOP return.
    StopReturn(Acknowledgement),
    /// RAF-TRANSFER-BUFFER with RAF-TRANSFER-DATA invocations.
    TransferBuffer(Vec<RafFrame>),
}

impl RafProviderPdu {
    /// Encodes the PDU.
    pub fn encode(&self) -> Bytes {
        let mut w = BerWriter::new();
        match self {
            Self::BindReturn(v) => w.constructed(TAG_BIND_RETURN, |w| v.write(w)),
            Self::UnbindReturn(v) => w.constructed(TAG_UNBIND_RETURN, |w| v.write(w)),
            Self::StartReturn(v) => w.constructed(TAG_START_RETURN, |w| v.write(w)),
            Self::StopReturn(v) => w.constructed(TAG_STOP_RETURN, |w| v.write(w)),
            Self::TransferBuffer(frames) => return encode_transfer_buffer(frames),
        }
        w.into_bytes()
    }

    /// Decodes a PDU. A transfer buffer with a RAF-SYNC-NOTIFY gives
    /// [`PduError::Unsupported`].
    pub fn decode(pdu: Bytes) -> Result<Self, PduError> {
        let (tag, mut r) = open_pdu(pdu)?;
        let pdu = match tag {
            TAG_BIND_RETURN => Self::BindReturn(BindReturn::read(&mut r)?),
            TAG_UNBIND_RETURN => Self::UnbindReturn(UnbindReturn::read(&mut r)?),
            TAG_START_RETURN => Self::StartReturn(RafStartReturn::read(&mut r)?),
            TAG_STOP_RETURN => Self::StopReturn(Acknowledgement::read(&mut r)?),
            TAG_TRANSFER_BUFFER => {
                let mut frames = Vec::new();
                while !r.is_empty() {
                    match r.peek_tag()? {
                        TAG_ANNOTATED_FRAME => frames.push(r.sequence(TAG_ANNOTATED_FRAME, RafFrame::read)?),
                        TAG_SYNC_NOTIFICATION => return Err(PduError::Unsupported(TAG_SYNC_NOTIFICATION)),
                        t => return Err(BerError::UnknownChoice(t).into()),
                    }
                }
                Self::TransferBuffer(frames)
            }
            other => return Err(BerError::UnknownChoice(other).into()),
        };
        finish(r, pdu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(n: u8) -> RafFrame {
        RafFrame {
            earth_receive_time: Time::Ccsds([0x5d, 0x7a, 0, 0, 0x10, 0, 0, 1]),
            antenna_id: AntennaId::Local(Bytes::from_static(b"ANT1")),
            data_link_continuity: 0,
            quality: FrameQuality::Good,
            private_annotation: None,
            data: Bytes::from(vec![n; 1115]),
        }
    }

    #[test]
    fn user_pdus_roundtrip() {
        let pdus = [
            RafUserPdu::Start(RafStartInvocation {
                credentials: Credentials::Unused,
                invoke_id: 7,
                start_time: ConditionalTime::Undefined,
                stop_time: ConditionalTime::Known(Time::Ccsds([1, 2, 3, 4, 5, 6, 7, 8])),
                requested_quality: RequestedFrameQuality::AllFrames,
            }),
            RafUserPdu::Stop(StopInvocation {
                credentials: Credentials::Unused,
                invoke_id: 8,
            }),
            RafUserPdu::Unbind(UnbindInvocation {
                credentials: Credentials::Used(Bytes::from_static(&[9; 20])),
                reason: unbind_reason::END,
            }),
        ];
        for p in pdus {
            assert_eq!(RafUserPdu::decode(p.encode()).unwrap(), p);
        }
    }

    #[test]
    fn start_wire_format() {
        let p = RafUserPdu::Start(RafStartInvocation {
            credentials: Credentials::Unused,
            invoke_id: 1,
            start_time: ConditionalTime::Undefined,
            stop_time: ConditionalTime::Undefined,
            requested_quality: RequestedFrameQuality::GoodFramesOnly,
        });
        assert_eq!(
            &p.encode()[..],
            &[
                0xa0, 0x0c, 0x80, 0x00, 0x02, 0x01, 0x01, 0x80, 0x00, 0x80, 0x00, 0x02, 0x01, 0x00
            ]
        );
    }

    #[test]
    fn provider_pdus_roundtrip() {
        let pdus = [
            RafProviderPdu::StartReturn(RafStartReturn {
                credentials: Credentials::Unused,
                invoke_id: 7,
                result: Err(Diagnostic::Specific(start_diagnostic::UNABLE_TO_COMPLY)),
            }),
            RafProviderPdu::StartReturn(RafStartReturn {
                credentials: Credentials::Unused,
                invoke_id: 7,
                result: Ok(()),
            }),
            RafProviderPdu::StopReturn(Acknowledgement::positive(8)),
            RafProviderPdu::UnbindReturn(UnbindReturn::default()),
            RafProviderPdu::TransferBuffer(vec![frame(1), frame(2), {
                let mut f = frame(3);
                f.private_annotation = Some(Bytes::from_static(b"x"));
                f.antenna_id = AntennaId::Global(vec![1, 3, 112, 4, 7, 0]);
                f.data_link_continuity = -1;
                f.quality = FrameQuality::Erred;
                f
            }]),
        ];
        for p in pdus {
            assert_eq!(RafProviderPdu::decode(p.encode()).unwrap(), p);
        }
    }

    #[test]
    fn unsupported_operations_are_reported() {
        let mut w = BerWriter::new();
        w.constructed(TAG_GET_PARAMETER, |w| w.null(Tag::ctx(0)));
        assert_eq!(
            RafUserPdu::decode(w.into_bytes()),
            Err(PduError::Unsupported(TAG_GET_PARAMETER))
        );
    }
}
