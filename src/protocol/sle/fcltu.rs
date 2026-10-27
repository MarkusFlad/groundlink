//! Forward CLTU (FCLTU) PDUs, CCSDS 912.1-B-4 (subset: BIND, UNBIND,
//! START, STOP, TRANSFER-DATA).
//!
//! The user sends [`FcltuUserPdu`]s, the provider [`FcltuProviderPdu`]s.
//! Each CLTU-TRANSFER-DATA invocation carries one CLTU and is answered by a
//! [`CltuTransferDataReturn`] with the identifier of the CLTU the provider
//! expects next.
//!
//! ```
//! use bytes::Bytes;
//! use groundlink::protocol::sle::common::{ConditionalTime, Credentials};
//! use groundlink::{CltuTransferDataInvocation, FcltuUserPdu};
//!
//! let transfer = FcltuUserPdu::TransferData(CltuTransferDataInvocation {
//!     credentials: Credentials::Unused,
//!     invoke_id: 2,
//!     cltu_id: 42,
//!     earliest_transmission_time: ConditionalTime::Undefined,
//!     latest_transmission_time: ConditionalTime::Undefined,
//!     delay_time_us: 0,
//!     produce_notification: false,
//!     data: Bytes::from_static(&[0xeb, 0x90, 1, 2, 3, 4, 5, 6, 7, 0xc5]),
//! });
//! assert_eq!(FcltuUserPdu::decode(transfer.encode()).unwrap(), transfer);
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
const TAG_THROW_EVENT: Tag = Tag::ctx_cons(8);
const TAG_TRANSFER_DATA_INVOCATION: Tag = Tag::ctx_cons(10);
const TAG_TRANSFER_DATA_RETURN: Tag = Tag::ctx_cons(11);

/// The identification of a CLTU, counted up by one per CLTU.
pub type CltuId = u32;

/// CLTU-START specific diagnostics.
pub mod start_diagnostic {
    /// The service instance is out of service.
    pub const OUT_OF_SERVICE: i64 = 0;
    /// The provider cannot comply with the request.
    pub const UNABLE_TO_COMPLY: i64 = 1;
    /// The production time has expired.
    pub const PRODUCTION_TIME_EXPIRED: i64 = 2;
    /// The first CLTU identification is invalid.
    pub const INVALID_CLTU_ID: i64 = 3;
}

/// CLTU-TRANSFER-DATA specific diagnostics.
pub mod transfer_diagnostic {
    /// The provider cannot process the CLTU.
    pub const UNABLE_TO_PROCESS: i64 = 0;
    /// The CLTU buffer has no room for the CLTU.
    pub const UNABLE_TO_STORE: i64 = 1;
    /// The CLTU identification is not the one expected.
    pub const OUT_OF_SEQUENCE: i64 = 2;
    /// The earliest transmission time is after the latest.
    pub const INCONSISTENT_TIME_RANGE: i64 = 3;
    /// A transmission time is invalid.
    pub const INVALID_TIME: i64 = 4;
    /// The latest transmission time has passed.
    pub const LATE_SLDU: i64 = 5;
    /// The delay time is invalid.
    pub const INVALID_DELAY_TIME: i64 = 6;
    /// The CLTU is empty or too long.
    pub const CLTU_ERROR: i64 = 7;
}

/// CLTU-START invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CltuStartInvocation {
    /// The credentials of the user.
    pub credentials: Credentials,
    /// The invoke identifier.
    pub invoke_id: InvokeId,
    /// The identification of the first CLTU the user will send.
    pub first_cltu_id: CltuId,
}

impl CltuStartInvocation {
    fn write(&self, w: &mut BerWriter) {
        self.credentials.write(w);
        w.integer(Tag::INTEGER, i64::from(self.invoke_id));
        w.integer(Tag::INTEGER, i64::from(self.first_cltu_id));
    }

    fn read(r: &mut BerReader) -> BerResult<Self> {
        Ok(Self {
            credentials: Credentials::read(r)?,
            invoke_id: r.integer_as(Tag::INTEGER)?,
            first_cltu_id: r.integer_as(Tag::INTEGER)?,
        })
    }
}

/// The result of a positive CLTU-START return.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CltuStartPositive {
    /// When radiation starts.
    pub start_radiation_time: Time,
    /// When radiation stops, if known.
    pub stop_radiation_time: ConditionalTime,
}

/// CLTU-START return.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CltuStartReturn {
    /// The credentials of the provider.
    pub credentials: Credentials,
    /// The invoke identifier of the invocation.
    pub invoke_id: InvokeId,
    /// `Err` carries a common diagnostic or one of [`start_diagnostic`].
    pub result: Result<CltuStartPositive, Diagnostic>,
}

impl CltuStartReturn {
    fn write(&self, w: &mut BerWriter) {
        self.credentials.write(w);
        w.integer(Tag::INTEGER, i64::from(self.invoke_id));
        match &self.result {
            Ok(p) => w.constructed(Tag::ctx_cons(0), |w| {
                p.start_radiation_time.write(w);
                p.stop_radiation_time.write(w);
            }),
            Err(d) => w.constructed(Tag::ctx_cons(1), |w| d.write(w)),
        }
    }

    fn read(r: &mut BerReader) -> BerResult<Self> {
        let credentials = Credentials::read(r)?;
        let invoke_id = r.integer_as(Tag::INTEGER)?;
        let result = match r.peek_tag()? {
            t if t == Tag::ctx_cons(0) => Ok(r.sequence(t, |r| {
                Ok(CltuStartPositive {
                    start_radiation_time: Time::read(r)?,
                    stop_radiation_time: ConditionalTime::read(r)?,
                })
            })?),
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

/// CLTU-TRANSFER-DATA invocation: one CLTU to radiate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CltuTransferDataInvocation {
    /// The credentials of the user.
    pub credentials: Credentials,
    /// The invoke identifier.
    pub invoke_id: InvokeId,
    /// The identification of the CLTU.
    pub cltu_id: CltuId,
    /// Do not radiate before this time.
    pub earliest_transmission_time: ConditionalTime,
    /// Do not radiate after this time.
    pub latest_transmission_time: ConditionalTime,
    /// Minimum delay after the previous CLTU, in microseconds.
    pub delay_time_us: u64,
    /// Whether the user asked for a radiation notification (not produced
    /// by this implementation).
    pub produce_notification: bool,
    /// The CLTU.
    pub data: Bytes,
}

impl CltuTransferDataInvocation {
    fn write(&self, w: &mut BerWriter) {
        self.credentials.write(w);
        w.integer(Tag::INTEGER, i64::from(self.invoke_id));
        w.integer(Tag::INTEGER, i64::from(self.cltu_id));
        self.earliest_transmission_time.write(w);
        self.latest_transmission_time.write(w);
        w.integer(Tag::INTEGER, i64::try_from(self.delay_time_us).unwrap_or(i64::MAX));
        w.integer(Tag::INTEGER, if self.produce_notification { 0 } else { 1 });
        w.octets(Tag::OCTET_STRING, &self.data);
    }

    fn read(r: &mut BerReader) -> BerResult<Self> {
        Ok(Self {
            credentials: Credentials::read(r)?,
            invoke_id: r.integer_as(Tag::INTEGER)?,
            cltu_id: r.integer_as(Tag::INTEGER)?,
            earliest_transmission_time: ConditionalTime::read(r)?,
            latest_transmission_time: ConditionalTime::read(r)?,
            delay_time_us: r.integer_as(Tag::INTEGER)?,
            produce_notification: match r.integer(Tag::INTEGER)? {
                0 => true,
                1 => false,
                _ => return Err(BerError::InvalidValue("sldu radiation notification")),
            },
            data: r.octets(Tag::OCTET_STRING)?,
        })
    }
}

/// CLTU-TRANSFER-DATA return.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CltuTransferDataReturn {
    /// The credentials of the provider.
    pub credentials: Credentials,
    /// The invoke identifier of the invocation.
    pub invoke_id: InvokeId,
    /// Identification of the next CLTU the provider expects.
    pub cltu_id: CltuId,
    /// Free CLTU buffer space in octets.
    pub buffer_available: u64,
    /// `Err` carries a common diagnostic or one of [`transfer_diagnostic`].
    pub result: Result<(), Diagnostic>,
}

impl CltuTransferDataReturn {
    fn write(&self, w: &mut BerWriter) {
        self.credentials.write(w);
        w.integer(Tag::INTEGER, i64::from(self.invoke_id));
        w.integer(Tag::INTEGER, i64::from(self.cltu_id));
        w.integer(Tag::INTEGER, i64::try_from(self.buffer_available).unwrap_or(i64::MAX));
        match &self.result {
            Ok(()) => w.null(Tag::ctx(0)),
            Err(d) => w.constructed(Tag::ctx_cons(1), |w| d.write(w)),
        }
    }

    fn read(r: &mut BerReader) -> BerResult<Self> {
        let credentials = Credentials::read(r)?;
        let invoke_id = r.integer_as(Tag::INTEGER)?;
        let cltu_id = r.integer_as(Tag::INTEGER)?;
        let buffer_available = r.integer_as(Tag::INTEGER)?;
        let result = match r.peek_tag()? {
            t if t == Tag::ctx(0) => Ok(r.null(t)?),
            t if t == Tag::ctx_cons(1) => Err(r.sequence(t, Diagnostic::read)?),
            t => return Err(BerError::UnknownChoice(t)),
        };
        Ok(Self {
            credentials,
            invoke_id,
            cltu_id,
            buffer_available,
            result,
        })
    }
}

/// PDUs sent by the FCLTU user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FcltuUserPdu {
    /// CLTU-BIND invocation.
    Bind(BindInvocation),
    /// CLTU-UNBIND invocation.
    Unbind(UnbindInvocation),
    /// CLTU-START invocation.
    Start(CltuStartInvocation),
    /// CLTU-STOP invocation.
    Stop(StopInvocation),
    /// CLTU-TRANSFER-DATA invocation.
    TransferData(CltuTransferDataInvocation),
}

impl FcltuUserPdu {
    /// Encodes the PDU.
    pub fn encode(&self) -> Bytes {
        let mut w = BerWriter::new();
        match self {
            Self::Bind(v) => w.constructed(TAG_BIND_INVOCATION, |w| v.write(w)),
            Self::Unbind(v) => w.constructed(TAG_UNBIND_INVOCATION, |w| v.write(w)),
            Self::Start(v) => w.constructed(TAG_START_INVOCATION, |w| v.write(w)),
            Self::Stop(v) => w.constructed(TAG_STOP_INVOCATION, |w| v.write(w)),
            Self::TransferData(v) => w.constructed(TAG_TRANSFER_DATA_INVOCATION, |w| v.write(w)),
        }
        w.into_bytes()
    }

    /// Decodes a PDU. GET-PARAMETER, SCHEDULE-STATUS-REPORT and THROW-EVENT
    /// give [`PduError::Unsupported`].
    pub fn decode(pdu: Bytes) -> Result<Self, PduError> {
        let (tag, mut r) = open_pdu(pdu)?;
        let pdu = match tag {
            TAG_BIND_INVOCATION => Self::Bind(BindInvocation::read(&mut r)?),
            TAG_UNBIND_INVOCATION => Self::Unbind(UnbindInvocation::read(&mut r)?),
            TAG_START_INVOCATION => Self::Start(CltuStartInvocation::read(&mut r)?),
            TAG_STOP_INVOCATION => Self::Stop(StopInvocation::read(&mut r)?),
            TAG_TRANSFER_DATA_INVOCATION => Self::TransferData(CltuTransferDataInvocation::read(&mut r)?),
            TAG_SCHEDULE_STATUS_REPORT | TAG_GET_PARAMETER | TAG_THROW_EVENT => {
                return Err(PduError::Unsupported(tag));
            }
            other => return Err(BerError::UnknownChoice(other).into()),
        };
        finish(r, pdu)
    }
}

/// PDUs sent by the FCLTU provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FcltuProviderPdu {
    /// CLTU-BIND return.
    BindReturn(BindReturn),
    /// CLTU-UNBIND return.
    UnbindReturn(UnbindReturn),
    /// CLTU-START return.
    StartReturn(CltuStartReturn),
    /// CLTU-STOP return.
    StopReturn(Acknowledgement),
    /// CLTU-TRANSFER-DATA return.
    TransferDataReturn(CltuTransferDataReturn),
}

impl FcltuProviderPdu {
    /// Encodes the PDU.
    pub fn encode(&self) -> Bytes {
        let mut w = BerWriter::new();
        match self {
            Self::BindReturn(v) => w.constructed(TAG_BIND_RETURN, |w| v.write(w)),
            Self::UnbindReturn(v) => w.constructed(TAG_UNBIND_RETURN, |w| v.write(w)),
            Self::StartReturn(v) => w.constructed(TAG_START_RETURN, |w| v.write(w)),
            Self::StopReturn(v) => w.constructed(TAG_STOP_RETURN, |w| v.write(w)),
            Self::TransferDataReturn(v) => w.constructed(TAG_TRANSFER_DATA_RETURN, |w| v.write(w)),
        }
        w.into_bytes()
    }

    /// Decodes a PDU.
    pub fn decode(pdu: Bytes) -> Result<Self, PduError> {
        let (tag, mut r) = open_pdu(pdu)?;
        let pdu = match tag {
            TAG_BIND_RETURN => Self::BindReturn(BindReturn::read(&mut r)?),
            TAG_UNBIND_RETURN => Self::UnbindReturn(UnbindReturn::read(&mut r)?),
            TAG_START_RETURN => Self::StartReturn(CltuStartReturn::read(&mut r)?),
            TAG_STOP_RETURN => Self::StopReturn(Acknowledgement::read(&mut r)?),
            TAG_TRANSFER_DATA_RETURN => Self::TransferDataReturn(CltuTransferDataReturn::read(&mut r)?),
            other => return Err(BerError::UnknownChoice(other).into()),
        };
        finish(r, pdu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_pdus_roundtrip() {
        let pdus = [
            FcltuUserPdu::Start(CltuStartInvocation {
                credentials: Credentials::Unused,
                invoke_id: 1,
                first_cltu_id: 4_000_000_000,
            }),
            FcltuUserPdu::TransferData(CltuTransferDataInvocation {
                credentials: Credentials::Unused,
                invoke_id: 2,
                cltu_id: 42,
                earliest_transmission_time: ConditionalTime::Undefined,
                latest_transmission_time: ConditionalTime::Known(Time::Ccsds([0; 8])),
                delay_time_us: 1500,
                produce_notification: true,
                data: Bytes::from_static(&[0xeb, 0x90, 1, 2, 3, 4, 5, 6, 7, 0xc5]),
            }),
            FcltuUserPdu::Stop(StopInvocation {
                credentials: Credentials::Unused,
                invoke_id: 3,
            }),
        ];
        for p in pdus {
            assert_eq!(FcltuUserPdu::decode(p.encode()).unwrap(), p);
        }
    }

    #[test]
    fn provider_pdus_roundtrip() {
        let pdus = [
            FcltuProviderPdu::StartReturn(CltuStartReturn {
                credentials: Credentials::Unused,
                invoke_id: 1,
                result: Ok(CltuStartPositive {
                    start_radiation_time: Time::Ccsds([1; 8]),
                    stop_radiation_time: ConditionalTime::Undefined,
                }),
            }),
            FcltuProviderPdu::StartReturn(CltuStartReturn {
                credentials: Credentials::Unused,
                invoke_id: 1,
                result: Err(Diagnostic::Specific(start_diagnostic::OUT_OF_SERVICE)),
            }),
            FcltuProviderPdu::TransferDataReturn(CltuTransferDataReturn {
                credentials: Credentials::Unused,
                invoke_id: 2,
                cltu_id: 43,
                buffer_available: 1 << 20,
                result: Err(Diagnostic::Specific(transfer_diagnostic::OUT_OF_SEQUENCE)),
            }),
            FcltuProviderPdu::StopReturn(Acknowledgement::positive(3)),
        ];
        for p in pdus {
            assert_eq!(FcltuProviderPdu::decode(p.encode()).unwrap(), p);
        }
    }

    #[test]
    fn transfer_data_return_wire_format() {
        let p = FcltuProviderPdu::TransferDataReturn(CltuTransferDataReturn {
            credentials: Credentials::Unused,
            invoke_id: 2,
            cltu_id: 43,
            buffer_available: 4096,
            result: Ok(()),
        });
        assert_eq!(
            &p.encode()[..],
            &[
                0xab, 0x0e, 0x80, 0x00, 0x02, 0x01, 0x02, 0x02, 0x01, 0x2b, 0x02, 0x02, 0x10, 0x00, 0x80, 0x00
            ]
        );
    }

    #[test]
    fn unsupported_operations_are_reported() {
        let mut w = BerWriter::new();
        w.constructed(TAG_THROW_EVENT, |w| w.null(Tag::ctx(0)));
        assert_eq!(
            FcltuUserPdu::decode(w.into_bytes()),
            Err(PduError::Unsupported(TAG_THROW_EVENT))
        );
    }
}
