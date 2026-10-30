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
const TAG_GET_PARAMETER_INVOCATION: Tag = Tag::ctx_cons(6);
const TAG_GET_PARAMETER_RETURN: Tag = Tag::ctx_cons(7);
const TAG_THROW_EVENT: Tag = Tag::ctx_cons(8);
const TAG_TRANSFER_DATA_INVOCATION: Tag = Tag::ctx_cons(10);
const TAG_TRANSFER_DATA_RETURN: Tag = Tag::ctx_cons(11);
const TAG_ASYNC_NOTIFY: Tag = Tag::ctx_cons(12);
const TAG_STATUS_REPORT: Tag = Tag::ctx_cons(13);

/// The newest version of the service, whose encoding [`FcltuProviderPdu::encode`]
/// and [`FcltuProviderPdu::decode`] use.
pub const LATEST_VERSION: u16 = 5;

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

/// CLTU parameter names for CLTU-GET-PARAMETER (`ParameterName`).
pub mod parameter_name {
    /// The identification of the next THROW-EVENT invocation expected.
    pub const EXPECTED_EVENT_INVOCATION_IDENTIFICATION: i64 = 9;
    /// The identification of the next CLTU expected.
    pub const EXPECTED_SLDU_IDENTIFICATION: i64 = 10;
}

/// CLTU-GET-PARAMETER specific diagnostics.
pub mod get_parameter_diagnostic {
    /// The parameter is unknown or not supported.
    pub const UNKNOWN_PARAMETER: i64 = 0;
}

/// CLTU-GET-PARAMETER invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CltuGetParameterInvocation {
    /// The credentials of the user.
    pub credentials: Credentials,
    /// The invoke identifier.
    pub invoke_id: InvokeId,
    /// The parameter asked for, see [`parameter_name`].
    pub parameter: i64,
}

impl CltuGetParameterInvocation {
    fn write(&self, w: &mut BerWriter) {
        self.credentials.write(w);
        w.integer(Tag::INTEGER, i64::from(self.invoke_id));
        w.integer(Tag::INTEGER, self.parameter);
    }

    fn read(r: &mut BerReader) -> BerResult<Self> {
        Ok(Self {
            credentials: Credentials::read(r)?,
            invoke_id: r.integer_as(Tag::INTEGER)?,
            parameter: r.integer(Tag::INTEGER)?,
        })
    }
}

/// A CLTU parameter value in a CLTU-GET-PARAMETER return (the subset this
/// implementation supports).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CltuParameter {
    /// The identification of the next CLTU expected.
    ExpectedSlduIdentification(CltuId),
    /// The identification of the next THROW-EVENT invocation expected.
    ExpectedEventInvocationIdentification(u32),
}

impl CltuParameter {
    /// The tag of the CHOICE alternative, which moved in version 4.
    fn tag(&self, version: u16) -> Tag {
        let number = match (self, version <= 3) {
            (CltuParameter::ExpectedSlduIdentification(_), true) => 1,
            (CltuParameter::ExpectedEventInvocationIdentification(_), true) => 2,
            (CltuParameter::ExpectedSlduIdentification(_), false) => 5,
            (CltuParameter::ExpectedEventInvocationIdentification(_), false) => 6,
        };
        Tag::ctx_cons(number)
    }

    fn write(&self, w: &mut BerWriter, version: u16) {
        let (name, value) = match *self {
            CltuParameter::ExpectedSlduIdentification(id) => (parameter_name::EXPECTED_SLDU_IDENTIFICATION, id),
            CltuParameter::ExpectedEventInvocationIdentification(id) => {
                (parameter_name::EXPECTED_EVENT_INVOCATION_IDENTIFICATION, id)
            }
        };
        w.constructed(self.tag(version), |w| {
            w.integer(Tag::INTEGER, name);
            w.integer(Tag::INTEGER, i64::from(value));
        });
    }

    fn read(r: &mut BerReader, version: u16) -> BerResult<Self> {
        let tag = r.peek_tag()?;
        let candidates = [
            CltuParameter::ExpectedSlduIdentification(0),
            CltuParameter::ExpectedEventInvocationIdentification(0),
        ];
        let Some(kind) = candidates.into_iter().find(|c| c.tag(version) == tag) else {
            return Err(BerError::UnknownChoice(tag));
        };
        r.sequence(tag, |r| {
            let _name = r.integer(Tag::INTEGER)?;
            let value = r.integer_as(Tag::INTEGER)?;
            Ok(match kind {
                CltuParameter::ExpectedSlduIdentification(_) => CltuParameter::ExpectedSlduIdentification(value),
                CltuParameter::ExpectedEventInvocationIdentification(_) => {
                    CltuParameter::ExpectedEventInvocationIdentification(value)
                }
            })
        })
    }
}

/// CLTU-GET-PARAMETER return. Its encoding depends on the version of the
/// service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CltuGetParameterReturn {
    /// The credentials of the provider.
    pub credentials: Credentials,
    /// The invoke identifier of the invocation.
    pub invoke_id: InvokeId,
    /// `Err` carries a common diagnostic or one of
    /// [`get_parameter_diagnostic`].
    pub result: Result<CltuParameter, Diagnostic>,
}

impl CltuGetParameterReturn {
    fn write(&self, w: &mut BerWriter, version: u16) {
        self.credentials.write(w);
        w.integer(Tag::INTEGER, i64::from(self.invoke_id));
        match &self.result {
            Ok(p) => w.constructed(Tag::ctx_cons(0), |w| p.write(w, version)),
            Err(d) => w.constructed(Tag::ctx_cons(1), |w| d.write(w)),
        }
    }

    fn read(r: &mut BerReader, version: u16) -> BerResult<Self> {
        let credentials = Credentials::read(r)?;
        let invoke_id = r.integer_as(Tag::INTEGER)?;
        let result = match r.peek_tag()? {
            t if t == Tag::ctx_cons(0) => Ok(r.sequence(t, |r| CltuParameter::read(r, version))?),
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

/// What happened to a CLTU (`ForwardDuStatus`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CltuStatus {
    /// Radiated completely.
    Radiated = 0,
    /// Not radiated before its latest transmission time.
    Expired = 1,
    /// Radiation was interrupted.
    Interrupted = 2,
    /// Radiation has started.
    ProductionStarted = 4,
    /// Radiation has not started.
    ProductionNotStarted = 5,
}

impl CltuStatus {
    fn from_i64(v: i64) -> BerResult<Self> {
        match v {
            0 => Ok(Self::Radiated),
            1 => Ok(Self::Expired),
            2 => Ok(Self::Interrupted),
            4 => Ok(Self::ProductionStarted),
            5 => Ok(Self::ProductionNotStarted),
            _ => Err(BerError::InvalidValue("CLTU status")),
        }
    }
}

/// The CLTU whose radiation started last, if any.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CltuLastProcessed {
    /// No CLTU processed yet.
    None,
    /// The CLTU and how far it got.
    Processed {
        /// Its identification.
        cltu_id: CltuId,
        /// When its radiation started, if it did.
        start_radiation_time: ConditionalTime,
        /// Its status.
        status: CltuStatus,
    },
}

impl CltuLastProcessed {
    fn write(&self, w: &mut BerWriter) {
        match self {
            CltuLastProcessed::None => w.null(Tag::ctx(0)),
            CltuLastProcessed::Processed {
                cltu_id,
                start_radiation_time,
                status,
            } => w.constructed(Tag::ctx_cons(1), |w| {
                w.integer(Tag::INTEGER, i64::from(*cltu_id));
                start_radiation_time.write(w);
                w.integer(Tag::INTEGER, *status as i64);
            }),
        }
    }

    fn read(r: &mut BerReader) -> BerResult<Self> {
        match r.peek_tag()? {
            t if t == Tag::ctx(0) => r.null(t).map(|_| CltuLastProcessed::None),
            t if t == Tag::ctx_cons(1) => r.sequence(t, |r| {
                Ok(CltuLastProcessed::Processed {
                    cltu_id: r.integer_as(Tag::INTEGER)?,
                    start_radiation_time: ConditionalTime::read(r)?,
                    status: CltuStatus::from_i64(r.integer(Tag::INTEGER)?)?,
                })
            }),
            t => Err(BerError::UnknownChoice(t)),
        }
    }
}

/// The CLTU radiated completely last, if any.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CltuLastOk {
    /// No CLTU radiated yet.
    None,
    /// The CLTU and when its radiation ended.
    Ok {
        /// Its identification.
        cltu_id: CltuId,
        /// When its radiation ended.
        stop_radiation_time: Time,
    },
}

impl CltuLastOk {
    fn write(&self, w: &mut BerWriter) {
        match self {
            CltuLastOk::None => w.null(Tag::ctx(0)),
            CltuLastOk::Ok {
                cltu_id,
                stop_radiation_time,
            } => w.constructed(Tag::ctx_cons(1), |w| {
                w.integer(Tag::INTEGER, i64::from(*cltu_id));
                stop_radiation_time.write(w);
            }),
        }
    }

    fn read(r: &mut BerReader) -> BerResult<Self> {
        match r.peek_tag()? {
            t if t == Tag::ctx(0) => r.null(t).map(|_| CltuLastOk::None),
            t if t == Tag::ctx_cons(1) => r.sequence(t, |r| {
                Ok(CltuLastOk::Ok {
                    cltu_id: r.integer_as(Tag::INTEGER)?,
                    stop_radiation_time: Time::read(r)?,
                })
            }),
            t => Err(BerError::UnknownChoice(t)),
        }
    }
}

/// The production status of an FCLTU service instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CltuProductionStatus {
    /// CLTUs are radiated.
    Operational = 0,
    /// Configured, but not radiating.
    Configured = 1,
    /// Radiation is interrupted.
    Interrupted = 2,
    /// Radiation is halted.
    Halted = 3,
}

impl CltuProductionStatus {
    fn from_i64(v: i64) -> BerResult<Self> {
        match v {
            0 => Ok(Self::Operational),
            1 => Ok(Self::Configured),
            2 => Ok(Self::Interrupted),
            3 => Ok(Self::Halted),
            _ => Err(BerError::InvalidValue("CLTU production status")),
        }
    }
}

/// The status of the uplink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UplinkStatus {
    /// Not known to the provider.
    NotAvailable = 0,
    /// No RF.
    NoRfAvailable = 1,
    /// The spacecraft has no bit lock.
    NoBitLock = 2,
    /// Nominal.
    Nominal = 3,
}

impl UplinkStatus {
    fn from_i64(v: i64) -> BerResult<Self> {
        match v {
            0 => Ok(Self::NotAvailable),
            1 => Ok(Self::NoRfAvailable),
            2 => Ok(Self::NoBitLock),
            3 => Ok(Self::Nominal),
            _ => Err(BerError::InvalidValue("uplink status")),
        }
    }
}

/// What a CLTU-ASYNC-NOTIFY reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CltuNotification {
    /// A CLTU with radiation notification requested was radiated.
    CltuRadiated,
    /// A CLTU expired.
    SlduExpired,
    /// Production was interrupted.
    ProductionInterrupted,
    /// Production was halted.
    ProductionHalted,
    /// Production is operational again.
    ProductionOperational,
    /// The CLTU buffer is empty.
    BufferEmpty,
    /// The action list of the event invocation completed.
    ActionListCompleted(u32),
    /// The action list of the event invocation did not complete.
    ActionListNotCompleted(u32),
    /// The event condition of the event invocation evaluated to false.
    EventConditionEvFalse(u32),
}

impl CltuNotification {
    fn write(&self, w: &mut BerWriter) {
        match *self {
            CltuNotification::CltuRadiated => w.null(Tag::ctx(0)),
            CltuNotification::SlduExpired => w.null(Tag::ctx(1)),
            CltuNotification::ProductionInterrupted => w.null(Tag::ctx(2)),
            CltuNotification::ProductionHalted => w.null(Tag::ctx(3)),
            CltuNotification::ProductionOperational => w.null(Tag::ctx(4)),
            CltuNotification::BufferEmpty => w.null(Tag::ctx(5)),
            CltuNotification::ActionListCompleted(id) => w.integer(Tag::ctx(6), i64::from(id)),
            CltuNotification::ActionListNotCompleted(id) => w.integer(Tag::ctx(7), i64::from(id)),
            CltuNotification::EventConditionEvFalse(id) => w.integer(Tag::ctx(8), i64::from(id)),
        }
    }

    fn read(r: &mut BerReader) -> BerResult<Self> {
        let tag = r.peek_tag()?;
        if tag.class != super::ber::Class::Context || tag.constructed {
            return Err(BerError::UnknownChoice(tag));
        }
        Ok(match tag.number {
            0 => r.null(tag).map(|_| CltuNotification::CltuRadiated)?,
            1 => r.null(tag).map(|_| CltuNotification::SlduExpired)?,
            2 => r.null(tag).map(|_| CltuNotification::ProductionInterrupted)?,
            3 => r.null(tag).map(|_| CltuNotification::ProductionHalted)?,
            4 => r.null(tag).map(|_| CltuNotification::ProductionOperational)?,
            5 => r.null(tag).map(|_| CltuNotification::BufferEmpty)?,
            6 => CltuNotification::ActionListCompleted(r.integer_as(tag)?),
            7 => CltuNotification::ActionListNotCompleted(r.integer_as(tag)?),
            8 => CltuNotification::EventConditionEvFalse(r.integer_as(tag)?),
            _ => return Err(BerError::UnknownChoice(tag)),
        })
    }
}

/// CLTU-ASYNC-NOTIFY invocation, sent by the provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CltuAsyncNotify {
    /// The credentials of the provider.
    pub credentials: Credentials,
    /// What happened.
    pub notification: CltuNotification,
    /// The CLTU processed last.
    pub last_processed: CltuLastProcessed,
    /// The CLTU radiated last.
    pub last_ok: CltuLastOk,
    /// The production status.
    pub production_status: CltuProductionStatus,
    /// The uplink status.
    pub uplink_status: UplinkStatus,
}

impl CltuAsyncNotify {
    fn write(&self, w: &mut BerWriter) {
        self.credentials.write(w);
        self.notification.write(w);
        self.last_processed.write(w);
        self.last_ok.write(w);
        w.integer(Tag::INTEGER, self.production_status as i64);
        w.integer(Tag::INTEGER, self.uplink_status as i64);
    }

    fn read(r: &mut BerReader) -> BerResult<Self> {
        Ok(Self {
            credentials: Credentials::read(r)?,
            notification: CltuNotification::read(r)?,
            last_processed: CltuLastProcessed::read(r)?,
            last_ok: CltuLastOk::read(r)?,
            production_status: CltuProductionStatus::from_i64(r.integer(Tag::INTEGER)?)?,
            uplink_status: UplinkStatus::from_i64(r.integer(Tag::INTEGER)?)?,
        })
    }
}

/// CLTU-STATUS-REPORT invocation, sent by the provider after a
/// SCHEDULE-STATUS-REPORT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CltuStatusReport {
    /// The credentials of the provider.
    pub credentials: Credentials,
    /// The CLTU processed last.
    pub last_processed: CltuLastProcessed,
    /// The CLTU radiated last.
    pub last_ok: CltuLastOk,
    /// The production status.
    pub production_status: CltuProductionStatus,
    /// The uplink status.
    pub uplink_status: UplinkStatus,
    /// CLTUs received.
    pub cltus_received: u32,
    /// CLTUs whose radiation started.
    pub cltus_processed: u32,
    /// CLTUs radiated.
    pub cltus_radiated: u32,
    /// Free CLTU buffer space in octets.
    pub buffer_available: u64,
}

impl CltuStatusReport {
    fn write(&self, w: &mut BerWriter) {
        self.credentials.write(w);
        self.last_processed.write(w);
        self.last_ok.write(w);
        w.integer(Tag::INTEGER, self.production_status as i64);
        w.integer(Tag::INTEGER, self.uplink_status as i64);
        w.integer(Tag::INTEGER, i64::from(self.cltus_received));
        w.integer(Tag::INTEGER, i64::from(self.cltus_processed));
        w.integer(Tag::INTEGER, i64::from(self.cltus_radiated));
        w.integer(Tag::INTEGER, i64::try_from(self.buffer_available).unwrap_or(i64::MAX));
    }

    fn read(r: &mut BerReader) -> BerResult<Self> {
        Ok(Self {
            credentials: Credentials::read(r)?,
            last_processed: CltuLastProcessed::read(r)?,
            last_ok: CltuLastOk::read(r)?,
            production_status: CltuProductionStatus::from_i64(r.integer(Tag::INTEGER)?)?,
            uplink_status: UplinkStatus::from_i64(r.integer(Tag::INTEGER)?)?,
            cltus_received: r.integer_as(Tag::INTEGER)?,
            cltus_processed: r.integer_as(Tag::INTEGER)?,
            cltus_radiated: r.integer_as(Tag::INTEGER)?,
            buffer_available: r.integer_as(Tag::INTEGER)?,
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
    /// CLTU-SCHEDULE-STATUS-REPORT invocation.
    ScheduleStatusReport(ScheduleStatusReportInvocation),
    /// CLTU-GET-PARAMETER invocation.
    GetParameter(CltuGetParameterInvocation),
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
            Self::ScheduleStatusReport(v) => w.constructed(TAG_SCHEDULE_STATUS_REPORT_INVOCATION, |w| v.write(w)),
            Self::GetParameter(v) => w.constructed(TAG_GET_PARAMETER_INVOCATION, |w| v.write(w)),
        }
        w.into_bytes()
    }

    /// Decodes a PDU. THROW-EVENT gives [`PduError::Unsupported`].
    pub fn decode(pdu: Bytes) -> Result<Self, PduError> {
        let (tag, mut r) = open_pdu(pdu)?;
        let pdu = match tag {
            TAG_BIND_INVOCATION => Self::Bind(BindInvocation::read(&mut r)?),
            TAG_UNBIND_INVOCATION => Self::Unbind(UnbindInvocation::read(&mut r)?),
            TAG_START_INVOCATION => Self::Start(CltuStartInvocation::read(&mut r)?),
            TAG_STOP_INVOCATION => Self::Stop(StopInvocation::read(&mut r)?),
            TAG_TRANSFER_DATA_INVOCATION => Self::TransferData(CltuTransferDataInvocation::read(&mut r)?),
            TAG_SCHEDULE_STATUS_REPORT_INVOCATION => {
                Self::ScheduleStatusReport(ScheduleStatusReportInvocation::read(&mut r)?)
            }
            TAG_GET_PARAMETER_INVOCATION => Self::GetParameter(CltuGetParameterInvocation::read(&mut r)?),
            TAG_THROW_EVENT => return Err(PduError::Unsupported(tag)),
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
    /// CLTU-SCHEDULE-STATUS-REPORT return.
    ScheduleStatusReportReturn(ScheduleStatusReportReturn),
    /// CLTU-GET-PARAMETER return.
    GetParameterReturn(CltuGetParameterReturn),
    /// CLTU-ASYNC-NOTIFY invocation.
    AsyncNotify(CltuAsyncNotify),
    /// CLTU-STATUS-REPORT invocation.
    StatusReport(CltuStatusReport),
}

impl FcltuProviderPdu {
    /// Encodes the PDU for the [latest version](LATEST_VERSION).
    pub fn encode(&self) -> Bytes {
        self.encode_version(LATEST_VERSION)
    }

    /// Encodes the PDU for `version` of the service. Only the
    /// CLTU-GET-PARAMETER return differs between versions.
    pub fn encode_version(&self, version: u16) -> Bytes {
        let mut w = BerWriter::new();
        match self {
            Self::BindReturn(v) => w.constructed(TAG_BIND_RETURN, |w| v.write(w)),
            Self::UnbindReturn(v) => w.constructed(TAG_UNBIND_RETURN, |w| v.write(w)),
            Self::StartReturn(v) => w.constructed(TAG_START_RETURN, |w| v.write(w)),
            Self::StopReturn(v) => w.constructed(TAG_STOP_RETURN, |w| v.write(w)),
            Self::TransferDataReturn(v) => w.constructed(TAG_TRANSFER_DATA_RETURN, |w| v.write(w)),
            Self::ScheduleStatusReportReturn(v) => w.constructed(TAG_SCHEDULE_STATUS_REPORT_RETURN, |w| v.write(w)),
            Self::GetParameterReturn(v) => w.constructed(TAG_GET_PARAMETER_RETURN, |w| v.write(w, version)),
            Self::AsyncNotify(v) => w.constructed(TAG_ASYNC_NOTIFY, |w| v.write(w)),
            Self::StatusReport(v) => w.constructed(TAG_STATUS_REPORT, |w| v.write(w)),
        }
        w.into_bytes()
    }

    /// Decodes a PDU of the [latest version](LATEST_VERSION).
    pub fn decode(pdu: Bytes) -> Result<Self, PduError> {
        Self::decode_version(pdu, LATEST_VERSION)
    }

    /// Decodes a PDU of `version` of the service.
    pub fn decode_version(pdu: Bytes, version: u16) -> Result<Self, PduError> {
        let (tag, mut r) = open_pdu(pdu)?;
        let pdu = match tag {
            TAG_BIND_RETURN => Self::BindReturn(BindReturn::read(&mut r)?),
            TAG_UNBIND_RETURN => Self::UnbindReturn(UnbindReturn::read(&mut r)?),
            TAG_START_RETURN => Self::StartReturn(CltuStartReturn::read(&mut r)?),
            TAG_STOP_RETURN => Self::StopReturn(Acknowledgement::read(&mut r)?),
            TAG_TRANSFER_DATA_RETURN => Self::TransferDataReturn(CltuTransferDataReturn::read(&mut r)?),
            TAG_SCHEDULE_STATUS_REPORT_RETURN => {
                Self::ScheduleStatusReportReturn(ScheduleStatusReportReturn::read(&mut r)?)
            }
            TAG_GET_PARAMETER_RETURN => Self::GetParameterReturn(CltuGetParameterReturn::read(&mut r, version)?),
            TAG_ASYNC_NOTIFY => Self::AsyncNotify(CltuAsyncNotify::read(&mut r)?),
            TAG_STATUS_REPORT => Self::StatusReport(CltuStatusReport::read(&mut r)?),
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
    fn negative_diagnostics_are_tagged_0_and_1() {
        // [11] { unused, invoke 2, CLTU 43, buffer 0, negative [1] { specific [1] outOfSequence } }
        let p = FcltuProviderPdu::TransferDataReturn(CltuTransferDataReturn {
            credentials: Credentials::Unused,
            invoke_id: 2,
            cltu_id: 43,
            buffer_available: 0,
            result: Err(Diagnostic::Specific(transfer_diagnostic::OUT_OF_SEQUENCE)),
        });
        assert_eq!(
            &p.encode()[..],
            &[
                0xab, 0x10, 0x80, 0x00, 0x02, 0x01, 0x02, 0x02, 0x01, 0x2b, 0x02, 0x01, 0x00, 0xa1, 0x03, 0x81, 0x01,
                0x02
            ]
        );
    }

    #[test]
    fn get_parameter_return_depends_on_the_version() {
        let p = FcltuProviderPdu::GetParameterReturn(CltuGetParameterReturn {
            credentials: Credentials::Unused,
            invoke_id: 3,
            result: Ok(CltuParameter::ExpectedSlduIdentification(7)),
        });
        // [7] { unused, invoke 3, positive [0] { parCltuIdentification [5] { 10, 7 } } }
        let v5 = p.encode_version(5);
        #[rustfmt::skip]
        assert_eq!(
            &v5[..],
            &[0xa7, 0x0f, 0x80, 0x00, 0x02, 0x01, 0x03, 0xa0, 0x08, 0xa5, 0x06, 0x02, 0x01, 0x0a, 0x02, 0x01, 0x07]
        );
        let v3 = p.encode_version(3);
        assert_eq!(v3[9], 0xa1, "parCltuIdentification is [1] in versions 1 to 3");
        assert_eq!(FcltuProviderPdu::decode_version(v3, 3).unwrap(), p);
        assert_eq!(FcltuProviderPdu::decode_version(v5, 4).unwrap(), p);

        let get = FcltuUserPdu::GetParameter(CltuGetParameterInvocation {
            credentials: Credentials::Unused,
            invoke_id: 3,
            parameter: parameter_name::EXPECTED_SLDU_IDENTIFICATION,
        });
        assert_eq!(
            &get.encode()[..],
            &[0xa6, 0x08, 0x80, 0x00, 0x02, 0x01, 0x03, 0x02, 0x01, 0x0a]
        );
    }

    #[test]
    fn async_notify_and_status_report_wire_format() {
        let time = Time::Ccsds([0, 1, 2, 3, 4, 5, 6, 7]);
        let notify = FcltuProviderPdu::AsyncNotify(CltuAsyncNotify {
            credentials: Credentials::Unused,
            notification: CltuNotification::CltuRadiated,
            last_processed: CltuLastProcessed::Processed {
                cltu_id: 5,
                start_radiation_time: ConditionalTime::Known(time),
                status: CltuStatus::Radiated,
            },
            last_ok: CltuLastOk::Ok {
                cltu_id: 5,
                stop_radiation_time: time,
            },
            production_status: CltuProductionStatus::Operational,
            uplink_status: UplinkStatus::Nominal,
        });
        #[rustfmt::skip]
        let expected = [
            0xac, 0x2d,
            0x80, 0x00,                                     // credentials unused
            0x80, 0x00,                                     // cltuRadiated [0] NULL
            0xa1, 0x12, 0x02, 0x01, 0x05,                   // cltuProcessed [1] { id 5,
            0xa1, 0x0a, 0x80, 0x08, 0, 1, 2, 3, 4, 5, 6, 7, //   known [1] { ccsds [0] time },
            0x02, 0x01, 0x00,                               //   radiated }
            0xa1, 0x0d, 0x02, 0x01, 0x05,                   // cltuOk [1] { id 5,
            0x80, 0x08, 0, 1, 2, 3, 4, 5, 6, 7,             //   ccsds [0] time }
            0x02, 0x01, 0x00,                               // operational
            0x02, 0x01, 0x03,                               // nominal
        ];
        assert_eq!(&notify.encode()[..], &expected);
        assert_eq!(FcltuProviderPdu::decode(notify.encode()).unwrap(), notify);

        let report = FcltuProviderPdu::StatusReport(CltuStatusReport {
            credentials: Credentials::Unused,
            last_processed: CltuLastProcessed::None,
            last_ok: CltuLastOk::None,
            production_status: CltuProductionStatus::Operational,
            uplink_status: UplinkStatus::Nominal,
            cltus_received: 0,
            cltus_processed: 0,
            cltus_radiated: 0,
            buffer_available: 262_144,
        });
        let encoded = report.encode();
        assert_eq!(&encoded[..8], &[0xad, 0x1a, 0x80, 0x00, 0x80, 0x00, 0x80, 0x00]);
        assert_eq!(FcltuProviderPdu::decode(encoded).unwrap(), report);
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
