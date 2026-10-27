//! SLE types shared by all transfer services: the ASN.1 modules
//! CCSDS-SLE-TRANSFER-SERVICE-COMMON-TYPES, -COMMON-PDUS and -BIND-TYPES
//! (CCSDS 911.1-B-4 and 912.1-B-4, annex A), and the PEER-ABORT
//! diagnostics of ISP1 (CCSDS 913.1-B-2).
//!
//! The PDU modules use IMPLICIT TAGS; CHOICE types inside a tagged field
//! are therefore encoded with an explicit (constructed) tag.
//!
//! ```
//! use groundlink::ServiceInstanceId;
//!
//! let si: ServiceInstanceId = "sagr=1.spack=VST-PASS0001.rsl-fg=1.raf=onlt1".parse().unwrap();
//! assert_eq!(si.attributes().count(), 4);
//! assert_eq!(si.to_string(), "sagr=1.spack=VST-PASS0001.rsl-fg=1.raf=onlt1");
//! ```

use std::fmt;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use super::ber::{BerError, BerReader, BerResult, BerWriter, Tag};

/// The invoke identifier of a confirmed operation.
pub type InvokeId = u16;

/// `ApplicationIdentifier` values (service types) used in BIND.
pub mod service_type {
    /// Return All Frames (RAF).
    pub const RTN_ALL_FRAMES: i64 = 0;
    /// Return Channel Frames (RCF).
    pub const RTN_CH_FRAMES: i64 = 2;
    /// Return Operational Control Field (ROCF).
    pub const RTN_CH_OCF: i64 = 4;
    /// Forward CLTU (FCLTU).
    pub const FWD_CLTU: i64 = 16;
}

/// `BindDiagnostic` values of a negative BIND return.
pub mod bind_diagnostic {
    /// The initiator is not allowed to bind.
    pub const ACCESS_DENIED: i64 = 0;
    /// The responder does not support the service type.
    pub const SERVICE_TYPE_NOT_SUPPORTED: i64 = 1;
    /// The responder does not support the requested version.
    pub const VERSION_NOT_SUPPORTED: i64 = 2;
    /// The service instance is unknown.
    pub const NO_SUCH_SERVICE_INSTANCE: i64 = 3;
    /// The service instance is bound already.
    pub const ALREADY_BOUND: i64 = 4;
    /// The service instance is not accessible to this initiator.
    pub const SI_NOT_ACCESSIBLE_TO_THIS_INITIATOR: i64 = 5;
    /// The service type does not match the service instance.
    pub const INCONSISTENT_SERVICE_TYPE: i64 = 6;
    /// Outside the provision period of the service instance.
    pub const INVALID_TIME: i64 = 7;
    /// The service instance is out of service.
    pub const OUT_OF_SERVICE: i64 = 8;
    /// Any other reason.
    pub const OTHER_REASON: i64 = 127;
}

/// `UnbindReason` values.
pub mod unbind_reason {
    /// The user has finished with the service instance.
    pub const END: i64 = 0;
    /// The user will bind again later in the provision period.
    pub const SUSPEND: i64 = 1;
    /// The version is not supported.
    pub const VERSION_NOT_SUPPORTED: i64 = 2;
    /// Any other reason.
    pub const OTHER: i64 = 127;
}

/// `PeerAbortDiagnostic` values, sent with
/// [`SlePeerAbort`](crate::SlePeerAbort) (CCSDS 913.1-B-2 sends them as TCP
/// urgent data).
pub mod peer_abort {
    /// Access denied.
    pub const ACCESS_DENIED: u8 = 0;
    /// The responder identifier was not the expected one.
    pub const UNEXPECTED_RESPONDER_ID: u8 = 1;
    /// An operational requirement.
    pub const OPERATIONAL_REQUIREMENT: u8 = 2;
    /// A PDU that is not allowed in the current state.
    pub const PROTOCOL_ERROR: u8 = 3;
    /// The underlying communication failed.
    pub const COMMUNICATIONS_FAILURE: u8 = 4;
    /// A PDU could not be decoded.
    pub const ENCODING_ERROR: u8 = 5;
    /// A return did not arrive in time.
    pub const RETURN_TIMEOUT: u8 = 6;
    /// The provision period of the service instance has ended.
    pub const END_OF_SERVICE_PROVISION_PERIOD: u8 = 7;
    /// A return with an invoke identifier that has no invocation.
    pub const UNSOLICITED_INVOKE_ID: u8 = 8;
    /// Any other reason.
    pub const OTHER_REASON: u8 = 127;
}

/// Common `Diagnostics` values of a negative return.
pub mod diagnostics {
    /// The invoke identifier is in use already.
    pub const DUPLICATE_INVOKE_ID: i64 = 100;
    /// Any other reason.
    pub const OTHER_REASON: i64 = 127;
}

pub(crate) const TAG_BIND_INVOCATION: Tag = Tag::ctx_cons(100);
pub(crate) const TAG_BIND_RETURN: Tag = Tag::ctx_cons(101);
pub(crate) const TAG_UNBIND_INVOCATION: Tag = Tag::ctx_cons(102);
pub(crate) const TAG_UNBIND_RETURN: Tag = Tag::ctx_cons(103);

// ---------------------------------------------------------------------------
// PDU errors
// ---------------------------------------------------------------------------

/// Why an SLE PDU could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PduError {
    /// The PDU is not valid BER, or not a valid PDU of the service.
    Ber(BerError),
    /// A valid operation of the service that this implementation does not
    /// support (e.g. GET-PARAMETER); carries the tag of the PDU.
    Unsupported(Tag),
}

impl fmt::Display for PduError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PduError::Ber(e) => write!(f, "{e}"),
            PduError::Unsupported(t) => write!(f, "unsupported SLE operation {t}"),
        }
    }
}

impl std::error::Error for PduError {}

impl From<BerError> for PduError {
    fn from(e: BerError) -> Self {
        PduError::Ber(e)
    }
}

/// Splits a PDU into its outer tag and a reader over its content.
pub(crate) fn open_pdu(pdu: Bytes) -> Result<(Tag, BerReader), PduError> {
    let mut r = BerReader::new(pdu);
    let (tag, content) = r.read_any()?;
    r.finish()?;
    Ok((tag, BerReader::new(content)))
}

pub(crate) fn finish<T>(r: BerReader, value: T) -> Result<T, PduError> {
    r.finish()?;
    Ok(value)
}

// ---------------------------------------------------------------------------
// Credentials, Time
// ---------------------------------------------------------------------------

/// The credentials of an invocation or return.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Credentials {
    /// No credentials.
    #[default]
    Unused,
    /// ISP1 credentials (time, random number, hash); not evaluated here.
    Used(Bytes),
}

impl Credentials {
    pub(crate) fn write(&self, w: &mut BerWriter) {
        match self {
            Credentials::Unused => w.null(Tag::ctx(0)),
            Credentials::Used(b) => w.octets(Tag::ctx(1), b),
        }
    }

    pub(crate) fn read(r: &mut BerReader) -> BerResult<Self> {
        match r.peek_tag()? {
            t if t == Tag::ctx(0) => r.null(t).map(|_| Credentials::Unused),
            t if t == Tag::ctx(1) => r.octets(t).map(Credentials::Used),
            t => Err(BerError::UnknownChoice(t)),
        }
    }
}

/// A time in the CCSDS Day Segmented time code (CCSDS 301.0-B-4): 16-bit
/// day since 1958-01-01, 32-bit millisecond of day and 16-bit microsecond
/// of millisecond, or 32-bit picosecond of millisecond in the picosecond
/// format.
///
/// Times compare by their value, also across the two formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Time {
    /// 8 octets, microsecond resolution.
    Ccsds([u8; 8]),
    /// 10 octets, picosecond resolution.
    CcsdsPico([u8; 10]),
}

/// Days from 1958-01-01 to 1970-01-01.
const DAYS_1958_TO_1970: u64 = 4383;

impl Time {
    /// The time `t` in microsecond resolution (leap seconds are not
    /// accounted for).
    pub fn from_system_time(t: SystemTime) -> Self {
        let since = t.duration_since(UNIX_EPOCH).unwrap_or_default();
        let secs = since.as_secs();
        let day = (secs / 86_400 + DAYS_1958_TO_1970) as u16;
        let ms_of_day = ((secs % 86_400) * 1000 + u64::from(since.subsec_millis())) as u32;
        let us_of_ms = (since.subsec_micros() % 1000) as u16;
        let mut b = [0u8; 8];
        b[0..2].copy_from_slice(&day.to_be_bytes());
        b[2..6].copy_from_slice(&ms_of_day.to_be_bytes());
        b[6..8].copy_from_slice(&us_of_ms.to_be_bytes());
        Time::Ccsds(b)
    }

    /// The current time.
    pub fn now() -> Self {
        Self::from_system_time(SystemTime::now())
    }

    /// (day, millisecond of day, picosecond of millisecond), for
    /// comparisons.
    pub fn components(&self) -> (u16, u32, u64) {
        match self {
            Time::Ccsds(b) => (
                u16::from_be_bytes([b[0], b[1]]),
                u32::from_be_bytes([b[2], b[3], b[4], b[5]]),
                u64::from(u16::from_be_bytes([b[6], b[7]])) * 1_000_000,
            ),
            Time::CcsdsPico(b) => (
                u16::from_be_bytes([b[0], b[1]]),
                u32::from_be_bytes([b[2], b[3], b[4], b[5]]),
                u64::from(u32::from_be_bytes([b[6], b[7], b[8], b[9]])),
            ),
        }
    }

    pub(crate) fn write(&self, w: &mut BerWriter) {
        match self {
            Time::Ccsds(b) => w.octets(Tag::ctx(0), b),
            Time::CcsdsPico(b) => w.octets(Tag::ctx(1), b),
        }
    }

    pub(crate) fn read(r: &mut BerReader) -> BerResult<Self> {
        match r.peek_tag()? {
            t if t == Tag::ctx(0) => r
                .octets(t)?
                .as_ref()
                .try_into()
                .map(Time::Ccsds)
                .map_err(|_| BerError::InvalidValue("CCSDS time must have 8 octets")),
            t if t == Tag::ctx(1) => r
                .octets(t)?
                .as_ref()
                .try_into()
                .map(Time::CcsdsPico)
                .map_err(|_| BerError::InvalidValue("CCSDS pico time must have 10 octets")),
            t => Err(BerError::UnknownChoice(t)),
        }
    }
}

impl PartialOrd for Time {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.components().cmp(&other.components()))
    }
}

/// A time that may be left undefined, e.g. the start time of RAF-START.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConditionalTime {
    /// No time given.
    #[default]
    Undefined,
    /// The time given.
    Known(Time),
}

impl ConditionalTime {
    pub(crate) fn write(&self, w: &mut BerWriter) {
        match self {
            ConditionalTime::Undefined => w.null(Tag::ctx(0)),
            ConditionalTime::Known(t) => w.constructed(Tag::ctx_cons(1), |w| t.write(w)),
        }
    }

    pub(crate) fn read(r: &mut BerReader) -> BerResult<Self> {
        match r.peek_tag()? {
            t if t == Tag::ctx(0) => r.null(t).map(|_| ConditionalTime::Undefined),
            t if t == Tag::ctx_cons(1) => r.sequence(t, Time::read).map(ConditionalTime::Known),
            t => Err(BerError::UnknownChoice(t)),
        }
    }
}

// ---------------------------------------------------------------------------
// Service instance identifier
// ---------------------------------------------------------------------------

/// Arc of all SLE service instance attribute OIDs: {1 3 112 4 3 1 2}.
const SI_OID_PREFIX: [u32; 7] = [1, 3, 112, 4, 3, 1, 2];

/// Known service instance attributes and their final OID arc.
const SI_ATTRIBUTES: &[(&str, u32)] = &[
    ("sagr", 52),
    ("spack", 53),
    ("rsl-fg", 38),
    ("fsl-fg", 14),
    ("raf", 22),
    ("rcf", 46),
    ("rocf", 49),
    ("cltu", 7),
];

/// A text that is not a valid [`ServiceInstanceId`]; carries what is
/// wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SiIdError(pub String);

impl fmt::Display for SiIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid service instance identifier: {}", self.0)
    }
}

impl std::error::Error for SiIdError {}

/// Service instance identifier such as
/// `sagr=1.spack=VST-PASS0001.rsl-fg=1.raf=onlt1`.
///
/// Parsed from and displayed as this text form. Known attributes are
/// `sagr`, `spack`, `rsl-fg`, `fsl-fg`, `raf`, `rcf`, `rocf` and `cltu`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServiceInstanceId {
    attributes: Vec<(&'static str, String)>,
}

impl ServiceInstanceId {
    /// The attributes as (name, value), in their order.
    pub fn attributes(&self) -> impl Iterator<Item = (&str, &str)> {
        self.attributes.iter().map(|(n, v)| (*n, v.as_str()))
    }

    fn attribute_name(arc: u32) -> Option<&'static str> {
        SI_ATTRIBUTES.iter().find(|(_, a)| *a == arc).map(|(n, _)| *n)
    }

    fn attribute_arc(name: &str) -> Option<(&'static str, u32)> {
        SI_ATTRIBUTES.iter().find(|(n, _)| *n == name).copied()
    }

    pub(crate) fn write(&self, w: &mut BerWriter) {
        w.constructed(Tag::SEQUENCE, |w| {
            for (name, value) in &self.attributes {
                let (_, arc) = Self::attribute_arc(name).expect("validated on construction");
                let mut oid = SI_OID_PREFIX.to_vec();
                oid.push(arc);
                w.constructed(Tag::SET, |w| {
                    w.constructed(Tag::SEQUENCE, |w| {
                        w.oid(Tag::OID, &oid);
                        w.visible_string(Tag::VISIBLE_STRING, value);
                    })
                });
            }
        });
    }

    pub(crate) fn read(r: &mut BerReader) -> BerResult<Self> {
        r.sequence(Tag::SEQUENCE, |r| {
            let mut attributes = Vec::new();
            while !r.is_empty() {
                let attr = r.sequence(Tag::SET, |r| {
                    r.sequence(Tag::SEQUENCE, |r| {
                        let oid = r.oid(Tag::OID)?;
                        let value = r.visible_string(Tag::VISIBLE_STRING)?;
                        let name = match oid.split_last() {
                            Some((arc, prefix)) if prefix == SI_OID_PREFIX => Self::attribute_name(*arc),
                            _ => None,
                        }
                        .ok_or(BerError::InvalidValue("unknown service instance attribute"))?;
                        Ok((name, value))
                    })
                })?;
                attributes.push(attr);
            }
            if attributes.is_empty() {
                return Err(BerError::InvalidValue("empty service instance identifier"));
            }
            Ok(ServiceInstanceId { attributes })
        })
    }
}

impl FromStr for ServiceInstanceId {
    type Err = SiIdError;

    fn from_str(s: &str) -> Result<Self, SiIdError> {
        let mut attributes = Vec::new();
        for part in s.split('.') {
            let (name, value) = part
                .split_once('=')
                .ok_or_else(|| SiIdError(format!("missing '=' in {part:?}")))?;
            let (name, _) =
                Self::attribute_arc(name).ok_or_else(|| SiIdError(format!("unknown attribute {name:?}")))?;
            if value.is_empty() || !value.bytes().all(|c| (0x20..=0x7e).contains(&c)) {
                return Err(SiIdError(format!("invalid value for {name}")));
            }
            attributes.push((name, value.to_string()));
        }
        Ok(ServiceInstanceId { attributes })
    }
}

impl fmt::Display for ServiceInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, (name, value)) in self.attributes.iter().enumerate() {
            if i > 0 {
                f.write_str(".")?;
            }
            write!(f, "{name}={value}")?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Diagnostics, BIND, UNBIND, STOP, acknowledgement
// ---------------------------------------------------------------------------

/// Negative result of a confirmed operation: `CHOICE { common [100]
/// Diagnostics, specific [101] INTEGER }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Diagnostic {
    /// A common diagnostic, see [`diagnostics`].
    Common(i64),
    /// A diagnostic specific to the operation, e.g.
    /// [`transfer_diagnostic`](super::fcltu::transfer_diagnostic).
    Specific(i64),
}

impl Diagnostic {
    pub(crate) fn write(&self, w: &mut BerWriter) {
        match self {
            Diagnostic::Common(v) => w.integer(Tag::ctx(100), *v),
            Diagnostic::Specific(v) => w.integer(Tag::ctx(101), *v),
        }
    }

    pub(crate) fn read(r: &mut BerReader) -> BerResult<Self> {
        match r.peek_tag()? {
            t if t == Tag::ctx(100) => r.integer(t).map(Diagnostic::Common),
            t if t == Tag::ctx(101) => r.integer(t).map(Diagnostic::Specific),
            t => Err(BerError::UnknownChoice(t)),
        }
    }
}

/// SLE-BIND invocation, the first PDU of every association.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindInvocation {
    /// The credentials of the initiator.
    pub credentials: Credentials,
    /// The identifier of the initiator.
    pub initiator_id: String,
    /// The logical name of the responder port.
    pub responder_port: String,
    /// The service type, see [`service_type`].
    pub service_type: i64,
    /// The version of the service the initiator wants to use.
    pub version: u16,
    /// The service instance to bind to.
    pub service_instance: ServiceInstanceId,
}

impl BindInvocation {
    pub(crate) fn write(&self, w: &mut BerWriter) {
        self.credentials.write(w);
        w.visible_string(Tag::VISIBLE_STRING, &self.initiator_id);
        w.visible_string(Tag::VISIBLE_STRING, &self.responder_port);
        w.integer(Tag::INTEGER, self.service_type);
        w.integer(Tag::INTEGER, i64::from(self.version));
        self.service_instance.write(w);
    }

    pub(crate) fn read(r: &mut BerReader) -> BerResult<Self> {
        Ok(Self {
            credentials: Credentials::read(r)?,
            initiator_id: r.visible_string(Tag::VISIBLE_STRING)?,
            responder_port: r.visible_string(Tag::VISIBLE_STRING)?,
            service_type: r.integer(Tag::INTEGER)?,
            version: r.integer_as(Tag::INTEGER)?,
            service_instance: ServiceInstanceId::read(r)?,
        })
    }

    /// Encodes as a complete BIND invocation PDU (identical for all
    /// services).
    pub fn encode_pdu(&self) -> Bytes {
        let mut w = BerWriter::new();
        w.constructed(TAG_BIND_INVOCATION, |w| self.write(w));
        w.into_bytes()
    }

    /// Decodes a BIND invocation PDU. Every association starts with one,
    /// before the service type is known.
    pub fn decode_pdu(pdu: Bytes) -> Result<Self, PduError> {
        let (tag, mut r) = open_pdu(pdu)?;
        if tag != TAG_BIND_INVOCATION {
            return Err(BerError::UnexpectedTag {
                expected: TAG_BIND_INVOCATION,
                found: tag,
            }
            .into());
        }
        let v = Self::read(&mut r)?;
        finish(r, v)
    }
}

/// SLE-BIND return.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindReturn {
    /// The credentials of the responder.
    pub credentials: Credentials,
    /// The identifier of the responder.
    pub responder_id: String,
    /// `Ok(version)` or `Err(bind diagnostic)`, see [`bind_diagnostic`].
    pub result: Result<u16, i64>,
}

impl BindReturn {
    pub(crate) fn write(&self, w: &mut BerWriter) {
        self.credentials.write(w);
        w.visible_string(Tag::VISIBLE_STRING, &self.responder_id);
        match self.result {
            Ok(version) => w.integer(Tag::ctx(0), i64::from(version)),
            Err(diag) => w.integer(Tag::ctx(1), diag),
        }
    }

    pub(crate) fn read(r: &mut BerReader) -> BerResult<Self> {
        let credentials = Credentials::read(r)?;
        let responder_id = r.visible_string(Tag::VISIBLE_STRING)?;
        let result = match r.peek_tag()? {
            t if t == Tag::ctx(0) => Ok(r.integer_as(t)?),
            t if t == Tag::ctx(1) => Err(r.integer(t)?),
            t => return Err(BerError::UnknownChoice(t)),
        };
        Ok(Self {
            credentials,
            responder_id,
            result,
        })
    }

    /// Encodes as a complete BIND return PDU (identical for all services).
    pub fn encode_pdu(&self) -> Bytes {
        let mut w = BerWriter::new();
        w.constructed(TAG_BIND_RETURN, |w| self.write(w));
        w.into_bytes()
    }
}

/// SLE-UNBIND invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnbindInvocation {
    /// The credentials of the initiator.
    pub credentials: Credentials,
    /// Why the initiator unbinds, see [`unbind_reason`].
    pub reason: i64,
}

impl UnbindInvocation {
    pub(crate) fn write(&self, w: &mut BerWriter) {
        self.credentials.write(w);
        w.integer(Tag::INTEGER, self.reason);
    }

    pub(crate) fn read(r: &mut BerReader) -> BerResult<Self> {
        Ok(Self {
            credentials: Credentials::read(r)?,
            reason: r.integer(Tag::INTEGER)?,
        })
    }
}

/// SLE-UNBIND return; always positive.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UnbindReturn {
    /// The credentials of the responder.
    pub credentials: Credentials,
}

impl UnbindReturn {
    pub(crate) fn write(&self, w: &mut BerWriter) {
        self.credentials.write(w);
        w.null(Tag::ctx(0));
    }

    pub(crate) fn read(r: &mut BerReader) -> BerResult<Self> {
        let credentials = Credentials::read(r)?;
        r.null(Tag::ctx(0))?;
        Ok(Self { credentials })
    }
}

/// STOP invocation, the same for all services.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopInvocation {
    /// The credentials of the user.
    pub credentials: Credentials,
    /// The invoke identifier.
    pub invoke_id: InvokeId,
}

impl StopInvocation {
    pub(crate) fn write(&self, w: &mut BerWriter) {
        self.credentials.write(w);
        w.integer(Tag::INTEGER, i64::from(self.invoke_id));
    }

    pub(crate) fn read(r: &mut BerReader) -> BerResult<Self> {
        Ok(Self {
            credentials: Credentials::read(r)?,
            invoke_id: r.integer_as(Tag::INTEGER)?,
        })
    }
}

/// `SleAcknowledgement`, e.g. the STOP return.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Acknowledgement {
    /// The credentials of the provider.
    pub credentials: Credentials,
    /// The invoke identifier of the invocation.
    pub invoke_id: InvokeId,
    /// `Err` carries a common `Diagnostics` value, see [`diagnostics`].
    pub result: Result<(), i64>,
}

impl Acknowledgement {
    /// A positive acknowledgement without credentials.
    pub fn positive(invoke_id: InvokeId) -> Self {
        Self {
            credentials: Credentials::Unused,
            invoke_id,
            result: Ok(()),
        }
    }

    pub(crate) fn write(&self, w: &mut BerWriter) {
        self.credentials.write(w);
        w.integer(Tag::INTEGER, i64::from(self.invoke_id));
        match self.result {
            Ok(()) => w.null(Tag::ctx(0)),
            Err(d) => w.integer(Tag::ctx(1), d),
        }
    }

    pub(crate) fn read(r: &mut BerReader) -> BerResult<Self> {
        let credentials = Credentials::read(r)?;
        let invoke_id = r.integer_as(Tag::INTEGER)?;
        let result = match r.peek_tag()? {
            t if t == Tag::ctx(0) => Ok(r.null(t)?),
            t if t == Tag::ctx(1) => Err(r.integer(t)?),
            t => return Err(BerError::UnknownChoice(t)),
        };
        Ok(Self {
            credentials,
            invoke_id,
            result,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn si() -> ServiceInstanceId {
        "sagr=1.spack=VST-PASS0001.rsl-fg=1.raf=onlt1".parse().unwrap()
    }

    #[test]
    fn service_instance_id_text_roundtrip() {
        assert_eq!(si().to_string(), "sagr=1.spack=VST-PASS0001.rsl-fg=1.raf=onlt1");
        assert!("sagr=1.foo=2".parse::<ServiceInstanceId>().is_err());
        assert!("sagr".parse::<ServiceInstanceId>().is_err());
    }

    #[test]
    fn service_instance_id_ber_layout() {
        let id: ServiceInstanceId = "sagr=1".parse().unwrap();
        let mut w = BerWriter::new();
        id.write(&mut w);
        assert_eq!(
            w.as_slice(),
            &[
                0x30, 0x10, // SEQUENCE OF
                0x31, 0x0e, // SET OF
                0x30, 0x0c, // SEQUENCE
                0x06, 0x07, 0x2b, 0x70, 0x04, 0x03, 0x01, 0x02, 0x34, // sagr OID
                0x1a, 0x01, b'1', // VisibleString
            ]
        );
    }

    #[test]
    fn bind_invocation_roundtrip() {
        let bind = BindInvocation {
            credentials: Credentials::Unused,
            initiator_id: "MOC1".into(),
            responder_port: "RAF_PORT".into(),
            service_type: service_type::RTN_ALL_FRAMES,
            version: 5,
            service_instance: si(),
        };
        let pdu = bind.encode_pdu();
        assert_eq!(&pdu[..2], &[0xbf, 0x64]);
        assert_eq!(BindInvocation::decode_pdu(pdu).unwrap(), bind);
    }

    #[test]
    fn bind_return_wire_format() {
        let ret = BindReturn {
            credentials: Credentials::Unused,
            responder_id: "PROV".into(),
            result: Ok(5),
        };
        assert_eq!(
            &ret.encode_pdu()[..],
            &[
                0xbf, 0x65, 0x0b, 0x80, 0x00, 0x1a, 0x04, b'P', b'R', b'O', b'V', 0x80, 0x01, 0x05
            ]
        );
    }

    #[test]
    fn time_conversion() {
        // 1970-01-01T00:00:01.002003Z
        let t = UNIX_EPOCH + std::time::Duration::from_micros(1_002_003);
        assert_eq!(Time::from_system_time(t).components(), (4383, 1002, 3_000_000));
        let a = Time::Ccsds([0, 1, 0, 0, 0, 5, 0, 0]);
        let b = Time::CcsdsPico([0, 1, 0, 0, 0, 5, 0, 0, 0, 1]);
        assert!(a < b);
    }
}
