//! SLE transfer service PDUs, BER-encoded as the [`TmlLinkActor`](crate::TmlLinkActor)
//! exchanges them in [`SlePdu`](crate::SlePdu) messages.
//!
//! - [`ber`]: a minimal BER (ITU-T X.690) encoder and decoder for the ASN.1
//!   subset of the SLE PDUs.
//! - [`common`]: the types shared by all services: service instance
//!   identifier, times, credentials, BIND, UNBIND, STOP (CCSDS 913.1-B-2
//!   and the common ASN.1 modules of the service specifications).
//! - [`raf`]: Return All Frames (CCSDS 911.1-B-4).
//! - [`fcltu`]: Forward CLTU (CCSDS 912.1-B-4).
//!
//! Each service has one enum for the PDUs the user sends
//! ([`RafUserPdu`](raf::RafUserPdu), [`FcltuUserPdu`](fcltu::FcltuUserPdu))
//! and one for the PDUs the provider sends
//! ([`RafProviderPdu`](raf::RafProviderPdu),
//! [`FcltuProviderPdu`](fcltu::FcltuProviderPdu)), each with `encode` and
//! `decode`. The subset covered is BIND, UNBIND, START, STOP,
//! TRANSFER-DATA, SCHEDULE-STATUS-REPORT and the status reports; for
//! FCLTU also ASYNC-NOTIFY and GET-PARAMETER (for the expected CLTU and
//! event identifications). The other operations (RAF-GET-PARAMETER,
//! CLTU-THROW-EVENT, RAF-SYNC-NOTIFY) are recognised and decoded as
//! [`PduError::Unsupported`](common::PduError::Unsupported).
//!
//! Every association starts with a BIND invocation, which is the same for
//! all services, so a provider can decode it with
//! [`BindInvocation::decode_pdu`](common::BindInvocation::decode_pdu)
//! before it knows the service type.
//!
//! ```
//! use groundlink::protocol::sle::common::{Credentials, ConditionalTime};
//! use groundlink::{RafStartInvocation, RafUserPdu, RequestedFrameQuality};
//!
//! let start = RafUserPdu::Start(RafStartInvocation {
//!     credentials: Credentials::Unused,
//!     invoke_id: 1,
//!     start_time: ConditionalTime::Undefined,
//!     stop_time: ConditionalTime::Undefined,
//!     requested_quality: RequestedFrameQuality::GoodFramesOnly,
//! });
//! let pdu = start.encode();
//! assert_eq!(pdu[0], 0xa0);
//! assert_eq!(RafUserPdu::decode(pdu).unwrap(), start);
//! ```

pub mod ber;
pub mod common;
pub mod fcltu;
pub mod raf;
