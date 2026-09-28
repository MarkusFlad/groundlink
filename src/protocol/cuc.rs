//! CCSDS Unsegmented Time Code (CUC, CCSDS 301.0-B-4, section 3.2).
//!
//! A CUC time counts seconds (coarse time, 1–4 bytes) and fractions of a
//! second (fine time, 0–3 bytes, unit `2^-(8·fine_len)` s) since an epoch.
//! It can be preceded by a 1-byte P-field that describes the epoch and the
//! field lengths.
//!
//! Supported epochs ([`CucEpoch`]):
//! - [`CucEpoch::Ccsds`]: 1958-01-01T00:00:00 TAI. The count runs in TAI,
//!   so leap seconds are applied from a built-in table ([`tai_minus_utc`])
//!   when converting from or to UTC.
//! - [`CucEpoch::Agency`]: a mission-specific epoch in UTC. Counted like
//!   Unix time, without leap seconds.
//!
//! The default format ([`CucFormat::default`]) is the CCSDS epoch, 4 bytes
//! coarse, 2 bytes fine, with P-field: 7 bytes in total, matching
//! [`crate::protocol::pus::DEFAULT_TM_TIME_LEN`].
//!
//! ```
//! use groundlink::{CucFormat, CucTime, PusTm};
//!
//! let t: CucTime = "2026-09-26T12:00:00.5Z".parse().unwrap();
//! assert_eq!(t.to_string(), "2026-09-26T12:00:00.500000Z");
//!
//! let now = CucTime::now(CucFormat::default()).unwrap();
//! let tm = PusTm::new(42, 0, 17, 2, now, &b""[..]);
//! ```

use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, NaiveDateTime, SecondsFormat, Utc};
use std::fmt;
use std::io;
use std::str::FromStr;

/// Seconds from 1958-01-01 to 1970-01-01 (4383 days).
const SECONDS_1958_TO_1970: i64 = 4383 * 86_400;

/// TAI−UTC in seconds, valid from the given UTC instant (Unix time).
/// Source: IERS Bulletin C. Must be extended when a new leap second is
/// announced.
const LEAP_SECONDS: &[(i64, i64)] = &[
    (63_072_000, 10),    // 1972-01-01
    (78_796_800, 11),    // 1972-07-01
    (94_694_400, 12),    // 1973-01-01
    (126_230_400, 13),   // 1974-01-01
    (157_766_400, 14),   // 1975-01-01
    (189_302_400, 15),   // 1976-01-01
    (220_924_800, 16),   // 1977-01-01
    (252_460_800, 17),   // 1978-01-01
    (283_996_800, 18),   // 1979-01-01
    (315_532_800, 19),   // 1980-01-01
    (362_793_600, 20),   // 1981-07-01
    (394_329_600, 21),   // 1982-07-01
    (425_865_600, 22),   // 1983-07-01
    (489_024_000, 23),   // 1985-07-01
    (567_993_600, 24),   // 1988-01-01
    (631_152_000, 25),   // 1990-01-01
    (662_688_000, 26),   // 1991-01-01
    (709_948_800, 27),   // 1992-07-01
    (741_484_800, 28),   // 1993-07-01
    (773_020_800, 29),   // 1994-07-01
    (820_454_400, 30),   // 1996-01-01
    (867_715_200, 31),   // 1997-07-01
    (915_148_800, 32),   // 1999-01-01
    (1_136_073_600, 33), // 2006-01-01
    (1_230_768_000, 34), // 2009-01-01
    (1_341_100_800, 35), // 2012-07-01
    (1_435_708_800, 36), // 2015-07-01
    (1_483_228_800, 37), // 2017-01-01
];

/// TAI−UTC in seconds at the UTC instant `utc`.
///
/// Returns `None` before 1972-01-01, when TAI−UTC was not an integer number
/// of seconds.
///
/// The value comes from a built-in leap second table that ends with the
/// leap second of 2017-01-01 (TAI−UTC = 37 s); it has to be extended when
/// IERS announces a new one.
///
/// ```
/// use chrono::{TimeZone, Utc};
/// use groundlink::protocol::cuc::tai_minus_utc;
///
/// assert_eq!(tai_minus_utc(&Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()), Some(37));
/// assert_eq!(tai_minus_utc(&Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap()), None);
/// ```
pub fn tai_minus_utc(utc: &DateTime<Utc>) -> Option<i64> {
    let unix = utc.timestamp();
    LEAP_SECONDS
        .iter()
        .rev()
        .find(|(since, _)| unix >= *since)
        .map(|&(_, offset)| offset)
}

fn invalid_input(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg)
}

fn invalid_data(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

/// Epoch from which a [`CucTime`] counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CucEpoch {
    /// CCSDS epoch 1958-01-01 TAI; counting in TAI (P-field time code ID `0b001`).
    Ccsds,
    /// Mission-specific epoch in UTC; counting without leap seconds
    /// (P-field time code ID `0b010`).
    Agency(DateTime<Utc>),
}

/// Format of a CUC time: epoch, field lengths and whether a P-field is
/// prepended.
///
/// The P-field can describe the agency epoch only by its time code ID, not
/// by its date; decoding therefore always needs the full format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CucFormat {
    /// Epoch the time counts from.
    pub epoch: CucEpoch,
    /// Length of the coarse time in bytes (`1..=4`).
    pub coarse_len: u8,
    /// Length of the fine time in bytes (`0..=3`).
    pub fine_len: u8,
    /// Whether the 1-byte P-field is prepended.
    pub p_field: bool,
}

impl Default for CucFormat {
    /// CCSDS epoch, 4 bytes coarse, 2 bytes fine, with P-field (7 bytes).
    fn default() -> Self {
        CucFormat {
            epoch: CucEpoch::Ccsds,
            coarse_len: 4,
            fine_len: 2,
            p_field: true,
        }
    }
}

impl CucFormat {
    /// Total length of the encoded time in bytes, including the P-field.
    pub fn len(&self) -> usize {
        self.p_field as usize + self.coarse_len as usize + self.fine_len as usize
    }

    /// The P-field byte for this format.
    pub fn p_field_byte(&self) -> u8 {
        let time_code_id: u8 = match self.epoch {
            CucEpoch::Ccsds => 0b001,
            CucEpoch::Agency(_) => 0b010,
        };
        time_code_id << 4 | (self.coarse_len - 1) << 2 | self.fine_len
    }

    fn validate(&self) -> io::Result<()> {
        if !(1..=4).contains(&self.coarse_len) || self.fine_len > 3 {
            return Err(invalid_input(format!(
                "invalid CUC format: coarse_len {} (allowed 1..=4), fine_len {} (allowed 0..=3)",
                self.coarse_len, self.fine_len
            )));
        }
        Ok(())
    }

    fn fine_bits(&self) -> u32 {
        8 * self.fine_len as u32
    }
}

/// A time in the CCSDS Unsegmented Time Code.
///
/// Created from UTC with [`now`](Self::now), [`from_utc`](Self::from_utc),
/// [`from_utc_str`](Self::from_utc_str) or [`str::parse`], or from its
/// binary form with [`from_bytes`](Self::from_bytes). Converts back with
/// [`to_utc`](Self::to_utc), [`to_bytes`](Self::to_bytes) and
/// [`Display`](fmt::Display).
///
/// ```
/// use groundlink::{CucFormat, CucTime};
///
/// let t = CucTime::from_utc_str("2026-09-26T12:00:00.25Z", CucFormat::default()).unwrap();
/// assert_eq!(t.fine, 0x4000); // 0.25 s in units of 2^-16 s
///
/// let bytes = t.to_bytes().unwrap();
/// assert_eq!(bytes[0], 0x1E); // P-field: CCSDS epoch, 4 + 2 bytes
/// assert_eq!(CucTime::from_bytes(&bytes, CucFormat::default()).unwrap(), t);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CucTime {
    /// Format of this time.
    pub format: CucFormat,
    /// Whole seconds since the epoch.
    pub coarse: u32,
    /// Fraction of a second in units of `2^-(8·fine_len)` s.
    pub fine: u32,
}

impl CucTime {
    /// The current system time as a CUC time.
    ///
    /// # Errors
    ///
    /// See [`from_utc`](Self::from_utc).
    pub fn now(format: CucFormat) -> io::Result<Self> {
        Self::from_utc(&Utc::now(), format)
    }

    /// Converts a UTC instant to a CUC time. The fraction of a second is
    /// truncated to the resolution of the fine time.
    ///
    /// # Errors
    ///
    /// Fails with [`io::ErrorKind::InvalidInput`] if `format` is invalid,
    /// if the instant lies before the epoch (for [`CucEpoch::Ccsds`]:
    /// before 1972, see [`tai_minus_utc`]), or if it does not fit into
    /// `coarse_len` bytes.
    pub fn from_utc(utc: &DateTime<Utc>, format: CucFormat) -> io::Result<Self> {
        format.validate()?;

        // During a leap second chrono reports nanos >= 10^9.
        let nanos = utc.timestamp_subsec_nanos().min(999_999_999) as i64;
        let (mut seconds, mut nanos) = match format.epoch {
            CucEpoch::Ccsds => {
                let leap = tai_minus_utc(utc).ok_or_else(|| {
                    invalid_input(format!(
                        "{utc} is before 1972, where TAI−UTC is not an integer number of seconds"
                    ))
                })?;
                (utc.timestamp() + SECONDS_1958_TO_1970 + leap, nanos)
            }
            CucEpoch::Agency(epoch) => (
                utc.timestamp() - epoch.timestamp(),
                nanos - epoch.timestamp_subsec_nanos() as i64,
            ),
        };
        if nanos < 0 {
            seconds -= 1;
            nanos += 1_000_000_000;
        }

        let coarse_max = (1u64 << (8 * format.coarse_len as u32)) - 1;
        if seconds < 0 || seconds as u64 > coarse_max {
            return Err(invalid_input(format!(
                "{utc} is outside the representable range ({seconds} s since epoch, max {coarse_max})"
            )));
        }

        let fine = ((nanos as u64) << format.fine_bits()) / 1_000_000_000;
        Ok(CucTime {
            format,
            coarse: seconds as u32,
            fine: fine as u32,
        })
    }

    /// Converts a human-readable UTC timestamp to a CUC time.
    ///
    /// Accepted formats (fraction of a second and trailing `Z` optional):
    /// - RFC 3339 / ISO 8601, e.g. `2026-09-26T12:34:56.789Z`, or with an
    ///   offset such as `2026-09-26T14:34:56+02:00`
    /// - CCSDS ASCII time code B (day of year), e.g. `2026-269T12:34:56.789Z`
    /// - a space instead of `T`, e.g. `2026-09-26 12:34:56`
    ///
    /// # Errors
    ///
    /// Fails with [`io::ErrorKind::InvalidInput`] if `s` has none of these
    /// formats, or for the reasons listed at [`from_utc`](Self::from_utc).
    pub fn from_utc_str(s: &str, format: CucFormat) -> io::Result<Self> {
        Self::from_utc(&parse_utc(s)?, format)
    }

    /// Converts the CUC time to a UTC instant, rounded to nanoseconds.
    ///
    /// # Errors
    ///
    /// Fails with [`io::ErrorKind::InvalidInput`] if the format is invalid,
    /// or with [`io::ErrorKind::InvalidData`] if the time lies before 1972
    /// (CCSDS epoch) or cannot be represented as a UTC instant.
    pub fn to_utc(&self) -> io::Result<DateTime<Utc>> {
        self.format.validate()?;

        let bits = self.format.fine_bits();
        let nanos = if bits == 0 {
            0
        } else {
            ((self.fine as u64 * 1_000_000_000 + (1 << (bits - 1))) >> bits) as u32
        };

        let unix = match self.format.epoch {
            CucEpoch::Ccsds => {
                let tai_unix = self.coarse as i64 - SECONDS_1958_TO_1970;
                let leap = LEAP_SECONDS
                    .iter()
                    .rev()
                    .find(|(since, offset)| tai_unix >= since + offset)
                    .map(|&(_, offset)| offset)
                    .ok_or_else(|| invalid_data(format!("CUC time {} s is before 1972", self.coarse)))?;
                tai_unix - leap
            }
            CucEpoch::Agency(epoch) => {
                return Ok(epoch
                    + chrono::Duration::seconds(self.coarse as i64)
                    + chrono::Duration::nanoseconds(nanos as i64));
            }
        };
        DateTime::from_timestamp(unix, nanos)
            .ok_or_else(|| invalid_data(format!("CUC time {} s cannot be represented as UTC", self.coarse)))
    }

    /// Appends the encoded time (with P-field, if configured) to `dst`.
    ///
    /// # Errors
    ///
    /// Fails with [`io::ErrorKind::InvalidInput`] if the format is invalid
    /// or `coarse`/`fine` do not fit into their configured lengths.
    pub fn encode(&self, dst: &mut BytesMut) -> io::Result<()> {
        self.format.validate()?;
        let coarse_len = self.format.coarse_len as usize;
        let fine_len = self.format.fine_len as usize;
        if coarse_len < 4 && self.coarse >> (8 * coarse_len) != 0 {
            return Err(invalid_input(format!(
                "coarse {} does not fit into {coarse_len} bytes",
                self.coarse
            )));
        }
        if self.fine >> (8 * fine_len) != 0 {
            return Err(invalid_input(format!(
                "fine {} does not fit into {fine_len} bytes",
                self.fine
            )));
        }

        dst.reserve(self.format.len());
        if self.format.p_field {
            dst.put_u8(self.format.p_field_byte());
        }
        dst.put_uint(self.coarse as u64, coarse_len);
        dst.put_uint(self.fine as u64, fine_len);
        Ok(())
    }

    /// Encodes the time (with P-field, if configured) into a new buffer.
    ///
    /// # Errors
    ///
    /// See [`encode`](Self::encode).
    pub fn to_bytes(&self) -> io::Result<Bytes> {
        let mut buf = BytesMut::new();
        self.encode(&mut buf)?;
        Ok(buf.freeze())
    }

    /// Decodes a CUC time in the given format.
    ///
    /// # Errors
    ///
    /// Fails with [`io::ErrorKind::InvalidInput`] if `format` is invalid, and
    /// with [`io::ErrorKind::InvalidData`] if `src` is not exactly
    /// [`CucFormat::len`] bytes long or its P-field does not match
    /// `format`.
    pub fn from_bytes(src: &[u8], format: CucFormat) -> io::Result<Self> {
        format.validate()?;
        if src.len() != format.len() {
            return Err(invalid_data(format!(
                "CUC time has {} bytes, expected {} bytes",
                src.len(),
                format.len()
            )));
        }

        let mut rest = src;
        if format.p_field {
            if src[0] != format.p_field_byte() {
                return Err(invalid_data(format!(
                    "P-field 0x{:02X} does not match the expected format (0x{:02X})",
                    src[0],
                    format.p_field_byte()
                )));
            }
            rest = &src[1..];
        }

        let (coarse, fine) = rest.split_at(format.coarse_len as usize);
        let be = |bytes: &[u8]| bytes.iter().fold(0u32, |acc, &b| acc << 8 | b as u32);
        Ok(CucTime {
            format,
            coarse: be(coarse),
            fine: be(fine),
        })
    }
}

/// Enables `"…".parse::<CucTime>()` with [`CucFormat::default`]; see
/// [`CucTime::from_utc_str`] for the accepted formats.
impl FromStr for CucTime {
    type Err = io::Error;

    fn from_str(s: &str) -> io::Result<Self> {
        Self::from_utc_str(s, CucFormat::default())
    }
}

/// Formats the time as UTC in RFC 3339 with microseconds, e.g.
/// `2026-09-26T12:00:00.500000Z`. Times that cannot be converted are shown
/// as `CUC(coarse=…, fine=…)`.
impl fmt::Display for CucTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.to_utc() {
            Ok(utc) => f.write_str(&utc.to_rfc3339_opts(SecondsFormat::Micros, true)),
            Err(_) => write!(f, "CUC(coarse={}, fine={})", self.coarse, self.fine),
        }
    }
}

/// The encoded bytes (including the P-field), e.g. as the time stamp for
/// [`PusTm::new`](crate::PusTm::new).
///
/// # Panics
///
/// Panics if the format is invalid or the values are too large; times
/// created with [`CucTime::from_utc`] or [`CucTime::from_bytes`] are always
/// valid.
impl From<CucTime> for Bytes {
    fn from(time: CucTime) -> Self {
        time.to_bytes().expect("invalid CucTime")
    }
}

fn parse_utc(s: &str) -> io::Result<DateTime<Utc>> {
    let s = s.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }
    let naive = s.strip_suffix('Z').unwrap_or(s);
    [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%jT%H:%M:%S%.f",
        "%Y-%j %H:%M:%S%.f",
    ]
    .iter()
    .find_map(|fmt| NaiveDateTime::parse_from_str(naive, fmt).ok())
    .map(|dt| dt.and_utc())
    .ok_or_else(|| invalid_input(format!("'{s}' is not a supported UTC timestamp")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, NaiveDate, TimeZone};

    fn utc(s: &str) -> DateTime<Utc> {
        parse_utc(s).unwrap()
    }

    #[test]
    fn leap_second_table_matches_calendar_dates() {
        let dates = [
            (1972, 1),
            (1972, 7),
            (1973, 1),
            (1974, 1),
            (1975, 1),
            (1976, 1),
            (1977, 1),
            (1978, 1),
            (1979, 1),
            (1980, 1),
            (1981, 7),
            (1982, 7),
            (1983, 7),
            (1985, 7),
            (1988, 1),
            (1990, 1),
            (1991, 1),
            (1992, 7),
            (1993, 7),
            (1994, 7),
            (1996, 1),
            (1997, 7),
            (1999, 1),
            (2006, 1),
            (2009, 1),
            (2012, 7),
            (2015, 7),
            (2017, 1),
        ];
        assert_eq!(dates.len(), LEAP_SECONDS.len());
        for ((y, m), &(unix, offset)) in dates.iter().zip(LEAP_SECONDS) {
            assert_eq!(
                Utc.with_ymd_and_hms(*y, *m, 1, 0, 0, 0).unwrap().timestamp(),
                unix,
                "{y}-{m}"
            );
            assert_eq!(
                offset,
                10 + LEAP_SECONDS.iter().position(|e| e.0 == unix).unwrap() as i64
            );
        }
    }

    #[test]
    fn ccsds_epoch_applies_leap_seconds() {
        let t = CucTime::from_utc(&utc("2017-01-01T00:00:00Z"), CucFormat::default()).unwrap();
        let days = NaiveDate::from_ymd_opt(2017, 1, 1)
            .unwrap()
            .signed_duration_since(NaiveDate::from_ymd_opt(1958, 1, 1).unwrap())
            .num_days();
        assert_eq!(t.coarse as i64, days * 86_400 + 37);
        assert_eq!(t.fine, 0);
    }

    #[test]
    fn fine_time_is_binary_fraction() {
        let t: CucTime = "2026-09-26T12:00:00.5Z".parse().unwrap();
        assert_eq!(t.fine, 0x8000);
        let t: CucTime = "2026-09-26T12:00:00.25Z".parse().unwrap();
        assert_eq!(t.fine, 0x4000);
    }

    #[test]
    fn encoding_with_p_field() {
        let t = CucTime {
            format: CucFormat::default(),
            coarse: 0x0102_0304,
            fine: 0x0506,
        };
        assert_eq!(&t.to_bytes().unwrap()[..], &[0x1E, 1, 2, 3, 4, 5, 6]);
        assert_eq!(
            CucTime::from_bytes(&[0x1E, 1, 2, 3, 4, 5, 6], CucFormat::default()).unwrap(),
            t
        );
    }

    #[test]
    fn encoding_without_p_field_and_other_lengths() {
        let format = CucFormat {
            coarse_len: 2,
            fine_len: 1,
            p_field: false,
            ..CucFormat::default()
        };
        let t = CucTime {
            format,
            coarse: 0xABCD,
            fine: 0xEF,
        };
        assert_eq!(&t.to_bytes().unwrap()[..], &[0xAB, 0xCD, 0xEF]);
        assert_eq!(CucTime::from_bytes(&[0xAB, 0xCD, 0xEF], format).unwrap(), t);
    }

    #[test]
    fn utc_roundtrip_within_resolution() {
        for s in [
            "2026-09-26T12:34:56.789123Z",
            "2016-12-31T23:59:59.999Z",
            "1999-01-01T00:00:00Z",
        ] {
            let original = utc(s);
            let back = CucTime::from_utc(&original, CucFormat::default())
                .unwrap()
                .to_utc()
                .unwrap();
            assert!((back - original).abs() < Duration::microseconds(16), "{s}: {back}");
        }
    }

    #[test]
    fn different_input_formats_give_same_time() {
        let expected: CucTime = "2026-09-26T12:34:56.5Z".parse().unwrap();
        for s in [
            "2026-09-26T14:34:56.5+02:00",
            "2026-09-26T12:34:56.5",
            "2026-09-26 12:34:56.5",
            "2026-269T12:34:56.5Z",
        ] {
            assert_eq!(s.parse::<CucTime>().unwrap(), expected, "{s}");
        }
        assert!("26.09.2026".parse::<CucTime>().is_err());
    }

    #[test]
    fn agency_epoch_without_leap_seconds() {
        let format = CucFormat {
            epoch: CucEpoch::Agency(utc("2000-01-01T12:00:00Z")),
            ..CucFormat::default()
        };
        let t = CucTime::from_utc_str("2000-01-02T12:00:01.5Z", format).unwrap();
        assert_eq!((t.coarse, t.fine), (86_401, 0x8000));
        assert_eq!(t.to_bytes().unwrap()[0], 0x2E);
        assert_eq!(t.to_string(), "2000-01-02T12:00:01.500000Z");
        assert!(CucTime::from_utc_str("1999-12-31T00:00:00Z", format).is_err());
    }

    #[test]
    fn error_cases() {
        assert!(CucTime::from_utc_str("1970-01-01T00:00:00Z", CucFormat::default()).is_err());
        let small = CucFormat {
            coarse_len: 1,
            ..CucFormat::default()
        };
        assert!(CucTime::from_utc_str("2026-01-01T00:00:00Z", small).is_err());
        let err = CucTime::from_bytes(&[0x2E, 0, 0, 0, 0, 0, 0], CucFormat::default()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(CucTime::from_bytes(&[0x1E, 0, 0], CucFormat::default()).is_err());
    }

    #[test]
    fn now_returns_current_time() {
        let t = CucTime::now(CucFormat::default()).unwrap();
        assert!((t.to_utc().unwrap() - Utc::now()).abs() < Duration::seconds(1));
    }
}
