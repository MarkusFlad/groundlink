//! CCSDS Unsegmented Time Code (CUC, CCSDS 301.0-B-4, Abschnitt 3.2).
//!
//! Eine CUC-Zeit zählt Sekunden (Coarse Time, 1–4 Byte) und
//! Sekundenbruchteile (Fine Time, 0–3 Byte, Einheit `2^-(8·fine_len)` s)
//! seit einer Epoche. Optional steht davor ein 1 Byte langes P-Field, das
//! Epoche und Feldlängen beschreibt.
//!
//! Unterstützte Epochen ([`CucEpoch`]):
//! - [`CucEpoch::Ccsds`]: 1958-01-01T00:00:00 TAI. Die Zählung läuft in
//!   TAI, d. h. bei der Umrechnung von bzw. nach UTC werden Schaltsekunden
//!   über eine eingebaute Tabelle ([`tai_minus_utc`]) berücksichtigt.
//! - [`CucEpoch::Agency`]: eine missionsspezifische Epoche in UTC. Gezählt
//!   wird wie bei Unix-Zeit ohne Schaltsekunden.
//!
//! Das Standardformat ([`CucFormat::default`]) ist CCSDS-Epoche, 4 Byte
//! Coarse, 2 Byte Fine, mit P-Field – zusammen 7 Byte, passend zu
//! [`crate::pus::DEFAULT_TM_TIME_LEN`].
//!
//! ```
//! use kameo_tcp_example::{CucFormat, CucTime, PusTm};
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

/// Sekunden von 1958-01-01 bis 1970-01-01 (4383 Tage).
const SECONDS_1958_TO_1970: i64 = 4383 * 86_400;

/// TAI−UTC in Sekunden, gültig ab dem jeweiligen UTC-Zeitpunkt (Unix-Zeit).
/// Quelle: IERS Bulletin C. Muss bei neuen Schaltsekunden ergänzt werden.
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

/// TAI−UTC in Sekunden für den UTC-Zeitpunkt `utc`, oder `None` vor
/// 1972-01-01 (davor gab es keine ganzzahligen Schaltsekunden).
pub fn tai_minus_utc(utc: &DateTime<Utc>) -> Option<i64> {
    let unix = utc.timestamp();
    LEAP_SECONDS.iter().rev().find(|(since, _)| unix >= *since).map(|&(_, offset)| offset)
}

fn invalid_input(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, msg)
}

fn invalid_data(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

/// Epoche, ab der eine [`CucTime`] zählt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CucEpoch {
    /// CCSDS-Epoche 1958-01-01 TAI; Zählung in TAI (P-Field Time Code ID `0b001`).
    Ccsds,
    /// Missionsspezifische Epoche in UTC; Zählung ohne Schaltsekunden
    /// (P-Field Time Code ID `0b010`).
    Agency(DateTime<Utc>),
}

/// Format einer CUC-Zeit: Epoche, Feldlängen und ob ein P-Field
/// vorangestellt wird.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CucFormat {
    pub epoch: CucEpoch,
    /// Länge der Coarse Time in Byte (`1..=4`).
    pub coarse_len: u8,
    /// Länge der Fine Time in Byte (`0..=3`).
    pub fine_len: u8,
    /// Ob das 1 Byte lange P-Field vorangestellt wird.
    pub p_field: bool,
}

impl Default for CucFormat {
    /// CCSDS-Epoche, 4 Byte Coarse, 2 Byte Fine, mit P-Field (7 Byte).
    fn default() -> Self {
        CucFormat { epoch: CucEpoch::Ccsds, coarse_len: 4, fine_len: 2, p_field: true }
    }
}

impl CucFormat {
    /// Gesamtlänge der kodierten Zeit in Byte (inkl. P-Field).
    pub fn len(&self) -> usize {
        self.p_field as usize + self.coarse_len as usize + self.fine_len as usize
    }

    /// Das P-Field-Byte zu diesem Format.
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
                "ungültiges CUC-Format: coarse_len {} (erlaubt 1..=4), fine_len {} (erlaubt 0..=3)",
                self.coarse_len, self.fine_len
            )));
        }
        Ok(())
    }

    fn fine_bits(&self) -> u32 {
        8 * self.fine_len as u32
    }
}

/// Eine Zeit im CCSDS Unsegmented Time Code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CucTime {
    pub format: CucFormat,
    /// Ganze Sekunden seit der Epoche.
    pub coarse: u32,
    /// Sekundenbruchteil in Einheiten von `2^-(8·fine_len)` s.
    pub fine: u32,
}

impl CucTime {
    /// Die aktuelle Systemzeit als CUC-Zeit.
    pub fn now(format: CucFormat) -> io::Result<Self> {
        Self::from_utc(&Utc::now(), format)
    }

    /// Wandelt einen UTC-Zeitpunkt in eine CUC-Zeit um. Der
    /// Sekundenbruchteil wird auf die Auflösung der Fine Time abgeschnitten.
    ///
    /// Schlägt fehl, wenn der Zeitpunkt vor der Epoche (bei
    /// [`CucEpoch::Ccsds`]: vor 1972, siehe [`tai_minus_utc`]) liegt oder
    /// nicht in `coarse_len` Byte passt.
    pub fn from_utc(utc: &DateTime<Utc>, format: CucFormat) -> io::Result<Self> {
        format.validate()?;

        // Während einer Schaltsekunde liefert chrono nanos >= 10^9.
        let nanos = utc.timestamp_subsec_nanos().min(999_999_999) as i64;
        let (mut seconds, mut nanos) = match format.epoch {
            CucEpoch::Ccsds => {
                let leap = tai_minus_utc(utc).ok_or_else(|| {
                    invalid_input(format!("{utc} liegt vor 1972; TAI−UTC ist dort nicht ganzzahlig definiert"))
                })?;
                (utc.timestamp() + SECONDS_1958_TO_1970 + leap, nanos)
            }
            CucEpoch::Agency(epoch) => {
                (utc.timestamp() - epoch.timestamp(), nanos - epoch.timestamp_subsec_nanos() as i64)
            }
        };
        if nanos < 0 {
            seconds -= 1;
            nanos += 1_000_000_000;
        }

        let coarse_max = (1u64 << (8 * format.coarse_len as u32)) - 1;
        if seconds < 0 || seconds as u64 > coarse_max {
            return Err(invalid_input(format!(
                "{utc} liegt außerhalb des darstellbaren Bereichs ({seconds} s seit Epoche, max {coarse_max})"
            )));
        }

        let fine = ((nanos as u64) << format.fine_bits()) / 1_000_000_000;
        Ok(CucTime { format, coarse: seconds as u32, fine: fine as u32 })
    }

    /// Wandelt einen lesbaren UTC-Zeitstempel in eine CUC-Zeit um.
    ///
    /// Akzeptierte Formate (Bruchteile optional, `Z` optional):
    /// - RFC 3339 / ISO 8601, z. B. `2026-09-26T12:34:56.789Z` oder mit
    ///   Offset `2026-09-26T14:34:56+02:00`
    /// - CCSDS ASCII Time Code B (Tag im Jahr), z. B. `2026-269T12:34:56.789Z`
    /// - Leerzeichen statt `T`, z. B. `2026-09-26 12:34:56`
    pub fn from_utc_str(s: &str, format: CucFormat) -> io::Result<Self> {
        Self::from_utc(&parse_utc(s)?, format)
    }

    /// Wandelt die CUC-Zeit in einen UTC-Zeitpunkt um (auf Nanosekunden
    /// gerundet).
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
                    .ok_or_else(|| invalid_data(format!("CUC-Zeit {} s liegt vor 1972", self.coarse)))?;
                tai_unix - leap
            }
            CucEpoch::Agency(epoch) => {
                return Ok(epoch
                    + chrono::Duration::seconds(self.coarse as i64)
                    + chrono::Duration::nanoseconds(nanos as i64));
            }
        };
        DateTime::from_timestamp(unix, nanos)
            .ok_or_else(|| invalid_data(format!("CUC-Zeit {} s nicht als UTC darstellbar", self.coarse)))
    }

    /// Kodiert die Zeit (ggf. mit P-Field) an `dst`.
    pub fn encode(&self, dst: &mut BytesMut) -> io::Result<()> {
        self.format.validate()?;
        let coarse_len = self.format.coarse_len as usize;
        let fine_len = self.format.fine_len as usize;
        if coarse_len < 4 && self.coarse >> (8 * coarse_len) != 0 {
            return Err(invalid_input(format!("coarse {} passt nicht in {coarse_len} Byte", self.coarse)));
        }
        if self.fine >> (8 * fine_len) != 0 {
            return Err(invalid_input(format!("fine {} passt nicht in {fine_len} Byte", self.fine)));
        }

        dst.reserve(self.format.len());
        if self.format.p_field {
            dst.put_u8(self.format.p_field_byte());
        }
        dst.put_uint(self.coarse as u64, coarse_len);
        dst.put_uint(self.fine as u64, fine_len);
        Ok(())
    }

    /// Kodiert die Zeit (ggf. mit P-Field) als eigenständigen Puffer.
    pub fn to_bytes(&self) -> io::Result<Bytes> {
        let mut buf = BytesMut::new();
        self.encode(&mut buf)?;
        Ok(buf.freeze())
    }

    /// Dekodiert eine CUC-Zeit im gegebenen Format. `src` muss genau
    /// [`CucFormat::len`] Byte lang sein; ein vorhandenes P-Field muss zum
    /// Format passen.
    pub fn from_bytes(src: &[u8], format: CucFormat) -> io::Result<Self> {
        format.validate()?;
        if src.len() != format.len() {
            return Err(invalid_data(format!(
                "CUC-Zeit hat {} Byte, erwartet werden {} Byte",
                src.len(),
                format.len()
            )));
        }

        let mut rest = src;
        if format.p_field {
            if src[0] != format.p_field_byte() {
                return Err(invalid_data(format!(
                    "P-Field 0x{:02X} passt nicht zum erwarteten Format (0x{:02X})",
                    src[0],
                    format.p_field_byte()
                )));
            }
            rest = &src[1..];
        }

        let (coarse, fine) = rest.split_at(format.coarse_len as usize);
        let be = |bytes: &[u8]| bytes.iter().fold(0u32, |acc, &b| acc << 8 | b as u32);
        Ok(CucTime { format, coarse: be(coarse), fine: be(fine) })
    }
}

/// Erlaubt `"…".parse::<CucTime>()` mit [`CucFormat::default`].
impl FromStr for CucTime {
    type Err = io::Error;

    fn from_str(s: &str) -> io::Result<Self> {
        Self::from_utc_str(s, CucFormat::default())
    }
}

/// Gibt die Zeit lesbar als UTC aus (RFC 3339, Mikrosekunden).
impl fmt::Display for CucTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.to_utc() {
            Ok(utc) => f.write_str(&utc.to_rfc3339_opts(SecondsFormat::Micros, true)),
            Err(_) => write!(f, "CUC(coarse={}, fine={})", self.coarse, self.fine),
        }
    }
}

/// Die kodierten Bytes (inkl. P-Field), z. B. als Zeitstempel für
/// [`crate::PusTm::new`].
///
/// # Panics
/// Bei ungültigem Format oder zu großen Werten; mit [`CucTime::from_utc`]
/// bzw. [`CucTime::from_bytes`] erzeugte Zeiten sind stets gültig.
impl From<CucTime> for Bytes {
    fn from(time: CucTime) -> Self {
        time.to_bytes().expect("ungültige CucTime")
    }
}

fn parse_utc(s: &str) -> io::Result<DateTime<Utc>> {
    let s = s.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }
    let naive = s.strip_suffix('Z').unwrap_or(s);
    ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f", "%Y-%jT%H:%M:%S%.f", "%Y-%j %H:%M:%S%.f"]
        .iter()
        .find_map(|fmt| NaiveDateTime::parse_from_str(naive, fmt).ok())
        .map(|dt| dt.and_utc())
        .ok_or_else(|| invalid_input(format!("'{s}' ist kein unterstützter UTC-Zeitstempel")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, NaiveDate, TimeZone};

    fn utc(s: &str) -> DateTime<Utc> {
        parse_utc(s).unwrap()
    }

    #[test]
    fn schaltsekundentabelle_stimmt_mit_kalenderdaten_ueberein() {
        let dates = [
            (1972, 1), (1972, 7), (1973, 1), (1974, 1), (1975, 1), (1976, 1), (1977, 1),
            (1978, 1), (1979, 1), (1980, 1), (1981, 7), (1982, 7), (1983, 7), (1985, 7),
            (1988, 1), (1990, 1), (1991, 1), (1992, 7), (1993, 7), (1994, 7), (1996, 1),
            (1997, 7), (1999, 1), (2006, 1), (2009, 1), (2012, 7), (2015, 7), (2017, 1),
        ];
        assert_eq!(dates.len(), LEAP_SECONDS.len());
        for ((y, m), &(unix, offset)) in dates.iter().zip(LEAP_SECONDS) {
            assert_eq!(Utc.with_ymd_and_hms(*y, *m, 1, 0, 0, 0).unwrap().timestamp(), unix, "{y}-{m}");
            assert_eq!(offset, 10 + LEAP_SECONDS.iter().position(|e| e.0 == unix).unwrap() as i64);
        }
    }

    #[test]
    fn ccsds_epoche_beruecksichtigt_schaltsekunden() {
        let t = CucTime::from_utc(&utc("2017-01-01T00:00:00Z"), CucFormat::default()).unwrap();
        let days = NaiveDate::from_ymd_opt(2017, 1, 1)
            .unwrap()
            .signed_duration_since(NaiveDate::from_ymd_opt(1958, 1, 1).unwrap())
            .num_days();
        assert_eq!(t.coarse as i64, days * 86_400 + 37);
        assert_eq!(t.fine, 0);
    }

    #[test]
    fn fine_time_als_binaerer_bruchteil() {
        let t: CucTime = "2026-09-26T12:00:00.5Z".parse().unwrap();
        assert_eq!(t.fine, 0x8000);
        let t: CucTime = "2026-09-26T12:00:00.25Z".parse().unwrap();
        assert_eq!(t.fine, 0x4000);
    }

    #[test]
    fn kodierung_mit_p_field() {
        let t = CucTime { format: CucFormat::default(), coarse: 0x0102_0304, fine: 0x0506 };
        assert_eq!(&t.to_bytes().unwrap()[..], &[0x1E, 1, 2, 3, 4, 5, 6]);
        assert_eq!(CucTime::from_bytes(&[0x1E, 1, 2, 3, 4, 5, 6], CucFormat::default()).unwrap(), t);
    }

    #[test]
    fn kodierung_ohne_p_field_und_andere_laengen() {
        let format = CucFormat { coarse_len: 2, fine_len: 1, p_field: false, ..CucFormat::default() };
        let t = CucTime { format, coarse: 0xABCD, fine: 0xEF };
        assert_eq!(&t.to_bytes().unwrap()[..], &[0xAB, 0xCD, 0xEF]);
        assert_eq!(CucTime::from_bytes(&[0xAB, 0xCD, 0xEF], format).unwrap(), t);
    }

    #[test]
    fn utc_roundtrip_innerhalb_der_aufloesung() {
        for s in ["2026-09-26T12:34:56.789123Z", "2016-12-31T23:59:59.999Z", "1999-01-01T00:00:00Z"] {
            let original = utc(s);
            let back = CucTime::from_utc(&original, CucFormat::default()).unwrap().to_utc().unwrap();
            assert!((back - original).abs() < Duration::microseconds(16), "{s}: {back}");
        }
    }

    #[test]
    fn verschiedene_eingabeformate_ergeben_dieselbe_zeit() {
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
    fn agency_epoche_ohne_schaltsekunden() {
        let format = CucFormat { epoch: CucEpoch::Agency(utc("2000-01-01T12:00:00Z")), ..CucFormat::default() };
        let t = CucTime::from_utc_str("2000-01-02T12:00:01.5Z", format).unwrap();
        assert_eq!((t.coarse, t.fine), (86_401, 0x8000));
        assert_eq!(t.to_bytes().unwrap()[0], 0x2E);
        assert_eq!(t.to_string(), "2000-01-02T12:00:01.500000Z");
        assert!(CucTime::from_utc_str("1999-12-31T00:00:00Z", format).is_err());
    }

    #[test]
    fn fehlerfaelle() {
        assert!(CucTime::from_utc_str("1970-01-01T00:00:00Z", CucFormat::default()).is_err());
        let small = CucFormat { coarse_len: 1, ..CucFormat::default() };
        assert!(CucTime::from_utc_str("2026-01-01T00:00:00Z", small).is_err());
        let err = CucTime::from_bytes(&[0x2E, 0, 0, 0, 0, 0, 0], CucFormat::default()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(CucTime::from_bytes(&[0x1E, 0, 0], CucFormat::default()).is_err());
    }

    #[test]
    fn now_liefert_aktuelle_zeit() {
        let t = CucTime::now(CucFormat::default()).unwrap();
        assert!((t.to_utc().unwrap() - Utc::now()).abs() < Duration::seconds(1));
    }
}
