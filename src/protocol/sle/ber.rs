//! Minimal BER (ITU-T X.690) encoder and decoder for the ASN.1 subset of
//! the SLE PDUs (CCSDS 911.1, 912.1, 913.1).
//!
//! Supports definite lengths only (SLE implementations do not use the
//! indefinite form) and the types the SLE PDUs need: INTEGER, NULL, OCTET
//! STRING, VisibleString, OBJECT IDENTIFIER, SEQUENCE/SET and
//! context-specific tags, including high tag numbers such as `[100]`.
//!
//! Decoded OCTET STRINGs are zero-copy slices of the input [`Bytes`].
//!
//! ```
//! use bytes::Bytes;
//! use groundlink::protocol::sle::ber::{BerReader, BerWriter, Tag};
//!
//! let mut w = BerWriter::new();
//! w.constructed(Tag::SEQUENCE, |w| {
//!     w.integer(Tag::INTEGER, 128);
//!     w.null(Tag::ctx(0));
//! });
//! let bytes = w.into_bytes();
//! assert_eq!(&bytes[..], &[0x30, 0x06, 0x02, 0x02, 0x00, 0x80, 0x80, 0x00]);
//!
//! let mut r = BerReader::new(bytes);
//! let value = r
//!     .sequence(Tag::SEQUENCE, |r| {
//!         let v = r.integer(Tag::INTEGER)?;
//!         r.null(Tag::ctx(0))?;
//!         Ok(v)
//!     })
//!     .unwrap();
//! assert_eq!(value, 128);
//! ```

use std::fmt;

use bytes::Bytes;

/// The class of a BER tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Class {
    /// Types defined by X.680, e.g. INTEGER.
    Universal = 0,
    /// Application-wide tags.
    Application = 1,
    /// Context-specific tags `[n]`, used throughout the SLE PDUs.
    Context = 2,
    /// Private tags.
    Private = 3,
}

/// A BER identifier: class, primitive or constructed form, and tag number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Tag {
    /// The class.
    pub class: Class,
    /// Whether the content consists of further BER elements.
    pub constructed: bool,
    /// The tag number.
    pub number: u32,
}

impl Tag {
    /// INTEGER.
    pub const INTEGER: Tag = Tag::universal(2, false);
    /// OCTET STRING.
    pub const OCTET_STRING: Tag = Tag::universal(4, false);
    /// NULL.
    pub const NULL: Tag = Tag::universal(5, false);
    /// OBJECT IDENTIFIER.
    pub const OID: Tag = Tag::universal(6, false);
    /// SEQUENCE and SEQUENCE OF.
    pub const SEQUENCE: Tag = Tag::universal(16, true);
    /// SET and SET OF.
    pub const SET: Tag = Tag::universal(17, true);
    /// VisibleString.
    pub const VISIBLE_STRING: Tag = Tag::universal(26, false);

    /// A universal tag.
    pub const fn universal(number: u32, constructed: bool) -> Tag {
        Tag {
            class: Class::Universal,
            constructed,
            number,
        }
    }

    /// Context-specific primitive tag `[n]`.
    pub const fn ctx(number: u32) -> Tag {
        Tag {
            class: Class::Context,
            constructed: false,
            number,
        }
    }

    /// Context-specific constructed tag `[n]`.
    pub const fn ctx_cons(number: u32) -> Tag {
        Tag {
            class: Class::Context,
            constructed: true,
            number,
        }
    }
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let form = if self.constructed { "constructed" } else { "primitive" };
        match self.class {
            Class::Context => write!(f, "[{}] {form}", self.number),
            Class::Universal => write!(f, "[UNIVERSAL {}] {form}", self.number),
            Class::Application => write!(f, "[APPLICATION {}] {form}", self.number),
            Class::Private => write!(f, "[PRIVATE {}] {form}", self.number),
        }
    }
}

/// Why BER data could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BerError {
    /// The data ends inside an element.
    Truncated,
    /// An element uses the indefinite length form.
    IndefiniteLength,
    /// A length does not fit into 4 octets.
    LengthOverflow,
    /// An element has another tag than the one required at this point.
    UnexpectedTag {
        /// The tag required.
        expected: Tag,
        /// The tag found.
        found: Tag,
    },
    /// A tag that is none of the alternatives of a CHOICE.
    UnknownChoice(Tag),
    /// An INTEGER does not fit into the target type.
    IntegerOverflow,
    /// A value that is not allowed; the text says which.
    InvalidValue(&'static str),
    /// Data left over after the last element of a constructed element.
    TrailingData,
}

impl fmt::Display for BerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BerError::Truncated => f.write_str("BER data truncated"),
            BerError::IndefiniteLength => f.write_str("indefinite length not supported"),
            BerError::LengthOverflow => f.write_str("BER length too large"),
            BerError::UnexpectedTag { expected, found } => write!(f, "expected tag {expected}, found {found}"),
            BerError::UnknownChoice(t) => write!(f, "unknown CHOICE alternative {t}"),
            BerError::IntegerOverflow => f.write_str("INTEGER out of range"),
            BerError::InvalidValue(what) => write!(f, "invalid value: {what}"),
            BerError::TrailingData => f.write_str("unexpected trailing data"),
        }
    }
}

impl std::error::Error for BerError {}

/// Result of a BER decoding step.
pub type BerResult<T> = Result<T, BerError>;

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// Writes BER elements into a growing buffer.
#[derive(Debug, Default)]
pub struct BerWriter {
    buf: Vec<u8>,
}

impl BerWriter {
    /// An empty writer.
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty writer with room for `capacity` octets.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
        }
    }

    /// The octets written so far.
    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    /// The octets written, as [`Bytes`].
    pub fn into_bytes(self) -> Bytes {
        Bytes::from(self.buf)
    }

    fn put_tag(&mut self, tag: Tag) {
        let lead = ((tag.class as u8) << 6) | if tag.constructed { 0x20 } else { 0 };
        if tag.number < 31 {
            self.buf.push(lead | tag.number as u8);
        } else {
            self.buf.push(lead | 0x1f);
            self.put_base128(tag.number);
        }
    }

    fn put_base128(&mut self, mut v: u32) {
        let mut tmp = [0u8; 5];
        let mut i = tmp.len();
        loop {
            i -= 1;
            tmp[i] = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                break;
            }
        }
        let last = tmp.len() - 1;
        for (k, b) in tmp.iter().enumerate().skip(i) {
            self.buf.push(if k == last { *b } else { *b | 0x80 });
        }
    }

    fn put_len(&mut self, len: usize) {
        if len < 0x80 {
            self.buf.push(len as u8);
        } else {
            let bytes = len.to_be_bytes();
            let skip = bytes.iter().take_while(|b| **b == 0).count();
            self.buf.push(0x80 | (bytes.len() - skip) as u8);
            self.buf.extend_from_slice(&bytes[skip..]);
        }
    }

    /// Writes a primitive element with the given content.
    pub fn primitive(&mut self, tag: Tag, content: &[u8]) {
        self.put_tag(tag);
        self.put_len(content.len());
        self.buf.extend_from_slice(content);
    }

    /// Writes a constructed element whose content is produced by `f`.
    pub fn constructed(&mut self, tag: Tag, f: impl FnOnce(&mut BerWriter)) {
        self.put_tag(tag);
        let len_pos = self.buf.len();
        self.buf.push(0);
        let start = self.buf.len();
        f(self);
        let len = self.buf.len() - start;
        if len < 0x80 {
            self.buf[len_pos] = len as u8;
        } else {
            let bytes = len.to_be_bytes();
            let skip = bytes.iter().take_while(|b| **b == 0).count();
            self.buf[len_pos] = 0x80 | (bytes.len() - skip) as u8;
            self.buf.splice(start..start, bytes[skip..].iter().copied());
        }
    }

    /// Writes an INTEGER in the minimal number of octets.
    pub fn integer(&mut self, tag: Tag, v: i64) {
        let b = v.to_be_bytes();
        let mut i = 0;
        while i < 7 && ((b[i] == 0x00 && b[i + 1] & 0x80 == 0) || (b[i] == 0xff && b[i + 1] & 0x80 != 0)) {
            i += 1;
        }
        self.primitive(tag, &b[i..]);
    }

    /// Writes a NULL.
    pub fn null(&mut self, tag: Tag) {
        self.primitive(tag, &[]);
    }

    /// Writes an OCTET STRING.
    pub fn octets(&mut self, tag: Tag, data: &[u8]) {
        self.primitive(tag, data);
    }

    /// Writes a VisibleString. The caller makes sure `s` contains only
    /// visible ASCII characters.
    pub fn visible_string(&mut self, tag: Tag, s: &str) {
        self.primitive(tag, s.as_bytes());
    }

    /// Writes an OBJECT IDENTIFIER with the given arcs.
    pub fn oid(&mut self, tag: Tag, arcs: &[u32]) {
        let mut content = BerWriter::new();
        if arcs.len() >= 2 {
            content.put_base128(arcs[0] * 40 + arcs[1]);
            for a in &arcs[2..] {
                content.put_base128(*a);
            }
        }
        self.primitive(tag, &content.buf);
    }
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

/// Reads BER elements one after the other from a buffer.
#[derive(Debug, Clone)]
pub struct BerReader {
    data: Bytes,
    pos: usize,
}

impl BerReader {
    /// A reader over `data`.
    pub fn new(data: Bytes) -> Self {
        Self { data, pos: 0 }
    }

    /// Whether all elements have been read.
    pub fn is_empty(&self) -> bool {
        self.pos >= self.data.len()
    }

    /// Returns (tag, header length, content length) of the next element.
    fn header(&self) -> BerResult<(Tag, usize, usize)> {
        let d = &self.data[self.pos..];
        let first = *d.first().ok_or(BerError::Truncated)?;
        let class = match first >> 6 {
            0 => Class::Universal,
            1 => Class::Application,
            2 => Class::Context,
            _ => Class::Private,
        };
        let constructed = first & 0x20 != 0;
        let mut i = 1;
        let mut number = u32::from(first & 0x1f);
        if number == 0x1f {
            number = 0;
            loop {
                let b = *d.get(i).ok_or(BerError::Truncated)?;
                i += 1;
                if number > (u32::MAX >> 7) {
                    return Err(BerError::InvalidValue("tag number too large"));
                }
                number = (number << 7) | u32::from(b & 0x7f);
                if b & 0x80 == 0 {
                    break;
                }
            }
        }
        let l = *d.get(i).ok_or(BerError::Truncated)?;
        i += 1;
        let len = if l < 0x80 {
            usize::from(l)
        } else if l == 0x80 {
            return Err(BerError::IndefiniteLength);
        } else {
            let n = usize::from(l & 0x7f);
            if n > 4 {
                return Err(BerError::LengthOverflow);
            }
            let mut len = 0usize;
            for _ in 0..n {
                len = (len << 8) | usize::from(*d.get(i).ok_or(BerError::Truncated)?);
                i += 1;
            }
            len
        };
        if d.len() - i < len {
            return Err(BerError::Truncated);
        }
        Ok((
            Tag {
                class,
                constructed,
                number,
            },
            i,
            len,
        ))
    }

    /// The tag of the next element, without reading it.
    pub fn peek_tag(&self) -> BerResult<Tag> {
        self.header().map(|(t, _, _)| t)
    }

    /// Reads the next element regardless of its tag; returns the tag and
    /// the content.
    pub fn read_any(&mut self) -> BerResult<(Tag, Bytes)> {
        let (tag, hdr, len) = self.header()?;
        let start = self.pos + hdr;
        let content = self.data.slice(start..start + len);
        self.pos = start + len;
        Ok((tag, content))
    }

    /// Reads the content of the next element, which must carry `expected`.
    pub fn read(&mut self, expected: Tag) -> BerResult<Bytes> {
        let found = self.peek_tag()?;
        if found != expected {
            return Err(BerError::UnexpectedTag { expected, found });
        }
        Ok(self.read_any()?.1)
    }

    /// A reader over the content of the next element, which must carry
    /// `tag`.
    pub fn constructed(&mut self, tag: Tag) -> BerResult<BerReader> {
        Ok(BerReader::new(self.read(tag)?))
    }

    /// Reads a constructed element and parses its whole content with `f`;
    /// content left over is an error.
    pub fn sequence<T>(&mut self, tag: Tag, f: impl FnOnce(&mut BerReader) -> BerResult<T>) -> BerResult<T> {
        let mut inner = self.constructed(tag)?;
        let value = f(&mut inner)?;
        inner.finish()?;
        Ok(value)
    }

    /// Reads an INTEGER.
    pub fn integer(&mut self, tag: Tag) -> BerResult<i64> {
        decode_integer(&self.read(tag)?)
    }

    /// Reads an INTEGER and converts it to a narrower or unsigned type.
    pub fn integer_as<T: TryFrom<i64>>(&mut self, tag: Tag) -> BerResult<T> {
        T::try_from(self.integer(tag)?).map_err(|_| BerError::IntegerOverflow)
    }

    /// Reads a NULL.
    pub fn null(&mut self, tag: Tag) -> BerResult<()> {
        if self.read(tag)?.is_empty() {
            Ok(())
        } else {
            Err(BerError::InvalidValue("NULL with content"))
        }
    }

    /// Reads an OCTET STRING, as a slice of the input.
    pub fn octets(&mut self, tag: Tag) -> BerResult<Bytes> {
        self.read(tag)
    }

    /// Reads a VisibleString.
    pub fn visible_string(&mut self, tag: Tag) -> BerResult<String> {
        let b = self.read(tag)?;
        if !b.iter().all(|c| (0x20..=0x7e).contains(c)) {
            return Err(BerError::InvalidValue("VisibleString with invalid character"));
        }
        String::from_utf8(b.to_vec()).map_err(|_| BerError::InvalidValue("VisibleString"))
    }

    /// Reads an OBJECT IDENTIFIER and returns its arcs.
    pub fn oid(&mut self, tag: Tag) -> BerResult<Vec<u32>> {
        let b = self.read(tag)?;
        let mut sub = Vec::new();
        let mut v: u32 = 0;
        for (i, byte) in b.iter().enumerate() {
            if v > (u32::MAX >> 7) {
                return Err(BerError::InvalidValue("OID arc too large"));
            }
            v = (v << 7) | u32::from(byte & 0x7f);
            if byte & 0x80 == 0 {
                sub.push(v);
                v = 0;
            } else if i + 1 == b.len() {
                return Err(BerError::Truncated);
            }
        }
        let first = *sub.first().ok_or(BerError::InvalidValue("empty OID"))?;
        let (a0, a1) = match first {
            0..=39 => (0, first),
            40..=79 => (1, first - 40),
            _ => (2, first - 80),
        };
        let mut arcs = vec![a0, a1];
        arcs.extend_from_slice(&sub[1..]);
        Ok(arcs)
    }

    /// Checks that all elements have been read.
    pub fn finish(&self) -> BerResult<()> {
        if self.is_empty() {
            Ok(())
        } else {
            Err(BerError::TrailingData)
        }
    }
}

fn decode_integer(c: &[u8]) -> BerResult<i64> {
    if c.is_empty() {
        return Err(BerError::InvalidValue("empty INTEGER"));
    }
    if c.len() > 8 {
        return Err(BerError::IntegerOverflow);
    }
    let mut v: i64 = if c[0] & 0x80 != 0 { -1 } else { 0 };
    for b in c {
        v = (v << 8) | i64::from(*b);
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(f: impl FnOnce(&mut BerWriter)) -> Vec<u8> {
        let mut w = BerWriter::new();
        f(&mut w);
        w.as_slice().to_vec()
    }

    #[test]
    fn integers_are_minimal() {
        for (v, expect) in [
            (0i64, vec![0x02, 1, 0x00]),
            (127, vec![0x02, 1, 0x7f]),
            (128, vec![0x02, 2, 0x00, 0x80]),
            (-1, vec![0x02, 1, 0xff]),
            (-128, vec![0x02, 1, 0x80]),
            (-129, vec![0x02, 2, 0xff, 0x7f]),
            (65535, vec![0x02, 3, 0x00, 0xff, 0xff]),
        ] {
            let b = enc(|w| w.integer(Tag::INTEGER, v));
            assert_eq!(b, expect, "value {v}");
            let mut r = BerReader::new(Bytes::from(b));
            assert_eq!(r.integer(Tag::INTEGER).unwrap(), v);
        }
        for v in [i64::MIN, i64::MAX, 1 << 40, -(1 << 33)] {
            let mut r = BerReader::new(Bytes::from(enc(|w| w.integer(Tag::INTEGER, v))));
            assert_eq!(r.integer(Tag::INTEGER).unwrap(), v);
        }
    }

    #[test]
    fn high_tag_numbers() {
        assert_eq!(enc(|w| w.constructed(Tag::ctx_cons(100), |_| {})), [0xbf, 0x64, 0x00]);
        assert_eq!(enc(|w| w.integer(Tag::ctx(104), 3)), [0x9f, 0x68, 0x01, 0x03]);
        let b = enc(|w| w.null(Tag::ctx(300)));
        assert_eq!(b, [0x9f, 0x82, 0x2c, 0x00]);
        assert_eq!(BerReader::new(Bytes::from(b)).peek_tag().unwrap(), Tag::ctx(300));
    }

    #[test]
    fn long_lengths_in_constructed() {
        let data = vec![0x55u8; 300];
        let b = enc(|w| w.constructed(Tag::SEQUENCE, |w| w.octets(Tag::OCTET_STRING, &data)));
        assert_eq!(&b[..8], &[0x30, 0x82, 0x01, 0x30, 0x04, 0x82, 0x01, 0x2c]);
        let mut r = BerReader::new(Bytes::from(b));
        let got = r.sequence(Tag::SEQUENCE, |r| r.octets(Tag::OCTET_STRING)).unwrap();
        assert_eq!(&got[..], &data[..]);
        assert!(r.is_empty());
    }

    #[test]
    fn oid_roundtrip() {
        let arcs = [1, 3, 112, 4, 3, 1, 2, 52];
        let b = enc(|w| w.oid(Tag::OID, &arcs));
        assert_eq!(b, [0x06, 0x07, 0x2b, 0x70, 0x04, 0x03, 0x01, 0x02, 0x34]);
        assert_eq!(BerReader::new(Bytes::from(b)).oid(Tag::OID).unwrap(), arcs);
    }

    #[test]
    fn decoder_errors() {
        let r = |b: &'static [u8]| BerReader::new(Bytes::from_static(b));
        assert_eq!(
            r(&[0x30, 0x80, 0, 0]).read_any().unwrap_err(),
            BerError::IndefiniteLength
        );
        assert_eq!(r(&[0x04, 0x05, 1, 2]).read_any().unwrap_err(), BerError::Truncated);
        assert!(matches!(
            r(&[0x05, 0x00]).integer(Tag::INTEGER),
            Err(BerError::UnexpectedTag { .. })
        ));
        assert_eq!(
            r(&[0x05, 0x01, 0x00]).null(Tag::NULL).unwrap_err(),
            BerError::InvalidValue("NULL with content")
        );
        assert_eq!(
            r(&[0x30, 0x03, 0x05, 0x00])
                .sequence(Tag::SEQUENCE, |r| r.null(Tag::NULL))
                .unwrap_err(),
            BerError::Truncated
        );
        assert_eq!(
            r(&[0x30, 0x03, 0x05, 0x00, 0x05])
                .sequence(Tag::SEQUENCE, |r| r.null(Tag::NULL))
                .unwrap_err(),
            BerError::TrailingData
        );
    }
}
