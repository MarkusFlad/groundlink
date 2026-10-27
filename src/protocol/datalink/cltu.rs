//! Communications Link Transmission Units with the (63,56) modified BCH
//! code, CCSDS 231.0-B-4 sections 3 and 5, without randomisation.
//!
//! A CLTU is the start sequence, codeblocks of 7 information octets and 1
//! octet with the complemented parity bits and a zero filler bit, and the
//! tail sequence. The last codeblock is filled up with `0x55`; the fill is
//! not removed when decoding, the transfer frame's length tells where the
//! frame ends.
//!
//! Decoding works in error-detecting mode: it stops at the first codeblock
//! with a parity error, as which the tail sequence is built to count.
//!
//! ```
//! use groundlink::protocol::datalink::cltu;
//!
//! let encoded = cltu::encode(b"hello");
//! assert_eq!(encoded.len(), 2 + 8 + 8);
//! assert_eq!(&cltu::decode(&encoded).unwrap()[..], b"hello\x55\x55");
//! ```

use bytes::{BufMut, Bytes, BytesMut};

use super::FrameError;

/// The start sequence for BCH coding.
pub const START_SEQUENCE: [u8; 2] = [0xeb, 0x90];
/// The tail sequence for BCH coding.
pub const TAIL_SEQUENCE: [u8; 8] = [0xc5, 0xc5, 0xc5, 0xc5, 0xc5, 0xc5, 0xc5, 0x79];
/// Information octets per codeblock.
pub const INFORMATION_LEN: usize = 7;
/// Octets per codeblock.
pub const CODEBLOCK_LEN: usize = 8;
/// The octet that fills up the last codeblock: alternating bits, starting
/// with zero.
pub const FILL: u8 = 0x55;

/// g(x) = x⁷ + x⁶ + x² + 1 without the x⁷ term.
const GENERATOR: u8 = 0b100_0101;

/// The last octet of a codeblock: the complemented parity bits of `info`
/// followed by the zero filler bit.
fn parity_octet(info: &[u8]) -> u8 {
    let mut register: u8 = 0;
    for byte in info {
        for i in (0..8).rev() {
            let feedback = ((byte >> i) & 1) ^ ((register >> 6) & 1);
            register = (register << 1) & 0x7f;
            if feedback != 0 {
                register ^= GENERATOR;
            }
        }
    }
    (!register & 0x7f) << 1
}

/// Encodes `data` (normally one TC Transfer Frame) as a CLTU.
pub fn encode(data: &[u8]) -> Bytes {
    let blocks = data.len().div_ceil(INFORMATION_LEN).max(1);
    let mut cltu = BytesMut::with_capacity(START_SEQUENCE.len() + blocks * CODEBLOCK_LEN + TAIL_SEQUENCE.len());
    cltu.put_slice(&START_SEQUENCE);
    for block in 0..blocks {
        let mut info = [FILL; INFORMATION_LEN];
        let chunk = &data[(block * INFORMATION_LEN).min(data.len())..((block + 1) * INFORMATION_LEN).min(data.len())];
        info[..chunk.len()].copy_from_slice(chunk);
        cltu.put_slice(&info);
        cltu.put_u8(parity_octet(&info));
    }
    cltu.put_slice(&TAIL_SEQUENCE);
    cltu.freeze()
}

/// Decodes a CLTU: the information octets of the codeblocks up to the
/// first one with a parity error (normally the tail sequence) or the end of
/// the data, fill included.
pub fn decode(cltu: &[u8]) -> Result<Bytes, FrameError> {
    let codeblocks = cltu.strip_prefix(&START_SEQUENCE).ok_or(FrameError::NoStartSequence)?;
    let mut data = BytesMut::with_capacity(codeblocks.len() / CODEBLOCK_LEN * INFORMATION_LEN);
    for block in codeblocks.as_chunks::<CODEBLOCK_LEN>().0 {
        let (info, parity) = block.split_at(INFORMATION_LEN);
        if parity_octet(info) != parity[0] {
            break;
        }
        data.put_slice(info);
    }
    if data.is_empty() {
        return Err(FrameError::NoCodeblock);
    }
    Ok(data.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The remainder of the 63-bit codeword `info · x⁷ + parity` divided by
    /// g(x), by long division.
    fn remainder(info: &[u8], parity: u8) -> u64 {
        let mut word: u64 = 0;
        for byte in info {
            word = (word << 8) | u64::from(*byte);
        }
        word = (word << 7) | u64::from(parity);
        let generator: u64 = 0b1100_0101;
        for bit in (7..63).rev() {
            if word & (1 << bit) != 0 {
                word ^= generator << (bit - 7);
            }
        }
        word
    }

    #[test]
    fn codeblocks_are_codewords_of_the_bch_code() {
        for info in [
            [0u8; 7],
            [0xff; 7],
            [1, 2, 3, 4, 5, 6, 7],
            [0x55; 7],
            [0x80, 0, 0, 0, 0, 0, 1],
        ] {
            let octet = parity_octet(&info);
            assert_eq!(octet & 1, 0, "filler bit");
            let parity = !(octet >> 1) & 0x7f;
            assert_eq!(remainder(&info, parity), 0, "{info:?}");
        }
        // All zeros: all parity bits 0, complemented 1.
        assert_eq!(parity_octet(&[0; 7]), 0xfe);
    }

    #[test]
    fn the_tail_sequence_is_not_a_codeblock() {
        assert_ne!(parity_octet(&TAIL_SEQUENCE[..7]), TAIL_SEQUENCE[7]);
    }

    #[test]
    fn encode_and_decode() {
        let data: Vec<u8> = (0..20).collect();
        let cltu = encode(&data);
        assert_eq!(cltu.len(), 2 + 3 * 8 + 8);
        assert_eq!(&cltu[cltu.len() - 8..], &TAIL_SEQUENCE);
        assert_eq!(&cltu[2 + 16 + 6..2 + 16 + 7], &[FILL]);
        let decoded = decode(&cltu).unwrap();
        assert_eq!(&decoded[..20], &data[..]);
        assert_eq!(&decoded[20..], &[FILL]);
    }

    #[test]
    fn decoding_stops_at_a_corrupted_codeblock() {
        let mut cltu = encode(&[0xaa; 21]).to_vec();
        cltu[2 + 8 + 3] ^= 0x10;
        assert_eq!(decode(&cltu).unwrap().len(), 7);
        cltu[2 + 3] ^= 0x01;
        assert_eq!(decode(&cltu), Err(FrameError::NoCodeblock));
        assert_eq!(decode(&cltu[1..]), Err(FrameError::NoStartSequence));
    }
}
