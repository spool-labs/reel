//! Per-column payload compression, applied at admission and undone at read
//!
//! The record header's codec byte says what happened, so the read side needs no column
//! context: zero is raw, nonzero names the codec that produced the stored bytes. The
//! header's length field and the checksum both keep meaning the stored bytes, so scrub,
//! recovery and compaction never decompress.

use crate::format::column::Codec;
use crate::reel::payload;

/// Bytes the logical length prefix takes ahead of the codec bytes
///
/// A stored compressed payload opens with its logical length, little endian, so a read
/// can size the output buffer exactly.
const LOGICAL_PREFIX: usize = 4;

/// Payloads below this never attempt compression
///
/// A payload this small cannot repay the codec byte and the length prefix even when it
/// shrinks, and the attempt itself costs a pass over the bytes.
const MIN_ATTEMPT: usize = 256;

/// Logical lengths past this are rejected as corruption at decode
///
/// A record is bounded by its segment, so a prefix claiming more is a lie behind a
/// checksum that happened to pass, and the read reports corrupt rather than allocating.
const MAX_LOGICAL: usize = 1 << 30;

/// A kept compression must shrink the stored bytes by at least an eighth
///
/// Anything less trades a decode on every future read for noise.
fn worth_keeping(logical: usize, stored: usize) -> bool {
    stored <= logical - logical / 8
}

/// Compress a payload at admission when the column asks and the payload cooperates
///
/// Returns the bytes to store and the codec byte for the header, raw with the byte at
/// zero unless the payload shrinks by an eighth. A form that would fit the column's
/// inline ceiling stays raw too: the inline paths serve stored bytes as the value, so a
/// record they could serve must never carry codec bytes.
pub fn admit(codec: Codec, inline_max: u16, payload: Vec<u8>) -> (Vec<u8>, u8) {
    if !matches!(codec, Codec::Lz4) || payload.len() < MIN_ATTEMPT {
        return (payload, 0);
    }

    let logical = payload.len();
    let ceiling = LOGICAL_PREFIX + lz4_flex::block::get_maximum_output_size(logical);

    // Sized exactly and outside the pool: this buffer leaves as the stored bytes, so
    // pooling it would take a buffer out of this thread's reads and put nothing back.
    let mut out = vec![0u8; ceiling];
    out[..LOGICAL_PREFIX].copy_from_slice(&(logical as u32).to_le_bytes());

    let Ok(written) = lz4_flex::block::compress_into(&payload, &mut out[LOGICAL_PREFIX..]) else {
        return (payload, 0);
    };

    let stored = LOGICAL_PREFIX + written;
    if !worth_keeping(logical, stored) || stored <= inline_max as usize {
        return (payload, 0);
    }

    out.truncate(stored);
    payload::give(payload);
    (out, Codec::Lz4.as_byte())
}

/// Decode a stored payload into a pooled buffer, or say it cannot be done
///
/// Nothing here distinguishes an unknown codec byte from codec bytes that do not decode:
/// both are corruption wearing a valid checksum.
pub fn decode(codec_byte: u8, stored: &[u8]) -> Option<Vec<u8>> {
    if codec_byte != Codec::Lz4.as_byte() || stored.len() < LOGICAL_PREFIX {
        return None;
    }

    let logical = u32::from_le_bytes(stored[..LOGICAL_PREFIX].try_into().ok()?) as usize;
    if logical > MAX_LOGICAL {
        return None;
    }

    // Taken at its length rather than empty and grown, since growing it zeroes the whole
    // output before the codec overwrites every byte of it.
    let mut out = payload::take_written(logical);
    match lz4_flex::block::decompress_into(&stored[LOGICAL_PREFIX..], &mut out) {
        Ok(written) if written == logical => Some(out),
        _ => {
            payload::give(out);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compressible(len: usize) -> Vec<u8> {
        (0..len).map(|at| (at / 64) as u8).collect()
    }

    fn incompressible(len: usize) -> Vec<u8> {
        let mut state = 0x2545F4914F6CDD1Du64;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect()
    }

    // a compressible payload shrinks, carries the codec byte, and round trips
    #[test]
    fn roundtrip_shrinks() {
        let raw = compressible(4096);
        let (stored, codec) = admit(Codec::Lz4, 0, raw.clone());
        assert_eq!(codec, Codec::Lz4.as_byte());
        assert!(stored.len() < raw.len() - raw.len() / 8);
        assert_eq!(decode(codec, &stored).expect("decode"), raw);
    }

    // an incompressible payload stays raw with the byte at zero
    #[test]
    fn incompressible_stays_raw() {
        let raw = incompressible(4096);
        let (stored, codec) = admit(Codec::Lz4, 0, raw.clone());
        assert_eq!(codec, 0);
        assert_eq!(stored, raw);
    }

    // a payload below the attempt floor is not even tried
    #[test]
    fn small_skips_the_attempt() {
        let raw = compressible(MIN_ATTEMPT - 1);
        let (stored, codec) = admit(Codec::Lz4, 0, raw.clone());
        assert_eq!(codec, 0);
        assert_eq!(stored, raw);
    }

    // a record that would shrink into the inline ceiling stays raw instead
    #[test]
    fn inline_ceiling_refuses_codec_bytes() {
        let raw = compressible(300);
        let shrunk = admit(Codec::Lz4, 0, raw.clone());
        assert!(
            shrunk.1 != 0 && shrunk.0.len() <= 255,
            "premise: 300 compressible bytes shrink under 255"
        );
        let (stored, codec) = admit(Codec::Lz4, 255, raw.clone());
        assert_eq!(codec, 0);
        assert_eq!(stored, raw);
    }

    // a column declaring nothing pays nothing
    #[test]
    fn no_codec_no_attempt() {
        let raw = compressible(4096);
        let (stored, codec) = admit(Codec::None, 0, raw.clone());
        assert_eq!(codec, 0);
        assert_eq!(stored, raw);
    }

    // garbage codec bytes and lying prefixes read as corrupt, not as panics
    #[test]
    fn decode_rejects_garbage() {
        assert!(decode(Codec::Lz4.as_byte(), &[1, 2]).is_none());
        assert!(decode(7, &compressible(64)).is_none());
        let mut lying = vec![0u8; 64];
        lying[..4].copy_from_slice(&(u32::MAX).to_le_bytes());
        assert!(decode(Codec::Lz4.as_byte(), &lying).is_none());
    }
}
