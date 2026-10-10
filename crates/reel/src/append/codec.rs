//! Per-column payload compression, applied at admission and undone at read

use crate::format::column::Codec;
use crate::reel::payload;

/// A compressed payload opens with its logical length in this many little-endian bytes
const LOGICAL_PREFIX: usize = 4;

/// Payloads below this never attempt compression
const MIN_ATTEMPT: usize = 256;

/// Logical lengths past this are rejected as corruption at decode
const MAX_LOGICAL: usize = 1 << 30;

/// A kept compression must shrink the stored bytes by at least an eighth
fn worth_keeping(logical: usize, stored: usize) -> bool {
    stored <= logical - logical / 8
}

/// Compress a payload when the column asks, returning the bytes to store and the codec byte
pub fn admit(codec: Codec, payload: Vec<u8>) -> (Vec<u8>, u8) {
    if !matches!(codec, Codec::Lz4) || payload.len() < MIN_ATTEMPT {
        return (payload, 0);
    }

    let logical = payload.len();
    let ceiling = LOGICAL_PREFIX + lz4_flex::block::get_maximum_output_size(logical);

    // Allocated outside the pool, since this buffer leaves as the stored bytes
    let mut out = vec![0u8; ceiling];
    out[..LOGICAL_PREFIX].copy_from_slice(&(logical as u32).to_le_bytes());

    let Ok(written) = lz4_flex::block::compress_into(&payload, &mut out[LOGICAL_PREFIX..]) else {
        return (payload, 0);
    };

    let stored = LOGICAL_PREFIX + written;
    if !worth_keeping(logical, stored) {
        return (payload, 0);
    }

    out.truncate(stored);
    payload::give(payload);
    (out, Codec::Lz4.as_byte())
}

/// Decode a stored payload into a pooled buffer, or say it cannot be done
pub fn decode(codec_byte: u8, stored: &[u8]) -> Option<Vec<u8>> {
    if codec_byte != Codec::Lz4.as_byte() || stored.len() < LOGICAL_PREFIX {
        return None;
    }

    let logical = u32::from_le_bytes(stored[..LOGICAL_PREFIX].try_into().ok()?) as usize;
    if logical > MAX_LOGICAL {
        return None;
    }

    // Take it at full length, since growing it would zero bytes the codec overwrites anyway
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

    // a compressible payload shrinks, sets the codec byte, and round trips
    #[test]
    fn roundtrip_shrinks() {
        let raw = compressible(4096);
        let (stored, codec) = admit(Codec::Lz4, raw.clone());
        assert_eq!(codec, Codec::Lz4.as_byte());
        assert!(stored.len() < raw.len() - raw.len() / 8);
        assert_eq!(decode(codec, &stored).expect("decode"), raw);
    }

    // an incompressible payload stays raw with the byte at zero
    #[test]
    fn incompressible_stays_raw() {
        let raw = incompressible(4096);
        let (stored, codec) = admit(Codec::Lz4, raw.clone());
        assert_eq!(codec, 0);
        assert_eq!(stored, raw);
    }

    // a payload below the attempt floor is not even tried
    #[test]
    fn small_skips_the_attempt() {
        let raw = compressible(MIN_ATTEMPT - 1);
        let (stored, codec) = admit(Codec::Lz4, raw.clone());
        assert_eq!(codec, 0);
        assert_eq!(stored, raw);
    }

    // a column declaring nothing pays nothing
    #[test]
    fn no_codec_no_attempt() {
        let raw = compressible(4096);
        let (stored, codec) = admit(Codec::None, raw.clone());
        assert_eq!(codec, 0);
        assert_eq!(stored, raw);
    }

    // garbage codec bytes and lying prefixes read as corrupt without panicking
    #[test]
    fn decode_rejects_garbage() {
        assert!(decode(Codec::Lz4.as_byte(), &[1, 2]).is_none());
        assert!(decode(7, &compressible(64)).is_none());
        let mut lying = vec![0u8; 64];
        lying[..4].copy_from_slice(&(u32::MAX).to_le_bytes());
        assert!(decode(Codec::Lz4.as_byte(), &lying).is_none());
    }
}
