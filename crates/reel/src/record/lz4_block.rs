//! The reference lz4 library, with the state held by the caller so a chunk allocates nothing

use std::os::raw::{c_char, c_int, c_void};

use lz4_sys::LZ4_decompress_safe;

/// The library's default speed setting
pub const ACCELERATION: c_int = 1;

unsafe extern "C" {
    fn LZ4_sizeofState() -> c_int;
    fn LZ4_compress_fast_extState(
        state: *mut c_void,
        source: *const c_char,
        destination: *mut c_char,
        source_size: c_int,
        destination_capacity: c_int,
        acceleration: c_int,
    ) -> c_int;
}

/// The compressor's working memory, allocated once
#[derive(Debug)]
pub struct Lz4State {
    words: Vec<u64>,
}

impl Lz4State {
    /// Allocate the state, in words because the library wants it aligned
    pub fn new() -> Self {
        // SAFETY: the call takes no arguments and reads no memory of ours
        let bytes = unsafe { LZ4_sizeofState() };
        let words = (bytes as usize).div_ceil(size_of::<u64>());
        Self { words: vec![0; words] }
    }

    /// Compress one chunk, or return nothing when it doesn't shrink
    pub fn compress(&mut self, input: &[u8], output: &mut [u8]) -> Option<usize> {
        let capacity = input.len().min(output.len()).saturating_sub(1);
        if capacity == 0 {
            return None;
        }

        // SAFETY: the state fits LZ4_sizeofState, and both spans stay valid for their stated lengths
        let stored = unsafe {
            LZ4_compress_fast_extState(
                self.words.as_mut_ptr().cast::<c_void>(),
                input.as_ptr().cast::<c_char>(),
                output.as_mut_ptr().cast::<c_char>(),
                input.len() as c_int,
                capacity as c_int,
                ACCELERATION,
            )
        };
        if stored <= 0 {
            return None;
        }
        Some(stored as usize)
    }
}

impl Default for Lz4State {
    fn default() -> Self {
        Self::new()
    }
}

/// Decompress one chunk and return its length, or nothing when the block is malformed
pub fn decompress(input: &[u8], output: &mut [u8]) -> Option<usize> {
    // SAFETY: both spans stay valid for their stated lengths, and the call never writes past output
    let length = unsafe {
        LZ4_decompress_safe(
            input.as_ptr().cast::<c_char>(),
            output.as_mut_ptr().cast::<c_char>(),
            input.len() as c_int,
            output.len() as c_int,
        )
    };
    if length < 0 {
        return None;
    }
    Some(length as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::codec::CHUNK_BYTES;

    fn pattern(length: usize) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(length);
        for index in 0..length {
            bytes.push((index % 17) as u8);
        }
        bytes
    }

    fn noise(length: usize) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(length);
        let mut state: u64 = 1;
        for _ in 0..length {
            state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            bytes.push((state >> 33) as u8);
        }
        bytes
    }

    // repeating data shrinks and comes back whole
    #[test]
    fn round_trip() {
        let input = pattern(CHUNK_BYTES);
        let mut state = Lz4State::new();
        let mut stored = vec![0; CHUNK_BYTES];
        let mut restored = vec![0; CHUNK_BYTES];

        let length = state.compress(&input, &mut stored).expect("shrinks");
        let restored_length = decompress(&stored[..length], &mut restored).expect("valid block");

        assert!(length < input.len());
        assert_eq!(restored_length, input.len());
        assert_eq!(restored, input);
    }

    // noise and empty input don't shrink, so the chunk is stored as written
    #[test]
    fn no_shrink() {
        let mut state = Lz4State::new();
        let mut stored = vec![0; CHUNK_BYTES];

        assert_eq!(state.compress(&noise(CHUNK_BYTES), &mut stored), None);
        assert_eq!(state.compress(&[], &mut stored), None);
    }

    // a malformed block is refused
    #[test]
    fn bad_block() {
        let mut restored = vec![0; 16];

        assert_eq!(decompress(&[0xFF, 0xFF, 0xFF], &mut restored), None);
    }
}
