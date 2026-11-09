//! The reference lz4 library, with the state held by the caller so a chunk allocates nothing

use std::os::raw::{c_char, c_int, c_void};

use lz4_sys::LZ4_decompress_safe;

use crate::pool::touched_capacity;

/// The library's default speed setting
pub const ACCELERATION: c_int = 1;
/// The longest input the library takes, LZ4_MAX_INPUT_SIZE in lz4.h. A longer length would also wrap the C int it passes as
pub const MOST_INPUT_BYTES: usize = 0x7E00_0000;

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
        let mut state = touched_capacity(words, 0);
        state.resize(words, 0);
        Self { words: state }
    }

    /// Compress one chunk, or return nothing when it doesn't shrink
    pub fn compress(&mut self, input: &[u8], output: &mut [u8]) -> Option<usize> {
        let capacity = input.len().min(output.len()).saturating_sub(1);
        if capacity == 0 || input.len() > MOST_INPUT_BYTES {
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
    if input.len() > MOST_INPUT_BYTES {
        return None;
    }
    // no block decompresses past the most input, and a longer output length would wrap its C int
    let reach = output.len().min(c_int::MAX as usize);
    let output = &mut output[..reach];
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
