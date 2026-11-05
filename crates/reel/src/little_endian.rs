//! Fixed-width little-endian reads out of a byte slice

/// Read a little-endian word
pub fn read_word(bytes: &[u8], at: usize) -> Option<u64> {
    let slice = bytes.get(at..at + size_of::<u64>())?;
    let mut word = [0; size_of::<u64>()];
    word.copy_from_slice(slice);
    Some(u64::from_le_bytes(word))
}

/// Read a little-endian half word
pub fn read_half(bytes: &[u8], at: usize) -> Option<u32> {
    let slice = bytes.get(at..at + size_of::<u32>())?;
    let mut half = [0; size_of::<u32>()];
    half.copy_from_slice(slice);
    Some(u32::from_le_bytes(half))
}
