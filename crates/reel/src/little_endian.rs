//! Fixed-width little-endian reads out of a byte slice

/// Read a little-endian word
pub fn read_word(bytes: &[u8], at: usize) -> Option<u64> {
    let slice = bytes.get(at..at.checked_add(size_of::<u64>())?)?;
    let mut word = [0; size_of::<u64>()];
    word.copy_from_slice(slice);
    Some(u64::from_le_bytes(word))
}

/// Read a little-endian half word
pub fn read_half(bytes: &[u8], at: usize) -> Option<u32> {
    let slice = bytes.get(at..at.checked_add(size_of::<u32>())?)?;
    let mut half = [0; size_of::<u32>()];
    half.copy_from_slice(slice);
    Some(u32::from_le_bytes(half))
}

/// Fields taken off the front of some stored bytes in order. A length read from the bytes themselves can't run a take past
/// their end, since each take splits what's left
#[derive(Clone, Copy, Debug)]
pub struct ByteReader<'bytes> {
    bytes: &'bytes [u8],
}

impl<'bytes> ByteReader<'bytes> {
    /// Start at the front of some bytes
    pub fn new(bytes: &'bytes [u8]) -> Self {
        Self { bytes }
    }

    /// Take raw bytes
    pub fn take(&mut self, length: usize) -> Option<&'bytes [u8]> {
        let (field, rest) = self.bytes.split_at_checked(length)?;
        self.bytes = rest;
        Some(field)
    }

    /// Take a word
    pub fn word(&mut self) -> Option<u64> {
        let mut word = [0; size_of::<u64>()];
        word.copy_from_slice(self.take(size_of::<u64>())?);
        Some(u64::from_le_bytes(word))
    }

    /// Take a half word
    pub fn half(&mut self) -> Option<u32> {
        let mut half = [0; size_of::<u32>()];
        half.copy_from_slice(self.take(size_of::<u32>())?);
        Some(u32::from_le_bytes(half))
    }

    /// Take a short
    pub fn short(&mut self) -> Option<u16> {
        let mut short = [0; size_of::<u16>()];
        short.copy_from_slice(self.take(size_of::<u16>())?);
        Some(u16::from_le_bytes(short))
    }

    /// Take one byte
    pub fn byte(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    /// Read what's left
    pub fn rest(&self) -> &'bytes [u8] {
        self.bytes
    }
}
