//! Variable-length integers, seven bits a byte

/// Bits of value in one byte
pub const VALUE_BITS: u32 = 7;
/// Set on every byte but the last
pub const CONTINUE_BIT: u8 = 0x80;
/// Most bytes a 64-bit number takes
pub const MAX_VARINT_BYTES: usize = 10;

/// Append a number and return how many bytes it took, or nothing when it doesn't fit
pub fn write_varint(out: &mut [u8], number: u64) -> Option<usize> {
    let mut remaining = number;
    let mut used = 0;
    loop {
        let byte = (remaining & u64::from(!CONTINUE_BIT)) as u8;
        remaining >>= VALUE_BITS;
        let slot = out.get_mut(used)?;
        used += 1;
        if remaining == 0 {
            *slot = byte;
            return Some(used);
        }
        *slot = byte | CONTINUE_BIT;
    }
}

/// Read a number and how many bytes it took, or nothing when the bytes end early
pub fn read_varint(bytes: &[u8]) -> Option<(u64, usize)> {
    // most numbers in a row block fit one byte
    let first = *bytes.first()?;
    if first & CONTINUE_BIT == 0 {
        return Some((u64::from(first), 1));
    }
    let mut number: u64 = 0;
    for index in 0..MAX_VARINT_BYTES {
        let byte = *bytes.get(index)?;
        number |= u64::from(byte & !CONTINUE_BIT) << (VALUE_BITS * index as u32);
        if byte & CONTINUE_BIT == 0 {
            return Some((number, index + 1));
        }
    }
    None
}

/// Count the bytes of the number at the front, without reading it, or nothing when the bytes end early
pub fn skip_varint(bytes: &[u8]) -> Option<usize> {
    let length = bytes.iter().take(MAX_VARINT_BYTES).position(|byte| byte & CONTINUE_BIT == 0)?;
    Some(length + 1)
}
