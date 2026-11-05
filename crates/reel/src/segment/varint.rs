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

#[cfg(test)]
mod tests {
    use super::*;

    // numbers of every width read back with the bytes they took
    #[test]
    fn varint_round() {
        for number in [0, 1, 127, 128, 300, 1 << 32, u64::MAX] {
            let mut bytes = [0; MAX_VARINT_BYTES];

            let used = write_varint(&mut bytes, number).expect("fits");

            assert_eq!(read_varint(&bytes[..used]), Some((number, used)));
        }
    }

    // a cut number and a full buffer both report nothing
    #[test]
    fn varint_edges() {
        let mut bytes = [0; MAX_VARINT_BYTES];
        let used = write_varint(&mut bytes, u64::MAX).expect("fits");

        assert_eq!(read_varint(&bytes[..used - 1]), None);
        assert_eq!(write_varint(&mut bytes[..1], 300), None);
    }
}
