//! The codec a column declares and a record stores

/// Bytes of value in one chunk
pub const CHUNK_BYTES: usize = 64 * 1024;
/// The stored byte of the none codec
pub const NONE_BYTE: u8 = 0;
/// The stored byte of the lz4 codec
pub const LZ4_BYTE: u8 = 1;

/// How a record's bytes are stored
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Codec {
    /// As written
    None,
    /// In lz4 blocks of one chunk each
    Lz4,
}

impl Codec {
    /// Read the byte that stores this codec
    pub fn as_byte(self) -> u8 {
        match self {
            Codec::None => NONE_BYTE,
            Codec::Lz4 => LZ4_BYTE,
        }
    }

    /// Read a stored codec back
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            NONE_BYTE => Some(Codec::None),
            LZ4_BYTE => Some(Codec::Lz4),
            _unknown => None,
        }
    }
}
