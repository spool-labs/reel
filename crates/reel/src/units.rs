//! Bytes, counted
//!
//! Only a quantity of storage carries a type here. The engine's other u64 are
//! offsets, sequence numbers and record counts, and they stay bare.

use crate::config::{GIB, MIB};

/// A quantity of storage, in bytes
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct ByteCount(pub u64);

impl ByteCount {
    /// A count of this many bytes
    #[inline]
    pub const fn from_bytes(bytes: u64) -> ByteCount {
        ByteCount(bytes)
    }

    /// The bytes counted
    #[inline]
    pub const fn to_bytes(self) -> u64 {
        self.0
    }

    /// A count of this many mebibytes
    #[inline]
    pub const fn mb(mebibytes: u64) -> ByteCount {
        ByteCount(mebibytes.saturating_mul(MIB))
    }

    /// A count of this many gibibytes
    #[inline]
    pub const fn gb(gibibytes: u64) -> ByteCount {
        ByteCount(gibibytes.saturating_mul(GIB))
    }
}
