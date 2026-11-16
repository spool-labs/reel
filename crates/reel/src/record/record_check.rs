//! The keyed check that ties a record to its key

use reel_core::column_id::ColumnId;
use reel_core::units::WritePlace;
use xxhash_rust::xxh3::{Xxh3, xxh3_64, xxh3_64_with_seed};

use crate::record::record_head::RecordForm;
use crate::row_kind::RowKind;

/// Bytes of the fields hashed beside the key
pub const CHECK_FIELDS_BYTES: usize = 11;

/// The random key of the record check, set once when the store is created
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct RecordKey(pub u64);

/// A record's 8 B check
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct RecordCheck(pub u64);

/// A checksum of stored bytes
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Checksum(pub u64);

/// What a record belongs to
#[derive(Clone, Copy, Debug)]
pub struct RecordIdentity<'key> {
    pub record_key: RecordKey,
    pub column: ColumnId,
    pub key: &'key [u8],
    pub kind: RowKind,
    pub place: WritePlace,
}

/// Sum stored bytes
pub fn checksum(bytes: &[u8]) -> Checksum {
    Checksum(xxh3_64(bytes))
}

/// A checksum over bytes that come a piece at a time. It equals the checksum of the whole however the pieces were cut, so a
/// reader with another buffer size checks what a writer wrote, and moving a piece changes it
#[derive(Clone)]
pub struct RunningSum(Xxh3);

impl RunningSum {
    /// Start over no bytes
    pub fn new() -> Self {
        Self(Xxh3::new())
    }

    /// Take the next piece
    pub fn add(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    /// Read the checksum of every piece so far
    pub fn sum(&self) -> Checksum {
        Checksum(self.0.digest())
    }
}

impl Default for RunningSum {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for RunningSum {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "RunningSum({:?})", self.sum())
    }
}

/// Fold a place into a check, so a record checks only for the one write that made it
pub fn place_check(check: RecordCheck, record_key: RecordKey, place: WritePlace) -> RecordCheck {
    RecordCheck(check.0 ^ xxh3_64_with_seed(&place.0.to_le_bytes(), record_key.0))
}

/// Hash the column, the key, the kind, the form and a payload checksum under the store's key, with no place yet
pub fn record_check(identity: &RecordIdentity, form: RecordForm, payload: Checksum) -> RecordCheck {
    let mut fields = [0; CHECK_FIELDS_BYTES];
    fields[0] = identity.column.0;
    fields[1] = identity.kind.as_byte();
    fields[2] = form.as_byte();
    fields[3..].copy_from_slice(&payload.0.to_le_bytes());

    let seed = xxh3_64_with_seed(&fields, identity.record_key.0);
    RecordCheck(xxh3_64_with_seed(identity.key, seed))
}
