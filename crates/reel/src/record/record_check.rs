//! The keyed check that ties a record to its key

use reel_api::column_id::ColumnId;
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

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
}

/// Sum stored bytes
pub fn checksum(bytes: &[u8]) -> Checksum {
    Checksum(xxh3_64(bytes))
}

/// Hash the column, the key, the kind, the form and a payload checksum under the store's key
pub fn record_check(identity: &RecordIdentity, form: RecordForm, payload: Checksum) -> RecordCheck {
    let mut fields = [0; CHECK_FIELDS_BYTES];
    fields[0] = identity.column.0;
    fields[1] = identity.kind.as_byte();
    fields[2] = form.as_byte();
    fields[3..].copy_from_slice(&payload.0.to_le_bytes());

    let seed = xxh3_64_with_seed(&fields, identity.record_key.0);
    RecordCheck(xxh3_64_with_seed(identity.key, seed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::codec::Codec;

    const RECORD_KEY: RecordKey = RecordKey(9);
    const FORM: RecordForm = RecordForm {
        codec: Codec::None,
        is_chunked: false,
    };
    const OTHER_FORM: RecordForm = RecordForm {
        codec: Codec::Lz4,
        is_chunked: false,
    };

    fn identity(key: &[u8]) -> RecordIdentity<'_> {
        RecordIdentity {
            record_key: RECORD_KEY,
            column: ColumnId(1),
            key,
            kind: RowKind::Value,
        }
    }

    // every field the check covers changes it
    #[test]
    fn check_fields() {
        let payload = checksum(b"payload");
        let base = record_check(&identity(b"key"), FORM, payload);

        let mut other_column = identity(b"key");
        other_column.column = ColumnId(2);
        let mut other_kind = identity(b"key");
        other_kind.kind = RowKind::RangeDelete;
        let mut other_store = identity(b"key");
        other_store.record_key = RecordKey(10);

        assert_ne!(base, record_check(&identity(b"other"), FORM, payload));
        assert_ne!(base, record_check(&other_column, FORM, payload));
        assert_ne!(base, record_check(&other_kind, FORM, payload));
        assert_ne!(base, record_check(&other_store, FORM, payload));
        assert_ne!(base, record_check(&identity(b"key"), OTHER_FORM, payload));
        assert_ne!(base, record_check(&identity(b"key"), FORM, checksum(b"other")));
    }
}
