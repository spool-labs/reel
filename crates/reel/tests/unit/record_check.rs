use reel::record::codec::Codec;
use reel::record::record_check::*;
use reel::record::record_head::RecordForm;
use reel::row_kind::RowKind;
use reel_api::column_id::ColumnId;
use reel_api::units::WritePlace;

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
        place: WritePlace(0),
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
