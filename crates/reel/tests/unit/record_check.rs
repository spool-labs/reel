use reel::record::codec::Codec;
use reel::record::record_check::*;
use reel::record::record_head::RecordForm;
use reel::row_kind::RowKind;
use reel_core::column_id::ColumnId;
use reel_core::units::WritePlace;

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

// a running sum is the whole's checksum however the pieces were cut, and moving a piece changes it
#[test]
fn running_sum_cuts() {
    let bytes: Vec<u8> = (0..20_000u32).map(|index| (index * 7 % 251) as u8).collect();
    let whole = checksum(&bytes);
    for cut in [1, 64, 4_096, 7_000] {
        let mut sum = RunningSum::new();
        for piece in bytes.chunks(cut) {
            sum.add(piece);
        }
        assert_eq!(sum.sum(), whole, "cut {cut}");
    }
    let mut moved = RunningSum::new();
    moved.add(&bytes[64..128]);
    moved.add(&bytes[..64]);
    moved.add(&bytes[128..]);
    assert_ne!(moved.sum(), whole);
}
