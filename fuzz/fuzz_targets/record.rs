//! A stored record, checked against a key the way a get checks one, plain or compressed, in one chunk or in parts

#![no_main]

use libfuzzer_sys::fuzz_target;
use reel::record::record_check::{RecordIdentity, RecordKey};
use reel::record::record_reader::{RecordReader, form_of, open_small};
use reel::row_kind::RowKind;
use reel_api::column_id::ColumnId;
use reel_api::units::WritePlace;

fuzz_target!(|bytes: &[u8]| {
    let Some(form) = form_of(bytes) else {
        return;
    };
    let identity = RecordIdentity { record_key: RecordKey(7), column: ColumnId(1), key: b"fuzz", kind: RowKind::Value, place: WritePlace(9) };
    let mut value = Vec::new();
    let mut decoded = vec![0; 1 << 16];
    let _small = open_small(bytes, form, &identity, &mut decoded, &mut value);
    let _held = RecordReader::new().check_held(bytes, form, &identity, &mut value);
});
