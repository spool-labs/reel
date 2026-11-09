//! A segment's tail: the fixed fields and inputs, then the columns behind them

#![no_main]

use libfuzzer_sys::fuzz_target;
use reel::little_endian::ByteReader;
use reel::segment::segment_tail::{ColumnTail, TailHead};

fuzz_target!(|bytes: &[u8]| {
    let mut reader = ByteReader::new(bytes);
    let Some((head, _inputs)) = TailHead::read(&mut reader) else {
        return;
    };
    for _column in 0..head.column_count.min(256) {
        if ColumnTail::read(&mut reader).is_none() {
            break;
        }
    }
});
