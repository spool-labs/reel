//! Every parser the crate exposes to bytes it did not write, under guided mutation
//!
//! The seeded sweep in `tests/format_fuzz.rs` offers each of these random strings and
//! proves none of them indexes before it checks. What it cannot do is get through a
//! magic number or a checksum, so everything behind the front door goes unvisited. A
//! coverage-guided run finds those gates on its own, which is the whole reason this
//! target exists beside the sweep rather than instead of it.

#![no_main]

use libfuzzer_sys::fuzz_target;

use reel::append::codec::decode;
use reel::format::column::{Codec, ColumnId, RecordKey};
use reel::format::filter::Filter;
use reel::format::footer::SegmentFooter;
use reel::format::prefix::PrefixRows;
use reel::format::record::{BatchFrame, RecordHeader};
use reel::format::segment_header::SegmentHeader;
use reel::index::column::ColumnMark;
use reel::index::persisted::PersistedIndex;

/// Partitions a filter region is read at, kept small so a case stays cheap
const PARTITIONS: usize = 4;

/// Every codec the format names, so a codec added behind the byte is swept with it
const CODECS: &[Codec] = &[Codec::Lz4];

fuzz_target!(|bytes: &[u8]| {
    let _ = RecordHeader::unpack(bytes);
    let _ = SegmentFooter::parse(bytes);
    let _ = SegmentHeader::unpack(bytes);
    let _ = PersistedIndex::unpack(bytes);
    let _ = PrefixRows::decode(bytes);
    let _ = ColumnMark::unpack(bytes);
    let _ = Filter::parse_region(bytes, PARTITIONS);
    let _ = RecordKey::from_bytes(ColumnId(0), bytes);

    for codec in CODECS {
        let _ = decode(codec.as_byte(), bytes);
    }

    // A frame is read against a header, so it is offered one these bytes really parse
    // to where they do, and one declaring a frame where they do not. Without the
    // second, the flag check refuses almost every case before the parser is reached.
    if let Ok(header) = RecordHeader::unpack(bytes) {
        let _ = BatchFrame::unpack(&header, bytes);
    }
    let declared = BatchFrame {
        count: u32::from_le_bytes([
            bytes.first().copied().unwrap_or(0),
            bytes.get(1).copied().unwrap_or(0),
            bytes.get(2).copied().unwrap_or(0),
            bytes.get(3).copied().unwrap_or(0),
        ]),
        span: bytes.len() as u64,
    };
    let _ = BatchFrame::unpack(&declared.header(), bytes);
});
