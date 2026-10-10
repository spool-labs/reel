//! Feeds every parser the crate exposes bytes it did not write, under guided mutation

#![no_main]

use libfuzzer_sys::fuzz_target;

use reel::append::codec::decode;
use reel::format::column::{Codec, ColumnId, RecordKey};
use reel::format::filter::Filter;
use reel::format::footer::SegmentFooter;
use reel::format::journal::read_groups;
use reel::format::prefix::{unpack, PrefixRows, Tail};
use reel::format::record::RecordHeader;
use reel::format::segment_header::SegmentHeader;
use reel::index::column::ColumnMark;

/// Partitions a filter region is read at, kept small so a case stays cheap
const PARTITIONS: usize = 4;

/// Every codec in the format, so a new one is swept as soon as it is listed
const CODECS: &[Codec] = &[Codec::Lz4];

/// A strided row width, the one the seeded sweep offers too
const ROW_LEN: usize = 33;

fuzz_target!(|bytes: &[u8]| {
    let _ = RecordHeader::unpack(bytes);
    let _ = SegmentFooter::parse(bytes);
    let _ = SegmentHeader::unpack(bytes);
    let _ = PrefixRows::decode(bytes, Tail::Entry);
    let _ = unpack(bytes, Tail::Entry, None);
    let _ = unpack(bytes, Tail::Entry, Some(ROW_LEN));
    let _ = ColumnMark::unpack(bytes);
    let _ = Filter::parse_region(bytes, PARTITIONS);
    let _ = RecordKey::from_bytes(ColumnId(0), bytes);
    let _ = read_groups(bytes);

    for codec in CODECS {
        let _ = decode(codec.as_byte(), bytes);
    }
});
