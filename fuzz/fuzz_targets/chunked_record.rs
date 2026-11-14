//! A record of any length read whole and by window from a file holding the fuzzed bytes, as a none and an lz4 column read
//! it: every read answers, and none panics on the lengths, the shape or the pieces' sums

#![no_main]

use std::io::IoSlice;
use std::path::Path;

use libfuzzer_sys::fuzz_target;
use reel::io::{InPlace, Io, IoFile, in_place};
use reel::record::codec::Codec;
use reel::record::record_check::{RecordIdentity, RecordKey};
use reel::record::record_reader::{RecordLocation, RecordReader};
use reel::row_kind::RowKind;
use reel_api::column_id::ColumnId;
use reel_api::reads::ValueWindow;
use reel_api::units::{ByteCount, FileOffset, WritePlace};
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::SimulatedIo;

/// Bytes at the front of an input that pick the window
const WINDOW_BYTES: usize = 16;

fuzz_target!(|input: &[u8]| {
    if input.len() < WINDOW_BYTES {
        return;
    }
    let (picks, bytes) = input.split_at(WINDOW_BYTES);
    let word = |at: usize| u64::from_le_bytes(picks[at..at + 8].try_into().expect("eight bytes"));
    let window = ValueWindow { offset: ByteCount(word(0) % (bytes.len() as u64 * 2 + 1)), length: ByteCount(word(8) % (1 << 20)) };

    let io = SimulatedIo::new(Seed(1));
    io.create_directory(Path::new("/fuzz")).expect("directory");
    let file = io.create_file(Path::new("/fuzz/record"), ByteCount(0)).expect("file");
    file.write_at(FileOffset(0), &[IoSlice::new(bytes)]).expect("write");
    let location = RecordLocation { offset: FileOffset(0), length: ByteCount(bytes.len() as u64) };
    let identity = RecordIdentity { record_key: RecordKey(7), column: ColumnId(1), key: b"fuzz", kind: RowKind::Value, place: WritePlace(9) };

    let mut reader = RecordReader::new();
    let mut value = Vec::new();
    for codec in [Codec::None, Codec::Lz4] {
        let _whole = in_place(reader.read_value(&InPlace, &file, location, &identity, codec, &mut value));
        let _window = in_place(reader.read_window(&InPlace, &file, location, &identity, codec, window, &mut value));
    }
});
