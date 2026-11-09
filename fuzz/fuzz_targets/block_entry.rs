//! A footer's block index line, and the offsets a reader works out from it once its shape passes

#![no_main]

use libfuzzer_sys::fuzz_target;
use reel::segment::block_entry::BlockEntry;

fuzz_target!(|bytes: &[u8]| {
    let Some(entry) = BlockEntry::read(bytes) else {
        return;
    };
    if entry.is_shaped(0, 1 << 20) {
        let _header = entry.header_at();
        let _length = entry.block_length();
    }
});
