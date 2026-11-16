//! A row block's rows, walked with and without the block's check, as a merge, a walk and a rebuild read them

#![no_main]

use libfuzzer_sys::fuzz_target;
use reel::config::MAX_KEY_BYTES;
use reel::fixed_list::FixedList;
use reel::segment::row_block::RowBlockWalk;
use reel_core::units::WritePlace;

fuzz_target!(|bytes: &[u8]| {
    let mut key: FixedList<u8> = FixedList::on_heap(MAX_KEY_BYTES as usize);
    for walk in [RowBlockWalk::open(bytes, WritePlace(0)), RowBlockWalk::open_checked(bytes, WritePlace(1 << 32))] {
        let Some(mut walk) = walk else {
            continue;
        };
        while walk.next_row(&mut key).is_some() {}
    }
});
