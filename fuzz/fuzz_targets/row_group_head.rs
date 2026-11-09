//! A rows file group's head, which open reads before any check of the group behind it

#![no_main]

use libfuzzer_sys::fuzz_target;
use reel::rows::row_group::parse_head;

fuzz_target!(|bytes: &[u8]| {
    let _head = parse_head(bytes);
});
