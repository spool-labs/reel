//! A superblock copy, which open reads before it trusts anything else on a root

#![no_main]

use libfuzzer_sys::fuzz_target;
use reel::superblock::Superblock;

fuzz_target!(|bytes: &[u8]| {
    let _superblock = Superblock::from_bytes(bytes);
});
