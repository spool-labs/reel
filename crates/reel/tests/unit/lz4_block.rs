use reel::record::codec::CHUNK_BYTES;
use reel::record::lz4_block::*;

fn pattern(length: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(length);
    for index in 0..length {
        bytes.push((index % 17) as u8);
    }
    bytes
}

fn noise(length: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(length);
    let mut state: u64 = 1;
    for _ in 0..length {
        state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        bytes.push((state >> 33) as u8);
    }
    bytes
}

// repeating data shrinks and comes back whole
#[test]
fn round_trip() {
    let input = pattern(CHUNK_BYTES);
    let mut state = Lz4State::new();
    let mut stored = vec![0; CHUNK_BYTES];
    let mut restored = vec![0; CHUNK_BYTES];

    let length = state.compress(&input, &mut stored).expect("shrinks");
    let restored_length = decompress(&stored[..length], &mut restored).expect("valid block");

    assert!(length < input.len());
    assert_eq!(restored_length, input.len());
    assert_eq!(restored, input);
}

// noise and empty input don't shrink, so the chunk is stored as written
#[test]
fn no_shrink() {
    let mut state = Lz4State::new();
    let mut stored = vec![0; CHUNK_BYTES];

    assert_eq!(state.compress(&noise(CHUNK_BYTES), &mut stored), None);
    assert_eq!(state.compress(&[], &mut stored), None);
}

// a malformed block is refused
#[test]
fn bad_block() {
    let mut restored = vec![0; 16];

    assert_eq!(decompress(&[0xFF, 0xFF, 0xFF], &mut restored), None);
}
