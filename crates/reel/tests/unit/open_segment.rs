use std::path::Path;

use reel::engine::open_segment::*;
use reel::segment::segment_tail::SegmentNumber;

// a segment's file names read back to its number, and other names don't
#[test]
fn file_names() {
    let root = Path::new("/disk");
    let number = SegmentNumber(0xABC);

    assert_eq!(parse_segment(&data_path(root, number), DATA_SUFFIX), Some(number));
    assert_eq!(parse_segment(&rows_path(root, number), ROWS_SUFFIX), Some(number));
    assert_eq!(parse_segment(Path::new("/disk/superblock"), DATA_SUFFIX), None);
    assert_eq!(parse_segment(&rows_path(root, number), DATA_SUFFIX), None);
}

// a window may hold a record that starts at or past its start and ends at or before its used end, and no other
#[test]
fn may_hold_edges() {
    let window = RecordWindow::new();
    window.reset(reel_core::units::FileOffset(1_000));
    window.push([&[1_u8; 30], &[2_u8; 20]]);
    let at = reel_core::units::FileOffset;
    assert!(!window.may_hold(at(999), 10), "an offset under the start");
    assert!(window.may_hold(at(1_000), 50), "a record ending at the used end");
    assert!(window.may_hold(at(1_010), 40), "a record inside");
    assert!(!window.may_hold(at(1_010), 41), "a record one byte past the used end");
}
