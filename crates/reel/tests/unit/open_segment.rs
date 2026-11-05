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
