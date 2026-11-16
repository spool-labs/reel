use reel::engine::scan_state::*;
use reel::segment::segment_tail::SegmentNumber;
use reel_core::direction::Direction;

const SEGMENT: SegmentNumber = SegmentNumber(7);
const FAR_END: u64 = 1 << 40;
const RECORD: usize = 100;

fn planned_length(window: &mut ScanWindow, offset: u64, direction: Direction) -> usize {
    window.plan(SEGMENT, (offset, RECORD), direction, FAR_END).1.len()
}

// a window answers for a record it holds whole, and for nothing past its end or in another segment
#[test]
fn holds_whole_records() {
    let mut window = ScanWindow::new();
    let _read = planned_length(&mut window, 1_000, Direction::Ascending);
    assert!(window.holding(SEGMENT, 1_000, RECORD).is_some());
    assert!(window.holding(SEGMENT, 1_000 + LEAST_WINDOW_BYTES as u64 - 10, RECORD).is_none());
    assert!(window.holding(SEGMENT, 999, RECORD).is_none());
    assert!(window.holding(SegmentNumber(8), 1_000, RECORD).is_none());
}

// rows side by side double the reach, up to the widest read
#[test]
fn reach_grows_along_a_run() {
    let mut window = ScanWindow::new();
    let mut offset = 0;
    let mut lengths = Vec::new();
    for _read in 0..6 {
        let length = planned_length(&mut window, offset, Direction::Ascending);
        lengths.push(length);
        offset += length as u64;
    }
    assert_eq!(lengths, [4_096, 8_192, 16_384, 16_384, 16_384, 16_384]);
}

// a row far from the window starts the reach over, so a scattered walk reads no more than its rows
#[test]
fn scattered_rows_stay_narrow() {
    let mut window = ScanWindow::new();
    for offset in [0, 1_000_000, 50_000, 9_000_000] {
        assert_eq!(planned_length(&mut window, offset, Direction::Ascending), LEAST_WINDOW_BYTES);
    }
}

// a walk downward reads the stretch that ends at its record, and grows the same way
#[test]
fn walks_down() {
    let mut window = ScanWindow::new();
    let offset = 1_000_000;
    let _read = planned_length(&mut window, offset, Direction::Descending);
    assert!(window.holding(SEGMENT, offset, RECORD).is_some());
    let below = offset + RECORD as u64 - LEAST_WINDOW_BYTES as u64 - RECORD as u64;
    assert_eq!(planned_length(&mut window, below, Direction::Descending), 2 * LEAST_WINDOW_BYTES);
    assert!(window.holding(SEGMENT, below, RECORD).is_some());
}

// a read stops at the segment's records
#[test]
fn stops_at_the_data_end() {
    let mut window = ScanWindow::new();
    let (_at, bytes) = window.plan(SEGMENT, (900, RECORD), Direction::Ascending, 1_000);
    assert_eq!(bytes.len(), RECORD);
}

// the clear forgets the window, and the reach starts over
#[test]
fn clear_forgets() {
    let mut window = ScanWindow::new();
    let _read = planned_length(&mut window, 0, Direction::Ascending);
    window.clear();
    assert!(window.holding(SEGMENT, 0, RECORD).is_none());
}
