//! A growing list maps its room with nothing in memory, brings in what a length asks, and takes no item past its pages

use reel::growing_list::GrowingList;

// a list with room for 4 GiB of items opens with none of it in memory, and its pages in memory follow what make_ready asked
#[test]
fn a_large_room_opens_with_nothing_in_memory() {
    let mut list: GrowingList<u64> = GrowingList::reserved((4 << 30) / size_of::<u64>()).expect("reserve");
    assert_eq!(list.ready_bytes(), 0);
    list.make_ready(1_000).expect("ready");
    let ready = list.ready_bytes();
    assert!(ready >= 1_000 * size_of::<u64>() && ready <= 1_000 * size_of::<u64>() + (1 << 20), "{ready} bytes in memory for 1000 items");
    assert!(list.extend_from((0..1_000).map(|number| number as u64)));
    assert_eq!(list.len(), 1_000);
    assert_eq!(list[999], 999);
    list.truncate(10);
    list.give_back_past(10);
    assert!(list.ready_bytes() < ready || ready <= (1 << 20), "a shrunk list kept {} of {ready} bytes", list.ready_bytes());
    assert_eq!(&list[..3], &[0, 1, 2]);
}

// a push past the pages in memory is a bug a debug build stops at
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "a push past the list's pages in memory")]
fn a_push_past_the_pages_panics_in_debug() {
    let mut list: GrowingList<u64> = GrowingList::reserved(1 << 20).expect("reserve");
    let _pushed = list.push(1);
}

// and a release build refuses it, leaving the list as it was
#[cfg(not(debug_assertions))]
#[test]
fn a_push_past_the_pages_is_refused_in_release() {
    let mut list: GrowingList<u64> = GrowingList::reserved(1 << 20).expect("reserve");
    assert!(!list.push(1));
    assert!(list.is_empty());
    list.make_ready(1).expect("ready");
    assert!(list.push(1));
    assert_eq!(list.len(), 1);
}
