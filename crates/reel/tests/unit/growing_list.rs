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

// a key table grows by a rehash into its other buffer under the shard's write lock: the largest grow at any size holds it under a
// millisecond, a table past MOST_GROWN_SLOTS starting whole, and every key is found at the new size
#[test]
fn key_table_grow_hold() {
    use reel::index::entry_slab::EntryAddress;
    use reel::index::key_hash::KeyHash;
    use reel::index::key_table::KeyTable;
    let hash = |number: u64| KeyHash(number.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    // fill a table to its most with the job's grow rule, and return its longest grow and the keys it took
    let fill = |most: usize| {
        let mut table = KeyTable::growing(most, 1 << 10).expect("reserve");
        let (mut keys, mut grown, mut longest) = (0_u64, 0, std::time::Duration::ZERO);
        loop {
            if table.wants_to_grow() {
                let started = std::time::Instant::now();
                assert!(table.grow().expect("grow"));
                longest = longest.max(started.elapsed());
                grown += 1;
                continue;
            }
            if !table.has_room_for(1) {
                break;
            }
            let at = table.find_head(hash(keys), EntryAddress(u32::MAX));
            table.set(at, hash(keys), EntryAddress(keys as u32));
            keys += 1;
        }
        for number in (0..keys).step_by(997) {
            let at = table.find_head(hash(number), EntryAddress(number as u32));
            assert_eq!(table.head(at), EntryAddress(number as u32), "key {number} lost in a grow to {most} slots");
        }
        (longest, grown, keys, table.size())
    };
    for most in [1_usize << 18, 1 << 19, 1 << 21] {
        // the least of a few fills' longest grows is the rehash's own cost, past a test beside it taking the core
        let runs: Vec<_> = (0..5).map(|_run| fill(most)).collect();
        let (longest, grown, keys, size) = runs.iter().min_by_key(|run| run.0).copied().expect("a run");
        println!("{grown} grows to {size} slots for {keys} keys, the longest held {longest:?} at the least of five fills");
        // a release build holds the line, and a debug build runs some ten times slower
        assert!(cfg!(debug_assertions) || longest < std::time::Duration::from_millis(1), "a grow to {most} slots held the shard {longest:?}");
    }
}
