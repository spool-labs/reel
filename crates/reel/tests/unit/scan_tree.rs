use reel::engine::scan_state::*;
use reel_api::direction::Direction;
use reel_sim::seeded_random::{Seed, SeededRandom};

const WIDEST_KEY: usize = 16;
const OPEN_SLOTS: usize = 2;
const SEEDS: u64 = 200;
const MOST_KEYS: u64 = 40;

// sorted lists of keys, one for each source, in the walk's order
fn lists(random: &mut SeededRandom, count: usize, direction: Direction) -> Vec<Vec<Vec<u8>>> {
    (0..count)
        .map(|_list| {
            let mut keys: Vec<Vec<u8>> = (0..random.below(MOST_KEYS) + 1).map(|_key| format!("{:06}", random.below(1_000)).into_bytes()).collect();
            keys.sort();
            keys.dedup();
            if direction == Direction::Descending {
                keys.reverse();
            }
            keys
        })
        .collect()
}

// the tree hands out every key of every source in the walk's order, through steps, drops and a source that joins late
#[test]
fn tree_merges_the_sources() {
    for seed in 0..SEEDS {
        let mut random = SeededRandom::new(Seed(seed));
        for direction in [Direction::Ascending, Direction::Descending] {
            let count = random.below(MOST_SCAN_SOURCES as u64) as usize + 1;
            let lists = lists(&mut random, count, direction);
            let mut scan = ScanState::new(WIDEST_KEY, OPEN_SLOTS);
            scan.direction = direction;
            let mut next = vec![0; count];
            let mut owner: Vec<usize> = (0..count).collect();
            // the last list joins after the first key
            scan.source_count = count - 1;
            for (index, list) in lists.iter().enumerate() {
                scan.sources[index].key = list[0].clone();
            }
            scan.is_tree_stale = true;

            let mut merged = Vec::new();
            let mut has_joined = count == 1;
            if has_joined {
                scan.source_count = 1;
            }
            loop {
                scan.settle_tree();
                let Some(index) = scan.closest_source() else {
                    break;
                };
                merged.push(scan.sources[index].key.clone());
                let list = owner[index];
                next[list] += 1;
                match lists[list].get(next[list]) {
                    Some(key) => {
                        scan.sources[index].key = key.clone();
                        scan.replay(index);
                    },
                    None => {
                        scan.drop_source(index);
                        owner.swap(index, scan.source_count);
                    },
                }
                if !has_joined {
                    has_joined = true;
                    let last = count - 1;
                    owner.swap(scan.source_count, last);
                    scan.sources.swap(scan.source_count, last);
                    // a late source starts at its first key not yet passed
                    let passed = merged.last().cloned().unwrap_or_default();
                    while lists[last].get(next[last]).is_some_and(|key| !scan.is_before(&passed, key) && *key != passed) {
                        next[last] += 1;
                    }
                    match lists[last].get(next[last]) {
                        Some(key) => {
                            scan.sources[scan.source_count].key = key.clone();
                            scan.source_count += 1;
                            scan.is_tree_stale = true;
                        },
                        None => {},
                    }
                }
            }
            let mut expected = merged.clone();
            expected.sort();
            if direction == Direction::Descending {
                expected.reverse();
            }
            assert_eq!(merged, expected, "seed {seed} {direction:?}");
            let total: usize = lists.iter().map(Vec::len).sum();
            assert!(merged.len() <= total && merged.len() + lists[count - 1].len() >= total, "seed {seed}");
        }
    }
}
