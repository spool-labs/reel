//! What it costs to accumulate a footer's rows and pack them at seal
//!
//! A tail appends rows as records land, in whatever order the writers finish, and a seal
//! puts them in key order and writes them out. Prefix-packed rows cannot be appended out
//! of order, so the packing happens after the sort, on the foreground write path. Two
//! accumulators are measured: paired holds a key and a tail per row as their own
//! allocations, flat holds every row in one buffer at a fixed stride. Paired moves less
//! memory and allocates far more, flat is the reverse.
//!
//! Ignored by default. Run with:
//!   cargo test -p reel --test footer_build --release -- --ignored --nocapture

use std::time::{Duration, Instant};

use reel::format::footer::{FooterEntry, FooterPartition, SegmentFooter};
use reel::format::lsn::Lsn;
use reel::format::prefix::PrefixRows;
use reel::format::record::Flags;
use reel::{ColumnId, RecordKey};

/// Bytes a row carries behind its key, matching the footer's entry tail
const TAIL: usize = 17;

/// Row counts a real segment reaches
const COUNTS: [usize; 3] = [10_000, 100_000, 1_000_000];

/// Passes per cell, reporting the best, so a stray preemption is not the result
const ROUNDS: usize = 3;

/// Keys shaped like the column this packing exists for
///
/// Object names, in the order writers would finish rather than in key order, so the sort
/// has real work to do. A slot-led column arrives nearly sorted and flatters both.
fn keys(count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|at| {
            // Scattered so the sort is a sort, and shaped so neighbours share a front.
            let scattered = (at as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            format!(
                "tenants/{:016x}/exports/2026/08/02/part-{:08}.parquet",
                scattered % 4096,
                scattered % 100_000,
            )
            .into_bytes()
        })
        .collect()
}

/// Names of wildly different lengths, which is what an object bucket holds
///
/// Flat pads every key to the widest one in the partition, so a column whose keys are all
/// one length costs it nothing. Real names run from a handful of bytes to a kibibyte, so
/// the padding is the whole question rather than a rounding error.
fn mixed_keys(count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|at| {
            let scattered = (at as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            // A few long names among many short ones, which is the shape a bucket takes.
            let depth = match scattered % 32 {
                0 => 24,
                1..=3 => 8,
                _ => 1,
            };
            let mut name = format!("t{:04x}", scattered % 4096);
            for level in 0..depth {
                name.push_str(&format!(
                    "/{:08x}",
                    scattered.wrapping_add(level) % 1_000_000
                ));
            }
            name.into_bytes()
        })
        .collect()
}

/// Hold each row as its own pair, sort the pairs, pack
fn paired(keys: &[Vec<u8>]) -> (Duration, PrefixRows) {
    let began = Instant::now();
    let mut staged: Vec<(Vec<u8>, [u8; TAIL])> = Vec::with_capacity(keys.len());
    for (at, key) in keys.iter().enumerate() {
        let mut tail = [0u8; TAIL];
        tail[..8].copy_from_slice(&(at as u64).to_le_bytes());
        staged.push((key.clone(), tail));
    }
    staged.sort_unstable_by(|left, right| left.0.cmp(&right.0));

    let mut rows = PrefixRows::new();
    for (key, tail) in &staged {
        rows.push(key, tail).expect("push");
    }
    (began.elapsed(), rows)
}

/// Hold every row in one buffer at a fixed stride, sort an index, pack
///
/// The stride is the widest key the partition holds, so a short key is padded to it for
/// as long as the accumulation lasts.
fn flat(keys: &[Vec<u8>]) -> (Duration, PrefixRows) {
    let width = keys.iter().map(Vec::len).max().unwrap_or(0);
    let stride = width + 2 + TAIL;

    let began = Instant::now();
    let mut packed: Vec<u8> = vec![0u8; keys.len() * stride];
    for (at, key) in keys.iter().enumerate() {
        let row = at * stride;
        packed[row..row + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
        packed[row + 2..row + 2 + key.len()].copy_from_slice(key);
        let tail_at = row + 2 + width;
        packed[tail_at..tail_at + 8].copy_from_slice(&(at as u64).to_le_bytes());
    }

    let mut order: Vec<u32> = (0..keys.len() as u32).collect();
    let key_of = |at: u32| {
        let row = at as usize * stride;
        let len = u16::from_le_bytes([packed[row], packed[row + 1]]) as usize;
        &packed[row + 2..row + 2 + len]
    };
    order.sort_unstable_by(|left, right| key_of(*left).cmp(key_of(*right)));

    let mut rows = PrefixRows::new();
    for at in &order {
        let row = *at as usize * stride;
        let len = u16::from_le_bytes([packed[row], packed[row + 1]]) as usize;
        let tail_at = row + 2 + width;
        rows.push(
            &packed[row + 2..row + 2 + len],
            &packed[tail_at..tail_at + TAIL],
        )
        .expect("push");
    }
    (began.elapsed(), rows)
}

/// Sort the rows and stop, which is what a seal does today
///
/// The number that decides whether packing is affordable, since the sort is already paid
/// and only the pack is new.
fn sort_only(keys: &[Vec<u8>]) -> Duration {
    let began = Instant::now();
    let mut staged: Vec<(Vec<u8>, [u8; TAIL])> = Vec::with_capacity(keys.len());
    for (at, key) in keys.iter().enumerate() {
        let mut tail = [0u8; TAIL];
        tail[..8].copy_from_slice(&(at as u64).to_le_bytes());
        staged.push((key.clone(), tail));
    }
    staged.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    std::hint::black_box(&staged);
    began.elapsed()
}

/// Bytes each strategy holds while it is accumulating, which is the other half
///
/// This runs on the foreground write path when a segment fills, so what it holds is
/// memory a node cannot use for anything else until the seal finishes.
fn held(keys: &[Vec<u8>]) -> (usize, usize) {
    // Paired: a heap allocation per key rounded as an allocator rounds, plus the pair.
    let paired: usize = keys
        .iter()
        .map(|key| (key.len() + 16).div_ceil(16) * 16 + 24 + TAIL)
        .sum();

    // Flat: one buffer, every key padded to the widest, plus the index vector.
    let width = keys.iter().map(Vec::len).max().unwrap_or(0);
    let flat = keys.len() * (width + 2 + TAIL) + keys.len() * 4;
    (paired, flat)
}

// which accumulator a seal should use, measured rather than assumed
#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn accumulator_shapes() {
    println!();
    println!("Sorting and packing one partition, best of {ROUNDS}");
    println!();
    println!(
        "| names | rows | sort only | paired | flat | pack over sort | paired held | flat held |"
    );
    println!("|---|---|---|---|---|---|---|---|");

    for (shape, count) in COUNTS
        .iter()
        .map(|count| ("uniform", *count))
        .chain(COUNTS.iter().map(|count| ("mixed", *count)))
    {
        let corpus = match shape {
            "mixed" => mixed_keys(count),
            _ => keys(count),
        };

        let mut best_paired = Duration::MAX;
        let mut best_flat = Duration::MAX;
        let mut best_sort = Duration::MAX;
        let mut packed_len = 0usize;
        for _ in 0..ROUNDS {
            best_sort = best_sort.min(sort_only(&corpus));
            let (took, rows) = paired(&corpus);
            best_paired = best_paired.min(took);
            packed_len = rows.packed_len();

            let (took, other) = flat(&corpus);
            best_flat = best_flat.min(took);

            // Both have to produce the same block, or this compares two answers rather
            // than two ways of reaching one.
            assert_eq!(other.len(), rows.len(), "row counts differ");
            assert_eq!(other.packed_len(), rows.packed_len(), "packed bytes differ");
        }

        let (paired_held, flat_held) = held(&corpus);
        println!(
            "| {shape} | {count} | {best_sort:?} | {best_paired:?} | {best_flat:?} | {:.2}x | {:.1} MB | {:.1} MB |",
            best_paired.as_secs_f64() / best_sort.as_secs_f64(),
            paired_held as f64 / 1e6,
            flat_held as f64 / 1e6,
        );
        let _ = packed_len;
    }
    println!();
}

// both strategies pack the same rows in the same order, duplicate keys included
#[test]
fn both_accumulators_pack_the_same_block() {
    let mut corpus = keys(5_000);
    // Duplicates, which is what an overwrite inside one segment leaves behind
    for at in 0..500 {
        corpus.push(corpus[at].clone());
    }

    let (_, one) = paired(&corpus);
    let (_, two) = flat(&corpus);

    assert_eq!(one.len(), two.len());
    assert_eq!(one.packed_len(), two.packed_len());
    assert_eq!(
        one.keys(TAIL).expect("keys"),
        two.keys(TAIL).expect("keys"),
        "the two accumulators disagree about the order",
    );
}

/// Keys led by a counter the producer advances, big endian so they sort by it
fn slot_keys(count: usize) -> Vec<Vec<u8>> {
    (0..count as u64)
        .map(|at| {
            let mut key = at.to_be_bytes().to_vec();
            key.extend_from_slice(&[0u8; 4]);
            key
        })
        .collect()
}

/// Keys that share nothing, which is what a content address is
fn random_keys(count: usize) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = (0..count)
        .map(|at| {
            let mut key = Vec::with_capacity(34);
            key.extend_from_slice(&((at % 1024) as u16).to_be_bytes());
            let mut seed = (at as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            for _ in 0..4 {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                key.extend_from_slice(&seed.to_le_bytes());
            }
            key
        })
        .collect();
    keys.sort();
    keys
}

// what the packing is worth per column shape, which is not one number
#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn what_packing_is_worth_by_column_shape() {
    println!();
    println!("| keys | whole | packed | saved |");
    println!("|---|---|---|---|");

    for (label, corpus) in [
        ("content address, 34 B", random_keys(50_000)),
        ("slot-led, big endian", slot_keys(50_000)),
        ("object names", mixed_keys(50_000)),
    ] {
        let mut sorted = corpus.clone();
        sorted.sort();
        let mut rows = PrefixRows::new();
        for (at, key) in sorted.iter().enumerate() {
            rows.push(key, &(at as u64).to_le_bytes()[..TAIL.min(8)])
                .ok();
        }
        let whole: usize = sorted.iter().map(|key| key.len() + TAIL.min(8)).sum();
        let packed = rows.packed_len();
        println!(
            "| {label} | {whole} | {packed} | {:.0}% |",
            100.0 * (1.0 - packed as f64 / whole as f64),
        );
    }
    println!();
}

/// Column every row in the sort bench below belongs to
const COLUMN: ColumnId = ColumnId(1);

/// One column's rows in the order they reached the partition
///
/// Built through the footer's own build rather than by hand, so a corpus of mixed widths
/// stops the striding exactly where the write path would stop it.
fn staged(keys: &[Vec<u8>]) -> FooterPartition {
    let rows: Vec<FooterEntry> = keys
        .iter()
        .enumerate()
        .map(|(at, key)| {
            FooterEntry::new(
                RecordKey::from_bytes(COLUMN, key).expect("key"),
                Lsn(at as u64 + 1),
                (at * 64) as u32,
                64,
                Flags::DATA,
            )
        })
        .collect();
    let mut footer = SegmentFooter::build(rows);
    footer.partitions.remove(0)
}

// what the seal's sort costs per shape; the scattered column is not comparable across runs
#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn seal_sort_by_arrival_order() {
    println!();
    println!("Sorting one partition at seal, best of {ROUNDS}");
    println!();
    println!("| shape | rows | as written | in key order |");
    println!("|---|---|---|---|");

    for count in COUNTS {
        for (shape, corpus) in [
            ("slot-led, 12 B", slot_keys(count)),
            ("content address, 34 B", random_keys(count)),
            ("object names, mixed", mixed_keys(count)),
            ("object names, uniform", keys(count)),
        ] {
            let mut ordered = corpus.clone();
            ordered.sort();

            let mut best_written = Duration::MAX;
            let mut best_ordered = Duration::MAX;
            for (rows, best) in [(&corpus, &mut best_written), (&ordered, &mut best_ordered)] {
                let held = staged(rows);
                for _ in 0..ROUNDS {
                    // A fresh copy per round, since a sorted partition is a different
                    // input from the one that arrived.
                    let mut one = held.clone();
                    let began = Instant::now();
                    one.sort();
                    *best = (*best).min(began.elapsed());
                    std::hint::black_box(&one);
                }
            }

            println!("| {shape} | {count} | {best_written:?} | {best_ordered:?} |");
        }
    }
    println!();
}

// the sort leaves rows that arrived in key order exactly where a sort would put them
#[test]
fn a_partition_in_key_order_sorts_to_itself() {
    let mut corpus = keys(2_000);
    corpus.sort();
    // Duplicates arrive after their first version, so their sequence numbers ascend.
    for at in (0..2_000).step_by(7) {
        corpus.insert(at, corpus[at].clone());
    }

    let held = staged(&corpus);
    let mut one = held.clone();
    one.sort();

    // Every row of the arrival, in arrival order, since nothing needed moving.
    let arrived: Vec<Vec<u8>> = (0..held.len())
        .map(|row| held.key_at(row).expect("key").to_vec())
        .collect();
    let sorted: Vec<Vec<u8>> = (0..one.len())
        .map(|row| one.key_at(row).expect("key").to_vec())
        .collect();
    assert_eq!(
        arrived, sorted,
        "the sort moved rows that were already in order"
    );

    let lsns: Vec<u64> = (0..one.len())
        .map(|row| one.entry_at(row).expect("row").lsn.as_u64())
        .collect();
    let mut wanted = lsns.clone();
    wanted.sort_unstable();
    assert_eq!(
        lsns, wanted,
        "a rewritten key came back with its versions out of order"
    );
}
