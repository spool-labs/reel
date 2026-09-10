//! What a merge of overlapping sorted runs costs, before any policy decides when
//!
//! A prototype rather than the mechanism: it opens sealed footers, k-way merges their
//! rows newest-wins, and writes the winners through a plain file, at a carried row
//! shape and a pointer one. Three things keep the answer honest: each run gets its own
//! volume, since a single volume shadows its own rows and reel's compaction reclaims
//! them before a merge could see them, leaving the output ratio exactly 1.000 at every
//! run count; no key is written twice inside a run, so nothing is dead and nothing is
//! reclaimed; and only the ratio column is exact, since the runs are parsed into
//! memory before the clock starts and nothing in the MB/s column has met a device.
//!
//! Opt-in, run with:
//!   cargo test -p tape-reel --release --test probes -- merge_rate

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Instant;

use tempfile::TempDir;

use reel::format::footer::{FooterPartition, SegmentFooter};
use reel::format::record::checksum;
use reel::units::ByteCount;
use reel::{
    Codec, ColumnId, ColumnSet, ColumnSpec, CompactPass, CompactRate, IndexResidency, KeyWidth,
    MapShape, Preallocate, RecordKey, ReelConfig, ReelStore, SyncPolicy, ThreadBudget,
    SEGMENT_SUFFIX,
};

const ACCOUNTS: ColumnId = ColumnId(1);

/// Key width every run is keyed at, a pubkey
const KEY_WIDTH: usize = 32;

/// Bytes past the key that every row carries whether or not it carries a value
const ROW_TAIL: usize = 17;

/// Bytes a carried row spends on its own checksum
const ROW_CRC: usize = 4;

/// Value each record holds, and what a carrying column declares room for
const VALUE: usize = 200;

/// Bytes one record spans on disk: header, key and payload
const RECORD_SPAN: u64 = 21 + KEY_WIDTH as u64 + VALUE as u64;

/// Records a segment takes before it seals, which is what sizes a run
const ROWS_PER_RUN: u64 = 6_000;

/// Records written past a segment's capacity, so the segment seals
///
/// A tail seals when the next record will not fit, so a volume writing exactly its
/// segment's worth never seals at all.
const SEAL_MARGIN: u64 = 64;

/// Writes that go to a key every run holds, for every one that opens a fresh key
///
/// Three in four, which leaves a run about three quarters shared with every other run.
/// The real distribution is more skewed still, so this is the conservative side.
const HOT_SHARE: u64 = 4;

/// Key numbers reserved for the shared set, above which every run's own keys begin
const HOT_KEYS: u64 = 8_192;

/// Key numbers each run takes for the keys nothing else holds
const COLD_SPAN: u64 = 8_192;

/// Runs the sweep stands up, which is the widest merge it does
const RUNS: usize = 64;

/// Milliseconds a run waits for its seal before giving the rewrite up
const SEAL_WAITS: u32 = 200;

/// Run counts the sweep merges, taken as prefixes of the same standing set
const COUNTS: &[usize] = &[4, 16, 64];

/// Merges each row runs at least, past one discarded first pass
///
/// A file created, a buffer first touched and a footer first read are fixed costs
/// inside a two millisecond merge, and left alone they read as the small merge being
/// slower per byte than the large one.
const REPEATS: usize = 5;

/// Rows a table row merges in total before its median is taken
///
/// Every row of the table does the same amount of work rather than the same number of
/// passes, since a two millisecond merge is scheduler noise at this width.
const TARGET_ROWS: u64 = 2_000_000;

const fn column(row_carry: u16) -> ColumnSpec {
    ColumnSpec {
        id: ACCOUNTS,
        name: "accounts",
        key_width: KeyWidth::Fixed(KEY_WIDTH as u16),
        shard_bytes: 0,
        inline_max: 0,
        row_carry,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    }
}

const CARRIED: ColumnSet = &[column(VALUE as u16)];
const POINTER: ColumnSet = &[column(0)];

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(ROWS_PER_RUN * RECORD_SPAN),
        alloc_chunk: ByteCount::from_bytes(1024 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        index: IndexResidency::Resident,
        rewrite_on_seal: true,
        // The gate off, since a gated pass answers `Held` and a caller driving
        // compaction to exhaustion cannot tell that from work remaining.
        compact_mbps: CompactRate::Mbps(100_000),
        // Only a wholly dead segment is worth reclaiming, so the rewrite the runs
        // need is the ordering one and nothing collapses a run for space.
        compact_dead_ratio: 1.0,
        filter_bits: 0,
        ..ReelConfig::default()
    }
}

/// A key nothing about its bytes says the order of, so every run's range covers
/// every other run's and no merge can skip a comparison
fn key(at: u64) -> RecordKey {
    let mixed = at
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .rotate_left(29)
        .wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let mut bytes = [0u8; KEY_WIDTH];
    bytes[..8].copy_from_slice(&mixed.to_be_bytes());
    bytes[8..16].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(ACCOUNTS, &bytes).expect("key")
}

/// Which key the nth write of a run goes to
///
/// One in four opens a key only this run holds and the rest go once each to the shared
/// set, so no write shadows another and the volume ends with no dead bytes to reclaim.
fn written(run: usize, at: u64) -> u64 {
    match at % HOT_SHARE {
        0 => HOT_KEYS + run as u64 * COLD_SPAN + at / HOT_SHARE,
        _ => at - at / HOT_SHARE,
    }
}

/// Stand up the runs, one volume apiece, and give their sorted footers back
///
/// One volume per run because a single volume shadows its own rows and reel's
/// compaction then reclaims them, leaving every run holding only the keys nothing
/// overwrote and no overlap for a merge to see.
///
/// The runs are age ordered by their place in this list, run zero oldest. Sequence
/// numbers cannot order across volumes, each counting from its own start, so the merge
/// breaks a tie on the run first.
fn runs(columns: ColumnSet) -> Vec<SegmentFooter> {
    (0..RUNS).map(|run| one_run(run, columns)).collect()
}

/// Write one run's keys, seal them, rewrite the segment sorted, and take its footer
fn one_run(run: usize, columns: ColumnSet) -> SegmentFooter {
    let dir = TempDir::new().expect("tempdir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(), columns).expect("open");
    let payload = vec![0x5Au8; VALUE];
    for at in 0..ROWS_PER_RUN + SEAL_MARGIN {
        store.put(&key(written(run, at)), &payload).expect("put");
    }
    store.flush().expect("flush");
    assert!(
        rewrite_round(&store) > 0,
        "run {run} sealed nothing for the rewrite to sort"
    );
    drop(store);

    let mut best: Option<SegmentFooter> = None;
    for entry in std::fs::read_dir(dir.path()).expect("read dir").flatten() {
        if !entry
            .file_name()
            .to_string_lossy()
            .ends_with(SEGMENT_SUFFIX)
        {
            continue;
        }
        let bytes = std::fs::read(entry.path()).expect("read segment");
        // The open tail carries no footer: a segment nothing sealed is not a run.
        let Ok(footer) = SegmentFooter::parse(&bytes) else {
            continue;
        };
        let Some(rows) = footer
            .partitions
            .iter()
            .find(|rows| rows.column == ACCOUNTS)
        else {
            continue;
        };
        if !rows.is_sorted_run() {
            continue;
        }
        let widest = best
            .as_ref()
            .and_then(|held| held.partitions.iter().find(|rows| rows.column == ACCOUNTS))
            .map(|held| held.len())
            .unwrap_or(0);
        if rows.len() > widest {
            best = Some(footer);
        }
    }
    best.unwrap_or_else(|| panic!("run {run} left no sorted run behind"))
}

/// Drive the rewrite until the run's sealed segment has been sorted
///
/// A seal is asynchronous, so a pass arriving while the sealer's hold stands finds
/// nothing to select and answers idle. One idle pass is not the end of the work.
fn rewrite_round(store: &ReelStore) -> u32 {
    let mut copied = 0u32;
    let mut quiet = 0u32;
    for _ in 0..SEAL_WAITS {
        let mut passes = 0u32;
        for _ in 0..16 {
            match store.compact_once().expect("compact") {
                CompactPass::Copied => passes += 1,
                CompactPass::Idle => break,
                CompactPass::Held => {}
            }
        }
        copied += passes;
        // Two idle attempts either side of a wait say the seal has landed and been
        // rewritten rather than that it has not arrived yet.
        quiet = match passes {
            0 => quiet + 1,
            _ => 0,
        };
        if quiet >= 2 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    copied
}

/// One run's place in the merge: its rows and how far through them the merge is
struct Cursor<'run> {
    rows: &'run FooterPartition,
    at: usize,
}

/// The row at the front of one run, as the heap orders it
#[derive(Eq, PartialEq)]
struct Head {
    key: [u8; KEY_WIDTH],
    run: usize,
    at: usize,
    lsn: u64,
}

impl Ord for Head {
    fn cmp(&self, other: &Head) -> std::cmp::Ordering {
        self.key.cmp(&other.key).then(self.run.cmp(&other.run))
    }
}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Head) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// What one merge did, in the terms a pacing budget is set in
struct Merged {
    in_rows: u64,
    out_rows: u64,
    in_bytes: u64,
    out_bytes: u64,
    secs: f64,
    cpu_secs: f64,
}

/// Merge N sorted runs into one output stream, the newest version of a key winning
///
/// A heap over the run heads rather than a concatenate and sort, since a merge holding
/// every row would be a different cost and a different memory bound. Newest is the
/// later run first and the higher sequence number inside a run, each volume counting
/// its sequence numbers from its own start.
fn merge(runs: &[&FooterPartition], into: &Path) -> Merged {
    let carry = runs[0].inline_width as usize;
    let in_rows: u64 = runs.iter().map(|rows| rows.len() as u64).sum();
    let in_bytes: u64 = runs.iter().map(|rows| rows.encoded_len() as u64).sum();

    let file = File::create(into).expect("create output");
    let mut out = BufWriter::with_capacity(1 << 20, file);
    let mut cursors: Vec<Cursor<'_>> = runs.iter().map(|rows| Cursor { rows, at: 0 }).collect();
    let mut heap: BinaryHeap<Reverse<Head>> = BinaryHeap::with_capacity(runs.len());
    let mut row = Vec::with_capacity(KEY_WIDTH + ROW_TAIL + ROW_CRC + carry);
    let mut out_rows = 0u64;
    let mut out_bytes = 0u64;

    let head = |cursor: &Cursor<'_>, run: usize| -> Option<Head> {
        let bytes = cursor.rows.key_at(cursor.at)?;
        let mut key = [0u8; KEY_WIDTH];
        key.copy_from_slice(bytes);
        let lsn = cursor.rows.row_at(cursor.at).expect("row").lsn.as_u64();
        Some(Head {
            key,
            run,
            at: cursor.at,
            lsn,
        })
    };

    let cpu_before = process_cpu_secs();
    let began = Instant::now();
    for (run, cursor) in cursors.iter().enumerate() {
        if let Some(head) = head(cursor, run) {
            heap.push(Reverse(head));
        }
    }

    while let Some(Reverse(first)) = heap.pop() {
        let key = first.key;
        let mut best = first;
        advance(&mut cursors, &mut heap, best.run, &head);
        while heap.peek().is_some_and(|Reverse(next)| next.key == key) {
            let Reverse(next) = heap.pop().expect("peeked");
            let from = next.run;
            if (next.run, next.lsn) >= (best.run, best.lsn) {
                best = next;
            }
            advance(&mut cursors, &mut heap, from, &head);
        }

        let rows = cursors[best.run].rows;
        row.clear();
        row.extend_from_slice(&key);
        let found = rows.row_at(best.at).expect("row");
        row.extend_from_slice(&found.lsn.pack());
        // The prototype relocates no record, so the winner keeps the offset it named
        // in the run it came from.
        row.extend_from_slice(&found.offset.to_le_bytes());
        row.extend_from_slice(&found.len.to_le_bytes());
        row.push(found.flags.bits());
        if carry > 0 {
            let crc_at = row.len();
            row.extend_from_slice(&0u32.to_le_bytes());
            let held = rows.carried_at(best.at).expect("carried").unwrap_or(&[]);
            row.extend_from_slice(held);
            row.resize(crc_at + ROW_CRC + carry, 0);
            let crc = checksum(&row);
            row[crc_at..crc_at + ROW_CRC].copy_from_slice(&crc.to_le_bytes());
        }
        out.write_all(&row).expect("write row");
        out_bytes += row.len() as u64;
        out_rows += 1;
    }

    out.flush().expect("flush output");
    let secs = began.elapsed().as_secs_f64();
    let cpu_secs = process_cpu_secs() - cpu_before;
    // Outside the clock, deliberately: an fsync is about four milliseconds whatever it
    // is syncing, which over a four-run merge is more than the merge.
    out.into_inner()
        .expect("output file")
        .sync_all()
        .expect("sync output");

    Merged {
        in_rows,
        out_rows,
        in_bytes,
        out_bytes,
        secs,
        cpu_secs,
    }
}

/// Step one run forward and put its next row back in the heap
fn advance(
    cursors: &mut [Cursor<'_>],
    heap: &mut BinaryHeap<Reverse<Head>>,
    run: usize,
    head: &impl Fn(&Cursor<'_>, usize) -> Option<Head>,
) {
    cursors[run].at += 1;
    if cursors[run].at < cursors[run].rows.len() {
        if let Some(next) = head(&cursors[run], run) {
            heap.push(Reverse(next));
        }
    }
}

/// Processor seconds this process has burned, every thread of it counted
///
/// Reported beside the wall clock because the per-key constant is what a policy would
/// be built on, and a wall clock would hide a stall in it.
fn process_cpu_secs() -> f64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return 0.0;
    }
    let seconds = |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1e6;
    seconds(usage.ru_utime) + seconds(usage.ru_stime)
}

// what merging N overlapping sorted runs costs, at two row shapes
//
// Measurement only, apart from one check that the sweep saw its variable: a merge
// whose output ratio does not fall as runs are added merged runs that did not overlap.
pub fn merge_by_run_count() {
    println!();
    println!(
        "{:>9} {:>4} {:>10} {:>10} {:>9} {:>9} {:>8} {:>9} {:>8} {:>11} {:>11}",
        "shape",
        "runs",
        "in rows",
        "out rows",
        "in MiB",
        "out MiB",
        "ratio",
        "merge s",
        "MB/s",
        "ns/in row",
        "cpu s/Mkey",
    );

    let out = TempDir::new().expect("tempdir");
    for (shape, columns) in [("carried", CARRIED), ("pointer", POINTER)] {
        let built = Instant::now();
        let footers = runs(columns);
        let standing: Vec<&FooterPartition> = footers
            .iter()
            .filter_map(|footer| {
                footer
                    .partitions
                    .iter()
                    .find(|partition| partition.column == ACCOUNTS)
            })
            .collect();
        let rows: Vec<usize> = standing.iter().map(|rows| rows.len()).collect();
        println!(
            "{shape}: {} sorted runs of {} to {} rows, {:.1?} to write and rewrite them",
            standing.len(),
            rows.iter().min().copied().unwrap_or(0),
            rows.iter().max().copied().unwrap_or(0),
            built.elapsed(),
        );
        assert_eq!(
            standing.len(),
            RUNS,
            "{shape} stood up the wrong number of runs"
        );

        let mut ratios = Vec::with_capacity(COUNTS.len());
        for count in COUNTS {
            let into = out.path().join("merged.rows");
            let in_rows: u64 = standing[..*count]
                .iter()
                .map(|rows| rows.len() as u64)
                .sum();
            let passes = (TARGET_ROWS / in_rows.max(1)) as usize;
            let mut runs = Vec::with_capacity(passes);
            for _ in 0..=passes.clamp(REPEATS, 200) {
                runs.push(merge(&standing[..*count], &into));
            }
            std::fs::remove_file(&into).expect("remove output");
            // The first quarter are warm passes and go: standing up the runs wrote
            // hundreds of megabytes the kernel is still writing back. The fastest of
            // the rest is quoted rather than the median, since every pass writes its
            // output to the same file and stacks dirty pages under the passes after it.
            runs.drain(..(runs.len() / 4).max(1));
            runs.sort_by(|left, right| left.secs.total_cmp(&right.secs));
            let merged = &runs[0];

            let ratio = merged.out_bytes as f64 / merged.in_bytes as f64;
            let nanos = merged.secs * 1e9 / merged.in_rows as f64;
            ratios.push(ratio);
            println!(
                "{shape:>9} {count:>4} {:>10} {:>10} {:>9.1} {:>9.1} {:>7.3}x {:>9.4} {:>8.0} {:>11.1} {:>11.3}",
                merged.in_rows,
                merged.out_rows,
                merged.in_bytes as f64 / (1 << 20) as f64,
                merged.out_bytes as f64 / (1 << 20) as f64,
                ratio,
                merged.secs,
                merged.in_bytes as f64 / 1e6 / merged.secs,
                nanos,
                merged.cpu_secs * 1e6 / merged.in_rows as f64,
            );
        }

        assert!(
            ratios[ratios.len() - 1] < ratios[0],
            "{shape} collapsed no more at {} runs than at {}: the runs do not overlap",
            COUNTS[COUNTS.len() - 1],
            COUNTS[0],
        );
        // Nothing is asserted about the timing columns: a nanosecond figure asserted
        // against another would fail on a busy laptop and prove nothing about the merge.
    }
}
