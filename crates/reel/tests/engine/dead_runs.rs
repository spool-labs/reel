//! Where the dead bytes sit inside a segment, and what could be reclaimed without copying

use std::path::Path;

use tempfile::TempDir;

use reel::format::footer::SegmentFooter;
use reel::format::loc::SegmentId;
use reel::format::record::{BLOCK, HEADER_LEN};
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, CompactRate, KeyWidth, RecordKey,
    ReelConfig, ReelStore, SyncPolicy,
};

const RECORDS: ColumnId = ColumnId(1);
const BLOB: ColumnId = ColumnId(2);

const COLUMNS: ColumnSet = &[
    ColumnSpec {
        id: RECORDS,
        name: "records",
        key_width: KeyWidth::Fixed(34),
        shard_bytes: 2,
        purge_mark: None,
        codec: Codec::None,
    },
    ColumnSpec {
        id: BLOB,
        name: "blob_data",
        key_width: KeyWidth::Fixed(32),
        shard_bytes: 0,
        purge_mark: None,
        codec: Codec::None,
    },
];

/// Bytes of first-write payload before any key is rewritten
const VOLUME_BYTES: u64 = 128 * 1024 * 1024;

/// Small enough that the volume spans sixteen of them
const SEGMENT_BYTES: u64 = 8 * 1024 * 1024;

/// The hot keys are rewritten this many times
const ROUNDS: usize = 3;

/// Page size of a fixed-page reclamation scheme
const PAGE: u64 = 1024 * 1024;

/// Record sizes: a large record, a middling one, and the metadata shape
const SIZES: &[(&str, usize)] = &[("256K", 256 * 1024), ("4K", 4096), ("256B", 256)];

/// How much of the key set is rewritten, as the stride taken over it
const HOT: &[(&str, usize)] = &[("all", 1), ("half", 2), ("tenth", 10)];

fn unique_id() -> [u8; 32] {
    rand::random()
}

fn record_key(group: u16, id: [u8; 32]) -> RecordKey {
    let mut bytes = [0u8; 34];
    bytes[..2].copy_from_slice(&group.to_be_bytes());
    bytes[2..].copy_from_slice(&id);
    RecordKey::from_bytes(RECORDS, &bytes).expect("key")
}

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT_BYTES),
        sync: SyncPolicy::Never,
        compact_mbps: CompactRate::Mbps(0),
        scrub_mbps: 0,
        ..ReelConfig::default()
    }
}

/// What the sealed segments' dead space looks like around the survivors
#[derive(Default)]
struct Shape {
    /// Segments holding at least one live record
    partly_live: usize,

    /// Segments holding none, which are unlinked whole and cost nothing
    whole_dead: usize,

    /// Dead bytes in the partly-live segments
    dead: u64,

    /// Dead bytes in runs shorter than a block
    under_block: u64,

    /// Dead bytes in runs from a block up to 64 KiB
    to_64k: u64,

    /// Dead bytes in runs from 64 KiB up to a page
    to_page: u64,

    /// Dead bytes in runs of a page or longer
    from_page: u64,

    /// What a block-aligned punch could take out of those runs
    punchable: u64,

    /// Longest single dead run seen
    longest: u64,
}

impl Shape {
    fn credit(&mut self, run: u64) {
        self.dead += run;
        self.longest = self.longest.max(run);
        match run {
            r if r < BLOCK => self.under_block += r,
            r if r < 64 * 1024 => self.to_64k += r,
            r if r < PAGE => self.to_page += r,
            r => self.from_page += r,
        }
    }
}

fn pct(part: u64, whole: u64) -> f64 {
    match whole {
        0 => 0.0,
        _ => part as f64 * 100.0 / whole as f64,
    }
}

/// Walks every sealed segment and classifies its bytes against the live index
fn measure(dir: &Path, store: &ReelStore) -> Shape {
    let mut shape = Shape::default();
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .expect("read volume dir")
        .filter_map(|entry| entry.ok().map(|found| found.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "reel"))
        .collect();
    files.sort();

    for path in files {
        let bytes = std::fs::read(&path).expect("read segment");
        // An unsealed tail has no footer and holds the newest writes, so skip it
        let Ok(footer) = SegmentFooter::parse(&bytes) else {
            continue;
        };
        let segment = SegmentId(
            path.file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.parse().ok())
                .expect("segment file is numbered"),
        );

        let mut live: Vec<(u64, u64)> = vec![(0, BLOCK)];
        let mut region_end = BLOCK;
        let mut live_rows = 0usize;

        for partition in &footer.partitions {
            assert!(
                !partition.is_varying(),
                "a varying-width partition cannot have its span derived from the directory"
            );
            let width = partition.key_width as usize;
            for row in partition.entries() {
                let row = row.expect("footer row");
                let start = row.offset as u64;
                let span = (HEADER_LEN + width + row.len as usize) as u64;
                region_end = region_end.max(start + span);

                let held = row.is_tombstone() || row.is_range_tombstone();
                // Compare the place too, since a compaction copy keeps its source's lsn
                let resolved = match store.index().get(&row.key).expect("index get") {
                    Some(found) => found.loc.segment == segment && found.loc.offset as u64 == start,
                    None => false,
                };
                if held || resolved {
                    live.push((start, start + span));
                    live_rows += 1;
                }
            }
        }

        // A segment with no survivors is unlinked whole, so its bytes are not punchable
        if live_rows == 0 {
            shape.whole_dead += 1;
            continue;
        }
        shape.partly_live += 1;

        live.sort_unstable();
        let mut at = 0u64;
        for (start, end) in live {
            if start > at {
                shape.credit(start - at);
                shape.punchable += punchable(at, start);
            }
            at = at.max(end);
        }
        if region_end > at {
            shape.credit(region_end - at);
            shape.punchable += punchable(at, region_end);
        }
    }

    shape
}

/// Whole blocks strictly inside a run, which is all a punch can take
fn punchable(start: u64, end: u64) -> u64 {
    let first = start.div_ceil(BLOCK) * BLOCK;
    let last = end / BLOCK * BLOCK;
    last.saturating_sub(first)
}

// dead-run length distribution across record sizes and rewrite fractions
#[test]
#[ignore = "writes real files, run explicitly with --ignored --nocapture"]
fn how_long_the_dead_runs_are() {
    println!();
    println!(
        "{} MiB written in {} MiB segments, {ROUNDS} rewrite rounds, tombstones counted as held\n",
        VOLUME_BYTES / (1024 * 1024),
        SEGMENT_BYTES / (1024 * 1024),
    );
    println!(
        "{:>5} {:>6} {:>7} {:>6} {:>10} {:>8} {:>8} {:>8} {:>8} {:>9} {:>10}",
        "size",
        "hot",
        "partly",
        "whole",
        "dead MiB",
        "<4K",
        "<64K",
        "<1M",
        ">=1M",
        "punch%",
        "longest",
    );

    let group = 1u16;
    for (size_label, record_bytes) in SIZES {
        for (hot_label, stride) in HOT {
            let count = (VOLUME_BYTES / *record_bytes as u64) as usize;
            let dir = TempDir::new().expect("tempdir");
            let store = ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("open");

            let ids: Vec<[u8; 32]> = (0..count).map(|_| unique_id()).collect();
            let body = vec![0x5au8; *record_bytes];
            for id in &ids {
                store
                    .put_owned(&record_key(group, *id), body.clone())
                    .expect("put");
            }
            for round in 0..ROUNDS {
                let body = vec![0xB0u8.wrapping_add(round as u8); *record_bytes];
                for id in ids.iter().step_by(*stride) {
                    store
                        .put_owned(&record_key(group, *id), body.clone())
                        .expect("update");
                }
            }
            store.flush().expect("flush");

            let shape = measure(dir.path(), &store);
            println!(
                "{size_label:>5} {hot_label:>6} {:>7} {:>6} {:>10.1} {:>7.1}% {:>7.1}% {:>7.1}% {:>7.1}% {:>8.1}% {:>10}",
                shape.partly_live,
                shape.whole_dead,
                shape.dead as f64 / (1024.0 * 1024.0),
                pct(shape.under_block, shape.dead),
                pct(shape.to_64k, shape.dead),
                pct(shape.to_page, shape.dead),
                pct(shape.from_page, shape.dead),
                pct(shape.punchable, shape.dead),
                shape.longest,
            );
        }
    }
}
