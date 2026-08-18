//! Where the dead bytes sit inside a segment, and what could be taken back
//! without copying anything
//!
//! A page chain, a smaller segment and a PUNCH_HOLE all reclaim a region only when
//! the whole region is dead, so they live or die on how long the contiguous dead
//! stretches are. Two things keep a number honest: whole-dead segments are left out,
//! since those are already unlinked at no cost, and a run is charged at block
//! alignment, since a punch can only take the whole blocks strictly inside it.
//! Record size is swept because it decides the answer, a dead 256 KiB record being a
//! run past any page size worth having and a dead 256 byte one a run below the block.

use std::path::Path;

use tempfile::TempDir;

use reel::format::footer::SegmentFooter;
use reel::format::loc::SegmentId;
use reel::format::record::{BLOCK, HEADER_LEN};
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, CompactRate, KeyWidth, MapShape, RecordKey,
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
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
    ColumnSpec {
        id: BLOB,
        name: "blob_data",
        key_width: KeyWidth::Fixed(32),
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
];

/// Bytes of first-write payload before any key is rewritten
///
/// A ratio inside a segment rather than a rate, so a laptop-sized volume answers it.
const VOLUME_BYTES: u64 = 128 * 1024 * 1024;

/// Small enough that the volume spans sixteen of them
const SEGMENT_BYTES: u64 = 8 * 1024 * 1024;

/// Times the hot keys are rewritten
const ROUNDS: usize = 3;

/// The page size a fixed-page reclamation scheme would be framed at
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
        alloc_chunk: ByteCount::from_bytes(SEGMENT_BYTES / 8),
        sync: SyncPolicy::Never,
        compact_mbps: CompactRate::Mbps(0),
        scrub_mbps: 0,
        ..ReelConfig::default()
    }
}

/// What one segment's dead space looks like once the survivors are removed
#[derive(Default)]
struct Shape {
    /// Segments holding at least one live record
    partly_live: usize,

    /// Segments holding none, which are unlinked whole and cost nothing
    whole_dead: usize,

    /// Dead bytes in the partly-live segments
    dead: u64,

    /// Those bytes again, by the length of the run they sit in
    under_block: u64,
    to_64k: u64,
    to_page: u64,
    from_page: u64,

    /// What a block-aligned punch could actually take out of those runs
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

/// Walk every sealed segment and classify its bytes against the live index
///
/// A row is live when the index still resolves its key to this exact place. A
/// tombstone counts as live, since compaction carries it until nothing older can
/// surface. Everything the live spans do not cover is dead, which folds pads and
/// shadowed records together the way a reclaimer would meet them.
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
        // An unsealed tail has no footer, and it holds the newest writes rather
        // than the shadows, so skipping it leaves the question unchanged.
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
                // The lsn alone would not do it: a compaction copy carries its
                // source's lsn, so the place has to be compared as well or a
                // relocated record would keep its source counted as live.
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

        // A segment with no survivors is retired by unlinking the file, so counting
        // its bytes as punchable would credit a punch with free reclamation.
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
