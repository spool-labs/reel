//! Placement bands: what a segment holds once a column declares its keys marked
//!
//! A segment a band drew holds that band's records and nothing else, the file says
//! which band that was across a restart, and a rewrite puts the survivors back under
//! the same one.

use std::collections::HashMap;

use tempfile::TempDir;

use reel::format::footer::SegmentFooter;
use reel::format::loc::SegmentId;
use reel::format::record::{RecordHeader, HEADER_LEN};
use reel::format::segment_header::SegmentHeader;
use reel::{
    Band, ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, CompactPass, CompactRate, KeyWidth,
    MapShape, Preallocate, PurgeMark, RecordKey, RecordWrite, ReelConfig, ReelStore, SyncPolicy,
    ThreadBudget,
};

const MARKED: ColumnId = ColumnId(1);
const PLAIN: ColumnId = ColumnId(2);

/// A marked key is an identifier and then the position it dies at
const DEATH_AT: u8 = 8;

const COLUMNS: ColumnSet = &[
    ColumnSpec {
        id: MARKED,
        name: "marked",
        key_width: KeyWidth::Fixed(16),
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: Some(PurgeMark::placing(DEATH_AT)),
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
    ColumnSpec {
        id: PLAIN,
        name: "plain",
        key_width: KeyWidth::Fixed(8),
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
];

/// Small enough that a few hundred records fill several of them
const SEGMENT_BYTES: u64 = 256 * 1024;

const PAYLOAD: usize = 1024;

/// Far enough out that the floors these tests set stay under every death
const FLOOR: u64 = 1_000;

fn key(at: u64, death: u64) -> RecordKey {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&at.to_be_bytes());
    bytes[8..].copy_from_slice(&death.to_be_bytes());
    RecordKey::from_bytes(MARKED, &bytes).expect("key")
}

fn plain_key(at: u64) -> RecordKey {
    RecordKey::from_bytes(PLAIN, &at.to_be_bytes()).expect("key")
}

fn config(tails: u32) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT_BYTES),
        alloc_chunk: ByteCount::from_bytes(SEGMENT_BYTES),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(tails),
        compact_mbps: CompactRate::Mbps(0),
        scrub_mbps: 0,
        ..ReelConfig::default()
    }
}

/// Every sealed segment in the volume, with the band its header says it was drawn under
fn sealed_bands(dir: &std::path::Path) -> Vec<(SegmentId, Option<Band>, SegmentFooter)> {
    let mut found = Vec::new();
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .expect("read volume dir")
        .filter_map(|entry| entry.ok().map(|held| held.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "reel"))
        .collect();
    files.sort();
    for path in files {
        let bytes = std::fs::read(&path).expect("read segment");
        // An unsealed tail has no footer, and what it holds has not been placed for
        // good yet, so it is not part of the question.
        let Ok(footer) = SegmentFooter::parse(&bytes) else {
            continue;
        };
        let header = RecordHeader::unpack(&bytes).expect("a segment starts with its header");
        assert!(header.flags.is_segment_header());
        let payload = &bytes[HEADER_LEN..HEADER_LEN + header.length as usize];
        let stamp = SegmentHeader::unpack(payload).expect("segment header");
        found.push((stamp.segment, stamp.band, footer));
    }
    found
}

/// Every key one segment's footer names
fn keys_of(footer: &SegmentFooter) -> Vec<RecordKey> {
    let mut keys = Vec::new();
    for partition in &footer.partitions {
        for row in partition.entries() {
            keys.push(row.expect("footer row").key);
        }
    }
    keys
}

fn put(store: &ReelStore, key: RecordKey) {
    store.put_owned(&key, vec![0x5a; PAYLOAD]).expect("put");
}

// a segment a band drew holds that band's records and nothing else
//
// Written interleaved, one record at a time, so only the routing keeps them apart.
#[test]
fn a_segment_holds_one_band() {
    let dir = TempDir::new().expect("tempdir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(4), COLUMNS).expect("open");
    store.purge_below(FLOOR);

    let mut bands: HashMap<Vec<u8>, Option<Band>> = HashMap::new();
    for at in 0..900u64 {
        if at % 3 == 0 {
            let key = plain_key(at);
            bands.insert(key.as_slice().to_vec(), None);
            store.put_owned(&key, vec![0x5a; PAYLOAD]).expect("put");
            continue;
        }
        let death = match at % 3 {
            1 => FLOOR + 4,
            _ => FLOOR + 4096,
        };
        let key = key(at, death);
        bands.insert(key.as_slice().to_vec(), Some(Band::of(death, FLOOR)));
        put(&store, key);
    }
    store.flush().expect("flush");
    store.cue().expect("cue");

    let sealed = sealed_bands(dir.path());
    let mut banded = 0;
    for (segment, band, footer) in &sealed {
        let keys = keys_of(footer);
        if keys.is_empty() {
            continue;
        }
        if band.is_some() {
            banded += 1;
        }
        for held in keys {
            assert_eq!(
                bands.get(held.as_slice()).copied().flatten(),
                *band,
                "segment {} carries a record of another band",
                segment.as_u32(),
            );
        }
    }
    assert!(banded >= 2, "neither window ever drew a segment of its own");
    store.close().expect("close");
}

// the survivors of a banded segment are rewritten under the same band
//
// Placement the writer paid for is undone otherwise: one rewrite would put a window's
// records back in the mixture they were kept out of.
#[test]
fn a_rewrite_keeps_the_band() {
    let dir = TempDir::new().expect("tempdir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(4), COLUMNS).expect("open");
    store.purge_below(FLOOR);

    let death = FLOOR + 64;
    let band = Band::of(death, FLOOR);
    for at in 0..600u64 {
        put(&store, key(at, death));
    }
    store.flush().expect("flush");
    store.cue().expect("cue");
    while store.sweep_covers().expect("sweep") {}

    // Most of the window dies, which is what puts its segments over the rewrite bar
    // with survivors still in them.
    for at in 0..600u64 {
        if at % 5 != 0 {
            store.delete(&key(at, death)).expect("delete");
        }
    }
    store.flush().expect("flush");
    store.cue().expect("cue");
    while store.sweep_covers().expect("sweep") {}

    let mut copied = false;
    for _ in 0..200 {
        match store.compact_once().expect("compact") {
            CompactPass::Copied => copied = true,
            CompactPass::Idle => break,
            CompactPass::Held => break,
        }
    }
    assert!(copied, "nothing was rewritten, so nothing was placed");
    store.flush().expect("flush");
    store.cue().expect("cue");

    let sealed = sealed_bands(dir.path());
    let mut survivors = 0;
    for at in (0..600u64).step_by(5) {
        let entry = store
            .index()
            .get(&key(at, death))
            .expect("index get")
            .expect("a survivor still resolves");
        let (_, stamp, _) = sealed
            .iter()
            .find(|(segment, _, _)| *segment == entry.loc.segment)
            .expect("the survivor's segment is sealed");
        assert_eq!(*stamp, Some(band), "a survivor left its band");
        survivors += 1;
    }
    assert_eq!(survivors, 120);
    store.close().expect("close");
}

// a band outlives the process that wrote it, since the segment carries it
//
// The pool is in memory and a reopen starts it empty, so a rewrite after a restart can
// only place its survivors from what the file says.
#[test]
fn a_band_survives_a_reopen() {
    let dir = TempDir::new().expect("tempdir");
    let death = FLOOR + 64;
    let band = Band::of(death, FLOOR);
    {
        let store = ReelStore::open(dir.path().to_path_buf(), config(4), COLUMNS).expect("open");
        store.purge_below(FLOOR);
        for at in 0..600u64 {
            put(&store, key(at, death));
        }
        store.flush().expect("flush");
        store.cue().expect("cue");
        store.close().expect("close");
    }

    let store = ReelStore::open(dir.path().to_path_buf(), config(4), COLUMNS).expect("reopen");
    store.purge_below(FLOOR);
    assert!(
        store.tail_bands().iter().all(Option::is_none),
        "a reopened pool remembers nothing"
    );
    for at in 0..600u64 {
        if at % 5 != 0 {
            store.delete(&key(at, death)).expect("delete");
        }
    }
    store.flush().expect("flush");
    store.cue().expect("cue");
    while store.sweep_covers().expect("sweep") {}

    let mut copied = false;
    for _ in 0..200 {
        match store.compact_once().expect("compact") {
            CompactPass::Copied => copied = true,
            CompactPass::Idle | CompactPass::Held => break,
        }
    }
    assert!(copied, "nothing was rewritten, so nothing was placed");
    store.flush().expect("flush");
    store.cue().expect("cue");

    let sealed = sealed_bands(dir.path());
    let mut survivors = 0;
    for at in (0..600u64).step_by(5) {
        let entry = store
            .index()
            .get(&key(at, death))
            .expect("index get")
            .expect("a survivor still resolves");
        let (_, stamp, _) = sealed
            .iter()
            .find(|(segment, _, _)| *segment == entry.loc.segment)
            .expect("the survivor's segment is sealed");
        assert_eq!(
            *stamp,
            Some(band),
            "a survivor lost its band across the open"
        );
        survivors += 1;
    }
    assert_eq!(survivors, 120);
    store.close().expect("close");
}

// a floor moved past a window gives its tail back, with nobody asking
#[test]
fn a_finished_window_gives_its_tail_back() {
    let dir = TempDir::new().expect("tempdir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(4), COLUMNS).expect("open");
    store.purge_below(FLOOR);

    let death = FLOOR + 4;
    let band = Band::of(death, FLOOR);
    put(&store, key(1, death));
    assert_eq!(
        store.tail_bands().iter().flatten().count(),
        1,
        "the window took no tail"
    );

    // Past the end of the window, so everything it holds is dead.
    store.purge_below(band.as_u64());
    put(&store, key(2, band.as_u64() + 4096));
    assert!(
        !store
            .tail_bands()
            .iter()
            .flatten()
            .any(|held| *held == band),
        "a finished window kept its tail"
    );

    store.flush().expect("flush");
    store.cue().expect("cue");
    for (segment, held, footer) in sealed_bands(dir.path()) {
        if held == Some(band) {
            assert!(
                !keys_of(&footer).contains(&key(2, band.as_u64() + 4096)),
                "segment {} took a record after its window was finished",
                segment.as_u32(),
            );
        }
    }
    store.close().expect("close");
}

// more live windows than tails falls back to unbanded rather than stalling or thrashing
#[test]
fn a_band_with_no_tail_falls_back() {
    let dir = TempDir::new().expect("tempdir");
    // Two tails, so one band can be held and the other tail stays unbanded.
    let store = ReelStore::open(dir.path().to_path_buf(), config(2), COLUMNS).expect("open");
    store.purge_below(FLOOR);

    for at in 0..300u64 {
        put(&store, key(at, FLOOR + 4 + (at % 8) * 32));
    }
    store.flush().expect("flush");

    assert!(
        store.band_fallbacks() > 0,
        "eight windows over one banded tail should have fallen back"
    );
    // Placement is the only thing that gives: every record is still where the index
    // says it is.
    for at in 0..300u64 {
        let key = key(at, FLOOR + 4 + (at % 8) * 32);
        assert!(store.get(&key).expect("get").is_some(), "lost {at}");
    }
    store.close().expect("close");
}

// a column declaring no mark is routed exactly as it was before bands existed
#[test]
fn an_unmarked_column_claims_nothing() {
    let dir = TempDir::new().expect("tempdir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(4), COLUMNS).expect("open");
    store.purge_below(FLOOR);

    for at in 0..300u64 {
        store
            .put_owned(&plain_key(at), vec![0x5a; PAYLOAD])
            .expect("put");
    }
    store.flush().expect("flush");

    assert!(store.tail_bands().iter().all(Option::is_none));
    assert_eq!(store.band_fallbacks(), 0);
    store.close().expect("close");
}

// a batch mixing windows takes the one covering the last of them to die
#[test]
fn a_batch_takes_the_band_that_covers_it() {
    let dir = TempDir::new().expect("tempdir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(4), COLUMNS).expect("open");
    store.purge_below(FLOOR);

    let deaths = [FLOOR + 4, FLOOR + 40, FLOOR + 400];
    let covering = deaths
        .iter()
        .map(|death| Band::of(*death, FLOOR))
        .max()
        .expect("bands");
    store
        .apply_batch(
            deaths
                .iter()
                .enumerate()
                .map(|(at, death)| RecordWrite::Put {
                    key: key(at as u64, *death),
                    payload: vec![0x5a; PAYLOAD],
                })
                .collect(),
        )
        .expect("batch");

    assert!(
        store.tail_bands().contains(&Some(covering)),
        "the batch took a window it does not outlive"
    );

    // A batch carrying anything unplaced takes no window at all.
    store
        .apply_batch(vec![
            RecordWrite::Put {
                key: key(9, FLOOR + 4),
                payload: vec![0x5a; PAYLOAD],
            },
            RecordWrite::Put {
                key: plain_key(9),
                payload: vec![0x5a; PAYLOAD],
            },
        ])
        .expect("batch");
    assert_eq!(store.tail_bands().iter().flatten().count(), 1);
    store.close().expect("close");
}
