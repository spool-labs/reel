//! Placement bands: what a segment holds once a caller names death windows
//!
//! The claim the mechanism makes is narrow and checkable on one volume: a segment a
//! band drew holds that band's records and nothing else, the file says which band that
//! was across a restart, and a rewrite puts the survivors back under the same one.
//! Everything the win is made of rests on those, so they are asserted rather than
//! measured here.

use std::collections::HashMap;

use tempfile::TempDir;

use reel::format::footer::SegmentFooter;
use reel::format::loc::SegmentId;
use reel::format::record::{RecordHeader, HEADER_LEN};
use reel::format::segment_header::SegmentHeader;
use reel::{
    Band, ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, CompactPass, CompactRate, KeyWidth,
    MapShape, Preallocate, RecordKey, RecordWrite, ReelConfig, ReelStore, SyncPolicy, ThreadBudget,
};

const RECORDS: ColumnId = ColumnId(1);

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: RECORDS,
    name: "records",
    key_width: KeyWidth::Fixed(8),
    shard_bytes: 0,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// Small enough that a few hundred records fill several of them
const SEGMENT_BYTES: u64 = 256 * 1024;

const PAYLOAD: usize = 1024;

fn key(at: u64) -> RecordKey {
    RecordKey::from_bytes(RECORDS, &at.to_be_bytes()).expect("key")
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

fn put(store: &ReelStore, at: u64, band: Option<Band>) {
    store
        .apply_batch_banded(
            vec![RecordWrite::Put {
                key: key(at),
                payload: vec![0x5a; PAYLOAD],
            }],
            band,
        )
        .expect("put");
}

// a segment a band drew holds that band's records and nothing else
//
// The bands are written interleaved, one record at a time, so nothing but the routing
// could be keeping them apart.
#[test]
fn a_segment_holds_one_band() {
    let dir = TempDir::new().expect("tempdir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(4), COLUMNS).expect("open");

    let mut bands: HashMap<Vec<u8>, Option<Band>> = HashMap::new();
    for at in 0..900u64 {
        let band = match at % 3 {
            0 => None,
            held => Some(Band(held)),
        };
        bands.insert(key(at).as_slice().to_vec(), band);
        put(&store, at, band);
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
    assert!(banded >= 2, "neither band ever drew a segment of its own");
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

    let band = Band(7);
    for at in 0..600u64 {
        put(&store, at, Some(band));
    }
    store.flush().expect("flush");
    store.cue().expect("cue");
    while store.sweep_covers().expect("sweep") {}

    // Most of the band dies, which is what puts its segments over the rewrite bar with
    // survivors still in them.
    for at in 0..600u64 {
        if at % 5 != 0 {
            store.delete(&key(at)).expect("delete");
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

    // Every survivor now reads out of a segment stamped with the band it was written
    // under, whichever pass moved it.
    let sealed = sealed_bands(dir.path());
    let mut survivors = 0;
    for at in (0..600u64).step_by(5) {
        let entry = store
            .index()
            .get(&key(at))
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
    let band = Band(11);
    {
        let store = ReelStore::open(dir.path().to_path_buf(), config(4), COLUMNS).expect("open");
        for at in 0..600u64 {
            put(&store, at, Some(band));
        }
        store.flush().expect("flush");
        store.cue().expect("cue");
        store.close().expect("close");
    }

    let store = ReelStore::open(dir.path().to_path_buf(), config(4), COLUMNS).expect("reopen");
    assert!(
        store.tail_bands().iter().all(Option::is_none),
        "a reopened pool remembers nothing"
    );
    for at in 0..600u64 {
        if at % 5 != 0 {
            store.delete(&key(at)).expect("delete");
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
            .get(&key(at))
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

// a released band gives its tail back, and nothing follows the band into its segments
#[test]
fn a_released_band_gives_its_tail_back() {
    let dir = TempDir::new().expect("tempdir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(4), COLUMNS).expect("open");

    put(&store, 1, Some(Band(1)));
    assert_eq!(
        store.tail_bands().iter().flatten().count(),
        1,
        "the band took no tail"
    );

    assert!(store.release_band(Band(1)).expect("release"));
    assert_eq!(store.tail_bands().iter().flatten().count(), 0);
    assert!(
        !store.release_band(Band(1)).expect("release"),
        "a band holding no tail cannot give one back"
    );

    // The window is closed, so what follows must not land where its records did.
    put(&store, 2, None);
    store.flush().expect("flush");
    store.cue().expect("cue");
    for (segment, band, footer) in sealed_bands(dir.path()) {
        if band == Some(Band(1)) {
            assert!(
                !keys_of(&footer).contains(&key(2)),
                "segment {} took a record after its band was given back",
                segment.as_u32(),
            );
        }
    }
    store.close().expect("close");
}

// more live bands than tails falls back to unbanded rather than stalling or thrashing
#[test]
fn a_band_with_no_tail_falls_back() {
    let dir = TempDir::new().expect("tempdir");
    // Two tails, so one band can be held and the other tail stays unbanded.
    let store = ReelStore::open(dir.path().to_path_buf(), config(2), COLUMNS).expect("open");

    for at in 0..300u64 {
        put(&store, at, Some(Band(at % 8)));
    }
    store.flush().expect("flush");

    assert!(
        store.band_fallbacks() > 0,
        "eight bands over one banded tail should have fallen back"
    );
    // Placement is the only thing that gives: every record is still where the index
    // says it is.
    for at in 0..300u64 {
        assert!(store.get(&key(at)).expect("get").is_some(), "lost {at}");
    }
    store.close().expect("close");
}
