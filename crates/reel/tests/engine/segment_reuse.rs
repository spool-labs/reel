//! What a restart costs on disk: an empty tail leaves no file behind, and a
//! sealed segment keeps its records rather than its reservation
//!
//! Before these held, every clean stop sealed a header-only tail at its full
//! preallocation, and a node restarted daily banked a segment of slack per tail
//! per day. A store holding megabytes could sit on tens of gigabytes of shells.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;

use reel::config::{ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec, RecordKey};
use reel::io::fault::FaultPlan;
use reel::io::sim_backend::SimIo;
use reel::units::ByteCount;
use reel::{KeyWidth, Preallocate, ReelStore};

const ROWS: ColumnId = ColumnId(1);

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ROWS,
    name: "rows",
    key_width: KeyWidth::Fixed(16),
    shard_bytes: 0,
    purge_mark: None,
    codec: Codec::None,
}];

const SEGMENT: u64 = 1024 * 1024;

/// Zeros a tail lays down ahead of its head: four of this segment's draw margins
const WINDOW: u64 = (SEGMENT / 16) * 4;

fn key(at: u64) -> RecordKey {
    let mut bytes = [0u8; 16];
    bytes[8..].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(ROWS, &bytes).expect("key")
}

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT),
        alloc_chunk: ByteCount::from_bytes(SEGMENT / 4),
        preallocate: Preallocate::Full,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        ..ReelConfig::default()
    }
}

/// The segment files a root holds, smallest id first
fn segments_in(root: &Path) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = std::fs::read_dir(root)
        .expect("read root")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "reel"))
        .collect();
    found.sort();
    found
}

fn bytes_in(root: &Path) -> u64 {
    segments_in(root)
        .iter()
        .map(|path| std::fs::metadata(path).expect("metadata").len())
        .sum()
}

/// Where the bytes written to a file end: its first hole, or its length when it has none
///
/// A mapped tail stands at the whole segment while it is open, and the blocks past
/// what it wrote are reserved and unwritten, which a seek for a hole finds.
fn written_end(path: &Path) -> u64 {
    use std::os::unix::io::AsRawFd;
    let file = std::fs::File::open(path).expect("open segment");
    let end = unsafe { libc::lseek(file.as_raw_fd(), 0, libc::SEEK_HOLE) };
    assert!(end >= 0, "a seek for the first hole failed");
    end as u64
}

/// Copy every file in a root, the image a process that died would leave behind
fn copy_root(from: &Path, to: &Path) {
    for entry in std::fs::read_dir(from).expect("read root") {
        let entry = entry.expect("entry");
        if entry.path().is_file() {
            std::fs::copy(entry.path(), to.join(entry.file_name())).expect("copy");
        }
    }
}

// a new segment is zeroed to its window edge, and a walk over one stops at the records
#[test]
fn a_new_segment_is_written_through() {
    let home = TempDir::new().expect("home");
    let payload = vec![0x5Au8; 8 * 1024];

    let store = ReelStore::open(home.path().to_path_buf(), config(), COLUMNS).expect("open");
    store.put(&key(0), &payload).expect("put");

    let segments = segments_in(home.path());
    assert_eq!(segments.len(), 1, "the tail drew more than one segment");
    let bytes = std::fs::read(&segments[0]).expect("read segment");
    assert_eq!(
        written_end(&segments[0]),
        WINDOW,
        "the window was not written through at creation"
    );
    assert!(
        bytes[WINDOW as usize / 2..].iter().all(|byte| *byte == 0),
        "the fill past the records is not zeros"
    );

    // A flush journals the record's row, which is what a reopen finds it by, and the
    // copy is taken with the tail still open, as a crash leaves it.
    store.flush().expect("flush");
    let crashed = TempDir::new().expect("crashed");
    copy_root(home.path(), crashed.path());
    store.close().expect("close");
    drop(store);

    let store = ReelStore::open(crashed.path().to_path_buf(), config(), COLUMNS).expect("reopen");
    assert!(
        store.get(&key(0)).expect("get").is_some(),
        "the record before the fill went missing"
    );
    store.put(&key(1), &payload).expect("put after the crash");
    assert_eq!(
        segments_in(crashed.path()).len(),
        1,
        "the reopen drew a segment instead of resuming the one it found"
    );
}

// a tail zeros its next window only when its last one took enough syncs to pay for it
#[test]
fn only_a_tail_that_syncs_often_fills_its_next_window() {
    const BIG_SEGMENT: u64 = 64 * 1024 * 1024;
    const BIG_WINDOW: u64 = 4 * 1024 * 1024;
    let payload = vec![0x5Au8; 64 * 1024];
    let length_after = |sync: SyncPolicy| {
        let home = TempDir::new().expect("home");
        let config = ReelConfig {
            segment_bytes: ByteCount::from_bytes(BIG_SEGMENT),
            alloc_chunk: ByteCount::from_bytes(BIG_SEGMENT / 4),
            sync,
            ..config()
        };
        let store = ReelStore::open(home.path().to_path_buf(), config, COLUMNS).expect("open");
        for at in 0..80 {
            store.put(&key(at), &payload).expect("put");
        }
        let segments = segments_in(home.path());
        assert_eq!(segments.len(), 1, "the tail drew more than one segment");
        written_end(&segments[0])
    };

    for sync in [SyncPolicy::Never, SyncPolicy::Bytes(ByteCount::mb(1))] {
        let length = length_after(sync);
        assert!(
            length > BIG_WINDOW && length < 2 * BIG_WINDOW,
            "a tail with {sync:?} wrote zeros past its records, length {length}"
        );
    }
    assert_eq!(
        length_after(SyncPolicy::EveryPut),
        2 * BIG_WINDOW,
        "a tail that syncs every put did not zero its next window"
    );
}

// a store restarted idle keeps its one tail rather than drawing another
#[test]
fn an_idle_restart_keeps_one_segment() {
    let home = TempDir::new().expect("home");

    let mut seen: Option<Vec<PathBuf>> = None;
    for round in 0..5 {
        let store = ReelStore::open(home.path().to_path_buf(), config(), COLUMNS).expect("open");
        store.close().expect("close");
        drop(store);
        let left = segments_in(home.path());
        assert_eq!(left.len(), 1, "restart {round} changed the segment count");
        if let Some(before) = &seen {
            assert_eq!(&left, before, "restart {round} drew a fresh segment");
        }
        seen = Some(left);
    }
}

// a restart appends into the tail it left, and everything reads back
#[test]
fn a_restart_resumes_the_tail() {
    let home = TempDir::new().expect("home");
    let payload = vec![0x5Au8; 8 * 1024];

    let store = ReelStore::open(home.path().to_path_buf(), config(), COLUMNS).expect("open");
    store.put(&key(0), &payload).expect("put");
    store.close().expect("close");
    drop(store);

    let store = ReelStore::open(home.path().to_path_buf(), config(), COLUMNS).expect("reopen");
    assert_eq!(
        segments_in(home.path()).len(),
        1,
        "the reopen drew a segment instead of resuming"
    );
    store.put(&key(1), &payload).expect("put after resume");
    store.close().expect("close");
    drop(store);

    let store = ReelStore::open(home.path().to_path_buf(), config(), COLUMNS).expect("third open");
    for at in 0..2u64 {
        assert!(
            store.get(&key(at)).expect("get").is_some(),
            "key {at} went missing across the resumes"
        );
    }
    assert_eq!(segments_in(home.path()).len(), 1);
}

// a segment seals when it fills, and the sealed file ends at its footer
#[test]
fn a_full_segment_seals_at_its_footer() {
    let home = TempDir::new().expect("home");

    let store = ReelStore::open(home.path().to_path_buf(), config(), COLUMNS).expect("open");
    let payload = vec![0x5Au8; 8 * 1024];
    let puts = (SEGMENT / payload.len() as u64) + 8;
    for at in 0..puts {
        store.put(&key(at), &payload).expect("put");
    }
    store.close().expect("close");
    drop(store);

    let segments = segments_in(home.path());
    assert!(segments.len() >= 2, "the tail never rolled");
    let sealed = segments
        .iter()
        .filter(|path| {
            std::fs::read(path)
                .expect("read segment")
                .ends_with(b"REEL")
        })
        .count();
    assert_eq!(
        sealed,
        segments.len() - 1,
        "every rolled segment ends at its footer, the tail at its records"
    );

    let reopened = ReelStore::open(home.path().to_path_buf(), config(), COLUMNS).expect("reopen");
    for at in 0..puts {
        assert!(
            reopened.get(&key(at)).expect("get").is_some(),
            "key {at} went missing after the roll"
        );
    }
}

// a crash leaves only the records: the reservation never lives in the length
#[test]
fn a_crash_leaves_only_the_records() {
    let config = ReelConfig {
        sync: SyncPolicy::EveryPut,
        ..config()
    };
    let sim = SimIo::new(FaultPlan::new(1));
    let store = ReelStore::open_with_io(
        PathBuf::from("/reel"),
        config.clone(),
        COLUMNS,
        Arc::new(sim.clone()),
    )
    .expect("open");
    store.put(&key(0), &vec![0x5Au8; 8 * 1024]).expect("put");
    let image = sim.durable_image();
    drop(store);
    let widest = image
        .iter()
        .filter(|(path, _)| is_segment(path))
        .map(|(_, bytes)| bytes.len() as u64)
        .max()
        .expect("the crash image holds the tail");
    assert!(
        widest < SEGMENT / 4,
        "a crash image carried {widest} of a {SEGMENT} byte reservation"
    );

    let survivor = SimIo::from_image(image);
    let reopened = ReelStore::open_with_io(
        PathBuf::from("/reel"),
        config,
        COLUMNS,
        Arc::new(survivor.clone()),
    )
    .expect("reopen");
    assert!(reopened.get(&key(0)).expect("get").is_some());
}

// a flush after a close settles instead of parking on the doomed tail
#[test]
fn a_flush_after_close_settles() {
    let home = TempDir::new().expect("home");
    let store = ReelStore::open(home.path().to_path_buf(), config(), COLUMNS).expect("open");
    store.close().expect("close");
    store.flush().expect("a flush after close");
    drop(store);
}

fn is_segment(path: &Path) -> bool {
    path.extension()
        .is_some_and(|extension| extension == "reel")
}

// restarts after the data landed cost nothing further
#[test]
fn idle_restarts_do_not_grow_the_store() {
    let home = TempDir::new().expect("home");

    let store = ReelStore::open(home.path().to_path_buf(), config(), COLUMNS).expect("open");
    store.put(&key(0), &vec![0x5Au8; 8 * 1024]).expect("put");
    store.close().expect("close");
    drop(store);
    let settled = bytes_in(home.path());

    for _ in 0..5 {
        let store = ReelStore::open(home.path().to_path_buf(), config(), COLUMNS).expect("reopen");
        store.close().expect("close");
        drop(store);
    }

    assert_eq!(
        bytes_in(home.path()),
        settled,
        "idle restarts changed what the store weighs"
    );
}

// a clean stop leaves no segment holding blocks past its end
#[cfg(target_os = "linux")]
#[test]
fn a_close_gives_back_every_reservation() {
    use std::os::unix::fs::MetadataExt;

    let home = TempDir::new().expect("home");
    let config = ReelConfig {
        active_tails: ThreadBudget::threads(4),
        ..config()
    };
    let payload = vec![0x5Au8; 8 * 1024];
    for round in 0..2u64 {
        let store =
            ReelStore::open(home.path().to_path_buf(), config.clone(), COLUMNS).expect("open");
        std::thread::scope(|scope| {
            for writer in 0..4u64 {
                let (store, payload) = (&store, &payload);
                scope.spawn(move || {
                    for at in 0..300u64 {
                        let at = round << 32 | writer << 16 | at;
                        store.put(&key(at), payload).expect("put");
                    }
                });
            }
        });
        store.close().expect("close");
        drop(store);
    }

    let segments = segments_in(home.path());
    assert!(segments.len() > 4, "the tails never rolled");
    for path in segments {
        let meta = std::fs::metadata(&path).expect("metadata");
        let held = meta.blocks() * 512;
        let written = meta.len().next_multiple_of(4096);
        assert!(
            held <= written + 4096,
            "{} holds {held} bytes on disk for {} written",
            path.display(),
            meta.len()
        );
    }
}
