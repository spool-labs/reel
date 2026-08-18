//! The ring backend serving a whole volume, on the platform that has one
//!
//! The simulator cannot stand in here: what these cover is the real ring taking
//! real submissions, so they run against a temporary directory on Linux and are
//! absent everywhere else.
//!
//! Knobs, all optional, so one binary sweeps the ring instead of one build each:
//! REEL_RING_NO_REGISTERED_BUFFERS, REEL_RING_WAIT (kernel|spin|auto).

#![cfg(target_os = "linux")]

use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use tempfile::tempdir;

use reel::config::{IoBackend, RingTuning, RingWait, SyncPolicy};
use reel::io::op::{Completion, FileId, Op, Outcome, ReadBuf, Tag};
use reel::io::uring_backend::UringBackend;
use reel::io::ReelIo;
use reel::reel::segment::IoDriver;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, RecordKey, ReelConfig,
    ReelStore,
};

/// Bytes the group takes at the front of a record key
const GROUP_PREFIX_LEN: usize = 2;

/// Bytes a record key occupies
const RECORD_KEY_LEN: usize = GROUP_PREFIX_LEN + 32;

const RECORDS: ColumnId = ColumnId(1);
const RECORDS_CF: &str = "records";

const BLOB: ColumnId = ColumnId(2);
const BLOB_CF: &str = "blob_data";

const COLUMNS: ColumnSet = &[
    ColumnSpec {
        id: RECORDS,
        name: RECORDS_CF,
        key_width: KeyWidth::Fixed(RECORD_KEY_LEN as u16),
        shard_bytes: GROUP_PREFIX_LEN as u8,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
    ColumnSpec {
        id: BLOB,
        name: BLOB_CF,
        key_width: KeyWidth::Fixed(32),
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
];

/// Group the fixtures write into
const GROUP: u16 = 7;

/// A record key: the group big endian, then the identifier
fn record_key(group: u16, id: [u8; 32]) -> RecordKey {
    let mut bytes = [0u8; RECORD_KEY_LEN];
    bytes[..GROUP_PREFIX_LEN].copy_from_slice(&group.to_be_bytes());
    bytes[GROUP_PREFIX_LEN..].copy_from_slice(&id);
    RecordKey::from_bytes(RECORDS, &bytes).expect("record key")
}

fn id(byte: u8) -> [u8; 32] {
    [byte; 32]
}

/// Open a volume on a ring built here, so a downgrade cannot pass for a pass
///
/// Letting the selector choose would make these tests agree with themselves anywhere: a
/// machine that refuses the ring hands back the posix backend and every assertion below
/// still holds, having tested nothing.
fn open_on_ring(root: &std::path::Path) -> ReelStore {
    let ring = UringBackend::new(false, tuning()).unwrap_or_else(|error| {
        panic!(
            "io_uring is unavailable here ({error}). A container blocks the three \
                 io_uring syscalls under the stock seccomp profile: either run with \
                 --security-opt seccomp=unconfined, or take moby's default profile and \
                 add io_uring_setup, io_uring_enter, and io_uring_register to it, which \
                 leaves the rest of the filtering in place."
        )
    });
    // The engine only creates directories on the path that owns the filesystem,
    // and handing it a backend puts it on the other one, so the layout is made here.
    std::fs::create_dir_all(root.join("reel-0007")).expect("reel dir");
    ReelStore::open_with_io(root.to_path_buf(), ring_config(), COLUMNS, Arc::new(ring))
        .expect("open")
}

/// Open a volume whose descriptors bypass the page cache, on a ring built here
///
/// Direct is named here rather than read off the environment, since the registered
/// buffers a direct volume's ops fly through are reachable no other way. The backend
/// comes back beside the store so a caller can ask which door its ops took.
fn open_direct_on_ring(root: &std::path::Path) -> (ReelStore, Arc<UringBackend>) {
    let ring = Arc::new(UringBackend::new(true, tuning()).expect("ring"));
    std::fs::create_dir_all(root.join("reel-0007")).expect("reel dir");
    let config = ReelConfig {
        io_backend: IoBackend::UringDirect,
        ..ring_config()
    };
    let io = Arc::clone(&ring) as Arc<dyn ReelIo>;
    let store = ReelStore::open_with_io(root.to_path_buf(), config, COLUMNS, io).expect("open");
    (store, ring)
}

/// Ring tunables for this run, so one binary sweeps the wait instead of one build each
///
///   REEL_RING_WAIT=kernel cargo test --test uring_backend
fn tuning() -> RingTuning {
    let flag = |name: &str, unset: bool| {
        std::env::var(name)
            .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
            .unwrap_or(unset)
    };
    RingTuning {
        registered_buffers: !flag("REEL_RING_NO_REGISTERED_BUFFERS", false),
        wait: match std::env::var("REEL_RING_WAIT").as_deref() {
            Ok("kernel") => RingWait::Kernel,
            Ok("spin") => RingWait::Spin,
            _ => RingWait::Auto,
        },
    }
}

fn ring_config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::mb(4),
        alloc_chunk: ByteCount::mb(1),
        sync: SyncPolicy::Bytes(ByteCount::mb(1)),
        scrub_mbps: 0,
        io_backend: IoBackend::Uring,
        uring: tuning(),
        ..ReelConfig::default()
    }
}

// the ring sets up at all, which is what a blocked syscall would deny
#[test]
fn ring_opens() {
    let backend = UringBackend::new(false, tuning());
    assert!(
        backend.is_ok(),
        "io_uring setup failed, so the ring is blocked here: {:?}",
        backend.err()
    );
}

// a volume on the ring writes, reads back, and deletes through it
#[test]
fn ring_serves_a_volume() {
    let dir = tempdir().expect("tempdir");
    let store = open_on_ring(dir.path());

    for byte in 1..=32u8 {
        let payload = vec![byte; 4096 + byte as usize];
        store
            .put(&record_key(GROUP, id(byte)), &payload)
            .expect("put");
    }
    store.flush().expect("flush");

    for byte in 1..=32u8 {
        let read = store
            .get(&record_key(GROUP, id(byte)))
            .expect("get")
            .expect("present");
        assert_eq!(read.len(), 4096 + byte as usize);
        assert!(
            read.iter().all(|held| *held == byte),
            "payload came back whole"
        );
    }

    // A batch read goes down as one submission on the reading thread's own ring.
    let wanted: Vec<_> = (1..=32u8).map(|byte| record_key(GROUP, id(byte))).collect();
    let many = store.get_many(&wanted).expect("get_many");
    assert_eq!(many.len(), 32);
    for (at, held) in many.into_iter().enumerate() {
        let byte = at as u8 + 1;
        let read = held.expect("present");
        assert_eq!(read.len(), 4096 + byte as usize);
        assert!(
            read.iter().all(|held| *held == byte),
            "record {byte} came back whole"
        );
    }

    store.delete(&record_key(GROUP, id(1))).expect("delete");
    assert_eq!(store.get(&record_key(GROUP, id(1))).expect("get"), None);
    assert_eq!(store.totals().count, 31);
}

// a window of a record on the ring is its own bytes, through either door
#[test]
fn the_ring_serves_a_window() {
    let dir = tempdir().expect("tempdir");
    let store = open_on_ring(dir.path());
    let key = record_key(GROUP, id(3));
    let payload: Vec<u8> = (0..64 * 1024).map(|at| (at % 251) as u8).collect();
    store.put(&key, &payload).expect("put");
    store.flush().expect("flush");

    // Near and deep windows both take the vouched single read down the ring.
    for (at, len) in [
        (0u64, 64usize),
        (128, 4096),
        (40_000, 4_000),
        (65_000, 4_000),
    ] {
        let blocked = store
            .get_range(&key, at, len)
            .expect("range")
            .expect("present");
        let awaited = block_on(store.get_range_wait(&key, at, len))
            .expect("awaited range")
            .expect("present");

        let from = at as usize;
        let to = from.saturating_add(len).min(payload.len());
        assert_eq!(
            &*blocked,
            &payload[from..to],
            "the window at {at} came back wrong"
        );
        assert_eq!(
            blocked, awaited,
            "the doors disagree about the window at {at}"
        );
    }
}

// a volume on the ring survives a reopen, so what it wrote was really written
#[test]
fn ring_reopens() {
    let dir = tempdir().expect("tempdir");
    {
        let store = open_on_ring(dir.path());
        for byte in 1..=16u8 {
            store
                .put(&record_key(GROUP, id(byte)), &vec![byte; 8192])
                .expect("put");
        }
        store.flush().expect("flush");
    }

    let reopened = open_on_ring(dir.path());

    assert_eq!(reopened.totals().count, 16);
    for byte in 1..=16u8 {
        let read = reopened
            .get(&record_key(GROUP, id(byte)))
            .expect("get")
            .expect("present");
        assert_eq!(read, vec![byte; 8192]);
    }
}

/// Ask the backend for completions until it has handed back this many
///
/// Nothing else is submitting here, so a ring reporting nothing is still working.
fn collect(backend: &UringBackend, wanted: usize) -> Vec<Completion> {
    let mut out = Vec::with_capacity(wanted);
    while out.len() < wanted {
        backend.poll(&mut out).expect("poll");
    }
    out
}

/// Open a file through the backend, which answers it off the ring
fn open_through(backend: &UringBackend, path: &std::path::Path) -> FileId {
    let open = Op::Open {
        tag: Tag(0),
        path: path.to_path_buf(),
        create: false,
        direct: false,
    };
    backend.submit(vec![open]).expect("submit the open");
    match collect(backend, 1).pop().expect("one completion").outcome {
        Outcome::Opened(file) => file.expect("the file opened"),
        other => panic!("an open came back as {other:?}"),
    }
}

// a batch past what the completion queue can report waits for a slot, losing nothing
#[test]
fn a_batch_past_the_completion_queue_answers_every_read() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("overrun");
    let payload: Vec<u8> = (0..4096u32).map(|at| at as u8).collect();
    std::fs::write(&path, &payload).expect("the file the reads come from");

    let backend = UringBackend::new(false, tuning()).expect("ring");
    let file = open_through(&backend, &path);

    // Well past the completion queue a 256 entry submission queue is built with.
    const READS: usize = 4096;
    let mut ops = Vec::with_capacity(READS);
    for at in 0..READS {
        ops.push(Op::Pread {
            tag: Tag(at as u64),
            file,
            offset: 0,
            buf: ReadBuf::new(64),
        });
    }
    backend.submit(ops).expect("submit the whole batch at once");

    let mut answered = vec![false; READS];
    for completion in collect(&backend, READS) {
        let at = completion.tag.0 as usize;
        assert!(!answered[at], "tag {at} came back twice");
        answered[at] = true;
        match completion.outcome {
            Outcome::Read { result, buf } => {
                assert_eq!(result.expect("the read succeeded"), 64);
                assert_eq!(
                    &buf.into_vec()[..],
                    &payload[..64],
                    "tag {at} read the file"
                );
            }
            other => panic!("a read came back as {other:?}"),
        }
    }
    assert!(answered.into_iter().all(|had| had), "every read answered");
}

// a ring answers a batch of reads on the submitting thread
#[test]
fn a_batch_answers_on_its_thread() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("batched");
    let payload: Vec<u8> = (0..4096u32).map(|at| at as u8).collect();
    std::fs::write(&path, &payload).expect("the file the reads come from");

    let backend = UringBackend::new(false, tuning())
        .unwrap_or_else(|error| panic!("a ring was refused: {error}"));
    let file = open_through(&backend, &path);

    let mut ops = Vec::new();
    for at in 0..64u64 {
        ops.push(Op::Pread {
            tag: Tag(at),
            file,
            offset: at * 64,
            buf: ReadBuf::new(64),
        });
    }
    let mut answered = Vec::new();
    assert!(
        backend.submit_batch(&mut ops, &mut answered),
        "the batch did not run on this thread",
    );

    assert_eq!(answered.len(), 64);
    for (at, completion) in answered.into_iter().enumerate() {
        assert_eq!(
            completion.tag,
            Tag(at as u64),
            "answers came back out of order"
        );
        match completion.outcome {
            Outcome::Read { result, buf } => {
                assert_eq!(result.expect("the read succeeded"), 64);
                let want = &payload[at * 64..at * 64 + 64];
                assert_eq!(&buf.into_vec()[..], want);
            }
            other => panic!("a read came back as {other:?}"),
        }
    }
}

// a batch on the caller's own ring comes back in submit order, however the kernel ran it
#[test]
fn a_batch_answers_in_submit_order() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("ordered");
    let payload: Vec<u8> = (0..8192u32).map(|at| at as u8).collect();
    std::fs::write(&path, &payload).expect("the file the reads come from");

    let backend = UringBackend::new(false, tuning()).expect("ring");
    let file = open_through(&backend, &path);

    // Read backwards, so submit order and offset order disagree.
    let mut ops = Vec::new();
    for step in 0..128u64 {
        ops.push(Op::Pread {
            tag: Tag(step),
            file,
            offset: (127 - step) * 64,
            buf: ReadBuf::new(64),
        });
    }
    let mut answered = Vec::new();
    assert!(
        backend.submit_batch(&mut ops, &mut answered),
        "the batch did not run on this thread",
    );

    assert_eq!(answered.len(), 128);
    for (step, completion) in answered.into_iter().enumerate() {
        let at = (127 - step as u64) * 64;
        match completion.outcome {
            Outcome::Read { result, buf } => {
                assert_eq!(result.expect("the read succeeded"), 64);
                let want = &payload[at as usize..at as usize + 64];
                assert_eq!(&buf.into_vec()[..], want, "step {step} read another offset");
            }
            other => panic!("a read came back as {other:?}"),
        }
    }
}

// a close mid-batch retires the descriptor table, and queued reads must reach the kernel first
#[test]
fn a_mid_batch_close_keeps_queued_reads_whole() {
    let dir = tempdir().expect("tempdir");
    let first_path = dir.path().join("first");
    let second_path = dir.path().join("second");
    let closed_path = dir.path().join("closed");
    let first_fill: Vec<u8> = vec![0x11; 4096];
    let second_fill: Vec<u8> = vec![0x22; 4096];
    std::fs::write(&first_path, &first_fill).expect("the first file");
    std::fs::write(&second_path, &second_fill).expect("the second file");
    std::fs::write(&closed_path, b"going away").expect("the file the close takes");

    let backend = UringBackend::new(false, tuning()).expect("ring");
    let first = open_through(&backend, &first_path);
    let second = open_through(&backend, &second_path);
    let closed = open_through(&backend, &closed_path);

    // The two reads queue against registered slots 0 and 1, and the rebuild seats the
    // second file where the first sat, so a stale entry reads the wrong file.
    let mut ops = vec![
        Op::Pread {
            tag: Tag(1),
            file: first,
            offset: 0,
            buf: ReadBuf::new(64),
        },
        Op::Pread {
            tag: Tag(2),
            file: second,
            offset: 0,
            buf: ReadBuf::new(64),
        },
        Op::Close {
            tag: Tag(3),
            file: closed,
        },
        Op::Pread {
            tag: Tag(4),
            file: second,
            offset: 64,
            buf: ReadBuf::new(64),
        },
    ];
    let mut answered = Vec::new();
    assert!(
        backend.submit_batch(&mut ops, &mut answered),
        "the batch did not run on this thread",
    );

    assert_eq!(answered.len(), 4);
    let wants: [Option<u8>; 4] = [Some(0x11), Some(0x22), None, Some(0x22)];
    for (completion, want) in answered.into_iter().zip(wants) {
        match completion.outcome {
            Outcome::Read { result, buf } => {
                let read = result.expect("a queued read survived the table clear");
                assert_eq!(read, 64);
                let bytes = buf.into_vec();
                let fill = want.expect("a read answered where the close belongs");
                assert!(
                    bytes[..64].iter().all(|byte| *byte == fill),
                    "a queued read answered with another file's bytes"
                );
            }
            Outcome::Done(result) => {
                assert!(want.is_none(), "a close answered where a read belongs");
                result.expect("the close itself succeeded");
            }
            other => panic!("an op came back as {other:?}"),
        }
    }
}

// a batch past what one ring can have in flight still answers every read in place
#[test]
fn a_wide_batch_keeps_its_places() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("wide");
    let payload: Vec<u8> = (0..8192u32).map(|at| at as u8).collect();
    std::fs::write(&path, &payload).expect("the file the reads come from");

    let backend = UringBackend::new(false, tuning()).expect("ring");
    let file = open_through(&backend, &path);

    // Well past the completion queue a 256 entry submission queue is built with.
    const READS: usize = 4096;
    let mut ops = Vec::with_capacity(READS);
    for at in 0..READS {
        ops.push(Op::Pread {
            tag: Tag(at as u64),
            file,
            offset: ((at % 128) * 64) as u64,
            buf: ReadBuf::new(64),
        });
    }
    let mut answered = Vec::new();
    assert!(
        backend.submit_batch(&mut ops, &mut answered),
        "the batch did not run on this thread",
    );

    assert_eq!(answered.len(), READS);
    for (at, completion) in answered.into_iter().enumerate() {
        assert_eq!(
            completion.tag,
            Tag(at as u64),
            "answers came back out of order"
        );
        match completion.outcome {
            Outcome::Read { result, buf } => {
                assert_eq!(result.expect("the read succeeded"), 64);
                let from = (at % 128) * 64;
                assert_eq!(&buf.into_vec()[..], &payload[from..from + 64]);
            }
            other => panic!("a read came back as {other:?}"),
        }
    }
}

// a caller with no thread takes the async door, answered by the engine thread's ring
#[test]
fn the_async_door_answers_a_read() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("awaited");
    let payload: Vec<u8> = (0..4096u32).map(|at| at as u8).collect();
    std::fs::write(&path, &payload).expect("the file the reads come from");

    let backend = UringBackend::new(false, tuning()).expect("ring");
    let driver = IoDriver::new(Arc::new(backend));
    let file = driver.open(&path, false).expect("open");

    let mut answered = Vec::new();
    for at in 0..16u64 {
        let op = Op::Pread {
            tag: driver.next_tag(),
            file,
            offset: at * 64,
            buf: ReadBuf::new(64),
        };
        answered.push(block_on(driver.wait_op(op)).expect("submit"));
    }

    for (at, completion) in answered.into_iter().enumerate() {
        match completion.outcome {
            Outcome::Read { result, buf } => {
                assert_eq!(result.expect("the read succeeded"), 64);
                assert_eq!(&buf.into_vec()[..], &payload[at * 64..at * 64 + 64]);
            }
            other => panic!("an awaited read came back as {other:?}"),
        }
    }
    assert_eq!(driver.outstanding(), 0, "every slot was given back");
}

// a whole batch on the async door pays one kick and comes back in submit order
#[test]
fn the_async_door_answers_a_batch() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("awaited-batch");
    let payload: Vec<u8> = (0..8192u32).map(|at| at as u8).collect();
    std::fs::write(&path, &payload).expect("the file the reads come from");

    let backend = UringBackend::new(false, tuning()).expect("ring");
    let driver = IoDriver::new(Arc::new(backend));
    let file = driver.open(&path, false).expect("open");

    let mut ops = Vec::new();
    for step in 0..128u64 {
        ops.push(Op::Pread {
            tag: driver.next_tag(),
            file,
            offset: (127 - step) * 64,
            buf: ReadBuf::new(64),
        });
    }
    let answered = block_on(driver.wait_batch(ops)).expect("submit");

    assert_eq!(answered.len(), 128);
    for (step, completion) in answered.into_iter().enumerate() {
        let at = ((127 - step as u64) * 64) as usize;
        match completion.outcome {
            Outcome::Read { result, buf } => {
                assert_eq!(result.expect("the read succeeded"), 64);
                assert_eq!(&buf.into_vec()[..], &payload[at..at + 64]);
            }
            other => panic!("an awaited read came back as {other:?}"),
        }
    }
    assert_eq!(driver.outstanding(), 0, "every slot was given back");
}

// a future dropped while its op is on the ring gives its slot back, with nobody woken
#[test]
fn a_dropped_future_leaks_nothing() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("dropped");
    std::fs::write(&path, vec![7u8; 4096]).expect("the file the reads come from");

    let backend = UringBackend::new(false, tuning()).expect("ring");
    let driver = IoDriver::new(Arc::new(backend));
    let file = driver.open(&path, false).expect("open");

    const DROPPED: usize = 8;
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    // Only a poll that came back pending left an op on the ring to abandon: a read the
    // kernel finishes inside the first poll leaves nothing in flight to drop.
    let mut abandoned = 0usize;
    for _ in 0..DROPPED {
        let op = Op::Pread {
            tag: driver.next_tag(),
            file,
            offset: 0,
            buf: ReadBuf::new(4096),
        };
        let mut waiting = Box::pin(driver.wait_op(op));
        if waiting.as_mut().poll(&mut cx).is_pending() {
            abandoned += 1;
        }
        drop(waiting);
    }

    assert!(
        settles(|| driver.reclaimed() as usize == abandoned),
        "the engine reclaimed {} of {abandoned} abandoned reads",
        driver.reclaimed()
    );
    assert_eq!(driver.outstanding(), 0, "every slot came back");
    println!("{abandoned} of {DROPPED} reads were still in flight when dropped");
}

// every thread reads through a ring of its own, and none sees another's completion
#[test]
fn threads_read_on_their_own_rings() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("shared");
    let payload: Vec<u8> = (0..8192u32).map(|at| at as u8).collect();
    std::fs::write(&path, &payload).expect("the file the reads come from");

    let backend = Arc::new(UringBackend::new(false, tuning()).expect("ring"));
    let file = open_through(&backend, &path);

    let mut readers = Vec::new();
    for worker in 0..8u64 {
        let backend = Arc::clone(&backend);
        let payload = payload.clone();
        readers.push(std::thread::spawn(move || {
            for round in 0..32u64 {
                let at = ((worker * 32 + round) % 128) * 64;
                let op = Op::Pread {
                    tag: Tag(round),
                    file,
                    offset: at,
                    buf: ReadBuf::new(64),
                };
                let completion = ReelIo::submit_inline(&*backend, op).expect("the ring answered");
                match completion.outcome {
                    Outcome::Read { result, buf } => {
                        assert_eq!(result.expect("the read succeeded"), 64);
                        let want = &payload[at as usize..at as usize + 64];
                        assert_eq!(
                            &buf.into_vec()[..],
                            want,
                            "worker {worker} read another range"
                        );
                    }
                    other => panic!("a read came back as {other:?}"),
                }
            }
        }));
    }
    for reader in readers {
        reader.join().expect("reader joins");
    }
}

// the backend says which door its ops took, and a fall-through is counted
#[test]
fn the_backend_says_which_door_its_ops_took() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("doors");
    std::fs::write(&path, vec![7u8; 4096]).expect("the file the reads come from");

    // Buffered explicitly rather than off the environment: a direct volume takes no ring
    // at all, which is one of the fall-throughs under test. The polled flag goes with it,
    // since the kernel refuses polled completions over the page cache.
    let backend = UringBackend::new(false, tuning()).expect("ring");

    let fresh = ReelIo::door_counts(&backend);
    assert!(
        !fresh.reached_ring,
        "a backend that has read nothing reached a ring"
    );
    assert_eq!(
        fresh.off_ring, 0,
        "a backend that has read nothing fell off one"
    );

    // An open names no ring file either, so every count below is read against it.
    let file = open_through(&backend, &path);
    let opened = ReelIo::door_counts(&backend).off_ring;
    assert_eq!(opened, 1, "an open is not a ring op and was counted as one");

    let read = Op::Pread {
        tag: Tag(1),
        file,
        offset: 0,
        buf: ReadBuf::new(64),
    };
    ReelIo::submit_inline(&backend, read).expect("the ring answered");
    let after_read = ReelIo::door_counts(&backend);
    assert!(after_read.reached_ring, "a read never reached the ring");
    assert_eq!(after_read.off_ring, opened, "a read fell off the ring");

    // A sync is not a ring op on this backend, so it takes the posix door.
    ReelIo::submit_inline(&backend, Op::SyncData { tag: Tag(2), file }).expect("the sync answered");
    assert_eq!(
        ReelIo::door_counts(&backend).off_ring,
        opened + 1,
        "an op the ring cannot take was counted as though it went on one",
    );
}

// a direct volume's reads go down the ring and come back cut to the window asked for
#[test]
fn a_direct_read_reaches_the_ring() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("staged");
    let payload: Vec<u8> = (0..16384u32).map(|at| (at % 251) as u8).collect();
    std::fs::write(&path, &payload).expect("the file the reads come from");

    let backend = UringBackend::new(true, tuning()).expect("ring");
    let file = open_through(&backend, &path);
    let opened = ReelIo::door_counts(&backend).off_ring;

    // Offsets and lengths aligned to nothing, so the read is widened to the blocks
    // holding them and cut back down on the way out.
    let windows = [(100u64, 300usize), (4095, 4098), (4096, 64), (8191, 1)];
    let mut ops = Vec::new();
    for (at, (offset, len)) in windows.into_iter().enumerate() {
        ops.push(Op::Pread {
            tag: Tag(at as u64),
            file,
            offset,
            buf: ReadBuf::new(len),
        });
    }
    let mut answered = Vec::new();
    assert!(
        backend.submit_batch(&mut ops, &mut answered),
        "the batch did not run on this thread",
    );

    assert_eq!(answered.len(), windows.len());
    for (at, completion) in answered.into_iter().enumerate() {
        let (offset, len) = windows[at];
        match completion.outcome {
            Outcome::Read { result, buf } => {
                assert_eq!(result.expect("the read succeeded"), len);
                let from = offset as usize;
                let want = &payload[from..from + len];
                assert_eq!(&buf.into_vec()[..], want, "window {at} read another range");
            }
            other => panic!("a read came back as {other:?}"),
        }
    }

    let doors = ReelIo::door_counts(&backend);
    assert!(
        doors.reached_ring,
        "a direct volume's reads never reached the ring"
    );
    assert_eq!(doors.off_ring, opened, "a direct read fell off the ring");
}

// a direct volume answers the async door too, on the engine thread's own ring
#[test]
fn a_direct_read_takes_the_async_door() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("awaited-direct");
    let payload: Vec<u8> = (0..8192u32).map(|at| (at % 251) as u8).collect();
    std::fs::write(&path, &payload).expect("the file the reads come from");

    let backend = UringBackend::new(true, tuning()).expect("ring");
    let driver = IoDriver::new(Arc::new(backend));
    let file = driver.open(&path, false).expect("open");

    for at in 0..16u64 {
        let op = Op::Pread {
            tag: driver.next_tag(),
            file,
            offset: at * 100,
            buf: ReadBuf::new(100),
        };
        let completion = block_on(driver.wait_op(op)).expect("submit");
        match completion.outcome {
            Outcome::Read { result, buf } => {
                assert_eq!(result.expect("the read succeeded"), 100);
                let from = (at * 100) as usize;
                assert_eq!(&buf.into_vec()[..], &payload[from..from + 100]);
            }
            other => panic!("an awaited read came back as {other:?}"),
        }
    }
    assert_eq!(driver.outstanding(), 0, "every slot was given back");
}

// a whole direct volume writes and reads back through its rings' buffers
/// Whether this kernel puts a direct op through the ring at all
///
/// A runner can set the ring up and still refuse its direct ops, downgrading
/// every one to posix. That is an environment verdict, not a routing bug, so
/// the direct test skips on it; where the probe passes, the assert below still
/// holds the code to the ring.
fn direct_ring_serves_here() -> bool {
    let dir = tempdir().expect("tempdir");
    let (store, ring) = open_direct_on_ring(dir.path());
    store
        .put(&record_key(GROUP, id(1)), &vec![1u8; 4096])
        .expect("put");
    store.flush().expect("flush");
    let _ = store.get(&record_key(GROUP, id(1))).expect("get");
    ReelIo::door_counts(&*ring).reached_ring
}

#[test]
fn a_direct_volume_serves_a_volume() {
    if !direct_ring_serves_here() {
        eprintln!("skipping: this kernel refuses direct ops on the ring");
        return;
    }
    let dir = tempdir().expect("tempdir");
    let (store, ring) = open_direct_on_ring(dir.path());

    for byte in 1..=32u8 {
        let payload = vec![byte; 4096 + byte as usize];
        store
            .put(&record_key(GROUP, id(byte)), &payload)
            .expect("put");
    }
    store.flush().expect("flush");

    for byte in 1..=32u8 {
        let read = store
            .get(&record_key(GROUP, id(byte)))
            .expect("get")
            .expect("present");
        assert_eq!(read.len(), 4096 + byte as usize);
        assert!(
            read.iter().all(|held| *held == byte),
            "payload came back whole"
        );
    }

    let wanted: Vec<_> = (1..=32u8).map(|byte| record_key(GROUP, id(byte))).collect();
    let many = store.get_many(&wanted).expect("get_many");
    assert_eq!(many.len(), 32);
    for (at, held) in many.into_iter().enumerate() {
        let byte = at as u8 + 1;
        let read = held.expect("present");
        assert!(
            read.iter().all(|held| *held == byte),
            "record {byte} came back whole"
        );
    }

    assert!(
        ReelIo::door_counts(&*ring).reached_ring,
        "the whole run was served on posix, so nothing above measured the ring",
    );
}

// a file closed and another opened on the same ring reads the second one
#[test]
fn a_reopened_file_reads_itself() {
    let dir = tempdir().expect("tempdir");
    let first = dir.path().join("first");
    let second = dir.path().join("second");
    std::fs::write(&first, vec![1u8; 4096]).expect("first");
    std::fs::write(&second, vec![2u8; 4096]).expect("second");

    let backend = UringBackend::new(false, tuning()).expect("ring");
    let driver = IoDriver::new(Arc::new(backend));

    for round in 0..4 {
        let opened = driver.open(&first, false).expect("open first");
        assert_eq!(
            driver.pread(opened, 0, 64).expect("read first"),
            vec![1u8; 64]
        );
        driver.close(opened).expect("close first");

        let opened = driver.open(&second, false).expect("open second");
        assert_eq!(
            driver.pread(opened, 0, 64).expect("read second"),
            vec![2u8; 64],
            "round {round} read the file that closed"
        );
        driver.close(opened).expect("close second");
    }
}

/// Drive one future to its answer on the calling thread, with no runtime
///
/// The waker unparks the polling thread, so a future the engine never wakes hangs here
/// rather than passing.
fn block_on<Answered: Future>(future: Answered) -> Answered::Output {
    struct Unparker(Thread);

    impl Wake for Unparker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let mut future = Box::pin(future);
    let waker = Waker::from(Arc::new(Unparker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(answer) = future.as_mut().poll(&mut cx) {
            return answer;
        }
        thread::park();
    }
}

/// Wait a bounded while for something the engine thread does out of band
///
/// The ring answers in microseconds, so anything still false after this is a completion
/// that never landed.
fn settles(mut ready: impl FnMut() -> bool) -> bool {
    for _ in 0..2_000 {
        if ready() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    ready()
}

// many writers on one volume each write through a ring of their own
#[test]
fn ring_takes_concurrent_writers() {
    let dir = tempdir().expect("tempdir");
    let store = Arc::new(open_on_ring(dir.path()));

    let mut writers = Vec::new();
    for worker in 0..8u8 {
        let store = Arc::clone(&store);
        writers.push(std::thread::spawn(move || {
            for step in 0..16u8 {
                let byte = worker * 16 + step;
                store
                    .put(&record_key(GROUP, id(byte)), &vec![byte; 2048])
                    .expect("put");
            }
        }));
    }
    for writer in writers {
        writer.join().expect("writer joins");
    }
    store.flush().expect("flush");

    assert_eq!(store.totals().count, 128);
    for byte in 0..128u8 {
        let read = store
            .get(&record_key(GROUP, id(byte)))
            .expect("get")
            .expect("present");
        assert_eq!(read, vec![byte; 2048]);
    }
}
