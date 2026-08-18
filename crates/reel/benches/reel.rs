//! Reel store engine benchmarks
//!
//! Group commit ingest, the single record read, a rebuild of an unsealed tail, the
//! sealed segment scan and the playbacks, all against the real posix backend on a
//! temporary directory so syscalls show up in the numbers. Sync is off and the page
//! cache is kept, so a number is the engine's own work rather than flush latency.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use tempfile::TempDir;

use reel_core::{Direction, Store};

use reel::format::lsn::Lsn;
use reel::format::record::{checksum, RecordHeader};
use reel::io::posix_backend::PosixBackend;
use reel::reel::segment::IoDriver;
use reel::{
    rebuild_reel, ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape,
    Preallocate, RecordKey, ReelConfig, ReelStore, SyncPolicy, ThreadBudget,
};

const RECORDS: ColumnId = ColumnId(1);
const BLOB: ColumnId = ColumnId(2);

/// Name the store trait addresses the record column by
const RECORDS_CF: &str = "records";

/// Bytes a record key occupies: two group bytes then a thirty-two byte id
const RECORD_KEY_LEN: usize = 34;

/// The columns every volume here is opened with
const COLUMNS: ColumnSet = &[
    ColumnSpec {
        id: RECORDS,
        name: RECORDS_CF,
        key_width: KeyWidth::Fixed(RECORD_KEY_LEN as u16),
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

/// The group every record here is written under
const GROUP: u16 = 7;
const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;

/// A store whose files live in a temporary directory removed when it drops
struct Volume {
    store: ReelStore,
    dir: TempDir,
}

impl Volume {
    fn open(config: ReelConfig) -> Volume {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = ReelStore::open(dir.path().to_path_buf(), config, COLUMNS).expect("open");
        Volume { store, dir }
    }

    fn root(&self) -> &Path {
        self.dir.path()
    }
}

fn base_config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(64 * MIB),
        alloc_chunk: ByteCount::from_bytes(4 * MIB),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        scrub_mbps: 4,
        ..ReelConfig::default()
    }
}

/// The record column key for a group and an id, big endian group at the front
fn record_key(group: u16, id: [u8; 32]) -> RecordKey {
    let mut bytes = [0u8; RECORD_KEY_LEN];
    bytes[..2].copy_from_slice(&group.to_be_bytes());
    bytes[2..].copy_from_slice(&id);
    RecordKey::from_bytes(RECORDS, &bytes).expect("key")
}

fn id(index: u64) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&index.to_be_bytes());
    bytes
}

fn payload(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|index| (index as u8).wrapping_add(seed))
        .collect()
}

fn fill(store: &ReelStore, count: u64, len: usize) {
    let body = payload(len, 0x5a);
    for index in 0..count {
        store
            .put_owned(&record_key(GROUP, id(index)), body.clone())
            .expect("put");
    }
}

/// Group commit ingest through the owned put path
fn ingest(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest");
    for (len, count) in [(1024usize, 512u64), (65_536, 64)] {
        group.throughput(Throughput::Bytes(len as u64 * count));
        group.bench_function(BenchmarkId::from_parameter(format!("{len}B")), |b| {
            let body = payload(len, 0x11);
            b.iter_batched(
                || Volume::open(base_config()),
                |volume| {
                    for index in 0..count {
                        volume
                            .store
                            .put_owned(&record_key(GROUP, id(index)), body.clone())
                            .expect("put");
                    }
                    volume
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

/// Rewriting keys that already exist, against inserting fresh ones
///
/// A log answers an update by appending a new record and shadowing the old one, so an
/// update costs an insert plus the garbage it makes. The keys are written once outside
/// the timed section and rewritten inside it, at the ingest sizes and counts.
fn update(c: &mut Criterion) {
    let mut group = c.benchmark_group("update");
    for (len, count) in [(1024usize, 512u64), (65_536, 64)] {
        group.throughput(Throughput::Bytes(len as u64 * count));
        group.bench_function(BenchmarkId::from_parameter(format!("{len}B")), |b| {
            let body = payload(len, 0x11);
            b.iter_batched(
                || {
                    let volume = Volume::open(base_config());
                    fill(&volume.store, count, len);
                    volume
                },
                |volume| {
                    for index in 0..count {
                        volume
                            .store
                            .put_owned(&record_key(GROUP, id(index)), body.clone())
                            .expect("put");
                    }
                    volume
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

/// Concurrent group commit across many writer threads on one tail
fn ingest_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest_concurrent");
    group.sample_size(20);
    for writers in [4usize, 16, 64] {
        let per_writer = 64u64;
        group.throughput(Throughput::Elements(writers as u64 * per_writer));
        group.bench_function(BenchmarkId::from_parameter(format!("{writers}w")), |b| {
            b.iter_batched(
                || Arc::new(Volume::open(base_config())),
                |volume| {
                    let barrier = Arc::new(Barrier::new(writers));
                    let mut handles = Vec::with_capacity(writers);
                    for writer in 0..writers {
                        let volume = Arc::clone(&volume);
                        let barrier = Arc::clone(&barrier);
                        handles.push(thread::spawn(move || {
                            let body = payload(1024, writer as u8);
                            barrier.wait();
                            for index in 0..per_writer {
                                let key = id(writer as u64 * per_writer + index);
                                volume
                                    .store
                                    .put_owned(&record_key(GROUP, key), body.clone())
                                    .expect("put");
                            }
                        }));
                    }
                    for handle in handles {
                        handle.join().expect("join");
                    }
                    volume
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

/// One resolved read, header check and checksum verify included
fn read(c: &mut Criterion) {
    let mut group = c.benchmark_group("read");
    for len in [1024usize, 65_536, 1_048_576] {
        let count = (16 * MIB / len as u64).clamp(16, 4096);
        let volume = Volume::open(base_config());
        fill(&volume.store, count, len);
        group.throughput(Throughput::Bytes(len as u64));
        group.bench_function(BenchmarkId::from_parameter(format!("{len}B")), |b| {
            let mut cursor = 0u64;
            b.iter(|| {
                cursor = (cursor + 1) % count;
                black_box(
                    volume
                        .store
                        .get(&record_key(GROUP, id(cursor)))
                        .expect("get"),
                )
            })
        });
    }
    group.finish();
}

/// Many reader threads sharing the descriptor cache
fn read_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_concurrent");
    group.sample_size(20);
    let len = 4096usize;
    let count = 4096u64;
    let volume = Arc::new(Volume::open(base_config()));
    fill(&volume.store, count, len);

    for readers in [4usize, 16] {
        group.throughput(Throughput::Elements(readers as u64));
        group.bench_function(BenchmarkId::from_parameter(format!("{readers}r")), |b| {
            b.iter_custom(|iterations| {
                let cursor = Arc::new(AtomicUsize::new(0));
                let barrier = Arc::new(Barrier::new(readers + 1));
                let mut handles = Vec::with_capacity(readers);
                for _ in 0..readers {
                    let volume = Arc::clone(&volume);
                    let cursor = Arc::clone(&cursor);
                    let barrier = Arc::clone(&barrier);
                    handles.push(thread::spawn(move || {
                        barrier.wait();
                        for _ in 0..iterations {
                            let next = cursor.fetch_add(1, Ordering::Relaxed) as u64;
                            black_box(
                                volume
                                    .store
                                    .get(&record_key(GROUP, id(next % count)))
                                    .expect("get"),
                            );
                        }
                    }));
                }
                barrier.wait();
                let started = Instant::now();
                for handle in handles {
                    handle.join().expect("join");
                }
                started.elapsed()
            })
        });
    }
    group.finish();
}

/// Rebuilding an unsealed tail, the crash recovery path
fn recovery(c: &mut Criterion) {
    let mut group = c.benchmark_group("recovery");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for chunk_mib in [1u64, 4] {
        let config = ReelConfig {
            alloc_chunk: ByteCount::from_bytes(chunk_mib * MIB),
            ..base_config()
        };
        let volume = Volume::open(config);
        fill(&volume.store, 64, 1024);
        let reel_dir = reel_dir_of(volume.root());
        drop(volume.store);

        group.bench_function(
            BenchmarkId::from_parameter(format!("{chunk_mib}MiB_chunk")),
            |b| {
                b.iter(|| {
                    let driver = IoDriver::new(Arc::new(PosixBackend::new()));
                    black_box(
                        rebuild_reel(&driver, std::slice::from_ref(&reel_dir), &[false], false)
                            .expect("rebuild"),
                    )
                })
            },
        );
        drop(volume.dir);
    }
    group.finish();
}

/// The sealed segment scan the scrub and the compactor share
fn scrub_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("scrub_scan");
    group.sample_size(10);

    let config = ReelConfig {
        segment_bytes: ByteCount::from_bytes(4 * MIB),
        alloc_chunk: ByteCount::from_bytes(MIB),
        ..base_config()
    };
    let volume = Volume::open(config);
    fill(&volume.store, 4096, 1024);
    volume.store.flush().expect("flush");

    group.throughput(Throughput::Elements(4096));
    group.bench_function("4096_records", |b| {
        b.iter(|| black_box(volume.store.scrub_once().expect("scrub")))
    });
    group.finish();
}

/// Playbacks over the resident index
///
/// The two key counts separate a cost paid per page from a cost paid per key: a
/// per-page cost shrinks as a fraction of the playback as the group grows.
fn iterate(c: &mut Criterion) {
    let mut group = c.benchmark_group("iterate");
    group.sample_size(20);

    for count in [4_096u64, 65_536] {
        let volume = Volume::open(base_config());
        fill(&volume.store, count, 256);
        let prefix = group_prefix(GROUP);

        group.throughput(Throughput::Elements(count));
        group.bench_function(BenchmarkId::new("keys_only", count), |b| {
            b.iter(|| {
                black_box(
                    volume
                        .store
                        .iter_keys_prefix(RECORDS_CF, &prefix)
                        .expect("keys")
                        .len(),
                )
            })
        });
        group.bench_function(BenchmarkId::new("values", count), |b| {
            b.iter(|| black_box(volume.store.iter(RECORDS_CF).expect("iter").count()))
        });
    }
    group.finish();
}

/// One page of a paginated group playback, the shape a paged scan takes
///
/// The key counts are the measurement: a page that costs the page rather than the
/// group reads the same at all of them.
fn paginate(c: &mut Criterion) {
    let mut group = c.benchmark_group("paginate");
    const PAGE: usize = 100;

    for count in [4_096u64, 16_384, 65_536] {
        let volume = Volume::open(base_config());
        fill(&volume.store, count, 256);
        let start = wire_key(GROUP, id(count / 2));

        group.throughput(Throughput::Elements(PAGE as u64));
        group.bench_function(BenchmarkId::from_parameter(format!("{count}_keys")), |b| {
            b.iter(|| {
                let page: Vec<_> = volume
                    .store
                    .iter_from(RECORDS_CF, &start, Direction::Asc)
                    .expect("iter")
                    .take(PAGE)
                    .collect();
                black_box(page.len())
            })
        });
    }
    group.finish();
}

/// Counting a group's records, the shape a periodic total takes
///
/// Collecting the keys to take their length allocates a buffer per key for a number
/// that is then discarded, so the two arms are the same question asked two ways.
fn count(c: &mut Criterion) {
    let mut group = c.benchmark_group("count");
    group.sample_size(20);

    let count = 65_536u64;
    let volume = Volume::open(base_config());
    fill(&volume.store, count, 256);
    let prefix = group_prefix(GROUP);

    group.throughput(Throughput::Elements(count));
    group.bench_function("collect_then_len", |b| {
        b.iter(|| {
            black_box(
                volume
                    .store
                    .iter_keys_prefix(RECORDS_CF, &prefix)
                    .expect("keys")
                    .len(),
            )
        })
    });
    group.bench_function("count_prefix", |b| {
        b.iter(|| {
            black_box(
                volume
                    .store
                    .count_prefix(RECORDS_CF, &prefix)
                    .expect("count"),
            )
        })
    });
    group.finish();
}

/// The checksum primitives every appended record pays for
///
/// A put checksums its header and payload as one record and builds one pad header per
/// drain, which are separate entry points and do not cost the same.
fn checksum_primitives(c: &mut Criterion) {
    let mut group = c.benchmark_group("checksum");
    let header = [0x5au8; 24];
    let payload = payload(1024, 0x11);
    let key = record_key(GROUP, id(1));

    group.bench_function("checksum_24B", |b| {
        b.iter(|| black_box(checksum(black_box(&header))))
    });
    group.bench_function("checksum_1KiB", |b| {
        b.iter(|| black_box(checksum(black_box(&payload))))
    });
    // The clone is the key's, not the header's: a key holds an Arc for the widths
    // that spill, so on this fixed-width column the row carries a memcpy of the
    // inline array and no allocation.
    group.bench_function("record_header", |b| {
        b.iter(|| {
            black_box(RecordHeader::data(
                black_box(key.clone()),
                Lsn(7),
                black_box(&payload),
            ))
        })
    });
    group.bench_function("pad_header", |b| {
        b.iter(|| black_box(RecordHeader::pad(black_box(4152))))
    });
    group.finish();
}

fn wire_key(group: u16, id: [u8; 32]) -> Vec<u8> {
    record_key(group, id).as_slice().to_vec()
}

fn group_prefix(group: u16) -> Vec<u8> {
    group.to_be_bytes().to_vec()
}

fn reel_dir_of(root: &Path) -> PathBuf {
    root.to_path_buf()
}

criterion_group!(
    benches,
    ingest,
    update,
    ingest_concurrent,
    read,
    read_concurrent,
    recovery,
    scrub_scan,
    iterate,
    paginate,
    count,
    checksum_primitives,
);
criterion_main!(benches);
