# tape-reel

A log-structured key-value store with sequential writes, immutable segments, and whole-segment reclaim.

A reel is one log of segment files over one or more volume roots. Writers append at the
head of several active tails, one open segment each, and a segment seals when it fills.
Nothing is updated in place. A delete writes a tombstone and compaction gives the space back.
An index maps each live key to its segment and offset. It stays in memory or pages out to the
footers the seals wrote. One process owns a volume for writing. Others can open it read-only and follow.

- Columns are declared by the caller: key width (fixed or variable), sharding, lz4, and purge
  marks.
- Point reads, batched reads, ranged reads, ordered walks, and prefix sweeps. A prefix sweep
  takes any prefix, in key order.
- A write batch is one durability point. Every call has a blocking and an awaited form.
- Extra roots join as fast or capacity volumes, and compaction demotes aged data to capacity.
- Cue points read the volume as it stood. A checkpoint copies it at a cue into a store of its own.
- IO runs on POSIX everywhere, and on io_uring or io_uring with direct descriptors on Linux.
- A new segment is written through once, and a tail keeps synced zeros ahead of its head.

```rust
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, ReelConfig, ReelStore, Store,
};

const BLOCKS: &str = "blocks";
const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: BLOCKS,
    key_width: KeyWidth::Fixed(16),
    shard_bytes: 2,
    purge_mark: None,
    codec: Codec::None,
}];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ReelConfig { segment_bytes: ByteCount::mb(64), ..ReelConfig::default() };
    let store = ReelStore::open(std::env::temp_dir().join("reel-demo"), config, COLUMNS)?;
    Store::put(&store, BLOCKS, &[7u8; 16], b"payload")?;
    assert_eq!(&*Store::get(&store, BLOCKS, &[7u8; 16])?.expect("stored"), b"payload");
    store.maintain_once()?;
    store.close()?;
    Ok(())
}
```

The crate is published as `tape-reel` and imported as `reel`. `ReelStore` has inherent methods
with the same names that take a resolved `RecordKey`, so trait calls are written `Store::get`.
Compaction, index paging, tombstone pruning, merges, and the scrub run only inside
`maintain_once`. Call it on a timer, about once a second. A volume nobody ticks grows forever.

## Crates and features

`tape-reel-core` holds the `Store` trait and write batches. `tape-reel-cli` builds
the `reel` binary to inspect, verify, and checkpoint a volume ([README](crates/reel-cli/README.md)).
`tape-reel-mock` is an in-memory `Store` for tests and stays unpublished.

Nothing is on by default. `serde` adds `Serialize` for the reports and `Deserialize` for
`IoBackend`. `sim` adds the deterministic io simulator and fault plans. `rendezvous` adds points
where a test parks a thread.

## Docs and tests

The design notes live in [crates/reel/docs](crates/reel/docs/README.md), starting at
[overview.md](crates/reel/docs/overview.md). `cargo run -p tape-reel --example <name>` runs `basic`,
`follower`, `servo`, `async_doors`, `volumes`, `cohorts`, `simulated_crash`, or `checkpoint`.
`cargo test` needs no flags. The io_uring and kernel tests run only on Linux. Rust 1.89 or newer.

Licensed under Apache-2.0. See [LICENSE](LICENSE).
