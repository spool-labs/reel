# tape-reel

A log-structured key-value store: sequential writes, immutable segments, whole-segment reclaim.

A reel is one log of segment files spread over one or more volumes. A write lands
at the tail of an open segment and the segment seals when it fills; nothing is
ever updated in place, so a delete is a tombstone and space comes back from
compaction rather than from the write path. An index maps every live key to the
segment and offset that holds it, either resident in memory or paged out of the
footers the seals wrote.

Writers run concurrently across multiple active tails, one open segment file per
tail, so the kernel never serializes them on a shared inode. Readers run beside
them without locks. One process owns a store for writing at a time; other
processes can open it read-only and follow.

- **Segments.** Values are stored whole and read back verbatim. A
  record is one append, and a read is one device op placed by the index.
- **One reel, many volumes.** Extra roots are declared as fast or capacity;
  compaction demotes aged survivors onto the capacity tier without changing
  anything else about how they are read.
- **Caller-declared columns.** The engine ships no columns of its own. A caller
  hands `ReelStore::open` the set it wants, each with its own key width,
  sharding, inline and carry budgets, and codec.
- **Pluggable io.** POSIX, io_uring, and io_uring over direct descriptors with
  registered buffers, plus a deterministic simulator that can be told to fail any
  op, so crash and fault behaviour is testable without a device. The simulator is
  behind the `sim` feature and is not one of the backends a config can name.
- **Both doors.** Every read and write has a blocking form and an awaited form.
  A batch is one durability point for the whole of it either way.

Why it is shaped this way, what was measured, and what is still owed:
[crates/reel/docs/](crates/reel/docs/README.md).

## Using it

```rust
use std::path::PathBuf;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, ReelConfig, ReelStore,
    Store,
};

const BLOCKS: &str = "blocks";

/// Sixteen byte keys, sharded on their leading two bytes, values stored raw
const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: BLOCKS,
    key_width: KeyWidth::Fixed(16),
    shard_bytes: 2,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

fn main() -> reel::Result<()> {
    let config = ReelConfig {
        segment_bytes: ByteCount::gb(1),
        ..ReelConfig::default()
    };
    let store = ReelStore::open(PathBuf::from("/mnt/bulk"), config, COLUMNS)?;

    let key = [7u8; 16];
    Store::put(&store, BLOCKS, &key, b"payload").expect("put");
    let found = Store::get(&store, BLOCKS, &key).expect("get");
    assert_eq!(found.map(|value| value.into_vec()), Some(b"payload".to_vec()));

    Ok(())
}
```

The `Store::` prefix is not decoration: `ReelStore` has inherent methods of the
same names that take a resolved `RecordKey`, so a call meant for the trait says
so. The trait, and the types in its signatures, are re-exported from this crate.

Reads and writes are addressed by column family name. A name the reel was not
opened with is refused rather than created.

## Operating a volume

The engine starts no threads of its own. Compaction, the retry of failed seals,
paging the index into the footers, tombstone pruning, the sorted-run merge, and
the scrub all happen inside `maintain_once`, on the thread that calls it. A store
nobody ticks never compacts: dead records stay on disk, graves are never pruned,
sealed keys never leave memory, and the volume grows forever.

So a process that opens a volume for writing owes it one loop:

```rust
loop {
    store.maintain_once()?;
    thread::sleep(Duration::from_secs(1));
}
```

Every pass inside the tick is bounded and paced by the volume's own settings, so
the caller sets a cadence and never a budget or a deadline. `Store::maintain` is
the same call behind the trait, and `Store::reclaim_space` is one compaction pass
alone, for a caller that wants space back now rather than on the next tick. A
read-only open ticks to nothing, so the loop is safe to run either way.

## Examples

Each demonstrates one capability and checks its own claims:

```sh
cargo run --example basic            # open, put/get/delete, trait vs inherent calls
cargo run --example follower         # a read-only open refreshing beside a live writer
cargo run --example servo            # what the machine reports and what the engine picks from it
cargo run --example async_doors      # the awaited forms beside their blocking twins, no runtime required
cargo run --example volumes          # one reel over three roots, classes, and a degraded open
cargo run --example cohorts          # whole-segment reclaim: delete a group, watch it unlink, not copy
cargo run --example simulated_crash  # a torn write and an ENOSPC injected deterministically, recovered
cargo run --example checkpoint       # a durable copy at a cue that opens as its own store
```

## Building

```sh
cargo build --release
cargo test
```

Nothing is on by default: what a caller links is the engine and no test rig.

- `serde`: `Deserialize` for `ReelConfig` and every enum it holds, for settings
  kept in a file rather than written in code.
- `sim`: the deterministic io simulator and its fault plans, `io::sim_backend`
  and `io::fault`. Not a backend, so `IoBackend` has no arm for it; a test builds
  one and hands it to the engine.
- `rendezvous`: named points a test can park a thread at, mid-write. Off, every
  site on a production path is a call to an empty function.
- `alloc-mimalloc`, `alloc-snmalloc`: the allocator this crate's own probe
  binaries link.

The allocator pair is not a knob for a consumer. A global allocator belongs to
whoever builds the final binary, so a library can only set one for binaries it
owns, which here are its probes: linking `reel` with `alloc-mimalloc` changes
nothing about how your process allocates. Set `#[global_allocator]` in your own
binary instead.

## Tests

`cargo test` needs no feature flags: the crate depends on itself as a dev
dependency and turns `sim`, `rendezvous`, and `serde` on for its own tests,
probes, and examples. `cargo build` sees none of that.

Most of the suite runs anywhere, on the simulator backend or on a temporary
directory. The tests that assert on what the kernel actually did (io_uring
submission and completion, direct descriptors, inode locking, preallocation) are
gated to Linux and compile away elsewhere. A macOS
or BSD run is a real run of everything portable and says nothing about the io.

Tests that write real files honour `TMPDIR`. On a distribution where `/tmp` is a
tmpfs, leaving it alone measures memory rather than the device.
