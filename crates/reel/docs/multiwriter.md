# Multiwriter

How more than one process shares a store while each store keeps a single writer.

| | rule |
|---|---|
| unit of ownership | the store: one log, one lock, one owner, over one or more volume roots that the lock on the first root covers together |
| writers per store | exactly one, held by the flock on `reel.lock` in the first root |
| readers | any process, through a read-only open plus `refresh()`. Many processes can read a store, and only its owner writes |
| stores per process | any number. A caller that separates metadata from bulk owns two |
| per-column ownership | put the column alone on its own store |

A column lives on exactly one store, and owning a store means owning every column on it. So "a
process owns a column and shares the others" becomes: a process owns the store a column group is
placed on, and read-shares whichever other stores it needs. A process can never co-write a store
another process owns.

## Why the store is the unit

Every column on a store shares the one log. Tails, segment files, the LSN counter, the in-memory
index and compaction are all per store, and records from different columns interleave in the same
segments. A second appender on the same store would collide on tail offsets, and its compactor
would treat the other writer's live rows as garbage and unlink them. The engine wouldn't detect
that. The ownership lock is a tripwire for the rule, and the shared log is the reason for it.

LSM engines share one write-ahead log across column families for the same reason, atomic
cross-family batches, and they can't do per-family multiprocess either. This matches the state of
the art.

## What the engine provides

A caller-side router that places each column family on one of two stores is the two-store version
of this, and it runs with one process owning both halves. Underneath it, the engine has every piece
a foreign process needs:

- `ReelStore::open_read_only` plus `refresh()` gives a foreign process a live view. The reader's
  index catches up to what has reached the disk and drops descriptors for segments compaction
  removed under it.
- The ownership lock takes `flock` through std, records pid, acquire time, boot id and build
  version, and puts them in the contention error. A dead owner releases it by construction, so
  there is no lease and nothing to steal.
- `ReelStore::destroy` refuses a store whose owner is alive.

The router doesn't let the two halves have different owners.

## Placement rules

Placement is the whole design. Two rules bound it:

1. A write batch must stay on one store. In the router that is a debug assert with a non-atomic
   fallback. With stores under different owners, that fallback silently loses atomicity between
   processes, so there it has to be a hard error.
2. Columns a batch couples must sit on the same store. An audit of every `write_batch` call site in
   one caller found three coupled pairs, and the shapes generalise:

| Pair | Why it must be atomic |
|------|----------------------|
| a record and its size sidecar | hot ingest writes both per record |
| a ledger entry and the reservation it consumes | a crash between them double-spends |
| a multipart part's metadata and its payload | the two land together or not at all |

Read paths that join columns across stores get bounded staleness with no point-in-time view, and
have to be audited the same way before a split ships.

## Visibility

A reader of a foreign store sees a record once it has reached the disk and a `refresh()` has run.
The owner's in-memory tail stays out of view until then. Cross-process read-your-writes takes an
explicit flush on the owner followed by a refresh on the reader. A consumer that can't tolerate
that lag belongs in the owning process.

## Scaling across disks

Multi-disk throughput is the engine's own job and needs no extra stores or processes. One store
spans its disks directly: a volume root per drive, one pinned tail per fast root, and aggregate
ingest sums across them. A column isn't bounded by one disk either, since its segments spread
across every root the store spans. Splitting into several stores is a choice about ownership and
failure isolation, and speed doesn't enter into it.

- One process can own one store over several drives. The engine is storage-bound, so a single
  process drives them to their caps. Choose the process count by service topology and failure
  isolation.
- Where columns are split across stores for ownership, each store spans whatever disks it is given,
  and the coupled-pair rule decides which columns travel together.

This holds until CPU binds before the devices do. More processes don't move that wall, since they
share the same cores.

## Non-goals

- **Concurrent writers on one store.** The shared-memory rewrite it would take (index and robust
  mutex in a shared map, crash-consistent updates on every structure) reworks the whole index for a
  design that still serializes writers. Rejected.
- **Leases, expiry, takeover.** A stale lock can't exist, since the flock dies with its process.
  Contention always means a live owner, and refusing is correct.
- **Cross-store atomic batches.** Two stores are two durability points, and no design gives one
  atomic commit across them. Placement removes the need.
- **Throughput on a shared device.** A second writer against the same disk adds no MB/s, since the
  device is the cap. Across devices the store scales itself.

## What is left above the engine

The engine's half is done: the lock, the read-only open, `refresh()`, and a destroy that refuses a
live owner. Three pieces sit above the engine, and none of them is built.

- **A routed store.** The router handles two stores with one owner. Nothing generalizes it to N
  stores behind a placement table of store, directory, column list and mode (owner or reader).
  Routing would go by column-family name, with a batch touching two stores as a hard error. The
  table comes from a write-path inventory: for every column family, which code paths write it and
  which service can reach them, with the coupled pairs above as its first constraints.
- **Foreign reads on a cadence.** `refresh()` exists and a caller calls it. Nothing opens
  reader-mode stores and refreshes them on a schedule, nothing refreshes on demand after a path
  flushed a foreign owner, and no metric tracks the time since a store's last successful refresh.
- **A two-process test.** The lock, the read-only open and `refresh()` are covered in-process. No
  test runs an owner and a reader against the same store from separate processes, so three claims
  rest on the code alone: the reader sees records after flush plus refresh, a killed owner releases
  the lock without cleanup, and a second would-be owner is refused with the owner's details in the
  error.
