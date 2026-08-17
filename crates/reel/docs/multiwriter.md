# Multiwriter

How more than one process shares a store, without giving up the single-writer
reel that makes the engine correct.

## The ownership model

The unit of ownership is the store, not the column. A store is one reel: one
log, one lock, one owner, spanning one or more volume roots (`volumes.md`) that
the lock on the first root owns together. A column lives on exactly one store,
and owning a store means owning every column on it.

- A process may own several stores. A caller that separates metadata from bulk
  owns two.
- A store has exactly one writing owner at a time, enforced by the flock in
  `reel.lock` on its first root, which dies with the owner and names it on
  contention.
- Any process may additionally read any store it does not own, through a
  read-only open plus `refresh()`. Reads share, writes never do.
- Per-column ownership is the degenerate case: place that column alone on its
  own store.

So "a process owns a column and shares the others" comes out as: a process owns
the store a column group is placed on, and read-shares whichever other stores
it needs. What a process can never do is co-write a store another process owns.

## Why the store is the unit

Every column on a store shares the one log. Tails, segment files, the LSN
counter, the in-memory index, and compaction are all per store, and records
from different columns interleave in the same segments (`reel.rs` module doc).
A second appender on the same store is not a conflict the engine detects, it is
offset collision on the tail plus a compactor that treats the other writer's
live rows as garbage and unlinks them. The ownership lock is the tripwire for
that invariant, not the cause of it.

This matches the state of the art rather than trailing it. LSM engines share
one WAL across column families for the same reason, atomic cross-family
batches, and they cannot do per-family multiprocess either.

## What the engine carries

A caller-side router placing each column family on one of two stores is the
two-store version of this, and it runs with one process owning both halves.
Underneath it, the engine carries every piece a foreign process needs:

- `ReelStore::open_read_only` plus `refresh()` (`engine.rs`) gives a foreign
  process a live view: the reader's index catches up to what has reached the
  disk, retiring descriptors for segments compaction removed under it.
- The ownership lock takes `flock` via std, records pid, acquire time, boot id
  and build version, and folds them into the contention error. A dead owner
  releases by construction, so there is no lease and nothing to steal.
- `ReelStore::destroy` refuses a store whose owner is alive.

What the router does not do is let the two halves have different owners.

## Placement rules

Placement is the whole design. Two rules bound it:

1. A write batch must stay on one store. In the router that is a debug assert
   with a non-atomic fallback. Stores with different owners turn that fallback
   into a silent atomicity loss between processes, so there it has to be a hard
   error.
2. Columns a batch couples must co-locate. An audit of every `write_batch` call
   site in one caller found three coupled pairs, and the shapes generalise:

| Pair | Why it must be atomic |
|------|----------------------|
| a record and its size sidecar | hot ingest writes both per record |
| a ledger entry and the reservation it consumes | a crash between them double-spends |
| a multipart part's metadata and its payload | the two land together or not at all |

Read paths that join columns across stores get bounded staleness instead of a
point-in-time view, and have to be audited the same way before a split ships.

## Visibility contract

A reader of a foreign store sees a record once it has reached the disk and a
`refresh()` has run. The owner's in-memory tail is out of view until then.
Cross-process read-your-writes therefore takes an explicit flush on the owner
followed by a refresh on the reader, and any consumer that cannot tolerate that
lag belongs in the owning process.

## Scaling across disks

Multi-disk throughput is the engine's own job, not a reason to split stores or
add processes. One store spans its disks directly: a volume root per drive,
one pinned tail per fast root, and aggregate ingest sums across them
(`volumes.md`). A column is not bounded by one disk either, since its segments
place across every root the store spans. Splitting into several stores remains
a choice about ownership and failure isolation, not about speed.

Two things follow:

- Multiple processes are not required for multi-disk throughput. One process
  owns one store over several drives, and the engine is storage-bound, so a
  single process drives them to their caps. Choose process count by service
  topology and failure isolation, not speed.
- Where columns are split across stores for ownership reasons, each store
  spans whatever disks it is given, and the coupled-pair rule above decides
  which columns must travel together.

The scaling claim holds until CPU binds instead of the devices. More processes
do not move that wall, they share the same cores.

## Non-goals

- Concurrent writers on one store. The shared-memory rewrite that would allow
  it (index and robust mutex in a shared map, crash-consistent updates on every
  structure) costs a rework of the whole index for a design that still
  serializes writers. Rejected.
- Leases, expiry, takeover. A stale lock cannot exist, the flock dies with its
  process. Contention always means a live owner, and refusing is correct.
- Cross-store atomic batches. Two stores are two durability points, no design
  gives one atomic commit across them. Placement makes the need disappear
  instead.
- Throughput on a shared device. A second writer against the same disk adds no
  MB/s, the device is the cap. Across devices, the store scales itself, see
  scaling across disks.

## Where the boundary sits

The engine's half is here: the lock, the read-only open, `refresh()`, and a
destroy that refuses a live owner. Three pieces sit above the engine and none of
them is built.

**There is no routed store.** The router handles two stores with one owner, and
nothing generalizes it to N stores behind a placement table: store name,
directory, column list, and mode, owner or reader. Routing would be by
column-family name, with a batch touching two stores a hard error. The table
falls out of a write-path inventory: for every column family, which code paths
write it and from which service they are reachable, with the coupled pairs
above as its first constraints.

**Nothing drives foreign reads on a cadence.** `refresh()` exists and a caller
calls it; there is no loop that opens reader-mode stores and refreshes them on a
schedule, no on-demand hook for paths that just flushed a foreign owner, and no
staleness metric for the time since a store's last successful refresh.

**Nothing proves it across two processes.** The lock, the read-only open and
`refresh()` are covered in-process. No test runs an owner and a reader against
the same store from separate processes, so three claims stand on the code rather
than on a run: the reader sees records after flush plus refresh, a killed owner
releases the lock without cleanup, and a second would-be owner is refused with
the owner named in the error.
