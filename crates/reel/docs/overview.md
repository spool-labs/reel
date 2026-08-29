# Overview: what a reel is, and how a byte gets in and out

A log-structured key-value store: sequential writes, immutable segments,
whole-segment reclaim. One log of segment files spread over one or more volume
roots. A write lands at the end of an open segment and that segment seals when
it fills. Nothing is updated in place, so a delete is a tombstone and space
comes back from the maintenance plane rather than from the write path. An index
maps every live key to the segment and offset holding it, either resident in
memory or paged out of the footers the seals wrote.

This file is the way in. Everything it states is stated at length somewhere
else, and the reading order at the bottom says where.

## A volume on disk

The engine writes five kinds of file and nothing else.

| name | where | what it is |
|---|---|---|
| `NNNNNN.reel` | every root | one segment, zero-padded six digits, monotonic across roots |
| `reel.volumes` | the first root | the manifest naming every root this reel spans |
| `reel.volume` | every root past the first | the marker saying this root was mounted where the manifest says |
| `reel.lock` | the first root | the advisory lock one writing process holds for its lifetime |
| `reel.index` | the first root | the resident index a `checkpoint_index()` wrote down at a cue |

A segment opens with a header record carrying the format version and its own
segment number, so a file that is not this reel's is quarantined rather than
truncated. Segment numbers are global: a root holds a subset of the numbering,
never a numbering of its own, and the index never encodes a path.

## The write path

```
put / write_batch
      |
      v  plan: column check, capacity check, codec, carry
   route to the least loaded tail
      |
      v  reserve a byte range at the write head   (one atomic step, the only
      |                                            point writers contend on)
   copy header, key and payload into the reservation
      |
      v  a batch of more than one opens with a frame declaring its run
   sync owed by the policy?  ->  one flush covering a position
      |
      v  publish: the index moves, under the barrier for a batch
   reservation ran past the end?  ->  roll, and hand the old segment to a sealer
      |
      v
   footer written, synced, spans queued for the index
```

The copy runs on the thread that wanted the write. A batch takes one
reservation, writes with nothing between its records, syncs once, and only then
moves the index, so it is one durability point and one recovery domain. The
seal runs off the append path on a per-tail sealer thread.

## The read path

The index is asked first. A resident column answers from its map: an entry
holds the location, the sequence number, and up to four inline value bytes for
a column that declared them, so a small value never reaches the device at all.
Everything else is one device op placed by the entry.

A paged or hot column answers its unsealed keys the same way and sends the rest
to the footers. That search is a funnel: a whole-column filter over every
sealed key, then the per-segment key spans, then each surviving segment's own
filter, then its directory, then one block of rows, then the row. A row may
carry the value's leading bytes itself, which ends the read there. Otherwise
the row names a record and the driver fetches it.

The search reads every candidate rather than stopping at the first, because a
segment number is not a version: several tails write at once, so a newer record
can land in a lower-numbered segment. The highest sequence number wins.

**Neither path adds a copy of its own.** A copy census over both directions and
the allocation audit in `tests/alloc_counts.rs` agree: `put_owned` moves the
caller's buffer to the tail untouched and the drain gathers header, key and
payload rather than concatenating them, and a read lands the payload in a pooled
buffer that is handed up as the answer. The owned-buffer shape that costs is
load-bearing rather than lazy: a submission outlives the caller's stack frame, so
it needs a buffer it owns.

## The maintenance plane

```
maintain_once            one tick, every step bounded and paced
  retry broken seals
  publish footprint
  page out sealed        paged and hot volumes only
  sweep covers
  prune graves
  shed carried
  compact once  ->  drain wholly dead segments        unlink, nothing copied
                ->  select the highest dead fraction past the threshold
                    gates: claim, pending cover, cue floor, rot pin
                    fetch in offset order, apply in key order
                    repoint each index entry, guarded on its version
                    flush the destination, then unlink the source
  merge when due         armed volumes only
  scrub once
```

Selection is greedy on the highest reclaimable fraction, so the threshold
decides what is allowed and never what is chosen. A segment with nothing live
left is unlinked whole: no bytes copied, and the charge that would pace the
pass is zero.

## Glossary

Terms this codebase uses with meanings a newcomer cannot guess.

- **door**: the two forms every read and write has, blocking and awaited. Both
  do the same work in the same place; what differs is who waits.
- **tail**: one open segment being appended to, with its own file and its own
  write head. A volume runs several, and writers spread across them so the
  kernel never serializes them on a shared inode.
- **run**: two senses. A *sorted run* is a segment whose records sit in key
  order, which is what sealing by rewrite produces. A *dead run* is a
  contiguous stretch of dead bytes inside a segment, which is what a hole punch
  can give back.
- **cover**: the in-memory footprint of a range delete. One record stands for
  however many keys the range holds, and the cover hides every entry older than
  itself until a sweep settles the keys underneath it.
- **grave**: the in-memory mark left by a delete of one key. It has to outlive
  the versions it hides, so it is given up on a segment rather than on a
  sequence number alone.
- **cue**: a marked position the volume can be read as it stood at. Taking one
  seals every tail first, so every version at or below it sits in a footer.
- **repoint**: moving one index entry onto a copy of its record, guarded on the
  version the copy was made from, which is how compaction relocates a record
  without losing a write that raced it.
- **carry**: holding leading value bytes beside the key so a read answers
  without reaching the record: in the index entry, in the sealed row, or both,
  bounded by `carried_budget`. Separately, a tombstone is *carried* into a
  compaction's destination while anything old enough for it to hide survives.
- **footer**: the packed sorted index a seal writes at the end of a segment.
  Partitioned by column, each partition sorted by key and strided at that
  column's own key width, behind a directory and a fixed tail.
- **servo**: the running half of the io-path decision: watch what the work is
  doing, move one knob, keep it if it helped. Its other half is *bias*, the
  startup pass that resolves the path once from what the machine already knows.
  Both names come from the transport mechanism the engine is named after.
- **plane**: one background concern with its own pass and its own rate. The
  maintenance plane is compaction, the merge and the scrub. The read planes are
  the routes a read can take to its bytes: cached, probed, direct, mapped.
- **lane**: one concurrent caller's stream of blocking reads. Lanes and awaited
  depth are two ways to buy the same overlap, and they cost different things.
- **cohort**: a workload shape rather than a structure. Records written once,
  never updated, expiring in whole families. Its dead bytes arrive in whole
  segments, which is the shape whole-segment reclaim is cheap for.

## Backends, bluntly

Take **posix** unless readers await. It is the portable floor, one syscall per
op on the calling thread, and it is a benchmarked path rather than a last
resort. Take a **ring** only on Linux, only when the caller genuinely awaits,
and only if it is fed batches: nothing selects a ring for you, so it has to be
named, and per-record ring submissions land under per-record posix. Set
**`map_above`** only where the working set stays resident and the median is what
the caller is judged on, because mapped reads buy the median and sell the tail,
and lose cold outright.

## Platforms

| capability | Linux | macOS |
|---|---|---|
| posix backend | yes | yes |
| io_uring backend | yes, when the kernel sets a ring up | absent, the request runs posix |
| direct descriptors | yes | request resolves to buffered |
| warm cache probe | yes | reports cold, read goes to the driver |
| range sync | yes | no-op |
| preallocation | one call | reserve then extend |
| hole punch over dead runs | yes | reported, never punched |
| power-cut durability | yes, under the policy | no claim: the call that waits for the drive is not issued |
| tail count | `min(cores, 8)`, floored at one per fast volume | same |
| compaction rate | unpaced | unpaced |

A macOS or BSD run is a real run of everything portable and says nothing about
the io.

## Reading order

1. [format.md](format.md): what is actually on disk. Read this first; every
   other file assumes it.
2. [durability.md](durability.md): what a crash costs, what recovery promises,
   and why there is no write-ahead log.
3. [io.md](io.md): the backends, the page cache, and what the ring is and is
   not worth.
4. [index-shape.md](index-shape.md) then [index-tier.md](index-tier.md): what
   the resident index is held in, then what happens when it will not fit.
5. [compaction.md](compaction.md): the maintenance plane, what a pass may retire,
   and what a rate cap costs the foreground.
6. [why-not-an-lsm.md](why-not-an-lsm.md): where this design sits in the
   lineage and where it departs.
7. [volumes.md](volumes.md) and [servo.md](servo.md): several devices, and
   setting the io path across them.
8. [checksum.md](checksum.md) and [compression.md](compression.md): one
   mechanism each.
9. [cue-points.md](cue-points.md), [checkpoint.md](checkpoint.md),
   [multiwriter.md](multiwriter.md): reading the past, copying it, and sharing
   it.
10. [testing.md](testing.md) and [unsafe.md](unsafe.md): how the claims above
    are held up, and where the compiler stops checking.
