# The index tier

The design record for the index. The map holds the open tails' keys, the graves and
the covers. A sealed segment's keys go to its footer, and the spot index places each
one's record for one device read. It is the gate on the full-history deployment
class, where holding every key in memory at 95 bytes a key is 19 GB at 200 million
keys and an open at a billion cannot happen at all. The spot index costs a 16 byte
slot a sealed key, 19 to 28 bytes with its tables' spare room, so 200 million sealed
keys hold 3.8 to 5.6 GB.

Each shard tracks its bytes, its graves and its paged count, so `live_count` is
`map.len() - graves + paged`.

```
what a lookup walks when the spot index cannot settle it, and what each step takes out

  the map                 unsealed keys, graves, covers
    | miss
  per-segment key spans   segments whose range cannot hold the key
    | candidates
  per-segment filter      candidates the bloom rules out
    | survivors
  directory, then blocks of rows        a block per halving
    | rows
  the newest row wins     a segment number is not a version
```

## What the index has to get right

Every rule below was a shipped defect first. `paged_single_tail` and `paged_multi_tail`
in `differential.rs` run seeded streams against a memory oracle
with `page_out_sealed` driven inside the stream, so every op after a seal runs
against a half-paged index, and they assert the run paged something out, because a
run that pages nothing never reaches a footer. Five cases in
`src/engine/tests.rs` take the playback, the delete, the overwrite, the grave and the
compaction window one at a time.

- **A playback merges the map with the footers**, its own page against a cursor into
  each sealed segment whose key range reaches into the span, in one order.
- **A shard that paged out every key stays in the walk set**, `note_emptied` keeping
  it while it has handed anything over, or `totals` skips its whole paged count.
- **An overwrite of a paged key asks the footers**, since a map that gave a key up
  cannot tell an overwrite from an insert, and guessing books the key twice and
  leaves the record it replaced live forever. This is the footer search on the write
  path.
- **A delete of a paged key settles from the footers**, the grave going in over an
  empty place and booking nothing dead otherwise. A range delete enumerates the
  covered rows and settles them a run at a time before the cover goes up.
- **A put or delete over an empty place asks the spot index for a newer version.** A
  hand-over takes the key's entry out of the map, and its sequence number with it, so
  a write drawn before a newer version that has since sealed and been handed over
  would publish over that version, serve the older value and count the key twice.
  With the shard held, the write reads the header behind each slot of its key whose
  segment may hold anything newer than the write, and a newer version there books the
  write dead. A write above the newest version an open or a hand-over gave the spot
  index skips the check, which is every fresh write not parked across a hand-over,
  and a follower's older records stay at or below it. `late_put`, `late_delete` and
  `late_batch` in `rendezvous_races.rs` drive each door through the race,
  `a_shadowed_write_over_an_empty_place_is_refused` in `index/column.rs` pins the
  three arms, and `holds_newer_reads_only_the_slots_a_ceiling_admits` in
  `index/spot.rs` pins the read.
- **A grave goes once its tombstone's segment is noted and the 2^20 window has passed
  it.** The noted segment is the point a search finds the tombstone row without the
  grave. A tombstone compaction drops takes its grave with it, since nothing older is
  left for the grave to hide, and an eviction's grave has no tombstone behind it, so it
  stands for the life of the process. A range cover is held while any sealed segment
  overlaps it.
- **A key comes back into the map for a compaction window**, between the source
  retiring and the destination sealing, where nothing else resolves it.
- **`get` takes the newest candidate, not the first.** A segment number is not a
  version: several tails write at once, so a rewrite can land in a lower-numbered
  segment than the version it replaces.
- **A read settles the sealed queue itself**, or between a seal and the next
  maintenance tick a paged read searches a footer set with the holding segment
  missing and reads a key absent or stale. `settle_sealed` sits behind one relaxed
  load, so nothing waiting costs nothing.
- **An unsealed record is answerable only by the map, so nothing evicts it.**
  `evict_at` removes an entry with no grave, right for a sealed record whose footer
  goes on answering and data loss for an unsealed one. Refused when no footer covers
  the location.
- **A copy the index cannot place is dropped.** With spans lagging, the adopt branch
  wrote the source's sequence number into the map, outranking a newer footer row and
  resurrecting a replaced version. Now the copy is refused, which is safe because the
  source holds the record until the pass retires it, so the copy goes and the key stays.

## What a playback costs, measured

A playback merges the map's page with a cursor into every sealed segment whose key
range reaches into the span. Measured 2026-07-29 on a ccx33, a playback over
twenty-five sealed segments cost about four times one the map answered alone. It was
the segment count that moved it: the merge scanned every open cursor three times per
key emitted, so the per-key work was linear in how many segments overlapped the span.

That is fixed. The cursors are heap ordered by the key each sits on, holding
positions rather than keys, so a key costs the depth rather than the width and only
the cursors holding it are touched. On the simulator, which measures index work alone,
the per-key merge cost at 257 sealed segments falls from 1.21 us to 88 ns and the
whole playback from 160.64 ms to 11.84 ms, and at 2 and 5 segments the two arms tie,
so nothing was traded for it.

## The open

A rebuild sweeps each footer: one key span per partition into the sealed ranges, the
range covers with their footprint, the tombstones held, and each segment's tally
booked against that segment. Every sealed footer's rows then go into the spot index on
`LOADERS` threads, a 16 byte slot a sealed key. Up to eight readers parse footers
ahead of the join, one waits in the hand-off queue and eight loaders hold one each, so
an open holds up to about eighteen parsed footers at once on posix and about ten on
the ring. Sealed keys never enter the map, and `an_open_never_installs_its_sealed_keys`
is the check.

Measured 2026-07-30 by `tests/probes/open_time.rs`, before the spot index, reopening
a volume whose segments are all sealed:

| segments | keys | open | index |
|---|---|---|---|
| 65 | 33,281 | 1.59 ms | 0 KiB |
| 257 | 133,121 | 6.13 ms | 0 KiB |
| 1025 | 532,481 | 24.68 ms | 0 KiB |

Zero then, because the open installed nothing. The spot index load changed both
columns: the open now takes every sealed row, and the index figure grows a slot a
sealed key. The probe prints the spot index beside the total, and these rows wait on
a fresh run. This is the simulator, so it times index work alone.

**Only one derived number is correctness.** A rebuild does not have to reproduce each
segment's byte split or each column's live count exactly, since those steer
`select_target`, the pressure gate and reporting, so a drifted counter costs a
mispriced pass and cannot lose data. `min_lsn` is the exception, because
`should_carry` drops tombstones against it and a floor too high resurrects deleted
keys. The sweep reads every row anyway, so it folds each non-tombstone mark as it
passes and the per-segment minimum comes out exactly as the resolver defines it.

**The rest of the accounting is openly stale at open and trued up behind it.** Each
segment writes what it weighed into its own footer at seal, and the sweep books that
tally, so what needs truing is only the shadowing after the seal. Three paths true it.
The rebuild's join against the sealed footers books dead the newest sealed row each
surviving walked entry shadows. The spot index load meets every version of a key it
takes, and a footer settles each one against the version that came next. The scrub
settles a segment's dead tally as its lap completes it. The footer field `sealed_at`
is the sequence frontier the segment sealed under, so both joins debit only a
shadowing at or past it, and anything below is already in the tally. A version a key
run left out and a key only the headers could settle stay the scrub's, and a read-only
open never scrubs. `a_paged_open_recovers_its_split_from_the_tally` and
`a_walked_tail_settles_the_sealed_split` are the checks.

## What the counters promise

Each shard counts the live keys and bytes the spot index answers for beside what its
map holds, and the spot index is the ledger: one live slot is one counted key. A
path that takes a live slot out or marks it displaced books the shard down once, and
a path that finds no live slot books nothing, so a delete, an overwrite, a cover's
release, an eviction and a compaction move never count one record twice.

| step | what it books |
|---|---|
| hand-over | the key moves from the map to `paged`, bytes unchanged |
| put or delete over a paged key | the displaced slot out, bytes by its length class |
| cover release | each covered record the spot index holds live |
| open | every key the load takes a fresh slot for, at its row's length |
| tail over a sealed key at open | the sealed version out of the count and the spot index |

An open loads a covered segment's keys from the key run over it, since the run
keeps one row a key and a footer can still hold a version whose newer one died in a
retired segment, and it releases every standing cover before it returns. `totals()`,
`column_totals` and `prefix_totals` answer exactly, up to spot hash collisions in
the count and `spot_slack()` in the bytes, and a reopen answers what the volume
answered before it. `paged_totals_model.rs` and the differential fixture check it.

## The filter field

What ships is the seam and one kind: a blocked bloom behind a kind byte, sized by
`ReelConfig.filter_bits` at ten by default and zero to turn it off. The region sits
between the packed rows and the directory, one header per partition so the walk stays
in step with it, and `FooterMap` takes both in one read. Merge output spends no bits:
its rows span the whole keyspace, so its fence answers placement and a search
reaching it is nearly always a hit.

Measured on the counts, which say the same thing on any machine.
`tests/probes/filter_cost.rs` writes six thousand scattered sixteen byte keys over
segments small enough that a volume of them overlaps completely, then asks for two
thousand keys nothing wrote:

| bits per key | segments searched per miss | searches removed | filter bytes |
|---|---|---|---|
| 0 | 11.9 | | 0 |
| 4 | 1.43 | 88.0% | 3,000 |
| 7 | 0.30 | 97.5% | 5,250 |
| 10 | 0.08 | 99.3% | 7,500 |
| 14 | 0.03 | 99.7% | 10,500 |

Ten bits leaves 0.71 percent of searches standing, the textbook rate for the shape,
which confirms the probes are independent enough. On the same probe a miss goes from
1.04 us to 0.59 us and a hit from 1.60 to 1.11, hits gaining because a footer search
reads every candidate even after it finds a row. Both are floors: a search there
is a memcpy off a warm block, about 38 ns, where a cold read costs thousands of times
more.

**The format rules the filter has to keep.** It covers every key the segment holds a
row for, deletes included, since a tombstone missing from its own segment's filter
lets a probe skip the segment and an older version resurrect. A zero-entry column
writes a zero-length filter that reads as always-false, which removes those segments
from the candidate set for free. Everything fails open: an unrecognized kind or a
truncated region degrades to searching the segment, never to skipping it.

**The filter is why the footer cache is bounded.** A footer map used to be exempt
from the byte budget, a directory being a handful of spans whatever its segment
holds, and the filter rides in the map, sized by the segment's key count: megabytes
on a metadata segment of small records and gigabytes on a volume of them, held for
good. `FooterCache` splits its capacity into three equal pools, footers, directories
and blocks, each evicted oldest first against its own third, and an entry weighing
more than its pool is turned away rather than admitted alone, since the caller keeps
what it just read either way and taking it in would empty the pool for a tenant that
fits nothing beside it. Losing one costs the next reader a read and a segment it
cannot rule out, a cache miss rather than a wrong answer.

## The footprint formula reads low, and one shard shape is ruinous

`resident_bytes` was a formula, the key width plus `size_of::<Entry>()` plus the
shape's own per-key overhead, 37 bytes on a tree, so it could not see the tree at all.
It now adds up what every shard's map allocated, spare capacity included, plus the
shard array, the filters in front of it through `ShardMap::heap_bytes`, and the spot
index's buckets. The weighing route is `Scale` in `tests/raw_throughput.rs`, a
per-thread counting `GlobalAlloc` behind a `WEIGHING` flag, reported by `cpu_terms`. A million keys, node width 64 against 16, counted
layout bytes rather than timings, so the machine matters little:

| column | width 64 | width 16 | the formula says |
|---|---|---|---|
| records, 34 B key, 1 group | 170 | 175 | 103 |
| records, 34 B key, 8 groups | 170 | 175 | 103 |
| records, 34 B key, 50 groups | 137 | 140 | 103 |
| 16 B key, ascending | 73 | 137 | 69 |
| 16 B key, scattered | 1,325 | 1,329 | 69 |

**The formula read 1.33x to 1.65x low**, so every index footprint figure in these
docs taken from it is understated and the 95 and 103 bytes a key that so much rests on are gauges.
The node width change is not a footprint regression either way, neutral on the bulk
record column and a large win on a 16 byte ascending key.

**A column that shards two bytes wide and holds scattered keys costs 1,325 bytes a
key**, fifteen keys a shard paying the whole fixed cost of a shard, and node width is
not the cause, since 16 gives 1,329. The bulk record column escapes it only because
its caller's allocation rule holds one deployment to about fifty occupied groups, the
137 row. **Any other two-byte-sharded column with uniform keys pays the 1,325**, so
shard width is a decision to take against the expected occupancy rather than a
default to copy.

## Still open

- **An overwrite probe on the write path.** Every put of a key the map does not hold
  asks the spot index whether it is an overwrite. A slot whose segment holds nothing
  newer than the put is booked from its length class with no read, and the probe fires
  only when the map holds nothing at all for the key.
- **The block cursor, owed to the read path.** It would bound the footer cache and a
  playback's residency in bytes rather than in parsed footers, and it is no longer a
  gate on recovery. It has to carry a streaming verify when a footer is first opened,
  the checksum covering the whole footer rather than the directory alone, and a copy
  of the key each cursor sits on, because `FooterPartition::run` holds a borrowed key
  across a neighbour probe a block refill would invalidate.
- **A merged playback holds a cursor per overlapping segment**, one or two on a
  slot-led column and every sealed segment on the volume for a column with uniform
  keys, each holding its parsed footer. The count is unbounded and the duration is
  the caller's, since a playback is a store iterator kept for as long as it likes,
  and the layer that owns the residency budget cannot see them. They are at least
  opened once per playback rather than once per page (`PlaybackCursor`, with
  `a_playback_opens_its_footers_once` as the check), which removed the quadratic io
  past sixty-four overlapping segments.
- **A seal anywhere in a column invalidates every playback of it.** The generation
  counter is per column, so a slot-led volume taking seals at the high end while a
  long playback crosses the low end reopens that playback's cursors on every seal.
  The counter would have to carry the changed range rather than a count.
- **Benchmarking a run honestly.** A backend sweep must tick the handover
  between the fill and the reads and print how many keys answered from a footer, so a
  run that pages nothing says so, and it needs `REEL_BENCH_SEGMENT` below the sweep's
  payload or nothing seals at all.
