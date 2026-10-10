# The index tier

The map holds the open tails' keys, the graves and the covers. A sealed segment's keys go to its
footer, and the spot index places each one's record for one device read. This is what makes the
full-history deployment class possible: holding every key in memory at 95 bytes a key is 19 GB at
200 million keys, and an open at a billion cannot happen at all. The spot index costs a 16 byte
slot a sealed key, 19 to 28 bytes with its tables' spare room, so 200 million sealed keys take 3.8
to 5.6 GB.

Each shard tracks its bytes, its graves and its paged count, so `live_count` is
`map.count() - graves + paged`.

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

Every rule below was a shipped defect first. `paged_single_tail` and `paged_multi_tail` run seeded
streams against a memory oracle with `page_out_sealed` inside the stream, so every op after a seal
meets a half-paged index. They assert the run paged something out, since a run that pages nothing
never reaches a footer. Five engine tests take the playback, the delete, the overwrite, the grave
and the compaction window one at a time.

- **A playback merges the map with the footers**: its own page against a cursor into each sealed
  segment whose key range reaches the span, in one order.
- **A shard that paged out every key stays in the walk set.** `note_emptied` keeps it while it has
  handed anything over, or `totals` would skip its whole paged count.
- **An overwrite of a paged key asks the footers.** A map that gave a key up cannot tell an
  overwrite from an insert, and guessing books the key twice and leaves the replaced record live
  for ever. This is the footer search on the write path.
- **A delete of a paged key settles from the footers.** The grave goes in over an empty place and
  books nothing dead otherwise. A range delete lists the covered rows and settles them a run at a
  time before the cover goes up.
- **A put or delete over an empty place asks the spot index for a newer version.** A hand-over
  takes the key's entry, and its sequence number, out of the map. A write drawn before a newer
  version that has since sealed and been handed over would publish over that version, serve the
  older value and count the key twice. So with the shard held, the write reads the header behind
  each slot of its key whose segment may hold anything newer, and a newer version there books the
  write dead. A write above the newest version an open or a hand-over gave the spot index skips the
  check. That is every fresh write not parked across a hand-over, and a follower's older records
  stay at or below it. `late_put`, `late_delete` and `late_batch` drive each door through the
  race, `a_shadowed_write_over_an_empty_place_is_refused` pins the three arms, and
  `holds_newer_reads_only_the_slots_a_ceiling_admits` pins the read.
- **A grave goes once its tombstone's segment is noted, the 2^20 window has passed it, and no
  write drawn before it is still out.** Once the segment is noted, a search finds the tombstone row
  without the grave. A hand-over asks the map about a row at or below a pruned grave or cover
  before the spot index takes it. When compaction drops a tombstone its grave goes too, since
  nothing older is left to hide. An eviction's grave has no tombstone behind it, so it stands for
  the life of the process. A range cover stays while any sealed segment overlaps it.
- **A key comes back into the map for a compaction window**, between the source retiring and the
  destination sealing, where nothing else resolves it.
- **`get` takes the newest candidate.** A segment number is not a version. Several tails write at
  once, so a rewrite can land in a lower-numbered segment than the version it replaces.
- **A read settles the sealed queue itself.** Otherwise, between a seal and the next maintenance
  tick, a paged read searches a footer set missing the holding segment and reads a key as absent
  or stale. `settle_sealed` sits behind one relaxed load, so an empty queue costs nothing.
- **Nothing evicts an unsealed record, since only the map answers for it.** `evict_at` removes an
  entry with no grave. That is right for a sealed record whose footer still answers and data loss
  for an unsealed one, so it refuses when no sealed segment covers the location.
- **A copy the index cannot place is dropped.** With spans lagging, the adopt branch once wrote
  the source's sequence number into the map, outranked a newer footer row and resurrected a
  replaced version. Now the copy is refused. The source holds the record until the pass retires
  it, so the copy goes and the key stays.

## What a playback costs

A playback merges the map's page with a cursor into every sealed segment whose key range reaches
the span. On a ccx33 on 2026-07-29, a playback over twenty-five sealed segments cost about four
times one the map answered alone. The merge scanned every open cursor three times per key, so
per-key work grew with the number of overlapping segments.

Now the cursors sit in a heap ordered by the key each is on, holding positions, so a key costs
the heap depth and only the cursors holding it are touched. On the simulator, which measures index
work alone, at 257 sealed segments the per-key merge falls from 1.21 us to 88 ns and the whole
playback from 160.64 ms to 11.84 ms. At 2 and 5 segments the two arms tie.

## The open

A rebuild sweeps each footer: one key span per partition into the sealed ranges, the range covers
with their footprint, the tombstones held, and each segment's tally booked against that segment.
Then every sealed footer's rows go into the spot index on `LOADERS` (8) threads, a 16 byte slot a
sealed key. Up to eight readers parse footers ahead of the join, one waits in the hand-off queue
and eight loaders hold one each. So an open holds up to about eighteen parsed footers at once on
posix, and about ten on the ring, which reads footers on one thread. Sealed keys never enter the
map, and `an_open_never_installs_its_sealed_keys` checks it.

The `open_time` probe on 2026-07-30, before the spot index, reopening a volume whose segments are
all sealed:

| segments | keys | open | index |
|---|---|---|---|
| 65 | 33,281 | 1.59 ms | 0 KiB |
| 257 | 133,121 | 6.13 ms | 0 KiB |
| 1025 | 532,481 | 24.68 ms | 0 KiB |

The index was zero because that open installed nothing. The spot index changed both columns: the
open now takes every sealed row, and the index grows a slot a sealed key. The probe prints the
spot index beside the total, and these rows wait on a fresh run. It runs on the simulator, so it
times index work alone.

**Only one derived number is correctness.** A rebuild need not reproduce each segment's byte split
or each column's live count exactly. Those steer `select_target`, the pressure gate and
reporting, so a drifted counter costs a mispriced pass and cannot lose data. `min_lsn` is the
exception: `should_carry` drops tombstones against it, and a floor too high resurrects deleted
keys. The sweep reads every row anyway, so it folds in each non-tombstone mark as it passes and
the per-segment minimum comes out exactly as the resolver defines it.

**The rest of the accounting is stale at open and trued up after.** Each segment writes what it
weighed into its footer at seal, and the sweep books that tally, so only shadowing after the seal
needs truing. Three paths do it:

- The rebuild's join against the sealed footers books dead the newest sealed row that each
  surviving walked entry shadows.
- The spot index load meets every version of a key it takes, and a footer settles each one
  against the version that came next.
- The scrub settles a segment's dead tally as its lap completes it.

The footer's `sealed_at` field is the sequence frontier the segment sealed under, so both joins
debit only shadowing at or past it. Anything below is already in the tally. A version a key run
left out, and a key only the headers could settle, stay with the scrub, and a read-only open never
scrubs. `a_paged_open_recovers_its_split_from_the_tally` and `a_walked_tail_settles_the_sealed_split`
check it.

## What the counters promise

Each shard counts the live keys and bytes the spot index answers for, beside what its map holds.
The spot index is the ledger: one live slot is one counted key. A path that takes a live slot out
or marks it displaced books the shard down once, and a path that finds no live slot books
nothing. So a delete, an overwrite, a cover's release, an eviction and a compaction move never
count one record twice.

| step | what it books |
|---|---|
| hand-over | the key moves from the map to `paged`, bytes unchanged |
| put or delete over a paged key | the displaced slot out, bytes by its length class |
| cover release | each covered record the spot index holds live |
| open | every key the load takes a fresh slot for, at its row's length |
| tail over a sealed key at open | the sealed version out of the count and the spot index |

An open loads a covered segment's footer through the key run over it, taking only the rows the
run picked. The run keeps one row a key, and a footer can still hold a version whose newer one
died in a retired segment. The open releases every standing cover before it returns. `totals()`,
`column_totals` and `prefix_totals` answer exactly, up to spot hash collisions in the count and
`spot_slack()` in the bytes, and a reopen answers what the volume answered before. The
`paged_totals_model` test and the differential fixture check it.

## The filter field

What ships is the seam and one kind: a blocked bloom behind a kind byte, sized by
`ReelConfig.filter_bits`, ten by default and zero for off. The region sits between the packed rows
and the directory, one header per partition so the walk stays in step with it, and `FooterMap`
takes both in one read. Merge output spends no bits. Its rows span the whole keyspace, so its fence
answers placement, and a search that reaches it nearly always hits.

Measured on counts, which come out the same on any machine. The `filter_cost` probe writes six
thousand scattered sixteen byte keys over segments small enough that they all overlap, then asks
for two thousand keys nothing wrote:

| bits per key | segments searched per miss | searches removed | filter bytes |
|---|---|---|---|
| 0 | 11.9 | | 0 |
| 4 | 1.43 | 88.0% | 3,000 |
| 7 | 0.30 | 97.5% | 5,250 |
| 10 | 0.08 | 99.3% | 7,500 |
| 14 | 0.03 | 99.7% | 10,500 |

Ten bits leaves 0.71 percent of searches standing, the textbook rate for the shape, so the probes
are independent enough. On the same probe a miss goes from 1.04 us to 0.59 us and a hit from 1.60
to 1.11. Hits gain because a footer search reads every candidate even after it finds a row. Both
are floors: a search there is a memcpy off a warm block, about 38 ns, and a cold read costs
thousands of times more.

**Format rules the filter keeps.**

- It covers every key the segment holds a row for, deletes included. A tombstone missing from its
  own segment's filter lets a probe skip the segment and an older version resurrect.
- A zero-entry column writes a zero-length filter that reads as always-false, which drops those
  segments from the candidate set for free.
- Everything fails open: an unknown kind or a truncated region falls back to searching the segment.

**The filter is why the footer cache is bounded.** A footer map used to be exempt from the byte
budget, since a directory is a handful of spans whatever its segment holds. The filter lives in the
map and grows with the segment's key count: megabytes on a metadata segment of small records and
gigabytes on a volume of them, held for good. `FooterCache` splits its capacity into three equal
pools, footers, directories and blocks, each evicted oldest first against its own third. It turns
away an entry heavier than its pool, since the caller keeps what it just read either way, and
admitting it would empty the pool for one tenant. Losing an entry costs the next reader a read and
a segment it cannot rule out. That is a cache miss and never a wrong answer.

## The footprint formula read low, and one shard shape is ruinous

`resident_bytes` used to be a formula: the key width plus `size_of::<Entry>()` plus the shape's
per-key overhead, 37 bytes on a tree, so it could not see the tree at all. It now adds up what
every shard's map allocated, spare capacity included, plus the shard array and the filters through
`ShardMap::heap_bytes`, and the spot index's buckets. The throughput sweep's `Scale` route weighs
it with a per-thread counting `GlobalAlloc` behind a `WEIGHING` flag, reported by `cpu_terms`. A
million keys, node width 64 against 16, bytes a key, counted from layout so the machine matters
little:

| column | width 64 | width 16 | the formula says |
|---|---|---|---|
| records, 34 B key, 1 group | 170 | 175 | 103 |
| records, 34 B key, 8 groups | 170 | 175 | 103 |
| records, 34 B key, 50 groups | 137 | 140 | 103 |
| 16 B key, ascending | 73 | 137 | 69 |
| 16 B key, scattered | 1,325 | 1,329 | 69 |

**The formula read 1.33x to 1.65x low.** Every index footprint figure taken from it is
understated, and the 95 and 103 bytes a key that so much rests on are gauges. Node width is no
footprint regression either way: neutral on the bulk record column and a large win on a 16 byte
ascending key.

**A column that shards two bytes wide and holds scattered keys costs 1,325 bytes a key**, with
fifteen keys a shard paying a shard's whole fixed cost. Node width plays no part, since 16 gives
1,329. The bulk record column escapes only because its caller's allocation rule holds one
deployment to about fifty occupied groups, the 137 row. **Any other two-byte-sharded column with
uniform keys pays the 1,325**, so pick shard width against the expected occupancy.

## Still open

- **An overwrite probe on the write path.** Every put of a key the map does not hold asks the spot
  index whether it is an overwrite. A slot whose segment holds nothing newer than the put is booked
  from its length class with no read, and the probe fires only when the map holds nothing at all
  for the key.
- **The block cursor, owed to the read path.** It would bound the footer cache and a playback's
  residency in bytes, where today they are counted in parsed footers. It no longer gates recovery.
  It needs a streaming verify when a footer is first opened, with the checksum over the whole
  footer as well as the directory, and a copy of the key each cursor sits on, because
  `FooterPartition::run` holds a borrowed key across a neighbour probe that a block refill would
  invalidate.
- **A merged playback holds a cursor per overlapping segment**: one or two on a slot-led column,
  and every sealed segment on the volume for a column with uniform keys, each with its parsed
  footer. The count is unbounded and the duration is the caller's, since a playback is a store
  iterator kept as long as the caller likes, and the layer that owns the residency budget cannot
  see them. Each footer at least opens once per playback (`PlaybackCursor`, checked by
  `a_playback_opens_its_footers_once`), which removed the quadratic io past sixty-four
  overlapping segments.
- **A seal anywhere in a column invalidates every playback of it.** The generation counter is per
  column, so a slot-led volume taking seals at the high end reopens a long playback's cursors at
  the low end on every seal. A fix needs the counter to hold the changed range.
- **Benchmarking a run honestly.** A backend sweep must tick the hand-over between the fill and the
  reads and print how many keys a footer answered, so a run that pages nothing says so. It also
  needs `REEL_BENCH_SEGMENT` below the sweep's payload, or nothing seals.
