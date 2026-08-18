# The index tier

The design record for `Paged` and `Hot`, the on-disk index tier. It is the gate on
the full-history deployment class, where 95 bytes of resident RAM per key is 19 GB
at 200 million keys and the open cannot happen at all at a billion.

`Resident` is the default and what every measurement before the tier used: every live
key in a sharded map, each shard tracking its bytes, its graves and its paged count,
so `live_count` is `map.len() - graves + paged`. `Paged` hands a sealed segment's keys
to its footer and resolves them from there. `Hot { after_secs, budget }` is the same
machinery with a policy in front of it: a sealed segment's keys stay resident until
they have been sealed longer than the age or the maps weigh more than the budget,
whichever comes first, and then the oldest go. What that buys is the shape a caller
actually reads in, one io for the recent past and two for the history behind it. None
of the three is a migration of another; the default is unchanged.

```
where a key's location lives                              ops per read

  Resident   every live key in the sharded map                     1
  Hot        recent seals in the map, older ones in footers      1..2
  Paged      only unsealed keys, graves and covers in the map       2

what a paged lookup walks, and what each step takes out

  the map                 unsealed keys, graves, covers
    | miss
  sealed-key filter       one stack per column over every sealed key
    | maybe
  per-segment key spans   segments whose range cannot hold the key
    | candidates
  per-segment filter      candidates the bloom rules out
    | survivors
  directory, then a block of rows       one block read per survivor
    | rows
  the newest row wins     a segment number is not a version
```

A fence, where one is armed, replaces the halvings inside a partition with a scan
over leads, so a search reads one block rather than one per halving.

## What a paged index has to get right

Every rule below was a shipped defect first. `paged_single_tail`, `paged_multi_tail`
and `hot_single_tail` in `differential.rs` run seeded streams against a memory oracle
with `page_out_sealed` driven inside the stream, so every op after a seal runs
against a half-paged index, and they assert the run paged something out, because a
paged run that pages nothing is a resident run wearing a config. Five named cases in
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
- **A grave is given up on its own segment, not on a sequence number**, once the
  segment its tombstone landed in has a footer, which is the point a search finds the
  tombstone row without it. A range cover is held while any sealed segment overlaps
  it.
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
- **A copy the index cannot place is dropped, not adopted.** With spans lagging, the
  adopt branch wrote the source's sequence number into the map, outranking a newer
  footer row and resurrecting a replaced version. Now a counted refusal,
  `unclaimed_copies()`, safe because the source holds the record until the pass
  retires it, so the copy goes rather than the key.

## What a paged playback costs, measured

Measured 2026-07-29 on a ccx33, cached shape, posix, a playback through `iter_prefix`
driven by a caller-side backend sweep. The pair that means anything is a resident and
a paged run at the **same** segment size, since segment size moves the reel on its
own: at 4 MiB it takes 135 flushes where 32 MiB takes 24, and that costs the resident
tier its concurrent write throughput regardless of where the keys live.

| payload | resident, 3 sealed | paged, 3 sealed | resident, 25 sealed | paged, 25 sealed |
|---|---|---|---|---|
| 1 KiB | 1.45 ms | 1.48 ms | 1.36 ms | 3.14 ms |
| 2 KiB | 1.48 ms | 2.64 ms | 1.50 ms | 3.91 ms |
| 4 KiB | 1.49 ms | 2.93 ms | 1.38 ms | 5.62 ms |
| 16 KiB | 392 us | 709 us | 401 us | 1.38 ms |
| 64 KiB | 119 us | 190 us | 111 us | 364 us |
| 1 MiB | 20.9 us | 30.7 us | 20.9 us | 44.4 us |

**A paged playback costs about twice a resident one over three sealed segments and
about four times over twenty-five.** It was the segment count that moved it: the
merge scanned every open cursor three times per key emitted, so the per-key work was
linear in how many segments overlapped the span.

That is fixed. The cursors are heap ordered by the key each sits on, holding
positions rather than keys, so a key costs the depth rather than the width and only
the cursors holding it are touched. By `tests/probes/playback_speed.rs`, index work
on the simulator rather than device work, the per-key merge cost at 257 sealed
segments falls from 1.21 us to 88 ns and the whole playback from 160.64 ms to
11.84 ms, and at 2 and 5 segments the two arms tie, so nothing was traded for it.

A point read pays much less: at twenty-five sealed segments it is within a few
percent of resident at every size, peaking at fourteen percent on the middle rows.
The footer search is fine; it was the merge that was not.

The hot tier lands between the two, over the same twenty-five sealed segments with a
one mebibyte budget and the age set past the run, so the budget alone decides. Only
that arm has been measured; `after_secs` never has.

| payload | keys | resident | hot | paged |
|---|---|---|---|---|
| 1 KiB | 25,600 | 1.36 ms | 2.89 ms | 3.14 ms |
| 4 KiB | 25,600 | 1.38 ms | 3.78 ms | 5.62 ms |
| 16 KiB | 6,400 | 401 us | 413 us | 1.38 ms |
| 64 KiB | 1,600 | 111 us | 121 us | 364 us |
| 1 MiB | 100 | 20.9 us | 19.9 us | 44.4 us |

The cases the budget fits stay resident and play at resident speed; the ones that
overflow it track the paged tier. It handed 233,115 keys to footers where the paged
run handed 404,355, which is the tier working rather than the tier being skipped.

## What the tier saves, measured

Over fifty thousand record keys on the same box, 2026-07-29:

| residency | index held |
|---|---|
| resident | 4,638 KiB |
| hot, one mebibyte budget | 987 KiB |
| paged | 51 KiB |

**Paged holds ninety-one times less than resident.** The hot run settled at 987 KiB
against the 1,024 KiB it was given, so the budget is honoured to within four percent.
The resident row is 95.0 bytes a key, the same figure a counting allocator produced
on another day by another method. Accounted rather than observed, and it had to be:
the sweep's resident-footprint columns read a process RSS delta, and on a warmed heap
the allocator satisfies a four megabyte map off its free list without the process
growing at all.

## Lazy open

Recovery used to install every sealed key into the map and hand them back on the
first tick, so a paged volume peaked at exactly the resident footprint it exists to
avoid, once, at open. A paging rebuild sweeps each footer instead of collecting it:
one key span per partition into the sealed ranges, the range covers with their
footprint, the tombstones held, and each segment's rows booked against that segment,
one parsed footer in memory at a time. Sealed keys are born paged rather than
installed and evicted, and `a_paged_open_never_installs_its_sealed_keys` is the
check.

Measured 2026-07-30 by `tests/probes/open_time.rs`, reopening a volume whose segments
are all sealed:

| segments | keys | resident open | resident index | paged open | paged index |
|---|---|---|---|---|---|
| 65 | 33,281 | 11.53 ms | 3,087 KiB | 1.59 ms | 0 KiB |
| 257 | 133,121 | 55.25 ms | 12,350 KiB | 6.13 ms | 0 KiB |
| 1025 | 532,481 | 244.40 ms | 49,400 KiB | 24.68 ms | 0 KiB |

Zero, because nothing is installed: every key stays in the footer it was already in.
The open is 7.3x to 9.9x faster as well as smaller, and the ratio widens with segment
count because a resident open resolves every key and a paged one reads a span per
partition. Two honest edges: the paged figure is what the key maps hold, and the
sealed ranges are kilobytes held elsewhere; and this is the simulator, so it is index
work rather than device work.

**Only one derived number is correctness.** A rebuild does not have to reproduce each
segment's byte split or each column's live count exactly, since those steer
`select_target`, the pressure gate and reporting, so a drifted counter costs a
mispriced pass and cannot lose data. `min_lsn` is the exception, because
`should_carry` drops tombstones against it and a floor too high resurrects deleted
keys. The sweep reads every row anyway, so it folds each non-tombstone mark as it
passes and the per-segment minimum comes out exactly as the resolver defines it.

**The rest of the accounting is openly stale at open and trued up behind it.** The
sweep seeds each segment's counters from its own rows, overstating by exactly the
rows other segments have shadowed. Each segment writes what it weighed into its own
footer at seal, so what needs truing is only the shadowing after that: the scrub
settles a segment's dead tally as its lap completes it, and the rebuild's join
against the sealed footers books dead the newest sealed row each surviving walked
entry shadows. The footer field `sealed_at` makes that debit exact rather than
double-counted, being the sequence frontier the segment sealed under, so only
shadowings at or past it are debited and anything below is already in the tally.
Sealed-over-sealed shadowing stays the scrub's, being the cross-segment join a paged
open exists to avoid. `a_paged_open_recovers_its_split_from_the_tally` and
`a_walked_tail_settles_the_sealed_split` are the checks.

## Born segments, and what the counters promise

Deferring the seal widened when a reopen can see a sealed live segment, a state
production reaches on any restart, and two defects lived there. Compacting such a
segment lost its keys, since `repoint_paged` refused a key the counters never held,
`copy_live` discarded the refusal after appending the copy, and the retire took the
footer: data loss, pinned by `compaction_carries_a_key_through_a_paged_reopen`. And
overwrites and deletes of those keys moved shard counters that never held them,
corrupting the totals once a shard mixed counted and uncounted keys.

Both are fixed by segment attribution: the segments a rebuild leaves sealed are
marked born in the `SegmentTable`, and every settle and repoint takes its
counted-ness from the row's segment, so a born key enters the counters exactly once,
as compaction or an overwrite touches it.

**While a born segment stands, `totals()` is a floor**: never above what a scan
finds, exact again the moment the last born segment retires. The differential fixture
asserts strict equality whenever `born_segments()` is zero and the floor otherwise,
and the visibility assert stays strict throughout. Exact counts at any instant of the
born window would need the cross-segment join a paged open exists to avoid.
`paged_sim_backend_storm` covers paged residency under concurrency.

## The filter field

What ships is the seam and one kind: a blocked bloom behind a kind byte, sized by
`ReelConfig.filter_bits` at ten by default and zero to turn it off. The region sits
between the packed rows and the directory, one header per partition so the walk stays
in step with it, and `FooterMap` takes both in one read. A resident volume spends no
bits whatever the knob says, since it never searches a footer, and merge output
spends none either: its rows span the whole keyspace, so its fence answers placement
and a search reaching it is nearly always a hit.

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
1.04 us to 0.59 us and a hit from 1.60 to 1.11, hits gaining because a paged read
searches every candidate even after it finds a row. Both are floors: a search there
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

**A third line above the per-segment filters: the sealed-keys filter.** Those answer
per segment, so a hash-keyed column pays one check per sealed segment to learn what a
fresh key already is: absent everywhere. `index/sealed_keys.rs` holds every sealed
key of a column as one stack of filters, asked ahead of the candidate fan-out in all
three sealed searches, and a no skips the walk and every per-segment filter behind
it. Levels start at 256 Ki keys and grow fourfold, so a billion keys is seven levels
and a ruled-out key costs seven cache lines. It is fed where sealed spans are
recorded and before they are visible, the paged rebuild's sweep and `note_spans` at a
seal, which is the invariant that makes the skip safe. Nothing is removed: a retired
segment's bits stay as false positives, a search that finds nothing. The overwrite
probe is one of those three searches, so a fresh key skips the fan-out going in as it
does coming out. `a_fresh_key_skips_the_sealed_search` pins both feeds and
`sealed_skips()` counts the skips.

## The footprint formula reads low, and one shard shape is ruinous

`resident_bytes` is a formula, the key width plus `size_of::<Entry>()` plus the
shape's own per-key overhead, 37 bytes on a tree and arithmetic on the slot for an
open table, so it cannot see the tree at all. The weighing route is `Scale` in
`tests/raw_throughput.rs`, a per-thread counting `GlobalAlloc` behind a `WEIGHING`
flag, reported by `cpu_terms`. A million keys, node width 64 against 16, counted
layout bytes rather than timings, so the machine matters little:

| column | width 64 | width 16 | the formula says |
|---|---|---|---|
| records, 34 B key, 1 group | 170 | 175 | 103 |
| records, 34 B key, 8 groups | 170 | 175 | 103 |
| records, 34 B key, 50 groups | 137 | 140 | 103 |
| 16 B key, ascending | 73 | 137 | 69 |
| 16 B key, scattered | 1,325 | 1,329 | 69 |

**The formula reads 1.33x to 1.65x low**, so every index footprint figure in these
docs is understated and the 95 and 103 bytes a key that so much rests on are gauges.
The node width change is not a footprint regression either way, neutral on the bulk
record column and a large win on a 16 byte ascending key.

**A column that shards two bytes wide and holds scattered keys costs 1,325 bytes a
key**, fifteen keys a shard paying the whole fixed cost of a shard, and node width is
not the cause, since 16 gives 1,329. The bulk record column escapes it only because
its caller's allocation rule holds one deployment to about fifty occupied groups, the
137 row. **Any other two-byte-sharded column with uniform keys pays the 1,325**, so
shard width is a decision to take against the expected occupancy rather than a
default to copy.

## Merge output is handed over first, plain compaction output is not

A hot volume builds its handover queue in seal order and evicts from the front, so
the most recently sealed segment is handed over last. Rewritten rows sealing now
would take the longest residency while genuinely recent segments are evicted ahead of
them, which after one base merge on an accounts volume is the whole eviction order
upside down.

**The merge half is closed.** A merge writes through its own appender at
`tail_count + 1`, past every tail `Reel::route` hands foreground writes to, so its
output is pure rather than interleaved with fresh records. Each output segment is
marked merge output as it is drawn, and `hold_sealed` reads that mark: a promotable
segment goes to the back of the queue with the residency wait on it, merge output to
the front with no wait at all. Merge output is never recent, since a row written
recently lives in a newer run and shadows the merged copy. A hot volume with
everything else inside its window hands over nothing until a merge runs and hands its
output over the moment one does, which is what `merge_output_is_not_promotable`
asserts either side of the pass.

**Plain compaction is not closed, and that is accepted for now, ruled 2026-08-17.** A
rewrite draws its destination from `least_loaded_tail`, which returns the reserved
tail only where `rewrite_on_seal` is set or the volume has capacity tiers. An
ordinary volume has neither, so compaction output shares a foreground tail with fresh
records and seals promotable, and a rewritten segment's keys are handed back to the
resident tier as though they were fresh. A flag on that shared file would demote the
fresh writes riding in it.

## A resident loc is a pointer, so a merge has to repoint it

Not a defect, a cost that was never written down. Paged mode finds a row by fence and
holds no pointer, so a merge repoints nothing. Resident mode holds an exact `Loc` for
every key, so a merge repoints every entry it moves, and hot mode repoints whatever
fraction it still holds. The per-entry cost is a map write, small against the io the
merge is already paying; what it costs is granularity, since each batch of repoints
takes the publish barrier, so a jitter bound on a merge applies to its index side and
not only to its io. The machinery exists, compaction repointing as it copies. What
would be new is the volume.

## Still open

- **A footer search on the write path.** Every put of a key the map does not hold
  asks the footers whether it is an overwrite, which a column with uniform keys asks
  of every segment. The sealed-keys filter answers it for a fresh key and the
  per-segment filters for the rest, and the probe fires only when the map holds
  nothing at all for the key.
- **What "how many blobs are in here" answers.** `totals().count` is a floor during
  the born window, right if every consumer of it is a metric. The live candidates are
  a per-segment live-key count in the footer tail, which `live_rows()` computes at
  seal for nothing and which turns the floor into an estimate that settles, and an
  exact counting walk paid by the caller that asked. Refuted: a cross-segment join at
  open, and counting distinct keys per footer without one, which double-counts
  rewritten keys.
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
- **Benchmarking a paged run honestly.** A backend sweep must tick the handover
  between the fill and the reads and print how many keys answered from a footer, so a
  run that pages nothing says so, and it needs `REEL_BENCH_SEGMENT` below the sweep's
  payload or nothing seals at all.
