# Compaction

Two workload shapes run through everything below and behave nothing alike. **The
cohort shape** writes a record once and expires whole families together, so its dead
bytes arrive in whole segments. **The overwrite shape** rewrites the same keys, so
its dead bytes land scattered inside segments that are otherwise live, and every
cheap answer here is cheap only for the cohort. Every number carries the box and the
date it was taken on; anything called arithmetic is arithmetic over a measured rate
rather than a measured cell.

## The pieces

- `compaction/compactor.rs`: selection, the rewrite, the whole-dead drain, the hole
  punch, and the scrub. The scrub evicts a record failing its checksum through the
  same path a read-time failure uses, and settles a segment's dead count when its
  sweep completes, which is how a paged open's optimistic accounting is trued up.
- `compaction/pressure.rs`: the tier model, the maintenance reserve, and the rate
  gates for compaction and the scrub.
- `engine/maintain.rs::maintain_once`, one tick: retry failed seals, publish the
  footprint, page sealed keys into their footers, sweep covers, prune the graves
  nothing older can reach, shed carried rows, one compaction pass, one merge when due,
  one scrub pass. Each is bounded and paced, so the caller drives this on a timer.

Dead bytes are tracked per segment as `SegmentBytes`, moved live to dead when a
record is superseded, plus the tombstone footprint and newest mark that price a
rewrite dropping them.

## What a pass may and may not retire

`select_target` picks the segment with the highest reclaimable fraction at or above
the effective threshold. Four gates stand in front of it.

- **The claim.** `in_flight` takes what the walk chose, under the lock the choice was
  made under, or two callers leave with the same segment.
- **The pending cover.** Nothing retires while a cover owes its sweep: an unsettled
  record leaves the shard counters holding a segment that is gone. Checked again
  after the copy, since a cover can go up mid-pass.
- **The cue floor.** Dead means no live entry points at those versions, which is
  what an older reader came for. A segment whose oldest mark sits at or below the
  floor stands.
- **The rot pin.** A pass meeting a failed checksum on a sole copy leaves its
  segment standing, and that segment stands nearly all dead once the live records
  are copied off, so unpinned it ranks highest and is chosen every tick for a file
  that can never retire. The pin is against the dead bytes the failed pass left, so
  anything dying in the segment afterwards offers it again, and
  `segments_pinned_by_rot` counts what stands for this reason.

**A pass may not take a segment whose spans are in flight.** `select_ranked` works
from two copies, `index.ranking()` and `shared.pending_seals()`, plus one live read,
`shared.is_held`. A seal writes its footer, syncs it, queues its note, and only then
gives up the hold, so a segment sealing between the queue copy and the walk reaching
it reads as a tail just freed: in the ranking, absent from the copied queue, held by
nobody. Taking it retires a file whose note is still travelling, and that note then
records spans for a file that is gone. The guard is one more live read, the queue
asked again for the winner alone before the claim.
`a_late_note_cannot_bring_back_a_retired_segment` in `tests/rendezvous_races.rs`
holds the interleaving; the seeded stresser hits it about once in 2,300 walks.

**A tombstone is carried while any other segment can still surface a record it hides,
and dropped once none can.** `Floors` keeps the oldest data mark and its runner-up,
so one exclusion is a comparison rather than a walk of the segment table. A range
tombstone carries its exclusive end, since the end says how far the delete reached
and a rebuild has to read it back.

**Retire order.** Flush the destination, seal it, have the index forget the segment,
then mark the file doomed; it unlinks when the last reader's handle drops. The index
stops naming a segment before the file goes, not after: a paged read chooses its
segment from a footer search, and the other order offers one already unlinked.

## Selection and what a rewrite costs

Rewriting a segment that is `r` dead copies its `1 - r` live bytes and frees its `r`
dead ones, so bytes written per byte reclaimed is `(1 - r) / r`: 0.11 at 0.90 dead,
1.00 at 0.50, 4.00 at 0.20.

**The threshold decides what is allowed and never what is chosen**, because
selection is greedy on the highest reclaimable fraction. Escalation lowering the bar
from 0.50 to 0.20 admits more work rather than making each unit dearer. Measured by
`tests/compaction/compact_rate_sim.rs` under uniform and age-weighted death, the
segments actually rewritten are 0.84 to 0.91 dead whatever the law, and the three
candidate laws land within noise of each other: the shipped fixed bar, a rate scaled
by segment cost and a bar never lowered write 673, 735 and 650 GB for 72.2%, 72.7%
and 72.3% end dead. The sim models selection and pacing, not io, so read those as
relative. Whether greedy selection keeps finding nearly dead segments under an
update-heavy mix is what a soak on a real device still has to say.

A segment with no live records retires by unlink instead. `select_whole_dead` is the
same ranking with no fallback, drained to exhaustion at the head of every pass and
charging near nothing, since it copies nothing and reads little past the footer. It
still runs inside a pass, so a shut gate holds it with everything else.
`select_unsorted` is the other reason to rewrite, order rather than space, gated
behind `rewrite_on_seal`.

**The rewrite fetches in offset order and applies in key order**, since only the
applies need the footer's order, the two meeting in a stripe bounded at
`STRIPE_STAGE_BYTES`, 256 MiB. That split is load bearing: `SegmentReader` holds one
1 MiB window that only moves forward, so a walk in key order alone refills it per
record wherever the orders disagree, 256x read amplification at 4 KiB records and
4096x at 256 B. `a_reversed_key_order_reads_the_region_once` pins it. The fetch runs
in waves of `FETCH_DEPTH` because one outstanding `pread` cannot overlap its writes
(ccx33, 2026-08-09: fio put 1 MiB sequential reads at 3,247 MiB/s at queue depth one
against 6,360 at depth eight), and its liveness answer travels to the apply in a
`Staged` carrier that must keep dead apart from purged, which
`purged_records_are_not_copied` catches.

## Pacing

**There is no default cap.** `CompactRate::Auto` is unpaced: the pass runs at device
speed while there is debt above the threshold, and a cap exists for the operator who
wants maintenance held below the foreground. **The gate is charged what the device
did**, `read_bytes + copied_bytes`, counted at the reader's own refills rather than
modelled, so a named cap is a read-plus-copy budget and reclaim per charged byte is
`r / (2 - r)`.

What a cap costs the foreground, measured on a ccx33, 2026-08-16
(`tests/probes/interference.rs`): 140 GiB volume, 60 GiB live against 30 GiB of
memory, 8 readers on the blocking door, every loud arm paired with a quiet one.

| arm | background MB/s | reads/s | p50 us | p99 us | p99.9 us |
|---|---|---|---|---|---|
| quiet | 0 | 77-80k | 119 | 172 | 279-377 |
| paced 40 | 38 | 80,478 | 119 | 188 | 344 |
| paced 100 | 88 | 77,596 | 119 | 221 | 377 |
| paced 200 | 154 | 74,966 | 119 | 279 | 410 |
| paced 400 | 253 | 73,694 | 127 | 311 | 442 |
| unpaced | 689 | 64,323 | 127 | 377 | 557 |

**No cliff.** p50 moves one histogram bucket across the sweep and p99 grows
sublinearly. No rate breaks the foreground and none is free, so the cap is a point
the operator picks on a measured curve. At that volume's 0.60 dead fraction the
charge law returns 0.43 of the cap as reclaim and 0.286 as written bytes, which the
run's written share fits to the digit.

**The gate binds on average, not inside one pass.** `PassPace` charges and consults
the gate as the copy runs, in steps of what the rate earns in `PACE_STEP`, 5 ms. But
a pass is one whole segment, about 130 ms of device ownership at every cap (ccx33,
2026-08-16), so the 5 ms unit is missed by more than an order of magnitude. Those
tails say a 130 ms pass is not destructive on NVMe at depth; on a slower device it
is unmeasured. And an unpaced figure is not quotable unless the volume is bigger
than memory, which is why `compact_throughput` sizes its volume from `MemTotal` plus
a quarter.

**Auto is not adaptive because no in-process signal reflects the device under the
default sync policy**, where a buffered write returns at the page cache. Observed
foreground bandwidth measures memcpy speed; AIMD on achieved rate absorbs
compaction's own writes and ratchets to its ceiling; a demand loop off how fast debt
forms needs nothing from the device, but an EWMA stable enough to trust is slower
than the bursts it must absorb. Sync duration under a syncing policy would work. A
demand loop is the second half of the job anyway, since it cannot discover a ceiling
it may never exceed, and there is no bandwidth probe to derive one from; `servo.md`
rules out the bias pass benchmarking for it. A ceiling comes from a new probe or
from config at provisioning.

## The knobs

| knob | default | what measurably moves |
|---|---|---|
| `compact_mbps` | `auto`, unpaced | background MB/s and read p99, both on the curve above (ccx33, 2026-08-16). Charged reads plus copies |
| `compact_dead_ratio` | `0.50` | eligibility, not choice: selection rewrites 0.84 to 0.91 dead segments whatever the bar (sim). Escalation lowers it to 0.20 under debt |
| `merge_dead_ratio` | `0.50` | leave it alone. A merge pass whose rows carry mints one output segment that never rolls (64-thread EPYC, 2026-08); `config.md` carries the run |
| `scrub_mbps` | `64`, clamped to `compact_mbps` unless that is unpaced | the lap length, nothing else. An integrity sweep cannot outbid space reclamation |

## Reclaiming without copying

A hole punch, a smaller segment and the proposed page chain all reclaim a region only
when the whole region is dead, so all three live or die on how long the contiguous
dead stretches inside a segment are. `erase_dead_runs` punches real `fallocate`
holes, with a rebuild test proving a punched volume answers every live key after
reopen. **Measured on ext4 on a ccx33, 2026-08-11** (`tests/probes/erase_probe.rs`):
at 4 KiB records and 60% random death the punch returns 59.7% of the dead bytes,
66.2% when the deaths are correlated, and 0.1% at 256 B. Random churn leaves
geometric runs averaging two and a half records and those punch; sub-block records
reclaim nothing without copying survivors, at any correlation.

**Dead space is bimodal, which is what kills the page.** `tests/engine/dead_runs.rs`
computes the run distribution off the footers and the index. On every record size but
256 KiB the share a 4 KiB punch takes lands within a point of the share held in runs
of a megabyte or more: the garbage is either a multi-megabyte stretch or crumbs below
one block, nothing between. Its uniform stride is the pessimistic floor, which is why
it under-predicted the real punch by 4x. Where granularity does matter the proposed
page has it backwards: at 256 KiB records a block punch takes 98.6% and a page 5.5%.

**Verdict.** A punch is a cheap first tier for columns whose records are a block or
larger, returning most of their dead space with no copying and no format change.
Compaction remains the only answer for crumbs and the only thing that restores
locality, so the two compose: punch early, rewrite when nearly dead. The page chain
never beats a block punch and costs a format change, so it is refuted with them.

## Cohort against overwrite

For a cohort volume reclamation is an unlink. What makes a segment die whole is not
that its dead keys are adjacent but that they were **written** at about the same
time, since a segment is a stretch of the log rather than of the key space. That much
is reasoning; the measurement is a sustained churn harness, one 50 GiB run per leg,
on a 9950X with a Samsung PM9A3, 2026-08-02:

| leg | copied | rewritten | unlinked whole | amp |
|---|---|---|---|---|
| cohort | 0 MiB | 0 | 106 | 1.01 |
| cohort-1m | 430 MiB | 1 | 25 | 1.01 |
| read4-mapped | 8 MiB | 1 | 31 | 1.01 |
| **overwrite** | **1042 MiB** | **6** | **0** | 1.03 |

Every cohort-shaped leg reclaims by unlink and copies almost nothing. The overwrite
leg copied 1042 MiB in 36 s, 30 MB/s of copies against the 40 MB/s cap that run
named, and reclaimed nothing by unlink at all: an average at three quarters of the
ceiling on a bursty workload means the gate was shutting.

**Routing writes to their own tails by expected lifetime was refused as a column
setting and shipped as an argument on the write.** Separating a churning key set from
a write-once one takes an interleaved workload from 0.93 bytes copied per byte
reclaimed to 0.02, but the shape this engine is built for has nothing to give up: a
cohort volume already retires 80 of 81 segments by unlink and copies 0.9 MB against
676 MB reclaimed, and banding it only splits the tails and the segments finer. So the
band is not a `ColumnSpec` field every caller pays for. A caller with mixed lifetimes
names a death window per write and gets the placement; a caller without names nothing
and routes exactly as before. What naming one is worth is measured below.

**So "low stakes" is a property of the shape, not of the engine.** Columns share
segments, so one column that churns scatters dead bytes inside segments otherwise
live, and a group drop is a cover push plus a range tombstone rather than a directory
unlink, which is that same scattered shape. `CompactionCounters::move_ratio` says
which path a volume is on: the closer to zero, the more of its reclamation was an
unlink.

## Placement bands

**A band is the caller's own death window, and the whole mechanism is that the engine
keeps one to its own tail.** `put_banded`, `put_owned_banded` and
`apply_batch_banded` carry one; `route` sends every write naming it to the tail that
band is on, claiming a tail from the pool on the first write and giving it back at
`release_band`. The pool is the tail count already configured, never more: bands do
not add tails. One tail is always left unclaimed, so traffic naming no band is never
mixed into a banded segment, and a band that finds no tail free writes there too
rather than stalling, which `band_fallbacks` counts. The band goes into the segment
header at the draw, so a segment says what it was drawn for; `compact_segment` reads
it off the file and routes that segment's survivors to the same band's tail. Both
halves are required: the paper (Lee, Ziegler, Leis, VLDB'26, section 4) is explicit
that placement whose GC is not band-aware intermixes back to the baseline.
`tests/engine/bands.rs` holds what the win rests on: a segment carries one band's
records and no others, and a rewrite puts the survivors back under the same one.

The workload is the deathtime campaign's, replayed against the mechanism instead of a
model of it: one volume, 9,448 B coded slices under a 34 B key, lifetimes log-uniform
over 2..256 epochs weighted young, per-key tombstones at expiry, compaction driven to
a fixpoint and every tail cued at every epoch boundary in every arm. **A** is arrival
order on one tail, what the engine did before. **T** is arrival order on the band
run's tail count, so a win reads as placement rather than as having drawn more tails.
**E** hands each record its death window and nothing else. macOS, 2026-08-29.

| run | shape | A rewritten / waf | T | E | E vs A |
|---|---|---|---|---|---|
| R2 | 8 MiB seg, 20 ep, 800 MiB | 427 MiB / 1.532 | 427 / 1.532 | **106 / 1.132** | 4.0x |
| R5 | 32 MiB seg, 20 ep | 478 MiB / 1.596 | 478 / 1.596 | **106 / 1.132** | 4.5x |
| R7 | 8 MiB seg, 40 ep, 1600 MiB | 1348 MiB / 1.840 | 1348 / 1.840 | **303 / 1.189** | 4.5x |
| R8 | 37,648 B slice, 8 MiB seg | 411 MiB / 1.513 | 411 / 1.513 | **105 / 1.131** | 3.9x |

T is A to the byte in all four, so tails alone move nothing: the tail a record goes to
is what moves it. E also beats the caller-side buffering it replaces, which reached
119 / 1.148 on R2 and 332 / 1.207 on R7 by holding 32 MiB of writes back; E holds
none. Move ratio falls from 1.000 to 0.53, so half of what A could only rewrite is now
an unlink.

**What it costs is open segments.** The pool wants a tail per window that is live at
once, 14 in this workload, and below that bands fall back: R2 at 7 banded tails is
147 MiB rather than 106. Handing a closed window back matters as much as the count.
Without `release_band` the same run needs 24 tails to reach 106 MiB, because a tail is
only taken from a band that has gone quiet since the last claim. Fifteen tails cued
every epoch also leave fifteen partial segments an epoch, which is why E's files run
253 against A's 66 and its peak footprint 916 MiB against 521; T's 346 files and
1109 MiB say that is the tail count and the cue, not the placement. A write-only probe
over 400 MiB says banded tails still fill their segments: median sealed size 8.0 MiB
of an 8 MiB segment at 8 and at 16 tails, the same as unbanded, and the interleave
index sits at the tail count exactly (14.98 mean, 15.05 worst at 16 tails) where
least-loaded routing spreads it (6.72 mean, 16.33 worst at 8).

## What 28 TiB costs

**Nothing in a pass scales with the volume.** `select_target` picks one segment,
`compact_segment` rewrites it, the gate paces the next, so a pass over a 1 GiB
segment costs the same at 28 TiB as at 28 GiB. What follows is arithmetic over the
defaults: 28 TiB is 30.79 TB and 28,672 segments, and a turn at dead fraction `r`
charges the whole sweep plus the `1 - r` it copies. The measured column is a ccx33
unpaced, 985 MB/s of reads at amp 1.00 carrying 394 MB/s of copies, 2026-08-10.

| shape | copied | charged | at a named 40 MB/s | at that ccx33 rate |
|---|---|---|---|---|
| every segment 0.85 dead | 4.6 TB | 35.4 TB | 10.2 days | 7.1 hours |
| every segment 0.50 dead | 15.4 TB | 46.2 TB | 13.4 days | 9.3 hours |
| every segment wholly dead | 0 | 0 | unlink only | unlink only |

The last row is the cohort workload: no copies, no charge, the gate never shuts. The
scattered small-record shape no longer breaks the measured column, since 4 KiB records
ran within 18% of the 1 MiB shape, 806 against 985 MB/s of reads (ccx33, 2026-08-10).

**Four things scale with the volume, and compaction is the least of them.**

1. **The scrub lap**, the only one that changes what the volume promises rather than
   what it costs. `for_scrub` clamps `scrub_mbps` to `compact_mbps`, so a volume
   capped at 40 laps at 40, and 30.79 TB at 40 MB/s is 8.9 days: arithmetic from the
   rate, not a measured lap. The cursor is process-local and `scrub_seed` rotates the
   start, but rotation only mitigates a lap shorter than the uptime. Past that,
   integrity coverage is a sampling rate.
2. **The resident index does not fit.** 95 bytes a key is 2.8 GB at 1 MiB records,
   11.2 GB at 256 KiB, 44.6 GB at 64 KiB, and `IndexResidency::Resident` is still
   the default. `Paged` holds about a byte a key, `Hot` a budget; `index-tier.md`.
3. **The tick sweeps every segment.** `index.ranking()` allocates a vector of every
   segment under a read lock and folds two atomics per entry, to choose one target.
   `select_unsorted` calls it again and asks the memoized footer facts per
   segment until one is out of order; a fact is derived from the footer once at
   first ask and never re-read for a sealed segment. Gated behind
   `rewrite_on_seal`; neither sweep has been run at 28,672 segments.
4. **The reserve is 0.031% of the volume.** `Compactor::new` sizes it as one segment
   per tail plus one, 9 GiB at the defaults, and the slowdown band is
   `SLOWDOWN_RESERVES` of them, 72 GiB, inside which `foreground_throttle` slows
   writers toward a 5% floor before `can_admit_foreground` refuses. On a volume
   ingesting 500 MB/s that band is two and a half minutes of runway: sized to follow
   segment size, following nothing about crossing time.

560 TiB across twenty devices is `volumes.md`; every number here assumes one volume.

## Open

- **A new dead run cannot merge with an adjacent existing hole.** New deaths merge
  with each other in one pass, but a hole row parses to nothing and its extent is
  unknown, so an edge block shared between a fresh run and an existing hole stays
  allocated until compaction retires the segment. Closing it needs one of two things
  verified: exact span arithmetic from a footer row's length plus its key width, which
  needs the prefix layout confirmed padding free, or a rule that nothing but the
  footer speaks for a sealed segment.
- **Whether the scrub needs its own reserve** under compaction pressure is untested.
- **What a cap buys on a slow device.** The curve above is one device, and the
  130 ms pass granularity has only NVMe tails behind it.
- **Segment count, which is separable from byte count.** Almost everything that
  hurts at 28 TiB hurts because of 28,672 segments, which `segment_bytes` at 1 MiB
  reaches on a 28 GiB volume. `tests/probes/open_time.rs` walks 65, 257 and 1025, so
  extending it and fitting the curve settles the tick cost. Per-pass invariance is
  believed rather than checked, and 28,672 files in one directory is a finding on
  real hardware or it is nothing.

## Rescue of broken segments

Unbuilt on purpose: three of the traps below lose data quietly, so this lands with
its own storm campaign or not at all. A segment marked terminal, and a parked seal
whose device keeps refusing, are what the tick's seal retry cannot touch: both stand
footerless, hold the volume's flushes refused, and are walked in full at every reopen.
Rescue is a walk-based twin of the rewrite, `walk_records` over the husk whose records
are readable from cache while the process lives, each record checked live against the
index and appended relocated under the same versioned repoint. The traps:

1. **Graves must be carried before any unlink**, by the rewrite's own
   `should_carry`, or the reopen resurrects the versions they covered.
2. **A relocated copy shares its source's sequence number**, so a crash before the
   unlink leaves two rows with one number and the rebuild may take the husk's, which
   books the wrong segment. The unlink orders after the destination's seal.
3. **Terminal is not retryable by accident.** `seal_segment` skips a terminal
   segment, and a late seal of a recovered device must bypass that guard knowingly.
4. **The husk's past-saving count drains at the end**, after the destination flush.

Validation is the seeded-stress campaign under contention, since every defect in
this family reproduced only there.
