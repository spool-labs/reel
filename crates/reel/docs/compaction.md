# Compaction

Two workload shapes behave nothing alike here.

| shape | how records die | where the dead bytes land | how space comes back |
|---|---|---|---|
| cohort | written once, whole families expire together | whole segments | unlink |
| overwrite | the same keys are rewritten | scattered inside live segments | copy the survivors |

Every cheap answer on this page is cheap only for the cohort. Every number has its box and date. "Arithmetic" means arithmetic over a measured rate.

## The pieces

- The compactor does selection, the rewrite, the whole-dead drain, the hole punch and the scrub. The scrub evicts a record that fails its checksum through the same path a failed read uses. When its sweep finishes a segment it settles that segment's dead count, which corrects the estimate an open starts with.
- The pressure model holds the tiers, the maintenance reserve and the rate gates for compaction and the scrub.
- `maintain_once` is one tick: retry broken seals, seal idle tails, publish the footprint, page sealed keys into their footers, sweep covers, prune the graves nothing older can reach, run the compaction passes and a merge when due, page out again, one scrub pass, a spot index scrub and a sweep of walk runs. Each step is bounded and paced, so the caller runs the tick on a timer.
- `compact_passes` passes run at once, one for every two tails and at most 64. Each pass drains the wholly dead segments first, then rewrites one target.

Dead bytes are tracked per segment in `SegmentBytes`. A superseded record moves its bytes from live to dead. The struct also keeps the tombstone footprint and the newest tombstone mark, which price a rewrite that drops them.

The tier comes from the store-wide dead fraction. Below 20% with hot ingest, passes defer. At 20% the plane escalates and lowers the bar to 0.20, and it relaxes below 15%.

## What a pass may retire

`select_target` picks the segment with the highest reclaimable fraction at or above the effective threshold. Four gates come first.

- **The claim.** `in_flight` takes the chosen segment under the lock the choice was made under. Without that, two callers leave with the same segment.
- **The pending cover.** Nothing retires while a cover still owes its sweep, since an unsettled record would leave the shard counters pointing at a segment that is gone. This is checked again after the copy, because a cover can go up mid-pass.
- **The cue floor.** A segment whose oldest mark is at or below the oldest held cue stands. Dead only means no live entry points at those versions, and an older reader still wants them.
- **The rot pin.** A pass that meets a failed checksum on a sole copy leaves the segment standing. Once its live records are copied off it is nearly all dead, so without the pin it would rank first on every tick and never retire. The pin holds at the dead bytes the failed pass left, so anything that dies in the segment later offers it again. `segments_pinned_by_rot` counts these.

**A pass never takes a segment whose spans are in flight.** A seal writes its footer, syncs it, queues its note, and only then drops its hold. `select_ranked` works from copies of `index.ranking()` and `shared.pending_seals()` plus a live `shared.is_held`, so a segment that seals between the queue copy and the walk reaching it looks like a freed tail. Retiring it would leave a note in transit that records spans for a file that is gone. So the walk asks the queue again for the winner alone, before the claim. `a_late_note_cannot_bring_back_a_retired_segment` holds this interleaving, and the seeded stress run hits it about once in 2,300 walks.

**Tombstones.** A rewrite copies a tombstone forward while any other segment can still surface a record it hides, and drops it once none can. The drop floor is the lower of the oldest mark in any other segment and the settled frontier, since a number drawn before the pass can still land after it. A key run that may hold an older version also keeps the tombstone. `Floors` keeps the oldest data mark and the runner-up, so leaving one segment out is a comparison with no walk of the segment table. A copied range tombstone keeps its exclusive end, since the end says how far the delete reached and a rebuild reads it back.

**Retire order.** Flush the destination, seal it when the pass leased a reserved tail, make the index forget the segment, then mark the file doomed. The file unlinks when the last reader drops its handle. The index forgets first because a read of a sealed key picks its segment from a footer search, and the other order can offer a segment that is already unlinked.

## Selection and rewrite cost

Rewriting a segment that is `r` dead copies its `1 - r` live bytes and frees its `r` dead ones, so bytes written per byte reclaimed is `(1 - r) / r`: 0.11 at 0.90 dead, 1.00 at 0.50, 4.00 at 0.20.

**The threshold sets what is allowed. Selection is greedy on the highest reclaimable fraction.** So escalation, which lowers the bar from 0.50 to 0.20, admits more work and leaves each unit's price alone. In the compaction rate sim, under uniform and age-weighted death, the rewritten segments were 0.84 to 0.91 dead under every law, and the three laws landed within noise:

| law | written | end dead |
|---|---|---|
| fixed bar (shipped) | 673 GB | 72.2% |
| rate scaled by segment cost | 735 GB | 72.7% |
| bar never lowered | 650 GB | 72.3% |

The sim models selection and pacing with no io, so read these as relative. Whether greedy selection keeps finding nearly dead segments under an update-heavy mix still needs a soak on a real device.

A segment with no live records retires by unlink. `select_whole_dead` is the same ranking at a ratio of 1.0, and every pass drains it to exhaustion first. It costs almost nothing, since it copies nothing and reads little past the footer. It still runs inside a pass, so a shut gate holds it too.

**The rewrite fetches in offset order and applies in key order.** Only the applies need key order, since that is the order records land in the destination. The two orders meet in a stripe of at most `STRIPE_STAGE_BYTES`, 256 MiB. `SegmentReader` holds one 1 MiB window that only moves forward, so a walk in key order alone would refill it per record wherever the orders disagree: 256x read amplification at 4 KiB records and 4096x at 256 B. `a_reversed_key_order_reads_the_region_once` pins this.

The fetch runs in waves of `FETCH_DEPTH` (8) ranged reads, since one outstanding `pread` cannot overlap its writes. On a ccx33, 2026-08-09, fio read 1 MiB sequential at 3,247 MiB/s at queue depth one and 6,360 at depth eight. The fetch only holds the bytes. The apply checks each record against the index and the purge floor, and `purged_records_are_not_copied` covers the purge side. A segment with no usable footer order is rewritten by walking its records where they lie.

## Pacing

**There is no default cap.** `CompactRate::Auto` is unpaced: the pass runs at device speed while debt sits above the threshold. A cap is for an operator who wants maintenance held below the foreground. **The gate is charged what the device did**, `read_bytes + copied_bytes`, counted at the reader's own refills. So a cap is a read-plus-copy budget, and reclaim per charged byte is `r / (2 - r)`.

What a cap costs the foreground, from the interference probe on a ccx33, 2026-08-16. The volume is 140 GiB with 60 GiB live against 30 GiB of memory, 8 readers on the blocking door, and each loud arm is paired with a quiet one.

| arm | background MB/s | reads/s | p50 us | p99 us | p99.9 us |
|---|---|---|---|---|---|
| quiet | 0 | 77-80k | 119 | 172 | 279-377 |
| paced 40 | 38 | 80,478 | 119 | 188 | 344 |
| paced 100 | 88 | 77,596 | 119 | 221 | 377 |
| paced 200 | 154 | 74,966 | 119 | 279 | 410 |
| paced 400 | 253 | 73,694 | 127 | 311 | 442 |
| unpaced | 689 | 64,323 | 127 | 377 | 557 |

**No cliff.** p50 moves one histogram bucket across the sweep and p99 grows sublinearly. No rate breaks the foreground and none is free, so the cap is a point the operator picks on a measured curve. At that volume's 0.60 dead fraction the charge law returns 0.43 of the cap as reclaim and 0.286 as written bytes, and the run's written share matches to the digit.

**The gate also holds inside a pass.** `PassPace` charges and waits on the gate in steps of what the rate earns in `PACE_STEP`, 5 ms. A fetch wave is one unit the gate cannot interrupt, so a paced pass cuts its wave at the first range that fills a step, and one range of up to 1 MiB is the floor. On the 2026-08-16 run a pass held the device for about 130 ms at every cap, more than an order of magnitude past the 5 ms step. Those tails say a 130 ms stretch does no harm on NVMe at depth. A slower device is unmeasured. An unpaced figure only means something when the volume is bigger than memory.

**Auto does not adapt, because under the default sync setting no in-process signal reflects the device.** A buffered write returns at the page cache, so foreground bandwidth measures memcpy speed. AIMD on achieved rate absorbs compaction's own writes and ratchets to its ceiling. A demand loop off how fast debt forms needs nothing from the device, but an EWMA stable enough to trust is slower than the bursts it must absorb, and it cannot discover the ceiling it must stay under. Sync duration under a syncing setting would work. The bias pass does not benchmark, so a ceiling has to come from a new probe or from config at provisioning.

## The knobs

| knob | default | what it moves |
|---|---|---|
| `compact_mbps` | `auto`, unpaced | background MB/s and read p99, on the curve above. Charged as reads plus copies |
| `compact_dead_ratio` | `0.50` | eligibility only, since selection rewrites 0.84 to 0.91 dead segments at any bar. Escalation lowers it to 0.20 |
| `scrub_mbps` | `64`, clamped to a capped `compact_mbps` | the lap length only, so an integrity sweep never outbids space reclamation. Zero turns the scrub off |

## Reclaiming without copying

A hole punch, a smaller segment and the proposed page chain all reclaim a region only when the whole region is dead. So all three depend on how long the dead stretches inside a segment are. `erase_dead_runs` punches real `fallocate` holes, and a rebuild test shows a punched volume answers every live key after reopen. Nothing schedules it, a caller runs it. It punches on Linux only. Elsewhere it reports what it would punch.

Measured with the erase probe on ext4 on a ccx33, 2026-08-11:

| records | deaths | share of dead bytes the punch returns |
|---|---|---|
| 4 KiB | 60% random | 59.7% |
| 4 KiB | 60% correlated | 66.2% |
| 256 B | 60% | 0.1% |

Random churn leaves geometric runs averaging two and a half records, and those punch. Sub-block records reclaim nothing without copying survivors, at any correlation.

**Dead space is bimodal, which rules out the page chain.** The dead-runs test computes the run distribution from the footers and the index. At every record size except 256 KiB, the share a 4 KiB punch takes is within a point of the share held in runs of a megabyte or more. The garbage is either a multi-megabyte stretch or crumbs below one block, with nothing between. The test's uniform death stride is the pessimistic floor, which is why it under-predicted the real punch by 4x. Where granularity does matter the page has it backwards: at 256 KiB records a block punch takes 98.6% and a page 5.5%.

So a punch is a cheap first tier for columns whose records are a block or larger. It returns most of their dead space with no copying and no format change. Compaction is still the only answer for crumbs and the only thing that restores locality, so the two compose: punch early, rewrite when nearly dead. The page chain never beats a block punch and costs a format change, so it is dropped.

## Cohort and overwrite

A segment dies whole when its records were written at about the same time and die together, since a segment is a stretch of the log. Key adjacency plays no part. The measurement is a sustained churn harness, one 50 GiB run per leg, on a 9950X with a Samsung PM9A3, 2026-08-02:

| leg | copied | rewritten | unlinked whole | amp |
|---|---|---|---|---|
| cohort | 0 MiB | 0 | 106 | 1.01 |
| cohort-1m | 430 MiB | 1 | 25 | 1.01 |
| read4-mapped | 8 MiB | 1 | 31 | 1.01 |
| **overwrite** | **1042 MiB** | **6** | **0** | 1.03 |

Every cohort-shaped leg reclaims by unlink and copies almost nothing. The overwrite leg copied 1042 MiB in 36 s, 30 MB/s against that run's 40 MB/s cap, and unlinked nothing. Averaging three quarters of the cap on a bursty workload means the gate was shutting.

**Routing writes to tails by expected lifetime was built and removed.** Separating a churning key set from a write-once one took an interleaved workload from 0.93 bytes copied per byte reclaimed to 0.02. On the deathtime replay, a column placed by its purge mark rewrote 3.9 to 4.5 times less than arrival order. The shape this engine is built for had nothing to gain: a cohort volume already retires 80 of 81 segments by unlink and copies 0.9 MB against 676 MB reclaimed. Placement cost an open segment per live window and no caller declared a column for it, so every write routes to the least-loaded tail.

**"Low stakes" belongs to the shape.** Columns share segments, so one churning column scatters dead bytes through segments that are otherwise live. A group drop is a cover push plus a range tombstone with no directory unlink, which is the same scattered shape. `CompactionCounters::move_ratio` shows which path a volume is on: the closer to zero, the more of its reclamation was an unlink.

## At 28 TiB

**Nothing in a pass scales with the volume.** `select_target` picks one segment, `compact_segment` rewrites it, and the gate paces the next, so a pass over a 1 GiB segment costs the same at 28 TiB as at 28 GiB. The table is arithmetic over the defaults: 28 TiB is 30.79 TB and 28,672 segments, and a turn at dead fraction `r` charges the whole sweep plus the `1 - r` it copies. The last column uses a ccx33 unpaced, 985 MB/s of reads at amp 1.00 alongside 394 MB/s of copies, 2026-08-10.

| shape | copied | charged | at a 40 MB/s cap | at that ccx33 rate |
|---|---|---|---|---|
| every segment 0.85 dead | 4.6 TB | 35.4 TB | 10.2 days | 7.1 hours |
| every segment 0.50 dead | 15.4 TB | 46.2 TB | 13.4 days | 9.3 hours |
| every segment wholly dead | 0 | 0 | unlink only | unlink only |

The last row is the cohort workload: no copies, no charge, and the gate never shuts. Scattered small records no longer break the measured column, since 4 KiB records ran within 18% of the 1 MiB shape, 806 against 985 MB/s of reads (ccx33, 2026-08-10).

**Four things do scale with the volume, and compaction is the least of them.** Every number here assumes one volume.

1. **The scrub lap.** It is the only one that changes what the volume promises. `for_scrub` clamps `scrub_mbps` to a capped `compact_mbps`, so a volume capped at 40 laps at 40, and 30.79 TB at 40 MB/s is 8.9 days (arithmetic). The cursor lives in the process and `scrub_seed` rotates the start, which only helps while a lap is shorter than the uptime. Past that, integrity coverage is a sampling rate.
2. **The spot index holds a slot per sealed key.** A slot is 16 bytes, 19 to 28 with spare table room. 30.79 TB is 0.6 to 0.8 GB at 1 MiB records, 2.2 to 3.3 GB at 256 KiB and 8.9 to 13.2 GB at 64 KiB, and an open loads all of it.
3. **The tick ranks every segment.** `index.ranking()` builds a vector of every segment under a read lock and reads two atomics per entry to choose one target. Nobody has run it at 28,672 segments.
4. **The reserve is 0.042% of the volume.** `Compactor::new` sizes it at one segment per tail plus one per compaction pass, 12 GiB at the defaults (8 tails, 4 passes). The slowdown band is `SLOWDOWN_RESERVES` (8) reserves, 96 GiB. Inside it `foreground_throttle` slows writers toward a 5% floor before `can_admit_foreground` refuses. On a volume ingesting 500 MB/s that band is about three and a half minutes of runway. It follows segment size and nothing about crossing time.

## Open

- **A new dead run cannot merge with an adjacent old hole.** New deaths merge with each other in one pass. A hole row parses to nothing and its extent is unknown, so an edge block shared by a fresh run and an old hole stays allocated until compaction retires the segment. Closing it needs one of two things verified: exact span arithmetic from a footer row's length plus its key width (which needs the prefix layout confirmed free of padding), or a rule that only the footer speaks for a sealed segment.
- **Whether the scrub needs its own reserve** under compaction pressure is untested.
- **What a cap buys on a slow device.** The curve above is one device, and the 130 ms pass stretch has only NVMe tails behind it.
- **Segment count, separate from byte count.** Almost everything that hurts at 28 TiB comes from 28,672 segments, which a 1 MiB `segment_bytes` reaches on a 28 GiB volume. The open-time probe walks 65, 257 and 1025 segments, so extending it and fitting the curve settles the tick cost. Per-pass invariance is believed and unchecked, and 28,672 files in one directory needs a finding on real hardware.

## Rescuing broken segments

Not built, on purpose. Three of the traps below lose data quietly, so this lands with its own storm campaign or not at all.

A segment marked terminal, and a parked seal whose device keeps refusing, are out of reach of the tick's seal retry. Both stand without a footer, keep every later flush from answering clean, and are walked in full at every reopen. Rescue would be a walk-based twin of the rewrite: a `RecordScan` over the husk, whose records are readable from cache while the process lives. Each record is checked live against the index and appended elsewhere under the same versioned repoint. The traps:

1. **Graves have to be copied forward before any unlink**, by the rewrite's own `should_carry`, or the reopen brings back the versions they covered.
2. **A relocated copy shares its source's sequence number.** A crash before the unlink leaves two rows with one number, and the rebuild may take the husk's and book the wrong segment. The unlink orders after the destination's seal.
3. **Terminal must never be retried by accident.** `seal_segment` skips a terminal segment, and a late seal of a recovered device has to bypass that guard knowingly.
4. **The husk's past-saving count drains at the end**, after the destination flush.

Validation is the seeded stress campaign under contention, since every defect in this family reproduced only there.
