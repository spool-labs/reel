# Configuration: what came out best, and what you have to measure yourself

These are the settings that came out best across the benches and the past
experiments, on four machines with different devices and different record sizes.
Take them as a starting point rather than an answer: every one of them turned on
a working set, a record size or a caller shape that this engine cannot see from
inside, and more than one of them reversed when a box changed. Run your own data
and your own flow through them before trusting a line of it.

## Where to start

| knob | start at | move it when |
|---|---|---|
| `io_backend` | `posix`, which is the default | readers await concurrently, then name `uring` |
| `index` | `resident` | the live key count outgrows the memory you will give it, then `paged` or `hot` |
| `filter_bits` | `10` | never on a resident volume: the seal already spends nothing there |
| `footer_cache` | `64 MiB`, raised until searches stop reading directories | a paged volume holds more sealed footers than the cache does |
| `map_above` | unset | the working set stays resident and the median matters more than the tail |
| `segment_bytes` | `1 GiB` | never downward on a rotational device |
| `active_tails` | `auto`, which is already one tail per fast volume | writers contend on one appender, and then upward only |
| `compact_mbps` | `auto` | foreground p99 has a budget, then pick a cap off the curve below |
| `compact_dead_ratio` | `0.50` | reclaim is not keeping up with the debt |
| `rewrite_on_seal` + `merge_sorted_runs` | both on for a read-heavy volume | a write-only volume that is never searched |
| `merge_dead_ratio` | `0.50` | leave it alone until the merge bounds its output; see the runs section |
| `sync` | whatever the caller promises its own users | never for throughput |

## The read path

### The backend decides who is allowed to have depth

`get_wait` and `get_many_wait` are the awaited door. On posix the read is served
by a `pread` on the calling thread, so an awaited read has no depth to gain and
the door is a wrapper. On the ring an awaited read is a submission, and depth is
the whole point.

Cold 4 KiB reads over a 308 GiB fill past 246 GiB of memory, caches dropped,
measured on a 64-thread EPYC 9375F with a dedicated NVMe, 2026-08:

| backend | door | depth | readers | MB/s | us/read | cpu us/op |
|---|---|---:|---:|---:|---:|---:|
| posix | blocking | 1 | 16 | 1,068 | 3.84 | 7.7 |
| posix | awaited | 8 | 16 | 1,077 | 3.80 | 8.0 |
| uring | blocking | 1 | 16 | 1,092 | 3.75 | 7.7 |
| uring | awaited | 8 | 16 | 4,831 | 0.85 | 16.8 |
| uring | awaited | 32 | 16 | 4,136 | 0.99 | 22.7 |
| posix | blocking | 1 | 32 | 1,965 | 2.08 | 8.9 |
| uring | awaited | 8 | 32 | 4,444 | 0.92 | 41.2 |

**Depth 8 is the knee.** The ring's awaited door at depth 8 reads 4,831 MB/s
cold, 1.18M IOPS, 4.5x the blocking door on the same box. Depth 32 overshoots by
14 percent and pays double the CPU per op. Below the knee the same shape holds at
one reader: depth 8 is 6.3x a lone blocking reader, 531 MB/s against 84.

The posix awaited rows are flat at every depth, and that is not a defect in the
sweep. Depth on posix is fake. If the caller is going to await, `io_backend`
has to name a ring, and it has to name it explicitly.

### Lanes or depth: pick the resource you would rather spend

Both answers are valid, and they cost different things. A cold set of 3,000
point reads against 259M live keys, resident index, on the same box and month:

| backend | door | lanes | set ms | cpu ms |
|---|---|---:|---:|---:|
| posix | blocking | 8 | 28.5 | 35 |
| posix | blocking | 32 | 8.8 | 40 |
| posix | blocking | 64 | 7.1 | 115 |
| uring | blocking | 32 | 8.6 | 51 |
| uring | awaited | 8 | 32.8 | 41 |
| uring | awaited | 32 | 16.0 | 29 |
| uring | awaited | 64 to 256 | 15.2 to 15.3 | 27 to 28 |
| posix | mapped | 16 to 64 | 62 to 63 | 165 to 170 |

Blocking fan-out over 32 to 64 lanes answers the set in 7 to 9 ms and spends up
to 115 ms of CPU doing it. The ring's awaited door floors at 15.3 ms for 28 ms of
CPU: **a quarter of the CPU for 2.2x the latency**. Neither is the tuned answer;
the question is whether the box has cores to burn or a latency to hold.

Nothing in the config sets the lane count or the awaited depth. Both belong to
the caller. What the config decides is whether the depth the caller asks for is
real, which is `io_backend`.

### Mapped reads buy the median and sell the tail

`map_above` is a per-read floor: records at or above it are served from a
read-only mapping of the segment file. Warm, on the same box and month, mapped
point reads won p50 by 13 to 25 percent, 0.96 us to 0.84 and 1.11 us to 0.88,
and **lost p99 by 1.7 to 1.8x**, 1.74 us to 3.15 and 1.90 to 3.13. Cold it loses
outright: the mapped rows in the table above floor at 62 ms against 7.1 ms on
blocking lanes.

So set it where the working set stays resident and the median is what the caller
is judged on. Leave it unset where reads go to the device or where the tail is
the promise. It is refused outright on a `uring_direct` volume, which holds no
page cache for a mapping to read.

### Filters only pay above a footer cache that holds the directory

`filter_bits` is spent at seal, and only on a volume whose index pages:
`seal_filter_bits` returns zero under `IndexResidency::Resident`, because a
column that answers every key from memory never searches a footer. The default of
10 bits per key is the value the sweeps ran on.

What the filter changes, measured on an 8-vCPU EPYC Milan cloud box with 30 GiB
of memory and local NVMe, 2026-08: block reads per search collapse from 63 to
0.7 to 1.7, except for a key present in every standing run, where it is 63 either
way. The cost is one directory read per run. **That is why `footer_cache` is the
knob to set first.** What it names is the ceiling over all of the sealed state a
paged volume holds, divided three ways between the parsed footers, the segment
directories and the row blocks, so the room for footers is a third of it. Size it
so the directory of every sealed segment stays held, and the per-search directory
reads go to zero rather than growing with the run count. On the 259M key run
above, 8 MiB of footers was enough to hold every one of them, and searches per get
read exactly 1.000.

## The index

Two arms of the same volume under the same driver, warm, 10,000 rounds of roughly
180 byte values, single-stream, measured on the 64-thread EPYC 9375F, 2026-08:

| index | writes/s | p50 us | p99 us | reads/get | resident MiB | disk/live |
|---|---:|---:|---:|---:|---:|---:|
| paged, carrying | 828,346 | 531.7 | 10,789.8 | 15.88 | 0.4 | 1.99x |
| resident | 1,023,718 | 70.6 | 134.7 | 0 | 372.2 | 3.30x |

Resident costs 372 MiB against 0.4 MiB and gives back a 7.5x p50 and an 80x p99.
Warm point reads on the same box read 0.96 to 1.14 us resident against 1.5 to
2.3 us paged. Take `resident` while the memory is there, and read the paged
column as what a volume too large for memory pays rather than as an argument
against paging.

That p99 is not all paging, which is the next section: this arm was carrying a
stack of 427 standing runs that never collapsed, at 15.9 asks per get.

Carrying and codecs are per column rather than per volume. `row_carry` puts a
value's leading bytes in the sealed row so a warm point read never reaches the
record; `codec` compresses the payload. With 128 bytes carried, an Lz4 column
held 0.72 GiB against 0.87 GiB raw, 21 percent less disk, and the raw column
answered warm points about 15 percent faster, 1.52 to 1.92 us against 1.79 to
2.26. Disk is the only clean verdict in that pair; the write difference sat
inside the harness drift. `carried_budget` bounds what the whole index carries.

## The write path

**Batch, whatever else you do.** A 1.2 KB-record ingest, warm, single-stream,
same box and month:

| backend | shape | records/s | slot p99 us |
|---|---|---:|---:|
| posix | per record | 985,304 | 1,067 |
| posix | batched | 2,014,619 | 649 |
| uring | per record | 143,120 | 5,704 |
| uring | batched | 2,024,633 | 477 |

Batching is worth 2.0x on the synchronous backend and **14.1x on the ring**,
because per-record ring submissions collapse: one submission per 1.2 KB record
drowns in submission overhead and lands at 6.9x *under* per-record posix. A ring
must be fed batches. Use `write_batch`, or `write_batch_wait` when the caller
awaits. The awaited door adds 2.7 to 3.1 percent over blocking on batched
ingest, consistently across backends but inside noise's neighborhood.

**One tail per fast volume, writers spread across them.** Measured on a
16-thread Ryzen 7 3700X with four 7200 rpm SATA drives, 1 MiB records, batch of
16, `sync` never, 2026-08: three drives with one tail each sustained 674 MB/s
aggregate, about 220 per drive and 82 percent of that drive's read baseline. On
one drive, four tails read 180 MB/s and eight read 173, so multi-stream
interleaving on a single device costs about 20 percent, and eight writers against
a single tail cost about 30 percent. `active_tails` on `auto` floors at one per
fast volume and stops well short of a wide machine's core count, which is the
shape that measured best. Name a number only to raise it.

**`segment_bytes` is already at its plateau.** Same box, one drive, one tail,
three writers, 96 GiB:

| segment_bytes | durable MB/s | syncs per thousand ops |
|---|---:|---:|
| 256 MiB | 204 | 4.2 |
| 1 GiB | 227 | 1.0 |
| 2 GiB | 226 | 0.5 |

Small segments cost about 10 percent on a rotational device, four times the seal
syncs for it, and the default is already flat against 2 GiB. What that run did
not measure is what a larger unit costs a compaction or scrub pass, so raising it
is unmeasured rather than safe.

## Compaction, and the two ratios that compose

### Pacing is a point on a curve, not a cliff

A 140 GiB volume, 60 GiB live against 30 GiB of memory, 8 readers on the
blocking door with compaction as the background copier, on the 8-vCPU cloud box,
2026-08:

| arm | background MB/s | reads/s | p50 us | p99 us | p99.9 us |
|---|---:|---:|---:|---:|---:|
| quiet, no background | 0 | 77 to 80k | 119 | 172 | 279 to 377 |
| cap 40 | 38 | 80,478 | 119 | 188 | 344 |
| cap 100 | 88 | 77,596 | 119 | 221 | 377 |
| cap 200 | 154 | 74,966 | 119 | 279 | 410 |
| cap 400 | 253 | 73,694 | 127 | 311 | 442 |
| `auto`, unpaced | 689 | 64,323 | 127 | 377 | 557 |

**There is no cliff to avoid.** Foreground p50 moves one histogram bucket across
the whole sweep, and p99 grows sublinearly with the background rate. There is
also no rate that is free. A 100 GB rewrite is about 88 minutes at a cap of 40
for 9 percent on p99, or about 13 minutes near unpaced for 81 percent. Pick the
point, do not look for the safe setting.

Two things to hold while picking. The cap is device traffic, read plus write, so
a pass that reads what it retires and writes the survivors charges roughly twice
what it reclaims: divide before sizing a cap against a deadline. And greedy
selection multiplies the budget in the other direction, because segments are
picked when they are mostly dead, so reclaimed bytes run ahead of copied bytes.

### The defaults do not collapse the runs, and that is the one to watch

`compact_dead_ratio` and `merge_dead_ratio` are separate triggers over the same
bytes, and they compose. In the control run above, with both at their 0.50
defaults: **427 standing runs and zero merges.** Reclaim at 0.50 keeps the
standing stack's dead share under the collapse trigger, so the collapse never
fires, and the paged arm paid for it at 15.9 reads per get and a 10.8 ms p99.

Asks per get equal the standing run count for any key that misses, because a
segment number is not a version and every candidate has to be asked. So the
symptom is reader-visible before it is fatal: `ReelStore::filter_probes` reports
asks per get, and `sorted_run_dead_ratio` reports the share the trigger is being
compared against.

Do not lower `merge_dead_ratio` on a carrying paged volume yet. Measured on a
64-thread EPYC, 2026-08: a merge pass whose rows carry lists them into one
output segment that never rolls, so the pass mints a segment bounded only by
the live set, and the maintenance tick then re-parses that footer forever. At
six million live keys the cell crawled at under a megabyte a second and never
finished. A ratio that fires the collapse currently buys fewer runs and a
better median at the price of worse asks per get and an unbounded segment.
Until the merge bounds its output, `0.50` (which in practice never fires) is
the shipped default, and the collapse is a knob to leave alone.

`merge_sorted_runs` requires `rewrite_on_seal` and is refused without it. A
volume that does not seal by rewriting produces nothing sorted to merge.

## What the config cannot choose for you

Three caller-side shapes moved more than any knob in this file, on the
64-thread EPYC 9375F, single-threaded and warm, 2026-08.

**Range deletes are flat where per-key deletes scale.** Over 16 KiB records,
`delete_range` cost 8.31 us at 1,000 keys, 8.39 us at 10,000 and 7.55 us at
50,000, against per-key loops of 2.41 ms, 20.73 ms and 104.33 ms. The range path
does not care how many keys it covers.

**Keys-only scans never read a payload.** `iter_keys_prefix` and `count_prefix`
answer from the index:

| records | size | value-reading | keys-only |
|---:|---|---:|---:|
| 25,000 | 16 KiB | 30.50 ms | 1.05 ms |
| 100,000 | 16 KiB | 136.26 ms | 4.21 ms |
| 25,000 | 64 KiB | 98.48 ms | 1.05 ms |
| 8,000 | 256 KiB | 101.78 ms | 338.75 us |

**Batch shape is a property of the calling code**, and the write table above
prices it. No pass at open and no loop while running can choose it for the
caller, which is why it is the largest single lever the config does not hold.

## Two knobs that are promises rather than dials

`sync` decides what a crash may cost. Fitting it to a device trades somebody
else's guarantee for throughput, so set it from what the caller promises its own
users and leave it there. `repair` says the same thing about corruption: `Peers`
turns a failed checksum into a miss because another copy exists, and `None` turns
it into an error because nothing else holds those bytes. Neither is a tuning
choice.

`preallocate`, `map_above` and `ranged_reads` are also the three knobs the bias
pass has an opinion about. It reads the box at open, logs what it would choose and
changes nothing, so a disagreement between the log line and the config is a
question for an operator rather than an override. `servo.md` records the rules.

## Before you trust any of this

Every number above came off a bench or a past experiment on somebody else's
hardware, with a record size, a working set and a concurrency that are almost
certainly not yours. The findings that reversed between boxes reversed hard: the
mapped plane wins warm and loses cold by 9x, the ring wins cold reads by 4.5x and
loses per-record writes by 6.9x, and a pair of defaults that each look reasonable
alone leave 427 runs standing when set together. Reproduce the two or three that
would decide your deployment against your own data and your own flow, and change
the defaults on what you measure rather than on what is written here.
