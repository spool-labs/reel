# Configuration

These settings came out best across the benches and past experiments, on four machines with different devices and record sizes. Treat them as a starting point. Every one turned on a working set, a record size or a caller shape the engine cannot see from inside, and more than one reversed when the box changed. Run your own data and your own flow through them before trusting any line.

## Where to start

| knob | start at | move it when |
|---|---|---|
| `io_backend` | `posix`, the default | readers await concurrently, then set `uring` |
| `filter_bits` | `10` | rarely: set `footer_cache` first |
| `footer_cache` | `64 MiB`, raised until searches stop reading directories | the volume holds more sealed footers than the cache does |
| `map_above` | unset | the working set stays resident and the median matters more than the tail |
| `segment_bytes` | `1 GiB` | never downward on a rotational device |
| `active_tails` | `auto`: the core count, capped at 8 and at least one per fast volume | writers contend on one appender, and then upward only |
| `compact_mbps` | `auto` | foreground p99 has a budget, then pick a cap off the curve below |
| `compact_dead_ratio` | `0.50` | reclaim is not keeping up with the debt |
| `sync` | whatever the caller promises its own users | never for throughput |

## The read path

### The backend decides who gets depth

`get_wait` and `get_many_wait` are the awaited door. On posix the read is a `pread` on the calling thread, so an awaited read gains no depth and the door is a wrapper. On the ring an awaited read is a submission, and depth is the whole point.

Cold 4 KiB reads over a 308 GiB fill past 246 GiB of memory, caches dropped, on a 64-thread EPYC 9375F with a dedicated NVMe, 2026-08:

| backend | door | depth | readers | MB/s | us/read | cpu us/op |
|---|---|---:|---:|---:|---:|---:|
| posix | blocking | 1 | 16 | 1,068 | 3.84 | 7.7 |
| posix | awaited | 8 | 16 | 1,077 | 3.80 | 8.0 |
| uring | blocking | 1 | 16 | 1,092 | 3.75 | 7.7 |
| uring | awaited | 8 | 16 | 4,831 | 0.85 | 16.8 |
| uring | awaited | 32 | 16 | 4,136 | 0.99 | 22.7 |
| posix | blocking | 1 | 32 | 1,965 | 2.08 | 8.9 |
| uring | awaited | 8 | 32 | 4,444 | 0.92 | 41.2 |

**Depth 8 is the knee.** The ring's awaited door at depth 8 reads 4,831 MB/s cold, 1.18M IOPS, 4.5x the blocking door on the same box. Depth 32 reads 14 percent less and pays double the CPU per op. The same shape holds at one reader: depth 8 is 6.3x a lone blocking reader, 531 MB/s against 84.

The posix awaited rows stay flat at every depth, and the sweep is right about that: depth on posix is fake. If the caller is going to await, `io_backend` has to be set to a ring, explicitly.

### Lanes or depth

Both answers are valid, and they spend different things. A cold set of 3,000 point reads against 259M live keys, on the same box and month:

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

Blocking fan-out over 32 to 64 lanes answers the set in 7 to 9 ms and spends up to 115 ms of CPU doing it. The ring's awaited door floors at 15.3 ms for 28 ms of CPU: **a quarter of the CPU for 2.2x the latency**. Neither is the tuned answer. It depends on whether the box has cores to burn or a latency to hold.

Nothing in the config sets the lane count or the awaited depth. Both belong to the caller. The config decides whether the depth the caller asks for is real, and that is `io_backend`.

### Mapped reads buy the median and sell the tail

`map_above` is a per-read floor: records at or above it are served from a read-only mapping of the segment file. Warm, on the same box and month, mapped point reads won p50 by 13 to 25 percent (0.96 us to 0.84, 1.11 us to 0.88) and **lost p99 by 1.7 to 1.8x** (1.74 us to 3.15, 1.90 to 3.13). Cold they lose outright: the mapped rows in the table above floor at 62 ms against 7.1 ms on blocking lanes.

Set it where the working set stays resident and the caller cares most about the median. Leave it unset where reads go to the device or where the tail is the promise. A `uring_direct` volume refuses it, since there is no page cache for a mapping to read.

### Filters pay only once the footer cache holds the directories

`filter_bits` is spent at seal. The default of 10 bits per key is what the sweeps ran on.

What the filter changes, on an 8-vCPU EPYC Milan cloud box with 30 GiB of memory and local NVMe, 2026-08: block reads per search fall from 63 to between 0.7 and 1.7, except for a key present in every standing run, where it is 63 either way. The cost is one directory read per run. **That is why `footer_cache` is the knob to set first.** It caps all the sealed state the volume holds, split three ways between parsed footers, segment directories and row blocks, so footers get a third of it. Size it so the directory of every sealed segment stays held. Then per-search directory reads go to zero and stop growing with the run count. On the 259M-key run above, 8 MiB of footers held every one of them, and searches per get read exactly 1.000.

## The index

There is no knob for it. A sealed segment's keys stay in its footer, memory holds the open tails' keys and what it takes to find the rest, and a get asks the spot index first, which places a sealed key's record for one device read.

## The write path

**Batch, whatever else you do.** A 1.2 KB-record ingest, warm, single stream, same box and month:

| backend | shape | records/s | slot p99 us |
|---|---|---:|---:|
| posix | per record | 985,304 | 1,067 |
| posix | batched | 2,014,619 | 649 |
| uring | per record | 143,120 | 5,704 |
| uring | batched | 2,024,633 | 477 |

Batching is worth 2.0x on the synchronous backend and **14.1x on the ring**. One ring submission per 1.2 KB record drowns in submission overhead and lands 6.9x *under* per-record posix. A ring must be fed batches: use `write_batch`, or `write_batch_wait` when the caller awaits. The awaited door adds 2.7 to 3.1 percent over blocking on batched ingest, consistent across backends and close to noise.

**One tail per fast volume, writers spread across them.** On a 16-thread Ryzen 7 3700X with four 7200 rpm SATA drives, 1 MiB records, batches of 16, `sync` at `Never`, 2026-08: three drives with one tail each sustained 674 MB/s together, about 220 per drive and 82 percent of that drive's read baseline. On one drive, four tails read 180 MB/s and eight read 173, so interleaving several streams on one device costs about 20 percent, and eight writers against a single tail cost about 30 percent. `active_tails` on `auto` floors at one per fast volume and stops at 8 however wide the machine is, which is the shape that measured best. Set a number only to raise it.

**`segment_bytes` is already at its plateau.** Same box, one drive, one tail, three writers, 96 GiB:

| segment_bytes | durable MB/s | syncs per thousand ops |
|---|---:|---:|
| 256 MiB | 204 | 4.2 |
| 1 GiB | 227 | 1.0 |
| 2 GiB | 226 | 0.5 |

Small segments cost about 10 percent on a rotational device, with four times the seal syncs, and the default is already level with 2 GiB. That run did not measure what a larger unit costs a compaction or scrub pass, so raising it is untested.

## Compaction

### Pacing is a point on a curve

A 140 GiB volume, 60 GiB live against 30 GiB of memory, 8 readers on the blocking door with compaction as the background copier, on the 8-vCPU cloud box, 2026-08:

| arm | background MB/s | reads/s | p50 us | p99 us | p99.9 us |
|---|---:|---:|---:|---:|---:|
| quiet, no background | 0 | 77 to 80k | 119 | 172 | 279 to 377 |
| cap 40 | 38 | 80,478 | 119 | 188 | 344 |
| cap 100 | 88 | 77,596 | 119 | 221 | 377 |
| cap 200 | 154 | 74,966 | 119 | 279 | 410 |
| cap 400 | 253 | 73,694 | 127 | 311 | 442 |
| `auto`, unpaced | 689 | 64,323 | 127 | 377 | 557 |

**There is no cliff to avoid.** Foreground p50 moves one histogram bucket across the whole sweep, and p99 grows sublinearly with the background rate. No rate is free either. A 100 GB rewrite takes about 88 minutes at a cap of 40 for 9 percent on p99, or about 13 minutes at a cap of 400 for 81 percent. Pick the point. There is no safe setting to find.

Two things to hold while picking. The cap is device traffic, read plus write: a pass reads the segment it retires and writes the survivors, so at dead fraction `r` it charges `(2 - r) / r` bytes per byte reclaimed. Divide by that before sizing a cap against a deadline. Greedy selection keeps `r` high, since segments are picked when they are mostly dead, so reclaimed bytes run close behind charged bytes.

### A walk merges at most eight runs

A volume needs no merge knob. Once more than eight runs stand over one key, the maintenance tick merges the walk's runs into a key run, and a walk reads it in place of the footers it covers. `merge_when_due` runs the same merge for a caller.

## What the config cannot choose for you

Three caller-side shapes moved more than any knob on this page, on the 64-thread EPYC 9375F, single-threaded and warm, 2026-08.

**Range deletes are flat where per-key deletes scale.** Over 16 KiB records:

| keys | `delete_range` | per-key loop |
|---:|---:|---:|
| 1,000 | 8.31 us | 2.41 ms |
| 10,000 | 8.39 us | 20.73 ms |
| 50,000 | 7.55 us | 104.33 ms |

The range path does not care how many keys it covers.

**Keys-only scans never read a payload.** `iter_keys_prefix` and `count_prefix` answer from the index:

| records | size | value-reading | keys-only |
|---:|---|---:|---:|
| 25,000 | 16 KiB | 30.50 ms | 1.05 ms |
| 100,000 | 16 KiB | 136.26 ms | 4.21 ms |
| 25,000 | 64 KiB | 98.48 ms | 1.05 ms |
| 8,000 | 256 KiB | 101.78 ms | 338.75 us |

**Batch shape belongs to the calling code**, and the write table above prices it. No pass at open and no loop while running can choose it, which makes it the largest single lever the config does not hold.

## Two knobs that are promises

`sync` decides what a crash may cost. Fitting it to a device trades somebody else's guarantee for throughput, so set it from what the caller promises its own users and leave it there. `repair` says the same about corruption: `Peers` turns a failed checksum into a miss because another copy exists, and `None` turns it into an error because nothing else holds those bytes. Neither is a tuning choice.

The bias pass also has an opinion about `map_above`. It reads the box at open, logs what it would choose and changes nothing, so a disagreement between the log line and the config is a question for an operator, and the config stays in force.

## Before you trust any of this

Every number above came off a bench or a past experiment on somebody else's hardware, with a record size, a working set and a concurrency that are almost certainly not yours. Findings that reversed between boxes reversed hard: the mapped plane wins warm and loses cold by 9x, the ring wins cold reads by 4.5x and loses per-record writes by 6.9x, and a pair of defaults that each look reasonable alone left 427 runs standing when set together. Reproduce the two or three findings that would decide your deployment against your own data and your own flow, and change the defaults on what you measure.
