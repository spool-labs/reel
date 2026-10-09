# Servo

A reel takes its io path from config. The right path depends on the box and the work, so any fixed default is wrong somewhere. This page covers what the measurements say, what the startup pass reads, and which knobs anything acts on.

## Bias and servo

Both terms come from the tape transport, like the engine's own vocabulary.

| | bias | servo |
|---|---|---|
| on a tape machine | set once per stock, ahead of recording | the loop that holds speed and tension against a changing load |
| here | the startup pass: read what the box already knows, resolve the path, log the verdict | watch the work, move one knob, keep it if it helped |
| binds | at open | per call |
| built | yes, and it only logs | no |

Some knobs are fixed when a file opens. `O_DIRECT` is the strict case: `fallback_posix` builds a backend with the volume's direct request at open, and whether the page cache stands behind a descriptor is a property of the file, fixed until the volume reopens. Other knobs can move per call: which submitter takes an op, how deep a batch is drained, and whether a waiter spins or sleeps. A bias pass settles the first kind from evidence, and a servo loop would steer the second inside what the bias pass chose. The pass runs at every open. No loop runs at all.

## What the 9950X says

From a 9950X sweep and a record-size sweep, both on a dedicated PCIe 5 NVMe.

- **The cache decision dominates.** The same cell read 43,204 MB/s buffered and warm against 5,726 MB/s direct, a 7.5x swing on one open-time decision. Every per-job knob is small next to it.
- **Direct io is the predictable one.** Across warm and dropped runs, `uring_direct` moved by 1.001x while posix moved by 9.3x. That argues for direct when a volume needs a tail latency it can promise, and against it for throughput on a resident working set.
- **Batch depth is the per-job lever.** At one thread, per-record puts favoured posix 6,107 against `uring_direct`'s 3,140. The same cell at `batch16` was 7,024 against 6,547. The gap follows how much work arrives per call and closes with depth. This is the rule a servo could act on.
- **Above eight threads everything converged on this box.** All three backends landed near 7,100 MB/s of writes against a 7,258 MB/s device ceiling, so a busy volume had nothing to steer. A second box overturned this.

## What the Threadripper PRO 9975WX says

Thirty-two cores against the 9950X's sixteen, on a 4Kn Solidigm doing 13.8 GiB/s read and 4,941 MiB/s sustained write. `raw_matrix`, 4 KiB records, pages dropped between phases, 8 GiB cells, durable MB/s for writes. The posix direct column comes from a retired measurement arm, a posix backend over direct descriptors, kept because it settled the submitter question.

| threads | posix | posix direct | uring | uring_direct |
|---|---|---|---|---|
| 1 | 699 | 449 | 1,017 | 450 |
| 8 | 1,562 | 2,891 | 3,910 | 3,014 |
| 32 | 234 | 3,675 | 7,633 | 3,680 |
| 64 | 114 | 3,688 | 7,304 | 3,690 |

Reads from the same cells: posix 4,321 to 6,937, uring 4,499 to 10,840, and both direct columns 73 to 3,472.

**Batching is the lever when threads are few, and the expected caller has few threads.** Steady-state writes come from one thread and take more only during recovery, repair and bootstrap, so the leftmost columns decide the product. Per-record puts against `batch16` on posix, 16 GiB cells, durable MB/s:

| threads | put | batch16 | batching is worth |
|---|---|---|---|
| 1 | 701 | 2,262 | 3.2x |
| 2 | 1,609 | 4,207 | 2.6x |
| 4 | 1,299 | 6,100 | 4.7x |
| 8 | 722 | 6,196 | 8.6x |

- **Batching is worth more than any backend choice at low thread counts**: 3.2x on a single writer, against a 1.5x spread between backends in the same cell.
- **The write cliff belongs to per-record puts.** Unbatched writes peak at two threads and fall to 722 by eight. Batched writes climb to 6,196 and stay there. A volume that batches does not have the cliff.

**Batching collapses the backend decision.** The same cells across all three working backends:

| threads | posix | uring | uring_direct |
|---|---|---|---|
| 1, per record | 701 | 1,013 | 451 |
| 1, batch16 | 2,262 | 2,303 | 2,150 |
| 4, per record | 1,299 | 3,285 | 1,706 |
| 4, batch16 | 6,100 | 5,533 | 6,739 |
| 8, batch16 | 6,196 | 5,834 | 6,417 |

Per record the backends spread 2.2x at one thread and 2.5x at four. Batched they land within 7 percent at one thread and 22 percent at four, in no consistent order. **A batching caller needs no backend verdict for its writes.** The largest gain goes to the backend that was worst without it: direct io goes from 451 to 2,150 at one thread, 4.8x, because a per-record direct write pays an alignment gather that a run amortises.

So the cache decision is still the largest open-time lever for reads, where direct costs 59x at 4 KiB. For writes the lever is the caller's batch (`write_batch`, `write_batch_wait`), which no bias pass or servo loop can choose, and the submitter matters least.

**Convergence above eight threads belonged to the 9950X.** Here the backends diverge 64x at 64 threads, and as a cliff: buffered posix writes peak at eight threads and fall from 1,562 to 114. A busy volume does need steering, and the busy case is where the largest wrong answer lives.

**The submitter stops mattering once the descriptor is direct.** The first posix-direct run reached direct io by an accident of configuration and matched the ring column suspiciously well. The real arm refused to write, with `a direct write starts at 27, which is not a block boundary`, because `writes_whole_blocks` matched `UringDirect` alone. The appender never framed a posix direct volume's drains on a block boundary, so the ring's alignment machinery (fill-length pads, `AlignedBuf`, the `direct_writev` gather) went unused. Every site that means "direct" now asks `IoBackend::is_direct()`.

With that fixed, 16 GiB cells, durable MB/s and read MB/s:

| cell | posix direct | uring_direct |
|---|---|---|
| 1 thread, per record | 448 | 451 |
| 1 thread, batch16 | 2,164 | 2,150 |
| 4 threads, per record | 1,662 | 1,706 |
| 4 threads, batch16 | 6,411 | 6,739 |
| reads, 1 thread | 74 | 73 |
| reads, 4 threads | 283 | 285 |

Within 1 to 5 percent in every cell. **A direct volume has one sensible submitter**, so the submitter question survives only for buffered volumes. There it is two different answers: buffered posix and buffered uring differ 64x at 64 threads, more than the cache decision, so a bias pass that leaves the submitter to config can still be wrong by more than the decision it made.

**Direct io costs reads at small records, badly.** 73 MB/s against buffered's 4,321 at one thread on 4 KiB records, a 59x penalty. A ccx33 measured 62x and the 9950X did not reproduce it, so a third box had to decide, and this one sides with the ccx33. The alignment tax is the common case and the 9950X is the exception, which makes record size a first-class input to the bias rule.

**What this does not settle.** Both direct columns sit at 3,690 while buffered uring reaches 7,304, so the fastest configuration here is buffered uring, and it cannot ship. It burns 1719 to 1784 percent CPU against posix's 443 to 483 for the same wall clock, about 17 cores of spin that 32 idle cores happen to absorb. Until that is fixed the bias pass chooses among configurations none of which is both fast and affordable, and the ordering above is provisional.

## The bias pass

It must not benchmark. A process that spends thirty seconds measuring its disk at every open is worse than one that guesses, and a measurement taken under its own startup load measures the wrong thing.

| part | design | built |
|---|---|---|
| inputs | RAM, the volume's size, device facts, a survey corpus | total memory, filesystem capacity, the bytes in the volume root, logical block size, rotational flag, readahead, actuator ranges, `RLIMIT_NOFILE`, ring availability |
| survey corpus | labelled boxes with `survey`, `disk` and `sync` envelopes, so a box matching a known fingerprint inherits its answer | not wired in. The pass derives its verdict from the box in front of it |
| output | a verdict file next to the volume with the backend, direct or buffered, and the fingerprint, re-derived when the fingerprint changes | logged beside the configured backend, and the facts kept on the store as `ReelStore::bias()`. Nothing acts on it |

Only one box is measured properly today. A threshold fitted to a 9950X with a PCIe 5 NVMe will be wrong on the cloud boxes in the corpus, where every disk sat between 160 and 230 MB/s and the crossover lands somewhere else entirely. The corpus is what makes this tractable, and it needs more entries before any threshold is worth trusting.

### What the pass reads

Every real-filesystem open reads the facts from the reel's root. Memory comes from `/proc/meminfo`, or `hw.memsize` on macOS. A simulated volume has no facts and answers `None`.

The actuator count is `queue/independent_access_ranges`, one directory per concurrent positioning range, published since Linux 5.15. A drive with two arm assemblies reports one range per actuator. An ordinary drive lacks the page, so the count reads absent, which is the normal answer. Nothing acts on it: placement still pins one tail per device.

**Every device fact reads absent on a filesystem with no device.** The lookup resolves the volume root's `st_dev` under `/sys/dev/block/{major}:{minor}`. btrfs subvolumes and virtiofs mounts get anonymous devices with major 0 and no entry there, so block size, the rotational flag and the actuator count all come back `None`, the same as on a machine that publishes nothing or on macOS. The pass does not say which happened, so a volume on btrfs runs a factless bias pass in silence. Confirmed in a Linux VM with a btrfs root and virtiofs passthrough, where a loop-mounted ext4 on the same kernel read its facts normally.

**Ring availability keeps its errno.** `Unsupported` is a kernel without io_uring (ENOSYS). `Denied` is any other refusal, such as `kernel.io_uring_disabled` or a sandbox. A probe that folds both into false makes a disabled ring look like an absent one, and that costs real time to diagnose.

### The verdict

The pass reaches three choices, each a one-line rule. It logs them and applies none. `fd_cache` is no config knob, so that row is a fact about the machine.

| choice | rule |
|---|---|
| plane | direct once the volume *holds* `DIRECT_AT_OCCUPANCY_RATIO` (1.5) times memory. An empty volume stays buffered |
| `map_above` | sixteen times the device's readahead (128 KiB when the device won't say), only on the buffered plane and only where the *filesystem's capacity* is under `DIRECT_AT_OCCUPANCY_RATIO` times memory. Absent otherwise |
| `fd_cache` | `DEFAULT_FD_CACHE` (4096), or half of `RLIMIT_NOFILE` when that is lower |

**The mapping rule turns on capacity and the plane rule on occupancy, on purpose.** A mapped point read wins warm p50 by 13 to 25 percent, loses p99 by 1.7 to 1.8x, and loses a cold read by up to 9x, so it pays only where records stay resident. Occupancy says what is resident today. Capacity says what will be once the volume fills. So the pass advises a floor only on the rare machine whose memory could hold the whole disk, and unset everywhere else, which matches the shipped default.

**The plane rule used to turn on capacity.** Capacity against memory at a ratio of eight said direct for a 2.9 TB disk holding 40 GiB on a box with 251 GB of RAM, a set that fits in memory six times over. The pass now sums `volume_bytes` with a readdir and a stat over every file in the reel's root and turns on `occupied_over_memory()`. `DIRECT_AT_OCCUPANCY_RATIO` dropped from 8.0 to 1.5, since the bar only had to be high while the proxy was weak.

**The threshold leans buffered on purpose.** Wrongly direct gives up 7.5x on a warm set and 59x on 4 KiB reads. Wrongly buffered gives up 1.13x. Being wrong toward buffered costs a tenth as much, which is why the rule leans that way at every binding time.

**`map_above` is a per-read floor.** A mapping skips the kernel crossing a pread pays for warm bytes, worth 2.1x on a small-record point-read workload. Cold, it pays a fixed toll: a fault pulls in a window around the record. Cold random on the 9950X, mapped throughput against unmapped, 100 GiB fills with caches dropped and the volume reopened between:

| record | mapped against unmapped |
|---|---|
| 4 KiB | 0.54x |
| 16 KiB | 0.57x |
| 64 KiB | 0.52x |
| 256 KiB | 0.61x |
| 1 MiB | 0.94x |
| 4 MiB | 1.23x |
| 9.72 MiB | 1.34x |

The extra device bytes sat between 205 and 471 KiB per access across the whole band, so the crossing comes where the record dwarfs that, between one and four megabytes. Sixteen times the usual 128 KiB readahead is 2 MiB, inside that window. A per-volume boolean was wrong at one end of the range whichever way it was set, since a caller's 9.72 MiB records want the mapping and its records under 1.75 MiB do not. The floor costs one comparison per read against a length the caller already holds. A direct volume refuses a mapping at validation, so the pass sets no floor there.

**The pass leaves two knobs alone.** `sync` is a promise about what a crash may cost, and fitting it to hardware trades someone else's guarantee for throughput. `compact_mbps` wants device bandwidth and the pass may not benchmark. There is no default cap, so a future pass could only derive the cap for an operator who sets one.

### Running it

The bias report probe prints the facts, the verdict and the disagreements:

```
REEL_BIAS_DIR=/var/lib/reel cargo test -p tape-reel \
  --test probes -- bias_report
```

Its companion test, `the_rule_is_a_function_of_its_facts`, pins the rule against facts it is handed, since the report's own output depends on where it runs.

## The servo loop does not exist

Nothing steers a per-job knob while a volume runs. Each constraint below came from a measurement, so a future loop has to meet them all.

- **Ride the maintenance tick.** A thread waking on its own schedule adds jitter on a path whose p99 took effort to lower.
- **No clock in the hot path.** At 700 ns per put a timestamp pair is a measurable share of the work. Use the counters the engine already keeps, and time one call in every few thousand.
- **One knob at a time, with hysteresis.** Two knobs moving together cannot be attributed, and a knob that flips every tick is an oscillator. Keep a bucket per (batch depth, record size) with an exponentially weighted outcome, and change only when the alternative has been better by a margin for several ticks in a row.
- **Say it does not know.** A bucket with too few samples keeps the bias pass's answer. Most volumes will spend most of their life there, and that is correct.

## Whether it is worth it

The open-time win is 7.5x, and a rule gets it with no loop. The per-job half looked bounded on the 9950X: roughly 2x on shallow writes at low concurrency and nothing above eight threads. On the 9975WX picking wrong on a busy volume costs more than on a quiet one, but that 64x divergence is between open-time choices, so the bias pass still outranks the servo.

The bias pass holds almost all the value and almost none of the risk: it runs once, off the hot path, and its worst failure is choosing what would have been configured by hand. The servo holds the smaller win and all the risk of a feedback system that oscillates or lies, so it is unbuilt.

## The four steps

| step | state |
|---|---|
| 1. Record | built. The verdict is logged, kept on the store, and reportable per volume. The disagreements it finds only mean something once it has run somewhere other than a developer's laptop |
| 2. Act at open | built and reverted, for a hole in the rule |
| 3. Observe while running | not built. Nothing records per-bucket outcomes on the tick or reports what a loop would have changed |
| 4. Steer one knob | not built. Batch-depth routing between submitters is the one rule the measurements support, and nothing else is a candidate ahead of it |

**Steering the read window was built and removed.** A 129-row backend sweep measured the plane on cold reads and found direct ahead in one cell, a megabyte record read eight ways, by 5 percent. It lost everywhere else: 2x at a megabyte read by one reader, 1.2x at 64 KiB read eight ways, 73x at 4 KiB read by one, 123x at a hundred bytes. Every window now reads through the page cache.

**Why step 2 was reverted.** The plane turns on `occupied_over_memory()` alone and ignores `is_rotational`, which the pass gathers. A common capacity SKU is four SATA spinning disks with no NVMe, tens of terabytes against 64 GB of memory, a permanent ratio of hundreds to one. The rule would put every such deployment on the direct plane, on spindles, for good. Every cell that priced direct against buffered ran on NVMe, where direct already lost by the margins above, and on a spindle declining the page cache costs a seek and a rotation per read and gives up write coalescing. Before it is built again the rule needs `is_rotational` at minimum, plus a direct-against-buffered measurement on spinning disks. Neither is a bench-box question.

Step 2 needed the posix-direct arm, since a bias pass that can choose direct io only through the ring is choosing two things at once. That arm showed **direct or buffered is the whole open-time decision**, because the submitter only matters on the buffered side, which halves what the bias pass has to get right.

## What would say this is a bad idea

If step 1 shows the bias verdict agreeing with hand configuration everywhere, the rule earns nothing and the deployment is already tuned. If step 3 ever runs and its per-bucket outcomes do not separate, the per-job knobs are not the lever on real workloads whatever the sweep says. Either result is a reason to stop after step 2, which is why the steps are in this order.
