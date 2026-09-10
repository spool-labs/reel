# Servo: setting the io path at open, and steering it while it runs

A reel takes its io path from configuration. The right path depends on the box
and on the work, and there is enough measurement to say that a fixed default is
wrong somewhere no matter which default is chosen. This file records what the
measurements say, what the startup pass derives from the box it opens on, and
which of those knobs anything acts on.

## The name

Both names come from the tape transport the engine is named after. A machine is
aligned to its stock before it records and holds itself there while it runs, and
those are two different mechanisms.

**Bias** is set once per stock formulation, ahead of recording. It is the startup
pass: read what the box already knows, resolve the path, write the verdict down.
It measures nothing that costs real time.

**Servo** is the loop a transport runs continuously to hold speed and tension
against a load that keeps changing. It is the running half: watch what the work
is actually doing, move one knob, keep it if it helped.

The two names are worth keeping separate because their binding times are
different, which is the whole design constraint below.

## Two binding times, and why they cannot be merged

Some knobs are fixed the moment a file is opened. `O_DIRECT` is the strict case:
`fallback_posix` builds a backend with the volume's direct request at open, and
whether the page cache stands behind a descriptor is a property of the file, not
of the caller. Nothing at runtime can change it without reopening the volume.

What can move per call is which descriptor a read names, and that is the seam
`ranged_reads` uses: a routed segment holds two descriptors on the same file, one
buffered and one direct, so a cold window picks a plane per read while neither
descriptor's own flag ever changes. The binding time is unmoved, and the cost is
a second descriptor per segment the route touches rather than a reopen. It is not
a general escape from this section: two descriptors are worth it where the read
shape is narrow and the win is large, which so far is one of them.

Other knobs are free to move per call. Which submitter takes an op, how deep a
batch is drained, and whether a waiter spins or sleeps are all per-job choices.

So this is not one adaptive system. It is two: a bias pass that settles the
open-time knobs from evidence, and a servo loop that would steer the per-job
knobs inside whatever the bias pass chose. The pass runs at every open. No loop
runs at all, for the reasons below.

## What the measurements say the rules are

From a 9950X sweep and a record-size sweep, both on a dedicated PCIe 5 NVMe.

**The cache decision dominates everything else.** The same cell read 43,204 MB/s
buffered-and-warm against 5,726 MB/s direct. That is a 7.5x swing on one
decision, and it is the open-time one. Every per-job knob is small next to it.

**Direct io is the predictable one, not the fast one.** Across warm and dropped
runs, `uring_direct` moved by 1.001x while posix moved by 9.3x. If a volume needs
a tail latency it can promise, that is the argument. If it needs throughput on a
resident working set, it is the argument against.

**Batch depth is the per-job lever, not job type.** At one thread, per-record
puts favoured posix 6,107 against uring_direct's 3,140. The same cell at
`batch16` was 7,024 against 6,547. The gap is a function of how much work arrives
per call, and it closes with depth. This is the rule the servo can act on.

**Above eight threads everything converges.** All three backends land near 7,100
MB/s of writes against a 7,258 MB/s device ceiling. There is nothing to steer on
a busy volume, which is convenient: the servo only needs to be right when the
volume is quiet. **This is the one rule here a second box overturned. See below.**

## What a second box says: Threadripper PRO 9975WX

Thirty-two cores against the 9950X's sixteen, on a 4Kn Solidigm doing 13.8 GiB/s
read and 4,941 MiB/s sustained write. `raw_matrix`, 4 KiB records, pages dropped
between phases, 8 GiB cells, durable MB/s for writes. The two direct columns are a
retired measurement arm, a posix backend over direct descriptors, kept here because
it is what settled the submitter question:

| threads | posix | posix direct | uring | uring_direct |
|---|---|---|---|---|
| 1 | 699 | 449 | 1,017 | 450 |
| 8 | 1,562 | 2,891 | 3,910 | 3,014 |
| 32 | 234 | 3,675 | 7,633 | 3,680 |
| 64 | 114 | 3,688 | 7,304 | 3,690 |

Reads from the same cells: posix 4,321 to 6,937, uring 4,499 to 10,840, and both
direct columns 73 to 3,472.

**Batching is the lever when threads are few, and the expected caller is a
few-thread workload.** Steady-state writes on one thread, taking more only during
recovery, repair and bootstrap, so the columns above that decide the product are
the leftmost ones. Per-record puts and `batch16` on posix, 16 GiB cells, durable
MB/s:

| threads | put | batch16 | batching is worth |
|---|---|---|---|
| 1 | 701 | 2,262 | 3.2x |
| 2 | 1,609 | 4,207 | 2.6x |
| 4 | 1,299 | 6,100 | 4.7x |
| 8 | 722 | 6,196 | 8.6x |

Two things sit in that table. **Batching is worth more than any backend choice
at low thread counts**, 3.2x on a single writer, against a spread of 1.5x between
backends in the same cell. And **the write cliff is a property of per-record puts
rather than of concurrency**: unbatched writes peak at two threads and fall to
722 by eight, while batched writes climb to 6,196 and stay there. A volume that
batches does not have the cliff at all.

Both readings point the same way for a bias rule. The submitter matters least of
the three decisions available, and the one that would pay is whether the caller
arrives with a batch.

**And batching collapses the backend decision it was competing with.** The same
cells across all three working backends:

| threads | posix | uring | uring_direct |
|---|---|---|---|
| 1, per record | 701 | 1,013 | 451 |
| 1, batch16 | 2,262 | 2,303 | 2,150 |
| 4, per record | 1,299 | 3,285 | 1,706 |
| 4, batch16 | 6,100 | 5,533 | 6,739 |
| 8, batch16 | 6,196 | 5,834 | 6,417 |

Per record the backends spread 2.2x at one thread and 2.5x at four. Batched they
land within 7 percent at one thread and 22 percent at four, in no consistent
order. **A batching caller does not need a backend verdict for its writes**, and
the largest gain belongs to the backend that was worst without it: direct io goes
from 451 to 2,150 at one thread, 4.8x, because a per-record direct write pays an
alignment gather that a run amortises.

This reorders the whole document. The cache decision is still the largest single
open-time lever for reads, where direct costs 59x at 4 KiB. For writes the lever
is the caller's batch, which no bias pass or servo loop can choose, because it is
a property of the code calling the engine rather than of the volume. The engine
already has `write_batch` and `write_batch_wait` for it.

**Convergence above eight threads is a property of that box, not of the engine.**
Here the backends diverge by 64x at 64 threads rather than landing together, and
the divergence is a cliff rather than a slope: buffered posix writes peak at
eight threads and fall from 1,562 to 114. The servo's convenient case, that a
busy volume needs no steering, does not hold, and the busy case is where the
largest wrong answer now lives.

**The submitter stops mattering once the descriptor is direct.** This is the
fourth column `io.md` asked for, and it took two attempts to get honestly. The
first attempt reached direct io by an accident of configuration rather than by
asking for it, and agreed with the ring column so exactly that it should have
been suspicious: the real posix-direct arm refused to write at all, with
`a direct write starts at 27, which is not a block boundary`.

The cause was one predicate. `writes_whole_blocks` matched `UringDirect` alone,
so the appender never framed a posix direct volume's drains on a boundary and
`direct_writev` rejected the first record. The alignment machinery was already
there, built for the ring: pad records carrying a fill length, `AlignedBuf`,
the gather in `direct_writev`. Only the volume was not asking for it. The rule
that keeps it honest is that every site meaning "direct" asks
`IoBackend::is_direct()` rather than naming one backend.

With that, 16 GiB cells, durable MB/s and read MB/s:

| cell | posix direct | uring_direct |
|---|---|---|
| 1 thread, per record | 448 | 451 |
| 1 thread, batch16 | 2,164 | 2,150 |
| 4 threads, per record | 1,662 | 1,706 |
| 4 threads, batch16 | 6,411 | 6,739 |
| reads, 1 thread | 74 | 73 |
| reads, 4 threads | 283 | 285 |

Within 1 to 5 percent in every cell. **A direct volume has one sensible
submitter**, so the bias pass chooses a cache policy and the submitter question
survives only for buffered volumes, where the two differ 64x at 64 threads.

**Buffered is two different answers, not one.** Buffered posix and buffered uring
differ 64x at 64 threads, which is a larger spread than the cache decision this
document calls dominant. A bias pass that resolves buffered against direct and
leaves the submitter to configuration will still be wrong by more than the
decision it just made.

**Direct io costs reads at small records, badly.** 73 MB/s against buffered's
4,321 at one thread on 4 KiB records, a 59x penalty. `io.md` records 62x on a
ccx33 and the 9950X did not reproduce it, which called for a third box to say
which was the outlier. This is that third box and it sides with the ccx33. So the
alignment tax is the common case and the 9950X is the exception, which makes
record size a first-class input to the bias rule rather than a footnote to it.

**What this does not settle.** Both direct columns sit at 3,690 while buffered
uring reaches 7,304, so the fastest configuration measured here is buffered, and
it is the one that cannot ship: that backend burns 1719 to 1784 percent CPU
against posix's 443 to 483 for the same wall clock, spending about 17 cores on a
spin that 32 idle cores happen to absorb. Until that is fixed the bias pass is
choosing among configurations none of which is both fast and affordable, and the
ordering above is provisional on it.

## The bias pass

It must not benchmark. A process that spends thirty seconds measuring its disk at
every open is worse than one that guesses, and a measurement taken under its own
startup load is not the measurement it thinks it is.

Everything it needs is either free or already recorded:

- RAM, and the volume's configured size, which together give the working-set
  ratio that decides buffered against direct.
- The device's own facts, rotational or not, queue depth, logical block size.
- A hardware survey corpus. Labelled boxes carrying `survey`, `disk` and `sync`
  envelopes, so a box that matches a known fingerprint inherits that box's answer
  rather than deriving one.

The output is a small verdict, written next to the volume: which backend, direct
or buffered, and the fingerprint it was decided from. It is re-derived only when
the fingerprint stops matching, so a reopen is free and a hardware change is not
silently ignored.

The honest limitation is that one box is measured properly today. A threshold
fitted to a 9950X with a PCIe 5 NVMe will be wrong on cloud boxes in the same
corpus, where every disk sat between 160 and 230 MB/s and the crossover has to
land somewhere else entirely. The corpus is what makes this tractable rather than
speculative, and it needs more entries before any threshold is worth trusting.

## What is built

`src/reel/bias.rs`. Every real-filesystem open reads the facts, derives a verdict,
logs both beside the configured backend, and keeps the facts on the store as
`ReelStore::bias()`. Nothing acts on it. A simulated volume has no facts and
answers `None`.

The facts, all free: total memory (`/proc/meminfo`, `hw.memsize` on macOS), the
filesystem's capacity, the device's logical block size and rotational flag, how
many actuators it seeks with, this process's `RLIMIT_NOFILE`, and whether a ring
sets up. The survey corpus is not wired in: the pass derives its verdict from the
box in front of it and inherits nobody else's.

The actuator count is `queue/independent_access_ranges`, one directory per
concurrent positioning range, which Linux has published since 5.15. A drive with
two arm assemblies serves half its platters from each and reports one range per
actuator; an ordinary drive does not implement the page at all, so absent is the
ordinary answer and does not mean one. Read and reported, acted on by nothing:
placement still pins one tail per device, and `volumes.md` holds the open
question of whether a device is even the right unit on a drive that seeks twice
at once.

**Every device fact reads absent on a filesystem that has no device.** The
lookup starts at the volume root's `st_dev` and resolves
`/sys/dev/block/{major}:{minor}`, and btrfs hands each subvolume an anonymous
device with major 0, as virtiofs does its mounts. `/sys/dev/block` holds no
entry for either, so logical block size, the rotational flag and the actuator
count all come back `None`, indistinguishable from a machine that does not
publish them, and from macOS, which publishes none of them. The pass does not
say which of the two happened. A volume on ext4 never meets this; one on btrfs
runs a factless bias pass in silence. Confirmed in a Linux VM whose root is
btrfs and whose host passthrough is virtiofs, where a loop-mounted ext4 on the
same kernel read its facts normally.

The pass reaches five choices, and their rules are one line each. It logs them and
applies none, and `fd_cache` is no longer a knob at all, so that row is a fact about
the machine rather than an opinion about a config:

| choice | rule |
|---|---|
| plane | direct once the volume *holds* `DIRECT_AT_OCCUPANCY_RATIO` times memory |
| `map_above` | sixteen times the device's readahead, but only where the *filesystem* is within `DIRECT_AT_OCCUPANCY_RATIO` of memory; absent otherwise |
| `ranged_reads` | follows the plane, so windows are read the way the volume is |
| `preallocate` | `Chunk` once the idle reservation passes an eighth of the disk |
| `fd_cache` | under half of `RLIMIT_NOFILE`, halved again under direct |

**The mapping rule turns on capacity where the plane rule turns on occupancy, and
that asymmetry is deliberate.** A mapped point read wins warm p50 by 13 to 25
percent, loses p99 by 1.7 to 1.8x, and loses a cold read by up to 9x, so the gain
is real only where the records stay resident. Occupancy says what is resident today;
capacity says what will be resident once the volume fills, and a disk larger than
memory will serve cold records whatever its set weighs now. So the pass advises a
floor on the rare machine whose disk memory could hold, and advises unset on every
ordinary one, which is where the shipped default already sits.

**The plane rule turns on occupancy, and it used to turn on capacity.** Reading
the filesystem's *capacity* against memory at a ratio of eight said direct for a
2.9 TB disk holding 40 GiB on a box with 251 GB of RAM, which is a set that fits
in memory six times over. Capacity is what the disk could take, not what the
volume holds. The pass reads `volume_bytes` from a readdir and a stat over the
segment files and turns on `occupied_over_memory()`, and
`DIRECT_AT_OCCUPANCY_RATIO` is 1.5 rather than 8.0, because the bar only had to
be high while the proxy was weak. An empty volume returns no ratio at all and
keeps the warm plane until a reopen sees otherwise.

**The threshold is still not centred, and that is the point.** Wrongly direct
gives up 7.5x on a warm set and 59x on 4 KiB reads. Wrongly buffered gives up
1.13x. Being wrong toward buffered costs a tenth as much, which is why the rule
errs that way at every binding time it appears.

**`map_above` is a floor, not a switch, and that is what the measurement said.**
A mapping skips the kernel crossing a pread pays for warm bytes, which is worth
2.1x on a small-record point-read workload. What it costs is paid cold, and it is
a toll rather than a ratio: a fault pulls a window in around the record instead
of the record. Cold random on the 9950X, mapped throughput against unmapped, 100
GiB fills with the caches dropped and the volume reopened between: 0.54x at
4 KiB, 0.57x at 16 KiB, 0.52x at 64 KiB, 0.61x at 256 KiB, 0.94x at 1 MiB, 1.23x
at 4 MiB and 1.34x at 9.72 MiB. The extra device bytes sat between 205 and 471
KiB an access across the whole band, so the crossing is where the record starts
to dwarf that, between one and four megabytes. Where the pass names a floor at all
it names sixteen times the box's own readahead, which is 2 MiB where readahead is
the usual 128 KiB, and lands inside that window.

Two things follow. A volume holding one size does not exist, so a per-volume
boolean was wrong for one end of the range whichever way it was set: a caller's
9.72 MiB records want the mapping and its records under 1.75 MiB do not. And the
floor is asked per read, at the three sites in `reel.rs` that already had the
flag, so it costs a comparison against a length the caller already holds.

**It stays coupled to the plane.** A direct volume refuses a mapping at
validation, so the pass names no floor at all there rather than one an operator
could not act on.

**Two knobs the pass will not touch.** `sync` is a promise about what a crash may
cost, and fitting it to hardware trades someone else's guarantee for throughput.
`compact_mbps` wants device bandwidth and the pass may not benchmark; there is no
default cap, as `compaction.md` records, so what a future pass could derive is the
cap for an operator who names one, not a default.

**Ring availability keeps its errno.** `Unsupported` is a kernel without
io_uring, `Denied` is `kernel.io_uring_disabled` or a sandbox. A probe that
collapses both to false makes a disabled ring indistinguishable from an absent
one, and that costs real time to diagnose.

### Running it

`tests/probes/bias_report.rs` prints the facts, the verdict and the disagreements:

    REEL_BIAS_DIR=/var/lib/reel cargo test -p tape-reel \
      --test probes -- bias_report

That is the recording step's comparison, runnable on a real box rather than
grepped out of a log. Its companion test pins the rule against facts it is
handed, since the report's own output depends on where it runs.

## The servo loop, which does not exist

Nothing steers a per-job knob while a volume runs. There is no bucket, no
sampling, no hysteresis and no thread. What follows is the shape a loop would
have to take, kept here because each constraint came out of a measurement rather
than a preference, and because the question is otherwise reopened from scratch
every time.

**It should not have its own thread.** The engine already runs a maintenance
plane with an entry point in `engine.rs`, and the settling work moved onto that
tick deliberately. A control loop that samples a counter and occasionally moves a
knob is a poor reason to add a thread, and a thread that wakes on its own
schedule is a new source of jitter on a path whose p99 we just spent effort
lowering. Ride the tick.

**It must not measure in the hot path.** Counters the engine already keeps are
fair game. Anything that needs a clock read per op is not, because at 700 ns per
put a timestamp pair is a measurable fraction of the work. Sample instead: one
call in every few thousand carries timing, and the rest are counted only.

**It should move one knob at a time, with hysteresis.** Two knobs moving together
cannot be attributed. A knob that flips on every tick is an oscillator, not a
controller. The shape that works is a bucket per (batch depth, record size), an
exponentially weighted outcome per bucket, and a change only when the alternative
has been better by a margin for several consecutive ticks.

**It must be able to say it does not know.** A bucket with too few samples keeps
the bias pass's answer rather than guessing from noise. Most volumes will spend
most of their life in that state, and that is the correct outcome, not a failure.

## Whether it is worth it

Worth stating plainly, since the question was whether it can be this smart at
all. The open-time win is 7.5x and needs no loop, only a rule.

The per-job half was written as bounded, roughly 2x on shallow writes at low
concurrency and nothing above eight threads. The 9975WX makes that read too
kindly: on a box where the backends diverge 64x at 64 threads instead of
converging, picking wrong on a busy volume costs more than picking wrong on a
quiet one. That does not promote the servo loop over the bias pass, since the
divergence is between open-time choices rather than per-job ones, but it does
retire the argument that a busy volume is safe to leave alone.

That ordering is why only one of the two exists. The bias pass carries almost
all of the value and almost none of the risk, because it runs once, off the hot
path, and its worst failure is choosing what would have been configured by hand.
The servo carries the smaller win and all of the risk of a feedback system that
oscillates or lies. The first is built; the second has not earned its place.

## The four steps, and where each one stands

1. **Record.** Built, see above. The verdict is logged, kept on the store, and
   reportable per volume. What it still lacks is exposure: the disagreements it
   finds are only worth anything once it has run somewhere other than a
   developer's laptop.

   **Steer the one knob that binds per read.** Built, out of order
   because it needed no loop and no new binding time. A 129-row backend sweep
   measured the plane on cold reads directly and found direct winning in exactly
   one cell of the matrix, a megabyte record read eight ways, by 5 percent. It
   loses everywhere else: 2x at a megabyte read by one reader, 1.2x at 64 KiB
   read eight ways, 73x at 4 KiB read by one, 123x at a hundred bytes. So
   `window_route` now asks two questions rather than one. `DIRECT_RECORD_FLOOR`
   rose from 64 KiB to a megabyte, since 64 KiB was margin against page sharing
   rather than a measured throughput bar, and a `DIRECT_DEPTH_FLOOR` of two
   in-flight cold reads was added beside it, because size alone would have sent
   the lone reader of a large record to the plane it loses 2x on. Both floors err
   toward the page cache.

   This is the one place read pressure can be answered while a volume runs. It is
   not the servo loop: there is no bucket, no weighting and no hysteresis, only a
   counter read at route time. It earns its place because a window picks its
   descriptor per read, so the answer can change without a reopen.
2. **Act at open.** The verdict selects the backend. **Built and reverted, and
   the reason is a hole in the rule rather than in the wiring.** The plane turns
   on `occupied_over_memory()` alone and never reads `is_rotational`, which the
   pass gathers and then ignores here. A common capacity SKU is four SATA
   spinning disks and no NVMe, holding tens of terabytes against 64 GB of memory,
   so the ratio there is not marginal but hundreds to one and permanent. An
   actuator on this rule therefore puts **every such deployment** on the direct
   plane, on spindles, for ever.

   That is the wrong direction on that hardware, and the numbers behind the rule
   never covered it: every cell that priced direct against buffered ran on NVMe,
   and even there direct lost 2x at a megabyte to one reader, 73x at 4 KiB and
   123x at a hundred bytes, which is why the per-read floors exist and why both
   err toward the page cache. Declining the page cache on a spindle costs a seek
   and a rotation per read and gives up write coalescing on the way in.

   Before this is built again it wants `is_rotational` in the rule at minimum,
   and a direct-against-buffered measurement on spinning disks. Neither is a
   bench-box question.

   The rest still holds: still no loop, and this needed a posix-direct arm to
   exist, since a bias pass that can choose direct io but only through the ring is
   choosing two things at once. It existed long enough to be measured, and the
   decomposition says the verdict is smaller than this document assumed:
   **direct or buffered is the whole open-time decision**, because the submitter
   only matters on the buffered side. That halves what the bias pass has to be
   right about.
3. **Observe while running.** Not built. Nothing records per-bucket outcomes on
   the maintenance tick, and nothing reports what a loop would have changed.
4. **Steer one knob.** Not built, and nothing else is a candidate ahead of it:
   batch depth routing between submitters is the one rule the measurements
   actually support.

## What would say this is a bad idea

If step 1 shows the bias verdict agreeing with hand configuration everywhere, the
rule earns nothing and the deployment is already correctly tuned. If step 3 ever
runs and its per-bucket outcomes do not separate, the per-job knobs are not the
lever on real workloads whatever the sweep says. Either result is a reason to
stop after step 2, which is why the steps are ordered this way.
