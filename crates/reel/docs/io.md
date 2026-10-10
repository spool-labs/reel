# IO: backends, the page cache, and the decisions

Every number here lists its machine and whether the working set fit in memory, since that has
flipped a finding twice. Trust a bare-metal box's device numbers and any box's CPU numbers.
Distrust device numbers from a mac or a VM: a VM's host cache faked two conclusions, and
`O_DIRECT` looked 6x faster under a virtualized filesystem and is 3x slower on real NVMe. The
config types document the operator knobs.

## Backends

| backend | what it does | where |
|---|---|---|
| `posix` (default) | Synchronous, one syscall per op on the calling thread, no completion queue, no handoff. A benchmarked production path. | everywhere |
| `uring` | Buffered ring that submits through the page cache. A production path. | Linux |
| `uring_direct` | Opens descriptors `O_DIRECT` and stages every op through block-aligned buffers. | Linux, a buffered volume elsewhere |

**On ext4 a buffered ring is a queue in front of a worker pool.** A buffered op there punts to
`io_wq`. One probe on one kernel saw two `iou-wrk` threads through every buffered phase on an
ext4 loop device, and zero through all of them on btrfs or through the direct phases on ext4.
So a `uring` volume on ext4 pays a submission to hand its blocking call to a kernel worker, and
that costs 41 percent of a compaction drain's cycles. The fleet runs ext4, so there
`uring_direct` is the answer: the request reaches the device from the submitting thread. For
the page cache on ext4, read the posix rows.

## How one is chosen

`select_backend` runs at open and never fails the open over a backend.

- `posix` is taken as configured, without asking the kernel anything.
- `uring` and `uring_direct` build a probe ring. If the kernel refuses, or off Linux, the volume
  logs one warning and runs posix. Docker's default seccomp profile blocks the io_uring
  syscalls, so in a container setup sees `EPERM` and the volume runs posix.
- A direct volume that ends up on posix keeps its direct descriptors. The backend only picks
  who submits the op.
- Each thread builds its own ring the first time it submits. A thread that cannot build one runs
  its ops on posix.

## What the ring measured

A cross-thread handoff costs 16,321 ns on a ccx33 and 2,395 ns on a native M4, while a 100 byte
write syscall on the ccx33 costs 860 ns. Anything that pays a handoff to save a syscall loses
there, and everything that did lost: the ring's completion inbox, the kernel wait, and a
batched drain at every record size. The machines also disagree on the answer. A batch at depth
32 loses everywhere on the ccx33 and pays at 4 KiB and above on the mac, so a batched drain
would need a startup probe. Neither exists.

The first ring sent completions back through one poller inbox at a lock and a wakeup each, and
context switches per op climbed with writer count where posix stayed flat. Today the blocking
doors run on the caller's own `SINGLE_ISSUER` ring, and an async caller hands ops to one engine
thread per shard through a bounded inbox.

**How a waiter waits.** It spins while the ring holds only writes and sleeps in the kernel once
a read is out. On the beast box a spin on a write-only ring gives 8.1 us against 13.0 us at the
commit p50, and the same spin with reads in the mix burns 10.8x the cycles for nothing. Ring
numbers recorded before a sweep could separate the two all included a spin. On a 9975WX, 1 to
64 threads, 4 KiB:

| | spin | sleep |
|---|---|---|
| speed | faster by 3 to 9 percent at low thread counts, 0.6 percent at 64 | |
| CPU | 543 percent | 405 percent |
| involuntary context switches | 263,263 | 106,523 |
| reads | 5,344 to 6,706 MB/s | the same, the device is the limit |

**The ring is no write lever on that box.** The 129-row backend sweep of the same day put posix
and uring within 1 percent from 64 KiB up. At 4 KiB and one thread uring is 14 percent slower,
2,632 against 3,067 MB/s. Under `O_DIRECT` they agree within 1 to 5 percent at every cell,
because the syscall stops being the cost once the volume is direct.

**Cold reads are a different trade.** A get is one `pread` on the calling thread, so queue depth
is the number of concurrent callers. One cold reader gets 7 percent of the device and sixty-four
saturate it. A 16 us handoff to gain depth on a 101 us cold read is a different trade from one
that saves a 0.9 us syscall. Measured, batching a lone reader is worth 1.54x against the 15.5x the
depth arithmetic implies, and batching a crowded volume takes depth away. So the ring follows
the callers. `get_many` resolves and submits a batch in one call and merges physically adjacent
records into one read, and only a ring turns a scattered batch into outstanding reads.

Agave's production ring serves accounts storage and snapshots, bulk sequential file movement,
and never the blockstore, the key-value workload this engine replaces. Expect a ring's win on
the cold read path and nowhere else.

## Which door an op took

The ring backend silently hands these ops to posix:

- every op on a direct volume with `registered_buffers` off (`takes_ring`)
- a vectored write past the kernel's iovec cap, which a submission cannot split, so posix walks
  it in capped calls
- a write wider than `DIRECT_REQUEST_BYTES`, or a read past the kernel's per-call cap
- every op from a thread that could not build a ring
- from `Ring::stage`, an op with no ring file, a descriptor the table refuses, or a direct op the
  registered pool cannot serve

So a uring row that fell through is a posix row with a uring label. When a compaction wave on
2026-08-11 lost 4 KiB to posix by 3.06x, the first suspicion was that its rows never touched the
ring. The ring was in use, and the cost was the kernel punting buffered writes to `io_wq`, 41
percent of the drain's cycles. Proving it took a box, `perf` on the drain and a look at open fds,
and an open `io_uring` fd shows a ring exists and says nothing about any one op.

`ReelIo::door_counts` answers directly, and `IoDriver` and `ReelStore` forward it beside
`sync_count`:

| field | meaning |
|---|---|
| `reached_ring` | any op on this backend went on a ring |
| `off_ring` | ops handed to another backend |
| `pool_refused` | the kernel refused a thread's buffer pool |
| `files_refused` | the kernel refused a ring's sparse file table |

The count cannot distort the rows it checks. The ring side is a relaxed load of a flag that
stops changing after the first op, so the line stays shared and the hot path pays a
predictable branch. Only a fall-through pays an atomic add. A backend with no ring reports that
nothing reached one and nothing fell off one.

## Wide writes leave the ring

Every write reel issues waits for its own completion: `writev` goes down `submit_inline`, which
stages one op and waits for it. So a ring write is worth only what else shares its
`io_uring_enter`.

A seal traced on ext4 in a container, 400k records into 24 MiB segments, put one 9,628,877 byte
`Writev` on the ring beside 1,563 record writes of 128 KiB or less. The wide one was a segment's
sorted footer, and the kernel served it on an `iou-wrk` worker while the sealer waited. Writes
now leave the ring above `DIRECT_REQUEST_BYTES` (128 KiB), where the direct door already stops,
so the footer blocks on the sealer, which already owns a footer sort and an `fsync`.

Splitting the footer into 512 KiB ring submissions was ruled out by the same trace. With every
write forced off the ring the peak `iou-wrk` count went from 2 to 0, so on ext4 each piece buys
another worker punt. Twenty pieces would also need short-write and ordering bookkeeping, and the
sealer's next step is `sync_full`, which serialises whatever they gained.

## Direct io

Bypassing the page cache puts the file offset, the byte count and the buffer address on a block
boundary.

- **Writes are already framed.** A whole-block volume reserves each record on a boundary and
  closes it with a pad, so staging a write is one gather. Framing follows
  `IoBackend::is_direct()`, so a direct volume that falls back to posix still frames on blocks.
- **Reads widen** to the blocks around the record, and the caller's bytes are copied out of the
  middle. That copy is the direct read tax.
- **The widening fetches no extra bytes.** `Advice::Random` is on every reader descriptor, so a
  buffered miss faults whole pages with no readahead and fetches `L + 4095` bytes on average.
  The covering span fetches `L + BLOCK - 1`, the same bytes at `BLOCK == 4096`.
- **The posix staging buffer is one per thread**, because a per-read aligned allocation is the
  whole of a small direct read's penalty.
- **On a ring, direct data ops use registered buffers**, 32 per ring at 128 KiB plus a block
  each, since a direct descriptor refuses the caller's own buffers. With `registered_buffers`
  off, or a pool the kernel refused, they take the posix staging path.

| box | direct reads against buffered |
|---|---|
| ccx33 | 62x worse |
| 9950X | did not reproduce |
| 9975WX | 59x worse at 4 KiB |

Two boxes of three, so the tax is the common case. Against a read the page cache would have
served anyway the copy is a straight loss. On a cold read whose pages nothing will ask for again
it is cheaper than the page cache work it replaces.

**The submitter stops mattering once the descriptor is direct.** A removed fourth arm, posix over
direct descriptors, agreed with `uring_direct` within 1 to 5 percent in every cell on the 9975WX
at 16 GiB cells, writes and reads alike. The ring's whole advantage is on the buffered side.

## Why `O_DIRECT` is not the default

On the one box whose device numbers are worth trusting, direct measured 3x slower than buffered
on writes and 62x worse on reads, and it pays a staging copy. A per-op cache hint gave buffered
writes without keeping pages or alignment rules, measured faster than direct, and still lost to
keeping pages. The one argument left for direct is that ingest stops coupling to a metadata
volume's dirty-page accounting, which is unmeasured on hardware that could show it.

The ruling covers the default for a whole volume. The 62x compared a warm set served from cache
with a volume that had no cache, and the same loss prices at 7.5x. It says nothing about a cold
read whose pages nothing will ask for again.

## The page cache

**A buffered volume always keeps its pages.** A knob to give them back, per op on Linux 6.14 and
up and by `posix_fadvise(DONTNEED)` behind the write head elsewhere, was removed. Keeping pages
is never badly wrong. Dropping cost 47x on reads that follow writes, and even past RAM the per-op
flag cost 2.9x on writes because it gives up dirty page batching. No regime on this kernel and
filesystem made dropping faster. A volume that would want it, a metadata volume or a tier whose
hot set lives behind a CDN edge, is not this engine's volume.

Two lessons stay:

- **The filesystem decides as well as the kernel.** ext4 took the per-op flag and btrfs refused
  it with `ENOTSUP` on the same kernel, so a Linux version check cannot tell whether a flag
  applies.
- **The retirement rule.** The first flagged op is the probe, and a flag the kernel has accepted
  once never retires, so a real error on a working kernel is reported. The warm read probe runs
  on this rule.

Writeback is paced a megabyte at a time behind the write head, so the device is busy while the
writer still copies.

## Readahead

Every segment a reader opens gets the readahead hint off. Each reader asks for a range it
already knows, a point read framed from the index or a whole scan window, so readahead only
faults pages nobody wants. The hint is one call per descriptor, and a platform without one skips
it.

## The mapped fault window

`map_above` is the smallest record served from a read-only mapping. A warm mapped read skips
the kernel crossing a pread pays, which took the agave point rows from 0.75x of the baseline
engine to 1.59x. A cold fault fetches a fixed window of about 225 KiB around the record. Cold
random on a 9950X, device bytes over bytes asked:

| record | amplification |
|---|---|
| 4 KiB | 55.4x |
| 16 KiB | 14.0x |
| 64 KiB | 6.0x |
| 256 KiB | 2.1x |

Above the floor a mapping is mostly a win and below it a large loss, so the setting is a byte
floor.

### Remeasured 2026-08-09

ccx33, kernel 7.0, ext4. Cold is past memory, 40 GiB a leg, so neither plane can keep its
pages. Warm is a second pass over a resident set. Above one the mapping wins.

| plane | record | mapped/unmapped |
|---|---|---|
| warm | 200 B | **16.4x** |
| warm | 300 B | **7.4x** |
| warm | 1228 B | **6.0x** |
| cold | 4 KiB | 0.81 |
| cold | 64 KiB | **1.25** |

- **The warm win at agave's record sizes is six to sixteen times**, far above 1.59x. The
  blockstore integration set `MAP_EVERYTHING` on that bet.
- **The cold penalty is gone at 64 KiB**, where a cold mapped read beats a pread. The 6.0x row
  is device bytes on another machine and kernel and this one is wall clock, so it does not
  refute the window. It does mean the 2 MiB floor the servo picks does not reproduce as time on
  this box, and refitting it needs both quantities from one machine.
- **`MADV_RANDOM` on the mapping was measured and reverted.** It measured neutral at a page,
  0.82 against 0.81, and 8x worse above one, 0.15 against 1.25 at 64 KiB. Fault-around and
  mapped readahead turn a sixteen-page record into one or two faults, and the advice switches
  that off.

### The mapped plane is blocking-only

`read_record_wait` never maps a sealed segment, since a page fault cannot be awaited and a
device error inside one arrives as SIGBUS on whichever worker was polling. It does read an open
tail through its mapping, since those pages were just written. A mapped read never reaches a
backend, because `read_framed` answers from the mapping first. So a caller on the async door
gives up the warm win, and a caller on the blocking door gives up queue depth. A warm plane of
small records wants the mapping. A cold burst of scattered keys wants depth.

The warm probe bridges the two. It is one non-blocking read ahead of the op, so the page cache
answers a warm record with no tag, slot or completion spent, and a cold one gets `EAGAIN` and
goes to the driver. Every read on a buffered volume asks it, on both doors
(`pread_split_reusing` and `wait_split_reusing`).

Prefer the probe to the mapping on media that fails by sector. A bad sector under a mapped read
is SIGBUS and a dead process, and through the door it is an error the caller can act on.

## The tail count is the write-path knob

One tail is one file. On Linux a buffered write takes the inode exclusively, so writers past
the first queue in the kernel. The tail count turns concurrent writers into concurrent files.

A direct write takes the inode shared when it is aligned and does not extend the file. The
engine pads to a block boundary under the direct backend and preallocates, so an append lands
inside the file's size. The `inode_lock` stress test checks below the engine that the shared
path survives writes into preallocated, never-written extents, so an engine sweep has a floor
to compare against. On a ccx33, ext4, 2026-08-19:

| case | result |
|---|---|
| direct first pass into reserved extents, sixteen writers on one file | scales 12 to 14x |
| buffered, sixteen writers on one file | flat at 1.00 |
| buffered, a file per writer over one file | 6.8x |
| one direct file, bare, every append extending | 222 MB/s |
| one direct file, reserved | 3,170 MB/s |
| reserving a piece at a time over the whole file | 28 percent slower |

So the shared path is real, and the tail count is the buffered answer, which the default backend
takes.

**One file for the whole reel was turned down.** It works only on the direct path with
reservation. A log grows for ever, so it would reserve a piece at a time and pay the 28 percent,
and at best it matches a file per writer, which is where segments already sit.

**Preallocation pays only on a shared inode.** With a file per tail, reserving the whole file,
reserving in pieces and not reserving land within run-to-run spread. How a segment reserves its
blocks decides when ENOSPC arrives and how many blocks sit idle, and leaves throughput alone.

## Polled completions, removed

The ring could poll the device for completions on direct volumes. The knob is gone. Three
findings stay.

- **A polled ring posts nothing without an enter.** The spin wait, the awaited door's reap and
  the submit path each read the queue without entering the kernel. The first build paid a fixed
  millisecond per write, 18 to 22x the interrupt-driven rows, and a cold read leg never finished
  in three attempts. Two on-box probes only moved the stall. Future work has to enter with `GETEVENTS` in all three places.
- **fio's numbers do not gate the engine.** A ccx33 with polled queues engaged (virtio-scsi,
  `virtscsi_poll_queues=4`, 0.0000 IRQ/IO) ran 80 cells. fio's hipri, a userspace busy-poll, was
  faster on 30 of 40 shapes at a median 1.05x and cheaper on CPU in zero of 40, a median 4.42x
  per op. The engine waits in the kernel, and on the same box with only the flag changed it
  measured faster and cheaper at once: writes 0.78 to 0.95x latency at every size up to 1 MiB on
  both doors, cold reads 0.85 to 0.99x across all ten sizes, total CPU 0.90 to 0.92x with user
  time collapsing.
- **It never earned a default.** Every number is virtio-scsi and no bare-metal run confirmed it.
  To reopen it: fio hipri against non-hipri, sizes 4/16/64/256 KiB, depths 1/8/32/128, one and
  four jobs, `poll_queues` sized to jobs, CPU per op beside IOPS.

## Deferred completion work

By default a ring interrupts its owning thread for every completion.
`IORING_SETUP_DEFER_TASKRUN` holds the work until the thread enters asking for completions. That
drops the inter-processor interrupt, stops completions running on a kernel entry made for
something else, and lands them in one batch. It needs `IORING_SETUP_SINGLE_ISSUER` and the enter
from the submitting thread, which ring-per-thread already gives.

`RingTuning::taskrun` picks the mode, `Deferred` by default. A refusal is one errno that does
not say which flag, so `build_ring` steps down one mode at a time:

| mode | flag | completion work runs |
|---|---|---|
| `Deferred` | `DEFER_TASKRUN` | when the thread asks |
| `Cooperative` | `COOP_TASKRUN`, kernel 5.19 | at any kernel exit, no interrupt |
| `Interrupt` | none | on an interrupt, a plain ring |

Both held modes also set `TASKRUN_FLAG`, so `IORING_SQ_TASKRUN` says when work is waiting and a
peek stays a flag read.

Nothing reaches the completion queue until this thread enters with `GETEVENTS`, and
`io-uring`'s `submit()` is `submit_and_wait(0)`, which sets `GETEVENTS` only when it also waits.
So `flush()` runs no completion work. Every path that reads the queue without sleeping goes
through `Ring::drain`, which asks first when the flag is up: the batch harvest, the spin, the
engine loop and `ReelIo::poll`. The ask is an enter that submits nothing and waits for nothing.

The spin pays for it. A seal on the container's ext4:

| seal | enters | submissions |
|---|---|---|
| buffered, `Interrupt` | 1,591 | 1,591 |
| buffered, `Deferred` | 3,179 | 1,591 |
| direct, `Deferred` | 1,594 | 1,597 |

A buffered spin pays one submit and one ask per op. The direct arm does not move, because its
completions are there when the submit enters. Folding `GETEVENTS` into the submission moved the
threaded direct phase from 2,116 enters to 2,103 and nothing else, and needed a hand-rolled
`enter`, so it was dropped. A wait that parks pays nothing extra, since `submit_and_wait`
already asks. `Interrupt` is the way back, and `REEL_RING_TASKRUN` sweeps the mode in tests.

Without the ask a write-only spin never enters the kernel, burns its million rounds, and then
sleeps. That is no hang and fails no correctness test: in the container a 64-write batch took
9.85 s against 0.37 s and gave up on four spins. `spin_outs` counts waits that gave up, and
`a_write_batch_spins_without_giving_up` asserts it stays zero. A climbing count says the ring's
completion mode and its wait no longer agree.

## What ring-per-thread asks of callers

None of these costs throughput in steady state, and all are invisible until something breaks.

- **Submit and poll are same-thread only.** `submit()` stages on the submitting thread's ring,
  and `poll()` drains only the calling thread's ring through `on_open_ring`. The driver's `run`,
  `run_op` and `collect` stay on one OS thread. Submitting on one thread and polling on another
  strands completions silently, which the docs on `ReelIo::submit` and `ReelIo::poll` warn
  about.
- **A dead backend's rings linger.** After a volume drops, a thread's ring for it survives until
  that thread next enters `on_ring` for any backend, or exits, and its descriptor table pins up
  to `MAX_REGISTERED_FILES` (4096) files in the kernel. That suits long-lived store threads.
  Eager reclaim would need a cross-thread registry and a lock on the hot path, so it stays
  unfixed.
- **Never add a door that returns with ops outstanding.** `Ring` drops `IoUring` before
  `Inflight`, but closing the ring fd does not wait for kernel exit work. So DMA into just-freed
  buffers is possible in theory on abnormal paths: thread exit, or the dead-ring sweep in
  `on_ring` after a caller left collect's error path. All four driver doors drain before
  returning, and that keeps the hazard shut. The drop order alone does not.
- **The `RefCell` borrow in `on_ring` is held across `act`**, kernel parks included. Nothing
  reenters today. A future path where completion handling or posix dispatch calls back into the
  same backend on the same thread would panic on a double borrow.

The pointer match in `on_ring` has no ABA risk. The held `Weak<Core>` keeps the old allocation
alive, so a live backend's `Arc::as_ptr` can never equal a stale entry's.

`callers_spread_across_the_shards` assumes `SHARDS_TAKEN` increments are consecutive while it
runs. The `PLACES` mutex serializes only the two tests that take it, so another test on the
async door at the same time can flake it.

## What the io-uring crate offers that the backend leaves unused

Read against the io-uring 0.7.13 source. The lockfile now pins 0.7.14.

In use: `setup_clamp`, `setup_single_issuer`, `setup_defer_taskrun`, `setup_coop_taskrun`,
`setup_taskrun_flag`, `Submitter::enter`, `register_buffers`, `register_files_sparse`,
`register_files_update`, the opcodes `Read`, `ReadFixed`, `Readv`, `Writev` and `WriteFixed`,
and `PollAdd.multi` for the inbox kick. `SQPOLL` is off: it costs a kernel thread per ring and
a 30 us wake, and seals arrive in bursts.

**Worth doing, in order.**

1. `register_iowq_max_workers`, uncalled. Buffered reads that miss cache punt to io-wq, and this
   caps those workers with a `[bounded, unbounded]` pair. The kernel default scales with cpu
   count, so on a 9950X a worker herd competes with the engine's threads for the same cores. One
   call at ring build, plumbed through `RingTuning`. The cheapest item here.
2. `register_iowq_aff`, uncalled. It pins io-wq workers to a cpu_set. Ring-per-thread places
   engine threads, and floating io-wq workers undo that. Measure it after 1, since one run
   cannot tell placement apart from the cap.
3. `MsgRingData`, the architectural one. Cross-thread handoff today is `submit_detached` into
   the engine inbox plus the `PollAdd.multi` kick fd. `MsgRingData` posts a completion with any
   `user_data` straight into another thread's completion queue, with no inbox, no kick fd and no
   shared lock. That is the op-owns-its-completion shape from agave, deferred because it
   needed ring-per-thread, and the pinned version has it. It replaces the inbox, so design
   around it from the start.

**`WritevFixed` keeps the staged-write memcpy.** Every caller span is copied into one registered
buffer before `WriteFixed` goes in. `WritevFixed`'s iovecs must all point inside the one
registered buffer at `buf_index`, and the engine's spans are unregistered heap. No opcode in
this crate removes the copy.

**Marginal.**

- `Flags::SKIP_SUCCESS` would free completion-queue slots that `make_room` caps in flight on.
  The result is read for short-write detection on nearly every op, so it covers few ops.
- `submit_with_args` takes a timespec through `IORING_ENTER_EXT_ARG`, which gives `park` a
  bounded wait without a `Timeout` sqe. A cheaper shutdown and liveness story, with no
  throughput gain.
- `register_probe` is a skip. `build_ring` falls back by itself and logs the mode the kernel
  refused and the one it runs.

The socket, xattr, futex and zero-copy receive families do not apply to a disk engine. `Fsync`
stays off the ring because a sync blocks in the kernel either way, so a ring only moves the wait
onto a worker thread.

## What is unmeasured

- The ring at high core counts. A ring win needs about 32 cores in flight, a device with IOPS
  headroom (no network disk), and records small enough that per-op cost is not lost behind the
  bytes. Miss any one and both backends hit the same ceiling and the table reads as a tie.
- Every ring tunable other than the shipped defaults.
