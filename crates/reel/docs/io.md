# IO: backends, the page cache, and the decisions

Every number here states the machine it came from and whether the working set
fit in memory, because on this engine that has already been the difference
between a finding and its opposite twice. The operator knobs are documented on
the config types; this file is the design record. Which machine to believe: a
bare-metal box's device numbers and any box's CPU numbers, never a mac's device
numbers, and never a VM's, whose host cache has already faked two conclusions
(`O_DIRECT` looked 6x faster under a virtualized filesystem and is 3x slower on
real NVMe).

## Three backends and what each is for

**posix** is the portable floor and the one that ships. Synchronous, one
syscall per op on the calling thread, no completion queue and no handoff. It
is a benchmarked production path rather than a last resort.

**uring** is the buffered ring on Linux. It submits through the page cache
rather than around it, and it is a production path.

**uring_direct** opens the volume's descriptors `O_DIRECT` and stages every op
through a block-aligned buffer. Linux only, and it resolves to a buffered
volume anywhere else.

## How one is chosen

`select_backend` runs at open and never fails the open over a backend choice.

- `posix` is the default and is taken as configured, without asking the kernel
  for anything.
- `uring` and `uring_direct` set a ring up at open. A setup the kernel refuses
  warns once and runs posix, so a volume never fails to open over a backend.

A direct volume that ends up on posix keeps its direct descriptors. The
backend choice picks who submits the op, and whether the page cache stands
behind the file is a property of the volume.

Containers are the one environment that refuses the ring from outside:
Docker's default seccomp profile blocks the io_uring syscalls, the setup sees the
`EPERM`, and the volume runs posix with one warning.

## What the ring measured, and the ruling

A cross-thread handoff costs 16,321 ns on a ccx33 against 2,395 ns on a native
M4, while the ccx33's syscalls are cheaper, 860 ns for a 100 byte write.
Anything that pays a handoff to save a syscall loses there, and everything that
does lost: the ring's completion inbox, the kernel wait, and a batched drain at
every record size. The two machines disagree about the answer, not just the
numbers, a batch at depth 32 loses everywhere on the ccx33 and pays at 4 KiB and
above on the mac, which is why a batched drain would have to probe at startup
rather than be compiled in. No such probe exists, and the batched drain is not
built.

The ring's own shape is the other half: completions come back through a
single poller inbox that costs a lock and a wakeup each, which shows up as
context switches per op climbing with writer count where posix stays flat.
No registered buffers, no single-issuer rings.

**How a waiter waits, and a caveat on early ring rows.** The ring can spin on the
completion queue or sleep in the kernel, and until a sweep could select between
them, every ring number ever recorded priced `RingWait::Spin` whether or not it
said so. Measured on a 9975WX across 1 to 64 threads at 4 KiB, spin is the faster
of the two at every thread count, by 3 to 9 percent low and 0.6 percent at 64.
What it costs is CPU: 543 percent against 405, and 263,263 involuntary context
switches against 106,523. `Kernel` ships for that reason, and the trade is stated
correctly as a quarter of the CPU and 2.5x fewer preemptions for a fraction of a
percent of throughput, not as faster and cheaper at once. Reads are flat across
both at 5,344 to 6,706 MB/s, which is the device rather than the backend.

**The ring is not a write lever on that box at all.** The 129-row backend sweep
of the same day put posix and uring within 1 percent of each other from 64 KiB
up, and at 4 KiB and one thread uring is 14 percent *slower*, 2,632 against
3,067 MB/s. Under O_DIRECT the two agree within 1 to 5 percent at every cell,
because the syscall is not the cost once the volume is direct.

That reasoning was taken entirely on writes that land in the page cache, and
cold reads are the other trade: a get is one `pread` on the calling thread,
so queue depth is the concurrent caller count, one cold reader gets 7 percent
of the device and sixty-four saturate it. Paying a 16 us handoff to gain depth
on a 101 us cold read is a different trade from paying it to save a 0.9 us
syscall. The measured ruling: batching a lone reader is worth 1.54x, not the
15.5x the depth arithmetic implies, and batching a crowded volume takes depth
away, so the ring follows the callers rather than leading them. `get_many`
resolves and submits a batch in one call and merges physically adjacent
records into one read; only a ring would turn a scattered batch into
outstanding reads.

Agave's own precedent points the same way: its production ring reaches
accounts storage and snapshots, bulk sequential file movement, and never the
blockstore, the key-value workload this engine replaces. Expect a ring's win
on the cold read path and nowhere else.

## Which door an op actually took, and why it has to be askable

The ring backend does not serve everything it is handed. A direct volume takes
no ring at all, since `takes_ring` reads `!is_direct`; a vectored write past the
kernel's iovec cap has nowhere to split inside a submission, so it goes to the
posix backend that can walk it in capped calls; a caller on a thread that could
not build a ring falls through as well; and `Ring::stage` sends anything whose
op names no ring file, or whose descriptor the table refuses, straight to
`posix.dispatch`. Every one of those is correct and every one of them is silent.

That is fine until a leg reports a ring number. A compaction wave measured
2026-08-11 lost 4 KiB to posix by 3.06x, and the first suspicion was that those
rows had never touched the ring, because a uring row that fell through is a posix
row wearing a uring label and no other column tells them apart. That suspicion
was wrong: the ring was genuinely in use and the cost is the kernel punting
buffered writes to `io_wq`, 41 percent of the drain's cycles.

What it cost to find out is the point. It took a box, `perf` attached to the
drain through a marker the bench had to learn to print, and an inspection of the
process's open fds, and two open `io_uring` fds prove a ring exists rather than
that any particular op went down it.

`ReelIo::door_counts` makes it a printed line instead. `reached_ring` says
whether any op on the backend went on a ring and `off_ring` counts the ones that
did not, forwarded through `IoDriver` and `ReelStore` beside `sync_count`. The
door tables in the backend sweep print it per sweep, so the reading that cost
a box session is now the row's own testimony, and a leg that never reached the
ring says so before anyone reads its numbers.

It is priced so it cannot distort the rows it exists to check. The ring side is a
relaxed load of a flag that stops changing after the first op, so the line stays
shared and the hot path pays a predictable branch rather than a store per op;
only the fall-through pays an atomic add, and a fall-through hot enough for that
to show is the answer rather than the cost. A backend with no ring answers that
nothing reached one and nothing fell off one, so a posix leg prints no line.

## The one write wide enough to leave the ring

Every write reel issues waits for its own completion: `writev` goes down
`submit_inline`, which stages one op and waits for that op. A ring write has no
batch to travel with, so what the ring is worth on the write path is not the
submission itself but whatever else rides in the same `io_uring_enter`.

A seal traced on ext4 in a container, 400k records into 24 MiB segments, put one
9,628,877 byte `Writev` on the ring against 1,563 record writes that were all
128 KiB or under. The wide one is a segment's whole sorted footer, and the
kernel answers it on an `iou-wrk` worker while the sealer waits on the
completion: the same wait, one thread further away. Writes leave the ring above
`DIRECT_REQUEST_BYTES` now, which is where the direct door already stopped
serving them, so both doors agree on what a ring write is and the footer blocks
on the thread that issued it. That thread is the sealer, which owns a footer
sort and an `fsync` already.

Chunking the footer into 512 KiB ring submissions was the alternative, so that
several requests reach the device from one enter. The same trace rules against
it: with every write forced off the ring the process's peak `iou-wrk` count went
2 to 0, so on ext4 a chunk buys another worker punt rather than another queued
request, and twenty of them would need short-write and ordering bookkeeping for
a write whose caller wants one count. The sealer's next act is `sync_full`,
which serialises whatever the chunks won.

## Direct io and its alignment tax

Bypassing the page cache moves three constraints onto the caller: the file
offset, the byte count, and the buffer's own address all have to sit on a
block boundary.

Writes are already framed. A whole-block volume reserves each record on a
boundary and closes it with a pad, so staging a write is a gather into one
aligned run.

Reads have no such trick. A record lives wherever the drain that wrote it put
it, so a point read asks for an offset and a length aligned to nothing. The
read widens to the blocks containing the record and the caller's bytes are
cut out of the middle, which is a copy the buffered path does not pay.
Against a buffered read that was going to be cached anyway, that is a
straight loss, and the measured 62x worse reads on the ccx33 are that.

The widening is not itself an amplification, which is what makes a ranged read
route possible. `Advice::Random` is on every reader descriptor, so a buffered
miss faults whole pages with no readahead and fetches `L + 4095` bytes on
average; the covering span fetches `L + BLOCK - 1`, and at `BLOCK == 4096` those
are the same bytes. The staging copy is the price, and on a cold read whose pages
nothing will ask for again it is cheaper than the page cache work it replaces.
The staging buffer is per thread rather than per read, because the per-read
aligned allocation is the whole of a small direct read's penalty.

The ring also takes nothing while a volume is direct. A data op's buffers
belong to the caller and sit wherever the allocator put them, which a direct
descriptor refuses, so those ops go to the staging path instead. Putting them
back on the ring is the registered-buffer work.

**The submitter stops mattering once the descriptor is direct.** A fourth arm,
a posix backend over direct descriptors without the ring, was built to price the
submitter and the cache policy separately. Measured on the 9975WX at 16 GiB cells
it agreed with `uring_direct` to within 1 to 5 percent in every cell, writes and
reads alike, so **the ring buys nothing once the descriptor is direct** and the
ring's whole advantage lives on the buffered side. The arm is gone; `servo.md`
carries the table it produced.

Getting there took one predicate. `writes_whole_blocks` matched the ring's direct
arm alone, so a posix direct volume was never framed on a boundary and
`direct_writev` refused its first record for starting at offset 27. Two other
sites had the same shape. They ask `IoBackend::is_direct()` now.

The read tax is the part that does not go away. A record sits wherever its drain
put it, so a direct point read widens to covering blocks and copies out of the
middle. The 62x above is the ccx33 and the 9950X did not reproduce it; the
9975WX does, at 59x on 4 KiB reads. Two boxes to one, so the tax is the common
case rather than the exception.

## The page cache

**A buffered volume keeps its pages, unconditionally.** There was a knob for
giving them back, per read and per write on Linux 6.14 and up and by
`posix_fadvise(DONTNEED)` behind the write head anywhere else, and the measured
story killed it: keeping pages is never badly wrong, dropping cost 47x on reads
that follow writes, and even past RAM, where the cache cannot help, the per-op
flag still cost 2.9x on writes because it gives up dirty page batching. There is
no regime on this kernel and filesystem where dropping is faster, and what was
left of its case, freeing memory for another process, never justified two code
paths and a probe.

Two things learned there are worth keeping. **The filesystem decides, not just the
kernel**: ext4 accepted the per-op flag and btrfs refused it with `ENOTSUP` on the
same kernel, so a Linux version test is not enough to know whether a flag applies.
And the retirement rule that made a refusal safe: the first flagged op is the
probe, and a flag that has once been accepted never retires, so a real error on a
working kernel is reported rather than swallowed. The cold-window route still runs
on exactly that rule.

Writeback is still paced: a megabyte at a time behind the write head, so the device
is busy while the writer is still copying.

## Readahead

Sealed segments open with the readahead hint off. Every reader here asks for
a range it already knows, a point read framed from the index or a whole scan
window, so a kernel guessing ahead of a small record faults pages nobody
wants. The hint is one call per descriptor rather than per read, and a
platform without an equivalent drops it.

## The mapped fault window

What `map_above` is priced against. A warm mapped read skips the kernel
crossing a pread pays, which took the agave point rows from 0.75x of the
baseline engine to 1.59x. A cold fault fetches a fixed window around the
record instead of the record. Measured cold random on a 9950X, device bytes
over bytes asked: 55.4x at 4 KiB, 14.0x at 16 KiB, 6.0x at 64 KiB, 2.1x at
256 KiB, a near constant fetch of about 225 KiB an access. Above the floor a
mapping is most of a win, below it a large loss, which is why the setting is
a byte floor and not a switch.

### Remeasured 2026-08-09, and the floor is unfitted

On a ccx33, kernel 7.0, ext4, by `tests/probes/mapped_reads.rs`. Cold is past memory,
40 GiB a leg, so neither plane can retain; warm is a second pass over a set
already resident. Mapped over unmapped, so above one the mapping wins.

| plane | record | mapped/unmapped |
|---|---|---|
| warm | 200 B | **16.4x** |
| warm | 300 B | **7.4x** |
| warm | 1228 B | **6.0x** |
| cold | 4 KiB | 0.81 |
| cold | 64 KiB | **1.25** |

Two things move.

**The warm win is far larger than 1.59x at the sizes agave stores.** Six to sixteen
times, sub-page, which is what the blockstore integration was betting on when it
set `MAP_EVERYTHING` and is now measured rather than inferred from a ratio
against the baseline engine.

**The cold penalty does not survive at 64 KiB.** 1.25 rather than the 6.0x
amplification the row above records, so a cold mapped read there beats a pread. The
amplification figure and this one are not the same measurement, device bytes against
wall clock, and they are also different machines and kernels, so this does not refute
the window. What it does mean is that **the 2 MiB floor is fitted to rows that do not
reproduce as time on this box**, and refitting it wants both quantities from one
machine.

**And `MADV_RANDOM` was tried and reverted.** The argument for it was symmetry, since
`POSIX_FADV_RANDOM` sits on the descriptor and cannot reach a mapping of the same
file, so the mapped plane was the only one still faulting ahead. It measures neutral
at a page, 0.82 against 0.81, and **8x worse above one**, 0.15 against 1.25 at 64 KiB,
because fault-around and mapped readahead are what turn a sixteen-page record into one
or two faults and the advice switches exactly that off. `io/mapping.rs` carries the
table at the point where someone would add it back.

### The mapped plane is blocking-only, so none of this speaks to the ring

`read_record_wait` never maps, by design: a page fault cannot be awaited and a device
error inside one arrives as SIGBUS on whichever worker was polling, so the async door
asks the driver whatever `map_above` says. A mapped read never reaches a backend at
all, since `read_framed` answers from the mapping before the driver is consulted.

So a warm win of six to sixteen times is available **only** to a caller on the
blocking door, and a caller that needs queue depth gives it up by construction. Those
are opposite doors for opposite workloads: a warm plane of small records wants the
mapping and no depth, and a cold burst of scattered keys wants depth and cannot have
the mapping.

The escape is a warm probe: one non-blocking read ahead of the op, so a record the
page cache holds is answered with no tag, slot or completion spent, and a cold one
takes EAGAIN and rides the driver. `PointReads::Probed` turns it on. Both doors have
it now, `wait_split_reusing` and `pread_split_reusing` alike.

Prefer the probe to the mapping on media that fails by sector. A bad sector under a
mapped read is SIGBUS and a dead process; the same read through the door comes back
an error the caller can act on. The probe keeps the warm win and leaves every cold
read on the reporting path.

## Why the tail count is the write-path knob

One tail is one file, and on Linux a buffered write takes the inode
exclusively, so writers past the first queue in the kernel however many the
engine admits. The tail count is what turns concurrent writers into
concurrent files.

A direct write is documented to take the inode shared where the write is
aligned and does not extend the file, which would let one file absorb all of
them, and the engine is shaped for that case on purpose: it pads to a block
boundary under the direct backend and it preallocates, so an append lands
inside a size the file already has. One condition the documentation does not
settle is whether the shared path survives writes into preallocated but
never-written extents, which is every write an append-only log issues.
`tests/stress/inode_lock.rs` measures that below the engine, so an engine sweep can
be read against a floor.

**It survives.** On a ccx33 on ext4, 2026-08-19, a direct first pass into reserved
but never-written extents scales 12 to 14 times over sixteen writers on one file,
while the buffered rows stay flat at 1.00 and a file per writer beats one file by
6.8x there. So the shared path is real and the tail count is the buffered answer to
it, which is the one the default backend takes.

**One file for the whole reel was refused on the same run.** It only works on the
direct path, where reservation is what makes it work at all: bare, every append
extending, it reads 222 MB/s against 3,170 reserved. A log grows without bound, so
a single file has to extend for ever, and reserving a chunk at a time rather than
the whole file costs 28% of that. What it buys back is nothing, since one file only
ever matches a file per writer, which is where segments already sit.

**Preallocation earns that keep only on a shared inode.** With a file per tail,
reserved, chunked and bare land inside the run-to-run spread of each other, so
`Preallocate` is a question about when ENOSPC arrives and how many blocks sit idle,
not about throughput.

## Why dropping pages is not a knob

Because it is not free and the volume that wants it is the exception. Dropping is
what costs, per the page cache section above, and keeping pages is never badly
wrong. The case that survives is a volume whose freed cache has a better claim,
a metadata volume rather than a bulk one, or a tier whose hot set is genuinely
held elsewhere such as behind a CDN edge, and none of those is this engine's
volume.

It was also never the answer for a cold read that wants the cache skipped rather
than dropped. Dropbehind still copies through a folio and still does the page
cache insertion and reclaim around it, which is the whole of what perf billed
the ranged path for; it saves the retention, not the work. That is why the cold
window route takes a second `O_DIRECT` descriptor rather than a per-op flag.

## Why not `O_DIRECT` as a default

The write-throughput argument for it is largely spent. A per-op cache hint
already gave buffered writes without retention and without alignment rules, and
it measured faster than direct while still losing to holding pages. Direct
measured 3x slower than buffered on writes and 62x worse on reads on the one box
whose device numbers are worth trusting, and it pays a staging copy the buffered
path does not. What survives is that direct ingest stops coupling to a metadata volume's
dirty-page accounting, which has not been measured on hardware that could show
it.

The ruling is about the default for a whole volume, and it is narrower than it
reads. What the 62x measured was a warm working set served from cache against a
volume that had no cache; the same loss prices at 7.5x. It
says nothing about a cold read whose pages nothing will ask for again, which is
the one shape where there is no warm plane to lose. `ranged_reads` is direct for
exactly that shape and nothing else: one record class, one read kind, sealed
segments only, with the buffered plane and the whole-record path untouched.

## Polled completions, measured and then removed

The ring could ask the kernel to poll the device for completions instead of taking
interrupts, direct volumes only. The knob is gone; three findings from it are worth
keeping.

**A polled ring posts nothing without an enter.** The spin wait, the awaited door's
reap, and the submit path each read the completion queue without entering the
kernel, so the first implementation paid a fixed millisecond per write, 18 to 22x
the interrupt-driven rows, and a cold read leg that never completed in three
attempts. Two on-box probes each moved the stall to the next place rather than
clearing it. Any future work here has to enter with `GETEVENTS` in all three.

**A gate that prices a reference implementation is not a gate on yours.** A ccx33
with polled queues genuinely engaged, virtio-scsi at `virtscsi_poll_queues=4`,
0.0000 IRQ/IO, ran 80 cells: fio's hipri was faster on 30 of 40 shapes at a median
1.05x and cheaper on CPU in zero of 40, median 4.42x per op, and on that reading the
feature was not worth having. But fio's hipri is a userspace busy-poll and the
engine's branch waited in the kernel, and the same box with only the flag moving
measured faster and cheaper at once: writes 0.78 to 0.95x latency at every size up
to 1 MiB on both doors, cold reads 0.85 to 0.99x across all ten sizes, total CPU
0.90 to 0.92x with user time collapsing.

**It never earned a default anyway.** Every number above is virtio-scsi, the one
bare-metal confirmation was never taken, and a knob that ships off and is measured
on one virtualized device is a surface with nothing behind it. The cheap re-gate if
the question reopens: fio hipri against non-hipri, sizes 4/16/64/256 KiB, depths
1/8/32/128, one and four jobs, `poll_queues` sized to jobs, CPU-per-op quoted beside
IOPS. A direct-volume read row has to leave `map_above` unset either way, because a
mapped read never reaches the ring.

## What the ring backend's thread_local design constrains

The per-thread ring is what makes `SINGLE_ISSUER` legal, and it is the right
shape. What it also does is put four constraints on any future caller, none of
which costs throughput in steady state and all of which are invisible until
something breaks.

**Submit and poll are same-thread only.** Completions staged through `submit()`
land on the submitting thread's ring, and `poll()` drains only the calling
thread's ring through `on_open_ring`. In-crate callers honour this: `run`,
`run_op` and `collect` in segment.rs submit and poll on one OS thread. A caller
pairing submit on thread A with poll on thread B strands completions silently,
which is why the contract is written on `ReelIo::submit` and `ReelIo::poll`
rather than left to be discovered.

**A dead backend's rings linger.** After a volume drops, a thread's ring for it
survives until that thread next enters `on_ring` for some backend, or exits, and
the registered descriptor table pins up to `MAX_REGISTERED_FILES` files in the
kernel until then. This is fine for the store's long-lived threads and it stays
unfixed on purpose: an eager reclaim needs a cross-thread registry plus a lock on
the hot path, which is the slower design.

**Never add a door that returns with ops outstanding.** `Ring` drops `IoUring`
before `Inflight`, but closing the ring fd does not wait for kernel exit work, so
in-flight DMA into just-freed buffers is theoretically reachable on abnormal
paths, thread exit or a retain after a caller abandoned collect's error path. All
four doors drain before returning, and that is the invariant holding the hazard
shut rather than anything in the drop order.

**The `RefCell` borrow in `on_ring` is held across `act`,** kernel parks
included. No reentrancy exists today. Any future path where completion handling
or posix dispatch calls back into the same backend on the same thread panics with
a double borrow. Latent constraint, not a defect.

One thing that looks like a hazard and is not: the pointer-identity match in
`on_ring` is not an ABA risk, because the held `Weak<Core>` keeps the old
ArcInner allocation alive, so a live backend's `Arc::as_ptr` can never collide
with a stale entry's.

A test note that belongs with them: `callers_spread_across_the_shards` assumes
`SHARDS_TAKEN` increments are consecutive for its duration. The `PLACES` mutex
serializes only the two tests in that file, so another test exercising the async
door concurrently can flake it.

## What the io-uring crate offers that this backend does not take

Read against the vendored source of the version pinned, `io-uring 0.7.13`,
rather than against docs or memory. The backend already uses most of the crate,
so the holes are narrow, but two of them point at something already measured.

Already in use, so nobody re-derives it: `setup_clamp`, `setup_single_issuer`,
`register_buffers`, `register_files_sparse`, `register_files_update`, and opcodes
`Read`, `ReadFixed`, `Readv`, `Writev`, `WriteFixed`, plus `PollAdd.multi` for the
inbox kick. The kernel's completion-work and submission-poll setup flags were
measured, found to buy nothing on this engine's shapes, and are not asked for.

**Worth doing, in order.**

1. `register_iowq_max_workers` is not called anywhere. The compaction
   depth-fetch work established that buffered reads missing cache punt to io-wq,
   and this is the knob for it, a `[bounded, unbounded]` pair of worker caps.
   The kernel default scales off cpu count, so on a 9950X a worker herd competes
   with the engine's own threads for the same cores. One call at ring build,
   plumbed through `RingTuning`. It is the cheapest item here because the
   depth-fetch bench already points at it.
2. `register_iowq_aff` pairs with 1 and is also uncalled. It pins io-wq workers
   to a cpu_set. Ring-per-thread does real work to place engine threads, and
   io-wq workers currently float across every core and undo that placement. Only
   worth measuring after 1, since a capped pool badly placed and an uncapped
   pool badly placed are not separable results.
3. `MsgRingData` is the architectural one. Cross-thread handoff today is
   `submit_detached` into the engine inbox plus the `PollAdd.multi` kick fd.
   `MsgRingData` posts a completion carrying arbitrary `user_data` directly into
   another thread's completion queue: no inbox, no kick fd, no shared lock. That
   is the same shape as the op owning its own completion, taken from agave and
   deferred because it implied ring-per-thread, and it is available in the
   version already pinned. Design around it rather than bolting it on: it
   replaces the inbox, it does not sit beside it.

**`WritevFixed` looks like the fix for the staged-write memcpy and is not.**
Every caller span is copied into one registered buffer before `WriteFixed` goes
in, and the opcode's name suggests scatter-gather out of that. Its iovecs must
all point inside the single registered buffer named by `buf_index`, so it does
vectored io within a registered region, not scatter-gather out of arbitrary
caller memory, and the engine's spans are unregistered heap. The copy is
inherent to moving arbitrary memory into an aligned registered buffer and no
opcode in this crate removes it. Recorded so it is not proposed again on the
strength of the name.

**Marginal, none of them leads.** `Flags::SKIP_SUCCESS` would buy back the
completion-queue slots that `make_room` caps in flight on, but the applicable
set is small because the result is read for short-write detection on nearly
every op, so it covers only ops where no news is good news. `submit_with_args`
takes a timespec through `IORING_ENTER_EXT_ARG`, giving `park` a bounded wait
without spending a `Timeout` sqe, which is a cheaper shutdown and liveness story
rather than throughput. `register_probe` is a skip outright: detection is by
trial and fallback, and `build_ring` reports a refused flag rather than silently
retrying without it, so an operator measuring a flag finds out it did not apply,
which is the better design already.

Nothing else in the crate surface is a gap. The socket, xattr, futex and
zero-copy receive families do not apply to a disk engine, and `Fsync` stays off
the ring for the reason written at its refusal site.

## What is unmeasured

- The ring at high core counts. Seeing a ring win needs enough cores in
  flight to make per-op cost visible, around 32, a device with IOPS headroom
  rather than a network disk, and records small enough that the cost is not
  lost behind the bytes. Miss any one and both backends sit against the same
  ceiling and the table reads as a tie.
- Every ring tunable other than the shipped defaults.
