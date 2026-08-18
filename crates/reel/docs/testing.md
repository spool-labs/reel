# Testing: the machinery, and what it still cannot reach

The engine's correctness rests on four planes of test, none of which is a unit
test in the usual sense: named rendezvous points that turn a race into a script,
a crash-point enumeration that walks every boundary of a rolling stream, a
seeded stresser that draws its own shape, and a differential oracle. This doc
says what each reaches, what it demonstrably catches, and what is still a
lottery.

One rule runs through all of it, learned three times the hard way: **a test that
passes without the fix is worse than no test.** Every claim below that says
"pinned" also says how it was falsified, because three separate times a test was
written, passed, and did not fail when the fix was removed.

## Rendezvous points

`src/sync/rendezvous.rs`, driven from `tests/rendezvous_races.rs`. Production code
marks named points and a test script gates them: hold, pass one arrival at a
time, release, await an arrival count, everything freed if the test fails.

Sixteen sites, all at plane boundaries, the first eight being: `seal/queued`, `seal/spans`,
`paged/handover`, `index/page-shard`, `compaction/owed`, `compaction/retire`,
`compaction/repoint`, and the checkpoint pair `checkpoint/staged` and
`checkpoint/published`. Unarmed cost is one acquire load and a cold branch.

The plane is behind the `rendezvous` feature and a build that does not ask for it
pays nothing at all: `at` is an empty function and there is no stage to load
from. The crate turns the feature on for its own test targets through a dev
dependency on itself, so nothing here needs a flag to run.

What this bought: three interleavings pinned in milliseconds that used to need a
campaign on a real box per hit. A delete inside the repoint window wins. Every key
answers while a seal stands unqueued. A read between two paged handovers sees
every key.

The points also reach where the crash harness cannot, which is the reason to
prefer them over drawing a crash. A checkpoint's directory work is `std::fs`
outright, `hard_link`, `create_dir_all`, `rename`, and only its two syncs go
through the driver, so `SimIo` has nothing to intercept and a drawn crash would
miss the boundary entirely. What a test observes while a thread stands parked at
`checkpoint/staged` or `checkpoint/published` is exactly what a crash there would
leave on disk.

## Crash points and the durability sweep

`tests/crash_points.rs`, with `compactor.rs` and `engine.rs` beside it. The
`repair` knob's whole contract is pinned: the sole-copy crash sweep enumerates
every boundary of a rolling stream under scatter and proves a footer never
outlives its records.

The sweep was falsified before it was trusted. Removing the body sync makes it
fail, so it demonstrably reaches the schedule it guards. **The stream must roll
segments or the sweep is theatre**: a stream that never seals passes for any
ordering, which is the failure mode this design most easily hides.

Also pinned here: a sole-copy scrub counts and keeps, compaction leaves a rotted
sole-copy segment standing, and a corrupt read reports while the key keeps its
place.

Batch atomicity is pinned here too. `a_torn_batch_leaves_nothing_of_itself`
writes a confirmed batch, a point write, and then a second batch, cuts that last
one at its frame, its first record, its middle and its last record in turn, and
requires the whole run to be absent on reopen while everything written before it
is still served. `a_torn_range_batch_applies_neither_half` does the same to a
batch carrying a range delete and requires the keys the range covered to survive,
since the delete went down with the run that never landed. Both were falsified
before they were trusted: a walk that keeps whatever a torn run had already read
fails them at the middle cut and at the last.

## The seeded stresser

`tests/seeded_stress.rs`. Caller count, residency, tails, op mix, batch width,
door per call, dropped futures, and a fault plan spanning eleven fault kinds,
scatter and crashes, all drawn from one `u64`.

The invariant follows the plan: honest plans hold live-equals-reopen, lying or
crashed plans hold served-is-whole-and-attempted. Every stall is bounded and
panics with its seed, because a hang that reports nothing is the one failure mode
the file exists to rule out.

Knobs: one test per default seed, `REEL_STRESS_REPLAY` to replay a campaign seed,
`REEL_STRESS_SEEDS` and `REEL_STRESS_OPS` to size a campaign, fault density
scaling with the op window, `REEL_STRESS_DEBUG` to arm the `SlotTable` filing
counters and the `debug_io` / `debug_counts` surfaces. Two shaped pins sit beside
the drawn ones: four awaited callers colliding on claims, and six hundred held
futures past the slot table.

### What it caught that nothing else would have

**The awaited door depended on blocking traffic to poll the backend.** The walk's
flush pump died of a fault-broken sync and a reader's completion sat in the
simulator's ready queue forever, with nobody polling. Fixed by the door serving
itself, so a pending future takes the drain turn when it is free, and pinned by
`a_lone_awaited_read_needs_no_pump`, which stalls for thirty seconds with the fix
removed.

**The paged span registry raced the segment lifecycle**, in a family with two
edges. On the seal edge a resident grave pruned to the exact floor while the
column's candidate spans did not yet offer the tombstone's segment, so the live
footer search resurrected an older version; paged volumes keep the 2^20 window as
the mitigation. On the retire edge candidates kept offering a segment compaction
had retired, and the live read met the missing footer and answered `None` while
its own remaining candidates held the row. `forget_segment` now takes the segment
out of every sealed shard's candidate registry and out of the footer, map and
block caches, so the registry and the segment lifecycle agree at the retire edge
rather than the search having to tolerate a candidate vanishing under it. The
interleaving is about a one in seventy draw, so it wants a campaign to reproduce:
`REEL_STRESS_OPS=300 REEL_STRESS_REPLAY=18097807408287489422`.

## Backpressure

`src/engine/tests.rs`. An idle tick prunes graves to the counter, and ingest heat is
a rate rather than a point sample.

## Probes

Measurement lives in `tests/probes/`, one serial binary behind `--test probes`.
A bare run executes the asserting probes; a substring argument runs everything
matching it, opt-in probes included. Timing rows are only comparable from a
serial run, which is why the binary declines the libtest harness.

## What the tests do not reach, ranked

- **The peers leg of the seal-window sweep.** The sole-copy leg is pinned. The
  peers leg asserts nothing, so the designed answer there, a checksum miss that
  reads as a repairable absence and never a wrong payload, rests on the design
  rather than on a run.
- **Exact prune floor raced.** Nothing parks a put between its draw and its claim
  while a tick prunes, which is what the drawn gauge exists for. Rendezvous
  candidate at the draw seam.
- **`compaction/retire` choreography.** The site is gated now, by the plane guard
  in `engine.rs` that holds a pass there and asserts a second caller sees `Held`.
  The interleaving still unpinned is the one the paged seed found: a paged read
  choosing a segment as it retires.
- **Churn, and the trap under it.** Nothing drives a volume that ingests and
  expires at once, which is the only shape a live deployment ever runs in.

  The trap comes first, because it has already produced a false result. A range
  delete lands as a cover and settles in `sweep_covers`, which rides
  `maintain_once` and not `compact_once`. A harness draining by compaction alone
  leaves every dropped key counted live and every dead segment on disk, and the
  run then reports `segments unlinked whole 0` while claiming to have deleted
  half its input. That turns a churn run into a fill wearing a churn label, and
  nothing in the output says so.

  The invariant that catches it is one line: after a drain, `totals()` equals
  what the workload left live. A churn test has to assert that before it reads
  any other column, because every column is wrong when it fails.

  Two deletion geometries, and they are not the same test. Cohort, whole groups
  dropped as they age, is the write-once shape and the easy one: segments die
  whole and unlink with no copying, which is also why it cannot demonstrate a
  leak. Scattered, single keys deleted across every live segment, is the one that
  makes compaction copy live neighbours and the one a reclamation claim has to
  survive. A run that only does cohort proves less than it looks.
- **Stresser axes not yet drawn.** Range deletes, playback walks under churn,
  `get_range` windows, and the `repair` knob. Deletes are only checked through
  reopen equality today, so the lying modes lean on served-was-attempted alone.
  Range deletes in particular need the cover sweep driven, per the entry above,
  or they assert nothing.
- **Read-only volumes under faults.** The follower's refresh under a drawn fault
  plan, and sole-copy corruption answers on a read-only open.
- **Hot residency in the stresser.** The walk draws resident and paged; `Hot` is
  untouched.
- **Multi-tail sole-copy crash sweep.** The sweep runs one tail, the ordering
  argument is per tail, and a multi-tail leg would say so.
- **The ring leg of the stresser on Linux.** The tag-wrap pin only means what it
  says over a real ring.

## Two decisions the fault work has not made

**The blocking door's give-up.** One empty drain round under a delayed completion
returns `never_completed`, which is harsher than the fault deserves.

**The awaited door has no give-up for a completion that never comes.** That is
why the stresser excludes `DropCompletion`, the one fault kind it does not
draw.

## What a checkpoint's tests already settled

Restore equality holds through `a_checkpoint_opens_as_the_volume_it_copied` and
`writes_after_the_cue_stay_out_of_the_copy`. Hard-link stability holds through
`the_copy_survives_the_original_compacting_it_away`, which drains the volume of
every segment the copy was made from and then reads the copy. Crash mid-checkpoint
holds through the rendezvous pair:
`a_crash_before_the_rename_leaves_only_staging` wants every link down and synced,
a directory under the staging name, nothing under the target, and the live volume
still answering; `a_crash_after_the_rename_leaves_a_whole_copy` wants the target
whole, the staging name gone, and the copy opening and reading every key from the
segment set that stood at the rename rather than anything the call did afterwards.

Falsified before trusted: swapping the two point names fails both tests, so each
discriminates its own side of the rename rather than passing on a state both
share. What stays out of reach in-process is whether the published name survives
a power cut, which is the parent sync's promise and a filesystem's rather than
something a test here can ask.

One defect these tests caught that the design did not name: a tail draws its next
segment when it seals rather than when it next writes, so the boundary alone left
the volume's live segment in the linked set and the copy shared an inode that then
grew. The set is filtered by `is_held` now.
