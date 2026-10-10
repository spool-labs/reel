# Testing: the machinery, and what it still cannot reach

The engine's correctness rests on four planes of test, and none is a unit test in the usual sense:

| plane | where | what it does |
|---|---|---|
| rendezvous points | `src/sync/rendezvous.rs`, `tests/rendezvous_races.rs` | points in production code that turn a race into a script |
| crash points | `tests/crash_points.rs` | walks every boundary of a rolling stream |
| seeded stresser | `tests/seeded_stress.rs` | draws its own shape from one seed |
| differential oracle | `tests/differential.rs` | runs seeded streams against a memory oracle |

One rule runs through all of it: **a test that passes without the fix is worse than no test.**
Three times a test was written, passed, and did not fail when the fix was removed. So every claim
below that says pinned also says how it was falsified.

## Rendezvous points

Production code marks points and a test script gates them: hold, pass one arrival at a time,
release, await an arrival count, and free everything if the test fails. The points sit at plane
boundaries: `seal/queued`, `seal/spans`, `paged/handover`, `paged/handover-spot`,
`index/page-shard`, `put/landed`, `delete/landed`, `batch/landed`, `compaction/owed`,
`compaction/retire`, `compaction/repoint`, `scrub/segment`, `slots/self-drain`, and the checkpoint
pair `checkpoint/staged` and `checkpoint/published`.

An unarmed point costs one acquire load and a cold branch. The plane sits behind the `rendezvous`
feature, and a build without it pays nothing: `at` is an empty function and there is no stage to
load. The crate turns the feature on for its own test targets through a dev dependency on itself,
so nothing here needs a flag.

Interleavings that used to need a campaign on a real box per hit now pin in milliseconds:

- A delete inside the repoint window wins.
- Every key answers while a seal stands unqueued.
- A read between two paged handovers sees every key.
- A put, a delete or a batch that publishes after a newer version of its key has sealed and been
  handed over loses to that version.

The points also reach where the crash harness cannot. A checkpoint's directory work is plain
`std::fs` (`hard_link`, `create_dir_all`, `rename`), and only its two syncs go through the
driver, so `SimIo` has nothing to intercept and a drawn crash would miss the boundary. What a test
sees while a thread is parked at `checkpoint/staged` or `checkpoint/published` is exactly what a
crash there would leave on disk.

## Crash points and the durability sweep

The `repair` knob's whole contract is pinned. The sole-copy crash sweep walks every boundary of a
rolling stream under scatter and proves a footer never outlives its records. It was falsified
first: removing the body sync makes it fail. **The stream must roll segments or the sweep proves
nothing.** A stream that never seals passes for any ordering, which is the failure this design
hides most easily.

| test | file | what it pins |
|---|---|---|
| `a_sole_copy_footer_never_outlives_its_records` | `tests/crash_points.rs` | the sole-copy crash sweep above |
| `a_sole_copy_scrub_counts_and_keeps` | `src/compaction/compactor.rs` | a scrub hit on a sole copy is counted, and the key keeps resolving |
| `a_sole_copy_rotted_segment_is_not_retired` | `src/compaction/compactor.rs` | compaction leaves a rotted sole-copy segment standing |
| `a_sole_copy_reports_corruption_and_keeps_the_key` | `src/engine/tests.rs` | a corrupt read reports, and the key keeps its place |
| `a_torn_batch_leaves_nothing_of_itself` | `tests/crash_points.rs` | after a confirmed batch, a point write and a second batch, it cuts the last batch at its first record, its middle and its last record in turn, and requires the whole run absent on reopen while everything before it is still served |
| `a_torn_range_batch_applies_neither_half` | `tests/crash_points.rs` | the same for a batch with a range delete, and the keys the range covered must survive, since the delete went down with the run |

Both batch tests were falsified first: a walk that keeps whatever a torn run had already read fails
them at the middle cut and at the last.

## The seeded stresser

The stresser draws caller count, tails, op mix, batch width, door per call, dropped futures, and a
fault plan over twelve fault kinds, scatter and crashes, all from one `u64`.

The invariant follows the plan. Honest plans hold live-equals-reopen, and lying or crashed plans
hold served-is-whole-and-attempted. Every stall is bounded and panics with its seed, because a
hang that reports nothing is the failure the file exists to rule out.

| knob | effect |
|---|---|
| default seeds | one test each |
| `REEL_STRESS_REPLAY` | replays a campaign seed |
| `REEL_STRESS_SEEDS`, `REEL_STRESS_OPS` | size a campaign, with fault density scaling with the op window |
| `REEL_STRESS_DEBUG` | a watchdog prints `debug_counts` and `debug_io`, both sides of the completion seam, when a walk runs past 15 seconds |

Two shaped pins sit beside the drawn ones: `inline_callers_colliding_on_claims`, four awaited
callers colliding on claims, and `a_backlog_of_futures_past_the_tag_table`, six hundred held
futures past the tag table.

### What it caught that nothing else would have

**The awaited door relied on blocking traffic to poll the backend.** The walk's flush pump died of
a fault-broken sync, and a reader's completion sat in the simulator's ready queue for ever with
nobody polling. The fix lets the door serve itself, so a pending future takes the drain turn when
it is free. `a_lone_awaited_read_needs_no_pump` pins it, and stalls for thirty seconds with the fix
removed.

**The paged span registry raced the segment lifecycle**, at two edges.

- Seal edge: a grave was pruned to the exact floor while the column's candidate spans did not yet
  offer the tombstone's segment, so the live footer search resurrected an older version. Graves
  now keep the 2^20 window for it.
- Retire edge: candidates kept offering a segment compaction had retired, and the live read met
  the missing footer and answered `None` while its own remaining candidates held the row. The
  retire step now takes the segment out of every column's sealed registry (`forget_segment`) and
  out of the footer, map and block caches before the unlink, so the registry and the lifecycle
  agree.

The interleaving is about a one in seventy draw, so it takes a campaign to reproduce:
`REEL_STRESS_OPS=300 REEL_STRESS_REPLAY=18097807408287489422`.

## Backpressure

Two engine tests: `a_tick_keeps_a_grave_inside_the_window` checks that a tick keeps a grave until
the 2^20 window has passed it, and `ingest_heat_is_a_rate_not_a_point_sample` checks that ingest
heat is a rate over the ask interval.

## Probes

Measurement lives in one serial binary, `--test probes`. A bare run executes the asserting probes,
and a substring argument runs every probe matching it, opt-in ones included. Timing rows only
compare from a serial run, which is why the binary skips the libtest harness.

## What the tests do not reach, ranked

1. **Peers repair in the seal-window sweep.** The sole-copy leg is pinned. Under peers nothing
   asserts the designed answer, a checksum miss that reads as a repairable absence and never as a
   wrong payload, so that rests on the design alone.
2. **`compaction/retire` choreography.** The site is gated: `a_running_pass_turns_the_next_caller_away`
   holds a pass there and asserts a second caller sees `Held`. The interleaving the paged seed
   found is still unpinned: a paged read choosing a segment as it retires.
3. **Churn, and the trap under it.** Nothing drives a volume that ingests and expires at once, the
   only shape a live deployment ever runs.

   The trap comes first, because it already produced a false result. A range delete lands as a
   cover and settles in `sweep_covers`, which runs from `maintain_once` and never from
   `compact_once`. A harness that drains by compaction alone leaves every dropped key counted live
   and every dead segment on disk. The run then reports `segments unlinked whole 0` while claiming
   to have deleted half its input. It is a fill with a churn label, and nothing in the output says
   so.

   The invariant that catches it is one line: after a drain, `totals()` equals what the workload
   left live. A churn test has to assert that before it reads any other column, because every
   column is wrong when it fails.

   The two deletion geometries are different tests. Cohort, whole groups dropped as they age, is
   the write-once shape and the easy one: segments die whole and unlink with no copying, so it
   cannot show a leak. Scattered, single keys deleted across every live segment, makes compaction
   copy live neighbours, and a reclamation claim has to survive it. A run that only does cohort
   proves less than it looks.
4. **Stresser axes not yet drawn.** Range deletes, playback walks under churn, `get_range`
   windows, and the `repair` knob. Deletes are only checked through reopen equality today, so the
   lying modes lean on served-was-attempted alone. Range deletes need the cover sweep driven, as
   above, or they assert nothing.
5. **Read-only volumes under faults.** The follower's refresh under a drawn fault plan, and
   sole-copy corruption answers on a read-only open.
6. **A multi-tail sole-copy crash sweep.** The sweep runs one tail, and the ordering argument is
   per tail, which a multi-tail leg would confirm.
7. **The ring leg of the stresser on Linux.** The tag-wrap pin only means what it says over a real
   ring.

## Two decisions the fault work has not made

- **The blocking door's give-up.** One empty drain round under a delayed completion returns
  `never_completed`, which is harsher than the fault deserves.
- **The awaited door has no give-up for a completion that never comes.** So the stresser does not
  draw `DropCompletion`. It does not draw `ListError` either.

## What a checkpoint's tests settled

| property | test | what it checks |
|---|---|---|
| restore equality | `a_checkpoint_opens_as_the_volume_it_copied`, `writes_after_the_cue_stay_out_of_the_copy` | the copy opens as the volume it copied, and later writes stay out |
| hard-link stability | `the_copy_survives_the_original_compacting_it_away` | drains the volume of every segment the copy came from, then reads the copy |
| crash before the rename | `a_crash_before_the_rename_leaves_only_staging` | every link down and synced, a directory under the staging name, nothing under the target, the live volume still answering |
| crash after the rename | `a_crash_after_the_rename_leaves_a_whole_copy` | the target whole, the staging name gone, and the copy reading every key from the segment set that stood at the rename, with nothing the call did afterwards |

The two crash tests run on the rendezvous pair. They were falsified first: swapping the two points
fails both, so each one checks its own side of the rename and cannot pass on a state both
share. Whether the published name survives a power cut stays out of reach in-process. That is the
parent sync's promise and the filesystem's.

These tests caught one defect the design missed. A tail draws its next segment when it seals,
before it next writes, so the boundary alone left the volume's live segment in the linked set, and
the copy shared an inode that then grew. The set is filtered by `is_held` now.
