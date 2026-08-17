# Volumes: one reel across several devices

One reel places its segments across several devices. The mechanism (roots, the
segment table, the manifest, the markers, and their refusals), the placement
policy (pinned tails, most-free draws, the watermark, the ENOSPC hop), the
classes with demotion, the degraded open, and the tooling (checkpoint, destroy)
are all built. This file is the design record behind them, written before the
first line of code so the invariants were fixed while they were still cheap.
The engine once ran one reel over one directory and anything finer was the
block layer's business. A deployment is expected to outgrow one drive, so
placement became the engine's business, and this file records the shape it
takes and what is rejected.

## The shape

A reel places whole segments across a list of volumes. Each volume is a
directory root, nothing more, and the engine never asks what stands behind
it, a bare NVMe, an md array, and a ZFS dataset are indistinguishable from
where it sits. Configuration grows one field, a list of volume entries, each
a path and a class. An absent list means one volume at the reel root, which
is the single-directory store byte for byte. Single device is not a
compatibility mode, it is the one-element case of the same mechanism.

This inverts one sentence the docs said until this landed. The engine's story
was one reel per volume. It becomes one reel owning a list of volumes, and
the properties the io design already calls properties of the volume, the backend
and whether its descriptors are direct, become properties of each entry. `io.md`
already treats them per filesystem, so the list gives that treatment a place to
live.

## What holds everywhere

Four invariants keep the two operator pictures equivalent and keep the hot
paths out of this file entirely.

- No record spans volumes. Placement is segment-granular, the granularity at
  which the engine already allocates, seals, compacts, and scrubs.
- Segment ids stay global and monotonic. A volume holds a subset of the
  numbering, never its own numbering.
- The index never encodes a path. It maps a key to a segment and an offset,
  as it does today, and a table built at open maps segment to volume. The
  table is an array indexed by segment number, one lookup, no lock.
- Open discovers segments by scanning every root, exactly as it scans one
  root today. The scan sorts by id across roots, and recovery order is
  untouched because order was never a property of the directory.

Under these, a store is physically portable between pictures. Segment files
copied from a dying array onto separate drives, with the new list in config,
open as the same store.

## Placement

Tails pin to volumes. `active_tails` resolves to at least one tail per fast
volume, each tail draws its next segment on its own volume, and the existing
least-loaded routing becomes device balancing for free, a backed-up drive
accumulates queue and stops attracting writes. Preallocation and roll stay
on-volume, so the roll path is unchanged.

Within a class, a new segment goes to the volume with the most free space.
That single rule absorbs mixed drive sizes and absorbs growth, a drive added
to a running store is one config entry, the empty volume wins every draw
until it catches up, and compaction's rewrites finish the rebalancing
without being asked. There is no reshape and there is no rebalancer,
compaction already is one.

Each volume carries a free-space watermark feeding admission, so one full
drive degrades placement rather than failing writes. ENOSPC on a draw
retries on the next volume in class and only fails when the class is full.

## Classes

Two classes, fast and capacity. Tails and fresh segments live on fast.
Compaction places its output in the same class by default, and demotes
survivors past an age threshold to capacity when a capacity class exists.
Demotion rides work compaction was already paying for, the segment was being
rewritten anyway, only the destination changes.

This is the arrangement that makes a mixed box price correctly. The SSDs are
the ingest surface and the hot tier, sized for rate. The HDDs are capacity,
sized for bytes. Neither is sized for the other's job, and a striped mix of
the two, which runs at the slow member's speed, stops being the only way to
present both to the engine.

A capacity volume wants different knobs than a fast one, larger
`segment_bytes` so a spindle's work stays sequential, readahead on for scan
service, possibly a different backend. Per-volume overrides of the io fields
cover that, and defaults per class are a measurement question, not a design
one.

## Either picture works

The operator chooses the aggregation layer, and the engine is indifferent.

One entry pointing at a RAID0 is legal and is the single-directory
deployment unchanged. Aggregation happens below, the blast radius of a member
failure is the whole store, and that trade is the operator's to make,
redundancy above the engine tolerates the loss either way. N entries over raw
drives moves aggregation into placement, and what a member failure costs drops
from the store to one drive's segments. Hybrids compose, a class describes a
volume, not how the volume was built, so two SSDs striped into one fast entry
alongside four raw capacity entries is a legal list.

The recommendation, as documentation rather than enforcement: raw volumes,
one entry per drive. A striped entry trades away bounded loss, and bounded
loss is worth more here than in a filesystem, because what backs a lost
segment is not a backup but a re-fetch from peers, priced by the byte.

## Losing a volume

The dangerous confusion is not a dead drive, it is a missing mount read as
one. A reel that opens with a root absent and concludes those segments are
gone would trigger re-fetch, or self-report loss, over an fstab typo.

Two files close that. The manifest on the first volume names every root, so
a root dropped from configuration refuses by name. And every root past the
first carries a marker naming itself, because an unmounted mountpoint is an
empty directory standing exactly where the volume should be, and the marker
is the only thing that tells it apart from a fresh volume just added. A
manifest-named root that cannot show its marker refuses the open, names the
volume, and stops.

The explicit dead flag on a volume's config entry is the operator stating
the drive is truly dead, and only then does the reel open over what
remains. The entry stays in the list, because the placement table is
indexed by list order; the flag takes the root out of every scan, every
draw, and every pin, and what the degraded open provides is that each
record the drive held answers as a clean miss rather than an error. The
layer above, which holds the authoritative record of what should exist,
walks those misses into a targeted re-fetch list. The design once said the
reel would enumerate the missing segment ids; it cannot and does not need
to. Placement is derived from the scan by construction and never persisted,
so a dead volume's ids are unknowable, and they would name nothing anyway:
the index entries for the lost records died with the volume's footers, so
ids map to no keys. The authority on what should exist is the caller, and
the degraded reel's job is to make asking it cheap. Recovery is still the
difference between resyncing one drive's segments and resyncing the whole
store, and the exposure window is shorter accordingly.

The lock stays one lock. `reel.lock` lives on the first volume, the
manifest beside it, and holding the first volume's lock is holding the
store. The first volume is also always fast and never declarable dead:
losing it is losing the store's identity, and rebuilding from peers is the
honest recovery.

## Performance

The hot paths do not appear in this design, which is the main performance
claim. Append and read code are untouched, placement only decides where a
drawn segment's file lands, and the one new read-path cost is the
segment-to-volume array lookup.

Write aggregation comes from the tails. Routing already fans a single
producer's batches across tails, so with tails pinned one per volume, one
writer drives every fast device and ingest sums across them, the same
aggregate a stripe gives. LSN order is already global across tails and the
durable point already takes the minimum across them, so ordering and
durability are indifferent to where the files sit. Roll fsyncs the segment's
own directory, which is now per-volume, same count of syncs, different
directory fd.

On spindles the pinning is better than a stripe, not equal to it. A pinned
tail gives each HDD one pure sequential stream. A stripe interleaves every
concurrent stream, tails plus compaction, across every member at chunk
granularity, and every member seeks. Sequential purity per spindle is the
entire performance model of cheap capacity, and it is the thing only
placement can promise.

Reads distribute statistically in both pictures, by segment here, by chunk
offset there, and neither holds an advantage worth claiming without a
measurement. What placement adds is class routing, point reads served from
fast volumes' page cache, scans hitting capacity volumes with readahead on,
and per-volume backend choice under one store, which a stripe cannot
express at all.

## The prior art

A general-purpose LSM store carries the nearest prior art, `db_paths` with
target sizes, `cf_paths` per column family, `wal_dir` for the log device. It
validates the granularity, an SST never spans paths, files place whole,
exactly the segment rule here. Everything else is instructive by absence.
Placement is capacity spillover only, newer data in earlier paths until a
target size, with no class or temperature model. There is no manifest and
no degraded open, a missing path is a broken database that cannot say what
it lost, so recovery is a restore rather than a targeted re-fetch. And the
feature is a sparsely used corner with known rough edges in compaction
output sizing. The design here keeps the granularity and builds the missing
halves, class-aware placement and enumerable loss.

## Rejected

- Block striping inside the engine. Interleaving a record across devices
  rebuilds md RAID0 without its maturity and surrenders bounded loss while
  doing it, strictly dominated by handing a stripe to the engine as one
  volume.
- Intra-node parity. Redundancy belongs to the layer above, where it is
  priced and challenged at its own scale. Parity inside one store pays for
  redundancy twice, and a drive loss is designed to be a bounded re-fetch
  instead.
- Per-volume segment numbering. It would let two volumes disagree about an
  id and buys nothing, the global sequence is what makes recovery order and
  the segment table trivial.
- Path in the index. Encoding placement into index entries couples the
  index format to the volume list and breaks portability between pictures
  for zero lookups saved.

## What is not yet measured

The mechanism runs; none of the numbers below has been taken. Per the standing
rule, device numbers come from real hardware, never a mac and never a VM.

- Pinned tails against a stripe on the same SSDs, ingest and mixed
  read-write, to put a number on "the same aggregate."
- The spindle claim, one sequential stream per HDD against a striped pair
  under concurrent tails plus compaction. This is the largest claimed win and
  it stands on the argument alone.
- Demotion rate against compaction bandwidth, whether demotion by age
  (counted in bytes of later ingest, at half the fast tier's capacity) keeps fast
  volumes from filling under sustained ingest.
- Capacity-class segment size, whether larger segments on spindles earn
  their longer scrub and compaction units.

## Open questions

- Whether a volume is the right unit on a multi-actuator drive. Pinning one
  tail per device assumes a device seeks once at a time, which a Mach.2 or an
  HS760 does not: two arm assemblies serve half the platters each and Linux
  publishes one concurrent positioning range per actuator under
  `queue/independent_access_ranges`. The bias pass reads and reports the count
  and the spans; nothing places on them. Two points to settle before anything
  does. The unit would be (device, range) rather than device, since the ranges
  are a fact about the disk while a volume is a filesystem, and two volumes cut
  from one drive can land in the same actuator's span and be one stream while
  the placement table believes they are two. And a SAS drive of this kind
  already presents its actuators as separate LUNs, so it needs nothing: the
  question is only live for a single-device drive, where exploiting the split
  means partitioning at the range boundary. Unmeasured, and no drive in reach
  has a second actuator to measure with.

One question this list used to hold is settled. There is no missing-segment
set to channel, per the Losing a volume section, so degraded state is reported
as the dead volumes themselves (`dead_volumes()`), and the re-fetch list is
the caller's to build out of clean misses. Checkpoints span the volumes by
per-volume staging (`checkpoint.md`), and destroy walks the manifest.
