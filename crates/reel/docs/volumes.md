# Volumes

One reel places whole segments across several devices, and everything on this page except the open question is built. A checkpoint stages each living volume under its own root, and destroy walks the manifest.

## The shape

Each volume is a directory root and nothing more. The engine never asks what stands behind it: a bare NVMe, an md array and a ZFS dataset look the same from where it sits. Config has one field for it, `volumes`, a list of entries that each hold a path, a class and a dead flag. The reel's own root is always the first volume. An empty list means one volume at the reel root, which is the single-directory store byte for byte. A single device is the one-element case of the same mechanism.

The io backend and direct mode are store-wide settings. No volume entry overrides them.

## What holds everywhere

These four invariants keep every layout equivalent and keep the hot paths out of this design.

- No record spans volumes. Placement is per segment, the unit at which the engine already allocates, seals, compacts and scrubs.
- Segment ids stay global and monotonic. A volume holds a subset of the numbering, never its own numbering. One id found on two volumes refuses the open.
- The index never encodes a path. It maps a key to a segment and an offset, and a table built at open maps segment to volume. The table is an array indexed by segment number, one lookup under a read lock.
- Open finds segments by scanning every root. The scan sorts by id across roots, and recovery order is untouched because order was never a property of the directory.

So a store is portable between layouts. Segment files copied from a dying array onto separate drives, with the new list in config, open as the same store.

## Placement

| draw | rule |
|---|---|
| a tail's fresh segment | tail i pins to the ith living fast volume and draws there while that volume stays above the watermark |
| a tail past the fast list, or a pinned volume under the watermark | the volume in class with the most free space above the watermark, else the most free in class |
| ENOSPC on a draw | retry on the next volume in class, and fail only when the class is full |
| compaction output | with a capacity tier, a reserved tail (one per compaction pass) that draws in the class the pass chose. Otherwise the least-loaded tail |

`active_tails` resolves to at least one tail per fast volume. Least-loaded routing across tails becomes device balancing for free: a backed-up drive builds a queue and stops attracting writes. Preallocation and roll stay on the volume, so the roll path is unchanged.

Most-free placement absorbs mixed drive sizes and growth. A drive added to a running store is one config entry. The empty volume wins every unpinned draw until it catches up, and compaction's rewrites finish the rebalancing. There is no reshape and no rebalancer.

Each volume keeps a free-space watermark of `WATERMARK_SEGMENTS` (2) segments, the segment being drawn plus room for compaction output beside it. Under it the volume stops attracting draws, so one full drive degrades placement and writes keep going. The class boundary still holds: a full fast tier never spills fresh writes onto capacity volumes. The write path's admission guard reads the footprint summed over every volume.

## Classes

Two classes, fast and capacity. Tails and fresh segments live on fast. Compaction writes its output in the source's class. A fast segment's survivors demote to capacity once later ingest has passed it by half the fast tier's capacity and a capacity volume exists. Demotion is one way, and it rides work compaction was already paying for: the segment was being rewritten anyway, and only the destination changes.

This is what makes a mixed box price correctly. The SSDs are the ingest surface and the hot tier, sized for rate. The HDDs are capacity, sized for bytes. A striped mix of the two runs at the slow member's speed, and it stops being the only way to present both to the engine.

A capacity volume would want different knobs from a fast one: larger `segment_bytes` so a spindle's work stays sequential, readahead on for scans, maybe a different backend. None of those can be set per volume today. Defaults per class are a measurement question.

## Either layout works

The operator picks the aggregation layer, and the engine doesn't care.

| layout | where aggregation happens | what a member failure costs |
|---|---|---|
| one entry on a RAID0 | below the engine | the whole store |
| one entry per raw drive | in placement | one drive's segments |

Hybrids compose, since a class describes the volume and ignores how it was built. Two SSDs striped into one fast entry beside four raw capacity entries is a legal list. Redundancy above the engine tolerates the loss either way.

The recommendation, as documentation with no enforcement: raw volumes, one entry per drive. A striped entry gives up bounded loss, and bounded loss is worth more here than in a filesystem, because a lost segment comes back as a re-fetch from peers, priced by the byte.

## Losing a volume

The danger is a missing mount read as a dead drive. A reel that opened with a root absent and decided those segments were gone would trigger re-fetch, or report loss, over an fstab typo. Two files close that.

- **The manifest**, `reel.volumes` on the first volume, lists every root in order. A root dropped from config, or a reordered list, refuses the open. New roots append, which is how a drive is added. A single-volume store writes no manifest until it grows a second root, and a read-only open verifies without writing.
- **The marker**, `reel.volume` on every root past the first, holds that root's own path. An unmounted mountpoint is an empty directory standing exactly where the volume should be, and the marker is the only thing that tells it from a fresh volume. A root in the manifest that cannot show its marker refuses the open, reports the volume, and stops. A marker with some other path means two stores' volumes are crossed.

The dead flag on a volume's config entry is the operator saying the drive is truly dead, and only then does the reel open over what remains. The entry stays in the list, because the placement table is indexed by list order. The flag takes the root out of every scan, draw and pin. In the degraded open each record the drive held answers as a clean miss with no error. The layer above knows what should exist, and it turns those misses into a targeted re-fetch list.

The reel cannot list the missing segment ids, and doesn't need to. Placement comes from the scan and is never persisted, so a dead volume's ids are unknowable, and they would map to no keys anyway, since the index entries for the lost records died with the volume's footers. The degraded reel's job is to make asking the caller cheap. Recovery is then resyncing one drive's segments, which shortens the exposure window against resyncing the whole store. Degraded state is reported as the dead volumes themselves, `dead_volumes()`.

The lock stays one lock. `reel.lock` lives on the first volume with the manifest beside it, and holding the first volume's lock is holding the store. The first volume is always fast and can never be declared dead: losing it loses the store's identity, and rebuilding from peers is the honest recovery.

## Performance

The hot paths do not appear in this design, which is the main performance claim. Append and read code are untouched. Placement only decides where a drawn segment's file lands, and the one new read-path cost is the segment-to-volume lookup.

Write aggregation comes from the tails. Routing fans a single producer's batches across tails, so with one tail pinned per volume one writer drives every fast device, the same aggregate a stripe gives. LSN order is global across tails and the durable point takes the minimum across them, so ordering and durability don't care where the files sit. A roll syncs the segment's own directory, now per volume: the same count of syncs on a different directory.

On spindles pinning beats a stripe. A pinned tail gives each HDD one pure sequential stream. A stripe interleaves every concurrent stream, tails plus compaction, across every member at stripe-unit granularity, and every member seeks. Sequential purity per spindle is the whole performance model of cheap capacity, and only placement can promise it.

Reads spread statistically in both layouts, by segment here and by stripe offset there, and neither has an advantage worth claiming without a measurement. What placement adds is class routing: point reads served from fast volumes' page cache, and scans hitting capacity volumes with readahead on.

## Prior art

A general-purpose LSM store has the nearest prior art: `db_paths` with target sizes, `cf_paths` per column family, `wal_dir` for the log device. An SST never spans paths and files place whole, the same rule as segments here. Placement there is capacity spillover only, newer data in earlier paths until a target size, with no class or temperature model. There is no manifest and no degraded open, so a missing path is a broken database that cannot say what it lost, and recovery is a full restore. The feature is a sparsely used corner with known rough edges in compaction output sizing. This design keeps the granularity and adds class-aware placement and a degraded open.

## Rejected

- **Block striping inside the engine.** Interleaving a record across devices rebuilds md RAID0 without its maturity and gives up bounded loss doing it. Handing a stripe to the engine as one volume beats it outright.
- **Parity inside a node.** Redundancy belongs to the layer above, where it is priced and challenged at its own scale. Parity inside one store pays for redundancy twice, and a drive loss is designed to be a bounded re-fetch.
- **Segment numbering per volume.** It would let two volumes disagree about an id and buys nothing. The global sequence is what keeps recovery order and the segment table trivial.
- **Paths in the index.** Encoding placement into index entries ties the index format to the volume list and breaks portability between layouts, for zero lookups saved.

## Not yet measured

The mechanism runs, and none of these numbers has been taken. Device numbers come from real hardware, never a mac or a VM.

- Pinned tails against a stripe on the same SSDs, for ingest and mixed read-write, to put a number on "the same aggregate".
- The spindle claim: one sequential stream per HDD against a striped pair under concurrent tails plus compaction. This is the largest claimed win and it stands on the argument alone.
- Demotion rate against compaction bandwidth: whether demotion by age (bytes of later ingest, at half the fast tier's capacity) keeps fast volumes from filling under sustained ingest.
- Capacity-class segment size: whether larger segments on spindles earn their longer scrub and compaction units.

## Open

**Whether a volume is the right unit on a multi-actuator drive.** Pinning one tail per device assumes a device seeks once at a time. A Mach.2 or an HS760 doesn't: two arm assemblies serve half the platters each, and Linux publishes one positioning range per actuator under `queue/independent_access_ranges`. The bias pass reads and reports the count and the spans, and nothing places on them. The unit would be the pair (device, range), since the ranges belong to the disk and a volume is a filesystem: two volumes cut from one drive can land in one actuator's span and be one stream while the placement table counts two. A SAS drive of this kind already presents its actuators as separate LUNs and needs nothing, so the question is live only for a single-device drive, partitioned at the range boundary. It is unmeasured, and no drive in reach has a second actuator.
