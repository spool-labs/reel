# Checkpoint

A checkpoint is a durable copy of the volume at a cue. It serves a bootstrap and a backup alike. Restore is `ReelStore::open` pointed at the copy. The cost claims below are arithmetic over operations the engine already prices, since nothing here has been measured on a volume that weighs anything.

## Against an LSM checkpoint

| step | LSM store | reel |
|---|---|---|
| stop deletions | `DisableFileDeletions`, global | the cue floor, scoped to segments a cue can still read |
| immutable files | SSTs, hard linked | sealed segments, hard linked |
| mutable files | live WAL and MANIFEST copied with a size limit at the flushed prefix | none, the cue sealed every tail |
| manifest | the MANIFEST must be captured consistently | none, recovery reads the segments |
| publish | `.tmp` sibling, fsync, rename | the same |
| result | means nothing without its MANIFEST | an openable volume |

An LSM store's `CreateCheckpoint` also creates small derived files outright, re-enables deletions after the rename, and records the checkpoint's sequence number. Everything in it except the hard link exists because that engine has mutable files and a manifest. The reel is almost entirely the hard-link part.

Three facts make the reel's version smaller.

- **A cue seals the tails.** `ReelStore::cue` seals every tail before it returns a number, so every version at or below the cue sits in a sealed segment with a footer. There is no WAL, so there is no WAL-prefix problem, and the cue closed the open files.
- **The cue floor is a scoped `DisableFileDeletions`.** Compaction checks `CuePoints::floor` before it selects, so a segment holding a version some cue can see does not retire while the cue stands. Segments whose records all sit above the floor compact freely while the checkpoint links.
- **The files are the truth.** Recovery rebuilds the index from the segment files alone: footers first, an open segment's journal where a footer is missing, every record checked. A directory of sealed segments is an openable volume, and there is no manifest to capture.

## The procedure

1. Take a cue point. This seals the tails and pins the floor.
2. List the sealed segments. Two traps live here.
   - The list comes from the directory, which is what recovery reads. It keeps each file whose file name parses as a segment number below the boundary. The boundary is the number the next segment will take, read after the cue has sealed, and it keeps a later roll out of the set.
   - The boundary alone is not enough. A tail draws its next segment when it seals, before it next writes, so that segment is numbered below the boundary while the volume still appends to it, and a hard link would share an inode that then grows. The set is filtered by `is_held`, the same question compaction asks before it selects. That also covers a segment holding a record nobody has published yet.

   Every segment left is immutable, holds nothing above the cue, and cannot retire while the cue is held.
3. Create the staging directory `<target>.tmp` and hard link every listed segment into it. Only a file with a segment-number file name is linked, so the lock file stays out and a restored volume takes its own. A quarantined file has a segment-number file name too, so it is linked, and the copy's open quarantines it again. A segment gone before its link is taken fails the whole checkpoint, since it means the floor did not hold, and a copy with a hole in it is worse than no copy. There is no cross-device fallback: staging sits beside the target, so a link across devices means a target on another filesystem from the volume, and that refuses.
4. Fsync the staging directory, rename it onto the target, fsync the parent. The rename makes a crash leave either a whole checkpoint or a `.tmp` to sweep, never half of one.
5. Drop the cue.

The result opens like any volume, writable or read-only.

The call refuses up front on a read-only volume, when the target or any piece of it exists, when a `.tmp` from an unfinished attempt is still there, when two pieces would land in one place (a target inside a volume root), and when the target's file name is one the reel uses itself: a segment number, `reel.volumes`, `reel.volume` or `reel.lock`. An error before the home rename removes everything the attempt staged or published.

## What it promises

- **The copy is exactly the volume at the cue.** No linked segment holds a record above the cue, because each one sealed before the cue was handed out and new appends land in segments the listing never saw. No record at or below the cue is missing, because the seal put every such version under a footer and the floor kept its segment alive through the link pass. `writes_after_the_cue_stay_out_of_the_copy` pins this.
- **No batch is torn.** A batch is one reservation on one tail, and a seal waits out every reservation the segment holds, so the cue's seal cannot land inside a batch and a restored checkpoint holds only whole batches.
- **A crash at any step is safe.** Before the rename there is only a staging directory to delete. After it the checkpoint is whole. The linked segments need no flush of their own, since a sealed segment was synced by its seal. The only durability point the checkpoint adds is the directory entries, which step 4 syncs. `a_crash_before_the_rename_leaves_only_staging` and `a_crash_after_the_rename_leaves_a_whole_copy` cover both sides.
- **Covers and graves come along.** A checkpoint taken mid-sweep links range tombstones like any record. The restored open reinstalls covers unswept and sweeps them, which is safe at any point of the sweep.

## The costs

| cost | what it is |
|---|---|
| time | metadata only. A terabyte volume at the default segment size is about a thousand links and two fsyncs, milliseconds, plus the seal per tail the cue takes anyway. It scales with the segment count and not at all with the bytes held |
| space | the divergence. A linked segment costs nothing until compaction retires the original. Then the link keeps the file alive and the cost is the churn since the checkpoint. A checkpoint deleted after upload costs nothing, one kept for a week costs a week of churn |
| the cue | sealing cuts the active segments short, so frequent checkpoints mean more, smaller segments. The floor holds reclamation back for as long as the link pass runs, milliseconds against a maintenance tick |

## Incremental backup

Segments are immutable and numbered in order, so two checkpoints differ only in which files exist. Ship the segments the last backup lacks, drop the ones that retired, and the transfer is the churn. An LSM store needs a backup engine with its own bookkeeping to say which SSTs a backup shares with the last one. Here a directory listing is that bookkeeping, and every record has its checksum, so a backup verifies itself byte by byte on the far side. Nothing more is designed here on purpose: a remote-backup tool should build on checkpoint.

## Across several volumes

A reel spanning several volumes checkpoints in one call, and the copy is itself a multi-volume store. Hard links cannot cross devices, so each living volume stages its own segments under its own root, under the target's file name. The home piece is the target itself and holds the copy's manifest. Every other piece holds a marker with its published path, which is how the live store proves itself too. Opening the copy is the standard multi-volume open, with config listing the pieces.

The ordering keeps one atomic point. Everything stages and syncs before anything publishes. The extra pieces rename and sync first, and the home rename comes last. The copy exists exactly when the piece holding its manifest does. Whatever a crash leaves short of that, staging directories or published extras without a home, is debris the next attempt refuses over and an operator sweeps. A volume declared dead contributes nothing, so the copy of a degraded store is the survivors, whole and openable on their own.

Destroying a multi-volume store walks the same manifest. Every listed root goes under home's one lock, extras first, so a destroy that dies partway leaves the manifest standing to finish the job.

## The call

`ReelStore::checkpoint(&self, target: &Path)` returns the cue number it stood at and the segment count it linked.

It writes where every other read-side command only reads, since sealing draws the line the copy is taken at. So it opens the store as primary and takes the ownership lock. A command-line tool built on it therefore refuses, loudly, while a process holds the volume, and cannot checkpoint a running store. The running process can, over whatever admin surface it already has, by calling the same method. A store spanning several roots is one call, and the manifest lists the extra roots.

## Settled

- **A cross-filesystem target refuses.** It refuses by construction: staging is a sibling of the target, so a link across devices means a target on another filesystem, and `hard_link` says so. A `--copy` escape stays unbuilt on purpose, so nobody gets a surprise terabyte copy from a command priced in milliseconds. If that changes it needs its own verb, since the cost model is the whole difference.
- **The read-only follower cannot checkpoint.** `a_read_only_volume_refuses_to_checkpoint` holds the refusal. A follower has no tails to seal and no cue machinery, though every sealed segment it has followed is as immutable there as anywhere. A follower checkpoint would be "the volume as far as the follower has read", a different promise and a different call if it is ever built.
- **Retention sits above the engine.** The engine hands out checkpoints. How many a caller keeps and when they are deleted belongs to the caller.
