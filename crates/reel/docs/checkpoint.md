# Checkpoint: a durable copy of the volume at a cue

`src/reel/checkpoint.rs` and `ReelStore::checkpoint`, with `tests/checkpoint.rs`
holding the promises. Checkpoint and restore are what stand between this engine
and being the only store a caller needs, for a bootstrap and for a backup both.
This is the design record: what an LSM store does, why the reel's version is
structurally smaller, the procedure, and what it promises. The cost claims are
arithmetic over operations the engine already prices, since nothing here has
been measured on a volume that weighs anything.

## What an LSM store does

Read against a shipping implementation,
`utilities/checkpoint/checkpoint_impl.cc`. `CreateCheckpoint` stages into a
`.tmp` sibling of the target directory, calls `DisableFileDeletions` so
compaction cannot unlink anything while the set is being captured, enumerates
the live files, then walks them with three callbacks: immutable SSTs are hard
linked, mutable files are copied with a size limit pinning the copy to the
flushed prefix, which is how a live WAL and MANIFEST are captured mid-write,
and small derived files are created outright. It fsyncs, renames the staging
directory onto the target, re-enables deletions, and records the sequence
number the checkpoint stands at.

Everything in that dance that is not a hard link exists because that engine
has mutable files and a manifest: the SST set means nothing without the MANIFEST
naming it, the MANIFEST is appended to concurrently, and the WAL holds the
unflushed memtable. The link trick itself, immutable file plus hard link
equals free copy, is the part worth taking, and the reel is almost entirely
made of it.

## Why the reel's version is smaller

Three standing facts do most of the work.

**A cue seals the tails.** `ReelStore::cue` seals every tail before it hands
back a number, so every version at or below the cue sits in a sealed segment
with a footer. There is no WAL-prefix problem because there is no WAL, and no
open-file problem because the cue closed them.

**The cue floor already is a scoped `DisableFileDeletions`.** Compaction
consults `CuePoints::floor` before selecting a target, so a segment still
holding a version some cue can see is not retired while the cue stands. The
hold is scoped where the LSM engine's is global: segments whose records all sit
above the floor compact freely while the checkpoint links.

**The files are the truth.** Recovery rebuilds the index from segment files
alone, footers first, record walk where a footer is missing, every record
checksummed. A directory of sealed segments is not an input to a restore
procedure, it is an openable volume. There is no manifest to capture
consistently because there is no manifest at all.

## The procedure

1. Take a cue point. This seals the tails, settles the sealed queue, and
   pins the floor.
2. Enumerate the sealed segments. This is the step with two traps in it.

   Not from the index's segment table: that table books footprint per record,
   and a destination holding nothing but listed rows has no record to book, so
   enumerating from it would miss a segment whose footer is the only home of a
   carried value. The directory is what recovery itself reads, and the number
   the next segment will take, read once the cue has sealed, is the boundary
   that keeps a later roll out of the set.

   And the boundary alone is not enough. A tail draws its next segment when it
   seals rather than when it next writes, so that segment is numbered below the
   boundary while still being the one the volume appends to, and a hard link
   shares an inode that then grows. The set is filtered by `is_held`, which is
   the same question compaction asks before it selects, and which covers the
   other case too: a segment holding a record nobody has published yet.

   Every segment left after that sealed at or below the cue, is immutable, and
   cannot retire while the cue is held.
3. Create the staging directory `<target>.tmp`. Hard link every enumerated
   segment into it. The lock file is not linked, since a restored volume takes
   its own, and quarantined files are not linked, since they were never part of
   the volume; both fall out of enumerating by segment number rather than by
   directory entry. A segment gone before its link is taken fails the whole
   checkpoint rather than being skipped, since it means the floor did not hold
   what it was supposed to and a copy with a hole in it is worse than no copy.
   There is no cross-device fallback: a staging directory beside the target is
   on the target's filesystem, so a link that crosses a device means a target on
   another filesystem from the volume, which the section below records as a
   refusal.
4. Fsync the staging directory, rename it onto the target, fsync the parent.
   The rename is what makes a crash leave either a whole checkpoint or a
   `.tmp` to sweep, never a half one.
5. Drop the cue.

The result opens like any volume, writable or read-only. Restore is not a
procedure; it is `ReelStore::open` pointed at the directory.

## What it promises, and the arguments

**The copy is exactly the volume at the cue.** No linked segment holds a
record above the cue, because every one of them sealed before the cue was
handed out and new appends land in segments the enumeration never saw. No
record at or below the cue is missing, because the seal put every such
version under a footer and the floor kept its segment alive through the link
pass. `writes_after_the_cue_stay_out_of_the_copy` is the pin.

**No batch is torn.** A batch is one reservation on one tail, and a seal
waits out every reservation the segment holds, so the cue's seal cannot land
inside a run. A restored checkpoint keeps whole batches for the same reason
recovery does: the frame and the run it declares are both in the linked bytes.

**A crash at any step is safe.** Before the rename there is only a staging
directory to delete, after it the checkpoint is whole. The linked segments
need no flush of their own: a sealed segment was synced by its seal, and the
one durability point the checkpoint adds is the directory entries, which step
4 takes.

**Covers and graves come along correctly.** A checkpoint taken mid-sweep
links range tombstones like any record. The restored open reinstalls covers
unswept and the re-run is the cheap no-op `durability.md` already argues for
a crash at any point of the sweep.

## The costs

**Time: metadata only.** A terabyte volume at the default segment size is
about a thousand links and two fsyncs, milliseconds, plus one footer write
and sync per tail for the seal the cue takes anyway. Nothing scales with the
bytes held, only with the segment count.

**Space: the divergence, not the copy.** A linked segment costs nothing
until compaction retires the original, at which point the checkpoint's link
keeps the file alive and the cost is the delta between then and now. This is
the standard hard-link story and it is the operator's dial: a checkpoint
deleted after upload costs nothing, one kept for a week costs a week of
churn.

**The cue's own cost.** Sealing cuts the active segments short, so frequent
checkpoints mean more, smaller segments, and the floor holds reclamation
back for as long as the link pass runs, which is milliseconds against a
maintenance tick.

## Incremental backup falls out

Segments are immutable and numbered monotonically, so two checkpoints differ
only in which files exist: ship the segments the last backup lacks, drop the
ones that retired, and the transfer is the churn rather than the volume.
An LSM store needs a backup engine with its own bookkeeping to say which SSTs
a backup shares with the last one; here a directory listing is that
bookkeeping, and every record carries its checksum so a backup verifies
itself byte by byte on the far side. Nothing more is designed here on
purpose: a remote-backup story should be a consumer of checkpoint, not a
sibling of it.

## Across several volumes

A reel spanning several volumes checkpoints as one call, and the copy is
itself a multi-volume store. Hard links cannot cross devices, so each living
volume stages its own segments under its own root, carrying the target's
name: the home piece is the target itself and holds the copy's manifest, and
every other piece holds a marker naming its published path, which is exactly
how the live store proves itself. Opening the copy is the standard
multi-volume open, config naming the pieces.

The ordering keeps the one atomic point. Everything stages and syncs before
anything publishes; the extra pieces rename and sync first; the home rename
comes last. The copy exists exactly when the piece holding its manifest
does, and whatever a crash leaves short of that, staging directories or
published extras without a home, is debris the next attempt refuses over and
an operator sweeps. A target whose name the reel would mistake for its own
files, a segment number or a reserved name, refuses before anything seals.
A volume declared dead contributes nothing, so the copy of a degraded store
is the survivors, whole and openable on its own.

Destroying a multi-volume store walks the same manifest: every named root
goes under home's one lock, extras first, so a destroy that dies partway
leaves the manifest standing to finish the job.

## The surface

One engine call: `ReelStore::checkpoint(&self, target: &Path)`, returning the
cue number it stood at and the segment count it linked, erroring if the target
or any piece of it exists.

A caller exposing this as a command should know that it writes where every
other read-side command reads, because sealing is what draws the line the copy
is taken at, so it opens the store primary and takes the ownership lock. A
command-line tool built on it therefore refuses while a process holds the
volume, loudly, and cannot checkpoint a running store. What can is that
process itself, over whatever admin surface it already has, calling the same
engine method. A store spanning several roots is one call, and naming the
extra roots is the manifest's job, per `volumes.md`.

## Settled and open, in order of consequence

- **Whether a cross-filesystem target refuses or quietly copies.** It refuses,
  by construction rather than by a check: the staging directory is a sibling of
  the target, so a link across devices is a target on another filesystem and
  `hard_link` says so. A `--copy` escape stays unbuilt on
  purpose, so nobody gets a surprise terabyte copy out of a command priced as
  milliseconds. What is owed if that changes is a distinct verb name, since the
  cost model is the whole difference.
- **Whether the read-only follower can checkpoint.** It cannot, and
  `a_read_only_volume_refuses_to_checkpoint` holds the refusal. It has no tails
  to seal and no cue machinery, but every sealed segment it has already followed
  is as immutable there as anywhere. A follower checkpoint would be "the volume
  as far as the follower has read", which is a different promise and should be
  named differently if it is ever built.
- **Retention above the engine.** The engine hands out checkpoints; how many a
  caller keeps and when they are deleted is policy, and belongs with the
  caller.
