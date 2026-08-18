# Why not an LSM tree

This is a design rationale rather than an argument. The log-structured merge
lineage produced most of the ideas this engine runs on, and the places where it
departs are departures with a price attached, named here so nobody has to
rediscover them.

The lineage's shape, stated plainly so the departures have something to be
measured against: writes are buffered in memory, flushed as sorted immutable
files, and those files are organised into levels with bounded overlap. A read
merges across the levels. Compaction rewrites files down the levels to keep the
overlap and the file count bounded. A write-ahead log makes the memory buffer
durable before the flush.

Four of those five have a counterpart here. The write-ahead log has none, and
the level structure is optional and off.

## The log is the store, so there is no write-ahead log

A write-ahead log exists to give a store a cheap sequential place to make a
write durable before it reaches its real home, which is otherwise a random write
into a tree. Here the record's real home already is a sequential append at the
end of a segment, and it is written there once, in its final position. A log in
front of that would write every byte twice and sync twice, to protect a window
between the log and the store that does not exist.

The one thing a write-ahead log adds that is genuinely needed is a commit marker
making a multi-record batch atomic across a crash, because a log record can
carry one. That is what the batch frame is, and it costs one header record at
the front of a batch rather than a second file.

Three consequences follow from that absence, and they are larger than the saved
write.

**"The files are the truth and the index is a cache" is a complete statement.**
There is no second durable structure holding writes the segments do not have, so
there is nothing to reconcile at open. Recovery reads the segment files in
number order and derives the index from them. A sealed segment comes from its
footer alone; the one unsealed segment per tail is walked record by record.

**A crash has one boundary to reason about, not two.** What a power cut can take
under the default policy is the active segment of each tail since its last seal.
There is no separate question about how far the log had been replayed.

**A durable copy of the volume is a set of hard links.** Nothing has to be
captured mid-write, because a cue seals every tail first and every version at or
below it then sits in a sealed file. There is no log prefix to pin and no
manifest naming a file set that has to be captured atomically with it.

## Keys and values are separated, and the value log is the primary structure

Separating keys from values is the lineage this engine belongs to. The argument
was made and measured by the WiscKey work (Lu et al., USENIX FAST 2016,
*WiscKey: Separating Keys from Values in SSD-Conscious Storage*): an LSM's
compaction cost is dominated by moving values that never needed to move, since
only the keys ever needed to be sorted. Put the values in a separate append-only
log and keep the keys in the tree, and compaction rewrites a small fraction of
the bytes it used to.

This engine takes the separation further, in two ways that change what the
structure is rather than how much of it moves.

**The value log is primary.** There is no key tree that owns the store with a
value log hanging off it. There is a log, and the index rides it. Every design
question is asked of the log first: a write is an append, a delete is an append,
a compaction is an append and an unlink. The index is derived from the files and
can be thrown away.

**The index materialises inside the log.** A seal writes the segment's index
into the end of that same segment as a footer: a packed sorted index of the rows
the segment took, partitioned by column, each partition sorted by key and
strided at that column's own key width, behind a directory and a fixed tail. A
sealed segment is therefore self-describing. It can be indexed with no structure
outside it, which is what makes both a paged index and a rebuild-from-files
recovery possible without a manifest.

**Where it differs from the lineage:** the index shape is not an LSM tree of
keys. There is no level structure, no key-space partitioning across files, and
no merge that a read has to walk by construction. The resident form is a sharded
ordered map, sharded on the leading key bytes the caller declares. The paged
form is the footers themselves, reached through a funnel of filters and key
spans. Neither is a tree of files.

One thing the separation gives back for free: the garbage collection problem the
value log creates is not a second mechanism bolted next to a tree compaction. It
is the whole reclaim story, and the next section is it.

## Reclaim is whole-segment, not leveled rewrite

Leveled compaction rewrites a file to merge it into the level below, so a byte is
written once per level it passes through and the write amplification is roughly
the level count times the size ratio. That is the price of the property leveling
buys, which is a bounded number of files a read has to consult.

Here a segment is retired, never merged down. There are two paths and no third.

**A segment with nothing live left is unlinked.** No bytes copied at all. The
charge that would pace the pass is therefore zero, so the drain that finds them
is ungated, and what bounds it is the cost of a directory operation rather than
of a device transfer.

**A segment with survivors has them copied into an active tail**, and its file is
unlinked once those copies are durable. Rewriting a segment that is `r` dead
copies its `1 - r` live bytes and frees its `r` dead ones, so bytes written per
byte reclaimed is `(1 - r) / r`: 0.11 at 0.90 dead, 1.00 at 0.50, 4.00 at 0.20.

Selection is greedy on the highest reclaimable fraction, which changes what the
threshold is for. **The threshold decides what is allowed and never what is
chosen.** Whatever the bar is set to, the segments actually rewritten are the
ones nearest the top of the ranking, so lowering it admits more work rather than
making each unit of work more expensive. `compaction.md` carries the sweep that
measured how far apart those two things are.

Two structural facts sit behind that. A byte is copied once per rewrite it
survives and never once per level, because there is no level to pass through.
And segments overlap in key space freely, because a segment is a stretch of the
log rather than a stretch of the key space.

That second fact is the whole trade. It is what makes whole-segment death
possible: records written at about the same time sit together, so a family of
records that expires together takes whole segments with it. It is equally what
makes the scattered case expensive, and the honest version of the claim is that
almost every cheap answer here is cheap only for the workload whose dead bytes
arrive in whole segments. A workload that rewrites the same keys forever leaves
its dead bytes scattered inside segments that are otherwise live, and that is the
copy path every time.

## Carry: the small-value case the separation makes worse

Separating keys from values costs a read. Where values sit inline in a file
block, a key found is a value found. Here a resolved key names a record that
still has to be fetched, and for a small value that fetch is the entire cost.

Three answers, all bounded, all per column rather than per volume.

- **Up to four bytes ride in the index entry.** A column declaring `inline_max`
  spends padding the entry already had, so the ceiling is free up to it and costs
  eight bytes on every key of every column past it.
- **Up to 256 bytes ride in the sealed footer row.** A column declaring
  `row_carry` puts a value's leading bytes in the row, so a warm point read is
  answered by the block the search was already going to fetch. Unlike the entry's
  inline bytes, which every resident key pays for, a row is read a block at a
  time and only the reader who wanted that block pays for what it carries.
- **Between the two, `carried_budget` bounds** what the resident index holds
  beside its entries, shed coldest first on the maintenance tick. Unset, nothing
  is shed and everything carried stays resident.

A carrying row carries its own checksum, because a row that answers from itself
is not covered by the record's checksum the way a row that names a record is.

The structural point is that this is a value-side answer to a value-side problem,
paid by the columns that have it. There is no store-wide setting that trades the
large-record path for the small-record one, because the two never share a
mechanism.

## Standing sorted runs and the collapse

This is where the level structure comes back, as an option.

A read that misses has to ask every candidate segment, because a segment number
is not a version: several tails write at once, so a newer record can land in a
lower-numbered segment than the one it replaces. With the index resident the map
answers and the candidate count costs nothing. With the index paged, asks per get
equal the standing run count for any key that misses.

Two knobs turn that into a leveled shape, and neither is on by default.

**`rewrite_on_seal`** makes a segment sorted by key when it seals. It costs
nothing extra: the seal is already reading and writing everything, and the
footer's rows are already in key order, so applying them in that order lands a
sorted run at no new io.

**`merge_sorted_runs`** collapses the standing runs into one. The pass reads the
runs together in key order, keeps the newest version of every key, and retires
the sources once its own output is sealed. Every tombstone is carried forward
whatever it shadows, so no merge can resurrect a deleted key, and a run holding a
version an open cue point can still read is not selected at all. It requires
`rewrite_on_seal` and is refused without it, because a volume that does not seal
by rewriting produces nothing sorted to merge.

That is a leveled merge in everything but the level count, and the position is
that it should be a choice. What it buys is the read amplification a level
structure buys by construction. What it costs is the write amplification a level
structure pays by construction. A write-only volume that is never searched should
pay neither.

**What is not finished, stated plainly.** With `compact_dead_ratio` and
`merge_dead_ratio` both at their 0.50 defaults the two triggers compose badly:
reclaim keeps the standing stack's dead share under the collapse trigger, so the
collapse never fires. A control run left 427 standing runs and zero merges, and
the paged arm paid for it at 15.9 asks per get and a 10.8 ms p99, measured on a
64-thread EPYC 9375F, 2026-08. Separately, a merge pass whose rows carry lists
them into one output segment that never rolls, so the pass mints a segment
bounded only by the live set. Until the merge bounds its output, 0.50 is the
shipped default and the collapse is a knob to leave alone.

## What the shape gives up

Named rather than argued, because each of these is real.

- **No global key order across files.** A prefix scan is a merge over every
  segment whose key range reaches into the span, heap ordered by the key each
  cursor sits on. Leveled files bound that fan-in by construction; here it is
  whatever the workload left standing.
- **No bounded read amplification without arming the merge**, and the merge is
  not finished.
- **A threshold that is an absolute bar.** A volume whose segments all sit just
  under it does nothing at all while its dead fraction climbs, until the tier
  escalates and lowers the bar in one step.
- **Cheap reclaim that depends on the workload.** The structure does not arrange
  for records to die together; it only exploits it when they do.

## What it gives back

- One write per byte on the ingest path, in its final position, with no second
  durable structure to reconcile at open.
- Reclaim that is an unlink for a workload whose records die together.
- A resident index that is a choice rather than a level count: one op per read
  while the memory is there, two when it is not.
- Values stored whole and read back verbatim, in one device op placed by the
  index, with no block framing to unpack around them.
- Recovery that is a walk of the files, because the files are the truth.

For a workload whose records are written once and expire in families, this is
strictly cheaper than a leveled tree and the arithmetic above says by how much.
For a workload that rewrites the same keys forever, it is a leveled tree with the
levels made optional, and the optional part is the part that is not done.
