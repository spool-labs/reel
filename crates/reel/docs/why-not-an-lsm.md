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
making a multi-record batch atomic across a crash. Here an open segment's journal
keeps it: a batch's rows go in as one checksummed group. The journal holds rows,
never values, so a value is still written once, and it goes away at the seal.

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
outside it, which is what makes both an index kept in the footers and a
rebuild-from-files recovery possible without a manifest.

**Where it differs from the lineage:** the index shape is not an LSM tree of
keys. There is no level structure, no key-space partitioning across files, and
no merge that a read has to walk by construction. The open tails' keys sit in a
sharded ordered map, sharded on the leading key bytes the caller declares. Sealed
keys stay in the footers themselves, reached through the spot index or a funnel of
filters and key spans. Neither is a tree of files.

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

## Key runs

This is where the level structure comes back, for keys alone.

A get of a sealed key asks the spot index, an in-memory table of where each
sealed key's record lies, and reads the record in one device read. A walk can't do
that. The log keeps no key order, so a walk merges the footer of every sealed
segment whose key range reaches into its span, and that fan-in grows with every
seal.

A key merge bounds it. Once more than eight runs stand over one key, the
maintenance tick merges the walk's runs into a key run, a file of sorted rows that
each give the place of a footer row: which segment, and which row of its footer. The merge
keeps the newest row of each key and moves no record. The spot index never hears
of it, and a covered segment keeps its records and its footer for gets and for
recovery. A walk reads the key run in place of the footers it covers, so it merges
eight runs at most. Runs join a merge smallest first while each is no bigger than
twice what the merge has taken, so young runs merge often and cheaply, and a large
run is merged again only once the pile below it has grown to its size.

That is a leveled merge of the keys alone. WiscKey splits keys from values the same
way, and here the values already sit apart in the log. A row is 8 bytes, since the
key and the rest of the row are already in the footer, so a merge rewrites a small
share of what was written and the run adds little to the volume.

Compaction still reclaims dead space by rewriting segments, and a rewrite moves
records out from under a key run. The run keeps standing. Each copy keeps its
sequence number, and when a run's row and a rewritten copy tie, the copy wins, so a
walk that meets a row pointing into a retired segment asks the index again. A
delete stays on the volume while a key run still holds an older row of its key.

Measured on W9, a Hetzner ccx33 (8 vCPU, 32 GB), 2026-10-06: 10M keys loaded, then
180 s of fresh inserts from seven writers beside one scan thread. Reel ran
uncapped, then held at 330k puts/s. RocksDB ran at its own maximum, 285k puts/s.

| | reel, uncapped | reel at 330k puts/s | RocksDB at 285k puts/s |
|---|---:|---:|---:|
| puts/s | 994k | 330k | 285k |
| write amp | 2.61 | 2.37 | 9.55 |
| scan10/s | | 58.5k | 23.1k |
| scan100/s | | 15.1k | 12.3k |
| scan1000/s | | 2,086 | 2,264 |
| cores | | 2.85 | 7.72 |

## What the shape gives up

Named rather than argued, because each of these is real.

- **No global key order across files.** A walk merges the key runs and every
  segment no run covers yet, heap ordered by the key each cursor sits on. Key
  merges hold that at eight runs, where leveled files bound it by construction.
- **Values stay in write order.** A key run orders the keys only, so a walk of a
  thousand keys reads up to a thousand records from wherever they lie. While the
  values sit in the page cache that costs little. Past RAM each one is a device
  read, and on W9 long scans collapsed once the volume outgrew memory, at about
  206 bytes a key against RocksDB's 126.
- **A threshold that is an absolute bar.** A volume whose segments all sit just
  under it does nothing at all while its dead fraction climbs, until the tier
  escalates and lowers the bar in one step.
- **Cheap reclaim that depends on the workload.** The structure does not arrange
  for records to die together; it only exploits it when they do.

## What it gives back

- One write per byte on the ingest path, in its final position, with no second
  durable structure to reconcile at open.
- Reclaim that is an unlink for a workload whose records die together.
- An index with no level count: a get places its record for one device read.
- Values stored whole and read back verbatim, in one device op placed by the
  index, with no block framing to unpack around them.
- Recovery that is a walk of the files, because the files are the truth.

For a workload whose records are written once and expire in families, this is
strictly cheaper than a leveled tree and the arithmetic above says by how much.
For a workload that rewrites the same keys forever, it is a leveled tree with the
levels made optional, and the optional part is the part that is not done.
