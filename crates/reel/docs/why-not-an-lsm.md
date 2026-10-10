# Why not an LSM tree

Most of this engine's ideas come from the log-structured merge lineage. An LSM buffers writes in
memory behind a write-ahead log, flushes them as sorted immutable files in levels with bounded
overlap, merges across levels on a read, and rewrites files down the levels in compaction. Here is
where each part went.

| LSM part | here |
|---|---|
| memory buffer | the open tails' keys in a sharded ordered map. Values go straight to their segment |
| sorted immutable files | sealed segments in write order, each ending in a footer sorted by key |
| levels with bounded overlap | none for values. Key runs bring back a leveled merge for keys alone |
| a read merges across levels | a get reads one record through the spot index. A walk merges key runs and uncovered footers |
| leveled compaction | whole-segment reclaim: unlink a dead segment, or copy a segment's survivors once |
| write-ahead log | none. The segment is the log, and an open segment's journal makes a batch atomic |

## No write-ahead log

A write-ahead log is a cheap sequential place to make a write durable before it reaches its real
home, which in a tree is a random write. Here the real home is already a sequential append, written
once in its final position. A log in front would write and sync every byte twice.

The one thing a log adds that is needed is a commit marker that makes a batch atomic across a crash.
An open segment's journal does that: a batch's rows go in as one checksummed group. The journal
holds rows with no values, and the seal cuts it away.

That gives three things worth more than the saved write.

- **The files are the truth and the index is a cache.** No second durable structure holds writes
  the segments lack, so an open has nothing to reconcile. Recovery reads the segments in number
  order: a sealed one from its footer alone, each tail's unsealed one through its journal.
- **A crash has one boundary.** Under the default sync setting a power cut can take each tail's
  active segment since its last seal, and there is no log replay position to reason about.
- **A durable copy of the volume is a set of hard links.** A cue seals every tail first, so every
  version at or below it sits in a sealed file. There is no log prefix to pin and no file-set
  manifest to capture with it.

## The value log comes first

WiscKey (Lu et al., USENIX FAST 2016, *WiscKey: Separating Keys from Values in SSD-Conscious
Storage*) showed that an LSM spends most of its compaction moving values that never needed to move,
since only keys need sorting. With values in an append-only log and keys in the tree, compaction
rewrites a small fraction of the bytes. This engine goes two steps further.

**The value log is primary.** A write, a delete and a compaction are all appends, and a compaction
ends in an unlink. The index is built from the files and can be thrown away.

**The index lives inside the log.** A seal writes the segment's index at its end as a footer: the
rows sorted and packed, one partition per column, behind a directory and a fixed tail. A sealed
segment describes itself, so the index can stay in the footers and recovery can rebuild from the
files without a manifest.

The index has no levels, no key-space partitioning across files, and no merge a read has to walk.
The open tails' keys sit in a map sharded on the leading key bytes the caller declares. Sealed keys
stay in the footers, reached through the spot index or through key spans and filters. The garbage
the value log leaves is the whole reclaim story, with no second mechanism beside a tree compaction.

## Whole-segment reclaim

Leveled compaction writes a byte once per level it passes, so write amplification is roughly the
level count times the size ratio. That buys a bounded number of files per read.

Here a whole segment is retired, by one of two paths.

- **Nothing live left: unlink it.** No bytes are copied, so the charge that would pace the pass is
  zero, and the drain that finds these segments runs ungated. A directory operation is the whole
  cost.
- **Survivors: copy them into an active tail**, then unlink the file once the copies are durable.
  A segment that is `r` dead costs `(1 - r) / r` bytes written per byte reclaimed.

| dead fraction | bytes written per byte freed |
|---:|---:|
| 0.90 | 0.11 |
| 0.50 | 1.00 |
| 0.20 | 4.00 |

Selection is greedy on the highest reclaimable fraction. **The threshold decides what is allowed,
and the ranking decides what is chosen.** Wherever the bar sits, the segments rewritten are the ones
nearest the top of the ranking, so a lower bar admits more work at the same price per unit.

A byte is copied once per rewrite it survives, since there are no levels. Segments overlap in key
space freely, since a segment is a stretch of the log. That is the whole trade. Records written
together sit together, so a family that expires together takes whole segments with it. The same
fact makes the scattered case expensive: almost every cheap answer here needs dead bytes to arrive
in whole segments. A workload that rewrites the same keys forever scatters its dead bytes inside
live segments, and every reclaim takes the copy path.

## Key runs

Key runs bring the levels back, for keys alone.

A get of a sealed key asks the spot index, an in-memory table of where each sealed key's record
lies, and reads the record in one device read. A walk needs key order, which the log doesn't keep,
so it merges the footer of every sealed segment whose key range reaches its span. That fan-in grows
with every seal.

A key merge bounds it. Once more than eight runs stand over one key, the maintenance tick merges
them into a key run: sorted 8-byte rows, each giving a footer row's place as segment and row. The
merge keeps each key's newest row and moves no record. The spot index never hears of it, and a
covered segment keeps its records and footer for gets and recovery. A walk reads the key run in
place of the footers it covers, so it merges eight runs at most. Runs join a merge smallest first
while each is no bigger than twice what the merge has taken, so young runs merge often and cheaply,
and a large run is merged again only once the pile below it has grown to its size.

That is the WiscKey split taken one step on: a leveled merge of keys whose values already sit apart
in the log. A row holds only a place, since the key and the rest are in the footer, so a merge
rewrites a small share of what was written and the run adds little to the volume.

Compaction still rewrites segments under a key run, and the run stays. Each copy keeps its sequence
number, and when a run's row and a rewritten copy tie, the copy wins, so a walk that meets a row
pointing into a retired segment asks the index again. A delete stays on the volume while a key run
still holds an older row of its key.

Measured on W9, a Hetzner ccx33 (8 vCPU, 32 GB), 2026-10-06: 10M keys loaded, then 180 s of fresh
inserts from seven writers beside one scan thread. Reel ran uncapped, then held at 330k puts/s.
RocksDB ran at its own maximum, 285k puts/s.

| | reel, uncapped | reel at 330k puts/s | RocksDB at 285k puts/s |
|---|---:|---:|---:|
| puts/s | 994k | 330k | 285k |
| write amp | 2.61 | 2.37 | 9.55 |
| scan10/s | | 58.5k | 23.1k |
| scan100/s | | 15.1k | 12.3k |
| scan1000/s | | 2,086 | 2,264 |
| cores | | 2.85 | 7.72 |

## What the shape gives up

- **No global key order across files.** A walk merges the key runs and every segment no run covers
  yet, in a heap ordered by each cursor's key. Key merges hold that at eight runs, where leveled
  files bound it by construction.
- **Values stay in write order.** A key run orders keys only, so a walk of a thousand keys reads up
  to a thousand records from wherever they lie. That is cheap while they sit in the page cache.
  Past RAM each is a device read, and on W9 long scans collapsed once the volume outgrew memory, at
  about 206 bytes a key against RocksDB's 126.
- **The threshold is an absolute bar.** A volume whose segments all sit just under it does nothing
  while its dead fraction climbs, until the tier escalates and lowers the bar in one step.
- **Cheap reclaim depends on the workload.** The structure doesn't make records die together. It
  only pays off when they do.

## What it gives back

- One write per byte on the ingest path, in its final position, and nothing to reconcile at open.
- Reclaim by unlink when records die together.
- An index with no level count: a get places its record for one device read.
- Values stored whole and read back verbatim in one device op, with no block framing to unpack.
- Recovery is a walk of the files, because the files are the truth.

For records written once and expiring in families, this is strictly cheaper than a leveled tree, by
the arithmetic above. For a workload that rewrites the same keys forever, every reclaim is a copy.
