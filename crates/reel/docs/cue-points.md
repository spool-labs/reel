# Cue points

A cue point reads the volume as it stood at one sequence number S. The name is the tape-transport term for a marked position you can return to.

**The index holds one version per key.** `WidthIndex::insert` replaces the entry outright and books the displaced record dead. So for a key whose live entry is at 900, a read at cue 500 needs a different record entirely, and a version bound would only tell the reader its answer is too new. The older versions are rows in sealed segment footers. Finding them costs one key span per segment per column and nothing per key, which is why this stayed a small feature.

Two engine properties make it possible. `SealedRanges` records the key span of every sealed segment, so a footer search costs in proportion to the segment count. And `Appender::seal()` is synchronous even though seals normally run off the append path, which gives a cue the sealed segment it needs.

## Taking one

`cue()` seals every active tail, reads the sequence number, then registers the hold. After the seal every record at or below S sits in a segment with a footer, so every version the cue can need is findable by key. Without the seal, a version overwritten inside the open tail would exist only as bytes nothing indexes. A read-only volume cannot take one.

## Reading at S

`get_at` calls `read_as_of`. The read settles the sealed queue first, so the segments the cue sealed have their spans noted. A key the map has let go asks the spot index, which answers in one read when the key's newest sealed version is at or below S. Otherwise, for key K:

1. Ask the map. An entry at lsn L <= S is the answer. A grave there, or a cover at or below S over it, means the key was gone at S.
2. An entry with L > S is invisible to this read. Fall through to the footers.
3. Search the candidate sealed segments and take the newest row with lsn <= S, which is `sealed_entry_at`.
4. A tombstone or range-tombstone row winning that search means the key was gone at S.

**Covers follow the opposite rule from the runtime one.** A standing cover hides entries older than itself. For a cue at S, a cover drawn at C > S records a deletion that had not happened yet, so it is ignored. A cover at C <= S applies as usual. Getting this backwards would show a range delete the cue's own timeline never saw.

There is no walk at a cue point. A cue read is one key at a time.

## What has to stop reclaiming

Four mechanisms can destroy a version a live cue still needs. Each one answers to the cue floor, the oldest held cue.

| mechanism | how the floor holds it |
|---|---|
| compaction | `copy_live` keeps a record only when the index still points at that exact location, so a shadowed version goes on the next pass over its segment. Selection skips every segment whose oldest mark is at or below the floor, beside the `has_pending_covers` check. It pins whole segments, which suits a cue held for a scan or a backup |
| the cover sweep | it drops covered map entries and books their footer rows dead. Dropping from the map is harmless once spans are recorded, since the row is still findable, and the compaction rule keeps the segments |
| grave pruning | `prune_tombstones` keeps its floor at or below the oldest cue, since a cue read past a later delete finds its version through that delete's grave |
| the purge floor | `is_purged` drops records whose column mark is below the operator's floor. It runs only inside a compaction pass, so the compaction rule bounds it too |

## What it costs

Measured on macOS with 1 KiB records, on a resident index this volume no longer has. The probe that took them is gone, so these stand until a fresh run.

| what | cost |
|---|---|
| taking one | 0.8 ms at one tail, 4.4 ms at eight |
| reading through one | 0.98 to 1.02x a live read |
| holding one, writer running | 0.99 to 1.02x |

Holding is free and taking is a seal per tail. A cue seals every tail and the next hand-over takes those keys out of the map, so a key unchanged since the cue answers from the spot index in one read. Only a key rewritten since the cue pays a footer search. `a_cue_read_takes_one_read_while_the_key_stands` checks this.

**An idle volume still pays a segment per tail.** `seal()` rolls unconditionally, so cueing an idle volume seals one empty segment per tail. The seal cuts each file down to its header and footer, so each costs a file and a segment number. Compaction and merges both skip a segment with no record bytes, so nothing retires these files. Frequent cueing needs a seal that declines an untouched segment, or a caller that cues sparingly. Fine weekly, wrong on a timer.

When nobody holds a cue the read path pays nothing. The cue-aware lookup is a separate entry point, span recording is bounded by the segment count, and the floor is one atomic read per compaction pass.

## What it does not give you

- **Serialisable transactions.** A cue point is a consistent read view. There is no conflict detection and no write set, and there never will be. Transactions belong to the caller, above an engine whose writes are appends with one durability point per batch.
- **Survival across a restart.** Cue points live in memory and a crash drops them. That keeps the on-disk format unchanged.
- **Free reads of unsealed data.** The seal inside `cue()` is what buys correctness, paid once per cue and never per read.

## Tests

| test | checks |
|---|---|
| `cue_holds_the_old_version` | a cue keeps serving the old value after an overwrite |
| `cue_hides_later_writes` | a key written after the cue is invisible to it |
| `cue_outlives_a_delete` | a delete after the cue leaves the value readable at the cue |
| `cue_outlives_a_pruned_delete` | a cue read past a later delete still finds its version once the window passes the delete's grave |
| `cue_ignores_a_later_drop` | a range delete drawn after the cue is invisible to it, the easiest rule to get backwards |
| `cue_pins_compaction` | compaction cannot retire what a held cue still reads |
| `floor_lifts_on_drop` | the floor lifts once the last holder lets go |
