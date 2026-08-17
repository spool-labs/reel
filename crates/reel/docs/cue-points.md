# Cue points: reading a volume as it stood, and what that costs

`src/reel/cue.rs`. Called a cue point rather than a snapshot: a cue point is the
tape-transport term for a marked position you can return to, which is exactly
what this is.

The feature looked almost free, and the half of that claim that was wrong
shaped what got built. The pitch was that every record carries a sequence
number from a total order, sealed segments are immutable, and the index only
ever replaces a version with a strictly newer one, so a read at a cue point is
the same index lookup with a bound on the version it accepts. Everything there
is true except the last clause: **the index holds exactly one version per
key.** `WidthIndex::insert` replaces the entry outright and books the record
it displaced dead, so for a key whose live entry is at sequence 900, a read at
cue point 500 does not want a bounded lookup, it wants a different record
entirely, and a bound only tells the reader its answer is too new. The
versions are not gone, though: they are rows in sealed segment footers, which
a footer lists whatever the index residency is, and finding them costs one key
span per segment per column rather than anything per key. That is why this
stayed a small feature rather than a rewrite.

Two properties of the engine are what make it possible at all. `SealedRanges`
records spans unconditionally, on resident volumes too, so footer search works
on every volume at a cost that tracks segment count. And `Appender::seal()` is
synchronous even though the seal itself runs off the append path, because a
caller reaching for it is asking for a sealed segment, which is the primitive a
cue point needs.

## The design

**Taking one seals first.** `cue()` seals every active tail, then reads the
sequence number, then registers it. Sealing first is what makes the cue point
answerable: after it, every record at or below S sits in a segment with a
footer, so every version the cue point can need is findable by key. Without
the seal, a version overwritten inside the still-open tail would exist only as
bytes nothing indexes.

**Reading at S**, which is `get_at`. For key K:

1. Ask the map. An entry at lsn L <= S is the answer, grave included, since a
   grave at or below S means the key was deleted before the cue point.
2. An entry with L > S is invisible to this read. Fall through to the footers.
3. Search the candidate sealed segments and take the newest row with lsn <= S,
   which is `sealed_entry` with one changed comparison.
4. A tombstone or range-tombstone row winning that search means the key was
   gone at S.

**Covers need their own rule, and it is the opposite of the runtime one.** A
standing cover hides entries older than itself. For a cue point at S, a cover
drawn at C > S records a deletion that had not happened yet, so it must be
ignored, and a cover at C <= S applies as usual. Getting this backwards would
make a cue point show a range delete its own timeline never saw.

**Walks** are the same merge as a paged playback with the same two rules per
key: skip map entries above S, take the newest footer row at or below S.

## What has to stop reclaiming

Three mechanisms can destroy a version a live cue point still needs, and each
answers to the same floor, `S_min`, the oldest live cue point.

**Compaction.** `copy_live` keeps a record only when the index still points at
that exact location, so a shadowed version is dropped on the next pass over
its segment. The floor is consulted in `select_target`, beside
`has_pending_covers`, refusing segments that could hold versions a cue point
needs. Coarse, since it pins whole segments rather than records, and
acceptable because a cue point is meant to be held for a scan or a backup
rather than forever.

**The cover sweep.** It drops covered map entries and books their footer rows
dead. Dropping from the map is harmless once spans are recorded, because the
row is still findable; what matters is that the segments survive, which the
same floor guarantees.

**The purge floor.** `is_purged` drops records whose column mark falls below
an operator floor, and it answers to nothing else. A live cue point bounds it
the same way.

## What it costs, measured

Measured on macOS, 1 KiB records:

| what | cost |
|---|---|
| taking one | 0.8 ms at one tail, 4.4 ms at eight |
| reading through one | 0.98 to 1.02x a live read |
| holding one, writer running | 0.99 to 1.02x |

Holding and reading are free, taking is a seal per tail. The read is free
because most keys still answer from the map; only a key overwritten since the
cue pays a footer search.

**The cost nobody should discover later:** `seal()` rolls unconditionally, so
cueing an idle volume still burns one segment per tail, a gibibyte per tail
per cue at the default segment size, reclaimed only when compaction notices
the empty segments. Frequent cueing needs either a seal that declines an
untouched segment or a caller that cues sparingly. Fine weekly, wrong on a
timer.

When nobody takes one, the read path pays nothing: the cue-aware lookup is a
separate entry point, span recording is bounded by segment count, and the
floor is one atomic read in `select_target`.

## What it does not give you

**Not serialisable transactions.** A cue point is a consistent read view.
There is no conflict detection and no write set, and there never will be:
transactions are a caller-side concern above an engine whose writes are
appends with one durability point per batch.

**Not durable across a restart.** Cue points live in memory. A crash drops
them, which is the normal contract and keeps the on-disk format unchanged.

**Not free for unsealed data.** The seal inside `cue()` is what buys
correctness, paid once per cue rather than per read.

## Gates, and where they live

The differential stream takes a cue point, keeps mutating, and checks it still
serves the older state while the live view moves on, including across a range
delete drawn after the cue, the cover rule above and the easiest thing to get
backwards. Compaction and the cover sweep run underneath a held cue point
without changing what it serves. The suite covers resident volumes too, since
recording spans unconditionally is what makes cue points not a paged-only
feature. `tests/probes/cue_speed.rs` is the cost table above.
