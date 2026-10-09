# Overview

A reel is a log-structured key-value store: sequential writes, immutable segments, whole-segment
reclaim. It is one log of segment files spread over one or more volume roots. A write lands at the
end of an open segment, and the segment seals when it fills. Nothing is updated in place, so a
delete is a tombstone and the maintenance plane gives the space back. An index maps every live key
to the segment and offset holding it. The open tails' keys sit in memory, and a sealed key is found
through the footers the seals wrote.

## A volume on disk

The engine writes five kinds of file and nothing else.

| file | where | what it is |
|---|---|---|
| `NNNNNN.reel` | every root | one segment, six-digit zero-padded number rising across roots. An open one keeps its journal of rows past its records |
| `<id>.keys` | the first root | a key run a merge wrote, twelve-digit id, `<id>.keys.part` until whole |
| `reel.volumes` | the first root | the manifest listing every root this reel spans |
| `reel.volume` | every root past the first | the marker saying this root was mounted where the manifest says |
| `reel.lock` | the first root | the advisory lock one writing process holds for its lifetime |

A segment opens with a header record that holds the format version and its own segment number. A
file that isn't this reel's is quarantined and left on disk. Segment numbers are global: a root
holds a subset of the one numbering, and the index never stores a path.

## The write path

```
put / write_batch
      |
      v  plan: column check, capacity check, codec
   route to the least loaded tail
      |
      v  reserve a byte range at the write head   (one atomic step, the only
      |                                            point writers contend on)
   copy header, key and payload into the reservation
      |
      v  queue the records' journal rows, a batch's rows as one group
   sync owed by the policy?  ->  one flush covering a position
      |
      v  publish: the index moves, under the barrier for a batch
   reservation ran past the end?  ->  roll, and hand the old segment to a sealer
      |
      v
   footer written, synced, spans queued for the index
```

The copy runs on the thread that asked for the write. A batch takes one reservation, writes its
records back to back, syncs once, and only then moves the index, so it is one durability point and
one recovery domain. The seal runs off the append path on a per-tail sealer thread.

## The read path

The index answers first. The map holds the open tails' keys: an entry has the location and the
sequence number, and the read is one device op at that location. A sealed key goes to the spot
index, which places its record for one read.

A key the spot index can't settle goes to the footers. That search narrows in steps: the
per-segment key spans, then each remaining segment's filter, then its directory, then one block of
rows, then the row. The row points at a record and the driver fetches it.

The search reads every candidate segment. Several tails write at once, so a newer record can land
in a lower-numbered segment. The highest sequence number wins.

Neither path adds a copy. `put_owned` moves the caller's buffer to the tail untouched, and the
drain gathers header, key and payload without joining them into one buffer. A read lands the
payload in a pooled buffer that goes up as the answer. A copy census and the allocation count tests
agree on this. The API takes owned buffers because a submission outlives the caller's stack frame.

## The maintenance plane

```
maintain_once            one tick, every step bounded and paced
  retry broken seals
  seal idle tails        a tail with no write for five seconds
  publish footprint
  page out sealed        sealed keys leave the map
  sweep covers
  prune graves
  compact once  ->  drain wholly dead segments        unlink, nothing copied
                ->  select the highest dead fraction past the threshold
                    gates: claim, pending cover, cue floor, rot pin
                    fetch in offset order, apply in key order
                    repoint each index entry, guarded on its version
                    flush the destination, then unlink the source
  merge when due         more than eight runs over one key
  scrub once
```

Selection is greedy on the highest reclaimable fraction, so the threshold decides what is allowed
and the ranking decides what is chosen. A segment with nothing live left is unlinked whole: no bytes
copied, and the charge that would pace the pass is zero. Compaction copies a tombstone into its
destination while anything old enough for it to hide survives.

## Glossary

Terms this codebase uses in ways a newcomer can't guess.

| term | meaning |
|---|---|
| door | the two forms of every read and write, blocking and awaited. Both do the same work in the same place. Only who waits differs |
| tail | one open segment being appended to, with its own file and write head. A volume runs several, and writers spread across them so the kernel never serializes them on one inode |
| sorted run | a segment whose records sit in key order, which a compaction rewrite produces |
| key run | a file of sorted key rows that a walk reads in place of the footers it covers |
| dead run | a contiguous stretch of dead bytes in a segment, which a hole punch can give back |
| cover | the in-memory footprint of a range delete. One record stands for every key in the range, and the cover hides every older entry until a sweep settles the keys under it |
| grave | the in-memory mark a single-key delete leaves. It has to outlive the versions it hides, so pruning checks the sealed segments as well as the sequence number |
| cue | a marked position the volume can be read at. Taking one seals every tail first, so every version at or below it sits in a footer |
| repoint | moving one index entry onto a copy of its record, guarded on the version the copy was made from. Compaction relocates a record this way without losing a write that raced it |
| footer | the packed sorted index a seal writes at the end of a segment. One partition per column, each sorted by key and prefix packed, behind a directory and a fixed tail |
| servo | the running half of the io-path decision: watch the work, move one knob, keep it if it helped. Its other half, bias, is the startup pass that sets the path once from what the machine already reports. Both words come from tape transports, and so does the word reel |
| plane | one background concern with its own pass and rate. The maintenance plane is compaction, the merge and the scrub. The read planes are the routes a read can take to its bytes: cached, probed, direct, mapped |
| lane | one concurrent caller's stream of blocking reads. Lanes and awaited depth are two ways to buy the same overlap, and they cost different things |
| cohort | a workload shape: records written once, never updated, expiring in whole families. Its dead bytes arrive in whole segments, which whole-segment reclaim handles cheaply |

## Choosing a backend

| choice | take it when |
|---|---|
| posix | by default, unless readers await. It is the portable floor, one syscall per op on the calling thread, and it is benchmarked as a first choice |
| a ring | on Linux only, only when the caller really awaits, and only when it is fed batches. Nothing selects a ring for you. Per-record ring submissions come out slower than per-record posix |
| `map_above` | only where the working set stays resident and the caller cares about the median. Mapped reads buy the median, cost the tail, and lose outright on cold reads |

## Platforms

| capability | Linux | macOS |
|---|---|---|
| posix backend | yes | yes |
| io_uring backend | yes, when the kernel sets a ring up | absent, the request runs posix |
| direct descriptors | yes | request resolves to buffered |
| warm cache probe | yes | reports cold, read goes to the driver |
| range sync | yes | no-op |
| preallocation | one call | reserve then extend |
| hole punch over dead runs | yes | yes |
| power-cut durability | yes, under the sync setting | no claim: the call that waits for the drive is not issued |
| tail count | `min(cores, 8)`, floored at one per fast volume | same |
| compaction rate | unpaced | unpaced |

A macOS or BSD run is a real run of everything portable and says nothing about the io.
