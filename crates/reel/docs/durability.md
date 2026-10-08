# Durability: what a crash costs, what recovery promises

Two claims hold this together. The files are the truth and the index is a cache,
so anything the index knows can be derived again from the segments. And a record
is durable when the sync covering it has returned, which the sync policy decides
and nothing else implies.

## The promise

What a put that already returned is worth, per `sync` setting. A process crash
takes the process and leaves the page cache standing; a power cut takes both.

| `sync` | a process crash | a power cut |
|---|---|---|
| `never`, the default | nothing on Linux with buffered writes. Elsewhere, what landed since the journal last went down, at most one 1 MiB writeback pace a tail | the active segment of each tail since its last seal is at risk, and everything sealed is on the medium |
| a byte count | nothing on Linux with buffered writes. Elsewhere, what landed since the journal last went down, at most one pace or that many bytes | everything after the last flush that returned, at most that many bytes |
| `0`, every put | nothing is lost | nothing is lost |
| any of the above, for a multi-record batch | a batch is confirmed or it never happened | a batch is confirmed or it never happened |

A small record is keyless, so an open segment's journal is what says which key each
record holds. The journal is a region of the segment's own file, so one sync makes a
record and its row durable together. On Linux with buffered writes a put writes its
record and its row before it returns, so a dead process leaves both in the page cache. Elsewhere rows wait in memory for the next pace or flush, and a record whose
row never reached the journal is not found again.

Four things hold at every setting.

- **A batch is confirmed or it never happened.** A batch's rows go into its
  segment's journal as one checksummed group, and a rebuild keeps the group only
  when every record it lists checks out. Whatever a crash lands in the middle of,
  no reader ever sees part of a batch.
- **A sealed segment is on the medium.** The seal syncs after writing the
  footer, whatever the policy says.
- **A segment's directory entry is on the medium.** Creating a segment syncs the
  volume directory, because a file's own sync says nothing about the entry
  naming it.
- **A torn record in an open segment costs its write's group**, one record or one
  batch, and not the records behind it.
- **A footer that rots after its seal costs its segment.** The records are keyless
  and the seal cut the journal off, so nothing lists them. A volume with peers
  repairs them from a peer.

What the batch row does not say is as fixed as what it does. The group promises
nothing about how one batch is ordered against another beyond what the log
already promises, which is the sequence number every record carries. And it
promises nothing new about syncs: a batch is durable exactly when the policy
says a record is. "Confirmed" is the apply returning after the sync it owed, so
under `never` a batch that returned can still be lost to a power cut, whole.

**None of the power-cut column holds on macOS.** The call that waits for the
drive is not issued there, so a number from that platform is about reaching the
drive and not about surviving the loss of power.

## Format stability

One version number is stamped on disk. `FORMAT_VERSION` in
`format/segment_header.rs` is **7**, written into the header record of every
segment. A segment whose header holds another version is quarantined whole and
never read.

**Before 1.0 no cross-version promise is made.** The number may move without
a conversion path, and the engine ships none: there is no reader for a retired
version and no writer for an older one. What is promised in the meantime is only
that the header record's own layout is frozen, so any build can read the version
and segment number of any file ever written and say "this is not mine" rather
than "this is corrupt".

## The sync ladder

| `sync` | policy | what a put's return means |
|---|---|---|
| `SyncPolicy::Never` (default) | no hot-path flush at all | the record is in the page cache and the write call succeeded |
| `SyncPolicy::Bytes` | flush once that many bytes have settled since the last flush | the record is durable if the put crossed the threshold, otherwise it is durable once a later one does |
| `SyncPolicy::EveryPut` | flush before every put returns | the record reached the device |

Three things flush regardless of the policy. A segment seal takes a full sync after
writing its footer. Creating a segment syncs the volume directory, because a file's
own sync says nothing about the entry naming it, and a crash that takes the
directory block takes the whole segment with it. A tail that syncs often also
keeps a window of zeros written and synced ahead of its write head, so its appends
land on blocks the filesystem has already given out.

The seal itself runs off the append path. A roll hands the retiring segment to
a per-tail sealer thread, `flush()` drains that thread before syncing so a
durability ask still covers everything rolled before it, and an explicit
`Appender::seal()` stays synchronous because a caller reaching for it is
asking for a sealed segment. A crash can land while a footer is queued but
unwritten. The seal cuts the journal off only after its footer is synced, so
recovery reads it back through the journal, and the crash suite covers it.

So under the default, what a power cut can take is the active segment of each tail,
bounded by the last seal. Everything sealed is on the medium. That default is a
deployment claim rather than an engine one: if what the volume holds is redundant
above the engine, a lost tail is refetched from peers, and paying a device flush
per write to avoid a refetch is the more expensive of the two. A deployment that
cannot repair from elsewhere sets a byte cadence.

What that default is worth, measured on a Hetzner ccx33 at eight writers with
four tails: 3,344 MB/s at 4 KiB under `never`, 1,840 under a 1 MiB cadence, and
57 under a flush per put. A flush per record costs 59x at 4 KiB and 9x at 64 KiB.

**On macOS none of this is a power-cut claim.** `fsync` there returns once the
drive has the bytes and before the drive has written them, and `F_FULLFSYNC` is
the call that waits. The engine does not issue it. Measured per 100 byte record on
an Apple M4 Max, APFS, native: buffered 1.6us, `fsync` 19us, `F_BARRIERFSYNC`
267us, `F_FULLFSYNC` 4068us. A durability number from that platform is about
reaching the drive.

## How writers share one flush

Writers cross a byte threshold together, so left alone each would ask the device
for a flush of the same bytes and the tail would pay for all of them. A writer
that finds a flush already running waits for it and asks again only if what came
back still falls short of its own position. Two flushes of one segment can finish
out of order, so the durable watermark only ever moves forward.

The flush covers a position, not a set of records, and a sync covers everything
written before it was asked for. That is why the flush is issued with the tail
given back rather than held: the position was settled before the ask, and writers
arriving behind it are not made to wait out a device round trip for a claim that
was already decided.

That wait is one of the two the engine has, and it can be taken either way. A
caller with a thread to spend parks on it; a caller with a runtime worker to
protect leaves a waker and is polled when the flush lands. Finding nobody at the
device is not a wait at all but a turn, handed back for the caller to run wherever
blocking is allowed, because the flush is a syscall on a thread and no future
changes that.

A sync that fails ends the segment. It is marked broken so waiters hear an error
instead of waiting on a flush nobody will take, the tail rolls to a fresh segment,
and the doomed one is deliberately left unsealed. Its footer would claim every
record it holds is there, and a failed sync is exactly the case where that cannot
be assumed, so the next open reads it back instead.

## A batch is one durability point, and one recovery domain

A batch takes one reservation on one tail, writes its records with nothing between
them, takes the sync it owes once, and only then moves the index. Letting each
write take the sync its policy owes would pay a flush per record for a promise
nobody made about the middle of a batch. Moving the index first would make a key
readable on the strength of a write the caller is about to be told failed, so a
batch that fails anywhere leaves nothing of itself visible.

Across a crash the journal keeps it. The records of a batch take one
reservation and one write, and their rows go into the segment's journal as one group
under one checksum. A rebuild keeps the group only when every record it lists sits
where its row says and checks out, and drops it whole otherwise.

A batch never spans segments. The reservation covers every record at once, and a
reservation that runs past the end of the segment is given up whole and retaken on
the next one, so its rows land in one journal.

A sealed segment needs none of this. Sealing waits for every reservation the
segment held and syncs, so a batch in a footer is a batch that completed.

## Recovery: the files are the truth

The index is rebuilt on open from the volume's segment files. On a posix backend
up to eight threads (`MAX_READERS`) read the files at once, and the opening thread
applies them in segment number order, so an exact tie still goes to the earlier
segment. The ring backend reads them one at a time on the opening thread.

**A file is only a segment if it says so.** Its first record has to be a segment
header record whose payload verifies and whose segment number and format version
match the file it was found in. Anything else is foreign or misplaced.

**Sealed segments come from the footer alone.** The last eight bytes give the
footer length and the magic, the footer body is checksum verified, and its rows are
decoded. The segment body is read only for the one thing a footer cannot carry,
which is the exclusive end a range tombstone holds in its payload. A footer whose
magic is wrong, whose length is out of range, or whose checksum fails is not a
footer. A segment sealed only part way through still runs out to its journal and
reads back through it.

**The rebuild sweeps each sealed footer.** It takes from each footer a key span per
column, the range tombstones with their footprint, the tombstones the segment
holds, and the segment's tally. The sealed rows then go into the spot index, a 16
byte slot a key, on eight loader threads. Up to eight readers parse footers ahead of
the join, one waits in the hand-off queue and each loader holds one, so the open
holds up to about eighteen parsed footers at once on posix and about ten on the ring.
Sealed keys are answered from the spot index afterwards.

**The active tail is read through its journal.** The journal's whole groups are
read in order, and a group is kept only when every record it lists sits at its
row's offset and checks out: a keyless record by its keyed check, a larger one by
its header and checksum. A torn group ends the journal, and a group listing a record
that did not land is dropped whole. The tail resumes after the last whole group,
writing its records past every place any whole group names, so a dropped group never
meets a record that checks out. A segment with no footer that stops short of its
journal is one whose footer went bad after its seal, and no tail resumes into it.

**Newest-wins is folded in as a tail's records arrive.** The tails' rows are fed
in key order, a tie going to the earlier segment, and a record that loses is booked
dead where it lies and dropped. This is also what makes runtime visibility and the
rebuilt index agree when a later put landed in a lower-numbered segment. Range
tombstones stand as covers for the length of the pass, since their effect is on
keys and no per-key comparison can express it. The sweep books each sealed segment's
tally, the live and dead split the segment wrote at its seal. The join against the
tails books dead the newest sealed row each tail entry outversions, and the spot
index load books dead each sealed version a footer settles against the version that
came next. Both debit only a shadowing at or past the segment's `sealed_at`, the
frontier its tally is current to. What neither reaches, a version a key run left out
or a key only the headers could settle, waits for the scrub to settle each segment's
dead count as its lap completes the segment.

The rebuild hands back the live entries per column, the sealed key spans,
per-segment byte counts, live and dead and the tombstone footprint held with its
newest mark, the oldest data record each segment can still surface, the highest
sequence number seen, the highest segment number present, how far it read each
tail's journal so a follower can resume from there, and the paths it quarantined. The sequence counter and the segment
numbering are both raised above what was found, so lost unsynced numbers are
harmlessly reissued.

**Range covers are reinstalled unswept and swept before the open returns.** The
sweep releases each covered record the load counted. A record counts once whichever
pass releases it, which is what makes a crash at any point of the cover sweep safe.
`cue-points.md` describes the sweep itself.

## Quarantine, never truncate

A file whose first record is not a segment header, whose header fails its
checksum, or which names another segment number or an unknown format version, is
left on disk untouched, kept out of the index, and logged. Nothing about a
misplaced file is worth destroying it over, and the one case where truncation
looks right is exactly the case where a mistake is unrecoverable.

## The seal takes one sync with peers, two without

The shape follows `RepairPath`, the volume's claim about where a lost record
can be fetched again. With peers, the footer is written and one full sync
closes the segment: writeback orders nothing, so a crash inside the seal can
leave a durable footer naming records whose bytes never landed, and that
resolves to a read-time checksum miss and a repair enqueue, which is already
the answer such a volume gives for rot. The second flush would buy nothing the
layer above does not.

A sole copy has no such answer, so it pays the second flush: the records are
synced first, then the footer is written and synced. A footer can then never
outlive the bytes it names, whatever order a crash freezes writeback in.
`a_sole_copy_footer_never_outlives_its_records` enumerates every crash
boundary of a rolling stream under a scattered writeback schedule and holds
the sole copy to it; the same sweep run without the ordering fails, which is
what says the schedule is actually reached.

The same claim decides what corruption is. With peers a corrupt record is
evicted so the miss becomes a repair; on a sole copy the read reports
`Corruption` and the key keeps its place, the scrub counts hits without
evicting, and compaction leaves a rotted record's segment standing rather than
unlinking the last copy or stamping a fresh checksum over rot. A segment left
standing that way is pinned out of the ranking, or the pass that cannot retire
it selects it again on every tick: `segments_pinned_by_rot` is what the volume
is holding for this reason and cannot reclaim until an operator acts.

## Clean shutdown against a crash

`close` flushes every tail, journal included, and leaves each one for the next
open to resume. `Drop` closes as well and traces a failure, since there is nobody
left to hand one to. A crash leaves the rows written since the journal last went
down unwritten, which is the difference between the two on disk.

## Corruption at read time

What a checksum failure answers is the `repair` claim's other half. With peers,
a record that is where the index says and fails is evicted on the spot, so the
miss becomes a repair enqueue rather than a pointer the index keeps handing
out. On a sole copy nothing is evicted: the read returns `Corruption`, the key
keeps its place, and the answer repeats until an operator intervenes, because a
miss there would hide the loss. Either way the corrupt bytes stay on disk as
dead space until the segment is retired. The eviction is guarded on the
location, so a version that overtook
the corruption keeps its place. The scrub and compaction's own copy step both
reach the same eviction path on sealed segments, and `verify_reads` decides
whether a read checks its own record or leans on that sweep.

## Why not a write-ahead log

A WAL exists to give a store a cheap sequential place to make a write durable
before it reaches its real home, which is otherwise a random write into a tree.
Here the record's real home already is a sequential append at the end of a
segment, and it is written there once, in its final position. A WAL in front of
that would write every byte twice, and it would sync twice, to protect a window
between the log and the store that does not exist.

The journal holds rows, never values: what each record's key, version and place
are, until the footer says the same at the seal. It sits in the segment's own file,
past the records and the footer, so it costs no second file and no second sync. It is
what lets a small record drop its key, and a batch's rows going into it as one group
is what makes the batch atomic across a crash. The seal cuts it off.

## What is not promised

- Anything the policy did not cover. Under `never` a power cut can take the active
  segment of each tail since its last seal. Off Linux, or on a direct volume, a
  process crash takes what landed since the journal last went down.
- A view of the volume held across several reads. A batch publishes under a
  barrier, so one read spanning many keys sees all of it or none of it, but the
  index keeps one version per key and two reads can still straddle a batch. The
  fixed view is a cue point.
- Anything about the order of one batch against another. The journal makes a batch
  whole; what orders it against everything else is the sequence number each of
  its records carries, exactly as for a single put.
- The window inside a seal, on a volume with peers. Between writeback of the
  footer and writeback of the body, a crash can leave a durable footer naming
  records whose bytes never landed. That surfaces as a read-time checksum
  failure and a repair enqueue, which is the designed answer there. A volume
  declaring `RepairPath::None` closes the window by ordering the seal.
- That a reader's graves outlast a writer stalled past the 2^20 window. A
  read-only open following the log cannot see which writes are still out, so it
  prunes a grave once 2^20 sequence numbers have passed it, and a write drawn
  before a delete and published more than 2^20 numbers later can show on that
  reader until it reopens. The writer counts every write from its draw to its
  publish, and neither its prune nor compaction passes a write still out.
- Any statement about power loss on macOS, for the reason above.
