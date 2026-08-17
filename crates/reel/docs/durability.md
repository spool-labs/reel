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
| `never`, the default | nothing is lost | the active segment of each tail since its last seal is at risk; everything sealed is on the medium |
| a byte count | nothing is lost | everything up to the last flush that returned, which trails the write head by at most that many bytes |
| `0`, every put | nothing is lost | nothing is lost |
| any of the above, for a multi-record batch | a batch is confirmed or it never happened | a batch is confirmed or it never happened |

Four things hold at every setting.

- **A batch is confirmed or it never happened.** The frame that opens a batch
  declares how many records follow and how many bytes they take, inside a
  checksummed header, and a rebuild keeps the run only when exactly that is
  there and verifies. Whatever a crash lands in the middle of, no reader ever
  sees part of a batch.
- **A sealed segment is on the medium.** The seal syncs after writing the
  footer, whatever the policy says.
- **A segment's directory entry is on the medium.** Creating a segment syncs the
  volume directory, because a file's own sync says nothing about the entry
  naming it.
- **A torn or rotted record costs one record**, not the segment behind it. The
  exception is a batch, where one bad record takes the whole run.

What the batch row does not say is as fixed as what it does. The frame promises
nothing about how one batch is ordered against another beyond what the log
already promises, which is the sequence number every record carries. And it
promises nothing new about syncs: a batch is durable exactly when the policy
says a record is. "Confirmed" is the apply returning after the sync it owed, so
under `never` a batch that returned can still be lost to a power cut, whole.

**None of the power-cut column holds on macOS.** The call that waits for the
drive is not issued there, so a number from that platform is about reaching the
drive and not about surviving the loss of power.

## Format stability

Two version numbers are stamped on disk. `FORMAT_VERSION` in
`format/segment_header.rs` is **4**, written into the header record of every
segment. `FORMAT_VERSION` in `index/persisted.rs` is **2**, written into the
index checkpoint file. A segment whose header names another version is
quarantined whole rather than walked, and an index checkpoint whose version does
not match is discarded and rebuilt from the segments.

**Before 1.0 no cross-version promise is made.** Either number may move without
a conversion path, and the engine ships none: there is no reader for a retired
version and no writer for an older one. What is promised in the meantime is only
that the header record's own layout is frozen, so any build can read the version
and segment number of any file ever written and say "this is not mine" rather
than "this is corrupt".

## The sync ladder

| `sync_bytes` | policy | what a put's return means |
|---|---|---|
| `never` (default) | no hot-path flush at all | the record is in the page cache and the write call succeeded |
| a byte count | flush once that many bytes have settled since the last flush | the record is durable if the put crossed the threshold, otherwise it is durable once a later one does |
| `0` | flush before every put returns | the record reached the device |

Two things flush regardless of the policy. A segment seal takes a full sync after
writing its footer. Creating a segment syncs the volume directory, because a file's
own sync says nothing about the entry naming it, and a crash that takes the
directory block takes the whole segment with it.

The seal itself runs off the append path. A roll hands the retiring segment to
a per-tail sealer thread, `flush()` drains that thread before syncing so a
durability ask still covers everything rolled before it, and an explicit
`Appender::seal()` stays synchronous because a caller reaching for it is
asking for a sealed segment. A crash can land while a footer is queued but
unwritten; recovery walks a segment without one record by record, the same
path a torn seal always took, and the crash suite covers it.

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

Across a crash the frame is what carries it. A batch of more than one record opens
with a frame record declaring two things about the run behind it: how many records
it holds and how many bytes they take. Both numbers are inside the frame's own
checksummed header, and the frame goes down in the same vectored write as the
records it declares, so nothing can leave a frame standing over a run that was
never written.

A rebuild reads the frame first and applies nothing until it has read the whole
run: exactly that many records, each carrying the batch mark in its own checksummed
header, each verifying, and the last of them ending exactly where the span said.
Anything else and the run is dropped whole. A record carrying the batch mark that
the walk meets without a frame in front of it is dropped too, because nothing
vouches for it as part of a run.

Both numbers are there because either alone is weaker. The count without the span
would take a run of the right length made of records that lie about their own; the
span without the count would take a run that reached the right byte in the wrong
number of steps. The frame also says where the batch ends before its records are
read, which is what lets a rot inside a batch cost the batch rather than the walk.

A batch of one record is exempt. There is no middle for a crash to land in, so it
is written as a plain record with neither a frame nor a mark, and it pays for
neither. That matters because a caller with one write path stages every mutation as
a batch, and most of those carry a single key.

A batch and its frame never span segments. The reservation covers the frame and
every record at once, and a reservation that runs past the end of the segment is
given up whole and retaken on the next one, so recovery never has to join a run
across two files.

A sealed segment needs none of this. Sealing waits for every reservation the
segment held and syncs, so a batch in a footer is a batch that completed.

## Recovery: the files are the truth

The index is rebuilt on open, by reading the volume's segment files in number
order on the thread that opened it. There is no fan-out: with one log there is
nothing to rebuild in parallel with anything else.

**A file is only a segment if it says so.** Its first record has to be a segment
header record whose payload verifies and whose segment number and format version
match the file it was found in. Anything else is foreign or misplaced.

**Sealed segments come from the footer alone.** The last eight bytes give the
footer length and the magic, the footer body is checksum verified, and its rows are
decoded. The segment body is read only for the one thing a footer cannot carry,
which is the exclusive end a range tombstone holds in its payload. A footer whose
magic is wrong, whose length is out of range, or whose checksum fails is not a
footer, and the segment falls back to the record walk, which is also what a segment
sealed only part way through gets.

**A paging volume sweeps the footer instead of collecting it.** The rebuild takes
whether the volume pages. Resident columns decode every row into entries as
below. A paging rebuild takes from each footer a key span per column, the range
tombstones with their footprint, the tombstones the segment holds, and its rows
booked against that segment, holding one parsed footer at a time, so the open's
peak is one footer rather than the key set. Sealed keys are answered from their
footers afterwards rather than installed.

**The active tail is walked once, in chunks.** The walk ends at the first byte
that cannot begin a record: a header that will not parse, unknown flag bits, a key
width past the widest a column may declare, a record whose span runs past the end
of the file, or a data header carrying no sequence number. That last one is the
reservation a tail preallocated ahead of its write head. It reads back as zeros,
and zeros parse as an empty data record, so a walk that did not recognise the shape
would step through the whole reservation one header at a time.

**Every walked record is checksum verified, and a failure drops one record.** A
crash can tear a record anywhere in a segment nothing has sealed, and rot can spoil
one anywhere at all. Cutting the walk at the first failure would throw away every
good record behind it for one rotted byte, so what is kept is what verifies. The
exception is a batch, where one bad record takes the whole run with it and the walk
carries on at the boundary the frame named. The cost is one checksum pass over the
bytes the tail holds, which is bounded by the segment size rather than by the
record count.

**Newest-wins is folded in as records arrive, on the resident path.** Only the
surviving version of each key is held, and a record that loses is booked dead
where it lies and dropped, so a rebuild costs memory for what the volume still
resolves rather than for every version it ever wrote. This is also what makes
runtime visibility and the rebuilt index agree when a later put landed in a
lower-numbered segment. Range tombstones are held for the length of the pass and
applied at the end, since their effect is on keys rather than on one key and no
per-key comparison can express it. The paged sweep cannot do the cross-segment
join, so it books every sealed row live and the scrub settles each segment's
dead count as its lap completes the segment.

The rebuild hands back the live entries per column, the sealed key spans where
the volume pages, per-segment byte counts, live and dead and the tombstone
footprint held with its newest mark, the oldest data record each segment can
still surface, the highest sequence number seen, the highest segment number
present, how far it read into each segment so a reader can carry on from there,
and the paths it quarantined. The sequence counter and the segment
numbering are both raised above what was found, so lost unsynced numbers are
harmlessly reissued.

**Range covers are reinstalled unswept.** A resident rebuild resolved every
record against them already, so the sweep finds nothing; a paged rebuild sets
its paged count to zero, so the release pass declines everything. Either way
the re-run is a cheap no-op, which is what makes a crash at any point of the
cover sweep safe. `cue-points.md` carries the sweep itself.

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

`close` seals every tail, so the next open resolves the volume from footers alone.
A store dropped without it leaves one unsealed segment per tail, and the open that
follows reads each of them back record by record. That work is the price of a
crash rather than of a shutdown, and the footer is what tells the two apart on
disk. `Drop` seals as well and traces a failure, since there is nobody left to
hand one to.

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

The one thing a WAL would add is a cheap way to make a multi-record batch atomic
across a crash, since a log record can carry a commit marker. That is what the
batch frame is, and it costs one header record per batch rather than a second
file, because the segment is already the log the marker would go in.

## What is not promised

- Anything the policy did not cover. Under `never` that is the active segment of
  each tail since its last seal.
- A view of the volume held across several reads. A batch publishes under a
  barrier, so one read spanning many keys sees all of it or none of it, but the
  index keeps one version per key and two reads can still straddle a batch. The
  fixed view is a cue point.
- Anything about the order of one batch against another. The frame makes a batch
  whole; what orders it against everything else is the sequence number each of
  its records carries, exactly as for a single put.
- The window inside a seal, on a volume with peers. Between writeback of the
  footer and writeback of the body, a crash can leave a durable footer naming
  records whose bytes never landed. That surfaces as a read-time checksum
  failure and a repair enqueue, which is the designed answer there. A volume
  declaring `RepairPath::None` closes the window by ordering the seal.
- That the grave and cover windows carry the safety alone. They are the
  fallback: the volume tracks the draw-to-publish gap exactly, a drawn gauge
  covering draw to claim and the per-segment holds covering claim to publish,
  so a tick that finds both empty prunes to the counter itself and only a tick
  that catches a record in flight falls back to the 2^20 window.
- Any statement about power loss on macOS, for the reason above.
