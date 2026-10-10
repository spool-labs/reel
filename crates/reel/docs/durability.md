# Durability

Two rules hold this together. The files are the truth and the index is a cache, so the index can always be rebuilt from the segments. A record is durable when the sync that covers it has returned, and the `sync` setting decides when that is.

## The promise

What a put that already returned is worth, per `sync` setting. A process crash takes the process and leaves the page cache. A power cut takes both.

| `sync` | a process crash | a power cut |
|---|---|---|
| `never`, the default | what landed since the rows last went down, at most one 1 MiB writeback pace a tail | the active segment of each tail since its last seal is at risk, and everything sealed is on the medium |
| a byte count | what landed since the rows last went down, at most one pace or that many bytes | everything after the last flush that returned, at most that many bytes |
| a durable put or batch, any setting | nothing is lost | nothing is lost |
| any of the above, for a multi-record batch | a batch is confirmed or it never happened | a batch is confirmed or it never happened |

A record with 4 KiB of payload or less is keyless, so an open segment's journal is what says which key each record holds. The journal is a region of the segment's own file, so one sync makes a record and its row durable together. A put writes only its record. Its row waits in memory for the next pace or flush, so a put costs one write, and a record whose row never reached the journal is not found again.

These hold at every setting:

- **A batch is confirmed or it never happened.** A batch's rows go into its segment's journal as one checksummed group, and a rebuild keeps the group only when every record it lists checks out. Wherever a crash lands, no reader ever sees part of a batch.
- **A sealed segment is on the medium.** The seal syncs after writing the footer, whatever the setting.
- **A segment's directory entry is on the medium.** Creating a segment syncs the volume directory, because a file's own sync says nothing about its directory entry.
- **A torn record in an open segment costs its write's group**, one record or one batch. The records behind it survive.
- **A footer that rots after its seal costs its segment.** The records are keyless and the seal cut the journal off, so nothing else lists them. A volume with peers repairs them from a peer.

The batch row promises nothing new about syncs: a batch is durable exactly when the setting says a record is. "Confirmed" is the apply returning after the sync it owed, so under `never` a batch that returned can still be lost to a power cut, whole.

**None of the power-cut column holds on macOS.** There `fsync` returns once the drive has the bytes and before the drive writes them. `F_FULLFSYNC` is the call that waits, and the engine does not issue it. Measured per 100-byte record on an Apple M4 Max, APFS, native: buffered 1.6us, `fsync` 19us, `F_BARRIERFSYNC` 267us, `F_FULLFSYNC` 4068us. A durability number from macOS is about reaching the drive.

## Format version

`FORMAT_VERSION` is **8**, written into the header record of every segment. A segment whose header holds another version is quarantined whole and never read.

**Before 1.0 there is no cross-version promise.** The number may move with no conversion path, and the engine ships none: no reader for a retired version and no writer for an older one. The one promise is that the header record's own layout is frozen. Any build can read the version and segment number of any file ever written, and report a foreign file as foreign.

## The sync ladder

| `sync` | what it does | what a put's return means |
|---|---|---|
| `SyncPolicy::Never` (default) | no flush on the hot path | the record is in the page cache and the write call succeeded |
| `SyncPolicy::Bytes` | flush once that many bytes have settled since the last flush, and zero syncs every write | the record is durable if the put crossed the threshold, otherwise once a later one does |
| `put_durable`, `apply_batch_durable` | flush the one tail the write went to before returning | the record reached the device |

Seals and segment creation sync whatever the setting, as listed above. A crash that took the directory block would take the whole segment with it. A tail whose syncs land less than 512 KiB apart also keeps a window of zeros written and synced ahead of its write head, so its appends land on blocks the filesystem has already handed out.

The seal runs off the append path. A roll hands the retiring segment to a per-tail sealer thread. `flush()` drains that thread before syncing, so a durability ask still covers everything rolled before it. An explicit `Appender::seal()` stays synchronous, because its caller is asking for a sealed segment. A crash can land while a footer is queued and unwritten. The seal cuts the journal off only after its footer is synced, so recovery reads such a segment back through its journal, and the crash suite covers it.

The `never` default is a deployment claim. If what the volume holds is redundant above the engine, a lost tail is refetched from peers, and paying a device flush per write to avoid a refetch costs more. A deployment that cannot repair from elsewhere sets a byte cadence.

What the default is worth, on a Hetzner ccx33 at eight writers with four tails, 4 KiB records:

| `sync` | MB/s |
|---|---|
| `never` | 3,344 |
| 1 MiB cadence | 1,840 |
| flush per put | 57 |

A flush per record costs 59x at 4 KiB and 9x at 64 KiB.

## How writers share one flush

Writers cross a byte threshold together, so left alone each would ask the device to flush the same bytes and the tail would pay for all of them. A writer that finds a flush already running waits for it, and asks again only if the result still falls short of its own position. Two flushes of one segment can finish out of order, so the durable watermark only ever moves forward.

The flush covers a position, and a sync covers everything written before it was asked for. So the flush is issued with the tail released: the position was settled before the ask, and writers arriving behind it don't wait out a device round trip for a claim already decided.

This is one of the engine's two waits, and a caller can take it two ways. A caller with a thread to spend parks on it. A caller with a runtime worker to protect leaves a waker and is polled when the flush lands. A caller that finds nobody at the device gets a turn handed back, to run wherever blocking is allowed, because the flush is a syscall on a thread and no future changes that.

A failed sync ends the segment. It is marked broken so waiters get an error and stop waiting on a flush nobody will take, the tail rolls to a fresh segment, and the doomed one is left unsealed on purpose. A footer would claim every record in it is there, which a failed sync cannot promise, so the next open reads it back through its journal.

## A batch is one durability point and one recovery domain

A batch takes one reservation on one tail, writes its records back to back, takes the sync it owes once, and only then moves the index. A sync per record would pay a flush per record for a promise nobody made about the middle of a batch. Moving the index first would make a key readable on the strength of a write the caller is about to hear failed, so a batch that fails anywhere leaves nothing visible.

Across a crash the journal keeps it: one group under one checksum, kept only when every record it lists sits where its row says and checks out, and dropped whole otherwise.

A batch never spans segments. The reservation covers every record at once. A reservation that runs past the end of the segment is given up whole and retaken on the next one, so its rows land in one journal.

A sealed segment needs none of this. Sealing waits for every reservation the segment held and syncs, so a batch in a footer is a batch that completed.

## Recovery

The index is rebuilt on open from the volume's segment files. On posix, up to eight threads (`MAX_READERS`) read the files at once and the opening thread applies them in segment number order, so an exact tie still goes to the earlier segment. The ring backend reads them one at a time on the opening thread.

**A file is a segment only if it says so.** Its first record has to be a segment header record whose payload verifies and whose segment number and format version match the file it was found in. Anything else is foreign or misplaced.

**Sealed segments come from the footer alone.** The last eight bytes give the footer length and the magic, the footer body is checksum-verified, and its rows are decoded. The segment body is read only for what a footer cannot hold, the exclusive end a range tombstone keeps in its payload. A footer with a wrong magic, an out-of-range length or a failed checksum is treated as no footer. A segment whose seal stopped partway still has its journal and is read back through it. A sealed segment whose cut after the footer never landed is cut to its footer by the next writable open.

**The rebuild sweeps each sealed footer.** From each footer it takes a key span per column, the range tombstones with their footprint, the tombstones the segment holds, and the segment's tally. The sealed rows then go into the spot index, a 16-byte slot per key, on eight loader threads. Up to eight readers parse footers ahead of the join, one waits in the hand-off queue and each loader holds one, so an open holds up to about eighteen parsed footers at once on posix and about ten on the ring. Sealed keys are answered from the spot index afterwards.

**The active tail is read through its journal.** Whole groups are read in order. A group is kept only when every record it lists sits at its row's offset and checks out: a keyless record by its keyed check, a larger one by its header and checksum. A torn group ends the journal, and a group listing a record that did not land is dropped whole. The tail resumes after the last whole group and writes new records past every record any group lists, so a dropped group can never match a new record. A footerless segment whose file ends before its journal had its footer go bad after its seal, and no tail resumes into it.

**Newest wins, folded in as a tail's records arrive.** The tails' rows are fed in key order, a tie going to the earlier segment, and a losing record is booked dead where it lies and dropped. This also keeps runtime visibility and the rebuilt index in agreement when a later put landed in a lower-numbered segment. Range tombstones stand as covers for the length of the pass, since they act on keys and no per-key comparison can express that.

Dead bytes are booked three ways:

- The sweep books each sealed segment's tally, the live and dead split the segment wrote at its seal.
- The join against the tails books dead the newest sealed row each tail entry outversions.
- The spot index load books dead each sealed version a footer settles against the version that came next.

The last two book only a shadowing at or past the segment's `sealed_at`, the frontier its tally is current to. What none of them reaches, a version a key run left out or a key only the headers could settle, waits for the scrub, which settles each segment's dead count when its lap completes that segment.

The rebuild hands back the live entries per column, the sealed key spans, per-segment byte counts (live, dead, and the tombstone footprint with its newest mark), the oldest data record each segment can still surface, the highest sequence number seen, the highest segment number present, how far it read each tail's journal so a follower can resume from there, and the paths it quarantined. The sequence counter and the segment numbering both start above what was found, so lost unsynced numbers are reissued harmlessly.

**Range covers are reinstalled unswept and swept before the open returns.** The sweep releases each covered record the load counted. A record counts once whichever pass releases it, which makes a crash at any point of the cover sweep safe.

## Quarantine, never truncate

A file whose first record is no segment header, whose header fails its checksum, or whose header holds another segment number or an unknown format version is left on disk untouched, kept out of the index, and logged. Nothing about a misplaced file is worth destroying it over, and the one case where truncation looks right is the case where a mistake can't be undone.

## The seal: two syncs with peers, three without

The shape follows `RepairPath`, the volume's claim about where a lost record can be fetched again.

| `repair` | seal steps |
|---|---|
| `Peers` | write the footer and the journal's seal mark, sync, cut the file at the footer, sync |
| `None` | sync the records first, then the same steps |

With peers, writeback orders nothing, so a crash inside the seal can leave a durable footer that lists records whose bytes never landed. That resolves to a read-time checksum miss and a repair enqueue, which is already how such a volume answers rot. An extra flush would buy nothing the layer above does not.

A sole copy has no such answer, so it syncs the records before the footer goes down. A footer can then never outlive the bytes it lists, whatever order a crash freezes writeback in. `a_sole_copy_footer_never_outlives_its_records` walks every crash boundary of a rolling stream under a scattered writeback schedule and holds the sole copy to it. The same sweep without the ordering fails, which shows the schedule is actually reached.

## Clean shutdown and a crash

`close` first drains compaction over every sealed segment past the dead ratio, ignoring the rate gate. Then it flushes every tail, journal included, gives back the space between the records and the rows, syncs, and leaves each tail unsealed for the next open to resume. `Drop` closes too and logs a failure, since nobody is left to hand one to. A crash leaves the rows written since the journal last went down unwritten, and that is the on-disk difference between the two.

## Corruption

`RepairPath` also decides what corruption is.

| | `Peers` | `None` (sole copy) |
|---|---|---|
| a read that fails its checksum | evicts the key on the spot, so the miss becomes a repair enqueue and the index stops handing out that pointer | returns `Corruption`, the key keeps its place, and the answer repeats until an operator steps in, since a miss would hide the loss |
| the scrub | evicts through the same path | counts hits without evicting |
| compaction's copy step | evicts through the same path | leaves the segment standing, and never unlinks the last copy or stamps a fresh checksum over rot |

Either way the corrupt bytes stay on disk as dead space until the segment retires. The eviction is guarded on the location, so a version that overtook the corruption keeps its place. A segment left standing for rot is pinned out of the ranking, or the pass that cannot retire it would select it again on every tick. `segments_pinned_by_rot` counts what the volume holds for this reason and cannot reclaim until an operator acts. `verify_reads` decides whether a read checks its own record or leans on the scrub.

## Why there is no write-ahead log

A WAL gives a store a cheap sequential place to make a write durable before it reaches its real home, which is otherwise a random write into a tree. Here a record's real home already is a sequential append at the end of a segment, written once in its final position. A WAL in front would write every byte twice and sync twice, to protect a window between the log and the store that does not exist.

The journal holds rows and no values: each record's key, version and place, until the footer says the same at the seal. It sits in the segment's own file past the records and the footer, so it costs no second file and no second sync. It is what lets a small record drop its key, and a batch's rows going in as one group is what makes the batch atomic across a crash. The seal cuts it off.

## Not promised

- Anything the setting did not cover, per the table at the top.
- A view held across several reads. A batch publishes under a barrier, so one read spanning many keys sees all of it or none of it. The index keeps one version per key, so two reads can still straddle a batch. The fixed view is a cue point.
- The order of one batch against another. The journal makes a batch whole, and the sequence number on each record orders it against everything else, exactly as for a single put.
- The window inside a seal on a volume with peers, described above. `RepairPath::None` closes it by ordering the seal.
- That a reader's graves outlast a writer stalled past the 2^20 window. A read-only open following the log cannot see which writes are still out, so it prunes a grave once 2^20 sequence numbers have passed it. A write drawn before a delete and published more than 2^20 numbers later can show on that reader until it reopens. The writer tracks every write from its draw to its publish, and neither its prune nor compaction passes a write still out.
- Anything about power loss on macOS.
