# tape-reel

[![Crates.io](https://img.shields.io/crates/v/tape-reel.svg)](https://crates.io/crates/tape-reel)
[![Documentation](https://docs.rs/tape-reel/badge.svg)](https://docs.rs/tape-reel)
[![License](https://img.shields.io/crates/l/tape-reel.svg)](LICENSE)

Reel is a key-value store and the default storage engine for Tapedrive storage nodes.

## How it works

Reel stores everything in large files called segments and only ever appends to them. A segment takes writes while it's open
and is sealed once it's full or has sat quiet. Its records die as their keys are overwritten or deleted, and cleanup deletes the segment once it
has copied out the records still alive.

![A segment's life: open, sealed, mostly dead and deleted, with surviving records copied back into an open segment](assets/segment-lifecycle.svg)

### Memory

Reel takes one memory budget when it opens and holds its whole heap inside it: the index of keys not yet sealed, the caches,
each segment's block index, the buffers every thread reads and writes through, and any index a column declares. It sizes each
part from the budget and writes every page before open returns, so once the store is open, gets, puts and scans allocate
nothing. A budget too small for what the columns declare is refused at open with the least budget that would do.

A store under 1 GiB holds fewer live scans, read slots and buffers, down to 4 scans at the least budget, and a fifth scan
fails with `ScansFull`. A caller that scans from several threads on a small budget bounds its own scans.

### Writing

A write goes onto the end of whichever open segment is least busy. Several segments are open at once, so writers don't queue
behind each other. When a segment fills up, or sits quiet for a while, reel seals it by writing a sorted index of its contents at the end
of the file, called the footer. After that the segment never changes. A batch of writes lands as one unit, so after a crash
either the whole batch is there or none of it is. Reel has no separate log in front of the data, because the segments themselves
are the log.

### Finding a key

Keys in open segments sit in an in-memory index. For keys in sealed segments a column keeps one of two indexes, and the
budget pays for it:

- A complete table holds a slot for every key by hash, for a column that is only read by key.
- A sorted index holds every key's newest place in key order, for a column that is also scanned. Its keys are one fixed width,
  and a column of fixed-width keys keeps one unless its config says `ColumnIndex::Plain`.

A column with neither searches the footers of the sealed segments, skipping any whose key range or filter rules the key out.
Either way, one disk read usually fetches the value. The files are the source of truth, so reel can rebuild every index from
them when it opens.

A sorted index has no file of its own. Every open builds it again from the rows in the footers, which hold about 12 bytes a
key, on as many threads as the store keeps workers. At 10 million keys the build reads 118 MB and takes 0.36 s on 4 threads. A
complete table saves itself to a file at a clean close, and the next open reads that file in place of the rebuild.

An open in a new process also writes every page of the budget before it returns. At 10 million keys and a 2 GiB budget the
whole open takes 1.55 s, of which about 1.2 s is writing those pages, and a bigger budget takes longer.

### Walking keys in order

Segments are in the order things were written, so any key can sit in any segment. On a column with a sorted index a scan takes
its keys from the index in runs and reads their values together. On any other column it merges the footers of every segment
that holds part of its range, and cleanup keeps the number of segments over any key under a bound so that merge stays short.
A caller can tell a scan how many rows it will take, and the scan then locates and reads only that many at a time.

### Deleting

Nothing changes in place, so a delete appends a small marker called a grave, which says the key is gone, and a range delete
appends one marker over a whole range. The old values and the markers stay on disk until cleanup reaches them.

### Cleanup

Cleanup runs on reel's own worker threads, at a lower priority than the callers:

- A walk reads each sealed segment's rows and counts how much of it is dead, meaning overwritten or deleted.
- A wholly dead segment is deleted outright, which costs almost nothing.
- A mostly dead segment has its surviving records copied out, and then the old file is deleted.
- Where too many segments cover one key, a merge folds some of them into one.
- Graves are dropped during a copy once nothing older can come back.

## What it's good at

Reel works best when data is written once and expires in whole groups, as a storage node's slices do. Whole segments go dead
together and get dropped for free. It writes each value once and moves it again only when cleanup copies a mostly dead segment or
merges segments that cover the same keys. It's weaker when the same keys get written and deleted
over and over. The waste then spreads across segments that stay mostly alive, so cleanup either copies a lot or waits.

## Limits

A sorted index takes keys of one fixed width, and a column has one index kind, sorted or complete. A sorted index is sized at
each open from its column's rows on disk, dead rows among them: twice them with a share of the headroom past them, out of the
block, spot and record caches, which keep a quarter of their room. The store keeps a quarter of what the indexes may take as
headroom, shared evenly among them and at most 256 MiB each. A store that takes its first rows in one open sizes the index
from the rows its column declares, so a column that grows past them in that open marks its index until the next open.
A complete table holds the `least_rows` its column gives it. Rows past an index's room mark it, and from then on gets and
scans of that column read the footers until the store is opened again with room for them. The maintenance report shows each
index's rows, room and mark, and the bytes a sorted index asked for and got, so an index about to fill shows before it does
and a budget too small for it shows why. A count of the keys under a prefix reads
the segments, with or without a sorted index. A damaged segment that held range deletes stays damaged until someone clears
it, since reel can't tell what those deletes hid once their footer is gone.

## How it compares

Measured on 17 November 2026 at commit cc90e20 on a machine with 4 cores, 8 threads, 32 GB and local NVMe, one pass, with
transparent huge pages set to always and each run held to 8 GiB of memory. Every engine gets a 2 GiB cache, 8 loader and 8
reader threads, and syncs its writes to disk every 4 MiB written: RocksDB syncs its log, fjall its journal, LMDB runs with its
sync on each commit off and a forced sync at each 4 MiB, and redb can only sync at its next commit. Keys are 16 bytes and
values 100 bytes, 10 million records loaded in batches of 1,000. Reel runs as a caller gets it by default: a sorted index on
its fixed-width keys, and scans not told how many rows they take. The numbers are operations a second.

The feed workload loads records as posts, 50 to a user, then reads each user's newest 20 posts and scans 10, 100 and 1,000
keys in a row from random places:

| engine | load | feed of 20 | scan 10 | scan 100 | scan 1,000 | write amp | space amp |
|---|---|---|---|---|---|---|---|
| reel | 2.50M | 666k | 632k | 201k | 26k | 1.7 | 1.17 |
| RocksDB | 2.13M | 442k | 749k | 255k | 40k | 3.5 | 1.00 |
| LMDB | 1.30M | 2.44M | 2.74M | 843k | 117k | 3.3 | 2.09 |
| fjall | 1.20M | 288k | 336k | 167k | 25k | 3.9 | 1.06 |
| redb | 485k | 851k | 875k | 470k | 70k | 3.1 | 1.80 |

The point read workload loads records whose keys spread at random, then reads a random key 95 times in 100 and overwrites one
the other 5:

| engine | load | reads and writes | read p99 | write amp | space amp |
|---|---|---|---|---|---|
| reel | 2.15M | 1.94M | 8.5 us | 1.5 | 1.41 |
| RocksDB | 976k | 1.13M | 15.3 us | 3.9 | 1.41 |
| fjall | 500k | 979k | 16.9 us | 4.9 | 1.25 |
| LMDB | 126k | 573k | 106 us | 34.0 | 1.64 |
| redb | 58k | 240k | 122 us | 30.8 | 1.94 |

Reel loads fastest of the five, answers point reads fastest, and writes the least to disk for each byte stored. It scans
slower than LMDB and redb, which keep one B-tree, and slower than RocksDB on runs of 10 keys or more. A scan that says how many
rows it takes runs faster on the same store: 864k, 306k and 34k a second for 10, 100 and 1,000 keys. A scan beside a writer
putting 20,000 records a second falls further behind: at 16 million keys reel scans 100 keys 80k times a second against
RocksDB's 220k. RocksDB stores the feed workload in less space, since its keys share most of their bytes within a user.

## More

The API is documented on [docs.rs](https://docs.rs/tape-reel). Licensed under Apache-2.0. See [LICENSE](LICENSE).
