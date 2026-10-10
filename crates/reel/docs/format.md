# On-disk format

A volume holds segments of records. A sealed segment ends in a footer that indexes its records. An
open segment keeps the same rows in a journal region of its own file until its seal. A record of
4 KiB or less is its check, its shape and its payload, and its key lives only in its row. Beside the
segments, a volume keeps key runs: sorted rows its walks read in place of the footers they cover.
This page covers what the fields are for. The code has the byte offsets.

The format version is 8, stamped into every segment header record. A build meeting a version it
can't read refuses the whole file there. A new shape in the segment stream is a new version, every
time.

| version | what changed |
|---|---|
| 8 | the segment header record gained the journal's rows offset, when the journal moved into the segment file |
| 7 | the segment header record dropped its band field |
| 6 | keyless records, whose prefix a version 5 build would read as a header |

## A segment

```
offsets rising
+--------------------------------------------------------------+
| segment header record      no sequence number, no key        |
+--------------------------------------------------------------+
| record | record | record | ...                               |
+--------------------------------------------------------------+
| footer                                                       |
|   column partitions | filter region | directory | fixed tail |
|   in column order                                     64 B   |
+--------------------------------------------------------------+
```

Until the seal, the space past the write head is the reservation the tail took from the
filesystem, which reads back as zeros, and past that the file holds the journal. The seal writes
the footer after the last record, writes a seal mark, syncs, and cuts the file at the footer's end,
which takes the journal with it. The seal mark is 20 bytes at the end of the block after the
journal's written rows: the footer's end, its length, a CRC32C and the magic `SEAL`. An open whose
cut never landed finds the footer through the mark.

## Records

| | keyless | keyed |
|---|---|---|
| used for | payloads of 4 KiB or less | larger payloads, and the segment header record |
| on disk | check (8), shape (2), payload | 21-byte header, key at the column's width, payload |
| key, sequence number, flags | in the footer row, or the journal row until the seal | in the header |
| integrity | SipHash-1-3 check under the segment's secret | CRC32C over header, key and payload |

A keyless record:

```
| check, 8 | shape, 2 | payload |
```

The check is a SipHash-1-3 over the record's column, key width, kind, shape, the payload's CRC32C
and its key, keyed by a 16-byte secret drawn at random for each segment and kept in its header
record. The shape is the payload length shifted up two bits over the codec. A reader that holds the
key confirms the record by computing the check again. A writer choosing keys can't make one key's
record check as another's, since the secret belongs to the segment.

A larger record and the segment header record keep a header and the key. A key is a small share of
a large record, and with the header a read can take a window of the payload without the rest:

```
| header, 21 bytes | key, the column's own width | payload |

  0     4     8            16    17    18      20    21
  +-----+-----+------------+-----+-----+-------+-----+
  | len | crc |    lsn     | flg | col | width | cdc |
  +-----+-----+------------+-----+-----+-------+-----+
    4     4         8         1     1      2      1
```

The header is fixed size. The key is variable: a column stores its keys at the width it declares
with no padding, and the width sits in the fixed part, so a reader knows where the payload starts
before it reads the key.

| field | bytes | what it is |
|---|---|---|
| length | 4 | payload bytes after the key |
| crc | 4 | CRC32C over the header with this field zeroed, then the key, then the payload |
| lsn | 8 | append sequence number, which orders this record within the volume |
| flags | 1 | what kind of record it is, and how it was committed |
| column | 1 | the key's column |
| key width | 2 | bytes of key between the header and the payload |
| codec | 1 | zero for raw bytes, else the codec that produced the stored payload |

The codec byte was the header's reserved byte until per-column compression used it. Zero is raw, so
a payload stored without a codec reads correctly whatever the column later declares. A compressed
payload opens with a four-byte logical length so a read can size its output buffer. The header's
length field still counts stored bytes.

The key width takes two bytes because a key can be longer than one byte can count. 21 bytes plus a
108-byte key is 129, exactly the io layer's inline write buffer, so a record's prefix never
allocates. The buffer is sized from the prefix by definition.

The format's key ceiling is 1056 bytes. That is the most the width field has to express, and it
says nothing about the widths columns use. A column declares one fixed width or declares that its
keys vary. A fixed width is admitted only where the index has an arm for it: 0, 2, 8, 12, 16, 20,
24, 32, 34, 36, 40, 44, 48, 72, 96 or 108. Any other width is refused at open, with no padding up,
because padding is paid on every record of every column, and the columns that dominate the byte
count have the shortest keys. A varying column pays a pointer per key, and so does any key wider
than 108 bytes.

## Flags

The low five bits say what kind of record it is and are exclusive. The relocated bit marks a
compaction copy and travels with a kind. A keyless record keeps its flags in its row, and its check
covers the kind.

| bit | value | meaning |
|---|---|---|
| none set | `0000_0000` | a data record with a payload |
| tombstone | `0000_0001` | a delete of one key, no payload |
| range tombstone | `0000_0010` | a delete of a half-open range, payload is the exclusive end |
| segment header | `0000_1000` | the first record of a segment, payload is the header |
| relocated | `0100_0000` | compaction's copy of a record written earlier |

Every other bit is unclaimed. A bit outside the kinds and the mark is refused, and so are shapes
made of legal bits that no writer produces. A torn flags byte fails before anything behind it is
read.

**relocated**: a compaction copy keeps the sequence number of the record it copied, because
newest-wins has to resolve exactly one version. So nothing else about the record says it is a copy.
In the writing process that never matters, since the copy repoints an entry the compactor already
resolved. A reader following the log from outside has only the record. Without this bit it would
read a relocation as a write that lost an ordering race, discard it, and keep pointing into a
segment about to be unlinked.

The bit is covered by a keyed record's checksum and lives in a keyless record's row, so it can't be
stamped onto a record after the fact.

## Zero is never a record

The sequence counter starts at one, so zero is reserved for the segment header record. A tail
reserves space from the filesystem ahead of its write head, and that space reads back as zeros. A
keyless prefix of zeros checks as unwritten, and a header of zeros parses as a data record with
sequence number zero, which no writer issues. Either way a read of a place nothing wrote answers
stale.

## Alignment

The block boundary is 4096. Only a volume whose writes go straight to the device covers whole
blocks, which today means a `UringDirect` volume. There each write's span is rounded up to the
boundary with zeros after its records, so the next reservation starts on one. Nothing reads the
zeros, since the rows list where every record sits. Every other backend lets the kernel assemble
the block and writes the records alone.

## The journal

An open segment keeps its rows in a journal region of its own file, from the offset its header
record gives: twice `segment_bytes`, rounded up to a block. The file is sized out to that offset
when the segment is created, so a file that ends before it was cut by a seal. Each write adds one
group: the rows of the records it put down, a batch's rows together. The journal's bytes count
against `segment_bytes`, so a segment rolls once its records and journal together fill it, and the
footer a seal writes after the records always ends before the journal. One sync of the file makes
a record and its row durable together.

```
| rows, 4 | bytes, 4 | row | row | ... | crc32c, 4 |

row: | column, 1 | key width, 2 | key | lsn, 8 | offset, 4 | length, 4 | flags, 1 | range end |
```

A range tombstone's row holds its exclusive end after its length, `0xFFFF` for none. The CRC
covers the group's head and rows, so a group a crash cut short fails it and the journal ends there.
A direct volume writes whole blocks, so a group can be followed by zeros up to a block boundary, and
the next group starts on that boundary.

A put leaves its group in memory. A flush writes the pending groups ahead of the file's sync, and
writeback pacing writes them too. A reopen reads the journal's whole groups and keeps a group only
when every record it lists sits where its row says and checks out, so a batch comes back whole or
not at all. A resumed tail appends after the last whole group and writes new records past every
record any group lists, so a dropped group can never match a new record.

The key lives only in the rows, and that has two costs. A process crash under `Never` or `Bytes`
loses what was written since the journal last went down: at most one pace or one sync threshold.
And a footer that rots after its seal leaves its segment's records unlisted, since no walk can find
a keyless record's key. A volume with peers repairs them from a peer.

## The segment header record

Every segment opens with one. Its payload starts with a frozen prefix, a two-byte format version
and a four-byte segment number. Then come a layout byte, the 16-byte secret its keyless records are
checked under, and the eight-byte offset of the journal rows. It takes no sequence number and no
key, since nothing resolves it.

Frozen means an older build can read the version and segment number of a file a newer build wrote,
so a longer payload from a future version still parses. That lets recovery tell a foreign file from
a corrupt one. A file whose first record is not a valid segment header, or whose header holds
another segment number or another format version, is quarantined and left on disk untouched.

## The footer

A sealed segment ends in a packed sorted index of its records. One segment holds records from
every column, so the footer has one partition per column, each sorted by key.

```
| column partitions, in column order | reserved filter region | directory | fixed tail |
```

A row is the key, then the sequence number, the offset, the payload length and the record's flags.
Every partition packs its rows in restart blocks of 16. A row stores only the key bytes it doesn't
share with the row before, and its sequence number and offset as varint differences from the row
before. The parse rebuilds whole rows, so this packing exists only on disk. The directory lists
each partition's column, key width, row count and encoded span, and a fixed-width partition whose
rows are packed sets the width's high bit.

In memory the rows sit end to end as the tail writes them, with no struct per row. A gibibyte of
kilobyte records is a million rows, and this form costs the bytes it will occupy and nothing more.

The fixed tail is 64 bytes. Reading backwards from the end it holds the magic, the footer length,
the footer's own checksum, the highest and lowest sequence numbers in it, the live and dead byte
tally, the row count, the partition count, the filter length, and the sequence frontier the seal
happened at. That frontier tells a rebuild which shadowings the tally already counted.

Three things about the footer are design choices:

- **Segment headers are not listed.** A reader never resolves one by key, so a footer indexes only
  what a key can reach.
- **The flags byte is in the row.** A point tombstone and an empty data record both have length
  zero, and a rebuild that couldn't tell them apart would go back to the segment for one header
  read per zero-length entry. The byte costs less than those reads.
- **The filter region holds the index tier's blooms.** One filter header per partition, in
  directory order, and a zero `filter_bits` writes the region at zero length. It was the format's
  last extension point, and the index tier used it up, as per-column compression used the
  header's spare byte. Anything more is a format version.

The footer's checksum covers the whole footer with its own field zeroed, including the length and
the magic. A footer with a wrong magic, a length out of range or a failing checksum is no footer. A
segment whose seal stopped part way still has its journal and is read back through it. A segment
whose footer went bad after its seal has nothing that lists its records.

## Key runs

A volume merges its walk's runs into a key run once more than eight stand over one key. Its file is
`<id>.keys` with a twelve-digit id, written as `<id>.keys.part` until it is whole. The writer syncs
it, renames it and syncs the directory, so a run under its own name is complete. An open unlinks
any `.part` it finds, and any run a newer run covers whole.

```
| column rows, in column order | column fences | directory | covered segments | trailer |
```

A row is 8 bytes: its segment's place in the covered list, then its row's place in that segment's
footer partition for the column. Everything else about the row, the key, the sequence number, the
offset, the length and the flags, is in that footer row, which never changes once the seal writes
it. A block is 8 rows. Each column's fences hold the first key of every block and the column's last
key, each after its length, so a seek finds its block without reading a row and then searches the
block through the footers. The directory gives each column its id, key width, block rows, row
count, where its rows start and where its fences start, 31 bytes a column. The covered segments are
the ones whose footers the run stands in for during a walk, in the order the rows point into them.
The trailer is 20 bytes: where the directory starts, the column count, the covered count and the
magic `KRN2`.

A key run holds nothing the footers lack, only an order over their rows. Gets and recovery still
read the footers, so a lost run only hands its segments back to the walk. A row into a segment that
compaction has retired reads as nothing: its live records were copied into the map or a newer
footer before the retire, and a walk skips it. An open unlinks a run it can't parse, so this layout
changes without a format version.

## What bounds a volume

A resident pointer is a segment number, a byte offset within it, and a payload length, all 32-bit.
The offset caps a segment below four gibibytes, which config validation enforces. The default
segment is one gibibyte.

## Sorted compaction output and the sparse footer

The sorted output is built. The sparse footer is not. They sit here together because the footer row
shape is cheap to change now and expensive later.

A footer has to list every row because append order is not key order. A compacted segment can be
different. Compaction already reads and writes everything, and the source footer is already sorted
by key, so a rewrite applies its records in key order and its destination is a sorted run at no new
io.

That allows something nothing uses yet. Once offset order equals key order inside a segment, the
footer can hold one entry per **page** of records, with a short linear scan inside the page. The
scan is free, because the page was being fetched anyway. At 1 KiB records that is 20 bytes per four
records against 33 per record, about 6.6x smaller. At 100-byte metadata records it is closer to
100x. It also puts a slot's or a group's records next to each other, so `get_many` merges them into
one read. The shape is two footer kinds behind the directory, dense for tails and sparse for sorted
segments, picked by a flag the seal already knows.

This ranks above bit-packing the tail because footer size is a small-record problem. An unpacked
row is the key plus 17 fixed bytes at the column's own key width, which lands very differently per
shape:

| shape | key | row, unpacked | footer as a share of a 1 GiB segment |
|---|---|---|---|
| bulk records, 64 KiB payloads | 34 | 51 B | 0.08% |
| agave shred, 1 KiB records | 16 | 33 B | 3.2% |
| metadata, 100 B records | 32 | 49 B | about 32% |

It gets worse as `filter_bits` rises: at 14 the filter adds another 1.75 bytes a key in every
sealed segment, whether anything probes it or not.

One smaller item belongs with it. **Rows a segment shadowed itself can be dropped at seal**, since a
key written twice into one segment only needs its newest row. Today the footer keeps every version.

Some of this is already done. The restart-block packing stores only what a key doesn't share with
the row before, with a search that never rebuilds a key to compare it, and every partition is
written through it. The row tails keep the sequence number and offset as differences from the row
before, so a per-partition sequence delta would add nothing.

## What would break the format

Each of these is cheap to change now and expensive later.

- **The checksum algorithm.** A stored value only reproduces under the algorithm that produced it.
  It changed once, from CRC64/NVME to CRC32C, and won't change again.
- **The record header layout and the keyless prefix.** Every read of a record parses one or the
  other.
- **The footer row shape.** Every sealed segment's footer is written in it.

The filter region extends the format without breaking it, and so would a whole-segment hash written
into it at seal time.
