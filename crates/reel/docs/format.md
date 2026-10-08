# The on-disk format, and what each field is carrying

A volume holds segments of records. A sealed segment ends in a footer that indexes
what it took, and an open one keeps the same rows in a journal region of its own file
until its seal. A record of 4 KiB or less is its check, its shape and its payload, and its key
lives only in its row. Beside the segments a volume keeps key runs, sorted rows
its walks read in place of the footers they cover. This document says what the
fields are for, not what the byte offsets are, which `format/` states once and does
not need restating.

The format version is 7, stamped into every segment header record. A build meeting a
version it cannot read refuses the whole file there.

It moved from 6 when the segment header record dropped its band field, and from 5 for
keyless records, whose prefix a version 5 build would read as a header. A new shape in
the segment stream is a version, every time.

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

Until the seal, the space past the write head is the reservation the tail took from
the filesystem and reads back as zeros, and the file runs on past it to the journal.
The seal writes the footer after the last record, syncs it, and cuts the file at the
footer's end, which takes the journal with it.

## A record

A record whose payload is 4 KiB or less lies keyless:

```
| check, 8 | shape, 2 | payload |
```

The check is a SipHash-1-3 over the record's column, key width, kind, shape, the
payload's CRC32C and its key, keyed by a 16-byte secret drawn at random for each
segment and kept in its header record. The shape is the payload length, shifted up
two bits over the codec. The record holds no key, no sequence number and no flags:
its footer row holds them, or its journal row until the seal, and a reader that holds
the key confirms the record by computing the check again. A writer choosing keys
cannot make one key's record check as another's, since the secret is the segment's
own.

A larger record, and the segment header record, keep a header and the key, since a
key is a small share of a large record and a window into one reads without the rest
of its payload:

```
| header, 21 bytes | key, the column's own width | payload |

  0     4     8            16    17    18      20    21
  +-----+-----+------------+-----+-----+-------+-----+
  | len | crc |    lsn     | flg | col | width | cdc |
  +-----+-----+------------+-----+-----+-------+-----+
    4     4         8         1     1      2      1
```

The header is fixed size on purpose. The key is variable, because a column stores
its keys at the width it declares, padded to nothing, and the width lives in the
fixed part, so a reader knows where the payload starts before it reads the key.

| field | bytes | what it is |
|---|---|---|
| length | 4 | payload bytes following the key |
| crc | 4 | CRC32C over the header with this field zeroed, then the key, then the payload |
| lsn | 8 | append sequence number ordering this record within the volume |
| flags | 1 | what kind of record it is, and how it was committed |
| column | 1 | which column the key belongs to |
| key width | 2 | bytes of key between the header and the payload |
| codec | 1 | zero for raw bytes, else the codec that produced the stored payload |

The codec byte was the header's reserved byte until per-column compression
spent it. Zero is raw, so a payload stored without a codec reads correctly under
the rule whatever the column later declares. A compressed payload opens with a
four byte logical length so a read can size its output buffer; the header's
length field keeps meaning stored bytes. `compression.md` carries the framing and
the rules around it.

The key width takes two bytes rather than one because a key runs past what a
byte can say. Twenty-one bytes plus a key of 108 is 129, which is exactly the
inline write buffer the io layer carries, so a record's prefix never allocates.
The buffer is sized from the prefix by definition rather than asserted against
it.

The format's key ceiling is 1056 bytes, which is what the width field has to be
able to say rather than a size anything occupies. A column either declares one
fixed width or declares that its keys vary. A fixed width is admitted only where
the index holds an arm for it: 0, 2, 8, 12, 16, 20, 24, 32, 34, 36, 40, 44, 48,
72, 96 or 108. Anything else is refused at open rather than quietly padded up,
because padding is paid on every record of every column and the columns that
dominate the byte count are the ones with the shortest keys. A varying column
pays a pointer per key instead, and 108 is the width past which it does.

## The flags, and why some of them ride along

The low five bits say what kind of record it is and are exclusive. The relocated bit
says a compaction wrote the record and travels with a kind. A keyless record keeps
its flags in its row, and its check covers the kind.

| bit | value | meaning |
|---|---|---|
| none set | `0000_0000` | a data record with a payload |
| tombstone | `0000_0001` | a delete of one key, no payload |
| range tombstone | `0000_0010` | a delete of a half-open range, payload is the exclusive end |
| segment header | `0000_1000` | the first record of a segment, payload is the header |
| relocated | `0100_0000` | compaction's copy of a record written earlier |

Every other bit is unclaimed, so a bit outside the kinds and the mark is refused,
and so are the shapes made of legal bits that no writer produces. A torn flags byte
fails before anything is read behind it.

**relocated** is what a compaction copy needs. The copy carries the sequence
number of the record it copied, because newest-wins has to resolve one version and
not two, which means nothing else about the record says it is a copy. In this
process that never matters, since the copy repoints an entry the compactor already
resolved. A reader following the log from outside has only the record, and without
this bit it would read a relocation as a write that lost an ordering race, discard
it, and go on pointing into a segment about to be unlinked.

The bit is covered by a keyed record's checksum and lives in a keyless record's row,
so it cannot be stamped onto a record after the fact.

## Zero is not a record

The sequence counter issues from one, so zero is reserved for the segment header
record. A tail reserves space from the filesystem ahead of its write head, and that
space reads back as zeros: a keyless prefix of zeros checks as unwritten, and a
header of zeros parses as a data record with sequence number zero, which no writer
issues. Either way a read of a place nothing wrote answers as stale, never as a
record.

## Alignment, and who pays for it

The block boundary is 4096. Only a volume whose writes go straight to the device
covers whole blocks, which today is the direct ring. There each write's span is
rounded up to the boundary with zeros behind its records, so the next reservation
starts on one. Nothing reads the zeros, since the rows list where every record sits.
Every other backend has the kernel assemble the block and writes the records alone.

## The journal

An open segment keeps its rows in a journal region of its own file, from the offset
its header record gives, twice `segment_bytes` rounded up to a block. The file is
sized out to that offset when the segment is drawn, so a file that stops short of it
was cut by a seal. Each write adds one group: the rows of the records it put down, a
batch's rows together. The journal's bytes count against `segment_bytes`, so a segment
rolls once its records and journal together fill it, and the footer a seal writes
after the records always ends short of the journal. One sync of the file makes a
record and its row durable together.

```
| rows, 4 | bytes, 4 | row | row | ... | crc32c, 4 |

row: | column, 1 | key width, 2 | key | lsn, 8 | offset, 4 | length, 4 | flags, 1 | range end |
```

A range tombstone's row holds its exclusive end behind its length, `0xFFFF` for
none. The CRC covers the group's head and rows, so a group a crash cut short fails it
and the journal ends there. A direct volume writes whole blocks, so its groups can end
in zeros out to a block boundary, and the next group opens on the boundary.

On Linux with buffered writes a put writes its group into the journal before it
returns. Elsewhere a flush writes the pending groups ahead of the file's sync, and
writeback pacing writes them too. A reopen reads the journal's whole groups and keeps
a group only when every record it lists sits where its row says and checks out, so a
batch comes back whole or not at all. A resumed tail appends after the last whole
group and writes its records past every place any whole group names, so a group
turned down once never meets a record that checks out.

Two things follow from the key living only in the rows. Off Linux, or on a direct
volume, a crash of the process under `Never` or `Bytes` loses what was written since
the journal last went down: at most one pace or one sync threshold. And a footer that rots after its seal leaves its segment's records
unlisted, since no walk can find a keyless record's key. A volume with peers repairs
them from a peer.

## The segment header record

Every segment opens with one. Its payload opens with a frozen prefix, a two byte
format version and a four byte segment number, then a layout byte and the 16-byte
secret its keyless records are checked under. It takes no sequence number and no key,
since nothing resolves it.

Frozen means an older build can read the version and segment number of a file a
newer build wrote, so a longer payload from a future version parses rather than
failing. That is what lets recovery say "this file is not mine" instead of "this
file is corrupt": a file whose first record is not a valid segment header, or
whose header names another segment number or another format version, is
quarantined and left on disk untouched.

## The footer

A sealed segment ends in a packed sorted index of the records it took. One segment
holds records from every column, so the footer is partitioned: each column's rows
sit together, sorted by key, fixed-stride at that column's own key width.

```
| column partitions, in column order | reserved filter region | directory | fixed tail |
```

One row is the key, then the sequence number, the offset, the payload length and the
record's flags. A partition packs its rows with `format/prefix.rs`'s restart-block
encoding: keys prefix compressed where their sorted fronts share enough to pay for it,
and each row's tail stored as varint differences from the row before. The parse
rebuilds whole rows, so the encoding lives only on disk. The directory lists each
partition by column, key width, row count and encoded span, which is what lets
the rows stride at the natural width instead of the widest one. The fixed tail is 64 bytes and
holds, reading backwards from the end: the magic, the footer length, the footer's
own checksum, the highest and lowest sequence numbers in it, the live and dead
byte tally, the row count, the partition count, the filter length, and the
sequence frontier the seal happened at, which is what tells a rebuild which
shadowings the tally already counted.

Rows are held in packed form as the tail writes them rather than as structs,
because a gibibyte of kilobyte records is a million rows and the packed form costs
the bytes it will occupy and nothing more.

Three things about the footer are worth saying because they are decisions rather
than layout:

**Segment headers are not listed.** A reader never resolves one by key, so a footer
indexes only what a key can reach.

**The flags byte is in the row.** A length of zero belongs to a point tombstone
and to an empty data record alike, and a rebuild that could not tell them apart
would go back to the segment for one header read per zero-length entry. The byte
is cheaper than the reads it saves.

**The filter region holds the index tier's blooms.** One filter header per
partition, walked in directory order, with a zero `filter_bits` writing the
region at zero length. It was the format's last extension point and the index
tier spent it, as the header's codec byte went to per-column compression.
Anything more is a format version.

The footer's checksum covers the whole footer with its own field zeroed, including
the length and the magic. A footer whose magic is wrong, whose length is out of
range, or whose checksum fails is not a footer. A segment whose seal stopped part
way through still runs out to its journal and reads back through it, and one whose
footer went bad after its seal has nothing that lists its records.

## Key runs

A volume merges its walk's runs into a key run once more than eight stand
over one key. Its file is `<id>.keys` with a twelve-digit id, written as
`<id>.keys.part` until it is whole. The writer syncs it, renames it and syncs the
directory, so a run under its own name is complete. An open unlinks any `.part` it
finds, and any run a newer run covers whole.

```
| column rows, in column order | column fences | directory | covered segments | trailer |
```

A row is the key, then the sequence number, the segment, offset and length of the
record, and its flags, which is 21 bytes past the key. A column of one key width
strides at it. A column whose keys vary puts each key's two-byte length ahead of
it, and a table of eight-byte row starts after its rows. A block is the span of
rows a search lands in: 8 KiB of rows at a fixed width, 128 rows at a varying one.
Each column's fences hold the first key of every block and the column's last key,
each behind its length. The directory gives each column its id, key width, block
rows, row count, where its rows start, their length and where its fences start,
39 bytes a column. The covered segments are the ones whose footers the run answers
for in a walk. The trailer is 20 bytes: where the directory starts, the column
count, the covered count and the magic `KRUN`.

A key run holds nothing the footers lack. The footers stay the authority for gets
and for recovery, so a run that is lost only gives its segments back to the walk.
An open unlinks a run it cannot parse, so this layout changes without a format
version.

## What bounds a volume

A resident pointer is a segment number, a byte offset within it, and a payload
length, all 32 bit. The offset is what caps a segment below four gibibytes, which
config validation enforces rather than leaving to be discovered. The default
segment is one gibibyte.

## Sorted compaction output, and the sparse footer it allows

The sorted output is built and the sparse footer is not. Recorded together
because the footer row shape is cheap to change now and expensive later, so the
decision belongs beside the format rather than in a backlog.

The footer has to name every row because append order is not key order. **A
compacted segment does not have to be like that.** Compaction already reads
everything and writes everything, and the source footer is already sorted by key,
so a rewrite applies its records in key order and its destination is a sorted run
at no new io.

What that unlocks and nothing yet spends: once offset order equals key order
inside a segment, the footer can hold one entry per **page** of records instead
of one per record, with a short linear scan inside the page. That scan is free,
because the page was being fetched anyway.

At 1 KiB records that is 20 bytes per four records instead of 33 per record,
about 6.6x smaller. At 100 byte metadata records it is closer to 100x. It also
makes front coding work and puts a slot's or a group's records physically next to
each other so `get_many` merges them into one read.

The shape is two footer kinds behind the directory, dense for tails and sparse
for sorted segments, picked by a flag the seal already knows.

Why this ranks above bit-packing the tail: footer size is not a large-record
problem, it is a small-record problem. A row is the key plus 17 fixed bytes, at
the column's own key width, which lands very differently per
shape:

| shape | key | row | footer as a share of a 1 GiB segment |
|---|---|---|---|
| bulk records, 64 KiB payloads | 34 | 51 B | 0.08% |
| agave shred, 1 KiB records | 16 | 33 B | 3.2% |
| metadata, 100 B records | 32 | 49 B | about 32% |

It gets worse as `filter_bits` rises: at 14 the filter is another 1.75 bytes a
key in every sealed segment whether anything probes it or not.

Two smaller format-adjacent items belong with it. **Rows a segment shadowed
itself can be dropped at seal**, since a key written twice into one segment only
needs its newest row. And the footer already stores min and max lsn per segment,
so **a per-partition lsn delta in the row costs nothing to derive at seal**.

One piece of it is already spent. `format/prefix.rs` is a full restart-block
encoder for footer rows that store only what a key does not share with the row
before it, with a search that never rebuilds a key to compare it, and the
varying-width partitions are written through it: `footer.rs` packs their rows
with `PrefixRows` and `block.rs` reads the restart table beside the directory.
The fixed-width partitions still stride, which is where the front coding is not
pointed.

## What would break the format

Named because each is cheap now and expensive later.

- **The checksum algorithm.** A stored value is only reproducible under the
  algorithm that produced it. `checksum.md` is the record of the one change made
  here and why it will not be made again.
- **The record header layout and the keyless prefix.** Every read of a record parses
  one or the other.
- **The footer row shape.** Which is why the value inlining shared the deadline
  the checksum had, and it met it: values at or below a 4 byte ceiling ride in
  the row and the entry, landed while the format was open.

The filter region extends the format rather than breaking it, and so would a
whole-segment hash written into it at seal time.
