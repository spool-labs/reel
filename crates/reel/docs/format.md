# The on-disk format, and what each field is carrying

Everything a volume holds is one shape repeated: a fixed header, a key, a payload.
A sealed segment ends in a footer that indexes what it took. There is nothing else
on disk. This document says what the fields are for, not what the byte offsets are,
which `format/` states once and does not need restating.

The format version is 4, stamped into every segment header record. A build
meeting a version it cannot read refuses the whole file there, rather than
truncating its walk at an unknown record kind and losing the tail silently.

It moved from 3 for the batch frame, which is a record kind a version 3 walk has
never seen. That walk would refuse the frame's flags byte and stop at it, taking
the rest of the tail with it, which is exactly the silent loss the version number
exists to prevent. A new kind in the segment stream is a version, every time.

## A segment

```
offsets rising
+--------------------------------------------------------------+
| segment header record      no sequence number, no key         |
+--------------------------------------------------------------+
| record | record | record | ...                                |
+--------------------------------------------------------------+
| record | pad | record | ...      pad only on a direct volume  |
+--------------------------------------------------------------+
| pad record bridging the reserved slack, written at the seal   |
+--------------------------------------------------------------+
| footer                                                        |
|   column partitions | filter region | directory | fixed tail  |
|   in column order                                     64 B    |
+--------------------------------------------------------------+
```

Until the seal, the space past the write head is the reservation the tail took
from the filesystem: it reads back as zeros, and the walk recognises that shape.
The seal does not give it back. It fills the gap with one pad record and writes
the footer past it, so the file's length never moves backwards.

## A record

```
| header, 21 bytes | key, the column's own width | payload |

  0     4     8            16    17    18      20    21
  +-----+-----+------------+-----+-----+-------+-----+
  | len | crc |    lsn     | flg | col | width | cdc |
  +-----+-----+------------+-----+-----+-------+-----+
    4     4         8         1     1      2      1
```

The header is fixed size on purpose. The key is variable, because a column stores
its keys at the width it declares rather than padded to the widest width any
column declares, and the width lives in the fixed part, so a walk finds the next
record without parsing a variable-length header first. That is the whole reason
the width is a header field rather than something the column table is consulted
for: recovery walks a segment before it knows which columns the volume serves.

| field | bytes | what it is |
|---|---|---|
| length | 4 | payload bytes, or pad fill, following the key |
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

The low five bits say what kind of record it is and are exclusive. The two above
them say how it was committed and travel with a kind.

| bit | value | meaning |
|---|---|---|
| none set | `0000_0000` | a data record with a payload |
| tombstone | `0000_0001` | a delete of one key, no payload |
| range tombstone | `0000_0010` | a delete of a half-open range, payload is the exclusive end |
| pad | `0000_0100` | alignment filler, no payload written |
| segment header | `0000_1000` | the first record of a segment, payload is the frozen header |
| batch frame | `0001_0000` | the record opening a batch, payload is the run it declares |
| batched | `0010_0000` | one record of a framed batch |
| relocated | `0100_0000` | compaction's copy of a record written earlier |

The high bit is unclaimed, so the tripwire is both: a bit outside the kinds and
the marks is refused, and so are the shapes made of legal bits that no writer
produces, since the kind bits are exclusive and marks ride only on records a
batch or a compaction can contain, which no control record is. Every byte a
writer ever produced passes, and a torn flags byte fails before anything is read
behind it.

The last three exist for a reader that is not the writer.

**batch frame** is what makes a batch atomic across a crash. The records of a
batch take one contiguous reservation, and a frame at the front of it declares
how many of them there are and how many bytes they take, so a rebuild keeps the
run only when exactly that is there and verifies. **batched** rides every record
of the run, so a record that reaches a walk without its frame is dropped rather
than applied on its own. Without the pair there is nothing on disk that says
where a batch starts or stops, and a crash inside one would half-apply.

**relocated** is what a compaction copy needs. The copy carries the sequence
number of the record it copied, because newest-wins has to resolve one version and
not two, which means nothing else about the record says it is a copy. In this
process that never matters, since the copy repoints an entry the compactor already
resolved. A reader following the log from outside has only the record, and without
this bit it would read a relocation as a write that lost an ordering race, discard
it, and go on pointing into a segment about to be unlinked.

Both are covered by the checksum, so neither can be stamped onto a record after
the fact: a remarked record would carry the checksum of the record it used to be.

## Zero is not a record

The sequence counter issues from one, so zero is reserved. Control records carry
it, and a header that parses as a data record with a zero sequence number is
unwritten space rather than a record.

That case is not theoretical. A tail reserves space from the filesystem ahead of
its write head, and that space reads back as zeros, which parse as an empty data
record. A walk that did not recognise the shape would step through the whole
reservation one header at a time to the end of the file.

## Alignment, and who pays for it

The block boundary is 4096. A pad record's header carries the fill length so a
scan hops the gap in one step.

Only a volume whose writes go straight to the device covers whole blocks, which
today is the direct ring. There a record reserves an aligned span and closes with
a pad, so the next reservation starts on a boundary. Every other backend has the
kernel assemble the block, so the fill would be bytes copied into a page and never
read back: the pad header alone is written, the space it names is reserved and
reads back as zeros either way.

## The batch frame record

A batch of more than one record opens with one. It carries no key and no sequence
number, since nothing resolves it and what orders the batch is the numbers its own
records carry, and its payload is twelve bytes: a four byte record count then an
eight byte span, both little endian.

```
| header, 21 bytes | count, 4 | span, 8 | record | record | ... |
                                        \_________________________/
                                           count records, span bytes
```

The span is measured from the end of the frame, so a walk that trusts the frame
knows where the batch ends before it has read any of it. Thirty-three bytes a
batch, and a batch of one record is written without a frame at all.

Both numbers are checksummed with the header, so neither can be edited after the
fact, and both are checked: a run of the right byte count in the wrong number of
records is refused, as is the reverse. The frame's own declaration is refused
before it is walked if it counts fewer than two records or claims a span too small
to hold the records it counts, since no writer produces either.

The frame and its records go down in one vectored write inside one reservation,
which is also what keeps them in one segment: a reservation that runs past the end
of the segment is given up whole and retaken on the next one.

## The segment header record

Every segment opens with one, and its payload layout is frozen: a two byte format
version and a four byte segment number. It takes no sequence number and no key,
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

One row is the key, then the sequence number, the offset, the payload length,
the record's own flags, which is 17 bytes past the key, and up to four inlined
value bytes where the column declares an inline width. A partition whose keys
are all one width strides at it. One whose keys vary is prefix compressed
instead, `format/prefix.rs`'s restart-block encoding, because its keys are
names and sorted names share their fronts; the parse rebuilds whole rows, so
the encoding lives only on disk. The directory names each
partition by column, key width, inline width and row count, which is what lets
the rows stride at the natural width instead of the widest one and lets a
column declaring no inlining pay nothing for it. The fixed tail is 64 bytes and
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

**Pads and segment headers are not listed.** A reader never resolves those by key,
so a footer indexes only what a key can reach.

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
range, or whose checksum fails is not a footer, and the segment falls back to the
record walk, which is also what a segment sealed only part way through gets.

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
at no new io; `rewrite_on_seal` extends that to segments that were merely sealed,
off by default.

What that unlocks and nothing yet spends: once offset order equals key order
inside a segment, the footer can hold one entry per **page** of records instead
of one per record, with a short linear scan inside the page. That scan is free,
because the page was being fetched anyway.

At 1 KiB records that is 20 bytes per four records instead of 33 per record,
about 6.6x smaller. At 100 byte metadata records it is closer to 100x. It also
makes front coding work, makes any fence array tiny, and puts a slot's or a
group's records physically next to each other so `get_many` merges them into one
read.

The shape is two footer kinds behind the directory, dense for tails and sparse
for sorted segments, picked by a flag the seal already knows.

Why this ranks above bit-packing the tail: footer size is not a large-record
problem, it is a small-record problem. A row is the key plus 17 fixed bytes plus
any inline bytes, at the column's own key width, which lands very differently per
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
- **The record header layout.** Fixed size and read by every walk.
- **The footer row shape.** Which is why the value inlining shared the deadline
  the checksum had, and it met it: values at or below a 4 byte ceiling ride in
  the row and the entry, landed while the format was open.

The filter region extends the format rather than breaking it, and so would a
whole-segment hash written into it at seal time.
