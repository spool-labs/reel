# Compression, and where it sits

The reel stores payload bytes verbatim except where a column asks otherwise.
For the columns it was built for that is the right call: bulk records are
erasure coded shards or caller content, both high entropy, so a codec would buy
nothing and charge a decode on the hottest read path in the store.

One agave column changes the calculus, and it is one rather than all of them.
`blockstore_db.rs` sets `DBCompressionType::None` on every column family by
default and `should_enable_compression` re-enables it for exactly
`TransactionStatus`, which is protobuf-serialized and the one place with
redundancy worth having. Shreds, the hot path and the bulk of the bytes, are
stored uncompressed in production today. So the codec is one column's
argument, not the blockstore's, and that is how it shipped: a per-column
property, off everywhere it is not asked for, lz4 on transaction status at a
measured 0.398 ratio on a real mainnet corpus. `src/append/codec.rs` is the
implementation.

## The codec byte

The record header's former reserved byte, covered by the checksum. Zero means
the payload is stored raw; a nonzero value names the codec that produced it,
so a reader needs no column context to open a record. A payload stored without
a codec carries zero and reads correctly under the rule, the same way the
footer's empty filter region extends the format without breaking it. The flags
byte has one bit left, and a record kind is a better use of it than a codec,
which is why the byte and not the bit.

## The record on disk

```
| header, codec byte set | key | logical length, 4 bytes | codec bytes |
```

The header's length field keeps its meaning: the stored bytes following the
key, which is what every walk, footer row, and resident pointer already
counts. A compressed payload opens with its logical length so a read can size
the output buffer exactly. The header cannot take that field without growing
by four bytes for every record of every column to serve the few that
compress, so the payload carries it.

The checksum covers the stored bytes, not the logical ones. Scrub and read
verification never decompress.

## The write path

Compression happens at admission, before the record is framed, on the
caller's thread, through the payload pool in both directions. The tails
append opaque bytes exactly as before.

The column declares its codec in the column spec, beside the key width and
the inline ceiling. A declared codec is an attempt, not a promise: a payload
the codec cannot shrink by at least an eighth is stored raw with the byte at
zero, so the read side has one rule per record rather than a column rule with
exceptions. Payloads under `MIN_ATTEMPT`, 256 bytes, skip the attempt
entirely. And a payload that would land at or under the column's inline
ceiling is stored raw whatever the codec says, because the inlined bytes in
an index entry and a footer row are payload bytes, and a codec would change
what they mean. That rule is load-bearing and the module doc says so.

One honest limit. The reel compresses a record at a time, so it can never
find redundancy between records the way a block-compressed store does when
it packs neighbouring small values into one frame. A column of many small,
similar records will compress worse here than under block compression, and
the eventual answer for such a column is a dictionary, which is one of the
reasons the codec field is a byte and not a bit.

## The read path

A point read issues the same single preadv it does today, since the resident
pointer's length is the stored length. A record whose codec byte is set is
decompressed into a buffer sized from the logical length prefix, one extra
allocation and one pass, paid only by columns that asked. A logical length
past `MAX_LOGICAL`, one gibibyte, is refused before any allocation, which is
what stops a lying prefix behind a passing checksum from sizing a buffer.

A checksum that passes followed by a decompress that fails is corruption
wearing a valid crc, which only a bug or a torn write the crc happened to
miss can produce. It is answered like a failed checksum: the read reports
corrupt, the entry is evicted, and the miss becomes a repair enqueue.

## What never decompresses

Compaction relocates the stored bytes as they are: the copy re-frames the
header and the payload is a copy, so a compacting volume pays no codec cost
whatever its columns declare. Scrub checksums stored bytes. Recovery and the
tailer walk headers and treat payloads as opaque. Footer rows list stored
lengths. None of them changed, which is most of the argument for putting the
logical length in the payload rather than anywhere the format's walkers would
have to learn about.

## What stays physical

Segment fill, appender accounting, compaction selection, and the byte
counters all count stored bytes, because disk is what they manage. Logical
sizes are the caller's business, the way a size sidecar column already
carries a length the record itself also knows.

## The codec choice

Lz4 first: the fastest decode of anything worth shipping, no window or level
to configure, and a cost model close enough to a memcpy that a column
choosing it is not choosing a new performance regime. Zstd is what the byte
leaves room for, for cold columns where ratio beats latency and for
dictionary support if the small-record limit above ever matters.

## Measuring it

Compression is evaluated on real corpora or not at all. A constant fill
overstates it by two orders of magnitude, which is the lesson a fixed-byte
benchmark taught, and a random fill denies it exists. The number that decided
this one: 0.398 stored-byte ratio on a real mainnet transaction status
corpus, with the decode cost on the point-read path measured beside it.
