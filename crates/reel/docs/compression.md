# Compression

The reel stores payload bytes verbatim unless a column asks for a codec. For the columns it was
built for that is right: bulk records are erasure-coded shards or caller content, both high
entropy, so a codec would buy nothing and add a decode to the hottest read path in the store.

One agave column changes that, and only one. Agave's blockstore sets `DBCompressionType::None` on
every column family by default, and `should_enable_compression` turns compression back on for
`TransactionStatus` alone, which is protobuf-serialized and the one place with redundancy worth
having. Shreds, the hot path and the bulk of the bytes, are stored uncompressed in production
today. So the codec is a per-column property, off unless asked for, with lz4 on transaction status
at a measured 0.398 ratio on a real mainnet corpus.

| | rule |
|---|---|
| codecs | `Codec::None` (0) and `Codec::Lz4` (1), declared in the column spec beside the key width |
| attempted on | payloads of `MIN_ATTEMPT`, 256 bytes, or more |
| kept when | it shrinks the payload by at least an eighth. Otherwise the payload is stored raw with codec 0 |
| stored as | a four-byte logical length, then the lz4 block |
| largest logical length | `MAX_LOGICAL`, one gibibyte. A larger prefix is refused as corruption before any allocation |
| checksum covers | the stored bytes |
| decompressed by | reads only |

## The codec byte

On a keyed record the codec is the header's former reserved byte, covered by the checksum. A keyless
record keeps it in the low two bits of its shape. Zero means the payload is stored raw, and any
other value identifies the codec that produced it, so a reader needs no column context to open a
record. A payload stored without a codec has zero there and reads correctly under the rule, the
same way the footer's empty filter region extends the format without breaking it. The flags byte
keeps its spare bits for record kinds, so the codec got a byte of its own.

## The record on disk

```
| header, codec byte set | key | logical length, 4 bytes | codec bytes |
```

The header's length field keeps its meaning: the stored bytes after the key, which every walk,
footer row and resident pointer already counts. The logical length lets a read size its output
buffer exactly. Putting it in the header would grow every record of every column by four bytes to
serve the few that compress, so the payload holds it.

## The write path

Compression happens at admission, before the record is framed, on the caller's thread. A kept
compression hands the original buffer back to the payload pool, and the tails append opaque bytes
as before.

A declared codec is an attempt. A payload it can't shrink enough is stored raw with the byte at
zero, so the read side has one rule per record and no column rule with exceptions.

The reel compresses one record at a time, so it never finds redundancy between records the way a
block-compressed store does when it packs neighbouring small values into one frame. A column of
many small, similar records compresses worse here than under block compression. The answer for such
a column would be a dictionary, which is one reason the codec field is a whole byte.

## The read path

A point read issues the same single preadv, since the resident pointer's length is the stored
length. A record whose codec is set is decompressed into a pooled buffer sized from the logical
length prefix: one extra allocation and one pass, paid only by columns that asked. The
`MAX_LOGICAL` check stops a lying prefix behind a passing checksum from sizing a buffer.

A checksum that passes followed by a decompress that fails is corruption with a valid crc, which
only a bug or a torn write the crc happened to miss can produce. It is answered like a failed
checksum: the read reports corrupt. With peers to repair from, the entry is evicted and the read
answers a miss for the caller to repair. With `RepairPath::None` the read fails with a corruption
error.

## What never decompresses

Compaction relocates the stored bytes as they are: the copy re-frames the header and copies the
payload, so a compacting volume pays no codec cost whatever its columns declare. Scrub and read
verification checksum the stored bytes. Recovery and the tailer treat payloads as opaque. Footer
rows list stored lengths. None of them had to change, which is most of the argument for putting
the logical length in the payload, where none of the format's walkers has to know about it.

## What stays physical

Segment fill, appender accounting, compaction selection and the byte counters all count stored
bytes, because disk is what they manage. Logical sizes are the caller's business, the way a size
sidecar column already holds a length the record itself also knows.

## The codec choice

Lz4 first: the fastest decode of anything worth shipping, no window or level to configure, and a
cost close enough to a memcpy that a column choosing it stays in the same performance regime. Zstd
is what the byte leaves room for, for cold columns where ratio beats latency, and for dictionary
support if the small-record limit above ever matters.

## Measuring it

Compression is evaluated on real corpora or not at all. A constant fill overstates it by two orders
of magnitude, as a fixed-byte benchmark showed, and a random fill hides it completely. The number
that decided this one is a 0.398 stored-byte ratio on a real mainnet transaction status corpus,
with the decode cost on the point-read path measured beside it.
