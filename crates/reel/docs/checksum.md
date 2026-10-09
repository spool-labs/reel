# Record checksum

| covered | how |
|---|---|
| keyed record | CRC32C over the header with the checksum field zeroed, then the key, then the payload |
| keyless record | its SipHash check folds in the payload's CRC32C |
| footer | CRC32C over the footer with its own field zeroed |
| journal group | CRC32C over the group's head and rows |

A stored value only reproduces under the same algorithm, so the choice is part of the on-disk
format, and changing it makes every segment already written unreadable. It was CRC64/NVME until the
last format break. The switch happened inside that break, the only time it could be made without
paying for a second one, and it won't be revisited.

## What the checksum is for

It detects local device corruption, so the record can be dropped from the index and refetched from
peers. On a repair-backed volume every record is committed to a Merkle root above this layer, and
the caller verifies leaves there. That layer decides whether a record is correct, so a checksum miss
costs a bad read, which the layer above catches.

Most of what a device does is caught before the checksum.

| fault | what catches it |
|---|---|
| torn write | the unwritten reservation reads as zeros, which a read answers as stale. A partial record fails to parse or fails its check over what did land |
| misdirected or stale sector write, keyed record | the header echo: a read compares the on-disk flags, column, key, sequence number and length against what the index resolved, before any checksum runs |
| misdirected write, keyless record | the check binds column, key, kind and length under the segment's secret, so another key's record fails it |
| rot inside a record that sits where it should | the checksum |

The sequence number completes the keyed echo. An older version of the same key resurfacing at a
pointer the index hasn't moved matches every other field and the checksum, since that record is
intact and where it was written. Nothing else in the engine could catch it, and the compare costs
one integer and no format change. A keyless record holds no version, so a key's old place still
reads its own older record (`a_superseded_place_reads_its_own_record`).

## Measured: the two machines disagree

The `cpu_terms` probe, one core, no io, the same binary on both. The 1 MiB buffer fits in cache on
both machines and is the per-record case. The 256 MiB one fits in neither last-level cache and is
the streaming case. Figures are MB/s. Both machines have hardware acceleration and `crc-fast` uses
it: the ccx33 reports `pclmulqdq` and `sha_ni`.

| machine | buffer | crc64 | crc32c | xxh3 | memcpy |
|---|---|---|---|---|---|
| Apple M4 Max, native | 1 MiB | 73,444 | 96,852 | 42,621 | 82,027 |
| Apple M4 Max, native | 256 MiB | 68,680 | 93,144 | 41,385 | 61,357 |
| Hetzner ccx33, EPYC-Milan | 1 MiB | 14,147 | 25,727 | 33,199 | 49,287 |
| Hetzner ccx33, EPYC-Milan | 256 MiB | 14,288 | 23,781 | 29,024 | 17,055 |

The machines rank the three in opposite orders. On the M4 the CRCs are hardware-fast and xxh3 is
slowest. On EPYC-Milan crc64 is slowest by a factor of two and xxh3 is fastest. A choice made on
either machine alone would have been wrong on the other, and crc32c is the one that wins on both.

Per record on the ccx33, which is what a small write pays:

| record | crc64 | crc32c | xxh3 |
|---|---|---|---|
| 100 B | 17 ns | 6 ns | 5 ns |
| 4 KiB | 287 ns | 196 ns | 125 ns |
| 64 KiB | 4,483 ns | 2,570 ns | 1,965 ns |
| 1 MiB | 71,859 ns | 40,525 ns | 32,831 ns |

## Why it mattered on x86

With the one copy a write already pays, on one core and memory resident, crc64 plus copy ran at
32,406 MB/s on the M4 and 7,775 MB/s on the ccx33. A PCIe Gen5 NVMe streams about 14 GB/s, so one
M4 core had more than twice a drive in hand, and one EPYC-Milan core a bit over half of one.

On the ccx33 a 1 MiB write at eight writers cost 127 us per op, and the crc64 of a 1 MiB record
there is 71.9 us. The checksum was 57 percent of the per-op cost, the largest single CPU term the
engine spent per byte. crc32c takes roughly a fifth off that. The switch also cut the record header
from 24 bytes to 20, four bytes off every record. It is 21 today, since the key width grew to two
bytes.

The 57 percent also answers the bandwidth argument, that a checksum running tens of gigabytes a
second can't matter against a disk running hundreds of megabytes. Device bandwidth is an aggregate
across the device. The checksum is a serial term inside one operation on one core, paid before the
record can be submitted, so spare aggregate bandwidth doesn't take it off the critical path. A
prediction of under one percent missed the measured 57 by two orders of magnitude, and any form of
"X runs n times faster than the disk" fails the same way.

## What 32 bits gave up

A CRC of width n detects every burst error up to n bits, so the guaranteed burst width fell from 64
to 32, and the residual miss probability past the guarantees rose from 2^-64 to 2^-32. Castagnoli
keeps Hamming distance 4 out to about 256 MiB of message, above the largest caller's 64 MiB record
ceiling, so every 1, 2 and 3 bit error in any record this store can write is still caught with
certainty. The 2^-32 residual is per corruption event, and those events are rare, so a missed
detection needs two small numbers to line up. On a repair-backed volume the Merkle layer then
catches it.

XXH3 was faster still on x86 and was turned down for what it lacks. It has good avalanche and is
excellent at random corruption, but gives no Hamming distance guarantee for the burst errors storage
devices produce, which is the property being bought. CRC32C keeps the guarantee, and ext4, iSCSI
and btrfs use it for this same job.

RocksDB used to be on that list too. It has defaulted to XXH3 since 6.27, and 10.4.2 sets
`ChecksumType checksum = kXXH3` in its table header. `table/format.cc` truncates,
`Lower32of64(XXH3_64bits(...))`, and the enum's own comment says every type it offers has 32 bits of
checking power. So it runs at the same 2^-32 residual as CRC32C with no burst guarantee and no
Hamming floor: it traded the guarantee for speed on its hardware and gained nothing on detection.

One idea of theirs is worth taking, from `format_version=6`: an additive modifier,
`base_context_checksum ^ (Lower32of64(offset) + Upper32of64(offset))`, folded onto the finished
checksum outside the digest, so it subtracts back off and survives a change of hash underneath. It
binds a block to its location. Here it would close only misplacement to a different offset within
the same segment, the one case the header echo misses, and it costs a format break. It stays on
record as the known next step.

## Volumes with no layer above

A volume with no Merkle root above it and no peer to repair from, such as a metadata volume or some
agave columns, has the 2^-32 residual as its last line. With `RepairPath::None` a record that fails
its check is an error on every read.

That is still defensible on the event-rate arithmetic above. A deployment that needs more gets
`verify_reads: true` on that volume, which is cheap at metadata record sizes, plus the scrub, plus,
if wanted, a whole-segment hash written at seal time into the footer's filter region, which extends
the format without breaking it. A wider record checksum, and the third format break it would take,
are off the table.

Two related proposals are settled the same way. A second checksum variant buys a dispatch and a test
matrix for no caller. RocksDB supports five types only because it must read files written since
2011, and the self-describing segment header keeps that door cheap to open if a volume ever wants
it. Per-leaf CRCs in a trailer are a coherent idea at the wrong layer: this crate has no concept of
a leaf and the read path frames whole records, so leaf-granular integrity is a footer question.

On a box below the SIMD tiers, `crc-fast` runs both CRC widths through the same table-walk path, so
a hardware fallback favours neither width. That only made the case for measuring a third machine.

## The third machine

`cpu_terms` ran on a 9975WX on 2026-08-05, the closest machine to the target deployment CPU measured
so far, and crc32c wins every cell.

| record | crc64 | crc32c |
|---|---:|---:|
| 100 B | 23 ns | 4 ns |
| 4 KiB | 56 ns | 49 ns |
| 64 KiB | 780 ns | 764 ns |
| 1 MiB | 12,254 ns | 12,246 ns |

The M4's 4 KiB inversion doesn't reproduce there, and bulk crc32c runs at 71.5 GB/s at 1 MiB. The
probe asserts that the shipped checksum is crc32c, so a future run can't measure one algorithm under
another's heading.
