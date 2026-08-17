# The record checksum, and the window that has now closed

Every record carries a CRC32C over its own header with the checksum field zeroed,
then its key, then its payload. Footers carry the same algorithm over their body
with their own field zeroed. The stored value is only reproducible under the same
algorithm, so the choice is part of the on-disk format: changing it makes every
segment already written unreadable.

It was CRC64/NVME until the last format break. The switch was made inside that
break, which was the only moment it could be made without paying a second one,
and it is not to be revisited. What follows is why, kept so the decision survives
the people who made it. `format.md` has what else the header carries and what
else would break it.

## What the checksum is actually for here

It detects local device corruption so the record can be dropped from the index and
refetched from peers. It is not the authority on whether a record is correct: on a
repair-backed volume every record is committed to a Merkle root above this layer
and the caller verifies leaves there. A checksum that misses one corruption in
some enormous number of events costs a bad read that the layer above catches, not
a silent acceptance of wrong data.

It is also not the only thing standing between a bad sector and a wrong answer.
The structure catches most of what a device actually does:

- **A torn write** leaves an unwritten reservation, which reads as a zero sequence
  number and ends the walk, or a partial record, which fails to parse or fails the
  checksum over what did land.
- **A misdirected or stale sector write** is caught by the header echo. A read
  compares the on-disk flags, column, key, sequence number and length against
  what the index resolved before the checksum is even computed, and a mismatch
  is a stale pointer rather than a payload. The sequence number is what
  completes the echo: an older version of the same key resurfacing at a pointer
  the index has not moved agrees with every other field and with the checksum,
  since that record is intact and exactly where it was written, and nothing
  else in the engine was positioned to catch it.
  `reads_reject_a_superseded_version` demonstrates the wrong answer with the
  comparison removed. It cost one integer compare and no format change.

What is left for the checksum is rot inside a record that is where it should be.

## Measured, and the two machines disagree

`tests/raw_throughput.rs cpu_terms`, one core, no io, same binary on both. The
small buffer fits in cache on both machines and is the per-record case. The large
one fits in neither last level cache and is the streaming case.

| machine | buffer | crc64 | crc32c | xxh3 | memcpy |
|---|---|---|---|---|---|
| Apple M4 Max, native | 1 MiB | 73,444 | 96,852 | 42,621 | 82,027 |
| Apple M4 Max, native | 256 MiB | 68,680 | 93,144 | 41,385 | 61,357 |
| Hetzner ccx33, EPYC-Milan | 1 MiB | 14,147 | 25,727 | 33,199 | 49,287 |
| Hetzner ccx33, EPYC-Milan | 256 MiB | 14,288 | 23,781 | 29,024 | 17,055 |

All figures MB/s. Both machines have hardware acceleration available and
`crc-fast` uses it: the ccx33 reports `pclmulqdq` and `sha_ni`.

The two machines rank the three algorithms in opposite orders. On the M4 the CRCs
are hardware-fast and xxh3 is the slowest of the three. On EPYC-Milan crc64 is the
slowest by a factor of two and xxh3 is the fastest. A decision taken on either
machine alone would have been wrong about the other, and crc32c is the one choice
that wins on both.

Per record, which is what a small write pays, on the ccx33:

| record | crc64 | crc32c | xxh3 |
|---|---|---|---|
| 100 B | 17 ns | 6 ns | 5 ns |
| 4 KiB | 287 ns | 196 ns | 125 ns |
| 64 KiB | 4,483 ns | 2,570 ns | 1,965 ns |
| 1 MiB | 71,859 ns | 40,525 ns | 32,831 ns |

## Why it mattered on x86 and not on ARM

Combining the checksum with the one copy a write already pays, on one core and
memory resident, crc64 plus copy ran at 32,406 MB/s on the M4 and 7,775 MB/s on
the ccx33. A PCIe Gen5 NVMe streams about 14 GB/s, so on the M4 a single core had
more than twice a drive in hand, and on EPYC-Milan it had a bit over half of one.

It showed up in the engine's own numbers. On the ccx33 a 1 MiB write at eight
writers cost 127 us per op, and the crc64 of a 1 MiB record on that box is 71.9
us: the checksum was 57 percent of the per-op cost at that point, the largest
single CPU term the engine spent per byte. crc32c takes roughly a fifth off that.

The header shrank as well, from 24 bytes to 20, which is four bytes off every
record on every volume.

That 57 percent is also the standing answer to the bandwidth argument, which
keeps being offered: that a checksum running tens of gigabytes a second cannot
matter against a disk running hundreds of megabytes. Device bandwidth is an
aggregate across the whole device, and the checksum is a serial term inside one
operation on one core, paid before the record can be submitted, so headroom in
aggregate bandwidth does not remove it from the critical path. A prediction of
under one percent missed the measured 57 by two orders of magnitude. Any form
of "X runs n times faster than the disk" fails the same way.

## What 32 bits gave up, exactly

A CRC of width n detects every burst error up to n bits, so the guaranteed burst
width fell from 64 to 32. The residual miss probability for corruption beyond the
guarantees rose from 2^-64 to 2^-32. Castagnoli keeps Hamming distance 4 out to
about 256 MiB of message, which is above the 64 MiB record ceiling of the largest
caller, so every 1, 2 and 3 bit error in any record this store can be asked to
write is still caught with certainty.

A 2^-32 residual is per corruption event, and corruption events are themselves
rare, so the expected rate of a missed detection is the product of two small
numbers. On a repair-backed volume the miss is then caught above by the Merkle
layer.

XXH3 was faster still on x86 and was rejected rather than lost on speed. It is not
a CRC: good avalanche, excellent at random corruption, and no Hamming distance
guarantee for the burst errors storage devices actually produce, which is the
property being bought. CRC32C keeps the guarantee and is what ext4, iSCSI and
btrfs use for exactly this job.

A widely deployed LSM engine used to be third on that list and no longer is. It
has defaulted to XXH3 since its 6.27 release, and its current 10.4.2 sets
`ChecksumType checksum = kXXH3` in its table header. That is worth knowing
rather than quietly dropping, because reading what it actually computes
strengthens the case here rather than weakening it. `table/format.cc` truncates:
`Lower32of64(XXH3_64bits(...))`, and the enum's own comment says every type it
offers carries 32 bits of checking power. So it runs at the same 2^-32 residual
as CRC32C with no burst guarantee and no Hamming floor underneath it, having
traded the guarantee for speed on its hardware and gained nothing back on
detection.

The one idea of theirs worth taking is from `format_version=6`: an additive
modifier, `base_context_checksum ^ (Lower32of64(offset) + Upper32of64(offset))`,
folded onto the finished checksum rather than fed into the digest, so it
subtracts back off and survives a change of hash underneath. It binds a block
to its location. For this engine it would close only misplacement to a
different offset within the same segment, the one case the header echo does
not cover, and it costs a format break, so it is recorded as the known next
step rather than taken.

## The one deployment the layering argument does not cover

A volume with no Merkle above it and no peer to repair from, which is what a
metadata volume is, and what some agave columns are. There the 2^-32 residual is
the last line rather than the first.

That is still a defensible place to stand on the event-rate arithmetic above, and
if a deployment class ever demands more, the answer is not a wider record checksum
and a third format break. It is `verify_reads: true` on that volume, which is
cheap at metadata record sizes, plus the scrub, plus, if ever wanted, a
whole-segment hash at seal time in the footer's reserved filter field, which
extends the format rather than breaking it.

Two related proposals are settled the same way. A second checksum variant buys
a dispatch and a test matrix against no caller, and the engine above carries
five types only because it must read files written since 2011. The record header's
reserved byte at offset 19 and the self-describing segment header keep that
door cheap to open if a volume ever wants it. Per-leaf CRCs in a trailer are a
coherent idea at the wrong layer: this crate has no concept of a leaf and the
read path frames whole records, so leaf-granular integrity is a footer
question. And on a box below the SIMD tiers, `crc-fast` lands both CRC widths
in the same table-walk path, so hardware fallback prefers neither width and
only argues for the third run below.

## The third machine, taken

`cpu_terms` ran on a 9975WX on 2026-08-05, the closest machine to the target
deployment CPU measured so far, and crc32c wins every cell: 4 ns against crc64's
23 at 100 bytes, 49 against 56 at 4 KiB, 764 against 780 at 64 KiB, 12,246
against 12,254 at 1 MiB. The M4's 4 KiB inversion does not reproduce there, and
bulk crc32c runs at 71.5 GB/s at 1 MiB. The verdict this document reached on two
machines holds on the third.

The probe asserts the shipped entry point matches its column headings, after
a relabeling defect had it measuring crc32c under a crc64 heading, so a
future run cannot be quietly pointed at the wrong algorithm.
