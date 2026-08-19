# Design notes

The record of why the engine is shaped the way it is: what was measured, what
was refused, and what is still owed. The crate README says how to use it; these
say why it works like that.

## Start here

- [overview.md](overview.md): what a reel is, the write and read paths, a
  volume on disk, the house glossary, and a reading order over the rest
- [why-not-an-lsm.md](why-not-an-lsm.md): where this design sits in the
  log-structured merge lineage, and where it deliberately departs

## Mechanism

- [format.md](format.md): the on-disk record, the flags, the footer, and what would break them
- [io.md](io.md): backends, direct io, the page cache, mapped reads, and what io_uring is and is not worth
- [index-shape.md](index-shape.md): what the resident index is held in, node widths, batching, the sweep, and the keys that defeat the lead
- [index-tier.md](index-tier.md): paging the index into the footers, the filter field, and what a paged open costs
- [checksum.md](checksum.md): why CRC32C, what 32 bits gave up, and why the choice is now frozen
- [compression.md](compression.md): the per-column codec, the codec byte, and what never decompresses
- [multiwriter.md](multiwriter.md): volume ownership, foreign reads, and what more than one writing process would take

## Operation

- [compaction.md](compaction.md): the maintenance plane: what a pass may retire, selection, pacing, and reclaiming without copying
- [durability.md](durability.md): what a crash costs and what recovery promises
- [checkpoint.md](checkpoint.md): a durable copy of a volume taken at a cue
- [cue-points.md](cue-points.md): reading a volume as it stood, and what that costs
- [volumes.md](volumes.md): one reel across several devices
- [servo.md](servo.md): setting the io path at open and steering it while it runs

## Tools

- [../../reel-cli/README.md](../../reel-cli/README.md): the `reel` binary and the report layer behind it

## Ground

- [testing.md](testing.md): the test machinery, and what it still cannot reach
- [unsafe.md](unsafe.md): every unsafe site, what it does, and what makes it sound
