# What the index map is held in

The index started as a `BTreeMap<K, Entry>` per shard. Both key types now use a purpose-built B+
tree. The tree beats the map on every axis that reaches the store, and the largest win comes from
asking it for many keys at once.

```
cargo test -p tape-reel --test index_shape --release -- --ignored --nocapture
```

Every number is one serial run on an idle machine, and arms compare only to each other. A row
with no box listed was taken on aarch64, Apple silicon, 2026-08.

## Shards set the regime

`shard_count()` is `1 << (8 * shard_bytes)`, so a column sharded two bytes wide is 65,536 maps
under 65,536 locks and a one-byte column is 256. Columns with no sharding are small by
construction: an epoch, a group or an operator string.

**A replacement has to beat a small map.** A shard holds the volume's keys divided by the shards
a column uses, which can be far fewer than it declares. On the bulk record column the two shard
bytes are the group, and the caller's allocation rule holds one store to a twentieth of the
thousand groups. So a tebibyte of 64 KiB records puts about 335,000 keys in each of fifty
occupied shards, the 262,144 row of the tables below.

**The lock is already spread thinner than a concurrent map would spread it**, so the lock-free
arms lose. At 16,384 keys a shard:

| map | insert | get |
|---|---|---|
| `skipmap` | 204 ns | 167 ns |
| `treeindex` | 194 ns | 99 ns |
| `BTreeMap` | 83 ns | 60 ns |

Both gaps widen with occupancy. `indexset` wins only its walk, 0.3 ns a key throughout.

**Ordering is required.** A listing seeks by prefix, rolls a folder up past a delimiter and
resumes from a name, so a hash cannot hold that column at any speed.

## The tree

Leaves and inner nodes live in two arenas, a child is a tagged `u32`, values sit in the leaves,
leaves are chained both ways, and node width is a const parameter. Each node keeps an eight byte
lead of every key in an array beside the keys, so a search reads 8 bytes a key against 34.

| size | btreemap get | tree get | btreemap scan | tree scan |
|---|---|---|---|---|
| 1024 | 32.9ns | 27.6ns | 1.9ns | 0.7ns |
| 16384 | 55.0ns | 43.3ns | 2.2ns | 0.6ns |
| 262144 | 105.5ns | 67.1ns | 11.6ns | 0.8ns |
| 4194304 | 306.8ns | 173.6ns | 15.1ns | 2.5ns |

**The node search matters more than the node width.** Bisecting the lead array lost to `BTreeMap`
by 8 to 18 percent. Counting the leads below the wanted one, with no branch to mispredict, won at
every size.

## What widens the count

`count_below` is that search. Whether a hand-written version pays depends on the instruction set:

| target | result | box |
|---|---|---|
| aarch64 | The optimiser widens the scalar filter-count to 8 u64 an iteration across four accumulators, and it beat a hand NEON arm by 1.19x at a node width of 64. aarch64 ships the scalar loop. | Apple silicon, 2026-08-17 |
| baseline x86-64 | SSE2 has no unsigned 64 bit compare and nothing widens the scalar loop, so the hand arms are the whole win. AVX-512 runs 1.25x to 1.59x over scalar and AVX2 1.06x to 1.24x across the widths, AVX2 biasing into signed space first. | GCP c3d, EPYC 9B14, 2026-08-17 |
| `-C target-cpu=x86-64-v3` | The scalar loop widens on x86 too and beats the dispatched arm net of the dispatch. | same |

On x86 the arm is picked at runtime from what the processor reports. `REEL_SCAN` forces a
narrower one, so one box runs all three through the correctness gates. `scan_backend()` reports
the choice, since a box that fell back quietly looks the same as one that did not.

## Node width follows the key

A node holds `B` whole keys, so an insert shifts `B` times the key's bytes and a tied run compares
that many whole keys along the leaf. The cost grows with that product, so a width that suits one
key size does not suit another.

`node_width_by_key_size` sweeps widths against the declared key sizes in two shapes: `scattered`,
where keys differ inside their lead, and `tied/asc`, where a run shares its whole lead and arrives
in slot order, as both agave status columns do. Inserting 16,384 keys a shard, scattered:

| key bytes | 16 | 32 | 34 | 72 | 108 |
|---|---|---|---|---|---|
| best width, keys | 32 | 32 | 32 | 16 | 16 |
| best width, bytes | 512 | 1,024 | 1,088 | 1,152 | 1,728 |
| ns a key | 69.3 | 72.2 | 74.4 | 81.9 | 119.0 |

Get moves the same way and by less.

**On aarch64 the scattered optima are a byte count.** In bytes a node they form one band.
`NODE_BUDGET` is a kibibyte, inside that band at every size, and `node_width` is the budget over
the declared width, clamped between `MIN_NODE_WIDTH` (16) and `MAX_NODE_WIDTH` (64). Under the
floor an extra level costs more than the bytes save, and over the ceiling the returns are flat.
`past_cache` holds the band at the occupancies a bulk record shard reaches: 110.2 ns against 120.8
at a width of 64 at 262,144 keys, and 258.8 against 305.1 at four million.

**On Zen 3 they are a key count, and the widths hold anyway.** That box put the scattered insert
optimum at 32 keys at every key size and the get optimum at 64, node bytes running 512 to 6,912.
So the band comes from the aarch64 build, and the tree does not produce it. The clamp and the tie
keep the widths right there, and the gate agreed: the two agave status rows came back 11.8 percent
and 21 to 23 percent faster over two passes, and the narrow columns stayed flat.

**A tied column wants a narrow node at every key size**, since its walk is a count of whole-key
comparisons and the width sets that count. A wide node costs 2.6x to 3.5x on insert and 1.7x to
2.0x on get whatever the key weighs. The budget takes the two agave status columns to 16 for their
bytes, the width their ties want, and leaves the 16 byte shred columns at the ceiling. That is
measured: `insert_shreds` does not move between 16 and 64, because the index is a small share of a
shred insert. The one exposure runs the other way. A 16 byte key that ties hard pays 1.53x on
insert at 64 against 32 on Zen, worth checking when a narrow tying column arrives.

What each declared width takes, pinned in `declared_widths_take_the_budget`:

| key bytes | 0, 2, 8, 12, 16 | 20 | 24 | 32 | 34 | 36 | 40 | 44 | 48 | 72, 96, 108 |
|---|---|---|---|---|---|---|---|---|---|---|
| node width | 64 | 51 | 42 | 32 | 30 | 28 | 25 | 23 | 21 | 16 |

## Batching is the largest result

A descent is a chain of dependent cache misses, one a level, and no search removes them because
the next node's address is unknown until the current one lands. Other work in flight hides them,
`LANES` of 16 keys at a time.

| size | one at a time, `BTreeMap` | batch 64 | batch 64, sorted |
|---|---|---|---|
| 1024 | 32.9ns | 15.4ns | 10.2ns |
| 16384 | 55.0ns | 18.9ns | 10.8ns |
| 1048576 | 187.8ns | 60.1ns | 27.3ns |
| 4194304 | 306.8ns | 84.5ns | 52.3ns |

**5.9x under `BTreeMap` at four million keys**, and 3.2x at a thousand. A sorted run wins twice,
since neighbouring keys share their upper nodes, so those levels are visited once a run and stay
hot across the batch. `get_many_sorted` checks its input and falls back to the unordered batch
when a run is unsorted, so a caller cannot get a wrong answer from the wrong input. Prefetch is
worth 4 to 11 percent past 262,144 keys.

**The gain survives the store's door.** `ReelIndex::get_many` groups by column and `entry_many` by
shard, so a run is one lock take and one batched descent, and on a bulk record column a shard is
one group. Through that door on a 9950X, fifty groups held, every round asking new keys:

| keys a group | batch | keys drawn from | one at a time | batched | gain |
|---|---|---|---|---|---|
| 65,536 | 8 | one group | 105.1ns | 74.8ns | 1.41x |
| 65,536 | 256 | every group | 385.9ns | 205.4ns | 1.88x |
| 1,048,576 | 8 | one group | 353.5ns | 159.1ns | 2.22x |
| 1,048,576 | 256 | every group | 760.7ns | 310.1ns | 2.45x |

**The run reaches the tree unsorted.** The shared descent saves about 30 ns a key at a million,
and sorting 34 byte keys costs four times that, twice that again through the caller's borrowed
slices. Copying the keys into one contiguous buffer first still lost. **A run under `BATCH_RUN`,
four keys a shard, is looked up a key at a time** with the shard held once, since the batch setup
costs more than the overlap buys. A read across blobs makes that shape, one record to a group.

## Install and deletion

`from_sorted` packs leaves bottom up with no splits: **6.1 to 8.5 ns a key, flat at every size**,
while insert-driven loading climbs from 19.5 to 44.6 ns. Recovery absorbs sorted runs, so an
install already has this shape.

Deletion has no borrow and no merge, by choice. A leftover separator still routes and an emptied
leaf keeps its place in the chain. Deleting three keys in four at a million:

| | leaves | get | walk a key |
|---|---|---|---|
| before | 32,768 | 121.3 ns | 1.1 ns |
| after, a quarter full | 32,768 | 80.7 ns | 2.1 ns |
| rebuilt from the survivors | 8,192 | 41.4 ns | 0.5 ns |

A rebuild costs 9.4 ns a key, which is fine because the index is rebuilt at every install and
every compaction anyway. A volume that deletes heavily and never compacts needs its own trigger.
`fill_factor()` is the gauge and `repack_owed` the trigger, which fires when a tree holds twice
the leaves its live keys need. It is a doubling guard and no fill threshold, since a shard
holding three keys reads as badly under-filled and packing it moves three keys into one leaf.
Grave pruning, `sweep_run`, `evict_at` and `page_out_lane` check it with the shard held, right
after they drop keys.

## What shipped

- **Every column is on a tree.** `ShardMap` is generic in its value as well as its key, and
  `Shape` picks the map a column's shards use. Sixteen fixed arms of `ColumnIndex` take `Trees<N>`
  and the variable arm takes `VarTrees`, each at the node width its key asks for.
- **The walk runs both ways.** Leaves are chained in both directions, so the chain serves
  `page_back`. Only the chain can hold the paged playback's downward cursor.
- **`OFF` and the tie counter are gone.** `tie_rate` walks what is held. Two atomics sharing one
  cache line across 65,536 shards would price a single-threaded bench honestly and a running store
  dishonestly.
- **The small books moved too.** `TreeKey` covers `u64`, `u32`, `SegmentId` and `Lsn`, so the
  occupied-shard set, the cue points, the tailer's positions, the sealed ranges and the segment
  holds are trees as well.

**One tree for both key kinds cost the fixed columns nothing.** At each column's production
width, against the branch before it, scattered keys run 1.01 on insert and 1.02 on get, tied keys
0.86 to 0.91, and the walk is flat. Two overrides hold that up:

- `TreeKey::open`, `close` and `hand_over` keep the plain slice copies for `[u8; N]`.
  `rotate_right` on a wide element is a cycle of swaps, and it cost 1.10 on scattered inserts.
- A tied inner walk steps a cursor from the front. Placing it by binary search adds four
  unpredictable branches over a stride as wide as the key, and costs 1.20x to 1.27x of a tied get
  at the widths a 72 or 108 byte key ships at.

## The scan prefix that defeats the lead

The tree searches a node on eight byte leads and reads whole keys only across a tie. Measured
2026-08-11 through a real volume, one key set per column family written the way that family's
prefix helper reads it: **seven of the eighteen fixed columns tied at 1.0000**, exactly the seven
whose scan shares eight bytes or more. The other eleven tied at 0.0000, and so did the five
variable columns, whose window retunes past what their keys share.

Each of the seven writes its scan's prefix first, an owner address or an epoch or a timestamp,
because that makes the scan a range. So the prefix that makes the scan possible defeats the lead.
`seek` counted the leads below the wanted one, found the whole node equal, and walked it comparing
full keys, so a tied column paid the node width in whole-key comparisons. Those seven declare keys
from 24 to 96 bytes, so the budget puts them between 42 and 16 slots a node, down from the 64 they
all held before.

**A fixed key takes the same window as a variable one**, `Shared<N>` at the key's own width, since a
node's entries cannot agree on more bytes than a key has. A per-column lead offset does not work.
It keeps order only where every key in the shard shares the bytes before it, and an epoch-keyed
column is a single shard holding every epoch. The window's invariant belongs to one node, and a
node inside that shard holds one epoch.

A million keys a shape against the same tree with its window off, both arms in one process, the
mean of two runs, ns a get and ns an entry walked:

| shape, 1M keys | flat get | window get | flat miss | window miss | flat walk | window walk | skip | tie |
|---|---|---|---|---|---|---|---|---|
| snapshot epoch/group/piece, 24B | 235.1 | **167.4** | 140.1 | **78.2** | 2.95 | **2.74** | 22.8 | 0.011 |
| tuple group/tape/track, 34B | 239.2 | **151.9** | 239.2 | **138.0** | 4.24 | **3.00** | 17.4 | 0.001 |
| wal generation/offset, 16B | 252.2 | **133.7** | 229.3 | **78.9** | 2.82 | **1.92** | 13.0 | 0.000 |
| spool then address, 34B | 109.1 | **101.7** | 121.2 | **113.7** | 2.74 | 2.85 | 0.0 | 0.000 |
| address, 32B | 103.1 | 105.9 | 123.3 | **117.2** | 2.36 | 2.76 | 0.0 | 0.000 |

**A shape whose lead already discriminated keeps its reads.** That was the gate, and three things
hold it:

- A window under eight bytes is dropped. The lead already reads past a run that short, so moving
  the window there costs every probe a placement for nothing. A column whose keys share nothing
  keeps `off` at zero and reads its lead from the front, like `Whole`.
- Placement and lead come from one call, `LeadWindow::word`. A node that asked for them separately
  read the same probe twice and cost 11 percent on the address shapes.
- A node is `repr(C)` with the window behind the length. Otherwise the compiler lays it past the
  values, a kilobyte from the length, and a search waits on a second cache line to learn where its
  lead starts.

The cost is 2 to 5 bytes a key: the window inline in every node, and every separator held whole,
which lets a node that retunes rebuild its leads at its new offset.

**A shape with nothing to skip pays those bytes on the walk.** Its gets land within 7 percent
either way across runs and its misses come back 5 and 6 percent quicker, but its nodes are wider to
step through. The 32 byte address shape walks 0.4 ns an entry slower, and one spool's shard of the
34 byte column, small enough to stay in cache, gets 5 percent slower. **Writes follow the gets.** A million inserts run 13, 13 and 55 percent faster on
the three skipping shapes, 5 percent faster on the spool column and 7 percent slower on the
address one, and steady state churn is no slower on any of them.

**The oracle holds correctness.** `a_shared_lead_moves_the_window_and_still_answers` checks that
every key of a fully shared shape answers, that a probe with none of the shared bytes is placed at
the edge, and that the ordered walk stays in order.
`a_refilled_node_places_probes_against_what_it_holds_now` checks what a fixed width finds and a
name shape mostly hid: **a window retunes on which bytes a node agrees on, and their count alone
is not enough.** Otherwise a node emptied and filled again goes on placing probes against keys
that left. `ReelStore::lead_tie_rates` reports tie rates walked off the leaves, so a column the
window cannot reach fails the caller's guard.

## The name columns, and the lead a bucket defeats

The five variable columns take `VarTrees`, the same tree at `Box<[u8]>`. Keys sit on the heap and
the node holds pointers to them, so a shift moves sixteen bytes a slot and the lead array stays
inline. The borrow contract at `ShardMap::walk` is unchanged, since a node still holds a
`Box<[u8]>` to hand back.

**An object key defeats the flat lead.** A listing key is a 32 byte bucket address and then a
name, so every key in a bucket shares its leading eight bytes and the flat lead ties at **1.0000 on
every name shape**. The offset has to come from something smaller than a shard. `object_list`
shards on one leading address byte, so a shard holds every bucket starting with that byte, and
skipping 32 bytes would order two buckets by name.

**A node is small enough.** `Shared` keeps the bytes one node's entries agree on and reads the lead
just past them, which on an object key is the name. The invariant is local to the node, and no two
nodes have to agree. Every entry begins with `pre[..off]`, so a probe that does too is ordered
against them by the eight bytes after it, and a probe that does not is below all of them or above
all of them. That is exact for a search and for an insert's slot alike, so one window serves
`seek`, `range` and the sorted batch. A leaf tunes from its keys and an inner node from its
separators, at a build and at a split. An insert retunes only where the arriving key broke what
the rest agreed on, and every separator is held whole because rebuilding leads at a new offset
reads them again.

`var_node_width` prices the window at 16,384 keys a shard over four object-key corpora, against
the `BTreeMap` this replaces and against the same tree with its window off:

| corpus | btreemap get | flat get | window get | window walk/key | window tie |
|---|---|---|---|---|---|
| opaque, 48B | 128.3 | 185.1 | **56.5** | 0.5 | 0.0000 |
| dated, 61B | 111.2 | 203.5 | **83.1** | 0.6 | 0.0582 |
| tenanted, 110B | 135.8 | 220.0 | **126.3** | 0.5 | 0.4518 |
| full length, 1056B | **702.2** | 804.1 | 1202.5 | 1.0 | 1.0000 |

- **The walk is the win, and listing pays for the walk.** Every name shape walks in half the time
  or better.
- **The window is what pays for the tree.** The flat lead loses every row to `BTreeMap` on a get.
  It pays the pointer chase and gets no discrimination back, which is what a tie rate of one means.

**Width and tie rate are coupled**, which the fixed arm never showed. A wider node holds keys that
agree on less, so its window moves less and its leads tie more. `tenanted` ties at 0.078, 0.202,
0.452, 0.950 and 0.977 as the width goes 8, 16, 32, 64, 128. **`VAR_NODE_WIDTH` is 32**, from that
sweep. `NODE_BUDGET` has nothing to divide when a node holds `B` pointers whatever the keys weigh.
At 32 the walk is at its floor, the get is within three percent of the best arm on every corpus,
and the ties stay under a half.

**The window is capped at what a node holds inline.** A cap under what a corpus shares buys
nothing, since the lead lands back inside the shared run: `tenanted` ties at 0.9844 under a cap of
64 against 0.4518 at 128, with gets of 167 ns and 123. A cap of 256 buys nothing over 128 and costs
eight bytes a key, so `SHARED_CAP` is **128**. Two ceilings remain. `full length` shares a
thousand bytes and keeps its ties at any cap. A leaf that straddles two buckets agrees on nothing
and reads its lead from the front: at 64 keys a bucket most leaves straddle and the rate stays at
0.81, and at 2,048 it falls to 0.49. A split that cuts on a prefix change would keep a leaf inside
one bucket, and it is not built.

## What a listing costs

This is the gate: a caller-side listing benchmark, six alternated runs an arm over one bucket of
16,384 objects at five page sizes plus a folder roll-up per corpus, against a control built where
`object_list` still used `BTreeMap`, the two binaries compared before either ran. **Ten of the eighteen rows are at or under the map they replace, and the other eight
run 0.2 to 3.7 percent over**, inside the control's spread, with the seek-heaviest shape five
percent faster. A listed row is a seek, a walk step and a record played back, and only the first
two touch the map, so index gains that come in factors show up in listings as percent.

## The sweep

A maintenance pass wants complete, resumable coverage of a column. A sweep pages the column in key
order, the map and the footers merged, and marks the last key it handed out. A mark is opaque.
`ColumnMark` holds the nonce of the opening that minted it, since a mark outlives its process
through a persisted cursor or a peer's request. A mark from another opening starts the sweep over,
so the promise is at-least-once, which is what the callers need. `sweep_prefix` narrows a sweep to
one prefix, whose keys are one run.

## Resident bytes per key

The tree loses here. Weighed by the counting allocator over the same corpora, each arm holding one
owned copy of every key, **the tree takes 6 to 25 percent more room than the map it replaces**:

| corpus | tree, bytes a key | `BTreeMap`, bytes a key |
|---|---|---|
| opaque | 158 | 126 |
| tenanted | 222 | 188 |
| full length, a kibibyte a key | 1,199 | 1,134 |

The boxed slot is where it goes: a sixteen byte pointer plus the whole key on the heap, 64 bytes
on the shortest corpus and 1,072 on the longest. Front coding inside the leaf would take the same
key to 6 to 18 bytes. It is unbuilt, because it changes what `walk` and `span` can yield, and that
is a trait change.

## A lossy key must never be the map key

Prefix keys keep getting proposed for the index map: the tree would hold eight bytes against
thirty-four, and a collision would cost one wasted read. The second half is false. **A collision costs a filter a wasted read. It
costs a map a key.** Keyed on the prefix, a key whose prefix matches a resident one overwrites it,
and the old key becomes unreachable with nothing to report it. The rate does not matter, because
people pick the keys: a content address comes from whoever uploads the data, so a colliding pair is
a birthday search over 72 bits once the shard byte is counted, about 2^36 hashes, under two hours
on one core. Manufacture one and the store answers for a record it cannot resolve.

**A lossy key must never be the map key in a structure that cannot hold two entries under one
prefix.** With duplicate support the prefix length is a performance knob. Without it nothing under
16 bytes is defensible, and 16 only because 2^68 is unreachable.

## What is not measured

- **Huge pages.** The index runs to gibibytes and 65,536 separately allocated shards scatter its
  pages. It would be a configuration change.
- **Resident bytes per key on the fixed columns.** `key_footprint` weighs tree, map and hash over
  32 byte keys at both loads, and the numbers are still owed.
- **Contention, and the window under churn.** Every arm is single threaded, so the claim that
  65,536 shards leave nothing for a lock-free map to win rests on shard count alone. Every variable
  arm is one thread building one shard in key order. A shard taking scattered inserts retunes more
  often, which is bounded, since a window only narrows between rebuilds, and unmeasured.
