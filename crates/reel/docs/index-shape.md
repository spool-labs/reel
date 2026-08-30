# What the resident index is held in

The index started as a `BTreeMap<K, Entry>` per shard. It is a purpose-built B+ tree now at
both key types, with an open-addressed arm a column may declare instead at four of the widths.
The tree beats the map on every axis that reaches the store, and the largest win is not in the
structure at all but in asking it for many keys at once.

```
cargo test -p reel --test index_shape --release -- --ignored --nocapture
```

Every number is one serial run on an idle machine, and arms are comparable to each other rather
than to any other box. A row carrying no box was taken on aarch64, Apple silicon, 2026-08.

## Shards decide the regime

The index is not one map. `shard_count()` is `1 << (8 * shard_bytes)`, so a column sharded two
bytes wide is 65,536 maps under 65,536 locks and a one-byte column is 256. The columns
declaring no sharding are small by construction: an epoch, a group or an operator string.

**A replacement has to beat a small map**, since a shard holds what the volume holds over its
shard count, and **the division is by the shards a column uses, not the shards it declares**.
On the bulk record column the two shard bytes are the group and the caller's allocation rule
holds one store to a twentieth of the thousand groups its key space has, so a tebibyte of
64 KiB records puts about 335,000 keys in each of fifty occupied shards: the 262,144 row of the
tables below rather than the 1,024 row.

**The lock is already spread thinner than a concurrent map would spread it**, which is what the
lock-free arms lose to. At 16,384 keys a shard `skipmap` inserts at 204ns and gets at 167ns and
`treeindex` at 194 and 99, against `BTreeMap` at 83 and 60, both gaps widening with occupancy.
`indexset` holds only its walk, 0.3ns a key throughout.

**Ordering is a gate, not a score.** A listing seeks by prefix, rolls a folder up past a
delimiter and resumes from a name, so a hash cannot hold that column whatever it costs. The
open-addressed arm is that trade taken deliberately, by a column answering point reads that
gathers and sorts for the rare walk.

## The tree

`index/tbtreemap.rs`. Leaves and inner nodes in two arenas, a child a tagged `u32` rather than
a pointer, values in the leaves with the leaves chained both ways, node width a const
parameter, and each node carrying an eight byte lead of every key in an array beside the keys
so a search reads 8 bytes a key rather than 34.

| size | btreemap get | tree get | btreemap scan | tree scan |
|---|---|---|---|---|
| 1024 | 32.9ns | 27.6ns | 1.9ns | 0.7ns |
| 16384 | 55.0ns | 43.3ns | 2.2ns | 0.6ns |
| 262144 | 105.5ns | 67.1ns | 11.6ns | 0.8ns |
| 4194304 | 306.8ns | 173.6ns | 15.1ns | 2.5ns |

**The node search decides more than the node width.** Bisecting the lead array lost to
`BTreeMap` by 8 to 18 percent. Counting the leads below the wanted one instead, with no branch
to mispredict, turned that into a win at every size.

## What widens the count

`count_below` is that search, and whether it wants writing by hand is a question about the
instruction set rather than about the tree. **On aarch64 the optimiser widens the scalar loop
and the hand arm is behind it**: the idiomatic filter-count disassembles to 8 u64 an iteration
across four accumulators and beats the hand NEON arm by 1.19x at a node width of 64 (Apple
silicon, 2026-08-17). The NEON arm still ships.

**On baseline x86-64 the hand arms are the whole win**, since SSE2 has no unsigned 64 bit
compare and nothing widens the scalar loop there: AVX-512 runs 1.25x to 1.59x over it and AVX2
1.06x to 1.24x across the widths, AVX2 biasing into signed space first (GCP c3d, EPYC 9B14,
2026-08-17). **Under `-C target-cpu=x86-64-v3` the scalar loop widens on x86 as well** and beats
the dispatched arm net of the dispatch, same box and date.

An arm is chosen at runtime from what the processor reports and `REEL_SCAN` forces a narrower
one, so one box runs all three through the correctness gates, and `scan_backend()` reports the
choice, because a box that fell back quietly is otherwise indistinguishable from one that did
not.

## Node width follows the key

A node holds `B` whole keys, so an insert shifts `B` times the key's bytes and a tied run
compares that many whole keys along the leaf. What a width costs grows with that product rather
than with `B` alone, so a width taken at one key size does not carry to another.
`node_width_by_key_size` sweeps the widths against the declared key sizes in the two shapes
their keys have: `scattered`, where keys differ inside their lead, and `tied/asc`, where a run
shares its whole lead and arrives in slot order, which is what both agave status columns do.
Inserting 16,384 keys a shard, the best width is 32 keys at 16, 32 and 34 byte keys and 16 at 72
and 108, at 69.3, 72.2, 74.4, 81.9 and 119.0ns a key, and get moves the same way and by less.

**On aarch64 the scattered optima are a byte count, not a key count.** In bytes a node the five
optima are 512, 1,024, 1,088, 1,152 and 1,728, one band rather than five answers. `NODE_BUDGET`
is a kibibyte, inside that band at every size, and `node_width` is the budget over the declared
width clamped between `MIN_NODE_WIDTH` of 16 and `MAX_NODE_WIDTH` of 64: under the floor an
extra level costs more than the bytes save, over it the returns are flat. `past_cache` holds the
band at the occupancies a bulk record shard reaches, 110.2ns against 120.8 at a width of 64 at
262,144 keys and 258.8 against 305.1 at four million.

**On Zen 3 they are a key count, and the widths hold anyway.** That box put the scattered insert
optimum at 32 keys at every key size and the get optimum at 64, node bytes running 512 to 6,912
rather than clustering, so the band is NEON's and not a property of the tree. What carries the
widths there is the clamp and the tie, and the gate agreed: the two agave status rows came back
11.8 percent and 21 to 23 percent faster over two passes, the narrow columns flat.

**A tied column wants a narrow node at every key size**, since the walk is a count of whole-key
comparisons and the width sets it: a wide node costs 2.6x to 3.5x on insert and 1.7x to 2.0x on
get whatever the key weighs. The budget takes the two agave status columns to 16 for their
bytes, the same answer their ties want, and leaves the 16 byte shred columns at the ceiling,
which is measured rather than assumed, `insert_shreds` being unmoved between 16 and 64 because
the index is a small share of what a shred insert does. The one exposure runs the other way, a
16 byte key tying hard paying 1.53x on insert at 64 against 32 on Zen, worth checking when a
narrow tying column arrives rather than a reason to move a number now.

What each declared width takes, pinned in `declared_widths_take_the_budget`:

| key bytes | 0, 2, 8, 12, 16 | 20 | 24 | 32 | 34 | 36 | 40 | 44 | 48 | 72, 96, 108 |
|---|---|---|---|---|---|---|---|---|---|---|
| node width | 64 | 51 | 42 | 32 | 30 | 28 | 25 | 23 | 21 | 16 |

## Batching is the largest result

A descent is a chain of dependent cache misses, one a level, and no search removes them because
the next node's address is not known until the current one lands. What removes them is other
work in flight, `LANES` of 16 keys at a time.

| size | one at a time | batch 64 | batch 64, sorted |
|---|---|---|---|
| 1024 | 32.9ns¹ | 15.4ns | 10.2ns |
| 16384 | 55.0ns¹ | 18.9ns | 10.8ns |
| 1048576 | 187.8ns¹ | 60.1ns | 27.3ns |
| 4194304 | 306.8ns¹ | 84.5ns | 52.3ns |

¹ `BTreeMap`, as the thing being replaced.

**5.9x under `BTreeMap` at four million keys**, and 3.2x at a thousand. A sorted run wins twice
over, since neighbouring keys share their upper nodes, so those levels are visited once a run
rather than once a key and stay hot across the batch. `get_many_sorted` checks its input and
falls back to the unordered batch when a run is not sorted, so a caller cannot get a wrong
answer by handing it the wrong thing. Prefetch is worth 4 to 11 percent past 262,144 keys.

**The gain survives the door.** `ReelIndex::get_many` groups by column and `entry_many` by
shard, so a run is one lock take and one batched descent, and on a bulk record column a shard is
one group. Through that door, on a 9950X, fifty groups held, every round asking new keys:

| keys a group | batch | keys drawn from | one at a time | batched | gain |
|---|---|---|---|---|---|
| 65,536 | 8 | one group | 105.1ns | 74.8ns | 1.41x |
| 65,536 | 256 | every group | 385.9ns | 205.4ns | 1.88x |
| 1,048,576 | 8 | one group | 353.5ns | 159.1ns | 2.22x |
| 1,048,576 | 256 | every group | 760.7ns | 310.1ns | 2.45x |

**The run reaches the tree unsorted.** The shared descent is worth about thirty nanoseconds a
key at a million; sorting thirty-four byte keys costs four times that, and twice over through
the caller's borrowed slices at two pointer chases a comparison. Materialising the keys
contiguously first still lost. **A short run is not batched** either: below `BATCH_RUN`, four
keys a shard, it is looked up a key at a time with the shard held once, because a run and a
vector to hand the answers back cost more than the overlap buys. That is the shape a read
crossing blobs makes, a blob's records landing one to a group.

## Install and deletion

`from_sorted` packs leaves bottom up with no splits: **6.1 to 8.5ns a key, flat at every
size**, against insert-driven loading climbing from 19.5 to 44.6ns. Recovery absorbs sorted
runs, so this is the shape an install already has.

Deletion has no borrow and no merge, which is a choice. Deleting three keys in four at a million
leaves the leaf count where it was, 32,768 at a quarter fill, since a separator left behind
still routes and an emptied leaf keeps its place in the chain. The get falls with the keys, 121.3
to 80.7ns, and the walk pays instead, 1.1 to 2.1ns a key; rebuilding from the survivors returns
8,192 leaves, a 41.4ns get and a 0.5ns walk. A rebuild at 9.4ns a key takes it back, which is
defensible only because the index is rebuilt at every install and every compaction anyway.
**A volume that deletes heavily and never compacts would degrade with nothing to stop it.**
`fill_factor()` is the gauge and `repack_owed` the trigger, leaves held at twice the leaves the
live keys need: a doubling guard rather than a fill threshold, since a shard holding three keys
reads as badly under-filled and packing it moves three keys into one leaf. The prune pass calls
it, having just dropped the graves with the shard held.

## What shipped

**Every column is on a tree unless it asks for the other shape.** `ShardMap` is generic in its
value as well as its key and `Shape` names the pair of maps a column's shards are built from.
Sixteen fixed arms of `ColumnIndex` take `Trees<N>`, four take `OpenTables<N>` at 32, 34, 72
and 108 bytes, and the variable arm takes `VarTrees`, each at the node width its key asks for.

**The open shard is a declaration, not a format.** `map_shape` asks for it, it is resident-side
only so no on-disk byte turns on it and a reopen may flip it, and a volume opened under
`ShardShapes::Tree` drops the request rather than refusing it. A variable column, or a width
with no open arm, asking for one is refused at open. Three parallel arrays and no nodes: a slot
costs its control byte, its key and its value, probing is linear from a home slot with a delete
shifting the run back over the hole, and the capacity is any size, from a multiply into it
rather than a mask, so a table built from a run of known length is sized once at its 7/8 load
factor.

**The walk runs both ways**, leaves chained in both directions so `page_back` is served by the
chain rather than by a reversed `BTreeMap` range, which is the only thing that could hold the
paged playback's downward cursor. **`OFF` is gone and the tie counter with it**: `tie_rate` is
a walk of what is held rather than two atomics sharing one cache line across 65,536 shards,
which would price a single-threaded bench honestly and a running store dishonestly. **The small
books moved too**, `TreeKey` covering `u64`, `u32`, `SegmentId` and `Lsn`, so the frontier
book, the cue points, the tailer's positions, the sealed ranges and the segment holds are trees
as well.

**One tree for both key kinds cost the fixed columns nothing.** At each column's own production
width, scattered keys run 1.01 on insert and 1.02 on get against the branch before it and tied
keys 0.86 to 0.91, with the walk flat. Two overrides hold that up: `TreeKey::open`, `close` and
`hand_over` keep the `copy_within` they always were for `[u8; N]`, since `rotate_right` on a
wide element is a cycle of swaps rather than one shift and cost 1.10 on scattered inserts, and
a tied inner walk steps a cursor from the front rather than placing it by binary search, which
adds four unpredictable branches over a stride as wide as the key and costs 1.20x to 1.27x of
a tied get at the widths a 72 or 108 byte key ships at.

## The scan prefix that defeats the lead

The tree searches a node on eight byte leads and reads whole keys only across a tie. Measured
2026-08-11 through a real volume, one key set per column family written the way that family's
own prefix helper reads it, **seven of the eighteen fixed columns tied at 1.0000** and they are
exactly the seven whose scan shares eight bytes or more. The other eleven tied at 0.0000, and so
did the five variable columns, whose window retunes past what their keys share.

None of the seven tied by accident. Each writes its scan's prefix first, an owner address or an
epoch or a timestamp, because that is what makes the scan a range: the prefix that makes the
scan possible is the prefix that defeats the lead. What it cost was the trick itself, since
`seek` counts the leads below the wanted one, found the whole node equal, and walked it
comparing full keys, so a tied column paid the node width in whole-key comparisons. Those seven
declare keys from 24 bytes to 96, so the budget puts them between 42 slots a node and 16,
against the 64 they all held before.

**A fixed key takes the window the names take**, `Shared<N>` at the key's own width, since a
node's entries cannot agree on more bytes than a key has. What does not work is a per-column
lead offset, which only preserves order where every key in the shard shares the bytes before
it, and an epoch-keyed column is a single shard holding every epoch; the window's invariant is
one node's, and a node inside that shard holds one epoch. A million keys a shape against the
same tree with its window switched off, both arms in one process, the mean of two runs, ns a
get and ns an entry walked:

| shape, 1M keys | flat get | window get | flat miss | window miss | flat walk | window walk | skip | tie |
|---|---|---|---|---|---|---|---|---|
| snapshot epoch/group/chunk, 24B | 235.1 | **167.4** | 140.1 | **78.2** | 2.95 | **2.74** | 22.8 | 0.011 |
| tuple group/tape/track, 34B | 239.2 | **151.9** | 239.2 | **138.0** | 4.24 | **3.00** | 17.4 | 0.001 |
| wal generation/offset, 16B | 252.2 | **133.7** | 229.3 | **78.9** | 2.82 | **1.92** | 13.0 | 0.000 |
| spool then address, 34B | 109.1 | **101.7** | 121.2 | **113.7** | 2.74 | 2.85 | 0.0 | 0.000 |
| address, 32B | 103.1 | 105.9 | 123.3 | **117.2** | 2.36 | 2.76 | 0.0 | 0.000 |

**A shape whose lead already discriminated keeps its reads**, which is the gate the fix had to
clear, and it takes three things to hold that. A window narrower than the lead is dropped: a
run under eight bytes is one the lead reads past already, so moving the window there buys no
discrimination and costs every probe a placement, and a column whose keys share nothing carries
`off` at zero and reads its lead from the front the way `Whole` does. The placement and the lead
are one call, `LeadWindow::word`, since a node asked for them separately read the same probe
twice and cost 11 percent on the address shapes. And a node is `repr(C)` with the window behind
the length, since the compiler otherwise lays it past the values, a kilobyte from the length,
and a search waits on a second line of the node to learn where its lead starts. What it costs
is bytes, 2 to 5 a key: the window inline in every node, and every separator now held whole,
which is what lets a node that retunes rebuild its leads at the offset it moved to.

**The bytes are what a shape with nothing to skip pays**, and it pays them on the walk rather
than on the search. Their gets land inside 7 percent either way across repeated runs and their
misses come back 5 and 6 percent quicker, but a node carrying a window it never moves is a
wider node to step through: the 32 byte address shape walks 0.4ns an entry slower, and one
spool's shard of the 34 byte column, small enough to sit in cache throughout, gets 5 percent
slower. **Writes go the way the gets do**, a million inserts running 13, 13 and 55 percent
faster on the three shapes, 5 percent faster on the spool column and 7 percent slower on the
address one, with steady state churn no slower on any of them.

**Correctness is held by the oracle**: `a_shared_lead_moves_the_window_and_still_answers` holds
that every key of a fully shared shape answers, that a probe carrying none of the shared bytes
is placed at the edge rather than by a lead read past bytes it does not have, and that the
ordered walk stays in order. `a_refilled_node_places_probes_against_what_it_holds_now` holds the
one a fixed width finds and a name shape mostly hid: **a window retunes on the bytes a node
agreed on, not on how many there were**, or a node emptied and filled again goes on placing
probes against the keys that left. `ReelStore::lead_tie_rates` is the surface, walked off the
leaves, so a column the window cannot reach fails the caller's guard rather than joining the
table quietly.

## The name columns, and the lead a bucket defeats

The five variable columns take `VarTrees`, the same tree at `Box<[u8]>`: the keys sit on the heap
and the node holds pointers to them, so a shift moves sixteen bytes a slot rather than a name and
the lead array a search reads stays inline. The borrow contract at `ShardMap::walk` is unchanged,
since a node still holds a `Box<[u8]>` to hand back.

**The lead does not survive an object key.** A listing key is a thirty-two byte bucket address
and then a name, so every key in a bucket shares its whole leading eight bytes and the flat lead
ties at **1.0000 on every name shape**. The offset that fixes it has to come from something
smaller than a shard: `object_list` shards on one leading address byte, so a shard holds every
bucket starting with that byte and skipping thirty-two would order two buckets by name instead
of by bucket.

**A node is small enough.** `Shared` keeps the bytes one node's own entries agree on and reads
the lead from just past them, which on an object key is the name rather than the bucket. The
invariant is local to that node and no two nodes have to agree on anything: every entry begins
with `pre[..off]`, so a probe that does too is ordered against them by the eight bytes after it
and a probe that does not is below all of them or above all of them. That is exact for a search
and for an insert's slot alike, which is what lets one window serve `seek`, `range` and the
sorted batch. A leaf tunes from its keys and an inner node from its separators, at a build and
at a split, and an insert retunes only where the arriving key broke what the rest agreed on,
holding every separator whole because rebuilding leads at a new offset reads them again.

`var_node_width` prices the window, 16,384 keys a shard over four object-key corpora, against
the `BTreeMap` this replaces and against the same tree with its window switched off:

| corpus | btreemap get | flat get | window get | window walk/key | window tie |
|---|---|---|---|---|---|
| opaque, 48B | 128.3 | 185.1 | **56.5** | 0.5 | 0.0000 |
| dated, 61B | 111.2 | 203.5 | **83.1** | 0.6 | 0.0582 |
| tenanted, 110B | 135.8 | 220.0 | **126.3** | 0.5 | 0.4518 |
| full length, 1056B | **702.2** | 804.1 | 1202.5 | 1.0 | 1.0000 |

**The walk is the win and it is the one listing pays for**, every name shape walking in half the
time or better where nothing else in the table is uniform. **The flat arm is worth nothing and
the window is what pays for the tree**: the flat lead loses every row to `BTreeMap` on a get,
paying the pointer chase and getting no discrimination back, which is what a tie rate of one
means.

**Width and tie rate are coupled, which the fixed arm never saw.** A wider node holds keys
agreeing on less, so its window moves less and its leads tie more: `tenanted` ties at 0.078,
0.202, 0.452, 0.950 and 0.977 as the width goes 8, 16, 32, 64, 128. **`VAR_NODE_WIDTH` is 32**,
from that sweep rather than from `NODE_BUDGET`, which has nothing to divide when a node holds
`B` pointers whatever the names weigh: there the walk is at its floor, the get is within three
percent of the best arm on every corpus, and the ties are under a half.

**The window is capped at what a node holds inline**, and a cap under what a corpus shares buys
nothing at all, since the lead lands back inside the shared run: `tenanted` ties at 0.9844 under
a cap of 64 against 0.4518 at 128, its gets running 167ns and 123. A cap of 256 buys nothing
over 128 and costs eight bytes a key, so `SHARED_CAP` is **128**. Two ceilings sit past it.
`full length` shares a thousand bytes and keeps its ties whatever the cap, and a leaf straddling
two buckets agrees on nothing and reads its lead from the front again: at 64 keys a bucket most
leaves straddle and the rate stays at 0.81, at 2,048 it falls to 0.49. A split cutting on a
prefix change rather than in half would hold a leaf inside one bucket, and it is not built.

## What a listing costs, which is the gate

A caller-side listing benchmark, six runs an arm alternated over one bucket of 16,384 objects at
five page sizes and a folder roll-up per corpus, against a control built from the checkout where
`object_list` still resolves to `BTreeMap` and compared binary against binary before either
runs. **Ten of the eighteen rows are at or under the map they replace, and the eight over it run
0.2 to 3.7 percent**, spreads overlapping the control's and the seek-heaviest shape five percent
faster. What that mostly says is how little of a listed row is the map at all: the index numbers
above move by factors and these move by percent, because a row is a seek, a walk step and a
record played back and only the first two are the map's.

## The sweep, and what an unordered shard can promise

A maintenance pass wants complete, resumable coverage of a column. It does not want key
order, and asking for order is what shuts an unordered shard out: `page` resumes from a
key, which on the open table means gathering the shard and sorting it to return a page.

`ShardMap::sweep` asks for coverage instead. The tree walks in key order and marks with
the last key it handed out; the open table walks slots and marks with a slot, which costs
the page rather than the shard. One method, both shapes, no new variant.

A mark is opaque and each shape only reads its own. It carries the table generation,
since a resize moves every slot, and `ColumnMark` carries the opening that minted it,
since a mark outlives its process through a persisted cursor or a peer's request. A mark
from elsewhere restarts rather than resuming into a layout that is not there.

So the promise is at-least-once, not exactly-once: a shard that resizes mid sweep starts
over. That is what the callers need, and it is all an unordered shape can give.

`sweep_prefix` narrows it, and only where the prefix is exactly the shard key. Shorter
spans shards, longer splits one, and neither can be served by walking one shard's slots,
so both are refused rather than served by scanning the family.

## Resident bytes per key, which the tree does not win

Weighed by the counting allocator over the same corpora, every arm cloning its keys in so each
holds one owned copy, **the tree costs 6 to 25 percent more room than the map it replaces**:
158 bytes a key against 126 on `opaque`, 222 against 188 on `tenanted`, 1,199 against 1,134
where a key is a kibibyte. The boxed slot is where it goes, a sixteen byte pointer and the whole
key on the heap, 64 bytes on the shortest corpus and 1,072 on the longest. Front coding inside
the leaf would take the same key to 6 to 18 bytes and is unbuilt, because it changes what `walk`
and `span` can yield and that is a trait change this cut does not make.

## A lossy key must never be the map key

Prefix keys keep getting proposed for the resident index, on the reasoning that the tree could
hold eight bytes instead of thirty-four and that a collision costs one wasted read. The first
half is attractive and the second half is false, and the difference is data loss rather than a
latency tail. **A collision costs a wasted read for a filter. It costs a key for a map.** Keyed
on the prefix, inserting a key whose prefix matches a resident one overwrites it and the old key
becomes unreachable, with nothing to report it. The rate is beside the point, because the keys
are not natural: a content address is chosen by whoever uploads the data, so a colliding pair is
a birthday search over 72 bits once the shard byte is counted, about 2^36 hashes, under two
hours on one core. Manufacture one and the store answers for a record it cannot resolve,
honestly and wrongly.

So the rule is not about length. **A lossy key must never be the map key in a structure that
cannot hold two entries under one prefix.** The open-addressed arm obeys it by construction:
the whole key is the map key and every hit compares it, and the control byte's seven bits of
hash are a probe hint and nothing a slot is claimed by. With duplicate support the prefix
length is a performance knob; without it nothing under 16 bytes is defensible, and 16 only
because 2^68 is unreachable.

## What is not measured

**Huge pages.** The index runs to gibibytes and 65,536 separately allocated shards scatter its
pages rather than pack them. A configuration change rather than a rewrite.

**Resident bytes per key on the fixed columns.** The harness is `key_footprint`, weighing tree,
map, hash and open table over 32 byte keys at both loads, and the numbers are still owed.

**Contention, and the window under churn.** Every arm is single threaded, so the claim that
65,536 shards leave nothing for a lock-free map to win is an argument from shard count, and
every variable arm is one thread building one shard in key order. A shard taking scattered
inserts retunes more often: bounded, since a window only narrows between rebuilds, and
unmeasured.
