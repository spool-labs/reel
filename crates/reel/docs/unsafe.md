# Unsafe: every site, and what makes it sound

**75 `unsafe` occurrences across 61 items, in 9 files, all of them in `reel`.**
`tape-reel-core` and `tape-reel-mock` have none. Test modules are excluded from those
counts; what follows is what a release build compiles.

Nearly all of it is one shape. The engine talks to the kernel directly, so a
syscall taking a descriptor or a pointer is an `unsafe` call with no safe
wrapper worth writing. The rest is a read buffer whose leading bytes a
completion has just reported, which is the one place the type system cannot see
what the kernel did.

Many of the sites below are `cfg` alternatives of each other, so no build
compiles all 61. A Linux x86_64 build compiles 55; a macOS aarch64 build
compiles 36, having no ring backend and no Linux-only syscalls.

## `io/posix_backend.rs`: 26 occurrences, 22 items

The syscall surface. Six shapes, then the wrappers.

| site | what it does | what makes it sound |
|---|---|---|
| `warm_read` | commits what a non-blocking read reported filling | the count came off the kernel's own return, capped at the room the buffer handed over |
| `nowait_preadv` | `preadv2` with `RWF_NOWAIT`, so a cold page refuses instead of waiting | the list names `count` buffers of the room their owners handed over, and the kernel writes no more than that |
| the split warm read | one non-blocking vectored read filling a header buffer and a payload buffer | the kernel reported filling the whole of both, and it fills the first before the second |
| `write_all_at` | `pwrite` in a resume loop over a direct staging buffer | the pointer names an allocation of `len` bytes and the resume offset is always inside it |
| `read_at` | `pread` in a resume loop over the same | as above; the range stays inside the owned allocation |
| the direct covering read | names the bytes a covering read landed in the stage | the read reported filling this many bytes from the buffer's start |
| `pread_into` | `pread` in a resume loop into a caller's buffer, then commits | `filled` is never past the room, so pointer and length name room the buffer owns |
| `one_pread` | one `pread` at an offset, without moving the descriptor's cursor | the pointer and length name room the caller's `ReadBuf` owns, and the kernel writes no more than that |
| the vectored write | `pwritev` over the drain's own buffers | the iovec list is built from live `WriteBuf` spans immediately above the call, and the kernel reads no more than the bytes they hand over |
| the split `preadv` | one vectored read filling header and payload, committed in that order | the kernel fills the first buffer before the second, so a short read cuts the body and a shorter one cuts the head |
| `raw_length`, `sync_dir`, `truncate_to` | `fstat`, `fsync` on a directory handle, `ftruncate` | **invariant undocumented.** Each is an ffi call on a descriptor the caller holds open across it, writing only into a struct this frame owns |
| `raw_sync_data`, `raw_sync_full`, `raw_sync_range` | `fdatasync`, `fsync`, `sync_file_range`, three `cfg` pairs | **invariant undocumented.** Descriptor-only calls that name no buffer |
| `raw_allocate` | `fallocate` on Linux, `F_PREALLOCATE` then extend on macOS | **invariant undocumented.** Descriptor plus an `fstore_t` this frame owns |
| `raw_advise` | `posix_fadvise` on Linux, `F_RDAHEAD` on macOS | **invariant undocumented.** Descriptor and integers only |

## `io/uring_backend.rs`: 14 occurrences, 12 items

Every site here carries a `SAFETY` comment in the code.

| site | what it does | what makes it sound |
|---|---|---|
| `unsafe impl Send for IoVecs` | lets an op's iovec array cross to the engine thread | the pointers name buffers the same record owns for the whole flight, and the slot holds list and record together until the completion returns |
| ring buffer registration | hands the kernel a pool of aligned buffers | every span names a buffer the pool owns and never moves, and the ring is dropped before the pool |
| `Buffers::filled` | names the leading bytes a completion landed in a registered buffer | the count has to be one a completion reported |
| read completion commit | commits a whole-record read | the kernel wrote exactly that many bytes into the buffer's room |
| split read completion commit | commits a header and payload pair | a vectored read fills the first buffer before the second |
| staged read, staged split | cuts the wanted window out of a registered staging buffer | the count came off the completion, so the range named is what the kernel wrote |
| submission push, kick arm | pushes an entry onto the submission queue | the entry names buffers held for the whole flight, or a descriptor that outlives the engine thread and no buffer at all |
| `kicked` | reads the counter a wake left | an ffi read of the eight bytes an eventfd counter holds |
| `Inbox::new` | creates the eventfd and takes ownership of it | the descriptor is fresh from the kernel and owned by nothing else |
| `kick` | writes one tick to the eventfd | exactly the eight bytes the counter takes, from a buffer of that width |

## `io/direct.rs`: 9 occurrences, 8 items

Aligned staging for direct io. Every site carries a comment.

| site | what it does | what makes it sound |
|---|---|---|
| `cut_into` | copies a landed window into the buffer that asked for it | the count is capped by the destination's room, and the commit names exactly the bytes the copy wrote |
| `unsafe impl Send for AlignedBuf` | lets a staging buffer cross threads | the buffer owns its allocation outright and hands out no interior references |
| `AlignedBuf::new` | zeroes a fresh allocation so pad bytes never carry heap residue | the allocation is `buf.len` bytes, so zeroing it is in bounds |
| `AlignedBuf::uninit` | allocates on a block boundary | the layout is non-zero and its alignment is a power of two |
| `AlignedBuf::filled` | names the leading bytes something wrote | `# Safety`: the count has to be one a read reported |
| `zero_from` | zeroes the tail past what a caller gathered | `at` is inside the allocation and the run reaches exactly its end |
| `as_mut_slice` | hands out the whole buffer for gathering a write | the exclusive borrow rules out an overlapping read |
| `Drop` | frees the allocation | the pointer came from `alloc` under this exact layout |

## `io/mapping.rs`: 5 occurrences, 5 items

| site | what it does | what makes it sound |
|---|---|---|
| `unsafe impl Send`, `unsafe impl Sync` | lets one mapping serve every reader | immutable shared memory over a file the format never truncates |
| `Mapping::open` | `mmap` of a whole segment file, read-only and shared | **stated on the type, not on the call.** The length is checked non-zero and in range immediately above, the hint is null, and the descriptor is live for the call |
| `Mapping::slice` | hands out a span of the mapping | in bounds of a live mapping over a file that is never truncated, checked against the mapped length first |
| `Drop` | `munmap` | the base and length are the ones `mmap` returned |

## `index/tbtreemap.rs`: 11 occurrences, 7 items

Vector scans over a node's leads, and a prefetch hint.

| site | what it does | what makes it sound |
|---|---|---|
| `count_below`, aarch64 | NEON compares counting the leads below a wanted key | NEON is baseline on aarch64, and every load is bounded by the chunk iterator rather than by arithmetic on the length |
| `count_below`, x86_64 | dispatches to the widest scan the processor reports | each arm runs only where detection found its feature, and every load is bounded by the chunk iterator |
| `count_avx512`, `count_avx2` | the 512-bit and 256-bit forms | **invariant undocumented at the declaration.** Both are `#[target_feature]` `unsafe fn` with no `# Safety` section; the feature requirement is stated at the dispatch above and in the `scans` wrappers below |
| `scans::avx512`, `scans::avx2` | the same two, callable directly so a test can hold them against each other | `# Safety`: the caller must have detected the feature first |
| `prefetch` | inline asm on aarch64, the intrinsic on x86_64 | a prefetch of any address is architecturally a hint and cannot fault, and the pointer comes from a live arena slot regardless |

## `io/op.rs`: 2 occurrences, 1 item

| site | what it does | what makes it sound |
|---|---|---|
| `ReadBuf::commit` | sets the length to what a backend reported filling | `# Safety`: the caller must have written at least that many bytes to the pointer `as_mut_ptr` returned; the count is also capped at the room asked for |

## `reel/bias.rs`: 5 occurrences, 4 items

The startup pass reading what the machine already knows. All commented.

| site | what it does | what makes it sound |
|---|---|---|
| `open_file_limit` | `getrlimit` | it writes into the struct it is handed and reads nothing else |
| `probe_ring` | `io_uring_setup` and a close, to learn whether a ring sets up here | the kernel writes at most one params struct into a wider buffer, and the descriptor returned is the one closed |
| `memory_bytes`, macOS | `sysctlbyname` for total memory | the name is a nul-terminated literal and the kernel writes at most `len` bytes into a `u64` this frame owns |
| `stat_volume` | `statvfs` for capacity and free space | it writes into the struct it is handed and reads a path this frame owns for the duration |

## `engine/store_impl.rs`: 2 occurrences, 1 item

| site | what it does | what makes it sound |
|---|---|---|
| `available_bytes` | `statvfs` behind the write path's capacity check | `statvfs` writes the whole struct it is handed, and the path is a nul-terminated buffer that outlives the call |

## `compaction/compactor.rs`: 1 occurrence, 1 item

| site | what it does | what makes it sound |
|---|---|---|
| `erase_range` | `fallocate` with hole-punch and keep-size, giving a dead run's blocks back | an ffi call against a descriptor the caller holds open across it |

## What this list is not

It says nothing about whether each invariant actually holds under every
interleaving. Where a site's soundness depends on a race, the thing that holds it
up is a test rather than a comment, and `testing.md` is where those live.
