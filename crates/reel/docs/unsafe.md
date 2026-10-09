# Unsafe: every site, and what makes it sound

**80 `unsafe` occurrences in 60 items across 10 files.** `tape-reel` has 78 of them in 59 items
across 9 files, and `tape-reel-cli` has 2 in one item. `tape-reel-core` and `tape-reel-mock` have
none. The count is the keyword in code outside test modules, with comments stripped, so it covers
what a release build compiles. An item is the function or impl a site sits in, and each `cfg`
alternative counts on its own.

Most of it is one shape. The engine calls the kernel directly, so a syscall that takes a
descriptor or a pointer is an `unsafe` call with no safe wrapper worth writing. The rest is a read
buffer whose leading bytes a completion just reported, the one place the type system cannot see
what the kernel did.

Many sites are `cfg` alternatives of each other, so no build compiles all 59 items in
`tape-reel`. A Linux x86_64 build compiles 53. A macOS aarch64 build compiles 35, with no ring
backend, no x86 scans and no Linux-only syscalls.

| file | occurrences | items | Linux x86_64 | macOS aarch64 |
|---|---|---|---|---|
| `crates/reel/src/io/posix_backend.rs` | 29 | 23 | 18 | 16 |
| `crates/reel/src/io/uring_backend.rs` | 15 | 10 | 10 | 0 |
| `crates/reel/src/io/direct.rs` | 9 | 8 | 8 | 8 |
| `crates/reel/src/index/tbtreemap.rs` | 8 | 5 | 5 | 0 |
| `crates/reel/src/io/mapping.rs` | 7 | 6 | 6 | 6 |
| `crates/reel/src/reel/bias.rs` | 5 | 4 | 3 | 3 |
| `crates/reel/src/io/op.rs` | 2 | 1 | 1 | 1 |
| `crates/reel/src/engine/store_impl.rs` | 2 | 1 | 1 | 1 |
| `crates/reel/src/compaction/compactor.rs` | 1 | 1 | 1 | 0 |
| `crates/reel-cli/src/term.rs` | 2 | 1 | 1 | 1 |

## Posix backend

The syscall surface.

| site | what it does | what makes it sound |
|---|---|---|
| `nowait_preadv`, Linux | `preadv2` with `RWF_NOWAIT`, so a cold page fails with `EAGAIN` and never waits | the list holds `count` buffers of the room their owners handed over, and the kernel writes no more than that |
| `warm_split` | one non-blocking vectored read filling a header buffer and a payload buffer | the kernel reported filling the whole of both, and it fills the first before the second |
| `write_all_at` | `pwrite` in a resume loop over a direct staging buffer | the pointer covers an allocation of `len` bytes and the resume offset is always inside it |
| `read_at` | `pread` in a resume loop over the same | as above, the range stays inside the owned allocation |
| `read_covering` | reads the blocks around a range into the stage and takes the bytes it landed | the read reported filling this many bytes from the buffer's start |
| `pread_into` | `pread` in a resume loop into a caller's buffer, then commits | `filled` never passes the room, so pointer and length stay in room the buffer owns |
| `one_pread` | one `pread` at an offset, leaving the descriptor's cursor alone | pointer and length cover room the caller's `ReadBuf` owns, and the kernel writes no more than that |
| `pwritev_all_into` | `pwritev` over the drain's own buffers | the iovec list is built from live `WriteBuf` spans right above the call, and the kernel reads no more than they hold |
| `preadv_into` | one vectored read filling header and payload, committed in that order | the kernel fills the first buffer before the second, so a short read cuts the body and a shorter one cuts the head |
| `raw_length`, `sync_dir`, `truncate_to` | `fstat`, `fsync` on a directory handle, `ftruncate` | **invariant undocumented.** Each is an ffi call on a descriptor the caller holds open across it, writing only into a struct this frame owns |
| `raw_sync_data`, `raw_sync_full`, `raw_sync_range` | `fdatasync` on Linux and `fsync` elsewhere, `fsync`, `sync_file_range` on Linux only | **invariant undocumented.** Descriptor-only calls with no buffer |
| `raw_allocate` | `fallocate` with keep-size on Linux, `fstat` then `F_PREALLOCATE` on macOS | **invariant undocumented.** A descriptor plus a `stat` and an `fstore_t` this frame owns |
| `raw_release` | `fallocate` with hole-punch and keep-size on Linux, `F_PUNCHHOLE` on macOS | **invariant undocumented.** A descriptor plus an `fpunchhole_t` this frame owns |
| `raw_advise` | `posix_fadvise` on Linux, `F_RDAHEAD` on macOS | **invariant undocumented.** A descriptor and integers only |

## Ring backend

Linux only.

| site | what it does | what makes it sound |
|---|---|---|
| `unsafe impl Send for IoVecs` | lets an op's iovec array cross to the engine thread | the pointers point at buffers the same record owns for the whole flight, and the slot holds list and record together until the completion returns |
| ring buffer registration | hands the kernel a pool of aligned buffers | every span is a buffer the pool owns and never moves, and the ring drops before the pool |
| `Buffers::filled` | the leading bytes a completion landed in a registered buffer | `# Safety`: the count must be at most what a completion reported |
| read completion commit | commits a whole-record read | the kernel wrote exactly that many bytes into the buffer's room |
| split read completion commit | commits a header and payload pair | a vectored read fills the first buffer before the second |
| staged read, staged split | cuts the wanted window out of a registered staging buffer | the count came off the completion, so the range is what the kernel wrote |
| `run_owed_work` | an enter that submits nothing and waits for nothing, so the kernel runs the completion work it holds for this thread | the call passes no buffer, and it runs on the thread that owns the ring, which deferred completion work requires |
| submission push, kick arm | pushes an entry onto the submission queue | the entry's buffers live in the record or the pool for the whole flight, or it is a poll with no buffer on an eventfd that outlives the engine thread |
| `kicked` | reads the counter a wake left | an ffi read of the eight bytes an eventfd counter holds |
| `Inbox::new` | creates the eventfd and takes ownership of it | the descriptor is fresh from the kernel and owned by nothing else |
| `kick` | writes one tick to the eventfd | exactly the eight bytes the counter takes, from a buffer of that width |

## Direct staging

Every site has a comment.

| site | what it does | what makes it sound |
|---|---|---|
| `cut_into` | copies a landed window into the buffer that asked for it | the count is capped by the destination's room, and the commit covers exactly the bytes the copy wrote |
| `unsafe impl Send for AlignedBuf` | lets a staging buffer cross threads | the buffer owns its allocation outright and hands out no interior references |
| `AlignedBuf::new` | zeroes a fresh allocation so pad bytes never hold heap residue | the allocation is `buf.len` bytes, so zeroing it is in bounds |
| `AlignedBuf::uninit` | allocates on a block boundary | the layout is non-zero and its alignment is a power of two |
| `AlignedBuf::filled` | the leading bytes something wrote | `# Safety`: the count must come from a read |
| `zero_from` | zeroes the tail past what a caller gathered | `at` is inside the allocation and the run reaches exactly its end |
| `as_mut_slice` | hands out the whole buffer for gathering a write | the exclusive borrow rules out an overlapping read |
| `Drop` | frees the allocation | the pointer came from `alloc` under this exact layout |

## Mapping

| site | what it does | what makes it sound |
|---|---|---|
| `prefetch` | inline asm `prfm` on aarch64, `_mm_prefetch` on x86_64 | a prefetch is a hint and cannot fault, and both callers pass live memory anyway, a tree arena slot or a record's bytes |
| `unsafe impl Send`, `unsafe impl Sync` for `Mapping` | lets one mapping serve every reader | read-only memory, and the format only truncates past the end of every record a read reaches |
| `Mapping::open` | `mmap` of a segment file over the span it may grow to, read-only and shared | **no comment at the call.** The span is checked non-zero and in range right above, the hint is null, and the descriptor is live for the call |
| `Mapping::slice` | hands out a span of the mapping | in bounds of the live mapping and below a length the file has had, checked against the seen length and the span first |
| `Drop` | `munmap` | the base and span are the ones `mmap` mapped |

## Tree scans

x86_64 only. Every other machine counts with the scalar loop, which needs no `unsafe`.

| site | what it does | what makes it sound |
|---|---|---|
| `count_below`, x86_64 | dispatches to the widest scan the processor reports | each arm runs only where detection found its feature, and every load stays in bounds |
| `count_avx512`, `count_avx2` | the 512-bit and 256-bit forms | **invariant undocumented at the declaration.** Both are `#[target_feature]` `unsafe fn` with no `# Safety` section. The dispatch and the `scans` wrappers state the feature requirement |
| `scans::avx512`, `scans::avx2` | the same two, callable directly so a test can compare them | `# Safety`: the caller must have detected the feature first |

## Read buffers

| site | what it does | what makes it sound |
|---|---|---|
| `ReadBuf::commit` | sets the length to what a backend reported filling | `# Safety`: the caller must have written at least that many bytes through `as_mut_ptr`. The count is also capped at the room asked for |

## Startup bias

The startup pass that reads what the machine already knows. All commented.

| site | what it does | what makes it sound |
|---|---|---|
| `open_file_limit` | `getrlimit` | it writes into the struct it is handed and reads nothing else |
| `probe_ring`, Linux | `io_uring_setup` and a close, to learn whether a ring sets up here | the kernel writes at most one params struct into a wider buffer, and the descriptor returned is the one closed |
| `memory_bytes`, macOS | `sysctlbyname` for total memory | the name is a nul-terminated literal and the kernel writes at most `len` bytes into a `u64` this frame owns |
| `stat_volume` | `statvfs` for capacity and free space | it writes into the struct it is handed and reads a path this frame owns for the duration |

## Store capacity check

| site | what it does | what makes it sound |
|---|---|---|
| `available_bytes` | `statvfs` behind the write path's capacity check | `statvfs` fills the whole struct it is handed, and the nul-terminated path outlives the call |

## Compaction

| site | what it does | what makes it sound |
|---|---|---|
| `erase_range`, Linux | `fallocate` with hole-punch and keep-size, giving a dead run's blocks back | an ffi call on a descriptor the caller holds open across it |

## CLI terminal width

| site | what it does | what makes it sound |
|---|---|---|
| `winsize` | zeroes a `libc::winsize` and asks `ioctl(TIOCGWINSZ)` for the terminal's width | **invariant undocumented.** The struct is plain integers, so all zeros is a valid value, and the kernel writes at most one `winsize` into a struct this frame owns |

## Out of scope

This list says nothing about whether each invariant holds under every interleaving. Where a
site's soundness depends on a race, a test holds it up.
