//! Deterministic fault-injecting simulator backend for the ring-shaped I/O trait
//!
//! An in-memory filesystem models a cached view reads see and a durable view a
//! crash keeps, and a seeded plan injects torn writes, lying syncs, bit flips,
//! crash points, and lost completions. The backend is a pure function of an op
//! stream and a plan, so a seed reproduces an image exactly.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::{ReelError, Result};
use crate::io::fault::{FaultKind, FaultPlan, ScheduledFault};
use crate::io::op::{Advice, Completion, FileId, Op, Outcome, ReadBuf, SegmentEntry, WriteBuf};
use crate::io::{ReelIo, ServingBackend};
use crate::sync::lock;

/// The durable bytes of every file, the image a crash leaves behind
pub type DurableImage = Vec<(PathBuf, Vec<u8>)>;

/// One recorded advise call, so cache hygiene is checkable without a page cache
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdviseTrace {
    pub file: FileId,
    pub offset: u64,
    pub len: u64,
    pub advice: Advice,
}

#[derive(Clone, Debug, Default)]
struct SimFile {
    /// Bytes a read sees, whether or not they reached the medium
    cached: Vec<u8>,

    /// Bytes a crash keeps
    durable: Vec<u8>,

    /// Whether the entry naming this file is durable, so a crash can drop it
    is_linked: bool,
}

impl SimFile {
    /// An empty file whose directory entry has not been made durable yet
    fn created() -> SimFile {
        SimFile::default()
    }

    /// A file recovered from a crash image, so its entry is on the medium
    fn restored(bytes: Vec<u8>) -> SimFile {
        SimFile {
            cached: bytes.clone(),
            durable: bytes,
            is_linked: true,
        }
    }
}

/// What one open handle names
///
/// An unlink takes the name and nothing else, so open handles go on reading and
/// only the last close frees the file.
#[derive(Clone, Debug, Eq, PartialEq)]
enum FileRef {
    /// A file still reachable by its path
    Named(PathBuf),

    /// A file whose name is gone, held open by the handles that share this number
    Detached(u64),
}

#[derive(Debug)]
struct DelayedCompletion {
    completion: Completion,
    remaining_polls: u32,
}

#[derive(Debug)]
struct SimState {
    plan: FaultPlan,

    // Ordered by path so a listing or a subtree rename costs the subtree rather
    // than the whole image.
    files: BTreeMap<PathBuf, SimFile>,
    handles: HashMap<FileId, FileRef>,
    detached: HashMap<u64, SimFile>,
    next_detached_id: u64,
    next_file_id: u64,
    op_position: u64,
    faults_fired: u64,
    faults_drawn: usize,

    ops_submitted: u64,
    swallowed: u64,
    dropped: u64,

    ready: VecDeque<Completion>,
    delayed: Vec<DelayedCompletion>,
    advise_traces: Vec<AdviseTrace>,
    dir_op_order: Vec<String>,
    sync_count: u64,

    read_count: u64,
    read_bytes: u64,

    widest_batch: usize,
    is_crashed: bool,
}

impl SimState {
    fn new(plan: FaultPlan) -> SimState {
        let faults_drawn = plan.faults.len();
        SimState {
            plan,
            files: BTreeMap::new(),
            handles: HashMap::new(),
            detached: HashMap::new(),
            next_detached_id: 0,
            next_file_id: 0,
            op_position: 0,
            faults_fired: 0,
            faults_drawn,
            ops_submitted: 0,
            swallowed: 0,
            dropped: 0,
            ready: VecDeque::new(),
            delayed: Vec::new(),
            advise_traces: Vec::new(),
            dir_op_order: Vec::new(),
            sync_count: 0,
            read_count: 0,
            read_bytes: 0,
            widest_batch: 0,
            is_crashed: false,
        }
    }
}

#[derive(Debug)]
struct SimShared {
    state: Mutex<SimState>,

    /// Ops this backend has executed, the count a leg proves an op count against
    ops: AtomicU64,
}

/// Deterministic in-memory backend that injects faults from a seeded plan
#[derive(Clone, Debug)]
pub struct SimIo {
    shared: Arc<SimShared>,
}

impl SimIo {
    /// Build a simulator backend from a fault plan
    pub fn new(plan: FaultPlan) -> SimIo {
        SimIo {
            shared: Arc::new(SimShared {
                state: Mutex::new(SimState::new(plan)),
                ops: AtomicU64::new(0),
            }),
        }
    }

    /// Reopen a fresh device from a durable image, as a crash survivor would
    pub fn from_image(image: DurableImage) -> SimIo {
        SimIo::from_image_with_plan(image, FaultPlan::new(0))
    }

    /// Reopen from a durable image under a plan, so the open itself can fault
    pub fn from_image_with_plan(image: DurableImage, plan: FaultPlan) -> SimIo {
        let mut state = SimState::new(plan);
        for (path, bytes) in image {
            state.files.insert(path, SimFile::restored(bytes));
        }
        SimIo {
            shared: Arc::new(SimShared {
                state: Mutex::new(state),
                ops: AtomicU64::new(0),
            }),
        }
    }

    /// Seed the simulator was built with
    pub fn seed(&self) -> u64 {
        lock(&self.shared.state).plan.seed
    }

    /// Fault plan the simulator replays
    pub fn plan(&self) -> FaultPlan {
        lock(&self.shared.state).plan.clone()
    }

    /// Stop scheduling new faults, keeping the damage the plan has already done
    ///
    /// A caller disarms before inspecting the result, so the inspection reads the
    /// damage rather than taking its own. Reordering and scatter stay, since they
    /// model a device rather than a failure.
    pub fn disarm(&self) {
        let mut state = lock(&self.shared.state);
        state.plan.faults.clear();
        state.plan.crash_at = None;
    }

    /// Arm one fault kind across the next few ops the volume submits
    ///
    /// A plan pins faults to global op positions a caller can only name by
    /// counting every io so far; this arms the window one call runs in instead.
    pub fn arm_next_ops(&self, ops: u64, kind: FaultKind) {
        let mut state = lock(&self.shared.state);
        let from = state.op_position;
        for at_op in from..from + ops {
            state.plan.faults.push(ScheduledFault { at_op, kind });
        }
        state.faults_drawn = state.plan.faults.len();
    }

    /// Faults the plan actually reached, against the count it scheduled
    ///
    /// A run finishing short of its op estimate leaves the tail of its plan
    /// unreached, so its fault count overstates what it searched.
    pub fn fault_reach(&self) -> (u64, usize) {
        let state = lock(&self.shared.state);
        (state.faults_fired, state.faults_drawn)
    }

    /// Whether a scheduled crash boundary has been reached
    pub fn is_crashed(&self) -> bool {
        lock(&self.shared.state).is_crashed
    }

    /// The bytes every file holds if power is lost now, sorted for a stable image
    ///
    /// A file created without a directory sync behind it is absent rather than
    /// empty, since the crash took the entry that named it.
    pub fn durable_image(&self) -> DurableImage {
        let state = lock(&self.shared.state);
        let mut image: DurableImage = state
            .files
            .iter()
            .filter(|(_, file)| file.is_linked)
            .map(|(path, file)| (path.clone(), crashed_bytes(path, file, &state.plan)))
            .collect();
        image.sort_by(|left, right| left.0.cmp(&right.0));
        image
    }

    /// The bytes one file holds if power is lost now, for crash reopen assertions
    pub fn durable_bytes(&self, path: &Path) -> Option<Vec<u8>> {
        let state = lock(&self.shared.state);
        state
            .files
            .get(path)
            .filter(|file| file.is_linked)
            .map(|file| crashed_bytes(path, file, &state.plan))
    }

    /// Bytes written but not yet synced, the region a crash is free to scatter
    pub fn unsynced_bytes(&self) -> u64 {
        let state = lock(&self.shared.state);
        let mut total = 0u64;
        for file in state.files.values() {
            let overlap = file.cached.len().min(file.durable.len());
            let differing = file.cached[..overlap]
                .iter()
                .zip(&file.durable[..overlap])
                .filter(|(cached, durable)| cached != durable)
                .count();
            let beyond = file.cached.len().saturating_sub(file.durable.len());
            total += (differing + beyond) as u64;
        }
        total
    }

    /// Advise calls recorded so far
    pub fn advise_traces(&self) -> Vec<AdviseTrace> {
        lock(&self.shared.state).advise_traces.clone()
    }

    /// Order directory renames and unlinks were actually applied in
    pub fn dir_op_order(&self) -> Vec<String> {
        lock(&self.shared.state).dir_op_order.clone()
    }

    /// The op ledger against the slot table's, for stall diagnosis
    ///
    /// Submitted minus dropped minus ready and delayed is what callers should
    /// have seen, and a gap says which side of the seam lost a completion.
    pub fn debug_counts(&self) -> String {
        let state = lock(&self.shared.state);
        format!(
            "submitted {} swallowed {} dropped {} ready {} delayed {} crashed {}",
            state.ops_submitted,
            state.swallowed,
            state.dropped,
            state.ready.len(),
            state.delayed.len(),
            state.is_crashed,
        )
    }

    /// Reads that reached the volume, so a caller can prove one never happened
    pub fn read_count(&self) -> u64 {
        lock(&self.shared.state).read_count
    }

    /// Bytes the backend has handed back, which is what a read costs past its count
    pub fn read_bytes(&self) -> u64 {
        lock(&self.shared.state).read_bytes
    }

    /// File syncs asked for so far, the count a durability cadence is judged by
    pub fn sync_count(&self) -> u64 {
        lock(&self.shared.state).sync_count
    }

    /// Most ops handed over in one submission, the depth a caller actually built
    pub fn widest_batch(&self) -> usize {
        lock(&self.shared.state).widest_batch
    }

    /// Ops this backend has executed, for a leg counting what a read cost
    pub fn ops(&self) -> u64 {
        self.shared.ops.load(Ordering::Relaxed)
    }
}

impl ReelIo for SimIo {
    /// The simulation's own backend, which no configuration selects
    fn serving(&self) -> ServingBackend {
        ServingBackend::Sim
    }

    fn sync_count(&self) -> u64 {
        SimIo::sync_count(self)
    }

    fn submit(&self, ops: Vec<Op>) -> Result<()> {
        let mut state = lock(&self.shared.state);
        if state.is_crashed {
            state.swallowed += ops.len() as u64;
            return Ok(());
        }

        let batch_start = state.op_position;
        let submitted = ops.len() as u64;
        state.ops_submitted += submitted;
        state.widest_batch = state.widest_batch.max(ops.len());
        let mut effective = ops;
        if let Some(crash_at) = state.plan.crash_at {
            if crash_at >= batch_start && crash_at < batch_start + effective.len() as u64 {
                let keep = (crash_at - batch_start) as usize;
                effective.truncate(keep);
                state.is_crashed = true;
            }
        }

        let is_reordering = plan_reorders_dir(&state.plan, batch_start, effective.len() as u64);
        let mut indexed: Vec<(usize, Op)> = effective.into_iter().enumerate().collect();
        if is_reordering {
            reorder_dir_ops(&mut indexed);
        }

        for (submit_index, op) in indexed {
            let position = batch_start + submit_index as u64;
            self.shared.ops.fetch_add(1, Ordering::Relaxed);
            let completion = execute_op(&mut state, op, position);
            route_completion(&mut state, position, completion);
        }

        state.op_position = batch_start + submitted;
        Ok(())
    }

    fn poll(&self, out: &mut Vec<Completion>) -> Result<usize> {
        let mut state = lock(&self.shared.state);

        let delayed = std::mem::take(&mut state.delayed);
        let mut still_delayed = Vec::new();
        for mut item in delayed {
            if item.remaining_polls <= 1 {
                state.ready.push_back(item.completion);
            } else {
                item.remaining_polls -= 1;
                still_delayed.push(item);
            }
        }
        state.delayed = still_delayed;

        let drained = state.ready.len();
        if state.plan.reorder_completions {
            while let Some(completion) = state.ready.pop_back() {
                out.push(completion);
            }
        } else {
            while let Some(completion) = state.ready.pop_front() {
                out.push(completion);
            }
        }
        Ok(drained)
    }
}

fn plan_reorders_dir(plan: &FaultPlan, batch_start: u64, len: u64) -> bool {
    plan.faults.iter().any(|fault| {
        matches!(fault.kind, FaultKind::ReorderDir)
            && fault.at_op >= batch_start
            && fault.at_op < batch_start + len
    })
}

fn reorder_dir_ops(indexed: &mut [(usize, Op)]) {
    let dir_slots: Vec<usize> = (0..indexed.len())
        .filter(|&slot| is_dir_op(&indexed[slot].1))
        .collect();
    let count = dir_slots.len();
    for offset in 0..count / 2 {
        indexed.swap(dir_slots[offset], dir_slots[count - 1 - offset]);
    }
}

fn is_dir_op(op: &Op) -> bool {
    match op {
        Op::Rename { .. } | Op::Unlink { .. } => true,
        Op::Open { .. }
        | Op::Writev { .. }
        | Op::Pread { .. }
        | Op::PreadCold { .. }
        | Op::PreadSplit { .. }
        | Op::SyncData { .. }
        | Op::SyncFull { .. }
        | Op::SyncRange { .. }
        | Op::SyncDir { .. }
        | Op::List { .. }
        | Op::Length { .. }
        | Op::Close { .. }
        | Op::Allocate { .. }
        | Op::Truncate { .. }
        | Op::Advise { .. } => false,
    }
}

fn route_completion(state: &mut SimState, position: u64, completion: Completion) {
    match state.plan.fault_at(position) {
        Some(FaultKind::DropCompletion) => {
            state.dropped += 1;
        }
        Some(FaultKind::DelayCompletion { polls }) => {
            if polls == 0 {
                state.ready.push_back(completion);
            } else {
                state.delayed.push(DelayedCompletion {
                    completion,
                    remaining_polls: polls,
                });
            }
        }
        Some(FaultKind::ShortWrite { .. })
        | Some(FaultKind::TornWrite { .. })
        | Some(FaultKind::LyingSync)
        | Some(FaultKind::LyingSyncRange)
        | Some(FaultKind::EnospcAppend)
        | Some(FaultKind::EnospcAllocate)
        | Some(FaultKind::SyncError)
        | Some(FaultKind::ListError)
        | Some(FaultKind::ReadError)
        | Some(FaultKind::ReorderDir)
        | Some(FaultKind::BitFlip { .. })
        | None => state.ready.push_back(completion),
    }
}

fn execute_op(state: &mut SimState, op: Op, position: u64) -> Completion {
    let fault = state.plan.fault_at(position);
    if fault.is_some() {
        state.faults_fired += 1;
    }
    match op {
        Op::Open {
            tag, path, create, ..
        } => {
            let outcome = if create || state.files.contains_key(&path) {
                state
                    .files
                    .entry(path.clone())
                    .or_insert_with(SimFile::created);
                let id = FileId(state.next_file_id);
                state.next_file_id += 1;
                state.handles.insert(id, FileRef::Named(path));
                Outcome::Opened(Ok(id))
            } else {
                Outcome::Opened(Err(no_such_file()))
            };
            Completion { tag, outcome }
        }
        Op::Writev {
            tag,
            file,
            offset,
            bufs,
        } => Completion {
            tag,
            outcome: write_v(state, file, offset, bufs, fault),
        },
        Op::Pread {
            tag,
            file,
            offset,
            mut buf,
        } => {
            let result = match fault {
                Some(FaultKind::ReadError) => Err(input_output()),
                _ => read_into(state, file, offset, &mut buf),
            };
            Completion {
                tag,
                outcome: Outcome::Read { result, buf },
            }
        }
        // The image is in memory, so the buffered descriptor is the only one.
        Op::PreadCold {
            tag,
            file,
            offset,
            mut buf,
            ..
        } => {
            let result = match fault {
                Some(FaultKind::ReadError) => Err(input_output()),
                _ => read_into(state, file, offset, &mut buf),
            };
            Completion {
                tag,
                outcome: Outcome::Read { result, buf },
            }
        }
        Op::PreadSplit {
            tag,
            file,
            offset,
            mut head,
            mut body,
        } => {
            let result = match fault {
                Some(FaultKind::ReadError) => Err(input_output()),
                _ => read_split(state, file, offset, &mut head, &mut body),
            };
            Completion {
                tag,
                outcome: Outcome::ReadSplit { result, head, body },
            }
        }
        Op::SyncData { tag, file } => {
            state.sync_count += 1;
            Completion {
                tag,
                outcome: Outcome::Done(sync_file(state, file, fault)),
            }
        }
        Op::SyncFull { tag, file } => Completion {
            tag,
            outcome: Outcome::Done(sync_file(state, file, fault)),
        },
        Op::SyncRange {
            tag,
            file,
            offset,
            len,
            mode: _,
        } => Completion {
            tag,
            outcome: Outcome::Done(sync_range(state, file, offset, len, fault)),
        },
        Op::SyncDir { tag, dir } => {
            for (path, file) in state.files.iter_mut() {
                if path.parent() == Some(dir.as_path()) {
                    file.is_linked = true;
                }
            }
            Completion {
                tag,
                outcome: Outcome::Done(Ok(())),
            }
        }
        Op::Rename { tag, from, to } => {
            state.dir_op_order.push(file_label(&from));
            let outcome = match state.files.remove(&from) {
                Some(file) => {
                    // A handle follows the file it was opened on, not the name,
                    // so handles on the old path move with it.
                    for held in state.handles.values_mut() {
                        if *held == FileRef::Named(from.clone()) {
                            *held = FileRef::Named(to.clone());
                        }
                    }
                    state.files.insert(to, file);
                    Outcome::Done(Ok(()))
                }
                None => Outcome::Done(Err(no_such_file())),
            };
            Completion { tag, outcome }
        }
        Op::Unlink { tag, path } => {
            state.dir_op_order.push(file_label(&path));
            let outcome = match state.files.remove(&path) {
                Some(file) => {
                    detach(state, &path, file);
                    Outcome::Done(Ok(()))
                }
                None => Outcome::Done(Err(no_such_file())),
            };
            Completion { tag, outcome }
        }
        Op::List { tag, dir } if matches!(fault, Some(FaultKind::ListError)) => {
            let _ = dir;
            Completion {
                tag,
                outcome: Outcome::Listed(Err(input_output())),
            }
        }
        Op::List { tag, dir } => {
            // The directory's files are contiguous, so a listing costs the directory.
            let mut entries: Vec<SegmentEntry> = state
                .files
                .range(dir.clone()..)
                .take_while(|(path, _)| path.starts_with(&dir))
                .filter(|(path, _)| path.parent() == Some(dir.as_path()))
                .map(|(path, file)| SegmentEntry {
                    name: file_label(path),
                    len: file.cached.len() as u64,
                })
                .collect();
            entries.sort_by(|left, right| left.name.cmp(&right.name));
            Completion {
                tag,
                outcome: Outcome::Listed(Ok(entries)),
            }
        }
        Op::Close { tag, file } => {
            let outcome = match state.handles.remove(&file) {
                // The last close of a file nothing names any more is what frees it.
                Some(FileRef::Detached(id)) => {
                    if !holds_detached(state, id) {
                        state.detached.remove(&id);
                    }
                    Outcome::Done(Ok(()))
                }
                Some(FileRef::Named(_)) => Outcome::Done(Ok(())),
                None => Outcome::Done(Err(unknown_file())),
            };
            Completion { tag, outcome }
        }
        Op::Length { tag, file } => Completion {
            tag,
            outcome: Outcome::Length(length_of(state, file)),
        },
        Op::Allocate {
            tag,
            file,
            offset,
            len,
        } => Completion {
            tag,
            outcome: Outcome::Done(allocate(state, file, offset, len, fault)),
        },
        Op::Truncate { tag, file, len } => Completion {
            tag,
            outcome: Outcome::Done(truncate(state, file, len)),
        },
        Op::Advise {
            tag,
            file,
            offset,
            len,
            advice,
        } => {
            state.advise_traces.push(AdviseTrace {
                file,
                offset,
                len,
                advice,
            });
            Completion {
                tag,
                outcome: Outcome::Done(Ok(())),
            }
        }
    }
}

fn write_v(
    state: &mut SimState,
    file: FileId,
    offset: u64,
    bufs: Vec<WriteBuf>,
    fault: Option<FaultKind>,
) -> Outcome {
    if let Err(error) = held_file(state, file) {
        return Outcome::Wrote {
            result: Err(error),
            bufs,
        };
    }
    if matches!(fault, Some(FaultKind::EnospcAppend)) {
        return Outcome::Wrote {
            result: Err(out_of_space()),
            bufs,
        };
    }

    let mut data = Vec::new();
    for buf in &bufs {
        data.extend_from_slice(buf.as_slice());
    }
    let full_len = data.len() as u64;
    let (persist_len, reported) = write_effect(fault, full_len);

    let file_ref = match held_file_mut(state, file) {
        Ok(file_ref) => file_ref,
        Err(error) => {
            return Outcome::Wrote {
                result: Err(error),
                bufs,
            }
        }
    };
    let start = offset as usize;
    let end = start + persist_len as usize;
    if file_ref.cached.len() < end {
        file_ref.cached.resize(end, 0);
    }
    file_ref.cached[start..end].copy_from_slice(&data[..persist_len as usize]);
    if let Some(FaultKind::BitFlip { at_byte, bit }) = fault {
        let index = at_byte as usize;
        if index < file_ref.cached.len() {
            file_ref.cached[index] ^= 1u8 << (bit % 8);
        }
    }
    Outcome::Wrote {
        result: Ok(reported),
        bufs,
    }
}

fn write_effect(fault: Option<FaultKind>, full_len: u64) -> (u64, u64) {
    match fault {
        Some(FaultKind::ShortWrite { written_bytes }) => {
            let persisted = written_bytes.min(full_len);
            (persisted, persisted)
        }
        Some(FaultKind::TornWrite { durable_bytes }) => (durable_bytes.min(full_len), full_len),
        Some(FaultKind::LyingSync)
        | Some(FaultKind::LyingSyncRange)
        | Some(FaultKind::EnospcAppend)
        | Some(FaultKind::EnospcAllocate)
        | Some(FaultKind::SyncError)
        | Some(FaultKind::ListError)
        | Some(FaultKind::ReadError)
        | Some(FaultKind::ReorderDir)
        | Some(FaultKind::BitFlip { .. })
        | Some(FaultKind::DropCompletion)
        | Some(FaultKind::DelayCompletion { .. })
        | None => (full_len, full_len),
    }
}

/// The file one handle was opened on, by name or after its name was taken
fn held_file(state: &SimState, file: FileId) -> Result<&SimFile> {
    match state.handles.get(&file) {
        Some(FileRef::Named(path)) => state.files.get(path).ok_or_else(no_such_file),
        Some(FileRef::Detached(id)) => state.detached.get(id).ok_or_else(no_such_file),
        None => Err(unknown_file()),
    }
}

fn held_file_mut(state: &mut SimState, file: FileId) -> Result<&mut SimFile> {
    match state.handles.get(&file).cloned() {
        Some(FileRef::Named(path)) => state.files.get_mut(&path).ok_or_else(no_such_file),
        Some(FileRef::Detached(id)) => state.detached.get_mut(&id).ok_or_else(no_such_file),
        None => Err(unknown_file()),
    }
}

/// Hand an unlinked file to the handles still open on it, or let it go
///
/// A file nobody has open goes with its name; one still open outlives it until
/// the last handle closes.
fn detach(state: &mut SimState, path: &Path, file: SimFile) {
    let named = FileRef::Named(path.to_path_buf());
    let holders: Vec<FileId> = state
        .handles
        .iter()
        .filter(|(_, held)| **held == named)
        .map(|(id, _)| *id)
        .collect();
    if holders.is_empty() {
        return;
    }
    let id = state.next_detached_id;
    state.next_detached_id += 1;
    for holder in holders {
        state.handles.insert(holder, FileRef::Detached(id));
    }
    state.detached.insert(id, file);
}

/// Whether any handle is still open on a file whose name is gone
fn holds_detached(state: &SimState, id: u64) -> bool {
    state
        .handles
        .values()
        .any(|held| *held == FileRef::Detached(id))
}

fn read_into(state: &mut SimState, file: FileId, offset: u64, buf: &mut ReadBuf) -> Result<usize> {
    state.read_count += 1;
    let file_ref = held_file(state, file)?;
    let start = (offset as usize).min(file_ref.cached.len());
    let available = file_ref.cached.len() - start;
    let count = buf.wanted().min(available);
    buf.fill_from(&file_ref.cached[start..start + count]);
    state.read_bytes += count as u64;
    Ok(count)
}

/// Fill a header buffer and a payload buffer from one contiguous range
fn read_split(
    state: &mut SimState,
    file: FileId,
    offset: u64,
    head: &mut ReadBuf,
    body: &mut ReadBuf,
) -> Result<usize> {
    let wanted = head.wanted() as u64;
    let filled = read_into(state, file, offset, head)?;
    if filled < wanted as usize {
        return Ok(filled);
    }
    let tail = read_into(state, file, offset + wanted, body)?;
    Ok(filled + tail)
}

fn length_of(state: &SimState, file: FileId) -> Result<u64> {
    Ok(held_file(state, file)?.cached.len() as u64)
}

fn sync_file(state: &mut SimState, file: FileId, fault: Option<FaultKind>) -> Result<()> {
    if matches!(fault, Some(FaultKind::SyncError)) {
        return Err(input_output());
    }
    let file_ref = held_file_mut(state, file)?;
    if matches!(fault, Some(FaultKind::LyingSync)) {
        return Ok(());
    }
    file_ref.durable = file_ref.cached.clone();
    Ok(())
}

fn sync_range(
    state: &mut SimState,
    file: FileId,
    offset: u64,
    len: u64,
    fault: Option<FaultKind>,
) -> Result<()> {
    if matches!(fault, Some(FaultKind::SyncError)) {
        return Err(input_output());
    }
    let file_ref = held_file_mut(state, file)?;
    if matches!(fault, Some(FaultKind::LyingSyncRange)) {
        return Ok(());
    }
    let start = offset as usize;
    let end = (start + len as usize).min(file_ref.cached.len());
    if file_ref.durable.len() < end {
        file_ref.durable.resize(end, 0);
    }
    if start < end {
        file_ref.durable[start..end].copy_from_slice(&file_ref.cached[start..end]);
    }
    Ok(())
}

fn allocate(
    state: &mut SimState,
    file: FileId,
    offset: u64,
    len: u64,
    fault: Option<FaultKind>,
) -> Result<()> {
    if matches!(fault, Some(FaultKind::EnospcAllocate)) {
        return Err(out_of_space());
    }
    // The reservation is invisible to the format: it claims blocks without
    // touching the length, so the simulated file keeps ending at its last
    // written byte the way a real one does under a keep-size fallocate.
    held_file_mut(state, file)?;
    let _ = (offset, len);
    Ok(())
}

/// Cut the cached view to a length; the durable image follows at the next sync
fn truncate(state: &mut SimState, file: FileId, len: u64) -> Result<()> {
    let file_ref = held_file_mut(state, file)?;
    file_ref.cached.resize(len as usize, 0);
    Ok(())
}

/// The bytes a medium holds for one file if power is lost now
///
/// A synced byte is kept and an unsynced byte is absent, so what survives is a
/// prefix. Under a scatter setting each unsynced sector lands independently, so
/// a fresh sector can come back after a stale one.
fn crashed_bytes(path: &Path, file: &SimFile, plan: &FaultPlan) -> Vec<u8> {
    let sector = match plan.scatter_bytes {
        Some(bytes) => bytes as usize,
        None => return file.durable.clone(),
    };

    let mut out = file.durable.clone();
    if out.len() < file.cached.len() {
        out.resize(file.cached.len(), 0);
    }
    let stream = path_seed(path) ^ plan.seed;
    let mut at = 0usize;
    let mut ordinal = 0u64;
    while at < file.cached.len() {
        let end = (at + sector).min(file.cached.len());
        if sector_persists(stream, ordinal) {
            out[at..end].copy_from_slice(&file.cached[at..end]);
        }
        at = end;
        ordinal += 1;
    }
    out
}

/// Whether the device had committed one sector when the power went, from the plan
fn sector_persists(stream: u64, ordinal: u64) -> bool {
    let mut hash = stream.wrapping_add(ordinal.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    hash = (hash ^ (hash >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    hash = (hash ^ (hash >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    hash ^= hash >> 31;
    hash & 1 == 0
}

/// A per-file discriminator, so two files do not scatter identically
fn path_seed(path: &Path) -> u64 {
    let mut hash = 0xCBF2_9CE4_8422_2325u64;
    for byte in path.as_os_str().as_encoded_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x1000_0000_01B3);
    }
    hash
}

fn file_label(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn unknown_file() -> ReelError {
    ReelError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "unknown reel file handle",
    ))
}

fn no_such_file() -> ReelError {
    ReelError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "no such reel file",
    ))
}

fn out_of_space() -> ReelError {
    ReelError::Io(std::io::Error::from_raw_os_error(libc::ENOSPC))
}

fn input_output() -> ReelError {
    ReelError::Io(std::io::Error::from_raw_os_error(libc::EIO))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::op::{OwnedBuf, SyncRangeMode, Tag};

    fn poll_all(io: &SimIo) -> Vec<Completion> {
        let mut out = Vec::new();
        io.poll(&mut out).expect("poll");
        out
    }

    fn one(io: &SimIo) -> Completion {
        let mut out = poll_all(io);
        assert_eq!(out.len(), 1);
        out.pop().expect("one completion")
    }

    fn opened(outcome: Outcome) -> Result<FileId> {
        match outcome {
            Outcome::Opened(result) => result,
            _ => Err(ReelError::Backend("expected opened".to_string())),
        }
    }

    fn wrote(outcome: Outcome) -> Result<u64> {
        match outcome {
            Outcome::Wrote { result, .. } => result,
            _ => Err(ReelError::Backend("expected wrote".to_string())),
        }
    }

    fn read_bytes(outcome: Outcome) -> (Result<usize>, OwnedBuf) {
        match outcome {
            Outcome::Read { result, buf } => (result, buf.into_vec()),
            _ => (
                Err(ReelError::Backend("expected read".to_string())),
                Vec::new(),
            ),
        }
    }

    fn done(outcome: Outcome) -> Result<()> {
        match outcome {
            Outcome::Done(result) => result,
            _ => Err(ReelError::Backend("expected done".to_string())),
        }
    }

    // a scatter leaves unsynced sectors partly landed rather than truncated
    #[test]
    fn scatter_lands_some_unsynced_sectors() {
        let sectors = 64usize;
        let sector = 8usize;
        let io = SimIo::new(FaultPlan::new(7).with_scatter(sector as u32));
        let path = Path::new("/reel/scatter.seg");
        let file = open(&io, path, true);
        write(&io, 2, file, 0, &vec![0xab; sectors * sector]);
        poll_all(&io);

        let image = io.durable_bytes(path).expect("image");

        assert_eq!(image.len(), sectors * sector);
        let landed = image.chunks(sector).filter(|run| run[0] == 0xab).count();
        let lost = image.chunks(sector).filter(|run| run[0] == 0x00).count();
        assert!(landed > 0, "no sector survived the crash");
        assert!(lost > 0, "every sector survived, which is not a scatter");
        assert_eq!(
            landed + lost,
            sectors,
            "a sector landed whole or not at all"
        );
    }

    // a scattered image is not a prefix, which is the whole point of the fault
    #[test]
    fn scatter_is_not_a_prefix() {
        let sectors = 64usize;
        let sector = 8usize;
        let io = SimIo::new(FaultPlan::new(11).with_scatter(sector as u32));
        let path = Path::new("/reel/holes.seg");
        let file = open(&io, path, true);
        write(&io, 2, file, 0, &vec![0xcd; sectors * sector]);
        poll_all(&io);

        let image = io.durable_bytes(path).expect("image");
        let landed: Vec<bool> = image.chunks(sector).map(|run| run[0] == 0xcd).collect();
        let first_lost = landed
            .iter()
            .position(|kept| !kept)
            .expect("a sector was lost");

        assert!(
            landed[first_lost..].iter().any(|kept| *kept),
            "a sector landed after a lost one, so the image is not a prefix"
        );
    }

    // everything synced before the crash survives the scatter untouched
    #[test]
    fn scatter_keeps_synced_bytes() {
        let sector = 8usize;
        let io = SimIo::new(FaultPlan::new(3).with_scatter(sector as u32));
        let path = Path::new("/reel/synced.seg");
        let file = open(&io, path, true);
        write(&io, 2, file, 0, &vec![0x5a; sector * 32]);
        poll_all(&io);
        io.submit(vec![Op::SyncFull { tag: Tag(3), file }])
            .expect("submit sync");
        poll_all(&io);

        let image = io.durable_bytes(path).expect("image");

        assert_eq!(
            image,
            vec![0x5a; sector * 32],
            "a synced byte is on the medium"
        );
    }

    // the same seed scatters the same way, so a failure reproduces
    #[test]
    fn scatter_is_deterministic() {
        let sector = 8usize;
        let image_of = |seed: u64| {
            let io = SimIo::new(FaultPlan::new(seed).with_scatter(sector as u32));
            let path = Path::new("/reel/repeat.seg");
            let file = open(&io, path, true);
            write(&io, 2, file, 0, &vec![0x77; sector * 32]);
            poll_all(&io);
            io.durable_bytes(path).expect("image")
        };

        assert_eq!(image_of(21), image_of(21));
        assert_ne!(
            image_of(21),
            image_of(22),
            "a different seed scatters differently"
        );
    }

    // A created file is linked down first, since these tests are about bytes.
    fn open(io: &SimIo, path: &Path, create: bool) -> FileId {
        io.submit(vec![Op::Open {
            tag: Tag(1),
            path: path.to_path_buf(),
            create,
            direct: false,
        }])
        .expect("submit open");
        let file = opened(one(io).outcome).expect("open result");
        if create {
            link_down(io, path);
        }
        file
    }

    // Marked durable in place: a directory sync op would shift every fault position.
    fn link_down(io: &SimIo, path: &Path) {
        lock(&io.shared.state)
            .files
            .get_mut(path)
            .expect("created file")
            .is_linked = true;
    }

    fn write(io: &SimIo, tag: u64, file: FileId, offset: u64, bytes: &[u8]) {
        io.submit(vec![Op::Writev {
            tag: Tag(tag),
            file,
            offset,
            bufs: vec![WriteBuf::owned(bytes.to_vec())],
        }])
        .expect("submit write");
    }

    fn path_in(root: &Path, name: &str) -> PathBuf {
        root.join(name)
    }

    // a plain write lands and reads back byte for byte
    #[test]
    fn write_read_roundtrip() {
        let io = SimIo::new(FaultPlan::new(1));
        let path = path_in(Path::new("/reel"), "segment-0");
        let file = open(&io, &path, true);

        write(&io, 2, file, 0, b"payload");
        assert_eq!(wrote(one(&io).outcome).expect("wrote"), 7);

        io.submit(vec![Op::Pread {
            tag: Tag(3),
            file,
            offset: 0,
            buf: ReadBuf::new(7),
        }])
        .expect("submit read");
        let (result, buf) = read_bytes(one(&io).outcome);
        assert_eq!(result.expect("read"), 7);
        assert_eq!(&buf, b"payload");
    }

    // an unlinked file serves the handles still open on it and goes with the last
    #[test]
    fn unlinked_file_outlives_its_name() {
        let io = SimIo::new(FaultPlan::new(1));
        let path = path_in(Path::new("/reel"), "segment-0");
        let file = open(&io, &path, true);
        write(&io, 2, file, 0, b"payload");
        let _ = poll_all(&io);

        io.submit(vec![Op::Unlink {
            tag: Tag(3),
            path: path.clone(),
        }])
        .expect("submit unlink");
        done(one(&io).outcome).expect("unlink");

        io.submit(vec![Op::Pread {
            tag: Tag(4),
            file,
            offset: 0,
            buf: ReadBuf::new(7),
        }])
        .expect("submit read");
        let (result, buf) = read_bytes(one(&io).outcome);
        assert_eq!(
            result.expect("read"),
            7,
            "an open handle still reads its file"
        );
        assert_eq!(&buf, b"payload");
        assert!(io.durable_bytes(&path).is_none(), "the name is gone");

        io.submit(vec![Op::Close { tag: Tag(5), file }])
            .expect("submit close");
        done(one(&io).outcome).expect("close");
        assert!(
            lock(&io.shared.state).detached.is_empty(),
            "the last close freed it"
        );
    }

    // a short write persists only a prefix and reports the short count
    #[test]
    fn short_write_persists_prefix() {
        let plan = FaultPlan::new(1).with_fault(1, FaultKind::ShortWrite { written_bytes: 3 });
        let io = SimIo::new(plan);
        let path = path_in(Path::new("/reel"), "segment-0");
        let file = open(&io, &path, true);

        write(&io, 2, file, 0, b"payload");
        assert_eq!(wrote(one(&io).outcome).expect("wrote"), 3);

        io.submit(vec![Op::SyncData { tag: Tag(3), file }])
            .expect("submit sync");
        done(one(&io).outcome).expect("sync");
        assert_eq!(io.durable_bytes(&path).expect("file"), b"pay");
    }

    // a torn write reports full success but only a prefix survives
    #[test]
    fn torn_write_reports_full() {
        let plan = FaultPlan::new(1).with_fault(1, FaultKind::TornWrite { durable_bytes: 4 });
        let io = SimIo::new(plan);
        let path = path_in(Path::new("/reel"), "segment-0");
        let file = open(&io, &path, true);

        write(&io, 2, file, 0, b"payload");
        assert_eq!(wrote(one(&io).outcome).expect("wrote"), 7);

        io.submit(vec![Op::SyncData { tag: Tag(3), file }])
            .expect("submit sync");
        done(one(&io).outcome).expect("sync");
        assert_eq!(io.durable_bytes(&path).expect("file"), b"payl");
    }

    // a lying sync reports success without persisting cached bytes
    #[test]
    fn lying_sync_loses_data() {
        let plan = FaultPlan::new(1).with_fault(2, FaultKind::LyingSync);
        let io = SimIo::new(plan);
        let path = path_in(Path::new("/reel"), "segment-0");
        let file = open(&io, &path, true);

        write(&io, 2, file, 0, b"payload");
        wrote(one(&io).outcome).expect("wrote");
        io.submit(vec![Op::SyncData { tag: Tag(3), file }])
            .expect("submit sync");
        done(one(&io).outcome).expect("lying sync reports ok");

        assert!(io.durable_bytes(&path).expect("file").is_empty());
    }

    // a lying range sync reports success without persisting the range
    #[test]
    fn lying_sync_range_loses_data() {
        let plan = FaultPlan::new(1).with_fault(2, FaultKind::LyingSyncRange);
        let io = SimIo::new(plan);
        let path = path_in(Path::new("/reel"), "segment-0");
        let file = open(&io, &path, true);

        write(&io, 2, file, 0, b"payload");
        wrote(one(&io).outcome).expect("wrote");
        io.submit(vec![Op::SyncRange {
            tag: Tag(3),
            file,
            offset: 0,
            len: 7,
            mode: SyncRangeMode::WaitBeforeWriteWaitAfter,
        }])
        .expect("submit sync range");
        done(one(&io).outcome).expect("lying range sync reports ok");

        assert!(io.durable_bytes(&path).expect("file").is_empty());
    }

    // running out of space fails the append
    #[test]
    fn enospc_on_append() {
        let plan = FaultPlan::new(1).with_fault(1, FaultKind::EnospcAppend);
        let io = SimIo::new(plan);
        let path = path_in(Path::new("/reel"), "segment-0");
        let file = open(&io, &path, true);

        write(&io, 2, file, 0, b"payload");
        assert!(wrote(one(&io).outcome).is_err());
    }

    // running out of space fails the reservation
    #[test]
    fn enospc_on_allocate() {
        let plan = FaultPlan::new(1).with_fault(1, FaultKind::EnospcAllocate);
        let io = SimIo::new(plan);
        let path = path_in(Path::new("/reel"), "segment-0");
        let file = open(&io, &path, true);

        io.submit(vec![Op::Allocate {
            tag: Tag(2),
            file,
            offset: 0,
            len: 4096,
        }])
        .expect("submit allocate");
        assert!(done(one(&io).outcome).is_err());
    }

    // a bit flip corrupts a stored byte
    #[test]
    fn bit_flip_corrupts() {
        let plan = FaultPlan::new(1).with_fault(1, FaultKind::BitFlip { at_byte: 0, bit: 0 });
        let io = SimIo::new(plan);
        let path = path_in(Path::new("/reel"), "segment-0");
        let file = open(&io, &path, true);

        write(&io, 2, file, 0, b"payload");
        wrote(one(&io).outcome).expect("wrote");

        io.submit(vec![Op::Pread {
            tag: Tag(3),
            file,
            offset: 0,
            buf: ReadBuf::new(7),
        }])
        .expect("submit read");
        let (_, buf) = read_bytes(one(&io).outcome);
        assert_ne!(&buf, b"payload");
        assert_eq!(buf[0], b'p' ^ 1);
    }

    // a sync that reports an input output error fails
    #[test]
    fn sync_error_surfaces() {
        let plan = FaultPlan::new(1).with_fault(2, FaultKind::SyncError);
        let io = SimIo::new(plan);
        let path = path_in(Path::new("/reel"), "segment-0");
        let file = open(&io, &path, true);
        write(&io, 2, file, 0, b"payload");
        wrote(one(&io).outcome).expect("wrote");

        io.submit(vec![Op::SyncData { tag: Tag(3), file }])
            .expect("submit sync");
        assert!(done(one(&io).outcome).is_err());
    }

    // directory ops in a batch apply in reverse under the reorder fault
    #[test]
    fn directory_ops_reorder() {
        let root = Path::new("/reel");
        let first = path_in(root, "a");
        let second = path_in(root, "b");
        let io = SimIo::new(FaultPlan::new(1).with_fault(2, FaultKind::ReorderDir));
        open(&io, &first, true);
        open(&io, &second, true);
        let _ = poll_all(&io);

        io.submit(vec![
            Op::Unlink {
                tag: Tag(3),
                path: first,
            },
            Op::Unlink {
                tag: Tag(4),
                path: second,
            },
        ])
        .expect("submit unlinks");
        let _ = poll_all(&io);
        assert_eq!(io.dir_op_order(), vec!["b".to_string(), "a".to_string()]);
    }

    // a crash freezes the durable image at the synced prefix and survives reopen
    #[test]
    fn crash_freezes_durable_prefix() {
        let io = SimIo::new(FaultPlan::new(1).with_crash(3));
        let path = path_in(Path::new("/reel"), "segment-0");
        let file = open(&io, &path, true);

        io.submit(vec![
            Op::Writev {
                tag: Tag(2),
                file,
                offset: 0,
                bufs: vec![WriteBuf::owned(b"aaaa".to_vec())],
            },
            Op::SyncData { tag: Tag(3), file },
            Op::Writev {
                tag: Tag(4),
                file,
                offset: 4,
                bufs: vec![WriteBuf::owned(b"bbbb".to_vec())],
            },
        ])
        .expect("submit batch");
        assert!(io.is_crashed());
        assert_eq!(io.durable_bytes(&path).expect("file"), b"aaaa");

        let reopened = SimIo::from_image(io.durable_image());
        assert_eq!(reopened.durable_bytes(&path).expect("file"), b"aaaa");
    }

    // a dropped completion never reaches poll
    #[test]
    fn completion_dropped() {
        let io = SimIo::new(FaultPlan::new(1).with_fault(1, FaultKind::DropCompletion));
        let path = path_in(Path::new("/reel"), "segment-0");
        let file = open(&io, &path, true);

        io.submit(vec![Op::SyncData { tag: Tag(2), file }])
            .expect("submit sync");
        assert!(poll_all(&io).is_empty());
    }

    // a delayed completion is withheld until its poll count elapses
    #[test]
    fn completion_delayed() {
        let io =
            SimIo::new(FaultPlan::new(1).with_fault(1, FaultKind::DelayCompletion { polls: 2 }));
        let path = path_in(Path::new("/reel"), "segment-0");
        let file = open(&io, &path, true);

        io.submit(vec![Op::SyncData { tag: Tag(2), file }])
            .expect("submit sync");
        assert!(poll_all(&io).is_empty());
        let released = poll_all(&io);
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].tag, Tag(2));
    }

    // reordered completions drain in reverse of their submit order
    #[test]
    fn completions_reorder() {
        let root = Path::new("/reel");
        let io = SimIo::new(FaultPlan::new(1).with_reorder());
        open(&io, &path_in(root, "segment-0"), true);
        let _ = poll_all(&io);

        io.submit(vec![
            Op::SyncDir {
                tag: Tag(2),
                dir: root.to_path_buf(),
            },
            Op::SyncDir {
                tag: Tag(3),
                dir: root.to_path_buf(),
            },
        ])
        .expect("submit dir syncs");
        let out = poll_all(&io);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].tag, Tag(3));
        assert_eq!(out[1].tag, Tag(2));
    }

    // advise is a no-op that leaves a recorded trace
    #[test]
    fn advise_recorded() {
        let io = SimIo::new(FaultPlan::new(1));
        let path = path_in(Path::new("/reel"), "segment-0");
        let file = open(&io, &path, true);

        io.submit(vec![Op::Advise {
            tag: Tag(2),
            file,
            offset: 0,
            len: 4096,
            advice: Advice::DontNeed,
        }])
        .expect("submit advise");
        done(one(&io).outcome).expect("advise");

        let traces = io.advise_traces();
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].advice, Advice::DontNeed);
    }

    // the same seed and op stream yield the same durable image
    #[test]
    fn same_seed_same_image() {
        let stream = |io: &SimIo| {
            let path = path_in(Path::new("/reel"), "segment-0");
            let file = open(io, &path, true);
            write(io, 2, file, 0, b"payload");
            let _ = poll_all(io);
            io.submit(vec![Op::SyncData { tag: Tag(3), file }])
                .expect("submit sync");
            let _ = poll_all(io);
        };

        let left = SimIo::new(FaultPlan::from_seed(4242));
        let right = SimIo::new(FaultPlan::from_seed(4242));
        stream(&left);
        stream(&right);
        assert_eq!(left.durable_image(), right.durable_image());
    }

    // a simulator keeps the seed and plan it was built from
    #[test]
    fn keeps_plan() {
        let io = SimIo::new(FaultPlan::new(42));

        assert_eq!(io.seed(), 42);
        assert_eq!(io.plan().seed, 42);
    }

    // the op count moves once per executed op
    #[test]
    fn the_op_count_moves_per_op() {
        let io = SimIo::new(FaultPlan::new(1));
        let path = path_in(Path::new("/reel"), "segment-0");
        assert_eq!(io.ops(), 0, "a fresh device has run nothing");

        open(&io, &path, true);
        assert_eq!(io.ops(), 1, "the open is one op");
    }
}
