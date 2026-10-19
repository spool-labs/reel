//! What the volume must hold: values that check themselves, the op log and the prefix check

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

/// Every value starts with this many bytes: column, version, key hash and length
pub const HEADER: usize = 21;

/// A key in one column
pub type Slot = (u8, Vec<u8>);

/// The version and length a key holds
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Held {
    pub version: u64,
    pub len: u32,
}

/// One logged mutation, or a flush or batch edge that returned
#[derive(Clone, Debug)]
pub enum Op {
    Put {
        column: u8,
        key: Vec<u8>,
        version: u64,
        len: u32,
    },
    Delete {
        column: u8,
        key: Vec<u8>,
        version: u64,
    },
    Flush,
    BatchBegin,
    BatchEnd,
}

const PUT: u8 = 1;
const DELETE: u8 = 2;
const FLUSH: u8 = 3;
const BEGIN: u8 = 4;
const END: u8 = 5;

pub fn key_hash(column: u8, key: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64 ^ column as u64;
    for byte in key {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn mix(mut state: u64) -> u64 {
    state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    state = (state ^ (state >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    state = (state ^ (state >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    state ^ (state >> 31)
}

/// The filler byte at one offset, so any window checks on its own
fn filler(seed: u64, at: usize, plain: bool) -> u8 {
    // Plain columns repeat a short pattern so lz4 has something to squeeze
    let block = match plain {
        true => (at % 64) as u64,
        false => (at / 8) as u64,
    };
    (mix(seed ^ block) >> ((at % 8) * 8)) as u8
}

fn u64_at(bytes: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(bytes.get(at..at + 8)?.try_into().ok()?))
}

fn u32_at(bytes: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(bytes.get(at..at + 4)?.try_into().ok()?))
}

fn u16_at(bytes: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(bytes.get(at..at + 2)?.try_into().ok()?))
}

/// A value that says which column, key and version wrote it
pub fn value_of(column: u8, key: &[u8], version: u64, len: u32, plain: bool) -> Vec<u8> {
    let len = (len as usize).max(HEADER);
    let hash = key_hash(column, key);
    let seed = version ^ hash;
    let mut value = Vec::with_capacity(len);
    value.push(column);
    value.extend_from_slice(&version.to_le_bytes());
    value.extend_from_slice(&hash.to_le_bytes());
    value.extend_from_slice(&(len as u32).to_le_bytes());
    value.extend((HEADER..len).map(|at| filler(seed, at, plain)));
    value
}

/// The version a value's header claims, once its column and key check out
pub fn header_of(column: u8, key: &[u8], bytes: &[u8]) -> Result<Held, String> {
    let short = || format!("a header of {} bytes", bytes.len());
    let found = *bytes.first().ok_or_else(short)?;
    let version = u64_at(bytes, 1).ok_or_else(short)?;
    let hash = u64_at(bytes, 9).ok_or_else(short)?;
    let len = u32_at(bytes, 17).ok_or_else(short)?;
    if found != column {
        return Err(format!("column {found} in a value of column {column}"));
    }
    if hash != key_hash(column, key) {
        return Err(format!("version {version} has another key's hash"));
    }
    Ok(Held { version, len })
}

/// Check a window of a value that starts at `offset`
pub fn check_window(
    column: u8,
    key: &[u8],
    held: Held,
    offset: usize,
    window: &[u8],
    plain: bool,
) -> Result<(), String> {
    if offset + window.len() > held.len as usize {
        return Err(format!("a window runs past the {} byte value", held.len));
    }
    let seed = held.version ^ key_hash(column, key);
    for (index, byte) in window.iter().enumerate() {
        let at = offset + index;
        if at >= HEADER && *byte != filler(seed, at, plain) {
            return Err(format!("byte {at} of version {} is wrong", held.version));
        }
    }
    Ok(())
}

/// A worker appends one record before each call, in one write each
pub struct OpLog {
    file: File,
}

impl OpLog {
    pub fn create(path: &Path) -> Result<OpLog, String> {
        let file =
            File::create(path).map_err(|error| format!("create {}: {error}", path.display()))?;
        Ok(OpLog { file })
    }

    pub fn append(&mut self, op: &Op) -> Result<(), String> {
        let mut record = Vec::with_capacity(64);
        match op {
            Op::Put {
                column,
                key,
                version,
                len,
            } => {
                push_slot(&mut record, PUT, *column, key, *version);
                record.extend_from_slice(&len.to_le_bytes());
            }
            Op::Delete {
                column,
                key,
                version,
            } => push_slot(&mut record, DELETE, *column, key, *version),
            Op::Flush => record.push(FLUSH),
            Op::BatchBegin => record.push(BEGIN),
            Op::BatchEnd => record.push(END),
        }
        // A kill keeps every record whose write returned
        self.file
            .write_all(&record)
            .map_err(|error| format!("op log: {error}"))
    }
}

fn push_slot(record: &mut Vec<u8>, kind: u8, column: u8, key: &[u8], version: u64) {
    record.push(kind);
    record.push(column);
    record.extend_from_slice(&(key.len() as u16).to_le_bytes());
    record.extend_from_slice(key);
    record.extend_from_slice(&version.to_le_bytes());
}

/// Every op a log holds, with a torn last record dropped
pub fn read_log(path: &Path) -> Result<Vec<Op>, String> {
    let mut bytes = Vec::new();
    File::open(path)
        .and_then(|mut file| file.read_to_end(&mut bytes))
        .map_err(|error| format!("read {}: {error}", path.display()))?;
    let mut ops = Vec::new();
    let mut at = 0usize;
    while at < bytes.len() {
        match read_op(&bytes, &mut at)? {
            Some(op) => ops.push(op),
            None => break,
        }
    }
    Ok(ops)
}

/// The op at `at`, or nothing when the log ends partway through it
fn read_op(bytes: &[u8], at: &mut usize) -> Result<Option<Op>, String> {
    let kind = bytes[*at];
    *at += 1;
    match kind {
        PUT | DELETE => {
            let Some(column) = bytes.get(*at).copied() else {
                return Ok(None);
            };
            let Some(key_len) = u16_at(bytes, *at + 1) else {
                return Ok(None);
            };
            let start = *at + 3;
            let Some(key) = bytes.get(start..start + key_len as usize) else {
                return Ok(None);
            };
            let Some(version) = u64_at(bytes, start + key_len as usize) else {
                return Ok(None);
            };
            *at = start + key_len as usize + 8;
            let key = key.to_vec();
            if kind == DELETE {
                return Ok(Some(Op::Delete {
                    column,
                    key,
                    version,
                }));
            }
            let Some(len) = u32_at(bytes, *at) else {
                return Ok(None);
            };
            *at += 4;
            Ok(Some(Op::Put {
                column,
                key,
                version,
                len,
            }))
        }
        FLUSH => Ok(Some(Op::Flush)),
        BEGIN => Ok(Some(Op::BatchBegin)),
        END => Ok(Some(Op::BatchEnd)),
        other => Err(format!("op kind {other} at byte {}", *at - 1)),
    }
}

/// The state a life starts from, and the version its first op takes
#[derive(Clone, Default)]
pub struct Baseline {
    pub held: HashMap<Slot, Held>,
    pub next_version: u64,
}

impl Baseline {
    pub fn save(&self, path: &Path) -> Result<(), String> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.next_version.to_le_bytes());
        bytes.extend_from_slice(&(self.held.len() as u64).to_le_bytes());
        for ((column, key), held) in &self.held {
            bytes.push(*column);
            bytes.extend_from_slice(&(key.len() as u16).to_le_bytes());
            bytes.extend_from_slice(key);
            bytes.extend_from_slice(&held.version.to_le_bytes());
            bytes.extend_from_slice(&held.len.to_le_bytes());
        }
        let staged = path.with_extension("part");
        std::fs::write(&staged, &bytes)
            .and_then(|()| std::fs::rename(&staged, path))
            .map_err(|error| format!("save {}: {error}", path.display()))
    }

    pub fn load(path: &Path) -> Result<Baseline, String> {
        if !path.exists() {
            return Ok(Baseline::default());
        }
        let bytes =
            std::fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?;
        let torn = || format!("{} ends partway through an entry", path.display());
        let next_version = u64_at(&bytes, 0).ok_or_else(torn)?;
        let count = u64_at(&bytes, 8).ok_or_else(torn)? as usize;
        let mut held = HashMap::with_capacity(count);
        let mut at = 16;
        for _ in 0..count {
            let column = *bytes.get(at).ok_or_else(torn)?;
            let key_len = u16_at(&bytes, at + 1).ok_or_else(torn)? as usize;
            let key = bytes
                .get(at + 3..at + 3 + key_len)
                .ok_or_else(torn)?
                .to_vec();
            at += 3 + key_len;
            let version = u64_at(&bytes, at).ok_or_else(torn)?;
            let len = u32_at(&bytes, at + 8).ok_or_else(torn)?;
            at += 12;
            held.insert((column, key), Held { version, len });
        }
        Ok(Baseline { held, next_version })
    }
}

/// How a reopened volume compares with a baseline and the ops a life logged
pub struct Verdict {
    pub kept: usize,
    pub logged: usize,
    pub next: Baseline,
}

/// Ranges of cut points, where a cut at `p` means the first `p` mutations landed
fn intersect(left: &[(usize, usize)], right: &[(usize, usize)]) -> Vec<(usize, usize)> {
    let mut both = Vec::new();
    for (left_low, left_high) in left {
        for (right_low, right_high) in right {
            let low = (*left_low).max(*right_low);
            let high = (*left_high).min(*right_high);
            if low <= high {
                both.push((low, high));
            }
        }
    }
    both
}

/// The mutations a life logged, the cut floor its last flush set, and the batches
struct Logged<'a> {
    mutations: Vec<&'a Op>,
    floor: usize,
    batches: Vec<(usize, usize)>,
}

fn sort_log(ops: &[Op]) -> Logged<'_> {
    let mut logged = Logged {
        mutations: Vec::new(),
        floor: 0,
        batches: Vec::new(),
    };
    let mut open = None;
    for op in ops {
        match op {
            Op::Put { .. } | Op::Delete { .. } => logged.mutations.push(op),
            Op::Flush => logged.floor = logged.mutations.len(),
            Op::BatchBegin => open = Some(logged.mutations.len()),
            Op::BatchEnd => {
                if let Some(start) = open.take() {
                    logged.batches.push((start, logged.mutations.len()));
                }
            }
        }
    }
    logged
}

/// What a mutation leaves its key holding
fn effect(op: &Op) -> Option<(Slot, Option<Held>)> {
    match op {
        Op::Put {
            column,
            key,
            version,
            len,
        } => Some((
            (*column, key.clone()),
            Some(Held {
                version: *version,
                len: *len,
            }),
        )),
        Op::Delete { column, key, .. } => Some(((*column, key.clone()), None)),
        Op::Flush | Op::BatchBegin | Op::BatchEnd => None,
    }
}

fn version_of(op: &Op) -> Option<u64> {
    match op {
        Op::Put { version, .. } | Op::Delete { version, .. } => Some(*version),
        Op::Flush | Op::BatchBegin | Op::BatchEnd => None,
    }
}

/// Find a prefix of the logged ops that explains what the volume holds
///
/// With `exact` the whole log must have landed, as after a clean close.
pub fn check_prefix(
    base: &Baseline,
    ops: &[Op],
    observed: &HashMap<Slot, Held>,
    exact: bool,
) -> Result<Verdict, String> {
    let logged = sort_log(ops);
    let count = logged.mutations.len();
    let low = match exact {
        true => count,
        false => logged.floor,
    };
    let mut cuts = vec![(low, count)];

    let mut history: HashMap<Slot, Vec<(usize, Option<Held>)>> = HashMap::new();
    for (index, op) in logged.mutations.iter().enumerate() {
        if let Some((slot, after)) = effect(op) {
            history.entry(slot).or_default().push((index, after));
        }
    }
    check_untouched(base, &history, observed)?;

    for (slot, steps) in &history {
        let seen = observed.get(slot).copied();
        let mut allowed = Vec::new();
        if base.held.get(slot).copied() == seen {
            allowed.push((0, steps[0].0));
        }
        for (at, (index, after)) in steps.iter().enumerate() {
            let until = steps.get(at + 1).map_or(count, |next| next.0);
            if *after == seen {
                allowed.push((index + 1, until));
            }
        }
        cuts = intersect(&cuts, &allowed);
        if cuts.is_empty() {
            return Err(no_prefix(slot, seen, steps, count, logged.floor));
        }
    }

    // A cut inside a batch splits writes that landed together
    let inside = |cut: usize| {
        logged
            .batches
            .iter()
            .any(|(start, end)| *start < cut && cut < *end)
    };
    let kept = cuts
        .iter()
        .filter_map(|(low, high)| (*low..=*high).rev().find(|cut| !inside(*cut)))
        .max()
        .ok_or_else(|| "every prefix that fits cuts a batch in two".to_string())?;

    let mut next = Baseline {
        held: base.held.clone(),
        next_version: base.next_version,
    };
    for op in logged.mutations.iter().take(kept) {
        match effect(op) {
            Some((slot, Some(held))) => {
                next.held.insert(slot, held);
            }
            Some((slot, None)) => {
                next.held.remove(&slot);
            }
            None => {}
        }
    }
    let highest = logged
        .mutations
        .iter()
        .filter_map(|op| version_of(op))
        .max();
    next.next_version = highest.map_or(base.next_version, |version| {
        (version + 1).max(base.next_version)
    });
    Ok(Verdict {
        kept,
        logged: count,
        next,
    })
}

/// Keys no op touched hold what the baseline holds, and nothing else appears
fn check_untouched(
    base: &Baseline,
    history: &HashMap<Slot, Vec<(usize, Option<Held>)>>,
    observed: &HashMap<Slot, Held>,
) -> Result<(), String> {
    for (slot, held) in &base.held {
        if history.contains_key(slot) {
            continue;
        }
        match observed.get(slot) {
            Some(seen) if seen == held => {}
            Some(seen) => {
                return Err(format!(
                    "{} holds version {} where an earlier life left version {}",
                    show(slot),
                    seen.version,
                    held.version
                ))
            }
            None => {
                return Err(format!(
                    "{} lost version {} from an earlier life",
                    show(slot),
                    held.version
                ))
            }
        }
    }
    for (slot, seen) in observed {
        if !history.contains_key(slot) && !base.held.contains_key(slot) {
            return Err(format!(
                "{} holds version {}, which no op wrote",
                show(slot),
                seen.version
            ));
        }
    }
    Ok(())
}

fn no_prefix(
    slot: &Slot,
    seen: Option<Held>,
    steps: &[(usize, Option<Held>)],
    count: usize,
    floor: usize,
) -> String {
    let shown = match seen {
        Some(held) => format!("version {}", held.version),
        None => "nothing".to_string(),
    };
    let steps: Vec<(usize, Option<u64>)> = steps
        .iter()
        .map(|(index, after)| (*index, after.map(|held| held.version)))
        .collect();
    format!("no prefix of the {count} logged ops (flushed through {floor}) explains {} holding {shown}, its ops {steps:?}", show(slot))
}

pub fn show(slot: &Slot) -> String {
    let hex: String = slot
        .1
        .iter()
        .take(12)
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("column {} key {hex}", slot.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(key: u8, version: u64) -> Op {
        Op::Put {
            column: 1,
            key: vec![key],
            version,
            len: 30,
        }
    }

    fn delete(key: u8, version: u64) -> Op {
        Op::Delete {
            column: 1,
            key: vec![key],
            version,
        }
    }

    fn seen(pairs: &[(u8, u64)]) -> HashMap<Slot, Held> {
        pairs
            .iter()
            .map(|(key, version)| {
                (
                    (1, vec![*key]),
                    Held {
                        version: *version,
                        len: 30,
                    },
                )
            })
            .collect()
    }

    // a kill that loses the last ops keeps a prefix
    #[test]
    fn suffix_lost() {
        let ops = vec![put(1, 0), put(2, 1), delete(1, 2)];
        let verdict = check_prefix(&Baseline::default(), &ops, &seen(&[(1, 0), (2, 1)]), false)
            .expect("a prefix fits");
        assert_eq!(verdict.kept, 2);
    }

    // a later write kept while an earlier delete is lost fits no prefix
    #[test]
    fn order_broken() {
        let ops = vec![put(1, 0), put(9, 1), delete(1, 2), put(2, 3)];
        let verdict = check_prefix(
            &Baseline::default(),
            &ops,
            &seen(&[(1, 0), (9, 1), (2, 3)]),
            false,
        );
        assert!(verdict.is_err());
    }

    // a flush sets a floor under what a kill may take
    #[test]
    fn flush_floor() {
        let ops = vec![put(1, 0), Op::Flush, put(2, 1)];
        assert!(check_prefix(&Baseline::default(), &ops, &seen(&[]), false).is_err());
    }

    // a clean close keeps every op
    #[test]
    fn exact_close() {
        let ops = vec![put(1, 0), put(2, 1)];
        assert!(check_prefix(&Baseline::default(), &ops, &seen(&[(1, 0)]), true).is_err());
    }

    // a value checks whole and window by window
    #[test]
    fn values_check() {
        let value = value_of(14, b"key", 7, 5000, false);
        let held = header_of(14, b"key", &value).expect("a header");
        assert_eq!(
            held,
            Held {
                version: 7,
                len: 5000
            }
        );
        check_window(14, b"key", held, 100, &value[100..900], false).expect("the window matches");
        assert!(header_of(14, b"other", &value).is_err());
    }
}
