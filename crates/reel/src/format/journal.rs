//! The rows an open segment journals beside itself, so a reopen finds the records no footer lists yet
//!
//! A keyless record carries no key, so until its segment's footer is down this file is
//! the only place that says which key each record holds. Every write a tail makes adds
//! one group: the rows of the records that write put down, a batch's rows together. A
//! group is its row count, the byte length of its rows, the rows, then a crc32c over all
//! of it, so a group a crash cut short fails its check and the journal ends there. The
//! seal writes the footer and unlinks the journal.

use std::path::{Path, PathBuf};

use crate::format::column::{ColumnId, KeyBytes, RecordKey};
use crate::format::lsn::Lsn;
use crate::format::record::{checksum, read_u16_le, read_u32_le, read_u64_le, Flags};

/// What a journal's file name ends in, which no segment scan takes for a segment
pub const JOURNAL_SUFFIX: &str = ".rows";

/// Where a segment's journal sits: beside it, under its number
pub fn journal_path(segment_path: &Path) -> PathBuf {
    segment_path.with_extension(&JOURNAL_SUFFIX[1..])
}

/// Bytes ahead of a group's rows: the row count and the rows' byte length
const GROUP_HEAD: usize = 4 + 4;

/// Bytes behind a group's rows: the checksum over the head and the rows
const GROUP_TAIL: usize = 4;

/// Bytes a row takes past its key: column, key length, sequence, offset, length and flags
const ROW_FIXED: usize = 1 + 2 + 8 + 4 + 4 + 1;

/// The end length a range tombstone stores when its range runs to its column's end
const NO_END: u16 = u16::MAX;

/// One record as its segment's journal holds it
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalRow {
    pub key: RecordKey,
    pub lsn: Lsn,
    pub offset: u32,
    pub len: u32,
    pub flags: Flags,

    /// Exclusive end of a range tombstone's range, nothing where it runs to the column's end
    pub range_end: Option<KeyBytes>,
}

/// Append one group of rows to a buffer
pub fn push_group<'row>(rows: impl IntoIterator<Item = &'row JournalRow>, out: &mut Vec<u8>) {
    let start = out.len();
    out.extend_from_slice(&[0u8; GROUP_HEAD]);
    let mut count = 0u32;
    for row in rows {
        count += 1;
        push_row(row, out);
    }
    let body = (out.len() - start - GROUP_HEAD) as u32;
    out[start..start + 4].copy_from_slice(&count.to_le_bytes());
    out[start + 4..start + GROUP_HEAD].copy_from_slice(&body.to_le_bytes());
    let crc = checksum(&out[start..]);
    out.extend_from_slice(&crc.to_le_bytes());
}

fn push_row(row: &JournalRow, out: &mut Vec<u8>) {
    let key = row.key.as_slice();
    out.push(row.key.column.as_u8());
    out.extend_from_slice(&(key.len() as u16).to_le_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&row.lsn.as_u64().to_le_bytes());
    out.extend_from_slice(&row.offset.to_le_bytes());
    out.extend_from_slice(&row.len.to_le_bytes());
    out.push(row.flags.bits());
    if row.flags.is_range_tombstone() {
        match &row.range_end {
            Some(end) => {
                out.extend_from_slice(&(end.as_slice().len() as u16).to_le_bytes());
                out.extend_from_slice(end.as_slice());
            }
            None => out.extend_from_slice(&NO_END.to_le_bytes()),
        }
    }
}

/// Every whole group at the front of a journal, and the bytes they take
///
/// The read stops at the first group that is cut short or fails its check, since
/// nothing past a torn group was written after it in one piece.
pub fn read_groups(bytes: &[u8]) -> (Vec<Vec<JournalRow>>, usize) {
    let mut groups = Vec::new();
    let mut at = 0usize;
    while let Some((rows, next)) = read_group(bytes, at) {
        groups.push(rows);
        at = next;
    }
    (groups, at)
}

fn read_group(bytes: &[u8], at: usize) -> Option<(Vec<JournalRow>, usize)> {
    let head = bytes.get(at..at + GROUP_HEAD)?;
    let count = read_u32_le(&head[..4]) as usize;
    let body = read_u32_le(&head[4..]) as usize;
    let end = at.checked_add(GROUP_HEAD + body)?;
    let stored = bytes.get(end..end + GROUP_TAIL)?;
    if count == 0 || checksum(&bytes[at..end]) != read_u32_le(stored) {
        return None;
    }
    let mut rows = Vec::with_capacity(count.min(body / ROW_FIXED));
    let mut cursor = at + GROUP_HEAD;
    for _ in 0..count {
        let (row, next) = read_row(&bytes[..end], cursor)?;
        rows.push(row);
        cursor = next;
    }
    (cursor == end).then_some((rows, end + GROUP_TAIL))
}

fn read_row(bytes: &[u8], at: usize) -> Option<(JournalRow, usize)> {
    let column = ColumnId(*bytes.get(at)?);
    let width = read_u16_le(bytes.get(at + 1..at + 3)?) as usize;
    let key_at = at + 3;
    let key = RecordKey::from_bytes(column, bytes.get(key_at..key_at + width)?).ok()?;
    let fixed = key_at + width;
    let tail = bytes.get(fixed..fixed + ROW_FIXED - 3)?;
    let flags = Flags::from_bits(tail[16]).ok()?;
    let mut next = fixed + ROW_FIXED - 3;
    let range_end = match flags.is_range_tombstone() {
        false => None,
        true => {
            let len = read_u16_le(bytes.get(next..next + 2)?);
            next += 2;
            match len {
                NO_END => None,
                len => {
                    let end = KeyBytes::new(bytes.get(next..next + len as usize)?).ok()?;
                    next += len as usize;
                    Some(end)
                }
            }
        }
    };
    let row = JournalRow {
        key,
        lsn: Lsn(read_u64_le(&tail[..8])),
        offset: read_u32_le(&tail[8..12]),
        len: read_u32_le(&tail[12..16]),
        flags,
        range_end,
    };
    Some((row, next))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(byte: u8, lsn: u64, flags: Flags) -> JournalRow {
        JournalRow {
            key: RecordKey::from_bytes(ColumnId(1), &[byte; 12]).expect("key"),
            lsn: Lsn(lsn),
            offset: 100 * u32::from(byte),
            len: 40,
            flags,
            range_end: flags.is_range_tombstone().then(|| KeyBytes::new(&[byte + 1; 12]).expect("end")),
        }
    }

    // groups of rows read back as they went in, a range end with its range
    #[test]
    fn groups_read_back() {
        let first = vec![row(1, 10, Flags::DATA)];
        let second = vec![row(2, 11, Flags::TOMBSTONE), row(3, 12, Flags::RANGE_TOMBSTONE)];
        let mut bytes = Vec::new();
        push_group(&first, &mut bytes);
        push_group(&second, &mut bytes);

        let (groups, len) = read_groups(&bytes);
        assert_eq!(groups, vec![first, second]);
        assert_eq!(len, bytes.len());
    }

    // a group cut short or flipped ends the journal there, keeping every group before it
    #[test]
    fn a_torn_group_ends_the_journal() {
        let mut bytes = Vec::new();
        push_group(&[row(1, 10, Flags::DATA)], &mut bytes);
        let whole = bytes.len();
        push_group(&[row(2, 11, Flags::DATA), row(3, 12, Flags::DATA)], &mut bytes);

        let (groups, len) = read_groups(&bytes[..bytes.len() - 1]);
        assert_eq!((groups.len(), len), (1, whole));

        let mut flipped = bytes.clone();
        flipped[whole + GROUP_HEAD + 2] ^= 0xff;
        let (groups, len) = read_groups(&flipped);
        assert_eq!((groups.len(), len), (1, whole));

        let (groups, len) = read_groups(&[0u8; 64]);
        assert_eq!((groups.len(), len), (0, 0));
    }
}
