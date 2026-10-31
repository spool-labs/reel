//! The journal: one checksummed group of rows per write, read back until the first torn group
//! A cut mark says every row at or below its sequence number is in its journal

use crate::format::column::{ColumnId, KeyBytes, RecordKey};
use crate::format::lsn::Lsn;
use crate::format::record::{
    checksum, read_u16_le, read_u32_le, read_u64_le, Flags, RecordHeader, BLOCK, HEADER_LEN,
};
use crate::format::segment_header::SegmentHeader;

/// A group opens with its row count and the byte length of its rows
const GROUP_HEAD: usize = 4 + 4;

/// A group closes with a checksum over its head and rows
const GROUP_TAIL: usize = 4;

/// A row takes these bytes besides its key: column, key length, sequence, offset, length and flags
const ROW_FIXED: usize = 1 + 2 + 8 + 4 + 4 + 1;

/// The end length a range tombstone stores when its range runs to its column's end
const NO_END: u16 = u16::MAX;

/// A cut mark opens with this tag, so zeros and rows never read as one
const CUT_MAGIC: [u8; 4] = *b"RCUT";

/// A cut mark holds its tag, the sequence number, and a CRC32C over both
pub const CUT_MARK_LEN: usize = 4 + 8 + 4;

/// The file at the reel's root that holds the reel's cut mark
pub const CUT_FILE: &str = "reel.cut";

/// One record as its segment's journal holds it
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalRow {
    pub key: RecordKey,
    pub lsn: Lsn,
    pub offset: u32,
    pub len: u32,
    pub flags: Flags,
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

/// Return the rows region of a segment file and its offset, or nothing if the file ends before it
pub fn rows_region(segment: &[u8]) -> Option<(u64, &[u8])> {
    let head = RecordHeader::unpack(segment.get(..HEADER_LEN)?).ok()?;
    let payload = segment.get(HEADER_LEN..HEADER_LEN + head.length as usize)?;
    let rows_at = SegmentHeader::unpack(payload).ok()?.rows_at;
    let rows = segment.get(rows_at as usize..).filter(|_| rows_at > 0)?;
    Some((rows_at, rows))
}

/// Every whole group at the front of a journal, and the bytes they take
pub fn read_groups(bytes: &[u8]) -> (Vec<Vec<JournalRow>>, usize) {
    read_groups_through(bytes, Lsn(u64::MAX))
}

/// The whole groups at the front of a journal up to the first with a row past the cut
pub fn read_groups_through(bytes: &[u8], cut: Lsn) -> (Vec<Vec<JournalRow>>, usize) {
    let mut groups = Vec::new();
    let mut at = 0usize;
    // Nothing past a torn group landed whole, so the read stops there
    loop {
        if let Some((rows, next)) = read_group(bytes, at) {
            // Only a write-out that never finished wrote this group and the ones after it
            if rows.iter().any(|row| row.lsn > cut) {
                return (groups, at);
            }
            groups.push(rows);
            at = next;
            continue;
        }
        // A whole-block volume pads each write with zeros, so skip to the next block boundary
        let boundary = (at as u64).next_multiple_of(BLOCK) as usize;
        let is_padding = boundary > at
            && bytes
                .get(at..boundary)
                .is_some_and(|pad| pad.iter().all(|byte| *byte == 0));
        match is_padding && read_group(bytes, boundary).is_some() {
            true => at = boundary,
            false => return (groups, at),
        }
    }
}

/// Where a segment keeps its copy of the reel's cut, the block just before its rows
pub fn cut_slot(rows_at: u64) -> u64 {
    rows_at.saturating_sub(BLOCK)
}

/// The cut mark for a sequence number
pub fn pack_cut(cut: Lsn) -> [u8; CUT_MARK_LEN] {
    let mut mark = [0u8; CUT_MARK_LEN];
    mark[..4].copy_from_slice(&CUT_MAGIC);
    mark[4..12].copy_from_slice(&cut.pack());
    let crc = checksum(&mark[..12]);
    mark[12..].copy_from_slice(&crc.to_le_bytes());
    mark
}

/// The sequence number a cut mark holds, or nothing when the bytes are not a whole mark
pub fn read_cut(bytes: &[u8]) -> Option<Lsn> {
    let mark = bytes.get(..CUT_MARK_LEN)?;
    let is_whole = mark[..4] == CUT_MAGIC && checksum(&mark[..12]) == read_u32_le(&mark[12..]);
    is_whole.then(|| Lsn(read_u64_le(&mark[4..12])))
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
            range_end: flags
                .is_range_tombstone()
                .then(|| KeyBytes::new(&[byte + 1; 12]).expect("end")),
        }
    }

    // groups of rows read back as they went in, a range end with its range
    #[test]
    fn groups_read_back() {
        let first = vec![row(1, 10, Flags::DATA)];
        let second = vec![
            row(2, 11, Flags::TOMBSTONE),
            row(3, 12, Flags::RANGE_TOMBSTONE),
        ];
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
        push_group(
            &[row(2, 11, Flags::DATA), row(3, 12, Flags::DATA)],
            &mut bytes,
        );

        let (groups, len) = read_groups(&bytes[..bytes.len() - 1]);
        assert_eq!((groups.len(), len), (1, whole));

        let mut flipped = bytes.clone();
        flipped[whole + GROUP_HEAD + 2] ^= 0xff;
        let (groups, len) = read_groups(&flipped);
        assert_eq!((groups.len(), len), (1, whole));

        let (groups, len) = read_groups(&[0u8; 64]);
        assert_eq!((groups.len(), len), (0, 0));
    }

    // the read stops at the first group past the cut, keeping a later group below it out too
    #[test]
    fn the_cut_ends_the_journal() {
        let first = vec![row(1, 10, Flags::DATA)];
        let mut bytes = Vec::new();
        push_group(&first, &mut bytes);
        let kept = bytes.len();
        push_group(&[row(2, 12, Flags::DATA)], &mut bytes);
        push_group(&[row(3, 11, Flags::DATA)], &mut bytes);

        assert_eq!(read_groups_through(&bytes, Lsn(11)), (vec![first], kept));
        assert_eq!(read_groups_through(&bytes, Lsn(12)).0.len(), 3);
    }

    // a cut mark reads back, and zeros or a flipped byte read as none
    #[test]
    fn a_cut_mark_reads_back() {
        let mark = pack_cut(Lsn(77));
        assert_eq!(read_cut(&mark), Some(Lsn(77)));
        assert_eq!(read_cut(&[0u8; CUT_MARK_LEN]), None);
        let mut flipped = mark;
        flipped[6] ^= 1;
        assert_eq!(read_cut(&flipped), None);
        assert_eq!(read_cut(&mark[..CUT_MARK_LEN - 1]), None);
    }

    // zero padding up to a block boundary is skipped
    #[test]
    fn padding_to_a_block_is_stepped_over() {
        let mut bytes = Vec::new();
        push_group(&[row(1, 10, Flags::DATA)], &mut bytes);
        bytes.resize(BLOCK as usize, 0);
        push_group(&[row(2, 11, Flags::DATA)], &mut bytes);
        let whole = bytes.len();
        bytes.resize(2 * BLOCK as usize, 0);

        let (groups, len) = read_groups(&bytes);
        assert_eq!((groups.len(), len), (2, whole));
    }
}
