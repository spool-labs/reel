//! On-disk record header, its control flags, and the checksum primitives
//!
//! A record is a fixed header, then its key, then its payload. The header says
//! which column the record belongs to and how wide its key is, so every column
//! stores keys at its own width and a walk still finds the next record without
//! parsing a variable-length header.

use std::sync::Arc;

use crc_fast::{CrcAlgorithm, Digest};

use crate::error::{ReelError, Result};
use crate::format::column::{ColumnId, RecordKey, INLINE_KEY_LEN, MAX_KEY_LEN};
use crate::format::lsn::Lsn;

/// Fixed size of a record header in bytes
///
/// The key width takes two bytes rather than one, because a key runs to
/// `MAX_KEY_LEN` and one byte cannot say more than 255.
pub const HEADER_LEN: usize = 21;

/// Bytes a record's header and an inline key take together
///
/// Sized by the inline key bound and not by the format's key ceiling, since it is
/// a stack buffer built per record. A wider key is written from a borrowed slice
/// rather than staged here.
pub const PREFIX_CAP: usize = HEADER_LEN + INLINE_KEY_LEN;

/// Alignment boundary every group commit drain starts on
pub const BLOCK: u64 = 4096;

const OFFSET_LENGTH: usize = 0;
const OFFSET_CRC: usize = 4;
const OFFSET_LSN: usize = 8;
const OFFSET_FLAGS: usize = 16;
const OFFSET_COLUMN: usize = 17;
const OFFSET_KEY_WIDTH: usize = 18;
const OFFSET_CODEC: usize = 20;

/// Bits saying what kind of record this is, the low five
const KIND_MASK: u8 = 0b0001_1111;

const FLAG_TOMBSTONE: u8 = 0b0000_0001;
const FLAG_RANGE_TOMBSTONE: u8 = 0b0000_0010;
const FLAG_PAD: u8 = 0b0000_0100;
const FLAG_SEGMENT_HEADER: u8 = 0b0000_1000;
const FLAG_BATCH_FRAME: u8 = 0b0001_0000;
const FLAG_BATCHED: u8 = 0b0010_0000;
const FLAG_RELOCATED: u8 = 0b0100_0000;

/// The kinds nothing resolves by key, which a mark never rides on
const CONTROL_MASK: u8 = FLAG_PAD | FLAG_SEGMENT_HEADER | FLAG_BATCH_FRAME;

/// The marks that ride along with a kind rather than being one
const MARK_MASK: u8 = FLAG_BATCHED | FLAG_RELOCATED;

/// Every bit a writer sets, so anything else is a torn or foreign header
const KNOWN_MASK: u8 = KIND_MASK | MARK_MASK;

/// The checksum every record and footer is covered by
///
/// Part of the on-disk format: a stored value only reproduces under the same
/// algorithm, so changing it makes every segment already written unreadable.
const CRC: CrcAlgorithm = CrcAlgorithm::Crc32Iscsi;

/// A prefix has to fit the inline write buffer, or every record would allocate
const _: () = assert!(PREFIX_CAP <= crate::io::op::INLINE_CAP);

/// A batch frame stages its declaration where a key would, so it has to fit there
const _: () = assert!(HEADER_LEN + FRAME_PAYLOAD_LEN <= PREFIX_CAP);

/// Read a little endian word from an exact four byte slice
pub fn read_u32_le(bytes: &[u8]) -> u32 {
    let mut buf = [0u8; std::mem::size_of::<u32>()];
    buf.copy_from_slice(bytes);
    u32::from_le_bytes(buf)
}

/// Read a little endian word from an exact two byte slice
pub fn read_u16_le(bytes: &[u8]) -> u16 {
    let mut buf = [0u8; std::mem::size_of::<u16>()];
    buf.copy_from_slice(bytes);
    u16::from_le_bytes(buf)
}

/// Read a little endian word from an exact eight byte slice
pub fn read_u64_le(bytes: &[u8]) -> u64 {
    let mut buf = [0u8; std::mem::size_of::<u64>()];
    buf.copy_from_slice(bytes);
    u64::from_le_bytes(buf)
}

/// Checksum a byte range with the record and footer algorithm
pub fn checksum(bytes: &[u8]) -> u32 {
    crc_fast::checksum(CRC, bytes) as u32
}

/// The same checksum, fed a piece at a time
///
/// For a caller whose bytes are not one range: a record covers its header, its
/// key and its payload, which never exist in one buffer.
pub fn digest() -> Digest {
    Digest::new(CRC)
}

/// The control bits a record header carries
///
/// The low five bits say what kind of record it is and are exclusive; the two
/// above them say how it was committed and ride along with a data record or a
/// tombstone.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Flags(u8);

impl Flags {
    /// A plain data record with a payload
    pub const DATA: Flags = Flags(0);

    /// A delete marker for one key, with no payload
    pub const TOMBSTONE: Flags = Flags(FLAG_TOMBSTONE);

    /// A delete marker for a range of keys, its payload the exclusive end
    pub const RANGE_TOMBSTONE: Flags = Flags(FLAG_RANGE_TOMBSTONE);

    /// A drain aligning filler record with no payload
    pub const PAD: Flags = Flags(FLAG_PAD);

    /// The first record of a segment, carrying its self describing payload
    pub const SEGMENT_HEADER: Flags = Flags(FLAG_SEGMENT_HEADER);

    /// The record that opens a batch, its payload the run it declares
    pub const BATCH_FRAME: Flags = Flags(FLAG_BATCH_FRAME);

    /// The same record written as part of a batch
    pub fn batched(self) -> Flags {
        Flags(self.0 | FLAG_BATCHED)
    }

    /// The same record written again elsewhere by compaction
    ///
    /// A copy carries the sequence number of the record it copied, so without
    /// this nothing tells it apart from a write that lost an ordering race.
    pub fn relocated(self) -> Flags {
        Flags(self.0 | FLAG_RELOCATED)
    }

    /// Whether no kind bit is set, the mark of a data record
    pub fn is_data(self) -> bool {
        self.0 & KIND_MASK == 0
    }

    /// Whether the tombstone bit is set
    pub fn is_tombstone(self) -> bool {
        self.0 & FLAG_TOMBSTONE != 0
    }

    /// Whether the range tombstone bit is set
    pub fn is_range_tombstone(self) -> bool {
        self.0 & FLAG_RANGE_TOMBSTONE != 0
    }

    /// Whether the pad bit is set
    pub fn is_pad(self) -> bool {
        self.0 & FLAG_PAD != 0
    }

    /// Whether the segment header bit is set
    pub fn is_segment_header(self) -> bool {
        self.0 & FLAG_SEGMENT_HEADER != 0
    }

    /// Whether this record is the frame a batch opens with
    pub fn is_batch_frame(self) -> bool {
        self.0 & FLAG_BATCH_FRAME != 0
    }

    /// Whether the record went down as part of a batch
    pub fn is_batched(self) -> bool {
        self.0 & FLAG_BATCHED != 0
    }

    /// Whether the record is compaction's copy of one written earlier
    pub fn is_relocated(self) -> bool {
        self.0 & FLAG_RELOCATED != 0
    }

    /// The raw bits for serialization
    pub fn bits(self) -> u8 {
        self.0
    }

    /// Read flags from a header byte, rejecting shapes no writer produces
    ///
    /// The kind bits are exclusive, and the marks ride only on records a batch or
    /// a compaction can contain, which no control record is. A bit outside both
    /// sets was never written by this format.
    pub fn from_bits(byte: u8) -> Result<Flags> {
        if byte & !KNOWN_MASK != 0 {
            return Err(ReelError::Corruption(format!(
                "record header carries a bit no writer sets: {byte:#010b}"
            )));
        }
        let kind = byte & KIND_MASK;
        if kind.count_ones() > 1 {
            return Err(ReelError::Corruption(format!(
                "record header carries more than one kind bit: {byte:#010b}"
            )));
        }
        if kind & CONTROL_MASK != 0 && byte & MARK_MASK != 0 {
            return Err(ReelError::Corruption(format!(
                "record header marks a control record: {byte:#010b}"
            )));
        }
        Ok(Flags(byte))
    }
}

/// Bytes a batch frame declares its run in: the record count then their span
pub const FRAME_PAYLOAD_LEN: usize = 12;

/// What a batch frame says about the run of records behind it
///
/// Both numbers are here because either alone is weaker than the pair. The count says
/// where the run ends in records and the span says where it ends in bytes, so a walk
/// that reaches one without the other is looking at a run that did not land whole.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchFrame {
    /// Records of the batch, which follow the frame with nothing between them
    pub count: u32,

    /// Bytes those records occupy, measured from the end of the frame
    pub span: u64,
}

impl BatchFrame {
    /// Bytes the frame itself occupies ahead of the run it opens
    pub const SPAN: u64 = (HEADER_LEN + FRAME_PAYLOAD_LEN) as u64;

    /// The header the frame writes, checksummed over what it declares
    ///
    /// No key and no sequence number: nothing resolves a frame, and what orders the
    /// batch is the numbers its own records carry.
    pub fn header(&self) -> RecordHeader {
        RecordHeader::new(
            FRAME_PAYLOAD_LEN as u32,
            Lsn::NONE,
            Flags::BATCH_FRAME,
            RecordKey::none(),
            &self.declaration(),
        )
    }

    /// The frame's bytes, its header and its declaration staged together
    ///
    /// The declaration rides in the staging array where a key would, so a frame is one
    /// inline buffer in the batch's write and costs the batch no allocation at all.
    pub fn pack(&self) -> RecordPrefix {
        let mut prefix = self.header().pack();
        let end = prefix.len + FRAME_PAYLOAD_LEN;
        prefix.bytes[prefix.len..end].copy_from_slice(&self.declaration());
        prefix.len = end;
        prefix
    }

    /// The run a frame record declares, or nothing where these bytes are not one
    ///
    /// Every shape no writer produces is refused here rather than trusted: a frame
    /// with a key, one whose payload is the wrong width, one declaring a run of less
    /// than two, and one whose span cannot hold the records it counts.
    pub fn unpack(header: &RecordHeader, payload: &[u8]) -> Option<BatchFrame> {
        if !header.flags.is_batch_frame()
            || header.key.width() != 0
            || header.length as usize != FRAME_PAYLOAD_LEN
            || payload.len() < FRAME_PAYLOAD_LEN
        {
            return None;
        }
        let frame = BatchFrame {
            count: read_u32_le(&payload[..4]),
            span: read_u64_le(&payload[4..FRAME_PAYLOAD_LEN]),
        };
        if frame.count < 2 || frame.span < u64::from(frame.count) * HEADER_LEN as u64 {
            return None;
        }
        Some(frame)
    }

    fn declaration(&self) -> [u8; FRAME_PAYLOAD_LEN] {
        let mut out = [0u8; FRAME_PAYLOAD_LEN];
        out[..4].copy_from_slice(&self.count.to_le_bytes());
        out[4..].copy_from_slice(&self.span.to_le_bytes());
        out
    }
}

/// The bytes a record writes ahead of its payload: its header and then its key
///
/// Carried inline because one is built for every record appended and handed
/// straight to an inline write buffer.
pub struct RecordPrefix {
    /// Staging array for the header and, where it fits, the key
    bytes: [u8; PREFIX_CAP],

    /// Bytes of the staging array that are occupied
    len: usize,

    /// The key when it was too wide to stage, held as a piece for the drain
    tail: Option<Arc<[u8]>>,
}

impl RecordPrefix {
    /// The staged bytes: the header, and the key when it fit beside it
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }

    /// The key that did not fit the staging array, to be written after it
    pub fn tail(&self) -> Option<&[u8]> {
        self.tail.as_deref()
    }

    /// Bytes the prefix occupies on disk, staged and gathered together
    pub fn len(&self) -> usize {
        self.len + self.tail.as_ref().map_or(0, |tail| tail.len())
    }

    /// The staging array, the bytes of it occupied, and any spilled key
    ///
    /// The tail comes out with the head, since a caller taking one and leaving
    /// the other frames a record wider than it writes.
    pub fn into_parts(self) -> ([u8; PREFIX_CAP], usize, Option<Arc<[u8]>>) {
        (self.bytes, self.len, self.tail)
    }

    /// Whether the prefix carries nothing, which no real record produces
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// The fixed size header that precedes every record on disk
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordHeader {
    /// Length of the payload, or pad fill, that follows the key
    pub length: u32,

    /// Checksum over the header with this field zeroed, the key, and the payload
    pub crc: u32,

    /// Append sequence number that orders this record within its reel
    pub lsn: Lsn,

    /// Control bits marking tombstones, pads, and batch membership
    pub flags: Flags,

    /// Column and key this record is addressed by, empty for control records
    pub key: RecordKey,

    /// Codec that produced the stored payload, zero for raw bytes
    pub codec: u8,
}

impl RecordHeader {
    /// A data record header for a payload
    pub fn data(key: RecordKey, lsn: Lsn, payload: &[u8]) -> RecordHeader {
        RecordHeader::new(payload.len() as u32, lsn, Flags::DATA, key, payload)
    }

    /// A tombstone header recording a delete of one key
    pub fn tombstone(key: RecordKey, lsn: Lsn) -> RecordHeader {
        RecordHeader::new(0, lsn, Flags::TOMBSTONE, key, &[])
    }

    /// A tombstone header recording a delete of a half-open key range
    ///
    /// The record's key is the inclusive start of the range and its payload is
    /// the exclusive end. An empty payload means the range has no upper bound.
    pub fn range_tombstone(key: RecordKey, lsn: Lsn, end: &[u8]) -> RecordHeader {
        RecordHeader::new(end.len() as u32, lsn, Flags::RANGE_TOMBSTONE, key, end)
    }

    /// A pad header realigning a tail at a position to the next block boundary
    ///
    /// The header carries the fill length so a scan hops over the gap to the next
    /// aligned record; the fill bytes are not written and not checksummed.
    pub fn pad(position: u64) -> RecordHeader {
        RecordHeader::fill(pad_fill(position))
    }

    /// A pad header spanning an exact fill length
    pub fn fill(length: u32) -> RecordHeader {
        RecordHeader::new(length, Lsn::NONE, Flags::PAD, RecordKey::none(), &[])
    }

    /// A segment header record carrying the frozen self describing payload
    ///
    /// No sequence number and no key, since it is never indexed, but a payload
    /// the checksum covers.
    pub fn segment_header(payload: &[u8]) -> RecordHeader {
        RecordHeader::new(
            payload.len() as u32,
            Lsn::NONE,
            Flags::SEGMENT_HEADER,
            RecordKey::none(),
            payload,
        )
    }

    /// A header of any kind, checksummed over the payload it will carry
    ///
    /// The checksum covers the flags, so a caller needing flags no constructor
    /// above sets builds here rather than remarking afterwards.
    pub fn new(
        length: u32,
        lsn: Lsn,
        flags: Flags,
        key: RecordKey,
        payload: &[u8],
    ) -> RecordHeader {
        RecordHeader::new_coded(length, lsn, flags, key, 0, payload)
    }

    /// A header whose payload a codec produced, checksummed over the stored bytes
    ///
    /// The codec byte is covered by the crc, so it cannot be patched afterwards.
    pub fn new_coded(
        length: u32,
        lsn: Lsn,
        flags: Flags,
        key: RecordKey,
        codec: u8,
        payload: &[u8],
    ) -> RecordHeader {
        let mut header = RecordHeader {
            length,
            crc: 0,
            lsn,
            flags,
            key,
            codec,
        };
        header.crc = header.compute_crc(payload);
        header
    }

    /// Serialize the header and its key to the bytes they take on disk
    pub fn pack(&self) -> RecordPrefix {
        let tail = self.key.key.spilled_bytes();
        let staged = match tail {
            Some(_) => HEADER_LEN,
            None => HEADER_LEN + self.key.width() as usize,
        };
        let mut prefix = RecordPrefix {
            bytes: [0u8; PREFIX_CAP],
            len: staged,
            tail,
        };
        prefix.bytes[..HEADER_LEN].copy_from_slice(&self.fixed());
        prefix.bytes[OFFSET_CRC..OFFSET_LSN].copy_from_slice(&self.crc.to_le_bytes());
        if prefix.tail.is_none() {
            prefix.bytes[HEADER_LEN..staged].copy_from_slice(self.key.as_slice());
        }
        prefix
    }

    /// Parse a header and its key from the bytes that begin a record
    ///
    /// A slice too short for the fixed part or the key it claims is rejected
    /// rather than filled in, so a truncated tail ends a walk.
    pub fn unpack(bytes: &[u8]) -> Result<RecordHeader> {
        if bytes.len() < HEADER_LEN {
            return Err(ReelError::Corruption(
                "record header is shorter than one header".to_string(),
            ));
        }

        let length = read_u32_le(&bytes[OFFSET_LENGTH..OFFSET_CRC]);
        let crc = read_u32_le(&bytes[OFFSET_CRC..OFFSET_LSN]);
        let lsn = Lsn(read_u64_le(&bytes[OFFSET_LSN..OFFSET_FLAGS]));
        let flags = Flags::from_bits(bytes[OFFSET_FLAGS])?;
        let column = ColumnId(bytes[OFFSET_COLUMN]);
        let key_width = read_u16_le(&bytes[OFFSET_KEY_WIDTH..OFFSET_CODEC]) as usize;
        let codec = bytes[OFFSET_CODEC];

        if key_width > MAX_KEY_LEN {
            return Err(ReelError::Corruption(format!(
                "record header claims a key of {key_width} bytes"
            )));
        }
        if bytes.len() < HEADER_LEN + key_width {
            return Err(ReelError::Corruption(
                "record header is shorter than the key it claims".to_string(),
            ));
        }

        let key = RecordKey::from_bytes(column, &bytes[HEADER_LEN..HEADER_LEN + key_width])
            .map_err(|error| ReelError::Corruption(error.to_string()))?;

        Ok(RecordHeader {
            length,
            crc,
            lsn,
            flags,
            key,
            codec,
        })
    }

    /// Recompute the checksum and compare it to the stored one
    ///
    /// For a record with a payload pass the bytes the length describes; the ones
    /// without a payload carry no bytes past their key and ignore the argument.
    pub fn verify(&self, payload: &[u8]) -> bool {
        let covered = if self.has_payload() { payload } else { &[] };
        self.compute_crc(covered) == self.crc
    }

    /// Whether this record carries a payload the length describes
    ///
    /// Data records, range tombstones, segment headers and batch frames carry one;
    /// point tombstones and pads do not.
    pub fn has_payload(&self) -> bool {
        self.flags.is_data()
            || self.flags.is_segment_header()
            || self.flags.is_range_tombstone()
            || self.flags.is_batch_frame()
    }

    /// Whether these bytes are unwritten space rather than a record
    ///
    /// Reserved space reads back as zeros, which parse as a data record with no
    /// sequence number, and every real one draws a sequence number above zero.
    pub fn is_unwritten(&self) -> bool {
        self.flags.is_data() && self.lsn == Lsn::NONE
    }

    /// Bytes the header and its key take together, where the payload begins
    pub fn prefix_len(&self) -> u64 {
        HEADER_LEN as u64 + u64::from(self.key.width())
    }

    /// Total bytes this record spans on disk: header, key, and payload or fill
    pub fn span(&self) -> u64 {
        self.prefix_len() + u64::from(self.length)
    }

    /// Whether the record fits within the bytes remaining in the file
    pub fn fits_within(&self, remaining: u64) -> bool {
        self.span() <= remaining
    }

    /// The fixed part of the header, checksum field left zero
    fn fixed(&self) -> [u8; HEADER_LEN] {
        let mut out = [0u8; HEADER_LEN];
        out[OFFSET_LENGTH..OFFSET_CRC].copy_from_slice(&self.length.to_le_bytes());
        out[OFFSET_LSN..OFFSET_FLAGS].copy_from_slice(&self.lsn.pack());
        out[OFFSET_FLAGS] = self.flags.bits();
        out[OFFSET_COLUMN] = self.key.column.as_u8();
        out[OFFSET_KEY_WIDTH..OFFSET_CODEC].copy_from_slice(&self.key.width().to_le_bytes());
        out[OFFSET_CODEC] = self.codec;
        out
    }

    fn compute_crc(&self, payload: &[u8]) -> u32 {
        let mut digest = Digest::new(CRC);
        digest.update(&self.fixed());
        digest.update(self.key.as_slice());
        digest.update(payload);
        digest.finalize() as u32
    }
}

/// The key width a record claims, read from the fixed part of its header
///
/// A walk needs this before it can parse the record, since the key sits between
/// the fixed header and the payload.
pub fn peek_key_width(bytes: &[u8]) -> Option<usize> {
    let field = bytes.get(OFFSET_KEY_WIDTH..OFFSET_CODEC)?;
    let width = read_u16_le(field) as usize;
    if width > MAX_KEY_LEN {
        return None;
    }
    Some(width)
}

/// The fill length a pad at this position needs to reach the next block boundary
///
/// The pad spans at least a header and lands on a boundary, bumped one extra
/// block when the gap to the next boundary is smaller than a header.
pub fn pad_fill(position: u64) -> u32 {
    let boundary = align_up(position + HEADER_LEN as u64, BLOCK);
    (boundary - position - HEADER_LEN as u64) as u32
}

/// Round a value up to the next multiple of an alignment
pub(crate) fn align_up(value: u64, alignment: u64) -> u64 {
    value.div_ceil(alignment) * alignment
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::format::column::KeyBytes;

    const RECORD: ColumnId = ColumnId(1);
    const BLOB: ColumnId = ColumnId(2);

    fn sample_key(column: ColumnId, byte: u8, width: usize) -> RecordKey {
        RecordKey::from_bytes(column, &vec![byte; width]).expect("key")
    }

    // a known vector pins the checksum entry point
    #[test]
    fn known_checksum() {
        assert_eq!(checksum(b""), 0);
        assert_eq!(checksum(b"123456789"), 0xe306_9283);
    }

    // the staged prefix is sized by the inline key, never by the format's ceiling
    #[test]
    fn header_size() {
        assert_eq!(HEADER_LEN, OFFSET_CODEC + 1);
        assert_eq!(PREFIX_CAP, HEADER_LEN + INLINE_KEY_LEN);
        const {
            assert!(
                PREFIX_CAP < HEADER_LEN + MAX_KEY_LEN,
                "the staged prefix is sized by the key ceiling again",
            )
        };
    }

    // a record spans its header, its key, and its payload
    #[test]
    fn span_covers_key() {
        let header = RecordHeader::data(sample_key(RECORD, 0x11, 34), Lsn(1), &[0xab; 100]);

        assert_eq!(header.prefix_len(), HEADER_LEN as u64 + 34);
        assert_eq!(header.span(), HEADER_LEN as u64 + 34 + 100);
    }

    // a data record round trips and verifies against its payload
    #[test]
    fn data_roundtrip() {
        let payload = vec![0xab; 1600];
        let key = sample_key(RECORD, 0x11, 34);
        let header = RecordHeader::data(key.clone(), Lsn(9), &payload);

        let parsed = RecordHeader::unpack(header.pack().as_slice()).expect("unpack");

        assert_eq!(parsed, header);
        assert!(parsed.verify(&payload));
        assert!(parsed.has_payload());
        assert_eq!(parsed.length, 1600);
        assert_eq!(parsed.key, key);
    }

    // two columns of different key widths both round trip
    #[test]
    fn mixed_widths_roundtrip() {
        for (column, width) in [(RECORD, 34usize), (BLOB, 32), (ColumnId(3), 24)] {
            let key = sample_key(column, 0x5a, width);
            let header = RecordHeader::data(key, Lsn(3), &[0x01; 8]);

            let parsed = RecordHeader::unpack(header.pack().as_slice()).expect("unpack");

            assert_eq!(parsed.key.column, column);
            assert_eq!(parsed.key.width() as usize, width);
            assert_eq!(parsed.pack().len(), HEADER_LEN + width);
        }
    }

    // tombstone and pad records carry no payload
    #[test]
    fn control_records() {
        let tombstone = RecordHeader::tombstone(sample_key(RECORD, 0x22, 34), Lsn(4));
        let pad = RecordHeader::pad(100);

        for header in [tombstone.clone(), pad.clone()] {
            let parsed = RecordHeader::unpack(header.pack().as_slice()).expect("unpack");
            assert_eq!(parsed, header);
            assert!(parsed.verify(&[]));
            assert!(!parsed.has_payload());
        }

        assert!(tombstone.flags.is_tombstone());
        assert!(pad.flags.is_pad());
        assert_eq!(pad.key.width(), 0);
    }

    // a range tombstone carries its exclusive end as its payload
    #[test]
    fn range_tombstone_carries_end() {
        let start = sample_key(RECORD, 0x10, 34);
        let end = vec![0x11u8; 34];
        let header = RecordHeader::range_tombstone(start.clone(), Lsn(6), &end);

        let parsed = RecordHeader::unpack(header.pack().as_slice()).expect("unpack");

        assert!(parsed.flags.is_range_tombstone());
        assert!(parsed.has_payload());
        assert!(parsed.verify(&end));
        assert_eq!(parsed.length as usize, end.len());
        assert_eq!(parsed.key, start);
    }

    // an unbounded range tombstone carries no end at all
    #[test]
    fn unbounded_range_tombstone() {
        let header = RecordHeader::range_tombstone(sample_key(RECORD, 0xff, 34), Lsn(7), &[]);

        let parsed = RecordHeader::unpack(header.pack().as_slice()).expect("unpack");

        assert_eq!(parsed.length, 0);
        assert!(parsed.verify(&[]));
    }

    // a relocation mark rides along with a record's kind without changing it
    #[test]
    fn relocation_mark() {
        let key = sample_key(RECORD, 0x77, 34);
        let payload = vec![0x77; 32];
        let moved = RecordHeader::new(
            payload.len() as u32,
            Lsn(4),
            Flags::DATA.relocated(),
            key.clone(),
            &payload,
        );
        let plain = RecordHeader::data(key, Lsn(4), &payload);

        assert!(moved.flags.is_data());
        assert!(moved.flags.is_relocated());
        assert!(!plain.flags.is_relocated());
        assert!(moved.verify(&payload));
        assert_ne!(moved.crc, plain.crc, "the mark is covered by the checksum");
    }

    // the batch mark rides along with a record's kind without changing it
    #[test]
    fn batch_marks() {
        let key = sample_key(RECORD, 0x33, 34);
        let put = RecordHeader::new(4, Lsn(1), Flags::DATA.batched(), key.clone(), &[0x01; 4]);
        let grave = RecordHeader::new(0, Lsn(2), Flags::TOMBSTONE.batched(), key, &[]);

        assert!(put.flags.is_data());
        assert!(put.flags.is_batched());
        assert!(grave.flags.is_tombstone());
        assert!(grave.flags.is_batched());
        assert!(put.verify(&[0x01; 4]));
        assert!(grave.verify(&[]));
    }

    // a batch mark is covered by the checksum, so it cannot be added afterwards
    #[test]
    fn batch_mark_is_checksummed() {
        let payload = vec![0x44; 64];
        let key = sample_key(RECORD, 0x44, 34);
        let plain = RecordHeader::data(key.clone(), Lsn(5), &payload);
        let marked = RecordHeader::new(
            plain.length,
            plain.lsn,
            plain.flags.batched(),
            key,
            &payload,
        );

        assert!(marked.verify(&payload));
        assert_ne!(marked.crc, plain.crc);
    }

    // a frame round trips through the bytes it stages, declaration and all
    #[test]
    fn batch_frame_roundtrips() {
        let frame = BatchFrame {
            count: 7,
            span: 4_096,
        };
        let packed = frame.pack();

        assert!(packed.tail().is_none(), "a frame is one buffer");
        assert_eq!(packed.len(), BatchFrame::SPAN as usize);

        let bytes = packed.as_slice();
        let header = RecordHeader::unpack(bytes).expect("unpack");

        assert_eq!(header, frame.header());
        assert!(header.flags.is_batch_frame());
        assert!(!header.flags.is_data());
        assert!(header.has_payload());
        assert_eq!(header.lsn, Lsn::NONE);
        assert_eq!(header.span(), BatchFrame::SPAN);
        assert!(header.verify(&bytes[HEADER_LEN..]));
        assert_eq!(
            BatchFrame::unpack(&header, &bytes[HEADER_LEN..]),
            Some(frame)
        );
    }

    // a declaration no writer produces is refused rather than walked
    #[test]
    fn batch_frame_refuses_what_no_writer_wrote() {
        let frame = BatchFrame {
            count: 3,
            span: 300,
        };
        let packed = frame.pack();
        let bytes = packed.as_slice().to_vec();
        let header = RecordHeader::unpack(&bytes).expect("unpack");

        assert!(BatchFrame::unpack(&header, &bytes[HEADER_LEN..HEADER_LEN + 4]).is_none());

        let plain = RecordHeader::data(sample_key(RECORD, 0x01, 34), Lsn(1), &[0u8; 12]);
        assert!(BatchFrame::unpack(&plain, &bytes[HEADER_LEN..]).is_none());

        for (count, span) in [(0u32, 300u64), (1, 300), (3, 3 * HEADER_LEN as u64 - 1)] {
            let lying = BatchFrame { count, span };
            let packed = lying.pack();
            let bytes = packed.as_slice().to_vec();
            let header = RecordHeader::unpack(&bytes).expect("unpack");
            assert!(
                BatchFrame::unpack(&header, &bytes[HEADER_LEN..]).is_none(),
                "a frame of {count} records in {span} bytes passed",
            );
        }
    }

    // a flipped bit in a frame's declaration is caught by the record's checksum
    #[test]
    fn batch_frame_declaration_is_checksummed() {
        let frame = BatchFrame {
            count: 4,
            span: 1_000,
        };
        let header = frame.header();
        let packed = frame.pack();
        let mut declaration = packed.as_slice()[HEADER_LEN..].to_vec();

        declaration[0] ^= 0x01;

        assert!(!header.verify(&declaration));
    }

    // a segment header record round trips, carries its payload, and verifies
    #[test]
    fn segment_header_record() {
        let payload = [0x01u8, 0x00, 0x07, 0x00, 0x00, 0x01, 0x02, 0x03];
        let header = RecordHeader::segment_header(&payload);

        let parsed = RecordHeader::unpack(header.pack().as_slice()).expect("unpack");

        assert_eq!(parsed, header);
        assert!(parsed.flags.is_segment_header());
        assert!(parsed.has_payload());
        assert!(!parsed.flags.is_data());
        assert_eq!(parsed.length as usize, payload.len());
        assert_eq!(parsed.lsn, Lsn::NONE);
        assert!(parsed.verify(&payload));
    }

    // a flipped byte in a segment header payload is caught by the checksum
    #[test]
    fn segment_header_payload_flip() {
        let mut payload = vec![0x5au8; 32];
        let header = RecordHeader::segment_header(&payload);

        payload[9] ^= 0x40;

        assert!(!header.verify(&payload));
    }

    // an empty payload data record is distinct from a tombstone by its flags
    #[test]
    fn empty_payload() {
        let data = RecordHeader::data(sample_key(RECORD, 0x01, 34), Lsn(1), &[]);

        assert!(data.has_payload());
        assert!(!data.flags.is_tombstone());
        assert!(data.verify(&[]));
        assert_eq!(data.length, 0);
    }

    // a flipped bit anywhere in the header or the key is caught by the checksum
    #[test]
    fn header_bit_flip() {
        let payload = vec![0x33; 200];
        let header = RecordHeader::data(sample_key(RECORD, 0x44, 34), Lsn(6), &payload);
        let packed = header.pack();
        let bytes = packed.as_slice().to_vec();

        for index in 0..bytes.len() {
            if (OFFSET_CRC..OFFSET_LSN).contains(&index) {
                continue;
            }
            let mut torn = bytes.clone();
            torn[index] ^= 0x01;
            let parsed = match RecordHeader::unpack(&torn) {
                Ok(parsed) => parsed,
                Err(_) => continue,
            };
            assert!(!parsed.verify(&payload), "byte {index} went unnoticed");
        }
    }

    // a flipped bit in the payload is caught by the checksum
    #[test]
    fn payload_bit_flip() {
        let mut payload = vec![0x77; 512];
        let header = RecordHeader::data(sample_key(RECORD, 0x55, 34), Lsn(8), &payload);

        payload[300] ^= 0x08;

        assert!(!header.verify(&payload));
    }

    // a corrupted codec byte is caught by the checksum
    #[test]
    fn codec_flip() {
        let header = RecordHeader::tombstone(sample_key(RECORD, 0x01, 34), Lsn(2));
        let mut parsed = RecordHeader::unpack(header.pack().as_slice()).expect("unpack");

        parsed.codec = 1;

        assert!(!parsed.verify(&[]));
    }

    // the same key bytes under two columns are two different records
    #[test]
    fn column_separates_keys() {
        let payload = vec![0x12; 16];
        let one = RecordHeader::data(sample_key(RECORD, 0x09, 32), Lsn(1), &payload);
        let two = RecordHeader::data(sample_key(BLOB, 0x09, 32), Lsn(1), &payload);

        assert_ne!(one.crc, two.crc);
        assert_ne!(one.key, two.key);
    }

    // flag shapes no writer produces are rejected as corruption
    #[test]
    fn malformed_flags() {
        let header = RecordHeader::data(sample_key(RECORD, 0x01, 34), Lsn(1), &[0u8; 4]);
        let packed = header.pack().as_slice().to_vec();

        let two_kinds = 0b0000_0011;
        let marked_pad = 0b0010_0100;
        let marked_frame = 0b0011_0000;
        let unknown_bit = 0b1000_0000;
        for bits in [two_kinds, marked_pad, marked_frame, unknown_bit] {
            let mut torn = packed.clone();
            torn[OFFSET_FLAGS] = bits;
            assert!(RecordHeader::unpack(&torn).is_err(), "{bits:#010b} passed");
        }

        assert!(Flags::from_bits(0).is_ok());
    }

    // a header shorter than one header, or than the key it claims, is rejected
    #[test]
    fn short_header() {
        assert!(RecordHeader::unpack(&[0u8; HEADER_LEN - 1]).is_err());

        let header = RecordHeader::data(sample_key(RECORD, 0x01, 34), Lsn(1), &[]);
        let packed = header.pack().as_slice().to_vec();

        assert!(RecordHeader::unpack(&packed[..HEADER_LEN + 33]).is_err());
        assert!(RecordHeader::unpack(&packed).is_ok());
    }

    // a key width past what the format carries is rejected before any read
    #[test]
    fn over_wide_key_rejected() {
        let header = RecordHeader::data(sample_key(RECORD, 0x01, 34), Lsn(1), &[]);
        let mut packed = header.pack().as_slice().to_vec();

        packed[OFFSET_KEY_WIDTH..OFFSET_CODEC]
            .copy_from_slice(&((MAX_KEY_LEN + 1) as u16).to_le_bytes());
        assert!(RecordHeader::unpack(&packed).is_err());
    }

    // a key past the inline bound is a spilled key and round trips like any other
    #[test]
    fn a_spilled_key_round_trips() {
        let key = sample_key(RECORD, 0x5a, 200);
        assert!(
            key.key.is_spilled(),
            "200 bytes should not be an inline key"
        );

        let header = RecordHeader::data(key.clone(), Lsn(3), &[0xab; 16]);
        let prefix = header.pack();

        // The key is a second piece for the drain to gather, adjacent on disk.
        assert_eq!(prefix.as_slice().len(), HEADER_LEN);
        let tail = prefix
            .tail()
            .expect("a spilled key rides outside the prefix");
        assert_eq!(tail.len(), 200);
        assert_eq!(prefix.len(), HEADER_LEN + 200);

        let mut on_disk = prefix.as_slice().to_vec();
        on_disk.extend_from_slice(tail);
        let parsed = RecordHeader::unpack(&on_disk).expect("unpack");

        assert_eq!(parsed.key, key);
        assert_eq!(parsed.key.width(), 200);
    }

    // an inline key stays staged beside its header and grows no tail
    #[test]
    fn an_inline_key_needs_no_tail() {
        let header = RecordHeader::data(sample_key(RECORD, 0x11, 34), Lsn(1), &[0xab; 8]);
        let prefix = header.pack();

        assert!(prefix.tail().is_none());
        assert_eq!(prefix.len(), HEADER_LEN + 34);
        assert_eq!(prefix.as_slice().len(), prefix.len());
    }

    // a length past the end of the file fails the bounds check
    #[test]
    fn length_past_end() {
        let header = RecordHeader::data(sample_key(RECORD, 0x01, 34), Lsn(1), &vec![0u8; 10_000]);
        let prefix = header.prefix_len();

        assert!(!header.fits_within(prefix + 9_999));
        assert!(header.fits_within(prefix + 10_000));
    }

    // a pad always lands on a block boundary and spans at least a header
    #[test]
    fn pad_every_gap() {
        for position in 0u64..(BLOCK * 2) {
            let fill = pad_fill(position);
            let span = HEADER_LEN as u64 + u64::from(fill);
            let end = position + span;

            assert_eq!(end % BLOCK, 0);
            assert!(span >= HEADER_LEN as u64);
            assert!(end > position);
            assert_eq!(RecordHeader::pad(position).span(), span);
        }
    }

    // the boundary gap cases produce the expected pad fill
    #[test]
    fn pad_boundaries() {
        assert_eq!(pad_fill(0), BLOCK as u32 - HEADER_LEN as u32);

        let gap_is_header = BLOCK - HEADER_LEN as u64;
        assert_eq!(pad_fill(gap_is_header), 0);

        let gap_below_header = gap_is_header + 1;
        assert_eq!(
            HEADER_LEN as u64 + u64::from(pad_fill(gap_below_header)),
            BLOCK + HEADER_LEN as u64 - 1,
        );

        assert_eq!(
            HEADER_LEN as u64 + u64::from(pad_fill(BLOCK - 1)),
            BLOCK + 1
        );
    }

    // a record header covers its payload at every length, including none at all
    #[test]
    fn header_covers_its_payload() {
        let key = sample_key(RECORD, 0x66, 34);
        for len in [0usize, 1, 2048, 4096, 4097, 70_000] {
            let payload: Vec<u8> = (0..len).map(|index| (index * 31) as u8).collect();

            let header = RecordHeader::data(key.clone(), Lsn(12), &payload);

            assert!(header.verify(&payload));
            assert_eq!(header.length as usize, len);
        }
    }

    // an empty key is what a control record carries and what zeros parse as
    #[test]
    fn unwritten_space() {
        let zeros = RecordHeader::unpack(&[0u8; HEADER_LEN]).expect("unpack");

        assert!(zeros.is_unwritten());
        assert_eq!(zeros.key.key, KeyBytes::empty());
    }
}
