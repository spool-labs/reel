//! On-disk records: the keyless prefix, the keyed header, their flags and checksums

use std::hash::Hasher;
use std::sync::Arc;

use crc_fast::{CrcAlgorithm, Digest};
use siphasher::sip::SipHasher13;

use crate::error::{ReelError, Result};
use crate::format::column::{ColumnId, KeyRef, RecordKey, INLINE_KEY_LEN, MAX_KEY_LEN};
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

/// A keyless record writes these bytes ahead of its payload: its check, then its shape
pub const KEYLESS_PREFIX: usize = 10;

/// A keyless segment drops a record's key and header up to this many payload bytes, since every read checks it whole
pub const KEYLESS_MAX: u32 = 4096;

const KEYLESS_CHECK_AT: usize = 0;
const KEYLESS_SHAPE_AT: usize = 8;

/// A keyless record's shape keeps its codec in this many low bits, under its length
const CODEC_BITS: u32 = 2;

/// Every keyless length and codec fits the two bytes a shape takes
const _: () =
    assert!(((KEYLESS_MAX as u64) << CODEC_BITS) | ((1 << CODEC_BITS) - 1) <= u16::MAX as u64);

/// The check covers these bytes ahead of the key: column, key width, kind, shape and payload checksum
const KEYLESS_FIXED: usize = 10;

/// A keyless segment keys its checks with a secret this many bytes long
pub const CHECK_KEY_LEN: usize = 16;

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
const FLAG_SEGMENT_HEADER: u8 = 0b0000_1000;
const FLAG_RELOCATED: u8 = 0b0100_0000;

/// The kinds nothing resolves by key, which a mark never rides on
const CONTROL_MASK: u8 = FLAG_SEGMENT_HEADER;

/// The marks that ride along with a kind rather than being one
const MARK_MASK: u8 = FLAG_RELOCATED;

/// Every bit a writer sets, so anything else is a torn or foreign header
const KNOWN_MASK: u8 = KIND_MASK | MARK_MASK;

/// The checksum every record and footer is covered by
///
/// Part of the on-disk format: a stored value only reproduces under the same
/// algorithm, so changing it makes every segment already written unreadable.
const CRC: CrcAlgorithm = CrcAlgorithm::Crc32Iscsi;

/// A prefix has to fit the inline write buffer, or every record would allocate
const _: () = assert!(PREFIX_CAP <= crate::io::op::INLINE_CAP);

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

    /// The first record of a segment, carrying its self describing payload
    pub const SEGMENT_HEADER: Flags = Flags(FLAG_SEGMENT_HEADER);

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

    /// Whether the segment header bit is set
    pub fn is_segment_header(self) -> bool {
        self.0 & FLAG_SEGMENT_HEADER != 0
    }

    /// Whether the record is compaction's copy of one written earlier
    pub fn is_relocated(self) -> bool {
        self.0 & FLAG_RELOCATED != 0
    }

    /// Whether this is a control record, which no key resolves
    pub fn is_control(self) -> bool {
        self.0 & CONTROL_MASK != 0
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

    /// The prefix of a keyless record: its check, then its shape
    fn keyless(check: u64, shape: u16) -> RecordPrefix {
        let mut prefix = RecordPrefix {
            bytes: [0u8; PREFIX_CAP],
            len: KEYLESS_PREFIX,
            tail: None,
        };
        prefix.bytes[KEYLESS_CHECK_AT..KEYLESS_SHAPE_AT].copy_from_slice(&check.to_le_bytes());
        prefix.bytes[KEYLESS_SHAPE_AT..KEYLESS_PREFIX].copy_from_slice(&shape.to_le_bytes());
        prefix
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

    /// A header for a segment of this layout, checksummed the way that segment writes it
    pub fn framed(
        layout: RecordLayout,
        length: u32,
        lsn: Lsn,
        flags: Flags,
        key: RecordKey,
        codec: u8,
        payload: &[u8],
    ) -> RecordHeader {
        if !layout.is_keyless(length) || flags.is_control() {
            return RecordHeader::new_coded(length, lsn, flags, key, codec, payload);
        }
        // pack_in takes the check under the segment's key, so verify never runs on this header
        RecordHeader {
            length,
            crc: 0,
            lsn,
            flags,
            key,
            codec,
        }
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

    /// Whether this record has a payload
    pub fn has_payload(&self) -> bool {
        self.flags.is_data() || self.flags.is_segment_header() || self.flags.is_range_tombstone()
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

    /// Whether this record lies keyless in a segment of this layout
    pub fn is_keyless_in(&self, layout: RecordLayout) -> bool {
        // Readers find a control record by its header, so it keeps one in every layout
        layout.is_keyless(self.length) && !self.flags.is_control()
    }

    /// The span of this record in a segment of this layout
    pub fn span_in(&self, layout: RecordLayout) -> u64 {
        match self.is_keyless_in(layout) {
            true => (KEYLESS_PREFIX as u64) + u64::from(self.length),
            false => self.span(),
        }
    }

    /// The prefix of this record in a segment of this layout, over the payload it holds
    pub fn pack_in(&self, layout: RecordLayout, payload: &[u8]) -> RecordPrefix {
        match (layout, self.is_keyless_in(layout)) {
            (RecordLayout::Keyless(check), true) => {
                debug_assert!(
                    check != CheckKey::default(),
                    "a keyless record packed under no segment's key"
                );
                let shape = keyless_shape(self.length, self.codec);
                let covered = if self.has_payload() { payload } else { &[] };
                let sum = keyless_check(&check, self.key.as_ref(), self.flags, shape, covered);
                RecordPrefix::keyless(sum, shape)
            }
            _ => self.pack(),
        }
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

/// The codec of the data record a prefix starts, when it is this key's at this sequence number and length
///
/// Read in place, so checking a record builds no key.
pub fn data_codec(prefix: &[u8], key: KeyRef<'_>, lsn: Lsn, length: u32) -> Option<u8> {
    let fixed = prefix.get(..HEADER_LEN)?;
    let is_match = Flags::from_bits(fixed[OFFSET_FLAGS]).is_ok_and(Flags::is_data)
        && fixed[OFFSET_COLUMN] == key.column.as_u8()
        && usize::from(read_u16_le(&fixed[OFFSET_KEY_WIDTH..OFFSET_CODEC])) == key.bytes.len()
        && prefix.get(HEADER_LEN..HEADER_LEN + key.bytes.len()) == Some(key.bytes)
        && read_u64_le(&fixed[OFFSET_LSN..OFFSET_FLAGS]) == lsn.as_u64()
        && read_u32_le(&fixed[OFFSET_LENGTH..OFFSET_CRC]) == length;
    is_match.then_some(fixed[OFFSET_CODEC])
}

/// A record's version, length and kind, read in place when its prefix holds this key
pub fn head_for(prefix: &[u8], key: KeyRef<'_>) -> Option<(Lsn, u32, Flags)> {
    let fixed = prefix.get(..HEADER_LEN)?;
    let is_match = fixed[OFFSET_COLUMN] == key.column.as_u8()
        && usize::from(read_u16_le(&fixed[OFFSET_KEY_WIDTH..OFFSET_CODEC])) == key.bytes.len()
        && prefix.get(HEADER_LEN..HEADER_LEN + key.bytes.len()) == Some(key.bytes);
    if !is_match {
        return None;
    }
    let flags = Flags::from_bits(fixed[OFFSET_FLAGS]).ok()?;
    let lsn = Lsn(read_u64_le(&fixed[OFFSET_LSN..OFFSET_FLAGS]));
    Some((lsn, read_u32_le(&fixed[OFFSET_LENGTH..OFFSET_CRC]), flags))
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

/// Round a value up to the next multiple of an alignment
pub(crate) fn align_up(value: u64, alignment: u64) -> u64 {
    value.div_ceil(alignment) * alignment
}

/// How a segment frames its records, stamped in its header so every reader agrees
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RecordLayout {
    /// Every record has its header and its key
    #[default]
    Keyed,

    /// A record of at most `KEYLESS_MAX` payload bytes is keyless, checked under this key
    Keyless(CheckKey),
}

impl RecordLayout {
    /// The keyless layout under no segment's key, for sizing a record before its segment is drawn
    pub const KEYLESS: RecordLayout = RecordLayout::Keyless(CheckKey([0; CHECK_KEY_LEN]));

    /// A segment header stores this layout as this byte
    pub fn as_u8(self) -> u8 {
        match self {
            RecordLayout::Keyed => 0,
            RecordLayout::Keyless(_) => 1,
        }
    }

    /// Whether the segment drops the key of every record small enough
    pub fn is_keyless_layout(self) -> bool {
        matches!(self, RecordLayout::Keyless(_))
    }

    /// Whether a data record or tombstone of this stored length lies keyless
    pub fn is_keyless(self, len: u32) -> bool {
        self.is_keyless_layout() && fits_keyless(len)
    }

    /// The check key for a record of this stored length, or nothing when the record keeps its key
    pub fn keyless_key(self, len: u32) -> Option<CheckKey> {
        match self {
            RecordLayout::Keyless(check) if fits_keyless(len) => Some(check),
            RecordLayout::Keyed | RecordLayout::Keyless(_) => None,
        }
    }

    /// The prefix length for a key of this width and a payload of this length
    pub fn prefix_len(self, key_width: usize, len: u32) -> usize {
        match self.is_keyless(len) {
            true => KEYLESS_PREFIX,
            false => HEADER_LEN + key_width,
        }
    }
}

/// Whether a record of this stored length lies keyless in a keyless segment
pub fn fits_keyless(len: u32) -> bool {
    len <= KEYLESS_MAX
}

/// A random secret per segment that keys its checks, so a writer choosing keys cannot forge a check
#[derive(Clone, Copy, Default, Eq, PartialEq)]
pub struct CheckKey([u8; CHECK_KEY_LEN]);

impl CheckKey {
    /// A fresh key from the system's randomness
    pub fn random() -> Result<CheckKey> {
        let mut bytes = [0u8; CHECK_KEY_LEN];
        getrandom::getrandom(&mut bytes).map_err(|error| {
            ReelError::Io(std::io::Error::other(format!(
                "no randomness for a check key: {error}"
            )))
        })?;
        Ok(CheckKey(bytes))
    }

    /// Rebuild a key from a segment header's bytes
    pub const fn from_bytes(bytes: [u8; CHECK_KEY_LEN]) -> CheckKey {
        CheckKey(bytes)
    }

    /// The key's bytes, for a segment header to store
    pub fn to_bytes(self) -> [u8; CHECK_KEY_LEN] {
        self.0
    }
}

/// A secret stays out of logs
impl std::fmt::Debug for CheckKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CheckKey(..)")
    }
}

/// Pack a keyless record's stored length and codec into two bytes
fn keyless_shape(len: u32, codec: u8) -> u16 {
    debug_assert!(
        fits_keyless(len),
        "a keyless record is past the keyless ceiling"
    );
    debug_assert!(
        u32::from(codec) < 1 << CODEC_BITS,
        "a keyless record's codec is past its bits"
    );
    ((len << CODEC_BITS) | u32::from(codec)) as u16
}

/// Read a keyless record's stored length off its prefix, for a read that knows only a bound
pub fn keyless_len(prefix: &[u8]) -> Option<u32> {
    let shape = prefix.get(KEYLESS_SHAPE_AT..KEYLESS_PREFIX)?;
    Some(u32::from(read_u16_le(shape)) >> CODEC_BITS)
}

/// Read a keyless record's stored length and codec off its prefix, for a read a row or the map placed
pub fn keyless_len_codec(prefix: &[u8]) -> Option<(u32, u8)> {
    let shape = read_u16_le(prefix.get(KEYLESS_SHAPE_AT..KEYLESS_PREFIX)?);
    Some((
        u32::from(shape) >> CODEC_BITS,
        (shape & ((1 << CODEC_BITS) - 1)) as u8,
    ))
}

/// Read a keyless record's codec off its prefix
pub fn keyless_codec(prefix: &[u8; KEYLESS_PREFIX]) -> u8 {
    (read_u16_le(&prefix[KEYLESS_SHAPE_AT..KEYLESS_PREFIX]) & ((1 << CODEC_BITS) - 1)) as u8
}

/// Check a keyless record over its key, kind, length, codec and payload, under its segment's key
pub fn keyless_check(
    check: &CheckKey,
    key: KeyRef<'_>,
    flags: Flags,
    shape: u16,
    payload: &[u8],
) -> u64 {
    // The version stays out, since a spot read has no row to take it from
    let mut fixed = [0u8; KEYLESS_FIXED];
    fixed[0] = key.column.as_u8();
    fixed[1..3].copy_from_slice(&(key.bytes.len() as u16).to_le_bytes());
    fixed[3] = flags.bits() & KIND_MASK;
    fixed[4..6].copy_from_slice(&shape.to_le_bytes());
    // The payload goes in as its checksum, so the keyed hash stays short at any length
    fixed[6..10].copy_from_slice(&checksum(payload).to_le_bytes());
    let mut hasher = SipHasher13::new_with_key(&check.0);
    hasher.write(&fixed);
    hasher.write(key.bytes);
    hasher.finish()
}

/// How a keyless record compares with the record a reader came for
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeylessRead {
    /// The record is the one asked for, whole, its payload stored under this codec
    Intact(u8),

    /// Zeros: space nothing wrote, or a dead run that gave its blocks back
    Unwritten,

    /// Bytes that do not check out against the key, kind and length asked for
    Corrupt,
}

/// Check a keyless record against the key and kind a reader holds, under its segment's key
pub fn check_keyless(
    prefix: &[u8],
    payload: &[u8],
    key: KeyRef<'_>,
    flags: Flags,
    check: &CheckKey,
) -> KeylessRead {
    let Some(fixed) = prefix.get(..KEYLESS_PREFIX) else {
        return KeylessRead::Unwritten;
    };
    let stored = read_u64_le(&fixed[KEYLESS_CHECK_AT..KEYLESS_SHAPE_AT]);
    let shape = read_u16_le(&fixed[KEYLESS_SHAPE_AT..KEYLESS_PREFIX]);
    let len = u32::from(shape) >> CODEC_BITS;
    if len as usize == payload.len() && keyless_check(check, key, flags, shape, payload) == stored {
        return KeylessRead::Intact((shape & ((1 << CODEC_BITS) - 1)) as u8);
    }
    let is_zeros = fixed.iter().all(|byte| *byte == 0) && payload.iter().all(|byte| *byte == 0);
    match is_zeros {
        true => KeylessRead::Unwritten,
        false => KeylessRead::Corrupt,
    }
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

    // a tombstone has no payload
    #[test]
    fn tombstone_record() {
        let tombstone = RecordHeader::tombstone(sample_key(RECORD, 0x22, 34), Lsn(4));

        let parsed = RecordHeader::unpack(tombstone.pack().as_slice()).expect("unpack");
        assert_eq!(parsed, tombstone);
        assert!(parsed.verify(&[]));
        assert!(!parsed.has_payload());
        assert!(tombstone.flags.is_tombstone());
    }

    // a range tombstone carries its exclusive end as its payload
    #[test]
    fn range_tombstone_carries_end() {
        let start = sample_key(RECORD, 0x10, 34);
        let end = vec![0x11u8; 34];
        let header = RecordHeader::new(
            end.len() as u32,
            Lsn(6),
            Flags::RANGE_TOMBSTONE,
            start.clone(),
            &end,
        );

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
        let header = RecordHeader::new(
            0,
            Lsn(7),
            Flags::RANGE_TOMBSTONE,
            sample_key(RECORD, 0xff, 34),
            &[],
        );

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

    // an empty payload data record is distinct from a tombstone by its flags
    #[test]
    fn empty_payload() {
        let data = RecordHeader::data(sample_key(RECORD, 0x01, 34), Lsn(1), &[]);

        assert!(data.has_payload());
        assert!(!data.flags.is_tombstone());
        assert!(data.verify(&[]));
        assert_eq!(data.length, 0);
    }

    // a corrupted codec byte is caught by the checksum
    #[test]
    fn codec_flip() {
        let header = RecordHeader::tombstone(sample_key(RECORD, 0x01, 34), Lsn(2));
        let mut parsed = RecordHeader::unpack(header.pack().as_slice()).expect("unpack");

        parsed.codec = 1;

        assert!(!parsed.verify(&[]));
    }

    // flag shapes no writer produces are rejected as corruption
    #[test]
    fn malformed_flags() {
        let header = RecordHeader::data(sample_key(RECORD, 0x01, 34), Lsn(1), &[0u8; 4]);
        let packed = header.pack().as_slice().to_vec();

        let two_kinds = 0b0000_0011;
        let unused_mark = 0b0010_0000;
        let unknown_bit = 0b1000_0000;
        for bits in [two_kinds, unused_mark, unknown_bit] {
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
            matches!(key.key, KeyBytes::Spilled(_)),
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

    const SEGMENT_KEY: CheckKey = CheckKey([7; CHECK_KEY_LEN]);
    const LAID: RecordLayout = RecordLayout::Keyless(SEGMENT_KEY);

    /// A keyless record's prefix and payload, as a keyless segment writes them
    fn keyless(key: &RecordKey, lsn: Lsn, codec: u8, payload: &[u8]) -> Vec<u8> {
        let header = RecordHeader::framed(
            LAID,
            payload.len() as u32,
            lsn,
            Flags::DATA.relocated(),
            key.clone(),
            codec,
            payload,
        );
        let mut bytes = header.pack_in(LAID, payload).as_slice().to_vec();
        bytes.extend_from_slice(payload);
        assert_eq!(bytes.len() as u64, header.span_in(LAID));
        bytes
    }

    // a small record in a keyless segment is its check, its shape and its payload
    #[test]
    fn a_keyless_record_reads_back() {
        let key = sample_key(RECORD, 0x21, 108);
        let payload = [7u8; 8];

        let bytes = keyless(&key, Lsn(40), 3, &payload);

        assert_eq!(bytes.len(), KEYLESS_PREFIX + payload.len());
        let (prefix, body) = bytes.split_at(KEYLESS_PREFIX);
        assert_eq!(keyless_len(prefix), Some(8));
        assert_eq!(
            check_keyless(prefix, body, key.as_ref(), Flags::DATA, &SEGMENT_KEY),
            KeylessRead::Intact(3)
        );
    }

    // the check is the record's identity: another key, kind, length or segment key fails it
    #[test]
    fn a_keyless_record_answers_only_for_its_key() {
        let key = sample_key(RECORD, 0x21, 108);
        let bytes = keyless(&key, Lsn(40), 0, &[7u8; 8]);
        let (prefix, body) = bytes.split_at(KEYLESS_PREFIX);
        let check = |key: &RecordKey, flags: Flags, body: &[u8], segment: &CheckKey| {
            check_keyless(prefix, body, key.as_ref(), flags, segment)
        };

        let other = sample_key(RECORD, 0x22, 108);
        let column = sample_key(BLOB, 0x21, 108);
        let wider = sample_key(RECORD, 0x21, 109);
        assert_eq!(
            check(&other, Flags::DATA, body, &SEGMENT_KEY),
            KeylessRead::Corrupt
        );
        assert_eq!(
            check(&column, Flags::DATA, body, &SEGMENT_KEY),
            KeylessRead::Corrupt
        );
        assert_eq!(
            check(&wider, Flags::DATA, body, &SEGMENT_KEY),
            KeylessRead::Corrupt
        );
        assert_eq!(
            check(&key, Flags::TOMBSTONE, body, &SEGMENT_KEY),
            KeylessRead::Corrupt
        );
        assert_eq!(
            check(&key, Flags::DATA, &body[..7], &SEGMENT_KEY),
            KeylessRead::Corrupt
        );
        let elsewhere = CheckKey([8; CHECK_KEY_LEN]);
        assert_eq!(
            check(&key, Flags::DATA, body, &elsewhere),
            KeylessRead::Corrupt
        );
        assert_eq!(
            check(&key, Flags::DATA, body, &SEGMENT_KEY),
            KeylessRead::Intact(0)
        );
    }

    // the version is left out, so a spot read with no footer row confirms the record alone
    #[test]
    fn a_keyless_check_leaves_the_version_out() {
        let key = sample_key(RECORD, 0x21, 108);

        assert_eq!(
            keyless(&key, Lsn(40), 0, &[7u8; 8]),
            keyless(&key, Lsn(41), 0, &[7u8; 8])
        );
    }

    // zeros are space nothing wrote, which a reader tells apart from rot
    #[test]
    fn keyless_zeros_read_as_unwritten() {
        let key = sample_key(RECORD, 0x21, 108);

        let read = check_keyless(
            &[0u8; KEYLESS_PREFIX],
            &[0u8; 8],
            key.as_ref(),
            Flags::DATA,
            &SEGMENT_KEY,
        );

        assert_eq!(read, KeylessRead::Unwritten);
    }

    // a flipped payload bit reads as rot
    #[test]
    fn a_keyless_payload_flip_is_corrupt() {
        let key = sample_key(RECORD, 0x21, 34);
        let mut bytes = keyless(&key, Lsn(9), 0, &[0x5a; 64]);
        bytes[KEYLESS_PREFIX + 10] ^= 0x01;

        let (prefix, body) = bytes.split_at(KEYLESS_PREFIX);

        assert_eq!(
            check_keyless(prefix, body, key.as_ref(), Flags::DATA, &SEGMENT_KEY),
            KeylessRead::Corrupt
        );
    }

    // the largest keyless record keeps its length and codec in the shape
    #[test]
    fn the_keyless_ceiling_fits_its_shape() {
        let key = sample_key(RECORD, 0x21, 34);
        let payload = vec![0x33u8; KEYLESS_MAX as usize];

        let bytes = keyless(&key, Lsn(9), 3, &payload);

        let (prefix, body) = bytes.split_at(KEYLESS_PREFIX);
        assert_eq!(keyless_len(prefix), Some(KEYLESS_MAX));
        assert_eq!(
            check_keyless(prefix, body, key.as_ref(), Flags::DATA, &SEGMENT_KEY),
            KeylessRead::Intact(3)
        );
    }

    // past the keyless ceiling a record keeps its header, so a window reads without the rest
    #[test]
    fn a_large_record_keeps_its_header() {
        let key = sample_key(RECORD, 0x21, 34);
        let payload = vec![0x11u8; KEYLESS_MAX as usize + 1];

        let header = RecordHeader::framed(
            LAID,
            payload.len() as u32,
            Lsn(5),
            Flags::DATA,
            key.clone(),
            0,
            &payload,
        );

        assert!(!header.is_keyless_in(LAID));
        assert_eq!(header.span_in(LAID), header.span());
        assert_eq!(
            header.pack_in(LAID, &payload).as_slice(),
            header.pack().as_slice()
        );
        assert!(header.verify(&payload));
    }

    // a segment header keeps its header in a keyless segment
    #[test]
    fn the_segment_header_is_never_keyless() {
        let payload = [0u8; 16];
        let header = RecordHeader::segment_header(&payload);

        assert!(!header.is_keyless_in(LAID));
        assert_eq!(header.span_in(LAID), header.span());
    }

    // a tombstone in a keyless segment is a keyless record with no payload
    #[test]
    fn a_keyless_tombstone_is_its_prefix() {
        let key = sample_key(RECORD, 0x30, 108);

        let header = RecordHeader::framed(
            LAID,
            0,
            Lsn(77),
            Flags::TOMBSTONE.relocated(),
            key.clone(),
            0,
            &[],
        );

        assert_eq!(header.span_in(LAID), KEYLESS_PREFIX as u64);
        let prefix = header.pack_in(LAID, &[]);
        assert_eq!(
            check_keyless(
                prefix.as_slice(),
                &[],
                key.as_ref(),
                Flags::TOMBSTONE,
                &SEGMENT_KEY
            ),
            KeylessRead::Intact(0)
        );
        assert_eq!(
            check_keyless(
                prefix.as_slice(),
                &[],
                key.as_ref(),
                Flags::DATA,
                &SEGMENT_KEY
            ),
            KeylessRead::Corrupt
        );
    }

    // a keyed segment frames every record whole, whatever its length
    #[test]
    fn a_keyed_layout_keeps_every_key() {
        assert!(!RecordLayout::Keyed.is_keyless(0));
        assert_eq!(RecordLayout::Keyed.prefix_len(108, 8), HEADER_LEN + 108);
        assert_eq!(LAID.prefix_len(108, 8), KEYLESS_PREFIX);
        assert_eq!(LAID.prefix_len(108, KEYLESS_MAX + 1), HEADER_LEN + 108);
        assert_eq!(RecordLayout::KEYLESS.prefix_len(108, 8), KEYLESS_PREFIX);
        assert_eq!(RecordLayout::Keyed.as_u8(), 0);
        assert_eq!(LAID.as_u8(), 1);
    }

    // a check key never prints
    #[test]
    fn a_check_key_stays_out_of_logs() {
        assert_eq!(format!("{LAID:?}"), "Keyless(CheckKey(..))");
    }
}
