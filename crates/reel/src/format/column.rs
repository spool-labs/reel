//! Columns, the keys they are addressed by, and the widths those keys take

use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use crate::error::{ReelError, Result};

/// The format's widest key, a 32 byte id plus a 1024 byte name
pub const MAX_KEY_LEN: usize = 1056;

/// A record's prefix stages keys up to this width, and a wider key goes as a shared tail
pub const INLINE_KEY_LEN: usize = 108;

/// Keys up to this width sit in place, and wider ones go on the heap
pub const SHORT_KEY_LEN: usize = 40;

/// How a stored payload was encoded, stamped into the record header
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u8)]
pub enum Codec {
    /// Payload bytes stored verbatim
    #[default]
    None = 0,

    /// An lz4 block, opening with a four byte logical length
    Lz4 = 1,
}

impl Codec {
    /// The byte this codec stamps into a record header
    pub fn as_byte(self) -> u8 {
        self as u8
    }
}

/// Which column a record belongs to
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ColumnId(pub u8);

impl ColumnId {
    /// The raw identifier, as it is stamped into a record header
    pub fn as_u8(self) -> u8 {
        self.0
    }

    /// The identifier as an index into a table with one slot per column
    pub fn as_index(self) -> usize {
        self.0 as usize
    }
}

/// A key's bytes, in place where they fit and on the heap where they do not
#[derive(Clone)]
pub enum KeyBytes {
    /// Bytes in place, which fits the common fixed widths
    Inline {
        width: u8,
        bytes: [u8; SHORT_KEY_LEN],
    },

    /// Bytes on the heap, owned by the one key, still staged in a record's prefix
    Boxed(Box<[u8]>),

    /// Bytes on the heap, shared by refcount
    Spilled(Arc<[u8]>),
}

impl KeyBytes {
    /// A key from its bytes, rejecting one wider than `MAX_KEY_LEN`
    pub fn new(bytes: &[u8]) -> Result<KeyBytes> {
        if bytes.len() > MAX_KEY_LEN {
            return Err(ReelError::Rejected(format!(
                "a key of {} bytes is wider than the {MAX_KEY_LEN} byte maximum",
                bytes.len(),
            )));
        }
        if bytes.len() > INLINE_KEY_LEN {
            return Ok(KeyBytes::Spilled(Arc::from(bytes)));
        }
        if bytes.len() > SHORT_KEY_LEN {
            return Ok(KeyBytes::Boxed(Box::from(bytes)));
        }
        let mut inline = [0u8; SHORT_KEY_LEN];
        inline[..bytes.len()].copy_from_slice(bytes);
        Ok(KeyBytes::Inline {
            width: bytes.len() as u8,
            bytes: inline,
        })
    }

    /// The empty key of a control record
    pub fn empty() -> KeyBytes {
        KeyBytes::Inline {
            width: 0,
            bytes: [0u8; SHORT_KEY_LEN],
        }
    }

    /// The key bytes, at the width they were built from
    pub fn as_slice(&self) -> &[u8] {
        match self {
            KeyBytes::Inline { width, bytes } => &bytes[..*width as usize],
            KeyBytes::Boxed(bytes) => bytes,
            KeyBytes::Spilled(bytes) => bytes,
        }
    }

    /// The key's width in bytes
    pub fn width(&self) -> u16 {
        match self {
            KeyBytes::Inline { width, .. } => u16::from(*width),
            KeyBytes::Boxed(bytes) => bytes.len() as u16,
            KeyBytes::Spilled(bytes) => bytes.len() as u16,
        }
    }

    /// The shared heap bytes of a key the prefix does not stage
    pub fn spilled_bytes(&self) -> Option<Arc<[u8]>> {
        match self {
            KeyBytes::Inline { .. } | KeyBytes::Boxed(_) => None,
            KeyBytes::Spilled(bytes) => Some(Arc::clone(bytes)),
        }
    }
}

impl Default for KeyBytes {
    fn default() -> KeyBytes {
        KeyBytes::empty()
    }
}

impl PartialEq for KeyBytes {
    fn eq(&self, other: &KeyBytes) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for KeyBytes {}

impl Ord for KeyBytes {
    fn cmp(&self, other: &KeyBytes) -> Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl PartialOrd for KeyBytes {
    fn partial_cmp(&self, other: &KeyBytes) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for KeyBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("KeyBytes(")?;
        for byte in self.as_slice() {
            write!(formatter, "{byte:02x}")?;
        }
        formatter.write_str(")")
    }
}

/// The full address of one record: its column and its key within that column
#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct RecordKey {
    /// The record's column
    pub column: ColumnId,

    /// The key within the column, at the column's width
    pub key: KeyBytes,
}

/// A record's address borrowed from wherever the caller already holds the bytes
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KeyRef<'bytes> {
    /// The record's column
    pub column: ColumnId,

    /// The key's bytes, at the column's width
    pub bytes: &'bytes [u8],
}

impl<'bytes> KeyRef<'bytes> {
    /// A borrowed address from its parts
    pub fn new(column: ColumnId, bytes: &'bytes [u8]) -> KeyRef<'bytes> {
        KeyRef { column, bytes }
    }

    /// The key's bytes
    pub fn as_slice(&self) -> &[u8] {
        self.bytes
    }

    /// The key's width in bytes
    pub fn width(&self) -> usize {
        self.bytes.len()
    }

    /// An owned copy of the key
    pub fn to_owned_key(&self) -> Result<RecordKey> {
        RecordKey::from_bytes(self.column, self.bytes)
    }
}

impl RecordKey {
    /// A key addressing a record in one column
    pub fn new(column: ColumnId, key: KeyBytes) -> RecordKey {
        RecordKey { column, key }
    }

    /// A key from a column and raw bytes, rejecting an over-wide key
    pub fn from_bytes(column: ColumnId, bytes: &[u8]) -> Result<RecordKey> {
        Ok(RecordKey::new(column, KeyBytes::new(bytes)?))
    }

    /// The empty key of a control record, which addresses no column
    pub fn none() -> RecordKey {
        RecordKey::new(ColumnId(0), KeyBytes::empty())
    }

    /// The key bytes, at the width the column declares
    pub fn as_slice(&self) -> &[u8] {
        self.key.as_slice()
    }

    /// This key, borrowed
    pub fn as_ref(&self) -> KeyRef<'_> {
        KeyRef {
            column: self.column,
            bytes: self.key.as_slice(),
        }
    }

    /// The key's width in bytes
    pub fn width(&self) -> u16 {
        self.key.width()
    }
}

/// A column's key width, fixed or variable
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyWidth {
    /// Every key in the column is exactly this many bytes
    Fixed(u16),

    /// Keys of any width up to `MAX_KEY_LEN`
    Variable,
}

impl KeyWidth {
    /// The width, for a column that has one
    pub fn fixed(self) -> Option<u16> {
        match self {
            KeyWidth::Fixed(width) => Some(width),
            KeyWidth::Variable => None,
        }
    }

    /// Whether a key of this length belongs in the column
    pub fn admits(self, len: usize) -> bool {
        match self {
            KeyWidth::Fixed(width) => len == width as usize,
            KeyWidth::Variable => len <= MAX_KEY_LEN,
        }
    }
}

/// One column the reel serves and the shape of the keys it holds
pub struct ColumnSpec {
    /// Identifier stamped into every record of the column
    pub id: ColumnId,

    /// The column family name the store trait uses
    pub name: &'static str,

    /// The width of the column's keys
    pub key_width: KeyWidth,

    /// This many leading key bytes select a key's index shard
    pub shard_bytes: u8,

    /// Where a key says the record dies
    pub purge_mark: Option<PurgeMark>,

    /// The codec tried on this column's payloads at admission
    pub codec: Codec,
}

/// Length of a purge mark within a key
pub const MARK_LEN: usize = 8;

/// Where a column's keys say the record dies
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PurgeMark {
    /// Offset of the big endian u64 within a key
    pub at: u8,
}

impl PurgeMark {
    /// A mark the volume purges by
    pub const fn at(at: u8) -> PurgeMark {
        PurgeMark { at }
    }

    /// Where this key sits on the purge timeline
    pub fn read(&self, key: &[u8]) -> u64 {
        let at = self.at as usize;
        // A key too short to hold the mark reads as the bottom, so it is purged
        let Some(bytes) = key.get(at..at + MARK_LEN) else {
            return 0;
        };
        let mut mark = [0u8; MARK_LEN];
        mark.copy_from_slice(bytes);
        u64::from_be_bytes(mark)
    }
}

impl ColumnSpec {
    /// Where this key sits on the purge timeline, for a column that marks its keys
    pub fn mark_of(&self, key: &[u8]) -> Option<u64> {
        Some(self.purge_mark?.read(key))
    }

    /// Number of index shards the column splits into
    pub fn shard_count(&self) -> usize {
        1usize << (8 * self.shard_bytes as usize)
    }

    /// Which shard a key belongs to, from its leading bytes, reading missing bytes as zero
    pub fn shard_of(&self, key: &[u8]) -> usize {
        let mut shard = 0usize;
        for at in 0..self.shard_bytes as usize {
            let byte = key.get(at).copied().unwrap_or(0);
            shard = (shard << 8) | byte as usize;
        }
        shard
    }
}

pub type ColumnSet = &'static [ColumnSpec];

/// Resolve a column family name to its declaration
pub fn spec_by_name<'a>(columns: &'a [ColumnSpec], name: &str) -> Option<&'a ColumnSpec> {
    columns.iter().find(|spec| spec.name == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    const COLUMNS: &[ColumnSpec] = &[
        ColumnSpec {
            id: ColumnId(1),
            name: "record",
            key_width: KeyWidth::Fixed(34),
            shard_bytes: 2,
            purge_mark: None,
            codec: Codec::None,
        },
        ColumnSpec {
            id: ColumnId(2),
            name: "blob",
            key_width: KeyWidth::Fixed(32),
            shard_bytes: 0,
            purge_mark: None,
            codec: Codec::None,
        },
    ];

    // a record key stays in its size class, since a walk builds one per key stepped
    #[test]
    fn a_record_key_stays_in_its_size_class() {
        assert_eq!(
            std::mem::size_of::<RecordKey>(),
            56,
            "a record key changed size; the walk pays this per key stepped",
        );
    }

    // a key round trips its bytes and reports the width it was built from
    #[test]
    fn key_roundtrip() {
        let key = KeyBytes::new(&[1u8, 2, 3]).expect("key");

        assert_eq!(key.as_slice(), &[1, 2, 3]);
        assert_eq!(key.width(), 3);
    }

    // a key wider than any column may declare is refused
    #[test]
    fn over_wide_key_refused() {
        assert!(KeyBytes::new(&[0u8; MAX_KEY_LEN]).is_ok());
        assert!(KeyBytes::new(&[0u8; MAX_KEY_LEN + 1]).is_err());
    }

    // keys order lexicographically over their used bytes
    #[test]
    fn key_order() {
        let low = KeyBytes::new(&[1u8, 0xff]).expect("key");
        let high = KeyBytes::new(&[2u8, 0x00]).expect("key");
        let short = KeyBytes::new(&[1u8]).expect("key");

        assert!(low < high);
        assert!(short < low);
        assert_ne!(short, low);
    }

    // the empty key of a control record has no bytes at all
    #[test]
    fn empty_key() {
        assert_eq!(KeyBytes::empty().width(), 0);
        assert!(KeyBytes::empty().as_slice().is_empty());
        assert_eq!(RecordKey::none().width(), 0);
    }

    // a column splits into as many shards as its prefix bytes address
    #[test]
    fn shard_layout() {
        let record = &COLUMNS[0];
        let blob = &COLUMNS[1];

        assert_eq!(record.shard_count(), 65536);
        assert_eq!(blob.shard_count(), 1);
        assert_eq!(record.shard_of(&[0x03, 0xe8]), 1000);
        assert_eq!(blob.shard_of(&[0xff, 0xff]), 0);
    }

    // a bound shorter than the shard prefix reads its missing bytes as zero
    #[test]
    fn short_bound_shards_low() {
        let record = &COLUMNS[0];

        assert_eq!(record.shard_of(&[0x03]), 768);
        assert_eq!(record.shard_of(&[]), 0);
    }

    // a mark reads the key's big endian u64, and a short key reads as the bottom
    #[test]
    fn reads_a_mark() {
        let mark = PurgeMark::at(2);

        assert_eq!(mark.read(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 7]), 7);
        assert_eq!(mark.read(&[0, 0, 0]), 0);
    }

    // a marked column reads its mark off the key, and an unmarked one reads none
    #[test]
    fn marked_column() {
        let key = [0u8, 0, 0, 0, 0, 0, 0, 0, 0, 9];
        let purged = ColumnSpec {
            purge_mark: Some(PurgeMark::at(2)),
            ..spec()
        };

        assert_eq!(purged.mark_of(&key), Some(9));
        assert_eq!(spec().mark_of(&key), None);
    }

    /// An unmarked column for the mark tests to vary
    fn spec() -> ColumnSpec {
        ColumnSpec {
            id: ColumnId(1),
            name: "record",
            key_width: KeyWidth::Fixed(10),
            shard_bytes: 0,
            purge_mark: None,
            codec: Codec::None,
        }
    }

    // columns resolve by name
    #[test]
    fn resolves_columns() {
        assert_eq!(
            spec_by_name(COLUMNS, "record").expect("record").id,
            ColumnId(1)
        );
        assert!(spec_by_name(COLUMNS, "absent").is_none());
    }
}
