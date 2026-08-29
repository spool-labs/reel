//! Columns, the keys they are addressed by, and the widths those keys take
//!
//! A reel holds every column on one log, so a record says which column it belongs
//! to and how wide its key is. Keys are stored at their own column's width rather
//! than padded to the widest, and the common widths are carried in place rather
//! than on the heap, since one is built per record read and written.

use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use crate::error::{ReelError, Result};

/// Widest key the format carries
///
/// A 32 byte id and a name at the 1024 byte ceiling. Nothing is padded to this:
/// it is what a width field has to be able to say, not a size anything occupies.
pub const MAX_KEY_LEN: usize = 1056;

/// Widest key a record's prefix stages, past which a key rides as a shared tail
///
/// Every fixed-width column the reel serves sits at or under this, so none of them
/// splits its record into a second buffer; a wider variable key does.
pub const INLINE_KEY_LEN: usize = 108;

/// Widest key held in the key's own bytes, past which it holds a pointer
///
/// Sized so the common fixed widths, a 32 byte id and the 34 byte record key, sit
/// in place: a walk builds one key per row it steps, and this is what each weighs.
pub const SHORT_KEY_LEN: usize = 40;

/// Widest value a column may ask the index to carry for it
///
/// An index entry has this many bytes of padding to spend, so a ceiling up to it
/// is free and one past it costs eight bytes on every key of every column.
pub const INLINE_MAX: usize = 4;

/// Widest value a sealed row carries beside its key
///
/// Unlike the index entry's inline bytes, which every resident key of every
/// column pays for, a row is read a block at a time and only the reader who
/// wanted that block pays for what it carries.
pub const ROW_CARRY_MAX: usize = 256;

/// What a sealed row carries of a value, for a column that asked to carry one
///
/// On the heap and only where a row really carries something: most columns carry
/// nothing, and an inline array would cost its full width on every entry built.
pub type CarryBytes = Box<[u8]>;

/// The leading bytes of a payload, padded out to the width the row reserves
pub fn carry_bytes(payload: &[u8], width: u16) -> CarryBytes {
    let width = (width as usize).min(ROW_CARRY_MAX);
    let mut carry = vec![0u8; width];
    let taken = payload.len().min(width);
    carry[..taken].copy_from_slice(&payload[..taken]);
    carry.into_boxed_slice()
}

/// The bytes an index entry or a footer row carries of a value itself
pub type InlineBytes = [u8; INLINE_MAX];

/// The leading bytes of a payload, as far as one of those will hold
///
/// Bytes past a record's own length are never read back, so a caller hands over
/// whatever it has without measuring it first.
pub fn inline_bytes(payload: &[u8]) -> InlineBytes {
    let mut inline = [0u8; INLINE_MAX];
    let taken = payload.len().min(INLINE_MAX);
    inline[..taken].copy_from_slice(&payload[..taken]);
    inline
}

/// How a stored payload was encoded, stamped into the record header
///
/// Zero is raw bytes and a nonzero byte names the codec that produced the stored
/// bytes, so a reader needs no column context to open a record.
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

/// A key's bytes, carried in place where they fit and on the heap where they do not
///
/// Three widths rather than two, because a rebuild holds one of these per record
/// version and every one of them would otherwise be as wide as the widest key a
/// prefix stages. A key past the staging width holds an `Arc`, so cloning stays a
/// refcount rather than a copy of the name, and the type cannot be `Copy`.
#[derive(Clone)]
pub enum KeyBytes {
    /// Bytes in place, which is what the common fixed widths carry
    Inline {
        width: u8,
        bytes: [u8; SHORT_KEY_LEN],
    },

    /// Bytes on the heap, owned by the one key, still staged in a record's prefix
    Boxed(Box<[u8]>),

    /// Bytes on the heap, shared by refcount rather than copied
    Spilled(Arc<[u8]>),
}

impl KeyBytes {
    /// A key from its bytes, rejecting one wider than the format carries
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

    /// The empty key, which control records that address nothing carry
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

    /// Bytes this key occupies on disk and in an index
    pub fn width(&self) -> u16 {
        match self {
            KeyBytes::Inline { width, .. } => u16::from(*width),
            KeyBytes::Boxed(bytes) => bytes.len() as u16,
            KeyBytes::Spilled(bytes) => bytes.len() as u16,
        }
    }

    /// Whether this key rides outside the record prefix rather than within it
    pub fn is_spilled(&self) -> bool {
        matches!(self, KeyBytes::Spilled(_))
    }

    /// The heap bytes themselves, for a writer that would rather point than copy
    ///
    /// Nothing for a key the prefix stages, which is already copied into it.
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
///
/// Not `Copy`, because a key that can spill to the heap cannot be. Cloning one is
/// a memcpy of the inline buffer or a refcount bump, never a copy of a name.
#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct RecordKey {
    /// Column the record belongs to
    pub column: ColumnId,

    /// Key within the column, at the column's declared width
    pub key: KeyBytes,
}

/// A record's address borrowed from wherever the caller already holds the bytes
///
/// Sixteen bytes whatever the key's width, so a read path never owns a key beside
/// the buffer it was already lent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KeyRef<'bytes> {
    /// Column the record belongs to
    pub column: ColumnId,

    /// The key's bytes, at the column's declared width
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

    /// Bytes the key occupies, which is what a record's prefix is sized by
    pub fn width(&self) -> usize {
        self.bytes.len()
    }

    /// An owned key, for the one path that keeps one
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

    /// This key borrowed, for the read paths that only read it
    pub fn as_ref(&self) -> KeyRef<'_> {
        KeyRef {
            column: self.column,
            bytes: self.key.as_slice(),
        }
    }

    /// Bytes the key occupies
    pub fn width(&self) -> u16 {
        self.key.width()
    }
}

/// What structure a column's resident shards hold their keys in
///
/// The tree serves every column. The open-addressed table is for a clustered
/// column that is huge, small-valued and overwrite-heavy, and only the fixed
/// widths the index declares an open arm for may take it. The choice is
/// resident-side only, so no on-disk byte depends on it, a reopen may flip it,
/// and a volume that ignores the declaration gives every column the tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MapShape {
    /// Ordered tree shards, what a column takes unless it opts out
    Tree,

    /// Open-addressed shards: point reads first, ordered walks collect and sort
    Open,
}

/// What a column's keys measure, which is a width or the absence of one
///
/// A fixed column's width is what a footer partition strides by and what the
/// index monomorphises over; a variable column has neither. The distinction is a
/// type so a variable column cannot be declared as some number and strided by it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyWidth {
    /// Every key in the column is exactly this many bytes
    Fixed(u16),

    /// Keys run to whatever they run to, up to the format's ceiling
    Variable,
}

impl KeyWidth {
    /// The width, for a column that has one
    ///
    /// Nothing for a variable column, whose caller has to read each record's own
    /// width instead.
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
///
/// Shards split the column by leading key bytes, so writers to unrelated parts do
/// not contend and a playback is the shards in turn.
pub struct ColumnSpec {
    /// Identifier stamped into every record of the column
    pub id: ColumnId,

    /// Column family name the store trait addresses the column by
    pub name: &'static str,

    /// How wide the keys in this column are, or that they are not one width
    pub key_width: KeyWidth,

    /// Leading key bytes that select the index shard a key lives in
    pub shard_bytes: u8,

    /// Payload bytes at or below which a value is served from the index, zero to opt out
    pub inline_max: u16,

    /// Bytes a sealed row carries of this column's values, zero to carry none
    pub row_carry: u16,

    /// Where a key says the record dies, and whether the write is placed by it too
    pub purge_mark: Option<PurgeMark>,

    /// Codec attempted on this column's payloads at admission, not promised
    pub codec: Codec,

    /// Which structure the resident index holds this column's keys in
    pub map_shape: MapShape,
}

/// Bytes a purge mark takes within a key
pub const MARK_LEN: usize = 8;

/// Where a column's keys say the record dies, and what the volume does with that
///
/// Placement is opt-in on the same offset because it costs open segments, which a
/// column purging by the mark and nothing else has no reason to pay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PurgeMark {
    /// Bytes into a key where the big endian u64 sits
    pub at: u8,

    /// Whether writes are placed by that mark as well as purged by it
    pub places: bool,
}

impl PurgeMark {
    /// A mark the volume purges by, placing nothing
    pub const fn at(at: u8) -> PurgeMark {
        PurgeMark { at, places: false }
    }

    /// The same mark, with writes banded by it as well
    pub const fn placing(at: u8) -> PurgeMark {
        PurgeMark { at, places: true }
    }

    /// Where this key sits on the purge timeline
    pub fn read(&self, key: &[u8]) -> u64 {
        let at = self.at as usize;
        // A key too short to carry the mark reads as the bottom, so it is purged.
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

    /// The mark this column's writes are placed by, for a column that asked for that
    pub fn placement_mark(&self) -> Option<PurgeMark> {
        self.purge_mark.filter(|mark| mark.places)
    }

    /// Number of index shards the column splits into
    pub fn shard_count(&self) -> usize {
        1usize << (8 * self.shard_bytes as usize)
    }

    /// Which shard a key belongs to, from its leading bytes
    ///
    /// A lookup may be handed a bound rather than a key, so bytes short of the
    /// shard prefix read as zero.
    pub fn shard_of(&self, key: &[u8]) -> usize {
        let mut shard = 0usize;
        for at in 0..self.shard_bytes as usize {
            let byte = key.get(at).copied().unwrap_or(0);
            shard = (shard << 8) | byte as usize;
        }
        shard
    }
}

impl ColumnSpec {
    /// Bytes this column's sealed rows carry of a value
    pub const fn row_carry_width(&self) -> u16 {
        self.row_carry
    }
}

pub type ColumnSet = &'static [ColumnSpec];

/// Resolve a column family name to its declaration
pub fn spec_by_name<'a>(columns: &'a [ColumnSpec], name: &str) -> Option<&'a ColumnSpec> {
    columns.iter().find(|spec| spec.name == name)
}

/// Resolve a column identifier to its declaration
pub fn spec_by_id(columns: &[ColumnSpec], id: ColumnId) -> Option<&ColumnSpec> {
    columns.iter().find(|spec| spec.id == id)
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
            inline_max: 0,
            row_carry: 0,
            purge_mark: None,
            codec: Codec::None,
            map_shape: MapShape::Tree,
        },
        ColumnSpec {
            id: ColumnId(2),
            name: "blob",
            key_width: KeyWidth::Fixed(32),
            shard_bytes: 0,
            inline_max: 0,
            row_carry: 0,
            purge_mark: None,
            codec: Codec::None,
            map_shape: MapShape::Tree,
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

    // keys order lexicographically over their used bytes, not their padding
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

    // placement answers only for a column that asked for it, off the same offset
    #[test]
    fn placement_is_the_same_fact() {
        let key = [0u8, 0, 0, 0, 0, 0, 0, 0, 0, 9];
        let purged = ColumnSpec {
            purge_mark: Some(PurgeMark::at(2)),
            ..spec()
        };
        let placed = ColumnSpec {
            purge_mark: Some(PurgeMark::placing(2)),
            ..spec()
        };

        assert_eq!(purged.mark_of(&key), Some(9));
        assert_eq!(purged.placement_mark(), None);
        assert_eq!(placed.mark_of(&key), Some(9));
        assert_eq!(placed.placement_mark().expect("mark").read(&key), 9);
        assert_eq!(spec().placement_mark(), None);
    }

    /// An unmarked declaration the mark tests vary one field of
    fn spec() -> ColumnSpec {
        ColumnSpec {
            id: ColumnId(1),
            name: "record",
            key_width: KeyWidth::Fixed(10),
            shard_bytes: 0,
            inline_max: 0,
            row_carry: 0,
            purge_mark: None,
            codec: Codec::None,
            map_shape: MapShape::Tree,
        }
    }

    // columns resolve by name and by identifier
    #[test]
    fn resolves_columns() {
        assert_eq!(
            spec_by_name(COLUMNS, "record").expect("record").id,
            ColumnId(1)
        );
        assert_eq!(spec_by_id(COLUMNS, ColumnId(2)).expect("blob").name, "blob");
        assert!(spec_by_name(COLUMNS, "absent").is_none());
        assert!(spec_by_id(COLUMNS, ColumnId(9)).is_none());
    }
}
