//! One per root: the store's identity, its layout and the segment counter

use std::io::{Error, ErrorKind, IoSlice, Result};
use std::path::Path;

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, FileOffset};
use xxhash_rust::xxh3::xxh3_64;

use crate::config::{ColumnConfig, KeyShape};
use crate::io::{Io, IoFile};
use crate::record::record_check::RecordKey;
use crate::segment::segment_tail::SegmentNumber;

/// Opens every superblock
pub const SUPERBLOCK_MAGIC: u32 = 0x5245_4C53;
/// The superblock's file name
pub const SUPERBLOCK_NAME: &str = "superblock";
/// The name a new superblock is written under before it replaces the old one
pub const NEW_SUPERBLOCK_NAME: &str = "superblock.new";
/// The stored byte of a fixed key shape
pub const FIXED_SHAPE: u8 = 0;
/// The stored byte of a variable key shape
pub const VARIABLE_SHAPE: u8 = 1;

/// The store's random id, the same in every root
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct StoreId(pub u64);

/// A column as the store remembers it
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredColumn {
    pub id: ColumnId,
    pub name: String,
    pub key_shape: KeyShape,
    pub volume_name: String,
}

/// A root's superblock
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Superblock {
    pub store_id: StoreId,
    pub record_key: RecordKey,
    pub next_segment: SegmentNumber,
    pub columns: Vec<StoredColumn>,
    pub dead_roots: Vec<String>,
}

/// Appends a superblock's fields to a buffer
#[derive(Debug, Default)]
pub struct FieldWriter {
    bytes: Vec<u8>,
}

/// Reads a superblock's fields from its bytes
#[derive(Debug)]
pub struct FieldReader<'bytes> {
    bytes: &'bytes [u8],
}

impl FieldWriter {
    /// Append a word
    pub fn word(&mut self, word: u64) {
        self.bytes.extend_from_slice(&word.to_le_bytes());
    }

    /// Append text with its length in front
    pub fn text(&mut self, text: &str) {
        self.word(text.len() as u64);
        self.bytes.extend_from_slice(text.as_bytes());
    }
}

impl<'bytes> FieldReader<'bytes> {
    /// Take a word
    pub fn word(&mut self) -> Option<u64> {
        let (word, rest) = self.bytes.split_at_checked(size_of::<u64>())?;
        self.bytes = rest;
        let mut array = [0; size_of::<u64>()];
        array.copy_from_slice(word);
        Some(u64::from_le_bytes(array))
    }

    /// Take text
    pub fn text(&mut self) -> Option<String> {
        let length = self.word()? as usize;
        let (text, rest) = self.bytes.split_at_checked(length)?;
        self.bytes = rest;
        String::from_utf8(text.to_vec()).ok()
    }
}

impl Superblock {
    /// Turn the superblock into its stored bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut fields = FieldWriter::default();
        fields.word(u64::from(SUPERBLOCK_MAGIC));
        fields.word(self.store_id.0);
        fields.word(self.record_key.0);
        fields.word(self.next_segment.0);
        fields.word(self.columns.len() as u64);
        for column in &self.columns {
            fields.word(u64::from(column.id.0));
            fields.text(&column.name);
            let (shape, width) = match column.key_shape {
                KeyShape::Fixed { width } => (FIXED_SHAPE, width),
                KeyShape::Variable { most } => (VARIABLE_SHAPE, most),
            };
            fields.word(u64::from(shape));
            fields.word(u64::from(width));
            fields.text(&column.volume_name);
        }
        fields.word(self.dead_roots.len() as u64);
        for root in &self.dead_roots {
            fields.text(root);
        }

        let sum = xxh3_64(&fields.bytes);
        fields.word(sum);
        fields.bytes
    }

    /// Read a superblock from its stored bytes, or nothing when they fail their check
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let sum_at = bytes.len().checked_sub(size_of::<u64>())?;
        let mut tail = FieldReader { bytes: &bytes[sum_at..] };
        if xxh3_64(&bytes[..sum_at]) != tail.word()? {
            return None;
        }

        let mut fields = FieldReader { bytes: &bytes[..sum_at] };
        if fields.word()? != u64::from(SUPERBLOCK_MAGIC) {
            return None;
        }
        let store_id = StoreId(fields.word()?);
        let record_key = RecordKey(fields.word()?);
        let next_segment = SegmentNumber(fields.word()?);

        let column_count = fields.word()?;
        let mut columns = Vec::new();
        for _column in 0..column_count {
            let id = ColumnId(fields.word()? as u8);
            let name = fields.text()?;
            let shape = fields.word()? as u8;
            let width = fields.word()? as u16;
            let key_shape = match shape {
                FIXED_SHAPE => KeyShape::Fixed { width },
                VARIABLE_SHAPE => KeyShape::Variable { most: width },
                _unknown => return None,
            };
            let volume_name = fields.text()?;
            columns.push(StoredColumn {
                id,
                name,
                key_shape,
                volume_name,
            });
        }

        let dead_count = fields.word()?;
        let mut dead_roots = Vec::new();
        for _root in 0..dead_count {
            dead_roots.push(fields.text()?);
        }
        Some(Self {
            store_id,
            record_key,
            next_segment,
            columns,
            dead_roots,
        })
    }

    /// Read a root's superblock, or nothing when the root has none
    pub fn read<Backend: Io>(io: &Backend, root: &Path) -> Result<Option<Self>> {
        let file = match io.open_read_only(&root.join(SUPERBLOCK_NAME)) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };

        let length = file.length()?;
        let mut bytes = vec![0; length.0 as usize];
        file.read_exact_at(FileOffset(0), &mut bytes)?;
        io.close_file(file);
        match Self::from_bytes(&bytes) {
            Some(superblock) => Ok(Some(superblock)),
            None => Err(Error::from(ErrorKind::InvalidData)),
        }
    }

    /// Replace a root's superblock so a crash leaves the old one or the new one whole
    pub fn write<Backend: Io>(&self, io: &Backend, root: &Path) -> Result<()> {
        let new_path = root.join(NEW_SUPERBLOCK_NAME);
        let path = root.join(SUPERBLOCK_NAME);
        match io.remove_file(&new_path) {
            Ok(()) => {},
            Err(error) if error.kind() == ErrorKind::NotFound => {},
            Err(error) => return Err(error),
        }

        let file = io.create_file(&new_path, ByteCount(0))?;
        let bytes = self.to_bytes();
        file.write_at(FileOffset(0), &[IoSlice::new(&bytes)])?;
        file.sync()?;
        io.close_file(file);
        io.rename_file(&new_path, &path)?;
        io.sync_directory(root)
    }
}

/// Build the stored form of a declared column
pub fn stored_column(column: &ColumnConfig, volume_name: &str) -> StoredColumn {
    StoredColumn {
        id: column.id,
        name: column.name.clone(),
        key_shape: column.key_shape,
        volume_name: volume_name.to_string(),
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn superblock() -> Superblock {
        Superblock {
            store_id: StoreId(5),
            record_key: RecordKey(6),
            next_segment: SegmentNumber(700),
            columns: vec![
                StoredColumn {
                    id: ColumnId(1),
                    name: "track".to_string(),
                    key_shape: KeyShape::Fixed { width: 32 },
                    volume_name: "meta".to_string(),
                },
                StoredColumn {
                    id: ColumnId(2),
                    name: "slice".to_string(),
                    key_shape: KeyShape::Variable { most: 98 },
                    volume_name: "bulk".to_string(),
                },
            ],
            dead_roots: vec!["/mnt/gone".to_string()],
        }
    }

    // a superblock reads back field for field
    #[test]
    fn superblock_round() {
        let bytes = superblock().to_bytes();

        assert_eq!(Superblock::from_bytes(&bytes), Some(superblock()));
    }

    // a flipped byte and a cut file are refused
    #[test]
    fn superblock_rot() {
        let mut bytes = superblock().to_bytes();

        assert_eq!(Superblock::from_bytes(&bytes[..bytes.len() - 1]), None);
        bytes[12] ^= 1;
        assert_eq!(Superblock::from_bytes(&bytes), None);
    }
}
