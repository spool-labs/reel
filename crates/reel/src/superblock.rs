//! One per root: the store's identity, its layout, the segment counter and the write order's limit

use std::io::{Error, ErrorKind, IoSlice, Result};
use std::path::{Path, PathBuf};

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, FileOffset, WritePlace};
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

/// A volume as the store remembers it
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredVolume {
    pub name: String,
    pub roots: Vec<String>,
}

/// A root's paths, built once at open so a save allocates nothing
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SuperblockPaths {
    pub root: PathBuf,
    pub current: PathBuf,
    pub new: PathBuf,
}

/// A root's superblock
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Superblock {
    pub store_id: StoreId,
    pub record_key: RecordKey,
    pub revision: u64,
    pub next_segment: SegmentNumber,
    pub place_limit: WritePlace,
    pub volumes: Vec<StoredVolume>,
    pub columns: Vec<StoredColumn>,
    pub dead_roots: Vec<String>,
}

/// Appends a superblock's fields to a buffer
#[derive(Debug)]
pub struct FieldWriter<'bytes> {
    bytes: &'bytes mut Vec<u8>,
}

/// Reads a superblock's fields from its bytes
#[derive(Debug)]
pub struct FieldReader<'bytes> {
    bytes: &'bytes [u8],
}

impl SuperblockPaths {
    /// Build a root's paths
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            current: root.join(SUPERBLOCK_NAME),
            new: root.join(NEW_SUPERBLOCK_NAME),
        }
    }
}

impl FieldWriter<'_> {
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
    /// Lay the superblock out in a buffer the caller keeps, so a save allocates nothing
    pub fn write_bytes(&self, out: &mut Vec<u8>) {
        out.clear();
        let mut fields = FieldWriter { bytes: out };
        fields.word(u64::from(SUPERBLOCK_MAGIC));
        fields.word(self.store_id.0);
        fields.word(self.record_key.0);
        fields.word(self.revision);
        fields.word(self.next_segment.0);
        fields.word(self.place_limit.0);
        fields.word(self.volumes.len() as u64);
        for volume in &self.volumes {
            fields.text(&volume.name);
            fields.word(volume.roots.len() as u64);
            for root in &volume.roots {
                fields.text(root);
            }
        }
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

        let sum = xxh3_64(fields.bytes);
        fields.word(sum);
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
        let revision = fields.word()?;
        let next_segment = SegmentNumber(fields.word()?);
        let place_limit = WritePlace(fields.word()?);

        let volume_count = fields.word()?;
        let mut volumes = Vec::new();
        for _volume in 0..volume_count {
            let name = fields.text()?;
            let root_count = fields.word()?;
            let mut roots = Vec::new();
            for _root in 0..root_count {
                roots.push(fields.text()?);
            }
            volumes.push(StoredVolume { name, roots });
        }

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
            revision,
            next_segment,
            place_limit,
            volumes,
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

        let bytes = read_whole(&file);
        io.close_file(file);
        match Self::from_bytes(&bytes?) {
            Some(superblock) => Ok(Some(superblock)),
            None => Err(Error::from(ErrorKind::InvalidData)),
        }
    }

    /// Replace a root's superblock so a crash leaves the old one or the new one whole
    pub fn write<Backend: Io>(&self, io: &Backend, paths: &SuperblockPaths, bytes: &mut Vec<u8>) -> Result<()> {
        self.write_bytes(bytes);
        Self::write_file(io, paths, bytes)
    }

    /// Replace a root's superblock with bytes laid out already, which needs no lock on the superblock
    pub fn write_file<Backend: Io>(io: &Backend, paths: &SuperblockPaths, bytes: &[u8]) -> Result<()> {
        match io.remove_file(&paths.new) {
            Ok(()) => {},
            Err(error) if error.kind() == ErrorKind::NotFound => {},
            Err(error) => return Err(error),
        }

        let file = io.create_file(&paths.new, ByteCount(0))?;
        let written = file.write_at(FileOffset(0), &[IoSlice::new(bytes)]);
        let synced = written.and_then(|()| file.sync());
        io.close_file(file);
        synced?;
        io.rename_file(&paths.new, &paths.current)?;
        io.sync_directory(&paths.root)
    }
}

/// Read a file from end to end
pub fn read_whole<File: IoFile>(file: &File) -> Result<Vec<u8>> {
    let length = file.length()?;
    let mut bytes = vec![0; length.0 as usize];
    file.read_exact_at(FileOffset(0), &mut bytes)?;
    Ok(bytes)
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
