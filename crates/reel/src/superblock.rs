//! One per root: the store's identity, its layout, the segment counter and the write order's limit

use std::io::{Error, ErrorKind, IoSlice, Result};
use std::path::{Path, PathBuf};

use reel_core::column_id::ColumnId;
use reel_core::units::{FileOffset, WritePlace};
use xxhash_rust::xxh3::xxh3_64;

use crate::config::{ColumnConfig, KeyShape};
use crate::io::replace::replace_whole;
use crate::io::device_error::is_device_error;
use crate::io::{Io, IoFile};
use crate::little_endian::ByteReader;
use crate::record::record_check::RecordKey;
use crate::segment::segment_tail::SegmentNumber;

/// Opens every superblock. It changed when one-chunk records began keeping their payload's sum in the head
pub const SUPERBLOCK_MAGIC: u32 = 0x5245_4C32;
/// Opened a superblock of a store written before records kept their payload's sum, which this build refuses to open
pub const OLDER_SUPERBLOCK_MAGIC: u32 = 0x5245_4C53;
/// The superblock's file name
pub const SUPERBLOCK_NAME: &str = "superblock";
/// The name a new superblock is written under before it replaces the old one
pub const NEW_SUPERBLOCK_NAME: &str = "superblock.new";
/// The second copy's file name
pub const SECOND_SUPERBLOCK_NAME: &str = "superblock.second";
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
    pub second: PathBuf,
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

/// What a read of one copy found
#[derive(Debug)]
pub enum CopyRead {
    /// There's no such file
    Absent,
    /// The file is there and fails its check or can't be read
    Failed,
    /// The copy passes its check
    Passed(Superblock),
}

/// A root's two copies as they read
#[derive(Debug)]
pub struct RootCopies {
    pub copies: [CopyRead; 2],
}

/// Appends a superblock's fields to a buffer
#[derive(Debug)]
pub struct FieldWriter<'bytes> {
    bytes: &'bytes mut Vec<u8>,
}

impl SuperblockPaths {
    /// Build a root's paths
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            current: root.join(SUPERBLOCK_NAME),
            second: root.join(SECOND_SUPERBLOCK_NAME),
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

impl ByteReader<'_> {
    /// Take a superblock's text: its length, then its bytes
    pub fn text(&mut self) -> Option<String> {
        let length = usize::try_from(self.word()?).ok()?;
        String::from_utf8(self.take(length)?.to_vec()).ok()
    }
}

impl Superblock {
    /// Lay the superblock out in a new buffer, which open keeps so a save allocates nothing
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        self.write_bytes(&mut bytes);
        bytes
    }

    /// Lay the superblock out in a buffer the caller keeps
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
        let mut tail = ByteReader::new(&bytes[sum_at..]);
        if xxh3_64(&bytes[..sum_at]) != tail.word()? {
            return None;
        }

        let mut fields = ByteReader::new(&bytes[..sum_at]);
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
            let id = ColumnId(u8::try_from(fields.word()?).ok()?);
            let name = fields.text()?;
            let shape = u8::try_from(fields.word()?).ok()?;
            let width = u16::try_from(fields.word()?).ok()?;
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

    /// Read a root's newest copy that passes its check, or nothing when none does
    pub fn read<Backend: Io>(io: &Backend, root: &Path) -> Result<Option<Self>> {
        Ok(RootCopies::read(io, root)?.newest())
    }

    /// Read one copy, or nothing when it's absent, and say so when it's there and fails
    pub fn read_copy<Backend: Io>(io: &Backend, path: &Path) -> Result<CopyRead> {
        let file = match io.open_read_only(path) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(CopyRead::Absent),
            Err(error) => return Err(error),
        };

        let bytes = read_whole(&file);
        io.close_file(file);
        let bytes = match bytes {
            Ok(bytes) => bytes,
            Err(error) if is_device_error(&error) => return Ok(CopyRead::Failed),
            Err(error) => return Err(error),
        };
        // an older store's records lack the payload sum this build reads, so it is refused with no migration
        if bytes.get(..size_of::<u64>()).is_some_and(|word| word == u64::from(OLDER_SUPERBLOCK_MAGIC).to_le_bytes()) {
            return Err(Error::new(ErrorKind::Unsupported, "the store was written before records kept their payload sum, which this build can't read"));
        }
        Ok(Self::from_bytes(&bytes).map_or(CopyRead::Failed, CopyRead::Passed))
    }

    /// Replace a root's superblock so a crash leaves the old one or the new one whole
    pub fn write<Backend: Io>(&self, io: &Backend, paths: &SuperblockPaths, bytes: &mut Vec<u8>) -> Result<()> {
        self.write_bytes(bytes);
        Self::write_file(io, paths, bytes)
    }

    /// Replace a root's two copies with bytes laid out already, which needs no lock on the superblock, the
    /// second after the first is synced, so a crash leaves one whole
    pub fn write_file<Backend: Io>(io: &Backend, paths: &SuperblockPaths, bytes: &[u8]) -> Result<()> {
        Self::write_copy(io, paths, &paths.current, bytes)?;
        Self::write_copy(io, paths, &paths.second, bytes)
    }

    /// Replace one copy through the new file
    pub fn write_copy<Backend: Io>(io: &Backend, paths: &SuperblockPaths, target: &Path, bytes: &[u8]) -> Result<()> {
        replace_whole(io, &paths.root, &paths.new, target, &[IoSlice::new(bytes)])
    }
}

/// The most bytes a superblock copy can take, far past any store's, so a stray file can't size an allocation
pub const MOST_SUPERBLOCK_BYTES: u64 = 4 * 1024 * 1024;

/// Read a file from end to end
pub fn read_whole<File: IoFile>(file: &File) -> Result<Vec<u8>> {
    let length = file.length()?;
    if length.0 > MOST_SUPERBLOCK_BYTES {
        return Err(Error::from(ErrorKind::InvalidData));
    }
    let mut bytes = vec![0; length.0 as usize];
    file.read_exact_at(FileOffset(0), &mut bytes)?;
    Ok(bytes)
}

impl RootCopies {
    /// Read both copies
    pub fn read<Backend: Io>(io: &Backend, root: &Path) -> Result<Self> {
        let paths = SuperblockPaths::new(root);
        Ok(Self {
            copies: [Superblock::read_copy(io, &paths.current)?, Superblock::read_copy(io, &paths.second)?],
        })
    }

    /// Walk the copies that pass their check
    pub fn passed(&self) -> impl Iterator<Item = &Superblock> {
        self.copies.iter().filter_map(|copy| match copy {
            CopyRead::Passed(superblock) => Some(superblock),
            CopyRead::Absent | CopyRead::Failed => None,
        })
    }

    /// Check any copy is there and fails
    pub fn has_failed(&self) -> bool {
        self.copies.iter().any(|copy| matches!(copy, CopyRead::Failed))
    }

    /// Take the newest copy that passes
    pub fn newest(self) -> Option<Superblock> {
        let [first, second] = self.copies;
        match (first, second) {
            (CopyRead::Passed(first), CopyRead::Passed(second)) if second.revision > first.revision => Some(second),
            (CopyRead::Passed(first), _second) => Some(first),
            (_first, CopyRead::Passed(second)) => Some(second),
            (_first, _second) => None,
        }
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
