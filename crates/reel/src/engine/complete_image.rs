//! A complete table saved at a clean close, so the next open reads it and the footers of what it doesn't cover
//!
//! The file lists the segments it covers, so nothing past them is taken on trust. Open removes it before any write, and an open
//! after a crash finds none and reads every row block. Close allocates nothing, so the buffer and names come from open.

use std::io::{Error, ErrorKind, IoSlice, Result};
use std::path::{Path, PathBuf};

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, FileOffset};

use crate::engine::engine_state::EngineState;
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::index::complete_table::{COMPLETE_SHARDS, CompleteTable, SLOT_BYTES};
use crate::io::replace::move_into_place;
use crate::io::{Io, IoFile};
use crate::little_endian::read_word;
use crate::pool::touched_zeroes;
use crate::record::record_check::RunningSum;
use crate::segment::segment_tail::SegmentNumber;

/// The first bytes of an image
pub const IMAGE_MAGIC: u64 = 0x5245_454C_494D_4732;
/// Bytes of an image's header: magic, column, slots and overflow a shard, used slots and covered segments
pub const IMAGE_HEAD_BYTES: usize = 6 * size_of::<u64>();
/// Bytes an image's sum and writes go by. Every part of an image is a whole number of words, so none splits across these
pub const IMAGE_CHUNK_BYTES: usize = 1024 * 1024;

/// What a close needs to save a column's table
#[derive(Debug)]
pub struct ImageFiles {
    pub path: PathBuf,
    pub new_path: PathBuf,
    pub chunk: Vec<u8>,
}

/// An image on its way to disk, a chunk at a time
#[derive(Debug)]
pub struct ImageWriter<'file, File> {
    file: &'file File,
    chunk: &'file mut [u8],
    used: usize,
    at: u64,
    sum: RunningSum,
    outcome: Result<()>,
}

impl ImageFiles {
    /// Name a sorted column's image files and allocate the buffer
    pub fn sorted(root: &Path, column: ColumnId) -> Self {
        Self {
            path: root.join(format!("sorted-{}.image", column.0)),
            new_path: root.join(format!("sorted-{}.image.new", column.0)),
            chunk: touched_zeroes(IMAGE_CHUNK_BYTES),
        }
    }

    /// Name a column's files and allocate the buffer
    pub fn new(root: &Path, column: ColumnId) -> Self {
        Self {
            path: root.join(format!("complete-{}.image", column.0)),
            new_path: root.join(format!("complete-{}.image.new", column.0)),
            chunk: touched_zeroes(IMAGE_CHUNK_BYTES),
        }
    }
}

impl<'file, File: IoFile> ImageWriter<'file, File> {
    /// Start an image at the file's first byte, through a caller's chunk
    pub fn new(file: &'file File, chunk: &'file mut [u8]) -> Self {
        Self { file, chunk, used: 0, at: 0, sum: RunningSum::new(), outcome: Ok(()) }
    }

    /// Add one word, sending the chunk when it fills. The first failure stands
    pub fn put(&mut self, bytes: &[u8]) {
        self.chunk[self.used..self.used + bytes.len()].copy_from_slice(bytes);
        self.used += bytes.len();
        if self.used == self.chunk.len() {
            self.flush();
        }
    }

    /// Send what the chunk holds
    pub fn flush(&mut self) {
        if self.used > 0 && self.outcome.is_ok() {
            let bytes = &self.chunk[..self.used];
            self.sum.add(bytes);
            self.outcome = self.file.write_at(FileOffset(self.at), &[IoSlice::new(bytes)]);
            self.at += self.used as u64;
        }
        self.used = 0;
    }

    /// Send the rest and the sum, and sync the file
    pub fn finish(mut self) -> Result<()> {
        self.flush();
        self.outcome?;
        self.file.write_at(FileOffset(self.at), &[IoSlice::new(&self.sum.sum().0.to_le_bytes())])?;
        self.file.sync()
    }
}

impl<Backend: Io> EngineState<Backend> {
    /// Remove the images of columns that have no slot of that kind now: a column sorted last run and plain or complete now left
    /// one, and nothing would ever read or remove it. The read point refuses such a file anyway, so this only gives the space back
    pub fn remove_stale_images(&self) -> Result<()> {
        for volume in &self.volumes {
            let Some(root) = volume.roots.first() else {
                continue;
            };
            let mut has_removed = false;
            for path in self.io.list_directory(root)? {
                let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                    continue;
                };
                let is_held = self.complete_images.iter().flatten().any(|files| {
                    let files = hold(files);
                    // by name, since a backend may list a path in another form than the root it was given joined
                    let held = |slot: &PathBuf| slot.file_name() == path.file_name();
                    held(&files.path) || held(&files.new_path)
                });
                let is_image = (name.starts_with("sorted-") || name.starts_with("complete-")) && (name.ends_with(".image") || name.ends_with(".image.new"));
                if is_image && !is_held {
                    match self.io.remove_file(&path) {
                        Ok(()) => has_removed = true,
                        Err(error) if error.kind() == ErrorKind::NotFound => {},
                        Err(error) => return Err(error),
                    }
                }
            }
            if has_removed {
                self.io.sync_directory(root)?;
            }
        }
        Ok(())
    }

    /// Save every complete table of a volume. A table with a mark rebuilds at open, so it saves nothing
    pub fn save_images(&self, index: usize) -> Result<()> {
        let volume = &self.volumes[index];
        for (id, table) in self.complete.iter().enumerate() {
            let is_here = self.columns[id].is_some_and(|info| info.volume.0 as usize == index);
            if let (Some(table), Some(files)) = (table.as_ref().filter(|table| is_here && table.mark().is_none()), &self.complete_images[id]) {
                self.save_image(volume, ColumnId(id as u8), table, &mut hold(files))?;
            }
        }
        Ok(())
    }

    /// Write one table to a new file, sync it, and give it its name
    pub fn save_image(&self, volume: &Volume<Backend>, column: ColumnId, table: &CompleteTable, files: &mut ImageFiles) -> Result<()> {
        let mut segments = 0;
        volume.table.for_each_segment(|_number, _root, _file| segments += 1);
        let length = IMAGE_HEAD_BYTES + segments * size_of::<u64>() + table.slot_count() * SLOT_BYTES + size_of::<u64>();
        let file = self.io.create_file(&files.new_path, ByteCount(length as u64))?;
        let mut writer = ImageWriter {
            file: &file,
            chunk: &mut files.chunk,
            used: 0,
            at: 0,
            sum: RunningSum::new(),
            outcome: Ok(()),
        };
        let (slots, overflow) = table.shard_sizes();
        for word in [IMAGE_MAGIC, u64::from(column.0), slots as u64, overflow as u64, table.used_slots().0, segments as u64] {
            writer.put(&word.to_le_bytes());
        }
        volume.table.for_each_segment(|number, _root, _file| writer.put(&number.0.to_le_bytes()));
        for shard in 0..COMPLETE_SHARDS {
            table.visit_slots(shard, |slot| writer.put(&slot.to_bytes()));
        }
        let written = writer.finish();
        self.io.close_file(file);
        written?;
        move_into_place(&self.io, &volume.roots[0], &files.new_path, &files.path)
    }

    /// Read a column's saved image into its table and remove it, and return the segments it covers, sorted. Anything wrong with
    /// the file leaves the table empty, and open reads every row block
    pub fn take_image(&self, volume: &Volume<Backend>, column: ColumnId, table: &CompleteTable) -> Result<Vec<SegmentNumber>> {
        let Some(files) = &self.complete_images[column.0 as usize] else {
            return Ok(Vec::new());
        };
        let files = hold(files);
        // a close that stopped partway leaves this, and the next close needs the name
        let _removed = self.io.remove_file(&files.new_path);
        let file = match self.io.open_read_only(&files.path) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let read = read_image(&file, column, table);
        self.io.close_file(file);
        // an image that stayed could outlive the writes that follow
        self.io.remove_file(&files.path)?;
        self.io.sync_directory(&volume.roots[0])?;
        match read {
            Ok(numbers) => Ok(numbers),
            Err(error) if error.kind() == ErrorKind::InvalidData => {
                table.clear();
                Ok(Vec::new())
            },
            Err(error) => Err(error),
        }
    }
}

/// Check an image's sum, then read its head, its segments and its slots into a table, and return the segments sorted
pub fn read_image<File: IoFile>(file: &File, column: ColumnId, table: &CompleteTable) -> Result<Vec<SegmentNumber>> {
    let bad = || Error::from(ErrorKind::InvalidData);
    let length = file.length()?.0;
    let Some(body) = length.checked_sub(size_of::<u64>() as u64) else {
        return Err(bad());
    };
    let mut chunk = vec![0; IMAGE_CHUNK_BYTES];
    let (mut at, mut sum) = (0, RunningSum::new());
    while at < body {
        let part = (body - at).min(IMAGE_CHUNK_BYTES as u64) as usize;
        file.read_exact_at(FileOffset(at), &mut chunk[..part]).map_err(|_error| bad())?;
        sum.add(&chunk[..part]);
        at += part as u64;
    }
    let mut stored = [0; size_of::<u64>()];
    file.read_exact_at(FileOffset(body), &mut stored).map_err(|_error| bad())?;
    if u64::from_le_bytes(stored) != sum.sum().0 {
        return Err(bad());
    }

    let mut head = [0; IMAGE_HEAD_BYTES];
    file.read_exact_at(FileOffset(0), &mut head).map_err(|_error| bad())?;
    let word = |index: usize| read_word(&head, index * size_of::<u64>()).ok_or_else(bad);
    let (slots, overflow) = table.shard_sizes();
    if word(0)? != IMAGE_MAGIC || word(1)? != u64::from(column.0) || word(2)? != slots as u64 || word(3)? != overflow as u64 {
        return Err(bad());
    }
    let (used, count) = (word(4)?, word(5)? as usize);

    let mut at = IMAGE_HEAD_BYTES as u64;
    // the count comes off the disk, so the list it sizes has to fit the file it was read from
    let listed_bytes = count.checked_mul(size_of::<u64>()).filter(|bytes| IMAGE_HEAD_BYTES as u64 + *bytes as u64 <= length).ok_or_else(bad)?;
    let mut listed = vec![0; listed_bytes];
    file.read_exact_at(FileOffset(at), &mut listed).map_err(|_error| bad())?;
    at += listed.len() as u64;
    for shard in 0..COMPLETE_SHARDS {
        for part in 0..2 {
            let mut filled = 0;
            let length = table.part_length(shard, part);
            while filled < length {
                let slots = (length - filled).min(IMAGE_CHUNK_BYTES / SLOT_BYTES);
                let bytes = &mut chunk[..slots * SLOT_BYTES];
                file.read_exact_at(FileOffset(at), bytes).map_err(|_error| bad())?;
                at += bytes.len() as u64;
                table.fill_part(shard, part, filled, bytes);
                filled += slots;
            }
        }
    }
    table.finish_fill(used);
    let mut numbers: Vec<SegmentNumber> = listed.chunks_exact(size_of::<u64>()).filter_map(|bytes| read_word(bytes, 0)).map(SegmentNumber).collect();
    numbers.sort_unstable();
    Ok(numbers)
}
