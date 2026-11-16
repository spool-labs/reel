//! A sorted index saved at a clean close, so the next open reads its pages and fills nothing
//!
//! The file holds the index's shape, the read point at close, each segment it covers with that segment's highest place and seal
//! stamp, its free chain, each part's tree, and then the pool's pages up to the highest page ever taken. Pages past that were
//! never written, and the index open builds keeps their chain. Open reads the file into the index, removes it and syncs the
//! directory before any write, so no image outlives the open that took it. A file of another shape, another read point or other
//! segments, a short one, a long one or one whose sum fails leaves the index empty, and open reads every row block as before.
//! The save and the read both go through the one chunk `ImageFiles` holds from open, so neither allocates

use std::io::{Error, ErrorKind, Result};

use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, FileOffset};

use crate::engine::complete_image::{IMAGE_CHUNK_BYTES, ImageFiles, ImageWriter};
use crate::engine::engine_state::EngineState;
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::index::sorted_keys::{ImageHead, PAGE_BYTES, PARTS, PartTree, SortedKeys};
use crate::io::replace::move_into_place;
use crate::io::{Io, IoFile};
use crate::little_endian::read_word;
use crate::record::record_check::RunningSum;
use crate::segment::segment_tail::SegmentNumber;

/// The first bytes of a sorted index's image. A change to the format takes a new one
pub const SORTED_IMAGE_MAGIC: u64 = 0x5245_454C_534F_5254;
/// Words of the head before the part trees: magic, column, width, pages in the pool, pages saved, free head, free count, entries,
/// the read point at close and the covered segments
pub const SORTED_HEAD_WORDS: usize = 10;
/// Words of one part's tree: root, levels, first leaf, last leaf and entries
pub const PART_WORDS: usize = 5;
/// Words of one covered segment: its number, its highest place and its seal stamp
pub const LISTED_WORDS: usize = 3;
/// Bytes of the head with every part's tree
pub const SORTED_HEAD_BYTES: usize = (SORTED_HEAD_WORDS + PARTS * PART_WORDS) * size_of::<u64>();

/// One covered segment as the image lists it
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ListedSegment {
    pub number: SegmentNumber,
    pub highest: u64,
    pub seal_mark: u64,
}

/// Where an image's pages start: past the head and the list, on a page boundary, so no page splits across a chunk
pub fn pages_at(segments: usize) -> usize {
    (SORTED_HEAD_BYTES + segments * LISTED_WORDS * size_of::<u64>()).next_multiple_of(PAGE_BYTES)
}

impl<Backend: Io> EngineState<Backend> {
    /// Save every sorted index of a volume. An index with a mark rebuilds at open, so it saves nothing, and a short disk saves
    /// nothing either. Each saved image's bytes count in the report
    pub fn save_sorted_images(&self, index: usize) -> Result<()> {
        let volume = &self.volumes[index];
        // a merge that could not move an entry left the index behind its rows, so it rebuilds at open and saves nothing
        if volume.is_short_of_room() || self.sorted_moves_missed.load(std::sync::atomic::Ordering::Acquire) > 0 {
            return Ok(());
        }
        for (id, sorted) in self.sorted.iter().enumerate() {
            let is_here = self.columns[id].is_some_and(|info| info.volume.0 as usize == index);
            if let (Some(sorted), Some(files)) = (sorted.as_ref().filter(|sorted| is_here && sorted.mark().is_none()), &self.complete_images[id]) {
                // a failed save costs the next open its fast fill and no more, so it counts and the next column still saves
                match self.save_sorted_image(volume, ColumnId(id as u8), sorted, &mut hold(files)) {
                    Ok(written) => {
                        self.sorted_image_bytes.fetch_add(written, std::sync::atomic::Ordering::Relaxed);
                    },
                    Err(_error) => {
                        self.sorted_image_saves_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    },
                }
            }
        }
        Ok(())
    }

    /// Write one index to a new file, sync it, give it its name and sync the directory, and return the bytes written
    pub fn save_sorted_image(&self, volume: &Volume<Backend>, column: ColumnId, sorted: &SortedKeys, files: &mut ImageFiles) -> Result<u64> {
        let rows = hold(&volume.summary);
        let segments = rows.orders().len();
        let head = sorted.image_head();
        let start = pages_at(segments);
        let length = start + head.saved as usize * PAGE_BYTES + size_of::<u64>();
        let file = self.io.create_file(&files.new_path, ByteCount(length as u64))?;
        let mut writer = ImageWriter::new(&file, &mut files.chunk);
        // the newest place any listed segment holds, as the summary gives it on both sides: the read point moves to a new base at
        // every open, and a seal raises the volume's sealed place past its rows
        let sealed = newest_listed(&rows);
        let counts = [
            SORTED_IMAGE_MAGIC,
            u64::from(column.0),
            head.width as u64,
            u64::from(head.pages),
            u64::from(head.saved),
            u64::from(head.free_head),
            u64::from(head.free_count),
            head.entries,
            sealed,
            segments as u64,
        ];
        for word in counts {
            writer.put(&word.to_le_bytes());
        }
        for tree in &head.parts {
            for word in [u64::from(tree.root), tree.levels as u64, u64::from(tree.first_leaf), u64::from(tree.last_leaf), tree.entries] {
                writer.put(&word.to_le_bytes());
            }
        }
        let mut listed = 0;
        for row in rows.orders().present().iter().filter_map(|number| rows.get(*number)) {
            for word in [row.facts.segment.0, row.facts.places.highest.0, row.facts.seal_mark.0] {
                writer.put(&word.to_le_bytes());
            }
            listed += 1;
        }
        drop(rows);
        // an order with no row would leave the list short of the count the head gives, and the next open would refuse the
        // image on its length
        if listed != segments {
            self.io.close_file(file);
            let _removed = self.io.remove_file(&files.new_path);
            return Ok(0);
        }
        #[cfg(feature = "test-hooks")]
        if volume.holds.sorted_image_fails.load(std::sync::atomic::Ordering::Acquire) {
            self.io.close_file(file);
            return Err(Error::other("a test failed the image write"));
        }
        for _pad in (SORTED_HEAD_BYTES + segments * LISTED_WORDS * size_of::<u64>()..start).step_by(size_of::<u64>()) {
            writer.put(&0_u64.to_le_bytes());
        }
        let mut page = [0; PAGE_BYTES];
        for number in 0..head.saved {
            sorted.page_bytes(number, &mut page);
            writer.put(&page);
        }
        let written = writer.finish();
        self.io.close_file(file);
        written?;
        move_into_place(&self.io, &volume.roots[0], &files.new_path, &files.path)?;
        Ok(length as u64)
    }

    /// Read a column's saved image into its index and remove it, and say whether the index took it. Anything wrong with the file
    /// leaves the index empty and counts a refusal, and open reads every row block. The caller runs the cover pass after this
    /// whatever it says
    pub fn take_sorted_image(&self, volume: &Volume<Backend>, column: ColumnId, sorted: &SortedKeys) -> Result<bool> {
        let Some(files) = &self.complete_images[column.0 as usize] else {
            return Ok(false);
        };
        let mut files = hold(files);
        // a close that stopped partway leaves this, and the next close needs the name
        let _removed = self.io.remove_file(&files.new_path);
        let file = match self.io.open_read_only(&files.path) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        // an index that took a page or an entry already would run the saved chain into pages it wrote
        let is_pristine = sorted.is_pristine();
        let read = if is_pristine {
            let rows = hold(&volume.summary);
            let opened_at = newest_listed(&rows);
            let is_shape_here = |sealed: u64, count: usize| sealed == opened_at && rows.orders().len() == count;
            let matches = |listed: ListedSegment| {
                rows.get(listed.number).is_some_and(|row| row.facts.places.highest.0 == listed.highest && row.facts.seal_mark.0 == listed.seal_mark)
            };
            read_sorted_image(&file, column, sorted, &mut files.chunk, (&is_shape_here, &matches))
        } else {
            Err(Error::from(ErrorKind::InvalidData))
        };
        self.io.close_file(file);
        // an image that stayed could outlive the writes that follow
        self.io.remove_file(&files.path)?;
        self.io.sync_directory(&volume.roots[0])?;
        match read {
            Ok(()) => {
                self.sorted_images_taken.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(true)
            },
            Err(error) if error.kind() == ErrorKind::InvalidData => {
                // a refused image may have filled pages of a pristine index, and a used one was never touched
                if is_pristine {
                    sorted.reset();
                }
                self.sorted_images_refused.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(false)
            },
            Err(error) => Err(error),
        }
    }

}

/// Read an image's head, check its shape and its length, check the read point and the listed segments against the volume, then
/// read the file once through the chunk: the sum over every byte, and each saved page into the index. The head goes in only once
/// the sum holds, and a failed sum leaves pages the caller resets
pub fn read_sorted_image<File: IoFile>(
    file: &File,
    column: ColumnId,
    sorted: &SortedKeys,
    chunk: &mut [u8],
    (is_shape_here, matches): (&impl Fn(u64, usize) -> bool, &impl Fn(ListedSegment) -> bool),
) -> Result<()> {
    let bad = || Error::from(ErrorKind::InvalidData);
    // an index that took a page or an entry already would run the saved chain into pages it wrote
    if !sorted.is_pristine() {
        return Err(bad());
    }
    let length = file.length()?.0;
    let head_bytes = &mut chunk[..SORTED_HEAD_BYTES];
    file.read_exact_at(FileOffset(0), head_bytes).map_err(|_error| bad())?;
    let mut head_words = [0_u64; SORTED_HEAD_WORDS];
    for (index, word) in head_words.iter_mut().enumerate() {
        *word = read_word(head_bytes, index * size_of::<u64>()).ok_or_else(bad)?;
    }
    let [magic, image_column, width, pages, saved, free_head, free_count, entries, sealed, count] = head_words;
    // the pool may be another size than the one saved, as long as it holds every saved page
    let is_shape = magic == SORTED_IMAGE_MAGIC && image_column == u64::from(column.0) && width == sorted.width() as u64 && saved <= u64::from(sorted.page_count());
    if !is_shape || saved > pages {
        return Err(bad());
    }
    let mut parts = [PartTree::empty(); PARTS];
    for (part, tree) in parts.iter_mut().enumerate() {
        let at = (SORTED_HEAD_WORDS + part * PART_WORDS) * size_of::<u64>();
        let word = |index: usize| read_word(head_bytes, at + index * size_of::<u64>()).ok_or_else(bad);
        let page = |index: usize| word(index).and_then(|value| u32::try_from(value).map_err(|_error| bad()));
        *tree = PartTree { root: page(0)?, levels: word(1)? as usize, first_leaf: page(2)?, last_leaf: page(3)?, entries: word(4)? };
    }
    // the count comes off the disk, so the list it sizes has to fit the file it was read from
    let count = usize::try_from(count).ok().filter(|count| (*count as u64) < length).ok_or_else(bad)?;
    let start = pages_at(count);
    let body = (start + saved as usize * PAGE_BYTES) as u64;
    if length != body + size_of::<u64>() as u64 {
        return Err(bad());
    }
    // the list goes through the chunk a piece at a time, and the check compares each piece as it comes
    if !is_shape_here(sealed, count) || !check_listed(file, count, chunk, matches)? {
        return Err(bad());
    }

    let (mut at, mut sum) = (0_u64, RunningSum::new());
    while at < body {
        let part = (body - at).min(IMAGE_CHUNK_BYTES as u64) as usize;
        file.read_exact_at(FileOffset(at), &mut chunk[..part]).map_err(|_error| bad())?;
        sum.add(&chunk[..part]);
        // chunks start on page boundaries, so the pages a chunk holds are whole
        for offset in (at.max(start as u64)..at + part as u64).step_by(PAGE_BYTES) {
            let page = ((offset - start as u64) / PAGE_BYTES as u64) as u32;
            let within = (offset - at) as usize;
            sorted.fill_page(page, &chunk[within..within + PAGE_BYTES]);
        }
        at += part as u64;
    }
    let mut stored = [0; size_of::<u64>()];
    file.read_exact_at(FileOffset(body), &mut stored).map_err(|_error| bad())?;
    if u64::from_le_bytes(stored) != sum.sum().0 {
        return Err(bad());
    }
    let as_page = |value: u64| u32::try_from(value).map_err(|_error| bad());
    let head = ImageHead { width: sorted.width(), pages: as_page(pages)?, saved: as_page(saved)?, free_head: as_page(free_head)?, free_count: as_page(free_count)?, entries, parts };
    sorted.finish_image(&head);
    Ok(())
}

/// Read the listed segments through the chunk a piece at a time, whole triples a read so none splits, and ask each one of the
/// volume. The caller has already checked the count and the read point under the same hold of the summary the check reads
fn check_listed<File: IoFile>(file: &File, count: usize, chunk: &mut [u8], matches: &impl Fn(ListedSegment) -> bool) -> Result<bool> {
    let bad = || Error::from(ErrorKind::InvalidData);
    let triple = LISTED_WORDS * size_of::<u64>();
    let per_read = chunk.len() / triple;
    let mut done = 0;
    while done < count {
        let take = (count - done).min(per_read);
        let at = (SORTED_HEAD_BYTES + done * triple) as u64;
        file.read_exact_at(FileOffset(at), &mut chunk[..take * triple]).map_err(|_error| bad())?;
        for piece in chunk[..take * triple].chunks_exact(triple) {
            let word = |index: usize| read_word(piece, index * size_of::<u64>()).ok_or_else(bad);
            let listed = ListedSegment { number: SegmentNumber(word(0)?), highest: word(1)?, seal_mark: word(2)? };
            if !matches(listed) {
                return Ok(false);
            }
        }
        done += take;
    }
    Ok(true)
}


// the head and one listed triple fit the first read, and every chunk holds whole pages
const _: () = assert!(IMAGE_CHUNK_BYTES >= SORTED_HEAD_BYTES + LISTED_WORDS * size_of::<u64>());
const _: () = assert!(IMAGE_CHUNK_BYTES.is_multiple_of(PAGE_BYTES));

// Callers, in the tree:
// - open (complete_path.rs fill_sorted_indexes): take_sorted_image first. Whether it took or not, the cover pass that follows
//   the fill runs after it, `covers.read().visit(.. index.remove_covered ..)`, since a cover loaded at open may postdate the
//   image. A taken image skips the per-segment fill.
// - close (reel_store.rs): save_sorted_images runs after stop_jobs, with `debug_assert!(state.jobs_stopped())`, so no merge
//   moves an entry while the pages are copied.

/// Read the newest place any segment the summary lists holds
fn newest_listed(rows: &crate::segment::summary::SummaryRows) -> u64 {
    rows.orders().present().iter().filter_map(|number| rows.get(*number)).map(|row| row.facts.places.highest.0).max().unwrap_or(0)
}
