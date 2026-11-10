//! A walk block's lookups taken one candidate at a time, newest first, so a candidate's row block loads once for every row
//! of the walk block that needs it

use std::cmp::Reverse;
use std::io::{Error, ErrorKind};
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, WritePlace};

use crate::pool::{touched_capacity, touched_room};
use crate::engine::engine_state::EngineState;
use crate::engine::read_path::{KeyRead, SealedRow, corruption};
use crate::engine::volume::Volume;
use crate::io::device_error::is_device_error;
use crate::io::{InPlace, Io, ReadOn, in_place};
use crate::row_kind::RowKind;
use crate::segment::column_directory::BlockCandidate;
use crate::segment::footer_reader::{FilterAnswer, FooterAnswer, FooterCaches, FooterQuery, FooterReader};
use crate::segment::key_cursor::{CURSOR_SEGMENTS, CursorAnswer, KeyCursor};
use crate::segment::segment_tail::SegmentNumber;

/// Rows one walk block can look up together
pub const MOST_LOOKUPS: usize = 4096;
/// Key bytes those rows can take
pub const LOOKUP_KEY_BYTES: usize = 256 * 1024;

/// One row's lookup: its key in the arena, and what the candidates gave it
#[derive(Clone, Copy, Debug, Default)]
pub struct Lookup {
    pub key_at: u32,
    pub key_length: u32,
    pub best: Option<SealedRow>,
    pub is_damaged: bool,
}

/// The row block and filter the reader keeps from one row to the next while one candidate answers
#[derive(Clone, Copy, Debug, Default)]
pub struct HeldParts {
    pub block: Option<u32>,
    pub filter: Option<(u32, usize)>,
}

/// What one walk block's lookups did, for the report
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LookupCounts {
    pub blocks: u64,
    pub candidates: u64,
    pub whole: u64,
    pub key_checks: u64,
}

/// The keys of a walk block's rows, in order, and the room the candidate pass takes
#[derive(Debug)]
pub struct BlockLookups {
    keys: Vec<u8>,
    pub lookups: Vec<Lookup>,
    candidates: Vec<BlockCandidate>,
    pub counts: LookupCounts,
}

impl BlockLookups {
    /// Allocate the fixed room
    pub fn new() -> Self {
        Self {
            keys: touched_capacity(LOOKUP_KEY_BYTES, 0),
            lookups: touched_room(MOST_LOOKUPS),
            candidates: touched_room(CURSOR_SEGMENTS),
            counts: LookupCounts::default(),
        }
    }

    /// Read the heap the room holds
    pub fn heap_bytes(&self) -> ByteCount {
        let lookups = self.lookups.capacity() * size_of::<Lookup>();
        ByteCount((self.keys.capacity() + lookups + self.candidates.capacity() * size_of::<BlockCandidate>()) as u64)
    }

    /// Start a walk block over
    pub fn clear(&mut self) {
        self.keys.clear();
        self.lookups.clear();
    }

    /// Add a row's key, or return false when the room is full
    pub fn add(&mut self, key: &[u8]) -> bool {
        if self.lookups.len() == self.lookups.capacity() || self.keys.len() + key.len() > self.keys.capacity() {
            return false;
        }
        self.lookups.push(Lookup { key_at: self.keys.len() as u32, key_length: key.len() as u32, ..Lookup::default() });
        self.keys.extend_from_slice(key);
        true
    }

    /// Read a row's key
    pub fn key(&self, lookup: &Lookup) -> &[u8] {
        &self.keys[lookup.key_at as usize..(lookup.key_at + lookup.key_length) as usize]
    }
}

impl Default for BlockLookups {
    fn default() -> Self {
        Self::new()
    }
}

impl<Backend: Io> EngineState<Backend> {
    /// Find every row's newest version among finished segments other than one, as `newest_walked_held` would row by row, and
    /// return false when the lookups can't run together and each row has to ask alone
    pub fn look_up_block(
        &self,
        volume: &Volume<Backend>,
        column: ColumnId,
        skipped: SegmentNumber,
        lookups: &mut BlockLookups,
        cursor: &mut KeyCursor<Backend::File>,
    ) -> Result<bool, StoreError> {
        if self.complete_table(column).is_some() || volume.is_cursor_held_off() {
            return Ok(false);
        }
        let Some(directory) = &self.directories[column.0 as usize] else {
            return Ok(true);
        };
        let (Some(first), Some(last)) = (lookups.lookups.first().copied(), lookups.lookups.last().copied()) else {
            return Ok(true);
        };
        let BlockLookups { keys, lookups: rows, candidates, counts } = lookups;
        let key_of = |lookup: &Lookup| &keys[lookup.key_at as usize..(lookup.key_at + lookup.key_length) as usize];
        let mut reader = self.job_footer_readers.lease();
        loop {
            let Some(generation) = directory.block_candidates(key_of(&first), key_of(&last), candidates) else {
                return Ok(false);
            };
            candidates.sort_unstable_by_key(|candidate| Reverse(candidate.entry.newest_place));
            for row in rows.iter_mut() {
                (row.best, row.is_damaged) = (None, false);
            }
            counts.blocks += 1;
            for candidate in candidates.iter().filter(|candidate| candidate.entry.segment != skipped) {
                if self.is_stopping.load(Ordering::Acquire) {
                    return Err(StoreError::Io {
                        action: "walking",
                        source: Error::from(ErrorKind::Interrupted),
                    });
                }
                counts.candidates += 1;
                counts.whole += u64::from(candidate.is_whole);
                let segment = candidate.entry.segment;
                let mut held = HeldParts::default();
                for row in rows.iter_mut() {
                    // newest first, so a row with something at or past this candidate's newest has its answer
                    if row.is_damaged || row.best.is_some_and(|best| best.row.place >= candidate.entry.newest_place) {
                        continue;
                    }
                    let key = key_of(row);
                    counts.key_checks += u64::from(!candidate.is_whole);
                    if !candidate.holds(key) {
                        continue;
                    }
                    let read = self.key_read(column, key, WritePlace(u64::MAX), false)?;
                    match self.ask_held_once(volume, &read, segment, cursor, &mut reader, &mut held, &mut row.best) {
                        Ok(()) => {},
                        // a version a damaged block hides counts nothing for that row, as one lookup alone would give it
                        Err(StoreError::Corruption { .. }) => (row.best, row.is_damaged) = (None, true),
                        Err(error) => return Err(error),
                    }
                }
            }
            // a segment that left under these lookups took rows with it, and the list that took it out has their new home
            if directory.generation() == generation {
                return Ok(true);
            }
        }
    }

    /// Ask one walked segment for a row through the cursor, keeping the block the reader holds for the next row. A range
    /// delete, a window past its lines or a key the cursor can't place asks as `ask_held` does
    #[allow(clippy::too_many_arguments, reason = "a walked segment, the read, the cursor and the block it holds")]
    pub fn ask_held_once(
        &self,
        volume: &Volume<Backend>,
        read: &KeyRead<'_>,
        segment: SegmentNumber,
        cursor: &mut KeyCursor<Backend::File>,
        reader: &mut FooterReader,
        held: &mut HeldParts,
        best: &mut Option<SealedRow>,
    ) -> Result<(), StoreError> {
        let (file, place_base, window) = match cursor.window(&volume.table, segment, read.column, read.key) {
            CursorAnswer::Skip => return Ok(()),
            CursorAnswer::Table => {
                *held = HeldParts::default();
                return in_place(self.ask_walked(&InPlace, volume, read, segment, reader, best));
            },
            CursorAnswer::Window { file, place_base, window } => (file.clone(), place_base, window),
        };
        let Some(window) = window else {
            return Ok(());
        };
        let caches = FooterCaches {
            resident: &self.filter_arena,
            filters: &self.filter_cache,
            rows: &self.row_cache,
        };
        let query = FooterQuery {
            hash: read.hash,
            key: read.key,
            read_point: read.read_point,
        };
        let footer_error = |source: Error| match is_device_error(&source) {
            true => corruption(read),
            false => StoreError::Io { action: "reading a footer", source },
        };
        for position in 0..window.count {
            let entry = window.entries[position];
            let filter_end = match window.entries.get(position + 1) {
                Some(next) if position + 1 < window.count => next.header_at().0,
                _last_line => window.filter_end_of_last,
            };
            match in_place(reader.probe_held(ReadOn::new(&InPlace, &file), segment, &entry, filter_end, &caches, &query, &mut held.filter)).map_err(footer_error)? {
                FilterAnswer::Damaged => return Err(corruption(read)),
                FilterAnswer::No => continue,
                FilterAnswer::Maybe => {},
            }
            if held.block != Some(entry.block_offset) {
                held.block = None;
                if !in_place(reader.hold_block(&InPlace, &file, (segment, place_base), &entry, &caches)).map_err(footer_error)? {
                    return Err(corruption(read));
                }
                held.block = Some(entry.block_offset);
            }
            match reader.search_loaded(&entry, place_base, &query) {
                FooterAnswer::Absent => {},
                FooterAnswer::Damaged => return Err(corruption(read)),
                // a range delete sends the search below it in this segment, which the plain path does
                FooterAnswer::Found(row) if row.kind == RowKind::RangeDelete => {
                    *held = HeldParts::default();
                    return self.ask_held(volume, read, segment, cursor, reader, best);
                },
                FooterAnswer::Found(row) => {
                    if best.is_none_or(|current| current.row.place < row.place) {
                        *best = Some(SealedRow { segment, row, block_offset: entry.block_offset, at: reader.found_at });
                    }
                    return Ok(());
                },
            }
        }
        // the lines past a full window come from the table, as the plain path reads them
        if window.has_more {
            *held = HeldParts::default();
            return self.ask_held(volume, read, segment, cursor, reader, best);
        }
        Ok(())
    }
}
