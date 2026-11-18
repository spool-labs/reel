//! A complete column's gets and open: slots point a get at row blocks, and the blocks' keys say which slot is the key's

use std::ops::{Deref, DerefMut};
use std::io::Result as IoResult;
use std::sync::Mutex; use std::sync::atomic::Ordering;
use std::mem;

use reel_core::column_id::ColumnId;
use reel_core::store_error::StoreError;
use reel_core::units::{RowCount, WritePlace};

use crate::config::MAX_KEY_BYTES;
use crate::engine::engine_state::EngineState;
use crate::engine::read_path::{KeyRead, SealedRow, WalkAsk, corruption};
use crate::engine::recovery::{whole_columns, widen_row};
use crate::engine::volume::{NO_SEAL, Volume};
use crate::fixed_list::{FixedList, ListFull, ListOut, full};
use crate::guard::hold;
use crate::index::complete_table::{CompleteSlot, CompleteTable, TagMatches, tag_of};
use crate::index::open_slots::OpenSlot;
use crate::engine::seal_job::SortedRun;
use crate::index::sorted_keys::{Floors, HOLD_ROWS, KeyEntry, SortedKeys};
use crate::index::key_hash::{KeyHash, hash_key};
use crate::io::{DeviceReads, InPlace, Io, in_place};
use crate::io::device_error::is_device_error;
use crate::pool::Pool;
use crate::row_kind::RowKind;
use crate::segment::block_cache::BlockKey;
use crate::segment::footer_reader::{FooterAnswer, FooterCaches, FooterQuery, FooterReader, RowAt};
use crate::segment::row_block::{FooterRow, RowBlockWalk};
use crate::segment::segment_tail::SegmentNumber;

/// Row blocks a refill step gives slots, so a unit stays near a walk block's work
pub const REFILL_STEP_BLOCKS: u32 = 16;

/// A span of one segment's row blocks
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockSpan {
    pub segment: SegmentNumber,
    pub first: u32,
    pub end: u32,
}

/// A refill of a complete table past its mark, a few row blocks a step
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Refill {
    pub column: ColumnId,
    pub mark: SegmentNumber,
    pub missed: RowCount,
    pub segment: Option<SegmentNumber>,
    pub block: u32,
}

impl<Backend: Io> EngineState<Backend> {
    /// Read the heap the small block cache holds
    pub fn complete_cache_bytes(&self) -> u64 {
        self.complete_cache.as_ref().map_or(0, |cache| cache.heap_bytes().0)
    }

    /// Find a column's table, if it declares one
    pub fn complete_table(&self, column: ColumnId) -> Option<&CompleteTable> {
        self.complete.get(column.0 as usize).and_then(Option::as_ref)
    }

    /// Check a column's gets go by the spot cache: a complete table or a sorted index answers them with no footer search
    pub fn uses_spot(&self, column: ColumnId) -> bool {
        self.complete_table(column).is_none() && self.sorted_index(column).is_none()
    }

    /// Find a column's sorted index, if it declares one
    pub fn sorted_index(&self, column: ColumnId) -> Option<&SortedKeys> {
        self.sorted.get(column.0 as usize).and_then(Option::as_ref)
    }

    /// Read the floors a seal gives a sorted index. A seal hands the index one row a key, the key's newest in its segment, so an
    /// older row of a key in the sealing segment never comes, and the drop floor is the lowest place in the other open segments.
    /// The sealing segment's rows of other keys are not in the index yet, so the purge floor counts them too. Past a mark, sealed
    /// rows are out of the index too, so nothing drops
    pub fn sorted_floor(&self, volume: &Volume<Backend>, index: &SortedKeys, sealing: OpenSlot) -> Floors {
        if index.mark().is_some() {
            return Floors::at(WritePlace(0));
        }
        let claims = hold(&volume.claims);
        let next = WritePlace(self.places.last_given().0.saturating_add(1));
        let lowest = |others: bool| {
            let open = claims.open.iter().filter(|segment| (segment.slot != sealing || !others) && segment.row_count.0 > 0);
            open.map(|segment| segment.lowest_place).min().unwrap_or(next).min(next)
        };
        Floors { drops: lowest(true), purges: lowest(false) }
    }

    /// Find a key's newest sealed row: through the table on a complete column, and through the footers on any other
    pub async fn search_sealed(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        read: &KeyRead<'_>,
    ) -> Result<Option<SealedRow>, StoreError> {
        if let Some(index) = self.sorted_index(read.column) {
            return self.search_sorted(device, volume, read, index).await;
        }
        match self.complete_table(read.column) {
            Some(table) => self.search_complete(device, volume, read, table).await,
            None => self.search_footers(device, volume, read).await,
        }
    }

    /// Answer a key from its column's sorted index with no footer read, then the footers from the mark on, where rows have no
    /// entry. A read behind the newest sealed row may need a row the index holds no more, and an entry newer than the read point
    /// or in a segment with no pin can't answer, so those reads ask the footers
    pub async fn search_sorted(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        read: &KeyRead<'_>,
        index: &SortedKeys,
    ) -> Result<Option<SealedRow>, StoreError> {
        let entry = index.find(read.key);
        let is_behind = volume.may_hide_under_sealed(read.read_point);
        // a grave answers absent wherever it sits, so only a value needs its segment
        if is_behind || entry.is_some_and(|entry| entry.place > read.read_point || (!entry.is_grave && volume.table.pin(entry.segment).is_none())) {
            self.sorted_footer_asks.fetch_add(1, Ordering::Relaxed);
            return self.search_footers(device, volume, read).await;
        }
        let found = entry.as_ref().map(sealed_of);
        let Some(mark) = index.mark() else {
            self.sorted_answers.fetch_add(1, Ordering::Relaxed);
            return Ok(found);
        };
        self.sorted_footer_asks.fetch_add(1, Ordering::Relaxed);
        let Some(directory) = &self.directories[read.column.0 as usize] else {
            return Ok(found);
        };
        let mut reader = self.footer_reader_for(device);
        let ask = WalkAsk { first: mark, ..WalkAsk::EVERY };
        let marked = self.walk_candidates(device, volume, directory, read, &mut reader, ask).await?;
        Ok(newest_sealed(found, marked))
    }

    /// Ask each block a key's tag points at and take the highest place, then the footers from the mark on, where rows found
    /// no slot
    pub async fn search_complete(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        read: &KeyRead<'_>,
        table: &CompleteTable,
    ) -> Result<Option<SealedRow>, StoreError> {
        let mut matches = TagMatches::new();
        table.find(read.hash, &mut matches);
        // a superseded row's slot goes, so a read behind the newest sealed row, which may need one, asks the footers. The check
        // comes after the slots, so a slot this read saw go has its sealed row counted
        let is_behind = volume.may_hide_under_sealed(read.read_point);
        // a sticky slot says the footers answer for this tag
        if is_behind || matches.slots().iter().any(CompleteSlot::is_sticky) {
            return self.search_footers(device, volume, read).await;
        }
        // a miss asks no footer, so it takes no reader
        if matches.slots().is_empty() && table.mark().is_none() {
            return Ok(None);
        }
        let mut reader = self.footer_reader_for(device);
        let mut best = None;
        for slot in matches.slots() {
            if !slot.is_stale() {
                self.ask_slot(device, volume, read, slot, &mut reader, &mut best).await?;
            }
        }
        let (Some(mark), Some(directory)) = (table.mark(), &self.directories[read.column.0 as usize]) else {
            return Ok(best);
        };
        let ask = WalkAsk { first: mark, ..WalkAsk::EVERY };
        let marked = self.walk_candidates(device, volume, directory, read, &mut reader, ask).await?;
        Ok(newest_sealed(best, marked))
    }

    /// Ask one slot's row block for a key. A segment that left took its rows to wherever the key's other slots point
    pub async fn ask_slot(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        read: &KeyRead<'_>,
        slot: &CompleteSlot,
        reader: &mut FooterReader,
        best: &mut Option<SealedRow>,
    ) -> Result<(), StoreError> {
        let segment = slot.segment_number();
        let Some(pin) = volume.table.pin(segment) else {
            return Ok(());
        };
        let Some((entry, _replacement)) = volume.table.block_line(segment, u32::from(slot.block)) else {
            return Ok(());
        };
        let caches = FooterCaches {
            resident: &self.filter_arena,
            filters: &self.filter_cache,
            rows: self.complete_cache.as_ref().unwrap_or(&self.row_cache),
        };
        let query = FooterQuery {
            hash: read.hash,
            key: read.key,
            read_point: read.read_point,
        };
        let answer = match reader.search_cached(device, &pin.file, (segment, pin.head.place_base), &entry, &caches, &query).await {
            Ok(answer) => answer,
            Err(source) if is_device_error(&source) => FooterAnswer::Damaged,
            Err(source) => {
                return Err(StoreError::Io {
                    action: "reading a footer",
                    source,
                });
            },
        };
        match answer {
            FooterAnswer::Found(row) if best.is_none_or(|current| current.row.place < row.place) => {
                *best = Some(SealedRow { segment, row, block_offset: entry.block_offset, at: reader.found_at });
                Ok(())
            },
            FooterAnswer::Found(..) | FooterAnswer::Absent => Ok(()),
            FooterAnswer::Damaged => Err(corruption(read)),
        }
    }

    /// Find a key's newest row among finished segments through its slots. A stale slot only saves a block read here, since the row
    /// that made it stale stands higher and the highest place wins. Rows past
    /// the mark have no slot, so the footers answer for those
    pub fn newest_walked_complete(
        &self,
        volume: &Volume<Backend>,
        read: &KeyRead<'_>,
        table: &CompleteTable,
        skipped: &[SegmentNumber],
        above: Option<WritePlace>,
    ) -> Result<Option<SealedRow>, StoreError> {
        let mut matches = TagMatches::new();
        table.find(read.hash, &mut matches);
        if matches.slots().iter().any(CompleteSlot::is_sticky) {
            return self.newest_walked_footers(volume, read, skipped, above);
        }
        self.job_footer_readers.with(|reader| {
            let mut best = None;
            for slot in matches.slots().iter().filter(|slot| !slot.is_stale() && !skipped.contains(&slot.segment_number())) {
                if volume.table.pin(slot.segment_number()).is_some_and(|pin| pin.is_walked) {
                    in_place(self.ask_slot(&InPlace, volume, read, slot, reader, &mut best))?;
                }
            }
            let (Some(mark), Some(directory)) = (table.mark(), &self.directories[read.column.0 as usize]) else {
                return Ok(best);
            };
            let ask = WalkAsk { first: mark, skipped, is_finished_only: true, above };
            let marked = in_place(self.walk_candidates(&InPlace, volume, directory, read, reader, ask))?;
            Ok(newest_sealed(best, marked))
        })
    }

    /// Free a charged row's slot, so a get reads one block for its key and the table holds a slot a key. A slot that may stand for
    /// other keys of its tag goes only once each of them has a newer row than its row in the block, and any answer short of that keeps it
    pub fn free_superseded_slot(&self, volume: &Volume<Backend>, column: ColumnId, hash: KeyHash, segment: SegmentNumber, block_offset: u32) {
        let (Some(table), Some(block)) = (self.complete_table(column), volume.table.block_at(segment, block_offset)) else {
            return;
        };
        let block = block as u16;
        // the count goes first, so a row that shares the slot after it moves the count and the free keeps a slot it never proved
        let seen = table.shares();
        let Some(is_guarded) = table.slot_guard(hash, segment, block) else {
            return;
        };
        let is_proved = is_guarded && self.is_block_superseded(volume, (table, column), hash, (segment, block));
        table.free_superseded(hash, segment, block, (seen, is_proved));
    }

    /// Check every key of a hash's tag in a block, the hash's own apart, has a newer row than its row there. A block that can't be
    /// read or a sticky slot leaves it unproven, and so does a key whose newest row sits in that block
    pub fn is_block_superseded(&self, volume: &Volume<Backend>, (table, column): (&CompleteTable, ColumnId), hash: KeyHash, (segment, block): (SegmentNumber, u16)) -> bool {
        let tag = tag_of(hash);
        let (mut key, mut other) = (KeyBuffer::new(), KeyBuffer::new());
        // one key at a time, so the block's reader goes back before the key's slots are read with another
        let mut from = 0;
        loop {
            let found = self.job_footer_readers.with(|reader| {
                let (Some(pin), Some((entry, _replacement))) = (volume.table.pin(segment), volume.table.block_line(segment, u32::from(block))) else {
                    return Err(());
                };
                let block_key = BlockKey { segment, offset: entry.block_offset };
                let Ok(Some(loaded)) = in_place(reader.load_block(&InPlace, &pin.file, block_key, &entry, self.complete_cache.as_ref().unwrap_or(&self.row_cache))) else {
                    return Err(());
                };
                let Some(mut walk) = RowBlockWalk::open(loaded, WritePlace(pin.head.place_base.0)) else {
                    return Err(());
                };
                let (mut at, mut last) = (0, None);
                while let Some(row) = walk.next_row(&mut key) {
                    at += 1;
                    let row_hash = hash_key(column, &key);
                    // a key's rows in a block run newest first, so its first row is the one to ask about
                    if row.kind == RowKind::RangeDelete || row_hash == hash || tag_of(row_hash) != tag || last == Some(row_hash) {
                        continue;
                    }
                    last = Some(row_hash);
                    if at > from {
                        other.set(&key).map_err(|_full| ())?;
                        return Ok(Some((at, row.place)));
                    }
                }
                Ok(None)
            });
            let (at, place) = match found {
                Ok(Some(found)) => found,
                Ok(None) => return true,
                Err(()) => return false,
            };
            if !self.has_newer_row(volume, (table, column), &other, place) {
                return false;
            }
            from = at;
        }
    }

    /// Check a key's slots locate a row newer than a place
    pub fn has_newer_row(&self, volume: &Volume<Backend>, (table, column): (&CompleteTable, ColumnId), key: &[u8], place: WritePlace) -> bool {
        let Ok(read) = self.key_read(column, key, WritePlace(u64::MAX), false) else {
            return false;
        };
        let mut matches = TagMatches::new();
        table.find(read.hash, &mut matches);
        if matches.slots().iter().any(CompleteSlot::is_sticky) {
            return false;
        }
        self.job_footer_readers.with(|reader| {
            let mut best = None;
            for slot in matches.slots().iter().filter(|slot| !slot.is_stale()) {
                if in_place(self.ask_slot(&InPlace, volume, &read, slot, reader, &mut best)).is_err() {
                    return false;
                }
            }
            best.is_some_and(|found: SealedRow| found.row.place > place)
        })
    }

    /// Check a slot's segment has left the volume. A seal's own segment enters the table only when it finishes
    pub fn is_slot_gone(&self, volume: &Volume<Backend>, segment: u32) -> bool {
        let number = SegmentNumber(u64::from(segment));
        number.0 != volume.sealing.load(Ordering::Acquire) && number.0 < volume.output_floor() && volume.table.pin(number).is_none()
    }

    /// Fill every complete table: from its saved image where one stands, then from the row blocks of every segment the image
    /// doesn't cover. A block that can't be read damages its segment, since the keys in it would otherwise read as absent
    pub fn fill_complete_tables(&self) -> IoResult<()> {
        for (id, table) in self.complete.iter().enumerate() {
            let (Some(table), Some(info)) = (table, self.columns[id]) else {
                continue;
            };
            let column = ColumnId(id as u8);
            let index = info.volume.0 as usize;
            let volume = &self.volumes[index];
            let covered = self.take_image(volume, column, table)?;
            let mut numbers = Vec::new();
            volume.table.for_each_segment(|number, _root, _file| numbers.push(number));
            numbers.sort_unstable();
            for number in numbers.into_iter().filter(|number| covered.binary_search(number).is_err()) {
                if !self.fill_from_segment(volume, column, number)? {
                    // the rows past the unread block have no slot, so the table sends reads of this segment on to the footers
                    table.mark_from(number);
                    self.damage_unread(index, number);
                }
            }
        }
        Ok(())
    }

    /// Pick the threads a sorted fill runs on and the footer readers they take: a writer's workers with the job readers, or the
    /// opening thread alone in a store with none, and a reader's read threads with the caller readers no call holds yet
    pub fn fill_threads(&self) -> (usize, &Pool<FooterReader>) {
        if self.is_read_only {
            return (self.read_tasks.len().max(1), &self.footer_readers);
        }
        (self.sizing.workers.max(1), &self.job_footer_readers)
    }

    /// Fill every sorted index from the row blocks of every segment, in any order and on as many threads as fill_threads gives,
    /// since the newest place wins and a grave stays while nothing is dropped at open. Then take out what each live cover
    /// hides. A block that can't be read damages its segment
    pub fn fill_sorted_indexes(&self) -> IoResult<()> {
        let started = self.io.now();
        let (threads, readers) = self.fill_threads();
        for (id, index) in self.sorted.iter().enumerate() {
            let (Some(index), Some(info)) = (index, self.columns[id]) else {
                continue;
            };
            let column = ColumnId(id as u8);
            let volume_index = info.volume.0 as usize;
            let volume = &self.volumes[volume_index];
            let mut numbers = Vec::new();
            volume.table.for_each_segment(|number, _root, _file| numbers.push(number));
            // each segment's blocks split into a piece for each thread, and the units hand out one segment's pieces in turn, so
            // threads at work at once read different key ranges and take different parts' locks
            let failed = Mutex::new(Vec::new());
            let error = Mutex::new(None);
            self.io.run_units(threads, numbers.len() * threads, &|unit| {
                if hold(&error).is_some() {
                    return;
                }
                let number = numbers[unit / threads];
                match self.fill_sorted_piece(volume, (column, index), number, (unit % threads, threads), readers) {
                    Ok(true) => {},
                    Ok(false) => hold(&failed).push(number),
                    Err(fault) => {
                        hold(&error).get_or_insert(fault);
                    },
                }
            });
            if let Some(fault) = hold(&error).take() {
                return Err(fault);
            }
            let mut failed = mem::take(&mut *hold(&failed));
            failed.sort_unstable();
            failed.dedup();
            for number in failed {
                // the rows past the unread block have no entry, so the index sends reads of this segment on to the footers
                index.mark_from(number);
                self.damage_unread(volume_index, number);
            }
            volume.covers.read().visit(|cover, start, end| {
                if cover.column == column {
                    index.remove_covered(start, end, cover.place);
                }
            });
        }
        self.sorted_fill_ns.store(self.io.now().0.saturating_sub(started.0), Ordering::Relaxed);
        Ok(())
    }

    /// Give one piece of a segment's blocks of a sorted column their entries, a block at a time, and say whether every block read
    /// clean. The blocks go through a job's reader with nothing added to the row cache, since no get or scan of a sorted column
    /// reads them
    pub fn fill_sorted_piece(
        &self,
        volume: &Volume<Backend>,
        (column, index): (ColumnId, &SortedKeys),
        number: SegmentNumber,
        (piece, pieces): (usize, usize),
        readers: &Pool<FooterReader>,
    ) -> IoResult<bool> {
        let (Some((first, count)), Some(pin)) = (volume.table.column_blocks(number, column), volume.table.pin(number)) else {
            return Ok(true);
        };
        let (start, end) = (first + count * piece as u32 / pieces as u32, first + count * (piece as u32 + 1) / pieces as u32);
        let caches = FooterCaches {
            resident: &self.filter_arena,
            filters: &self.filter_cache,
            rows: &self.row_cache,
        };
        let place_base = WritePlace(pin.head.place_base.0);
        let mut run = SortedRun::new(index.width());
        readers.with(|reader| {
            for block_index in start..end {
                let Some((entry, _replacement)) = volume.table.block_line(number, block_index) else {
                    return Ok(false);
                };
                match in_place(reader.hold_block(&InPlace, &pin.file, (number, place_base), &entry, &caches)) {
                    Ok(true) => {},
                    Ok(false) => return Ok(false),
                    Err(error) if is_device_error(&error) => return Ok(false),
                    Err(error) => return Err(error),
                }
                self.sorted_fill_bytes.fetch_add(entry.block_length().0, Ordering::Relaxed);
                // a key's versions sort newest first, so its first row in the block is the one that can win
                reader.visit_loaded(&entry, place_base, |key, row| {
                    if row.kind == RowKind::RangeDelete || run.is_last(key) {
                        return;
                    }
                    if run.entries.len() == HOLD_ROWS {
                        index.upsert_many(&run.keys, &run.entries, Floors::at(WritePlace(0)), |_row, _upserted| {});
                        run.clear();
                    }
                    run.push(column, key, number, row);
                });
                index.upsert_many(&run.keys, &run.entries, Floors::at(WritePlace(0)), |_row, _upserted| {});
                run.clear();
            }
            Ok(true)
        })
    }


    /// Point the sorted entries of a merge output's rows at it, each where the entry is that row. The install calls this once the
    /// output is listed and before the inputs retire, so an entry points at a listed segment at every moment. The blocks go through
    /// the reader with nothing added to the row cache, since no get or scan of a sorted column reads them. A block that can't be
    /// read leaves its entries on the inputs, counted, and a get of those keys asks the footers once the inputs go. Nothing here
    /// fails the install
    pub fn move_sorted_entries(&self, volume: &Volume<Backend>, number: SegmentNumber) {
        for (id, index) in self.sorted.iter().enumerate() {
            let (Some(index), Some(info)) = (index, self.columns[id]) else {
                continue;
            };
            if info.volume != volume.id {
                continue;
            }
            let column = ColumnId(id as u8);
            let (Some((first, count)), Some(pin)) = (volume.table.column_blocks(number, column), volume.table.pin(number)) else {
                continue;
            };
            let caches = FooterCaches {
                resident: &self.filter_arena,
                filters: &self.filter_cache,
                rows: &self.row_cache,
            };
            let place_base = WritePlace(pin.head.place_base.0);
            self.job_footer_readers.with(|reader| {
                for block_index in first..first + count {
                    let Some((entry, _replacement)) = volume.table.block_line(number, block_index) else {
                        self.sorted_moves_missed.fetch_add(1, Ordering::Relaxed);
                        continue;
                    };
                    if !matches!(in_place(reader.hold_block(&InPlace, &pin.file, (number, place_base), &entry, &caches)), Ok(true)) {
                        self.sorted_moves_missed.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    reader.visit_loaded(&entry, place_base, |key, row| {
                        if row.kind != RowKind::RangeDelete {
                            index.move_entry(key, row.place, number, row.offset);
                        }
                    });
                }
            });
        }
    }

    /// Damage a segment with a row block open or a merge couldn't read. The block's keys have no slot, so they would otherwise read
    /// as absent. The segment stays in the table and its row keeps its facts
    pub fn damage_unread(&self, index: usize, number: SegmentNumber) {
        let volume = &self.volumes[index];
        // a bit can't say which row set it, and the row may sit in what was lost
        volume.dead_bits.spoil_all();
        let counter = self.places.last_given();
        for range in whole_columns(self, index, number, counter) {
            volume.damage.add(&range);
        }
        let mut damaged = hold(&volume.damaged);
        if !damaged.contains(&number) {
            damaged.push(number);
            self.damaged_segments.fetch_add(1, Ordering::AcqRel);
        }
        drop(damaged);
        widen_row(self, index, &mut hold(&volume.summary), number, counter);
    }

    /// Give one segment's rows of a column their slots, and say whether every block read clean
    pub fn fill_from_segment(&self, volume: &Volume<Backend>, column: ColumnId, number: SegmentNumber) -> IoResult<bool> {
        let Some((first, count)) = volume.table.column_blocks(number, column) else {
            return Ok(true);
        };
        let blocks = BlockSpan { segment: number, first, end: first + count };
        self.fill_blocks(volume, column, blocks, &self.footer_readers, &mut Vec::new(), None)
    }

    /// Find a complete column of a volume whose table has a mark and room again, holding fewer slots than a pass that failed
    /// left, and start a refill from the mark
    pub fn start_refill(&self, index: usize, below: u64) -> Option<Refill> {
        for (id, table) in self.complete.iter().enumerate() {
            let (Some(table), Some(info)) = (table, self.columns[id]) else {
                continue;
            };
            let is_due = info.volume.0 as usize == index && table.has_room() && table.used_slots().0 < below;
            let Some(mark) = table.mark().filter(|_mark| is_due) else {
                continue;
            };
            // a seal or a merge output can hold rows past the mark that found no slot before the summary lists it, so a refill now
            // would pass it and clear the mark over them
            let volume = &self.volumes[index];
            let sealing = volume.sealing.load(Ordering::Acquire);
            if (sealing != NO_SEAL && sealing >= mark.0) || volume.output_floor() != NO_SEAL {
                continue;
            }
            // newest first, so a key's newest row past the mark takes its slot ahead of the rows it superseded
            let segment = hold(&self.volumes[index].summary).orders().after(SegmentNumber(mark.0.saturating_sub(1))).last().copied();
            return Some(Refill { column: ColumnId(id as u8), mark, missed: table.past_capacity(), segment, block: 0 });
        }
        None
    }

    /// Give a few row blocks past a refill's mark their slots, and clear the mark once no segment past it is left. Say whether the
    /// refill goes on. A block that can't be read ends it with the mark standing, since its rows have no slot
    pub fn refill_step(&self, index: usize, refill: &mut Refill, (key, check): (&mut FixedList<u8>, &mut FooterReader)) -> IoResult<bool> {
        let volume = &self.volumes[index];
        let Some(table) = self.complete_table(refill.column) else {
            return Ok(false);
        };
        let Some(segment) = refill.segment else {
            table.clear_mark(refill.mark, refill.missed);
            return Ok(false);
        };
        let mark = refill.mark;
        let next = |segment: SegmentNumber| hold(&volume.summary).orders().below(segment).last().copied().filter(|below| *below >= mark);
        let Some((first, count)) = volume.table.column_blocks(segment, refill.column) else {
            refill.segment = next(segment);
            refill.block = 0;
            return Ok(true);
        };
        let from = first + refill.block;
        let end = (from + REFILL_STEP_BLOCKS).min(first + count);
        let blocks = BlockSpan { segment, first: from, end };
        if !self.fill_blocks(volume, refill.column, blocks, &self.job_footer_readers, key, Some(check))? {
            return Ok(false);
        }
        if end == first + count {
            refill.segment = next(segment);
            refill.block = 0;
        } else {
            refill.block = end - first;
        }
        Ok(true)
    }

    /// Find a key's slotted row at or above a place, reading its slots' blocks so a tag collision never counts
    pub fn slotted_row(&self, volume: &Volume<Backend>, (table, column): (&CompleteTable, ColumnId), (key, place): (&[u8], WritePlace), reader: &mut FooterReader) -> Option<SealedRow> {
        let Ok(read) = self.key_read(column, key, WritePlace(u64::MAX), false) else {
            return None;
        };
        let mut matches = TagMatches::new();
        table.find(read.hash, &mut matches);
        let mut best = None;
        for slot in matches.slots() {
            // a block that can't be read says nothing, and the row takes a slot of its own
            if !slot.is_stale() && !slot.is_sticky() && in_place(self.ask_slot(&InPlace, volume, &read, slot, reader, &mut best)).is_err() {
                return None;
            }
        }
        best.filter(|found| found.row.place >= place)
    }

    /// Give a span of a segment's row blocks of a column their slots, and say whether every block read clean
    pub fn fill_blocks(
        &self,
        volume: &Volume<Backend>,
        column: ColumnId,
        blocks: BlockSpan,
        readers: &Pool<FooterReader>,
        key: &mut impl ListOut<u8>,
        mut newer: Option<&mut FooterReader>,
    ) -> IoResult<bool> {
        let number = blocks.segment;
        let (Some(table), Some(pin)) = (self.complete_table(column), volume.table.pin(number)) else {
            return Ok(true);
        };
        readers.with(|reader| {
            for index in blocks.first..blocks.end {
                let Some((entry, _replacement)) = volume.table.block_line(number, index) else {
                    return Ok(false);
                };
                let block_key = BlockKey { segment: number, offset: entry.block_offset };
                let block = match in_place(reader.load_block(&InPlace, &pin.file, block_key, &entry, self.complete_cache.as_ref().unwrap_or(&self.row_cache))) {
                    Ok(Some(block)) => block,
                    Ok(None) => return Ok(false),
                    Err(error) if is_device_error(&error) => return Ok(false),
                    Err(error) => return Err(error),
                };
                let Some(mut walk) = RowBlockWalk::open(block, WritePlace(pin.head.place_base.0)) else {
                    return Ok(false);
                };
                let mut last = None;
                while let Some(row) = walk.next_row(key) {
                    if row.kind != RowKind::RangeDelete {
                        let hash = hash_key(column, &key[..]);
                        if last != Some(hash) {
                            last = Some(hash);
                            // a refill gives no slot to a row a slotted row stands at or above, so a superseded row stays out
                            if let Some(check) = newer.as_deref_mut()
                                && let Some(found) = self.slotted_row(volume, (table, column), (&key[..], row.place), check)
                            {
                                // a slot another key of the tag holds in this block stands for this row, and the insert says so
                                if found.segment == number && volume.table.block_at(number, found.block_offset) == Some(index) {
                                    table.insert(hash, number, index as u16, |segment| self.is_slot_gone(volume, segment));
                                }
                                continue;
                            }
                            table.insert(hash, number, index as u16, |segment| self.is_slot_gone(volume, segment));
                        }
                    }
                }
            }
            Ok(true)
        })
    }
}

/// Pick the row with the higher place
pub fn newest_sealed(left: Option<SealedRow>, right: Option<SealedRow>) -> Option<SealedRow> {
    match (left, right) {
        (Some(left), Some(right)) if right.row.place > left.row.place => Some(right),
        (Some(left), _right) => Some(left),
        (None, right) => right,
    }
}

/// Build the sealed row an index entry stands for. Its block is unknown, so a reader of it reads the record by place
pub fn sealed_of(entry: &KeyEntry) -> SealedRow {
    SealedRow {
        segment: entry.segment,
        row: FooterRow {
            place: entry.place,
            offset: entry.offset,
            length: entry.length,
            kind: if entry.is_grave { RowKind::Grave } else { RowKind::Value },
        },
        block_offset: 0,
        at: RowAt::UNKNOWN,
    }
}

/// A key read out of a row block, held on the stack, so a walk's free allocates nothing
pub struct KeyBuffer {
    bytes: [u8; MAX_KEY_BYTES as usize],
    length: usize,
}

impl KeyBuffer {
    /// Build an empty buffer
    pub const fn new() -> Self {
        Self { bytes: [0; MAX_KEY_BYTES as usize], length: 0 }
    }

    /// Hold a copy of a key
    pub fn set(&mut self, key: &[u8]) -> Result<(), ListFull> {
        ListOut::clear(self);
        ListOut::extend_from_slice(self, key)
    }
}

impl Default for KeyBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for KeyBuffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.bytes[..self.length]
    }
}

impl DerefMut for KeyBuffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.bytes[..self.length]
    }
}

impl ListOut<u8> for KeyBuffer {
    fn clear(&mut self) {
        self.length = 0;
    }

    fn truncate(&mut self, length: usize) {
        self.length = self.length.min(length);
    }

    fn push(&mut self, item: u8) -> Result<(), ListFull> {
        let Some(slot) = self.bytes.get_mut(self.length) else {
            return Err(full());
        };
        *slot = item;
        self.length += 1;
        Ok(())
    }

    fn extend_from_slice(&mut self, items: &[u8]) -> Result<(), ListFull> {
        let end = self.length + items.len();
        let Some(room) = self.bytes.get_mut(self.length..end) else {
            return Err(full());
        };
        room.copy_from_slice(items);
        self.length = end;
        Ok(())
    }

    fn push_if_room(&mut self, item: u8) -> bool {
        self.push(item).is_ok()
    }
}
