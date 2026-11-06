//! A complete column's gets and open: slots point a get at row blocks, and the blocks' keys say which slot is the key's

use std::io::Result as IoResult;
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::WritePlace;

use crate::engine::engine_state::EngineState;
use crate::engine::read_path::{KeyRead, SealedRow, WalkAsk, corruption};
use crate::engine::recovery::{whole_columns, widen_row};
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::index::complete_table::{CompleteSlot, CompleteTable, TagMatches};
use crate::index::key_hash::hash_key;
use crate::io::{DeviceReads, InPlace, Io, in_place};
use crate::io::device_error::is_device_error;
use crate::row_kind::RowKind;
use crate::segment::block_cache::BlockKey;
use crate::segment::footer_reader::{FooterAnswer, FooterCaches, FooterQuery, FooterReader};
use crate::segment::row_block::RowBlockWalk;
use crate::segment::segment_tail::SegmentNumber;

impl<Backend: Io> EngineState<Backend> {
    /// Read the heap the small block cache holds
    pub fn complete_cache_bytes(&self) -> u64 {
        self.complete_cache.as_ref().map_or(0, |cache| cache.heap_bytes().0)
    }

    /// Find a column's table, if it declares one
    pub fn complete_table(&self, column: ColumnId) -> Option<&CompleteTable> {
        self.complete.get(column.0 as usize).and_then(Option::as_ref)
    }

    /// Find a key's newest sealed row: through the table on a complete column, and through the footers on any other
    pub async fn search_sealed(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        read: &KeyRead<'_>,
    ) -> Result<Option<SealedRow>, StoreError> {
        match self.complete_table(read.column) {
            Some(table) => self.search_complete(device, volume, read, table).await,
            None => self.search_footers(device, volume, read).await,
        }
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
        // a stale mark speaks for reads at or above the row that caused it, and a read behind the newest sealed row may
        // need what it hides. The check comes after the slots, so a mark this read saw has its sealed row counted
        let is_behind = volume.may_hide_under_sealed(read.read_point);
        // a sticky slot says the footers answer for this tag
        if matches.slots().iter().any(CompleteSlot::is_sticky) {
            return self.search_footers(device, volume, read).await;
        }
        let mut reader = self.footer_reader_for(device);
        let mut best = None;
        for slot in matches.slots() {
            if !slot.is_stale() || is_behind {
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
                *best = Some(SealedRow { segment, row, block_offset: entry.block_offset });
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
    ) -> Result<Option<SealedRow>, StoreError> {
        let mut matches = TagMatches::new();
        table.find(read.hash, &mut matches);
        if matches.slots().iter().any(CompleteSlot::is_sticky) {
            return self.newest_walked_footers(volume, read, skipped);
        }
        self.footer_readers.with(|reader| {
            let mut best = None;
            for slot in matches.slots().iter().filter(|slot| !slot.is_stale() && !skipped.contains(&slot.segment_number())) {
                if volume.table.pin(slot.segment_number()).is_some_and(|pin| pin.is_walked) {
                    in_place(self.ask_slot(&InPlace, volume, read, slot, reader, &mut best))?;
                }
            }
            let (Some(mark), Some(directory)) = (table.mark(), &self.directories[read.column.0 as usize]) else {
                return Ok(best);
            };
            let ask = WalkAsk { first: mark, skipped, is_finished_only: true };
            let marked = in_place(self.walk_candidates(&InPlace, volume, directory, read, reader, ask))?;
            Ok(newest_sealed(best, marked))
        })
    }

    /// Mark a charged row's slot stale, so a get reads one block for its key
    pub fn mark_slot_stale(&self, volume: &Volume<Backend>, column: ColumnId, key: &[u8], segment: SegmentNumber, block_offset: u32) {
        if let (Some(table), Some(block)) = (self.complete_table(column), volume.table.block_at(segment, block_offset)) {
            table.mark_stale(hash_key(column, key), segment, block as u16);
        }
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
                    self.damage_unread(index, number);
                }
            }
        }
        Ok(())
    }

    /// Damage a segment with a row block open couldn't read. The block's keys have no slot, so they would otherwise read as absent.
    /// The segment stays in the table and its row keeps its facts
    pub fn damage_unread(&self, index: usize, number: SegmentNumber) {
        let volume = &self.volumes[index];
        let counter = self.order.mark();
        for range in whole_columns(self, index, number, counter) {
            volume.damage.add(&range);
        }
        let mut damaged = hold(&volume.damaged);
        if !damaged.contains(&number) {
            damaged.push(number);
        }
        drop(damaged);
        widen_row(self, index, &mut hold(&volume.summary), number, counter);
    }

    /// Give one segment's rows of a column their slots, and say whether every block read clean
    pub fn fill_from_segment(&self, volume: &Volume<Backend>, column: ColumnId, number: SegmentNumber) -> IoResult<bool> {
        let (Some(table), Some(pin), Some((first, count))) =
            (self.complete_table(column), volume.table.pin(number), volume.table.column_blocks(number, column))
        else {
            return Ok(true);
        };
        let mut key = Vec::new();
        self.footer_readers.with(|reader| {
            for index in first..first + count {
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
                while let Some(row) = walk.next_row(&mut key) {
                    if row.kind != RowKind::RangeDelete {
                        let hash = hash_key(column, &key);
                        if last != Some(hash) {
                            table.insert(hash, number, index as u16, |_segment| false);
                            last = Some(hash);
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
