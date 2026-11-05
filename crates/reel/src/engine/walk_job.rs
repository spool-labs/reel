//! The dead-byte walk: after a seal, count the versions each segment's rows supersede, a row block at a time
//!
//! A walk looks only at segments whose own walk has finished, so after each walk exactly the newest row of a key
//! among them is live and every other row is counted once. A segment not yet walked counts nothing until its turn,
//! which only understates. Nothing is written per row: the newest row in write order decides every read.
//!
//! Range delete rows take no part on either side, and the range-delete apply job and compaction see to rows a cover
//! hides, so neither counts here.

use std::io::{Error, Result};
use std::sync::Arc;

use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, Nanoseconds, WritePlace};

use crate::config::MAX_KEY_BYTES;
use crate::engine::engine_state::EngineState;
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::io::device_error::is_device_error;
use crate::io::job::{Job, JobStep};
use crate::io::{Io, IoFile};
use crate::pool::touched_capacity;
use crate::row_kind::RowKind;
use crate::segment::footer_writer::MOST_BLOCK_ROWS;
use crate::segment::row_block::{FooterRow, ROW_BLOCK_BYTES, RowBlockWalk};
use crate::segment::segment_table::{SegmentPin, WalkBlock};
use crate::segment::segment_tail::SegmentNumber;
use crate::segment::summary::DeadCount;

/// How long a walk waits after a failed read before it tries the block again
pub const WALK_RETRY: Nanoseconds = Nanoseconds(1_000_000_000);
/// How far back a count looks for an entry to add to
pub const COUNT_SEARCH: usize = 32;

/// The segment a walk is on
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalkTarget {
    pub segment: SegmentNumber,
    pub next_block: u32,
    pub block_count: u32,
    pub is_backward: bool,
    pub is_damaged: bool,
}

/// The walk of one volume's sealed segments
pub struct WalkJob<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    pub volume: usize,
    pub target: Option<WalkTarget>,
    block: Vec<u8>,
    key: Vec<u8>,
    last_key: Vec<u8>,
    last_column: Option<ColumnId>,
    counts: Vec<DeadCount>,
}

impl<Backend: Io> WalkJob<Backend> {
    /// Allocate the walk's fixed buffers
    pub fn new(state: Arc<EngineState<Backend>>, volume: usize) -> Self {
        Self {
            state,
            volume,
            target: None,
            block: vec![0; ROW_BLOCK_BYTES],
            key: touched_capacity(MAX_KEY_BYTES as usize, 0),
            last_key: touched_capacity(MAX_KEY_BYTES as usize, 0),
            last_column: None,
            counts: touched_capacity(MOST_BLOCK_ROWS, DeadCount::default()),
        }
    }

    /// Read the heap the walk's buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let keys = self.key.capacity() + self.last_key.capacity();
        ByteCount((self.block.capacity() + keys + self.counts.capacity() * size_of::<DeadCount>()) as u64)
    }

    /// Pick the lowest segment whose walk hasn't finished
    pub fn pick_target(&mut self) -> bool {
        let state = Arc::clone(&self.state);
        let rows = hold(&state.volumes[self.volume].summary);
        let waiting = rows.rows().filter(|row| !row.facts.is_walked());
        self.target = waiting.min_by_key(|row| row.facts.segment).map(|row| WalkTarget {
            segment: row.facts.segment,
            next_block: row.facts.walked_blocks,
            block_count: row.facts.block_count,
            is_backward: row.facts.is_backward,
            is_damaged: row.facts.is_damaged,
        });
        self.last_column = None;
        self.target.is_some()
    }

    /// Walk one row block, and land its counts and the new position under one hold
    pub fn walk_block(&mut self, target: WalkTarget) -> Result<()> {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        self.counts.clear();

        // a damaged segment answers for nothing, so its rows count nothing and the walk moves past it
        let pin = volume.table.pin(target.segment).filter(|_pin| !target.is_damaged);
        let block = volume.table.walk_block(target.segment, target.next_block);
        let mut next_block = target.block_count;
        if let (Some(pin), Some(block)) = (pin, block) {
            next_block = target.next_block + 1;
            if self.last_column.is_none() && target.next_block > 0 {
                self.load_last_key(volume, &pin, target)?;
            }
            match self.count_block(volume, &pin, &block, target) {
                Err(error) if is_device_error(&error) => self.counts.clear(),
                outcome => outcome?,
            }
        }

        self.counts.sort_unstable_by_key(|count| count.segment);
        hold(&volume.summary).apply_walk(target.segment, next_block, &self.counts);
        for count in self.counts.iter().filter(|count| count.rows > 0) {
            volume.table.drop_live(count.segment, count.block, count.rows);
        }
        if next_block < target.block_count {
            self.target = Some(WalkTarget { next_block, ..target });
            return Ok(());
        }

        volume.table.mark_walked(target.segment);
        self.target = None;
        state.save_summary(volume)
    }

    /// Find where the key before this block ended, since a key's versions can run across two blocks
    pub fn load_last_key(
        &mut self,
        volume: &Volume<Backend>,
        pin: &SegmentPin<'_, Backend::File>,
        target: WalkTarget,
    ) -> Result<()> {
        let Some(block) = volume.table.walk_block(target.segment, target.next_block - 1) else {
            return Ok(());
        };
        let length = block.entry.block_length().0 as usize;
        pin.file.read_exact_at(block.entry.block_at(), &mut self.block[..length])?;
        let Some(mut walk) = RowBlockWalk::open(&self.block[..length], pin.head.place_base) else {
            return Ok(());
        };
        while let Some(row) = walk.next_row(&mut self.key) {
            if row.kind != RowKind::RangeDelete {
                self.last_key.clear();
                self.last_key.extend_from_slice(&self.key);
                self.last_column = Some(block.column);
            }
        }
        Ok(())
    }

    /// Count one block's rows
    pub fn count_block(
        &mut self,
        volume: &Volume<Backend>,
        pin: &SegmentPin<'_, Backend::File>,
        block: &WalkBlock,
        target: WalkTarget,
    ) -> Result<()> {
        let length = block.entry.block_length().0 as usize;
        pin.file.read_exact_at(block.entry.block_at(), &mut self.block[..length])?;
        let Some(mut walk) = RowBlockWalk::open(&self.block[..length], pin.head.place_base) else {
            return Ok(());
        };
        // a key's rows run newest first, so the first one the walk meets is its top, and carrying the last key across
        // a block is enough
        let mut last_place = WritePlace(u64::MAX);

        while let Some(row) = walk.next_row(&mut self.key) {
            debug_assert!(self.last_column != Some(block.column) || self.last_key != self.key || row.place < last_place);
            last_place = row.place;
            if row.kind == RowKind::RangeDelete {
                continue;
            }
            if target.is_backward {
                add_count(&mut self.counts, target.segment, block.column, target.next_block, &row, false);
            }
            let is_top = self.last_column != Some(block.column) || self.last_key != self.key;
            if !is_top {
                add_count(&mut self.counts, target.segment, block.column, target.next_block, &row, true);
                continue;
            }

            self.last_key.clear();
            self.last_key.extend_from_slice(&self.key);
            self.last_column = Some(block.column);
            let found = self.state.newest_walked(volume, block.column, &self.key, target.segment);
            let newest = match found {
                Ok(newest) => newest,
                // a version a damaged block hides counts nothing, which only understates
                Err(StoreError::Corruption { .. }) => None,
                Err(_error) => return Err(Error::other("searching for a newer version")),
            };
            match newest {
                Some(found) if found.row.place > row.place => {
                    add_count(&mut self.counts, target.segment, block.column, target.next_block, &row, true);
                },
                Some(found) if !target.is_backward => {
                    if let Some(index) = volume.table.block_at(found.segment, found.block_offset) {
                        add_count(&mut self.counts, found.segment, block.column, index, &found.row, true);
                    }
                },
                Some(_) | None => {},
            }
        }
        Ok(())
    }
}

impl<Backend: Io> Job for WalkJob<Backend> {
    fn step(&mut self) -> JobStep {
        if self.state.volumes[self.volume].has_failed() {
            return JobStep::Idle;
        }
        if self.target.is_none() && !self.pick_target() {
            return JobStep::Idle;
        }
        let Some(target) = self.target else {
            return JobStep::Idle;
        };
        match self.walk_block(target) {
            Ok(()) => JobStep::Worked,
            Err(_) => {
                let now = self.state.io.now();
                JobStep::IdleUntil {
                    deadline: Nanoseconds(now.0 + WALK_RETRY.0),
                }
            },
        }
    }
}

/// Add one row to a block's counts. A dead row moves its bytes from live to dead, and a backward walk first adds
/// every row's bytes as live, since it rebuilds them
pub fn add_count(
    counts: &mut Vec<DeadCount>,
    segment: SegmentNumber,
    column: ColumnId,
    block: u32,
    row: &FooterRow,
    is_dead: bool,
) {
    // keys run in order, so a repeat is near the end, and a miss only costs another entry
    let recent = counts.len().saturating_sub(COUNT_SEARCH);
    let found = counts[recent..].iter().rposition(|count| count.segment == segment && count.block == block);
    let index = match found {
        Some(index) => recent + index,
        None => {
            counts.push(DeadCount {
                segment,
                column,
                block,
                ..DeadCount::default()
            });
            counts.len() - 1
        },
    };
    let count = &mut counts[index];
    if !is_dead {
        count.kept_bytes = ByteCount(count.kept_bytes.0 + row.length.0);
        return;
    }
    count.rows += 1;
    count.graves += u32::from(row.kind == RowKind::Grave);
    count.bytes = ByteCount(count.bytes.0 + row.length.0);
}
