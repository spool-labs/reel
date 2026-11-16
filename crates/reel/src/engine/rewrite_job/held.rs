//! Holding a rewrite's inputs: walk charges on them go to bits the drain lands where each row went

use std::io::{Error, ErrorKind, Result};
use std::mem;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_core::maintenance_report::UnitKind;
use reel_core::units::ByteCount;

use crate::segment::summary_orders::ROW_FOOTER_BYTES;
use crate::engine::dead_bits::INEXACT_KEPT;
use crate::engine::volume::Volume;
use crate::engine::walk_job::add_count;
use crate::guard::hold;
use crate::io::{Io, IoFile};
use crate::fixed_list::FixedList;
use crate::engine::held_bits::{HeldBits, WORD_BITS};
use crate::segment::row_block::RowBlockWalk;
use crate::segment::segment_tail::SegmentNumber;
use crate::segment::summary::DeadCount;

use crate::engine::rewrite_job::{HOME_BLOCK_BITS, HOME_INDEX_BITS, NO_HOME, RewriteJob, RewritePhase};

impl<Backend: Io> RewriteJob<Backend> {
    /// Hold the inputs from here on: lay their bits out over the region's words and publish them on the lane, so a walk that
    /// would charge an input sets a bit, which the drain lands on the outputs before the install
    pub fn hold_inputs(&mut self, volume: &Volume<Backend>) {
        let inputs = self.inputs[..self.input_count].iter().map(|input| (input.segment, input.block_count, input.most_block_rows));
        let bits = HeldBits::open(inputs, mem::take(&mut self.held_words));
        let _claim = hold(&volume.claim_lock);
        *hold(&volume.lanes[self.lane].held) = bits;
        volume.lanes[self.lane].is_holding.store(true, Ordering::Release);
        volume.claims_changed.fetch_add(1, Ordering::AcqRel);
        self.is_holding = true;
        self.home_count = 0;
        self.drain_input = 0;
        self.drain_block = 0;
    }

    /// Note where the row being kept went: its output, its block there and its place among the block's rows, for a held bit on it
    pub fn note_home(&mut self, output: SegmentNumber, block: u32, index: u32) {
        if self.row_home.is_empty() {
            return;
        }
        let known = self.home_outputs[..self.home_count].iter().position(|number| *number == output);
        let slot = match known {
            Some(index) => index,
            None if self.home_count < self.home_outputs.len() => {
                self.home_outputs[self.home_count] = output;
                self.home_count += 1;
                self.home_count - 1
            },
            None => return,
        };
        // a block past what a home holds leaves the row with none, and its held bit counts again at the output's next walk
        if block >= 1 << HOME_BLOCK_BITS {
            return;
        }
        if let Some(home) = self.row_home.get_mut(self.row_at as usize) {
            *home = ((slot as u32) << (HOME_BLOCK_BITS + HOME_INDEX_BITS)) | (block << HOME_INDEX_BITS) | index;
        }
    }

    /// Land one held block on the outputs, closing the bits at the first step so no walk sets another, and go on to the
    /// install once none is left
    pub fn drain_step(&mut self) -> Result<()> {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        if !self.held.is_closed {
            let _claim = hold(&volume.claim_lock);
            let mut held = hold(&volume.lanes[self.lane].held);
            self.held = mem::take(&mut *held);
            self.held.is_closed = true;
            // the lane keeps refusing, so a block that would set a bit waits for the release and counts again
            held.is_closed = true;
        }
        self.unit = (UnitKind::HeldDrain, 0);
        let Some((input, block)) = self.next_held_block() else {
            self.phase = RewritePhase::Installing;
            return Ok(());
        };
        let landed = self.land_held_block(volume, input, block, true)?;
        self.unit.1 = landed;
        self.drain_block = block + 1;
        Ok(())
    }

    /// Find the next input block with a bit set, from where the drain stands
    pub fn next_held_block(&mut self) -> Option<(usize, u32)> {
        while self.drain_input < self.held.input_count {
            let input = self.drain_input;
            let block_count = self.held.inputs[input].block_count;
            while self.drain_block < block_count {
                let block = self.drain_block;
                if self.held.block_bits(input, block).iter().any(|word| *word != 0) {
                    return Some((input, block));
                }
                self.drain_block += 1;
            }
            self.drain_input += 1;
            self.drain_block = 0;
        }
        None
    }

    /// Read one held block of an input and count each held row dead where it went: on the output block a home says, nothing
    /// for a row the merge dropped, or on the input itself when the rewrite gives way. Return the rows counted
    pub fn land_held_block(&mut self, volume: &Volume<Backend>, input: usize, block: u32, is_to_outputs: bool) -> Result<u64> {
        let segment = self.held.inputs[input].segment;
        let (Some(pin), Some(line)) = (volume.table.pin(segment), volume.table.walk_block(segment, block)) else {
            return Ok(0);
        };
        let length = line.entry.block_length().0 as usize;
        if length > self.buffer.len() {
            return Err(Error::from(ErrorKind::InvalidData));
        }
        pin.file.read_exact_at(line.entry.block_at(), &mut self.buffer[..length])?;
        volume.held_drain_blocks.fetch_add(1, Ordering::Relaxed);
        volume.held_drain_bytes.fetch_add(length as u64, Ordering::Relaxed);
        // a block that no longer reads lands nothing, and its rows count again at its segment's next walk
        let Some(mut walk) = RowBlockWalk::open_checked(&self.buffer[..length], pin.head.place_base) else {
            return Ok(0);
        };
        let first = self.inputs.get(input).and_then(|merge_input| Some(merge_input.base + merge_input.block_first(block)?));
        let replacement = self.state.read_point(volume);
        let bits = self.held.block_bits(input, block);
        let mut landed = 0;
        while let Some(row) = walk.next_row(&mut self.key) {
            let index = walk.row_index();
            if bits.get(index / WORD_BITS).is_none_or(|word| word >> (index % WORD_BITS) & 1 == 0) {
                continue;
            }
            let (segment, block, index) = if is_to_outputs {
                let home = first.and_then(|first| self.row_home.get((first + index as u64) as usize)).copied().unwrap_or(NO_HOME);
                // the merge dropped the row, so nothing it went to holds it
                if home == NO_HOME {
                    continue;
                }
                let output = self.home_outputs[(home >> (HOME_BLOCK_BITS + HOME_INDEX_BITS)) as usize];
                (output, (home >> HOME_INDEX_BITS) & ((1 << HOME_BLOCK_BITS) - 1), home & ((1 << HOME_INDEX_BITS) - 1))
            } else {
                (segment, block, index as u32)
            };
            if self.drained.len() == self.drained.capacity() {
                coalesce(&mut self.drained);
            }
            // a row whose bit is set counted when it was set, and one with no exact bits to set leaves its segment's bits to stop answering
            match volume.dead_bits.set_row(segment, block, index) {
                Some(false) => continue,
                Some(true) => {},
                None => volume.dead_bits.spoil_segment(segment, INEXACT_KEPT),
            }
            // a bit set with no count to match leaves the segment's bits to stop answering
            add_count(&mut self.drained, segment, line.column, block, &row, Some(replacement)).map_err(|_full| {
                volume.dead_bits.spoil_segment(segment, INEXACT_KEPT);
                Error::from(ErrorKind::OutOfMemory)
            })?;
            landed += 1;
        }
        Ok(landed)
    }

    /// Land the bits on the inputs themselves, for a rewrite that gives way before its install: the inputs stay, and a row
    /// the walk found dead in one counts there
    pub fn drain_to_inputs(&mut self, volume: &Volume<Backend>) {
        if !self.is_holding {
            return;
        }
        if !self.held.is_closed {
            let _claim = hold(&volume.claim_lock);
            let mut held = hold(&volume.lanes[self.lane].held);
            self.held = mem::take(&mut *held);
            self.held.is_closed = true;
            held.is_closed = true;
        }
        self.drained.clear();
        self.drain_input = 0;
        self.drain_block = 0;
        while let Some((input, block)) = self.next_held_block() {
            // a block that fails to read leaves its rows to count again at its segment's next walk
            let _landed = self.land_held_block(volume, input, block, false);
            self.drain_block = block + 1;
        }
        self.land_drained(volume);
    }

    /// Land the drain's counts on the summary and the table
    pub fn land_drained(&mut self, volume: &Volume<Backend>) {
        if self.drained.is_empty() {
            return;
        }
        coalesce(&mut self.drained);
        let mut rows = hold(&volume.summary);
        let mut first = 0;
        while let Some(count) = self.drained.get(first) {
            let length = self.drained[first..].partition_point(|other| other.segment == count.segment);
            rows.apply_counts(count.segment, &self.drained[first..first + length], None);
            first += length;
        }
        drop(rows);
        for count in self.drained.iter().filter(|count| count.rows > 0) {
            // a walk that charged the input while the merge ran may still be in flight, so the output reads these apart until it ends
            if let Some(walk) = volume.walk_in_flight() {
                volume.table.note_unbacked(count.segment, count.bytes.0 + u64::from(count.rows) * ROW_FOOTER_BYTES, walk);
            }
            volume.table.drop_live(count.segment, count.block, count.rows, count.replacement);
        }
        self.drained.clear();
    }

    /// Stop holding the inputs, once the install has retired them or the rewrite has given way, and bring a walk that waited
    /// on them back to count again
    pub fn release_hold(&mut self, volume: &Volume<Backend>) {
        if !self.is_holding {
            return;
        }
        let _claim = hold(&volume.claim_lock);
        volume.lanes[self.lane].is_holding.store(false, Ordering::Release);
        *hold(&volume.lanes[self.lane].held) = HeldBits::default();
        volume.claims_changed.fetch_add(1, Ordering::AcqRel);
        self.held = HeldBits::default();
        self.is_holding = false;
    }
}

/// Sort counts by segment and block and fold the ones of one block together
pub fn coalesce(counts: &mut FixedList<DeadCount>) {
    counts.sort_unstable_by_key(|count| (count.segment, count.block));
    let mut kept = 0;
    for index in 0..counts.len() {
        let count = counts[index];
        if kept > 0 && counts[kept - 1].segment == count.segment && counts[kept - 1].block == count.block {
            let into = &mut counts[kept - 1];
            into.rows += count.rows;
            into.graves += count.graves;
            into.bytes = ByteCount(into.bytes.0 + count.bytes.0);
            into.kept_bytes = ByteCount(into.kept_bytes.0 + count.kept_bytes.0);
            into.replacement = into.replacement.max(count.replacement);
        } else {
            counts[kept] = count;
            kept += 1;
        }
    }
    counts.truncate(kept);
}
