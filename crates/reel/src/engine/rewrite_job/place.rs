//! Placing an offset-order merge's records: each input's kept records go across in offset order, a run to a read

use std::io::{Error, ErrorKind, IoSlice, Result};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_api::maintenance_report::{
    SPAN_RUN_BUCKETS, UnitKind,
};
use reel_api::units::FileOffset;

use crate::io::{Io, IoFile};

use crate::engine::rewrite_job::{COPY_STEP_BYTES, MERGE_STEP_ROWS, RewriteJob, RewritePhase, SPAN_GAP_BYTES, StripeRow};

impl<Backend: Io> RewriteJob<Backend> {
    /// Walk a step of an input's rows, listing its marked ones, and once its rows run out copy them in offset order a step at a
    /// time, then go on to the next input, and to the copying pass once every input's records are across
    pub fn place_rows(&mut self) -> Result<()> {
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        let input = self.placing_input;
        if input >= self.input_count {
            return self.end_placing();
        }
        let Some(pin) = volume.table.pin(self.numbers[input]) else {
            return Err(Error::from(ErrorKind::NotFound));
        };
        if self.stripe_copied.is_some() {
            return self.copy_stripe(&pin.file).map_err(|error| self.note_unreadable(input, error));
        }
        if self.inputs[input].row.is_none() && self.inputs[input].length == 0 {
            self.inputs[input].advance(volume, &pin).map_err(|error| self.note_unreadable(input, error))?;
        }
        for _row in 0..MERGE_STEP_ROWS {
            self.unit.1 += 1;
            let Some(row) = self.inputs[input].row else {
                // the input's kept records go in offset order, whichever output each one belongs to
                self.stripe.sort_unstable_by_key(|listed| listed.offset);
                self.stripe_copied = Some(0);
                return self.copy_stripe(&pin.file).map_err(|error| self.note_unreadable(input, error));
            };
            let at = (self.inputs[input].base + self.inputs[input].ordinal()) as usize;
            if self.is_marked(at) {
                // a record's place and length fit four bytes each, as the places the copy keeps do
                let (Ok(offset), Ok(length)) = (u32::try_from(row.offset.0), u32::try_from(row.length.0)) else {
                    return Err(Error::from(ErrorKind::InvalidData));
                };
                self.stripe.push(StripeRow { offset, length, ordinal: at as u32 })?;
            }
            self.inputs[input].advance(volume, &pin).map_err(|error| self.note_unreadable(input, error))?;
        }
        Ok(())
    }

    /// Copy a step of an input's listed records in offset order, a run to a read, staging each into its output's buffer, and once
    /// they're all across go on to the next input
    pub fn copy_stripe(&mut self, from: &Backend::File) -> Result<()> {
        // a step takes runs until a buffer's bytes are across, and a run ends only at a gap or the buffer's edge
        let end = self.stripe.len();
        let (mut lone, mut runs, mut run_records, mut gaps, mut copied) = (0, 0, 0, 0, 0);
        let mut index = self.stripe_copied.unwrap_or(0);
        let state = Arc::clone(&self.state);
        let volume = &state.volumes[self.volume];
        while index < end && copied < COPY_STEP_BYTES {
            let last = self.run_end(index, end);
            copied += self.stripe[index..=last].iter().map(|listed| u64::from(listed.length)).sum::<u64>();
            gaps += self.copy_run(from, index, last)?;
            let records = (last + 1 - index) as u64;
            if records == 1 {
                lone += 1;
            } else {
                runs += 1;
                run_records += records;
            }
            volume.span_runs[(records.ilog2() as usize).min(SPAN_RUN_BUCKETS - 1)].fetch_add(1, Ordering::Relaxed);
            index = last + 1;
        }
        self.unit = (UnitKind::StripeCopy, copied);
        volume.lone_copies.fetch_add(lone, Ordering::Relaxed);
        volume.span_reads.fetch_add(runs, Ordering::Relaxed);
        volume.span_records.fetch_add(run_records, Ordering::Relaxed);
        volume.span_gap_bytes.fetch_add(gaps, Ordering::Relaxed);
        if index < end {
            self.stripe_copied = Some(index);
            return Ok(());
        }
        self.stripe_copied = None;
        self.stripe.clear();
        self.placing_input += 1;
        Ok(())
    }

    /// Read an output's file: the first is the output in hand, and the later ones wait in their slots
    pub fn slot_file(&self, slot: usize) -> Result<Backend::File> {
        let output = if slot == 0 { self.output.as_ref() } else { self.slot_outputs.get(slot).and_then(Option::as_ref) };
        output.map(|output| output.file.clone()).ok_or_else(|| Error::from(ErrorKind::NotFound))
    }

    /// End the placing: write out every output's staged records, and start the copying pass on the first output
    pub fn end_placing(&mut self) -> Result<()> {
        for output in 0..=self.cuts {
            self.flush_stage(output)?;
        }
        self.records_end = FileOffset(self.outputs_end[0]);
        self.used = 0;
        for input in &mut self.inputs[..self.input_count] {
            input.rewind();
        }
        self.phase = RewritePhase::Copying;
        Ok(())
    }

    /// Find the last listed record a run from this one takes: each next one starts within the gap bound of the one before, and
    /// the run's bytes fit the read buffer
    pub fn run_end(&self, index: usize, end: usize) -> usize {
        let start = self.stripe[index].start();
        let mut span_end = self.stripe[index].end();
        let mut last = index;
        while last + 1 < end {
            let next = self.stripe[last + 1];
            let next_end = next.end();
            if next.start() > span_end + SPAN_GAP_BYTES || next_end - start > self.buffer.len() as u64 {
                break;
            }
            span_end = next_end;
            last += 1;
        }
        last
    }

    /// Copy a run of listed records with one read into the buffer, staging each into its output's buffer, and return the gap bytes
    /// the read took. A record past the buffer goes across alone, in parts
    pub fn copy_run(&mut self, from: &Backend::File, index: usize, last: usize) -> Result<u64> {
        let start = self.stripe[index].start();
        let span = (self.stripe[last].end() - start) as usize;
        if span > self.buffer.len() {
            return self.copy_large(from, index).map(|()| 0);
        }
        from.read_exact_at(FileOffset(start), &mut self.buffer[..span])?;
        let mut kept = 0;
        for position in index..=last {
            let listed = self.stripe[position];
            let output = self.listed_output(listed.ordinal);
            let at = self.stage(output, (listed.start() - start) as usize, listed.length as usize)?;
            self.place(listed.ordinal, at)?;
            kept += u64::from(listed.length);
        }
        Ok(span as u64 - kept)
    }

    /// Read which output a listed record of the input being placed goes to
    pub fn listed_output(&self, ordinal: u32) -> usize {
        let input = self.placing_input;
        self.output_of(input, u64::from(ordinal) - self.inputs[input].base)
    }

    /// Put a record from the read buffer in its output's stage, writing the stage out first when it lacks the room, and return
    /// where the record lands in the output
    pub fn stage(&mut self, output: usize, read: usize, length: usize) -> Result<FileOffset> {
        if length > self.stages[output].room() {
            self.flush_stage(output)?;
        }
        let at = self.outputs_end[output] + self.stages[output].len() as u64;
        // the row blocks sit past this, and a key's versions kept for a cue are all that can reach them
        if self.target.is_none_or(|target| at + length as u64 > target.blocks_at.0) {
            return Err(Error::from(ErrorKind::OutOfMemory));
        }
        if length > self.stages[output].capacity() {
            let file = self.slot_file(output)?;
            file.write_at(FileOffset(at), &[IoSlice::new(&self.buffer[read..read + length])])?;
            self.outputs_end[output] += length as u64;
            return Ok(FileOffset(at));
        }
        self.stages[output].extend_from_slice(&self.buffer[read..read + length])?;
        Ok(FileOffset(at))
    }

    /// Write an output's staged records out at its records' end
    pub fn flush_stage(&mut self, output: usize) -> Result<()> {
        if self.stages[output].is_empty() {
            return Ok(());
        }
        let file = self.slot_file(output)?;
        file.write_at(FileOffset(self.outputs_end[output]), &[IoSlice::new(&self.stages[output])])?;
        self.outputs_end[output] += self.stages[output].len() as u64;
        self.stages[output].clear();
        Ok(())
    }

    /// Copy one listed record past the read buffer to its output in parts, after the records staged before it
    pub fn copy_large(&mut self, from: &Backend::File, index: usize) -> Result<()> {
        let listed = self.stripe[index];
        let output = self.listed_output(listed.ordinal);
        self.flush_stage(output)?;
        let at = self.outputs_end[output];
        let length = u64::from(listed.length);
        if self.target.is_none_or(|target| at + length > target.blocks_at.0) {
            return Err(Error::from(ErrorKind::OutOfMemory));
        }
        let file = self.slot_file(output)?;
        let mut done = 0;
        while done < length {
            let part = (length - done).min(self.buffer.len() as u64) as usize;
            from.read_exact_at(FileOffset(listed.start() + done), &mut self.buffer[..part])?;
            file.write_at(FileOffset(at + done), &[IoSlice::new(&self.buffer[..part])])?;
            done += part as u64;
        }
        self.outputs_end[output] += length;
        self.place(listed.ordinal, FileOffset(at))
    }

    /// Note where a listed record landed in the output
    pub fn place(&mut self, ordinal: u32, at: FileOffset) -> Result<()> {
        let Some(placed) = self.placed.get_mut(ordinal as usize) else {
            return Err(Error::from(ErrorKind::InvalidData));
        };
        *placed = at.0 as u32;
        Ok(())
    }
}
