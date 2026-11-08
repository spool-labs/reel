//! Rows the dead-byte walk finds dead in a rewrite's claimed inputs, kept as bits in the rewrite's region until its install
//! lands them on the outputs

use crate::fixed_list::FixedList;
use crate::segment::footer_reader::RowAt;
use crate::segment::segment_tail::{MOST_MERGE_SEGMENTS, SegmentNumber};

/// Bits one word of the bitmap holds
pub const WORD_BITS: usize = u64::BITS as usize;

/// A walk's charge on a row of an input a rewrite holds, for the lane's bits
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HeldRow {
    /// The input the row sits in
    pub segment: SegmentNumber,
    /// The lane whose rewrite holds the input
    pub lane: u8,
    /// The row's block in the input
    pub block: u32,
    /// The row's place among its block's rows
    pub at: RowAt,
}

/// One input's place in the bitmap
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HeldInput {
    /// The input
    pub segment: SegmentNumber,
    /// The first word of its blocks' bits
    pub words_at: usize,
    /// Its blocks
    pub block_count: u32,
    /// Words each of its blocks takes, enough for the most rows any of them holds
    pub block_words: usize,
}

/// Why a row's bit can't be set
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Refusal {
    /// The rewrite has begun to land its bits, and takes no more
    Closed,
    /// The row lies outside what the input's blocks were laid out for, or its place is unknown
    NoRoom,
}

/// A lane's bits while its rewrite runs: the walk sets them under the claim lock and the rewrite's drain reads them
#[derive(Debug, Default)]
pub struct HeldBits {
    /// Each input's place in the words
    pub inputs: [HeldInput; MOST_MERGE_SEGMENTS],
    /// Inputs laid out
    pub input_count: usize,
    /// The bits, carved from the rewrite's region
    pub words: FixedList<u64>,
    /// Whether the drain has begun, after which no bit is set
    pub is_closed: bool,
    /// Bits set since the rewrite began holding
    pub marked: u64,
}

impl HeldBits {
    /// Count the words a set of inputs takes, each given by its blocks and the most rows one of them holds
    pub fn words_for(inputs: impl Iterator<Item = (u32, u32)>) -> usize {
        inputs.map(|(block_count, most_block_rows)| block_count as usize * block_words(most_block_rows)).sum()
    }

    /// Lay the inputs out over cleared words
    pub fn open(inputs: impl Iterator<Item = (SegmentNumber, u32, u32)>, words: FixedList<u64>) -> Self {
        let mut held = Self { words, ..Self::default() };
        let mut words_at = 0;
        for (segment, block_count, most_block_rows) in inputs.take(MOST_MERGE_SEGMENTS) {
            let block_words = block_words(most_block_rows);
            held.inputs[held.input_count] = HeldInput { segment, words_at, block_count, block_words };
            held.input_count += 1;
            words_at += block_count as usize * block_words;
        }
        held
    }

    /// Find the word and mask of a row's bit
    pub fn bit_of(&self, row: &HeldRow) -> Result<(usize, u64), Refusal> {
        if !row.at.is_known() {
            return Err(Refusal::NoRoom);
        }
        let input = self.inputs[..self.input_count].iter().find(|input| input.segment == row.segment).ok_or(Refusal::NoRoom)?;
        let index = row.at.index as usize;
        if row.block >= input.block_count || index >= input.block_words * WORD_BITS {
            return Err(Refusal::NoRoom);
        }
        let word = input.words_at + row.block as usize * input.block_words + index / WORD_BITS;
        if word >= self.words.len() {
            return Err(Refusal::NoRoom);
        }
        Ok((word, 1 << (index % WORD_BITS)))
    }

    /// Check a row's bit can be set, setting nothing, so a block marks all of its rows or none
    pub fn reserve(&self, row: &HeldRow) -> Result<(), Refusal> {
        if self.is_closed {
            return Err(Refusal::Closed);
        }
        self.bit_of(row).map(|_bit| ())
    }

    /// Set a row's bit, which a reserve has passed. Setting it twice changes nothing
    pub fn mark(&mut self, row: &HeldRow) {
        if let Ok((word, mask)) = self.bit_of(row) {
            self.marked += u64::from(self.words[word] & mask == 0);
            self.words[word] |= mask;
        }
    }

    /// Read the bits of one block of an input
    pub fn block_bits(&self, input: usize, block: u32) -> &[u64] {
        let held = &self.inputs[input];
        let start = held.words_at + block as usize * held.block_words;
        &self.words[start..start + held.block_words]
    }
}

/// Count the words one block's rows take
pub fn block_words(most_block_rows: u32) -> usize {
    (most_block_rows as usize).max(1).div_ceil(WORD_BITS)
}
