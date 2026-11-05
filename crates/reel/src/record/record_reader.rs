//! Reads a record back and checks every byte it returns

use std::io::Result;
use std::ops::Range;

use reel_api::reads::ValueWindow;
use reel_api::units::{ByteCount, FileOffset};

use crate::io::IoFile;
use crate::record::chunk_table::{ChunkSpan, ENTRIES_PER_PAGE, TABLE_PAGE_BYTES, chunk_span, page_bytes, table_bytes};
use crate::record::codec::{CHUNK_BYTES, Codec};
use crate::record::lz4_block::decompress;
use crate::record::record_check::{RecordIdentity, checksum, record_check};
use crate::record::record_head::{
    FORM_AT, RecordForm, SMALL_HEAD_BYTES, chunk_count, chunked_head_bytes, read_check, read_page_sum,
    read_value_length,
};

/// Room for a head beside the first table page, which one read fills
pub const FIRST_READ_BYTES: usize = TABLE_PAGE_BYTES + 4 * 1024;
/// A record this long or less fits the first read whole
pub const SMALL_RECORD_BYTES: usize = CHUNK_BYTES + SMALL_HEAD_BYTES;

/// Where a record sits in its segment
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordLocation {
    pub offset: FileOffset,
    pub length: ByteCount,
}

/// How a read ended
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordRead {
    /// Every byte passed its check
    Valid,
    /// The bytes aren't this key's record, or they rotted
    Failed,
}

/// A chunked record's checked head
#[derive(Clone, Copy, Debug)]
pub struct ChunkedHead {
    pub codec: Codec,
    pub value_length: ByteCount,
    pub chunks: u64,
    pub head_length: usize,
}

/// The buffers one read borrows from the pool
#[derive(Debug)]
pub struct RecordReader {
    first: Vec<u8>,
    page: Vec<u8>,
    chunk: Vec<u8>,
    decoded: Vec<u8>,
}

impl RecordReader {
    /// Allocate the buffers
    pub fn new() -> Self {
        Self {
            first: vec![0; FIRST_READ_BYTES],
            page: vec![0; TABLE_PAGE_BYTES],
            chunk: vec![0; CHUNK_BYTES],
            decoded: vec![0; CHUNK_BYTES],
        }
    }

    /// Read the heap these buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let total = self.first.capacity() + self.page.capacity() + self.chunk.capacity() + self.decoded.capacity();
        ByteCount(total as u64)
    }

    /// Fill the buffer with the whole value
    pub fn read_value<File: IoFile>(
        &mut self,
        file: &File,
        location: RecordLocation,
        identity: &RecordIdentity,
        buf: &mut Vec<u8>,
    ) -> Result<RecordRead> {
        let length = self.read_front(file, location)?;
        let Some(form) = self.form() else {
            return Ok(RecordRead::Failed);
        };
        if !form.is_chunked {
            return Ok(self.finish_small(form, length, identity, buf));
        }

        let Some(head) = self.check_chunked(form, length, identity) else {
            return Ok(RecordRead::Failed);
        };
        let window = ValueWindow {
            offset: ByteCount(0),
            length: head.value_length,
        };
        self.read_chunks(file, location, &head, window, buf)
    }

    /// Fill the buffer with a window of the value, cut at the value's end
    pub fn read_window<File: IoFile>(
        &mut self,
        file: &File,
        location: RecordLocation,
        identity: &RecordIdentity,
        window: ValueWindow,
        buf: &mut Vec<u8>,
    ) -> Result<RecordRead> {
        let length = self.read_front(file, location)?;
        let Some(form) = self.form() else {
            return Ok(RecordRead::Failed);
        };
        if !form.is_chunked {
            let read = self.finish_small(form, length, identity, buf);
            cut_to_window(buf, window);
            return Ok(read);
        }

        let Some(head) = self.check_chunked(form, length, identity) else {
            return Ok(RecordRead::Failed);
        };
        self.read_chunks(file, location, &head, window, buf)
    }

    /// Read the head and the first table page, or all of a small record, in one read
    pub fn read_front<File: IoFile>(&mut self, file: &File, location: RecordLocation) -> Result<usize> {
        let length = (location.length.0 as usize).min(FIRST_READ_BYTES);
        file.read_exact_at(location.offset, &mut self.first[..length])?;
        Ok(length)
    }

    /// Read the form byte of the first read
    pub fn form(&self) -> Option<RecordForm> {
        RecordForm::from_byte(*self.first.get(FORM_AT)?)
    }

    /// Check a one-chunk record and hand its value over
    pub fn finish_small(
        &mut self,
        form: RecordForm,
        length: usize,
        identity: &RecordIdentity,
        buf: &mut Vec<u8>,
    ) -> RecordRead {
        if length < SMALL_HEAD_BYTES {
            return RecordRead::Failed;
        }
        let payload = &self.first[SMALL_HEAD_BYTES..length];
        let check = record_check(identity, form.as_byte(), checksum(payload));
        if read_check(&self.first) != Some(check) {
            return RecordRead::Failed;
        }

        buf.clear();
        match form.codec {
            Codec::None => buf.extend_from_slice(payload),
            Codec::Lz4 => {
                let Some(restored) = decompress(payload, &mut self.decoded) else {
                    return RecordRead::Failed;
                };
                buf.extend_from_slice(&self.decoded[..restored]);
            },
        }
        RecordRead::Valid
    }

    /// Check a chunked record's head against the record check
    pub fn check_chunked(&self, form: RecordForm, length: usize, identity: &RecordIdentity) -> Option<ChunkedHead> {
        let value_length = read_value_length(&self.first)?;
        let head_length = chunked_head_bytes(value_length);
        if head_length > length {
            return None;
        }

        let check = record_check(identity, form.as_byte(), checksum(&self.first[FORM_AT..head_length]));
        if read_check(&self.first) != Some(check) {
            return None;
        }
        Some(ChunkedHead {
            codec: form.codec,
            value_length,
            chunks: chunk_count(value_length),
            head_length,
        })
    }

    /// Fill the buffer from the chunks a window touches
    pub fn read_chunks<File: IoFile>(
        &mut self,
        file: &File,
        location: RecordLocation,
        head: &ChunkedHead,
        window: ValueWindow,
        buf: &mut Vec<u8>,
    ) -> Result<RecordRead> {
        buf.clear();
        let start = window.offset.0.min(head.value_length.0);
        let end = start.saturating_add(window.length.0).min(head.value_length.0);
        if start == end {
            return Ok(RecordRead::Valid);
        }

        let first_chunk = start / CHUNK_BYTES as u64;
        let last_chunk = (end - 1) / CHUNK_BYTES as u64;
        let mut loaded_page = None;
        for chunk in first_chunk..=last_chunk {
            let page = chunk / ENTRIES_PER_PAGE as u64;
            if loaded_page != Some(page) {
                if !self.load_page(file, location, head, page)? {
                    return Ok(RecordRead::Failed);
                }
                loaded_page = Some(page);
            }

            let Some(span) = self.span_of(location, head, chunk, page) else {
                return Ok(RecordRead::Failed);
            };
            let chunk_start = chunk * CHUNK_BYTES as u64;
            let from = start.saturating_sub(chunk_start) as usize;
            let to = (end - chunk_start).min(span.entry.value_length.0) as usize;
            if !self.read_chunk(file, location, head, &span, from..to, buf)? {
                return Ok(RecordRead::Failed);
            }
        }
        Ok(RecordRead::Valid)
    }

    /// Bring a table page into the page buffer and check it against the head
    pub fn load_page<File: IoFile>(
        &mut self,
        file: &File,
        location: RecordLocation,
        head: &ChunkedHead,
        page: u64,
    ) -> Result<bool> {
        let length = page_bytes(head.chunks, page);
        let table_end = head.head_length + table_bytes(head.chunks) as usize;
        let start = head.head_length + page as usize * TABLE_PAGE_BYTES;
        if start + length > table_end {
            return Ok(false);
        }

        if page == 0 && start + length <= FIRST_READ_BYTES.min(location.length.0 as usize) {
            self.page[..length].copy_from_slice(&self.first[start..start + length]);
        } else {
            let offset = FileOffset(location.offset.0 + start as u64);
            file.read_exact_at(offset, &mut self.page[..length])?;
        }

        let sum = checksum(&self.page[..length]);
        Ok(read_page_sum(&self.first, page as usize) == Some(sum))
    }

    /// Find one chunk's stored span in the loaded page
    pub fn span_of(&self, location: RecordLocation, head: &ChunkedHead, chunk: u64, page: u64) -> Option<ChunkSpan> {
        let length = page_bytes(head.chunks, page);
        let index = (chunk % ENTRIES_PER_PAGE as u64) as usize;
        chunk_span(&self.page[..length], index, FileOffset(location.length.0))
    }

    /// Read one chunk whole, check it, and add its part of the window to the buffer
    pub fn read_chunk<File: IoFile>(
        &mut self,
        file: &File,
        location: RecordLocation,
        head: &ChunkedHead,
        span: &ChunkSpan,
        part: Range<usize>,
        buf: &mut Vec<u8>,
    ) -> Result<bool> {
        let stored = span.stored_length.0 as usize;
        if stored > CHUNK_BYTES {
            return Ok(false);
        }

        let offset = FileOffset(location.offset.0 + span.entry.stored_offset.0);
        file.read_exact_at(offset, &mut self.chunk[..stored])?;
        if checksum(&self.chunk[..stored]) != span.entry.checksum {
            return Ok(false);
        }

        let is_compressed = head.codec == Codec::Lz4 && span.stored_length != span.entry.value_length;
        if !is_compressed {
            buf.extend_from_slice(&self.chunk[part]);
            return Ok(true);
        }
        let Some(restored) = decompress(&self.chunk[..stored], &mut self.decoded) else {
            return Ok(false);
        };
        if restored as u64 != span.entry.value_length.0 {
            return Ok(false);
        }
        buf.extend_from_slice(&self.decoded[part]);
        Ok(true)
    }
}

impl Default for RecordReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Keep only a window of a value already in the buffer
pub fn cut_to_window(buf: &mut Vec<u8>, window: ValueWindow) {
    let start = (window.offset.0 as usize).min(buf.len());
    let end = start.saturating_add(window.length.0 as usize).min(buf.len());
    buf.copy_within(start..end, 0);
    buf.truncate(end - start);
}
