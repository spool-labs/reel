//! Reads a record back and checks every byte it returns

use std::io::{IoSliceMut, Result};
use std::ops::Range;

use reel_api::reads::ValueWindow;
use reel_api::units::{ByteCount, FileOffset};

use crate::pool::{HollowItem, touched_zeroes};
use crate::io::{DeviceReads, IoFile, ReadOn};
use crate::record::chunk_table::{
    ChunkCount, ChunkIndex, ChunkSpan, ENTRIES_PER_PAGE, TABLE_PAGE_BYTES, TablePageIndex, chunk_span, page_bytes,
    page_count, page_of, table_bytes,
};
use crate::record::codec::{CHUNK_BYTES, Codec};
use crate::record::lz4_block::decompress;
use crate::record::record_check::{RecordIdentity, checksum, place_check, record_check};
use crate::record::record_head::{
    FORM_AT, RecordForm, SMALL_HEAD_BYTES, chunk_count, chunked_head_bytes, head_bytes_for, read_check, read_page_sum,
    read_value_length,
};

/// Room for a head beside the first table page, which one read fills
pub const FIRST_READ_BYTES: usize = TABLE_PAGE_BYTES + 4 * 1024;
/// A longer record can't be a one-chunk record
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
    pub chunks: ChunkCount,
    pub head_length: usize,
}

/// The split a record stored as written has, worked out from its length alone
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WrittenShape {
    pub front_length: usize,
    pub value_length: ByteCount,
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
            first: touched_zeroes(FIRST_READ_BYTES),
            page: touched_zeroes(TABLE_PAGE_BYTES),
            chunk: touched_zeroes(CHUNK_BYTES),
            decoded: touched_zeroes(CHUNK_BYTES),
        }
    }

    /// Read the heap these buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let total = self.first.capacity() + self.page.capacity() + self.chunk.capacity() + self.decoded.capacity();
        ByteCount(total as u64)
    }

    /// Check a one-chunk record still in memory and fill the buffer from it
    pub fn read_held(
        &mut self,
        record: &[u8],
        identity: &RecordIdentity<'_>,
        window: Option<ValueWindow>,
        buf: &mut Vec<u8>,
    ) -> RecordRead {
        self.first[..record.len()].copy_from_slice(record);
        let Some(form) = self.form() else {
            return RecordRead::Failed;
        };
        let read = self.finish_small(form, record.len(), identity, buf);
        if let Some(window) = window {
            cut_to_window(buf, window);
        }
        read
    }

    /// Fill the buffer with the whole value, in one read when the column stores values as written
    pub async fn read_value<File: IoFile>(
        &mut self,
        device: &impl DeviceReads<File>,
        file: &File,
        location: RecordLocation,
        identity: &RecordIdentity<'_>,
        declared: Codec,
        buf: &mut Vec<u8>,
    ) -> Result<RecordRead> {
        if declared == Codec::None
            && let Some(shape) = written_shape(location.length)
            && shape.front_length <= FIRST_READ_BYTES
        {
            buf.resize(shape.value_length.0 as usize, 0);
            let mut parts = [IoSliceMut::new(&mut self.first[..shape.front_length]), IoSliceMut::new(buf)];
            device.read_parts(file, location.offset, &mut parts).await?;

            if let Some(form) = self.form()
                && form.is_chunked
                && form.codec == Codec::None
            {
                let Some(head) = self.check_chunked(form, shape.front_length, identity) else {
                    return Ok(RecordRead::Failed);
                };
                if head.value_length != shape.value_length {
                    return Ok(RecordRead::Failed);
                }
                return self.check_payload(device, file, location, &head, buf).await;
            }
        }

        let length = self.read_front(device, file, location).await?;
        let Some(form) = self.form() else {
            return Ok(RecordRead::Failed);
        };
        if !form.is_chunked {
            return Ok(self.finish_small(form, length, identity, buf));
        }

        let Some(head) = self.check_chunked(form, length, identity) else {
            return Ok(RecordRead::Failed);
        };
        if head.codec == Codec::None {
            return self.read_payload(device, file, location, &head, buf).await;
        }
        let window = ValueWindow {
            offset: ByteCount(0),
            length: head.value_length,
        };
        self.read_chunks(device, file, location, &head, window, buf).await
    }

    /// Fill the buffer with a window of the value, cut at the value's end
    pub async fn read_window<File: IoFile>(
        &mut self,
        device: &impl DeviceReads<File>,
        file: &File,
        location: RecordLocation,
        identity: &RecordIdentity<'_>,
        window: ValueWindow,
        buf: &mut Vec<u8>,
    ) -> Result<RecordRead> {
        let length = self.read_front(device, file, location).await?;
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
        self.read_chunks(device, file, location, &head, window, buf).await
    }

    /// Read the head and the first table page, or all of a small record, in one read
    pub async fn read_front<File: IoFile>(&mut self, device: &impl DeviceReads<File>, file: &File, location: RecordLocation) -> Result<usize> {
        let length = (location.length.0 as usize).min(FIRST_READ_BYTES);
        device.read_exact(file, location.offset, &mut self.first[..length]).await?;
        Ok(length)
    }

    /// Read the form byte of the first read
    pub fn form(&self) -> Option<RecordForm> {
        RecordForm::from_byte(*self.first.get(FORM_AT)?)
    }

    /// Check a small record where it lies and hand its value over, using this reader's room to restore a compressed one
    pub fn check_held(&mut self, record: &[u8], form: RecordForm, identity: &RecordIdentity<'_>, buf: &mut Vec<u8>) -> RecordRead {
        open_small(record, form, identity, &mut self.decoded, buf)
    }

    /// Check a one-chunk record and hand its value over
    pub fn finish_small(
        &mut self,
        form: RecordForm,
        length: usize,
        identity: &RecordIdentity<'_>,
        buf: &mut Vec<u8>,
    ) -> RecordRead {
        open_small(&self.first[..length], form, identity, &mut self.decoded, buf)
    }

    /// Check a chunked record's head against the record check
    pub fn check_chunked(&self, form: RecordForm, length: usize, identity: &RecordIdentity) -> Option<ChunkedHead> {
        let value_length = read_value_length(&self.first)?;
        let head_length = chunked_head_bytes(value_length);
        if head_length > length {
            return None;
        }

        let sum = checksum(&self.first[FORM_AT..head_length]);
        let check = place_check(record_check(identity, form, sum), identity.record_key, identity.place);
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

    /// Read a value stored as written in one read, past a table the first read couldn't hold
    pub async fn read_payload<File: IoFile>(
        &mut self,
        device: &impl DeviceReads<File>,
        file: &File,
        location: RecordLocation,
        head: &ChunkedHead,
        buf: &mut Vec<u8>,
    ) -> Result<RecordRead> {
        buf.resize(head.value_length.0 as usize, 0);
        let payload_start = payload_start(head);
        device.read_exact(file, FileOffset(location.offset.0 + payload_start), buf).await?;
        self.check_payload(device, file, location, head, buf).await
    }

    /// Check each chunk of a value stored as written where it sits in the buffer
    pub async fn check_payload<File: IoFile>(
        &mut self,
        device: &impl DeviceReads<File>,
        file: &File,
        location: RecordLocation,
        head: &ChunkedHead,
        buf: &[u8],
    ) -> Result<RecordRead> {
        let start = payload_start(head);
        for page in 0..page_count(head.chunks).0 {
            let page = TablePageIndex(page);
            if !self.load_page(device, file, location, head, page).await? {
                return Ok(RecordRead::Failed);
            }

            let first = page.0 * ENTRIES_PER_PAGE as u64;
            let last = (first + ENTRIES_PER_PAGE as u64).min(head.chunks.0);
            for chunk in first..last {
                let chunk = ChunkIndex(chunk);
                let Some(span) = self.span_of(location, head, chunk, page) else {
                    return Ok(RecordRead::Failed);
                };
                if !is_chunk_in_place(&span, chunk, start, buf) {
                    return Ok(RecordRead::Failed);
                }
            }
        }
        Ok(RecordRead::Valid)
    }

    /// Fill the buffer from the chunks a window touches
    pub async fn read_chunks<File: IoFile>(
        &mut self,
        device: &impl DeviceReads<File>,
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
            let chunk = ChunkIndex(chunk);
            let page = page_of(chunk);
            if loaded_page != Some(page) {
                if !self.load_page(device, file, location, head, page).await? {
                    return Ok(RecordRead::Failed);
                }
                loaded_page = Some(page);
            }

            let Some(span) = self.span_of(location, head, chunk, page) else {
                return Ok(RecordRead::Failed);
            };
            let chunk_start = chunk.0 * CHUNK_BYTES as u64;
            let from = start.saturating_sub(chunk_start) as usize;
            let to = (end - chunk_start).min(span.entry.value_length.0) as usize;
            if !self.read_chunk(ReadOn::new(device, file), location, head, &span, from..to, buf).await? {
                return Ok(RecordRead::Failed);
            }
        }
        Ok(RecordRead::Valid)
    }

    /// Bring a table page into the page buffer and check it against the head
    pub async fn load_page<File: IoFile>(
        &mut self,
        device: &impl DeviceReads<File>,
        file: &File,
        location: RecordLocation,
        head: &ChunkedHead,
        page: TablePageIndex,
    ) -> Result<bool> {
        let length = page_bytes(head.chunks, page);
        let start = head.head_length + page.0 as usize * TABLE_PAGE_BYTES;
        let table_end = head.head_length + table_bytes(head.chunks).0 as usize;
        if start + length > table_end {
            return Ok(false);
        }

        if page.0 == 0 && start + length <= FIRST_READ_BYTES.min(location.length.0 as usize) {
            self.page[..length].copy_from_slice(&self.first[start..start + length]);
        } else {
            let offset = FileOffset(location.offset.0 + start as u64);
            device.read_exact(file, offset, &mut self.page[..length]).await?;
        }

        let sum = checksum(&self.page[..length]);
        Ok(read_page_sum(&self.first, page) == Some(sum))
    }

    /// Find one chunk's stored span in the loaded page
    pub fn span_of(
        &self,
        location: RecordLocation,
        head: &ChunkedHead,
        chunk: ChunkIndex,
        page: TablePageIndex,
    ) -> Option<ChunkSpan> {
        let length = page_bytes(head.chunks, page);
        let index = (chunk.0 % ENTRIES_PER_PAGE as u64) as usize;
        chunk_span(&self.page[..length], index, FileOffset(location.length.0))
    }

    /// Read one chunk whole, check it, and add its part of the window to the buffer
    pub async fn read_chunk<File: IoFile>(
        &mut self,
        on: ReadOn<'_, impl DeviceReads<File>, File>,
        location: RecordLocation,
        head: &ChunkedHead,
        span: &ChunkSpan,
        part: Range<usize>,
        buf: &mut Vec<u8>,
    ) -> Result<bool> {
        let ReadOn { device, target: file } = on;
        let stored = span.stored_length.0 as usize;
        if stored > CHUNK_BYTES || span.stored_length > span.entry.value_length {
            return Ok(false);
        }

        let offset = FileOffset(location.offset.0 + span.entry.stored_offset.0);
        device.read_exact(file, offset, &mut self.chunk[..stored]).await?;
        if checksum(&self.chunk[..stored]) != span.entry.checksum {
            return Ok(false);
        }

        if span.stored_length == span.entry.value_length {
            buf.extend_from_slice(&self.chunk[part]);
            return Ok(true);
        }
        if head.codec != Codec::Lz4 {
            return Ok(false);
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

/// Find where a chunked record's payload starts
pub fn payload_start(head: &ChunkedHead) -> u64 {
    head.head_length as u64 + table_bytes(head.chunks).0
}

/// Check one chunk of a value stored as written: its span, then its bytes
pub fn is_chunk_in_place(span: &ChunkSpan, chunk: ChunkIndex, payload_start: u64, buf: &[u8]) -> bool {
    let start = chunk.0 * CHUNK_BYTES as u64;
    let is_where_expected = span.entry.stored_offset.0 == payload_start + start;
    if !is_where_expected || span.stored_length != span.entry.value_length {
        return false;
    }

    let end = start + span.entry.value_length.0;
    match buf.get(start as usize..end as usize) {
        Some(bytes) => checksum(bytes) == span.entry.checksum,
        None => false,
    }
}

/// Work out how a record stored as written splits, from its length alone
pub fn written_shape(length: ByteCount) -> Option<WrittenShape> {
    if length.0 as usize <= SMALL_RECORD_BYTES {
        return None;
    }

    let estimate = length.0 / CHUNK_BYTES as u64;
    for chunks in estimate.saturating_sub(1)..=estimate + 1 {
        let chunks = ChunkCount(chunks);
        let front = head_bytes_for(chunks) as u64 + table_bytes(chunks).0;
        let value_length = ByteCount(length.0.checked_sub(front)?);
        if chunk_count(value_length) == chunks {
            return Some(WrittenShape {
                front_length: front as usize,
                value_length,
            });
        }
    }
    None
}

/// Keep only a window of a value already in the buffer
pub fn cut_to_window(buf: &mut Vec<u8>, window: ValueWindow) {
    let start = (window.offset.0 as usize).min(buf.len());
    let end = start.saturating_add(window.length.0 as usize).min(buf.len());
    buf.copy_within(start..end, 0);
    buf.truncate(end - start);
}

impl HollowItem for RecordReader {
    fn hollow() -> Self {
        Self {
            first: Vec::new(),
            page: Vec::new(),
            chunk: Vec::new(),
            decoded: Vec::new(),
        }
    }
}

/// Read a record's form where it lies
pub fn form_of(record: &[u8]) -> Option<RecordForm> {
    RecordForm::from_byte(*record.get(FORM_AT)?)
}

/// Check a one-chunk record and hand its value over, restoring a compressed one through `decoded`
pub fn open_small(
    record: &[u8],
    form: RecordForm,
    identity: &RecordIdentity<'_>,
    decoded: &mut [u8],
    buf: &mut Vec<u8>,
) -> RecordRead {
    let Some(payload) = record.get(SMALL_HEAD_BYTES..) else {
        return RecordRead::Failed;
    };
    let check = place_check(record_check(identity, form, checksum(payload)), identity.record_key, identity.place);
    if read_check(record) != Some(check) {
        return RecordRead::Failed;
    }

    buf.clear();
    match form.codec {
        Codec::None => buf.extend_from_slice(payload),
        Codec::Lz4 => {
            let Some(restored) = decompress(payload, decoded) else {
                return RecordRead::Failed;
            };
            buf.extend_from_slice(&decoded[..restored]);
        },
    }
    RecordRead::Valid
}
