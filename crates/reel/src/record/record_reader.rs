//! Reads a record back and checks every byte it returns

use std::io::{IoSliceMut, Result};
use std::ops::Range;

use reel_core::reads::ValueWindow;
use reel_core::units::{ByteCount, FileOffset};

use crate::pool::{HollowItem, touched_zeroes};
use crate::io::{DeviceReads, IoFile, ManyRead, ReadOn};
use crate::record::chunk_table::{
    ChunkCount, ChunkIndex, ChunkSpan, ENTRIES_PER_PAGE, TABLE_ENTRY_BYTES, TABLE_PAGE_BYTES, TablePageIndex, chunk_span, page_bytes,
    page_count, page_of, table_bytes,
};
use crate::record::codec::{CHUNK_BYTES, Codec};
use crate::record::lz4_block::{decompress, decompress_into_room, decompress_onto};
use crate::little_endian::read_word;
use crate::record::record_check::{Checksum, RecordIdentity, checksum, place_check, record_check};
use crate::record::record_head::{
    FORM_AT, PIECE_BYTES, PIECE_SUM_BYTES, PIECES_PER_CHUNK, RecordForm, SMALL_HEAD_BYTES, chunk_count, chunked_head_bytes,
    head_bytes_for, piece_count, read_check, read_page_sum, sums_bytes, written_chunk_sum,
    read_value_length, holds_short_sum, trusted_sum_holds,
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
    pub sums_at: usize,
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
    /// Chunks whose stored piece sums didn't match their entry while the chunk's bytes did, since the engine last took the count
    damaged_sums: u64,
    /// The form and length of a record `read_trusted` handed over with no record check. Its head stays in `first`, a plain payload in the
    /// caller's buffer and a compressed one beside its head
    trusted: Option<(RecordForm, usize)>,
    /// The offset and length of a short record already whole in `first`, which turned out chunked, so the read that follows reads
    /// it no more
    front: Option<(FileOffset, usize)>,
    /// The length of the record the current read holds whole in `first`, whose chunks it takes from there with no read
    whole_in_first: Option<usize>,
}

impl RecordReader {
    /// Allocate the buffers
    pub fn new() -> Self {
        Self {
            first: touched_zeroes(FIRST_READ_BYTES),
            page: touched_zeroes(TABLE_PAGE_BYTES),
            chunk: touched_zeroes(CHUNK_BYTES),
            decoded: touched_zeroes(CHUNK_BYTES),
            damaged_sums: 0,
            trusted: None,
            front: None,
            whole_in_first: None,
        }
    }

    /// Forget any record an earlier read left in `first`, so a read from another file never takes it
    pub fn forget_front(&mut self) {
        self.front = None;
        self.whole_in_first = None;
        self.trusted = None;
    }

    /// Take the count of damaged sums this reader met since the last take
    pub fn take_damaged_sums(&mut self) -> u64 {
        std::mem::take(&mut self.damaged_sums)
    }

    /// Read the heap these buffers hold
    pub fn heap_bytes(&self) -> ByteCount {
        let total = self.first.capacity() + self.page.capacity() + self.chunk.capacity() + self.decoded.capacity();
        ByteCount(total as u64)
    }

    /// Check a one-chunk record still in memory and fill the buffer from it. The fill copies the record into the reader's room,
    /// and returns false when memory no longer holds it whole, which answers nothing
    pub fn read_held(
        &mut self,
        length: usize,
        fill: impl FnOnce(&mut [u8]) -> bool,
        identity: &RecordIdentity<'_>,
        window: Option<ValueWindow>,
        buf: &mut Vec<u8>,
    ) -> Option<RecordRead> {
        if !fill(&mut self.first[..length]) {
            return None;
        }
        let Some(form) = self.form() else {
            return Some(RecordRead::Failed);
        };
        let read = self.finish_small(form, length, identity, buf);
        if let Some(window) = window {
            cut_to_window(buf, window);
        }
        Some(read)
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
        self.read_value_trusting(device, file, location, (identity, declared), buf, || false).await
    }

    /// Fill the buffer with the whole value, and skip the sums of a value read in one go as written while `holds` says nothing
    /// could have changed its bytes since the read began. The head is still checked against the record's identity
    pub async fn read_value_trusting<File: IoFile>(
        &mut self,
        device: &impl DeviceReads<File>,
        file: &File,
        location: RecordLocation,
        (identity, declared): (&RecordIdentity<'_>, Codec),
        buf: &mut Vec<u8>,
        holds: impl Fn() -> bool,
    ) -> Result<RecordRead> {
        let whole = location.length.0 as usize;
        let mut is_whole_in_first = self.front.take() == Some((location.offset, whole));
        if !is_whole_in_first
            && declared == Codec::None
            && let Some(shape) = written_shape(location.length)
            && shape.front_length <= FIRST_READ_BYTES
        {
            let mut unfilled = UnfilledRead::new(buf, shape.value_length.0 as usize);
            {
                let mut parts = [IoSliceMut::new(&mut self.first[..shape.front_length]), IoSliceMut::new(unfilled.buf())];
                device.read_parts(file, location.offset, &mut parts).await?;
            }
            unfilled.filled();

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
                if holds() {
                    return Ok(RecordRead::Valid);
                }
                return self.check_payload(device, file, location, &head, buf).await;
            }
        }

        // a one-chunk record's payload lands straight in the buffer and is checked there, and only a compressed one is copied as
        // it is restored. A chunked record that short moves beside its head, so it isn't read again
        if !is_whole_in_first && (SMALL_HEAD_BYTES..=SMALL_RECORD_BYTES).contains(&whole) {
            let mut unfilled = UnfilledRead::new(buf, whole - SMALL_HEAD_BYTES);
            {
                let mut parts = [IoSliceMut::new(&mut self.first[..SMALL_HEAD_BYTES]), IoSliceMut::new(unfilled.buf())];
                device.read_parts(file, location.offset, &mut parts).await?;
            }
            unfilled.filled();
            match self.form() {
                Some(form) if !form.is_chunked => return Ok(self.open_split(form, identity, buf)),
                Some(_chunked) => {
                    self.first[SMALL_HEAD_BYTES..whole].copy_from_slice(buf);
                    buf.clear();
                    is_whole_in_first = true;
                },
                None => {
                    buf.clear();
                    return Ok(RecordRead::Failed);
                },
            }
        }

        let length = if is_whole_in_first { whole } else { self.read_front(device, file, location).await? };
        if is_whole_in_first {
            self.whole_in_first = Some(whole);
        }
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
            let mut unfilled = UnfilledRead::new(buf, head.value_length.0 as usize);
            device.read_exact(file, FileOffset(location.offset.0 + payload_start(&head)), unfilled.buf()).await?;
            unfilled.filled();
            if holds() {
                return Ok(RecordRead::Valid);
            }
            return self.check_payload(device, file, location, &head, buf).await;
        }
        let window = ValueWindow {
            offset: ByteCount(0),
            length: head.value_length,
        };
        self.read_chunks(device, file, location, &head, window, buf).await
    }

    /// Read a one-chunk record whose bytes nothing changed since the caller located it, and hand its value over with no record
    /// check: its form still parses, the read takes the located length, and when `is_summed` a payload up to MOST_SUMMED_PAYLOAD
    /// must match its head's stored sum or the read fails. Nothing for any other record, which takes `read_value`. The bytes stay held, so
    /// `check_trusted` can check them after
    pub async fn read_trusted<File: IoFile>(
        &mut self,
        device: &impl DeviceReads<File>,
        file: &File,
        location: RecordLocation,
        buf: &mut Vec<u8>,
        is_summed: bool,
    ) -> Result<Option<RecordRead>> {
        self.trusted = None;
        self.front = None;
        self.whole_in_first = None;
        let length = location.length.0 as usize;
        if length < SMALL_HEAD_BYTES || length > self.first.len() {
            return Ok(None);
        }
        // the head lands in `first` and the payload straight in the buffer, so a plain value is never copied
        let mut unfilled = UnfilledRead::new(buf, length - SMALL_HEAD_BYTES);
        {
            let mut parts = [IoSliceMut::new(&mut self.first[..SMALL_HEAD_BYTES]), IoSliceMut::new(unfilled.buf())];
            device.read_parts(file, location.offset, &mut parts).await?;
        }
        unfilled.filled();
        let Some(form) = self.form() else {
            buf.clear();
            return Ok(None);
        };
        // a plain chunked record reads its payload where it lies, so it isn't copied here
        if form.is_chunked && form.codec == Codec::None {
            buf.clear();
            return Ok(None);
        }
        // a compressed chunked record moves beside its head, so the chunked read that follows reads it no more
        if form.is_chunked {
            self.first[SMALL_HEAD_BYTES..length].copy_from_slice(buf);
            self.front = Some((location.offset, length));
            buf.clear();
            return Ok(None);
        }
        // the stored sum catches rot in a short sealed payload with no identity hashes. A mismatch reads as failed, and a reader
        // whose counts moved reads the record again with every check. An open record this run wrote isn't summed, since its
        // bytes never left this process
        if is_summed && !trusted_sum_holds(&self.first[..SMALL_HEAD_BYTES], buf) {
            buf.clear();
            return Ok(Some(RecordRead::Failed));
        }
        // a compressed payload restores straight from the buffer, and a later check reads the record again
        if form.codec == Codec::Lz4 {
            let Some(restored) = decompress(buf, &mut self.decoded) else {
                buf.clear();
                return Ok(None);
            };
            buf.clear();
            buf.extend_from_slice(&self.decoded[..restored]);
        }
        self.trusted = Some((form, length));
        Ok(Some(RecordRead::Valid))
    }

    /// Check the record `read_trusted` handed over last, for a read whose counts moved while it read. Nothing when the record was
    /// compressed, whose stored bytes the reader didn't keep, or when no record was handed over, and the caller reads it again
    /// with every check
    pub fn check_trusted(&mut self, identity: &RecordIdentity<'_>, buf: &mut Vec<u8>) -> Option<RecordRead> {
        match self.trusted.take() {
            Some((form, _length)) if form.codec == Codec::None => Some(self.open_split(form, identity, buf)),
            Some(_compressed) => None,
            None => None,
        }
    }

    /// Fill the buffer with a window of the value, cut at the value's end
    #[allow(clippy::too_many_arguments, reason = "a record's place and identity, its column's codec, and the window it fills")]
    pub async fn read_window<File: IoFile>(
        &mut self,
        device: &impl DeviceReads<File>,
        file: &File,
        location: RecordLocation,
        identity: &RecordIdentity<'_>,
        declared: Codec,
        window: ValueWindow,
        buf: &mut Vec<u8>,
    ) -> Result<RecordRead> {
        if declared == Codec::None
            && let Some(read) = self.read_written_window(device, file, location, identity, window, buf).await
        {
            return Ok(read);
        }
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

    /// Fill the buffer with a window of a value stored as written, sending the head and the window's 4 KiB pieces to the device
    /// together, since the record's length says where both sit. Each touched chunk's entry checks its pieces' sums, and each
    /// sum checks its piece, so the window checks what it returns and the head, table and sums on the way. Damage in a piece
    /// it didn't read goes unseen by this read, and a whole read or a rewrite finds it. Nothing when the record turns out other
    /// than its length says, or a read comes back short, and the window then takes the long way, which gives the answer that
    /// counts
    pub async fn read_written_window<File: IoFile>(
        &mut self,
        device: &impl DeviceReads<File>,
        file: &File,
        location: RecordLocation,
        identity: &RecordIdentity<'_>,
        window: ValueWindow,
        buf: &mut Vec<u8>,
    ) -> Option<RecordRead> {
        let shape = written_shape(location.length).filter(|shape| shape.sums_at <= FIRST_READ_BYTES)?;
        let value_end = shape.value_length.0;
        let start = window.offset.0.min(value_end);
        let end = start.saturating_add(window.length.0).min(value_end);
        if start == end {
            return None;
        }
        let (piece_bytes, chunk_bytes) = (PIECE_BYTES as u64, CHUNK_BYTES as u64);
        let span_start = start / piece_bytes * piece_bytes;
        let span_end = end.div_ceil(piece_bytes).saturating_mul(piece_bytes).min(value_end);
        let (first_chunk, last_chunk) = (span_start / chunk_bytes, (span_end - 1) / chunk_bytes);
        // the touched chunks' slice of the sums, which their entries check, so the rest of the sums is never read
        let chunk_sums = (PIECES_PER_CHUNK * PIECE_SUM_BYTES) as u64;
        let sums_from = shape.sums_at as u64 + first_chunk * chunk_sums;
        let sums_to = (shape.sums_at as u64 + (last_chunk + 1) * chunk_sums).min(shape.front_length as u64);
        let sums_length = (sums_to - sums_from) as usize;
        if sums_length > self.chunk.len() {
            return None;
        }
        // the read fills every byte, so a buffer long enough already is cut to size and only new room is zeroed
        buf.resize((span_end - span_start) as usize, 0);
        let (front, pieces_at) = (shape.sums_at, FileOffset(location.offset.0 + shape.front_length as u64 + span_start));
        let mut reads = [
            Some(ManyRead { file, offset: location.offset, buf: &mut self.first[..front], is_done: false }),
            Some(ManyRead { file, offset: FileOffset(location.offset.0 + sums_from), buf: &mut self.chunk[..sums_length], is_done: false }),
            Some(ManyRead { file, offset: pieces_at, buf: &mut buf[..], is_done: false }),
        ];
        device.read_many(&mut reads).await;
        if !reads.iter().flatten().all(|read| read.is_done) {
            return None;
        }

        let form = self.form().filter(|form| form.is_chunked && form.codec == Codec::None)?;
        let head = self.check_chunked(form, front, identity).filter(|head| head.value_length == shape.value_length)?;
        let payload = payload_start(&head);
        for chunk in first_chunk..=last_chunk {
            let chunk = ChunkIndex(chunk);
            let page = page_of(chunk);
            // the first page sits in the head read and loads with no read. A value long enough for a second page takes the long way
            if page.0 != 0 || !self.load_page(device, file, location, &head, page).await.ok()? {
                return None;
            }
            let span = self.span_of(location, &head, chunk, page)?;
            let chunk_start = chunk.0 * chunk_bytes;
            if span.entry.stored_offset.0 != payload + chunk_start || span.stored_length != span.entry.value_length {
                return None;
            }
            let sums_from = ((chunk.0 - first_chunk) * chunk_sums) as usize;
            let sums = self.chunk.get(sums_from..sums_from + piece_count(span.entry.value_length) as usize * PIECE_SUM_BYTES)?;
            if checksum(sums) != span.entry.checksum {
                // the sums are damaged, and the long way checks the chunk from its own bytes, which can still be whole
                self.damaged_sums += 1;
                return None;
            }
            // the pieces of this chunk the window read, each against its own sum
            let from = span_start.max(chunk_start);
            let to = span_end.min(chunk_start + span.entry.value_length.0);
            for piece_start in (from..to).step_by(PIECE_BYTES) {
                let piece_end = (piece_start + piece_bytes).min(to);
                let at = ((piece_start - chunk_start) / piece_bytes) as usize * PIECE_SUM_BYTES;
                let stored = Checksum(read_word(sums, at)?);
                let piece = buf.get((piece_start - span_start) as usize..(piece_end - span_start) as usize)?;
                if checksum(piece) != stored {
                    return Some(RecordRead::Failed);
                }
            }
        }
        buf.copy_within((start - span_start) as usize..(end - span_start) as usize, 0);
        buf.truncate((end - start) as usize);
        Some(RecordRead::Valid)
    }

    /// Read the head and the first table page, or all of a small record, in one read
    pub async fn read_front<File: IoFile>(&mut self, device: &impl DeviceReads<File>, file: &File, location: RecordLocation) -> Result<usize> {
        self.whole_in_first = None;
        let length = (location.length.0 as usize).min(FIRST_READ_BYTES);
        device.read_exact(file, location.offset, &mut self.first[..length]).await?;
        // a record no longer than the first read is whole in `first`, so its chunks need no read of their own
        self.whole_in_first = (length == location.length.0 as usize).then_some(length);
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

    /// Check a one-chunk record whose head sits in `first` and whose payload sits in the buffer, and restore a compressed one
    fn open_split(&mut self, form: RecordForm, identity: &RecordIdentity<'_>, buf: &mut Vec<u8>) -> RecordRead {
        if !checks_split(&self.first[..SMALL_HEAD_BYTES], buf, form, identity) {
            buf.clear();
            return RecordRead::Failed;
        }
        if form.codec == Codec::Lz4 {
            let Some(restored) = decompress(buf, &mut self.decoded) else {
                buf.clear();
                return RecordRead::Failed;
            };
            buf.clear();
            buf.extend_from_slice(&self.decoded[..restored]);
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

    /// Check each chunk of a value stored as written where it sits in the buffer, and its stored pieces' sums when the first
    /// read brought them in. Sums that don't match an entry whose chunk passed are counted, and the read goes on
    pub async fn check_payload<File: IoFile>(
        &mut self,
        device: &impl DeviceReads<File>,
        file: &File,
        location: RecordLocation,
        head: &ChunkedHead,
        buf: &[u8],
    ) -> Result<RecordRead> {
        let start = payload_start(head);
        let has_sums = head.codec == Codec::None && start as usize <= FIRST_READ_BYTES.min(location.length.0 as usize);
        let sums_at = sums_start(head) as usize;
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
                if !is_chunk_in_span(&span, chunk, start, buf, 0) {
                    return Ok(RecordRead::Failed);
                }
                let from = sums_at + chunk.0 as usize * PIECES_PER_CHUNK * PIECE_SUM_BYTES;
                let to = from + piece_count(span.entry.value_length) as usize * PIECE_SUM_BYTES;
                // the chunk's bytes passed its entry, so damaged sums cost a window its fast way and nothing more: counted, and
                // the read goes on
                if has_sums && self.first.get(from..to).is_none_or(|sums| checksum(sums) != span.entry.checksum) {
                    self.damaged_sums += 1;
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

    /// Check a record's stored bytes against its identity and its chunks' sums, and hand no value over: a one-chunk record where
    /// the first read holds it, and a chunked one a chunk at a time through the reader's own room, so the check makes no
    /// allocation and restores nothing
    pub async fn check_stored<File: IoFile>(
        &mut self,
        device: &impl DeviceReads<File>,
        file: &File,
        location: RecordLocation,
        identity: &RecordIdentity<'_>,
    ) -> Result<RecordRead> {
        let length = self.read_front(device, file, location).await?;
        let Some(form) = self.form() else {
            return Ok(RecordRead::Failed);
        };
        if !form.is_chunked {
            // a one-chunk record fits the first read whole, and a longer one is no one-chunk record
            let is_sound = self.whole_in_first.is_some() && checked_payload(&self.first[..length], form, identity).is_some();
            return Ok(if is_sound { RecordRead::Valid } else { RecordRead::Failed });
        }
        let Some(head) = self.check_chunked(form, length, identity) else {
            return Ok(RecordRead::Failed);
        };
        for page in 0..page_count(head.chunks).0 {
            let page = TablePageIndex(page);
            if !self.load_page(device, file, location, &head, page).await? {
                return Ok(RecordRead::Failed);
            }
            let first = page.0 * ENTRIES_PER_PAGE as u64;
            let last = (first + ENTRIES_PER_PAGE as u64).min(head.chunks.0);
            for chunk in first..last {
                let Some(span) = self.span_of(location, &head, ChunkIndex(chunk), page) else {
                    return Ok(RecordRead::Failed);
                };
                if !self.check_chunk(ReadOn { device, target: file }, location, &head, &span).await? {
                    return Ok(RecordRead::Failed);
                }
            }
        }
        Ok(RecordRead::Valid)
    }

    /// Say a chunk's stored bytes sit whole in the first read
    fn is_chunk_held(&self, span: &ChunkSpan) -> bool {
        let (at, stored) = (span.entry.stored_offset.0 as usize, span.stored_length.0 as usize);
        self.whole_in_first.is_some_and(|whole| at.checked_add(stored).is_some_and(|end| end <= whole))
    }

    /// Bring a chunk's stored bytes in, from the first read when it holds them and into the reader's chunk room when it doesn't,
    /// and check them against the chunk's sum
    pub async fn check_chunk<File: IoFile>(
        &mut self,
        on: ReadOn<'_, impl DeviceReads<File>, File>,
        location: RecordLocation,
        head: &ChunkedHead,
        span: &ChunkSpan,
    ) -> Result<bool> {
        let ReadOn { device, target: file } = on;
        let stored = span.stored_length.0 as usize;
        if stored > CHUNK_BYTES || span.stored_length > span.entry.value_length {
            return Ok(false);
        }
        let at = span.entry.stored_offset.0 as usize;
        let is_held = self.is_chunk_held(span);
        if !is_held {
            let offset = FileOffset(location.offset.0 + span.entry.stored_offset.0);
            device.read_exact(file, offset, &mut self.chunk[..stored]).await?;
        }
        let source = if is_held { &self.first[at..at + stored] } else { &self.chunk[..stored] };
        // a record stored as written sums a chunk over its pieces' sums, and a compressed one over its stored bytes
        let sum = if head.codec == Codec::None { written_chunk_sum(source) } else { checksum(source) };
        Ok(sum == span.entry.checksum)
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
        if !self.check_chunk(on, location, head, span).await? {
            return Ok(false);
        }
        // a record the read holds whole in `first` gives its chunks from there, and any other read each chunk into the room
        let (at, stored) = (span.entry.stored_offset.0 as usize, span.stored_length.0 as usize);
        let source = if self.is_chunk_held(span) { &self.first[at..at + stored] } else { &self.chunk[..stored] };

        if span.stored_length == span.entry.value_length {
            buf.extend_from_slice(&source[part]);
            return Ok(true);
        }
        if head.codec != Codec::Lz4 {
            return Ok(false);
        }

        // a whole chunk restores straight into the caller's buffer, and only part of one goes through the reader's room
        let length = span.entry.value_length.0 as usize;
        if part == (0..length) {
            return Ok(decompress_onto(source, buf, length));
        }
        let Some(restored) = decompress(source, &mut self.decoded) else {
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

/// Find where a chunked record's piece sums start: right after its table. Only a record stored as written has them
pub fn sums_start(head: &ChunkedHead) -> u64 {
    head.head_length as u64 + table_bytes(head.chunks).0
}

/// Find where a chunked record's payload starts, past the piece sums of a record stored as written
pub fn payload_start(head: &ChunkedHead) -> u64 {
    let sums = if head.codec == Codec::None { sums_bytes(head.value_length) } else { 0 };
    sums_start(head) + sums
}

/// Check one chunk of a value stored as written: its span, then its bytes against the sum over its pieces' sums, in a buffer
/// that starts at a value byte
pub fn is_chunk_in_span(span: &ChunkSpan, chunk: ChunkIndex, payload_start: u64, buf: &[u8], buf_start: u64) -> bool {
    let start = chunk.0 * CHUNK_BYTES as u64;
    let is_where_expected = span.entry.stored_offset.0 == payload_start + start;
    if !is_where_expected || span.stored_length != span.entry.value_length || start < buf_start {
        return false;
    }

    let (from, to) = (start - buf_start, start - buf_start + span.entry.value_length.0);
    match buf.get(from as usize..to as usize) {
        Some(bytes) => written_chunk_sum(bytes) == span.entry.checksum,
        None => false,
    }
}

/// Work out how a record stored as written splits, from its length alone: a head, a table, the pieces' sums and the value
pub fn written_shape(length: ByteCount) -> Option<WrittenShape> {
    if length.0 as usize <= SMALL_RECORD_BYTES {
        return None;
    }

    // a chunk stores its bytes, its pieces' sums and its table entry, so the estimate is off by a chunk at most
    let estimate = length.0 / (CHUNK_BYTES + PIECES_PER_CHUNK * PIECE_SUM_BYTES + TABLE_ENTRY_BYTES) as u64;
    for chunks in estimate.saturating_sub(1)..=estimate + 2 {
        let chunks = ChunkCount(chunks);
        let sums_at = head_bytes_for(chunks) as u64 + table_bytes(chunks).0;
        let rest = length.0.checked_sub(sums_at)?;
        // the value and its sums take the rest, and the value grows with the rest, so one piece count fits
        let pieces = rest / (PIECE_BYTES + PIECE_SUM_BYTES) as u64;
        for pieces in pieces.saturating_sub(1)..=pieces + 1 {
            let Some(value_length) = rest.checked_sub(pieces * PIECE_SUM_BYTES as u64).map(ByteCount) else {
                continue;
            };
            if piece_count(value_length) == pieces && chunk_count(value_length) == chunks {
                return Some(WrittenShape {
                    sums_at: sums_at as usize,
                    front_length: (sums_at + pieces * PIECE_SUM_BYTES as u64) as usize,
                    value_length,
                });
            }
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
            damaged_sums: 0,
            trusted: None,
            front: None,
            whole_in_first: None,
        }
    }
}

/// Read a record's form where it lies
pub fn form_of(record: &[u8]) -> Option<RecordForm> {
    RecordForm::from_byte(*record.get(FORM_AT)?)
}

/// Check a one-chunk record read in two parts, its head and its payload, with the payload already where its reader wants it
pub fn checks_split(head: &[u8], payload: &[u8], form: RecordForm, identity: &RecordIdentity<'_>) -> bool {
    let sum = checksum(payload);
    let check = place_check(record_check(identity, form, sum), identity.record_key, identity.place);
    // the stored low bytes must match too, so rot in them is found and can't leave a record slow under trust
    read_check(head) == Some(check) && holds_short_sum(head, sum)
}

/// Check a one-chunk record and give its payload where it lies, still in the codec it was stored with
pub fn checked_payload<'record>(record: &'record [u8], form: RecordForm, identity: &RecordIdentity<'_>) -> Option<&'record [u8]> {
    let payload = record.get(SMALL_HEAD_BYTES..)?;
    checks_split(record, payload, form, identity).then_some(payload)
}

/// A buffer sized for a read that writes every byte of it, with no fill first. Until the read finishes whole it clears the buffer
/// when dropped, so a read that fails, or whose future is dropped part way, leaves no byte it didn't write
pub struct UnfilledRead<'buf> {
    buf: &'buf mut Vec<u8>,
    is_filled: bool,
}

impl<'buf> UnfilledRead<'buf> {
    /// Size the buffer for the read
    #[allow(clippy::uninit_vec, reason = "the read that follows writes every byte, and the guard clears the buffer when it doesn't")]
    pub fn new(buf: &'buf mut Vec<u8>, length: usize) -> Self {
        buf.clear();
        buf.reserve(length);
        // SAFETY: a u8 has no invalid value, and no byte is read before the read wrote all of them: until filled() the drop clears
        // the buffer
        unsafe { buf.set_len(length) };
        Self { buf, is_filled: false }
    }

    /// Lend the buffer to the read
    pub fn buf(&mut self) -> &mut Vec<u8> {
        self.buf
    }

    /// Say the read wrote every byte
    pub fn filled(mut self) {
        self.is_filled = true;
    }
}

impl Drop for UnfilledRead<'_> {
    fn drop(&mut self) {
        if !self.is_filled {
            self.buf.clear();
        }
    }
}

/// Give a one-chunk record's payload where it lies, for a record its reader trusts. The identity goes unchecked. A sealed
/// record's payload up to MOST_SUMMED_PAYLOAD is checked against its head's stored sum, so rot in it still reads as a failed
/// record. An open record this run wrote isn't summed, since its bytes never left this process
pub fn trusted_payload(record: &[u8], is_summed: bool) -> Option<&[u8]> {
    let payload = record.get(SMALL_HEAD_BYTES..)?;
    (!is_summed || trusted_sum_holds(record, payload)).then_some(payload)
}

/// Check a one-chunk record and hand its value over, restoring a compressed one through `decoded`
pub fn open_small(
    record: &[u8],
    form: RecordForm,
    identity: &RecordIdentity<'_>,
    decoded: &mut [u8],
    buf: &mut Vec<u8>,
) -> RecordRead {
    let Some(payload) = checked_payload(record, form, identity) else {
        return RecordRead::Failed;
    };
    buf.clear();
    match form.codec {
        Codec::None => buf.extend_from_slice(payload),
        // a buffer with the room `decoded` has takes the value straight from the decode, under the same bound
        Codec::Lz4 if buf.capacity() >= decoded.len() => {
            if decompress_into_room(payload, buf, decoded.len()).is_none() {
                return RecordRead::Failed;
            }
        },
        Codec::Lz4 => {
            let Some(restored) = decompress(payload, decoded) else {
                return RecordRead::Failed;
            };
            buf.extend_from_slice(&decoded[..restored]);
        },
    }
    RecordRead::Valid
}
