//! Gets: the write index, then the spot cache, then the footers, newest by place

use std::io::{Error, ErrorKind};
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::reads::ValueWindow;
use reel_api::store_error::StoreError;
use reel_api::value_list::ValueList;
use reel_api::units::{ByteCount, FileOffset, WritePlace};

use crate::engine::column_info::ColumnInfo;
use crate::engine::engine_state::EngineState;
use crate::engine::open_segment::MOST_WINDOW_RECORD_BYTES;
use crate::engine::volume::Volume;
use crate::index::key_hash::{KeyHash, hash_key};
use crate::index::spot_index::SpotLocation;
use crate::index::write_row::WriteRow;
use crate::io::Io;
use crate::io::device_error::is_device_error;
use crate::record::record_check::RecordIdentity;
use crate::record::record_reader::{RecordLocation, RecordRead};
use crate::row_kind::RowKind;
use crate::segment::column_directory::ColumnDirectory;
use crate::segment::footer_reader::{FooterAnswer, FooterCaches, FooterQuery, FooterReader, WindowRead};
use crate::segment::row_block::FooterRow;
use crate::segment::segment_table::{SegmentPin, WINDOW_BLOCKS};
use crate::segment::segment_tail::SegmentNumber;

/// Tries a get makes before it reports the store as changing too fast to read
pub const MOST_READ_TRIES: u32 = 64;

/// A row in a sealed segment
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SealedRow {
    pub segment: SegmentNumber,
    pub row: FooterRow,
    pub block_offset: u32,
}

/// Where a key's newest row sits
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Located {
    /// The key has no row
    Absent,
    /// The row is in an open segment
    Open(WriteRow),
    /// The row is in a sealed segment
    Sealed(SealedRow),
}

/// What a read does with a record once it has found it
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordWant {
    /// All of the value
    Whole,
    /// A window of the value
    Window(ValueWindow),
}

/// One key being read
#[derive(Clone, Copy, Debug)]
pub struct KeyRead<'key> {
    pub column: ColumnId,
    pub info: ColumnInfo,
    pub key: &'key [u8],
    pub hash: KeyHash,
    pub read_point: WritePlace,
    pub cover: Option<WritePlace>,
    pub is_cue: bool,
}

/// A key being read at the newest point, with the counts its answer has to outlive
#[derive(Clone, Copy, Debug)]
pub struct LatestRead<'key> {
    pub read: KeyRead<'key>,
    pub reclaims: u64,
    pub handovers: u64,
}

/// Many keys being read at one point
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManyRead {
    pub point: WritePlace,
    pub reclaims: u64,
    pub handovers: u64,
    pub pin: Option<usize>,
}

impl Located {
    /// Read the row's place, if there is a row
    pub fn place(&self) -> Option<WritePlace> {
        match self {
            Located::Absent => None,
            Located::Open(row) => Some(row.place),
            Located::Sealed(sealed) => Some(sealed.row.place),
        }
    }

    /// Check whether the key's newest row says it has a value
    pub fn has_value(&self) -> bool {
        match self {
            Located::Absent => false,
            Located::Open(row) => row.kind == RowKind::Value,
            Located::Sealed(sealed) => sealed.row.kind == RowKind::Value,
        }
    }
}

impl<Backend: Io> EngineState<Backend> {
    /// Fill a buffer with a key's newest value, and say whether it has one
    pub fn get_latest(
        &self,
        column: ColumnId,
        key: &[u8],
        want: RecordWant,
        buf: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        for _try in 0..MOST_READ_TRIES {
            let latest = self.latest_read(column, key)?;
            if let Some(answer) = self.answer_latest(&latest, want, buf) {
                return answer;
            }
        }
        Err(moving_segments())
    }

    /// Take the newest point for a key. Both counts come first. A reclaim counted before them has its replacements under
    /// the point, and a hand-over only starts past its rows' places, so the point can't be below a row a hand-over under
    /// this count gave out
    pub fn latest_read<'key>(&self, column: ColumnId, key: &'key [u8]) -> Result<LatestRead<'key>, StoreError> {
        let info = self.column(column)?;
        let reclaims = self.cues.reclaims();
        let handovers = self.volumes[info.volume.0 as usize].handover_count();
        let read = self.key_read_now(column, key)?;
        Ok(LatestRead { read, reclaims, handovers })
    }

    /// Answer a read at its point, or return nothing when a reclaim crossed it and took what the point could see
    pub fn answer_latest(
        &self,
        latest: &LatestRead,
        want: RecordWant,
        buf: &mut Vec<u8>,
    ) -> Option<Result<bool, StoreError>> {
        let answer = self.get_at(&latest.read, latest.handovers, want, buf);
        (self.cues.reclaims() == latest.reclaims).then_some(answer)
    }

    /// Answer every key at one read point, so a batch from one write_batch is seen whole or not at all
    pub fn get_many_now(&self, column: ColumnId, keys: &[&[u8]], values: &mut ValueList) -> Result<(), StoreError> {
        let info = self.column(column)?;
        let volume = &self.volumes[info.volume.0 as usize];
        let mut scratch = values.take_scratch();
        let mut outcome = Err(moving_segments());
        for _try in 0..MOST_READ_TRIES {
            let Some(many) = self.many_read(volume) else {
                continue;
            };
            if let Some(answer) = self.answer_many(&many, column, keys, values, &mut scratch) {
                outcome = answer;
                break;
            }
        }
        values.put_scratch(scratch);
        outcome
    }

    /// Take a point for many keys and pin it as a cue does, so no reclaim reaches below it. A reclaim that got in first costs
    /// the try, and a full table leaves the read unpinned to start over if one crosses it
    pub fn many_read(&self, volume: &Volume<Backend>) -> Option<ManyRead> {
        let reclaims = self.cues.reclaims();
        let handovers = volume.handover_count();
        let point = self.read_point();
        self.pin_many(point, reclaims, handovers)
    }

    /// Pin a point read after these counts, unless a reclaim moved the count first
    pub fn pin_many(&self, point: WritePlace, reclaims: u64, handovers: u64) -> Option<ManyRead> {
        let pin = self.cues.take(point);
        if pin.is_some_and(|slot| !self.pin_holds(slot, reclaims)) {
            return None;
        }
        Some(ManyRead { point, reclaims, handovers, pin })
    }

    /// Answer keys at their point and give the pin back, or return nothing when a reclaim crossed an unpinned read
    pub fn answer_many(
        &self,
        many: &ManyRead,
        column: ColumnId,
        keys: &[&[u8]],
        values: &mut ValueList,
        scratch: &mut Vec<u8>,
    ) -> Option<Result<(), StoreError>> {
        values.clear();
        let answer = self.read_keys(column, keys, many.point, many.handovers, values, scratch);
        if let Some(slot) = many.pin {
            self.cues.release(slot);
            // a reclaim that stood aside for this pin moved the count, and nothing else would look again
            if self.cues.reclaims() != many.reclaims {
                self.ask_compaction();
            }
        }
        (many.pin.is_some() || self.cues.reclaims() == many.reclaims).then_some(answer)
    }

    /// Answer keys in order at a read point, with a bad record as that key's answer
    pub fn read_keys(
        &self,
        column: ColumnId,
        keys: &[&[u8]],
        point: WritePlace,
        handovers: u64,
        values: &mut ValueList,
        scratch: &mut Vec<u8>,
    ) -> Result<(), StoreError> {
        for key in keys {
            // a value read below the newest must stay out of the spot cache, as a cue's does
            let read = self.key_read(column, key, point, true)?;
            match self.get_at(&read, handovers, RecordWant::Whole, scratch) {
                Ok(true) => values.push_found(scratch),
                Ok(false) => values.push_missing(),
                Err(StoreError::Corruption { .. }) => values.push_corrupt(),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Fill a buffer with a key's newest value at a read point, and say whether it has one
    pub fn get_at(
        &self,
        read: &KeyRead,
        handovers: u64,
        want: RecordWant,
        buf: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        let volume = &self.volumes[read.info.volume.0 as usize];
        let cover = read.cover;
        self.check_damage(volume, read, cover)?;
        let is_hidden = |place: WritePlace| cover.is_some_and(|cover| cover > place);

        for _try in 0..MOST_READ_TRIES {
            let found = self.index.lookup(read.column, read.key, read.read_point, volume.view());
            if let Some(row) = found.row
                && !found.should_ask_footers
            {
                if row.kind != RowKind::Value || is_hidden(row.place) {
                    return Ok(false);
                }
                if self.read_open(read, &row, want, buf)? {
                    return Ok(true);
                }
                continue;
            }

            let newest_below = found.row.map(|row| row.place).max(cover);
            if let Some(answer) = self.try_spot(volume, read, newest_below, want, buf)? {
                return Ok(answer);
            }

            let sealed = self.search_footers(volume, read)?;
            let located = newest_of(found.row, sealed);
            match located {
                Located::Absent => return Ok(false),
                Located::Open(row) => {
                    if row.kind != RowKind::Value || is_hidden(row.place) {
                        return Ok(false);
                    }
                    if self.read_open(read, &row, want, buf)? {
                        return Ok(true);
                    }
                },
                Located::Sealed(row) => {
                    if row.row.kind != RowKind::Value || is_hidden(row.row.place) {
                        return Ok(false);
                    }
                    if self.read_sealed(volume, read, &row, want, buf)? {
                        self.fill_spot(volume, read, &row, handovers);
                        return Ok(true);
                    }
                },
            }
        }
        Err(moving_segments())
    }

    /// Refuse a key whose newest row could be in a damaged segment: the row found must be above it
    pub fn check_damage(
        &self,
        volume: &Volume<Backend>,
        read: &KeyRead,
        cover: Option<WritePlace>,
    ) -> Result<(), StoreError> {
        let Some(highest) = volume.damage.covering(read.column, read.key) else {
            return Ok(());
        };

        let found = self.index.lookup(read.column, read.key, read.read_point, volume.view());
        let sealed = self.search_footers(volume, read)?;
        let newest = newest_of(found.row, sealed);
        match newest.place().max(cover) {
            Some(place) if place > highest => Ok(()),
            Some(_at_or_below) => Err(corruption(read)),
            None => Err(corruption(read)),
        }
    }

    /// Say whether a key has a value at the newest point, without reading the value
    pub fn contains_now(&self, column: ColumnId, key: &[u8]) -> Result<bool, StoreError> {
        for _try in 0..MOST_READ_TRIES {
            let latest = self.latest_read(column, key)?;
            if let Some(answer) = self.contains_latest(&latest) {
                return answer;
            }
        }
        Err(moving_segments())
    }

    /// Say whether a key has a value at its point, or return nothing when a reclaim crossed the read
    pub fn contains_latest(&self, latest: &LatestRead) -> Option<Result<bool, StoreError>> {
        let answer = self.contains_at(&latest.read);
        (self.cues.reclaims() == latest.reclaims).then_some(answer)
    }

    /// Say whether a key has a value at a read, without reading the value
    pub fn contains_at(&self, read: &KeyRead) -> Result<bool, StoreError> {
        let (column, key, read_point) = (read.column, read.key, read.read_point);
        let volume = &self.volumes[read.info.volume.0 as usize];
        let cover = read.cover;
        self.check_damage(volume, read, cover)?;
        let is_hidden = |place: WritePlace| cover.is_some_and(|cover| cover > place);
        let found = self.index.lookup(column, key, read_point, volume.view());
        if let Some(row) = found.row
            && !found.should_ask_footers
        {
            return Ok(row.kind == RowKind::Value && !is_hidden(row.place));
        }

        let sealed = self.search_footers(volume, read)?;
        let newest = newest_of(found.row, sealed);
        Ok(newest.has_value() && !newest.place().is_some_and(is_hidden))
    }

    /// Check a key and gather what reading it at the newest point takes
    pub fn key_read_now<'key>(&self, column: ColumnId, key: &'key [u8]) -> Result<KeyRead<'key>, StoreError> {
        let info = self.column(column)?;
        self.check_key(column, key)?;
        let covers = &self.volumes[info.volume.0 as usize].covers;
        let (cover, read_point) = covers.newest_now(column, key, || self.read_point());
        Ok(KeyRead {
            column,
            info,
            key,
            hash: hash_key(column, key),
            read_point,
            cover,
            is_cue: false,
        })
    }

    /// Check a key and gather what reading it takes
    pub fn key_read<'key>(
        &self,
        column: ColumnId,
        key: &'key [u8],
        read_point: WritePlace,
        is_cue: bool,
    ) -> Result<KeyRead<'key>, StoreError> {
        let info = self.column(column)?;
        self.check_key(column, key)?;
        Ok(KeyRead {
            column,
            info,
            key,
            hash: hash_key(column, key),
            read_point,
            cover: self.volumes[info.volume.0 as usize].covers.newest_over(column, key, read_point),
            is_cue,
        })
    }

    /// Answer from the spot cache, when it has a row newer than the index's and its record checks out
    pub fn try_spot(
        &self,
        volume: &Volume<Backend>,
        read: &KeyRead,
        newest_below: Option<WritePlace>,
        want: RecordWant,
        buf: &mut Vec<u8>,
    ) -> Result<Option<bool>, StoreError> {
        let Some(spot) = self.spot.lookup(read.hash) else {
            return Ok(None);
        };
        let is_newest = spot.place <= read.read_point && newest_below.is_none_or(|place| place < spot.place);
        if !is_newest {
            return Ok(None);
        }

        let Some(pin) = volume.table.pin(spot.segment) else {
            return Ok(None);
        };
        let location = RecordLocation {
            offset: spot.offset,
            length: spot.length,
        };
        // a slot holds no key, so a failed check here means another key or a moved record: a miss
        match self.read_record(&pin.file, location, spot.place, read, want, buf)? {
            RecordRead::Valid => Ok(Some(true)),
            RecordRead::Failed => Ok(None),
        }
    }

    /// Answer from a record still in its slot's windows, or return nothing when it has reached the kernel. This takes
    /// no lock and waits for nothing, since a window keeps its bytes until the write call that sends them returns
    pub fn read_held(
        &self,
        read: &KeyRead,
        row: &WriteRow,
        want: RecordWant,
        buf: &mut Vec<u8>,
    ) -> Result<Option<RecordRead>, StoreError> {
        let length = row.length.0 as usize;
        if length > MOST_WINDOW_RECORD_BYTES {
            return Ok(None);
        }

        let mut held = [0; MOST_WINDOW_RECORD_BYTES];
        let windows = &self.windows[row.slot.0 as usize];
        if !windows.iter().any(|window| window.read(row.offset, &mut held[..length])) {
            return Ok(None);
        }

        let identity = RecordIdentity {
            record_key: self.record_key,
            column: read.column,
            key: read.key,
            kind: RowKind::Value,
            place: row.place,
        };
        let window = match want {
            RecordWant::Whole => None,
            RecordWant::Window(window) => Some(window),
        };
        let outcome = self.record_readers.with(|reader| reader.read_held(&held[..length], &identity, window, buf));
        Ok(Some(outcome))
    }

    /// Read a record from an open segment, and return false when the slot has moved on
    pub fn read_open(
        &self,
        read: &KeyRead,
        row: &WriteRow,
        want: RecordWant,
        buf: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        let Some(pin) = self.slot_files[row.slot.0 as usize].pin(row.segment) else {
            return Ok(false);
        };
        // the slot is pinned, so its queue still speaks for this segment
        match self.read_held(read, row, want, buf)? {
            Some(RecordRead::Valid) => return Ok(true),
            Some(RecordRead::Failed) => return Err(corruption(read)),
            None => {},
        };
        let location = RecordLocation {
            offset: row.offset,
            length: row.length,
        };
        match self.read_record(&pin.file, location, row.place, read, want, buf)? {
            RecordRead::Valid => Ok(true),
            RecordRead::Failed => Err(corruption(read)),
        }
    }

    /// Read a record from a sealed segment, and return false when the segment has left the table
    pub fn read_sealed(
        &self,
        volume: &Volume<Backend>,
        read: &KeyRead,
        sealed: &SealedRow,
        want: RecordWant,
        buf: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        let Some(pin) = volume.table.pin(sealed.segment) else {
            return Ok(false);
        };
        let location = RecordLocation {
            offset: sealed.row.offset,
            length: sealed.row.length,
        };
        match self.read_record(&pin.file, location, sealed.row.place, read, want, buf)? {
            RecordRead::Valid => Ok(true),
            RecordRead::Failed => Err(corruption(read)),
        }
    }

    /// Read and check one record through a pooled reader
    pub fn read_record(
        &self,
        file: &Backend::File,
        location: RecordLocation,
        place: WritePlace,
        read: &KeyRead,
        want: RecordWant,
        buf: &mut Vec<u8>,
    ) -> Result<RecordRead, StoreError> {
        let identity = RecordIdentity {
            record_key: self.record_key,
            column: read.column,
            key: read.key,
            kind: RowKind::Value,
            place,
        };
        let outcome = self.record_readers.with(|reader| match want {
            RecordWant::Whole => reader.read_value(file, location, &identity, read.info.codec, buf),
            RecordWant::Window(window) => reader.read_window(file, location, &identity, window, buf),
        });
        // a bad sector comes back as a read error, and it means what a failed check means
        match outcome {
            Ok(read) => Ok(read),
            Err(source) if is_device_error(&source) => Ok(RecordRead::Failed),
            Err(source) => Err(StoreError::Io {
                action: "reading a record",
                source,
            }),
        }
    }

    /// Find the key's newest row in the sealed segments, asking every one its range can't rule out
    pub fn search_footers(&self, volume: &Volume<Backend>, read: &KeyRead) -> Result<Option<SealedRow>, StoreError> {
        let Some(directory) = &self.directories[read.column.0 as usize] else {
            return Ok(None);
        };

        self.footer_readers.with(|reader| self.search_directory(volume, directory, read, reader))
    }

    /// Check an older row of a key could still be read: in a sealed segment other than one, or among the open rows
    pub fn has_older_row(
        &self,
        volume: &Volume<Backend>,
        column: ColumnId,
        key: &[u8],
        place: WritePlace,
        skipped: SegmentNumber,
    ) -> Result<bool, StoreError> {
        let Some(directory) = &self.directories[column.0 as usize] else {
            return Ok(false);
        };
        let read = self.key_read(column, key, WritePlace(place.0.saturating_sub(1)), false)?;
        let found = self.index.lookup(column, key, read.read_point, volume.view());
        let sealed = self.footer_readers.with(|reader| {
            self.walk_candidates(directory, key, reader, |segment, reader, best| {
                if segment == skipped {
                    return Ok(());
                }
                self.ask_segment(volume, &read, segment, reader, best)
            })
        })?;
        Ok(newest_of(found.row, sealed).place().is_some())
    }

    /// Find a key's newest row among the segments whose walks have finished, leaving out one segment and range rows
    pub fn newest_walked(
        &self,
        volume: &Volume<Backend>,
        column: ColumnId,
        key: &[u8],
        skipped: SegmentNumber,
    ) -> Result<Option<SealedRow>, StoreError> {
        let Some(directory) = &self.directories[column.0 as usize] else {
            return Ok(None);
        };
        let read = self.key_read(column, key, WritePlace(u64::MAX), false)?;
        self.footer_readers.with(|reader| {
            self.walk_candidates(directory, key, reader, |segment, reader, best| {
                if segment == skipped {
                    return Ok(());
                }
                self.ask_walked(volume, &read, segment, reader, best)
            })
        })
    }

    /// Ask a finished segment for a key's newest row that isn't a range delete, and keep it when it's the newest so far.
    /// A stop ends the walk here, so it waits for one read and never for a key's whole set of probes
    pub fn ask_walked(
        &self,
        volume: &Volume<Backend>,
        read: &KeyRead,
        segment: SegmentNumber,
        reader: &mut FooterReader,
        best: &mut Option<SealedRow>,
    ) -> Result<(), StoreError> {
        if self.is_stopping.load(Ordering::Acquire) {
            return Err(StoreError::Io {
                action: "walking",
                source: Error::from(ErrorKind::Interrupted),
            });
        }
        let Some(pin) = volume.table.pin(segment) else {
            return Ok(());
        };
        if !pin.is_walked || best.is_some_and(|row| row.row.place >= pin.head.places.highest) {
            return Ok(());
        }
        let mut point = *read;
        while let Some(found) = self.search_segment(volume, &point, segment, &pin, reader)? {
            if found.kind != RowKind::RangeDelete {
                if best.is_none_or(|current| current.row.place < found.place) {
                    *best = Some(SealedRow { segment, row: found, block_offset: reader.found_block });
                }
                return Ok(());
            }
            let Some(below) = found.place.0.checked_sub(1) else {
                return Ok(());
            };
            point.read_point = WritePlace(below);
        }
        Ok(())
    }

    /// Ask every segment a directory lists for a key
    pub fn search_directory(
        &self,
        volume: &Volume<Backend>,
        directory: &ColumnDirectory,
        read: &KeyRead,
        reader: &mut FooterReader,
    ) -> Result<Option<SealedRow>, StoreError> {
        self.walk_candidates(directory, read.key, reader, |segment, reader, best| {
            self.ask_segment(volume, read, segment, reader, best)
        })
    }

    /// Walk a directory's pages, starting over when the list changes between them
    pub fn walk_candidates(
        &self,
        directory: &ColumnDirectory,
        key: &[u8],
        reader: &mut FooterReader,
        mut ask: impl FnMut(SegmentNumber, &mut FooterReader, &mut Option<SealedRow>) -> Result<(), StoreError>,
    ) -> Result<Option<SealedRow>, StoreError> {
        let mut best: Option<SealedRow> = None;
        let mut skip = 0;
        let mut generation = None;
        loop {
            let mut candidates = std::mem::take(&mut reader.candidates);
            let page = directory.candidates(key, skip, &mut candidates);

            // a list that changed between pages could hide a segment, and asking twice is harmless
            if generation.is_some_and(|seen| seen != page.generation) {
                skip = 0;
                generation = None;
                reader.candidates = candidates;
                continue;
            }
            generation = Some(page.generation);

            let mut outcome = Ok(());
            for segment in &candidates {
                outcome = ask(*segment, reader, &mut best);
                if outcome.is_err() {
                    break;
                }
            }
            skip += candidates.len();
            reader.candidates = candidates;
            outcome?;

            // a segment that left under this walk took rows with it, and the list that took it out has their new home
            if !page.is_cut && directory.generation() == page.generation {
                return Ok(best);
            }
            if !page.is_cut {
                skip = 0;
                generation = None;
            }
        }
    }

    /// Ask one segment for the key, and keep its row when it's the newest so far
    pub fn ask_segment(
        &self,
        volume: &Volume<Backend>,
        read: &KeyRead,
        segment: SegmentNumber,
        reader: &mut FooterReader,
        best: &mut Option<SealedRow>,
    ) -> Result<(), StoreError> {
        let Some(pin) = volume.table.pin(segment) else {
            return Ok(());
        };
        if best.is_some_and(|row| row.row.place >= pin.head.places.highest) {
            return Ok(());
        }
        let row = self.search_segment(volume, read, segment, &pin, reader)?;
        if let Some(found) = row
            && best.is_none_or(|current| current.row.place < found.place)
        {
            *best = Some(SealedRow { segment, row: found, block_offset: reader.found_block });
        }
        Ok(())
    }

    /// Search a pinned segment window by window
    pub fn search_segment(
        &self,
        volume: &Volume<Backend>,
        read: &KeyRead,
        segment: SegmentNumber,
        pin: &SegmentPin<'_, Backend::File>,
        reader: &mut FooterReader,
    ) -> Result<Option<FooterRow>, StoreError> {
        let caches = FooterCaches {
            filters: &self.filter_cache,
            rows: &self.row_cache,
        };
        let query = FooterQuery {
            hash: read.hash,
            key: read.key,
            read_point: read.read_point,
        };

        let mut skip = 0;
        loop {
            let Some(window) = volume.table.block_window(segment, read.column, read.key, skip) else {
                return Ok(None);
            };
            let window_read = WindowRead {
                segment,
                window: &window,
                place_base: pin.head.place_base,
            };
            let looked_up = reader.lookup_window(&pin.file, &window_read, &caches, &query);
            let answer = match looked_up {
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
                FooterAnswer::Found(row) => return Ok(Some(row)),
                FooterAnswer::Damaged => return Err(corruption(read)),
                FooterAnswer::Absent => {},
            }
            if !window.has_more {
                return Ok(None);
            }
            skip += WINDOW_BLOCKS;
        }
    }

    /// Remember a sealed value's location, unless a hand-over began since the read did
    pub fn fill_spot(&self, volume: &Volume<Backend>, read: &KeyRead, sealed: &SealedRow, handovers: u64) {
        if read.is_cue {
            return;
        }
        let location = SpotLocation {
            segment: sealed.segment,
            offset: FileOffset(sealed.row.offset.0),
            length: ByteCount(sealed.row.length.0),
            place: sealed.row.place,
        };
        self.spot
            .insert_from_read(read.hash, &location, || volume.handover_count() == handovers);
    }

}

/// Pick the newer of the index's row and the footers' row
pub fn newest_of(open: Option<WriteRow>, sealed: Option<SealedRow>) -> Located {
    match (open, sealed) {
        (None, None) => Located::Absent,
        (Some(row), None) => Located::Open(row),
        (None, Some(row)) => Located::Sealed(row),
        (Some(open_row), Some(sealed_row)) => {
            if open_row.place >= sealed_row.row.place {
                Located::Open(open_row)
            } else {
                Located::Sealed(sealed_row)
            }
        },
    }
}

/// Build the error for a key's damaged record
pub fn corruption(read: &KeyRead) -> StoreError {
    StoreError::Corruption {
        column: read.column,
    }
}

/// Report a read that kept losing its segments to seals and reclaims
pub fn moving_segments() -> StoreError {
    StoreError::Io {
        action: "reading a key whose segments keep moving",
        source: Error::from(ErrorKind::Interrupted),
    }
}
