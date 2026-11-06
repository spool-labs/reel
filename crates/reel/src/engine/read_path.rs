//! Gets: the write index, then the spot cache, then the footers, newest by place

use std::cmp::Reverse;
use std::io::{Error, ErrorKind};
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::reads::ValueWindow;
use reel_api::store_error::StoreError;
use reel_api::value_list::ValueList;
use reel_api::units::{ByteCount, FileOffset, WritePlace};

use crate::engine::column_info::ColumnInfo;
use crate::engine::cue::JobCounts;
use crate::engine::engine_state::EngineState;
use crate::engine::open_segment::MOST_WINDOW_RECORD_BYTES;
use crate::engine::volume::Volume;
use crate::index::key_hash::{KeyHash, hash_key};
use crate::index::spot_index::SpotLocation;
use crate::index::write_row::WriteRow;
use crate::io::{DeviceReads, InPlace, Io, ReadOn, in_place};
use crate::pool::PoolLease;
use crate::io::device_error::is_device_error;
use crate::record::record_check::RecordIdentity;
use crate::record::record_reader::{RecordLocation, RecordRead, RecordReader};
use crate::row_kind::RowKind;
use crate::segment::column_directory::ColumnDirectory;
use crate::segment::footer_reader::{FooterAnswer, FooterCaches, FooterQuery, FooterReader, WindowRead};
use crate::segment::row_block::FooterRow;
use crate::segment::segment_table::{SegmentPin, WINDOW_BLOCKS};
use crate::segment::segment_tail::SegmentNumber;

/// Tries a read makes before it takes a pin, and tries it makes for the pin
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
    pub counts: JobCounts,
    pub handovers: u64,
}

/// Which listed segments a directory walk asks for a key
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalkAsk<'list> {
    pub first: SegmentNumber,
    pub skipped: &'list [SegmentNumber],
    pub is_finished_only: bool,
}

impl WalkAsk<'_> {
    /// Ask every segment listed
    pub const EVERY: Self = Self {
        first: SegmentNumber(0),
        skipped: &[],
        is_finished_only: false,
    };
}

/// A read's pin on its point, which goes back however the read ends, a dropped read included
pub struct HeldPin<'state, Backend: Io> {
    pub state: &'state EngineState<Backend>,
    pub slot: usize,
    pub counts: JobCounts,
}

impl<Backend: Io> Drop for HeldPin<'_, Backend> {
    fn drop(&mut self) {
        self.state.unpin(self.slot, self.counts);
    }
}

/// Many keys being read at one point
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManyRead {
    pub point: WritePlace,
    pub counts: JobCounts,
    pub handovers: u64,
    pub pin: Option<usize>,
}

/// Where a read of keys stands: its read point, and the handovers it counted when it took that point
#[derive(Clone, Copy, Debug)]
pub struct ReadAt {
    pub point: WritePlace,
    pub handovers: u64,
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

    /// Read the bytes the row's record takes on disk
    pub fn length(&self) -> ByteCount {
        match self {
            Located::Absent => ByteCount(0),
            Located::Open(row) => row.length,
            Located::Sealed(sealed) => sealed.row.length,
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
    /// Lease a record reader. One from the blocking pool could wait behind a read that waits on this runtime thread
    pub fn record_reader_for(&self, device: &impl DeviceReads<Backend::File>) -> PoolLease<'_, RecordReader> {
        if device.holds_across_waits() { self.ring_record_readers.lease() } else { self.record_readers.lease() }
    }

    /// Lease a footer reader, as a record reader
    pub fn footer_reader_for(&self, device: &impl DeviceReads<Backend::File>) -> PoolLease<'_, FooterReader> {
        if device.holds_across_waits() { self.ring_footer_readers.lease() } else { self.footer_readers.lease() }
    }

    /// Fill a buffer with a key's newest value, and say whether it has one
    pub fn get_latest(
        &self,
        column: ColumnId,
        key: &[u8],
        want: RecordWant,
        buf: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        in_place(self.get_latest_on(&InPlace, column, key, want, buf))
    }

    /// Answer a get through a device's read path, starting over when a job crosses it
    pub async fn get_latest_on(
        &self,
        device: &impl DeviceReads<Backend::File>,
        column: ColumnId,
        key: &[u8],
        want: RecordWant,
        buf: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        for _try in 0..MOST_READ_TRIES {
            let latest = self.latest_read(column, key)?;
            if let Some(answer) = self.answer_latest(device, &latest, want, buf).await {
                return answer;
            }
        }
        let (read, handovers, _held) = self.pinned_read(column, key)?;
        self.get_at(device, &read, handovers, want, buf).await
    }

    /// Pin a key's read, once starting over has failed, so no job can cross this try. A free needs its replacement at or
    /// below the newest point, and a replacement at or below this fresh point would be the row found here, so the row found
    /// has its replacement above the pin, and the pin stands inside that record's window. A full table has no pin to give,
    /// so the read gives up and the count shows it
    pub fn pinned_read<'key>(&self, column: ColumnId, key: &'key [u8]) -> Result<(KeyRead<'key>, u64, HeldPin<'_, Backend>), StoreError> {
        let info = self.column(column)?;
        let volume = &self.volumes[info.volume.0 as usize];
        let many = (0..MOST_READ_TRIES).find_map(|_try| self.many_read(volume));
        let Some((many, Some(slot))) = many.map(|many| (many, many.pin)) else {
            return self.refuse_read();
        };
        let held = HeldPin { state: self, slot, counts: many.counts };
        let read = self.key_read(column, key, many.point, true)?;
        Ok((read, many.handovers, held))
    }

    /// Give up on a read and count it
    pub fn refuse_read<Answer>(&self) -> Result<Answer, StoreError> {
        self.cues.note_refused_read();
        Err(moving_segments())
    }

    /// Give a pin back. A job that stood aside for it moved a count, and nothing else would look again
    pub fn unpin(&self, slot: usize, counts: JobCounts) {
        self.cues.release(slot);
        if self.cues.counts() != counts {
            self.ask_compaction();
        }
    }

    /// Check a job left a read's answer standing. A reclaim can hide a row from any read, and a free can only fail a record
    /// check, so a read that succeeded never starts over for one
    pub fn has_outlived(&self, counts: JobCounts, is_bad: bool) -> bool {
        let now = self.cues.counts();
        now.reclaims == counts.reclaims && (!is_bad || now.frees == counts.frees)
    }

    /// Take the newest point for a key. The counts come first. A job counted before them has its replacements under
    /// the point, and a seal's walk only starts past its rows' places, so the point can't be below a row a walk under
    /// this count gave out
    pub fn latest_read<'key>(&self, column: ColumnId, key: &'key [u8]) -> Result<LatestRead<'key>, StoreError> {
        let info = self.column(column)?;
        let counts = self.cues.counts();
        let handovers = self.volumes[info.volume.0 as usize].handover_count();
        let read = self.key_read_now(column, key)?;
        Ok(LatestRead { read, counts, handovers })
    }

    /// Answer a read at its point, or return nothing when a job crossed it and took what the point could see
    pub async fn answer_latest(
        &self,
        device: &impl DeviceReads<Backend::File>,
        latest: &LatestRead<'_>,
        want: RecordWant,
        buf: &mut Vec<u8>,
    ) -> Option<Result<bool, StoreError>> {
        let answer = self.get_at(device, &latest.read, latest.handovers, want, buf).await;
        let is_bad = matches!(answer, Err(StoreError::Corruption { .. }));
        self.has_outlived(latest.counts, is_bad).then_some(answer)
    }

    /// Answer every key at one read point, so a batch from one write_batch is seen whole or not at all
    pub fn get_many_now(&self, column: ColumnId, keys: &[&[u8]], values: &mut ValueList) -> Result<(), StoreError> {
        in_place(self.get_many_on(&InPlace, column, keys, values))
    }

    /// Answer every key at one read point through a device's read path
    pub async fn get_many_on(
        &self,
        device: &impl DeviceReads<Backend::File>,
        column: ColumnId,
        keys: &[&[u8]],
        values: &mut ValueList,
    ) -> Result<(), StoreError> {
        let info = self.column(column)?;
        let volume = &self.volumes[info.volume.0 as usize];
        let mut scratch = values.take_scratch();
        let mut outcome = None;
        for _try in 0..MOST_READ_TRIES {
            let Some(many) = self.many_read(volume) else {
                continue;
            };
            if let Some(answer) = self.answer_many(device, &many, column, keys, values, &mut scratch).await {
                outcome = Some(answer);
                break;
            }
        }
        values.put_scratch(scratch);
        outcome.unwrap_or_else(|| self.refuse_read())
    }

    /// Take a point for many keys and pin it as a cue does, so no job reaches below it. A job that got in first costs
    /// the try, and a full table leaves the read unpinned to start over if one crosses it
    pub fn many_read(&self, volume: &Volume<Backend>) -> Option<ManyRead> {
        let counts = self.cues.counts();
        let handovers = volume.handover_count();
        let point = self.read_point(volume);
        self.pin_many(volume, point, counts, handovers)
    }

    /// Pin a point read after these counts, unless a job moved one first
    pub fn pin_many(&self, volume: &Volume<Backend>, point: WritePlace, counts: JobCounts, handovers: u64) -> Option<ManyRead> {
        let pin = self.cues.take(volume.id.0 as usize, point);
        if pin.is_some_and(|slot| !self.pin_holds(slot, counts)) {
            return None;
        }
        Some(ManyRead { point, counts, handovers, pin })
    }

    /// Answer keys at their point and give the pin back, or return nothing when a job crossed an unpinned read
    pub async fn answer_many(
        &self,
        device: &impl DeviceReads<Backend::File>,
        many: &ManyRead,
        column: ColumnId,
        keys: &[&[u8]],
        values: &mut ValueList,
        scratch: &mut Vec<u8>,
    ) -> Option<Result<(), StoreError>> {
        values.clear();
        let held = many.pin.map(|slot| HeldPin { state: self, slot, counts: many.counts });
        let answer = self.read_keys(device, column, keys, ReadAt { point: many.point, handovers: many.handovers }, values, scratch).await;
        if held.is_none() {
            return self.has_outlived(many.counts, values.has_corrupt()).then_some(answer);
        }
        drop(held);
        Some(answer)
    }

    /// Answer keys in order at a read point, with a bad record as that key's answer
    pub async fn read_keys(
        &self,
        device: &impl DeviceReads<Backend::File>,
        column: ColumnId,
        keys: &[&[u8]],
        at: ReadAt,
        values: &mut ValueList,
        scratch: &mut Vec<u8>,
    ) -> Result<(), StoreError> {
        let ReadAt { point, handovers } = at;
        for key in keys {
            // a value read below the newest must stay out of the spot cache, as a cue's does
            let read = self.key_read(column, key, point, true)?;
            match self.get_at(device, &read, handovers, RecordWant::Whole, scratch).await {
                Ok(true) => values.push_found(scratch),
                Ok(false) => values.push_missing(),
                Err(StoreError::Corruption { .. }) => values.push_corrupt(),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Fill a buffer with a key's newest value at a read point, and say whether it has one
    pub async fn get_at(
        &self,
        device: &impl DeviceReads<Backend::File>,
        read: &KeyRead<'_>,
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
                if self.read_open(device, read, &row, want, buf).await? {
                    return Ok(true);
                }
                continue;
            }

            let newest_below = found.row.map(|row| row.place).max(cover);
            if self.complete_table(read.column).is_none()
                && let Some(answer) = self.try_spot(device, volume, read, newest_below, want, buf).await?
            {
                return Ok(answer);
            }

            let sealed = self.search_sealed(device, volume, read).await?;
            let located = newest_of(found.row, sealed);
            match located {
                Located::Absent => return Ok(false),
                Located::Open(row) => {
                    if row.kind != RowKind::Value || is_hidden(row.place) {
                        return Ok(false);
                    }
                    if self.read_open(device, read, &row, want, buf).await? {
                        return Ok(true);
                    }
                },
                Located::Sealed(row) => {
                    if row.row.kind != RowKind::Value || is_hidden(row.row.place) {
                        return Ok(false);
                    }
                    if self.read_sealed(device, volume, read, &row, want, buf).await? {
                        if self.complete_table(read.column).is_none() {
                            self.fill_spot(volume, read, &row, handovers);
                        }
                        return Ok(true);
                    }
                },
            }
        }
        self.refuse_read()
    }

    /// Refuse a key whose newest row could be in a damaged segment: the row found must be above it
    pub fn check_damage(
        &self,
        volume: &Volume<Backend>,
        read: &KeyRead<'_>,
        cover: Option<WritePlace>,
    ) -> Result<(), StoreError> {
        let Some(highest) = volume.damage.covering(read.column, read.key) else {
            return Ok(());
        };

        let found = self.index.lookup(read.column, read.key, read.read_point, volume.view());
        let sealed = in_place(self.search_sealed(&InPlace, volume, read))?;
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
        let (read, _handovers, _held) = self.pinned_read(column, key)?;
        self.contains_at(&read)
    }

    /// Say whether a key has a value at its point, or return nothing when a reclaim crossed the read
    pub fn contains_latest(&self, latest: &LatestRead) -> Option<Result<bool, StoreError>> {
        let answer = self.contains_at(&latest.read);
        self.has_outlived(latest.counts, false).then_some(answer)
    }

    /// Say whether a key has a value at a read, without reading the value
    pub fn contains_at(&self, read: &KeyRead<'_>) -> Result<bool, StoreError> {
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

        let sealed = in_place(self.search_sealed(&InPlace, volume, read))?;
        let newest = newest_of(found.row, sealed);
        Ok(newest.has_value() && !newest.place().is_some_and(is_hidden))
    }

    /// Check a key and gather what reading it at the newest point takes
    pub fn key_read_now<'key>(&self, column: ColumnId, key: &'key [u8]) -> Result<KeyRead<'key>, StoreError> {
        let info = self.column(column)?;
        self.check_key(column, key)?;
        let volume = &self.volumes[info.volume.0 as usize];
        let covers = &volume.covers;
        let (cover, read_point) = covers.newest_now(column, key, || self.read_point(volume));
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
    pub async fn try_spot(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        read: &KeyRead<'_>,
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
        match self.read_record(ReadOn::new(device, &pin.file), location, spot.place, read, want, buf).await? {
            RecordRead::Valid => Ok(Some(true)),
            RecordRead::Failed => Ok(None),
        }
    }

    /// Answer from a record still in its slot's windows, or return nothing when it has reached the kernel. This takes
    /// no lock and waits for nothing, since a window keeps its bytes until the write call that sends them returns
    pub fn read_held(
        &self,
        read: &KeyRead<'_>,
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
    pub async fn read_open(
        &self,
        device: &impl DeviceReads<Backend::File>,
        read: &KeyRead<'_>,
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
        match self.read_record(ReadOn::new(device, &pin.file), location, row.place, read, want, buf).await? {
            RecordRead::Valid => Ok(true),
            RecordRead::Failed => Err(corruption(read)),
        }
    }

    /// Read a record from a sealed segment, and return false when the segment has left the table
    pub async fn read_sealed(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        read: &KeyRead<'_>,
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
        match self.read_record(ReadOn::new(device, &pin.file), location, sealed.row.place, read, want, buf).await? {
            RecordRead::Valid => Ok(true),
            RecordRead::Failed => Err(corruption(read)),
        }
    }

    /// Read and check one record through a pooled reader
    pub async fn read_record(
        &self,
        on: ReadOn<'_, impl DeviceReads<Backend::File>, Backend::File>,
        location: RecordLocation,
        place: WritePlace,
        read: &KeyRead<'_>,
        want: RecordWant,
        buf: &mut Vec<u8>,
    ) -> Result<RecordRead, StoreError> {
        let ReadOn { device, target: file } = on;
        let identity = RecordIdentity {
            record_key: self.record_key,
            column: read.column,
            key: read.key,
            kind: RowKind::Value,
            place,
        };
        let mut reader = self.record_reader_for(device);
        let outcome = match want {
            RecordWant::Whole => reader.read_value(device, file, location, &identity, read.info.codec, buf).await,
            RecordWant::Window(window) => reader.read_window(device, file, location, &identity, window, buf).await,
        };
        drop(reader);
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
    pub async fn search_footers(&self, device: &impl DeviceReads<Backend::File>, volume: &Volume<Backend>, read: &KeyRead<'_>) -> Result<Option<SealedRow>, StoreError> {
        let Some(directory) = &self.directories[read.column.0 as usize] else {
            return Ok(None);
        };

        let mut reader = self.footer_reader_for(device);
        self.search_directory(device, volume, directory, read, &mut reader).await
    }

    /// Check an older row of a key could still be read: in a sealed segment other than one, or among the open rows
    pub fn has_older_row(
        &self,
        volume: &Volume<Backend>,
        column: ColumnId,
        key: &[u8],
        place: WritePlace,
        skipped: &[SegmentNumber],
    ) -> Result<bool, StoreError> {
        let Some(directory) = &self.directories[column.0 as usize] else {
            return Ok(false);
        };
        let read = self.key_read(column, key, WritePlace(place.0.saturating_sub(1)), false)?;
        let found = self.index.lookup(column, key, read.read_point, volume.view());
        let mut reader = self.footer_readers.lease();
        let sealed = in_place(self.walk_candidates(&InPlace, volume, directory, &read, &mut reader, WalkAsk { skipped, ..WalkAsk::EVERY }))?;
        Ok(newest_of(found.row, sealed).place().is_some())
    }

    /// Find a key's newest row among the segments whose walks have finished, leaving out one segment and range rows
    pub fn newest_walked(
        &self,
        volume: &Volume<Backend>,
        column: ColumnId,
        key: &[u8],
        skipped: &[SegmentNumber],
    ) -> Result<Option<SealedRow>, StoreError> {
        let read = self.key_read(column, key, WritePlace(u64::MAX), false)?;
        match self.complete_table(column) {
            Some(table) => self.newest_walked_complete(volume, &read, table, skipped),
            None => self.newest_walked_footers(volume, &read, skipped),
        }
    }

    /// Find a key's newest row among finished segments through the footers
    pub fn newest_walked_footers(
        &self,
        volume: &Volume<Backend>,
        read: &KeyRead<'_>,
        skipped: &[SegmentNumber],
    ) -> Result<Option<SealedRow>, StoreError> {
        let Some(directory) = &self.directories[read.column.0 as usize] else {
            return Ok(None);
        };
        let mut reader = self.footer_readers.lease();
        in_place(self.walk_candidates(&InPlace, volume, directory, read, &mut reader, WalkAsk { skipped, is_finished_only: true, ..WalkAsk::EVERY }))
    }

    /// Ask a finished segment for a key's newest row that isn't a range delete, and keep it when it's the newest so far.
    /// A stop ends the walk here, so it waits for one read and never for a key's whole set of probes
    pub async fn ask_walked(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        read: &KeyRead<'_>,
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
        if !pin.is_walked {
            return Ok(());
        }
        self.ask_rows(ReadOn::new(device, volume), read, segment, &pin, reader, best).await
    }

    /// Ask one pinned segment for a key's newest row that isn't a range delete, whether or not its walk has finished
    pub async fn ask_rows(
        &self,
        on: ReadOn<'_, impl DeviceReads<Backend::File>, Volume<Backend>>,
        read: &KeyRead<'_>,
        segment: SegmentNumber,
        pin: &SegmentPin<'_, Backend::File>,
        reader: &mut FooterReader,
        best: &mut Option<SealedRow>,
    ) -> Result<(), StoreError> {
        let ReadOn { device, target: volume } = on;
        let mut point = *read;
        while let Some(found) = self.search_segment(device, volume, &point, segment, pin, reader).await? {
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
    pub async fn search_directory(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        directory: &ColumnDirectory,
        read: &KeyRead<'_>,
        reader: &mut FooterReader,
    ) -> Result<Option<SealedRow>, StoreError> {
        self.walk_candidates(device, volume, directory, read, reader, WalkAsk::EVERY).await
    }

    /// Walk a directory's pages, starting over when the list changes between them
    pub async fn walk_candidates(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        directory: &ColumnDirectory,
        read: &KeyRead<'_>,
        reader: &mut FooterReader,
        ask: WalkAsk<'_>,
    ) -> Result<Option<SealedRow>, StoreError> {
        reader.asked = 0;
        let mut best: Option<SealedRow> = None;
        let mut skip = 0;
        let mut generation = None;
        loop {
            let page = directory.candidates(read.key, skip, &mut reader.candidates);

            // a list that changed between pages could hide a segment, and asking twice is harmless
            if generation.is_some_and(|seen| seen != page.generation) {
                skip = 0;
                generation = None;
                continue;
            }
            generation = Some(page.generation);
            // newest first, so a key with a row stops at its previous version and the older segments cost nothing
            reader.candidates.sort_unstable_by_key(|candidate| Reverse(candidate.newest_place));

            for index in 0..reader.candidates.len() {
                let candidate = reader.candidates[index];
                if best.is_some_and(|row| row.row.place >= candidate.newest_place) {
                    break;
                }
                self.ask_candidate(ReadOn::new(device, volume), read, candidate.segment, reader, &mut best, ask).await?;
            }
            skip += reader.candidates.len();

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

    /// Ask one listed segment for a key, if a walk's rule takes it
    pub async fn ask_candidate(
        &self,
        on: ReadOn<'_, impl DeviceReads<Backend::File>, Volume<Backend>>,
        read: &KeyRead<'_>,
        segment: SegmentNumber,
        reader: &mut FooterReader,
        best: &mut Option<SealedRow>,
        ask: WalkAsk<'_>,
    ) -> Result<(), StoreError> {
        let ReadOn { device, target: volume } = on;
        if segment < ask.first || ask.skipped.contains(&segment) {
            return Ok(());
        }
        reader.asked += 1;
        if ask.is_finished_only {
            return self.ask_walked(device, volume, read, segment, reader, best).await;
        }
        self.ask_segment(device, volume, read, segment, reader, best).await
    }

    /// Ask one segment for the key, and keep its row when it's the newest so far
    pub async fn ask_segment(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        read: &KeyRead<'_>,
        segment: SegmentNumber,
        reader: &mut FooterReader,
        best: &mut Option<SealedRow>,
    ) -> Result<(), StoreError> {
        let Some(pin) = volume.table.pin(segment) else {
            return Ok(());
        };
        let row = self.search_segment(device, volume, read, segment, &pin, reader).await?;
        if let Some(found) = row
            && best.is_none_or(|current| current.row.place < found.place)
        {
            *best = Some(SealedRow { segment, row: found, block_offset: reader.found_block });
        }
        Ok(())
    }

    /// Search a pinned segment window by window
    pub async fn search_segment(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        read: &KeyRead<'_>,
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
            let looked_up = reader.lookup_window(device, &pin.file, &window_read, &caches, &query).await;
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

    /// Remember a sealed value's location, unless a seal's walk began since the read did
    pub fn fill_spot(&self, volume: &Volume<Backend>, read: &KeyRead<'_>, sealed: &SealedRow, handovers: u64) {
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
pub fn corruption(read: &KeyRead<'_>) -> StoreError {
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
