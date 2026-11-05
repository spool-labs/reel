//! Gets: the write index, then the spot cache, then the footers, newest by place

use reel_api::column_id::ColumnId;
use reel_api::reads::ValueWindow;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, WritePlace};

use crate::engine::column_info::ColumnInfo;
use crate::engine::engine_state::EngineState;
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
    pub is_cue: bool,
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
        let info = self.column(column)?;
        let volume = &self.volumes[info.volume.0 as usize];

        // the count comes first: a hand-over only starts past its rows' places, so a read point
        // taken after the count can't be below a row a hand-over under this count gave out
        let handovers = volume.handover_count();
        let read = self.key_read(column, key, self.read_point(), false)?;
        self.get_at(&read, handovers, want, buf)
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
        self.check_damage(volume, read)?;

        for _try in 0..MOST_READ_TRIES {
            let found = self.index.lookup(read.column, read.key, read.read_point, volume.view());
            if let Some(row) = found.row
                && !found.should_ask_footers
            {
                if row.kind != RowKind::Value {
                    return Ok(false);
                }
                if self.read_open(read, &row, want, buf)? {
                    return Ok(true);
                }
                continue;
            }

            if let Some(answer) = self.try_spot(volume, read, found.row, want, buf)? {
                return Ok(answer);
            }

            let sealed = self.search_footers(volume, read)?;
            let located = newest_of(found.row, sealed);
            match located {
                Located::Absent => return Ok(false),
                Located::Open(row) => {
                    if row.kind != RowKind::Value {
                        return Ok(false);
                    }
                    if self.read_open(read, &row, want, buf)? {
                        return Ok(true);
                    }
                },
                Located::Sealed(row) => {
                    if row.row.kind != RowKind::Value {
                        return Ok(false);
                    }
                    if self.read_sealed(volume, read, &row, want, buf)? {
                        self.fill_spot(volume, read, &row, handovers);
                        return Ok(true);
                    }
                },
            }
        }
        Err(StoreError::Io {
            action: "reading a key whose segments keep moving",
            source: std::io::Error::other("too many tries"),
        })
    }

    /// Refuse a key whose newest row could be in a damaged segment: the row found must be above it
    pub fn check_damage(&self, volume: &Volume<Backend>, read: &KeyRead) -> Result<(), StoreError> {
        let Some(highest) = volume.damage.covering(read.column, read.key) else {
            return Ok(());
        };

        let found = self.index.lookup(read.column, read.key, read.read_point, volume.view());
        let sealed = self.search_footers(volume, read)?;
        let newest = newest_of(found.row, sealed);
        match newest.place() {
            Some(place) if place > highest => Ok(()),
            Some(_at_or_below) => Err(corruption(read)),
            None => Err(corruption(read)),
        }
    }

    /// Say whether a key has a value at a read point, without reading the value
    pub fn contains_at(&self, column: ColumnId, key: &[u8], read_point: WritePlace) -> Result<bool, StoreError> {
        let read = self.key_read(column, key, read_point, false)?;
        let volume = &self.volumes[read.info.volume.0 as usize];
        self.check_damage(volume, &read)?;
        let found = self.index.lookup(column, key, read_point, volume.view());
        if let Some(row) = found.row
            && !found.should_ask_footers
        {
            return Ok(row.kind == RowKind::Value);
        }

        let sealed = self.search_footers(volume, &read)?;
        Ok(newest_of(found.row, sealed).has_value())
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
            is_cue,
        })
    }

    /// Answer from the spot cache, when it has a row newer than the index's and its record checks out
    pub fn try_spot(
        &self,
        volume: &Volume<Backend>,
        read: &KeyRead,
        open_row: Option<WriteRow>,
        want: RecordWant,
        buf: &mut Vec<u8>,
    ) -> Result<Option<bool>, StoreError> {
        let Some(spot) = self.spot.lookup(read.hash) else {
            return Ok(None);
        };
        let is_newest = spot.place <= read.read_point && open_row.is_none_or(|row| row.place < spot.place);
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

    /// Walk a directory's pages, starting over when the list changes between them
    pub fn search_directory(
        &self,
        volume: &Volume<Backend>,
        directory: &ColumnDirectory,
        read: &KeyRead,
        reader: &mut FooterReader,
    ) -> Result<Option<SealedRow>, StoreError> {
        let mut best: Option<SealedRow> = None;
        let mut skip = 0;
        let mut generation = None;
        loop {
            let mut candidates = std::mem::take(&mut reader.candidates);
            let page = directory.candidates(read.key, skip, &mut candidates);

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
                outcome = self.ask_segment(volume, read, *segment, reader, &mut best);
                if outcome.is_err() {
                    break;
                }
            }
            skip += candidates.len();
            reader.candidates = candidates;
            outcome?;

            if !page.is_cut {
                return Ok(best);
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
            *best = Some(SealedRow { segment, row: found });
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
