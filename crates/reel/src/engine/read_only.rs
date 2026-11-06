//! A read-only open beside a running writer: no lock, no write, no job but the readers, and a fresh look when asked
//!
//! A segment is live exactly when its tail passes, and an open file keeps it readable through any unlink. A merge's output
//! and its inputs can both show up, and a key's rows then stand at the same places, so a read takes either.

use std::io::{Error, ErrorKind, Result as IoResult};
use std::sync::Arc;

use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, RowCount, WritePlace};

use crate::config::StoreConfig;
use crate::engine::engine_sizing::GROUP_BYTES;
use crate::engine::engine_state::{EngineState, SlotData};
use crate::engine::open_segment::{OpenSegment, SegmentPhase};
use crate::engine::read_pool::ReadWorker;
use crate::engine::recovery::{
    OpenMode, RecoveredRows, ReplayTarget, RootScan, RowsWalked, ScannedOpen, list_roots, load_covers, next_group, read_summaries, row_ranges,
    scan_roots,
    pass_open_rows, walk_rows,
};
use crate::engine::reel_store::{build_state, open_error, read_superblock, store_sizing};
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::io::Io;
use crate::lifetime::MarkRange;
use crate::record::record_reader::RecordReader;

/// Times an open lists its roots again after a file it listed went away
pub const MOST_LIST_TRIES: u32 = 8;

/// Build a view of the store as it stands, listing again when the writer takes a file away first
pub fn load_read_only<Backend: Io>(io: &Backend, config: &StoreConfig) -> Result<Arc<EngineState<Backend>>, StoreError> {
    for _try in 0..MOST_LIST_TRIES {
        match try_load(io, config) {
            Err(StoreError::Io { source, .. }) if source.kind() == ErrorKind::NotFound => {},
            outcome => return outcome,
        }
    }
    Err(StoreError::Io {
        action: "listing roots under a running writer",
        source: Error::from(ErrorKind::NotFound),
    })
}

/// Build a view from one listing
pub fn try_load<Backend: Io>(io: &Backend, config: &StoreConfig) -> Result<Arc<EngineState<Backend>>, StoreError> {
    let superblock = read_superblock(io, config)?;
    let sizing = store_sizing(config)?;
    let roots: Vec<_> = config.volumes.iter().enumerate().map(|(index, volume)| (index, volume.roots.clone())).collect();
    let listings = list_roots(io, &roots).map_err(open_error("listing a root"))?;
    let mode = OpenMode::ReadOnly;
    let mut scan = scan_roots(io, superblock.record_key, &listings, mode).map_err(open_error("reading a tail"))?;
    let summaries = match read_summaries(io, &roots) {
        Ok(summaries) => summaries,
        Err(error) => {
            scan.close_files(io);
            return Err(open_error("reading a summary")(error));
        },
    };
    let locks = config.volumes.iter().map(|_volume| Vec::new()).collect();
    let mut state = build_state(io.clone(), config, &sizing, superblock, locks, &scan, summaries);
    state.is_read_only = true;
    let state = Arc::new(state);
    let outcome = bring_up_read_only(&state, &mut scan);
    match outcome {
        Ok(()) => Ok(state),
        Err(error) => {
            scan.close_files(&state.io);
            close_read_only(&state);
            Err(error)
        },
    }
}

/// Take in what the listing found, and start the readers
pub fn bring_up_read_only<Backend: Io>(
    state: &Arc<EngineState<Backend>>,
    scan: &mut RootScan<Backend::File>,
) -> Result<(), StoreError> {
    for found in &scan.sealed {
        state.order.resume_past(found.head.places.highest);
    }
    pass_open_rows(state, scan);
    load_covers(state, scan).map_err(open_error("reading pending range deletes"))?;
    load_damage(state);

    scan.sealed.reverse();
    while let Some(found) = scan.sealed.last() {
        let volume = &state.volumes[found.volume];
        state.install_sealed(volume, found).map_err(open_error("installing a segment"))?;
        scan.sealed.pop();
    }
    while let Some(scanned) = scan.scanned.pop() {
        let volume = &state.volumes[scanned.found.volume];
        if let Err(error) = state.view_open(volume, &scanned) {
            state.io.close_file(scanned.found.data);
            state.io.close_file(scanned.rows);
            return Err(open_error("reading a rows file")(error));
        }
    }

    for cell in &state.read_tasks {
        let worker = ReadWorker {
            state: Arc::clone(state),
        };
        let task = state.io.spawn_job("reel-read", worker).map_err(open_error("starting a job"))?;
        let _set = cell.set(task);
    }
    Ok(())
}

/// Take the damaged spans the summaries hold
pub fn load_damage<Backend: Io>(state: &EngineState<Backend>) {
    let mut columns = Vec::new();
    for volume in &state.volumes {
        for row in hold(&volume.summary).rows().filter(|row| row.facts.is_damaged) {
            for range in row_ranges(&row, row.facts.segment, &mut columns) {
                volume.damage.add(&range);
            }
        }
    }
}

/// Stop the readers and let go of every file. The disk stays as it was
pub fn close_read_only<Backend: Io>(state: &EngineState<Backend>) {
    let _stopped = state.stop_jobs();
    for volume in &state.volumes {
        state.close_volume(volume);
    }
}

impl<Backend: Io> EngineState<Backend> {
    /// Put an open segment's rows in the index group by group, up to a torn group or one that lists a record not there yet
    pub fn view_open(&self, volume: &Volume<Backend>, scanned: &ScannedOpen<Backend::File>) -> IoResult<()> {
        let Some(slot) = self.take_slot() else {
            return Err(Error::other("more open segments than slots"));
        };
        let found = &scanned.found;
        let mut recovered = RecoveredRows {
            write_end: FileOffset(0),
            row_count: RowCount(0),
            lowest_place: WritePlace(u64::MAX),
            highest_place: WritePlace(0),
            lost_region: None,
            cover_rows: 0,
            marks: MarkRange::NONE,
        };
        let target = ReplayTarget {
            volume,
            data: &found.data,
            slot,
            segment: found.number,
        };
        self.view_rows(scanned, &target, &mut recovered)?;
        self.slot_files[slot.0 as usize].set(Some(SlotData {
            segment: found.number,
            file: found.data.clone(),
        }));
        volume.open_slot(slot);
        hold(&volume.claims).open.push(OpenSegment {
            number: found.number,
            slot,
            root: found.root,
            data: found.data.clone(),
            rows: scanned.rows.clone(),
            phase: SegmentPhase::Taking,
            write_end: recovered.write_end,
            row_count: recovered.row_count,
            cover_rows: recovered.cover_rows,
            in_flight: 0,
            lowest_place: recovered.lowest_place,
            highest_place: recovered.highest_place,
            ceiling: recovered.marks.highest(),
            marks: recovered.marks,
        });
        Ok(())
    }

    /// Walk the rows a listing found. A group whose records all read back goes in whole, and the first that doesn't ends the walk
    pub fn view_rows(
        &self,
        scanned: &ScannedOpen<Backend::File>,
        target: &ReplayTarget<Backend>,
        recovered: &mut RecoveredRows,
    ) -> IoResult<()> {
        let (rows, scan) = (&scanned.rows, &scanned.scan);
        let mut bytes = vec![0; GROUP_BYTES.0 as usize];
        let mut records = RecordReader::new();
        let mut value = Vec::new();
        let mut at = FileOffset(0);
        let end = ByteCount(scan.valid_end.0);
        while at.0 < scan.valid_end.0 {
            let Some((start, head)) = next_group(rows, at, end, &mut bytes, self.record_key)? else {
                break;
            };
            at = start;
            let whole = walk_rows(rows, at, &head, &mut bytes, |row| self.is_record_whole(target.data, row, &mut records, &mut value))?;
            if whole != RowsWalked::Whole {
                break;
            }
            let walked = walk_rows(rows, at, &head, &mut bytes, |row| {
                Ok(self.replay_row(row, target, recovered, &mut records, &mut value))
            })?;
            if walked != RowsWalked::Whole {
                return Err(Error::other("a writer's open rows don't fit this budget"));
            }
            recovered.marks = recovered.marks.join(head.marks);
            at = FileOffset(at.0 + head.total_length.0);
        }
        Ok(())
    }
}
