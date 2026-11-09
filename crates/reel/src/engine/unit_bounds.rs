//! The bound each kind of maintenance unit is held to, which the sims and benches check units against

use reel_api::maintenance_report::UnitKind;

use crate::engine::complete_path::REFILL_STEP_BLOCKS;
use crate::engine::filter_job::FILTER_STEP_BLOCKS;
use crate::engine::rewrite_job::{COPY_STEP_BYTES, MERGE_BUFFER_BYTES, MERGE_STEP_ROWS};
use crate::engine::seal_job::MOST_SEGMENT_BLOCKS;
use crate::engine::volume::MOST_MERGE_OUTPUTS;
use crate::index::complete_table::SWEEP_STEP_BUCKETS;
use crate::segment::footer_writer::MOST_BLOCK_ROWS;
use crate::segment::range_covers::MOST_RANGE_COVERS;
use crate::segment::row_block::ROW_BLOCK_BYTES;
use crate::segment::segment_tail::MOST_MERGE_SEGMENTS;

/// Read the most work one unit of a kind does, the named number each kind is held to. A row block of walk work is the
/// measure the others sit near. The summary's saves have no bound yet, since a save writes the whole summary
pub fn unit_bound(kind: UnitKind) -> Option<u64> {
    let row_block = MOST_BLOCK_ROWS as u64;
    match kind {
        UnitKind::WalkBlock | UnitKind::UnlinkCheck | UnitKind::HeldFrees | UnitKind::HeldDrain => Some(row_block),
        UnitKind::MergeRows => Some(MERGE_STEP_ROWS as u64),
        UnitKind::StripeCopy => Some(COPY_STEP_BYTES + MERGE_BUFFER_BYTES as u64),
        UnitKind::BlockMove => Some(MERGE_BUFFER_BYTES.div_ceil(ROW_BLOCK_BYTES) as u64),
        UnitKind::Footer => Some(u64::from(MOST_SEGMENT_BLOCKS.0)),
        UnitKind::Install => Some((MOST_MERGE_SEGMENTS + MOST_MERGE_OUTPUTS + 1) as u64),
        UnitKind::CoverSweep => Some(MOST_RANGE_COVERS as u64),
        UnitKind::FilterLoad => Some(FILTER_STEP_BLOCKS as u64),
        UnitKind::SlotRefill => Some(REFILL_STEP_BLOCKS as u64),
        UnitKind::SlotSweep => Some(SWEEP_STEP_BUCKETS as u64),
        UnitKind::CoverApply | UnitKind::ParkedClear | UnitKind::Pick => Some(1),
        UnitKind::SummaryCopy | UnitKind::SummarySave | UnitKind::Start => None,
    }
}
