//! The store does at run time for a sorted index what a reopen would do, and nothing a reopen wouldn't
//!
//! Open shares the caches' room past their floor among the sorted indexes by the keys each held. A store that runs on fills an
//! index past what open gave it, and a reopen would give it more: out of other indexes' shares when their keys ask less, and out
//! of the caches. The reserve job does that here: for an index with rows past its room, or a step from full, it works out with
//! the open-time rule what the index's keys now ask. It takes the difference first from other indexes' ceilings past what the
//! rule gives their keys, never under what each holds and a step, which moves no memory, and then off the caches' shares, down
//! to the floor that rule keeps and never under it. The indexes' ceilings together never pass the cap. A cache gives only what its
//! shards can free, since the filter arena and the dead bits live in shares too, and it frees at least that much in whole shards
//! before the index's ceiling rises by it, so held never passes the target because of a rebalance. The write index, the tables
//! and the summaries are never part of it. A shard above a cache's ceiling stays away until a reopen

use std::sync::atomic::{AtomicU64, Ordering};

use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, RowCount};

use crate::engine::engine_sizing::{CACHE_FLOOR_QUARTERS, IndexAsk, index_ask};
use crate::engine::engine_state::EngineState;
use crate::engine::reserve_job::{WholeCache, whole_caches};
use crate::index::sorted_keys::{PAGE_BYTES, SortedKeys, planned_index_bytes, rows_for_bytes};
use crate::io::Io;

/// The spot cache, the block caches and the record cache, in the open-time rule's order
const CACHES: usize = 3;
/// The row cache's place among the whole caches. Its shards pay the block share's part first, then the filter and complete caches'
const ROW_CACHE: usize = 1;
/// The spot cache's place among the whole caches
const SPOT_CACHE: usize = 0;

/// What open shared the caches and the sorted indexes from, and what rebalances took since
#[derive(Debug)]
pub struct RebalancePlan {
    /// The spot, block and record caches' shares before the indexes took any
    pub caches: [u64; CACHES],
    /// Their shares once open gave the indexes theirs
    pub opened: [u64; CACHES],
    /// The bytes of each share rebalances gave the indexes since open
    pub taken: [AtomicU64; CACHES],
    /// Every column is sorted, so the spot cache's share went to the indexes first and keeps no floor
    pub is_all_sorted: bool,
    /// The most the indexes may take of the caches together
    pub cap: u64,
    /// The headroom each index's ask carries past its keys
    pub headroom: ByteCount,
    /// Each sorted column, its key width and its least rows, in the order open asked for them
    pub columns: Vec<(ColumnId, usize, Option<RowCount>)>,
}

impl RebalancePlan {
    /// Read each cache's floor: a quarter of its share before the indexes took any, and none for the spot cache in a store of
    /// sorted columns
    pub fn floors(&self) -> [u64; CACHES] {
        let floor = |share: u64| share / 4 * CACHE_FLOOR_QUARTERS;
        [if self.is_all_sorted { 0 } else { floor(self.caches[0]) }, floor(self.caches[1]), floor(self.caches[2])]
    }

    /// Read each cache's ceiling: its share once open gave the indexes theirs, less what rebalances took since
    pub fn ceilings(&self) -> [u64; CACHES] {
        std::array::from_fn(|cache| self.opened[cache].saturating_sub(self.taken[cache].load(Ordering::Acquire)))
    }
}

/// Read the bytes each cache may hold now, in the shares open works in: a reader's or a sim's exact store reports its shares as
/// they opened
pub fn cache_ceilings<Backend: Io>(state: &EngineState<Backend>) -> [u64; CACHES] {
    match &state.rebalance_plan {
        Some(plan) => plan.ceilings(),
        None => [state.sizing.spot_share.0, state.sizing.cache_share.0, state.sizing.record_share.0],
    }
}

/// Read the bytes each cache holds now: the spot and block caches' present shards, and the record cache's arenas in memory
pub fn cache_present<Backend: Io>(state: &EngineState<Backend>) -> [u64; CACHES] {
    let present = |cache: &dyn WholeCache| (cache.shard_count() - cache.absent_shards()) as u64 * cache.shard_bytes() as u64;
    let caches: Vec<&dyn WholeCache> = whole_caches(state).collect();
    let record: u64 = state.record_cache.rooms().iter().map(|room| room.committed() as u64).sum();
    [present(caches[SPOT_CACHE]), caches[ROW_CACHE..].iter().map(|cache| present(*cache)).sum(), record]
}

/// Check an index wants room: rows past its room that its ceiling can't hold with a step to spare, or less than a step free a step
/// under its ceiling. A marked index whose ceiling holds every row it holds and missed waits for its refill and asks no more. A
/// dropped index came back from pressure and waits for its refill too
fn wants_room(index: &SortedKeys) -> bool {
    if index.is_dropped() {
        return false;
    }
    let room = index.room();
    let rows = RowCount(index.entry_count().0 + index.past_capacity().0);
    let is_past = index.mark().is_some()
        && index.past_capacity().0 > 0
        && planned_index_bytes(index.width(), rows).0 as usize + room.step() > room.ceiling();
    let is_near = room.committed() + room.step() >= room.ceiling() && (index.free_pages() as usize) * PAGE_BYTES < room.step();
    is_past || is_near
}

/// Give a sorted index that wants room what the open-time rule gives its keys now: first out of other indexes' ceilings past
/// what the rule gives their keys, which moves no memory since an index never grew into its unused ceiling, and then out of the
/// caches' shares. Return whether any index's ceiling rose. The job calls it each step it isn't pressed, so it allocates nothing
pub fn rebalance<Backend: Io>(state: &EngineState<Backend>) -> bool {
    let Some(plan) = &state.rebalance_plan else {
        return false;
    };
    let index_at = |place: usize| state.sorted_index(plan.columns[place].0);
    let places = 0..plan.columns.len();
    if !places.clone().filter_map(index_at).any(wants_room) {
        state.indexes_at_cap.store(0, Ordering::Relaxed);
        return false;
    }
    // the open-time rule over every index's keys now, the keys it holds and the rows past its room, as share_indexes shares it,
    // each grant rounded down to whole rows' pages as open rounds it
    let keys_of = |place: usize| index_at(place).map_or(0, |index| index.entry_count().0 + index.past_capacity().0);
    let is_store_empty = places.clone().all(|place| keys_of(place) == 0);
    let ask = |place: usize| {
        let (_column, width, least) = plan.columns[place];
        match index_ask(width, keys_of(place), least, plan.headroom, is_store_empty) {
            IndexAsk::Bytes(bytes) => bytes.0,
            IndexAsk::Part => plan.cap / plan.columns.len().max(1) as u64,
        }
    };
    let asked: u64 = places.clone().map(ask).sum();
    let grant = |place: usize| {
        let wanted = ask(place);
        let bytes = if asked <= plan.cap { wanted } else { (u128::from(wanted) * u128::from(plan.cap) / u128::from(asked)) as u64 };
        let width = plan.columns[place].1;
        planned_index_bytes(width, rows_for_bytes(width, ByteCount(bytes))).0 as usize
    };
    let mut is_moved = false;
    // a pass that runs gives every index what the rule gives its keys now, not only the one that asked, as a reopen would: an index
    // whose fill ran ahead of its use doesn't read as near full at the same look as one beside it with the same keys
    for place in places.clone() {
        let Some(index) = index_at(place).filter(|index| wants_room(index) || (!index.is_dropped() && grant(place).min(index.room().reserved()) > index.room().ceiling())) else {
            continue;
        };
        // a room held to its share, or one whose reservation ends short of the grant, takes no more than it can hold
        let target = grant(place).min(index.room().reserved());
        let ceiling = index.room().ceiling();
        if target <= ceiling {
            continue;
        }
        let wanted = target - ceiling;
        let from_indexes = take_from_indexes(places.clone().map(|other| (other, index_at(other))), &grant, place, wanted);
        // the caches give no more than keeps the indexes' ceilings together under the cap, the taker's raise counted
        let ceilings: usize = places.clone().filter_map(index_at).map(|index| index.room().ceiling()).sum();
        let under_cap = (plan.cap as usize).saturating_sub(ceilings + from_indexes);
        // a remainder under the taker's own step is page rounding between the grants and the ceilings, which isn't worth a cache
        let left = wanted - from_indexes;
        // the caches free whole shards and may give a little past what was asked, which stays off them unused, so the index takes
        // no more than it asked and the ceilings together stay under the cap
        let asked = left.min(under_cap);
        let from_caches = if left < index.room().step() { 0 } else { (take_from_caches(state, plan, asked as u64) as usize).min(asked) };
        let taken = from_indexes + from_caches;
        if taken > 0 {
            index.room().set_ceiling(ceiling + taken);
            state.rebalances.fetch_add(1, Ordering::Relaxed);
            is_moved = true;
        }
    }
    // an index is at the cap when its ceiling can't hold the keys it holds and missed: the rule gives no more, or the caches are
    // at their floor. A marked index whose ceiling holds them only waits for its refill
    let at_cap = places
        .filter_map(index_at)
        .filter(|index| index.mark().is_some())
        .filter(|index| planned_index_bytes(index.width(), RowCount(index.entry_count().0 + index.past_capacity().0)).0 as usize > index.room().ceiling())
        .count();
    state.indexes_at_cap.store(at_cap as u64, Ordering::Relaxed);
    is_moved
}

/// Lower other indexes' ceilings toward what the rule gives their keys, never under the pages each has in use and a step past them,
/// nor under what it still holds once its empty prefetch went back, so a column that wakes later neither marks at once nor finds
/// its room gone for good, and return the bytes they gave. A dropped index waits for its refill and gives nothing. The job reads
/// committed() while the compact job's refill may grow that room, so a room can end one grow past its lowered ceiling: its cap
/// holds it there, the step in the floor takes up the difference, and the next rebalance finds no spare in it
fn take_from_indexes<'index>(indexes: impl Iterator<Item = (usize, Option<&'index SortedKeys>)>, grant: &impl Fn(usize) -> usize, taker: usize, wanted: usize) -> usize {
    let mut taken = 0;
    for (place, index) in indexes {
        let Some(index) = index.filter(|index| place != taker && !index.is_dropped()) else {
            continue;
        };
        if taken >= wanted {
            break;
        }
        let room = index.room();
        // empty prefetch past a step above the pages in use goes back first, through the room's own release, so the rule's
        // room for a busy index wins over it. An extent with a row in it never goes, and the ceiling stays over what the room holds
        let in_use = room.used() + room.step();
        if room.committed() > in_use && room.ceiling() > grant(place).max(in_use) {
            let _given = room.release_spare();
        }
        let floor = grant(place).max(in_use).max(room.committed());
        let spare = room.ceiling().saturating_sub(floor).min(wanted - taken);
        if spare > 0 {
            room.set_ceiling(room.ceiling() - spare);
            taken += spare;
        }
    }
    taken
}

/// Make room for the filter arena to grow this many bytes inside the caches' ceilings, and return the room there is: what the
/// caches' structures leave unused, then whole shards of the record cache and the block caches down to their floors, as open lets
/// the arena take from the record share past the filter share. No ceiling moves, so the caches and the arena stay under them
/// together. A store with no plan has its shares fixed and leaves the target to bound it
pub fn room_for_arena<Backend: Io>(state: &EngineState<Backend>, wanted: u64) -> u64 {
    let Some(plan) = &state.rebalance_plan else {
        return wanted;
    };
    let floors = plan.floors();
    let mut room = state.filter_arena.planned_left() + cache_slack(state, plan, ROW_CACHE) + cache_slack(state, plan, 2);
    for cache in [2, ROW_CACHE] {
        if room >= wanted {
            break;
        }
        let can = plan.ceilings()[cache].saturating_sub(floors[cache]).min(freeable(state, cache));
        if can > 0 {
            room += free_shards(state, cache, (wanted - room).min(can), can);
        }
    }
    room.min(wanted)
}

/// Lower the caches' shares until they give this many bytes or reach their floors, each round split by share among the caches
/// still past their floor, as open splits what the indexes take, and return the bytes they gave. Each cache frees at least what
/// it gives, in whole shards, before the bytes count
fn take_from_caches<Backend: Io>(state: &EngineState<Backend>, plan: &RebalancePlan, wanted: u64) -> u64 {
    let floors = plan.floors();
    let mut taken = 0;
    // a round leaves only what whole-byte rounding or a cache reaching its floor kept, so a few rounds always settle it
    for _round in 0..4 {
        let left = wanted.saturating_sub(taken);
        // a cache gives no more than its share's room past the floor, and no more than its shards can free: the filter arena and
        // the dead bits live in shares too, and give nothing back
        let rooms: [u64; CACHES] = std::array::from_fn(|cache| plan.ceilings()[cache].saturating_sub(floors[cache]).min(freeable(state, cache)));
        let shares: u64 = (0..CACHES).filter(|cache| rooms[*cache] > 0).map(|cache| plan.caches[cache]).sum();
        if left == 0 || shares == 0 {
            break;
        }
        for cache in (0..CACHES).filter(|cache| rooms[*cache] > 0) {
            let part = (u128::from(plan.caches[cache]) * u128::from(left)).div_ceil(u128::from(shares)) as u64;
            // a share past what its cache's structures hold, the spot index's buckets rounded down to a power of two and the like,
            // goes first and cuts no shard. Past it the shards give whole, one for one, so the structures stay under the share and
            // held under the target, and each cache keeps shards as a reopen at the smaller share sizes them
            let wanted_here = part.min(wanted.saturating_sub(taken)).min(rooms[cache]);
            let slack = cache_slack(state, plan, cache).min(wanted_here);
            let freed = if wanted_here > slack { free_shards(state, cache, wanted_here - slack, rooms[cache] - slack) } else { 0 };
            let given = (slack + freed).min(rooms[cache]);
            if given > 0 {
                plan.taken[cache].fetch_add(given, Ordering::AcqRel);
                taken += given;
            }
        }
    }
    taken
}

/// Read the bytes of a cache's share its structures leave unused: the share's ceiling past the shards under the cache's ceilings,
/// the filter arena and the dead bits, which never give back. Open lets the arena take from the record share what the filter share
/// can't hold, so the arena counts in the block share up to its room and the rest in the record share
fn cache_slack<Backend: Io>(state: &EngineState<Backend>, plan: &RebalancePlan, cache: usize) -> u64 {
    let ceilings = plan.ceilings();
    // the arena counts by its plan, which it grows into, so a rebalance never hands on room the arena hasn't brought in yet
    let arena = state.filter_arena.share_bytes().0;
    let arena_in_block = arena.min(ceilings[1].saturating_sub(shards_held(state, 1)));
    let held = match cache {
        0 => shards_held(state, cache),
        1 => shards_held(state, cache) + arena_in_block,
        _ => state.record_cache.ceiling_bytes() + state.volumes.iter().map(|volume| volume.dead_bits.heap_bytes().0).sum::<u64>() + (arena - arena_in_block),
    };
    ceilings[cache].saturating_sub(held)
}

/// Read the bytes a whole cache's shards hold under its ceilings, the spot index's or the row, filter and complete caches' together
fn shards_held<Backend: Io>(state: &EngineState<Backend>, cache: usize) -> u64 {
    places(cache)
        .iter()
        .filter_map(|place| whole_caches(state).nth(*place).map(|whole| (*place, whole)))
        .map(|(place, whole)| whole.shard_count().min(state.cache_ceilings[place].load(Ordering::Acquire)) as u64 * whole.shard_bytes() as u64)
        .sum()
}

/// Find a block cache by its place among the whole caches
fn block_at<Backend: Io>(state: &EngineState<Backend>, place: usize) -> Option<&crate::segment::block_cache::BlockCache> {
    match place {
        1 => Some(&state.row_cache),
        2 => Some(&state.filter_cache),
        3 => state.complete_cache.as_ref(),
        _ => None,
    }
}

/// Read the bytes a cache's shards could still free: the spot cache's and the block caches' shards under their ceilings, of a
/// page or more, past the one shard each keeps, and the record cache's shards not held yet past the last
pub fn freeable<Backend: Io>(state: &EngineState<Backend>, cache: usize) -> u64 {
    if cache == 2 {
        let record = &state.record_cache;
        return record.shard_count().saturating_sub(record.held_shards() + 1) as u64 * record.shard_give();
    }
    // each whole cache keeps one shard, so its keys keep a place
    places(cache)
        .iter()
        .filter_map(|place| whole_caches(state).nth(*place).map(|whole| (*place, whole)))
        .filter(|(_place, whole)| whole.shard_bytes() >= crate::region::page_bytes())
        .map(|(place, whole)| whole.shard_count().min(state.cache_ceilings[place].load(Ordering::Acquire)).saturating_sub(1) as u64 * whole.shard_bytes() as u64)
        .sum()
}

/// Read the whole caches a cache's shards come from: the spot index, or the row, filter and complete caches, the row cache first
fn places(cache: usize) -> &'static [usize] {
    if cache == 0 { &[SPOT_CACHE] } else { &[ROW_CACHE, ROW_CACHE + 1, ROW_CACHE + 2] }
}

/// Free at least these bytes of a cache in whole shards from the top, the block share's taken from the row, filter and complete
/// caches in proportion to what each holds, and spread the keys over the shards left, so every key keeps a place in the cache as a
/// reopen at the smaller share gives it. A cache keeps one shard. The shards over a lowered ceiling go back to the kernel now
fn free_shards<Backend: Io>(state: &EngineState<Backend>, cache: usize, bytes: u64, room: u64) -> u64 {
    if cache == 2 {
        let freed = hold_record_shards(state, bytes, room);
        state.record_cache.spread_over_unheld();
        return freed;
    }
    // the bytes each whole cache holds under its ceiling, counted twice over the few places so the job allocates nothing
    let holds = |place: usize| {
        whole_caches(state)
            .nth(place)
            .filter(|whole| whole.shard_bytes() >= crate::region::page_bytes())
            .map_or(0, |whole| whole.shard_count().min(state.cache_ceilings[place].load(Ordering::Acquire)) as u64 * whole.shard_bytes() as u64)
    };
    let total: u64 = places(cache).iter().map(|place| holds(*place)).sum();
    let mut given = 0;
    for &place in places(cache) {
        let held = holds(place);
        let Some(whole) = whole_caches(state).nth(place).filter(|_whole| held > 0) else {
            continue;
        };
        let share = |bytes: u64, is_up: bool| {
            let scaled = u128::from(bytes) * u128::from(held);
            (if is_up { scaled.div_ceil(u128::from(total.max(1))) } else { scaled / u128::from(total.max(1)) }) as u64
        };
        let (part, limit) = (share(bytes, true), share(room, false));
        let mut freed = 0;
        while freed < part {
            let shards = whole.shard_count().min(state.cache_ceilings[place].load(Ordering::Acquire));
            // the spot index halves, so its live count stays a power of two: a key's shard among fewer is then its shard among more
            // taken modulo, and no key's shard comes back to a slot an older version of it left. A spot slot answers a get, while a
            // block or record slot holds bytes that never change, so the block caches give one shard at a time
            let kept = if place == SPOT_CACHE { shards / 2 } else { shards - 1 };
            let bytes = (shards - kept) as u64 * whole.shard_bytes() as u64;
            // a cache keeps one shard, so its keys still have a place, and gives none that would take it past its room
            if kept == 0 || freed + bytes > limit {
                break;
            }
            state.cache_ceilings[place].store(kept, Ordering::Release);
            for shard in kept..shards {
                if whole.give_back_shard(shard) > 0 {
                    state.cache_shards_given.fetch_add(1, Ordering::Relaxed);
                }
            }
            freed += bytes;
        }
        given += freed;
        let live = whole.shard_count().min(state.cache_ceilings[place].load(Ordering::Acquire));
        match block_at(state, place) {
            Some(block) => block.set_live_shards(live),
            None => state.spot.set_live_shards(live),
        }
    }
    given
}

/// Lower one cache, the spot (0), block (1) or record (2) cache, by these bytes as a rebalance would, for the tests that check
/// reads across a ceiling change
#[cfg(feature = "test-hooks")]
pub fn shrink_cache<Backend: Io>(state: &EngineState<Backend>, cache: usize, bytes: u64) {
    let _freed = free_shards(state, cache, bytes, u64::MAX);
}

/// Hold the record cache's next shards to an extent until they give these bytes of share, never past its room and never the
/// last shard, and return the bytes of share they gave
fn hold_record_shards<Backend: Io>(state: &EngineState<Backend>, bytes: u64, room: u64) -> u64 {
    let record = &state.record_cache;
    let mut freed = 0;
    while freed < bytes && record.held_shards() + 1 < record.shard_count() && freed + record.shard_give() <= room {
        freed += record.hold_shards(record.held_shards() + 1);
    }
    freed
}
