//! A maintenance job's region, admitted by its need against the host's free memory less a reserve, read at each start

use std::sync::atomic::{AtomicU64, Ordering};

use reel_core::maintenance_report::MaintenanceMemory;
use reel_core::units::ByteCount;

use crate::fixed_list::lists_full;

use crate::io::host_memory::HostMemory;

/// The reserve is at least a tenth of the host's memory
pub const RESERVE_LEAST_DIVISOR: u64 = 10;
/// The reserve is at most a quarter of the host's memory, however many footer bytes the store has
pub const RESERVE_MOST_DIVISOR: u64 = 4;
/// With no control group limit, one admission takes at most this part of the room past the reserve
pub const UNLIMITED_SHARE_DIVISOR: u64 = 4;
/// The times a pick halves its inputs to fit the room before its lane waits
pub const PICK_HALVINGS: usize = 3;

/// What a maintenance job does, for the need and touched high water each role reports
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobRole {
    /// A merge or a record rewrite on a lane
    Rewrite,
    /// The dead-byte walk of one segment
    Walk,
}

/// How many roles there are
pub const ROLE_COUNT: usize = 2;

impl JobRole {
    /// Read the role's place in the report
    pub fn index(self) -> usize {
        match self {
            Self::Rewrite => 0,
            Self::Walk => 1,
        }
    }
}

/// One role's largest need and largest touched high water so far
#[derive(Debug, Default)]
pub struct RoleMarks {
    /// The largest need a job of this role was admitted with
    pub need_peak: AtomicU64,
    /// The most bytes a job of this role wrote in its lists
    pub touched_peak: AtomicU64,
}

/// What admission has held and found, for the report
#[derive(Debug, Default)]
pub struct Admission {
    /// Bytes the regions held now take
    pub held: AtomicU64,
    /// The most bytes the regions have taken at once
    pub peak: AtomicU64,
    /// Bytes every region has mapped since open
    pub mapped_total: AtomicU64,
    /// Regions held now
    pub regions: AtomicU64,
    /// The free bytes the last start read
    pub free_at_start: AtomicU64,
    /// The reserve the last start kept back
    pub reserve_at_start: AtomicU64,
    /// Starts that found too little room and waited
    pub waits: AtomicU64,
    /// Starts admitted past the room because no region was held
    pub floor_admits: AtomicU64,
    /// Starts admitted in all
    pub admits: AtomicU64,
    /// Each role's marks
    pub roles: [RoleMarks; ROLE_COUNT],
}

impl Admission {
    /// Note a reading of the host's memory and the reserve it kept
    pub fn note_reading(&self, free: ByteCount, reserve: ByteCount) {
        self.free_at_start.store(free.0, Ordering::Relaxed);
        self.reserve_at_start.store(reserve.0, Ordering::Relaxed);
    }

    /// Check a need fits the room, or that no region is held so the floor lets it in
    pub fn decide(&self, need: ByteCount, room: ByteCount) -> Decision {
        if need <= room {
            return Decision::Admit;
        }
        if self.regions.load(Ordering::Acquire) == 0 {
            return Decision::Floor;
        }
        Decision::Wait
    }

    /// Count a region admitted at its need, which its job maps on its own thread
    pub fn take(&self, role: JobRole, need: ByteCount, is_floor: bool) {
        self.regions.fetch_add(1, Ordering::AcqRel);
        let held = self.held.fetch_add(need.0, Ordering::AcqRel) + need.0;
        self.peak.fetch_max(held, Ordering::Relaxed);
        self.admits.fetch_add(1, Ordering::Relaxed);
        if is_floor {
            self.floor_admits.fetch_add(1, Ordering::Relaxed);
        }
        self.roles[role.index()].need_peak.fetch_max(need.0, Ordering::Relaxed);
    }

    /// Read what the regions hold and what admission found, for the report
    pub fn reading(&self) -> MaintenanceMemory {
        let role = |role: JobRole| &self.roles[role.index()];
        MaintenanceMemory {
            now: ByteCount(self.held.load(Ordering::Acquire)),
            peak: ByteCount(self.peak.load(Ordering::Relaxed)),
            mapped_total: ByteCount(self.mapped_total.load(Ordering::Relaxed)),
            free_at_start: ByteCount(self.free_at_start.load(Ordering::Relaxed)),
            reserve_at_start: ByteCount(self.reserve_at_start.load(Ordering::Relaxed)),
            admits: self.admits.load(Ordering::Relaxed),
            waits: self.waits.load(Ordering::Relaxed),
            floor_admits: self.floor_admits.load(Ordering::Relaxed),
            lists_full: lists_full(),
            rewrite_need: ByteCount(role(JobRole::Rewrite).need_peak.load(Ordering::Relaxed)),
            rewrite_touched: ByteCount(role(JobRole::Rewrite).touched_peak.load(Ordering::Relaxed)),
            walk_need: ByteCount(role(JobRole::Walk).need_peak.load(Ordering::Relaxed)),
            walk_touched: ByteCount(role(JobRole::Walk).touched_peak.load(Ordering::Relaxed)),
            walk_fixed: ByteCount(0),
        }
    }

    /// Count a region given back at the need it was admitted with, and the bytes its lists wrote
    pub fn give_back(&self, role: JobRole, need: ByteCount, touched: ByteCount) {
        self.regions.fetch_sub(1, Ordering::AcqRel);
        self.held.fetch_sub(need.0, Ordering::AcqRel);
        self.roles[role.index()].touched_peak.fetch_max(touched.0, Ordering::Relaxed);
    }
}

/// A need admission let in, which its job maps on its own thread and gives back at its end
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Admit {
    /// The job's role
    pub role: JobRole,
    /// The bytes admitted
    pub need: ByteCount,
}

/// What admission says to a need
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decision {
    /// The need fits the room
    Admit,
    /// The need is past the room, and goes in since no region is held
    Floor,
    /// The need is past the room, and the job waits
    Wait,
}

/// Work out the reserve: the store's footer bytes, at least a tenth of the host and at most a quarter
pub fn reserve_for(total: ByteCount, footer_bytes: ByteCount) -> ByteCount {
    let least = total.0 / RESERVE_LEAST_DIVISOR;
    let most = total.0 / RESERVE_MOST_DIVISOR;
    ByteCount(footer_bytes.0.min(most).max(least))
}

/// Work out the room a start can take: free past the reserve, and a share of that when no control group limit binds
pub fn room_for(memory: &HostMemory, reserve: ByteCount) -> ByteCount {
    let room = memory.free().0.saturating_sub(reserve.0);
    match memory.group_room {
        Some(_limit) => ByteCount(room),
        None => ByteCount(room / UNLIMITED_SHARE_DIVISOR),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;

    #[test]
    fn reserve_runs_between_a_tenth_and_a_quarter() {
        assert_eq!(reserve_for(ByteCount(40 * GIB), ByteCount(0)), ByteCount(4 * GIB));
        assert_eq!(reserve_for(ByteCount(40 * GIB), ByteCount(7 * GIB)), ByteCount(7 * GIB));
        assert_eq!(reserve_for(ByteCount(40 * GIB), ByteCount(300 * GIB)), ByteCount(10 * GIB));
    }

    #[test]
    fn room_takes_a_share_without_a_limit() {
        let open = HostMemory { total: ByteCount(32 * GIB), available: ByteCount(28 * GIB), group_room: None, group_limit: None };
        assert_eq!(room_for(&open, ByteCount(4 * GIB)), ByteCount(6 * GIB));
        let limited = HostMemory { group_room: Some(ByteCount(GIB)), group_limit: Some(ByteCount(2 * GIB)), ..open };
        assert_eq!(limited.reserve_total(), ByteCount(2 * GIB));
        assert_eq!(room_for(&limited, ByteCount(4 * GIB)), ByteCount(0));
        let admission = Admission::default();
        assert_eq!(admission.decide(ByteCount(10), ByteCount(0)), Decision::Floor);
        admission.take(JobRole::Rewrite, ByteCount(4_096), true);
        assert_eq!(admission.decide(ByteCount(10), ByteCount(0)), Decision::Wait);
        admission.give_back(JobRole::Rewrite, ByteCount(4_096), ByteCount(8));
        assert_eq!(admission.held.load(Ordering::Relaxed), 0);
        assert_eq!(admission.peak.load(Ordering::Relaxed), 4_096);
    }
}
