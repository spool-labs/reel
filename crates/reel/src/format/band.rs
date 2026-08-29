//! The death window a write is placed in, derived from the key its column marks

/// A group of records that are all dead by one position on the purge timeline
///
/// The number is that position, so a band compares against a purge floor directly.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Band(pub u64);

impl Band {
    /// The band a record dying at `death` belongs in, on a volume whose floor is `floor`
    pub fn of(death: u64, floor: u64) -> Band {
        // Zone by expected death time (Lee, Ziegler, Leis, VLDB'26 s4): the width is the
        // distance to death, since grouping on death alone opens a band per unit.
        let width = death.saturating_sub(floor).max(1).ilog2();
        let step = 1u64 << width;
        // The window's end, plus the width, which keeps two windows cut at different
        // precisions apart when they end on the same multiple.
        let end = (death & !(step - 1)).saturating_add(step);
        Band(end.saturating_add(u64::from(width)))
    }

    /// Whether the floor has passed every death this band covers
    pub fn is_finished(self, floor: u64) -> bool {
        self.0 <= floor
    }

    /// The timeline position this band is finished at
    #[inline]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // the width is the distance to death, so a near death bands finer than a far one
    #[test]
    fn precision_scales_with_the_distance() {
        assert_eq!(Band::of(101, 100), Band(102));

        assert_eq!(Band::of(116, 100), Band(132));
        assert_eq!(Band::of(127, 100), Band(132));
        assert_ne!(Band::of(128, 100), Band(132));
    }

    // everything a band covers dies before the number, so the floor settles it
    #[test]
    fn a_band_ends_past_every_death_it_holds() {
        for death in 100..400u64 {
            let band = Band::of(death, 100);

            assert!(band.as_u64() > death, "band {band:?} holds {death}");
            assert!(!band.is_finished(death));
            assert!(band.is_finished(band.as_u64()));
        }
    }

    // two windows ending together stay apart when they were cut at different widths
    #[test]
    fn widths_that_end_together_stay_apart() {
        assert_eq!(Band::of(120, 112), Band(131));
        assert_eq!(Band::of(96, 64), Band(133));
    }

    // a volume that has purged nothing reads every death as far off and bands coarsely
    #[test]
    fn no_floor_bands_coarsely() {
        assert_eq!(Band::of(1000, 0), Band(1033));
        assert_eq!(Band::of(1001, 0), Band(1033));
        assert_eq!(Band::of(1023, 0), Band(1033));
    }

    // a death already under the floor lands in a band the floor has passed
    #[test]
    fn a_dead_record_bands_finished() {
        assert!(Band::of(90, 100).is_finished(100));
        assert!(Band::of(100, 100).is_finished(101));
    }

    // the top of the timeline saturates rather than wrapping to the bottom of it
    #[test]
    fn the_last_window_has_no_end_past_it() {
        assert_eq!(Band::of(u64::MAX, 0), Band(u64::MAX));
        assert!(!Band::of(u64::MAX, 0).is_finished(u64::MAX - 1));
    }
}
