//! What a reader has to know to read a report's numbers right
//!
//! A figure this open could not count is not a zero, and a total a rebuild left
//! bytes out of is a floor rather than the number. Saying so used to be prose the
//! text renderer wrote, which left every other consumer of a report reading
//! floors as totals. A caveat is a field on the report instead, so it serialises
//! with the figures it qualifies and no format can drop it.

use super::doc::Note;

/// Something standing between a figure and what a reader would take it for
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Caveat {
    /// What is uncounted, or what is counted only as a floor
    pub what: String,

    /// The flag or command that answers it, where one does
    pub fix: Option<String>,
}

impl Caveat {
    /// A caveat nothing can be run about
    pub fn new(what: impl Into<String>) -> Caveat {
        Caveat {
            what: what.into(),
            fix: None,
        }
    }

    /// Name the flag or command that answers the caveat
    pub fn fix(mut self, fix: impl Into<String>) -> Caveat {
        self.fix = Some(fix.into());
        self
    }
}

impl From<&Caveat> for Note {
    fn from(caveat: &Caveat) -> Note {
        Note {
            what: caveat.what.clone(),
            fix: caveat.fix.clone(),
        }
    }
}

/// The findings a block of caveats renders as
pub fn notes(caveats: &[Caveat]) -> Vec<Note> {
    caveats.iter().map(Note::from).collect()
}
