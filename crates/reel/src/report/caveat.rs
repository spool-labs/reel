//! Caveats on a report's figures, kept as data so every format shows them

use super::doc::Note;

/// A note that a figure is uncounted or only a floor
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Caveat {
    /// What is uncounted, or what is counted only as a floor
    pub what: String,

    /// The flag or command that answers it, where one does
    pub fix: Option<String>,
}

impl Caveat {
    /// A caveat with no fix
    pub fn new(what: impl Into<String>) -> Caveat {
        Caveat {
            what: what.into(),
            fix: None,
        }
    }

    /// Set the flag or command that answers the caveat
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

/// The caveats as notes for a report block
pub fn notes(caveats: &[Caveat]) -> Vec<Note> {
    caveats.iter().map(Note::from).collect()
}

/// The caveat that segment weights start from each seal's tally and may count dead bytes as live
pub fn seal_tally() -> Caveat {
    Caveat::new(
        "segment live and dead bytes start from each seal's tally, so a version outversioned \
         since its seal that the open could not join reads as live until a scrub lap settles \
         it, and a read-only open never scrubs",
    )
}
