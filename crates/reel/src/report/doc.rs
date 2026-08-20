//! The shape a report takes before it is a string
//!
//! A report says itself once, as a head, a verdict and a list of blocks, and a
//! renderer turns that into a terminal's text or into markdown. Widths are
//! measured from the content rather than declared, so a long column name widens
//! its column instead of running into the next one, and the two renderers cannot
//! drift from each other because neither holds a layout of its own.

/// How a line reads: a plain fact, a clean verdict, a caveat, or a fault
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Tone {
    /// A fact, which is most of what a report says
    #[default]
    Plain,

    /// The answer a reader hoped for
    Good,

    /// True, but not the whole of it: a floor, or a figure nothing counted
    Warn,

    /// Something is wrong
    Bad,
}

/// Which side of its column a cell sits on
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Align {
    /// Names, which read down their left edge
    Left,

    /// Figures, which compare down their right
    Right,
}

/// One column of a table: its heading, and which way its cells sit
#[derive(Clone, Debug)]
pub struct Column {
    /// The heading over the column
    pub head: String,

    /// Which edge the cells line up on
    pub align: Align,
}

impl Column {
    /// A column of names
    pub fn left(head: impl Into<String>) -> Column {
        Column {
            head: head.into(),
            align: Align::Left,
        }
    }

    /// A column of figures
    pub fn right(head: impl Into<String>) -> Column {
        Column {
            head: head.into(),
            align: Align::Right,
        }
    }
}

/// One row of a table, with the note a renderer hangs off its end
#[derive(Clone, Debug, Default)]
pub struct Row {
    /// The cells, in the order the columns were declared
    pub cells: Vec<String>,

    /// What is worth saying about this row and no other
    pub note: Option<String>,

    /// How the row reads, which is what colours it
    pub tone: Tone,
}

impl Row {
    /// A row of cells, in column order
    pub fn new<Cell>(cells: impl IntoIterator<Item = Cell>) -> Row
    where
        Cell: Into<String>,
    {
        Row {
            cells: cells.into_iter().map(Into::into).collect(),
            note: None,
            tone: Tone::Plain,
        }
    }

    /// Hang a note off the end of the row
    pub fn note(mut self, note: impl Into<String>) -> Row {
        self.note = Some(note.into());
        self
    }

    /// Say how the row reads
    pub fn toned(mut self, tone: Tone) -> Row {
        self.tone = tone;
        self
    }
}

/// Rows under headings, and what the listing left out
#[derive(Clone, Debug)]
pub struct Table {
    /// The headings, which also declare how many cells a row has
    pub columns: Vec<Column>,

    /// The rows, in the order they should be read
    pub rows: Vec<Row>,

    /// What the listing is out of, said rather than left silent
    pub caption: Option<String>,
}

impl Table {
    /// A table under these headings
    pub fn new(columns: impl IntoIterator<Item = Column>) -> Table {
        Table {
            columns: columns.into_iter().collect(),
            rows: Vec::new(),
            caption: None,
        }
    }

    /// Add a row
    pub fn row(mut self, row: Row) -> Table {
        self.rows.push(row);
        self
    }

    /// Add every row of an iterator
    pub fn rows(mut self, rows: impl IntoIterator<Item = Row>) -> Table {
        self.rows.extend(rows);
        self
    }

    /// Say what the listing is out of
    pub fn caption(mut self, caption: impl Into<String>) -> Table {
        self.caption = Some(caption.into());
        self
    }
}

/// One finding, and the command that answers it
#[derive(Clone, Debug)]
pub struct Note {
    /// What was found, in a sentence
    pub what: String,

    /// The flag or command that answers it, where one does
    pub fix: Option<String>,
}

impl Note {
    /// A finding with nothing to run about it
    pub fn new(what: impl Into<String>) -> Note {
        Note {
            what: what.into(),
            fix: None,
        }
    }

    /// Name the command that answers the finding
    pub fn fix(mut self, fix: impl Into<String>) -> Note {
        self.fix = Some(fix.into());
        self
    }
}

/// A named block of findings, which is how a caveat reaches the reader
#[derive(Clone, Debug)]
pub struct Notes {
    /// What the block is, as its heading
    pub label: String,

    /// How the block reads, which is what colours it
    pub tone: Tone,

    /// The findings, most worth reading first
    pub items: Vec<Note>,
}

/// The answer, which is the line the reader came for
#[derive(Clone, Debug)]
pub struct Verdict {
    /// How it reads, which is what colours it
    pub tone: Tone,

    /// The word, which carries on its own
    pub label: String,

    /// The figures behind the word
    pub detail: String,
}

/// One part of a report
#[derive(Clone, Debug)]
pub enum Block {
    /// Prose, one line per line
    Lines(Vec<String>),

    /// Labels and their values, aligned down the label
    Facts(Vec<(String, String)>),

    /// Rows under headings
    Table(Table),

    /// A named block of findings
    Notes(Notes),

    /// A term and what stands under it, for the checked against the not checked
    Terms(Vec<(String, Vec<String>)>),

    /// Commands worth running next
    Footer(Vec<String>),
}

/// A whole report, said once and rendered any number of ways
#[derive(Clone, Debug, Default)]
pub struct Doc {
    /// What was looked at, rendered as one line of parts
    pub head: Vec<String>,

    /// The answer, where the report has one
    pub verdict: Option<Verdict>,

    /// Everything under the verdict, in order
    pub blocks: Vec<Block>,
}

impl Doc {
    /// An empty report
    pub fn new() -> Doc {
        Doc::default()
    }

    /// Add a part to the identity line
    pub fn head(mut self, part: impl Into<String>) -> Doc {
        self.head.push(part.into());
        self
    }

    /// Say the answer
    pub fn verdict(
        mut self,
        tone: Tone,
        label: impl Into<String>,
        detail: impl Into<String>,
    ) -> Doc {
        self.verdict = Some(Verdict {
            tone,
            label: label.into(),
            detail: detail.into(),
        });
        self
    }

    /// Add a line of prose
    pub fn line(mut self, line: impl Into<String>) -> Doc {
        match self.blocks.last_mut() {
            Some(Block::Lines(lines)) => lines.push(line.into()),
            _ => self.blocks.push(Block::Lines(vec![line.into()])),
        }
        self
    }

    /// Add a block of labels and values
    pub fn facts<Label, Value>(mut self, facts: impl IntoIterator<Item = (Label, Value)>) -> Doc
    where
        Label: Into<String>,
        Value: Into<String>,
    {
        let facts: Vec<(String, String)> = facts
            .into_iter()
            .map(|(label, value)| (label.into(), value.into()))
            .collect();
        match facts.is_empty() {
            true => self,
            false => {
                self.blocks.push(Block::Facts(facts));
                self
            }
        }
    }

    /// Add a table, unless it has no rows at all
    pub fn table(mut self, table: Table) -> Doc {
        match table.rows.is_empty() {
            true => self,
            false => {
                self.blocks.push(Block::Table(table));
                self
            }
        }
    }

    /// Add a named block of findings, unless there are none
    pub fn notes(mut self, label: impl Into<String>, tone: Tone, items: Vec<Note>) -> Doc {
        match items.is_empty() {
            true => self,
            false => {
                self.blocks.push(Block::Notes(Notes {
                    label: label.into(),
                    tone,
                    items,
                }));
                self
            }
        }
    }

    /// Add a term and what stands under it
    pub fn term<Item>(
        mut self,
        term: impl Into<String>,
        items: impl IntoIterator<Item = Item>,
    ) -> Doc
    where
        Item: Into<String>,
    {
        let items: Vec<String> = items.into_iter().map(Into::into).collect();
        let pair = (term.into(), items);
        match self.blocks.last_mut() {
            Some(Block::Terms(terms)) => terms.push(pair),
            _ => self.blocks.push(Block::Terms(vec![pair])),
        }
        self
    }

    /// Name the commands worth running next
    pub fn footer<Item>(mut self, commands: impl IntoIterator<Item = Item>) -> Doc
    where
        Item: Into<String>,
    {
        let commands: Vec<String> = commands.into_iter().map(Into::into).collect();
        match commands.is_empty() {
            true => self,
            false => {
                self.blocks.push(Block::Footer(commands));
                self
            }
        }
    }
}
