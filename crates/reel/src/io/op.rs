//! Owned file operations and their completions for the ring-shaped I/O trait
//!
//! Every op owns the buffers it carries and echoes a tag its completion returns,
//! so a completion backend can move ops through a real ring while a synchronous
//! backend services them in place, and neither borrows a caller buffer.

use std::path::PathBuf;
use std::sync::Arc;

use crate::error::Result;
use crate::format::record::{RecordPrefix, PREFIX_CAP};

/// Correlates a submitted op with the completion it later yields
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Tag(pub u64);

/// Handle to an open file, returned by an open and named by later ops
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FileId(pub u64);

/// Which plane answers one window of a large record
///
/// A descriptor carries O_DIRECT or it does not, so a read that may go around the
/// page cache names a second descriptor on the same file rather than a flag.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColdRoute {
    /// The buffered read the volume has always done
    Cached,

    /// Ask the page cache first and go around it only when the pages are absent
    Probed(FileId),

    /// Go around the page cache without asking
    Direct(FileId),
}

/// Whether an awaited whole-record read asks the page cache before it queues
///
/// One descriptor either way, so this picks only whether the future's first poll
/// issues a non-blocking read before anything reaches the driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WarmFirst {
    /// Ask the page cache without blocking, and queue the op only if it refuses
    Ask,

    /// Queue the op without asking
    Skip,
}

/// Owned buffer a read fills and returns in its completion
pub type OwnedBuf = Vec<u8>;

/// A buffer a read fills, handed over empty and returned holding what it read
///
/// The buffer travels with room and no length, and the backend commits the count
/// it filled, so nothing outside this type can name what was never written.
#[derive(Debug, Eq, PartialEq)]
pub struct ReadBuf {
    bytes: Vec<u8>,
    wanted: usize,
}

impl ReadBuf {
    /// Room for this many bytes, holding none of them yet
    pub fn new(wanted: usize) -> ReadBuf {
        ReadBuf {
            bytes: crate::reel::payload::take(wanted),
            wanted,
        }
    }

    /// Room for this many bytes, taken from a buffer a caller is done reading
    ///
    /// Growth goes to the pool's capacity class, never the exact want: the pool
    /// takes back only exact classes, and an odd capacity is refused forever.
    pub fn reusing(mut bytes: Vec<u8>, wanted: usize) -> ReadBuf {
        bytes.clear();
        let room = crate::reel::payload::pooled_capacity(wanted);
        if bytes.capacity() < room {
            bytes.reserve_exact(room);
        }
        ReadBuf { bytes, wanted }
    }

    /// How many bytes the read asked for
    pub fn wanted(&self) -> usize {
        self.wanted
    }

    /// How many bytes have been committed so far
    pub fn filled(&self) -> usize {
        self.bytes.len()
    }

    /// The room a backend reads into, as the pointer and length a syscall takes
    ///
    /// The bytes behind the pointer are uninitialized until a read commits them.
    pub fn as_mut_ptr(&mut self) -> (*mut u8, usize) {
        (self.bytes.as_mut_ptr(), self.wanted)
    }

    /// Take the leading bytes a read filled
    ///
    /// # Safety
    ///
    /// The caller must have written at least filled bytes to the pointer
    /// as_mut_ptr returned; committing more hands out memory nothing wrote.
    pub unsafe fn commit(&mut self, filled: usize) {
        let filled = filled.min(self.wanted);
        unsafe { self.bytes.set_len(filled) }
    }

    /// Fill from bytes already in memory, for a backend that holds its own image
    pub fn fill_from(&mut self, source: &[u8]) {
        self.bytes.clear();
        self.bytes
            .extend_from_slice(&source[..source.len().min(self.wanted)]);
    }

    /// The bytes the read committed
    pub fn into_vec(self) -> Vec<u8> {
        self.bytes
    }
}

/// Bytes a write buffer carries inline before it needs the heap
///
/// Wide enough for a record header and an inline key. A key past the bound is
/// not staged at all: it rides in its own buffer, shared from the index.
pub const INLINE_CAP: usize = PREFIX_CAP;

/// Span of the shared zero page a fill buffer names
pub const ZERO_PAGE_LEN: usize = 4096;

/// The zero bytes every alignment fill is served from
static ZERO_PAGE: [u8; ZERO_PAGE_LEN] = [0u8; ZERO_PAGE_LEN];

/// One buffer in a vectored write, owned for the life of the submission
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteBuf {
    /// A short buffer carried in place, the shape a record header takes
    Inline { bytes: [u8; INLINE_CAP], len: u8 },

    /// A heap buffer the caller handed over, the shape a payload takes
    Owned(OwnedBuf),

    /// A buffer shared by refcount, the shape a spilled key takes without a copy
    Shared(Arc<[u8]>),

    /// A run of zero fill served from the shared zero page
    Zeros(usize),
}

impl WriteBuf {
    /// Push the buffers one record prefix occupies onto a vectored write
    ///
    /// A prefix is one buffer when its key is staged beside the header and two
    /// when the key spilled, and both go on here so that no caller can push the
    /// head and drop the key.
    pub fn push_prefix(bufs: &mut Vec<WriteBuf>, prefix: RecordPrefix) {
        let (bytes, len, tail) = prefix.into_parts();
        bufs.push(WriteBuf::Inline {
            bytes,
            len: len as u8,
        });
        if let Some(tail) = tail {
            bufs.push(WriteBuf::Shared(tail));
        }
    }

    /// A buffer taking ownership of a heap payload
    pub fn owned(bytes: OwnedBuf) -> WriteBuf {
        WriteBuf::Owned(bytes)
    }

    /// A run of zero fill, allocated only when longer than the shared page
    pub fn zeros(len: usize) -> WriteBuf {
        if len > ZERO_PAGE_LEN {
            return WriteBuf::Owned(vec![0u8; len]);
        }
        WriteBuf::Zeros(len)
    }

    /// The bytes this buffer contributes to the write
    pub fn as_slice(&self) -> &[u8] {
        match self {
            WriteBuf::Inline { bytes, len } => &bytes[..*len as usize],
            WriteBuf::Owned(bytes) => bytes,
            WriteBuf::Shared(bytes) => bytes,
            WriteBuf::Zeros(len) => &ZERO_PAGE[..*len],
        }
    }

    /// How many bytes this buffer contributes
    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    /// Whether this buffer contributes nothing
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One owned file operation carrying the tag its completion echoes back
#[derive(Debug)]
pub enum Op {
    /// Open or create a segment file, yielding a file handle
    ///
    /// A direct descriptor is a second view of a segment a buffered volume
    /// already has open; a volume that is direct throughout opens that way anyway.
    Open {
        tag: Tag,
        path: PathBuf,
        create: bool,
        direct: bool,
    },
    /// Append owned buffers at an offset in one vectored write
    Writev {
        tag: Tag,
        file: FileId,
        offset: u64,
        bufs: Vec<WriteBuf>,
    },
    /// Read a byte range into the buffer, returned holding what it read
    Pread {
        tag: Tag,
        file: FileId,
        offset: u64,
        buf: ReadBuf,
    },
    /// Read a byte range that may go around the page cache
    ///
    /// Both descriptors ride along: the warm probe reads the buffered one and the
    /// cold read the direct one, and a second flight would double slot traffic.
    PreadCold {
        tag: Tag,
        file: FileId,
        direct: FileId,
        offset: u64,
        buf: ReadBuf,
        probe: bool,
    },
    /// Read one contiguous range into two buffers, so a framed record splits
    /// into its header and its payload without a copy
    PreadSplit {
        tag: Tag,
        file: FileId,
        offset: u64,
        head: ReadBuf,
        body: ReadBuf,
    },
    /// Flush a file's data, the cadence sync
    SyncData { tag: Tag, file: FileId },
    /// Flush a file fully, at seal and before an unlink
    SyncFull { tag: Tag, file: FileId },
    /// Queue or wait on writeback for a byte range
    SyncRange {
        tag: Tag,
        file: FileId,
        offset: u64,
        len: u64,
        mode: SyncRangeMode,
    },
    /// Flush a directory so a create, rename, or unlink is durable
    SyncDir { tag: Tag, dir: PathBuf },
    /// Rename a file within the volume
    Rename {
        tag: Tag,
        from: PathBuf,
        to: PathBuf,
    },
    /// Remove a file
    Unlink { tag: Tag, path: PathBuf },
    /// Release a file handle, freeing the descriptor behind it
    Close { tag: Tag, file: FileId },
    /// List the segment files under a reel directory
    List { tag: Tag, dir: PathBuf },
    /// Read one open file's length without walking its directory
    Length { tag: Tag, file: FileId },
    /// Reserve space ahead of the write head without extending logical length
    Allocate {
        tag: Tag,
        file: FileId,
        offset: u64,
        len: u64,
    },
    /// Advise the kernel on access pattern or drop cached pages
    Advise {
        tag: Tag,
        file: FileId,
        offset: u64,
        len: u64,
        advice: Advice,
    },
}

impl Op {
    /// Tag this op will echo back in its completion
    pub fn tag(&self) -> Tag {
        match self {
            Op::Open { tag, .. } => *tag,
            Op::Writev { tag, .. } => *tag,
            Op::Pread { tag, .. } => *tag,
            Op::PreadCold { tag, .. } => *tag,
            Op::PreadSplit { tag, .. } => *tag,
            Op::SyncData { tag, .. } => *tag,
            Op::SyncFull { tag, .. } => *tag,
            Op::SyncRange { tag, .. } => *tag,
            Op::SyncDir { tag, .. } => *tag,
            Op::Rename { tag, .. } => *tag,
            Op::Unlink { tag, .. } => *tag,
            Op::Close { tag, .. } => *tag,
            Op::List { tag, .. } => *tag,
            Op::Length { tag, .. } => *tag,
            Op::Allocate { tag, .. } => *tag,
            Op::Advise { tag, .. } => *tag,
        }
    }
}

/// One completion, tagged back to the op that produced it
#[derive(Debug)]
pub struct Completion {
    /// Tag echoed from the op this completes
    pub tag: Tag,

    /// Result of the op and any buffers it returns
    pub outcome: Outcome,
}

/// Result of one op, so a failure on one leaves the rest of a batch readable
#[derive(Debug)]
pub enum Outcome {
    /// An open resolved to a file handle
    Opened(Result<FileId>),
    /// A vectored write returned the bytes it wrote and its buffers back
    Wrote {
        result: Result<u64>,
        bufs: Vec<WriteBuf>,
    },
    /// A read returned how many bytes it filled and its buffer back
    Read { result: Result<usize>, buf: ReadBuf },
    /// A split read returned how many bytes it filled across both buffers
    ReadSplit {
        result: Result<usize>,
        head: ReadBuf,
        body: ReadBuf,
    },
    /// A list returned the segment directory rows
    Listed(Result<Vec<SegmentEntry>>),
    /// A length read returned the file size in bytes
    Length(Result<u64>),
    /// An op with no payload return succeeded or failed
    Done(Result<()>),
}

/// How a range sync queues or waits on writeback
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncRangeMode {
    /// Queue writeback for the range and return
    Write,
    /// Wait on the prior range, queue this one, then wait on it
    WaitBeforeWriteWaitAfter,
}

/// Kernel access-pattern advice for a byte range
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Advice {
    /// Reads will be sequential
    Sequential,
    /// Reads will be random
    Random,
    /// The range will not be read again soon
    DontNeed,
}

/// One row of a reel directory listing
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentEntry {
    /// File name of the segment
    pub name: String,

    /// Size of the segment file in bytes
    pub len: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    // every op reports the tag it carries
    #[test]
    fn tag_roundtrip() {
        let open = Op::Open {
            tag: Tag(7),
            path: PathBuf::from("/reel/segment-0"),
            create: true,
            direct: false,
        };
        let writev = Op::Writev {
            tag: Tag(9),
            file: FileId(1),
            offset: 0,
            bufs: vec![WriteBuf::owned(vec![1, 2, 3])],
        };

        assert_eq!(open.tag(), Tag(7));
        assert_eq!(writev.tag(), Tag(9));
    }

    use crate::format::column::{ColumnId, RecordKey};
    use crate::format::lsn::Lsn;
    use crate::format::record::{Flags, RecordHeader};

    // a packed prefix rides inline as the bytes it already is
    #[test]
    fn a_prefix_rides_inline_unchanged() {
        let key = RecordKey::from_bytes(ColumnId(1), &[0x5a; 32]).expect("key");
        let header = RecordHeader::new(4, Lsn(7), Flags::DATA, key, &[0x11; 4]);
        let packed = header.pack();
        let wanted = packed.as_slice().to_vec();

        let mut bufs = Vec::new();
        WriteBuf::push_prefix(&mut bufs, header.pack());

        assert_eq!(bufs.len(), 1, "an inline key needs no buffer of its own");
        assert!(matches!(bufs[0], WriteBuf::Inline { .. }));
        assert_eq!(bufs[0].len(), wanted.len());
        assert_eq!(bufs[0].as_slice(), wanted.as_slice());
    }

    // a spilled key rides beside its header, the pair carrying the framed bytes
    #[test]
    fn a_spilled_key_rides_in_its_own_buffer() {
        let key = RecordKey::from_bytes(ColumnId(1), &[0x5a; 200]).expect("key");
        let header = RecordHeader::new(4, Lsn(7), Flags::DATA, key, &[0x11; 4]);

        let mut bufs = Vec::new();
        WriteBuf::push_prefix(&mut bufs, header.pack());

        assert_eq!(bufs.len(), 2, "a spilled key needs a buffer of its own");
        assert!(matches!(bufs[1], WriteBuf::Shared(_)));
        assert_eq!(bufs[1].as_slice(), &[0x5a; 200]);
        let written: usize = bufs.iter().map(|buf| buf.len()).sum();
        assert_eq!(
            written as u64,
            header.prefix_len(),
            "the buffers carry exactly the prefix the header framed",
        );
    }

    // a fill buffer serves zeros from the shared page and allocates past it
    #[test]
    fn zeros_share_a_page() {
        let page = WriteBuf::zeros(ZERO_PAGE_LEN);
        let past = WriteBuf::zeros(ZERO_PAGE_LEN + 1);

        assert!(matches!(page, WriteBuf::Zeros(_)));
        assert!(matches!(past, WriteBuf::Owned(_)));
        assert!(page.as_slice().iter().all(|byte| *byte == 0));
        assert_eq!(past.len(), ZERO_PAGE_LEN + 1);
        assert!(WriteBuf::zeros(0).is_empty());
    }
}
