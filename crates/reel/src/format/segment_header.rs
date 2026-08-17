//! The frozen first record payload that makes every segment self describing

use crate::error::{ReelError, Result};
use crate::format::loc::SegmentId;
use crate::format::record::read_u32_le;

/// The format version this build stamps into every new segment header
///
/// A build meeting a version it cannot read refuses the whole file here, rather
/// than truncating its walk at an unknown record kind and losing the tail silently.
pub const FORMAT_VERSION: u16 = 4;

const VERSION_LEN: usize = std::mem::size_of::<u16>();
const SEGMENT_LEN: usize = std::mem::size_of::<u32>();

const VERSION_AT: usize = 0;
const SEGMENT_AT: usize = VERSION_AT + VERSION_LEN;

/// Bytes the frozen segment header payload occupies
pub const SEGMENT_HEADER_LEN: usize = SEGMENT_AT + SEGMENT_LEN;

/// The fixed payload carried by the first record of every segment
///
/// Its layout never changes, so any future build can read the version and segment
/// number of any file ever written and identify it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentHeader {
    /// Format version the segment was written under
    pub version: u16,

    /// Monotonic segment number within the reel
    pub segment: SegmentId,
}

impl SegmentHeader {
    /// A header for a segment written under the current format version
    pub fn new(segment: SegmentId) -> SegmentHeader {
        SegmentHeader {
            version: FORMAT_VERSION,
            segment,
        }
    }

    /// Serialize to the frozen on-disk payload bytes
    pub fn pack(self) -> [u8; SEGMENT_HEADER_LEN] {
        let mut out = [0u8; SEGMENT_HEADER_LEN];
        out[VERSION_AT..SEGMENT_AT].copy_from_slice(&self.version.to_le_bytes());
        out[SEGMENT_AT..SEGMENT_HEADER_LEN].copy_from_slice(&self.segment.as_u32().to_le_bytes());
        out
    }

    /// Parse the frozen prefix, tolerating a longer payload from a future version
    pub fn unpack(bytes: &[u8]) -> Result<SegmentHeader> {
        if bytes.len() < SEGMENT_HEADER_LEN {
            return Err(ReelError::Corruption(
                "segment header payload is shorter than the frozen prefix".to_string(),
            ));
        }

        let version = read_u16_le(&bytes[VERSION_AT..SEGMENT_AT]);
        let segment = read_u32_le(&bytes[SEGMENT_AT..SEGMENT_HEADER_LEN]);

        Ok(SegmentHeader {
            version,
            segment: SegmentId(segment),
        })
    }
}

fn read_u16_le(bytes: &[u8]) -> u16 {
    let mut buf = [0u8; std::mem::size_of::<u16>()];
    buf.copy_from_slice(bytes);
    u16::from_le_bytes(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::format::lsn::Lsn;
    use crate::format::record::RecordHeader;

    // the frozen payload round trips through its byte form
    #[test]
    fn roundtrip() {
        let header = SegmentHeader::new(SegmentId(9));

        let parsed = SegmentHeader::unpack(&header.pack()).expect("unpack");

        assert_eq!(parsed, header);
        assert_eq!(parsed.version, FORMAT_VERSION);
        assert_eq!(parsed.segment, SegmentId(9));
    }

    // the frozen layout pins exact bytes so offsets cannot drift
    #[test]
    fn frozen_layout() {
        let header = SegmentHeader {
            version: 2,
            segment: SegmentId(0x0302_0100),
        };

        assert_eq!(header.pack(), [0x02, 0x00, 0x00, 0x01, 0x02, 0x03]);
        assert_eq!(SEGMENT_HEADER_LEN, VERSION_LEN + SEGMENT_LEN);
    }

    // a future version may lengthen the payload and an older build still reads it
    #[test]
    fn tolerates_longer_payload() {
        let header = SegmentHeader::new(SegmentId(4));
        let mut extended = header.pack().to_vec();
        extended.extend_from_slice(&[0xff; 16]);

        let parsed = SegmentHeader::unpack(&extended).expect("unpack");

        assert_eq!(parsed, header);
    }

    // a payload shorter than the frozen prefix is rejected
    #[test]
    fn short_payload_rejected() {
        let header = SegmentHeader::new(SegmentId(1));
        let packed = header.pack();

        assert!(SegmentHeader::unpack(&packed[..SEGMENT_HEADER_LEN - 1]).is_err());
        assert!(SegmentHeader::unpack(&[]).is_err());
    }

    // the payload rides a segment header record and verifies as its first record
    #[test]
    fn wraps_in_first_record() {
        let payload = SegmentHeader::new(SegmentId(2)).pack();
        let record = RecordHeader::segment_header(&payload);

        let parsed = RecordHeader::unpack(record.pack().as_slice()).expect("unpack");

        assert!(parsed.flags.is_segment_header());
        assert!(parsed.verify(&payload));
        assert_eq!(parsed.lsn, Lsn::NONE);

        let decoded = SegmentHeader::unpack(&payload).expect("decode");
        assert_eq!(decoded.segment, SegmentId(2));
    }
}
