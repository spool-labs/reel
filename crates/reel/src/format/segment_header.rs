//! The frozen first record payload that makes every segment self describing

use crate::error::{ReelError, Result};
use crate::format::loc::SegmentId;
use crate::format::record::{read_u32_le, CheckKey, RecordLayout, CHECK_KEY_LEN};

/// The format version this build stamps into every new segment header
pub const FORMAT_VERSION: u16 = 8;

const VERSION_LEN: usize = std::mem::size_of::<u16>();
const SEGMENT_LEN: usize = std::mem::size_of::<u32>();

const VERSION_AT: usize = 0;
const SEGMENT_AT: usize = VERSION_AT + VERSION_LEN;

/// Length of the frozen prefix of the segment header payload
pub const SEGMENT_HEADER_LEN: usize = SEGMENT_AT + SEGMENT_LEN;

/// Where the record layout byte sits, behind the frozen prefix
const LAYOUT_AT: usize = SEGMENT_HEADER_LEN;

/// Where a keyless segment's check key sits, behind its layout byte
const CHECK_AT: usize = LAYOUT_AT + 1;

/// Byte position of the rows offset, after the check key
const ROWS_AT: usize = CHECK_AT + CHECK_KEY_LEN;

/// Length of the payload this build writes: prefix, layout, check key and rows offset
pub const SEGMENT_HEADER_SPAN: usize = ROWS_AT + std::mem::size_of::<u64>();

/// The payload of every segment's first record
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentHeader {
    /// The segment's format version
    pub version: u16,

    /// Monotonic segment number within the reel
    pub segment: SegmentId,

    /// How the segment frames its records
    pub layout: RecordLayout,

    /// Offset of an open segment's journal rows, or zero for none
    pub rows_at: u64,
}

impl SegmentHeader {
    /// A header for a segment written under the current format version
    pub fn new(segment: SegmentId) -> SegmentHeader {
        SegmentHeader {
            version: FORMAT_VERSION,
            segment,
            layout: RecordLayout::Keyed,
            rows_at: 0,
        }
    }

    /// The same header for a segment whose records lie in this layout
    pub fn laid_out(self, layout: RecordLayout) -> SegmentHeader {
        SegmentHeader { layout, ..self }
    }

    /// The same header with the rows offset set
    pub fn rows_from(self, rows_at: u64) -> SegmentHeader {
        SegmentHeader { rows_at, ..self }
    }

    /// Serialize to the on-disk payload bytes
    pub fn pack(self) -> [u8; SEGMENT_HEADER_SPAN] {
        let mut out = [0u8; SEGMENT_HEADER_SPAN];
        out[VERSION_AT..SEGMENT_AT].copy_from_slice(&self.version.to_le_bytes());
        out[SEGMENT_AT..SEGMENT_HEADER_LEN].copy_from_slice(&self.segment.as_u32().to_le_bytes());
        out[LAYOUT_AT] = self.layout.as_u8();
        if let RecordLayout::Keyless(check) = self.layout {
            out[CHECK_AT..ROWS_AT].copy_from_slice(&check.to_bytes());
        }
        out[ROWS_AT..SEGMENT_HEADER_SPAN].copy_from_slice(&self.rows_at.to_le_bytes());
        out
    }

    /// Parse the frozen prefix, tolerating a payload that stops before the layout
    pub fn unpack(bytes: &[u8]) -> Result<SegmentHeader> {
        if bytes.len() < SEGMENT_HEADER_LEN {
            return Err(ReelError::Corruption(
                "segment header payload is shorter than the frozen prefix".to_string(),
            ));
        }

        let version = read_u16_le(&bytes[VERSION_AT..SEGMENT_AT]);
        let segment = read_u32_le(&bytes[SEGMENT_AT..SEGMENT_HEADER_LEN]);

        let layout = match bytes.get(LAYOUT_AT) {
            None | Some(0) => RecordLayout::Keyed,
            Some(1) => {
                let check: [u8; CHECK_KEY_LEN] = bytes
                    .get(CHECK_AT..ROWS_AT)
                    .and_then(|key| key.try_into().ok())
                    .ok_or_else(|| {
                        ReelError::Corruption(
                            "a keyless segment header holds no check key".to_string(),
                        )
                    })?;
                RecordLayout::Keyless(CheckKey::from_bytes(check))
            }
            Some(byte) => {
                return Err(ReelError::Corruption(format!(
                    "segment header holds record layout {byte}, which no writer stores"
                )))
            }
        };

        let rows_at = bytes.get(ROWS_AT..SEGMENT_HEADER_SPAN).map_or(0, |at| {
            u64::from_le_bytes(at.try_into().unwrap_or_default())
        });
        Ok(SegmentHeader {
            version,
            segment: SegmentId(segment),
            layout,
            rows_at,
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

    const CHECK: CheckKey = CheckKey::from_bytes([0xc0; CHECK_KEY_LEN]);

    // the frozen layout pins exact bytes so offsets cannot drift
    #[test]
    fn frozen_layout() {
        let header = SegmentHeader {
            version: 2,
            segment: SegmentId(0x0302_0100),
            layout: RecordLayout::Keyless(CHECK),
            rows_at: 0x0807_0605_0403_0201,
        };

        let mut wanted = vec![0x02, 0x00, 0x00, 0x01, 0x02, 0x03, 0x01];
        wanted.extend_from_slice(&[0xc0; CHECK_KEY_LEN]);
        wanted.extend_from_slice(&[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
        assert_eq!(header.pack().to_vec(), wanted);
        assert_eq!(SEGMENT_HEADER_LEN, VERSION_LEN + SEGMENT_LEN);
        assert_eq!(
            SEGMENT_HEADER_SPAN,
            SEGMENT_HEADER_LEN + 1 + CHECK_KEY_LEN + 8
        );
    }

    // a keyless layout survives a round trip, and a payload without the layout byte reads keyed
    #[test]
    fn layout_roundtrip() {
        let header = SegmentHeader::new(SegmentId(9)).laid_out(RecordLayout::Keyless(CHECK));
        let packed = header.pack();

        let parsed = SegmentHeader::unpack(&packed).expect("unpack");
        let short = SegmentHeader::unpack(&packed[..LAYOUT_AT]).expect("unpack");

        assert_eq!(parsed.layout, RecordLayout::Keyless(CHECK));
        assert_eq!(short.layout, RecordLayout::Keyed);
    }

    // a keyless header cut before its check key refuses the file
    #[test]
    fn keyless_without_its_key_rejected() {
        let packed = SegmentHeader::new(SegmentId(9))
            .laid_out(RecordLayout::Keyless(CHECK))
            .pack();

        assert!(SegmentHeader::unpack(&packed[..ROWS_AT - 1]).is_err());
    }

    // a layout byte no writer stores refuses the file
    #[test]
    fn unknown_layout_rejected() {
        let mut packed = SegmentHeader::new(SegmentId(9)).pack();
        packed[LAYOUT_AT] = 7;

        assert!(SegmentHeader::unpack(&packed).is_err());
    }

    // a future version may lengthen the payload and this build still reads it
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
