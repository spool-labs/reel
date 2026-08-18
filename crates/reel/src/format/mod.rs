//! On-disk format for reel segments: records, footers, sequence numbers, pointers
//!
//! Records pack back to back behind fixed size headers, each drain closes on a
//! block boundary with a pad, and a sealed segment ends in a packed sorted footer
//! that indexes its live records. Every structure is assembled and parsed by hand
//! with fixed endianness so the layout is stable across targets.

pub mod block;
pub mod column;
pub mod fence;
pub mod filter;
pub mod footer;
pub mod loc;
pub mod lsn;
pub mod prefix;
pub mod record;
pub mod segment_header;
