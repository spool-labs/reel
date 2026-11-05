//! Records: the payload bytes of a segment, each with its check

pub mod chunk_table;
pub mod codec;
pub mod lz4_block;
pub mod record_check;
pub mod record_encoder;
pub mod record_head;
pub mod record_reader;
