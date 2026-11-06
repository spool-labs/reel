//! Unit tests for the pure functions, one module per source file
#![cfg(test)]

mod block_cache;
mod block_entry;
mod block_filter;
mod chunk_table;
mod column_directory;
mod complete_table;
mod damage_list;
mod device_error;
mod engine_sizing;
mod lz4_block;
mod open_segment;
mod pool;
mod range_covers;
mod record_check;
mod record_head;
mod record_reader;
mod row_block;
mod row_group;
mod segment_table;
mod segment_tail;
mod summary;
mod superblock;
mod varint;
mod write_order;
