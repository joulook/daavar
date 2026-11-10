//! Shim of neon's `libs/postgres_ffi` (fa50421), written from the acceptor's
//! usage lines (the original needs the Postgres headers): two constants and two
//! type aliases. See NOTES.md for where each value comes from.

/// Postgres WAL page size (XLOG_BLCKSZ).
pub const XLOG_BLCKSZ: usize = 8192;

/// Largest WAL chunk sent in one AppendRequest (sk.rs:577 checks it; walproposer.c
/// SendAppendRequests clamps to it).
pub const MAX_SEND_SIZE: usize = XLOG_BLCKSZ * 16;

/// Postgres default WAL segment size, 16 MiB.
pub const WAL_SEGMENT_SIZE: usize = 16 * 1024 * 1024;

/// Postgres TimeLineID (uint32).
#[allow(non_camel_case_types, clippy::upper_case_acronyms)]
pub type TimeLineID = u32;

/// Postgres XLogSegNo (uint64).
pub type XLogSegNo = u64;
