//! Shim of neon's `libs/pq_proto` (fa50421), written from usage: `SystemId`
//! (safekeeper.rs:13, state.rs via safekeeper_api::ServerInfo) and `PG_EPOCH`
//! (utils/src/pageserver_feedback.rs, `empty` and `serialize`). See NOTES.md.

use std::sync::LazyLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Postgres system identifier.
pub type SystemId = u64;

/// The Postgres epoch, 2000-01-01 00:00:00 UTC (946684800 s after the Unix epoch).
pub static PG_EPOCH: LazyLock<SystemTime> =
    LazyLock::new(|| UNIX_EPOCH + Duration::from_secs(946_684_800));
