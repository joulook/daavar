//! Types used in safekeeper http API. Many of them are also reused internally.

use serde::{Deserialize, Serialize};

/// postgres_ffi_types::TimestampTz (not vendored; i64 as the v3 wire puts it, safekeeper.rs:808).
pub type TimestampTz = i64;

pub type FullTransactionId = u64;

/// Hot standby feedback received from replica
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct HotStandbyFeedback {
    pub ts: TimestampTz,
    pub xmin: FullTransactionId,
    pub catalog_xmin: FullTransactionId,
}

pub const INVALID_FULL_TRANSACTION_ID: FullTransactionId = 0;

impl HotStandbyFeedback {
    pub fn empty() -> HotStandbyFeedback {
        HotStandbyFeedback {
            ts: 0,
            xmin: 0,
            catalog_xmin: 0,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TimelineTermBumpResponse {
    // before the request
    pub previous_term: u64,
    pub current_term: u64,
}
