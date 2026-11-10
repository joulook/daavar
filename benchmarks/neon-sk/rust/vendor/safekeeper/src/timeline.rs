//! Replacement of safekeeper/src/timeline.rs (neon fa50421): only `TimelineError`
//! (timeline.rs:405-423, copied), which state.rs names. See NOTES.md.

use utils::id::TenantTimelineId;

#[derive(Debug, thiserror::Error)]
pub enum TimelineError {
    #[error("Timeline {0} was cancelled and cannot be used anymore")]
    Cancelled(TenantTimelineId),
    #[error("Timeline {0} was not found in global map")]
    NotFound(TenantTimelineId),
    #[error("Timeline {0} has been deleted")]
    Deleted(TenantTimelineId),
    #[error("Timeline {0} creation is in progress")]
    CreationInProgress(TenantTimelineId),
    #[error("Timeline {0} exists on disk, but wasn't loaded on startup")]
    Invalid(TenantTimelineId),
    #[error("Timeline {0} is already exists")]
    AlreadyExists(TenantTimelineId),
    #[error("Timeline {0} is not initialized, wal_seg_size is zero")]
    UninitializedWalSegSize(TenantTimelineId),
    #[error("Timeline {0} is not initialized, pg_version is unknown")]
    UninitialinzedPgVersion(TenantTimelineId),
}
