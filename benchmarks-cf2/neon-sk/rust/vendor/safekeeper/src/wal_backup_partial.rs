//! Replacement of safekeeper/src/wal_backup_partial.rs (neon fa50421): the types
//! `UploadStatus`, `PartialRemoteSegment` and `State` (wal_backup_partial.rs:43-68 and 82-87,
//! derives and fields only), which state.rs names. See NOTES.md.

use safekeeper_api::Term;
use serde::{Deserialize, Serialize};
use utils::lsn::Lsn;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum UploadStatus {
    /// Upload is in progress. This status should be used only for garbage collection,
    /// don't read data from the remote storage with this status.
    InProgress,
    /// Upload is finished. There is always at most one segment with this status.
    /// It means that the segment is actual and can be used.
    Uploaded,
    /// Deletion is in progress. This status should be used only for garbage collection,
    /// don't read data from the remote storage with this status.
    Deleting,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartialRemoteSegment {
    pub status: UploadStatus,
    pub name: String,
    pub commit_lsn: Lsn,
    pub flush_lsn: Lsn,
    // We should use last_log_term here, otherwise it's possible to have inconsistent data in the
    // remote storage.
    //
    // More info here: https://github.com/neondatabase/neon/pull/8022#discussion_r1654738405
    pub term: Term,
}

// NB: these structures are a part of a control_file, you can't change them without
// changing the control file format version.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct State {
    pub segments: Vec<PartialRemoteSegment>,
}
