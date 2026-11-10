//! Shim of neon's `storage_broker` (fa50421), written from usage: the two fields
//! of `proto::SafekeeperTimelineInfo` that `record_safekeeper_info`
//! (safekeeper.rs:1407-1417) reads. The original is generated protobuf. Never
//! constructed by the harness (broker gossip is cut). See NOTES.md.

pub mod proto {
    #[derive(Debug, Clone, Default, PartialEq)]
    pub struct SafekeeperTimelineInfo {
        pub last_log_term: u64,
        pub commit_lsn: u64,
    }
}
