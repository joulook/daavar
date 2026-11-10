//! Control file serialization, deserialization and persistence.

use std::future::Future;
use std::ops::Deref;
use std::time::Instant;

use anyhow::Result;

use crate::state::TimelinePersistentState;

pub const CHECKSUM_SIZE: usize = size_of::<u32>();

/// Storage should keep actual state inside of it. It should implement Deref
/// trait to access state fields and have persist method for updating that state.
pub trait Storage: Deref<Target = TimelinePersistentState> {
    /// Persist safekeeper state on disk and update internal state.
    fn persist(&mut self, s: &TimelinePersistentState) -> impl Future<Output = Result<()>> + Send;

    /// Timestamp of last persist.
    fn last_persist_at(&self) -> Instant;
}
