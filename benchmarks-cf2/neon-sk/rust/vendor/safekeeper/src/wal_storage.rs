//! This module has everything to deal with WAL -- reading and writing to disk.
//!
//! Safekeeper WAL is stored in the timeline directory, in format similar to pg_wal.
//! PG timeline is always 1, so WAL segments are usually have names like this:
//! - 000000010000000000000001
//! - 000000010000000000000002.partial
//!
//! Note that last file has `.partial` suffix, that's different from postgres.

use std::future::Future;

use anyhow::Result;
use futures::future::BoxFuture;
use postgres_ffi::XLogSegNo;
use utils::lsn::Lsn;

use crate::metrics::WalStorageMetrics;

pub trait Storage {
    // Last written LSN.
    fn write_lsn(&self) -> Lsn;
    /// LSN of last durably stored WAL record.
    fn flush_lsn(&self) -> Lsn;

    /// Initialize segment by creating proper long header at the beginning of
    /// the segment and short header at the page of given LSN. This is only used
    /// for timeline initialization because compute will stream data only since
    /// init_lsn. Other segment headers are included in compute stream.
    fn initialize_first_segment(
        &mut self,
        init_lsn: Lsn,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Write piece of WAL from buf to disk, but not necessarily sync it.
    fn write_wal(&mut self, startpos: Lsn, buf: &[u8]) -> impl Future<Output = Result<()>> + Send;

    /// Truncate WAL at specified LSN, which must be the end of WAL record.
    fn truncate_wal(&mut self, end_pos: Lsn) -> impl Future<Output = Result<()>> + Send;

    /// Durably store WAL on disk, up to the last written WAL record.
    fn flush_wal(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Remove all segments <= given segno. Returns function doing that as we
    /// want to perform it without timeline lock.
    fn remove_up_to(&self, segno_up_to: XLogSegNo) -> BoxFuture<'static, anyhow::Result<()>>;

    /// Release resources associated with the storage -- technically, close FDs.
    /// Currently we don't remove timelines until restart (#3146), so need to
    /// spare descriptors. This would be useful for temporary tli detach as
    /// well.
    fn close(&mut self) {}

    /// Get metrics for this timeline.
    fn get_metrics(&self) -> WalStorageMetrics;
}
