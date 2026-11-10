//! Replacement of safekeeper/src/metrics.rs (neon fa50421) for the vendored
//! acceptor: the three items safekeeper.rs and wal_storage.rs name, without
//! effect (no Prometheus registry, no clock). See NOTES.md.

/// A label set of a metric vector; every operation does nothing.
pub struct NoopMetric;

impl NoopMetric {
    pub fn inc(&self) {}
    pub fn start_timer(&self) -> NoopTimer {
        NoopTimer
    }
}

/// The timer a histogram's `start_timer` returns; dropping it records nothing.
pub struct NoopTimer;

/// A metric vector: `with_label_values` returns a metric that does nothing.
pub struct NoopMetricVec;

impl NoopMetricVec {
    pub fn with_label_values(&self, _labels: &[&str]) -> NoopMetric {
        NoopMetric
    }
}

/// metrics.rs:63 (an IntCounterVec in the original).
pub static PROPOSER_ACCEPTOR_MESSAGES_TOTAL: NoopMetricVec = NoopMetricVec;

/// metrics.rs:143 (a HistogramVec in the original).
pub static MISC_OPERATION_SECONDS: NoopMetricVec = NoopMetricVec;

/// Metrics for WalStorage in a single timeline.
#[derive(Clone, Default)]
pub struct WalStorageMetrics {
    /// How much bytes were written in total.
    write_wal_bytes: u64,
    /// How much time spent writing WAL to disk, waiting for write(2).
    write_wal_seconds: f64,
    /// How much time spent syncing WAL to disk, waiting for fsync(2).
    flush_wal_seconds: f64,
}
