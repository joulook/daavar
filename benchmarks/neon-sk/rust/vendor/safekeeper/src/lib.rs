//! The acceptor of Neon's safekeeper (neondatabase/neon fa50421), vendored for
//! the TraceForge harness `bench_neon_sk`: safekeeper.rs and state.rs
//! byte-identical, control_file.rs and wal_storage.rs cut to their `Storage`
//! traits, metrics.rs, timeline.rs and wal_backup_partial.rs replaced. See NOTES.md.

pub mod control_file;
pub mod metrics;
pub mod safekeeper;
pub mod state;
pub mod timeline;
pub mod wal_backup_partial;
pub mod wal_storage;

// Re-exports for the harness (its Cargo.toml names only traceforge and this crate).
pub use {anyhow, bytes, postgres_ffi, postgres_versioninfo, safekeeper_api, utils};
