//! Shim of neon's `libs/utils` (fa50421) for the vendored acceptor: the four
//! modules it uses, each cut from the fetched original. See NOTES.md.

pub mod bin_ser;
pub mod id;
pub mod lsn;
pub mod pageserver_feedback;
