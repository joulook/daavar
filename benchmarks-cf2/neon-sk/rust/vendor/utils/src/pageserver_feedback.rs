use std::time::{Duration, SystemTime};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use pq_proto::PG_EPOCH;
use serde::{Deserialize, Serialize};
use tracing::{trace, warn};

use crate::lsn::Lsn;

/// Feedback pageserver sends to safekeeper and safekeeper resends to compute.
///
/// Serialized in custom flexible key/value format. In replication protocol, it
/// is marked with NEON_STATUS_UPDATE_TAG_BYTE to differentiate from postgres
/// Standby status update / Hot standby feedback messages.
///
/// serde Serialize is used only for human readable dump to json (e.g. in
/// safekeepers debug_dump).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageserverFeedback {
    /// Last known size of the timeline. Used to enforce timeline size limit.
    pub current_timeline_size: u64,
    /// LSN last received and ingested by the pageserver. Controls backpressure.
    pub last_received_lsn: Lsn,
    /// LSN up to which data is persisted by the pageserver to its local disc.
    /// Controls backpressure.
    pub disk_consistent_lsn: Lsn,
    /// LSN up to which data is persisted by the pageserver on s3; safekeepers
    /// consider WAL before it can be removed.
    pub remote_consistent_lsn: Lsn,
    // Serialize with RFC3339 format.
    pub replytime: SystemTime,
    /// Used to track feedbacks from different shards. Always zero for unsharded tenants.
    pub shard_number: u32,
    /// If true, the pageserver has detected corruption and the safekeeper and postgres
    /// should stop sending WAL.
    pub corruption_detected: bool,
}

impl PageserverFeedback {
    pub fn empty() -> PageserverFeedback {
        PageserverFeedback {
            current_timeline_size: 0,
            last_received_lsn: Lsn::INVALID,
            remote_consistent_lsn: Lsn::INVALID,
            disk_consistent_lsn: Lsn::INVALID,
            replytime: *PG_EPOCH,
            shard_number: 0,
            corruption_detected: false,
        }
    }

    // Serialize PageserverFeedback using custom format
    // to support protocol extensibility.
    //
    // Following layout is used:
    // char - number of key-value pairs that follow.
    //
    // key-value pairs:
    // null-terminated string - key,
    // uint32 - value length in bytes
    // value itself
    //
    // TODO: change serialized fields names once all computes migrate to rename.
    pub fn serialize(&self, buf: &mut BytesMut) {
        let buf_ptr = buf.len();
        buf.put_u8(0); // # of keys, will be filled later
        let mut nkeys = 0;

        nkeys += 1;
        buf.put_slice(b"current_timeline_size\0");
        buf.put_i32(8);
        buf.put_u64(self.current_timeline_size);

        nkeys += 1;
        buf.put_slice(b"ps_writelsn\0");
        buf.put_i32(8);
        buf.put_u64(self.last_received_lsn.0);

        nkeys += 1;
        buf.put_slice(b"ps_flushlsn\0");
        buf.put_i32(8);
        buf.put_u64(self.disk_consistent_lsn.0);

        nkeys += 1;
        buf.put_slice(b"ps_applylsn\0");
        buf.put_i32(8);
        buf.put_u64(self.remote_consistent_lsn.0);

        let timestamp = self
            .replytime
            .duration_since(*PG_EPOCH)
            .expect("failed to serialize pg_replytime earlier than PG_EPOCH")
            .as_micros() as i64;

        nkeys += 1;
        buf.put_slice(b"ps_replytime\0");
        buf.put_i32(8);
        buf.put_i64(timestamp);

        if self.shard_number > 0 {
            nkeys += 1;
            buf.put_slice(b"shard_number\0");
            buf.put_i32(4);
            buf.put_u32(self.shard_number);
        }

        if self.corruption_detected {
            nkeys += 1;
            buf.put_slice(b"corruption_detected\0");
            buf.put_i32(1);
            buf.put_u8(1);
        }

        buf[buf_ptr] = nkeys;
    }
}
