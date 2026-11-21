//! The specification program: the server of one Put racing one Cancel of the same request.
//! Rules (README "Protocol"):
//! * R1 a Put is written to the disk (`Write`) and answered exactly once, `PutOk` or `Canceled`;
//! * R2 `PutOk` only after the disk's `Written`;
//! * R3 a Cancel is answered exactly once, `CancelOk` or `CancelErr`;
//! * R4 a Cancel of an unanswered Put MAY be accepted: `Canceled`, then `CancelOk`; the Put then
//!   never gets `PutOk`. It MAY also be refused: `CancelErr`, and the Put completes (R2);
//! * R5 a Cancel of an answered Put gets `CancelErr`;
//! * R6 on `Shutdown` the server waits for an outstanding `Written`, sends `Shutdown` to the
//!   disk and ends.
//!
//! R4's choice is the specification's only nondeterminism; the race itself comes from the order
//! in which the server receives the client's `Cancel` and the disk's `Written`.

use traceforge::thread::ThreadId;
use traceforge::{nondet, recv_msg_block, send_msg};

use crate::msgs::*;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Put {
    /// No Put received yet.
    None,
    /// Received and written to the disk, not answered.
    Open,
    /// Answered `PutOk`.
    Done,
    /// Answered `Canceled`.
    Canceled,
}

pub fn server(client: ThreadId, disk: ThreadId, _args: Args) {
    let mut put = Put::None;
    let mut written = false;
    loop {
        match recv_msg_block::<ToServer>() {
            ToServer::Put { req, key, val } => {
                send_msg(disk, ToDisk::Write { req, key, val }); // R1
                put = Put::Open;
            }
            ToServer::Written { req } => {
                written = true;
                if put == Put::Open {
                    send_msg(client, ToClient::PutOk { req }); // R1, R2
                    put = Put::Done;
                }
            }
            ToServer::Cancel { req } => {
                if put == Put::Open && nondet() {
                    send_msg(client, ToClient::Canceled { req }); // R4 accepted
                    send_msg(client, ToClient::CancelOk { req });
                    put = Put::Canceled;
                } else {
                    send_msg(client, ToClient::CancelErr { req }); // R4 refused, R5
                }
            }
            ToServer::Shutdown => {
                if put != Put::None && !written {
                    while !matches!(recv_msg_block::<ToServer>(), ToServer::Written { .. }) {} // R6
                }
                send_msg(disk, ToDisk::Shutdown);
                return;
            }
        }
    }
}
