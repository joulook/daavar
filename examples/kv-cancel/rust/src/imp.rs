//! The implementation: a store server written as a request table, the way a real server keeps
//! in-flight requests. It never refuses the cancel of an in-flight Put (a choice R4 allows), and
//! it forgets a request once it has answered it.
//!
//! `mutant1` (the injected bug, `args.mutant == 1`): the cancel of an in-flight Put is acknowledged
//! (`CancelOk`) but the request is left in the table, so the disk's `Written` later answers it
//! `PutOk`: a PutOk after the store acknowledged the Cancel, which R4 forbids.

use traceforge::thread::ThreadId;
use traceforge::{recv_msg_block, send_msg};

use crate::msgs::*;

/// An in-flight request: its id; removed when it is answered.
struct InFlight {
    req: u32,
}

pub fn server(client: ThreadId, disk: ThreadId, args: Args) {
    let mut table: Vec<InFlight> = Vec::new();
    let mut outstanding_writes = 0u32;
    loop {
        match recv_msg_block::<ToServer>() {
            ToServer::Put { req, key, val } => {
                table.push(InFlight { req });
                send_msg(disk, ToDisk::Write { req, key, val });
                outstanding_writes += 1;
            }
            ToServer::Written { req } => {
                outstanding_writes -= 1;
                if let Some(i) = table.iter().position(|f| f.req == req) {
                    table.remove(i);
                    send_msg(client, ToClient::PutOk { req });
                }
                // not in the table: cancelled, no reply for the write
            }
            ToServer::Cancel { req } => match table.iter().position(|f| f.req == req) {
                Some(_) if args.mutant == 1 => {
                    // BUG (mutant1): acknowledged, but the request stays in the table.
                    send_msg(client, ToClient::CancelOk { req });
                }
                Some(i) => {
                    table.remove(i);
                    send_msg(client, ToClient::Canceled { req });
                    send_msg(client, ToClient::CancelOk { req });
                }
                None => send_msg(client, ToClient::CancelErr { req }),
            },
            ToServer::Shutdown => {
                while outstanding_writes > 0 {
                    if let ToServer::Written { .. } = recv_msg_block::<ToServer>() {
                        outstanding_writes -= 1;
                    }
                }
                send_msg(disk, ToDisk::Shutdown);
                return;
            }
        }
    }
}
