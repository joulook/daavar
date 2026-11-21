//! The implementation program: the server core of rmcp (modelcontextprotocol/rust-sdk at
//! 165f8fd514afb8f6d1be3e86260ee9abfce58541, rmcp v3.11.0: `crates/rmcp/src/service.rs`
//! `serve_inner`, `crates/rmcp/src/transport/async_rw.rs`) rewritten as one sequential Must
//! process, run in the shared environment (`env::launch`). Source lines are cited below as
//! `service.rs:N` and `async_rw.rs:N`.
//!
//! Configuration rewritten: a `ServerHandler` served over `stdio()` (`AsyncRwTransport`), the
//! `local` feature off, the 2026-07-28 inline lifecycle with complete request metadata, the tool
//! being the environment's handler of the request.
//!
//! Concurrency abstracted, nothing else changed. In rmcp one serve-loop task selects among the
//! transport, the handlers' response and notification channels and its own spawned write tasks
//! (`service.rs:1477-1525`); each request's handler is a spawned task (`service.rs:1669-1690`);
//! each message to the client is written by a spawned task that takes the writer mutex
//! (`service.rs:1573-1580`, `service.rs:1615-1621`, `async_rw.rs:233-245`). Here every incoming
//! message is one atomic step of this process, and the write tasks become the pending-write list
//! `w`: a dispatch appends to it, and a write takes one entry and sends it to the client. A message
//! counts as sent when it is written. Writes happen at two points inside a step, after the receive
//! and after the handling, any number of times and in any order (the tasks are scheduled in any
//! order). No receive is non-blocking.
//!
//! A handler that sent a progress notification is suspended until it is written
//! (`service.rs:894-904`, resolved at `service.rs:1536-1546`), so a request never has more than one
//! pending write. The environment's handlers do not wait, so this program does not take a
//! handler's next message while that request has a pending write. In rmcp the handler resumes
//! only after one more loop event, which reports the finished write (`service.rs:1501-1546`); here
//! that event is merged into the write: it has no visible effect, and the server may still delay
//! taking the handler's next message. The first write point of a step is skipped when handling the
//! message sends nothing, because there a write before and a write after the handling take the
//! same place in the server's word.
//!
//! Flush rule: rmcp's write tasks finish on their own, but a write is not an event this process can
//! wait for. A step may therefore end with `w` not empty only if a further message to the server is
//! certain; otherwise its last flush point keeps writing until `w` is empty or such a message
//! becomes certain, and a client waiting for those writes cannot block. The client decides
//! nondeterministically whether to send a second call and whether to cancel, so neither is certain.
//! Certain: the first `Call` not yet received; handler1's first `Prog` (sent in every branch) once
//! its `Work` was sent; handler2's `Done` once its `Work` was sent, while request 2 has no pending
//! write (otherwise its handler is still suspended); a client message once every request received
//! so far has its `Result` written or its cancel received (the client then sends its second call,
//! if it made one, or `Shutdown`).
//! Restriction: a pending write never stays pending past a step after which no further message is
//! certain, although handler1's second `Prog` or its `Done` (not sent in branch (c)), or a cancel
//! or second call of the client, might arrive.
//! Harness rule: after `Shutdown` nothing is written and `w` is discarded; rmcp's shutdown drain
//! (`service.rs:1829-1883`) is not modelled.

use traceforge::thread::ThreadId;
use traceforge::{nondet, recv_tagged_msg_block, send_msg, Nondet};

use crate::msgs::*;

/// The serve loop's state.
struct Server {
    client: ThreadId,
    /// The handler of request 1 and of request 2.
    handlers: [ThreadId; 2],
    /// `local_ct_pool` (`service.rs:1437`): the requests whose response has not been dispatched
    /// and that have not been cancelled. Inserted at the request (`service.rs:1652`), removed at
    /// the response (`service.rs:1568`) or the cancel (`service.rs:1721`).
    pool: Vec<u32>,
    /// The dispatched writes not yet written to the client: the spawned write tasks
    /// (`service.rs:1575`, `service.rs:1617`), at most one per request.
    w: Vec<ToClient>,
    // What the flush rule needs to know.
    /// The request's `Call` was received (its `Work` sent).
    call_seen: [bool; 2],
    prog1_seen: bool,
    done2_seen: bool,
    /// The request's cancel was received.
    cancel_seen: [bool; 2],
    /// A `Result` for the request was written.
    wres: [bool; 2],
}

impl Server {
    fn handler(&self, req: u32) -> ThreadId {
        self.handlers[req as usize - 1]
    }

    /// One write: a pending write task takes the writer mutex and writes its line to stdout
    /// (`async_rw.rs:236-245`). With two pending, either may be first.
    fn write_one(&mut self) {
        let i = if self.w.len() == 2 { (0..2usize).nondet() } else { 0 };
        let m = self.w.remove(i);
        if let ToClient::Result { req } = m {
            self.wres[req as usize - 1] = true;
        }
        send_msg(self.client, m);
    }

    /// Whether a further message to the server is certain and can be received with `w` as it is.
    fn certain(&self) -> bool {
        !self.call_seen[0]
            || (self.call_seen[0] && !self.prog1_seen)
            || (self.call_seen[1] && !self.done2_seen && !self.pending(2))
            || (0..2).all(|i| !self.call_seen[i] || self.wres[i] || self.cancel_seen[i])
    }

    /// A flush point: any number of the pending writes happen here. At the last point of a step
    /// the writing may stop early only while a further message is certain.
    fn flush(&mut self, last: bool) {
        while !self.w.is_empty() && ((last && !self.certain()) || nondet()) {
            self.write_one();
        }
    }

    fn pending(&self, req: u32) -> bool {
        self.w.iter().any(|m| m.req() == req)
    }

    /// The next message: from the client always; from a handler only while its request has no
    /// pending write (the handler would still be suspended in rmcp).
    fn receive(&self) -> ToServer {
        let client = self.client;
        let [h1, h2] = self.handlers;
        let (open1, open2) = (!self.pending(1), !self.pending(2));
        recv_tagged_msg_block::<_, ToServer>(move |tid, _| {
            tid == client || (tid == h1 && open1) || (tid == h2 && open2)
        })
    }

    /// Whether handling `m` sends to a handler. Only then can a write before the handling be told
    /// apart from one after it; other steps skip the first flush point, which loses no word.
    fn sends(&self, m: &ToServer) -> bool {
        match m {
            ToServer::Call { .. } => true,
            ToServer::Cancelled { req } => self.pool.contains(req),
            _ => false,
        }
    }

    /// Handles one message; returns false when the loop ends.
    fn handle(&mut self, m: ToServer) -> bool {
        match m {
            // A request: `local_ct_pool.insert` and the handler task spawned
            // (`service.rs:1623-1692`); the association check passes for a server
            // (`service.rs:213-220`).
            ToServer::Call { req, token } => {
                self.call_seen[req as usize - 1] = true;
                self.pool.push(req);
                send_msg(self.handler(req), ToHandler::Work { req, token });
            }
            // A handler's progress notification: dispatched with no check of the request's state
            // (`service.rs:1602-1621`), so also after the request was cancelled.
            ToServer::Prog { req, token, value } => {
                if req == 1 && value == 1 {
                    self.prog1_seen = true;
                }
                self.w.push(ToClient::Progress { token, value });
            }
            // The handler's response: dispatched only if the request is still in the pool,
            // otherwise dropped (`service.rs:1562-1580`: "dropping response for cancelled
            // request").
            ToServer::Done { req } => {
                if req == 2 {
                    self.done2_seen = true;
                }
                if let Some(i) = self.pool.iter().position(|r| *r == req) {
                    self.pool.remove(i);
                    self.w.push(ToClient::Result { req });
                }
            }
            // `notifications/cancelled`: removed from the pool and its cancellation token cancelled
            // (`service.rs:1718-1726`); an unknown or answered request is ignored. Writes already
            // dispatched are not touched.
            ToServer::Cancelled { req } => {
                self.cancel_seen[req as usize - 1] = true;
                if let Some(i) = self.pool.iter().position(|r| *r == req) {
                    self.pool.remove(i);
                    send_msg(self.handler(req), ToHandler::Stop { req });
                }
            }
            // Standard input closed (`service.rs:1485-1492`). Harness rule: the handlers are
            // told to end, nothing is written, the pending writes are discarded.
            ToServer::Shutdown => {
                self.w.clear();
                send_msg(self.handlers[0], ToHandler::Shutdown);
                send_msg(self.handlers[1], ToHandler::Shutdown);
                return false;
            }
        }
        true
    }
}

pub fn server(client: ThreadId, h1: ThreadId, h2: ThreadId, _args: Args) {
    let mut s = Server {
        client,
        handlers: [h1, h2],
        pool: Vec::new(),
        w: Vec::new(),
        call_seen: [false; 2],
        prog1_seen: false,
        done2_seen: false,
        cancel_seen: [false; 2],
        wres: [false; 2],
    };
    loop {
        let m = s.receive();
        if s.sends(&m) {
            s.flush(false);
        }
        if !s.handle(m) {
            return;
        }
        s.flush(true);
    }
}
