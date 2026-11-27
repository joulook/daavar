//! The implementation program for the fixed rmcp: the server core of rmcp at
//! modelcontextprotocol/rust-sdk 7d6c30ed332fd99953ebccdb7469e41d6ecd9cf8 (upstream main
//! 86c65108 plus the commit "fix(service): stop progress for a request once its cancellation is
//! read"; `crates/rmcp/src/service.rs` `serve_inner`, `crates/rmcp/src/service/server.rs`,
//! `crates/rmcp/src/transport/async_rw.rs`) rewritten as one sequential Must process, run in the
//! shared environment (`env::launch`). Source lines are cited as `service.rs:N`, `server.rs:N`
//! and `async_rw.rs:N`, all at that commit. `imp.rs` is the same rewrite of the unfixed rmcp; this
//! file differs from it only where the fixed code differs.
//!
//! Configuration rewritten: a `ServerHandler` served over `stdio()` (`AsyncRwTransport`), the
//! `local` feature off, the 2026-07-28 inline lifecycle with complete request metadata, the tool
//! being the environment's handler of the request, `peer_cancels_subscriptions` false (the server
//! default, `service.rs:180-182`).
//!
//! Concurrency abstracted, nothing else changed. In rmcp one serve-loop task selects among the
//! transport, the handlers' response and notification channels, its spawned write tasks and its own
//! set of progress writes (`service.rs:1652-1707`); each request's handler is a spawned task
//! (`service.rs:1877-1924`). A response, and a notification without a known progress token, is
//! written by a spawned task that takes the writer mutex (`service.rs:1758-1765`,
//! `service.rs:1836-1851`, `async_rw.rs:233-245`). A progress notification whose token a peer
//! request named is written by a future the loop itself polls, in `progress_send_tasks`
//! (`service.rs:1793-1833`, polled at `service.rs:1691`). Here every incoming message is one atomic
//! step of this process, and the pending writes of both kinds become the list `w`, each entry
//! marked with whether it is a polled (gated) write: a dispatch appends to it, and a write takes
//! one entry and sends it to the client. A message counts as sent when it is written, which is the
//! transport's acceptance of the frame (`start_send` under the writer mutex, `async_rw.rs:242`);
//! the model's write is atomic, so a frame accepted and not yet flushed does not arise. Writes
//! happen at two points inside a step, after the receive and after the handling, any number of
//! times and in any order: the spawned tasks are scheduled in any order, and the gated writes can
//! reach the writer mutex in either order (a spent cooperative budget, see `write_one`). A gated
//! write happens only at the second point, because only the loop's `select!` polls it, and the
//! loop is not in its `select!` between the receive and the end of the handling. No receive is
//! non-blocking.
//!
//! The progress gate (`service.rs:601-734`). A request that names a progress token opens that
//! token's gate (`service.rs:1883-1885`, `InboundProgress::insert` `service.rs:669-689`). A
//! cancel of a request in the pool closes its token's gate (`service.rs:1957`). A progress
//! notification whose token has a gate is dropped at dispatch if the gate is closed
//! (`service.rs:1796-1800`), else becomes a gated write, which is abandoned unwritten at its next
//! poll once its gate is closed (the `biased` select, gate first, `service.rs:1807-1811`). The end
//! of a request (its response reaching the loop, dispatched or dropped, `service.rs:1752`) removes
//! an open gate, so that progress still pending under it drains, and keeps a closed gate as a
//! tombstone among the last 1024 (`InboundProgress::finish` `service.rs:707-734`).
//!
//! A handler that sent a progress notification is suspended until the loop answers it
//! (`service.rs:1055-1065`): after the write (`service.rs:1717-1727`), or at once with `Cancelled`
//! when the progress is dropped at dispatch (`service.rs:1798`), or with `Cancelled` when its gated
//! write is abandoned (`service.rs:1825-1830`, `service.rs:1737-1738`). So a request never has more
//! than one pending write. The environment's handlers do not wait, so this program does not take a
//! handler's next message while that request has a pending write. In rmcp the handler resumes only
//! after one more loop event, which reports the finished or abandoned write; here that event is
//! merged into the write, or into the poll that abandons it: it has no visible effect, and the
//! server may still delay taking the handler's next message. The handler's reaction to a
//! `Cancelled` result is not modelled: the environment's handler keeps its branch whatever the
//! result. The first write point of a step is skipped when handling the message sends nothing,
//! because there a write before and a write after the handling take the same place in the server's
//! word.
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
//! (`service.rs:2068-2124`, which now first completes the gated writes) is not modelled.

use std::collections::VecDeque;

use traceforge::thread::ThreadId;
use traceforge::{nondet, recv_tagged_msg_block, send_msg, Nondet};

use crate::msgs::*;

/// `CANCELLED_PROGRESS_TOKENS` (`service.rs:603`).
const CANCELLED_PROGRESS_TOKENS: usize = 1024;

/// One entry of `InboundProgress::by_token` (`TokenGate`, `service.rs:626-630`): the token, the
/// gate (an index into `InboundProgress::closed`) and the live requests naming the token.
struct TokenGate {
    token: String,
    gate: usize,
    live: Vec<u32>,
}

/// `InboundProgress` (`service.rs:643-734`). A gate (`ProgressGate`, `service.rs:605-624`) is an
/// index into `closed`; a pending gated write keeps the index of the gate it was dispatched
/// under, as rmcp's future keeps its clone of the gate (`service.rs:1794`).
struct InboundProgress {
    by_token: Vec<TokenGate>,
    by_request: Vec<(u32, String)>,
    /// The tokens kept only for a cancelled request, oldest first; each at most once.
    cancelled: VecDeque<String>,
    capacity: usize,
    /// Whether gate `i` is closed (`ProgressGate::closed`).
    closed: Vec<bool>,
}

impl InboundProgress {
    fn new() -> Self {
        InboundProgress {
            by_token: Vec::new(),
            by_request: Vec::new(),
            cancelled: VecDeque::new(),
            capacity: CANCELLED_PROGRESS_TOKENS,
            closed: Vec::new(),
        }
    }

    fn entry(&self, token: &str) -> Option<usize> {
        self.by_token.iter().position(|e| e.token == token)
    }

    fn new_gate(&mut self) -> usize {
        self.closed.push(false);
        self.closed.len() - 1
    }

    /// `InboundProgress::insert` (`service.rs:669-689`).
    fn insert(&mut self, id: u32, token: String) {
        self.finish(id);
        let i = match self.entry(&token) {
            Some(i) => i,
            None => {
                let gate = self.new_gate();
                self.by_token.push(TokenGate { token: token.clone(), gate, live: Vec::new() });
                self.by_token.len() - 1
            }
        };
        if self.by_token[i].live.is_empty() {
            if self.closed[self.by_token[i].gate] {
                self.cancelled.retain(|kept| *kept != token);
            }
            let gate = self.new_gate();
            self.by_token[i].gate = gate;
        }
        self.by_token[i].live.push(id);
        self.by_request.push((id, token));
    }

    /// `InboundProgress::gate` (`service.rs:692-694`): the gate of `token`, if a peer request
    /// named it.
    fn gate(&self, token: &str) -> Option<usize> {
        self.entry(token).map(|i| self.by_token[i].gate)
    }

    /// `InboundProgress::cancel` (`service.rs:698-704`): closes the gate of the request's token.
    fn cancel(&mut self, id: u32) {
        if let Some((_, token)) = self.by_request.iter().find(|(r, _)| *r == id) {
            if let Some(i) = self.entry(token) {
                let g = self.by_token[i].gate;
                self.closed[g] = true;
            }
        }
    }

    /// `InboundProgress::finish` (`service.rs:707-734`): the request's handler finished. An open
    /// gate is forgotten; a closed one is kept, with the oldest kept ones forgotten beyond
    /// `capacity`.
    fn finish(&mut self, id: u32) {
        let Some(k) = self.by_request.iter().position(|(r, _)| *r == id) else {
            return;
        };
        let (_, token) = self.by_request.remove(k);
        let Some(i) = self.entry(&token) else {
            return;
        };
        self.by_token[i].live.retain(|live| *live != id);
        if !self.by_token[i].live.is_empty() {
            return;
        }
        if !self.closed[self.by_token[i].gate] {
            self.by_token.remove(i);
            return;
        }
        self.cancelled.push_back(token);
        while self.cancelled.len() > self.capacity {
            if let Some(oldest) = self.cancelled.pop_front() {
                if let Some(j) = self.entry(&oldest) {
                    if self.by_token[j].live.is_empty() {
                        self.by_token.remove(j);
                    }
                }
            }
        }
    }
}

/// A pending write: the message and, for a gated progress write, its gate.
struct Pending {
    m: ToClient,
    gate: Option<usize>,
}

/// The serve loop's state.
struct Server {
    client: ThreadId,
    /// The handler of request 1 and of request 2.
    handlers: [ThreadId; 2],
    /// `local_ct_pool` (`service.rs:1598`): the requests whose response has not been dispatched
    /// and that have not been cancelled. Inserted at the request (`service.rs:1882`), removed at
    /// the response (`service.rs:1753`) or the cancel (`service.rs:1954`).
    pool: Vec<u32>,
    /// `inbound_progress` (`service.rs:1616`).
    progress: InboundProgress,
    /// The dispatched writes not yet written to the client: the spawned write tasks
    /// (`service.rs:1760`, `service.rs:1847`) and the gated progress writes
    /// (`service.rs:1806`), at most one per request.
    w: Vec<Pending>,
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

    /// One write: a pending write takes the writer mutex and writes its line to stdout
    /// (`async_rw.rs:236-245`). `gated`: whether a gated write may be the one (only the loop's
    /// `select!` polls those). With two eligible, either may be first, also two gated writes:
    /// the inner `select!` of a gated write (`service.rs:1807`) returns `Pending` before polling
    /// the send when the task's cooperative budget is spent (tokio `macros/select.rs:667`), and
    /// the deferred wakers are woken last first (tokio `runtime/scheduler/defer.rs:33-35`), so a
    /// later gated write can reach the writer mutex (`async_rw.rs:240`) before an earlier one.
    fn write_one(&mut self, gated: bool) {
        let eligible: Vec<usize> =
            (0..self.w.len()).filter(|&i| gated || self.w[i].gate.is_none()).collect();
        let i = if eligible.len() == 2 { eligible[(0..2usize).nondet()] } else { eligible[0] };
        let m = self.w.remove(i).m;
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

    /// A flush point: any number of the pending writes happen here. The first point of a step
    /// (`last` false) is inside the loop's handling of the message, where only the spawned tasks
    /// run. The last point is the loop's next `select!`: it first polls the gated writes whose gate
    /// is closed, which end unwritten (`service.rs:1807-1811`, `service.rs:1824-1830`), and may
    /// then write any pending write; the writing may stop early only while a further message is
    /// certain.
    fn flush(&mut self, last: bool) {
        if last {
            let closed = &self.progress.closed;
            self.w.retain(|p| !p.gate.is_some_and(|g| closed[g]));
        }
        loop {
            let any = self.w.iter().any(|p| last || p.gate.is_none());
            if !any || !((last && !self.certain()) || nondet()) {
                break;
            }
            self.write_one(last);
        }
    }

    fn pending(&self, req: u32) -> bool {
        self.w.iter().any(|p| p.m.req() == req)
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
            // A request: `local_ct_pool.insert`, the token's gate opened, and the handler task
            // spawned (`service.rs:1877-1924`, gate `service.rs:1883-1885`); the association
            // check passes for a server (`service.rs:220-226`).
            ToServer::Call { req, token } => {
                self.call_seen[req as usize - 1] = true;
                self.pool.push(req);
                self.progress.insert(req, token.clone());
                send_msg(self.handler(req), ToHandler::Work { req, token });
            }
            // A handler's progress notification (`service.rs:1787-1852`). Its token
            // (`server.rs:54-64`) has a gate: closed, the progress is dropped and the handler
            // answered at once (`service.rs:1796-1800`); open, it becomes a gated write
            // (`service.rs:1801-1833`). No gate: the unchanged spawned write
            // (`service.rs:1836-1851`).
            ToServer::Prog { req, token, value } => {
                if req == 1 && value == 1 {
                    self.prog1_seen = true;
                }
                let gate = self.progress.gate(&token);
                if gate.is_some_and(|g| self.progress.closed[g]) {
                    return true;
                }
                self.w.push(Pending { m: ToClient::Progress { token, value }, gate });
            }
            // The handler's response: the request leaves its token's gate
            // (`service.rs:1752`), then it is dispatched only if the request is still in the
            // pool, otherwise dropped (`service.rs:1753-1756`: "dropping response for cancelled
            // request").
            ToServer::Done { req } => {
                if req == 2 {
                    self.done2_seen = true;
                }
                self.progress.finish(req);
                if let Some(i) = self.pool.iter().position(|r| *r == req) {
                    self.pool.remove(i);
                    self.w.push(Pending { m: ToClient::Result { req }, gate: None });
                }
            }
            // `notifications/cancelled`: removed from the pool, its token's gate closed and its
            // cancellation token cancelled (`service.rs:1953-1959`); an unknown or answered
            // request is ignored. A pending gated write of the request ends at the step's last
            // flush point; spawned writes are not touched.
            ToServer::Cancelled { req } => {
                self.cancel_seen[req as usize - 1] = true;
                if let Some(i) = self.pool.iter().position(|r| *r == req) {
                    self.pool.remove(i);
                    self.progress.cancel(req);
                    send_msg(self.handler(req), ToHandler::Stop { req });
                }
            }
            // Standard input closed (`service.rs:1660-1667`). Harness rule: the handlers are
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
        progress: InboundProgress::new(),
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
