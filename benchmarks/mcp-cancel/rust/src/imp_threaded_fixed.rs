//! The threaded implementation program for the fixed rmcp: the server core of rmcp at
//! modelcontextprotocol/rust-sdk 7d6c30ed332fd99953ebccdb7469e41d6ecd9cf8 (upstream main
//! 86c65108 plus "fix(service): stop progress for a request once its cancellation is read";
//! `crates/rmcp/src/service.rs` `serve_inner` and `InboundProgress`,
//! `crates/rmcp/src/transport/async_rw.rs`) with its tasks and shared objects as Must processes.
//! Source lines are cited as `service.rs:N` and `async_rw.rs:N`, at that commit. `imp_threaded.rs`
//! is the same encoding of the unfixed rmcp; this file uses its client, tools, wire, writer mutex,
//! pool and write tasks, which are unchanged by the fix (`async_rw.rs` is byte-identical at both
//! commits), and differs from it in the serve loop and the gate map:
//! * `progress`, hidden daemon: `inbound_progress` (`service.rs:1616`, `InboundProgress`
//!   `service.rs:643-734`), the per-token gate map with the history of cancelled tokens and the
//!   gates' `closed` tokens; one request per method call or `closed` read (`progress_object`).
//! * The gated progress writes (`progress_send_tasks`, `service.rs:1617-1620`, polled by the serve
//!   loop at `service.rs:1691`) are not processes: they are futures the serve loop itself polls
//!   in its `select!`, so their steps are steps of the serve loop, taken at `PollGated` (a hidden
//!   polling step: a future to poll for the first time, or woken by its gate's closing; an arm of
//!   `select!` only if a future completes in it, otherwise `poll_next` is `Pending` and the loop
//!   waits again) and at the arm `Granted` (woken by the writer mutex).
//!
//! Configuration as `imp_fixed.rs`: `peer_cancels_subscriptions` false (the server default,
//! `service.rs:180-182`).

use std::collections::VecDeque;

use traceforge::thread::ThreadId;
use traceforge::{nondet, recv_tagged_msg_block, send_tagged_msg};

use crate::imp_threaded::{daemon, launch, lock_op, Loop, Relay, ARM, OBJ, REPLY, WRITTEN};
use crate::msgs::*;

// The gate map, copied from imp_fixed.rs lines 82-194 (its `InboundProgress`, unchanged except
// that `cancel` returns the gate it closed).

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
    /// Returns the gate it closed (threaded program: the futures waiting on that gate are woken,
    /// tokio-util `CancellationToken::cancel`; rmcp's `cancel` returns nothing).
    fn cancel(&mut self, id: u32) -> Option<usize> {
        if let Some((_, token)) = self.by_request.iter().find(|(r, _)| *r == id) {
            if let Some(i) = self.entry(token) {
                let g = self.by_token[i].gate;
                self.closed[g] = true;
                return Some(g);
            }
        }
        None
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

/// A request to the `progress` object: one per `InboundProgress` call, and `IsClosed` for a read of
/// a gate's `closed` token (`gate.closed.is_cancelled()`, `service.rs:1796`, and the gated
/// future's `biased` check, `service.rs:1807-1811`).
#[derive(Clone, PartialEq, Debug)]
enum ProgReq {
    Insert { req: u32, token: String },
    Gate { token: String },
    IsClosed { gate: usize },
    Cancel { req: u32 },
    Finish { req: u32 },
}

/// The `progress` object's replies.
#[derive(Clone, PartialEq, Debug)]
enum ProgRep {
    Ack,
    Gate(Option<usize>),
    Closed(bool),
    /// The gate `cancel` closed, if any.
    Cancelled(Option<usize>),
}

/// `inbound_progress` (daemon).
fn progress_object(serve: ThreadId) {
    let mut p = InboundProgress::new();
    loop {
        let r = match recv_tagged_msg_block::<_, ProgReq>(move |tid, tag| tid == serve && tag == Some(OBJ)) {
            ProgReq::Insert { req, token } => {
                p.insert(req, token);
                ProgRep::Ack
            }
            ProgReq::Gate { token } => ProgRep::Gate(p.gate(&token)),
            ProgReq::IsClosed { gate } => ProgRep::Closed(p.closed[gate]),
            ProgReq::Cancel { req } => ProgRep::Cancelled(p.cancel(req)),
            ProgReq::Finish { req } => {
                p.finish(req);
                ProgRep::Ack
            }
        };
        send_tagged_msg(serve, REPLY, r);
    }
}

/// One future of `progress_send_tasks` (`service.rs:1806-1833`): the progress `m` of request `req`,
/// under gate `gate`, with mutex waiter id `id`.
struct Gated {
    id: u32,
    req: u32,
    m: ToClient,
    gate: usize,
    /// Its `lock()` was polled: the waiter is queued at the mutex (or holds it).
    queued: bool,
    /// Its gate closed since it was queued: the future is woken.
    woken: bool,
}

/// The serve loop's state beyond `Loop`.
struct Fixed {
    s: Loop,
    progress: ThreadId,
    /// `progress_send_tasks`.
    gated: Vec<Gated>,
    /// A `PollGated` is on its way to the wire.
    poll_sent: bool,
}

impl Fixed {
    fn ask(&self, q: ProgReq) -> ProgRep {
        send_tagged_msg(self.progress, OBJ, q);
        let progress = self.progress;
        recv_tagged_msg_block(move |tid, tag| tid == progress && tag == Some(REPLY))
    }

    fn is_closed(&self, gate: usize) -> bool {
        self.ask(ProgReq::IsClosed { gate }) == ProgRep::Closed(true)
    }

    /// The `PollGated` step (an arm only if a future completes): `progress_send_tasks` is polled (`service.rs:1691`). Its ready futures
    /// (each not yet polled, or woken by its gate) are polled in turn until one completes, as
    /// `FuturesUnordered::poll_next` returns at the first ready one (futures-util
    /// `stream/futures_unordered/mod.rs:538-562`); with two, either order (the wake order, and a spent
    /// cooperative budget re-queues them last first: MAP-rust §8.3 "Order of the gated writes").
    /// Each poll first reads the gate (`biased`, `service.rs:1807-1811`): closed, the future
    /// completes as dropped (`ProgressDropped`, `service.rs:1825-1830`; its waiter withdrawn when
    /// the `lock()` future is dropped, before the tool is answered at `service.rs:1737-1738`). Open,
    /// it polls `lock()` for the first time: a free lock is taken and the frame written in this same
    /// poll, which completes; otherwise the waiter is queued (`Pending`; the grant comes as the
    /// `Granted` arm) and the next ready future is polled.
    fn poll_gated(&mut self) {
        let mut ready: Vec<u32> =
            self.gated.iter().filter(|f| !f.queued || f.woken).map(|f| f.id).collect();
        if ready.len() == 2 && nondet() {
            ready.swap(0, 1);
        }
        for id in ready {
            let i = self.gated.iter().position(|f| f.id == id).unwrap();
            if self.is_closed(self.gated[i].gate) {
                let f = self.gated.remove(i);
                if f.queued {
                    lock_op(self.s.lock, Relay::Withdraw { id, from: self.s.me });
                }
                self.s.req(Relay::Settled);
                self.s.resume(f.req);
                return;
            }
            if self.gated[i].queued {
                continue;
            }
            let acquire = Relay::Acquire { id, from: self.s.me, reply: self.s.wire, tag: ARM };
            if lock_op(self.s.lock, acquire) == (Relay::Acquired { now: true }) {
                let f = self.gated.remove(i);
                self.write(f);
                return;
            }
            self.gated[i].queued = true;
        }
    }

    /// The `Granted` arm: the future holding the lock is polled. Gate first: closed, it completes as
    /// dropped and the lock is given back (the `lock()` future is dropped); open, the frame is
    /// written. A future already dropped (its grant crossed its withdrawal) is not there: nothing
    /// happens.
    fn granted(&mut self, id: u32) {
        let Some(i) = self.gated.iter().position(|f| f.id == id) else {
            return;
        };
        let f = self.gated.remove(i);
        if self.is_closed(f.gate) {
            lock_op(self.s.lock, Relay::Withdraw { id, from: self.s.me });
            self.s.req(Relay::Settled);
            self.s.resume(f.req);
        } else {
            self.write(f);
        }
    }

    /// A gated future holding the lock: the frame is written (`async_rw.rs:242`), the guard dropped
    /// at the end of `send` (`async_rw.rs:240-249`), and the future completes; the tool is answered
    /// (`service.rs:1717-1727`).
    fn write(&mut self, f: Gated) {
        self.s.req(Relay::Write { m: f.m, from: self.s.me });
        let wire = self.s.wire;
        recv_tagged_msg_block::<_, Relay>(move |tid, tag| tid == wire && tag == Some(WRITTEN));
        lock_op(self.s.lock, Relay::Release { id: f.id, from: self.s.me });
        self.s.resume(f.req);
    }
}

/// The serve loop (`service.rs:1608-2132`), one step per arm of its `select!`
/// (`service.rs:1652-1707`).
fn serve(wire: ThreadId, handlers: [ThreadId; 2]) {
    let s = Loop::new(wire, handlers);
    let me = s.me;
    let progress = daemon("progress", move || progress_object(me));
    let mut f = Fixed { s, progress, gated: Vec::new(), poll_sent: false };
    loop {
        // The progress branch is ready while a future of the set needs a poll.
        if !f.poll_sent && f.gated.iter().any(|g| !g.queued || g.woken) {
            send_tagged_msg(f.s.wire, ARM, Relay::PollGated);
            f.poll_sent = true;
        }
        match f.s.select() {
            // A request (`service.rs:1856-1924`): association check passes; `local_ct_pool.insert`
            // (`service.rs:1882`); the token's gate opened (`service.rs:1883-1885`); the tool's
            // task spawned (`service.rs:1902`).
            Relay::Msg(ToServer::Call { req, token }) => {
                f.s.pool_insert(req);
                f.ask(ProgReq::Insert { req, token: token.clone() });
                f.s.req(Relay::Work { req, token });
            }
            // A tool's progress (`service.rs:1787-1852`). Its token has a gate
            // (`service.rs:1793-1795`): closed, the progress is dropped and the tool answered at
            // once (`service.rs:1796-1800`); open, a gated future is pushed
            // (`service.rs:1801-1833`). No gate: the unchanged spawned write
            // (`service.rs:1836-1851`).
            Relay::Msg(ToServer::Prog { req, token, value }) => {
                let m = ToClient::Progress { token: token.clone(), value };
                match f.ask(ProgReq::Gate { token }) {
                    ProgRep::Gate(Some(gate)) => {
                        if f.is_closed(gate) {
                            f.s.resume(req);
                        } else {
                            let id = f.s.next_id;
                            f.s.next_id += 1;
                            f.s.req(Relay::Pending);
                            f.gated.push(Gated { id, req, m, gate, queued: false, woken: false });
                        }
                    }
                    _ => f.s.spawn_write(m, true),
                }
            }
            // The tool's response (`service.rs:1746-1766`): the request leaves its token's gate
            // (`service.rs:1752`), then it is dispatched only if the request is in the pool
            // (`service.rs:1753-1765`).
            Relay::Msg(ToServer::Done { req }) => {
                f.ask(ProgReq::Finish { req });
                if f.s.pool_remove(req) {
                    f.s.spawn_write(ToClient::Result { req }, false);
                }
            }
            // `notifications/cancelled` (`service.rs:1926-1959`): `local_ct_pool.remove`, and if it
            // was there, the token's gate closed (which wakes the futures under it) and
            // `ct.cancel()`.
            Relay::Msg(ToServer::Cancelled { req }) => {
                if f.s.pool_remove(req) {
                    if let ProgRep::Cancelled(Some(gate)) = f.ask(ProgReq::Cancel { req }) {
                        for g in f.gated.iter_mut().filter(|g| g.gate == gate && g.queued) {
                            g.woken = true;
                        }
                    }
                    f.s.req(Relay::Stop { req });
                }
            }
            // EOF (`service.rs:1660-1667`); the drain (`service.rs:2068-2124`) is not modelled
            // (harness rule).
            Relay::Msg(ToServer::Shutdown) => {
                f.s.end();
                return;
            }
            // An ungated notification's write finished (`service.rs:1717-1736`).
            // (A response's write finishing, `service.rs:1740-1744`, does nothing; not sent.)
            Relay::TaskDone { req } => f.s.resume(req),
            Relay::PollGated => {
                f.poll_sent = false;
                f.poll_gated();
            }
            Relay::Granted { id } => f.granted(id),
            r => unreachable!("serve: {r:?}"),
        }
    }
}

/// The entry point of the threaded fixed program (`impltf`).
pub fn entry(args: Args) -> traceforge::program::Entry {
    std::sync::Arc::new(move || launch(args, serve))
}
