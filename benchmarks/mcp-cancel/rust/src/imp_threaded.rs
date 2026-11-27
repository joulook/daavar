//! The threaded implementation program: the server core of rmcp (modelcontextprotocol/rust-sdk at
//! 165f8fd514afb8f6d1be3e86260ee9abfce58541, rmcp v3.11.0; `crates/rmcp/src/service.rs`
//! `serve_inner` and `Peer::send_notification`, `crates/rmcp/src/transport/async_rw.rs`) with its
//! tasks and shared objects as Must processes, so that their interleavings are enumerated instead
//! of chosen by hand as in `imp.rs`. Source lines are cited as `service.rs:N` and `async_rw.rs:N`.
//! The configuration is `imp.rs`'s: a `ServerHandler` served over `stdio()`
//! (`AsyncRwTransport`), the `local` feature off, the 2026-07-28 inline lifecycle with complete
//! request metadata, the tool being the request's handler.
//!
//! Processes (the encoding rules are fix/THREADED-BRIEF.md §1 and §4):
//! * `server`, visible: the stdio transport and the tools' boundary (`wire`). It takes the
//!   client's lines and the tools' messages, writes frames to the client, starts and cancels the
//!   tools. Its word is the server word the specification's `server` role is compared against.
//! * `serve`, hidden: the serve-loop task (`service.rs:1447-1879`), one step per `select!` arm
//!   (`serve`).
//! * `write`, hidden daemons: each spawned write task (`service.rs:1575`, `service.rs:1617`), one
//!   process per spawn (`write_task`).
//! * `mutex`, hidden daemon: the transport's writer mutex, a tokio `Mutex` (`async_rw.rs:238-240`)
//!   (`mutex`).
//! * `pool`, hidden daemon: `local_ct_pool` (`service.rs:1437`) (`pool`).
//! * `handler1`, `handler2`, hidden: the tools, env.rs's handlers with the wait of
//!   `Peer::send_notification` (`service.rs:894-904`) (`handler`); `client`, visible: env.rs's
//!   client (`client`). Both are copies, because they send through the doorbell below; env.rs is
//!   unchanged.
//!
//! Every access to a shared object is one request message and one reply; the caller blocks on the
//! reply, so its accesses take effect in program order, and the order among processes is Must's.
//!
//! Visibility: only `server` and `client` are visible, and only the vocabulary of msgs.rs is
//! visible on them. Every other message is a `Relay`, tagged, and sent and received with
//! `Visibility::Invisible` on a visible thread, so the exported words need no projection. A receive
//! has one visibility, fixed before it reads, so the wire never waits for a visible and a hidden
//! message in the same receive: the client and the tools ring first (`Relay::Ring`, hidden), and
//! the wire then takes the rung sender's message with a visible receive aimed at that sender.
//!
//! The `select!`. The serve loop's `select!` (`service.rs:1477-1525`) takes one ready arm at a time:
//! a client line (`transport.receive()`), a tool's progress (`peer_rx`), a tool's response
//! (`sink_proxy_rx`), a finished write task (`send_task_set`, `response_send_tasks`). The wire is
//! where these meet: it takes an arm event (tag `ARM`) only while the serve loop waits in its
//! `select!` (after `Relay::Ready`), passes it on, and takes no other arm event until the next
//! `Ready`. A client line or a tool's message is therefore received when the loop's arm takes it
//! (ruling R-A, MAP-rust §8.1), never earlier. Requests (tag `REQ`: start or cancel a tool, write
//! a frame, `Ready`) are served at any time, so a spawned write can be written while the loop is
//! inside an arm.

use std::collections::VecDeque;
use std::sync::Arc;

use traceforge::program::Entry;
use traceforge::thread::{self, ThreadId};
use traceforge::{
    nondet, recv_msg_block, recv_tagged_msg_block, recv_tagged_msg_block_as, send_msg,
    send_tagged_msg, send_tagged_msg_as, Nondet, Visibility,
};

use crate::msgs::*;

// Tags of the `Relay` messages. A receive selects by sender and tag; no sender sends two message
// types to one receiver under one tag.
/// To the wire: a request it serves at any time.
pub(crate) const REQ: u32 = 1;
/// To the wire: an event of the serve loop's `select!`, taken only while the loop waits.
pub(crate) const ARM: u32 = 2;
/// Wire to serve loop: the arm that fired.
pub(crate) const EVENT: u32 = 3;
/// Wire to a writer: the frame was written.
pub(crate) const WRITTEN: u32 = 4;
/// To the writer mutex.
pub(crate) const LOCK: u32 = 5;
/// Writer mutex to a write task: the lock is the task's.
pub(crate) const GRANT: u32 = 6;
/// Writer mutex to the caller of an operation: it took effect (`Acquired`, `Ack`).
pub(crate) const LOCKED: u32 = 10;
/// To an object process (`pool`; the fixed program's `progress`).
pub(crate) const OBJ: u32 = 7;
/// Object process to the serve loop: the reply.
pub(crate) const REPLY: u32 = 8;
/// Serve loop to a tool: the answer to its progress (`Resume`), or the end (`End`).
pub(crate) const RESUME: u32 = 9;
/// Client to the wire: the `Ring` before its `Shutdown` (restriction R-EOF, MAP-rust §9.3 V12).
pub(crate) const LAST: u32 = 11;

/// The hidden messages of the threaded programs (both `imp_threaded.rs` and
/// `imp_threaded_fixed.rs`; the variants marked "fixed" are sent only by the fixed program).
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Relay {
    // To the wire, `ARM`.
    /// The client or a tool: my next message to the server follows.
    Ring { from: ThreadId },
    /// A spawned notification write task finished (`send_task_set`). A response's write task
    /// (`response_send_tasks`) sends nothing: its arm does nothing (`service.rs:1556-1560`).
    TaskDone { req: u32 },
    /// Writer mutex (fixed): the serve loop's gated write `id` holds the lock.
    Granted { id: u32 },
    /// Serve loop (fixed): `progress_send_tasks` has a future to poll.
    PollGated,
    // To the wire, `REQ`.
    /// The serve loop is in its `select!`.
    Ready,
    /// Spawn the tool of request `req` (`service.rs:1669`).
    Work { req: u32, token: String },
    /// Cancel the tool's token (`service.rs:1724`).
    Stop { req: u32 },
    /// A writer holding the mutex writes the frame `m` (`async_rw.rs:242`).
    Write { m: ToClient, from: ThreadId },
    /// Restriction R-EOF (V12): the serve loop created a write (a spawned task, or in the fixed
    /// program a gated future); it is pending until its `Write` is served.
    Pending,
    /// Restriction R-EOF (V12), fixed: a pending gated future was dropped (its gate closed).
    Settled,
    // Wire to serve loop, `EVENT`: the arm that fired. A client line or a tool's message is
    // `Msg`; `TaskDone`, `Granted` and `PollGated` are passed on as they are.
    Msg(ToServer),
    // Wire to writer, `WRITTEN`.
    Written,
    // To the writer mutex, `LOCK`; each is answered to `from` (tag `LOCKED`) once it took effect.
    /// `lock().await` is polled for the first time (tokio 1.54.1 `sync/batch_semaphore.rs:432-578`):
    /// the lock is taken at once if it is free (`Acquired { now: true }`), otherwise the waiter is
    /// queued (`batch_semaphore.rs:573`; `Acquired { now: false }`) and the grant goes later to
    /// `reply` with tag `tag`.
    Acquire { id: u32, from: ThreadId, reply: ThreadId, tag: u32 },
    /// The guard is dropped (`sync/mutex.rs:964-966`); answered `Ack`.
    Release { id: u32, from: ThreadId },
    /// The `lock()` future is dropped (fixed): out of the queue, or the lock given back if it was
    /// granted (`batch_semaphore.rs:758-781`); answered `Ack`.
    Withdraw { id: u32, from: ThreadId },
    // Writer mutex to the caller, `LOCKED`.
    Acquired { now: bool },
    // Writer mutex to write task, `GRANT`: `Granted` as above; `Ack` with `LOCKED`.
    // Serve loop to `pool`, `OBJ`.
    PoolInsert { req: u32 },
    PoolRemove { req: u32 },
    // `pool` to serve loop, `REPLY`.
    Ack,
    /// `remove` returned `Some`.
    Found(bool),
    // Serve loop to tool, `RESUME`.
    /// The responder of `send_notification` answered (`service.rs:1546`).
    Resume,
    /// The serve loop ended: a tool still waiting ends.
    End,
}

/// The client: env.rs's `client`, sending each message after a `Ring` (hidden). Everything else is
/// unchanged: the same messages, order and choice points.
pub(crate) fn client(wire: ThreadId, cancel2: bool) {
    let me = thread::current().id();
    let line = move |m: ToServer| {
        // R-EOF (V12): the `Shutdown`'s ring has its own tag, which the wire takes only while no
        // write is pending.
        let tag = if m == ToServer::Shutdown { LAST } else { ARM };
        send_tagged_msg_as(wire, Visibility::Invisible, tag, Relay::Ring { from: me });
        send_msg(wire, m);
    };
    line(ToServer::Call { req: 1, token: token(1) });
    let sent2 = nondet();
    if sent2 {
        line(ToServer::Call { req: 2, token: token(2) });
    }
    let sent = [true, sent2];
    let cancellable = [true, sent2 && cancel2];
    let (mut answered, mut cancelled) = ([false; 2], [false; 2]);
    let maybe_cancel = |answered: &[bool; 2], cancelled: &mut [bool; 2]| {
        for i in 0..2 {
            if cancellable[i] && !answered[i] && !cancelled[i] && nondet() {
                line(ToServer::Cancelled { req: i as u32 + 1 });
                cancelled[i] = true;
            }
        }
    };
    maybe_cancel(&answered, &mut cancelled);
    while (0..2).any(|i| sent[i] && !answered[i] && !cancelled[i]) {
        match recv_msg_block::<ToClient>() {
            ToClient::Result { req } | ToClient::Error { req, .. } => answered[req as usize - 1] = true,
            ToClient::Progress { .. } => {}
        }
        if (0..2).any(|i| sent[i] && !answered[i] && !cancelled[i]) {
            maybe_cancel(&answered, &mut cancelled);
        }
    }
    line(ToServer::Shutdown);
}

/// A tool: env.rs's `handler`, sending each message after a `Ring`, and waiting after each
/// progress for the serve loop's answer, as `Peer::send_notification` awaits its responder
/// (`service.rs:894-904`). The answer's value (a `Cancelled` error in the fixed program) is not
/// looked at: the branch is kept whatever it is. `End` instead of the answer ends the tool.
pub(crate) fn handler(wire: ThreadId, first: bool, branch: Branch) {
    let me = thread::current().id();
    let signal = move |m: ToServer| {
        send_tagged_msg(wire, ARM, Relay::Ring { from: me });
        send_msg(wire, m);
    };
    // Sends a progress and waits for its answer; false: the serve loop ended.
    let progress = move |req: u32, token: String, value: u32| {
        signal(ToServer::Prog { req, token, value });
        recv_tagged_msg_block::<_, Relay>(|_, tag| tag == Some(RESUME)) == Relay::Resume
    };
    loop {
        match recv_tagged_msg_block::<_, ToHandler>(|_, tag| tag.is_none()) {
            ToHandler::Work { req, token } => {
                if !progress(req, token.clone(), 1) {
                    return;
                }
                let b = match branch {
                    Branch::Any if first => [Branch::A, Branch::B, Branch::C][(0..3usize).nondet()],
                    Branch::Any => {
                        if nondet() {
                            Branch::A
                        } else {
                            Branch::B
                        }
                    }
                    b => b,
                };
                match b {
                    Branch::A => signal(ToServer::Done { req }),
                    Branch::B => {
                        if !progress(req, token, 2) {
                            return;
                        }
                        signal(ToServer::Done { req });
                    }
                    _ => {}
                }
            }
            ToHandler::Stop { .. } => {}
            ToHandler::Shutdown => return,
        }
    }
}

/// The wire (`server`, visible): the stdio transport (`async_rw.rs`) and the tools' boundary.
/// * `Ready`: the serve loop waits in its `select!`; one arm event may now be taken.
/// * `Ring` from the client or a tool, while the loop waits: that sender's message is taken
///   (visible receive) and passed on as the arm event. A client `Shutdown` is stdin's EOF
///   (`service.rs:1485-1492`); harness rule as `imp.rs`: both tools are told to end, nothing more
///   is written, the wire ends.
/// * `TaskDone`, `Granted`, `PollGated`, while the loop waits: passed on as the event (`TaskDone`,
///   `Granted`: an arm; `PollGated`: a hidden polling step of the progress branch, an arm only if a
///   future completes in it).
/// * `Work`, `Stop`: the tool is started or cancelled (visible send).
/// * `Write`: the frame is written to the client (visible send) and the writer told.
/// * Restriction R-EOF (V12, a labelled restriction): the client's `Shutdown` (its ring, tag
///   `LAST`) is taken only while no write is pending (`Pending` counted up, a served `Write` or a
///   `Settled` counted down). A stand-in for the drains (`service.rs:1829-1883` /
///   `service.rs:2068-2124`), which are not modelled: a pending write is made before EOF is taken.
pub(crate) fn wire(serve: ThreadId, client: ThreadId, handlers: [ThreadId; 2]) {
    let mut waiting = false;
    let mut pending: i32 = 0;
    let pass = |m: Relay| send_tagged_msg_as(serve, Visibility::Invisible, EVENT, m);
    loop {
        let (w, idle) = (waiting, pending == 0);
        let r: Relay = recv_tagged_msg_block_as(Visibility::Invisible, move |_, tag| {
            tag == Some(REQ) || (w && (tag == Some(ARM) || (idle && tag == Some(LAST))))
        });
        match r {
            Relay::Ready => waiting = true,
            Relay::Pending => pending += 1,
            Relay::Settled => pending -= 1,
            Relay::Ring { from } => {
                let m: ToServer = recv_tagged_msg_block(move |tid, tag| tid == from && tag.is_none());
                waiting = false;
                let end = m == ToServer::Shutdown;
                if end {
                    send_msg(handlers[0], ToHandler::Shutdown);
                    send_msg(handlers[1], ToHandler::Shutdown);
                }
                pass(Relay::Msg(m));
                if end {
                    return;
                }
            }
            a @ (Relay::TaskDone { .. } | Relay::Granted { .. } | Relay::PollGated) => {
                waiting = false;
                pass(a);
            }
            Relay::Work { req, token } => send_msg(handlers[req as usize - 1], ToHandler::Work { req, token }),
            Relay::Stop { req } => send_msg(handlers[req as usize - 1], ToHandler::Stop { req }),
            Relay::Write { m, from } => {
                pending -= 1;
                send_msg(client, m);
                send_tagged_msg_as(from, Visibility::Invisible, WRITTEN, Relay::Written);
            }
            r => unreachable!("wire: {r:?}"),
        }
    }
}

/// The writer mutex (daemon): tokio's `Mutex`, FIFO among the waiters queued (tokio 1.54.1
/// `sync/mutex.rs:20`, `112-114`). Each operation takes effect when the mutex receives it and is
/// answered then, so the caller's operations take effect in its program order; the order among
/// callers is Must's. A first poll of `lock()` takes a free lock at once; otherwise the waiter is
/// queued. When the lock is released or given back, the oldest waiter gets it (`Granted`).
pub(crate) fn mutex() {
    let mut queue: VecDeque<(u32, ThreadId, u32)> = VecDeque::new();
    let mut holder: Option<u32> = None;
    loop {
        match recv_tagged_msg_block::<_, Relay>(|_, tag| tag == Some(LOCK)) {
            Relay::Acquire { id, from, reply, tag } => {
                let now = holder.is_none() && queue.is_empty();
                if now {
                    holder = Some(id);
                } else {
                    queue.push_back((id, reply, tag));
                }
                send_tagged_msg(from, LOCKED, Relay::Acquired { now });
            }
            Relay::Release { id, from } => {
                assert_eq!(holder, Some(id));
                holder = None;
                send_tagged_msg(from, LOCKED, Relay::Ack);
            }
            Relay::Withdraw { id, from } => {
                if holder == Some(id) {
                    holder = None;
                } else {
                    queue.retain(|w| w.0 != id);
                }
                send_tagged_msg(from, LOCKED, Relay::Ack);
            }
            r => unreachable!("mutex: {r:?}"),
        }
        if holder.is_none() {
            if let Some((id, reply, tag)) = queue.pop_front() {
                holder = Some(id);
                send_tagged_msg(reply, tag, Relay::Granted { id });
            }
        }
    }
}

/// A request to the writer mutex, answered once it took effect.
pub(crate) fn lock_op(lock: ThreadId, m: Relay) -> Relay {
    send_tagged_msg(lock, LOCK, m);
    recv_tagged_msg_block(move |tid, tag| tid == lock && tag == Some(LOCKED))
}

/// `local_ct_pool` (daemon, `service.rs:1437`): the requests that have a cancellation token.
/// `insert` and `remove` are one request each; ids are used once, so the token is a presence bit.
pub(crate) fn pool(serve: ThreadId) {
    let mut set: Vec<u32> = Vec::new();
    loop {
        match recv_tagged_msg_block::<_, Relay>(move |tid, tag| tid == serve && tag == Some(OBJ)) {
            Relay::PoolInsert { req } => {
                set.push(req);
                send_tagged_msg(serve, REPLY, Relay::Ack);
            }
            Relay::PoolRemove { req } => {
                let found = set.iter().position(|r| *r == req).map(|i| set.remove(i)).is_some();
                send_tagged_msg(serve, REPLY, Relay::Found(found));
            }
            r => unreachable!("pool: {r:?}"),
        }
    }
}

/// A spawned write task (daemon): `transport.send(m)` awaited (`async_rw.rs:233-245`): the mutex
/// (taken at once if free, else after the grant), the frame, the guard dropped; then the task
/// ends. A notification's task (`progress`) is seen by the serve loop as a finished task of
/// `send_task_set` (`service.rs:1501-1514`); a response's (`response_send_tasks`,
/// `service.rs:1516-1520`) just ends, since that arm does nothing (`service.rs:1556-1560`).
pub(crate) fn write_task(wire: ThreadId, lock: ThreadId, id: u32, m: ToClient, progress: bool) {
    let me = thread::current().id();
    let req = m.req();
    if lock_op(lock, Relay::Acquire { id, from: me, reply: me, tag: GRANT }) != (Relay::Acquired { now: true }) {
        recv_tagged_msg_block::<_, Relay>(move |tid, tag| tid == lock && tag == Some(GRANT));
    }
    send_tagged_msg(wire, REQ, Relay::Write { m, from: me });
    recv_tagged_msg_block::<_, Relay>(move |tid, tag| tid == wire && tag == Some(WRITTEN));
    lock_op(lock, Relay::Release { id, from: me });
    if progress {
        send_tagged_msg(wire, ARM, Relay::TaskDone { req });
    }
}

/// A daemon named `name` running `f`; its id.
pub(crate) fn daemon(name: &str, f: impl FnOnce() + Send + 'static) -> ThreadId {
    thread::Builder::new().name(name.to_string()).spawn_daemon(f).unwrap().thread().id()
}

/// What the serve loop of both programs shares: the processes it talks to and the id counter.
pub(crate) struct Loop {
    pub(crate) me: ThreadId,
    pub(crate) wire: ThreadId,
    pub(crate) handlers: [ThreadId; 2],
    pub(crate) lock: ThreadId,
    pub(crate) pool: ThreadId,
    /// The next waiter id of the mutex.
    pub(crate) next_id: u32,
}

impl Loop {
    pub(crate) fn new(wire: ThreadId, handlers: [ThreadId; 2]) -> Loop {
        let me = thread::current().id();
        let lock = daemon("mutex", mutex);
        let pool = daemon("pool", move || pool(me));
        Loop { me, wire, handlers, lock, pool, next_id: 0 }
    }

    pub(crate) fn req(&self, m: Relay) {
        send_tagged_msg(self.wire, REQ, m);
    }

    /// Waits in the `select!` for the next arm.
    pub(crate) fn select(&self) -> Relay {
        self.req(Relay::Ready);
        let wire = self.wire;
        recv_tagged_msg_block(move |tid, tag| tid == wire && tag == Some(EVENT))
    }

    /// `local_ct_pool.insert` (`service.rs:1652`).
    pub(crate) fn pool_insert(&self, req: u32) {
        send_tagged_msg(self.pool, OBJ, Relay::PoolInsert { req });
        let pool = self.pool;
        recv_tagged_msg_block::<_, Relay>(move |tid, tag| tid == pool && tag == Some(REPLY));
    }

    /// `local_ct_pool.remove`; whether it was there.
    pub(crate) fn pool_remove(&self, req: u32) -> bool {
        send_tagged_msg(self.pool, OBJ, Relay::PoolRemove { req });
        let pool = self.pool;
        recv_tagged_msg_block::<_, Relay>(move |tid, tag| tid == pool && tag == Some(REPLY))
            == Relay::Found(true)
    }

    /// A spawned write task for `m`.
    pub(crate) fn spawn_write(&mut self, m: ToClient, progress: bool) {
        let (wire, lock, id) = (self.wire, self.lock, self.next_id);
        self.next_id += 1;
        self.req(Relay::Pending);
        daemon("write", move || write_task(wire, lock, id, m, progress));
    }

    /// The responder of the tool's progress answers (`service.rs:1546`).
    pub(crate) fn resume(&self, req: u32) {
        send_tagged_msg(self.handlers[req as usize - 1], RESUME, Relay::Resume);
    }

    /// The loop ends (`QuitReason::Closed`): a tool still waiting for an answer ends too.
    pub(crate) fn end(&self) {
        for h in self.handlers {
            send_tagged_msg(h, RESUME, Relay::End);
        }
    }
}

/// The serve loop (`service.rs:1447-1879`), one step per arm of its `select!`.
fn serve(wire: ThreadId, handlers: [ThreadId; 2]) {
    let mut s = Loop::new(wire, handlers);
    loop {
        match s.select() {
            // A request (`service.rs:1623-1692`): the association check passes for a server
            // (`service.rs:213-220`); `local_ct_pool.insert`; the tool's task spawned.
            Relay::Msg(ToServer::Call { req, token }) => {
                s.pool_insert(req);
                s.req(Relay::Work { req, token });
            }
            // A tool's progress (`service.rs:1602-1621`): not a cancellation, so spawned as a
            // write task of `send_task_set`, with no check of the request's state.
            Relay::Msg(ToServer::Prog { token, value, .. }) => {
                s.spawn_write(ToClient::Progress { token, value }, true);
            }
            // The tool's response (`service.rs:1562-1580`): dispatched only if the request is in
            // the pool (then its token is cancelled: the tool has returned, so nobody observes
            // it), otherwise dropped.
            Relay::Msg(ToServer::Done { req }) => {
                if s.pool_remove(req) {
                    s.spawn_write(ToClient::Result { req }, false);
                }
            }
            // `notifications/cancelled` (`service.rs:1694-1726`): `invalidate_response_cache` is
            // `async {}` for a server; `peer_cancels_subscriptions` is false (`service.rs:180-182`);
            // `local_ct_pool.remove`, and if it was there, `ct.cancel()`. The notification is then
            // handed to the service (`service.rs:1766-1789`), which does nothing observable here.
            Relay::Msg(ToServer::Cancelled { req }) => {
                if s.pool_remove(req) {
                    s.req(Relay::Stop { req });
                }
            }
            // EOF (`service.rs:1485-1492`); the shutdown drain (`service.rs:1829-1883`) is not
            // modelled (harness rule).
            Relay::Msg(ToServer::Shutdown) => {
                s.end();
                return;
            }
            // (A response's write finishing, `service.rs:1556-1560`, does nothing; not sent.)
            // A notification's write finished (`service.rs:1536-1555`): its responder answers.
            Relay::TaskDone { req } => s.resume(req),
            r => unreachable!("serve: {r:?}"),
        }
    }
}

/// The program: the wire (`server`) spawns the tools, the client and the serve loop, and runs.
pub(crate) fn launch(args: Args, serve: fn(ThreadId, [ThreadId; 2])) {
    let named = |n: &str| thread::Builder::new().name(n.to_string());
    let srv = named("server")
        .spawn(move || {
            let me = thread::current().id();
            let named = |n: &str| thread::Builder::new().name(n.to_string());
            let h1 = named("handler1").spawn(move || handler(me, true, args.h1)).unwrap().thread().id();
            let h2 = named("handler2").spawn(move || handler(me, false, args.h2)).unwrap().thread().id();
            let cl = named("client").spawn(move || client(me, args.cancel2)).unwrap().thread().id();
            let sv = named("serve").spawn(move || serve(me, [h1, h2])).unwrap().thread().id();
            wire(sv, cl, [h1, h2]);
        })
        .unwrap();
    srv.join().unwrap();
}

/// The entry point of the threaded program (`implt`).
pub fn entry(args: Args) -> Entry {
    Arc::new(move || launch(args, serve))
}
