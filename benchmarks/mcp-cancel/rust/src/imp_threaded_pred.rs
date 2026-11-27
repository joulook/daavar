//! `imp_threaded.rs` (selector `implt`) with its object processes rebuilt on `traceforge::sync`
//! (selector `impltp`; plan/case-studies/mcp/fix/PREDICATED-NOTE.md). Everything visible, and every
//! `Relay` the wire sends or receives, is `imp_threaded.rs`'s: the client, the tools, the wire, the
//! launch and the serve loop's arms are reused or copied unchanged. What changes:
//! * the writer mutex is `traceforge::sync::Mutex` (a daemon whose receive predicate depends on its
//!   state, `sync/mutex.rs:45`, `:73`: free, it takes only a `Lock`; held, only an `Unlock`).
//!   tokio's FIFO among queued waiters (`imp_threaded.rs` `mutex`) becomes the order in which the
//!   daemon takes the `Lock`s; in both, a grant order is the order in which the mutex takes the
//!   waiters' requests, any order of requests already sent, so the grant orders are the same. A
//!   write task's `lock().await` blocks it until the grant, as before; `Acquired`, `Release` and
//!   their `Ack`s are gone (the guard's drop is the `Unlock`).
//! * `local_ct_pool` is two `traceforge::sync::atomic::AtomicBool`s, one per request: `insert` a
//!   store of `true`, `remove` a compare-exchange `true -> false`, whose success is `Some`.
//! The fixed program (`imp_threaded_fixed_pred.rs`) keeps a hand-written writer mutex, since its
//! serve loop polls `lock()` without blocking, withdraws it, and gets the grant as an arm event,
//! which `sync::Mutex` does not provide; that mutex (`mutex_pred`) gets `sync::Mutex`'s
//! state-dependent predicate for the write tasks' requests.

use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

use traceforge::program::Entry;
use traceforge::sync::atomic::AtomicBool;
use traceforge::sync::Mutex;
use traceforge::thread::{self, ThreadId};
use traceforge::{recv_tagged_msg_block, send_tagged_msg};

use std::collections::VecDeque;

use crate::imp_threaded::{
    daemon, launch, lock_op, Relay, ARM, EVENT, GRANT, LOCK, LOCKED, REQ, RESUME, WRITTEN,
};
use crate::msgs::*;

/// The writer mutex: `traceforge::sync::Mutex` (`impltp`) or `mutex_pred` (`impltfp`, whose serve
/// loop also takes it).
pub(crate) enum Writer {
    Sync(Arc<Mutex<()>>),
    Pred(ThreadId),
}

/// The tag of a write task's `Acquire` to `mutex_pred` (the serve loop's, and every `Release` and
/// `Withdraw`, keep `LOCK`). Internal to the mutex and its callers; the wire never sees it.
const ACQ: u32 = 12;

/// The writer mutex of the fixed program (daemon): `imp_threaded.rs`'s `mutex` (tokio's FIFO
/// `Mutex`, the same operations and replies) with `sync::Mutex`'s state-dependent predicate
/// (`sync/mutex.rs:45`, `:73`) for the write tasks: a write task's `Acquire` (tag `ACQ`) is taken
/// only while the lock is free and no waiter is queued, so it is granted at once. A write task
/// blocks on its grant either way, and taking its request later changes no observation (the
/// exports of all seven pairs are identical to `impltf`'s, PREDICATED-NOTE.md). The serve loop's
/// `Acquire` (its gated future's first poll, which must return at once), and every `Release` and
/// `Withdraw` (tag `LOCK`), are taken at any time, as before.
fn mutex_pred() {
    let mut queue: VecDeque<(u32, ThreadId, u32)> = VecDeque::new();
    let mut holder: Option<u32> = None;
    loop {
        let free = holder.is_none() && queue.is_empty();
        match recv_tagged_msg_block::<_, Relay>(move |_, tag| tag == Some(LOCK) || (free && tag == Some(ACQ))) {
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

/// A spawned write task on `mutex_pred`: `imp_threaded.rs`'s `write_task`, its `Acquire` sent with
/// tag `ACQ`.
fn write_task_pred(wire: ThreadId, lock: ThreadId, id: u32, m: ToClient, progress: bool) {
    let me = thread::current().id();
    let req = m.req();
    send_tagged_msg(lock, ACQ, Relay::Acquire { id, from: me, reply: me, tag: GRANT });
    let r: Relay = recv_tagged_msg_block(move |tid, tag| tid == lock && tag == Some(LOCKED));
    if r != (Relay::Acquired { now: true }) {
        recv_tagged_msg_block::<_, Relay>(move |tid, tag| tid == lock && tag == Some(GRANT));
    }
    send_tagged_msg(wire, REQ, Relay::Write { m, from: me });
    recv_tagged_msg_block::<_, Relay>(move |tid, tag| tid == wire && tag == Some(WRITTEN));
    lock_op(lock, Relay::Release { id, from: me });
    if progress {
        send_tagged_msg(wire, ARM, Relay::TaskDone { req });
    }
}

/// A spawned write task on `sync::Mutex`: `transport.send(m)` awaited (`async_rw.rs:233-245`): the
/// lock, the frame, the guard dropped; then, for a notification, the finished task is reported.
fn write_task_sync(wire: ThreadId, lock: Arc<Mutex<()>>, m: ToClient, progress: bool) {
    let me = thread::current().id();
    let req = m.req();
    let guard = lock.blocking_lock();
    send_tagged_msg(wire, REQ, Relay::Write { m, from: me });
    recv_tagged_msg_block::<_, Relay>(move |tid, tag| tid == wire && tag == Some(WRITTEN));
    drop(guard);
    if progress {
        send_tagged_msg(wire, ARM, Relay::TaskDone { req });
    }
}

/// `imp_threaded.rs`'s `Loop` with the pool as atomics and the writer mutex as `Writer`; the same
/// method names, so the serve loops are copied unchanged.
pub(crate) struct Loop {
    pub(crate) me: ThreadId,
    pub(crate) wire: ThreadId,
    pub(crate) handlers: [ThreadId; 2],
    /// The hand-written writer mutex (fixed program only; `Writer::Pred`).
    pub(crate) lock: ThreadId,
    writer: Writer,
    pool: [AtomicBool; 2],
    pub(crate) next_id: u32,
}

impl Loop {
    /// `sync_mutex`: `traceforge::sync::Mutex` (`impltp`); otherwise `mutex_pred` (`impltfp`).
    pub(crate) fn new(wire: ThreadId, handlers: [ThreadId; 2], sync_mutex: bool) -> Loop {
        let me = thread::current().id();
        let (writer, lock) = if sync_mutex {
            (Writer::Sync(Arc::new(Mutex::new(()))), me)
        } else {
            let lock = daemon("mutex", mutex_pred);
            (Writer::Pred(lock), lock)
        };
        let pool = [AtomicBool::new(false), AtomicBool::new(false)];
        Loop { me, wire, handlers, lock, writer, pool, next_id: 0 }
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
        self.pool[req as usize - 1].store(true, SeqCst);
    }

    /// `local_ct_pool.remove`; whether it was there.
    pub(crate) fn pool_remove(&self, req: u32) -> bool {
        self.pool[req as usize - 1].compare_exchange(true, false, SeqCst, SeqCst).is_ok()
    }

    /// A spawned write task for `m`.
    pub(crate) fn spawn_write(&mut self, m: ToClient, progress: bool) {
        let (wire, id) = (self.wire, self.next_id);
        self.next_id += 1;
        self.req(Relay::Pending);
        match &self.writer {
            Writer::Sync(lock) => {
                let lock = lock.clone();
                daemon("write", move || write_task_sync(wire, lock, m, progress));
            }
            Writer::Pred(lock) => {
                let lock = *lock;
                daemon("write", move || write_task_pred(wire, lock, id, m, progress));
            }
        }
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

/// The serve loop: `imp_threaded.rs`'s `serve`, unchanged but for `Loop`.
fn serve(wire: ThreadId, handlers: [ThreadId; 2]) {
    let mut s = Loop::new(wire, handlers, true);
    loop {
        match s.select() {
            Relay::Msg(ToServer::Call { req, token }) => {
                s.pool_insert(req);
                s.req(Relay::Work { req, token });
            }
            Relay::Msg(ToServer::Prog { token, value, .. }) => {
                s.spawn_write(ToClient::Progress { token, value }, true);
            }
            Relay::Msg(ToServer::Done { req }) => {
                if s.pool_remove(req) {
                    s.spawn_write(ToClient::Result { req }, false);
                }
            }
            Relay::Msg(ToServer::Cancelled { req }) => {
                if s.pool_remove(req) {
                    s.req(Relay::Stop { req });
                }
            }
            Relay::Msg(ToServer::Shutdown) => {
                s.end();
                return;
            }
            Relay::TaskDone { req } => s.resume(req),
            r => unreachable!("serve: {r:?}"),
        }
    }
}

/// The entry point (`impltp`).
pub fn entry(args: Args) -> Entry {
    Arc::new(move || launch(args, serve))
}
