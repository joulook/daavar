//! The environment shared by the specification program and the rewrites: the scripted client,
//! the scripted executor, and the launcher that spawns them around a given server. Only the
//! server differs between programs; this file is the reference the Java twin mirrors event for
//! event (same messages, same order, same choice points).
//!
//! Observation: `client` and `server` are observed with every send and receive (no annotation),
//! `exec` is not observed. Termination (no daemons, no messages from the main thread): the client's
//! last send is `Shutdown` to the server; the server, once it has received `Shutdown` and every
//! cancel callback it sent has been answered by a `Canceled`, sends `Shutdown` to the executor and
//! ends; the executor ends on `Shutdown`. Fifo server -> executor puts every callback before that
//! `Shutdown`, and fifo executor -> server puts every executor report before the last `Canceled`
//! (with no callback, the stream closed only after `Completed` was read), so no message is left
//! unread. Thread ids: the server spawns the executor and the client and
//! knows both; each receives the server's id at its spawn (no setup messages).
//!
//! Delegation scenario ([`launch2`]): the same rules per server; the observed roles are `client`,
//! `serverA` and `serverB`; the delegating executor [`exec_a`] is not observed.

use std::sync::Arc;

use traceforge::program::Entry;
use traceforge::thread::{self, ThreadId};
use traceforge::{nondet, recv_msg_block, send_msg};

use crate::msgs::*;

/// The client: streaming send, cancel while the stream is open, read, (re-cancel run) a second
/// cancel, then `Shutdown` to the server. It ends.
///
/// It reads the stream of request 1 until the stream closes. On the first
/// `Status{1,t1,Working,false}` while the stream is open it cancels (request 2) and then also
/// waits for the cancel's answer (any message for request 2). A stream that carried a Message
/// (no task) is followed by the read only; otherwise the read is followed, in the re-cancel run, by
/// a second cancel.
pub fn client(server: ThreadId, recancel: bool) {
    send_msg(server, ToServer::SendStream { req: 1 });
    let (mut canc, mut answered, mut closed, mut message) = (false, false, false, false);
    while !(closed && (!canc || answered)) {
        let m = recv_msg_block::<ToClient>();
        match m {
            ToClient::Status { req: 1, state: State::Working, fin: false, .. } if !canc && !closed => {
                send_msg(server, ToServer::Cancel { req: 2, task: t1() });
                canc = true;
            }
            ToClient::MessageItem { req: 1 } => message = true,
            ToClient::StreamClose { req: 1 } => closed = true,
            _ if m.req() == 2 => answered = true,
            _ => {}
        }
    }
    send_msg(server, ToServer::Get { req: 3, task: t1() });
    while recv_msg_block::<ToClient>().req() != 3 {}
    if recancel && !message {
        send_msg(server, ToServer::Cancel { req: 4, task: t1() });
        while recv_msg_block::<ToClient>().req() != 4 {}
    }
    send_msg(server, ToServer::Shutdown);
}

/// The agent's answer to `Exec`, as a nondeterministic procedure (`call_llm`): a plain Message
/// (`Reply`), or a task: first report `Submit` (the Task object first) or `Working` (a status
/// first); then `Completed` at once, or `Working` and then `Completed` or nothing more (it keeps
/// working).
pub fn call_llm(server: ThreadId, task: String) {
    call_llm_fixed(server, task, Fixed::default())
}

/// Choices of [`call_llm`] fixed in advance, used only to split an exploration into parts: the
/// Message (`Some(true)`) or a task (`Some(false)`); the first report `Submit` (`Some(true)`) or
/// `Working`; `Completed` at once (`Some(true)`) or `Working` first.
#[derive(Clone, Copy, Default)]
pub struct Fixed {
    pub message: Option<bool>,
    pub submit: Option<bool>,
    pub complete: Option<bool>,
}

/// [`call_llm`] with some choices fixed (see [`Fixed`]).
pub fn call_llm_fixed(server: ThreadId, task: String, fx: Fixed) {
    if fx.message.unwrap_or_else(nondet) {
        send_msg(server, ToServer::Reply { task });
        return;
    }
    if fx.submit.unwrap_or_else(nondet) {
        send_msg(server, ToServer::Submit { task: task.clone() });
    } else {
        send_msg(server, ToServer::Working { task: task.clone() });
    }
    if fx.complete.unwrap_or_else(nondet) {
        send_msg(server, ToServer::Completed { task });
    } else {
        send_msg(server, ToServer::Working { task: task.clone() });
        if nondet() {
            send_msg(server, ToServer::Completed { task });
        }
    }
}

/// The executor. With `llm`, on `Exec` it runs [`call_llm`]. Otherwise (the former fixed
/// scripts): in variant A report `Submit` first; report `Working`; then exec branch (a) finish at
/// once (`Completed`) or exec branch (b) keep working (an execution that has not returned). On
/// every `CancelCb`: exactly one `Canceled`, whatever it sent before (a `Completed` already sent
/// stays in flight). On `Shutdown`: it ends.
pub fn exec(server: ThreadId, task_first: bool, llm: bool) {
    exec_fixed(server, task_first, llm, Fixed::default())
}

/// [`exec`] with some choices of [`call_llm`] fixed (see [`Fixed`]).
pub fn exec_fixed(server: ThreadId, task_first: bool, llm: bool, fx: Fixed) {
    loop {
        match recv_msg_block::<ToExec>() {
            ToExec::Exec { task } if llm => call_llm_fixed(server, task, fx),
            ToExec::Exec { task } => {
                if task_first {
                    send_msg(server, ToServer::Submit { task: task.clone() });
                }
                send_msg(server, ToServer::Working { task: task.clone() });
                if nondet() {
                    send_msg(server, ToServer::Completed { task });
                }
            }
            ToExec::CancelCb { task } => send_msg(server, ToServer::Canceled { task }),
            ToExec::Shutdown => return,
            m => panic!("exec: unexpected {m:?}"),
        }
    }
}

/// Spawns `server`; the server's thread first spawns `exec` and `client`, then runs
/// `server(client, exec)`, which must end as described above. The main thread waits for the server.
pub fn launch(args: Args, server: fn(ThreadId, ThreadId)) {
    let named = |n: &str| thread::Builder::new().name(n.to_string());
    let srv = named("server")
        .spawn(move || {
            let me = thread::current().id();
            let named = |n: &str| thread::Builder::new().name(n.to_string());
            let ex = named("exec").spawn(move || exec(me, args.task_first, args.llm)).unwrap().thread().id();
            let cl = named("client").spawn(move || client(me, args.recancel)).unwrap().thread().id();
            server(cl, ex);
        })
        .unwrap();
    srv.join().unwrap();
}

/// The delegating executor of server A (delegation scenario). It is A's executor and, towards
/// server B, a client with requests 1 (streaming send) and 2 (cancel). Event script:
/// * `Exec{t}` from A: with `llm`, first a choice: answer locally ([`call_llm`] towards A; then
///   every `CancelCb` from A is answered by one `Canceled` to A, and B is never asked), or delegate
///   as follows. Delegating: in variant A `Submit{t}` to A; `Working{t}` to A; `SendStream{1}` to B.
/// * from B, stream 1, `MessageItem{1}` (B answered without a task): remembered; at
///   `StreamClose{1}` it counts as B's completion.
/// * from B, stream 1: `TaskItem` and non-final `Status`: nothing. A final `Status{..,c,true}`:
///   `c` is remembered. `Err{1,..}`: nothing (not relayed). `StreamClose{1}`: if the remembered
///   final state is `Completed`, `Completed{t}` to A (once); then the cancel check below.
/// * `CancelCb{t}` from A (at most one): `Cancel{2,t1}` to B; then the cancel check. With `gated`,
///   the `Cancel{2,t1}` is sent only once B's `Status{1,t1,Working,false}` has arrived: a callback
///   received earlier is remembered and the cancel goes out when that status arrives; if B's stream
///   closes first (before or after the callback), no downstream cancel is sent and the callback
///   counts as answered.
/// * from B, the answer to request 2 (any message with req 2): remembered; then the cancel check.
/// * cancel check: once a callback was received, B's answer to request 2 has arrived and B's
///   stream has closed, exactly one `Canceled{t}` to A.
/// * `Shutdown` from A: `Shutdown` to B, and it ends.
pub fn exec_a(a: ThreadId, b: ThreadId, task_first: bool, gated: bool, llm: bool, split: u8) {
    let (mut b_final, mut b_closed) = (None::<State>, false);
    let (mut b_working, mut cancel_due, mut b_message, mut local) = (false, false, false, false);
    let (mut cb, mut answered, mut completed_sent, mut canceled_sent) = (false, false, false, false);
    loop {
        let m = recv_msg_block::<ToClient>();
        match m {
            ToClient::Exec { task } if llm && (split == 1 || (split == 0 && nondet())) => {
                // The agent answers by itself, without delegating; B is never asked.
                local = true;
                call_llm(a, task);
            }
            ToClient::Exec { task } => {
                if task_first {
                    send_msg(a, ToServer::Submit { task: task.clone() });
                }
                send_msg(a, ToServer::Working { task });
                send_msg(b, ToServer::SendStream { req: 1 });
            }
            ToClient::CancelCb { task } if local => send_msg(a, ToServer::Canceled { task }),
            ToClient::CancelCb { .. } => {
                assert!(!cb, "execA: one cancel callback");
                cb = true;
                if !gated || b_working {
                    send_msg(b, ToServer::Cancel { req: 2, task: t1() });
                } else if b_closed {
                    // Gated, and B's stream already closed without a working status: nothing is left
                    // to cancel downstream; the callback is settled without a downstream cancel.
                    answered = true;
                } else {
                    cancel_due = true;
                }
            }
            ToClient::Shutdown => {
                send_msg(b, ToServer::Shutdown);
                return;
            }
            ToClient::Status { req: 1, state, fin: true, .. } => b_final = Some(state),
            ToClient::Status { req: 1, state: State::Working, fin: false, .. } => {
                b_working = true;
                if cancel_due {
                    cancel_due = false;
                    send_msg(b, ToServer::Cancel { req: 2, task: t1() });
                }
            }
            ToClient::TaskItem { req: 1, .. } | ToClient::Status { req: 1, .. } | ToClient::Err { req: 1, .. } => {}
            ToClient::MessageItem { req: 1 } => b_message = true,
            ToClient::StreamClose { req: 1 } => {
                b_closed = true;
                if cancel_due {
                    // Gated: B's stream closed before any working status (B answered with a Message,
                    // or finished without publishing one), so there is nothing left to cancel
                    // downstream; the callback is settled without a downstream cancel.
                    cancel_due = false;
                    answered = true;
                }
                if (b_final == Some(State::Completed) || b_message) && !completed_sent {
                    completed_sent = true;
                    send_msg(a, ToServer::Completed { task: t1() });
                }
            }
            _ if m.req() == 2 => answered = true,
            m => panic!("execA: unexpected {m:?}"),
        }
        if cb && answered && b_closed && !canceled_sent {
            canceled_sent = true;
            send_msg(a, ToServer::Canceled { task: t1() });
        }
    }
}

/// The delegation scenario's launcher. Spawn tree (thread names in quotes): main spawns
/// "serverA"; serverA spawns "execA" and "client", then runs `server(client, execA)`; execA spawns
/// "serverB", then runs [`exec_a`]; serverB spawns "execB" (the one-server executor, [`exec`]), then runs
/// `server(execA, execB)`. Both servers run the same `server` function. The main thread waits for
/// serverA. Shutdown chain: client -> serverA -> execA -> serverB -> execB.
pub fn launch2(args: Args, server: fn(ThreadId, ThreadId)) {
    let named = |n: &str| thread::Builder::new().name(n.to_string());
    let srv_a = named("serverA")
        .spawn(move || {
            let a = thread::current().id();
            let named = |n: &str| thread::Builder::new().name(n.to_string());
            let ea = named("execA")
                .spawn(move || {
                    let ea = thread::current().id();
                    let named = |n: &str| thread::Builder::new().name(n.to_string());
                    let b = named("serverB")
                        .spawn(move || {
                            let b = thread::current().id();
                            let named = |n: &str| thread::Builder::new().name(n.to_string());
                            let opt = |v: u8| match v {
                                1 => Some(true),
                                2 => Some(false),
                                _ => None,
                            };
                            let fx = Fixed {
                                message: match args.split {
                                    3 => Some(true),
                                    4 => Some(false),
                                    _ => None,
                                },
                                submit: opt(args.b_kind),
                                complete: opt(args.b_cont),
                            };
                            let eb = named("execB").spawn(move || exec_fixed(b, args.task_first, args.llm, fx)).unwrap().thread().id();
                            server(ea, eb);
                        })
                        .unwrap()
                        .thread()
                        .id();
                    exec_a(a, b, args.task_first, args.gated, args.llm, args.split);
                })
                .unwrap()
                .thread()
                .id();
            let cl = named("client").spawn(move || client(a, args.recancel)).unwrap().thread().id();
            server(cl, ea);
        })
        .unwrap();
    srv_a.join().unwrap();
}

/// An entry point running `run(args)`.
pub fn entry(args: Args, run: fn(Args)) -> Entry {
    Arc::new(move || run(args))
}
