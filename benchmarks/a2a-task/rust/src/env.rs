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

use std::sync::Arc;

use traceforge::program::Entry;
use traceforge::thread::{self, ThreadId};
use traceforge::{nondet, recv_msg_block, send_msg};

use crate::msgs::*;

/// The client: streaming send, cancel while the stream is open, read, (re-cancel run) a second
/// cancel, then `Shutdown` to the server. It ends.
pub fn client(server: ThreadId, recancel: bool) {
    send_msg(server, ToServer::SendStream { req: 1 });
    // Stream items of request 1 until `Status{1,t1,Working,false}`; a Task item before it is consumed.
    loop {
        if let ToClient::Status { req: 1, state: State::Working, fin: false, .. } = recv_msg_block::<ToClient>() {
            break;
        }
    }
    // Cancel while the stream is open; then consume stream items until both the cancel's answer
    // and the end of the stream have arrived (any message for request 2 is its answer).
    send_msg(server, ToServer::Cancel { req: 2, task: t1() });
    let (mut answered, mut closed) = (false, false);
    while !(answered && closed) {
        let m = recv_msg_block::<ToClient>();
        match m {
            ToClient::StreamClose { req: 1 } => closed = true,
            _ if m.req() == 2 => answered = true,
            _ => {}
        }
    }
    send_msg(server, ToServer::Get { req: 3, task: t1() });
    while recv_msg_block::<ToClient>().req() != 3 {}
    if recancel {
        send_msg(server, ToServer::Cancel { req: 4, task: t1() });
        while recv_msg_block::<ToClient>().req() != 4 {}
    }
    send_msg(server, ToServer::Shutdown);
}

/// The executor. On `Exec`: in variant A report `Submit` first; report `Working`; then
/// exec branch (a) finish at once (`Completed`) or exec branch (b) keep working (an execution that
/// has not returned). On every `CancelCb`: exactly one `Canceled`, whatever it sent before (a
/// `Completed` already sent stays in flight). On `Shutdown`: it ends.
pub fn exec(server: ThreadId, task_first: bool) {
    loop {
        match recv_msg_block::<ToExec>() {
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
            let ex = named("exec").spawn(move || exec(me, args.task_first)).unwrap().thread().id();
            let cl = named("client").spawn(move || client(me, args.recancel)).unwrap().thread().id();
            server(cl, ex);
        })
        .unwrap();
    srv.join().unwrap();
}

/// The delegating executor of server A (delegation scenario). It is A's executor and, towards
/// server B, a client with requests 1 (streaming send) and 2 (cancel). Event script:
/// * `Exec{t}` from A: in variant A `Submit{t}` to A; `Working{t}` to A; `SendStream{1}` to B.
/// * from B, stream 1: `TaskItem` and non-final `Status`: nothing. A final `Status{..,c,true}`:
///   `c` is remembered. `Err{1,..}`: nothing (not relayed). `StreamClose{1}`: if the remembered
///   final state is `Completed`, `Completed{t}` to A (once); then the cancel check below.
/// * `CancelCb{t}` from A (at most one): `Cancel{2,t1}` to B; then the cancel check.
/// * from B, the answer to request 2 (any message with req 2): remembered; then the cancel check.
/// * cancel check: once a callback was received, B's answer to request 2 has arrived and B's
///   stream has closed, exactly one `Canceled{t}` to A.
/// * `Shutdown` from A: `Shutdown` to B, and it ends.
pub fn exec_a(a: ThreadId, b: ThreadId, task_first: bool) {
    let (mut b_final, mut b_closed) = (None::<State>, false);
    let (mut cb, mut answered, mut completed_sent, mut canceled_sent) = (false, false, false, false);
    loop {
        let m = recv_msg_block::<ToClient>();
        match m {
            ToClient::Exec { task } => {
                if task_first {
                    send_msg(a, ToServer::Submit { task: task.clone() });
                }
                send_msg(a, ToServer::Working { task });
                send_msg(b, ToServer::SendStream { req: 1 });
            }
            ToClient::CancelCb { .. } => {
                assert!(!cb, "execA: one cancel callback");
                cb = true;
                send_msg(b, ToServer::Cancel { req: 2, task: t1() });
            }
            ToClient::Shutdown => {
                send_msg(b, ToServer::Shutdown);
                return;
            }
            ToClient::Status { req: 1, state, fin: true, .. } => b_final = Some(state),
            ToClient::TaskItem { req: 1, .. } | ToClient::Status { req: 1, .. } | ToClient::Err { req: 1, .. } => {}
            ToClient::StreamClose { req: 1 } => {
                b_closed = true;
                if b_final == Some(State::Completed) && !completed_sent {
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
/// "serverB", then runs [`exec_a`]; serverB spawns "execB" (the experiment-1 executor), then runs
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
                            let eb = named("execB").spawn(move || exec(b, args.task_first)).unwrap().thread().id();
                            server(ea, eb);
                        })
                        .unwrap()
                        .thread()
                        .id();
                    exec_a(a, b, args.task_first);
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
