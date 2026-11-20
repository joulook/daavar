//! The environment shared by the specification program and the rewrites: the scripted client, the
//! two scripted tool handlers, and the launcher that spawns them around a given server. Only the
//! server differs between programs; this file is the reference the Java twin mirrors event for
//! event (same messages, same order, same choice points).
//!
//! Observation: `client` and `server` are observed with every send and receive (no annotation);
//! `handler1` and `handler2` are not observed. Termination (no daemons, no messages from the main
//! thread): the client's last send is `Shutdown` to the server; on it the server sends `Shutdown`
//! to both handlers, sends nothing else, and ends; each handler ends on `Shutdown`. Messages may be
//! left unread at the end (a late handler message at the server, or a message the server wrote
//! before the cancel reached it, after the client stopped reading). Thread ids: the server spawns
//! the handlers and the client and knows all three; each receives the server's id at its spawn.

use std::sync::Arc;

use traceforge::program::Entry;
use traceforge::thread::{self, ThreadId};
use traceforge::{nondet, recv_msg_block, send_msg, Nondet};

use crate::msgs::*;

/// The client: the host whose model decides, modelled as a nondeterministic procedure. It sends
/// `Call{1,"t1"}`, then chooses whether to send `Call{2,"t2"}`. Before reading anything, and after
/// each message it reads, it may cancel request 1 while that request has no response yet, at most
/// once; with `cancel2`, likewise request 2 if it was sent. It stops once every request it sent has
/// a response or its cancel, then sends `Shutdown` to the server. It waits for nothing more for a
/// cancelled request (the server must not send anything for it once the cancel arrives); a message
/// arriving before the stop is consumed and has no effect.
pub fn client(server: ThreadId, cancel2: bool) {
    send_msg(server, ToServer::Call { req: 1, token: token(1) });
    let sent2 = nondet();
    if sent2 {
        send_msg(server, ToServer::Call { req: 2, token: token(2) });
    }
    let sent = [true, sent2];
    let cancellable = [true, sent2 && cancel2];
    let (mut answered, mut cancelled) = ([false; 2], [false; 2]);
    let maybe_cancel = |answered: &[bool; 2], cancelled: &mut [bool; 2]| {
        for i in 0..2 {
            if cancellable[i] && !answered[i] && !cancelled[i] && nondet() {
                send_msg(server, ToServer::Cancelled { req: i as u32 + 1 });
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
    send_msg(server, ToServer::Shutdown);
}

/// A tool handler (`handler1` serves request 1, `handler2` request 2). On `Work{r,t}`: `Prog{r,t,1}`,
/// then branch (a) `Done{r}`, (b) `Prog{r,t,2}` and `Done{r}`, or (c, handler1 only) nothing more
/// (it keeps working). `Stop`: nothing (a handler in (c) stays silent). `Shutdown`: it ends.
pub fn handler(server: ThreadId, first: bool, branch: Branch) {
    loop {
        match recv_msg_block::<ToHandler>() {
            ToHandler::Work { req, token } => {
                send_msg(server, ToServer::Prog { req, token: token.clone(), value: 1 });
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
                    Branch::A => send_msg(server, ToServer::Done { req }),
                    Branch::B => {
                        send_msg(server, ToServer::Prog { req, token, value: 2 });
                        send_msg(server, ToServer::Done { req });
                    }
                    _ => {}
                }
            }
            ToHandler::Stop { .. } => {}
            ToHandler::Shutdown => return,
        }
    }
}

/// The server's signature: `server(client, handler1, handler2, args)`; it must end as described in
/// the module comment.
pub type Server = fn(ThreadId, ThreadId, ThreadId, Args);

/// Spawns `server`; the server's thread first spawns `handler1`, `handler2` and `client`, then runs
/// `server(client, handler1, handler2, args)`. The main thread waits for the server.
pub fn launch(args: Args, server: Server) {
    let named = |n: &str| thread::Builder::new().name(n.to_string());
    let srv = named("server")
        .spawn(move || {
            let me = thread::current().id();
            let named = |n: &str| thread::Builder::new().name(n.to_string());
            let h1 = named("handler1").spawn(move || handler(me, true, args.h1)).unwrap().thread().id();
            let h2 = named("handler2").spawn(move || handler(me, false, args.h2)).unwrap().thread().id();
            let cl = named("client").spawn(move || client(me, args.cancel2)).unwrap().thread().id();
            server(cl, h1, h2, args);
        })
        .unwrap();
    srv.join().unwrap();
}

/// An entry point running `launch(args, server)`.
pub fn entry(args: Args, server: Server) -> Entry {
    Arc::new(move || launch(args, server))
}
