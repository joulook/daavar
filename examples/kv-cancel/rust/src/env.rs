//! The environment shared by every program: the scripted client, the scripted disk, and the
//! launcher that spawns them around a given server. Only the server differs between programs; the
//! Java twin mirrors this file event for event (same messages, same order, same choice points).
//!
//! Observation: `client` and `server` are observed with every send and receive; `disk` is not.
//! Termination (no daemons, no messages from the main thread, nothing left unread): the client's
//! last send is `Shutdown`; the server then waits for an outstanding `Written`, sends `Shutdown`
//! to the disk and ends; the disk ends on `Shutdown`.

use std::sync::Arc;

use traceforge::program::Entry;
use traceforge::thread::{self, ThreadId};
use traceforge::{nondet, recv_msg_block, send_msg};

use crate::msgs::*;

/// The client: `Put{1,"k",1}`; then, by a nondeterministic choice, `Cancel{1}` at once or no
/// cancel. It receives until the Put has its answer (`PutOk` or `Canceled`) and, if it cancelled,
/// the cancel has its answer (`CancelOk` or `CancelErr`); then it sends `Shutdown`.
pub fn client(server: ThreadId) {
    send_msg(server, ToServer::Put { req: 1, key: key(), val: 1 });
    let cancel = nondet();
    if cancel {
        send_msg(server, ToServer::Cancel { req: 1 });
    }
    let (mut put_answered, mut cancel_answered) = (false, !cancel);
    while !(put_answered && cancel_answered) {
        match recv_msg_block::<ToClient>() {
            ToClient::PutOk { .. } | ToClient::Canceled { .. } => put_answered = true,
            ToClient::CancelOk { .. } | ToClient::CancelErr { .. } => cancel_answered = true,
        }
    }
    send_msg(server, ToServer::Shutdown);
}

/// The disk: on `Write{r,..}` it answers `Written{r}`; on `Shutdown` it ends.
pub fn disk(server: ThreadId) {
    loop {
        match recv_msg_block::<ToDisk>() {
            ToDisk::Write { req, .. } => send_msg(server, ToServer::Written { req }),
            ToDisk::Shutdown => return,
        }
    }
}

/// The server's signature: `server(client, disk, args)`; it must end as described above.
pub type Server = fn(ThreadId, ThreadId, Args);

/// Spawns `server`; the server's thread first spawns `disk` and `client`, then runs
/// `server(client, disk, args)`. The main thread waits for the server.
pub fn launch(args: Args, server: Server) {
    let named = |n: &str| thread::Builder::new().name(n.to_string());
    let srv = named("server")
        .spawn(move || {
            let me = thread::current().id();
            let named = |n: &str| thread::Builder::new().name(n.to_string());
            let dk = named("disk").spawn(move || disk(me)).unwrap().thread().id();
            let cl = named("client").spawn(move || client(me)).unwrap().thread().id();
            server(cl, dk, args);
        })
        .unwrap();
    srv.join().unwrap();
}

/// An entry point running `launch(args, server)`.
pub fn entry(args: Args, server: Server) -> Entry {
    Arc::new(move || launch(args, server))
}
