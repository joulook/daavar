//! 3FS chain replication (CRAQ) of one chunk on one chain of three targets: the specification
//! derived from 3FS's documentation ("Strong Consistency ... CRAQ") and its P model
//! (specs/DataStorage). What the two clients can observe is exactly what one atomic register,
//! initially 0, shows: every Write and every Read takes effect at one instant between its request
//! and its reply, in one total order consistent with each client's program order. The order of the
//! writes is the order in which the write serialiser takes them; a read takes effect at the
//! committed version its replica holds (see NOTES.md). Revision (owner, 2026-11-06, coordinator
//! harness change): a read may also end with `Unavailable(c)`, one `nondet()` choice per read.
//! Revision (owner, 2026-11-06 19:55, coordinator change; plan/case-studies/3fs/craq/
//! SPEC-FIX-DESIGN.md): the one object that served every request in one loop is split into a write
//! serialiser `obj`, one committed-state holder `p<i>` per client and one front end `f<i>` per
//! client, so that two reads of one committed version are not causally ordered; the rules are the
//! same.
//! Usage: `bench_craq_derived count spec|impl [--model fifo|bag] -- <q>`.
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{self, Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

#[derive(Clone, Debug, PartialEq)]
enum Req {
    Write(usize, u64),
    Read(usize),
} // client -> node: (client, value) / (client)
#[derive(Clone, Debug, PartialEq)]
enum Rep {
    Ack(usize),
    Value(usize, u64),
    Unavailable(usize),
} // node -> client: (client) / (client, value)

/// The invisible setup message: every process's address, sent once by `init` to `obj`, the
/// `p<i>` and the `f<i>`.
#[derive(Clone, Debug, PartialEq)]
struct Setup {
    obj: ThreadId,
    holders: Vec<ThreadId>,
    clients: Vec<ThreadId>,
}
/// The invisible messages among `obj`, `p<i>` and `f<i>`.
#[derive(Clone, Debug, PartialEq)]
enum Ctl {
    Read(usize),
    Prepare,
    Released,
    Install(u64),
}
const SETUP_TAG: u32 = 1;
const PREPARE_TAG: u32 = 2;
const RELEASED_TAG: u32 = 3;
const INSTALL_TAG: u32 = 4;

const CLIENTS: usize = 2;
fn value(i: usize, j: usize) -> u64 {
    (1 + i + CLIENTS * j) as u64
}

/// The interface's client loop; in the specification `head` is the write serialiser `obj` and the
/// only element of `targets` is the client's front end `f<i>`, so there is no not-committed answer
/// and no retry.
fn client(i: usize, q: usize, head: ThreadId, targets: Vec<ThreadId>) {
    for j in 0..q {
        match (i + j) % 2 {
            0 => {
                send_msg(head, Req::Write(i, value(i, j)));
                let Rep::Ack(_) = recv_msg_block::<Rep>() else { panic!("Ack expected") };
            }
            _ => {
                send_msg(targets[0], Req::Read(i));
                let (Rep::Value(..) | Rep::Unavailable(_)) = recv_msg_block::<Rep>() else {
                    panic!("Value or Unavailable expected")
                };
            }
        }
    }
}

/// The chunk as the clients see it: one register, initially 0 (the chunk committed at version 0
/// at every target). Writes of the chunk are applied and committed one at a time, in one order
/// (the head's version order): `obj` takes the pending writes one at a time; for each, it makes
/// every committed-state holder stop answering at the old version (Prepare / Released), installs
/// the new committed value at every holder, and only then acknowledges (a write is acknowledged
/// after its commit along the whole chain). Never ends (like the implementations' nodes).
fn writer() {
    let Setup { holders, clients, .. } = recv_tagged_msg_block::<_, Setup>(|_, t| t == Some(SETUP_TAG));
    loop {
        let Req::Write(c, v) = recv_tagged_msg_block::<_, Req>(|_, t| t.is_none()) else {
            panic!("Write expected")
        };
        for &h in &holders {
            send_tagged_msg(h, PREPARE_TAG, Ctl::Prepare);
        }
        for _ in &holders {
            let Ctl::Released = recv_tagged_msg_block::<_, Ctl>(|_, t| t == Some(RELEASED_TAG)) else {
                panic!("Released expected")
            };
        }
        for &h in &holders {
            send_tagged_msg(h, INSTALL_TAG, Ctl::Install(v));
        }
        send_msg(clients[c], Rep::Ack(c));
    }
}

/// Client i's committed-state holder: the content of the last committed write (initially 0). A
/// read returns the content of the last committed write at the instant it is served. A read taken
/// before a Prepare takes effect before that write; after Released, no read is answered until the
/// new value is installed (a replica never returns uncommitted data). Never ends.
fn holder(i: usize) {
    let Setup { obj, clients, .. } = recv_tagged_msg_block::<_, Setup>(|_, t| t == Some(SETUP_TAG));
    let mut chunk: u64 = 0;
    loop {
        match recv_tagged_msg_block::<_, Ctl>(|_, t| t.is_none() || t == Some(PREPARE_TAG)) {
            Ctl::Read(c) => send_msg(clients[c], Rep::Value(c, chunk)),
            Ctl::Prepare => {
                send_tagged_msg(obj, RELEASED_TAG, Ctl::Released);
                let Ctl::Install(v) = recv_tagged_msg_block::<_, Ctl>(|_, t| t == Some(INSTALL_TAG)) else {
                    panic!("Install expected")
                };
                chunk = v;
            }
            m => panic!("p{i}: unexpected {m:?}"),
        }
    }
}

/// Client i's front end: receives only client i's reads. Revision (owner, 2026-11-06, coordinator
/// harness change): any read may instead be answered `Unavailable(c)` (a nondeterministic choice);
/// such a read leaves the chunk unchanged and constrains no later read (a failed read is not a
/// read). Otherwise the read goes to the holder `p<i>`. Never ends.
fn front(i: usize) {
    let Setup { holders, clients, .. } = recv_tagged_msg_block::<_, Setup>(|_, t| t == Some(SETUP_TAG));
    loop {
        match recv_tagged_msg_block::<_, Req>(|_, t| t.is_none()) {
            Req::Read(c) => {
                if nondet() {
                    send_msg(clients[c], Rep::Unavailable(c));
                } else {
                    send_msg(holders[i], Ctl::Read(c));
                }
            }
            r => panic!("f{i}: unexpected {r:?}"),
        }
    }
}

fn spec_program(q: usize) {
    let named = |n: String| thread::Builder::new().name(n);
    let oid = named("obj".to_string()).spawn(writer).unwrap().thread().id();
    let hids: Vec<ThreadId> =
        (0..CLIENTS).map(|i| named(format!("p{i}")).spawn(move || holder(i)).unwrap().thread().id()).collect();
    let fids: Vec<ThreadId> =
        (0..CLIENTS).map(|i| named(format!("f{i}")).spawn(move || front(i)).unwrap().thread().id()).collect();
    let mut cs = Vec::new();
    for i in 0..CLIENTS {
        let f = fids[i];
        cs.push(named(format!("c{i}")).spawn(move || client(i, q, oid, vec![f])).unwrap());
    }
    let cids: Vec<ThreadId> = cs.iter().map(|h| h.thread().id()).collect();
    let setup = Setup { obj: oid, holders: hids.clone(), clients: cids };
    let to: Vec<ThreadId> = std::iter::once(oid).chain(hids).chain(fids).collect();
    let init = named("init".to_string())
        .spawn(move || {
            for t in to {
                send_tagged_msg(t, SETUP_TAG, setup.clone());
            }
        })
        .unwrap();
    init.join().unwrap();
    for c in cs {
        c.join().unwrap();
    }
}

fn q_of(a: &[String]) -> usize {
    a.first().and_then(|s| s.parse().ok()).unwrap_or(1)
}

fn entry(a: &[String]) -> Entry {
    let q = q_of(a);
    Arc::new(move || spec_program(q))
}

fn visible(_a: &[String]) -> Vec<String> {
    (0..CLIENTS).map(|i| format!("c{i}")).collect()
}

fn main() -> ExitCode {
    program::main(Program { visible: &visible, spec: &entry, imp: &entry })
}
