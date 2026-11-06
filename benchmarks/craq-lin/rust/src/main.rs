//! S_lin for the 3FS chain-replication (CRAQ) study: the linearizable single-chunk register that one replication
//! chain of 3FS presents to its clients, as they see it. Written by the coordinator (2026-11-05) as the hand-written
//! specification of the 3FS study, from pineapple-lin with the two-kind client loop of
//! plan/case-studies/3fs/INTERFACE.md (Write, Read; no read-modify-write). Entries: `spec` = this program; `imp` =
//! the same (this crate is a specification only). ARGS `<q> [fifo|bag]`.
//!
//! Revision (owner, 2026-11-06 19:55; plan/case-studies/3fs/craq/SPEC-FIX-DESIGN.md): the one object that served
//! every request in one receive loop ordered every two operations causally, also two reads of one version, which
//! the register does not order. The register is now three kinds of invisible processes: `obj` serialises the writes
//! (Prepare to both version holders, wait for both Released, Install, Ack); `p<i>` holds the installed version for
//! client i and answers its reads from it; `f<i>` is client i's front end (forwards a Write to `obj`, and a Read
//! either to `p<i>` or answers it `Unavailable` itself). The causal order produced is exactly program order plus the
//! conflict order of the linearization (write/any and reads of different versions), no more.
use std::sync::Arc;
use traceforge::thread::{self, ThreadId};
use traceforge::{nondet, recv_msg_block, recv_tagged_msg_block, send_msg, send_tagged_msg};

const CLIENTS: usize = 2;
const NODES: usize = 3;

/// The clients' requests; their `Debug` rendering is the interface's: `Write(i, v)`, `Read(i)`.
#[derive(Clone, Debug, PartialEq)]
enum Req { Write(usize, u64), Read(usize) }
#[derive(Clone, Debug, PartialEq)]
enum Rep { Ack(usize), Value(usize, u64), Unavailable(usize) }
/// The invisible setup: every process's address, sent once by `init` to `obj`, the `p<i>` and the `f<i>`.
#[derive(Clone, Debug, PartialEq)]
struct Setup { obj: ThreadId, holders: Vec<ThreadId>, clients: Vec<ThreadId> }
/// The invisible messages among `obj`, `p<i>` and `f<i>`.
#[derive(Clone, Debug, PartialEq)]
enum Ctl { Read(usize), Prepare, Released, Install(u64) }
/// Setup is tagged so that each process takes it first; requests that arrive earlier wait in its queue.
const SETUP_TAG: u32 = 1;
const PREPARE_TAG: u32 = 2;
const RELEASED_TAG: u32 = 3;
const INSTALL_TAG: u32 = 4;

fn value(i: usize, j: usize) -> u64 { (1 + i + CLIENTS * j) as u64 }

fn client(i: usize, q: usize, node: ThreadId) {
    for j in 0..q {
        match (i + j) % 2 {
            0 => { send_msg(node, Req::Write(i, value(i, j))); let Rep::Ack(_) = recv_msg_block::<Rep>() else { panic!("Ack expected") }; }
            _ => { send_msg(node, Req::Read(i)); let (Rep::Value(..) | Rep::Unavailable(_)) = recv_msg_block::<Rep>() else { panic!("Value or Unavailable expected") }; }
        }
    }
}

/// The write serialiser: takes the pending writes one at a time (the order taken is the linearization order of the
/// writes); for each, Prepare to both version holders, wait for both Released (every read served at the old
/// version precedes the write), Install the value at both, Ack the writer. Never ends.
fn writer() {
    let Setup { holders, clients, .. } = recv_tagged_msg_block::<_, Setup>(|_, tag| tag == Some(SETUP_TAG));
    loop {
        let Req::Write(i, x) = recv_tagged_msg_block::<_, Req>(|_, tag| tag.is_none()) else { panic!("Write expected") };
        for &h in &holders { send_tagged_msg(h, PREPARE_TAG, Ctl::Prepare); }
        for _ in &holders { let Ctl::Released = recv_tagged_msg_block::<_, Ctl>(|_, tag| tag == Some(RELEASED_TAG)) else { panic!("Released expected") }; }
        for &h in &holders { send_tagged_msg(h, INSTALL_TAG, Ctl::Install(x)); }
        send_msg(clients[i], Rep::Ack(i));
    }
}

/// Client i's version holder: the chunk value installed last (initially 0). Takes a forwarded read or a Prepare,
/// in either order (a read taken first is linearized before the write); a read is answered with the held value; a
/// Prepare is answered Released, after which no read is served until the new value is installed. Never ends.
fn holder(i: usize) {
    let Setup { obj, clients, .. } = recv_tagged_msg_block::<_, Setup>(|_, tag| tag == Some(SETUP_TAG));
    let mut v: u64 = 0;
    loop {
        match recv_tagged_msg_block::<_, Ctl>(|_, tag| tag.is_none() || tag == Some(PREPARE_TAG)) {
            Ctl::Read(c) => send_msg(clients[c], Rep::Value(c, v)),
            Ctl::Prepare => {
                send_tagged_msg(obj, RELEASED_TAG, Ctl::Released);
                let Ctl::Install(x) = recv_tagged_msg_block::<_, Ctl>(|_, tag| tag == Some(INSTALL_TAG)) else { panic!("Install expected") };
                v = x;
            }
            m => panic!("p{i}: unexpected {m:?}"),
        }
    }
}

/// Client i's front end: receives only client i's requests. A Write goes to `obj`; a Read goes to `p<i>`, or is
/// answered `Unavailable(i)` here (revision 2026-11-06, a nondeterministic choice): such a read touches no state and
/// is causally independent of the other client. Never ends.
fn front(i: usize) {
    let Setup { obj, holders, clients } = recv_tagged_msg_block::<_, Setup>(|_, tag| tag == Some(SETUP_TAG));
    loop {
        match recv_tagged_msg_block::<_, Req>(|_, tag| tag.is_none()) {
            Req::Write(c, x) => send_msg(obj, Req::Write(c, x)),
            Req::Read(c) => { if nondet() { send_msg(clients[c], Rep::Unavailable(c)); } else { send_msg(holders[i], Ctl::Read(c)); } }
        }
    }
}

type Prog = traceforge::program::Entry;
fn prog<F: Fn() + Send + Sync + 'static>(f: F) -> Prog { Arc::new(f) }
fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> { thread::Builder::new().name(n.to_string()).spawn(f).unwrap() }

fn op_spec(q: usize) -> Prog {
    prog(move || {
        let obj = named("obj", writer).thread().id();
        let holders: Vec<ThreadId> = (0..CLIENTS).map(|i| named(&format!("p{i}"), move || holder(i)).thread().id()).collect();
        let fronts: Vec<ThreadId> = (0..CLIENTS).map(|i| named(&format!("f{i}"), move || front(i)).thread().id()).collect();
        let cs: Vec<thread::JoinHandle<()>> = (0..CLIENTS).map(|i| { let f = fronts[i]; named(&format!("c{i}"), move || client(i, q, f)) }).collect();
        let clients: Vec<ThreadId> = cs.iter().map(|h| h.thread().id()).collect();
        let setup = Setup { obj, holders: holders.clone(), clients };
        let to: Vec<ThreadId> = std::iter::once(obj).chain(holders).chain(fronts).collect();
        let init = named("init", move || for t in to { send_tagged_msg(t, SETUP_TAG, setup.clone()); });
        init.join().unwrap();
        for c in cs { c.join().unwrap(); }
    })
}

fn parse(a: &[String]) -> usize {
    let usage = || -> ! { panic!("craq-lin: ARGS are `<q> [fifo|bag]`, got {a:?}") };
    if a.is_empty() { usage(); }
    a[0].parse().unwrap_or_else(|_| usage())
}

fn main() -> std::process::ExitCode {
    let _ = NODES;
    traceforge::program::main(traceforge::program::Program {
        visible: &|_| (0..CLIENTS).map(|i| format!("c{i}")).collect(),
        spec: &|a| op_spec(parse(a)),
        imp: &|a| op_spec(parse(a)),
    })
}
