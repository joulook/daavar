//! S_lin: the linearizable single-key register that Pineapple (NSDI 2025) implements on one key, as its clients
//! see it: one invisible object serves Write, Read and Rmw in arrival order from the sequential state. Written by
//! the coordinator (2026-11-04), the hand-written specification of the Pineapple study; the interface is
//! plan/twins-contextfree/pineapple/INTERFACE.md. Entries: `spec` = this object; `imp` = the same (this crate is a
//! specification only). ARGS `<q> [fifo|bag]`.
use std::sync::Arc;
use traceforge::thread::{self, ThreadId};
use traceforge::{recv_msg_block, recv_tagged_msg_block, send_msg, send_tagged_msg};

const CLIENTS: usize = 2;
const NODES: usize = 3;

/// The clients' requests; their `Debug` rendering is the interface's: `Write(i, v)`, `Read(i)`, `Rmw(i)`.
#[derive(Clone, Debug, PartialEq)]
enum Req { Write(usize, u64), Read(usize), Rmw(usize) }
#[derive(Clone, Debug, PartialEq)]
enum Rep { Ack, Value(u64), Result(u64) }
#[derive(Clone, Debug, PartialEq)]
struct Setup { nodes: Vec<ThreadId>, clients: Vec<ThreadId> }
/// The invisible Setup is tagged so that the object takes it first; requests that arrive earlier wait in its queue.
const SETUP_TAG: u32 = 1;

fn value(i: usize, j: usize) -> u64 { (1 + i + CLIENTS * j) as u64 }

fn client(i: usize, q: usize, node: ThreadId) {
    for j in 0..q {
        match (i + j) % 3 {
            0 => { send_msg(node, Req::Write(i, value(i, j))); let Rep::Ack = recv_msg_block::<Rep>() else { panic!("Ack expected") }; }
            1 => { send_msg(node, Req::Read(i)); let Rep::Value(_) = recv_msg_block::<Rep>() else { panic!("Value expected") }; }
            _ => { send_msg(node, Req::Rmw(i)); let Rep::Result(_) = recv_msg_block::<Rep>() else { panic!("Result expected") }; }
        }
    }
}

/// The object: takes Setup, then applies `total` requests one at a time in arrival order (the linearization
/// order) to the single integer, replying to the client named in the request; then it ends.
fn object(total: usize) {
    let Setup { clients, .. } = recv_tagged_msg_block::<_, Setup>(|_, tag| tag == Some(SETUP_TAG));
    let mut v: u64 = 0;
    for _ in 0..total {
        match recv_tagged_msg_block::<_, Req>(|_, tag| tag.is_none()) {
            Req::Write(i, x) => { v = x; send_msg(clients[i], Rep::Ack); }
            Req::Read(i) => { send_msg(clients[i], Rep::Value(v)); }
            Req::Rmw(i) => { v += 10; send_msg(clients[i], Rep::Result(v)); }
        }
    }
}

type Prog = traceforge::program::Entry;
fn prog<F: Fn() + Send + Sync + 'static>(f: F) -> Prog { Arc::new(f) }
fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> { thread::Builder::new().name(n.to_string()).spawn(f).unwrap() }

fn op_spec(q: usize) -> Prog {
    prog(move || {
        let obj = named("obj", move || object(CLIENTS * q)).thread().id();
        let clients: Vec<ThreadId> = (0..CLIENTS).map(|i| named(&format!("c{i}"), move || client(i, q, obj)).thread().id()).collect();
        named("init", move || send_tagged_msg(obj, SETUP_TAG, Setup { nodes: vec![], clients }));
    })
}

fn parse(a: &[String]) -> usize {
    let usage = || -> ! { panic!("pineapple-lin: ARGS are `<q> [fifo|bag]`, got {a:?}") };
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
