//! S_cas: the linearizable compare-and-set register that Cassandra's lightweight transactions implement on one
//! partition, as its clients see it, with unknown outcomes: a request may be answered `Failed` (Cassandra's contention
//! or write timeout), in which case its effect, if it is a Cas whose condition holds, may still take place at any later
//! point (the in-progress proposal is completed by a later coordinator) or never. Written by the coordinator
//! (2026-11-04); the interface is plan/case-studies/cassandra/INTERFACE.md. ARGS `<q> [v1|v1nolin] [fifo|bag]` (only
//! q is used). Entries: `spec` = this object; `imp` = the same (a specification-only crate).
use std::sync::Arc;
use traceforge::thread::{self, ThreadId};
use traceforge::{nondet, recv_msg_block, recv_tagged_msg_block, send_msg, send_tagged_msg};

const CLIENTS: usize = 2;

#[derive(Clone, Debug, PartialEq)]
enum Req { Cas(usize, u64, u64), Read(usize) }
#[derive(Clone, Debug, PartialEq)]
enum Rep { CasResult(usize, u64, u64), Value(usize, u64), Failed(usize) }
#[derive(Clone, Debug, PartialEq)]
struct Setup { nodes: Vec<ThreadId>, clients: Vec<ThreadId> }
const SETUP_TAG: u32 = 1;

fn value(i: usize, j: usize) -> u64 { (1 + i + CLIENTS * j) as u64 }
fn client(i: usize, q: usize, node: ThreadId) {
    let mut known: u64 = 0;
    for j in 0..q {
        if (i + j) % 2 == 0 { send_msg(node, Req::Cas(i, known, value(i, j))); } else { send_msg(node, Req::Read(i)); }
        match recv_msg_block::<Rep>() {
            Rep::CasResult(_, _, v) => known = v,
            Rep::Value(_, v) => known = v,
            Rep::Failed(_) => {}
        }
    }
}

/// The object: takes Setup, then serves `total` requests in arrival order. Before serving a request it may apply any
/// of the pending unknown-outcome Cas operations (each at most once, in any order, `nondet` each); a request is
/// either applied and answered, or answered `Failed` (nondet), a failed Cas joining the pending set.
fn object(total: usize) {
    let Setup { clients, .. } = recv_tagged_msg_block::<_, Setup>(|_, tag| tag == Some(SETUP_TAG));
    let mut x: u64 = 0;
    let mut pending: Vec<(u64, u64)> = Vec::new();   // (expected, value) of Cas requests answered Failed
    for _ in 0..total {
        let req = recv_tagged_msg_block::<_, Req>(|_, tag| tag.is_none());
        // linearize some pending unknown-outcome operations before this request
        let mut k = 0;
        while k < pending.len() {
            if nondet() { let (e, v) = pending.remove(k); if x == e { x = v; } } else { k += 1; }
        }
        match req {
            Req::Cas(i, e, v) => {
                if nondet() { pending.push((e, v)); send_msg(clients[i], Rep::Failed(i)); }
                else if x == e { x = v; send_msg(clients[i], Rep::CasResult(i, 1, v)); }
                else { send_msg(clients[i], Rep::CasResult(i, 0, x)); }
            }
            Req::Read(i) => {
                if nondet() { send_msg(clients[i], Rep::Failed(i)); } else { send_msg(clients[i], Rep::Value(i, x)); }
            }
        }
    }
    // pending operations may still take effect after the last request; unobservable, so nothing to do
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
    let usage = || -> ! { panic!("cassandra-cas: ARGS are `<q> [v1|v1nolin] [fifo|bag]`, got {a:?}") };
    if a.is_empty() { usage(); }
    a[0].parse().unwrap_or_else(|_| usage())
}

fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &|_| (0..CLIENTS).map(|i| format!("c{i}")).collect(),
        spec: &|a| op_spec(parse(a)),
        imp: &|a| op_spec(parse(a)),
    })
}
