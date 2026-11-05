//! S_rr: the client-visible contract of Cassandra's quorum read path on one partition with two cells, as the
//! documentation states it for the two read-repair strategies (doc/modules/cassandra/pages/operating/read_repair.adoc,
//! 4.1): `blocking` = monotonic quorum reads (per cell, no read returns a value older than one already returned or
//! acknowledged; the cells of one read may come from different writes), `none` = partition write atomicity (both cells
//! of one read come from one write; reads are monotonic only with respect to acknowledged writes). Under `blocking` a
//! read of both cells may be torn (cell a newer than cell b) only after a read of cell a alone returned that newer value,
//! since only such a projected read repairs cell a without cell b at a replica; cell b is never newer than cell a (every
//! write sets both, a projection repairs only a). Written by the coordinator (2026-11-05, revised after Astra's review); the interface is plan/case-studies/cassandra/rr/INTERFACE.md. ARGS
//! `<q> <blocking|none> [fifo|bag]`. Entries: `spec` = this object; `imp` = the same (a specification-only crate).
use std::sync::Arc;
use traceforge::thread::{self, ThreadId};
use traceforge::{nondet, recv_msg_block, recv_tagged_msg_block, send_msg, send_tagged_msg};

const CLIENTS: usize = 2;

#[derive(Clone, Debug, PartialEq)]
enum Req { Write(usize, u64), ReadA(usize), ReadAB(usize) }
#[derive(Clone, Debug, PartialEq)]
enum Rep { Ack(usize), ValueA(usize, u64), ValueAB(usize, u64, u64) }
#[derive(Clone, Debug, PartialEq)]
struct Setup { nodes: Vec<ThreadId>, clients: Vec<ThreadId> }
const SETUP_TAG: u32 = 1;

#[derive(Clone, Copy, PartialEq)]
enum Strategy { Blocking, None }

fn value(i: usize, j: usize) -> u64 { (1 + i + CLIENTS * j) as u64 }
fn client(i: usize, q: usize, node: ThreadId) {
    for j in 0..q {
        match (i + j) % 3 {
            0 => { send_msg(node, Req::Write(i, value(i, j))); let Rep::Ack(_) = recv_msg_block::<Rep>() else { panic!("Ack expected") }; }
            1 => { send_msg(node, Req::ReadA(i)); let Rep::ValueA(..) = recv_msg_block::<Rep>() else { panic!("ValueA expected") }; }
            _ => { send_msg(node, Req::ReadAB(i)); let Rep::ValueAB(..) = recv_msg_block::<Rep>() else { panic!("ValueAB expected") }; }
        }
    }
}

/// One of `k` choices, each reachable by exactly one sequence of `nondet` answers.
fn choose(k: usize) -> usize { for i in 0..k - 1 { if nondet() { return i; } } k - 1 }

/// The object: takes Setup, then serves `total` requests in arrival order. A write is applied (visible to later reads)
/// as soon as it arrives; its acknowledgement is sent at once or deferred (`nondet`) and sent at a later step. A read
/// returns, per cell, any applied write not older than the cell's floor: under `blocking` the floor is raised by every
/// acknowledgement and by every returned value (monotonic quorum reads), under `none` only by acknowledgements, and a
/// ReadAB under `none` takes both cells from one write (write atomicity).
fn object(total: usize, strategy: Strategy) {
    let Setup { clients, .. } = recv_tagged_msg_block::<_, Setup>(|_, tag| tag == Some(SETUP_TAG));
    let mut applied: Vec<u64> = vec![0];            // timestamps (= values) of the writes applied so far; 0 = the initial state
    let mut unacked: Vec<(usize, u64)> = Vec::new();  // writes whose acknowledgement is deferred
    let mut acked: u64 = 0;                           // the newest acknowledged write
    let (mut floor_a, mut floor_b) = (0u64, 0u64);    // blocking: the newest returned or acknowledged value per cell
    let mut projected: u64 = 0;                       // blocking: the newest value a ReadA returned (the only way a replica gets a without b)
    let flush = |unacked: &mut Vec<(usize, u64)>, acked: &mut u64, floor_a: &mut u64, floor_b: &mut u64, all: bool, clients: &Vec<ThreadId>| {
        let mut k = 0;
        while k < unacked.len() {
            if all || nondet() {
                let (i, t) = unacked.remove(k);
                *acked = (*acked).max(t); *floor_a = (*floor_a).max(t); *floor_b = (*floor_b).max(t);
                send_msg(clients[i], Rep::Ack(i));
            } else { k += 1; }
        }
    };
    let mut remaining: Vec<usize> = vec![total / CLIENTS; CLIENTS];   // requests each client has still to send
    for _ in 0..total {
        // a client whose acknowledgement is deferred sends nothing more: when every client with requests left is such a
        // client, the deferred acknowledgements are sent now (otherwise the object would wait for a request that never comes)
        if (0..CLIENTS).all(|i| remaining[i] == 0 || unacked.iter().any(|&(c, _)| c == i)) {
            flush(&mut unacked, &mut acked, &mut floor_a, &mut floor_b, true, &clients);
        }
        let req = recv_tagged_msg_block::<_, Req>(|_, tag| tag.is_none());
        flush(&mut unacked, &mut acked, &mut floor_a, &mut floor_b, false, &clients);   // some deferred acknowledgements may precede this request
        match req {
            Req::Write(i, v) => {
                remaining[i] -= 1; applied.push(v);
                if nondet() { unacked.push((i, v)); }
                else { acked = acked.max(v); floor_a = floor_a.max(v); floor_b = floor_b.max(v); send_msg(clients[i], Rep::Ack(i)); }
            }
            Req::ReadA(i) => {
                remaining[i] -= 1;
                let fa = if strategy == Strategy::Blocking { floor_a } else { acked };
                let cands: Vec<u64> = applied.iter().copied().filter(|&t| t >= fa).collect();
                let a = cands[choose(cands.len())];
                if strategy == Strategy::Blocking { floor_a = floor_a.max(a); projected = projected.max(a); }
                send_msg(clients[i], Rep::ValueA(i, a));
            }
            Req::ReadAB(i) => {
                remaining[i] -= 1;
                let (a, b) = if strategy == Strategy::Blocking {
                    // the reconciled pair (a, b): b <= a always (every write sets both cells, a projection repairs only a),
                    // and b < a only when a projected read has spread a (a <= the newest value a ReadA returned) without b.
                    // Exact for reads that do not overlap other operations; a read in flight across a later write and a
                    // later projected read can reconcile a pair this rule forbids (Astra, round 2: q = 5 under bag).
                    let ca: Vec<u64> = applied.iter().copied().filter(|&t| t >= floor_a).collect();
                    let a = ca[choose(ca.len())];
                    let cb: Vec<u64> = applied.iter().copied().filter(|&t| t >= floor_b && (t == a || (t < a && a <= projected))).collect();
                    let b = cb[choose(cb.len())];
                    floor_a = floor_a.max(a); floor_b = floor_b.max(b);
                    (a, b)
                } else {
                    let c: Vec<u64> = applied.iter().copied().filter(|&t| t >= acked).collect();
                    let w = c[choose(c.len())];
                    (w, w)
                };
                send_msg(clients[i], Rep::ValueAB(i, a, b));
            }
        }
        flush(&mut unacked, &mut acked, &mut floor_a, &mut floor_b, false, &clients);   // some deferred acknowledgements may follow this reply, before the next request
    }
    flush(&mut unacked, &mut acked, &mut floor_a, &mut floor_b, true, &clients);   // every write is acknowledged in the end
}

type Prog = traceforge::program::Entry;
fn prog<F: Fn() + Send + Sync + 'static>(f: F) -> Prog { Arc::new(f) }
fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> { thread::Builder::new().name(n.to_string()).spawn(f).unwrap() }

fn op_spec(q: usize, strategy: Strategy) -> Prog {
    prog(move || {
        let obj = named("obj", move || object(CLIENTS * q, strategy)).thread().id();
        let clients: Vec<ThreadId> = (0..CLIENTS).map(|i| named(&format!("c{i}"), move || client(i, q, obj)).thread().id()).collect();
        named("init", move || send_tagged_msg(obj, SETUP_TAG, Setup { nodes: vec![], clients }));
    })
}

fn parse(a: &[String]) -> (usize, Strategy) {
    let usage = || -> ! { panic!("cassandra-rr: ARGS are `<q> <blocking|none> [fifo|bag]`, got {a:?}") };
    if a.len() < 2 { usage(); }
    let q = a[0].parse().unwrap_or_else(|_| usage());
    let s = match a[1].as_str() { "blocking" => Strategy::Blocking, "none" => Strategy::None, _ => usage() };
    (q, s)
}

fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &|_| (0..CLIENTS).map(|i| format!("c{i}")).collect(),
        spec: &|a| { let (q, s) = parse(a); op_spec(q, s) },
        imp: &|a| { let (q, s) = parse(a); op_spec(q, s) },
    })
}
