//! Derived specification of what the two clients of Cassandra 4.1's QUORUM read path
//! (one partition, cells `a` and `b`, read repair `blocking` or `none`) can observe.
//! Derived from the read-repair documentation page only; see NOTES.md.
//!
//! No replica, digest, repair message or timestamp is modelled. The partition is an
//! abstract state held by one thread (named `n2`, the replica without a client):
//!   - `lo_a`, `lo_b`: per cell, the value every later QUORUM read of that cell is
//!     guaranteed to see at least (the floor);
//!   - `pending`: the values of the writes that have started but not yet reached a
//!     quorum (visible to a read, or not, at the read's choice).
//! `n0` and `n1` are the coordinators of `c0` and `c1`: they turn each client request
//! into steps on the abstract state and send the client its reply.
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{self, Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ---- the interface (verbatim) ----
#[derive(Clone, Debug, PartialEq)]
enum Req {
    Write(usize, u64),
    ReadA(usize),
    ReadAB(usize),
}
#[derive(Clone, Debug, PartialEq)]
enum Rep {
    Ack(usize),
    ValueA(usize, u64),
    ValueAB(usize, u64, u64),
}

const CLIENTS: usize = 2;
fn value(i: usize, j: usize) -> u64 {
    (1 + i + CLIENTS * j) as u64
}
fn client(i: usize, q: usize, node: ThreadId) {
    for j in 0..q {
        match (i + j) % 3 {
            0 => { send_msg(node, Req::Write(i, value(i, j))); let Rep::Ack(_) = recv_msg_block::<Rep>() else { panic!("Ack expected") }; }
            1 => { send_msg(node, Req::ReadA(i)); let Rep::ValueA(..) = recv_msg_block::<Rep>() else { panic!("ValueA expected") }; }
            _ => { send_msg(node, Req::ReadAB(i)); let Rep::ValueAB(..) = recv_msg_block::<Rep>() else { panic!("ValueAB expected") }; }
        }
    }
}

// ---- internal (invisible) messages ----
#[derive(Clone, Debug, PartialEq)]
enum Int {
    /// main -> n0, n1, n2: the addresses (n0, n1, n2, c0, c1)
    Setup(Vec<ThreadId>),
    /// coordinator -> state: a write of value v has started (it may now be seen)
    Begin(u64),
    /// coordinator -> state: the write of value v has reached a quorum
    Commit(u64),
    /// state -> coordinator: the commit is recorded
    Committed,
    /// coordinator -> state: a QUORUM read of `a` (false) or of `a, b` (true)
    Read(bool),
    /// state -> coordinator: the read's result (b is unused for a read of `a`)
    Result(u64, u64),
}

/// A choice among `n >= 1` alternatives, as a chain of `nondet()` (n distinct branches).
fn choose(n: usize) -> usize {
    for k in 0..n.saturating_sub(1) {
        if nondet() {
            return k;
        }
    }
    n - 1
}

/// The values a QUORUM read of one cell may return: the floor, or the value of any
/// write still on its way to a quorum that is newer than the floor.
fn candidates(lo: u64, pending: &[u64]) -> Vec<u64> {
    let mut c = vec![lo];
    for &v in pending {
        if v > lo && !c.contains(&v) {
            c.push(v);
        }
    }
    c.sort();
    c
}

fn kinds(i: usize, q: usize) -> (usize, usize) {
    // (writes, reads) of client i
    let w = (0..q).filter(|j| (i + j) % 3 == 0).count();
    (w, q - w)
}

/// The abstract partition (thread `n2`).
fn state(q: usize, blocking: bool, main: ThreadId) {
    let Int::Setup(ids) = recv_tagged_msg_block::<_, Int>(move |t, _| t == main) else { panic!("Setup expected") };
    // the number of steps it will be asked for: 2 per write (Begin, Commit), 1 per read
    let mut steps = 0;
    for i in 0..CLIENTS {
        let (w, r) = kinds(i, q);
        steps += 2 * w + r;
    }
    let (mut lo_a, mut lo_b) = (0u64, 0u64);
    let mut pending: Vec<u64> = Vec::new();
    for _ in 0..steps {
        let (from, m): (usize, Int) = recv_msg_block::<(usize, Int)>();
        let to = ids[from]; // the coordinator n<from>
        match m {
            Int::Begin(v) => pending.push(v),
            Int::Commit(v) => {
                pending.retain(|&p| p != v);
                lo_a = lo_a.max(v);
                lo_b = lo_b.max(v);
                send_msg(to, Int::Committed);
            }
            Int::Read(false) => {
                let c = candidates(lo_a, &pending);
                let x = c[choose(c.len())];
                if blocking {
                    // blocking repair of cell a on the replicas read: x is now on a quorum
                    lo_a = x;
                }
                send_msg(to, Int::Result(x, x));
            }
            Int::Read(true) => {
                if blocking {
                    let ca = candidates(lo_a, &pending);
                    let xa = ca[choose(ca.len())];
                    // b never newer than a: only cell a is ever repaired alone
                    let cb: Vec<u64> = candidates(lo_b, &pending).into_iter().filter(|&v| v <= xa).collect();
                    let xb = cb[choose(cb.len())];
                    lo_a = xa;
                    lo_b = xb;
                    send_msg(to, Int::Result(xa, xb));
                } else {
                    // none: both cells come from one partition state (write atomicity)
                    let c = candidates(lo_a, &pending);
                    let x = c[choose(c.len())];
                    send_msg(to, Int::Result(x, x));
                }
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}

/// The coordinator `n<i>` of client `c<i>`.
fn coordinator(i: usize, q: usize, main: ThreadId) {
    let Int::Setup(ids) = recv_tagged_msg_block::<_, Int>(move |t, _| t == main) else { panic!("Setup expected") };
    let (st, cl) = (ids[2], ids[3 + i]);
    for _ in 0..q {
        let r: Req = recv_tagged_msg_block(move |t, _| t == cl);
        match r {
            Req::Write(c, v) => {
                send_msg(st, (i, Int::Begin(v)));
                send_msg(st, (i, Int::Commit(v)));
                let Int::Committed = recv_tagged_msg_block::<_, Int>(move |t, _| t == st) else { panic!() };
                send_msg(cl, Rep::Ack(c));
            }
            Req::ReadA(c) => {
                send_msg(st, (i, Int::Read(false)));
                let Int::Result(a, _) = recv_tagged_msg_block::<_, Int>(move |t, _| t == st) else { panic!() };
                send_msg(cl, Rep::ValueA(c, a));
            }
            Req::ReadAB(c) => {
                send_msg(st, (i, Int::Read(true)));
                let Int::Result(a, b) = recv_tagged_msg_block::<_, Int>(move |t, _| t == st) else { panic!() };
                send_msg(cl, Rep::ValueAB(c, a, b));
            }
        }
    }
}

fn run(q: usize, blocking: bool) {
    let main = thread::current().id();
    let mut nodes = Vec::new();
    for k in 0..3 {
        let h = thread::Builder::new()
            .name(format!("n{k}"))
            .spawn(move || match k {
                0 | 1 => coordinator(k, q, main),
                _ => state(q, blocking, main),
            })
            .unwrap();
        nodes.push(h);
    }
    let ids: Vec<ThreadId> = nodes.iter().map(|h| h.thread().id()).collect();
    let mut clients = Vec::new();
    for i in 0..CLIENTS {
        let node = ids[i];
        clients.push(
            thread::Builder::new()
                .name(format!("c{i}"))
                .spawn(move || client(i, q, node))
                .unwrap(),
        );
    }
    let mut all = ids.clone();
    all.extend(clients.iter().map(|h| h.thread().id()));
    for &n in &ids {
        send_msg(n, Int::Setup(all.clone()));
    }
    for c in clients {
        c.join().unwrap();
    }
    for n in nodes {
        n.join().unwrap();
    }
}

fn parse(args: &[String]) -> (usize, bool) {
    let q: usize = args.first().and_then(|s| s.parse().ok()).expect("q >= 1");
    assert!(q >= 1, "q >= 1");
    let blocking = match args.get(1).map(String::as_str) {
        Some("blocking") => true,
        Some("none") => false,
        m => panic!("strategy blocking|none expected, got {m:?}"),
    };
    (q, blocking)
}

fn entry(args: &[String]) -> Entry {
    let (q, blocking) = parse(args);
    Arc::new(move || run(q, blocking))
}

fn main() -> ExitCode {
    program::main(Program {
        visible: &|_| vec!["c0".to_string(), "c1".to_string()],
        spec: &entry,
        imp: &entry,
    })
}
