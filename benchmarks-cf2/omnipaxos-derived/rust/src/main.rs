//! Specification of Omni-Paxos log replication (Sequence Paxos + BLE, no reconfiguration),
//! derived from the paper's correctness properties only: no nodes, ballots, quorums or
//! node-to-node messages. One internal thread `log` holds the decided sequence D.
//!
//! Visible behaviour allowed (see NOTES.md for the paper sentences):
//!  - each proposal is either decided once or never decided (nondet per proposal);
//!  - D is an interleaving of the decided subsequence of c0's values and of c1's values,
//!    each client's decided values in the order the client sent them (nondet merge);
//!  - every client receives all of D, in D's order (so each client sees a prefix of the same
//!    sequence at every point: the prefix property); nothing outside D is ever committed.
//! Usage: `count spec --model fifo -- <variant> <e> <t> [<cand>]` (only `<e>` is used).
use std::collections::VecDeque;
use std::sync::Arc;
use traceforge::thread::{self, ThreadId};
use traceforge::{nondet, recv_msg_block, recv_tagged_msg_block, send_msg};

const CLIENTS: usize = 2;
type Prog = traceforge::program::Entry;

#[derive(Clone, Debug, PartialEq)]
enum NIn {
    Setup(Setup),
    Propose(u64),
}
#[derive(Clone, Debug, PartialEq)]
struct Setup {
    me: usize,
    nodes: Vec<ThreadId>,
    client: Option<ThreadId>,
}
#[derive(Clone, Debug, PartialEq)]
enum CIn {
    Committed(u64),
}

fn value(client: usize, j: usize) -> u64 {
    (1 + client + CLIENTS * j) as u64
}
fn client(i: usize, e: usize, target: ThreadId) {
    let mine: Vec<u64> = (0..e).map(|j| value(i, j)).collect();
    for v in &mine {
        send_msg(target, NIn::Propose(*v));
    }
    let mut seen = 0usize;
    while seen < e {
        let CIn::Committed(v) = recv_msg_block::<CIn>();
        if mine.contains(&v) {
            seen += 1;
        }
    }
}

/// The specification's only internal thread; `e` is the instance parameter (how many
/// proposals each client sends), which `log` needs to know when every Propose has arrived.
fn spec_log(e: usize) {
    // 1. Setup. Proposals that arrive before it are kept, attributed to their client by
    //    value (v(i, j) = 1 + i + 2j), in arrival order (FIFO per client).
    let owner = |v: u64| ((v - 1) as usize) % CLIENTS;
    let mut pending: Vec<Vec<u64>> = vec![Vec::new(); CLIENTS];
    let clients: Vec<ThreadId> = loop {
        match recv_msg_block::<NIn>() {
            NIn::Setup(s) => break s.nodes,
            NIn::Propose(v) => pending[owner(v)].push(v),
        }
    };
    // 2. Every Propose is eventually delivered: receive the rest, one client at a time
    //    (the order of reception is invisible to the clients).
    for i in 0..CLIENTS {
        let src = clients[i];
        while pending[i].len() < e {
            let NIn::Propose(v) = recv_tagged_msg_block::<_, NIn>(move |t, _| t == src) else {
                unreachable!("a client sends only Propose")
            };
            pending[i].push(v);
        }
    }
    // 3a. Freedom 1, the fate of each proposal: decided (once) or never decided.
    let mut kept: Vec<VecDeque<u64>> = pending
        .iter()
        .map(|vs| vs.iter().copied().filter(|_| nondet()).collect())
        .collect();
    // 3b. Freedom 2, the order of the decided entries across clients: any interleaving that
    //     keeps each client's own order. Each decided entry goes to every client, in order.
    loop {
        let i = match (kept[0].is_empty(), kept[1].is_empty()) {
            (true, true) => break,
            (false, true) => 0,
            (true, false) => 1,
            (false, false) => {
                if nondet() {
                    0
                } else {
                    1
                }
            }
        };
        let v = kept[i].pop_front().unwrap();
        for c in &clients {
            send_msg(*c, CIn::Committed(v));
        }
    }
}

fn prog<F: Fn() + Send + Sync + 'static>(f: F) -> Prog {
    Arc::new(f)
}
fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}
fn op_spec(e: usize) -> Prog {
    prog(move || {
        let log = named("log", move || spec_log(e)).thread().id();
        let clients: Vec<ThreadId> = (0..CLIENTS)
            .map(|i| named(&format!("c{i}"), move || client(i, e, log)).thread().id())
            .collect();
        named("init", move || {
            send_msg(log, NIn::Setup(Setup { me: 0, nodes: clients, client: None }))
        });
    })
}

/// `<variant> <e> <t> [<cand>]`: only `<e>` is used (default 1).
fn parse(a: &[String]) -> usize {
    a.get(1).and_then(|s| s.parse().ok()).unwrap_or(1)
}

fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &|_| (0..CLIENTS).map(|i| format!("c{i}")).collect(),
        spec: &|a| op_spec(parse(a)),
        imp: &|a| op_spec(parse(a)), // this crate is a specification only; imp = spec
    })
}
