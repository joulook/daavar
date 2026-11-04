//! Specification of Omni-Paxos log replication (Sequence Paxos + BLE, no reconfiguration),
//! derived from the paper's correctness properties only: no nodes, ballots, quorums or
//! node-to-node messages. One internal thread `log` holds the decided sequence D.
//!
//! Visible behaviour allowed (see NOTES.md for the paper sentences):
//!  - each proposal, when it arrives, is either lost or queued (nondet); a queued value is
//!    decided at any later point, without waiting for other proposals (nondet step choice);
//!  - D is an interleaving of the decided subsequence of c0's values and of c1's values,
//!    each client's decided values in the order the client sent them;
//!  - every client receives all of D, in D's order (so each client sees a prefix of the same
//!    sequence at every point: the prefix property); nothing outside D is ever committed.
//! Usage: `count spec --model fifo -- <variant> <e> <t> [<cand>]` (only `<e>` is used).
use std::collections::VecDeque;
use std::sync::Arc;
use traceforge::thread::{self, ThreadId};
use traceforge::{nondet, recv_msg_block, send_msg};

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
///
/// Each proposal is handled when it arrives, without waiting for any other proposal:
/// it is lost (nondet) or queued behind its own client's earlier queued values; at any
/// point `log` may decide the head of either client's queue (nondet), appending it to the
/// one decided sequence and sending `Committed` to every client, or receive the next
/// proposal. A decision may thus happen right at arrival or after later arrivals.
fn spec_log(e: usize) {
    // 1. Setup. Proposals that arrive before it are buffered, attributed to their client
    //    by value (v(i, j) = 1 + i + 2j), in arrival order (FIFO per client).
    let owner = |v: u64| ((v - 1) as usize) % CLIENTS;
    let mut early: Vec<u64> = Vec::new();
    let clients: Vec<ThreadId> = loop {
        match recv_msg_block::<NIn>() {
            NIn::Setup(s) => break s.nodes,
            NIn::Propose(v) => early.push(v),
        }
    };
    let mut queue: Vec<VecDeque<u64>> = vec![VecDeque::new(); CLIENTS];
    // Freedom 1, the fate of a proposal, chosen when it arrives: lost or queued.
    let arrive = |v: u64, queue: &mut Vec<VecDeque<u64>>| {
        if nondet() {
            queue[owner(v)].push_back(v);
        }
    };
    let mut remaining = CLIENTS * e - early.len();
    for v in early {
        arrive(v, &mut queue);
    }
    // 2. Freedom 2, when and in which cross-client order queued values are decided.
    loop {
        // the possible steps: receive (if a proposal is still to arrive), decide c0's
        // head, decide c1's head; one nondet() per extra option
        let mut opts: Vec<usize> = Vec::new();
        if remaining > 0 {
            opts.push(CLIENTS); // receive
        }
        for i in 0..CLIENTS {
            if !queue[i].is_empty() {
                opts.push(i);
            }
        }
        if opts.is_empty() {
            break;
        }
        let mut k = 0;
        while k + 1 < opts.len() && !nondet() {
            k += 1;
        }
        match opts[k] {
            i if i < CLIENTS => {
                let v = queue[i].pop_front().unwrap();
                for c in &clients {
                    send_msg(*c, CIn::Committed(v));
                }
            }
            _ => {
                let NIn::Propose(v) = recv_msg_block::<NIn>() else {
                    unreachable!("Setup is sent once")
                };
                remaining -= 1;
                arrive(v, &mut queue);
            }
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
