//! Specification of Omni-Paxos log replication (Sequence Paxos + BLE, no reconfiguration),
//! derived from the paper's correctness properties only: no nodes, ballots, quorums or
//! node-to-node messages. One internal thread `log` holds the decided sequence D.
//!
//! Visible behaviour allowed (see NOTES.md for the paper sentences):
//!  - each proposal, when it arrives, is either lost or queued (nondet); a queued value is
//!    placed in the log and decided at any later point, without waiting for other proposals;
//!  - the undecided suffix of the log may be replayed behind itself (Figure 3b P3/P4), so a
//!    value may be decided more than once (at most t × cand replays per execution: the instance's election budget);
//!  - the first occurrences in D interleave the decided values of c0 and of c1, each
//!    client's values in the order the client sent them;
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
/// State: per client, the queue of arrived, kept values not yet placed in the log; `suffix`,
/// the placed but undecided part of the one log, in log order. Steps, each a nondet() choice:
/// receive the next proposal (lost or queued, nondet); place the head of a client's queue at
/// the end of `suffix`; decide the head of `suffix` (send `Committed` to every client);
/// replay `suffix` behind itself (suffix <- suffix ++ suffix, at most `max_replays` times).
///
/// `max_replays` = t × cand: a replay (P3/P4) happens at a leader change, and the instance
/// allows at most t election timeouts on each of the cand candidate nodes. The protocol itself
/// has no bound; one is needed for a finite instance, since a blocked client would otherwise
/// receive an unbounded number of replayed values.
fn spec_log(e: usize, max_replays: usize) {
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
    let mut suffix: VecDeque<u64> = VecDeque::new();
    let mut replays = 0usize;
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
    #[derive(Clone, Copy)]
    enum Step {
        Receive,
        Place(usize),
        Decide,
        Replay,
    }
    // 2. Freedoms 2-4: when values are placed (cross-client order), when the head of the
    //    undecided suffix is decided, and whether the undecided suffix is replayed.
    loop {
        let mut opts: Vec<Step> = Vec::new();
        if remaining > 0 {
            opts.push(Step::Receive);
        }
        for i in 0..CLIENTS {
            if !queue[i].is_empty() {
                opts.push(Step::Place(i));
            }
        }
        if !suffix.is_empty() {
            opts.push(Step::Decide);
            if replays < max_replays {
                opts.push(Step::Replay);
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
            Step::Receive => {
                let NIn::Propose(v) = recv_msg_block::<NIn>() else {
                    unreachable!("Setup is sent once")
                };
                remaining -= 1;
                arrive(v, &mut queue);
            }
            Step::Place(i) => {
                let v = queue[i].pop_front().unwrap();
                suffix.push_back(v);
            }
            Step::Decide => {
                let v = suffix.pop_front().unwrap();
                for c in &clients {
                    send_msg(*c, CIn::Committed(v));
                }
            }
            Step::Replay => {
                let again: Vec<u64> = suffix.iter().copied().collect();
                suffix.extend(again);
                replays += 1;
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
fn op_spec((e, max_replays): (usize, usize)) -> Prog {
    prog(move || {
        let log = named("log", move || spec_log(e, max_replays)).thread().id();
        let clients: Vec<ThreadId> = (0..CLIENTS)
            .map(|i| named(&format!("c{i}"), move || client(i, e, log)).thread().id())
            .collect();
        named("init", move || {
            send_msg(log, NIn::Setup(Setup { me: 0, nodes: clients, client: None }))
        });
    })
}

/// `<variant> <e> <t> [<cand>]` -> (e, t × cand). `<variant>` is ignored; defaults: e = 1,
/// t = 1, cand = 3.
fn parse(a: &[String]) -> (usize, usize) {
    let num = |k: usize, d: usize| a.get(k).and_then(|s| s.parse().ok()).unwrap_or(d);
    (num(1, 1), num(2, 1) * num(3, 3))
}

fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &|_| (0..CLIENTS).map(|i| format!("c{i}")).collect(),
        spec: &|a| op_spec(parse(a)),
        imp: &|a| op_spec(parse(a)), // this crate is a specification only; imp = spec
    })
}
