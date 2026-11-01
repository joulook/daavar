//! Feasibility spike: tikv/raft-rs (unmodified, ad13f3d) under TraceForge's Must API.
//!
//! impl: three invisible node threads `n0 n1 n2`, each owning a `raft::RawNode<MemStorage>`;
//!       mailbox -> `step`/`propose`, `ready()` -> sends; election timeouts by bounded `nondet()`.
//! spec: Raft's TLA+ (ongardie raft.tla) abstracted to its client-visible safety content: one
//!       invisible `log` thread holding the (single, totally ordered) committed log; a proposal
//!       is either appended or lost; every node front-end delivers the committed log in order.
//! Visible: clients `c0 c1`, `c_i` attached to `n_i`; each proposes `e` values, then receives
//! `Committed(v)` from its node until all its own values have been seen.
//!
//! ARGS: `<variant> <e> <t> [<cand>]`: variant = `correct` (raft-rs as is) or `reorder` (a fault of
//! this harness's delivery layer, not of raft-rs: a node hands a batch of committed entries to its
//! client in reverse order), e = entries per client, t = election timeouts each candidate node may
//! fire, cand = number of nodes allowed to time out (n0..n{cand-1}; default 3).

use std::collections::VecDeque;
use std::sync::Arc;

use raft::eraftpb::{ConfState, EntryType, Message as RMessage};
use raft::storage::MemStorage;
use raft::{Config, RawNode, StateRole, INVALID_ID};
use traceforge::thread::{self, ThreadId};
use traceforge::{nondet, recv_msg_block, send_msg};

type Prog = traceforge::program::Entry;

fn prog<F: Fn() + Send + Sync + 'static>(f: F) -> Prog {
    Arc::new(f)
}

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

const NODES: usize = 3;
const CLIENTS: usize = 2;
const ELECTION_TICK: usize = 2;

/// A raft-rs wire message, carried opaquely between invisible nodes.
#[derive(Clone, PartialEq)]
struct Wire(RMessage);
impl std::fmt::Debug for Wire {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Wire({:?}, {}, {}, {})", self.0.get_msg_type(), self.0.from, self.0.to, self.0.term)
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Setup {
    me: usize,
    /// impl: the nodes, by raft id - 1; spec: `[log]`.
    nodes: Vec<ThreadId>,
    client: Option<ThreadId>,
}

/// Everything a node (impl) or node front-end (spec) receives.
#[derive(Clone, Debug, PartialEq)]
enum NIn {
    Setup(Setup),
    Raft(Wire),
    /// visible: client -> its node.
    Propose(u64),
}

/// visible: node -> its client.
#[derive(Clone, Debug, PartialEq)]
enum CIn {
    Committed(u64),
}

fn value(client: usize, j: usize) -> u64 {
    (1 + client + CLIENTS * j) as u64
}

fn client(i: usize, e: usize, node: ThreadId) {
    let mine: Vec<u64> = (0..e).map(|j| value(i, j)).collect();
    for v in &mine {
        send_msg(node, NIn::Propose(*v));
    }
    let mut seen = 0usize;
    while seen < e {
        let CIn::Committed(v) = recv_msg_block::<CIn>();
        if mine.contains(&v) {
            seen += 1;
        }
    }
}

// ------------------------------------------------------------ impl ----

/// A client's Propose may overtake main's Setup (different senders): buffer it.
/// Likewise a peer's raft message or (spec) a Commit.
fn wait_setup() -> (Setup, VecDeque<NIn>) {
    let mut early = VecDeque::new();
    loop {
        match recv_msg_block::<NIn>() {
            NIn::Setup(s) => return (s, early),
            m => early.push_back(m),
        }
    }
}

fn next(early: &mut VecDeque<NIn>) -> NIn {
    early.pop_front().unwrap_or_else(recv_msg_block::<NIn>)
}

fn raft_node(timeouts: usize) {
    let (setup, mut early) = wait_setup();
    let id = setup.me as u64 + 1;
    let peers: Vec<u64> = (1..=NODES as u64).collect();
    let storage = MemStorage::new_with_conf_state(ConfState::from((peers, vec![])));
    let cfg = Config {
        id,
        election_tick: ELECTION_TICK,
        heartbeat_tick: 1,
        min_election_tick: ELECTION_TICK,
        max_election_tick: ELECTION_TICK + 1, // randomized timeout range [2,3) = {2}
        ..Default::default()
    };
    let logger = slog::Logger::root(slog::Discard, slog::o!());
    let mut rn = RawNode::new(&cfg, storage, &logger).unwrap();
    let mut pending: VecDeque<u64> = VecDeque::new();
    let mut budget = timeouts;
    loop {
        // An election timeout may fire only while no leader is known (a leader's heartbeats
        // would keep resetting it); it is ELECTION_TICK ticks in a row.
        if budget > 0 && rn.raft.leader_id == INVALID_ID && rn.raft.state != StateRole::Leader && nondet() {
            budget -= 1;
            for _ in 0..ELECTION_TICK {
                rn.tick();
                if rn.has_ready() {
                    break;
                }
            }
        } else {
            match next(&mut early) {
                NIn::Raft(Wire(m)) => {
                    let _ = rn.step(m);
                }
                NIn::Propose(v) => pending.push_back(v),
                m => panic!("node: unexpected {m:?}"),
            }
        }
        // Proposals wait at the node until it knows a leader (a client library's retry).
        while let Some(&v) = pending.front() {
            if rn.raft.leader_id == INVALID_ID {
                break;
            }
            match rn.propose(vec![], vec![v as u8]) {
                Ok(()) => {
                    pending.pop_front();
                }
                Err(_) => break,
            }
        }
        on_ready(&mut rn, &setup);
    }
}

fn send_all(msgs: Vec<RMessage>, s: &Setup) {
    for m in msgs {
        let to = s.nodes[(m.to - 1) as usize];
        send_msg(to, NIn::Raft(Wire(m)));
    }
}

fn apply(mut entries: Vec<raft::eraftpb::Entry>, s: &Setup) {
    if REORDER.load(std::sync::atomic::Ordering::Relaxed) {
        entries.reverse(); // the fault: a batch of committed entries delivered in reverse log order
    }
    for e in entries {
        if e.data.is_empty() || e.get_entry_type() != EntryType::EntryNormal {
            continue;
        }
        if let Some(c) = s.client {
            send_msg(c, CIn::Committed(e.data[0] as u64));
        }
    }
}

fn on_ready(rn: &mut RawNode<MemStorage>, s: &Setup) {
    if !rn.has_ready() {
        return;
    }
    let store = rn.raft.raft_log.store.clone();
    let mut rd = rn.ready();
    send_all(rd.take_messages(), s);
    assert!(rd.snapshot().is_empty(), "no snapshots in this harness");
    apply(rd.take_committed_entries(), s);
    store.wl().append(rd.entries()).unwrap();
    if let Some(hs) = rd.hs() {
        store.wl().set_hardstate(hs.clone());
    }
    send_all(rd.take_persisted_messages(), s);
    let mut light = rn.advance(rd);
    if let Some(c) = light.commit_index() {
        store.wl().mut_hard_state().set_commit(c);
    }
    send_all(light.take_messages(), s);
    apply(light.take_committed_entries(), s);
    rn.advance_apply();
}

fn spawn_nodes_clients<F: Fn(usize) -> Box<dyn FnOnce() + Send>>(
    e: usize,
    node: F,
) -> (Vec<ThreadId>, Vec<ThreadId>) {
    // Nodes first, then clients: the ids agree between spec and impl.
    let nodes: Vec<ThreadId> = (0..NODES).map(|i| named(&format!("n{i}"), node(i)).thread().id()).collect();
    let clients: Vec<ThreadId> = (0..CLIENTS)
        .map(|i| {
            let n = nodes[i];
            named(&format!("c{i}"), move || client(i, e, n)).thread().id()
        })
        .collect();
    (nodes, clients)
}

fn send_setups(nodes: &[ThreadId], clients: &[ThreadId], peers: &[ThreadId]) {
    for (i, n) in nodes.iter().enumerate() {
        send_msg(*n, NIn::Setup(Setup { me: i, nodes: peers.to_vec(), client: clients.get(i).copied() }));
    }
}

fn raft_impl(e: usize, t: usize, cand: usize) -> Prog {
    prog(move || {
        let (nodes, clients) = spawn_nodes_clients(e, |i| {
            let t = if i < cand { t } else { 0 };
            Box::new(move || raft_node(t))
        });
        // main performs no message-passing event (SUMMARY-FORMAT.md): an invisible `init` does.
        named("init", move || send_setups(&nodes, &clients, &nodes));
    })
}

// ------------------------------------------------------------ spec ----

/// raft.tla, abstracted to what a client can observe: the committed log is one sequence (State
/// Machine Safety + Log Matching), a `ClientRequest` either ends up committed or is lost with a
/// deposed leader (or never reaches one), and each client learns the committed log in order
/// (its node applies committed entries in index order). One invisible thread `log`; the
/// clients send their proposals to it directly (destinations are not observed).
fn spec_log() {
    let mut clients: Option<Vec<ThreadId>> = None;
    let mut early: VecDeque<NIn> = VecDeque::new();
    loop {
        match recv_msg_block::<NIn>() {
            NIn::Setup(s) => clients = Some(s.nodes),
            m => early.push_back(m),
        }
        if let Some(cs) = clients.as_ref() {
            while let Some(m) = early.pop_front() {
                if let NIn::Propose(v) = m {
                    if nondet() {
                        for c in cs {
                            send_msg(*c, CIn::Committed(v));
                        }
                    }
                }
            }
        }
    }
}

fn raft_spec(e: usize) -> Prog {
    prog(move || {
        let log = named("log", spec_log).thread().id();
        let clients: Vec<ThreadId> =
            (0..CLIENTS).map(|i| named(&format!("c{i}"), move || client(i, e, log)).thread().id()).collect();
        // main performs no message-passing event (SUMMARY-FORMAT.md): an invisible `init` does.
        named("init", move || send_msg(log, NIn::Setup(Setup { me: 0, nodes: clients, client: None })));
    })
}

// ------------------------------------------------------ the arguments ----

/// The `reorder` variant, set once by `main` before any exploration; read by every node's `apply`.
static REORDER: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

struct Args {
    reorder: bool,
    e: usize,
    t: usize,
    cand: usize,
}

fn usage(a: &[String]) -> ! {
    panic!("raft-rs: ARGS are `<variant> <e> <t> [<cand>]` with variant correct|reorder, got {a:?}")
}

fn parse(a: &[String]) -> Args {
    if a.len() != 3 && a.len() != 4 {
        usage(a);
    }
    let reorder = match a[0].as_str() {
        "correct" => false,
        "reorder" => true,
        _ => usage(a),
    };
    let num = |i: usize| -> usize { a[i].parse().unwrap_or_else(|_| usage(a)) };
    Args { reorder, e: num(1), t: num(2), cand: if a.len() == 4 { num(3) } else { NODES } }
}

fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &|_| (0..CLIENTS).map(|i| format!("c{i}")).collect(),
        spec: &|a| raft_spec(parse(a).e),
        imp: &|a| {
            let p = parse(a);
            REORDER.store(p.reorder, std::sync::atomic::Ordering::SeqCst);
            raft_impl(p.e, p.t, p.cand)
        },
    })
}
