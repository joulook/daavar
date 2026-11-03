//! OmniPaxos (haraldng/omnipaxos, unmodified, 9c772dc) under TraceForge's Must API.
//!
//! impl: three invisible node threads `n0 n1 n2`, each owning one sans-I/O
//!       `omnipaxos::OmniPaxos<Val, MemoryStorage<Val>>` (Sequence Paxos + Ballot Leader Election);
//!       mailbox -> `handle_incoming`/`append`, `take_outgoing_messages` -> sends,
//!       `read_decided_suffix` -> `Committed(v)` to the attached client; BLE timeouts by bounded `nondet()`.
//! spec: the client-visible content of a replicated log (the same program as benchmarks/raft-rs):
//!       one invisible `log` thread holding the single, totally ordered decided log; a proposal is
//!       either appended or lost; every client receives the decided log in order.
//! Visible: clients `c0 c1`, `c_i` attached to `n_i`; each proposes `e` values, then receives
//! `Committed(v)` from its node until all its own values have been seen.
//!
//! ARGS: `<variant> <e> <t> [<cand>]`: variant = `correct` (OmniPaxos as is) or `reorder` (a fault of
//! this harness's delivery layer, not of OmniPaxos: a node hands a batch of decided entries to its
//! client in reverse order), e = entries per client, t = election timeouts each candidate node may
//! fire, cand = number of nodes allowed to time out (n0..n{cand-1}; default 3).

use std::collections::VecDeque;
use std::sync::Arc;

use omnipaxos::messages::ballot_leader_election::HeartbeatMsg;
use omnipaxos::messages::Message as OMessage;
use omnipaxos::storage::{Entry, NoSnapshot};
use omnipaxos::util::LogEntry;
use omnipaxos::{ClusterConfig, OmniPaxos, ServerConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;
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

/// The log entry type: one proposed value.
#[derive(Clone, Debug, PartialEq)]
struct Val(u64);

impl Entry for Val {
    type Snapshot = NoSnapshot;
}

/// An OmniPaxos wire message, carried opaquely between invisible nodes. OmniPaxos's `Message`
/// derives `Clone + Debug` but not `PartialEq`; equality is that of the derived Debug rendering
/// (every field of the message).
#[derive(Clone)]
struct Wire(OMessage<Val>);
impl PartialEq for Wire {
    fn eq(&self, o: &Self) -> bool {
        format!("{:?}", self.0) == format!("{:?}", o.0)
    }
}
impl std::fmt::Debug for Wire {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let kind = match &self.0 {
            OMessage::SequencePaxos(p) => {
                let d = format!("{:?}", p.msg);
                d.split(|c: char| !c.is_alphanumeric()).next().unwrap_or("").to_string()
            }
            OMessage::BLE(b) => match b.msg {
                HeartbeatMsg::Request(_) => "HbRequest".to_string(),
                HeartbeatMsg::Reply(_) => "HbReply".to_string(),
            },
        };
        write!(f, "Wire({}, {}, {})", kind, self.0.get_sender(), self.0.get_receiver())
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Setup {
    me: usize,
    /// impl: the nodes, by pid - 1; spec: the clients.
    nodes: Vec<ThreadId>,
    client: Option<ThreadId>,
}

/// Everything a node (impl) or the log (spec) receives.
#[derive(Clone, Debug, PartialEq)]
enum NIn {
    Setup(Setup),
    Paxos(Wire),
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

/// A client's Propose may overtake init's Setup (different senders): buffer it.
/// Likewise a peer's OmniPaxos message.
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

fn op_node(timeouts: usize) {
    let (setup, mut early) = wait_setup();
    let pid = setup.me as u64 + 1;
    let cluster = ClusterConfig {
        configuration_id: 1,
        nodes: (1..=NODES as u64).collect(),
        flexible_quorum: None,
    };
    // Library defaults: election_tick_timeout = 1 (one `tick()` is one BLE heartbeat timeout),
    // resend_message_tick_timeout = 100, flush_batch_tick_timeout = 200, batch_size = 1.
    let server = ServerConfig { pid, ..Default::default() };
    let mut op: OmniPaxos<Val, MemoryStorage<Val>> =
        cluster.build_for_server(server, MemoryStorage::default()).unwrap();
    let mut delivered = 0usize; // decided-log index up to which the client has been served
    let mut budget = timeouts;
    let mut out: Vec<OMessage<Val>> = Vec::new();
    // BLE's first heartbeat round is queued at construction.
    flush(&mut op, &mut out, &setup);
    loop {
        // A BLE timeout may fire only while no leader is known (no promised ballot): a leader's
        // heartbeats would keep the cluster happy.
        if budget > 0 && op.get_current_leader().is_none() && nondet() {
            budget -= 1;
            op.tick();
        } else {
            match next(&mut early) {
                NIn::Paxos(Wire(m)) => op.handle_incoming(m),
                // OmniPaxos buffers or forwards a proposal itself while no leader is in Accept.
                NIn::Propose(v) => op.append(Val(v)).expect("append: no reconfiguration in this harness"),
                m => panic!("node: unexpected {m:?}"),
            }
        }
        flush(&mut op, &mut out, &setup);
        deliver(&op, &mut delivered, &setup);
    }
}

fn flush(op: &mut OmniPaxos<Val, MemoryStorage<Val>>, out: &mut Vec<OMessage<Val>>, s: &Setup) {
    op.take_outgoing_messages(out);
    for m in out.drain(..) {
        let to = s.nodes[(m.get_receiver() - 1) as usize];
        send_msg(to, NIn::Paxos(Wire(m)));
    }
}

fn deliver(op: &OmniPaxos<Val, MemoryStorage<Val>>, delivered: &mut usize, s: &Setup) {
    let Some(entries) = op.read_decided_suffix(*delivered) else {
        return;
    };
    *delivered += entries.len();
    let mut vals: Vec<u64> = entries
        .into_iter()
        .map(|e| match e {
            LogEntry::Decided(Val(v)) => v,
            other => panic!("no snapshots, trims or reconfiguration in this harness: {other:?}"),
        })
        .collect();
    if REORDER.load(std::sync::atomic::Ordering::Relaxed) {
        vals.reverse(); // the fault: a batch of decided entries delivered in reverse log order
    }
    if let Some(c) = s.client {
        for v in vals {
            send_msg(c, CIn::Committed(v));
        }
    }
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

fn op_impl(e: usize, t: usize, cand: usize) -> Prog {
    prog(move || {
        let (nodes, clients) = spawn_nodes_clients(e, |i| {
            let t = if i < cand { t } else { 0 };
            Box::new(move || op_node(t))
        });
        // main performs no message-passing event (SUMMARY-FORMAT.md): an invisible `init` does.
        named("init", move || send_setups(&nodes, &clients, &nodes));
    })
}

// ------------------------------------------------------------ spec ----

/// A replicated log, as a client can observe it: the decided log is one sequence, a proposal
/// either ends up decided or is lost, and each client learns the decided log in order.
/// One invisible thread `log`; the clients send their proposals to it directly (destinations
/// are not observed). The same specification program as benchmarks/raft-rs.
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

fn op_spec(e: usize) -> Prog {
    prog(move || {
        let log = named("log", spec_log).thread().id();
        let clients: Vec<ThreadId> =
            (0..CLIENTS).map(|i| named(&format!("c{i}"), move || client(i, e, log)).thread().id()).collect();
        // main performs no message-passing event (SUMMARY-FORMAT.md): an invisible `init` does.
        named("init", move || send_msg(log, NIn::Setup(Setup { me: 0, nodes: clients, client: None })));
    })
}

// ------------------------------------------------------ the arguments ----

/// The `reorder` variant, set once by `main` before any exploration; read by every node's `deliver`.
static REORDER: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

struct Args {
    reorder: bool,
    e: usize,
    t: usize,
    cand: usize,
}

fn parse(a: &[String]) -> Args {
    let usage = || -> ! { panic!("omnipaxos: ARGS are `<variant> <e> <t> [<cand>]` with variant correct|reorder, got {a:?}") };
    if a.len() != 3 && a.len() != 4 {
        usage();
    }
    let reorder = match a[0].as_str() {
        "correct" => false,
        "reorder" => true,
        _ => usage(),
    };
    let num = |i: usize| -> usize { a[i].parse().unwrap_or_else(|_| usage()) };
    Args { reorder, e: num(1), t: num(2), cand: if a.len() == 4 { num(3) } else { NODES } }
}

fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &|_| (0..CLIENTS).map(|i| format!("c{i}")).collect(),
        spec: &|a| op_spec(parse(a).e),
        imp: &|a| {
            let p = parse(a);
            REORDER.store(p.reorder, std::sync::atomic::Ordering::SeqCst);
            op_impl(p.e, p.t, p.cand)
        },
    })
}
