//! ChainPaxos (github.com/pfouto/chain, cut in plan/case-studies/chainpaxos/code-src) on one
//! integer register, ported to TraceForge's Must API.
//!
//! Fixed chain n0 -> n1 -> n2, n0 the leader for the whole run, quorum 2 of 3, clients c0 (at n0)
//! and c1 (at n2). Variant `mixed` = ChainPaxosMixedProto (reads ordered through the chain),
//! variant `local` = ChainPaxosDelayedProto (reads attached to the next instance at the client's
//! replica, answered when that instance is acknowledged there). See NOTES.md for every decision.
//!
//! Usage: `<bin> count impl [--model fifo|bag] -- <q> <mixed|local> [<fifo|bag>]`.
use std::collections::BTreeMap;
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ------------------------------------------------------------------------------------------
// The interface's fixed parts (INTERFACE.md, verbatim).
// ------------------------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum Req {
    Write(usize, u64),
    Read(usize),
    Rmw(usize),
} // client -> replica: (client index, ...)
#[derive(Clone, Debug, PartialEq)]
enum Rep {
    Ack,
    Value(u64),
    Result(u64),
} // replica -> client

const CLIENTS: usize = 2;
fn value(i: usize, j: usize) -> u64 {
    (1 + i + CLIENTS * j) as u64
}
fn client(i: usize, q: usize, node: ThreadId) {
    for j in 0..q {
        match (i + j) % 3 {
            0 => { send_msg(node, Req::Write(i, value(i, j))); let Rep::Ack = recv_msg_block::<Rep>() else { panic!("Ack expected") }; }
            1 => { send_msg(node, Req::Read(i)); let Rep::Value(_) = recv_msg_block::<Rep>() else { panic!("Value expected") }; }
            _ => { send_msg(node, Req::Rmw(i)); let Rep::Result(_) = recv_msg_block::<Rep>() else { panic!("Result expected") }; }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Setup {
    nodes: Vec<ThreadId>,
    clients: Vec<ThreadId>,
}

// ------------------------------------------------------------------------------------------
// Constants of the instance.
// ------------------------------------------------------------------------------------------

const N: usize = 3;
const QUORUM: u32 = 2;
const LEADER: usize = 0;
/// Tag of the Setup message (init -> replica).
const SETUP_TAG: u32 = 1;
/// Tags of replica -> replica messages: PEER_BASE + per-channel sequence number (TCP FIFO).
const PEER_BASE: u32 = 1000;

/// The replica client `i` is connected to (c0 -> n0, c1 -> n2).
fn replica_of_client(i: usize) -> usize {
    if i == 0 {
        0
    } else {
        2
    }
}
/// The client connected to replica `r`, if any.
fn client_of_replica(r: usize) -> Option<usize> {
    (0..CLIENTS).find(|&i| replica_of_client(i) == r)
}
fn kind(i: usize, j: usize) -> usize {
    (i + j) % 3
}
/// Number of commands of client `i` that are reads.
fn reads_of(i: usize, q: usize) -> usize {
    (0..q).filter(|&j| kind(i, j) == 1).count()
}

// ------------------------------------------------------------------------------------------
// Replica-internal messages and values.
// ------------------------------------------------------------------------------------------

/// One client operation of the register (HashMapOp, one per request: no batching).
#[derive(Clone, Debug, PartialEq)]
enum Op {
    Write(usize, u64),
    Read(usize),
    Rmw(usize),
}

/// PaxosValue: AppOpBatch of one operation (with its issuer replica, OpBatch.issuer) or NoOpValue.
#[derive(Clone, Debug, PartialEq)]
enum PVal {
    App { issuer: usize, op: Op },
    NoOp,
}

#[derive(Clone, Debug, PartialEq)]
enum Peer {
    /// PeerBatchMessage: the front of the issuer replica sends the request to writesTo (the leader).
    Redirect { issuer: usize, op: Op },
    /// AcceptMsg(iN, sN, nodeCounter, value, ack); sN is constant (fixed leader) and omitted.
    Accept { inst: i64, counter: u32, value: PVal, ack: i64 },
    /// AcceptAckMsg(instanceNumber): from the last replica of the chain to the leader.
    AcceptAck { inst: i64 },
}

/// A replica message with its sender's index (Babel's `from` host).
#[derive(Clone, Debug, PartialEq)]
struct Wire {
    from: usize,
    msg: Peer,
}

/// InstanceState (without highestAccept/sN and prepareResponses: fixed leader).
#[derive(Default)]
struct Inst {
    value: Option<PVal>,
    counter: u32,
    decided: bool,
    /// attachedReads (local variant): the clients whose read waits for this instance.
    reads: Vec<usize>,
}

struct Replica {
    me: usize,
    q: usize,
    local: bool,
    nodes: Vec<ThreadId>,
    clients: Vec<ThreadId>,
    // ChainPaxos state
    instances: BTreeMap<i64, Inst>,
    highest_acknowledged: i64,
    highest_accepted: i64,
    highest_decided: i64,
    last_accept_sent: i64,
    next_ok: usize,
    // the register (HashMapApp's store)
    x: u64,
    // per-channel FIFO sequence numbers (TCP)
    send_seq: Vec<u32>,
    recv_seq: Vec<u32>,
    // client front bookkeeping
    my_client: Option<usize>,
    client_received: usize,
    client_replied: usize,
    // receive-choice bookkeeping (see NOTES.md, "Which message is received next")
    redirects_received: usize,
    redirects_sent: usize,
    accepts_seen: [usize; N],
    noops_seen: usize,
    // local variant: no-op budget (leader only)
    noops_left: usize,
    noop_budget: usize,
}

impl Replica {
    fn new(me: usize, q: usize, local: bool, setup: Setup) -> Self {
        let total_reads = (0..CLIENTS).map(|i| reads_of(i, q)).sum();
        Replica {
            me,
            q,
            local,
            nodes: setup.nodes,
            clients: setup.clients,
            instances: BTreeMap::new(),
            highest_acknowledged: -1,
            highest_accepted: -1,
            highest_decided: -1,
            last_accept_sent: -1,
            next_ok: (me + 1) % N, // membership.nextLivingInChain(self)
            x: 0,
            send_seq: vec![0; N],
            recv_seq: vec![0; N],
            my_client: client_of_replica(me),
            client_received: 0,
            client_replied: 0,
            redirects_received: 0,
            redirects_sent: 0,
            accepts_seen: [0; N],
            noops_seen: 0,
            noops_left: if local && me == LEADER { total_reads } else { 0 },
            noop_budget: if local { total_reads } else { 0 },
        }
    }

    fn am_leader(&self) -> bool {
        self.me == LEADER
    }

    fn send_peer(&mut self, to: usize, m: Peer) {
        assert!(to != self.me);
        let tag = PEER_BASE + self.send_seq[to];
        self.send_seq[to] += 1;
        send_tagged_msg(self.nodes[to], tag, Wire { from: self.me, msg: m });
    }

    fn reply(&mut self, client: usize, r: Rep) {
        assert_eq!(Some(client), self.my_client);
        self.client_replied += 1;
        send_msg(self.clients[client], r);
    }

    // ---------------- Membership (fixed) ----------------

    /// Membership.distanceFrom(current, initial)
    fn distance_from(current: usize, initial: usize) -> usize {
        (current + N - initial) % N
    }
    /// Membership.isAfterLeader(me, leader, other)
    fn is_after_leader(me: usize, leader: usize, other: usize) -> bool {
        if me == other {
            return true;
        }
        let mut dist_leader = Self::distance_from(leader, me);
        let dist_other = Self::distance_from(other, me);
        if dist_leader == 0 {
            dist_leader += N;
        }
        dist_other >= dist_leader
    }
    /// Membership.nextNodesUntil(self, h)
    fn next_nodes_until(me: usize, h: usize) -> Vec<usize> {
        let dist = Self::distance_from(h, me);
        (1..=dist).map(|i| (me + i) % N).collect()
    }

    // ---------------- Client front ----------------

    /// A request of the client connected to this replica (ServerHandler.channelRead ->
    /// Front.submitOperation -> sendNewBatch/sendNewReadBatch).
    fn on_client_request(&mut self, r: Req) {
        self.client_received += 1;
        let op = match r {
            Req::Write(c, v) => Op::Write(c, v),
            Req::Read(c) => Op::Read(c),
            Req::Rmw(c) => Op::Rmw(c),
        };
        if self.local {
            if let Op::Read(c) = op {
                // ChainPaxosDelayedFront: STRONG_READ -> SubmitReadRequest -> onSubmitRead
                self.on_submit_read(c);
                return;
            }
        }
        // sendBatchToWritesTo: writesTo = supportedLeader()
        if self.am_leader() {
            self.on_submit_batch(PVal::App { issuer: self.me, op });
        } else {
            self.redirects_sent += 1;
            self.send_peer(LEADER, Peer::Redirect { issuer: self.me, op });
        }
    }

    /// ChainPaxosDelayedProto.onSubmitRead
    fn on_submit_read(&mut self, client: usize) {
        let read_instance = self.highest_accepted + 1;
        let ins = self.instances.entry(read_instance).or_default();
        assert!(!ins.decided);
        ins.reads.push(client);
    }

    /// onSubmitBatch (the leader is always amQuorumLeader)
    fn on_submit_batch(&mut self, v: PVal) {
        assert!(self.am_leader());
        self.send_next_accept(v);
    }

    // ---------------- Protocol ----------------

    /// sendNextAccept
    fn send_next_accept(&mut self, v: PVal) {
        assert!(self.am_leader());
        let inst = self.last_accept_sent + 1;
        {
            let ins = self.instances.entry(inst).or_default();
            assert!(ins.value.is_none());
        }
        let ack = self.highest_acknowledged;
        self.upon_accept(Peer::Accept { inst, counter: 0, value: v, ack });
        self.last_accept_sent = inst;
    }

    /// uponAcceptMsg
    fn upon_accept(&mut self, m: Peer) {
        let Peer::Accept { inst, counter, value, ack } = m else { unreachable!() };
        match &value {
            PVal::App { issuer, .. } => self.accepts_seen[*issuer] += 1,
            PVal::NoOp => self.noops_seen += 1,
        }
        {
            let ins = self.instances.entry(inst).or_default();
            assert!(!ins.decided, "accept for a decided instance");
            ins.value = Some(value);
            ins.counter = counter + 1;
        }
        if self.highest_accepted < inst {
            self.highest_accepted += 1;
            assert_eq!(self.highest_accepted, inst, "gap in accepted instances");
        }
        self.forward(inst);
        let ins = &self.instances[&inst];
        if !ins.decided && ins.counter >= QUORUM {
            self.decide_and_execute(inst);
        }
        self.ack_instance(ack);
    }

    /// forward
    fn forward(&mut self, inst: i64) {
        let (counter, value) = {
            let ins = &self.instances[&inst];
            (ins.counter, ins.value.clone().unwrap())
        };
        for target in Self::next_nodes_until(self.me, self.next_ok) {
            assert!(target != self.me);
            if Self::is_after_leader(self.me, LEADER, target) {
                assert_eq!(target, LEADER);
                assert!(counter >= QUORUM, "last living in chain cannot decide");
                self.send_peer(target, Peer::AcceptAck { inst });
            } else {
                let ack = self.highest_acknowledged;
                self.send_peer(target, Peer::Accept { inst, counter, value: value.clone(), ack });
            }
        }
    }

    /// uponAcceptAckMsg
    fn upon_accept_ack(&mut self, inst: i64) {
        if inst <= self.highest_acknowledged {
            return;
        }
        assert!(self.am_leader(), "received Ack without being leader");
        self.ack_instance(inst);
    }

    /// ackInstance
    fn ack_instance(&mut self, n: i64) {
        // For nodes in the first half of the chain only
        let mut i = self.highest_decided + 1;
        while i <= n {
            assert!(!self.instances[&i].decided);
            self.decide_and_execute(i);
            assert_eq!(self.highest_decided, i);
            i += 1;
        }
        // For everyone
        let mut i = self.highest_acknowledged + 1;
        while i <= n {
            let ins = self.instances.remove(&i).expect("acknowledged instance");
            assert!(ins.decided);
            // ChainPaxosDelayedProto: ExecuteReadReply for the attached reads
            for c in ins.reads {
                let v = self.x;
                self.reply(c, Rep::Value(v));
            }
            self.highest_acknowledged += 1;
            assert_eq!(self.highest_acknowledged, i);
            i += 1;
        }
    }

    /// decideAndExecute (+ Front.onExecuteBatch + HashMapApp.executeOperation)
    fn decide_and_execute(&mut self, inst: i64) {
        assert_eq!(self.highest_decided, inst - 1);
        let value = {
            let ins = self.instances.get_mut(&inst).unwrap();
            assert!(!ins.decided);
            ins.decided = true;
            ins.value.clone().unwrap()
        };
        self.highest_decided += 1;
        if let PVal::App { issuer, op } = value {
            let local = issuer == self.me;
            match op {
                Op::Write(c, v) => {
                    self.x = v;
                    if local {
                        self.reply(c, Rep::Ack);
                    }
                }
                Op::Read(c) => {
                    if local {
                        let v = self.x;
                        self.reply(c, Rep::Value(v));
                    }
                }
                Op::Rmw(c) => {
                    self.x += 10;
                    if local {
                        let v = self.x;
                        self.reply(c, Rep::Result(v));
                    }
                }
            }
        }
    }

    fn on_peer(&mut self, from: usize, m: Peer) {
        match m {
            Peer::Redirect { issuer, op } => {
                assert_eq!(from, issuer);
                self.redirects_received += 1;
                // Front.onPeerBatchMessage -> SubmitBatchRequest -> onSubmitBatch
                self.on_submit_batch(PVal::App { issuer, op });
            }
            m @ Peer::Accept { .. } => self.upon_accept(m),
            Peer::AcceptAck { inst } => self.upon_accept_ack(inst),
        }
    }

    // ---------------- Which message is received next (NOTES.md) ----------------

    /// Instances (Paxos values) client `i` creates over the whole run.
    fn instances_of_client(&self, i: usize) -> usize {
        if self.local {
            self.q - reads_of(i, self.q)
        } else {
            self.q
        }
    }

    /// The connected client's next request will certainly be sent (it has its previous reply).
    fn client_request_due(&self) -> bool {
        self.my_client.is_some()
            && self.client_received == self.client_replied
            && self.client_received < self.q
    }

    /// A replica message to this replica can still arrive without this replica first receiving
    /// its client's next request (exact in `mixed`; an over-approximation in `local`).
    fn peer_message_due(&self) -> bool {
        match self.me {
            0 => {
                self.last_accept_sent > self.highest_acknowledged
                    || self.redirects_received < self.instances_of_client(1)
            }
            2 => {
                self.accepts_seen[0] < self.instances_of_client(0)
                    || self.redirects_sent > self.accepts_seen[2]
                    || self.noops_seen < self.noop_budget
            }
            _ => true,
        }
    }

    fn run(&mut self) -> ! {
        loop {
            // local variant: the NoOp timer, as a nondeterministic choice of the idle leader
            if self.local
                && self.am_leader()
                && self.noops_left > 0
                && self.last_accept_sent == self.highest_acknowledged
                && nondet()
            {
                self.noops_left -= 1;
                self.send_next_accept(PVal::NoOp);
                continue;
            }
            let client_due = self.client_request_due();
            let take_client = if client_due {
                if self.peer_message_due() {
                    nondet()
                } else {
                    true
                }
            } else {
                false
            };
            if take_client {
                let c = self.clients[self.my_client.unwrap()];
                let r: Req = recv_tagged_msg_block(move |tid, tag| tid == c && tag.is_none());
                self.on_client_request(r);
            } else {
                let expect: Vec<(ThreadId, u32)> = (0..N)
                    .filter(|&k| k != self.me)
                    .map(|k| (self.nodes[k], PEER_BASE + self.recv_seq[k]))
                    .collect();
                let Wire { from, msg } = recv_tagged_msg_block::<_, Wire>(move |tid, tag| {
                    expect.iter().any(|(t, s)| *t == tid && tag == Some(*s))
                });
                self.recv_seq[from] += 1;
                self.on_peer(from, msg);
            }
        }
    }
}

fn replica(me: usize, q: usize, local: bool) {
    let setup: Setup = recv_tagged_msg_block(|_, tag| tag == Some(SETUP_TAG));
    let mut r = Replica::new(me, q, local, setup);
    r.run();
}

fn system(q: usize, local: bool) {
    let mut nodes = Vec::new();
    for i in 0..N {
        let h = thread::Builder::new()
            .name(format!("n{i}"))
            .spawn(move || replica(i, q, local))
            .unwrap();
        nodes.push(h);
    }
    let ntids: Vec<ThreadId> = nodes.iter().map(|h| h.thread().id()).collect();
    let mut clients = Vec::new();
    for i in 0..CLIENTS {
        let node = ntids[replica_of_client(i)];
        let h = thread::Builder::new()
            .name(format!("c{i}"))
            .spawn(move || client(i, q, node))
            .unwrap();
        clients.push(h);
    }
    let ctids: Vec<ThreadId> = clients.iter().map(|h| h.thread().id()).collect();
    let init = thread::Builder::new()
        .name("init".to_string())
        .spawn(move || {
            for &n in &ntids {
                send_tagged_msg(n, SETUP_TAG, Setup { nodes: ntids.clone(), clients: ctids.clone() });
            }
        })
        .unwrap();
    init.join().unwrap();
    for c in clients {
        c.join().unwrap();
    }
    // replicas never end: the execution ends with them blocked on receive
}

fn parse(args: &[String]) -> (usize, bool) {
    let q: usize = args
        .first()
        .and_then(|s| s.parse().ok())
        .filter(|&q| q >= 1)
        .expect("usage: <q> <mixed|local> [<fifo|bag>]");
    let local = match args.get(1).map(String::as_str) {
        Some("mixed") => false,
        Some("local") => true,
        _ => panic!("usage: <q> <mixed|local> [<fifo|bag>]"),
    };
    (q, local)
}

fn entry(args: &[String]) -> Entry {
    let (q, local) = parse(args);
    Arc::new(move || system(q, local))
}

fn visible(_args: &[String]) -> Vec<String> {
    (0..CLIENTS).map(|i| format!("c{i}")).collect()
}

fn main() -> ExitCode {
    traceforge::program::main(Program { visible: &visible, spec: &entry, imp: &entry })
}
