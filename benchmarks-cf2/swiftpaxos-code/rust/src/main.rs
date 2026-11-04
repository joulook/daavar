//! SwiftPaxos (code port): the prototype's ballot-0 normal operation with leader n0, three
//! replicas, fast quorum {n0, n1}, one integer register, written against TraceForge's Must API.
//! See NOTES.md for the protocol role by role and every decision (with the Go source lines).
//! Usage (shared program main): `<bin> count impl --model fifo -- <q> [<fifo|bag>]`.
use std::collections::{BTreeMap, BTreeSet};
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ---------------------------------------------------------------------------
// Fixed interface (INTERFACE.md): client <-> library messages and the client code.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum Req {
    Write(usize, u64),
    Read(usize),
    Rmw(usize),
} // client -> library: (client index, ...)
#[derive(Clone, Debug, PartialEq)]
enum Rep {
    Ack,
    Value(u64),
    Result(u64),
} // library -> client

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

/// Setup sent by the invisible `init` thread to every replica and library.
#[derive(Clone, Debug, PartialEq)]
struct Setup {
    nodes: Vec<ThreadId>,
    libs: Vec<ThreadId>,
    clients: Vec<ThreadId>,
}

// ---------------------------------------------------------------------------
// Protocol constants and internal messages.
// ---------------------------------------------------------------------------

const N: usize = 3; // replicas n0, n1, n2
const LEADER: usize = 0; // leader(0) = n0, ballot 0 for the whole run
const FAST_QUORUM: [usize; 2] = [0, 1]; // configuration (C2): the unique fast quorum {n0, n1}

const TAG_SETUP: u32 = 1; // Setup (to replicas and libraries)
const TAG_REPLICA: u32 = 2; // everything a replica receives after Setup
const TAG_LIB_BASE: u32 = 100; // replica -> library messages of command (i, seq): tag 100 + seq

// phases (defs.go:25-30)
const START: u8 = 0;
const PRE_ACCEPT: u8 = 1;
const ACCEPT: u8 = 2;
const COMMIT: u8 = 3;

/// CommandId (defs.go:36-39): (client, sequence number).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct CmdId {
    client: usize,
    seq: u32,
}

/// The register's commands. Write and Rmw play the prototype's PUT, Read its GET.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Op {
    Write(u64),
    Read,
    Rmw,
}
impl Op {
    fn is_put(&self) -> bool {
        !matches!(self, Op::Read)
    }
}

/// SHash (dpath.go:11-13): the prototype's chained SHA-256 of command ids; represented by the
/// chain itself (the sequence of command ids folded into it), so equality of two SHash values
/// is equality of the chains (collision-free hashing).
type SHash = Vec<CmdId>;

/// Messages received by replicas (tag TAG_REPLICA).
#[derive(Clone, Debug, PartialEq)]
enum RMsg {
    /// Propagate(c): GPropose from a client library (replica.go:514-523).
    Propose { cmd: CmdId, op: Op },
    /// MFastAck between replicas (defs.go:53-60; sent as a one-element MOptAcks, batcher.go).
    FastAck { replica: usize, cmd: CmdId, dep: Vec<CmdId>, checksum: Vec<SHash>, seqnum: usize },
    /// MLightSlowAck between replicas (defs.go:77-81; NilDep entry of MOptAcks).
    LightSlowAck { replica: usize, cmd: CmdId },
}

/// Messages received by client libraries (tag TAG_LIB_BASE + seq).
#[derive(Clone, Debug, PartialEq)]
enum LMsg {
    /// MReply from the leader (defs.go:101-107, replychan.go:20-28).
    Reply { replica: usize, cmd: CmdId, checksum: Vec<SHash>, rep: u64 },
    /// MFastAckClient (defs.go:62-67, batcher.go:52-59, 122-125).
    FastAckClient { replica: usize, cmd: CmdId, checksum: Vec<SHash> },
    /// MLightSlowAck to the client (batcher.go:126-129).
    LightSlowAck { replica: usize, cmd: CmdId },
}

/// The in-memory MFastAck used by the message sets (replica and client side).
/// `dep == None` is Go's nil Dep (a LightSlowAck turned into a FastAck).
#[derive(Clone, Debug)]
struct Fa {
    replica: usize,
    cmd: CmdId,
    dep: Option<Vec<CmdId>>,
    checksum: Vec<SHash>,
    seqnum: usize,
}

// ---------------------------------------------------------------------------
// Quorums and message sets (replica/quorum.go, replica/mset.go).
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum Quorum {
    /// Majority (quorum.go:301-313): size N/2+1, contains every replica.
    Majority(usize),
    /// An explicit quorum (quorum.go:329-361).
    Set(Vec<usize>),
}
impl Quorum {
    fn size(&self) -> usize {
        match self {
            Quorum::Majority(s) => *s,
            Quorum::Set(v) => v.len(),
        }
    }
    fn contains(&self, r: usize) -> bool {
        match self {
            Quorum::Majority(_) => true,
            Quorum::Set(v) => v.contains(&r),
        }
    }
}

type Accept = fn(&Fa, Option<&Fa>) -> bool;

/// MsgSet (mset.go:3-87). `add` returns the leader's message when the handler would be called
/// with a non-nil leader message (the handlers return at once on a nil leader message).
#[derive(Clone, Debug)]
struct MsgSet {
    q: Quorum,
    msgs: BTreeMap<usize, Fa>,
    leader: Option<Fa>,
}
impl MsgSet {
    fn new(q: Quorum) -> Self {
        MsgSet { q, msgs: BTreeMap::new(), leader: None }
    }
    fn add(&mut self, rep: usize, is_leader: bool, msg: Fa, accept: Accept) -> Option<Fa> {
        if !self.q.contains(rep) {
            return None;
        }
        if self.leader.is_some() && self.msgs.contains_key(&rep) {
            return None;
        }
        if is_leader {
            self.leader = Some(msg);
            let l = self.leader.clone();
            self.msgs.retain(|_, m| accept(m, l.as_ref()));
        } else if accept(&msg, self.leader.as_ref()) {
            self.msgs.insert(rep, msg);
        }
        let n = self.msgs.len();
        let s = self.q.size();
        if n >= s || (n + 1 >= s && self.leader.is_some()) {
            self.leader.clone()
        } else {
            None
        }
    }
}

/// Dep.Equals (defs.go:196-224): equality up to order.
fn dep_equals(a: &[CmdId], b: &[CmdId]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut x = a.to_vec();
    let mut y = b.to_vec();
    x.sort();
    y.sort();
    x == y
}

/// SHashesEq (dpath.go:165-184).
fn shashes_eq(a: &[SHash], b: &[SHash]) -> bool {
    a.len() == b.len() && a.iter().all(|h| b.contains(h))
}

/// acceptFastAndSlowAck of the replica (swift.go:665-673).
fn replica_accept(msg: &Fa, leader: Option<&Fa>) -> bool {
    match leader {
        None => true,
        Some(l) => msg.dep.is_none() || dep_equals(l.dep.as_deref().unwrap_or(&[]), msg.dep.as_deref().unwrap_or(&[])),
    }
}

/// accept of the client side (client.go:80-100), without the slow-path statistics.
fn client_accept(msg: &Fa, leader: Option<&Fa>) -> bool {
    match leader {
        None => true,
        Some(l) => shashes_eq(&l.checksum, &msg.checksum),
    }
}

// ---------------------------------------------------------------------------
// Dependencies and hash log (key.go lightKeyInfo, dpath.go HashLog) on the single key.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
struct KeyInfo {
    last_write: Vec<CmdId>,
    last_cmd: Vec<CmdId>,
}
impl KeyInfo {
    // key.go:482-488
    fn add(&mut self, op: Op, cmd: CmdId) {
        self.last_cmd = vec![cmd];
        if op.is_put() {
            self.last_write = vec![cmd];
        }
    }
    // key.go:500-506
    fn conflict_cmds(&self, op: Op) -> Vec<CmdId> {
        if !op.is_put() { self.last_write.clone() } else { self.last_cmd.clone() }
    }
}

#[derive(Clone, Debug)]
struct HashLog {
    synced: i64,
    synced_hash: SHash,
    pending: Vec<CmdId>, // the pending linked list, head first
    hash: SHash,
    pending_upd: BTreeMap<CmdId, (usize, SHash)>,
}
impl HashLog {
    fn new() -> Self {
        HashLog { synced: -1, synced_hash: vec![], pending: vec![], hash: vec![], pending_upd: BTreeMap::new() }
    }
    // dpath.go:60-88
    fn append(&mut self, cmd: CmdId) -> SHash {
        if let Some((s, h)) = self.pending_upd.remove(&cmd) {
            self.update(cmd, s, h.clone(), false);
            return h;
        }
        if self.pending.is_empty() {
            self.pending.push(cmd);
            let mut h = self.synced_hash.clone();
            h.push(cmd);
            self.hash = h;
            return self.hash.clone();
        }
        self.pending.push(cmd);
        self.hash.push(cmd);
        self.hash.clone()
    }
    // dpath.go:100-143
    fn update(&mut self, cmd: CmdId, s: usize, h: SHash, save: bool) {
        let pos = self.pending.iter().position(|c| *c == cmd);
        let exists = pos.is_some();
        if !exists && save {
            self.pending_upd.insert(cmd, (s, h));
            return;
        }
        if let Some(p) = pos {
            self.pending.remove(p);
        }
        if (s as i64) <= self.synced && !exists {
            return;
        }
        if (s as i64) > self.synced {
            self.synced = s as i64;
            self.synced_hash = h;
        }
        let mut initial = self.synced_hash.clone();
        initial.extend(self.pending.iter().copied());
        self.hash = initial;
    }
}

// ---------------------------------------------------------------------------
// Replica (swift.go).
// ---------------------------------------------------------------------------

/// commandDesc (swift.go:66-89), the fields that matter for ballot 0.
#[derive(Clone, Debug)]
struct Desc {
    phase: u8,
    dep: Vec<CmdId>,
    hs: Vec<SHash>,
    propose: Option<Op>, // desc.propose / desc.cmd
    propose_dep: Option<Vec<CmdId>>,
    slow_h: MsgSet,
    fast_h: MsgSet,
    /// afterPropagate (hook.OptCondF): the leader's FastAck waiting for the proposal.
    after_propagate: Option<Fa>,
    slow_path: bool,
    successors: Vec<CmdId>,
}
impl Desc {
    // newDesc (swift.go:634-687)
    fn new() -> Self {
        Desc {
            phase: START,
            dep: vec![],
            hs: vec![],
            propose: None,
            propose_dep: None,
            slow_h: MsgSet::new(Quorum::Majority(N / 2 + 1)),
            fast_h: MsgSet::new(Quorum::Set(FAST_QUORUM.to_vec())),
            after_propagate: None,
            slow_path: false,
            successors: vec![],
        }
    }
}

struct Replica {
    id: usize,
    setup: Setup,
    fq: Quorum,
    sq: Quorum,
    descs: BTreeMap<CmdId, Desc>,
    delivered: BTreeSet<CmdId>,
    keys: Option<KeyInfo>,
    hlog: Option<HashLog>,
    seqnum: usize,
    pending_hash_upds: BTreeMap<CmdId, (usize, Vec<SHash>)>,
    proposes: BTreeSet<CmdId>,
    x: u64, // the register
}

impl Replica {
    fn new(id: usize, setup: Setup) -> Self {
        Replica {
            id,
            setup,
            fq: Quorum::Set(FAST_QUORUM.to_vec()),
            sq: Quorum::Majority(N / 2 + 1),
            descs: BTreeMap::new(),
            delivered: BTreeSet::new(),
            keys: None,
            hlog: None,
            seqnum: 0,
            pending_hash_upds: BTreeMap::new(),
            proposes: BTreeSet::new(),
            x: 0,
        }
    }

    fn lib_of(&self, cmd: CmdId) -> ThreadId {
        self.setup.libs[cmd.client]
    }
    fn send_lib(&self, cmd: CmdId, m: LMsg) {
        send_tagged_msg(self.lib_of(cmd), TAG_LIB_BASE + cmd.seq, m);
    }
    /// sendToAll (sender.go:149-159): every other replica, in index order.
    fn send_all(&self, m: RMsg) {
        for p in 0..N {
            if p != self.id {
                send_tagged_msg(self.setup.nodes[p], TAG_REPLICA, m.clone());
            }
        }
    }
    fn send_fast_ack_all(&self, fa: &Fa) {
        self.send_all(RMsg::FastAck {
            replica: fa.replica,
            cmd: fa.cmd,
            dep: fa.dep.clone().unwrap_or_default(),
            checksum: fa.checksum.clone(),
            seqnum: fa.seqnum,
        });
    }

    /// The run loop's cases (swift.go:271-342).
    fn handle(&mut self, m: RMsg) {
        match m {
            RMsg::Propose { cmd, op } => self.on_propose(cmd, op),
            RMsg::FastAck { replica, cmd, dep, checksum, seqnum } => {
                // swift.go:321-341 (MOptAcks entry with a real dependency set)
                let fa = Fa { replica, cmd, dep: Some(dep), checksum, seqnum };
                if replica == LEADER && self.id != LEADER {
                    self.record_leader_hash(cmd, seqnum, fa.checksum.clone());
                }
                if self.get_desc_seq(cmd, None, None) {
                    self.handle_fast_ack(fa);
                }
            }
            RMsg::LightSlowAck { replica, cmd } => {
                // swift.go:321-341 (MOptAcks entry with NilDep: Dep = nil)
                let fa = Fa { replica, cmd, dep: None, checksum: vec![], seqnum: 0 };
                if replica == LEADER && self.id != LEADER {
                    self.record_leader_hash(cmd, 0, vec![]);
                }
                if self.get_desc_seq(cmd, None, None) {
                    self.handle_fast_ack(fa);
                }
            }
        }
    }

    // swift.go:274-289
    fn on_propose(&mut self, cmd: CmdId, op: Op) {
        self.proposes.insert(cmd);
        let (dep, hs) = self.get_dep_and_hashes(op, cmd);
        if self.id != LEADER {
            if let Some((s, h)) = self.pending_hash_upds.remove(&cmd) {
                self.record_leader_hash(cmd, s, h);
            }
        }
        if !self.get_desc_seq(cmd, Some(dep), Some(hs)) {
            panic!("Got proposal for the delivered command {:?}", cmd);
        }
        self.handle_propose(cmd, op);
    }

    /// getCmdDescSeq (swift.go:589-632) without the message dispatch: false when delivered.
    fn get_desc_seq(&mut self, cmd: CmdId, dep: Option<Vec<CmdId>>, hs: Option<Vec<SHash>>) -> bool {
        if self.delivered.contains(&cmd) {
            return false;
        }
        let d = self.descs.entry(cmd).or_insert_with(Desc::new);
        if let Some(dep) = dep {
            if d.propose_dep.is_none() {
                d.propose_dep = Some(dep);
                if let Some(hs) = hs {
                    d.hs = hs;
                }
            }
        }
        true
    }

    fn desc(&mut self, cmd: CmdId) -> &mut Desc {
        self.descs.get_mut(&cmd).expect("descriptor")
    }

    /// afterPropagate.Recall(): run the stored leader FastAck if the proposal is there.
    fn recall(&mut self, cmd: CmdId) -> bool {
        let d = self.desc(cmd);
        if d.propose.is_some() {
            if let Some(fa) = d.after_propagate.take() {
                self.run_fast_ack_from_leader(cmd, fa);
                return true;
            }
        }
        false
    }

    // handlePropose (swift.go:355-407), optExec = true, fastRead = false, no slowAddrs
    fn handle_propose(&mut self, cmd: CmdId, op: Op) {
        if self.desc(cmd).propose.is_some() {
            return;
        }
        self.desc(cmd).propose = Some(op);
        if !self.fq.contains(self.id) {
            self.recall(cmd);
            return;
        }
        {
            let d = self.desc(cmd);
            d.dep = d.propose_dep.clone().unwrap_or_default();
            d.phase = PRE_ACCEPT;
        }
        let recalled = self.recall(cmd);
        if (recalled && self.desc(cmd).slow_path) || self.delivered.contains(&cmd) {
            return;
        }
        let (dep, hs) = {
            let d = self.desc(cmd);
            (d.dep.clone(), d.hs.clone())
        };
        let mut fa = Fa { replica: self.id, cmd, dep: Some(dep), checksum: hs, seqnum: 0 };
        if self.id == LEADER {
            fa.seqnum = self.seqnum;
            self.seqnum += 1;
        }
        if self.id == LEADER {
            // batcher.SendFastAck: to all replicas; then optimistic execution
            self.send_fast_ack_all(&fa);
            self.deliver(cmd);
        } else {
            // batcher.SendFastAckClient: FastAckClient to the client, FastAck to all replicas
            self.send_lib(cmd, LMsg::FastAckClient { replica: fa.replica, cmd, checksum: fa.checksum.clone() });
            self.send_fast_ack_all(&fa);
        }
        self.handle_fast_ack(fa);
    }

    // swift.go:409-415
    fn handle_fast_ack(&mut self, fa: Fa) {
        if fa.replica == LEADER {
            self.fast_ack_from_leader(fa);
        } else {
            self.common_case_fast_ack(fa);
        }
    }

    // swift.go:417-418: afterPropagate.Call
    fn fast_ack_from_leader(&mut self, fa: Fa) {
        let cmd = fa.cmd;
        if self.desc(cmd).propose.is_some() {
            self.run_fast_ack_from_leader(cmd, fa);
        } else {
            self.desc(cmd).after_propagate = Some(fa);
        }
    }

    // the body of fastAckFromLeader (swift.go:419-481)
    fn run_fast_ack_from_leader(&mut self, cmd: CmdId, fa: Fa) {
        let leader_dep = fa.dep.clone().unwrap_or_default();
        let (hs, neq) = {
            let d = self.desc(cmd);
            d.phase = ACCEPT;
            (d.hs.clone(), !dep_equals(&d.dep, &leader_dep))
        };
        let fast = self.fq.contains(self.id);
        let slow = self.sq.contains(self.id);
        let send_slow_ack = LEADER != self.id && (slow || (fast && neq));
        let msg_checksum = fa.checksum.clone();

        if let Some(l) = self.desc(cmd).slow_h.add(fa.replica, true, fa.clone(), replica_accept) {
            self.commit(cmd, l);
        }
        let mut delivered = self.delivered.contains(&cmd);
        if !delivered {
            if let Some(l) = self.desc(cmd).fast_h.add(fa.replica, true, fa.clone(), replica_accept) {
                self.commit(cmd, l);
            }
            delivered = self.delivered.contains(&cmd);
        }

        if send_slow_ack {
            {
                let d = self.desc(cmd);
                if neq && !delivered {
                    d.dep = leader_dep.clone();
                }
                d.slow_path = true;
            }
            // batcher.SendLightSlowAckClient: to the client, then to all replicas
            self.send_lib(cmd, LMsg::LightSlowAck { replica: self.id, cmd });
            self.send_all(RMsg::LightSlowAck { replica: self.id, cmd });
            if !delivered {
                self.handle_light_slow_ack(self.id, cmd);
            }
        }

        // the deferred function (swift.go:438-451)
        if LEADER == self.id || self.delivered.contains(&cmd) {
            return;
        }
        if !send_slow_ack && !shashes_eq(&hs, &msg_checksum) {
            self.send_lib(cmd, LMsg::LightSlowAck { replica: self.id, cmd });
        }
    }

    // swift.go:484-497
    fn common_case_fast_ack(&mut self, fa: Fa) {
        let cmd = fa.cmd;
        let is_leader = fa.replica == LEADER;
        if fa.dep.is_none() {
            if let Some(l) = self.desc(cmd).slow_h.add(fa.replica, is_leader, fa.clone(), replica_accept) {
                self.commit(cmd, l);
            }
            if self.delivered.contains(&cmd) {
                return;
            }
        }
        if let Some(l) = self.desc(cmd).fast_h.add(fa.replica, is_leader, fa, replica_accept) {
            self.commit(cmd, l);
        }
    }

    // swift.go:524-531
    fn handle_light_slow_ack(&mut self, replica: usize, cmd: CmdId) {
        self.common_case_fast_ack(Fa { replica, cmd, dep: None, checksum: vec![], seqnum: 0 });
    }

    // getFastAndSlowAcksHandler (swift.go:499-522)
    fn commit(&mut self, cmd: CmdId, leader_fa: Fa) {
        let dep = {
            let d = self.desc(cmd);
            d.phase = COMMIT;
            d.dep.clone()
        };
        for dc in dep {
            if self.delivered.contains(&dc) {
                continue;
            }
            self.descs.entry(dc).or_insert_with(Desc::new).successors.push(leader_fa.cmd);
        }
        self.deliver(leader_fa.cmd);
    }

    // deliver (swift.go:533-583), Exec = Dreply = optExec = true
    fn deliver(&mut self, cmd: CmdId) {
        let Some(d) = self.descs.get(&cmd) else { return };
        let Some(op) = d.propose else { return };
        if self.delivered.contains(&cmd) {
            return;
        }
        if d.phase != COMMIT && self.id != LEADER {
            return;
        }
        if d.dep.iter().any(|c| !self.delivered.contains(c)) {
            return;
        }
        let successors = d.successors.clone();
        let hs = d.hs.clone();
        self.delivered.insert(cmd);
        let v = self.execute(op);
        // replyChan (replychan.go:20-28): only the leader replies when optExec
        if self.id == LEADER {
            self.send_lib(cmd, LMsg::Reply { replica: self.id, cmd, checksum: hs, rep: v });
        }
        // successors: the "deliver" messages of deliverChan (swift.go:271-272, 558-565)
        for s in successors {
            if self.get_desc_seq(s, None, None) {
                self.deliver(s);
            }
        }
    }

    fn execute(&mut self, op: Op) -> u64 {
        match op {
            Op::Write(v) => {
                self.x = v;
                0
            }
            Op::Read => self.x,
            Op::Rmw => {
                self.x += 10;
                self.x
            }
        }
    }

    // getDepAndHashes (swift.go:767-792), the light key info on the single key
    fn get_dep_and_hashes(&mut self, op: Op, cmd: CmdId) -> (Vec<CmdId>, Vec<SHash>) {
        let dep = match &self.keys {
            Some(ki) => ki.conflict_cmds(op),
            None => {
                self.keys = Some(KeyInfo::default());
                vec![]
            }
        };
        self.keys.as_mut().unwrap().add(op, cmd);
        let l = self.hlog.get_or_insert_with(HashLog::new);
        let h = l.append(cmd);
        (dep, vec![h])
    }

    // recordLeaderHash / updateLogs (swift.go:794-821)
    fn record_leader_hash(&mut self, cmd: CmdId, s: usize, hs: Vec<SHash>) {
        if !self.proposes.contains(&cmd) {
            self.pending_hash_upds.insert(cmd, (s, hs));
            return;
        }
        if hs.len() != 1 {
            panic!("the number of hashes does not match number of objects");
        }
        let l = self.hlog.get_or_insert_with(HashLog::new);
        l.update(cmd, s, hs[0].clone(), true);
    }
}

fn replica(id: usize) {
    let setup: Setup = recv_tagged_msg_block(|_, t| t == Some(TAG_SETUP));
    let mut r = Replica::new(id, setup);
    loop {
        let m: RMsg = recv_tagged_msg_block(|_, t| t == Some(TAG_REPLICA));
        r.handle(m);
    }
}

// ---------------------------------------------------------------------------
// Client library (swift/client.go), one command at a time.
// ---------------------------------------------------------------------------

fn library(i: usize) {
    let setup: Setup = recv_tagged_msg_block(|_, t| t == Some(TAG_SETUP));
    let client_tid = setup.clients[i];
    let mut seq: u32 = 0;
    loop {
        let req: Req = recv_tagged_msg_block(move |tid, t| tid == client_tid && t.is_none());
        let op = match req {
            Req::Write(_, v) => Op::Write(v),
            Req::Read(_) => Op::Read,
            Req::Rmw(_) => Op::Rmw,
        };
        seq += 1; // client.go:198-199 (c.seqnum++)
        let cmd = CmdId { client: i, seq };
        // SendProposal with Fast = true: to every replica, in index order (client.go:186-194)
        for r in 0..N {
            send_tagged_msg(setup.nodes[r], TAG_REPLICA, RMsg::Propose { cmd, op });
        }
        // initMsgSets (client.go:70-115): SQ = FQ = Majority(3) = 2 (fixedMajority)
        let mut fast = MsgSet::new(Quorum::Majority(N / 2 + 1));
        let mut slow = MsgSet::new(Quorum::Majority(N / 2 + 1));
        let mut val: u64 = 0; // c.val
        let mut done = false; // c.delivered[cmd]
        let tag = TAG_LIB_BASE + seq;
        while !done {
            let m: LMsg = recv_tagged_msg_block(move |_, t| t == Some(tag));
            match m {
                // handleReply (client.go:243-260)
                LMsg::Reply { replica, cmd: c, checksum, rep } => {
                    if c != cmd {
                        continue;
                    }
                    let f = Fa { replica, cmd, dep: None, checksum, seqnum: 0 };
                    val = rep;
                    if fast.add(replica, true, f.clone(), client_accept).is_some() {
                        done = true;
                    }
                    if !done {
                        let mut f2 = f;
                        f2.checksum = vec![];
                        if slow.add(replica, true, f2, client_accept).is_some() {
                            done = true;
                        }
                    }
                }
                // fastAckClientChan case + handleFastAck (client.go:132-139, 186-202)
                LMsg::FastAckClient { replica, cmd: c, checksum } => {
                    if c != cmd {
                        continue;
                    }
                    let f = Fa { replica, cmd, dep: None, checksum, seqnum: 0 };
                    if fast.add(replica, false, f, client_accept).is_some() {
                        done = true;
                    }
                }
                // handleLightSlowAck (client.go:204-226)
                LMsg::LightSlowAck { replica, cmd: c } => {
                    if c != cmd {
                        continue;
                    }
                    let f = Fa { replica, cmd, dep: None, checksum: vec![], seqnum: 0 };
                    if fast.add(replica, false, f.clone(), client_accept).is_some() {
                        done = true;
                    }
                    if !done && slow.add(replica, false, f, client_accept).is_some() {
                        done = true;
                    }
                }
            }
        }
        // handleFastAndSlowAcks -> RegisterReply(c.val): answer the client
        let rep = match op {
            Op::Write(_) => Rep::Ack,
            Op::Read => Rep::Value(val),
            Op::Rmw => Rep::Result(val),
        };
        send_msg(client_tid, rep);
    }
}

// ---------------------------------------------------------------------------
// Program entry.
// ---------------------------------------------------------------------------

fn spawn_named<F: FnOnce() + Send + 'static>(name: String, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(name).spawn(f).unwrap()
}

fn implementation(q: usize) {
    let nodes: Vec<_> = (0..N).map(|r| spawn_named(format!("n{r}"), move || replica(r))).collect();
    let libs: Vec<_> = (0..CLIENTS).map(|i| spawn_named(format!("l{i}"), move || library(i))).collect();
    let clients: Vec<_> = (0..CLIENTS)
        .map(|i| {
            let node = libs[i].thread().id();
            spawn_named(format!("c{i}"), move || client(i, q, node))
        })
        .collect();
    let setup = Setup {
        nodes: nodes.iter().map(|h| h.thread().id()).collect(),
        libs: libs.iter().map(|h| h.thread().id()).collect(),
        clients: clients.iter().map(|h| h.thread().id()).collect(),
    };
    let init = spawn_named("init".to_string(), move || {
        for n in &setup.nodes {
            send_tagged_msg(*n, TAG_SETUP, setup.clone());
        }
        for l in &setup.libs {
            send_tagged_msg(*l, TAG_SETUP, setup.clone());
        }
    });
    for c in clients {
        c.join().unwrap();
    }
    init.join().unwrap();
    for l in libs {
        l.join().unwrap();
    }
    for n in nodes {
        n.join().unwrap();
    }
}

fn q_of(args: &[String]) -> usize {
    let q = args.first().and_then(|s| s.parse().ok()).unwrap_or(1);
    assert!(q >= 1, "q >= 1");
    q
}

fn entry(args: &[String]) -> Entry {
    let q = q_of(args);
    Arc::new(move || implementation(q))
}

fn visible(_args: &[String]) -> Vec<String> {
    (0..CLIENTS).map(|i| format!("c{i}")).collect()
}

fn main() -> ExitCode {
    traceforge::program::main(Program { visible: &visible, spec: &entry, imp: &entry })
}
