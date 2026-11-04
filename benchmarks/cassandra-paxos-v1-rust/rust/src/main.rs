//! Cassandra 4.1 lightweight transactions, Paxos v1 (`v1` and
//! `v1_without_linearizable_reads_or_rejected_writes`), on one partition with one
//! integer register `x`, as a TraceForge Must program. See NOTES.md for the
//! port, method by method, and every decision.
//!
//! Arguments: `<q> <v1|v1nolin> [<fifo|bag>]` (the model itself is chosen by the
//! harness's `--model`; the third argument is accepted and ignored).
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ---------------------------------------------------------------------------
// Interface (verbatim)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum Req {
    Cas(usize, u64, u64),
    Read(usize),
} // client -> replica: (client, expected, value) / (client)

#[derive(Clone, Debug, PartialEq)]
enum Rep {
    CasResult(usize, u64, u64),
    Value(usize, u64),
    Failed(usize),
} // replica -> client: (client, applied 0/1, value) / (client, value) / (client)

#[derive(Clone, Debug, PartialEq)]
struct Setup {
    nodes: Vec<ThreadId>,
    clients: Vec<ThreadId>,
}

const CLIENTS: usize = 2;
fn value(i: usize, j: usize) -> u64 {
    (1 + i + CLIENTS * j) as u64
}
fn client(i: usize, q: usize, node: ThreadId) {
    let mut known: u64 = 0;
    for j in 0..q {
        if (i + j) % 2 == 0 {
            send_msg(node, Req::Cas(i, known, value(i, j)));
        } else {
            send_msg(node, Req::Read(i));
        }
        match recv_msg_block::<Rep>() {
            Rep::CasResult(_, _, v) => known = v,
            Rep::Value(_, v) => known = v,
            Rep::Failed(_) => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Instance constants
// ---------------------------------------------------------------------------

const NODES: usize = 3;
/// SERIAL and QUORUM of RF = 3.
const QUORUM: usize = 2;
/// Attempt budget per request (replaces cas_contention_timeout and the backoff).
const ATTEMPTS: usize = 3;
/// Tag of the Setup message (coordinator side).
const TAG_SETUP: u32 = 1;
/// Response tags are `TAG_RSP0 + seq`, one fresh `seq` per round the coordinator awaits.
const TAG_RSP0: u32 = 2;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Variant {
    /// PaxosVariant.v1 (Paxos.isLinearizable() == true).
    V1,
    /// PaxosVariant.v1_without_linearizable_reads_or_rejected_writes.
    V1NoLin,
}

// ---------------------------------------------------------------------------
// Data types: Ballot, Commit (service/paxos/Ballot.java, Commit.java)
// ---------------------------------------------------------------------------

/// (counter, replica id), ordered lexicographically; `NONE` = Ballot.none().
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Ballot(u64, u64);
const NONE: Ballot = Ballot(0, 0);

/// Commit: a ballot and a partition update, which here is "set x := v" or empty.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Commit {
    ballot: Ballot,
    update: Option<u64>,
}

impl Commit {
    /// Commit.emptyCommit / Accepted.none / Committed.none.
    fn empty() -> Self {
        Commit { ballot: NONE, update: None }
    }
    /// Commit.isAfter(Commit).
    fn is_after(&self, other: &Commit) -> bool {
        self.ballot > other.ballot
    }
    fn is_empty(&self) -> bool {
        self.update.is_none()
    }
}

// ---------------------------------------------------------------------------
// Messages among the replicas (net/Verb.java l.124-129 and the QUORUM read)
// ---------------------------------------------------------------------------

/// Requests to the replica side of a node.
#[derive(Clone, Debug, PartialEq)]
enum NodeReq {
    /// PAXOS_PREPARE_REQ: Commit.newPrepare(key, ballot) (empty update).
    Prepare { from: ThreadId, tag: u32, ballot: Ballot },
    /// PAXOS_PROPOSE_REQ.
    Propose { from: ThreadId, tag: u32, proposal: Commit },
    /// PAXOS_COMMIT_REQ; `ack` is the response tag when the sender waits for the
    /// PAXOS_COMMIT_RSP (commitPaxos), None when it does not (sendCommit).
    Commit { from: ThreadId, ack: Option<u32>, commit: Commit },
    /// READ_REQ of the QUORUM read of x.
    Read { from: ThreadId, tag: u32 },
}

/// Responses from the replica side, sent tagged with the request's tag.
#[derive(Clone, Debug, PartialEq)]
enum NodeRsp {
    /// PAXOS_PREPARE_RSP: PrepareResponse(promised, inProgressCommit, mostRecentCommit), plus the replica's index
    /// (Message.from() in PrepareCallback.onResponse).
    Prepare { replica: usize, promised: bool, in_progress: Commit, most_recent: Commit },
    /// PAXOS_PROPOSE_RSP.
    Propose(bool),
    /// PAXOS_COMMIT_RSP.
    CommitAck,
    /// READ_RSP: x and its write timestamp (the committing ballot), absent row = (0, NONE).
    Read { value: u64, ts: Ballot },
}

// ---------------------------------------------------------------------------
// Replica side (PaxosState, PrepareVerbHandler, ProposeVerbHandler, PaxosCommit.RequestHandler)
// ---------------------------------------------------------------------------

struct ReplicaState {
    index: usize,
    /// Snapshot.promised (promisedWrite is always equal to it in v1: legacyPrepare sets both).
    promised: Ballot,
    /// Snapshot.accepted (null when none or superseded by a commit).
    accepted: Option<Commit>,
    /// Snapshot.committed.
    committed: Commit,
    /// The base table: x and the timestamp of the cell (the commit's ballot).
    data: u64,
    data_ts: Ballot,
}

impl ReplicaState {
    fn new(index: usize) -> Self {
        ReplicaState { index, promised: NONE, accepted: None, committed: Commit::empty(), data: 0, data_ts: NONE }
    }

    /// Snapshot.latestWitnessedOrLowBound(): latest(latest(accepted, committed).ballot, promised).
    fn latest_witnessed(&self) -> Ballot {
        let ac = match self.accepted {
            Some(a) if a.ballot >= self.committed.ballot => a.ballot,
            _ => self.committed.ballot,
        };
        if ac >= self.promised { ac } else { self.promised }
    }

    /// PaxosState.legacyPrepare (l.719-761).
    fn legacy_prepare(&mut self, ballot: Ballot) -> NodeRsp {
        let latest = self.latest_witnessed();
        if ballot > latest {
            let before_accepted = self.accepted;
            self.promised = ballot;
            NodeRsp::Prepare {
                replica: self.index,
                promised: true,
                in_progress: before_accepted.unwrap_or_else(Commit::empty),
                most_recent: self.committed,
            }
        } else {
            NodeRsp::Prepare {
                replica: self.index,
                promised: false,
                in_progress: Commit { ballot: self.promised, update: None },
                most_recent: self.committed,
            }
        }
    }

    /// PaxosState.legacyPropose (l.763-805).
    fn legacy_propose(&mut self, proposal: Commit) -> bool {
        let latest = self.latest_witnessed();
        // Commit.isSameOrAfter(latest)
        if proposal.ballot == latest || proposal.ballot > latest {
            if proposal.ballot != self.committed.ballot {
                self.accepted = Some(proposal);
            }
            true
        } else {
            false
        }
    }

    /// PaxosState.commitDirect / applyCommit (l.664-717) with Snapshot.merge on an UnsafeSnapshot (l.210-243).
    fn commit_direct(&mut self, commit: Commit) {
        // Keyspace.apply(commit.makeMutation()): the cell is last-write-wins by timestamp.
        if let Some(v) = commit.update {
            if commit.ballot > self.data_ts {
                self.data = v;
                self.data_ts = commit.ballot;
            }
        }
        // Committed.latestCommitted(current, new): the new one wins only if strictly later.
        if commit.ballot > self.committed.ballot {
            self.committed = commit;
            // accepted = isAfter(a.accepted, committed) ? a.accepted : null
            if let Some(a) = self.accepted {
                if !(a.ballot > commit.ballot) {
                    self.accepted = None;
                }
            }
        }
    }
}

/// The replica side of node `index`: the verb handlers, forever.
fn replica(index: usize) {
    let mut st = ReplicaState::new(index);
    loop {
        match recv_msg_block::<NodeReq>() {
            NodeReq::Prepare { from, tag, ballot } => {
                let rsp = st.legacy_prepare(ballot);
                send_tagged_msg(from, tag, rsp);
            }
            NodeReq::Propose { from, tag, proposal } => {
                let ok = st.legacy_propose(proposal);
                send_tagged_msg(from, tag, NodeRsp::Propose(ok));
            }
            NodeReq::Commit { from, ack, commit } => {
                st.commit_direct(commit);
                if let Some(tag) = ack {
                    send_tagged_msg(from, tag, NodeRsp::CommitAck);
                }
            }
            NodeReq::Read { from, tag } => {
                send_tagged_msg(from, tag, NodeRsp::Read { value: st.data, ts: st.data_ts });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Coordinator side (StorageProxy legacyCas/doPaxos/beginAndRepairPaxos/..., v1 callbacks)
// ---------------------------------------------------------------------------

/// PrepareCallback after await().
struct PrepareSummary {
    promised: bool,
    most_recent_commit: Commit,
    most_recent_in_progress: Commit,
    commits_by_replica: Vec<(usize, Commit)>,
}

impl PrepareSummary {
    /// PrepareCallback.replicasMissingMostRecentCommit.
    fn replicas_missing_most_recent_commit(&self) -> Vec<usize> {
        self.commits_by_replica
            .iter()
            .filter(|(_, c)| c.ballot != self.most_recent_commit.ballot)
            .map(|(r, _)| *r)
            .collect()
    }
}

enum ProposeOutcome {
    Accepted,
    /// Fully refused (or, with failFast, a quorum no longer reachable).
    Rejected,
    /// backoffIfPartial and accepted by some but not a quorum: CasWriteUnknownResultException.
    Unknown,
}

struct Coordinator {
    index: usize,
    variant: Variant,
    me: ThreadId,
    nodes: Vec<ThreadId>,
    clients: Vec<ThreadId>,
    /// Next response tag offset.
    seq: u32,
    /// Highest ballot counter seen (own ballots and every ballot in prepare responses).
    max_seen: u64,
}

impl Coordinator {
    fn fresh_tag(&mut self) -> u32 {
        let t = TAG_RSP0 + self.seq;
        self.seq += 1;
        t
    }

    /// BallotGenerator/nextBallot replaced: 1 + the highest counter seen, with this node's id.
    fn next_ballot(&mut self) -> Ballot {
        self.max_seen += 1;
        Ballot(self.max_seen, self.index as u64)
    }

    fn see(&mut self, b: Ballot) {
        if b.0 > self.max_seen {
            self.max_seen = b.0;
        }
    }

    fn recv_rsp(tag: u32) -> NodeRsp {
        recv_tagged_msg_block::<_, NodeRsp>(move |_, t| t == Some(tag))
    }

    /// StorageProxy.preparePaxos + PrepareCallback (targets = requiredParticipants = 2).
    fn prepare_paxos(&mut self, ballot: Ballot) -> PrepareSummary {
        let tag = self.fresh_tag();
        for n in 0..NODES {
            send_msg(self.nodes[n], NodeReq::Prepare { from: self.me, tag, ballot });
        }
        let mut s = PrepareSummary {
            promised: true,
            most_recent_commit: Commit::empty(),
            most_recent_in_progress: Commit::empty(),
            commits_by_replica: Vec::new(),
        };
        let mut count = QUORUM;
        while count > 0 {
            let NodeRsp::Prepare { replica, promised, in_progress, most_recent } = Self::recv_rsp(tag) else {
                unreachable!()
            };
            self.see(in_progress.ballot);
            self.see(most_recent.ballot);
            if in_progress.is_after(&s.most_recent_in_progress) {
                s.most_recent_in_progress = in_progress;
            }
            if !promised {
                s.promised = false;
                break;
            }
            s.commits_by_replica.push((replica, most_recent));
            if most_recent.is_after(&s.most_recent_commit) {
                s.most_recent_commit = most_recent;
            }
            count -= 1;
        }
        s
    }

    /// StorageProxy.proposePaxos + ProposeCallback (targets 3, required 2, failFast = !backoffIfPartial).
    fn propose_paxos(&mut self, proposal: Commit, backoff_if_partial: bool) -> ProposeOutcome {
        let tag = self.fresh_tag();
        for n in 0..NODES {
            send_msg(self.nodes[n], NodeReq::Propose { from: self.me, tag, proposal });
        }
        let fail_fast = !backoff_if_partial;
        let mut latch = NODES;
        let mut accepts = 0;
        while latch > 0 {
            let NodeRsp::Propose(ok) = Self::recv_rsp(tag) else { unreachable!() };
            if ok {
                accepts += 1;
            }
            latch -= 1;
            if accepts >= QUORUM || (fail_fast && latch + accepts < QUORUM) {
                break;
            }
        }
        if accepts >= QUORUM {
            ProposeOutcome::Accepted
        } else if backoff_if_partial && !(latch == 0 && accepts == 0) {
            ProposeOutcome::Unknown
        } else {
            ProposeOutcome::Rejected
        }
    }

    /// StorageProxy.commitPaxos at QUORUM (writeAll, block for 2 acks; the local one via commitPaxosLocal).
    fn commit_paxos(&mut self, commit: Commit) {
        let tag = self.fresh_tag();
        for n in 0..NODES {
            send_msg(self.nodes[n], NodeReq::Commit { from: self.me, ack: Some(tag), commit });
        }
        for _ in 0..QUORUM {
            let NodeRsp::CommitAck = Self::recv_rsp(tag) else { unreachable!() };
        }
    }

    /// StorageProxy.sendCommit: no wait.
    fn send_commit(&mut self, commit: Commit, replicas: &[usize]) {
        for &r in replicas {
            send_msg(self.nodes[r], NodeReq::Commit { from: self.me, ack: None, commit });
        }
    }

    /// readOne / fetchRows at QUORUM: the first 2 responses, the latest timestamp wins.
    fn quorum_read(&mut self) -> u64 {
        let tag = self.fresh_tag();
        for n in 0..NODES {
            send_msg(self.nodes[n], NodeReq::Read { from: self.me, tag });
        }
        let mut best: (u64, Ballot) = (0, NONE);
        let mut first = true;
        for _ in 0..QUORUM {
            let NodeRsp::Read { value, ts } = Self::recv_rsp(tag) else { unreachable!() };
            if first || ts > best.1 {
                best = (value, ts);
                first = false;
            }
        }
        best.0
    }

    /// StorageProxy.beginAndRepairPaxos, one iteration: Some(ballot) when it returns the ballot,
    /// None when it would `continue` (one attempt used).
    fn begin_and_repair_once(&mut self) -> Option<Ballot> {
        let ballot = self.next_ballot();
        let summary = self.prepare_paxos(ballot);
        if !summary.promised {
            return None;
        }
        let in_progress = summary.most_recent_in_progress;
        let most_recent = summary.most_recent_commit;
        if !in_progress.is_empty() && in_progress.is_after(&most_recent) {
            let refreshed = Commit { ballot, update: in_progress.update };
            if let ProposeOutcome::Accepted = self.propose_paxos(refreshed, false) {
                self.commit_paxos(refreshed);
            }
            return None;
        }
        let missing = summary.replicas_missing_most_recent_commit();
        if !missing.is_empty() {
            self.send_commit(most_recent, &missing);
            return None;
        }
        Some(ballot)
    }

    /// doPaxos with its createUpdateProposal for the request; the reply to the client.
    fn do_paxos(&mut self, req: &Req) -> Rep {
        let client = match *req {
            Req::Cas(c, _, _) | Req::Read(c) => c,
        };
        for _attempt in 0..ATTEMPTS {
            let Some(ballot) = self.begin_and_repair_once() else { continue };
            // createUpdateProposal(ballot)
            let (proposal, reply) = match *req {
                Req::Cas(c, expected, v) => {
                    // legacyCas: read the current value at QUORUM, check the condition.
                    let current = self.quorum_read();
                    if current != expected {
                        (Commit { ballot, update: None }, Rep::CasResult(c, 0, current))
                    } else {
                        (Commit { ballot, update: Some(v) }, Rep::CasResult(c, 1, v))
                    }
                }
                Req::Read(c) => {
                    // legacyReadWithPaxos l.1891-1894: null (no proposal) unless linearizable.
                    if self.variant == Variant::V1NoLin {
                        let x = self.quorum_read();
                        return Rep::Value(c, x);
                    }
                    (Commit { ballot, update: None }, Rep::Value(c, 0))
                }
            };
            match self.propose_paxos(proposal, true) {
                ProposeOutcome::Accepted => {
                    if !proposal.is_empty() {
                        self.commit_paxos(proposal);
                    }
                    return match reply {
                        // legacyReadWithPaxos: fetchRows at QUORUM after doPaxos.
                        Rep::Value(c, _) => {
                            let x = self.quorum_read();
                            Rep::Value(c, x)
                        }
                        r => r,
                    };
                }
                ProposeOutcome::Unknown => return Rep::Failed(client),
                ProposeOutcome::Rejected => continue,
            }
        }
        Rep::Failed(client)
    }

    fn handle(&mut self, req: Req) {
        let client = match req {
            Req::Cas(c, _, _) | Req::Read(c) => c,
        };
        let rep = self.do_paxos(&req);
        send_msg(self.clients[client], rep);
    }
}

/// Node `index`'s coordinator side (the named thread `n<index>`): Setup, then its client's requests, forever.
fn coordinator(index: usize, variant: Variant) {
    // A request that arrives before Setup stays in the mailbox until Setup is received.
    let setup: Setup = recv_tagged_msg_block(|_, t| t == Some(TAG_SETUP));
    let mut c = Coordinator {
        index,
        variant,
        me: thread::current_id(),
        nodes: setup.nodes,
        clients: setup.clients,
        seq: 0,
        max_seen: 0,
    };
    loop {
        let req: Req = recv_tagged_msg_block(|_, t| t.is_none());
        c.handle(req);
    }
}

// ---------------------------------------------------------------------------
// Program
// ---------------------------------------------------------------------------

fn imp(q: usize, variant: Variant) {
    let mut coords = Vec::new();
    let mut replicas = Vec::new();
    for i in 0..NODES {
        let h = thread::Builder::new()
            .name(format!("n{i}"))
            .spawn(move || coordinator(i, variant))
            .unwrap();
        coords.push(h.thread().id());
        let r = thread::Builder::new()
            .name(format!("n{i}.replica"))
            .spawn(move || replica(i))
            .unwrap();
        replicas.push(r.thread().id());
    }
    let mut clients = Vec::new();
    let mut cids = Vec::new();
    for i in 0..CLIENTS {
        let node = coords[i];
        let h = thread::Builder::new()
            .name(format!("c{i}"))
            .spawn(move || client(i, q, node))
            .unwrap();
        cids.push(h.thread().id());
        clients.push(h);
    }
    let coords2 = coords.clone();
    let init = thread::Builder::new()
        .name("init".to_string())
        .spawn(move || {
            for &n in &coords2 {
                send_tagged_msg(n, TAG_SETUP, Setup { nodes: replicas.clone(), clients: cids.clone() });
            }
        })
        .unwrap();
    for c in clients {
        c.join().unwrap();
    }
    init.join().unwrap();
}

fn parse(args: &[String]) -> (usize, Variant) {
    let q: usize = args.first().and_then(|s| s.parse().ok()).unwrap_or(1);
    let variant = match args.get(1).map(String::as_str) {
        Some("v1nolin") => Variant::V1NoLin,
        Some("v1") | None => Variant::V1,
        Some(v) => panic!("unknown variant `{v}`: v1 or v1nolin"),
    };
    (q.max(1), variant)
}

fn entry(args: &[String]) -> Entry {
    let (q, variant) = parse(args);
    Arc::new(move || imp(q, variant))
}

fn main() -> ExitCode {
    traceforge::program::main(Program {
        visible: &|_| vec!["c0".to_string(), "c1".to_string()],
        spec: &entry,
        imp: &entry,
    })
}
