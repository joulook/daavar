//! SwiftPaxos (Ryabinin, Gotsman, Sutra; NSDI 2024), normal operation only (Figures 3 and 4),
//! on one integer register, as a TraceForge Must-API message-passing program.
//! N = 3 replicas n0 (leader of ballot 0), n1, n2; quorum configuration (C2): the unique fast
//! quorum {n0, n1}; slow quorums {n0, n1} and {n0, n2}. K = 2 clients c0, c1 with their client
//! libraries l0, l1 (Figure 3). See NOTES.md for every decision.
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ---------------------------------------------------------------------------------------------
// The interface's visible messages and client code (verbatim).
// ---------------------------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------------------------
// Setup and the invisible protocol messages.
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
struct Setup {
    nodes: Vec<ThreadId>,
    libs: Vec<ThreadId>,
    clients: Vec<ThreadId>,
}

/// Tags: Setup from `init`; every protocol message (Propagate, Reply, FastAck, SlowAck).
/// Client <-> library messages are untagged (the client code above uses `send_msg`).
const TAG_SETUP: u32 = 1;
const TAG_PROTO: u32 = 2;

const N: usize = 3;
const LEADER: usize = 0; // leader(0) = n0
const BALLOT: u64 = 0; // ballot 0 for the whole run

/// A command identifier: (client index, sequence number).
type Id = (usize, usize);
type Deps = BTreeSet<Id>;
type Paths = BTreeSet<Vec<Id>>;

#[derive(Clone, Debug, PartialEq)]
enum Op {
    Write(u64),
    Read,
    Rmw,
}

#[derive(Clone, Debug, PartialEq)]
struct Cmd {
    id: Id,
    op: Op,
}

/// Every two commands conflict except two Reads.
fn conflicts(a: &Op, b: &Op) -> bool {
    !(matches!(a, Op::Read) && matches!(b, Op::Read))
}

/// Applies `op` to the register; returns (result, new state). A Write's result is unused (0).
fn apply(op: &Op, x: u64) -> (u64, u64) {
    match op {
        Op::Write(v) => (0, *v),
        Op::Read => (x, x),
        Op::Rmw => (x + 10, x + 10),
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Proto {
    /// client library -> R (Fig. 3 line 2)
    Propagate { cmd: Cmd },
    /// leader -> client library (Fig. 4 line 18)
    Reply { b: u64, id: Id, paths: Paths, r: u64 },
    /// fast-quorum replica `from` -> R (and the client for a follower) (lines 19, 21)
    FastAck { b: u64, id: Id, deps: Deps, paths: Paths, from: usize },
    /// replica `from` -> R ∪ {client} or -> client (lines 27, 29)
    SlowAck { b: u64, id: Id, from: usize },
}

/// fast(p, 0): the unique fast quorum {n0, n1} of configuration (C2).
fn fast(p: usize) -> bool {
    p == 0 || p == 1
}
/// slow(p, 0) as used at line 25: a slow-quorum replica outside the fast quorum (n2). See NOTES.md.
fn slow_only(p: usize) -> bool {
    !fast(p)
}

fn recv_proto() -> Proto {
    recv_tagged_msg_block::<_, Proto>(|_, t| t == Some(TAG_PROTO))
}
fn recv_setup() -> Setup {
    recv_tagged_msg_block::<_, Setup>(|_, t| t == Some(TAG_SETUP))
}

// ---------------------------------------------------------------------------------------------
// Replica (Figure 4).
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq)]
enum Phase {
    PreAccept,
    Accept,
    Commit,
} // START = absent from the map

struct Replica {
    me: usize,
    setup: Setup,
    bal: u64,
    phase: BTreeMap<Id, Phase>,
    cmd: BTreeMap<Id, Cmd>,
    dep: BTreeMap<Id, Deps>,
    paths: BTreeMap<Id, Paths>,
    exec: BTreeSet<Id>,
    state: u64,
    pending_log: Vec<Cmd>,
    /// Leader FastAcks whose precondition (line 23) does not hold yet, in arrival order.
    leader_acks: Vec<(Id, Deps, Paths)>,
    /// FastAck(b, id, D, _) received from followers: (id, follower) -> D.
    fast_acks: BTreeMap<(Id, usize), Deps>,
    /// SlowAck(b, id) received: (id, follower).
    slow_acks: BTreeSet<(Id, usize)>,
    /// Messages this replica sent to itself (part of "send ... to R").
    local: VecDeque<Proto>,
}

impl Replica {
    fn in_phase(&self, id: &Id, ph: Phase) -> bool {
        self.phase.get(id) == Some(&ph)
    }
    fn accepted_or_committed(&self, id: &Id) -> bool {
        matches!(self.phase.get(id), Some(Phase::Accept) | Some(Phase::Commit))
    }

    /// Sends `m` to every replica (R), the copy to itself delivered locally.
    fn send_r(&mut self, m: Proto) {
        for k in 0..N {
            if k == self.me {
                self.local.push_back(m.clone());
            } else {
                send_tagged_msg(self.setup.nodes[k], TAG_PROTO, m.clone());
            }
        }
    }
    fn send_client(&self, id: Id, m: Proto) {
        send_tagged_msg(self.setup.libs[id.0], TAG_PROTO, m);
    }

    /// pset (lines 37-39); `seen` guards against a cycle (none is expected, see NOTES.md).
    fn pset(&self, id: Id, seen: &mut Vec<Id>) -> Paths {
        let d = self.dep.get(&id).cloned().unwrap_or_default();
        let mut out = Paths::new();
        if d.is_empty() || seen.contains(&id) {
            out.insert(vec![id]);
            return out;
        }
        seen.push(id);
        for id2 in d {
            for l in self.pset(id2, seen) {
                let mut p = vec![id];
                p.extend(l);
                out.insert(p);
            }
        }
        seen.pop();
        out
    }

    /// Lines 6-21: Propagate(c) from the client library.
    fn on_propagate(&mut self, c: Cmd) {
        let id = c.id;
        if self.phase.contains_key(&id) {
            return; // pre: id(c) ∈ Start
        }
        self.phase.insert(id, Phase::PreAccept);
        self.cmd.insert(id, c.clone());
        if fast(self.me) {
            let d: Deps = self
                .cmd
                .iter()
                .filter(|(k, v)| **k != id && conflicts(&v.op, &c.op))
                .map(|(k, _)| *k)
                .collect();
            self.dep.insert(id, d.clone());
            let p = self.pset(id, &mut Vec::new());
            self.paths.insert(id, p.clone());
            if self.me == LEADER {
                self.pending_log.push(c.clone());
                let r = self.opt_exec();
                self.send_client(id, Proto::Reply { b: self.bal, id, paths: p.clone(), r });
                self.send_r(Proto::FastAck { b: self.bal, id, deps: d, paths: p, from: self.me });
            } else {
                let m = Proto::FastAck { b: self.bal, id, deps: d, paths: p, from: self.me };
                self.send_r(m.clone());
                self.send_client(id, m);
            }
        }
    }

    /// opt_exec(pending_log, state): the result of the last command of pending_log when the
    /// log is run in order from the replica's state, without modifying the state.
    fn opt_exec(&self) -> u64 {
        let mut x = self.state;
        let mut r = 0;
        for c in &self.pending_log {
            let (res, nx) = apply(&c.op, x);
            r = res;
            x = nx;
        }
        r
    }

    /// Lines 22-29 for one buffered leader FastAck, if its precondition holds.
    fn try_leader_ack(&mut self, i: usize) -> bool {
        let (id, d, p) = self.leader_acks[i].clone();
        let pre = self.in_phase(&id, Phase::PreAccept)
            && self.bal == BALLOT
            && d.iter().all(|x| self.accepted_or_committed(x));
        if !pre {
            return false;
        }
        self.leader_acks.remove(i);
        self.phase.insert(id, Phase::Accept);
        let me = self.me;
        let mine = self.dep.get(&id).cloned();
        if (fast(me) && mine.as_ref() != Some(&d)) || slow_only(me) {
            self.dep.insert(id, d);
            let m = Proto::SlowAck { b: self.bal, id, from: me };
            self.send_r(m.clone());
            self.send_client(id, m);
        } else if fast(me) && self.paths.get(&id) != Some(&p) {
            self.send_client(id, Proto::SlowAck { b: self.bal, id, from: me });
        }
        true
    }

    /// Lines 30-32: commit `id` if a fast or slow quorum's followers acknowledged it.
    fn try_commit(&mut self, id: Id) -> bool {
        if !self.in_phase(&id, Phase::Accept) || self.bal != BALLOT {
            return false;
        }
        let d = self.dep.get(&id).cloned().unwrap_or_default();
        if !d.iter().all(|x| self.in_phase(x, Phase::Commit)) {
            return false;
        }
        // Fast quorum {n0, n1}: follower n1 sent FastAck with D = dep[id], or SlowAck.
        let fq = self.fast_acks.get(&(id, 1)) == Some(&d) || self.slow_acks.contains(&(id, 1));
        // Slow quorums {n0, n1}, {n0, n2}: SlowAck from n1, or from n2.
        let sq = self.slow_acks.contains(&(id, 1)) || self.slow_acks.contains(&(id, 2));
        if fq || sq {
            self.phase.insert(id, Phase::Commit);
            return true;
        }
        false
    }

    /// Lines 33-36: execute one committed command whose dependencies are executed.
    fn try_exec(&mut self) -> bool {
        let next = self.phase.iter().find(|(id, ph)| {
            **ph == Phase::Commit
                && !self.exec.contains(*id)
                && self.dep.get(*id).map_or(true, |d| d.iter().all(|x| self.exec.contains(x)))
        });
        let Some((&id, _)) = next else { return false };
        let c = self.cmd[&id].clone();
        let (_, nx) = apply(&c.op, self.state);
        self.state = nx;
        self.exec.insert(id);
        if self.me == LEADER {
            self.pending_log.retain(|c| c.id != id);
        }
        true
    }

    fn handle(&mut self, m: Proto) {
        match m {
            Proto::Propagate { cmd } => self.on_propagate(cmd),
            Proto::FastAck { b, id, deps, paths, from } => {
                if b != self.bal {
                    return;
                }
                if from == LEADER {
                    self.leader_acks.push((id, deps, paths));
                } else {
                    self.fast_acks.insert((id, from), deps);
                }
            }
            Proto::SlowAck { b, id, from } => {
                if b == self.bal {
                    self.slow_acks.insert((id, from));
                }
            }
            Proto::Reply { .. } => {}
        }
    }

    /// Runs every enabled handler until none is enabled.
    fn settle(&mut self) {
        loop {
            if let Some(m) = self.local.pop_front() {
                self.handle(m);
                continue;
            }
            let mut progressed = false;
            for i in 0..self.leader_acks.len() {
                if self.try_leader_ack(i) {
                    progressed = true;
                    break;
                }
            }
            if progressed {
                continue;
            }
            let acc: Vec<Id> = self
                .phase
                .iter()
                .filter(|(_, p)| **p == Phase::Accept)
                .map(|(id, _)| *id)
                .collect();
            for id in acc {
                if self.try_commit(id) {
                    progressed = true;
                    break;
                }
            }
            if progressed {
                continue;
            }
            if self.try_exec() {
                continue;
            }
            break;
        }
    }
}

fn replica(me: usize) {
    let setup = recv_setup();
    let mut r = Replica {
        me,
        setup,
        bal: BALLOT,
        phase: BTreeMap::new(),
        cmd: BTreeMap::new(),
        dep: BTreeMap::new(),
        paths: BTreeMap::new(),
        exec: BTreeSet::new(),
        state: 0,
        pending_log: Vec::new(),
        leader_acks: Vec::new(),
        fast_acks: BTreeMap::new(),
        slow_acks: BTreeSet::new(),
        local: VecDeque::new(),
    };
    loop {
        let m = recv_proto();
        r.handle(m);
        r.settle();
    }
}

// ---------------------------------------------------------------------------------------------
// Client library (Figure 3).
// ---------------------------------------------------------------------------------------------

fn library(i: usize) {
    let setup = recv_setup();
    let me_client = setup.clients[i];
    let mut seq = 0usize;
    loop {
        // the next request of client ci (untagged)
        let req = recv_tagged_msg_block::<_, Req>(move |tid, t| t.is_none() && tid == me_client);
        let op = match req {
            Req::Write(_, v) => Op::Write(v),
            Req::Read(_) => Op::Read,
            Req::Rmw(_) => Op::Rmw,
        };
        let id: Id = (i, seq);
        seq += 1;
        // submit(c): send Propagate(c) to R (line 2)
        for k in 0..N {
            send_tagged_msg(setup.nodes[k], TAG_PROTO, Proto::Propagate { cmd: Cmd { id, op: op.clone() } });
        }
        let mut reply: Option<(Paths, u64)> = None;
        let mut fast_paths: BTreeMap<usize, Paths> = BTreeMap::new();
        let mut slow: BTreeSet<usize> = BTreeSet::new();
        let r = loop {
            match recv_proto() {
                Proto::Reply { b, id: x, paths, r } if x == id && b == BALLOT => reply = Some((paths, r)),
                Proto::FastAck { b, id: x, paths, from, .. } if x == id && b == BALLOT => {
                    fast_paths.insert(from, paths);
                }
                Proto::SlowAck { b, id: x, from } if x == id && b == BALLOT => {
                    slow.insert(from);
                }
                _ => {} // a message about an earlier command: dropped
            }
            if let Some((p, r)) = &reply {
                // line 3: fast quorum {n0, n1}: n1's FastAck with matching paths, or its SlowAck
                let fq = fast_paths.get(&1) == Some(p) || slow.contains(&1);
                // line 4: slow quorums {n0, n1}, {n0, n2}: SlowAck from n1 or from n2
                let sq = slow.contains(&1) || slow.contains(&2);
                if fq || sq {
                    break *r;
                }
            }
        };
        // response(id, r) (line 5), answered to ci
        let rep = match op {
            Op::Write(_) => Rep::Ack,
            Op::Read => Rep::Value(r),
            Op::Rmw => Rep::Result(r),
        };
        send_msg(me_client, rep);
    }
}

// ---------------------------------------------------------------------------------------------
// Program entry.
// ---------------------------------------------------------------------------------------------

fn spawn_named<F: FnOnce() + Send + 'static>(name: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(name.to_string()).spawn(f).unwrap()
}

fn swiftpaxos(q: usize) {
    let nodes: Vec<_> = (0..N).map(|k| spawn_named(&format!("n{k}"), move || replica(k))).collect();
    let libs: Vec<_> = (0..CLIENTS).map(|i| spawn_named(&format!("l{i}"), move || library(i))).collect();
    let node_ids: Vec<ThreadId> = nodes.iter().map(|h| h.thread().id()).collect();
    let lib_ids: Vec<ThreadId> = libs.iter().map(|h| h.thread().id()).collect();
    let clients: Vec<_> = (0..CLIENTS)
        .map(|i| {
            let l = lib_ids[i];
            spawn_named(&format!("c{i}"), move || client(i, q, l))
        })
        .collect();
    let client_ids: Vec<ThreadId> = clients.iter().map(|h| h.thread().id()).collect();
    let setup = Setup { nodes: node_ids.clone(), libs: lib_ids.clone(), clients: client_ids };
    let init = spawn_named("init", move || {
        for t in node_ids.iter().chain(lib_ids.iter()) {
            send_tagged_msg(*t, TAG_SETUP, setup.clone());
        }
    });
    for c in clients {
        c.join().unwrap();
    }
    init.join().unwrap();
    // libraries and replicas never terminate; the execution ends with them blocked on receive
}

fn q_of(args: &[String]) -> usize {
    args.first().and_then(|s| s.parse().ok()).unwrap_or(1)
}

fn entry(args: &[String]) -> Entry {
    let q = q_of(args);
    Arc::new(move || swiftpaxos(q))
}

fn main() -> std::process::ExitCode {
    traceforge::program::main(Program {
        visible: &|_| vec!["c0".to_string(), "c1".to_string()],
        spec: &entry,
        imp: &entry,
    })
}
