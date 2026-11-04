//! ChainPaxos (Fouto, Preguica, Leitao; USENIX ATC 2022), fault-free, fixed chain
//! `n0 -> n1 -> n2` with the fixed leader `n0`, one integer register, written against
//! TraceForge's Must API. Arguments: `<q> <mixed|local> [<fifo|bag>]`.
//! See NOTES.md for the protocol role by role and every decision.
use std::collections::BTreeMap;
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{self, Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ---------------------------------------------------------------------------
// The program interface (INTERFACE.md), verbatim.
// ---------------------------------------------------------------------------

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
    for j in 0..q {
        match (i + j) % 3 {
            0 => { send_msg(node, Req::Write(i, value(i, j))); let Rep::Ack = recv_msg_block::<Rep>() else { panic!("Ack expected") }; }
            1 => { send_msg(node, Req::Read(i)); let Rep::Value(_) = recv_msg_block::<Rep>() else { panic!("Value expected") }; }
            _ => { send_msg(node, Req::Rmw(i)); let Rep::Result(_) = recv_msg_block::<Rep>() else { panic!("Result expected") }; }
        }
    }
}

// ---------------------------------------------------------------------------
// Instance parameters.
// ---------------------------------------------------------------------------

const N: usize = 3; // chain n0 -> n1 -> n2
const LEADER: usize = 0; // n0, the head, leader for the whole run
const SETUP_TAG: u32 = 1;
const INTERNAL_TAG: u32 = 2;

/// The replica client `i` is connected to: c0 -> n0, c1 -> n2 (the tail).
fn front_of(i: usize) -> usize {
    if i == 0 {
        0
    } else {
        N - 1
    }
}

/// Number of Read commands in the whole run (bounds the no-op instances, `local`).
fn reads_in_run(q: usize) -> usize {
    (0..CLIENTS)
        .map(|i| (0..q).filter(|j| (i + j) % 3 == 1).count())
        .sum()
}

// ---------------------------------------------------------------------------
// Operations of the state machine and the replicas' internal messages.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpKind {
    Write = 0,
    Read = 1,
    Rmw = 2,
    NoOp = 3,
}

/// A request as ordered by ChainPaxos: the command, the client that issued it and the
/// client's sequence number (the front's count of that client's requests).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Op {
    kind: OpKind,
    client: usize,
    seq: usize,
    value: u64,
}

const NOOP: Op = Op { kind: OpKind::NoOp, client: 0, seq: 0, value: 0 };

/// Messages among the replicas (Alg. 2 and 3; fault-free subset).
#[derive(Clone, Copy, Debug, PartialEq)]
enum Wire {
    /// <REDIRECT_REQUEST, req>
    Redirect { op: Op },
    /// <ACCEPT, ni, ldr, na, val, nacpts, mack>
    Accept { ni: u64, ldr: usize, na: u64, val: Op, nacpts: u64, mack: u64 },
    /// <ACCEPT_ACK, ni>
    AcceptAck { ni: u64 },
}

// A replica has one mailbox, and a Must receive returns one Rust type: a replica must take
// a client request and a replica message in the order they arrive, so the replicas'
// messages travel as `Req::Write(a, b)` with `a >= MAGIC` (never a client index).
// See NOTES.md, decision D1.
const MAGIC: usize = 1 << 60;

fn field(x: u64, shift: u32, width: u32) -> u64 {
    (x >> shift) & ((1u64 << width) - 1)
}
fn put(v: u64, shift: u32, width: u32) -> u64 {
    assert!(v < (1u64 << width), "field overflow");
    v << shift
}

fn encode_op(op: Op) -> u64 {
    put(op.kind as u64, 48, 4) | put(op.client as u64, 44, 4) | put(op.seq as u64, 32, 12) | put(op.value, 16, 16)
}
fn decode_op(b: u64) -> Op {
    let kind = match field(b, 48, 4) {
        0 => OpKind::Write,
        1 => OpKind::Read,
        2 => OpKind::Rmw,
        _ => OpKind::NoOp,
    };
    Op { kind, client: field(b, 44, 4) as usize, seq: field(b, 32, 12) as usize, value: field(b, 16, 16) }
}

/// a = MAGIC | kind<<56 | from<<52 | chseq<<26 | ni ;
/// b = ldr<<60 | na<<52 | op (bits 16..51) | nacpts<<12 | mack.
fn encode(from: usize, chseq: u64, w: Wire) -> Req {
    let (kind, ni, b) = match w {
        Wire::Redirect { op } => (0u64, 0u64, encode_op(op)),
        Wire::Accept { ni, ldr, na, val, nacpts, mack } => (
            1,
            ni,
            put(ldr as u64, 60, 4) | put(na, 52, 8) | encode_op(val) | put(nacpts, 12, 4) | put(mack, 0, 12),
        ),
        Wire::AcceptAck { ni } => (2, ni, 0),
    };
    let a = put(kind, 56, 4) | put(from as u64, 52, 4) | put(chseq, 26, 26) | put(ni, 0, 26);
    Req::Write(MAGIC | a as usize, b)
}

fn decode(a: usize, b: u64) -> (usize, u64, Wire) {
    let a = (a & !MAGIC) as u64;
    let from = field(a, 52, 4) as usize;
    let chseq = field(a, 26, 26);
    let ni = field(a, 0, 26);
    let w = match field(a, 56, 4) {
        0 => Wire::Redirect { op: decode_op(b) },
        1 => Wire::Accept {
            ni,
            ldr: field(b, 60, 4) as usize,
            na: field(b, 52, 8),
            val: decode_op(b),
            nacpts: field(b, 12, 4),
            mack: field(b, 0, 12),
        },
        _ => Wire::AcceptAck { ni },
    };
    (from, chseq, w)
}

// ---------------------------------------------------------------------------
// The replica (Algorithm 1 state; Algorithms 2 and 3; section 4 for `local`).
// ---------------------------------------------------------------------------

/// PaxosInst : (na, val, nacpts, decided)
struct PaxosInst {
    na: u64,
    val: Op,
    nacpts: u64,
    decided: bool,
}

struct Replica {
    local: bool,
    nodes: Vec<ThreadId>,
    clients: Vec<ThreadId>,
    my_client: Option<usize>,
    // Algorithm 1
    chain: Vec<usize>,
    me: usize, // self
    cnextok: usize,
    csleader: usize,
    npleader: u64,
    inst: BTreeMap<u64, PaxosInst>,
    submitted: Vec<Op>,
    pending: Vec<Op>,
    maxack: u64,
    maxacpt: u64,
    am_leader: bool,
    // the replicated state machine: one register, executed in instance order
    x: u64,
    executed: u64,
    to_exec: BTreeMap<u64, Op>,
    // front: requests received from the client so far
    client_reqs: usize,
    // section 4 (local): reads waiting for the ack of the instance they depend on
    highest_seen: u64,
    acked: u64,
    local_reads: Vec<(usize, u64)>,
    // leader, local: no-op instances still allowed in this run
    noops_left: usize,
    // FIFO channels over the harness's network (decision D2)
    out_seq: Vec<u64>,
    in_next: Vec<u64>,
    in_buf: Vec<BTreeMap<u64, Wire>>,
}

impl Replica {
    fn new(me: usize, q: usize, local: bool, setup: Setup) -> Self {
        let my_client = (0..CLIENTS).find(|&i| front_of(i) == me);
        let chain: Vec<usize> = (0..N).collect();
        Replica {
            local,
            nodes: setup.nodes,
            clients: setup.clients,
            my_client,
            cnextok: chain[(me + 1) % N],
            chain,
            me,
            csleader: LEADER,
            npleader: 0,
            inst: BTreeMap::new(),
            submitted: Vec::new(),
            pending: Vec::new(),
            maxack: 0,
            maxacpt: 0,
            am_leader: me == LEADER,
            x: 0,
            executed: 0,
            to_exec: BTreeMap::new(),
            client_reqs: 0,
            highest_seen: 0,
            acked: 0,
            local_reads: Vec::new(),
            noops_left: if local && me == LEADER { reads_in_run(q) } else { 0 },
            out_seq: vec![0; N],
            in_next: vec![0; N],
            in_buf: (0..N).map(|_| BTreeMap::new()).collect(),
        }
    }

    fn send(&mut self, to: usize, w: Wire) {
        let s = self.out_seq[to];
        self.out_seq[to] += 1;
        send_tagged_msg(self.nodes[to], INTERNAL_TAG, encode(self.me, s, w));
    }

    fn run(mut self) {
        loop {
            // `local`: the NoOp timer, replaced by a nondeterministic choice taken whenever
            // the leader has no pending request (INTERFACE.md).
            if self.am_leader && self.local && self.pending.is_empty() && self.noops_left > 0 && nondet() {
                self.noops_left -= 1;
                self.start_instance(NOOP);
            }
            match recv_msg_block::<Req>() {
                Req::Write(a, b) if a >= MAGIC => {
                    let (from, chseq, w) = decode(a, b);
                    self.deliver(from, chseq, w);
                }
                Req::Write(i, v) => self.new_request(i, OpKind::Write, v),
                Req::Read(i) => self.new_request(i, OpKind::Read, 0),
                Req::Rmw(i) => self.new_request(i, OpKind::Rmw, 0),
            }
            self.serve_local_reads();
        }
    }

    /// FIFO per channel: deliver in send order, buffer what arrives early.
    fn deliver(&mut self, from: usize, chseq: u64, w: Wire) {
        self.in_buf[from].insert(chseq, w);
        while let Some(w) = self.in_buf[from].remove(&self.in_next[from]) {
            self.in_next[from] += 1;
            match w {
                Wire::Redirect { op } => self.on_redirect(op),
                Wire::Accept { ni, ldr, na, val, nacpts, mack } => self.on_accept(ni, ldr, na, val, nacpts, mack),
                Wire::AcceptAck { ni } => self.on_accept_ack(ni),
            }
        }
    }

    /// Alg. 2 line 1: upon receive <NEW_REQUEST, req> from client (the front's role).
    fn new_request(&mut self, i: usize, kind: OpKind, value: u64) {
        assert_eq!(Some(i), self.my_client, "request from a client not connected here");
        let op = Op { kind, client: i, seq: self.client_reqs, value };
        self.client_reqs += 1;
        if self.local && kind == OpKind::Read {
            // Section 4: the read depends on the lowest unseen consensus instance;
            // no information is sent to other nodes.
            let seen = self.highest_seen.max(if self.am_leader { self.maxacpt } else { 0 });
            self.local_reads.push((i, seen + 1));
            return;
        }
        self.submitted.push(op);
        let l = self.csleader;
        self.send(l, Wire::Redirect { op });
    }

    /// Alg. 2 line 5.
    fn on_redirect(&mut self, op: Op) {
        if self.me == self.csleader {
            self.pending.push(op);
            // Batching is off: the leader starts one instance per request on its receipt.
            self.start_instance(op);
        }
    }

    /// Alg. 2 line 9 (val = the one pending request, or NoOp).
    fn start_instance(&mut self, val: Op) {
        self.maxacpt += 1;
        let (ni, me, np, mack) = (self.maxacpt, self.me, self.npleader, self.maxack);
        self.send(me, Wire::Accept { ni, ldr: me, na: np, val, nacpts: 0, mack });
        self.pending.clear();
    }

    /// Alg. 2 line 13.
    fn on_accept(&mut self, ni: u64, ldr: usize, na: u64, val: Op, nacpts: u64, mack: u64) {
        if self.npleader <= na {
            self.update_leader_info(ldr, na);
            match self.inst.get_mut(&ni) {
                Some(i) if i.na >= na => i.nacpts = (nacpts + 1).max(i.nacpts),
                _ => {
                    self.inst.insert(ni, PaxosInst { na, val, nacpts: nacpts + 1, decided: false });
                }
            }
            self.highest_seen = self.highest_seen.max(ni);
            let i = &self.inst[&ni];
            if is_quorum(i.nacpts) && !i.decided {
                self.decide(ni);
            }
            self.decide_and_gc_up_to(mack);
            self.forward(ni);
        }
    }

    /// Alg. 2 line 27.
    fn on_accept_ack(&mut self, ni: u64) {
        self.decide_and_gc_up_to(ni);
    }

    /// Alg. 3 line 7.
    fn forward(&mut self, ni: u64) {
        let i = &self.inst[&ni];
        let (na, val, nacpts) = (i.na, i.val, i.nacpts);
        let next = self.cnextok;
        if next == self.csleader {
            self.send(next, Wire::AcceptAck { ni });
            // The tail sends the accept ack: every node has accepted ni (decision D7).
            self.acked = self.acked.max(ni);
        } else {
            let (ldr, mack) = (self.csleader, self.maxack);
            self.send(next, Wire::Accept { ni, ldr, na, val, nacpts, mack });
        }
    }

    /// Alg. 3 line 12 (fault-free: never fires, npleader stays 0).
    fn update_leader_info(&mut self, leader: usize, np: u64) {
        if self.npleader < np {
            self.am_leader = false;
            self.pending.clear();
            self.csleader = leader;
            self.npleader = np;
            for op in self.submitted.clone() {
                self.send(leader, Wire::Redirect { op });
            }
            self.cnextok = self.chain[(self.me + 1) % N];
        }
    }

    /// Alg. 3 line 22.
    fn decide(&mut self, ni: u64) {
        let i = self.inst.get_mut(&ni).unwrap();
        i.decided = true;
        let val = i.val;
        self.to_exec.insert(ni, val);
        self.execute_ready();
        self.pending.retain(|p| *p != val);
    }

    /// Alg. 3 line 35.
    fn decide_and_gc_up_to(&mut self, upto: u64) {
        let keys: Vec<u64> = self.inst.range(..=upto).map(|(k, _)| *k).collect();
        for k in keys {
            if !self.inst[&k].decided {
                self.decide(k);
            }
            self.inst.remove(&k);
        }
        self.maxack = self.maxack.max(upto); // decision D5
        self.acked = self.acked.max(self.maxack);
    }

    /// SMRExecute, in instance order; the front replies to its client.
    fn execute_ready(&mut self) {
        while let Some(op) = self.to_exec.remove(&(self.executed + 1)) {
            self.executed += 1;
            let rep = match op.kind {
                OpKind::Write => {
                    self.x = op.value;
                    Rep::Ack
                }
                OpKind::Read => Rep::Value(self.x),
                OpKind::Rmw => {
                    self.x += 10;
                    Rep::Result(self.x)
                }
                OpKind::NoOp => continue,
            };
            if let Some(pos) = self.submitted.iter().position(|s| *s == op) {
                self.submitted.remove(pos);
                send_msg(self.clients[op.client], rep);
            }
        }
    }

    /// Section 4: a read is performed in the local committed state once the instance it
    /// depends on is acknowledged.
    fn serve_local_reads(&mut self) {
        let done = self.acked.min(self.executed);
        let mut k = 0;
        while k < self.local_reads.len() {
            let (c, dep) = self.local_reads[k];
            if dep <= done {
                self.local_reads.remove(k);
                send_msg(self.clients[c], Rep::Value(self.x));
            } else {
                k += 1;
            }
        }
    }
}

fn is_quorum(nacpts: u64) -> bool {
    nacpts as usize > N / 2
}

fn replica(me: usize, q: usize, local: bool) {
    // A message that arrives before Setup stays in the mailbox until Setup is taken.
    let setup: Setup = recv_tagged_msg_block(|_, t| t == Some(SETUP_TAG));
    Replica::new(me, q, local, setup).run();
}

// ---------------------------------------------------------------------------
// The program.
// ---------------------------------------------------------------------------

fn imp(q: usize, local: bool) {
    let mut nodes = Vec::new();
    for r in 0..N {
        let h = thread::Builder::new()
            .name(format!("n{r}"))
            .spawn(move || replica(r, q, local))
            .unwrap();
        nodes.push(h.thread().id());
    }
    let mut clients = Vec::new();
    let mut handles = Vec::new();
    for i in 0..CLIENTS {
        let node = nodes[front_of(i)];
        let h = thread::Builder::new()
            .name(format!("c{i}"))
            .spawn(move || client(i, q, node))
            .unwrap();
        clients.push(h.thread().id());
        handles.push(h);
    }
    let setup = Setup { nodes: nodes.clone(), clients };
    let init = thread::Builder::new()
        .name("init".to_string())
        .spawn(move || {
            for n in &setup.nodes {
                send_tagged_msg(*n, SETUP_TAG, setup.clone());
            }
        })
        .unwrap();
    for h in handles {
        h.join().unwrap();
    }
    init.join().unwrap();
}

fn entry(args: &[String]) -> Entry {
    let q: usize = args.first().and_then(|s| s.parse().ok()).expect("usage: <q> <mixed|local> [<fifo|bag>]");
    assert!(q >= 1, "q >= 1");
    let local = match args.get(1).map(String::as_str) {
        Some("mixed") => false,
        Some("local") => true,
        v => panic!("variant must be mixed or local, got {v:?}"),
    };
    Arc::new(move || imp(q, local))
}

fn visible(_: &[String]) -> Vec<String> {
    (0..CLIENTS).map(|i| format!("c{i}")).collect()
}

fn main() -> ExitCode {
    program::main(Program { visible: &visible, spec: &entry, imp: &entry })
}
