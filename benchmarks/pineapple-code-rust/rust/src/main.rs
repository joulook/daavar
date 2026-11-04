//! Pineapple (single key), ported from the Go replica (pineapple.go, pineappleproto.go,
//! genericsmr.go) to TraceForge's Must API. See NOTES.md for the role-by-role description
//! and every decision the port takes.
//!
//! Threads: per node i a replica thread `n{i}.replica` (the Go `Run` loop and its handlers)
//! and the node thread `n{i}` (the Go `clientListener`: it takes its client's requests,
//! hands them to the replica as a Propose, and passes the replica's reply back to the
//! client); clients `c0`, `c1`; an invisible `init` thread sending `Setup`.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::process::ExitCode;

use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ---------------------------------------------------------------- interface (verbatim)

#[derive(Clone, Debug, PartialEq)] enum Req { Write(usize, u64), Read(usize), Rmw(usize) }   // client -> node: (client index, ...)
#[derive(Clone, Debug, PartialEq)] enum Rep { Ack, Value(u64), Result(u64) } // node -> client

const CLIENTS: usize = 2;
fn value(i: usize, j: usize) -> u64 { (1 + i + CLIENTS * j) as u64 }
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
struct Setup { nodes: Vec<ThreadId>, clients: Vec<ThreadId> }

// ---------------------------------------------------------------- protocol (pineappleproto.go)

const N: usize = 3;

/// pineappleproto.Tag
#[derive(Clone, Copy, Debug, PartialEq, Default)]
struct Tag { timestamp: i64, id: usize }

/// pineappleproto.Payload (Key omitted: one key)
#[derive(Clone, Copy, Debug, PartialEq, Default)]
struct Payload { tag: Tag, value: u64 }

/// Everything a replica thread receives: Setup, the Propose of its node thread, and the
/// replica-to-replica messages of pineappleproto.go (Key and Command omitted: one key, and
/// the command is not read by the receivers).
#[derive(Clone, Debug, PartialEq)]
enum RMsg {
    Setup(Setup),
    /// genericsmr.Propose: the client's request and the node thread to reply through
    Propose(Req, ThreadId),
    Get { replica_id: usize, instance: i32, write: bool, payload: Payload },
    GetReply { replica_id: usize, instance: i32, ok: bool, write: bool, payload: Payload },
    Set { replica_id: usize, instance: i32, write: bool, payload: Payload },
    SetReply { instance: i32 },
    RmwGet { leader_id: usize, instance: i32, ballot: i32 },
    RmwGetReply { instance: i32, ballot: i32, payload: Payload },
    RmwSet { leader_id: usize, instance: i32, ballot: i32, payload: Payload },
    RmwSetReply { instance: i32, ok: bool, ballot: i32 },
}

/// replica -> its node thread: the reply to pass to the client (ProposeReplyTS on the
/// client's writer).
#[derive(Clone, Debug, PartialEq)]
struct Fwd(ThreadId, Rep);

// ---------------------------------------------------------------- replica (pineapple.go)

#[derive(Clone, Copy, Debug, PartialEq)]
enum Op { Put, GetOp, Rmw }

/// LeaderBookkeeping
#[derive(Debug, Default)]
struct Lb {
    client: Option<(usize, Op, ThreadId)>, // clientProposals[0]: client index, op, reply writer
    has_max_tag: BTreeSet<usize>,
    get_oks: usize,
    set_oks: usize,
    get_done: bool,
    rmw_get_oks: usize,
    rmw_set_oks: usize,
    rmw_get_done: bool,
    completed: bool,
}

/// Instance (cmds, ballot, status are not read on any path that matters here; ballot is 0)
#[derive(Debug, Default)]
struct Instance {
    initial_tag: Tag,
    rmw_id: i32,
    received_rmw: Payload,
    received_data: Vec<(usize, Payload)>, // (ReplicaID, Payload) of the GetReplies
    received_rmw_data: Vec<Payload>,
    ballot: i32,
    lb: Option<Lb>,
}

struct Replica {
    id: usize,
    is_leader: bool,
    nodes: Vec<ThreadId>,
    clients: Vec<ThreadId>,
    data: Option<Payload>, // r.data[key]; None = key not in the map
    instance_space: BTreeMap<i32, Instance>,
    default_ballot: i32,
    crt_instance: i32,
    crt_rmw_id: i32,
    rmw_done_up_to: i32,
    pending_rmws: BTreeMap<i32, i32>, // rmwId -> instance
}

impl Replica {
    /// r.data[key] as Go reads it: the zero Payload when absent
    fn d(&self) -> Payload { self.data.unwrap_or_default() }

    fn peers(&self) -> Vec<usize> {
        // q = Id; q = (q+1) % N for N-1 replicas (all Alive)
        (1..N).map(|k| (self.id + k) % N).collect()
    }

    fn send(&self, q: usize, m: RMsg) { send_msg(self.nodes[q], m); }

    /// isLargerTag (pineapple.go:150)
    fn is_larger_tag(&self, current: Tag, received: Tag) -> bool {
        if received.timestamp > current.timestamp {
            true
        } else if received.timestamp == current.timestamp {
            if current.id == received.id {
                false
            } else if self.is_leader && current.id == self.id {
                true
            } else {
                current.id < received.id
            }
        } else {
            false
        }
    }

    /// The reply sent through the client's writer; its value is the replica's data now.
    fn reply(&self, client: usize, op: Op, port: ThreadId) {
        let rep = match op {
            Op::Put => Rep::Ack,
            Op::GetOp => Rep::Value(self.d().value),
            Op::Rmw => Rep::Result(self.d().value),
        };
        send_msg(port, Fwd(self.clients[client], rep));
    }

    /// replyClient (pineapple.go:168), Dreply = true
    fn reply_client(&mut self, instance: i32) {
        let target = {
            let lb = self.instance_space.get_mut(&instance).unwrap().lb.as_mut().unwrap();
            match lb.client {
                Some(c) if !lb.completed => { lb.completed = true; Some(c) }
                _ => None,
            }
        };
        if let Some((c, op, port)) = target { self.reply(c, op, port); }
    }

    /// bcastGet (pineapple.go:199)
    fn bcast_get(&self, instance: i32, write: bool) {
        let payload = if write { Payload::default() } else { self.d() };
        for q in self.peers() {
            self.send(q, RMsg::Get { replica_id: self.id, instance, write, payload });
        }
    }

    /// handleGet (pineapple.go:233)
    fn handle_get(&mut self, replica_id: usize, instance: i32, write: bool, payload: Payload) {
        let reply_payload = if !write {
            match self.data {
                None => { self.data = Some(payload); payload }
                Some(d) if self.is_larger_tag(d.tag, payload.tag) => { self.data = Some(payload); payload }
                Some(d) => d,
            }
        } else {
            Payload::default()
        };
        self.send(replica_id, RMsg::GetReply { replica_id: self.id, instance, ok: true, write, payload: reply_payload });
    }

    /// handleGetReply (pineapple.go:261)
    fn handle_get_reply(&mut self, replica_id: usize, instance: i32, ok: bool, write: bool, payload: Payload) {
        if self.instance_space[&instance].lb.as_ref().unwrap().get_done { return; }
        self.instance_space.get_mut(&instance).unwrap().received_data.push((replica_id, payload));
        if self.is_larger_tag(self.d().tag, payload.tag) { self.data = Some(payload); }
        if !ok { return; }
        let own_tag = self.d().tag;
        let id = self.id;
        let is_leader = self.is_leader;
        let larger = |cur: Tag, rcv: Tag| -> bool {
            // isLargerTag with this replica's Id and IsLeader (the closure keeps the borrow local)
            if rcv.timestamp > cur.timestamp { true }
            else if rcv.timestamp == cur.timestamp {
                if cur.id == rcv.id { false } else if is_leader && cur.id == id { true } else { cur.id < rcv.id }
            } else { false }
        };
        let inst = self.instance_space.get_mut(&instance).unwrap();
        let lb = inst.lb.as_mut().unwrap();
        lb.get_oks += 1;
        if lb.get_oks + 1 > N >> 1 {
            let mut identical = 0;
            let first = inst.received_data[0].1.tag;
            for (rid, p) in &inst.received_data {
                if p.tag == first { identical += 1; }
                if p.tag == own_tag && !write { lb.has_max_tag.insert(*rid); }
            }
            if inst.initial_tag == first || larger(inst.initial_tag, first) { identical += 1; }
            let count = inst.received_data.len();
            inst.received_data.clear();
            lb.get_done = true;
            if !write && identical == count + 1 {
                self.reply_client(instance);
                return;
            }
            if write {
                let d = self.d();
                let new_tag = Tag { timestamp: d.tag.timestamp + 1, id: self.id };
                // pineapple.go:317: the value is the replica's own, not the client's
                self.data = Some(Payload { tag: new_tag, value: d.value });
            }
            self.bcast_set(instance, write, self.d());
        }
    }

    /// bcastSet (pineapple.go:327)
    fn bcast_set(&self, instance: i32, write: bool, payload: Payload) {
        let lb = self.instance_space[&instance].lb.as_ref().unwrap();
        for q in self.peers() {
            if !write && lb.has_max_tag.contains(&q) { continue; }
            self.send(q, RMsg::Set { replica_id: self.id, instance, write, payload });
        }
    }

    /// handleSet (pineapple.go:368)
    fn handle_set(&mut self, replica_id: usize, instance: i32, payload: Payload) {
        if self.is_larger_tag(self.d().tag, payload.tag) { self.data = Some(payload); }
        self.send(replica_id, RMsg::SetReply { instance });
    }

    /// handleSetReply (pineapple.go:381)
    fn handle_set_reply(&mut self, instance: i32) {
        let lb = self.instance_space.get_mut(&instance).unwrap().lb.as_mut().unwrap();
        lb.set_oks += 1;
        let h = lb.has_max_tag.len();
        if (lb.set_oks + 1 > h && h < N >> 1 && h > 0) || lb.set_oks + 1 > N >> 1 {
            self.reply_client(instance);
        }
    }

    /// bcastRMWGet (pineapple.go:395)
    fn bcast_rmw_get(&self, instance: i32, ballot: i32) {
        for q in self.peers() {
            self.send(q, RMsg::RmwGet { leader_id: self.id, instance, ballot });
        }
    }

    /// handleRMWGet (pineapple.go:422)
    fn handle_rmw_get(&mut self, leader_id: usize, instance: i32, ballot: i32) {
        let payload = match self.instance_space.get(&instance) {
            None => {
                if ballot < self.default_ballot { panic!("outdated ballot received"); }
                self.instance_space.insert(instance, Instance { ballot, ..Default::default() });
                Payload::default() // pineapple.go:438: no Payload
            }
            Some(inst) => {
                if ballot < inst.ballot { panic!("outdated ballot received"); }
                self.d()
            }
        };
        self.send(leader_id, RMsg::RmwGetReply { instance, ballot: self.default_ballot, payload });
    }

    /// handleRMWGetReply (pineapple.go:456)
    fn handle_rmw_get_reply(&mut self, instance: i32, ballot: i32, payload: Payload) {
        let received = {
            let inst = self.instance_space.get_mut(&instance).unwrap();
            let lb = inst.lb.as_mut().unwrap();
            if lb.rmw_get_done { return; }
            inst.received_rmw_data.push(payload);
            lb.rmw_get_oks += 1;
            if lb.rmw_get_oks + 1 <= N >> 1 { return; }
            lb.rmw_get_done = true;
            std::mem::take(&mut inst.received_rmw_data)
        };
        for d in received {
            // pineapple.go:473: the current reply's payload is stored, not d
            if self.is_larger_tag(self.d().tag, d.tag) { self.data = Some(payload); }
        }
        let d = self.d();
        let new_tag = Tag { timestamp: d.tag.timestamp + 1, id: self.id };
        let new_value = d.value + 10; // the interface's f (pineapple.go:483 has + 1)
        self.data = Some(Payload { tag: new_tag, value: new_value });
        self.bcast_rmw_set(instance, ballot);
    }

    /// bcastRMWSet (pineapple.go:496)
    fn bcast_rmw_set(&self, instance: i32, ballot: i32) {
        let payload = self.d();
        for q in self.peers() {
            self.send(q, RMsg::RmwSet { leader_id: self.id, instance, ballot, payload });
        }
    }

    /// handleRMWSet (pineapple.go:526)
    fn handle_rmw_set(&mut self, leader_id: usize, instance: i32, ballot: i32, payload: Payload) {
        let default_ballot = self.default_ballot;
        let inst = match self.instance_space.get_mut(&instance) {
            None => {
                if ballot < default_ballot { panic!("outdated ballot received"); }
                self.instance_space.insert(instance, Instance { ballot, ..Default::default() });
                self.instance_space.get_mut(&instance).unwrap()
            }
            Some(inst) => {
                if inst.ballot > ballot { panic!("outdated ballot received"); }
                if inst.ballot < ballot { inst.ballot = ballot; }
                inst
            }
        };
        inst.received_rmw = payload;
        let received = inst.received_rmw;
        if self.is_larger_tag(self.d().tag, received.tag) { self.data = Some(received); }
        self.send(leader_id, RMsg::RmwSetReply { instance, ok: true, ballot: default_ballot });
    }

    /// handleRMWSetReply (pineapple.go:568) and, at the quorum, the reply of executeRMWs
    /// (pineapple.go:584), here on every replica (see NOTES.md)
    fn handle_rmw_set_reply(&mut self, instance: i32) {
        let done_up_to = self.rmw_done_up_to;
        let inst = self.instance_space.get_mut(&instance).unwrap();
        if inst.rmw_id <= done_up_to { return; }
        let rmw_id = inst.rmw_id;
        let lb = inst.lb.as_mut().unwrap();
        lb.rmw_set_oks += 1;
        if lb.rmw_set_oks + 1 > N >> 1 {
            self.pending_rmws.insert(rmw_id, instance);
            self.rmw_done_up_to += 1;
            self.execute_rmws();
        }
    }

    /// executeRMWs (pineapple.go:584), run at once
    fn execute_rmws(&mut self) {
        for i in 0..=self.rmw_done_up_to {
            let Some(&instance) = self.pending_rmws.get(&i) else { continue };
            self.reply_client(instance);
        }
    }

    /// handlePropose (pineapple.go:610)
    fn handle_propose(&mut self, req: Req, port: ThreadId) {
        while self.instance_space.contains_key(&self.crt_instance) { self.crt_instance += 1; }
        let inst_no = self.crt_instance;
        let (client, op) = match req {
            Req::Write(c, _) => (c, Op::Put),
            Req::Read(c) => (c, Op::GetOp),
            Req::Rmw(c) => (c, Op::Rmw),
        };
        let lb = Lb { client: Some((client, op, port)), ..Default::default() };
        match op {
            Op::Rmw => {
                let rmw_id = self.crt_rmw_id;
                self.crt_rmw_id += 1;
                self.instance_space.insert(inst_no, Instance { rmw_id, ballot: 0, lb: Some(lb), ..Default::default() });
                self.bcast_rmw_get(inst_no, 0);
            }
            Op::Put => {
                self.instance_space.insert(inst_no, Instance { ballot: 0, lb: Some(lb), ..Default::default() });
                self.bcast_get(inst_no, true);
            }
            Op::GetOp => {
                let mut inst = Instance { ballot: 0, lb: Some(lb), ..Default::default() };
                match self.data {
                    None => {
                        let tag = Tag { timestamp: 0, id: self.id };
                        inst.initial_tag = tag;
                        self.data = Some(Payload { tag, value: 0 });
                    }
                    Some(d) => inst.initial_tag = d.tag,
                }
                self.instance_space.insert(inst_no, inst);
                self.bcast_get(inst_no, false);
            }
        }
    }

    fn handle(&mut self, m: RMsg) {
        match m {
            RMsg::Setup(_) => {}
            RMsg::Propose(req, port) => self.handle_propose(req, port),
            RMsg::Get { replica_id, instance, write, payload } => self.handle_get(replica_id, instance, write, payload),
            RMsg::GetReply { replica_id, instance, ok, write, payload } => self.handle_get_reply(replica_id, instance, ok, write, payload),
            RMsg::Set { replica_id, instance, write: _, payload } => self.handle_set(replica_id, instance, payload),
            RMsg::SetReply { instance } => self.handle_set_reply(instance),
            RMsg::RmwGet { leader_id, instance, ballot } => self.handle_rmw_get(leader_id, instance, ballot),
            RMsg::RmwGetReply { instance, ballot, payload } => self.handle_rmw_get_reply(instance, ballot, payload),
            RMsg::RmwSet { leader_id, instance, ballot, payload } => self.handle_rmw_set(leader_id, instance, ballot, payload),
            RMsg::RmwSetReply { instance, ok: _, ballot: _ } => self.handle_rmw_set_reply(instance),
        }
    }
}

/// The replica's Run loop (pineapple.go:711): one receive per message, in arrival order;
/// messages before Setup are buffered.
fn replica(id: usize) {
    let mut buffered = Vec::new();
    let setup = loop {
        match recv_msg_block::<RMsg>() {
            RMsg::Setup(s) => break s,
            m => buffered.push(m),
        }
    };
    let mut r = Replica {
        id,
        is_leader: id == 0,
        nodes: setup.nodes,
        clients: setup.clients,
        data: None,
        instance_space: BTreeMap::new(),
        default_ballot: 0,
        crt_instance: 0,
        crt_rmw_id: 0,
        rmw_done_up_to: -1,
        pending_rmws: BTreeMap::new(),
    };
    for m in buffered { r.handle(m); }
    loop {
        let m = recv_msg_block::<RMsg>();
        r.handle(m);
    }
}

/// The node thread `n{i}`: genericsmr.go's clientListener (ProposeChan <- Propose) and the
/// client's writer (ReplyProposeTS).
fn node(rep: ThreadId) {
    let me = thread::current().id();
    loop {
        let req = recv_tagged_msg_block::<_, Req>(move |t, _| t != rep);
        send_msg(rep, RMsg::Propose(req, me));
        let Fwd(c, r) = recv_tagged_msg_block::<_, Fwd>(move |t, _| t == rep);
        send_msg(c, r);
    }
}

fn imp(q: usize) {
    let mut replicas = Vec::new();
    for i in 0..N {
        let h = thread::Builder::new().name(format!("n{i}.replica")).spawn(move || replica(i)).unwrap();
        replicas.push(h.thread().id());
    }
    let mut nodes = Vec::new();
    for i in 0..N {
        let rep = replicas[i];
        let h = thread::Builder::new().name(format!("n{i}")).spawn(move || node(rep)).unwrap();
        nodes.push(h.thread().id());
    }
    let mut clients = Vec::new();
    let mut ctids = Vec::new();
    for i in 0..CLIENTS {
        let n = nodes[i];
        let h = thread::Builder::new().name(format!("c{i}")).spawn(move || client(i, q, n)).unwrap();
        ctids.push(h.thread().id());
        clients.push(h);
    }
    let setup = Setup { nodes: replicas.clone(), clients: ctids };
    let init = thread::Builder::new().name("init".to_string()).spawn(move || {
        for &r in &setup.nodes { send_msg(r, RMsg::Setup(setup.clone())); }
    }).unwrap();
    for c in clients { c.join().unwrap(); }
    init.join().unwrap();
}

fn q_of(args: &[String]) -> usize {
    args.first().and_then(|s| s.parse().ok()).unwrap_or(1)
}

fn main() -> ExitCode {
    traceforge::program::main(Program {
        visible: &|_args: &[String]| (0..CLIENTS).map(|i| format!("c{i}")).collect(),
        spec: &|args: &[String]| -> Entry { let q = q_of(args); Arc::new(move || imp(q)) },
        imp: &|args: &[String]| -> Entry { let q = q_of(args); Arc::new(move || imp(q)) },
    })
}
