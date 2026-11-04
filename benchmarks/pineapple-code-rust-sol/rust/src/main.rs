use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::{recv_msg_block, send_msg};

#[derive(Clone, Debug, PartialEq)]
enum Req { Write(usize, u64), Read(usize), Rmw(usize) }
#[derive(Clone, Debug, PartialEq)]
enum Rep { Ack, Value(u64), Result(u64) }

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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Tag { timestamp: u64, id: usize }
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Payload { tag: Tag, value: u64 }

// pineapple.go:147-165: the comparison depends on the receiving replica.
fn larger(id: usize, current: Tag, received: Tag) -> bool {
    received.timestamp > current.timestamp ||
        (received.timestamp == current.timestamp && current.id != received.id &&
            ((id == 0 && current.id == id) || current.id < received.id))
}

#[derive(Clone, Debug, PartialEq)]
enum Protocol {
    Execute,
    Get { replica: usize, instance: usize, write: bool, payload: Payload },
    GetReply { replica: usize, instance: usize, ok: bool, write: bool, payload: Payload },
    Set { replica: usize, instance: usize, write: bool, payload: Payload },
    SetReply { instance: usize },
    RmwGet { leader: usize, instance: usize, ballot: i32, command: Req },
    RmwGetReply { instance: usize, ballot: i32, payload: Payload },
    RmwSet { leader: usize, instance: usize, ballot: i32, command: Req, payload: Payload },
    RmwSetReply { instance: usize, ok: bool, ballot: i32 },
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Status { Preparing, Prepared, Accepted }

struct Bookkeeping {
    proposal: Req,
    initial: Tag,
    has_max: [bool; 3],
    get_oks: usize,
    set_oks: usize,
    get_done: bool,
    received: Vec<(usize, Payload)>,
    rmw_get_oks: usize,
    rmw_set_oks: usize,
    rmw_get_done: bool,
    received_rmw: Vec<Payload>,
    completed: bool,
}
impl Bookkeeping {
    fn new(proposal: Req) -> Self {
        Self { proposal, initial: Tag::default(), has_max: [false; 3],
            get_oks: 0, set_oks: 0, get_done: false, received: Vec::new(),
            rmw_get_oks: 0, rmw_set_oks: 0, rmw_get_done: false,
            received_rmw: Vec::new(), completed: false }
    }
}
struct Instance {
    command: Req,
    ballot: i32,
    status: Status,
    rmw_id: usize,
    received_rmw: Payload,
    lb: Option<Bookkeeping>,
}
impl Instance {
    fn accepted(command: Req, ballot: i32) -> Self {
        Self { command, ballot, status: Status::Accepted, rmw_id: 0,
            received_rmw: Payload::default(), lb: None }
    }
}

struct Node {
    id: usize,
    setup: Setup,
    data: Payload,
    exists: bool,
    instances: BTreeMap<usize, Instance>,
    current: usize,
    next_rmw: usize,
    done_up_to: i64,
    execute_next: usize,
    pending: BTreeMap<usize, usize>,
}
impl Node {
    fn new(id: usize, setup: Setup) -> Self {
        Self { id, setup, data: Payload::default(), exists: false,
            instances: BTreeMap::new(), current: 0, next_rmw: 0,
            done_up_to: -1, execute_next: 0, pending: BTreeMap::new() }
    }
    fn put(&mut self, payload: Payload) { self.data = payload; self.exists = true; }
    fn broadcast(&self, message: Protocol, skip: [bool; 3]) {
        for offset in 1..3 {
            let peer = (self.id + offset) % 3;
            if !skip[peer] { send_msg(self.setup.nodes[peer], message.clone()); }
        }
    }
    fn complete(&mut self, instance: usize) {
        let lb = self.instances.get_mut(&instance).unwrap().lb.as_mut().unwrap();
        if lb.completed { return; }
        lb.completed = true;
        // Go returns NIL; interface replies use current replica data at completion.
        let (client, reply) = match lb.proposal {
            Req::Write(c, _) => (c, Rep::Ack),
            Req::Read(c) => (c, Rep::Value(self.data.value)),
            Req::Rmw(c) => (c, Rep::Result(self.data.value)),
        };
        send_msg(self.setup.clients[client], reply);
    }
    fn execute(&mut self) {
        // One iteration of executeRMWs, scheduled by a local mailbox event.
        let instance = *self.pending.get(&self.execute_next).expect("pending RMW hole");
        self.complete(instance);
        self.execute_next += 1;
    }
    fn propose(&mut self, request: Req) {
        while self.instances.contains_key(&self.current) { self.current += 1; }
        let instance = self.current;
        let mut inst = Instance { command: request.clone(), ballot: 0,
            status: Status::Preparing, rmw_id: 0, received_rmw: Payload::default(),
            lb: Some(Bookkeeping::new(request.clone())) };
        match request {
            Req::Rmw(_) => {
                inst.rmw_id = self.next_rmw;
                self.next_rmw += 1;
                self.instances.insert(instance, inst);
                // No leader forwarding: handlePropose has no IsLeader guard.
                self.broadcast(Protocol::RmwGet { leader: self.id, instance,
                    ballot: 0, command: request }, [false; 3]);
            }
            Req::Write(_, _) => {
                // The source never copies Command.V into data (NOTES.md).
                self.instances.insert(instance, inst);
                self.broadcast(Protocol::Get { replica: self.id, instance,
                    write: true, payload: Payload::default() }, [false; 3]);
            }
            Req::Read(_) => {
                if !self.exists { self.put(Payload { tag: Tag { timestamp: 0, id: self.id }, value: 0 }); }
                inst.lb.as_mut().unwrap().initial = self.data.tag;
                self.instances.insert(instance, inst);
                self.broadcast(Protocol::Get { replica: self.id, instance,
                    write: false, payload: self.data }, [false; 3]);
            }
        }
    }
    fn handle(&mut self, message: Protocol) {
        match message {
            Protocol::Execute => self.execute(),
            Protocol::Get { replica, instance, write, payload } => {
                let response = if write { Payload::default() } else {
                    if !self.exists || larger(self.id, self.data.tag, payload.tag) { self.put(payload); }
                    self.data
                };
                send_msg(self.setup.nodes[replica], Protocol::GetReply {
                    replica: self.id, instance, ok: true, write, payload: response });
            }
            Protocol::GetReply { replica, instance, ok, write, payload } => {
                let lb = self.instances.get_mut(&instance).unwrap().lb.as_mut().unwrap();
                if lb.get_done { return; }
                lb.received.push((replica, payload));
                if larger(self.id, self.data.tag, payload.tag) { self.put(payload); }
                let inst = self.instances.get_mut(&instance).unwrap();
                let lb = inst.lb.as_mut().unwrap();
                if !ok { return; }
                lb.get_oks += 1;
                if lb.get_oks + 1 <= 3 >> 1 { return; }
                let first = lb.received[0].1.tag;
                let mut identical = 0;
                for &(peer, data) in &lb.received {
                    if data.tag == first { identical += 1; }
                    if !write && data.tag == self.data.tag { lb.has_max[peer] = true; }
                }
                if lb.initial == first || larger(self.id, lb.initial, first) { identical += 1; }
                let count = lb.received.len();
                lb.received.clear();
                lb.get_done = true;
                if !write && identical == count + 1 { self.complete(instance); return; }
                inst.status = Status::Prepared;
                let skip = if write { [false; 3] } else { lb.has_max };
                if write { self.put(Payload { tag: Tag { timestamp: self.data.tag.timestamp + 1, id: self.id }, value: self.data.value }); }
                self.broadcast(Protocol::Set { replica: self.id, instance, write, payload: self.data }, skip);
            }
            Protocol::Set { replica, instance, write: _, payload } => {
                if larger(self.id, self.data.tag, payload.tag) { self.put(payload); }
                send_msg(self.setup.nodes[replica], Protocol::SetReply { instance });
            }
            Protocol::SetReply { instance } => {
                let lb = self.instances.get_mut(&instance).unwrap().lb.as_mut().unwrap();
                lb.set_oks += 1;
                let max_count = lb.has_max.iter().filter(|&&b| b).count();
                if (lb.set_oks + 1 > max_count && max_count < 3 >> 1 && max_count > 0)
                    || lb.set_oks + 1 > 3 >> 1 { self.complete(instance); }
            }
            Protocol::RmwGet { leader, instance, ballot, command } => {
                let payload = if let Some(inst) = self.instances.get_mut(&instance) {
                    assert!(ballot >= inst.ballot, "outdated ballot received");
                    inst.command = command;
                    inst.status = Status::Accepted;
                    self.data
                } else {
                    assert!(ballot >= 0, "outdated ballot received");
                    self.instances.insert(instance, Instance::accepted(command, ballot));
                    // Deliberately omit stored data for an unused instance.
                    Payload::default()
                };
                send_msg(self.setup.nodes[leader], Protocol::RmwGetReply { instance, ballot: 0, payload });
            }
            Protocol::RmwGetReply { instance, ballot, payload } => {
                let lb = self.instances.get_mut(&instance).unwrap().lb.as_mut().unwrap();
                if lb.rmw_get_done { return; }
                lb.received_rmw.push(payload);
                lb.rmw_get_oks += 1;
                if lb.rmw_get_oks + 1 <= 3 >> 1 { return; }
                for data in &lb.received_rmw {
                    if larger(self.id, self.data.tag, data.tag) { self.data = payload; self.exists = true; }
                }
                lb.received_rmw.clear();
                lb.rmw_get_done = true;
                self.put(Payload { tag: Tag { timestamp: self.data.tag.timestamp + 1, id: self.id },
                    value: self.data.value + 10 });
                self.broadcast(Protocol::RmwSet { leader: self.id, instance, ballot,
                    command: self.instances[&instance].command.clone(), payload: self.data }, [false; 3]);
            }
            Protocol::RmwSet { leader, instance, ballot, command, payload } => {
                let inst = self.instances.entry(instance).or_insert_with(|| {
                    assert!(ballot >= 0, "outdated ballot received");
                    Instance::accepted(command.clone(), ballot)
                });
                assert!(ballot >= inst.ballot, "outdated ballot received");
                inst.command = command;
                inst.ballot = ballot;
                inst.status = Status::Accepted;
                inst.received_rmw = payload;
                if larger(self.id, self.data.tag, payload.tag) { self.put(payload); }
                send_msg(self.setup.nodes[leader], Protocol::RmwSetReply { instance, ok: true, ballot: 0 });
            }
            Protocol::RmwSetReply { instance, ok: _, ballot: _ } => {
                let inst = self.instances.get_mut(&instance).unwrap();
                if inst.rmw_id as i64 <= self.done_up_to { return; }
                let lb = inst.lb.as_mut().unwrap();
                lb.rmw_set_oks += 1;
                if lb.rmw_set_oks + 1 > 3 >> 1 {
                    self.pending.insert(inst.rmw_id, instance);
                    self.done_up_to += 1;
                    send_msg(self.setup.nodes[self.id], Protocol::Execute);
                }
            }
        }
    }
}

// A node's mailbox is heterogeneous because the interface's clients send Req
// verbatim. The Must inbox primitive with bounds (1,1) receives exactly one
// message, retaining the harness's FIFO/bag delivery choice. See NOTES.md.
enum Incoming { Setup(Setup), Request(Req), Protocol(Protocol) }
fn receive() -> Incoming {
    let mut messages = traceforge::inbox_with_bounds(1, Some(1));
    let message = messages.pop().flatten().expect("one blocking mailbox message");
    if message.type_name == std::any::type_name::<Req>() {
        Incoming::Request(*message.as_any().downcast::<Req>().unwrap())
    } else if message.type_name == std::any::type_name::<Setup>() {
        Incoming::Setup(*message.as_any().downcast::<Setup>().unwrap())
    } else {
        Incoming::Protocol(*message.as_any().downcast::<Protocol>().unwrap())
    }
}
fn node(id: usize) {
    let mut buffered = VecDeque::new();
    let setup = loop {
        match receive() {
            Incoming::Setup(setup) => break setup,
            other => buffered.push_back(other),
        }
    };
    let mut node = Node::new(id, setup);
    loop {
        let message = buffered.pop_front().unwrap_or_else(receive);
        match message {
            Incoming::Request(request) => node.propose(request),
            Incoming::Protocol(message) => node.handle(message),
            Incoming::Setup(_) => panic!("duplicate Setup"),
        }
    }
}

fn implementation(q: usize) {
    let mut nodes = Vec::new();
    for i in 0..3 {
        nodes.push(thread::Builder::new().name(format!("n{i}")).spawn(move || node(i)).unwrap());
    }
    let addresses: Vec<_> = nodes.iter().map(|n| n.thread().id()).collect();
    let mut clients = Vec::new();
    for (i, &node) in addresses.iter().take(CLIENTS).enumerate() {
        clients.push(thread::Builder::new().name(format!("c{i}")).spawn(move || client(i, q, node)).unwrap());
    }
    let setup = Setup { nodes: addresses, clients: clients.iter().map(|c| c.thread().id()).collect() };
    let init = thread::Builder::new().name("init".to_owned()).spawn(move || {
        for &node in &setup.nodes { send_msg(node, setup.clone()); }
    }).unwrap();
    init.join().unwrap();
    for client in clients { client.join().unwrap(); }
    for node in nodes { node.join().unwrap(); }
}
fn entry(args: &[String]) -> Entry {
    assert!(!args.is_empty() && args.len() <= 2, "expected <q> [fifo|bag]");
    let q: usize = args[0].parse().expect("q must be an integer");
    assert!(q >= 1, "q must be positive");
    if let Some(model) = args.get(1) { assert!(model == "fifo" || model == "bag", "expected fifo or bag"); }
    Arc::new(move || implementation(q))
}
fn main() -> std::process::ExitCode {
    traceforge::program::main(Program {
        visible: &|_| vec!["c0".to_owned(), "c1".to_owned()],
        spec: &entry,
        imp: &entry,
    })
}
