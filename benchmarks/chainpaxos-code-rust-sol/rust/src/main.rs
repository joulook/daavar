use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

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
#[derive(Clone, Debug, PartialEq)]
struct Operation { origin: usize, sequence: usize, request: Req }
#[derive(Clone, Debug, PartialEq)]
enum LogValue { App(Operation), NoOp }
#[derive(Clone, Debug, PartialEq)]
struct Accept { instance: usize, counter: usize, value: LogValue, ack: usize }
#[derive(Clone, Debug, PartialEq)]
enum Internal {
    Redirect(Operation), Accept(Accept), AcceptAck(usize),
    ReadOpen(usize, usize), ReadReady(usize, usize, u64, usize), ExecuteRead(usize, u64),
}
#[derive(Default)]
struct Instance { value: Option<LogValue>, reads: Vec<(usize, usize)>, decided: bool }

struct Replica {
    index: usize, local: bool, setup: Setup, serial: u32,
    // Prefix lengths: source instance i is Rust instance i+1; zero is source -1.
    accepted: usize, decided: usize, acknowledged: usize, sent: usize,
    instances: BTreeMap<usize, Instance>, arrivals: BTreeMap<usize, Accept>,
    x: u64, sequence: usize,
    read_open: BTreeSet<(usize, usize)>, read_closed: BTreeSet<(usize, usize)>,
    noop_used: BTreeSet<(usize, usize)>,
}
impl Replica {
    // A typed payload plus an untagged wake frame allows one heterogeneous
    // mailbox without changing the externally prescribed Req enum.
    fn send(&mut self, target: usize, message: Internal) {
        self.serial = self.serial.checked_add(1).expect("transport tag overflow");
        assert!(self.serial < u32::MAX);
        send_tagged_msg(self.setup.nodes[target], self.serial, message);
        send_msg(self.setup.nodes[target], Req::Write(usize::MAX - self.index, self.serial as u64));
    }
    fn submit(&mut self, value: LogValue) {
        assert_eq!(self.index, 0);
        self.sent += 1;
        self.accept(Accept { instance: self.sent, counter: 0, value, ack: self.acknowledged });
    }
    fn execute(&mut self, instance: usize) {
        assert_eq!(instance, self.decided + 1);
        let state = self.instances.get_mut(&instance).unwrap();
        assert!(!state.decided);
        state.decided = true;
        self.decided = instance;
        if let LogValue::App(op) = state.value.clone().unwrap() {
            let (ci, reply) = match op.request {
                Req::Write(ci, v) => { self.x = v; (ci, Rep::Ack) }
                Req::Read(ci) => (ci, Rep::Value(self.x)),
                Req::Rmw(ci) => { self.x += 10; (ci, Rep::Result(self.x)) }
            };
            if op.origin == self.index { send_msg(self.setup.clients[ci], reply); }
        }
    }
    fn ack(&mut self, upto: usize) {
        assert!(upto <= self.accepted);
        while self.decided < upto { self.execute(self.decided + 1); }
        while self.acknowledged < upto {
            self.acknowledged += 1;
            let state = self.instances.remove(&self.acknowledged).unwrap();
            assert!(state.decided);
            for (ci, seq) in state.reads {
                if self.index == 0 {
                    self.read_closed.insert((ci, seq));
                    send_msg(self.setup.clients[ci], Rep::Value(self.x));
                } else {
                    // Retire the read's no-op credit before its front replies.
                    self.send(0, Internal::ReadReady(ci, seq, self.x, self.index));
                }
            }
        }
    }
    fn accept(&mut self, message: Accept) {
        if message.instance <= self.accepted { return; }
        self.arrivals.insert(message.instance, message);
        // The Java TCP handlers assume in-order ACCEPTs. Preserve their
        // contiguous-instance precondition also under bag delivery.
        while let Some(msg) = self.arrivals.remove(&(self.accepted + 1)) {
            self.accepted = msg.instance;
            let count = msg.counter + 1;
            self.instances.entry(msg.instance).or_default().value = Some(msg.value.clone());
            if self.index == 2 {
                assert!(count >= 2);
                self.send(0, Internal::AcceptAck(msg.instance));
            } else {
                self.send(self.index + 1, Internal::Accept(Accept {
                    instance: msg.instance, counter: count, value: msg.value,
                    ack: self.acknowledged,
                }));
            }
            if count >= 2 { self.execute(msg.instance); }
            self.ack(msg.ack);
        }
    }
    fn request(&mut self, request: Req) {
        let sequence = self.sequence;
        self.sequence += 1;
        if self.local {
            if let Req::Read(ci) = request {
                self.instances.entry(self.accepted + 1).or_default().reads.push((ci, sequence));
                if self.index == 0 { self.read_open.insert((ci, sequence)); }
                else { self.send(0, Internal::ReadOpen(ci, sequence)); }
                return;
            }
        }
        let op = Operation { origin: self.index, sequence, request };
        if self.index == 0 { self.submit(LogValue::App(op)); }
        else { self.send(0, Internal::Redirect(op)); }
    }
    fn internal(&mut self, msg: Internal) {
        match msg {
            Internal::Redirect(op) => self.submit(LogValue::App(op)),
            Internal::Accept(msg) => self.accept(msg),
            Internal::AcceptAck(i) => { assert_eq!(self.index, 0); self.ack(i); }
            Internal::ReadOpen(ci, seq) => { self.read_open.insert((ci, seq)); }
            Internal::ReadReady(ci, seq, value, origin) => {
                self.read_closed.insert((ci, seq));
                self.send(origin, Internal::ExecuteRead(ci, value));
            }
            Internal::ExecuteRead(ci, value) => { send_msg(self.setup.clients[ci], Rep::Value(value)); }
        }
    }
    fn maybe_noop(&mut self) {
        if self.index != 0 || !self.local { return; }
        // One finite opportunity per reported outstanding read at each handler
        // boundary; declining waits for the next incoming event.
        let eligible: Vec<_> = self.read_open.iter().copied()
            .filter(|r| !self.read_closed.contains(r) && !self.noop_used.contains(r)).collect();
        for read in eligible {
            if nondet() { self.noop_used.insert(read); self.submit(LogValue::NoOp); }
        }
    }
    fn run(mut self) {
        loop {
            self.maybe_noop();
            let req: Req = recv_tagged_msg_block(|_, tag| tag.is_none());
            if let Req::Write(sender, tag) = req {
                if sender >= usize::MAX - 2 {
                    let from = self.setup.nodes[usize::MAX - sender];
                    let message = recv_tagged_msg_block(move |tid, t| tid == from && t == Some(tag as u32));
                    self.internal(message);
                    continue;
                }
            }
            self.request(req);
        }
    }
}
fn replica(index: usize, local: bool) {
    // Select Setup without consuming earlier client requests/wake frames.
    let setup = recv_tagged_msg_block(|_, tag| tag == Some(u32::MAX));
    Replica { index, local, setup, serial: 0, accepted: 0, decided: 0,
        acknowledged: 0, sent: 0, instances: BTreeMap::new(), arrivals: BTreeMap::new(),
        x: 0, sequence: 0, read_open: BTreeSet::new(), read_closed: BTreeSet::new(),
        noop_used: BTreeSet::new() }.run();
}
fn implementation(q: usize, local: bool) {
    let mut replicas = Vec::new();
    for i in 0..3 {
        replicas.push(thread::Builder::new().name(format!("n{i}")).spawn(move || replica(i, local)).unwrap());
    }
    let nodes: Vec<_> = replicas.iter().map(|h| h.thread().id()).collect();
    let mut clients = Vec::new();
    for i in 0..CLIENTS {
        let node = nodes[if i == 0 { 0 } else { 2 }];
        clients.push(thread::Builder::new().name(format!("c{i}")).spawn(move || client(i, q, node)).unwrap());
    }
    let addresses = clients.iter().map(|h| h.thread().id()).collect();
    let init = thread::Builder::new().name("init".into()).spawn(move || {
        let setup = Setup { nodes, clients: addresses };
        for node in &setup.nodes { send_tagged_msg(*node, u32::MAX, setup.clone()); }
    }).unwrap();
    init.join().unwrap();
    for c in clients { c.join().unwrap(); }
    for n in replicas { n.join().unwrap(); }
}
fn entry(args: &[String]) -> Entry {
    assert!((2..=3).contains(&args.len()), "expected <q> <mixed|local> [<fifo|bag>]");
    let q: usize = args[0].parse().expect("q must be an integer");
    assert!(q >= 1);
    let local = match args[1].as_str() { "mixed" => false, "local" => true, _ => panic!("unknown variant") };
    if let Some(model) = args.get(2) { assert!(matches!(model.as_str(), "fifo" | "bag")); }
    Arc::new(move || implementation(q, local))
}
fn main() -> std::process::ExitCode {
    traceforge::program::main(Program {
        visible: &|_| vec!["c0".into(), "c1".into()], spec: &entry, imp: &entry,
    })
}
