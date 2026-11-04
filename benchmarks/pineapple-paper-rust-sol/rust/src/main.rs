use std::collections::VecDeque;
use std::sync::Arc;
use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::{recv_msg_block, send_msg};

#[derive(Clone, Debug, PartialEq)] enum Req { Write(usize, u64), Read(usize), Rmw(usize) }
#[derive(Clone, Debug, PartialEq)] enum Rep { Ack, Value(u64), Result(u64) }

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

const NODES: usize = 3;
const QUORUM: usize = NODES / 2 + 1;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
struct Pstamp { ts: usize, writer: usize, slot: usize }

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Stored { ps: Pstamp, value: u64 }

#[derive(Clone, Debug, PartialEq)]
struct Setup { nodes: Vec<ThreadId>, clients: Vec<ThreadId> }

// A uniform, explicitly serialized node inbox is necessary because Must's
// recv_msg_block<T> downcasts a message; it does not select messages by type.
// Client requests use indices 0 and 1, setup uses indices 2..7, and all
// protocol packets use indices >= 8. No protocol state is shared by threads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind { GetTag, AckTag, GetValue, AckValue, Put, AckPut, Forward, Done }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Packet { kind: Kind, from: usize, seq: usize, data: Stored }

#[derive(Clone, Copy)]
struct Codec { radix: usize }

impl Codec {
    fn new(q: usize) -> Self {
        let radix = q.checked_mul(CLIENTS).and_then(|x| x.checked_add(1)).expect("q too large");
        // All counters are <= the total number of client operations. Validate
        // the complete metadata range before starting any process.
        radix.checked_pow(3).and_then(|x| x.checked_mul(8 * NODES * NODES))
            .and_then(|x| x.checked_add(8)).expect("q exceeds wire metadata capacity");
        Self { radix }
    }
    fn encode(self, p: Packet) -> Req {
        assert!(p.seq < self.radix && p.data.ps.ts < self.radix && p.data.ps.slot < self.radix);
        let mut x = p.data.ps.slot;
        x = x * self.radix + p.data.ps.ts;
        x = x * self.radix + p.seq;
        x = x * NODES + p.data.ps.writer;
        x = x * NODES + p.from;
        x = x * 8 + p.kind as usize;
        Req::Write(x + 8, p.data.value)
    }
    fn decode(self, x: usize, value: u64) -> Packet {
        let mut x = x - 8;
        let kind = match x % 8 {
            0 => Kind::GetTag, 1 => Kind::AckTag, 2 => Kind::GetValue,
            3 => Kind::AckValue, 4 => Kind::Put, 5 => Kind::AckPut,
            6 => Kind::Forward, 7 => Kind::Done, _ => unreachable!(),
        };
        x /= 8;
        let from = x % NODES; x /= NODES;
        let writer = x % NODES; x /= NODES;
        let seq = x % self.radix; x /= self.radix;
        let ts = x % self.radix; x /= self.radix;
        Packet { kind, from, seq, data: Stored { ps: Pstamp { ts, writer, slot: x }, value } }
    }
}

#[derive(Clone, Copy)]
enum Op { Write(u64), Read, Rmw }

#[derive(Clone, Copy)]
struct Job { op: Op, proxy: usize, client: usize }

enum Phase { Get { seen: [bool; NODES], replies: Vec<(usize, Stored)> }, Put { seen: [bool; NODES] } }

struct Active { job: Job, seq: usize, phase: Phase, output: Stored }

struct Node {
    id: usize,
    setup: Setup,
    codec: Codec,
    stored: Stored,
    slot: usize,
    seq: usize,
    active: Option<Active>,
    jobs: VecDeque<Job>,
    forwarded: Option<usize>,
}

impl Node {
    fn send(&self, to: usize, kind: Kind, seq: usize, data: Stored) {
        send_msg(self.setup.nodes[to], self.codec.encode(Packet { kind, from: self.id, seq, data }));
    }
    fn broadcast(&self, kind: Kind, seq: usize, data: Stored) {
        for to in 0..NODES { self.send(to, kind, seq, data); }
    }
    fn start(&mut self) {
        if self.active.is_some() { return; }
        let Some(job) = self.jobs.pop_front() else { return; };
        self.seq += 1;
        let kind = if matches!(job.op, Op::Write(_)) { Kind::GetTag } else { Kind::GetValue };
        self.active = Some(Active {
            job, seq: self.seq,
            phase: Phase::Get { seen: [false; NODES], replies: Vec::new() },
            output: Stored::default(),
        });
        self.broadcast(kind, self.seq, Stored::default());
    }
    fn request(&mut self, req: Req) {
        let (client, op) = match req {
            Req::Write(c, v) => (c, Op::Write(v)),
            Req::Read(c) => (c, Op::Read),
            Req::Rmw(c) => (c, Op::Rmw),
        };
        assert_eq!(client, self.id);
        if matches!(op, Op::Rmw) && self.id != 0 {
            assert!(self.forwarded.replace(client).is_none());
            self.send(0, Kind::Forward, 0, Stored { value: client as u64, ..Stored::default() });
        } else {
            self.jobs.push_back(Job { op, client, proxy: self.id });
        }
    }
    fn packet(&mut self, p: Packet) {
        match p.kind {
            Kind::GetTag => self.send(p.from, Kind::AckTag, p.seq, Stored { value: 0, ..self.stored }),
            Kind::GetValue => self.send(p.from, Kind::AckValue, p.seq, self.stored),
            Kind::Put => {
                if self.stored.ps < p.data.ps { self.stored = p.data; }
                self.send(p.from, Kind::AckPut, p.seq, Stored::default());
            }
            Kind::Forward => {
                assert_eq!(self.id, 0);
                self.jobs.push_back(Job { op: Op::Rmw, proxy: p.from, client: p.data.value as usize });
            }
            Kind::Done => {
                assert_eq!(p.from, 0);
                let client = self.forwarded.take().expect("unsolicited RMW result");
                send_msg(self.setup.clients[client], Rep::Result(p.data.value));
            }
            Kind::AckTag | Kind::AckValue | Kind::AckPut => self.response(p),
        }
    }
    fn response(&mut self, p: Packet) {
        let Some(mut a) = self.active.take() else { return; };
        if p.seq != a.seq { self.active = Some(a); return; }
        let mut put = false;
        let mut done = false;
        match &mut a.phase {
            Phase::Get { seen, replies } => {
                let expected = if matches!(a.job.op, Op::Write(_)) { Kind::AckTag } else { Kind::AckValue };
                if p.kind == expected && !seen[p.from] {
                    seen[p.from] = true;
                    replies.push((p.from, p.data));
                    if replies.len() >= QUORUM && (!matches!(a.job.op, Op::Rmw) || seen[0]) {
                        // For an RMW select exactly a majority containing self:
                        // self plus the first other response in arrival order.
                        let best = if matches!(a.job.op, Op::Rmw) {
                            let own = replies.iter().find(|(i, _)| *i == 0).unwrap().1;
                            let other = replies.iter().find(|(i, _)| *i != 0).unwrap().1;
                            if own.ps >= other.ps { own } else { other }
                        } else {
                            replies.iter().map(|(_, s)| *s).max_by_key(|s| s.ps).unwrap()
                        };
                        a.output = match a.job.op {
                            Op::Read => best,
                            Op::Write(value) => Stored { value, ps: Pstamp { ts: best.ps.ts + 1, writer: self.id, slot: best.ps.slot } },
                            Op::Rmw => {
                                assert_eq!(self.id, 0);
                                self.slot += 1;
                                assert!(self.slot > best.ps.slot);
                                Stored { value: best.value.checked_add(10).expect("value overflow"), ps: Pstamp { slot: self.slot, ..best.ps } }
                            }
                        };
                        // Retain every leader decision locally before the next
                        // RMW, even when its PUT quorum excludes the leader.
                        if matches!(a.job.op, Op::Rmw) && self.stored.ps < a.output.ps {
                            self.stored = a.output;
                        }
                        a.phase = Phase::Put { seen: [false; NODES] };
                        put = true;
                    }
                }
            }
            Phase::Put { seen } => {
                if p.kind == Kind::AckPut {
                    seen[p.from] = true;
                    done = seen.iter().filter(|x| **x).count() >= QUORUM;
                }
            }
        }
        if put { self.broadcast(Kind::Put, a.seq, a.output); }
        if done {
            match a.job.op {
                Op::Write(_) => send_msg(self.setup.clients[a.job.client], Rep::Ack),
                Op::Read => send_msg(self.setup.clients[a.job.client], Rep::Value(a.output.value)),
                Op::Rmw if a.job.proxy == self.id => send_msg(self.setup.clients[a.job.client], Rep::Result(a.output.value)),
                Op::Rmw => self.send(a.job.proxy, Kind::Done, 0, a.output),
            }
        } else { self.active = Some(a); }
    }
}

fn node(id: usize, codec: Codec) {
    // Setup is serialized as five indexed address frames. Before all five
    // arrive, retain ordinary requests/packets in their receive order.
    let mut addresses = [None; NODES + CLIENTS];
    let mut buffered = VecDeque::new();
    while addresses.iter().any(Option::is_none) {
        let req = recv_msg_block::<Req>();
        if let Req::Write(field @ 2..=6, address) = req {
            // The address was produced by the inverse bit-preserving cast in
            // init, in this execution. It is never invented or incremented.
            addresses[field - 2] = Some(unsafe { std::mem::transmute::<u32, ThreadId>(address as u32) });
        } else { buffered.push_back(req); }
    }
    let mut n = Node {
        id, codec,
        setup: Setup {
            nodes: addresses[..NODES].iter().map(|a| a.unwrap()).collect(),
            clients: addresses[NODES..].iter().map(|a| a.unwrap()).collect(),
        },
        stored: Stored::default(), slot: 0, seq: 0,
        active: None, jobs: VecDeque::new(), forwarded: None,
    };
    loop {
        let req = buffered.pop_front().unwrap_or_else(recv_msg_block::<Req>);
        match req {
            Req::Write(code, value) if code >= 8 => n.packet(codec.decode(code, value)),
            req => n.request(req),
        }
        n.start();
    }
}

fn implementation(q: usize) {
    let codec = Codec::new(q);
    let mut nodes = Vec::new();
    for i in 0..NODES {
        nodes.push(thread::Builder::new().name(format!("n{i}")).spawn(move || node(i, codec)).unwrap());
    }
    let node_ids: Vec<_> = nodes.iter().map(|n| n.thread().id()).collect();
    let mut clients = Vec::new();
    for (i, &node) in node_ids.iter().take(CLIENTS).enumerate() {
        clients.push(thread::Builder::new().name(format!("c{i}")).spawn(move || client(i, q, node)).unwrap());
    }
    let setup = Setup { nodes: node_ids, clients: clients.iter().map(|c| c.thread().id()).collect() };
    let init = thread::Builder::new().name("init".into()).spawn(move || {
        for &node in &setup.nodes {
            for (i, address) in setup.nodes.iter().chain(&setup.clients).enumerate() {
                // Internal address serialization only; the compiler checks that
                // ThreadId and u32 have the same size for this dependency.
                let bits = unsafe { std::mem::transmute::<ThreadId, u32>(*address) };
                send_msg(node, Req::Write(2 + i, bits as u64));
            }
        }
    }).unwrap();
    for c in clients { c.join().unwrap(); }
    init.join().unwrap();
    for n in nodes { n.join().unwrap(); }
}

fn entry(args: &[String]) -> Entry {
    assert!(!args.is_empty() && args.len() <= 2, "arguments: <q> [fifo|bag]");
    let q: usize = args[0].parse().expect("q must be an integer");
    assert!(q >= 1, "q must be positive");
    if let Some(model) = args.get(1) { assert!(model == "fifo" || model == "bag", "model must be fifo or bag"); }
    // Delivery semantics belong to the harness (--model); the optional program
    // argument is accepted for the interface and never overrides the harness.
    Codec::new(q);
    Arc::new(move || implementation(q))
}

fn visible(_: &[String]) -> Vec<String> { vec!["c0".into(), "c1".into()] }

fn main() -> std::process::ExitCode {
    traceforge::program::main(Program { visible: &visible, spec: &entry, imp: &entry })
}
