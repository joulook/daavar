use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::{nondet, recv_msg_block, recv_tagged_msg_block, send_msg, send_tagged_msg};

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

type Id = (usize, usize);
type Deps = BTreeSet<Id>;
type Paths = BTreeSet<Vec<Id>>;
#[derive(Clone, Debug, PartialEq)]
struct Setup { nodes: Vec<ThreadId>, libs: Vec<ThreadId>, clients: Vec<ThreadId> }
#[derive(Clone, Debug, PartialEq)]
struct Vote { deps: Deps, paths: Paths }
#[derive(Clone, Debug, PartialEq)]
enum Wire {
    Setup(Setup),
    Propagate(Id, Req),
    FastAck { ballot: u64, from: usize, id: Id, vote: Vote },
    SlowAck { ballot: u64, from: usize, id: Id },
    Reply { ballot: u64, id: Id, paths: Paths, result: Rep },
}

// Wire traffic uses a tag so the library can distinguish it from the fixed,
// untagged Req type even before Setup has arrived.
fn send_wire(to: ThreadId, m: Wire) { send_tagged_msg(to, 1, m); }
fn recv_wire() -> Wire { recv_tagged_msg_block(|_, tag| tag == Some(1)) }

fn setup() -> (Setup, VecDeque<Wire>) {
    let mut early = VecDeque::new();
    loop {
        match recv_wire() {
            Wire::Setup(s) => return (s, early),
            m => early.push_back(m),
        }
    }
}
fn fast(p: usize) -> bool { p < 2 }
// Section 3.1: every replica belongs to some configured slow quorum.
fn slow(p: usize) -> bool { p < 3 }
fn conflicts(a: &Req, b: &Req) -> bool {
    !matches!((a, b), (Req::Read(_), Req::Read(_)))
}
fn exec(c: &Req, state: &mut u64) -> Rep {
    match c {
        Req::Write(_, v) => { *state = *v; Rep::Ack }
        Req::Read(_) => Rep::Value(*state),
        Req::Rmw(_) => { *state += 10; Rep::Result(*state) }
    }
}
// Every candidate is selectable; no arbitrary command-id tie breaking.
fn choose<T: Clone>(xs: &[T]) -> T {
    assert!(!xs.is_empty());
    for x in &xs[..xs.len() - 1] {
        if nondet() { return x.clone(); }
    }
    xs.last().unwrap().clone()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Phase { Preaccept, Accept, Commit }
#[derive(Clone, Debug)]
struct Command {
    req: Req,
    phase: Phase,
    deps: Deps,
    paths: Paths,
    executed: bool,
}
#[derive(Clone)]
enum Action { Accept(Id), Commit(Id), Execute(Id) }
struct Replica {
    me: usize,
    setup: Setup,
    commands: BTreeMap<Id, Command>,
    votes: BTreeMap<Id, BTreeMap<usize, Vote>>,
    slows: BTreeMap<Id, BTreeSet<usize>>,
    pending_log: Vec<Id>,
    state: u64,
}
impl Replica {
    fn pset(&self, id: Id) -> Paths {
        let d = &self.commands[&id].deps;
        if d.is_empty() { return BTreeSet::from([vec![id]]); }
        let mut out = Paths::new();
        for dep in d {
            for path in self.pset(*dep) {
                let mut p = vec![id];
                p.extend(path);
                out.insert(p);
            }
        }
        out
    }
    fn broadcast(&self, m: Wire, client: Option<usize>) {
        for n in &self.setup.nodes { send_wire(*n, m.clone()); }
        if let Some(c) = client { send_wire(self.setup.libs[c], m); }
    }
    fn receive(&mut self, m: Wire) {
        match m {
            Wire::Propagate(id, req) => {
                assert!(!self.commands.contains_key(&id));
                let deps = if fast(self.me) {
                    self.commands.iter().filter(|(_, c)| conflicts(&c.req, &req))
                        .map(|(id, _)| *id).collect()
                } else { Deps::new() };
                self.commands.insert(id, Command { req, phase: Phase::Preaccept,
                    deps: deps.clone(), paths: Paths::new(), executed: false });
                if fast(self.me) {
                    let paths = self.pset(id);
                    self.commands.get_mut(&id).unwrap().paths = paths.clone();
                    if self.me == 0 {
                        self.pending_log.push(id);
                        let mut speculative = self.state;
                        let mut result = Rep::Ack;
                        for pending in &self.pending_log {
                            result = exec(&self.commands[pending].req, &mut speculative);
                        }
                        send_wire(self.setup.libs[id.0], Wire::Reply {
                            ballot: 0, id, paths: paths.clone(), result });
                    }
                    self.broadcast(Wire::FastAck { ballot: 0, from: self.me, id,
                        vote: Vote { deps, paths } }, if self.me == 0 { None } else { Some(id.0) });
                }
            }
            Wire::FastAck { ballot, from, id, vote } => {
                assert_eq!(ballot, 0);
                assert!(fast(from));
                assert!(self.votes.entry(id).or_default().insert(from, vote).is_none());
            }
            Wire::SlowAck { ballot, from, id } => {
                assert_eq!(ballot, 0);
                self.slows.entry(id).or_default().insert(from);
            }
            _ => panic!("unexpected replica message"),
        }
    }
    fn has_slow(&self, id: Id, from: usize) -> bool {
        self.slows.get(&id).is_some_and(|s| s.contains(&from))
    }
    fn actions(&self) -> Vec<Action> {
        let mut out = Vec::new();
        for (&id, c) in &self.commands {
            if c.phase == Phase::Preaccept {
                if let Some(v) = self.votes.get(&id).and_then(|vs| vs.get(&0)) {
                    if v.deps.iter().all(|d| self.commands.get(d)
                        .is_some_and(|c| c.phase >= Phase::Accept)) {
                        out.push(Action::Accept(id));
                    }
                }
            }
            if c.phase == Phase::Accept && c.deps.iter().all(|d|
                self.commands[d].phase == Phase::Commit) {
                let fast_match = self.has_slow(id, 1) || self.votes.get(&id)
                    .and_then(|v| v.get(&1)).is_some_and(|v| v.deps == c.deps);
                // Unique fast quorum {0,1}; slow quorums {0,1}, {0,2}.
                if fast_match || self.has_slow(id, 1) || self.has_slow(id, 2) {
                    out.push(Action::Commit(id));
                }
            }
            if c.phase == Phase::Commit && !c.executed &&
                c.deps.iter().all(|d| self.commands[d].executed) {
                out.push(Action::Execute(id));
            }
        }
        out
    }
    fn apply(&mut self, a: Action) {
        match a {
            Action::Accept(id) => {
                let v = self.votes[&id][&0].clone();
                let c = self.commands.get_mut(&id).unwrap();
                c.phase = Phase::Accept;
                if (fast(self.me) && c.deps != v.deps) || slow(self.me) {
                    c.deps = v.deps;
                    self.broadcast(Wire::SlowAck { ballot: 0, from: self.me, id }, Some(id.0));
                } else if fast(self.me) && c.paths != v.paths {
                    send_wire(self.setup.libs[id.0], Wire::SlowAck { ballot: 0, from: self.me, id });
                }
            }
            Action::Commit(id) => self.commands.get_mut(&id).unwrap().phase = Phase::Commit,
            Action::Execute(id) => {
                let c = self.commands.get_mut(&id).unwrap();
                exec(&c.req, &mut self.state);
                c.executed = true;
                if self.me == 0 { self.pending_log.retain(|p| *p != id); }
            }
        }
    }
}
fn replica(me: usize) {
    let (s, mut early) = setup();
    let mut r = Replica { me, setup: s, commands: BTreeMap::new(), votes: BTreeMap::new(),
        slows: BTreeMap::new(), pending_log: Vec::new(), state: 0 };
    loop {
        let m = early.pop_front().unwrap_or_else(recv_wire);
        r.receive(m);
        loop {
            let enabled = r.actions();
            if enabled.is_empty() { break; }
            r.apply(choose(&enabled));
        }
    }
}

fn library(i: usize, q: usize) {
    let (s, mut early) = setup();
    // Keep acknowledgements for later sequence numbers if bag delivery reorders them.
    let mut inbox: BTreeMap<Id, Vec<Wire>> = BTreeMap::new();
    for seq in 0..q {
        let req = recv_tagged_msg_block::<_, Req>(|_, tag| tag.is_none());
        let id = (i, seq);
        for n in &s.nodes { send_wire(*n, Wire::Propagate(id, req.clone())); }
        let mut reply: Option<(Paths, Rep)> = None;
        let mut fasts: BTreeMap<usize, Paths> = BTreeMap::new();
        let mut slows = BTreeSet::new();
        let mut queued: VecDeque<Wire> = inbox.remove(&id).unwrap_or_default().into();
        loop {
            let m = queued.pop_front().or_else(|| early.pop_front())
                .unwrap_or_else(recv_wire);
            let mid = match &m {
                Wire::Reply { id, .. } | Wire::FastAck { id, .. } | Wire::SlowAck { id, .. } => *id,
                _ => panic!("unexpected library message"),
            };
            if mid != id {
                if mid.1 > seq { inbox.entry(mid).or_default().push(m); }
                continue;
            }
            match m {
                Wire::Reply { ballot, paths, result, .. } => {
                    assert_eq!(ballot, 0); reply = Some((paths, result));
                }
                Wire::FastAck { ballot, from, vote, .. } => {
                    assert_eq!(ballot, 0); fasts.insert(from, vote.paths);
                }
                Wire::SlowAck { ballot, from, .. } => {
                    assert_eq!(ballot, 0); slows.insert(from);
                }
                _ => unreachable!(),
            }
            if let Some((paths, result)) = &reply {
                let fast_ok = slows.contains(&1) || fasts.get(&1) == Some(paths);
                let slow_ok = slows.contains(&1) || slows.contains(&2);
                if fast_ok || slow_ok {
                    send_msg(s.clients[i], result.clone());
                    break;
                }
            }
        }
    }
    // Drain late acknowledgements indefinitely, even after the client exits.
    loop { let _ = recv_wire(); }
}

fn run(q: usize) {
    let mut nodes = Vec::new();
    for i in 0..3 {
        nodes.push(thread::Builder::new().name(format!("n{i}"))
            .spawn(move || replica(i)).unwrap());
    }
    let mut libs = Vec::new();
    for i in 0..CLIENTS {
        libs.push(thread::Builder::new().name(format!("l{i}"))
            .spawn(move || library(i, q)).unwrap());
    }
    let mut clients = Vec::new();
    for (i, lib) in libs.iter().enumerate() {
        let node = lib.thread().id();
        clients.push(thread::Builder::new().name(format!("c{i}"))
            .spawn(move || client(i, q, node)).unwrap());
    }
    let s = Setup { nodes: nodes.iter().map(|t| t.thread().id()).collect(),
        libs: libs.iter().map(|t| t.thread().id()).collect(),
        clients: clients.iter().map(|t| t.thread().id()).collect() };
    let init = thread::Builder::new().name("init".to_string()).spawn(move || {
        for to in s.nodes.iter().chain(&s.libs) { send_wire(*to, Wire::Setup(s.clone())); }
    }).unwrap();
    init.join().unwrap();
    for c in clients { c.join().unwrap(); }
    for l in libs { l.join().unwrap(); }
    for n in nodes { n.join().unwrap(); }
}
fn entry(args: &[String]) -> Entry {
    assert!((1..=2).contains(&args.len()), "expected <q> [fifo|bag]");
    let q: usize = args[0].parse().expect("q must be an integer");
    assert!(q >= 1);
    if let Some(model) = args.get(1) { assert!(model == "fifo" || model == "bag"); }
    Arc::new(move || run(q))
}
fn main() -> std::process::ExitCode {
    traceforge::program::main(Program {
        visible: &|_| vec!["c0".to_string(), "c1".to_string()],
        spec: &entry, imp: &entry,
    })
}
