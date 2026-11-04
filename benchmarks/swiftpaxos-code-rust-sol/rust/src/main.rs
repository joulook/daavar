use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::{nondet, recv_msg_block, recv_tagged_msg_block, send_msg, send_tagged_msg};

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

type Id = (usize, usize);
type Path = Vec<Id>;
#[derive(Clone, Debug, PartialEq)]
struct Setup { nodes: Vec<ThreadId>, libs: Vec<ThreadId>, clients: Vec<ThreadId> }
#[derive(Clone, Debug, PartialEq)]
struct FastAck { replica: usize, id: Id, dep: Vec<Id>, checksum: Path, seq: usize }
#[derive(Clone, Debug, PartialEq)]
enum Msg {
    Setup(Setup),
    Propagate(Id, Req),
    FastAck(FastAck),
    FastAckClient(usize, Id, Path),
    LightSlowAck(usize, Id),
    Reply(Id, Path, Rep),
}
const INTERNAL: u32 = 1;
fn to_lib(s: &Setup, id: Id, m: Msg) { send_tagged_msg(s.libs[id.0], INTERNAL, m); }
fn broadcast(s: &Setup, me: usize, m: Msg) {
    for (i, &node) in s.nodes.iter().enumerate() {
        if i != me { send_msg(node, m.clone()); }
    }
}

// The exact hash preimage chain, rather than a SHA-256 cache.
#[derive(Default)]
struct HashLog { synced: Option<usize>, base: Path, pending: Vec<Id>, hash: Path }
impl HashLog {
    fn append(&mut self, id: Id) -> Path {
        self.pending.push(id);
        self.hash.push(id);
        self.hash.clone()
    }
    fn update(&mut self, id: Id, seq: usize, hash: Path) {
        let existed = self.pending.contains(&id);
        self.pending.retain(|&x| x != id);
        if self.synced.is_some_and(|s| seq <= s) && !existed { return; }
        if self.synced.is_none_or(|s| seq > s) {
            self.synced = Some(seq);
            self.base = hash;
        }
        self.hash = self.base.clone();
        self.hash.extend(self.pending.iter().copied());
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Default)]
enum Phase { #[default] Start, PreAccept, Accept, Commit }
#[derive(Default)]
struct Desc {
    cmd: Option<Req>, dep: Vec<Id>, checksum: Path, phase: Phase,
    leader: Option<FastAck>, leader_handled: bool,
    fast: BTreeMap<usize, Vec<Id>>, slow: BTreeMap<usize, ()>,
    delivered: bool,
}
struct Replica {
    me: usize, setup: Setup, desc: BTreeMap<Id, Desc>,
    last_cmd: Option<Id>, last_write: Option<Id>, log: HashLog,
    pending_updates: BTreeMap<Id, FastAck>, seq: usize, x: u64,
}
impl Replica {
    fn on(&mut self, msg: Msg) {
        match msg {
            Msg::Propagate(id, cmd) => {
                if self.desc.get(&id).is_some_and(|d| d.cmd.is_some()) { return; }
                let dep = if matches!(cmd, Req::Read(_)) { self.last_write } else { self.last_cmd };
                self.last_cmd = Some(id);
                if !matches!(cmd, Req::Read(_)) { self.last_write = Some(id); }
                let checksum = self.log.append(id);
                if let Some(f) = self.pending_updates.remove(&id) {
                    self.log.update(id, f.seq, f.checksum);
                }
                let d = self.desc.entry(id).or_default();
                d.cmd = Some(cmd);
                d.checksum = checksum;
                if self.me < 2 { d.dep = dep.into_iter().collect(); d.phase = Phase::PreAccept; }
                // Recall the leader handler before deciding whether to send a FastAck.
                let slow = self.handle_leader(id);
                if self.me < 2 && !slow && !self.desc[&id].delivered {
                    let d = &self.desc[&id];
                    let f = FastAck { replica: self.me, id, dep: d.dep.clone(), checksum: d.checksum.clone(), seq: self.seq };
                    if self.me == 0 { self.seq += 1; }
                    if self.me != 0 { to_lib(&self.setup, id, Msg::FastAckClient(self.me, id, f.checksum.clone())); }
                    broadcast(&self.setup, self.me, Msg::FastAck(f.clone()));
                    // optExec: leader executes before processing its own FastAck.
                    if self.me == 0 { self.execute_ready(Some(id)); }
                    self.fast_ack(f);
                }
            }
            Msg::FastAck(f) => {
                if f.replica == 0 && self.me != 0 {
                    if self.desc.get(&f.id).is_some_and(|d| d.cmd.is_some()) {
                        self.log.update(f.id, f.seq, f.checksum.clone());
                    } else { self.pending_updates.insert(f.id, f.clone()); }
                }
                self.fast_ack(f);
            }
            Msg::LightSlowAck(r, id) => {
                let d = self.desc.entry(id).or_default();
                if !d.delivered { d.slow.insert(r, ()); }
                self.commit(id);
            }
            _ => panic!("unexpected replica message"),
        }
        self.execute_ready(None);
    }
    fn fast_ack(&mut self, f: FastAck) {
        let id = f.id;
        let d = self.desc.entry(id).or_default();
        if d.delivered { return; }
        if f.replica == 0 {
            d.leader = Some(f);
            self.handle_leader(id);
        } else {
            d.fast.entry(f.replica).or_insert(f.dep);
            self.commit(id);
        }
    }
    fn handle_leader(&mut self, id: Id) -> bool {
        let d = self.desc.entry(id).or_default();
        if d.cmd.is_none() || d.leader.is_none() || d.leader_handled || d.delivered { return false; }
        d.leader_handled = true;
        d.phase = Phase::Accept;
        let dep = d.leader.as_ref().unwrap().dep.clone();
        // SQ.Contains is true for every replica, so every follower sends slow.
        self.commit(id);
        if self.me == 0 { return false; }
        let d = self.desc.get_mut(&id).unwrap();
        if !d.delivered { d.dep = dep; }
        to_lib(&self.setup, id, Msg::LightSlowAck(self.me, id));
        broadcast(&self.setup, self.me, Msg::LightSlowAck(self.me, id));
        if !self.desc[&id].delivered {
            self.desc.get_mut(&id).unwrap().slow.insert(self.me, ());
            self.commit(id);
        }
        true
    }
    fn commit(&mut self, id: Id) {
        let d = self.desc.get_mut(&id).unwrap();
        if d.delivered || !d.leader_handled { return; }
        let dep = &d.leader.as_ref().unwrap().dep;
        let fast = d.fast.get(&1).is_some_and(|p| p == dep);
        let slow = d.slow.keys().any(|&r| r == 1 || r == 2);
        if fast || slow {
            d.phase = Phase::Commit;
            // The Go MsgSet handler calls deliver synchronously, before adoption.
            self.execute_ready(None);
        }
    }
    fn execute_ready(&mut self, optimistic: Option<Id>) {
        loop {
            let ready: Vec<Id> = self.desc.iter().filter_map(|(&id, d)| {
                (d.cmd.is_some() && !d.delivered &&
                 (d.phase == Phase::Commit || (self.me == 0 && optimistic == Some(id))) &&
                 d.dep.iter().all(|p| self.desc.get(p).is_some_and(|x| x.delivered)))
                    .then_some(id)
            }).collect();
            if ready.is_empty() { break; }
            let mut chosen = ready[0];
            for &id in &ready[1..] { if nondet() { chosen = id; } }
            let d = self.desc.get_mut(&chosen).unwrap();
            d.delivered = true;
            let rep = match d.cmd.as_ref().unwrap() {
                Req::Write(_, v) => { self.x = *v; Rep::Ack }
                Req::Read(_) => Rep::Value(self.x),
                Req::Rmw(_) => { self.x += 10; Rep::Result(self.x) }
            };
            if self.me == 0 { to_lib(&self.setup, chosen, Msg::Reply(chosen, d.checksum.clone(), rep)); }
        }
    }
}
fn replica(me: usize) {
    let mut pending = VecDeque::new();
    let setup = loop {
        match recv_msg_block::<Msg>() { Msg::Setup(s) => break s, m => pending.push_back(m) }
    };
    let mut r = Replica { me, setup, desc: BTreeMap::new(), last_cmd: None, last_write: None,
        log: HashLog::default(), pending_updates: BTreeMap::new(), seq: 0, x: 0 };
    loop { let m = pending.pop_front().unwrap_or_else(recv_msg_block::<Msg>); r.on(m); }
}

fn library(i: usize, q: usize) {
    // Select Setup without consuming an early Req of the fixed external type.
    let Msg::Setup(s) = recv_tagged_msg_block::<_, Msg>(|_, tag| tag == Some(INTERNAL)) else { unreachable!() };
    let mut fast: BTreeMap<Id, BTreeMap<usize, Path>> = BTreeMap::new();
    let mut slow: BTreeMap<Id, BTreeMap<usize, ()>> = BTreeMap::new();
    for seq in 0..q {
        let client = s.clients[i];
        let req = recv_tagged_msg_block::<_, Req>(move |sender, tag| sender == client && tag.is_none());
        let id = (i, seq);
        for &node in &s.nodes { send_msg(node, Msg::Propagate(id, req.clone())); }
        let mut reply = None;
        loop {
            let m = recv_tagged_msg_block::<_, Msg>(|_, tag| tag == Some(INTERNAL));
            match m {
                Msg::FastAckClient(r, cid, path) if cid == id => { fast.entry(cid).or_default().entry(r).or_insert(path); }
                Msg::LightSlowAck(r, cid) if cid == id => { slow.entry(cid).or_default().insert(r, ()); }
                Msg::Reply(cid, path, rep) if cid == id => { reply = Some((path, rep)); }
                _ => (), // Late messages for a completed command are ignored.
            }
            if let Some((path, rep)) = &reply {
                let fast_ok = fast.get(&id).and_then(|m| m.get(&1)).is_some_and(|h| h == path);
                let slow_ok = slow.get(&id).is_some_and(|m| m.contains_key(&1) || m.contains_key(&2));
                if fast_ok || slow_ok { send_msg(s.clients[i], rep.clone()); break; }
            }
        }
        fast.remove(&id); slow.remove(&id);
    }
    loop { let _ = recv_tagged_msg_block::<_, Msg>(|_, tag| tag == Some(INTERNAL)); }
}
fn implementation(q: usize) {
    let mut handles = Vec::new();
    let mut nodes = Vec::new();
    let mut libs = Vec::new();
    let mut clients = Vec::new();
    for i in 0..3 {
        let h = thread::Builder::new().name(format!("n{i}")).spawn(move || replica(i)).unwrap();
        nodes.push(h.thread().id()); handles.push(h);
    }
    for i in 0..CLIENTS {
        let h = thread::Builder::new().name(format!("l{i}")).spawn(move || library(i, q)).unwrap();
        libs.push(h.thread().id()); handles.push(h);
    }
    for i in 0..CLIENTS {
        let node = libs[i];
        let h = thread::Builder::new().name(format!("c{i}")).spawn(move || client(i, q, node)).unwrap();
        clients.push(h.thread().id()); handles.push(h);
    }
    let init = thread::Builder::new().name("init".into()).spawn(move || {
        let s = Setup { nodes, libs, clients };
        for &node in &s.nodes { send_msg(node, Msg::Setup(s.clone())); }
        for &lib in &s.libs { send_tagged_msg(lib, INTERNAL, Msg::Setup(s.clone())); }
    }).unwrap();
    init.join().unwrap();
    for h in handles { h.join().unwrap(); }
}
fn entry(args: &[String]) -> Entry {
    let q: usize = args.first().expect("expected q").parse().expect("q must be an integer");
    assert!(q >= 1);
    assert!(args.len() <= 2 && args.get(1).is_none_or(|m| m == "fifo" || m == "bag"));
    Arc::new(move || implementation(q))
}
fn main() -> std::process::ExitCode {
    traceforge::program::main(Program {
        visible: &|_| vec!["c0".into(), "c1".into()], spec: &entry, imp: &entry,
    })
}
