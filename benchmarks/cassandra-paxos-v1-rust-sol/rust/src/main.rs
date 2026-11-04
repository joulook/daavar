use std::sync::Arc;
use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::{recv_msg_block, recv_tagged_msg_block, send_msg, send_tagged_msg};

#[derive(Clone, Debug, PartialEq)]
enum Req { Cas(usize, u64, u64), Read(usize) }
#[derive(Clone, Debug, PartialEq)]
enum Rep { CasResult(usize, u64, u64), Value(usize, u64), Failed(usize) }

const CLIENTS: usize = 2;
fn value(i: usize, j: usize) -> u64 { (1 + i + CLIENTS * j) as u64 }
fn client(i: usize, q: usize, node: ThreadId) {
    let mut known: u64 = 0;
    for j in 0..q {
        if (i + j) % 2 == 0 { send_msg(node, Req::Cas(i, known, value(i, j))); } else { send_msg(node, Req::Read(i)); }
        match recv_msg_block::<Rep>() {
            Rep::CasResult(_, _, v) => known = v,
            Rep::Value(_, v) => known = v,
            Rep::Failed(_) => {}
        }
    }
}

const N: usize = 3;
const QUORUM: usize = 2;
const ATTEMPTS: usize = 3;
const PAYLOAD: u32 = 1;
const SETUP: u32 = 2;

#[derive(Clone, Debug, PartialEq)]
struct Setup { nodes: Vec<ThreadId>, clients: Vec<ThreadId> }

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
struct Ballot(u64, usize);
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Commit { ballot: Ballot, value: Option<u64> }
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Data { stamp: Ballot, value: u64 }

#[derive(Clone, Debug, PartialEq)]
enum Wire {
    Prepare { from: usize, rpc: u64, ballot: Ballot },
    Promise { from: usize, rpc: u64, promised: bool, progress: Commit, committed: Commit },
    Propose { from: usize, rpc: u64, proposal: Commit },
    Accepted { from: usize, rpc: u64, accepted: bool },
    Commit { from: usize, rpc: u64, proposal: Commit, ack: bool },
    CommitAck { from: usize, rpc: u64 },
    Read { from: usize, rpc: u64 },
    ReadResult { from: usize, rpc: u64, data: Data },
    ReadRepair { from: usize, rpc: u64, data: Data },
    RepairAck { from: usize, rpc: u64 },
}

#[derive(Default)]
struct Replica { promised: Ballot, accepted: Option<Commit>, committed: Commit, data: Data }
impl Replica {
    fn latest(&self) -> Ballot {
        self.promised.max(self.committed.ballot).max(self.accepted.unwrap_or_default().ballot)
    }
    fn legacy_prepare(&mut self, ballot: Ballot) -> (bool, Commit, Commit) {
        if ballot > self.latest() {
            self.promised = ballot;
            (true, self.accepted.unwrap_or_default(), self.committed)
        } else {
            (false, Commit { ballot: self.promised, value: None }, self.committed)
        }
    }
    fn legacy_propose(&mut self, proposal: Commit) -> bool {
        if proposal.ballot < self.latest() { return false; }
        if proposal.ballot != self.committed.ballot { self.accepted = Some(proposal); }
        true
    }
    fn apply_data(&mut self, data: Data) {
        if data.stamp > self.data.stamp { self.data = data; }
    }
    fn apply_commit(&mut self, proposal: Commit) {
        if let Some(value) = proposal.value {
            self.apply_data(Data { stamp: proposal.ballot, value });
        }
        if proposal.ballot > self.committed.ballot { self.committed = proposal; }
        if self.accepted.is_some_and(|a| a.ballot <= self.committed.ballot) { self.accepted = None; }
    }
}

struct Operation { req: Req, attempt: usize }
enum Phase {
    Idle,
    Prepare { ballot: Ballot, replies: Vec<(usize, Commit, Commit)> },
    Propose { proposal: Commit, replay: bool, result: Option<Rep>, accepts: usize, replies: usize },
    Commit { replay: bool, result: Option<Rep>, replies: usize },
    Read { ballot: Ballot, serial: bool, replies: Vec<(usize, Data)> },
    RepairRead { ballot: Ballot, serial: bool, value: u64, remaining: usize },
}
struct Node {
    id: usize,
    linearizable: bool,
    setup: Setup,
    replica: Replica,
    seen: u64,
    rpc: u64,
    op: Option<Operation>,
    phase: Phase,
}
impl Node {
    // Untagged Req is the inbox header type. Read(2 + sender) is reserved
    // for replica-to-replica envelopes; real client ids are only 0 and 1.
    fn send(&self, to: usize, msg: Wire) {
        send_msg(self.setup.nodes[to], Req::Read(CLIENTS + self.id));
        send_tagged_msg(self.setup.nodes[to], PAYLOAD, msg);
    }
    fn broadcast(&self, msg: Wire) {
        for to in 0..N { self.send(to, msg.clone()); }
    }
    fn next_rpc(&mut self) { self.rpc += 1; }
    fn observe(&mut self, ballot: Ballot) { self.seen = self.seen.max(ballot.0); }
    fn reply(&mut self, rep: Rep) {
        let client = match self.op.take().unwrap().req { Req::Cas(c, _, _) | Req::Read(c) => c };
        self.phase = Phase::Idle;
        send_msg(self.setup.clients[client], rep);
    }
    fn fail(&mut self) {
        let c = match self.op.as_ref().unwrap().req { Req::Cas(c, _, _) | Req::Read(c) => c };
        self.reply(Rep::Failed(c));
    }
    fn start(&mut self, req: Req) {
        assert!(self.op.is_none());
        self.op = Some(Operation { req, attempt: 1 });
        self.prepare_paxos();
    }
    fn retry(&mut self) {
        let op = self.op.as_mut().unwrap();
        if op.attempt == ATTEMPTS { self.fail(); }
        else { op.attempt += 1; self.prepare_paxos(); }
    }
    fn prepare_paxos(&mut self) {
        self.seen += 1;
        let ballot = Ballot(self.seen, self.id);
        self.next_rpc();
        self.phase = Phase::Prepare { ballot, replies: Vec::new() };
        self.broadcast(Wire::Prepare { from: self.id, rpc: self.rpc, ballot });
    }
    fn propose_paxos(&mut self, proposal: Commit, replay: bool, result: Option<Rep>) {
        self.next_rpc();
        self.phase = Phase::Propose { proposal, replay, result, accepts: 0, replies: 0 };
        self.broadcast(Wire::Propose { from: self.id, rpc: self.rpc, proposal });
    }
    fn commit_paxos(&mut self, proposal: Commit, replay: bool, result: Option<Rep>) {
        self.next_rpc();
        self.phase = Phase::Commit { replay, result, replies: 0 };
        self.broadcast(Wire::Commit { from: self.id, rpc: self.rpc, proposal, ack: true });
    }
    fn quorum_read(&mut self, ballot: Ballot, serial: bool) {
        self.next_rpc();
        self.phase = Phase::Read { ballot, serial, replies: Vec::new() };
        self.broadcast(Wire::Read { from: self.id, rpc: self.rpc });
    }
    fn read_complete(&mut self, ballot: Ballot, serial: bool, current: u64) {
        match self.op.as_ref().unwrap().req {
            Req::Read(c) => { assert!(serial); self.reply(Rep::Value(c, current)); }
            Req::Cas(c, expected, value) => {
                assert!(!serial);
                let applied = current == expected;
                let result = Rep::CasResult(c, u64::from(applied), if applied { value } else { current });
                self.propose_paxos(Commit { ballot, value: applied.then_some(value) }, false, Some(result));
            }
        }
    }
    fn begin_and_repair_paxos(&mut self, ballot: Ballot, replies: Vec<(usize, Commit, Commit)>) {
        let progress = replies.iter().map(|r| r.1).max_by_key(|c| c.ballot).unwrap();
        let committed = replies.iter().map(|r| r.2).max_by_key(|c| c.ballot).unwrap();
        if progress.value.is_some() && progress.ballot > committed.ballot {
            self.propose_paxos(Commit { ballot, value: progress.value }, true, None);
            return;
        }
        let missing: Vec<_> = replies.iter().filter(|r| r.2.ballot != committed.ballot).map(|r| r.0).collect();
        if !missing.is_empty() {
            for to in missing {
                self.send(to, Wire::Commit { from: self.id, rpc: self.rpc, proposal: committed, ack: false });
            }
            self.retry();
            return;
        }
        match self.op.as_ref().unwrap().req {
            Req::Cas(..) => self.quorum_read(ballot, false),
            Req::Read(_) if self.linearizable => {
                self.propose_paxos(Commit { ballot, value: None }, false, None);
            }
            Req::Read(_) => self.quorum_read(ballot, true),
        }
    }
    fn handle(&mut self, msg: Wire) {
        match msg {
            Wire::Prepare { from, rpc, ballot } => {
                self.observe(ballot);
                let (promised, progress, committed) = self.replica.legacy_prepare(ballot);
                self.send(from, Wire::Promise { from: self.id, rpc, promised, progress, committed });
            }
            Wire::Propose { from, rpc, proposal } => {
                self.observe(proposal.ballot);
                let accepted = self.replica.legacy_propose(proposal);
                self.send(from, Wire::Accepted { from: self.id, rpc, accepted });
            }
            Wire::Commit { from, rpc, proposal, ack } => {
                self.observe(proposal.ballot);
                self.replica.apply_commit(proposal);
                if ack { self.send(from, Wire::CommitAck { from: self.id, rpc }); }
            }
            Wire::Read { from, rpc } => {
                self.send(from, Wire::ReadResult { from: self.id, rpc, data: self.replica.data });
            }
            Wire::ReadRepair { from, rpc, data } => {
                self.observe(data.stamp);
                self.replica.apply_data(data);
                self.send(from, Wire::RepairAck { from: self.id, rpc });
            }
            Wire::Promise { from, rpc, promised, progress, committed } => {
                self.observe(progress.ballot);
                self.observe(committed.ballot);
                if rpc != self.rpc { return; }
                if let Phase::Prepare { ballot, replies } = &mut self.phase {
                    if !promised { self.retry(); return; }
                    replies.push((from, progress, committed));
                    if replies.len() == QUORUM {
                        let ballot = *ballot;
                        let replies = std::mem::take(replies);
                        self.begin_and_repair_paxos(ballot, replies);
                    }
                }
            }
            Wire::Accepted { from: _, rpc, accepted } => {
                if rpc != self.rpc { return; }
                if let Phase::Propose { proposal, replay, result, accepts, replies } = &mut self.phase {
                    *replies += 1;
                    *accepts += usize::from(accepted);
                    if *accepts >= QUORUM {
                        let (proposal, replay, result) = (*proposal, *replay, result.clone());
                        if proposal.value.is_some() { self.commit_paxos(proposal, replay, result); }
                        else if let Some(result) = result { self.reply(result); }
                        else { self.quorum_read(proposal.ballot, true); }
                    } else if (*replay && N - *replies + *accepts < QUORUM) || *replies == N {
                        self.retry();
                    }
                }
            }
            Wire::CommitAck { from: _, rpc } => {
                if rpc != self.rpc { return; }
                if let Phase::Commit { replay, result, replies } = &mut self.phase {
                    *replies += 1;
                    if *replies == QUORUM {
                        if *replay { self.retry(); }
                        else { let result = result.clone().unwrap(); self.reply(result); }
                    }
                }
            }
            Wire::ReadResult { from, rpc, data } => {
                self.observe(data.stamp);
                if rpc != self.rpc { return; }
                if let Phase::Read { ballot, serial, replies } = &mut self.phase {
                    replies.push((from, data));
                    if replies.len() == QUORUM {
                        let (ballot, serial) = (*ballot, *serial);
                        let replies = std::mem::take(replies);
                        let latest = replies.iter().map(|r| r.1).max_by_key(|d| d.stamp).unwrap();
                        let missing: Vec<_> = replies.iter().filter(|r| r.1 != latest).map(|r| r.0).collect();
                        if missing.is_empty() { self.read_complete(ballot, serial, latest.value); }
                        else {
                            self.next_rpc();
                            self.phase = Phase::RepairRead { ballot, serial, value: latest.value, remaining: missing.len() };
                            for to in missing { self.send(to, Wire::ReadRepair { from: self.id, rpc: self.rpc, data: latest }); }
                        }
                    }
                }
            }
            Wire::RepairAck { from: _, rpc } => {
                if rpc != self.rpc { return; }
                if let Phase::RepairRead { ballot, serial, value, remaining } = &mut self.phase {
                    *remaining -= 1;
                    if *remaining == 0 {
                        let (ballot, serial, value) = (*ballot, *serial, *value);
                        self.read_complete(ballot, serial, value);
                    }
                }
            }
        }
    }
}

fn replica(id: usize, linearizable: bool) {
    // Selective setup receive leaves early client requests buffered in the inbox.
    let setup: Setup = recv_tagged_msg_block(|_, tag| tag == Some(SETUP));
    let mut node = Node {
        id, linearizable, setup, replica: Replica::default(), seen: 0, rpc: 0,
        op: None, phase: Phase::Idle,
    };
    loop {
        let req: Req = recv_tagged_msg_block(|_, tag| tag.is_none());
        match req {
            Req::Read(sender) if sender >= CLIENTS => {
                let sender = node.setup.nodes[sender - CLIENTS];
                let msg: Wire = recv_tagged_msg_block(move |from, tag| from == sender && tag == Some(PAYLOAD));
                node.handle(msg);
            }
            req => node.start(req),
        }
    }
}

fn implementation(q: usize, linearizable: bool) {
    let mut replicas = Vec::new();
    for i in 0..N {
        replicas.push(thread::Builder::new().name(format!("n{i}")).spawn(move || replica(i, linearizable)).unwrap());
    }
    let nodes: Vec<_> = replicas.iter().map(|h| h.thread().id()).collect();
    let mut clients = Vec::new();
    for i in 0..CLIENTS {
        let node = nodes[i];
        clients.push(thread::Builder::new().name(format!("c{i}")).spawn(move || client(i, q, node)).unwrap());
    }
    let setup = Setup { nodes, clients: clients.iter().map(|h| h.thread().id()).collect() };
    let init = thread::Builder::new().name("init".into()).spawn(move || {
        for &node in &setup.nodes { send_tagged_msg(node, SETUP, setup.clone()); }
    }).unwrap();
    init.join().unwrap();
    for client in clients { client.join().unwrap(); }
    for replica in replicas { replica.join().unwrap(); }
}
fn entry(args: &[String]) -> Entry {
    assert!((2..=3).contains(&args.len()), "expected <q> <v1|v1nolin> [fifo|bag]");
    let q: usize = args[0].parse().expect("q must be an integer");
    assert!(q >= 1);
    let linearizable = match args[1].as_str() { "v1" => true, "v1nolin" => false, _ => panic!("unknown variant") };
    if let Some(model) = args.get(2) { assert!(model == "fifo" || model == "bag"); }
    Arc::new(move || implementation(q, linearizable))
}
fn main() -> std::process::ExitCode {
    traceforge::program::main(Program {
        visible: &|_| vec!["c0".into(), "c1".into()],
        spec: &entry,
        imp: &entry,
    })
}
