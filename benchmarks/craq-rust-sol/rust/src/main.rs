use std::collections::VecDeque;
use std::sync::Arc;
use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::{nondet, recv_msg_block, recv_tagged_msg_block, send_msg, send_msg_as,
    send_tagged_msg, send_tagged_msg_as, recv_msg_block_as, Visibility};

#[derive(Clone, Debug, PartialEq)]
enum Req { Write(usize, u64), Read(usize) }
#[derive(Clone, Debug, PartialEq)]
enum Rep { Ack(usize), Value(usize, u64) }

const CLIENTS: usize = 2;
const EVENT: u32 = 1;
const CHAIN_VERSION: u64 = 1;
#[derive(Clone, Debug, PartialEq)]
struct Setup { nodes: Vec<ThreadId>, clients: Vec<ThreadId> }
#[derive(Clone, Debug, PartialEq)]
enum Event {
    // A hidden dispatch header permits typed receives of the verbatim Req payload.
    Client(usize),
    Update { client: usize, value: u64, version: u64, chain: u64 },
    Committed { version: u64, checksum: u64, chain: u64 },
}
#[derive(Clone, Debug, PartialEq)]
enum ReadStatus { Ready, NotCommit { update: u64, commit: u64 } }
#[derive(Default)]
struct ChunkReplica { content: u64, update: u64, commit: u64 }
impl ChunkReplica {
    fn update(&mut self, content: u64, version: u64) {
        assert_eq!(self.update, self.commit);
        assert_eq!(version, self.update + 1);
        self.content = content;
        self.update = version;
    }
    fn commit(&mut self, version: u64) {
        assert_eq!(version, self.update);
        assert!(version > self.commit);
        self.commit = version;
    }
    fn aio_prepare_read(&self) -> ReadStatus {
        if self.commit == self.update { ReadStatus::Ready }
        else { ReadStatus::NotCommit { update: self.update, commit: self.commit } }
    }
    fn aio_finish_read(&self, prepared: u64) -> u64 {
        assert_eq!(prepared, self.update);
        self.content
    }
}
#[derive(Clone)]
struct Write { client: usize, value: u64, version: u64 }
fn forward_with_retry(index: usize, setup: &Setup, w: &Write) -> bool {
    if index == 2 { return false; }
    // There are no failures or timeouts: exactly one forward, no retry loop.
    send_tagged_msg(setup.nodes[index + 1], EVENT, Event::Update {
        client: w.client, value: w.value, version: w.version, chain: CHAIN_VERSION,
    });
    true
}
fn do_commit(index: usize, setup: &Setup, chunk: &mut ChunkReplica, w: Write) {
    chunk.commit(w.version);
    if index == 0 { send_msg(setup.clients[w.client], Rep::Ack(w.client)); }
    else { send_tagged_msg(setup.nodes[index - 1], EVENT, Event::Committed {
        version: chunk.commit, checksum: chunk.content, chain: CHAIN_VERSION,
    }); }
}
fn handle_update(index: usize, setup: &Setup, chunk: &mut ChunkReplica, mut w: Write) -> Option<Write> {
    if index == 0 { w.version = chunk.update + 1; }
    chunk.update(w.value, w.version);
    if forward_with_retry(index, setup, &w) { Some(w) }
    else { do_commit(index, setup, chunk, w); None }
}
fn batch_read(setup: &Setup, chunk: &ChunkReplica, client: usize) {
    let status = chunk.aio_prepare_read();
    let ready = status == ReadStatus::Ready;
    send_msg_as(setup.clients[client], Visibility::Invisible, status);
    if ready {
        let value = chunk.aio_finish_read(chunk.update);
        send_msg(setup.clients[client], Rep::Value(client, value));
    }
}
fn storage_node(index: usize) {
    let setup = recv_tagged_msg_block::<_, Setup>(|_, tag| tag == Some(0));
    let mut chunk = ChunkReplica::default();
    let mut active: Option<Write> = None;
    let mut waiting = VecDeque::new();
    loop {
        let event = recv_tagged_msg_block::<_, Event>(|_, tag| tag == Some(EVENT));
        match event {
            Event::Client(client) => {
                let sender = setup.clients[client];
                let req = recv_tagged_msg_block::<_, Req>(move |id, tag| id == sender && tag.is_none());
                match req {
                    Req::Read(i) => { assert_eq!(i, client); batch_read(&setup, &chunk, i); }
                    Req::Write(i, value) => {
                        assert_eq!(index, 0);
                        assert_eq!(i, client);
                        let w = Write { client, value, version: 0 };
                        if active.is_some() { waiting.push_back(w); }
                        else { active = handle_update(index, &setup, &mut chunk, w); }
                    }
                }
            }
            Event::Update { client, value, version, chain } => {
                assert!(index > 0);
                assert_eq!(chain, CHAIN_VERSION);
                let w = Write { client, value, version };
                if active.is_some() { waiting.push_back(w); }
                else { active = handle_update(index, &setup, &mut chunk, w); }
            }
            Event::Committed { version, checksum, chain } => {
                assert_eq!(chain, CHAIN_VERSION);
                let w = active.take().expect("successor reply requires locked write");
                assert_eq!(version, w.version);
                assert_eq!(checksum, chunk.content);
                do_commit(index, &setup, &mut chunk, w);
                if let Some(next) = waiting.pop_front() {
                    active = handle_update(index, &setup, &mut chunk, next);
                }
            }
        }
    }
}
fn value(i: usize, j: usize) -> u64 { (1 + i + CLIENTS * j) as u64 }
fn select_target() -> usize {
    if nondet() { 0 } else if nondet() { 1 } else { 2 }
}
fn request_header(target: ThreadId, client: usize) {
    send_tagged_msg_as(target, Visibility::Invisible, EVENT, Event::Client(client));
}
fn client(i: usize, q: usize, head: ThreadId, targets: Vec<ThreadId>) {
    for j in 0..q {
        match (i + j) % 2 {
            0 => {
                request_header(head, i);
                send_msg(head, Req::Write(i, value(i, j)));
                let Rep::Ack(_) = recv_msg_block::<Rep>() else { panic!("Ack expected") };
            }
            _ => {
                // Study cut: remember all failed answers per target for this read.
                let mut seen: [Vec<(u64, u64)>; 3] = Default::default();
                let mut initial = true;
                loop {
                    let target = select_target();
                    request_header(targets[target], i);
                    if initial { send_msg(targets[target], Req::Read(i)); }
                    else { send_msg_as(targets[target], Visibility::Invisible, Req::Read(i)); }
                    initial = false;
                    match recv_msg_block_as::<ReadStatus>(Visibility::Invisible) {
                        ReadStatus::Ready => break,
                        ReadStatus::NotCommit { update, commit } => {
                            let answer = (update, commit);
                            traceforge::assume(!seen[target].contains(&answer));
                            seen[target].push(answer);
                        }
                    }
                }
                let Rep::Value(..) = recv_msg_block::<Rep>() else { panic!("Value expected") };
            }
        }
    }
}
fn run(q: usize) {
    let mut nodes = Vec::new();
    for index in 0..3 {
        nodes.push(thread::Builder::new().name(format!("n{index}"))
            .spawn(move || storage_node(index)).unwrap());
    }
    let targets: Vec<_> = nodes.iter().map(|n| n.thread().id()).collect();
    let mut clients = Vec::new();
    for i in 0..CLIENTS {
        let targets = targets.clone();
        clients.push(thread::Builder::new().name(format!("c{i}"))
            .spawn(move || client(i, q, targets[0], targets)).unwrap());
    }
    let setup = Setup { nodes: targets, clients: clients.iter().map(|c| c.thread().id()).collect() };
    let init = thread::Builder::new().name("init".to_string()).spawn(move || {
        for node in setup.nodes.iter().copied() { send_tagged_msg(node, 0, setup.clone()); }
    }).unwrap();
    init.join().unwrap();
    for c in clients { c.join().unwrap(); }
    for n in nodes { n.join().unwrap(); }
}
fn entry(args: &[String]) -> Entry {
    let q = args.first().expect("expected q").parse::<usize>().expect("q must be an integer");
    assert!(q >= 1);
    Arc::new(move || run(q))
}
fn main() -> std::process::ExitCode {
    traceforge::program::main(Program {
        visible: &|_| vec!["c0".to_string(), "c1".to_string()], spec: &entry, imp: &entry,
    })
}
