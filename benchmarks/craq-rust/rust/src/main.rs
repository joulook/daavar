//! Port of DeepSeek 3FS's chain replication (CRAQ) of one chunk on one chain of three
//! targets (craq-src: client/storage, storage/service, storage/store) to TraceForge's Must API.
//! Usage: `bench_craq_port count|export|conf … -- <q> [fifo|bag]` (traceforge::program::main).
//! See NOTES.md for the roles, the messages, the function mapping and every decision.
use std::collections::VecDeque;
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{self, Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ---------------------------------------------------------------------------
// The visible messages (INTERFACE.md, verbatim).
// ---------------------------------------------------------------------------
#[derive(Clone, Debug, PartialEq)] enum Req { Write(usize, u64), Read(usize) }   // client -> node: (client, value) / (client)
#[derive(Clone, Debug, PartialEq)] enum Rep { Ack(usize), Value(usize, u64), Unavailable(usize) }    // node -> client: (client) / (client, value) / (client)

// ---------------------------------------------------------------------------
// Constants of the instance.
// ---------------------------------------------------------------------------
const N: usize = 3; // targets t0 (head), t1, t2 (tail)
const CHAIN_VER: u64 = 1; // the fixed chain version (no routing changes)
const TAG_SETUP: u32 = 1; // init -> node: the setup message
const TAG_READ_RSP: u32 = 2; // node -> client: the status of a read (invisible)

// ---------------------------------------------------------------------------
// Internal messages (the source's UpdateReq / UpdateRsp / BatchReadRsp status).
// ---------------------------------------------------------------------------

/// fbs UpdateReq, reduced: the payload (UpdateIO: full-chunk write of `value` with its
/// `update_ver`; the content stands for the checksum), the chain version, and
/// `UpdateOptions::fromClient`. `client` identifies the writer (the source's request tag).
#[derive(Clone, Debug, PartialEq)]
struct UpdateReq {
    client: usize,
    value: u64,
    update_ver: u64,
    chain_ver: u64,
    from_client: bool,
}

/// fbs UpdateRsp, reduced to the IOResult fields the predecessor reads:
/// commitVer, commitChainVer and the checksum (= the content).
#[derive(Clone, Debug, PartialEq)]
struct UpdateRsp {
    commit_ver: u64,
    commit_chain_ver: u64,
    checksum: u64,
}

/// Everything a node's service thread receives (one message type per mailbox).
#[derive(Clone, Debug, PartialEq)]
enum NodeMsg {
    /// init -> node: the addresses of all processes (tag TAG_SETUP).
    Setup { nodes: Vec<ThreadId>, clients: Vec<ThreadId> },
    /// a client's request, handed over by the node's receive thread.
    Client(Req),
    /// predecessor -> node: StorageMessenger::update (doForward).
    Update(UpdateReq),
    /// successor -> node: the reply to the forwarded update.
    UpdateRsp(UpdateRsp),
}

/// node -> client: the status part of the read's IOResult (tag TAG_READ_RSP, invisible).
/// `Ok` is followed by the visible `Rep::Value`; `NotCommit` is StorageCode::kChunkNotCommit
/// with the versions aioPrepareRead put into the result.
#[derive(Clone, Debug, PartialEq)]
enum ReadRsp {
    Ok,
    NotCommit { commit_ver: u64, update_ver: u64 },
}

// ---------------------------------------------------------------------------
// The chunk replica (storage/store/ChunkMetadata, ChunkReplica).
// ---------------------------------------------------------------------------
#[derive(Clone, Copy, Debug, PartialEq)]
enum ChunkState {
    Commit,
    Dirty,
    Clean,
}

#[derive(Debug)]
enum StorageCode {
    ChunkNotClean,
    ChainVersionMismatch,
    ChunkCommittedUpdate,
    ChunkStaleUpdate,
    ChunkMissingUpdate,
    ChunkAdvanceUpdate,
    ChunkVersionMismatch,
    ChunkStaleCommit,
}

/// ChunkMetadata: updateVer, commitVer, chainVer, chunkState; `content` is the chunk's data
/// (and its checksum).
struct Chunk {
    content: u64,
    update_ver: u64,
    commit_ver: u64,
    chain_ver: u64,
    state: ChunkState,
}

impl Chunk {
    /// The chunk exists from the start, committed at version 0 with content 0.
    fn initial() -> Self {
        Chunk { content: 0, update_ver: 0, commit_ver: 0, chain_ver: CHAIN_VER, state: ChunkState::Commit }
    }

    /// ChunkReplica::update (ChunkReplica.cc l.132-318), for a non-syncing full-chunk write.
    /// Returns the new update version.
    fn update(&mut self, io: &UpdateReq) -> Result<u64, StorageCode> {
        if self.state == ChunkState::Dirty {
            return Err(StorageCode::ChunkNotClean);
        }
        if io.chain_ver < self.chain_ver && self.state == ChunkState::Commit {
            return Err(StorageCode::ChainVersionMismatch);
        }
        if io.update_ver > 0 {
            if io.update_ver <= self.commit_ver {
                return Err(StorageCode::ChunkCommittedUpdate);
            } else if io.update_ver <= self.update_ver {
                return Err(StorageCode::ChunkStaleUpdate);
            } else if io.update_ver > self.update_ver + 1 {
                return Err(StorageCode::ChunkMissingUpdate);
            }
            self.update_ver = io.update_ver;
        } else {
            let next = self.update_ver + 1;
            if next > self.commit_ver + 1 {
                // the source has already incremented meta.updateVer in its local copy and
                // does not store it; nothing is stored here either.
                return Err(StorageCode::ChunkAdvanceUpdate);
            }
            self.update_ver = next;
        }
        // DIRTY while writing, CLEAN when the write finished: one atomic step here.
        self.state = ChunkState::Dirty;
        self.chain_ver = io.chain_ver;
        self.content = io.value; // doRealWrite: the full chunk
        self.state = ChunkState::Clean;
        Ok(self.update_ver)
    }

    /// ChunkReplica::commit (ChunkReplica.cc l.397-469), not forced. Returns the commit version.
    fn commit(&mut self, commit_ver: u64, commit_chain_ver: u64) -> Result<u64, StorageCode> {
        if commit_chain_ver < self.chain_ver {
            return Err(StorageCode::ChainVersionMismatch);
        }
        if commit_ver > self.update_ver {
            return Err(StorageCode::ChunkVersionMismatch);
        }
        if self.state == ChunkState::Dirty {
            return Err(StorageCode::ChunkNotClean);
        } else if self.commit_ver < commit_ver {
            self.commit_ver = commit_ver;
        } else {
            return Err(StorageCode::ChunkStaleCommit);
        }
        if self.commit_ver == self.update_ver {
            self.state = ChunkState::Commit;
            self.chain_ver = commit_chain_ver;
        }
        Ok(self.commit_ver)
    }
}

// ---------------------------------------------------------------------------
// A storage node: its target's service (StorageOperator write/update/handleUpdate/batchRead).
// ---------------------------------------------------------------------------

/// An update that holds the chunk lock and waits for its successor's reply
/// (handleUpdate suspended in forwardWithRetry).
struct InFlight {
    req: UpdateReq,
    local_update_ver: u64,
    local_checksum: u64,
}

struct Node {
    idx: usize,
    nodes: Vec<ThreadId>,
    clients: Vec<ThreadId>,
    chunk: Chunk,
    /// The chunk lock (StorageTarget::lockChunk): held by the in-flight update.
    in_flight: Option<InFlight>,
    /// Updates waiting for the chunk lock (co_await lockGuard.lock()), in arrival order.
    waiting: VecDeque<UpdateReq>,
}

impl Node {
    fn is_head(&self) -> bool {
        self.idx == 0
    }
    fn successor(&self) -> Option<ThreadId> {
        if self.idx + 1 < N { Some(self.nodes[self.idx + 1]) } else { None }
    }
    fn predecessor(&self) -> ThreadId {
        self.nodes[self.idx - 1]
    }

    /// StorageOperator::write (l.233): the client's WriteReq becomes an UpdateReq with
    /// fromClient = true (update version 0: allocated by the head) and goes to handleUpdate.
    fn write(&mut self, client: usize, value: u64) {
        let req = UpdateReq { client, value, update_ver: 0, chain_ver: CHAIN_VER, from_client: true };
        self.handle_update(req);
    }

    /// StorageOperator::update (l.284): an UpdateReq forwarded by the predecessor.
    fn update(&mut self, req: UpdateReq) {
        self.handle_update(req);
    }

    /// handleUpdate, part 1-2 (l.333-390): the head check and the chunk lock.
    fn handle_update(&mut self, req: UpdateReq) {
        if req.from_client && !self.is_head() {
            panic!("kRoutingError: non-head node receive a client update request");
        }
        if self.in_flight.is_some() {
            self.waiting.push_back(req); // a second write to the chunk waits for the lock
        } else {
            self.locked_update(req);
        }
    }

    /// handleUpdate, part 3-4 (l.392-460), with the lock held: doUpdate, the version
    /// rules, then forwardWithRetry (or, at the tail, kNoSuccessorTarget and the commit).
    fn locked_update(&mut self, mut req: UpdateReq) {
        // doUpdate (l.521) -> UpdateJob -> ChunkReplica::update
        let update_ver = match self.chunk.update(&req) {
            Ok(v) => {
                if req.update_ver == 0 {
                    req.update_ver = v;
                } else if req.update_ver != v {
                    panic!("kChunkVersionMismatch: write update version mismatch");
                }
                v
            }
            Err(StorageCode::ChunkCommittedUpdate) => {
                // committed update, considered as a successful write: reply at once
                let v = req.update_ver;
                self.reply_and_unlock(&req, v);
                return;
            }
            Err(StorageCode::ChunkStaleUpdate) => req.update_ver, // considered successful
            Err(e) => panic!("update failed: {e:?}"),
        };
        let checksum = self.chunk.content;
        // forward to the successor (ReliableForwarding::forward / doForward, fromClient = false)
        match self.successor() {
            Some(succ) => {
                let fwd = UpdateReq { from_client: false, chain_ver: CHAIN_VER, ..req.clone() };
                send_msg(succ, NodeMsg::Update(fwd));
                self.in_flight = Some(InFlight { req, local_update_ver: update_ver, local_checksum: checksum });
            }
            None => {
                // kNoSuccessorTarget: commitIO.commitVer = updateResult.updateVer, the
                // chain's own version as commitChainVer; commit at once.
                self.commit_and_reply(&req, update_ver, CHAIN_VER);
            }
        }
    }

    /// handleUpdate after forwardWithRetry returned the successor's reply (l.460-495):
    /// commitIO.commitVer from the successor, the version and checksum checks, then doCommit.
    fn on_successor_reply(&mut self, rsp: UpdateRsp) {
        let fl = self.in_flight.take().expect("a reply without an update in flight");
        if rsp.commit_chain_ver > CHAIN_VER {
            panic!("kChainVersionMismatch: the remote obtains a higher chain version");
        }
        if rsp.commit_ver != fl.local_update_ver {
            panic!("kChunkVersionMismatch: commit version mismatch, successor != local");
        }
        if rsp.checksum != fl.local_checksum {
            panic!("kChecksumMismatch: local checksum not equal to the successor's");
        }
        self.commit_and_reply(&fl.req, rsp.commit_ver, rsp.commit_chain_ver);
    }

    /// handleUpdate part 5 (l.496-520): doCommit (l.616) -> ChunkReplica::commit; a stale
    /// commit counts as a success; then the reply and the lock's release.
    fn commit_and_reply(&mut self, req: &UpdateReq, commit_ver: u64, commit_chain_ver: u64) {
        let committed = match self.chunk.commit(commit_ver, commit_chain_ver) {
            Ok(v) => v,
            Err(StorageCode::ChunkStaleCommit) => commit_ver,
            Err(e) => panic!("commit failed: {e:?}"),
        };
        self.reply_and_unlock(req, committed);
    }

    /// The handleUpdate coroutine returns: its result goes to the caller (the client's
    /// WriteRsp at the head, the predecessor's UpdateRsp elsewhere) and the lock guard is
    /// released, waking the next waiter.
    fn reply_and_unlock(&mut self, req: &UpdateReq, commit_ver: u64) {
        if req.from_client {
            send_msg(self.clients[req.client], Rep::Ack(req.client));
        } else {
            let rsp = UpdateRsp { commit_ver, commit_chain_ver: CHAIN_VER, checksum: self.chunk.content };
            send_msg(self.predecessor(), NodeMsg::UpdateRsp(rsp));
        }
        self.in_flight = None;
        if let Some(next) = self.waiting.pop_front() {
            self.locked_update(next);
        }
    }

    /// StorageOperator::batchRead (l.82-233) for one ReadIO, readUncommitted = false:
    /// ChunkReplica::aioPrepareRead (l.38-78) and aioFinishRead (l.79-131).
    fn batch_read(&mut self, client: usize) {
        let c = &self.chunk;
        let to = self.clients[client];
        if c.commit_ver != c.update_ver {
            send_tagged_msg(to, TAG_READ_RSP, ReadRsp::NotCommit { commit_ver: c.commit_ver, update_ver: c.update_ver });
            return;
        }
        // aioFinishRead: the update version cannot change during the (atomic) read here.
        send_tagged_msg(to, TAG_READ_RSP, ReadRsp::Ok);
        send_msg(to, Rep::Value(client, c.content));
    }
}

/// A storage node's service thread (named n0, n1, n2).
fn node(idx: usize) {
    let NodeMsg::Setup { nodes, clients } = recv_tagged_msg_block::<_, NodeMsg>(|_, t| t == Some(TAG_SETUP)) else {
        panic!("Setup expected")
    };
    let mut n = Node { idx, nodes, clients, chunk: Chunk::initial(), in_flight: None, waiting: VecDeque::new() };
    loop {
        match recv_msg_block::<NodeMsg>() {
            NodeMsg::Client(Req::Write(c, v)) => n.write(c, v),
            NodeMsg::Client(Req::Read(c)) => n.batch_read(c),
            NodeMsg::Update(u) => n.update(u),
            NodeMsg::UpdateRsp(r) => n.on_successor_reply(r),
            NodeMsg::Setup { .. } => panic!("a second Setup"),
        }
    }
}

/// A node's receive thread (named n0.rx, …): the address the clients send to; it hands
/// each client request to its node's service thread (one message type per mailbox).
fn node_rx(service: ThreadId) {
    loop {
        let r = recv_msg_block::<Req>();
        send_msg(service, NodeMsg::Client(r));
    }
}

// ---------------------------------------------------------------------------
// The client (StorageClientImpl): writes to the head, reads by LoadBalance, fast retry
// on kChunkNotCommit.
// ---------------------------------------------------------------------------

/// TargetSelection LoadBalance: a nondeterministic choice among the serving targets.
fn select_target() -> usize {
    if nondet() {
        0
    } else if nondet() {
        1
    } else {
        2
    }
}

const CLIENTS: usize = 2;
fn value(i: usize, j: usize) -> u64 { (1 + i + CLIENTS * j) as u64 }
fn client(i: usize, q: usize, head: ThreadId, targets: Vec<ThreadId>) {
    for j in 0..q {
        match (i + j) % 2 {
            0 => { send_msg(head, Req::Write(i, value(i, j))); let Rep::Ack(_) = recv_msg_block::<Rep>() else { panic!("Ack expected") }; }
            _ => {
                // batchReadWithRetry: select a target, send the read; on kChunkNotCommit
                // (isFastRetryError) select again and resend at once.
                let mut first = true;
                let mut seen: Vec<(usize, u64, u64)> = Vec::new();
                loop {
                    let t = select_target();
                    if first {
                        send_msg(targets[t], Req::Read(i));
                        first = false;
                    } else {
                        send_msg_as(targets[t], Visibility::Invisible, Req::Read(i));
                    }
                    let st = recv_tagged_msg_block_as::<_, ReadRsp>(Visibility::Invisible, |_, tag| tag == Some(TAG_READ_RSP));
                    match st {
                        ReadRsp::Ok => break,
                        ReadRsp::NotCommit { commit_ver, update_ver } => {
                            // a repeated answer (same target, same versions) is a stutter:
                            // nothing changed in between; cut it (NOTES.md, D7).
                            let key = (t, commit_ver, update_ver);
                            if seen.contains(&key) {
                                // COORDINATOR (owner, 2026-11-06): the cut ends the read with the
                                // visible reply Unavailable(i), sent (invisibly) by this retry loop
                                // to the reading client, instead of assume(false).
                                send_msg_as(thread::current().id(), Visibility::Invisible, Rep::Unavailable(i));
                                break;
                            }
                            seen.push(key);
                        }
                    }
                }
                let (Rep::Value(..) | Rep::Unavailable(_)) = recv_msg_block::<Rep>() else { panic!("Value or Unavailable expected") };
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The program entry.
// ---------------------------------------------------------------------------
fn imp(q: usize) {
    let mut services = Vec::new();
    for k in 0..N {
        let h = thread::Builder::new().name(format!("n{k}")).spawn(move || node(k)).unwrap();
        services.push(h.thread().id());
    }
    let mut targets = Vec::new();
    for k in 0..N {
        let s = services[k];
        let h = thread::Builder::new().name(format!("n{k}.rx")).spawn(move || node_rx(s)).unwrap();
        targets.push(h.thread().id());
    }
    let head = targets[0];
    let mut clients = Vec::new();
    let mut client_ids = Vec::new();
    for i in 0..CLIENTS {
        let ts = targets.clone();
        let h = thread::Builder::new().name(format!("c{i}")).spawn(move || client(i, q, head, ts)).unwrap();
        client_ids.push(h.thread().id());
        clients.push(h);
    }
    let svc = services.clone();
    let init = thread::Builder::new()
        .name("init".to_string())
        .spawn(move || {
            for &s in &svc {
                send_tagged_msg(s, TAG_SETUP, NodeMsg::Setup { nodes: svc.clone(), clients: client_ids.clone() });
            }
        })
        .unwrap();
    init.join().unwrap();
    for c in clients {
        c.join().unwrap();
    }
}

fn entry(args: &[String]) -> Entry {
    let q: usize = args.first().and_then(|s| s.parse().ok()).unwrap_or(1);
    Arc::new(move || imp(q))
}

fn main() -> ExitCode {
    program::main(Program {
        visible: &|_| vec!["c0".to_string(), "c1".to_string()],
        spec: &entry,
        imp: &entry,
    })
}
