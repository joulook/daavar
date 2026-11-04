use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::{recv_msg_block, recv_tagged_msg_block, send_msg, send_tagged_msg};

#[derive(Clone, Debug, PartialEq)]
enum Req {
    Propose(u64),
}
#[derive(Clone, Debug, PartialEq)]
enum Rep {
    Committed(u64),
}

const CLIENTS: usize = 2;
fn value(i: usize, j: usize) -> u64 {
    (1 + i + CLIENTS * j) as u64
}
fn client(i: usize, e: usize, server: ThreadId) {
    let mine: Vec<u64> = (0..e).map(|j| value(i, j)).collect();
    for v in &mine {
        send_msg(server, Req::Propose(*v));
    }
    let mut seen = 0usize;
    while seen < e {
        let Rep::Committed(v) = recv_msg_block::<Rep>();
        if mine.contains(&v) {
            seen += 1;
        }
    }
}

const EPOCH: u64 = 1;
const NEW_ZXID: u64 = EPOCH << 32;
const SETUP: u32 = u32::MAX;
#[derive(Clone, Debug, PartialEq)]
struct Setup {
    nodes: Vec<ThreadId>,
    clients: Vec<ThreadId>,
}
#[derive(Clone, Debug, PartialEq)]
struct Txn {
    zxid: u64,
    value: u64,
}
#[derive(Clone, Debug, PartialEq)]
enum Packet {
    Start,
    FollowerInfo { sid: usize, accepted: u64 },
    LeaderInfo { epoch: u64 },
    AckEpoch { sid: usize, current: i64, last: u64 },
    Diff(u64),
    Trunc(u64),
    Snap { last: u64, tree: Vec<Txn> },
    NewLeader(u64),
    UpToDate,
    Proposal(Txn),
    Ack { sid: usize, zxid: u64 },
    Commit(u64),
    Request(u64),
}

// One ordered socket stream per directed server connection. Tags represent
// positions in that stream, not additional Zab packets or acknowledgements.
struct Wire {
    id: usize,
    nodes: Vec<ThreadId>,
    sent: [u32; 3],
    received: [u32; 3],
}
impl Wire {
    fn new(id: usize, nodes: Vec<ThreadId>) -> Self {
        Self {
            id,
            nodes,
            sent: [0; 3],
            received: [0; 3],
        }
    }
    fn send(&mut self, to: usize, p: Packet) {
        let tag = self.sent[to];
        self.sent[to] = tag.checked_add(1).expect("stream position overflow");
        send_tagged_msg(self.nodes[to], tag, (self.id, p));
    }
    fn recv(&mut self) -> Packet {
        let nodes = self.nodes.clone();
        let expected = self.received;
        // The sender is encoded into a private envelope so we can advance
        // precisely the selected stream without exposing runtime internals.
        let (from, p): (usize, Packet) = recv_tagged_msg_block(move |tid, tag| {
            nodes
                .iter()
                .position(|n| *n == tid)
                .is_some_and(|i| tag == Some(expected[i]))
        });
        self.received[from] += 1;
        p
    }
}

#[derive(Default)]
struct Database {
    log: Vec<Txn>,
    tree: Vec<Txn>,
    committed_log: Vec<Txn>,
    last: u64,
}
impl Database {
    fn append(&mut self, t: Txn) {
        self.log.push(t);
    }
    fn apply(&mut self, t: Txn, client: Option<ThreadId>) {
        assert!(t.zxid > self.last, "data tree zxids must increase");
        self.last = t.zxid;
        self.tree.push(t.clone());
        self.committed_log.push(t.clone());
        if let Some(c) = client {
            send_msg(c, Rep::Committed(t.value));
        }
    }
    fn truncate(&mut self, zxid: u64) {
        self.log.retain(|t| t.zxid <= zxid);
        self.tree.retain(|t| t.zxid <= zxid);
        self.committed_log.retain(|t| t.zxid <= zxid);
        self.last = zxid;
    }
}

struct Proposal {
    txn: Txn,
    acks: [bool; 3],
}
struct Leader {
    wire: Wire,
    db: Database,
    // 0: absent, 1: connected, 2: LEADERINFO sent, 3: ACKEPOCH,
    // 4: synchronising, 5: NEWLEADER ack, 6: UPTODATE queued.
    phase: [u8; 3],
    summaries: [u64; 3],
    epoch_acks: [bool; 3],
    newleader_acks: [bool; 3],
    election_finished: bool,
    started: bool,
    admitted: bool,
    late: bool,
    late_started: bool,
    last_proposed: u64,
    last_committed: u64,
    outstanding: BTreeMap<u64, Proposal>,
    requests: VecDeque<u64>,
    local: Vec<u64>,
    client: ThreadId,
}
fn quorum(acks: &[bool; 3]) -> bool {
    acks.iter().filter(|a| **a).count() >= 2
}
impl Leader {
    fn sync_follower(&mut self, sid: usize, peer: u64) {
        // Retained in-memory txn log supplies the same prefix as the disk-log
        // fallback in syncFollower. No history is evicted in this model.
        let max = self
            .db
            .committed_log
            .last()
            .map_or(self.db.last, |t| t.zxid);
        let new_epoch_zxid = peer & 0xffff_ffff == 0;
        let mut cursor = peer;
        if peer == self.db.last {
            self.wire.send(sid, Packet::Diff(peer));
        } else if peer > max && !new_epoch_zxid {
            self.wire.send(sid, Packet::Trunc(max));
            cursor = max;
        } else if new_epoch_zxid || self.db.committed_log.iter().any(|t| t.zxid == peer) {
            self.wire.send(sid, Packet::Diff(max));
        } else if let Some(next) = self.db.committed_log.iter().find(|t| t.zxid > peer) {
            if next.zxid >> 32 == peer >> 32 {
                cursor = self
                    .db
                    .committed_log
                    .iter()
                    .filter(|t| t.zxid < peer)
                    .map(|t| t.zxid)
                    .last()
                    .unwrap_or(0);
                self.wire.send(sid, Packet::Trunc(cursor));
            } else {
                cursor = self.db.last;
                self.wire.send(
                    sid,
                    Packet::Snap {
                        last: cursor,
                        tree: self.db.tree.clone(),
                    },
                );
            }
        } else {
            cursor = self.db.last;
            self.wire.send(
                sid,
                Packet::Snap {
                    last: cursor,
                    tree: self.db.tree.clone(),
                },
            );
        }
        // queueCommittedProposals, followed by startForwarding, then the
        // modern LearnerHandler's queued NEWLEADER (source order).
        for t in &self.db.committed_log {
            if t.zxid > cursor {
                self.wire.send(sid, Packet::Proposal(t.clone()));
                self.wire.send(sid, Packet::Commit(t.zxid));
            }
        }
        for p in self.outstanding.values() {
            if p.txn.zxid > cursor {
                self.wire.send(sid, Packet::Proposal(p.txn.clone()));
            }
        }
        self.phase[sid] = 4; // startForwarding includes this follower now
        self.wire.send(sid, Packet::NewLeader(NEW_ZXID));
    }
    fn advance_barriers(&mut self) {
        // getEpochToPropose: n0 plus one connected participant.
        if self.phase[1..].iter().any(|p| *p >= 1) {
            for sid in 1..3 {
                if self.phase[sid] == 1 {
                    self.wire.send(sid, Packet::LeaderInfo { epoch: EPOCH });
                    self.phase[sid] = 2;
                }
            }
        }
        if quorum(&self.epoch_acks) {
            self.election_finished = true;
        }
        if self.election_finished {
            for sid in 1..3 {
                if self.phase[sid] == 3 {
                    self.sync_follower(sid, self.summaries[sid]);
                }
            }
        }
        if !self.started && quorum(&self.newleader_acks) {
            self.started = true;
            self.last_committed = NEW_ZXID;
            self.db.last = NEW_ZXID;
        }
        if self.started {
            for sid in 1..3 {
                if self.phase[sid] == 5 {
                    self.wire.send(sid, Packet::UpToDate);
                    self.phase[sid] = 6;
                }
            }
        }
        // INTERFACE's all-join gate strengthens the source's quorum startup.
        if !self.admitted && self.started && (self.late || self.phase[1..].iter().all(|p| *p == 6))
        {
            self.admitted = true;
            // Represent PrepRequestProcessor's local queue with self messages;
            // arrival order chooses between this queue and forwarded REQUESTs.
            for v in std::mem::take(&mut self.local) {
                self.wire.send(0, Packet::Request(v));
            }
        }
        if self.admitted {
            while let Some(v) = self.requests.pop_front() {
                self.propose(v);
            }
        }
    }
    fn propose(&mut self, value: u64) {
        self.last_proposed += 1;
        assert!(
            self.last_proposed & 0xffff_ffff != 0xffff_ffff,
            "zxid rollover needs an election"
        );
        let txn = Txn {
            zxid: self.last_proposed,
            value,
        };
        self.outstanding.insert(
            txn.zxid,
            Proposal {
                txn: txn.clone(),
                acks: [false; 3],
            },
        );
        for sid in 1..3 {
            if self.phase[sid] >= 4 {
                self.wire.send(sid, Packet::Proposal(txn.clone()));
            }
        }
        // ProposalRequestProcessor -> SyncRequestProcessor -> AckRequestProcessor.
        self.db.append(txn.clone());
        self.process_ack(0, txn.zxid);
    }
    fn process_ack(&mut self, sid: usize, zxid: u64) {
        if zxid & 0xffff_ffff == 0 || zxid <= self.last_committed {
            return;
        }
        let Some(p) = self.outstanding.get_mut(&zxid) else {
            return;
        };
        p.acks[sid] = true;
        // In the one-configuration FIFO ACK stream, preceding proposals have
        // a quorum first. Keep tryToCommit's predecessor guard explicitly.
        if self.outstanding.contains_key(&(zxid - 1)) || !quorum(&self.outstanding[&zxid].acks) {
            return;
        }
        let p = self.outstanding.remove(&zxid).unwrap();
        assert_eq!(zxid, self.last_committed + 1);
        self.last_committed = zxid;
        for sid in 1..3 {
            if self.phase[sid] >= 4 {
                self.wire.send(sid, Packet::Commit(zxid));
            }
        }
        self.db.apply(p.txn, Some(self.client));
        if self.late && !self.late_started {
            self.late_started = true;
            self.wire.send(2, Packet::Start);
        }
    }
    fn run(mut self) {
        loop {
            match self.wire.recv() {
                Packet::FollowerInfo { sid, accepted } => {
                    assert!(accepted < EPOCH);
                    assert_eq!(self.phase[sid], 0);
                    self.phase[sid] = 1;
                }
                Packet::AckEpoch { sid, current, last } => {
                    assert_eq!(self.phase[sid], 2);
                    if !self.election_finished && current != -1 {
                        assert!(
                            current <= 0 && last == 0,
                            "follower ahead of elected leader"
                        );
                        self.epoch_acks[sid] = true;
                    }
                    self.summaries[sid] = last;
                    self.phase[sid] = 3;
                }
                Packet::Ack { sid, zxid } => {
                    if self.phase[sid] == 4 {
                        assert_eq!(zxid, NEW_ZXID);
                        self.newleader_acks[sid] = true;
                        self.phase[sid] = 5;
                    } else {
                        self.process_ack(sid, zxid);
                    }
                }
                Packet::Request(v) => self.requests.push_back(v),
                p => panic!("unexpected leader packet: {p:?}"),
            }
            self.advance_barriers();
        }
    }
}

struct Follower {
    sid: usize,
    wire: Wire,
    db: Database,
    pending_txns: VecDeque<Txn>,
    not_logged: VecDeque<Txn>,
    commits: VecDeque<u64>,
    last_queued: u64,
    current_epoch: u64,
    accepted_epoch: u64,
    client: Option<ThreadId>,
    local: Vec<u64>,
}
impl Follower {
    fn ack(&mut self, zxid: u64) {
        self.wire.send(
            0,
            Packet::Ack {
                sid: self.sid,
                zxid,
            },
        );
    }
    fn log_request(&mut self, t: Txn) {
        self.pending_txns.push_back(t.clone());
        self.db.append(t.clone());
        self.ack(t.zxid);
    }
    fn commit(&mut self, zxid: u64) {
        let Some(t) = self.pending_txns.front() else {
            return;
        };
        assert_eq!(t.zxid, zxid, "UNMATCHED_TXN_COMMIT: pendingTxns head");
        let t = self.pending_txns.pop_front().unwrap();
        self.db.apply(t, self.client);
    }
    fn enforce_continuous_proposal(&mut self, zxid: u64) {
        assert!(
            self.last_queued == 0
                || zxid == self.last_queued + 1
                || zxid >> 32 > self.last_queued >> 32,
            "discontinuous DIFF proposal"
        );
        self.last_queued = zxid;
    }
    fn run(mut self, late: bool) {
        if late {
            assert_eq!(self.wire.recv(), Packet::Start);
        }
        self.wire.send(
            0,
            Packet::FollowerInfo {
                sid: self.sid,
                accepted: self.accepted_epoch,
            },
        );
        let Packet::LeaderInfo { epoch } = self.wire.recv() else {
            panic!("expected LEADERINFO")
        };
        assert!(epoch >= self.accepted_epoch);
        let current = if epoch == self.accepted_epoch {
            -1
        } else {
            self.current_epoch as i64
        };
        self.accepted_epoch = epoch;
        self.wire.send(
            0,
            Packet::AckEpoch {
                sid: self.sid,
                current,
                last: self.db.log.last().map_or(0, |t| t.zxid),
            },
        );
        match self.wire.recv() {
            Packet::Diff(_last) => (), // source deliberately keeps lastQueued = 0
            Packet::Trunc(zxid) => self.db.truncate(zxid),
            Packet::Snap { last, tree } => {
                self.db = Database {
                    tree,
                    last,
                    ..Database::default()
                };
            }
            p => panic!("expected DIFF/TRUNC/SNAP, got {p:?}"),
        }
        let mut got_newleader = false;
        loop {
            match self.wire.recv() {
                Packet::Proposal(t) => {
                    self.enforce_continuous_proposal(t.zxid);
                    self.not_logged.push_back(t);
                }
                Packet::Commit(zxid) => self.commits.push_back(zxid),
                Packet::NewLeader(zxid) => {
                    assert_eq!(zxid, epoch << 32);
                    assert!(!got_newleader);
                    // Learner.syncWithLeader persists and applies the committed
                    // replay prefix without per-proposal ACKs before this ACK.
                    while let Some(z) = self.commits.pop_front() {
                        let t = self
                            .not_logged
                            .pop_front()
                            .expect("sync commit without proposal");
                        assert_eq!(t.zxid, z);
                        self.db.append(t.clone());
                        self.db.apply(t, self.client);
                    }
                    self.current_epoch = epoch;
                    got_newleader = true;
                    self.ack(zxid);
                }
                Packet::UpToDate => {
                    assert!(got_newleader);
                    self.ack(epoch << 32);
                    while let Some(t) = self.not_logged.pop_front() {
                        self.log_request(t);
                    }
                    while let Some(z) = self.commits.pop_front() {
                        self.commit(z);
                    }
                    for v in std::mem::take(&mut self.local) {
                        self.wire.send(0, Packet::Request(v));
                    }
                    break;
                }
                p => panic!("unexpected sync packet: {p:?}"),
            }
        }
        loop {
            match self.wire.recv() {
                Packet::Proposal(t) => {
                    // Follower.processPacket only warns about gaps here; the
                    // pendingTxns check on COMMIT remains authoritative.
                    self.last_queued = t.zxid;
                    self.log_request(t);
                }
                Packet::Commit(zxid) => self.commit(zxid),
                p => panic!("unexpected broadcast packet: {p:?}"),
            }
        }
    }
}

fn server(sid: usize, e: usize, late: bool) {
    // Selective receive leaves all pre-Setup messages buffered in the mailbox.
    let setup: Setup = recv_tagged_msg_block(|_, tag| tag == Some(SETUP));
    // The mandated client sends its entire finite batch before receiving.
    // Collect that batch using its distinct wire type, then model admission
    // through the local request queue. No helper processes are introduced.
    let mut local = Vec::new();
    if sid < CLIENTS {
        let c = setup.clients[sid];
        for _ in 0..e {
            let Req::Propose(v) =
                recv_tagged_msg_block(move |from, tag| from == c && tag.is_none());
            local.push(v);
        }
    }
    let wire = Wire::new(sid, setup.nodes);
    if sid == 0 {
        Leader {
            wire,
            db: Database::default(),
            phase: [0; 3],
            summaries: [0; 3],
            epoch_acks: [true, false, false],
            newleader_acks: [true, false, false],
            election_finished: false,
            started: false,
            admitted: false,
            late,
            late_started: false,
            last_proposed: NEW_ZXID,
            last_committed: 0,
            outstanding: BTreeMap::new(),
            requests: VecDeque::new(),
            local,
            client: setup.clients[0],
        }
        .run();
    } else {
        Follower {
            sid,
            wire,
            db: Database::default(),
            pending_txns: VecDeque::new(),
            not_logged: VecDeque::new(),
            commits: VecDeque::new(),
            last_queued: 0,
            current_epoch: 0,
            accepted_epoch: 0,
            client: setup.clients.get(sid).copied(),
            local,
        }
        .run(late && sid == 2);
    }
}
fn implementation(e: usize, late: bool) {
    let mut servers = Vec::new();
    for sid in 0..3 {
        servers.push(
            thread::Builder::new()
                .name(format!("n{sid}"))
                .spawn(move || server(sid, e, late))
                .unwrap(),
        );
    }
    let nodes: Vec<_> = servers.iter().map(|s| s.thread().id()).collect();
    let mut clients = Vec::new();
    for (i, &server) in nodes.iter().take(CLIENTS).enumerate() {
        clients.push(
            thread::Builder::new()
                .name(format!("c{i}"))
                .spawn(move || client(i, e, server))
                .unwrap(),
        );
    }
    let ids = clients.iter().map(|c| c.thread().id()).collect();
    let init = thread::Builder::new()
        .name("init".into())
        .spawn(move || {
            let setup = Setup {
                nodes,
                clients: ids,
            };
            for &s in &setup.nodes {
                send_tagged_msg(s, SETUP, setup.clone());
            }
        })
        .unwrap();
    init.join().unwrap();
    for c in clients {
        c.join().unwrap();
    }
    for s in servers {
        s.join().unwrap();
    }
}
fn entry(args: &[String]) -> Entry {
    assert!(
        (2..=3).contains(&args.len()),
        "arguments: <e> <late> [fifo|bag]"
    );
    let e: usize = args[0].parse().expect("e must be a positive integer");
    let late: usize = args[1].parse().expect("late must be 0 or 1");
    assert!(e >= 1 && late <= 1);
    assert!((e as u128) * (CLIENTS as u128) < 0xffff_ffff);
    if let Some(model) = args.get(2) {
        assert!(model == "fifo" || model == "bag");
    }
    Arc::new(move || implementation(e, late == 1))
}
fn main() -> std::process::ExitCode {
    traceforge::program::main(Program {
        visible: &|_| vec!["c0".into(), "c1".into()],
        spec: &entry,
        imp: &entry,
    })
}
