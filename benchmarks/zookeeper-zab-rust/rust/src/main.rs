//! ZooKeeper 3.9's atomic broadcast (Zab) with the fixed leader n0 of epoch 1, ported to
//! TraceForge's Must API (see NOTES.md for the role-by-role description, the method map and
//! every decision). Program arguments: `<e> <late> [<fifo|bag>]`.
//!
//! Processes: servers `n0` (leader), `n1`, `n2` (followers), clients `c0`, `c1`, and the
//! invisible `init` thread that hands the servers their `Setup`.
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{self, Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ---------------------------------------------------------------------------------------------
// The interface (INTERFACE.md), verbatim where it says so.
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum Req {
    Propose(u64),
} // client -> server
#[derive(Clone, Debug, PartialEq)]
enum Rep {
    Committed(u64),
} // server -> client

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

/// Sent by `init` to every server (tagged `SETUP_TAG`), so that `main` sends nothing.
#[derive(Clone, Debug, PartialEq)]
struct Setup {
    nodes: Vec<ThreadId>,
    clients: Vec<ThreadId>,
}

/// N = 3 servers; the leader is n0; the quorum (QuorumMaj of 3) is 2, the leader counting itself.
const N: usize = 3;
const LEADER: usize = 0;
const QUORUM: usize = N / 2 + 1;
const SETUP_TAG: u32 = u32::MAX;
/// LearnerInfo / LEADERINFO protocol version (Learner.java:498, LearnerHandler.java:536).
const PROTOCOL_VERSION: u32 = 0x10000;

// ---------------------------------------------------------------------------------------------
// ZxidUtils
// ---------------------------------------------------------------------------------------------

fn make_zxid(epoch: u64, counter: u64) -> u64 {
    (epoch << 32) | (counter & 0xffff_ffff)
}
fn epoch_of(zxid: u64) -> u64 {
    zxid >> 32
}
fn counter_of(zxid: u64) -> u64 {
    zxid & 0xffff_ffff
}

/// A transaction: (zxid, value). The value is the client's `Propose` value (the txn body).
type Txn = (u64, u64);

/// The quorum packets among the servers (Leader.java:382-480), plus `Start` (INTERFACE.md).
/// PING, REVALIDATE, SYNC, INFORM, OBSERVERINFO, COMMITANDACTIVATE, INFORMANDACTIVATE are dropped.
#[derive(Clone, Debug, PartialEq)]
enum Packet {
    /// FOLLOWERINFO: zxid = makeZxid(acceptedEpoch, 0), data = LearnerInfo(sid, version).
    FollowerInfo { zxid: u64, sid: usize, version: u32 },
    /// LEADERINFO: zxid = makeZxid(newEpoch, 0), data = protocol version.
    LeaderInfo { zxid: u64, version: u32 },
    /// ACKEPOCH: zxid = lastLoggedZxid, data = currentEpoch or -1.
    AckEpoch { zxid: u64, epoch: i64 },
    Diff { zxid: u64 },
    Trunc { zxid: u64 },
    /// SNAP followed by the serialized data tree (here carried in the same packet).
    Snap { zxid: u64, tree: Vec<Txn> },
    NewLeader { zxid: u64 },
    UpToDate,
    Proposal { zxid: u64, value: u64 },
    Ack { zxid: u64 },
    Commit { zxid: u64 },
    /// REQUEST: a follower forwards its client's write (sessionId/cxid/type dropped).
    Request { value: u64 },
    /// INTERFACE.md: n0 -> n2 after n0's first delivery when late = 1.
    Start,
}

/// What travels between servers: the packet and the sender's index.
#[derive(Clone, Debug, PartialEq)]
struct Wire {
    from: usize,
    pkt: Packet,
}

// ---------------------------------------------------------------------------------------------
// The quorum sockets: one TCP-like (in-order) channel per ordered pair of servers.
// ---------------------------------------------------------------------------------------------

struct Net {
    nodes: Vec<ThreadId>,
    client: Option<ThreadId>,
    me: usize,
    /// Next sequence number to send on the channel me -> j.
    out_seq: Vec<u32>,
    /// Next sequence number to receive on the channel j -> me.
    in_seq: Vec<u32>,
}

impl Net {
    /// Receives `Setup` (anything else that arrives first stays in the mailbox).
    fn setup(me: usize) -> Net {
        let s: Setup = recv_tagged_msg_block(|_, tag| tag == Some(SETUP_TAG));
        Net {
            client: s.clients.get(me).cloned(),
            nodes: s.nodes,
            me,
            out_seq: vec![0; N],
            in_seq: vec![0; N],
        }
    }

    fn send(&mut self, to: usize, pkt: Packet) {
        let seq = self.out_seq[to];
        self.out_seq[to] += 1;
        send_tagged_msg(self.nodes[to], seq, Wire { from: self.me, pkt });
    }

    /// The next packet of some server channel, in that channel's send order (TCP).
    fn recv(&mut self) -> (usize, Packet) {
        let nodes = self.nodes.clone();
        let next = self.in_seq.clone();
        let w: Wire = recv_tagged_msg_block(move |tid, tag| match tag {
            Some(t) if t != SETUP_TAG => nodes
                .iter()
                .position(|n| *n == tid)
                .map_or(false, |i| next[i] == t),
            _ => false,
        });
        self.in_seq[w.from] += 1;
        (w.from, w.pkt)
    }

    /// The next `Propose` of this server's client.
    fn recv_client(&self) -> u64 {
        let c = self.client.expect("a server without a client takes no request");
        let Req::Propose(v): Req = recv_tagged_msg_block(move |tid, tag| tid == c && tag.is_none());
        v
    }

    /// FinalRequestProcessor's reply: `Committed` to this server's client, if it has one.
    fn reply(&self, v: u64) {
        if let Some(c) = self.client {
            send_msg(c, Rep::Committed(v));
        }
    }
}

// ---------------------------------------------------------------------------------------------
// ZKDatabase in memory: the txn log, the data tree, the committedLog.
// ---------------------------------------------------------------------------------------------

#[derive(Default)]
struct Db {
    /// The transaction log (TxnLog): appended by SyncRequestProcessor / appendRequest.
    log: Vec<Txn>,
    /// The data tree: the applied transactions, in apply order.
    tree: Vec<Txn>,
    /// dataTree.lastProcessedZxid.
    last_processed: u64,
    committed_log: Vec<Txn>,
    min_committed: u64,
    max_committed: u64,
}

impl Db {
    fn append(&mut self, t: Txn) {
        self.log.push(t);
    }

    /// ZooKeeperServer.processTxn -> DataTree.processTxn + addCommittedProposal.
    fn process_txn(&mut self, t: Txn) {
        self.tree.push(t);
        self.last_processed = t.0;
        self.add_committed_proposal(t);
    }

    /// ZKDatabase.addCommittedProposal (322-353); commitLogCount = 500 is never reached.
    fn add_committed_proposal(&mut self, t: Txn) {
        if self.committed_log.is_empty() {
            self.min_committed = t.0;
            self.max_committed = t.0;
        } else if t.0 <= self.max_committed {
            return;
        } else if t.0 != self.max_committed + 1 && epoch_of(t.0) <= epoch_of(self.max_committed) {
            panic!("Committed proposal cached out of order");
        }
        self.committed_log.push(t);
        self.max_committed = t.0;
    }

    /// ZKDatabase.clear (175-192): a new data tree, an empty committedLog.
    fn clear(&mut self) {
        self.tree.clear();
        self.last_processed = 0;
        self.committed_log.clear();
        self.min_committed = 0;
        self.max_committed = 0;
    }

    /// ZKDatabase.deserializeSnapshot (630-634).
    fn deserialize_snapshot(&mut self, tree: Vec<Txn>) {
        self.clear();
        self.last_processed = tree.last().map_or(0, |t| t.0);
        self.tree = tree;
    }

    /// ZKDatabase.truncateLog (611-623): clear, truncate the log, reload (replay the log).
    fn truncate_log(&mut self, zxid: u64) {
        self.clear();
        self.log.retain(|t| t.0 <= zxid);
        for t in self.log.clone() {
            self.process_txn(t);
        }
    }

    /// ZKDatabase.getProposalsFromTxnLog (386-...): empty unless the log holds a txn at or
    /// before `start` (the header check at 399); otherwise the whole log.
    fn proposals_from_txn_log(&self, start: u64) -> Vec<Txn> {
        match self.log.first() {
            Some(t) if t.0 > start => Vec::new(),
            _ => self.log.clone(),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// The leader: Leader.lead, one LearnerHandler per follower, the leader's request pipeline.
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Debug)]
enum HState {
    /// run(): reading the first packet (FOLLOWERINFO).
    AwaitFollowerInfo,
    /// blocked in getEpochToPropose.
    AwaitEpoch,
    /// LEADERINFO sent; reading ACKEPOCH.
    AwaitAckEpoch,
    /// blocked in waitForEpochAck.
    AwaitEpochAckQuorum,
    /// NEWLEADER queued; reading the NEWLEADER ACK.
    AwaitNewLeaderAck,
    /// blocked in waitForNewLeaderAck / waitForStartup.
    AwaitStartup,
    /// UPTODATE queued; the packet loop (LearnerHandler.java:655-718).
    Broadcast,
}

struct Handler {
    state: HState,
    /// StateSummary from ACKEPOCH.
    peer_ss: (i64, u64),
    need_op_packet: bool,
    /// Packets this follower will still send back for what the leader sent it (see NOTES.md,
    /// the liveness guard); REQUESTs are counted apart.
    owed: usize,
    requests_expected: usize,
    requests_got: usize,
}

struct LeaderPeer {
    net: Net,
    db: Db,
    late: bool,
    accepted_epoch: u64,
    current_epoch: u64,
    leader_ss: (i64, u64),
    // getEpochToPropose
    epoch: i64,
    waiting_for_new_epoch: bool,
    connecting: BTreeSet<usize>,
    // waitForEpochAck
    election_finished: bool,
    electing: BTreeSet<usize>,
    // waitForNewLeaderAck
    new_leader_zxid: u64,
    new_leader_acks: BTreeSet<usize>,
    new_leader_needed: usize,
    quorum_formed: bool,
    running: bool,
    /// ZooKeeperServer.hzxid (getNextZxid).
    hzxid: u64,
    last_proposed: u64,
    last_committed: u64,
    /// outstandingProposals: zxid -> (value, ackset).
    outstanding: BTreeMap<u64, (u64, BTreeSet<usize>)>,
    forwarding: Vec<usize>,
    h: Vec<Handler>,
    reqs_left: usize,
    start_sent: bool,
}

impl LeaderPeer {
    fn new(net: Net, e: usize, late: bool) -> Self {
        let reqs_left = if net.client.is_some() { e } else { 0 };
        let h = (0..N)
            .map(|sid| Handler {
                state: HState::AwaitFollowerInfo,
                peer_ss: (-1, 0),
                need_op_packet: true,
                // FOLLOWERINFO is owed by every follower that joins at once; n2 joins on Start
                // when late = 1.
                owed: if sid == LEADER || (late && sid == 2) { 0 } else { 1 },
                requests_expected: if sid != LEADER && sid < CLIENTS { e } else { 0 },
                requests_got: 0,
            })
            .collect();
        LeaderPeer {
            net,
            db: Db::default(),
            late,
            accepted_epoch: 0,
            current_epoch: 0,
            leader_ss: (0, 0),
            epoch: -1,
            waiting_for_new_epoch: true,
            connecting: BTreeSet::new(),
            election_finished: false,
            electing: BTreeSet::new(),
            new_leader_zxid: 0,
            new_leader_acks: BTreeSet::new(),
            // INTERFACE.md: with late = 0 all three servers join before any proposal.
            new_leader_needed: if late { QUORUM } else { N },
            quorum_formed: false,
            running: false,
            hzxid: 0,
            last_proposed: 0,
            last_committed: 0,
            outstanding: BTreeMap::new(),
            forwarding: Vec::new(),
            h,
            reqs_left,
            start_sent: false,
        }
    }

    /// Leader.lead (632-765), then the broadcast loop of the leader's threads.
    fn lead(mut self) {
        self.leader_ss = (self.current_epoch as i64, self.db.last_processed);
        if self.get_epoch_to_propose(LEADER, self.accepted_epoch) {
            self.release_epoch_waiters();
        }
        loop {
            let take_request =
                self.running && self.reqs_left > 0 && (!self.expect_packet() || nondet());
            if take_request {
                // LeaderRequestProcessor -> PrepRequestProcessor -> ProposalRequestProcessor
                let v = self.net.recv_client();
                self.reqs_left -= 1;
                self.propose(v);
            } else {
                let (from, p) = self.net.recv();
                self.on_packet(from, p);
            }
        }
    }

    /// Liveness guard: some follower still owes the leader a packet.
    fn expect_packet(&self) -> bool {
        self.h
            .iter()
            .any(|h| h.owed > 0 || h.requests_got < h.requests_expected)
    }

    fn take_owed(&mut self, f: usize) {
        let h = &mut self.h[f];
        h.owed = h.owed.checked_sub(1).expect("owed-packet accounting");
    }

    fn on_packet(&mut self, f: usize, p: Packet) {
        match (self.h[f].state, p) {
            (HState::AwaitFollowerInfo, Packet::FollowerInfo { zxid, sid, version }) => {
                self.take_owed(f);
                assert_eq!(sid, f);
                assert!(version >= PROTOCOL_VERSION);
                self.h[f].state = HState::AwaitEpoch;
                if self.get_epoch_to_propose(f, epoch_of(zxid)) {
                    self.release_epoch_waiters();
                }
            }
            (HState::AwaitAckEpoch, Packet::AckEpoch { zxid, epoch }) => {
                self.take_owed(f);
                self.h[f].peer_ss = (epoch, zxid);
                self.h[f].state = HState::AwaitEpochAckQuorum;
                if self.wait_for_epoch_ack(f, (epoch, zxid)) {
                    self.release_epoch_ack_waiters();
                }
            }
            (HState::AwaitNewLeaderAck, Packet::Ack { zxid }) => {
                self.take_owed(f);
                self.h[f].state = HState::AwaitStartup;
                self.wait_for_new_leader_ack(f, zxid);
                self.release_startup_waiters();
            }
            (HState::Broadcast, Packet::Ack { zxid }) => {
                self.take_owed(f);
                self.process_ack(f, zxid);
            }
            (HState::Broadcast, Packet::Request { value }) => {
                self.h[f].requests_got += 1;
                self.propose(value);
            }
            (s, p) => panic!("leader: unexpected packet {p:?} from n{f} in {s:?}"),
        }
    }

    /// Leader.getEpochToPropose (1462-1495): true when the epoch is (now) decided.
    fn get_epoch_to_propose(&mut self, sid: usize, last_accepted_epoch: u64) -> bool {
        if !self.waiting_for_new_epoch {
            return true;
        }
        if last_accepted_epoch as i64 >= self.epoch {
            self.epoch = last_accepted_epoch as i64 + 1;
        }
        self.connecting.insert(sid);
        if self.connecting.contains(&LEADER) && self.connecting.len() >= QUORUM {
            self.waiting_for_new_epoch = false;
            self.accepted_epoch = self.epoch as u64;
            // lead() continues (655-710): zk.setZxid, lastProposed, newLeaderProposal,
            // waitForEpochAck(self).
            let e = self.epoch as u64;
            self.hzxid = make_zxid(e, 0);
            self.last_proposed = self.hzxid;
            self.new_leader_zxid = self.hzxid;
            if self.wait_for_epoch_ack(LEADER, self.leader_ss) {
                self.release_epoch_ack_waiters();
            }
            true
        } else {
            false
        }
    }

    /// The handlers blocked in getEpochToPropose return and send LEADERINFO (525-540).
    fn release_epoch_waiters(&mut self) {
        let zxid = make_zxid(self.epoch as u64, 0);
        for f in 0..N {
            if f != LEADER && self.h[f].state == HState::AwaitEpoch {
                self.net.send(f, Packet::LeaderInfo { zxid, version: PROTOCOL_VERSION });
                self.h[f].owed += 1; // ACKEPOCH
                self.h[f].state = HState::AwaitAckEpoch;
            }
        }
    }

    /// Leader.waitForEpochAck (1508-1542): true when the election is (now) finished.
    fn wait_for_epoch_ack(&mut self, sid: usize, ss: (i64, u64)) -> bool {
        if self.election_finished {
            return true;
        }
        if ss.0 != -1 {
            let more_recent = ss.0 > self.leader_ss.0 || (ss.0 == self.leader_ss.0 && ss.1 > self.leader_ss.1);
            if more_recent {
                panic!("Follower is ahead of the leader");
            }
            self.electing.insert(sid);
        }
        if self.electing.contains(&LEADER) && self.electing.len() >= QUORUM {
            self.election_finished = true;
            // lead() continues (711-716): setCurrentEpoch, waitForNewLeaderAck(self).
            self.current_epoch = self.epoch as u64;
            let z = self.hzxid;
            self.wait_for_new_leader_ack(LEADER, z);
            true
        } else {
            false
        }
    }

    /// The handlers blocked in waitForEpochAck return: sync, then NEWLEADER (552-610).
    fn release_epoch_ack_waiters(&mut self) {
        for f in 0..N {
            if f != LEADER && self.h[f].state == HState::AwaitEpochAckQuorum {
                self.sync_and_newleader(f);
            }
        }
        self.release_startup_waiters();
    }

    fn sync_and_newleader(&mut self, f: usize) {
        let peer_last = self.h[f].peer_ss.1;
        let mut q = Vec::new();
        let need_snap = self.sync_follower(f, peer_last, &mut q);
        if need_snap {
            // Written to the socket directly, before the queued packets (568-585).
            let pkt = Packet::Snap { zxid: self.db.last_processed, tree: self.db.tree.clone() };
            self.net.send(f, pkt);
        }
        // A queued proposal followed by its COMMIT before NEWLEADER is applied by the learner
        // without an ACK (Learner.java:683-692, 769-800); every other one is ACKed after UPTODATE.
        for (i, p) in q.iter().enumerate() {
            if let Packet::Proposal { zxid, .. } = p {
                let committed = q[i + 1..]
                    .iter()
                    .any(|c| matches!(c, Packet::Commit { zxid: z } if z == zxid));
                if !committed {
                    self.h[f].owed += 1;
                }
            }
        }
        for p in q {
            self.net.send(f, p);
        }
        let z = make_zxid(self.epoch as u64, 0);
        self.net.send(f, Packet::NewLeader { zxid: z });
        self.h[f].owed += 1; // the NEWLEADER ACK
        self.h[f].state = HState::AwaitNewLeaderAck;
    }

    /// LearnerHandler.syncFollower (780-938), with the in-memory database.
    fn sync_follower(&mut self, f: usize, peer_last: u64, q: &mut Vec<Packet>) -> bool {
        let is_peer_new_epoch_zxid = counter_of(peer_last) == 0;
        let mut current = peer_last;
        let mut need_snap = true;
        // snapshotSizeFactor defaults to 0.33 >= 0 (ZKDatabase.java:92, 355-363).
        let txn_log_sync_enabled = true;
        let mut max = self.db.max_committed;
        let mut min = self.db.min_committed;
        let last_processed = self.db.last_processed;
        if self.db.committed_log.is_empty() {
            min = last_processed;
            max = last_processed;
        }
        if last_processed == peer_last {
            q.push(Packet::Diff { zxid: peer_last });
            self.h[f].need_op_packet = false;
            need_snap = false;
        } else if peer_last > max && !is_peer_new_epoch_zxid {
            q.push(Packet::Trunc { zxid: max });
            current = max;
            self.h[f].need_op_packet = false;
            need_snap = false;
        } else if max >= peer_last && min <= peer_last {
            let itr = self.db.committed_log.clone();
            current = self.queue_committed_proposals(f, &itr, peer_last, None, max, q);
            need_snap = false;
        } else if peer_last < min && txn_log_sync_enabled {
            let txn_itr = self.db.proposals_from_txn_log(peer_last);
            if !txn_itr.is_empty() {
                current = self.queue_committed_proposals(f, &txn_itr, peer_last, Some(min), max, q);
                if current < min {
                    current = peer_last;
                    q.clear();
                    self.h[f].need_op_packet = true;
                } else {
                    let itr = self.db.committed_log.clone();
                    current = self.queue_committed_proposals(f, &itr, current, None, max, q);
                    need_snap = false;
                }
            }
        }
        if need_snap {
            current = self.db.last_processed;
        }
        self.start_forwarding(f, current, q);
        if self.h[f].need_op_packet && !need_snap {
            need_snap = true;
        }
        need_snap
    }

    /// LearnerHandler.queueCommittedProposals (951-1042).
    fn queue_committed_proposals(
        &mut self,
        f: usize,
        itr: &[Txn],
        peer_last: u64,
        max_zxid: Option<u64>,
        last_committed_zxid: u64,
        q: &mut Vec<Packet>,
    ) -> u64 {
        let is_peer_new_epoch_zxid = counter_of(peer_last) == 0;
        let mut queued = peer_last;
        let mut prev: u64 = 0; // -1 in the source; only read for TRUNC, after a proposal
        for &(zxid, value) in itr {
            if max_zxid.is_some_and(|m| zxid > m) {
                break;
            }
            if zxid < peer_last {
                prev = zxid;
                continue;
            }
            if self.h[f].need_op_packet {
                if zxid == peer_last {
                    q.push(Packet::Diff { zxid: last_committed_zxid });
                    self.h[f].need_op_packet = false;
                    continue;
                }
                if is_peer_new_epoch_zxid {
                    q.push(Packet::Diff { zxid: last_committed_zxid });
                    self.h[f].need_op_packet = false;
                } else if zxid > peer_last {
                    if epoch_of(zxid) != epoch_of(peer_last) {
                        return queued;
                    }
                    q.push(Packet::Trunc { zxid: prev });
                    self.h[f].need_op_packet = false;
                }
            }
            if zxid <= queued {
                continue;
            }
            q.push(Packet::Proposal { zxid, value });
            q.push(Packet::Commit { zxid });
            queued = zxid;
        }
        if self.h[f].need_op_packet && is_peer_new_epoch_zxid {
            q.push(Packet::Diff { zxid: last_committed_zxid });
            self.h[f].need_op_packet = false;
        }
        queued
    }

    /// Leader.startForwarding (1360-1393). toBeApplied is always empty here (see NOTES.md).
    fn start_forwarding(&mut self, f: usize, last_seen: u64, q: &mut Vec<Packet>) -> u64 {
        if self.last_proposed > last_seen {
            for (&zxid, &(value, _)) in self.outstanding.iter() {
                if zxid <= last_seen {
                    continue;
                }
                q.push(Packet::Proposal { zxid, value });
            }
        }
        self.forwarding.push(f);
        self.last_proposed
    }

    /// Leader.waitForNewLeaderAck (1613-1653), without the blocking part.
    fn wait_for_new_leader_ack(&mut self, sid: usize, zxid: u64) {
        if self.quorum_formed {
            return;
        }
        if zxid != self.new_leader_zxid {
            return; // "NEWLEADER ACK from a different epoch"
        }
        self.new_leader_acks.insert(sid);
        if self.new_leader_acks.len() >= self.new_leader_needed {
            self.quorum_formed = true;
        }
    }

    /// Once the quorum is formed: lead() runs startZkServer (741); the handlers blocked in
    /// waitForNewLeaderAck / waitForStartup queue UPTODATE (646-653).
    fn release_startup_waiters(&mut self) {
        if !self.quorum_formed {
            return;
        }
        if !self.running {
            self.start_zk_server();
        }
        for f in 0..N {
            if f != LEADER && self.h[f].state == HState::AwaitStartup {
                self.net.send(f, Packet::UpToDate);
                self.h[f].owed += 1; // the ACK after UPTODATE (Learner.java:825-826)
                self.h[f].state = HState::Broadcast;
            }
        }
    }

    /// Leader.startZkServer (1563-1603).
    fn start_zk_server(&mut self) {
        self.last_committed = self.hzxid;
        self.running = true;
        self.db.last_processed = self.hzxid;
    }

    /// PrepRequestProcessor (assign zxid) + ProposalRequestProcessor: Leader.propose
    /// (1288-1328), then SyncRequestProcessor (append) and AckRequestProcessor (self ACK).
    fn propose(&mut self, value: u64) {
        self.hzxid += 1;
        let zxid = self.hzxid;
        self.outstanding.insert(zxid, (value, BTreeSet::new()));
        self.last_proposed = zxid;
        for f in self.forwarding.clone() {
            self.net.send(f, Packet::Proposal { zxid, value });
            self.h[f].owed += 1;
        }
        self.db.append((zxid, value));
        self.process_ack(LEADER, zxid);
    }

    /// Leader.processAck (1047-1115).
    fn process_ack(&mut self, sid: usize, zxid: u64) {
        if counter_of(zxid) == 0 {
            return; // the ACK of NEWLEADER / after UPTODATE
        }
        if self.outstanding.is_empty() {
            return;
        }
        if self.last_committed >= zxid {
            return;
        }
        let Some(p) = self.outstanding.get_mut(&zxid) else {
            return; // "Trying to commit future proposal"
        };
        p.1.insert(sid);
        self.try_to_commit(zxid);
    }

    /// Leader.tryToCommit (963-1036), commit (1212-1219), then the CommitProcessor and the
    /// FinalRequestProcessor: apply, reply to the client.
    fn try_to_commit(&mut self, zxid: u64) -> bool {
        if self.outstanding.contains_key(&(zxid - 1)) {
            return false;
        }
        if self.outstanding[&zxid].1.len() < QUORUM {
            return false;
        }
        let (value, _) = self.outstanding.remove(&zxid).unwrap();
        self.last_committed = zxid;
        for f in self.forwarding.clone() {
            self.net.send(f, Packet::Commit { zxid });
        }
        self.db.process_txn((zxid, value));
        self.net.reply(value);
        if self.late && !self.start_sent {
            self.start_sent = true;
            self.net.send(2, Packet::Start);
            self.h[2].owed += 1; // its FOLLOWERINFO
        }
        true
    }
}

// ---------------------------------------------------------------------------------------------
// A follower: Learner.registerWithLeader / syncWithLeader, Follower.followLeader /
// processPacket, FollowerZooKeeperServer, FollowerRequestProcessor.
// ---------------------------------------------------------------------------------------------

struct FollowerPeer {
    net: Net,
    db: Db,
    me: usize,
    late: bool,
    accepted_epoch: u64,
    current_epoch: u64,
    /// Follower.lastQueued.
    last_queued: u64,
    /// FollowerZooKeeperServer.pendingTxns.
    pending_txns: VecDeque<Txn>,
    running: bool,
    reqs_left: usize,
    forwarded: usize,
    /// Liveness guard: COMMITs received (and txns received in a snapshot), and the number of
    /// transactions the leader's client contributes.
    commits_seen: usize,
    leader_client_txns: usize,
}

impl FollowerPeer {
    fn new(net: Net, me: usize, e: usize, late: bool, n_clients: usize) -> Self {
        let reqs_left = if net.client.is_some() { e } else { 0 };
        FollowerPeer {
            net,
            db: Db::default(),
            me,
            late,
            accepted_epoch: 0,
            current_epoch: 0,
            last_queued: 0,
            pending_txns: VecDeque::new(),
            running: false,
            reqs_left,
            forwarded: 0,
            commits_seen: 0,
            leader_client_txns: if n_clients > LEADER { e } else { 0 },
        }
    }

    /// Follower.followLeader (71-149).
    fn follow_leader(mut self) {
        if self.late && self.me == 2 {
            let (_, p) = self.net.recv();
            assert_eq!(p, Packet::Start);
        }
        let new_epoch_zxid = self.register_with_leader();
        if epoch_of(new_epoch_zxid) < self.accepted_epoch {
            panic!("Error: Epoch of leader is lower");
        }
        self.sync_with_leader(new_epoch_zxid);
        loop {
            let take_request =
                self.running && self.reqs_left > 0 && (!self.expect_packet() || nondet());
            if take_request {
                // FollowerRequestProcessor -> Learner.request (251-268): REQUEST to the leader.
                let v = self.net.recv_client();
                self.reqs_left -= 1;
                self.forwarded += 1;
                self.net.send(LEADER, Packet::Request { value: v });
            } else {
                let (_, p) = self.net.recv();
                self.process_packet(p);
            }
        }
    }

    /// Liveness guard: a COMMIT is still on its way for a transaction already submitted.
    fn expect_packet(&self) -> bool {
        self.commits_seen < self.leader_client_txns + self.forwarded
    }

    /// Learner.registerWithLeader (486-540).
    fn register_with_leader(&mut self) -> u64 {
        let last_logged_zxid = self.db.last_processed; // QuorumPeer.getLastLoggedZxid
        let fi = Packet::FollowerInfo {
            zxid: make_zxid(self.accepted_epoch, 0),
            sid: self.me,
            version: PROTOCOL_VERSION,
        };
        self.net.send(LEADER, fi);
        let (_, p) = self.net.recv();
        match p {
            Packet::LeaderInfo { zxid, version: _ } => {
                let new_epoch = epoch_of(zxid);
                let epoch_bytes = if new_epoch > self.accepted_epoch {
                    let c = self.current_epoch as i64;
                    self.accepted_epoch = new_epoch;
                    c
                } else if new_epoch == self.accepted_epoch {
                    -1
                } else {
                    panic!("Leaders epoch is less than accepted epoch");
                };
                self.net.send(LEADER, Packet::AckEpoch { zxid: last_logged_zxid, epoch: epoch_bytes });
                make_zxid(new_epoch, 0)
            }
            Packet::NewLeader { zxid } => {
                if epoch_of(zxid) > self.accepted_epoch {
                    self.accepted_epoch = epoch_of(zxid);
                }
                zxid
            }
            p => panic!("First packet should have been NEWLEADER, got {p:?}"),
        }
    }

    /// Learner.enforceContinuousProposal (542-561).
    fn enforce_continuous_proposal(last_queued: u64, zxid: u64) -> u64 {
        if last_queued != 0 && zxid != last_queued + 1 && epoch_of(zxid) <= epoch_of(last_queued) {
            panic!("DIFF sync got a non-continuous proposal");
        }
        zxid
    }

    /// Applying a transaction (processTxn / CommitProcessor -> FinalRequestProcessor).
    fn apply(&mut self, t: Txn) {
        self.db.process_txn(t);
        self.net.reply(t.1);
    }

    /// Learner.syncWithLeader (570-848).
    fn sync_with_leader(&mut self, new_leader_zxid: u64) {
        let new_epoch = epoch_of(new_leader_zxid);
        let mut snapshot_needed = true;
        let (_, p) = self.net.recv();
        match p {
            Packet::Diff { .. } => snapshot_needed = false,
            Packet::Snap { zxid, tree } => {
                self.commits_seen += tree.len();
                self.db.deserialize_snapshot(tree);
                self.db.last_processed = zxid;
            }
            Packet::Trunc { zxid } => {
                self.db.truncate_log(zxid);
                self.db.last_processed = zxid;
            }
            p => panic!("Got unexpected packet from leader: {p:?}"),
        }
        let mut last_queued = 0u64;
        let mut is_pre_zab1_0 = true;
        let mut write_to_txn_log = !snapshot_needed;
        let mut packets_not_logged: VecDeque<Txn> = VecDeque::new();
        let mut packets_committed: VecDeque<u64> = VecDeque::new();
        loop {
            let (_, p) = self.net.recv();
            match p {
                Packet::Proposal { zxid, value } => {
                    last_queued = Self::enforce_continuous_proposal(last_queued, zxid);
                    packets_not_logged.push_back((zxid, value));
                }
                Packet::Commit { zxid } => {
                    self.commits_seen += 1;
                    let pif = *packets_not_logged.front().expect("COMMIT without a proposal");
                    if !write_to_txn_log {
                        if pif.0 == zxid {
                            self.apply(pif);
                            packets_not_logged.pop_front();
                        }
                    } else {
                        packets_committed.push_back(zxid);
                    }
                }
                Packet::UpToDate => {
                    if is_pre_zab1_0 {
                        self.current_epoch = new_epoch;
                    }
                    break;
                }
                Packet::NewLeader { .. } => {
                    write_to_txn_log = true;
                    is_pre_zab1_0 = false;
                    while let Some(zxid) = packets_committed.pop_front() {
                        match packets_not_logged.front() {
                            Some(pif) if pif.0 == zxid => {
                                let pif = packets_not_logged.pop_front().unwrap();
                                self.db.append(pif);
                                self.apply(pif);
                            }
                            _ => continue,
                        }
                    }
                    self.current_epoch = new_epoch;
                    self.net.send(LEADER, Packet::Ack { zxid: new_leader_zxid });
                }
                _ => {}
            }
        }
        self.net.send(LEADER, Packet::Ack { zxid: make_zxid(new_epoch, 0) });
        self.running = true; // zk.startup()
        for p in packets_not_logged {
            self.log_request(p);
        }
        for zxid in packets_committed {
            self.commit(zxid);
        }
    }

    /// FollowerZooKeeperServer.logRequest (79-84) -> SyncRequestProcessor (append) ->
    /// SendAckRequestProcessor (ACK).
    fn log_request(&mut self, t: Txn) {
        if counter_of(t.0) != 0 {
            self.pending_txns.push_back(t);
        }
        self.db.append(t);
        self.net.send(LEADER, Packet::Ack { zxid: t.0 });
    }

    /// FollowerZooKeeperServer.commit (99-113), then CommitProcessor -> FinalRequestProcessor.
    fn commit(&mut self, zxid: u64) {
        let Some(&first) = self.pending_txns.front() else {
            return; // "Committing without seeing txn"
        };
        if first.0 != zxid {
            panic!("UNMATCHED_TXN_COMMIT: committing {zxid:#x} but next pending txn {:#x}", first.0);
        }
        self.pending_txns.pop_front();
        self.apply(first);
    }

    /// Follower.processPacket (156-245).
    fn process_packet(&mut self, p: Packet) {
        match p {
            Packet::Proposal { zxid, value } => {
                // "Got zxid expected lastQueued + 1" is only a warning.
                self.last_queued = zxid;
                self.log_request((zxid, value));
            }
            Packet::Commit { zxid } => {
                self.commits_seen += 1;
                self.commit(zxid);
            }
            _ => {} // UPTODATE after start: an error log; anything else: "Unknown packet type"
        }
    }
}

// ---------------------------------------------------------------------------------------------
// QuorumPeer.run: the fixed roles; the program.
// ---------------------------------------------------------------------------------------------

fn server(i: usize, e: usize, late: bool) {
    let net = Net::setup(i);
    if i == LEADER {
        LeaderPeer::new(net, e, late).lead();
    } else {
        FollowerPeer::new(net, i, e, late, CLIENTS).follow_leader();
    }
}

fn run(e: usize, late: bool) {
    let servers: Vec<_> = (0..N)
        .map(|i| {
            thread::Builder::new()
                .name(format!("n{i}"))
                .spawn(move || server(i, e, late))
                .unwrap()
        })
        .collect();
    let nodes: Vec<ThreadId> = servers.iter().map(|h| h.thread().id()).collect();
    let clients: Vec<_> = (0..CLIENTS)
        .map(|i| {
            let s = nodes[i];
            thread::Builder::new()
                .name(format!("c{i}"))
                .spawn(move || client(i, e, s))
                .unwrap()
        })
        .collect();
    let client_ids: Vec<ThreadId> = clients.iter().map(|h| h.thread().id()).collect();
    let init = thread::Builder::new()
        .name("init".to_string())
        .spawn(move || {
            for s in &nodes {
                let setup = Setup { nodes: nodes.clone(), clients: client_ids.clone() };
                send_tagged_msg(*s, SETUP_TAG, setup);
            }
        })
        .unwrap();
    for c in clients {
        c.join().unwrap();
    }
    init.join().unwrap();
    for s in servers {
        s.join().unwrap();
    }
}

fn args_of(a: &[String]) -> (usize, bool) {
    let e: usize = a
        .first()
        .and_then(|s| s.parse().ok())
        .filter(|e| *e >= 1)
        .expect("usage: <e> <late> [<fifo|bag>], e >= 1");
    let late = match a.get(1).map(String::as_str) {
        Some("0") => false,
        Some("1") => true,
        _ => panic!("usage: <e> <late> [<fifo|bag>], late in {{0, 1}}"),
    };
    (e, late)
}

fn entry(a: &[String]) -> Entry {
    let (e, late) = args_of(a);
    Arc::new(move || run(e, late))
}

fn main() -> ExitCode {
    program::main(Program {
        visible: &|_| vec!["c0".to_string(), "c1".to_string()],
        spec: &entry,
        imp: &entry,
    })
}
