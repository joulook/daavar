//! Port of Apache Cassandra 4.1's quorum write path and quorum read path with read repair
//! (`ReadRepairStrategy.BLOCKING` and `ReadRepairStrategy.NONE`) to TraceForge's Must API.
//! One partition with two cells `a` and `b`, replication factor 3, every operation at QUORUM.
//! Arguments (after `--`): `<q> <blocking|none> [<fifo|bag>]`. See NOTES.md for the mapping
//! to the Cassandra sources and every decision.
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ---------------------------------------------------------------------------
// The interface (verbatim): visible messages and the client.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum Req {
    Write(usize, u64),
    ReadA(usize),
    ReadAB(usize),
} // client -> replica: (client, value) / (client) / (client)
#[derive(Clone, Debug, PartialEq)]
enum Rep {
    Ack(usize),
    ValueA(usize, u64),
    ValueAB(usize, u64, u64),
} // replica -> client: (client) / (client, a) / (client, a, b)

const CLIENTS: usize = 2;
fn value(i: usize, j: usize) -> u64 {
    (1 + i + CLIENTS * j) as u64
}
fn client(i: usize, q: usize, node: ThreadId) {
    for j in 0..q {
        match (i + j) % 3 {
            0 => { send_msg(node, Req::Write(i, value(i, j))); let Rep::Ack(_) = recv_msg_block::<Rep>() else { panic!("Ack expected") }; }
            1 => { send_msg(node, Req::ReadA(i)); let Rep::ValueA(..) = recv_msg_block::<Rep>() else { panic!("ValueA expected") }; }
            _ => { send_msg(node, Req::ReadAB(i)); let Rep::ValueAB(..) = recv_msg_block::<Rep>() else { panic!("ValueAB expected") }; }
        }
    }
}

// ---------------------------------------------------------------------------
// Data: two cells, each a value with its timestamp (timestamp 0 = absent cell).
// ---------------------------------------------------------------------------

const REPLICAS: usize = 3;
/// QUORUM of RF = 3 (ConsistencyLevel.quorumFor: 3 / 2 + 1).
const QUORUM: usize = 2;
/// Column indices: `a` = 0, `b` = 1.
const COL_A: usize = 0;
const COL_B: usize = 1;

#[derive(Clone, Copy, Debug, PartialEq)]
struct Cell {
    v: u64,
    ts: u64,
}
const ABSENT: Cell = Cell { v: 0, ts: 0 };

/// `Cells.reconcile` for regular live cells: the newer timestamp wins; equal timestamps carry
/// equal values here (the timestamp is the value), so either is returned.
fn reconcile(c1: Cell, c2: Cell) -> Cell {
    if c1.ts >= c2.ts {
        c1
    } else {
        c2
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Strategy {
    Blocking,
    None,
}

// ---------------------------------------------------------------------------
// Invisible messages among the replicas (MUTATION_REQ/RSP, READ_REQ/RSP, READ_REPAIR_REQ/RSP).
// `rid` identifies the callback (the source's message id with its RequestCallbacks entry).
// ---------------------------------------------------------------------------

/// Tags of the messages a coordinator thread receives; requests from clients are untagged.
const TAG_SETUP: u32 = 1;
const TAG_PEER: u32 = 2;
const TAG_RSP: u32 = 3;

/// The one setup message from the entry closure: the addresses of all processes.
#[derive(Clone, Debug, PartialEq)]
struct Setup {
    replicas: Vec<ThreadId>,
    clients: Vec<ThreadId>,
}

#[derive(Clone, Debug, PartialEq)]
enum Net {
    /// Coordinator -> coordinator at startup: the address of the sender's replica-side thread.
    Peer { replica: usize, store: ThreadId },
    /// MUTATION_REQ: both cells set to `ts` with timestamp `ts`.
    Mutation { rid: u32, from: ThreadId, ts: u64 },
    /// MUTATION_RSP.
    MutationAck { rid: u32, replica: usize },
    /// READ_REQ, a data read (`digest == false`) or a digest read of the columns `cols`.
    Read { rid: u32, from: ThreadId, cols: Vec<usize>, digest: bool },
    /// READ_RSP with data: the replica's cells of the columns read, in `cols` order.
    DataRsp { rid: u32, replica: usize, cells: Vec<Cell> },
    /// READ_RSP with a digest of the cells read.
    DigestRsp { rid: u32, replica: usize, digest: Vec<u64> },
    /// READ_REPAIR_REQ: a mutation of the listed (column, cell) pairs.
    Repair { rid: u32, from: ThreadId, cells: Vec<(usize, Cell)> },
    /// READ_REPAIR_RSP.
    RepairAck { rid: u32, replica: usize },
}

/// The digest of the cells read (ReadResponse.digest): a deterministic, injective function of
/// the values and timestamps.
fn digest_of(cells: &[Cell]) -> Vec<u64> {
    cells.iter().flat_map(|c| [c.v, c.ts]).collect()
}

// ---------------------------------------------------------------------------
// Replica side: MutationVerbHandler, ReadCommandVerbHandler, ReadRepairVerbHandler.
// ---------------------------------------------------------------------------

fn replica_store(me: usize) {
    let mut cells = [ABSENT; 2];
    loop {
        match recv_msg_block::<Net>() {
            Net::Mutation { rid, from, ts } => {
                // Mutation.apply: the memtable merges the update cell by cell (Cells.reconcile).
                let upd = Cell { v: ts, ts };
                cells[COL_A] = reconcile(cells[COL_A], upd);
                cells[COL_B] = reconcile(cells[COL_B], upd);
                send_tagged_msg(from, TAG_RSP, Net::MutationAck { rid, replica: me });
            }
            Net::Read { rid, from, cols, digest } => {
                let read: Vec<Cell> = cols.iter().map(|&c| cells[c]).collect();
                if digest {
                    send_tagged_msg(from, TAG_RSP, Net::DigestRsp { rid, replica: me, digest: digest_of(&read) });
                } else {
                    send_tagged_msg(from, TAG_RSP, Net::DataRsp { rid, replica: me, cells: read });
                }
            }
            Net::Repair { rid, from, cells: upd } => {
                for (c, cell) in upd {
                    cells[c] = reconcile(cells[c], cell);
                }
                send_tagged_msg(from, TAG_RSP, Net::RepairAck { rid, replica: me });
            }
            m => panic!("replica side: unexpected message {m:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Coordinator side.
// ---------------------------------------------------------------------------

struct Coordinator {
    me: usize,
    my_tid: ThreadId,
    stores: Vec<ThreadId>,
    strategy: Strategy,
    next_rid: u32,
}

impl Coordinator {
    fn new_rid(&mut self) -> u32 {
        self.next_rid += 1;
        self.next_rid
    }

    /// The next response of callback `rid`; responses of finished callbacks (the late third
    /// write acknowledgement) are dropped, as RequestCallbacks does for a completed handler.
    fn await_rsp(&self, rid: u32) -> Net {
        loop {
            let m = recv_tagged_msg_block::<_, Net>(|_, t| t == Some(TAG_RSP));
            let r = match &m {
                Net::MutationAck { rid, .. }
                | Net::DataRsp { rid, .. }
                | Net::DigestRsp { rid, .. }
                | Net::RepairAck { rid, .. } => *rid,
                other => panic!("coordinator: unexpected message {other:?}"),
            };
            if r == rid {
                return m;
            }
        }
    }

    /// StorageProxy.mutate / performWrite / sendToHintedReplicas, WriteResponseHandler.
    fn write(&mut self, ts: u64) {
        let rid = self.new_rid();
        // the local replica first (performLocally), then the others
        let mut order = vec![self.me];
        order.extend((0..REPLICAS).filter(|&r| r != self.me));
        for r in order {
            send_msg(self.stores[r], Net::Mutation { rid, from: self.my_tid, ts });
        }
        // responseHandler.get(): block for QUORUM acknowledgements
        let mut acks = 0;
        while acks < QUORUM {
            if let Net::MutationAck { .. } = self.await_rsp(rid) {
                acks += 1;
            }
        }
    }

    /// A full data read of `cols` to every replica of `contacts`, awaiting all of them
    /// (ReadCallback with blockFor = contacts.len() = QUORUM). Returns the cells per contact.
    fn data_reads(&mut self, contacts: &[usize], cols: &[usize]) -> Vec<Vec<Cell>> {
        let rid = self.new_rid();
        // AbstractReadRepair.sendReadCommand: the remote ones are sent, the local one runs on
        // Stage.READ; every contact gets a data read
        for &r in contacts {
            send_msg(self.stores[r], Net::Read { rid, from: self.my_tid, cols: cols.to_vec(), digest: false });
        }
        let mut got: Vec<Option<Vec<Cell>>> = vec![None; contacts.len()];
        while got.iter().any(Option::is_none) {
            if let Net::DataRsp { replica, cells, .. } = self.await_rsp(rid) {
                let k = contacts.iter().position(|&c| c == replica).unwrap();
                got[k] = Some(cells);
            }
        }
        got.into_iter().map(Option::unwrap).collect()
    }

    /// StorageProxy.fetchRows with a NeverSpeculatingReadExecutor, DigestResolver,
    /// the read repair of the strategy, DataResolver with the merge listeners.
    fn read(&mut self, cols: &[usize]) -> Vec<u64> {
        // ReplicaPlans.forRead: the contacts are the first QUORUM of the candidates sorted by
        // proximity: the local replica, then the "fastest" other one (a nondeterministic choice)
        let others: Vec<usize> = (0..REPLICAS).filter(|&r| r != self.me).collect();
        let other = if nondet() { others[0] } else { others[1] };
        let contacts = [self.me, other];

        // executeAsync: one data request to the first contact, a digest request to the other
        let rid = self.new_rid();
        send_msg(self.stores[other], Net::Read { rid, from: self.my_tid, cols: cols.to_vec(), digest: true });
        send_msg(self.stores[self.me], Net::Read { rid, from: self.my_tid, cols: cols.to_vec(), digest: false });

        // awaitResponses: ReadCallback signals at blockFor responses with the data present
        let mut data: Option<Vec<Cell>> = None;
        let mut digest: Option<Vec<u64>> = None;
        while data.is_none() || digest.is_none() {
            match self.await_rsp(rid) {
                Net::DataRsp { cells, .. } => data = Some(cells),
                Net::DigestRsp { digest: d, .. } => digest = Some(d),
                _ => {}
            }
        }
        let data = data.unwrap();
        // DigestResolver.responsesMatch
        if digest_of(&data) == digest.unwrap() {
            // DigestResolver.getData
            return data.iter().map(|c| c.v).collect();
        }

        // digest mismatch: readRepair.startRepair (AbstractReadRepair, both strategies): full
        // data reads to all contacts; awaitReads
        let versions = self.data_reads(&contacts, cols);

        // DataResolver.resolve: merge per cell by Cells.reconcile
        let merged: Vec<Cell> = (0..cols.len())
            .map(|k| versions.iter().map(|v| v[k]).fold(ABSENT, reconcile))
            .collect();

        if self.strategy == Strategy::Blocking {
            // RowIteratorMergeListener.onCell/close: per source, the queried cells whose
            // merged version differs from the source's; one mutation per such source
            let mut repairs: Vec<(usize, Vec<(usize, Cell)>)> = Vec::new();
            for (i, v) in versions.iter().enumerate() {
                let upd: Vec<(usize, Cell)> = (0..cols.len())
                    .filter(|&k| merged[k] != v[k])
                    .map(|k| (cols[k], merged[k]))
                    .collect();
                if !upd.is_empty() {
                    repairs.push((contacts[i], upd));
                }
            }
            // BlockingReadRepair.repairPartition -> BlockingPartitionRepair.sendInitialRepairs;
            // the latch: writeQuorum minus the contacts without a repair mutation
            let rrid = self.new_rid();
            let block_for = QUORUM.saturating_sub(contacts.len() - repairs.len());
            for (r, upd) in repairs {
                send_msg(self.stores[r], Net::Repair { rid: rrid, from: self.my_tid, cells: upd });
            }
            // concatAndBlockOnRepair -> awaitWrites -> awaitRepairsUntil
            let mut acks = 0;
            while acks < block_for {
                if let Net::RepairAck { .. } = self.await_rsp(rrid) {
                    acks += 1;
                }
            }
        }
        // ReadOnlyReadRepair (NONE): reconciled result, no repair
        merged.iter().map(|c| c.v).collect()
    }
}

fn replica(me: usize, strategy: Strategy) {
    // the setup message before anything else
    let setup = recv_tagged_msg_block::<_, Setup>(|_, t| t == Some(TAG_SETUP));
    let my_tid = thread::current().id();
    // the replica side of this process
    let store = thread::Builder::new()
        .name(format!("n{me}.replica"))
        .spawn(move || replica_store(me))
        .unwrap();
    let store_tid = store.thread().id();
    for (j, &n) in setup.replicas.iter().enumerate() {
        if j != me {
            send_tagged_msg(n, TAG_PEER, Net::Peer { replica: me, store: store_tid });
        }
    }
    let mut stores = vec![store_tid; REPLICAS];
    for j in 0..REPLICAS {
        if j != me {
            let from = setup.replicas[j];
            match recv_tagged_msg_block::<_, Net>(move |tid, t| t == Some(TAG_PEER) && tid == from) {
                Net::Peer { replica, store } => stores[replica] = store,
                m => panic!("peer expected, got {m:?}"),
            }
        }
    }
    let mut co = Coordinator { me, my_tid, stores, strategy, next_rid: 0 };
    loop {
        let req = recv_tagged_msg_block::<_, Req>(|_, t| t.is_none());
        match req {
            Req::Write(c, v) => {
                co.write(v);
                send_msg(setup.clients[c], Rep::Ack(c));
            }
            Req::ReadA(c) => {
                let r = co.read(&[COL_A]);
                send_msg(setup.clients[c], Rep::ValueA(c, r[0]));
            }
            Req::ReadAB(c) => {
                let r = co.read(&[COL_A, COL_B]);
                send_msg(setup.clients[c], Rep::ValueAB(c, r[0], r[1]));
            }
        }
    }
}

fn imp_program(q: usize, strategy: Strategy) {
    let mut replicas = Vec::new();
    for i in 0..REPLICAS {
        replicas.push(
            thread::Builder::new()
                .name(format!("n{i}"))
                .spawn(move || replica(i, strategy))
                .unwrap(),
        );
    }
    let rtids: Vec<ThreadId> = replicas.iter().map(|h| h.thread().id()).collect();
    let mut clients = Vec::new();
    for i in 0..CLIENTS {
        let node = rtids[i];
        clients.push(
            thread::Builder::new()
                .name(format!("c{i}"))
                .spawn(move || client(i, q, node))
                .unwrap(),
        );
    }
    let ctids: Vec<ThreadId> = clients.iter().map(|h| h.thread().id()).collect();
    for &r in &rtids {
        send_tagged_msg(r, TAG_SETUP, Setup { replicas: rtids.clone(), clients: ctids.clone() });
    }
    for c in clients {
        c.join().unwrap();
    }
    for r in replicas {
        r.join().unwrap();
    }
}

fn parse(args: &[String]) -> (usize, Strategy) {
    let q: usize = args.first().and_then(|s| s.parse().ok()).expect("q >= 1 expected");
    assert!(q >= 1, "q >= 1 expected");
    let strategy = match args.get(1).map(String::as_str) {
        Some("blocking") | None => Strategy::Blocking,
        Some("none") => Strategy::None,
        Some(s) => panic!("unknown read-repair strategy `{s}`: blocking or none"),
    };
    (q, strategy)
}

fn entry(args: &[String]) -> Entry {
    let (q, strategy) = parse(args);
    Arc::new(move || imp_program(q, strategy))
}

fn main() -> ExitCode {
    traceforge::program::main(Program {
        visible: &|_| (0..CLIENTS).map(|i| format!("c{i}")).collect(),
        spec: &entry,
        imp: &entry,
    })
}
