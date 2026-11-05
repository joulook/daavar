//! Port of Apache BookKeeper's ledger replication with fencing (main at 7b66b5d) to TraceForge's
//! Must API: one ledger, writer `w1` with handle `w1h`, recovery client `w2` with handle `w2h`,
//! bookies `b1`..`b3` (E = Qw = 3, Qa = 2) and the metadata store `zk`.
//! See NOTES.md for the role-by-role description, the method map and every decision.
//!
//! Usage: `bench_bk_port count impl [--model fifo|bag] -- <e>`.
use std::collections::BTreeMap;
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{self, Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ---------------------------------------------------------------------------------------------
// The visible messages (INTERFACE.md, verbatim).
// ---------------------------------------------------------------------------------------------
#[derive(Clone, Debug, PartialEq)]
enum Req {
    Add(u64),
    Recover,
}
#[derive(Clone, Debug, PartialEq)]
enum Rep {
    Acked(u64),
    Rejected(u64),
    Closed(i64),
}

// ---------------------------------------------------------------------------------------------
// The internal messages (handles, bookies, zk). Entry ids are the source's: 0-based,
// INVALID_ENTRY_ID = -1; the entry number i of `Add(i)` has entry id i - 1.
// ---------------------------------------------------------------------------------------------
const INVALID_ENTRY_ID: i64 = -1; // LedgerHandle.INVALID_ENTRY_ID
const LAST_ADD_CONFIRMED: i64 = -1; // BookieProtocol.LAST_ADD_CONFIRMED (read of the last entry)
const QW: usize = 3; // write quorum = ensemble size
const QA: usize = 2; // ack quorum
const COVER: usize = QW - QA + 1; // ReadLastConfirmedOp coverage / NoSuchEntry threshold

// Tags: the setup message, zk replies, the fencing LAC read, add and read responses by entry id.
const TAG_SETUP: u32 = 1;
const TAG_ZK: u32 = 2;
const TAG_LAC: u32 = 3;
const TAG_ADD: u32 = 1000;
const TAG_READ: u32 = 100_000;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Role {
    W1h,
    W2h,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum State {
    Open,
    InRecovery,
    Closed,
}

/// The ledger's metadata as stored at zk (the ensemble is fixed: b1, b2, b3).
#[derive(Clone, Copy, Debug, PartialEq)]
struct Meta {
    state: State,
    last: i64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum ReadRc {
    /// EOK: the entry's body: its payload and the LAC the writer piggybacked on it.
    Ok { payload: u64, lac: i64 },
    /// ENOENTRY (or ENOLEDGER: the bookie has no entry of the ledger).
    NoEntry,
}

#[derive(Clone, Debug, PartialEq)]
enum Net {
    Setup { b: [ThreadId; 3], zk: ThreadId, w1h: ThreadId, w2h: ThreadId, w1: ThreadId, w2: ThreadId },
    // handle -> bookie (AddRequest, flag RECOVERY_ADD) / bookie -> handle (AddResponse)
    AddReq { from: Role, id: i64, lac: i64, payload: u64, recovery: bool },
    AddResp { bookie: usize, id: i64, fenced: bool },
    // handle -> bookie (ReadRequest, flag FENCE_LEDGER) / bookie -> handle (ReadResponse)
    ReadReq { from: Role, id: i64, fence: bool },
    ReadResp { bookie: usize, id: i64, rc: ReadRc },
    // handle -> zk / zk -> handle (LedgerManager read / versioned write)
    ZkRead { from: Role },
    ZkWrite { from: Role, meta: Meta, expected: u64 },
    ZkMeta { meta: Meta, version: u64 },
    ZkWriteOk { version: u64 },
    ZkBadVersion,
}

#[derive(Clone, Copy)]
struct Addrs {
    b: [ThreadId; 3],
    zk: ThreadId,
    w1h: ThreadId,
    w2h: ThreadId,
    w1: ThreadId,
    w2: ThreadId,
}

impl Addrs {
    fn of(&self, r: Role) -> ThreadId {
        match r {
            Role::W1h => self.w1h,
            Role::W2h => self.w2h,
        }
    }
}

fn recv_setup() -> Addrs {
    match recv_tagged_msg_block::<_, Net>(|_, t| t == Some(TAG_SETUP)) {
        Net::Setup { b, zk, w1h, w2h, w1, w2 } => Addrs { b, zk, w1h, w2h, w1, w2 },
        m => panic!("setup expected, got {m:?}"),
    }
}

fn recv_tag(tag: u32) -> Net {
    recv_tagged_msg_block::<_, Net>(move |_, t| t == Some(tag))
}

/// Entry id -> the entry number of the interface (`Closed { last }`, -1 when empty).
fn entry_number(id: i64) -> i64 {
    if id == INVALID_ENTRY_ID {
        -1
    } else {
        id + 1
    }
}

// ---------------------------------------------------------------------------------------------
// The clients (INTERFACE.md, verbatim).
// ---------------------------------------------------------------------------------------------
fn writer(e: u64, h: ThreadId) {
    for i in 1..=e {
        send_msg(h, Req::Add(i));
        match recv_msg_block::<Rep>() {
            Rep::Acked(_) | Rep::Rejected(_) => {}
            _ => panic!("Acked or Rejected expected"),
        }
    }
}
fn recoverer(h: ThreadId) {
    send_msg(h, Req::Recover);
    let Rep::Closed(_) = recv_msg_block::<Rep>() else { panic!("Closed expected") };
}

// ---------------------------------------------------------------------------------------------
// Bookie: BookieRequestProcessor -> WriteEntryProcessorV3 / ReadEntryProcessorV3 over BookieImpl
// (addEntry, recoveryAddEntry, fenceLedger, readEntry) with in-memory ledger storage.
// ---------------------------------------------------------------------------------------------
fn bookie(idx: usize) {
    let a = recv_setup();
    // LedgerStorage of the one ledger: entry id -> (payload, piggybacked LAC); the fenced bit.
    let mut entries: BTreeMap<i64, (u64, i64)> = BTreeMap::new();
    let mut fenced = false;
    loop {
        match recv_msg_block::<Net>() {
            Net::AddReq { from, id, lac, payload, recovery } => {
                // WriteEntryProcessorV3 l.112-115: RECOVERY_ADD -> recoveryAddEntry, else addEntry;
                // BookieImpl.addEntry l.1112: LedgerFencedException when fenced -> EFENCED.
                let rejected = !recovery && fenced;
                if !rejected {
                    entries.insert(id, (payload, lac)); // addEntryInternal
                }
                send_tagged_msg(
                    a.of(from),
                    TAG_ADD + id as u32,
                    Net::AddResp { bookie: idx, id, fenced: rejected },
                );
            }
            Net::ReadReq { from, id, fence } => {
                // ReadEntryProcessorV3.getReadResponse: a fence request fences first (fenceLedger),
                // then readEntry; NoEntry / NoLedger -> ENOENTRY / ENOLEDGER.
                if fence {
                    fenced = true; // LedgerDescriptor.setFenced
                }
                let found = if id == LAST_ADD_CONFIRMED {
                    entries.iter().next_back().map(|(_, v)| *v)
                } else {
                    entries.get(&id).copied()
                };
                let rc = match found {
                    Some((payload, lac)) => ReadRc::Ok { payload, lac },
                    None => ReadRc::NoEntry,
                };
                let tag = if id == LAST_ADD_CONFIRMED { TAG_LAC } else { TAG_READ + id as u32 };
                send_tagged_msg(a.of(from), tag, Net::ReadResp { bookie: idx, id, rc });
            }
            m => panic!("bookie: unexpected {m:?}"),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// zk: the ledger's versioned metadata (LedgerManager read / write with a version check).
// ---------------------------------------------------------------------------------------------
fn zk() {
    let a = recv_setup();
    let mut meta = Meta { state: State::Open, last: INVALID_ENTRY_ID };
    let mut version: u64 = 0;
    loop {
        match recv_msg_block::<Net>() {
            Net::ZkRead { from } => send_tagged_msg(a.of(from), TAG_ZK, Net::ZkMeta { meta, version }),
            Net::ZkWrite { from, meta: m, expected } => {
                if expected == version {
                    meta = m;
                    version += 1;
                    send_tagged_msg(a.of(from), TAG_ZK, Net::ZkWriteOk { version });
                } else {
                    send_tagged_msg(a.of(from), TAG_ZK, Net::ZkBadVersion);
                }
            }
            m => panic!("zk: unexpected {m:?}"),
        }
    }
}

fn zk_read(a: &Addrs, me: Role) -> (Meta, u64) {
    send_msg(a.zk, Net::ZkRead { from: me });
    match recv_tag(TAG_ZK) {
        Net::ZkMeta { meta, version } => (meta, version),
        m => panic!("zk meta expected, got {m:?}"),
    }
}

/// One versioned write: Some(new version) or None on BadVersion.
fn zk_write(a: &Addrs, me: Role, meta: Meta, expected: u64) -> Option<u64> {
    send_msg(a.zk, Net::ZkWrite { from: me, meta, expected });
    match recv_tag(TAG_ZK) {
        Net::ZkWriteOk { version } => Some(version),
        Net::ZkBadVersion => None,
        m => panic!("zk write reply expected, got {m:?}"),
    }
}

/// LedgerHandle.doAsyncCloseInternal's MetadataUpdateLoop (l.614-660): write Closed(lastEntry) at
/// the cached version; on a version conflict re-read and test again. Returns the final metadata.
/// `stop_in_recovery`: the writer's decision (NOTES.md D5): a re-read InRecovery state ends the
/// writer's close (as LedgerHandle l.2181-2186 / l.2413-2419), it does not overwrite it.
fn close_metadata(a: &Addrs, me: Role, mut cache: (Meta, u64), last_entry: i64, stop_in_recovery: bool) -> Meta {
    loop {
        let (meta, version) = cache;
        if meta.state == State::Closed {
            // closed with the same lastEntry: nothing to do; otherwise BKMetadataVersionException
            return meta;
        }
        if stop_in_recovery && meta.state == State::InRecovery {
            return meta;
        }
        let target = Meta { state: State::Closed, last: last_entry };
        match zk_write(a, me, target, version) {
            Some(_v) => return target,
            None => cache = zk_read(a, me),
        }
    }
}

fn send_all(a: &Addrs, m: Net) {
    for b in a.b.iter() {
        send_msg(*b, m.clone());
    }
}

// ---------------------------------------------------------------------------------------------
// w1h: the writer's LedgerHandle (doAsyncAddEntry, PendingAddOp, sendAddSuccessCallbacks,
// handleUnrecoverableErrorDuringAdd, doAsyncCloseInternal).
// ---------------------------------------------------------------------------------------------
fn writer_handle() {
    let a = recv_setup();
    let w1 = a.w1;
    let mut last_add_pushed: i64 = INVALID_ENTRY_ID;
    let mut last_add_confirmed: i64 = INVALID_ENTRY_ID;
    let mut handle_open = true; // HandleState.OPEN
    // The handle's cached versioned metadata: the ledger as created (Open, version 0).
    let mut cache: (Meta, u64) = (Meta { state: State::Open, last: INVALID_ENTRY_ID }, 0);
    loop {
        let Req::Add(n) = recv_tagged_msg_block::<_, Req>(move |tid, _| tid == w1) else {
            panic!("w1h: Add expected")
        };
        // doAsyncAddEntry l.1716: isHandleWritable, else LedgerClosedException at once.
        if !(handle_open && cache.0.state != State::Closed) {
            send_msg(w1, Rep::Rejected(n));
            continue;
        }
        last_add_pushed += 1;
        let id = last_add_pushed;
        // PendingAddOp.initiate: one write per bookie of the write set, carrying lh.lastAddConfirmed.
        send_all(&a, Net::AddReq { from: Role::W1h, id, lac: last_add_confirmed, payload: n, recovery: false });
        // PendingAddOp.writeComplete: the ack set; the first Fenced answer before the ack quorum
        // is unrecoverable. Responses after completion are ignored (l.~300 `if (completed)`), so
        // they are never received.
        let mut acks = 0;
        let fenced = loop {
            match recv_tag(TAG_ADD + id as u32) {
                Net::AddResp { fenced: false, .. } => {
                    acks += 1;
                    if acks >= QA {
                        break false;
                    }
                }
                Net::AddResp { fenced: true, .. } => break true,
                m => panic!("w1h: add response expected, got {m:?}"),
            }
        };
        if !fenced {
            // sendAddSuccessCallbacks l.2225: in entry order (the client is synchronous: only
            // one op is ever pending), lastAddConfirmed advances.
            last_add_confirmed = id;
            send_msg(w1, Rep::Acked(n));
            continue;
        }
        // handleUnrecoverableErrorDuringAdd l.2180
        if cache.0.state == State::InRecovery {
            // errorOutPendingAdds only (unreachable: the cache is InRecovery only after a close)
            send_msg(w1, Rep::Rejected(n));
            continue;
        }
        // asyncCloseInternal -> doAsyncCloseInternal: drain and error out the pending adds,
        // lastEntry = lastAddPushed = lastAddConfirmed, handle CLOSED, then the metadata loop.
        handle_open = false;
        last_add_pushed = last_add_confirmed;
        send_msg(w1, Rep::Rejected(n));
        let m = close_metadata(&a, Role::W1h, cache, last_add_confirmed, true);
        cache.0 = m; // setLedgerMetadata (the version is not needed any more)
    }
}

// ---------------------------------------------------------------------------------------------
// w2h: the recovery client's LedgerHandle (open with recovery, LedgerRecoveryOp,
// ReadLastConfirmedOp.initiateWithFencing, RecoveryReadOp, asyncRecoveryAddEntry, close).
// ---------------------------------------------------------------------------------------------
fn recovery_handle() {
    let a = recv_setup();
    let w2 = a.w2;
    let Req::Recover = recv_tagged_msg_block::<_, Req>(move |tid, _| tid == w2) else {
        panic!("w2h: Recover expected")
    };
    // Open with recovery (LedgerOpenOp, outside the cut): read the metadata, mark InRecovery with
    // a versioned write; retry on a version conflict.
    let cache: (Meta, u64);
    loop {
        let (meta, version) = zk_read(&a, Role::W2h);
        if meta.state == State::Closed {
            // already closed: nothing to recover
            send_msg(w2, Rep::Closed(entry_number(meta.last)));
            park(w2);
        }
        let target = Meta { state: State::InRecovery, last: meta.last };
        if let Some(v) = zk_write(&a, Role::W2h, target, version) {
            cache = (target, v);
            break;
        }
    }
    // ReadLastConfirmedOp.initiateWithFencing: a fencing LAC read to every bookie; complete at
    // COVER valid responses (OK or NoSuchEntry) with the maximum LAC. Later responses are ignored.
    send_all(&a, Net::ReadReq { from: Role::W2h, id: LAST_ADD_CONFIRMED, fence: true });
    let mut valid = 0;
    let mut max_lac = INVALID_ENTRY_ID;
    while valid < COVER {
        match recv_tag(TAG_LAC) {
            Net::ReadResp { rc: ReadRc::Ok { lac, .. }, .. } => {
                max_lac = max_lac.max(lac);
                valid += 1;
            }
            Net::ReadResp { rc: ReadRc::NoEntry, .. } => valid += 1,
            m => panic!("w2h: LAC response expected, got {m:?}"),
        }
    }
    // LedgerRecoveryOp.readLastConfirmedDataComplete: lastAddPushed = lastAddConfirmed =
    // max(LAC, firstEntryOfLastEnsemble - 1) = max(LAC, -1).
    let mut last_add_confirmed = max_lac.max(INVALID_ENTRY_ID);
    let mut last_add_pushed = last_add_confirmed;
    // doRecoveryRead with batch size 1; reads and recovery adds are pipelined as in the source.
    let mut read_id = last_add_confirmed + 1;
    let mut next_replica = 0usize; // SequenceReadRequest.nextReplicaIndexToReadFrom
    let mut read_missing = 0usize;
    let mut read_done = false;
    let mut read_count = 0u64;
    let mut write_count = 0u64;
    // pending recovery adds: id -> acks (pendingAddOps); completed when acks reach QA
    let mut pending: BTreeMap<i64, usize> = BTreeMap::new();
    send_next_read(&a, read_id, &mut next_replica);
    while !(read_done && read_count == write_count) {
        let want_read = if read_done { None } else { Some(TAG_READ + read_id as u32) };
        let want_adds: Vec<u32> = pending
            .iter()
            .filter(|(_, acks)| **acks < QA)
            .map(|(id, _)| TAG_ADD + *id as u32)
            .collect();
        let m = recv_tagged_msg_block::<_, Net>(move |_, t| match t {
            Some(t) => Some(t) == want_read || want_adds.contains(&t),
            None => false,
        });
        match m {
            Net::ReadResp { id, rc, .. } => {
                assert_eq!(id, read_id);
                match rc {
                    ReadRc::Ok { payload, .. } => {
                        // SingleLedgerEntryRequest.complete: the first positive response
                        // completes the read (PendingReadOp.readEntryComplete).
                        // onEntryComplete(OK): asyncRecoveryAddEntry, then the next read.
                        read_count += 1;
                        last_add_pushed += 1; // doAsyncAddEntry: ++lastAddPushed
                        assert_eq!(last_add_pushed, id, "unexpected entryId during recovery add");
                        pending.insert(id, 0);
                        send_all(
                            &a,
                            Net::AddReq { from: Role::W2h, id, lac: last_add_confirmed, payload, recovery: true },
                        );
                        read_id += 1;
                        next_replica = 0;
                        read_missing = 0;
                        send_next_read(&a, read_id, &mut next_replica);
                    }
                    ReadRc::NoEntry => {
                        // PendingReadOp l.278/411: NoSuchEntry at COVER missing bookies.
                        read_missing += 1;
                        if read_missing >= COVER {
                            read_done = true;
                        } else {
                            // !readsOutstanding() -> sendNextRead()
                            send_next_read(&a, read_id, &mut next_replica);
                        }
                    }
                }
            }
            Net::AddResp { id, fenced, .. } => {
                // a recovery add is never rejected by fencing (recoveryAddEntry l.1024)
                assert!(!fenced, "recovery add rejected");
                let acks = pending.get_mut(&id).expect("pending recovery add");
                *acks += 1;
                // sendAddSuccessCallbacks: complete in entry order; addComplete counts writes.
                while let Some((&head, &acks)) = pending.iter().next() {
                    if acks < QA || head != last_add_confirmed + 1 {
                        break;
                    }
                    pending.remove(&head);
                    last_add_confirmed = head;
                    write_count += 1;
                }
            }
            m => panic!("w2h: unexpected {m:?}"),
        }
    }
    // Close (doAsyncCloseInternal): lastEntry = lastAddConfirmed, Closed at zk.
    let _ = last_add_pushed;
    let m = close_metadata(&a, Role::W2h, cache, last_add_confirmed, false);
    assert!(
        m.state == State::Closed && m.last == last_add_confirmed,
        "metadata conflict when closing the recovered ledger"
    );
    send_msg(w2, Rep::Closed(entry_number(last_add_confirmed)));
    park(w2);
}

/// SequenceReadRequest.sendNextRead with sendReadTo's recovery flags (FLAG_DO_FENCING, main
/// l.181-182): read entry `id` from the next replica of its write set, bookie (id + j) % 3
/// (RoundRobinDistributionSchedule l.59; no reordering, see NOTES.md).
fn send_next_read(a: &Addrs, id: i64, next_replica: &mut usize) {
    assert!(*next_replica < QW, "read failed at every replica");
    let b = a.b[(id as usize + *next_replica) % QW];
    *next_replica += 1;
    send_msg(b, Net::ReadReq { from: Role::W2h, id, fence: true });
}

/// A handle never terminates: it waits for a client request that never comes.
fn park(client: ThreadId) -> ! {
    loop {
        let _ = recv_tagged_msg_block::<_, Req>(move |tid, _| tid == client);
    }
}

// ---------------------------------------------------------------------------------------------
// The program entry.
// ---------------------------------------------------------------------------------------------
fn imp(e: u64) {
    let named = |n: &str| thread::Builder::new().name(n.to_string());
    let b1 = named("b1").spawn_daemon(|| bookie(0)).unwrap().thread().id();
    let b2 = named("b2").spawn_daemon(|| bookie(1)).unwrap().thread().id();
    let b3 = named("b3").spawn_daemon(|| bookie(2)).unwrap().thread().id();
    let zkt = named("zk").spawn_daemon(zk).unwrap().thread().id();
    let w1h = named("w1h").spawn_daemon(writer_handle).unwrap().thread().id();
    let w2h = named("w2h").spawn_daemon(recovery_handle).unwrap().thread().id();
    let w1 = named("w1").spawn(move || writer(e, w1h)).unwrap();
    let w2 = named("w2").spawn(move || recoverer(w2h)).unwrap();
    let (w1id, w2id) = (w1.thread().id(), w2.thread().id());
    let init = named("init")
        .spawn(move || {
            let s = Net::Setup { b: [b1, b2, b3], zk: zkt, w1h, w2h, w1: w1id, w2: w2id };
            for t in [b1, b2, b3, zkt, w1h, w2h] {
                send_tagged_msg(t, TAG_SETUP, s.clone());
            }
        })
        .unwrap();
    w1.join().unwrap();
    w2.join().unwrap();
    init.join().unwrap();
}

fn entry(args: &[String]) -> Entry {
    let e: u64 = args.first().and_then(|s| s.parse().ok()).unwrap_or(1);
    assert!(e >= 1, "e >= 1");
    Arc::new(move || imp(e))
}

fn main() -> ExitCode {
    program::main(Program {
        visible: &|_| vec!["w1".to_string(), "w2".to_string()],
        spec: &entry,
        imp: &entry,
    })
}
