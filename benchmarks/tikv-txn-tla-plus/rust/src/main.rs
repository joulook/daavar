//! S_dt+: the specification program S_dt of TiKV's distributed transactions
//! (daavar/benchmarks/tikv-txn-tla, from PingCAP's TLA+ model `DistributedTransaction.tla`,
//! pingcap/tla-plus 5649f86; "DT:<line>" below), against TraceForge's Must API, with exactly
//! three changes, each marked `// PLUS-A:`, `// PLUS-B:`, `// PLUS-C:` (NOTES.md of this
//! directory; owner's decision 2026-11-10 18:40, VERDICT-correct-r1.md causes A, B, C):
//! A a fresh for_update_ts at every lock_key; B removable pessimistic secondary locks;
//! C async-commit / 1PC commit timestamps from a per-key max_ts. Everything else is S_dt's.
//! Interface: INTERFACE.md (agents' copy). S_dt's decisions: ../../tikv-txn-tla/rust/NOTES.md.
//!
//! Processes: `k1..kn` (one per key: the model's `key_data`, `key_lock`, `key_write`),
//! `ts` (the model's `next_ts`), `h1..hm` (one invisible handler per client: the model's
//! client actions), `sup` (setup, quiescence detection, end of the run, the model's global
//! invariants), and the visible clients `c1..cm` (client code of INTERFACE.md 5.1, verbatim).

use std::collections::{BTreeMap, BTreeSet};
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ---------------------------------------------------------------------------------------
// Interface (INTERFACE.md sections 3, 4, 5.1): verbatim.

#[derive(Clone, Copy, Debug, PartialEq)] enum Kind { Optimistic, Pessimistic }
#[derive(Clone, Copy, Debug, PartialEq)] enum Op { Get(u8), GetForUpdate(u8), Put(u8) }
fn scenario(name: &str) -> (u8 /* n */, Vec<(Kind, Vec<Op>)>) {
    use Kind::*; use Op::*;
    match name {
        "S1"  => (1, vec![(Pessimistic, vec![Put(1)]), (Optimistic, vec![Get(1), Put(1)])]),
        "X2"  => (2, vec![(Optimistic, vec![Put(1), Put(2)]), (Optimistic, vec![Put(2), Put(1)])]),
        "P2"  => (2, vec![(Pessimistic, vec![Put(1), Put(2)]), (Pessimistic, vec![Put(2), Put(1)])]),
        "X2r" => (2, vec![(Optimistic, vec![Get(1), Get(2), Put(1), Put(2)]), (Optimistic, vec![Get(2), Get(1), Put(2), Put(1)])]),
        "M2"  => (2, vec![(Pessimistic, vec![Put(1), Put(2)]), (Optimistic, vec![Get(1), Get(2), Put(1), Put(2)])]),
        "L1"  => (1, vec![(Optimistic, vec![Get(1), Put(1)]), (Optimistic, vec![Get(1), Put(1)])]),
        "A3"  => (2, vec![(Optimistic, vec![Put(1), Put(2)]), (Optimistic, vec![Get(2)]), (Optimistic, vec![Get(1), Get(2)])]),
        s => panic!("unknown scenario {s}"),
    }
}

#[derive(Clone, Debug, PartialEq)] enum Req { Begin, Get(u8), GetForUpdate(u8), Put(u8, u8), Commit }
#[derive(Clone, Debug, PartialEq)] enum Rep { Begun, Value(u8), Done, Committed, Aborted }

fn client(i: u8, script: Vec<Op>, t: ThreadId) {
    send_msg(t, Req::Begin);
    let Rep::Begun = recv_msg_block::<Rep>() else { panic!("Begun expected") };
    for op in script {
        match op {
            Op::Get(k) | Op::GetForUpdate(k) => {
                send_msg(t, if let Op::Get(_) = op { Req::Get(k) } else { Req::GetForUpdate(k) });
                match recv_msg_block::<Rep>() { Rep::Value(_) => {}, Rep::Aborted => return, r => panic!("Value or Aborted expected, got {r:?}") }
            }
            Op::Put(k) => {
                send_msg(t, Req::Put(k, i));
                match recv_msg_block::<Rep>() { Rep::Done => {}, Rep::Aborted => return, r => panic!("Done or Aborted expected, got {r:?}") }
            }
        }
    }
    send_msg(t, Req::Commit);
    match recv_msg_block::<Rep>() { Rep::Committed | Rep::Aborted => {}, r => panic!("Committed or Aborted expected, got {r:?}") }
}

// ---------------------------------------------------------------------------------------
// R2 (INTERFACE.md section 8; NOTES.md D8): a request the model leaves unanswered in the
// current state may be answered `Aborted`. Every call site is such a state.

fn r2() -> bool { nondet() }

// ---------------------------------------------------------------------------------------
// Invisible messages (the model's requests, DT:116-143, and responses, DT:164-179).

type Ts = u32; // DT:109-110: Ts == Nat \ {0}, NoneTs == 0
const NONE_TS: Ts = 0;
/// Version of a key's state (lock, writes, data): incremented at every step that changes
/// it (NOTES.md D7). Not in the model; it only tells a handler that a re-evaluation of a
/// request at this key could differ from the last one.
type Ver = u32;

#[derive(Clone, Copy, Debug, PartialEq)]
enum LockTy { PrewriteOptimistic, PrewritePessimistic, LockKey } // DT:46

#[derive(Clone, Debug, PartialEq)]
struct Lock { start_ts: Ts, primary: u8, min_commit_ts: Ts, ty: LockTy } // DT:184-192

#[derive(Clone, Copy, Debug, PartialEq)]
enum WTy { Commit, Rollback }

#[derive(Clone, Debug, PartialEq)]
struct Write { ts: Ts, start_ts: Ts, ty: WTy, protected: bool } // DT:193-195

/// Requests to a key process (DT:116-143), the resolution messages one key sends another
/// (DT:655, 674, 683), the watches (NOTES.md D7), the setup and the stop.
#[derive(Clone, Debug, PartialEq)]
enum KeyMsg {
    Setup { keys: Vec<ThreadId>, sup: ThreadId },
    ReadOptimistic { from: ThreadId, start_ts: Ts },
    // PLUS-C: `need_value` (GetForUpdate) raises the key's max_ts (tikv-src
    // actions/acquire_pessimistic_lock.rs:76-82); `async_fut` = Some(for_update_ts) when the
    // prewrite carries use_async_commit or try_one_pc (client-src transaction.rs:1434-1435),
    // None for 2PC (S_dt's prewrite, unchanged).
    LockKey { from: ThreadId, start_ts: Ts, primary: u8, for_update_ts: Ts, need_value: bool },
    PrewriteOptimistic { from: ThreadId, start_ts: Ts, primary: u8, value: u8, async_fut: Option<Ts> },
    PrewritePessimistic { from: ThreadId, start_ts: Ts, primary: u8, value: u8, async_fut: Option<Ts> },
    Commit { from: ThreadId, start_ts: Ts, commit_ts: Ts },
    /// `job`: the key of the handler's request that met the lock (routes the reply).
    /// `cid`: the check's id, which tags the acknowledgements of its resolution messages.
    /// `wid`: the watch the key registers when the check's next evaluation can differ only
    /// after a change of the key; `vk`: the version at which the job's request met the lock.
    CheckTxnStatus { from: ThreadId, job: u8, cid: u32, wid: u32, vk: Ver, start_ts: Ts, caller_start_ts: Ts, resolving_pessimistic_lock: bool },
    ResolveCommitted { start_ts: Ts, primary: u8, commit_ts: Ts, ack: ThreadId, cid: u32 },
    ResolveRollbacked { start_ts: Ts, primary: u8, ack: ThreadId, cid: u32 },
    // PLUS-B: PessimisticRollback of one encountered lock (client-src lock.rs:189-217,
    // `pessimistic_rollback_lock`; tikv-src commands/pessimistic_rollback.rs:60-90): removes
    // the `lock_key` lock of start_ts at this key, writes no record; acknowledged like a
    // resolution message (`ResolveDone`, NOTES.md D11 of S_dt).
    PessimisticRollback { start_ts: Ts, ack: ThreadId, cid: u32 },
    /// A handler's request to the key with watch id `wid`: if the answer is `key_is_locked`,
    /// the key registers the watch in the same step (`Fired` at its next change; NOTES.md D7).
    Tracked { wid: u32, req: Box<KeyMsg> },
    /// Cancel watch `id`; answered `UnwatchAck`.
    Unwatch { from: ThreadId, id: u32 },
    Stop,
}

/// Result of one evaluation of check_txn_status (DT:643-686).
#[derive(Clone, Copy, Debug, PartialEq)]
enum CheckOutcome { Pushed, Unlocked, RolledBack, CommitFound, Unchanged }

/// What the handler does with a check after it (NOTES.md D7).
#[derive(Clone, Copy, Debug, PartialEq)]
enum CheckNext { ResendNow, Settled, Watching }

/// Messages to a handler: the setup, timestamps, the model's responses (DT:164-179), and
/// the program's own bookkeeping.
#[derive(Clone, Debug, PartialEq)]
enum HMsg {
    Setup { client: ThreadId, sup: ThreadId },
    Ts(Ts),
    ReadOptimisticSucceed { key: u8, value_ts: Ts, value: u8 },
    LockKeySucceed { key: u8, for_update_ts: Ts, value_ts: Ts, value: u8 },
    /// `ver`: the key's version at this evaluation.
    KeyIsLocked { key: u8, lock_primary: u8, lock_ts: Ts, lock_ty: LockTy, ver: Ver },
    LockKeyFailedWriteConflict { key: u8, latest_commit_ts: Ts },
    // PLUS-C: `min_commit_ts` = max(max_ts, start_ts, for_update_ts) + 1 at an async-commit /
    // 1PC prewrite (tikv-src actions/prewrite.rs:940-973), 0 at a 2PC prewrite.
    Prewrited { key: u8, min_commit_ts: Ts },
    CommitTsExpired { min_commit_ts: Ts },
    // RespMessages (DT:176-179): they end the transaction.
    Committed,
    CommitAborted,
    LockKeyAborted { key: u8 },
    PrewriteAborted { key: u8 },
    /// The model takes no action on the request in this state (no reply in the model).
    Silent { key: u8 },
    /// End of one evaluation of a CheckTxnStatus (not in the model; NOTES.md D5): its
    /// outcome, the keys it sent a resolution message to, the primary's version after it.
    CheckDone { job: u8, cid: u32, key: u8, outcome: CheckOutcome, resolving: Vec<u8>, ver: Ver, next: CheckNext },
    /// A resolution message has been evaluated (NOTES.md D11); `tag`: its own tag.
    ResolveDone { tag: u32 },
    Fired { id: u32 },
    UnwatchAck { id: u32, fired: bool },
    /// From `sup`: every handler is idle or finished (NOTES.md D7).
    Quiesce { epoch: u32 },
    /// From `sup` after the report: the number of `Quiesce` still to be taken.
    Release { pending: Vec<u32> /* epochs */ },
}

#[derive(Clone, Debug, PartialEq)]
enum TsMsg { Next(ThreadId), Stop(ThreadId /* sup */) }

/// Messages to `sup`.
#[derive(Clone, Debug, PartialEq)]
enum SupMsg {
    /// Handler h waits only for changes that may never come (NOTES.md D7). `seq`: the
    /// number of notifications (`Idle`, `Busy`) h has sent, this one included (D11).
    Idle { h: u8, epoch: u32, seq: u32 },
    /// Handler h left the wait of `epoch`.
    Busy { h: u8, epoch: u32, seq: u32 },
    /// Handler h has served its client and taken every message it is owed. `notes_sent`:
    /// the number of `Idle`/`Busy` it has sent; `sup` takes all of them before it ends (D11).
    Handler { h: u8, start_ts: Ts, for_update_ts: Ts, kind: Kind, primary: u8, write_keys: Vec<u8>,
              read: Vec<(u8, Ts)>, quiesce_received: Vec<u32> /* epochs */, notes_sent: u32 },
    Key { lock: Option<Lock>, writes: Vec<Write>, resps: Vec<(Ts, bool /* committed */)>, seen_ts: Vec<Ts> },
    NextTs(Ts),
}

const TAG_SETUP: u32 = 1;
const TAG_RESP: u32 = 2;
const TAG_CHECK: u32 = 3;
const TAG_TS: u32 = 4;
const TAG_RELEASE: u32 = 7;
const TAG_REPORT_H: u32 = 100; // + handler index
const TAG_REPORT_K: u32 = 200; // + key
const TAG_REPORT_TS: u32 = 300;
const TAG_FIRED: u32 = 0x1000_0000; // | watch id
const TAG_ACK: u32 = 0x2000_0000;   // | watch id
/// One tag per acknowledgement and per `Quiesce`, so that no receive can choose among equal
/// messages (under `bag` such choices multiply the executions without new behaviour).
const TAG_RESOLVE_DONE: u32 = 0x4000_0000; // | cid << 4 | key
const TAG_QUIESCE: u32 = 0x5000_0000;      // | epoch
/// A handler's `Idle`/`Busy` (NOTES.md D11, round 3): one tag per notification, so that
/// `sup` can take the ones still in flight at the end by their tags, in a fixed order.
/// During the run `sup` accepts every tag of this range, as it accepted the one shared tag.
const TAG_NOTE: u32 = 0x6000_0000;         // | h << 16 | seq
const TAG_CLASS: u32 = 0xF000_0000;
fn ack_tag(cid: u32, k: u8) -> u32 { TAG_RESOLVE_DONE | (cid << 4) | k as u32 }
/// Bound (fixed scenarios only, not arbitrary DT instances; round 4, note 3): at most ~50
/// key-changing steps (12 lock creations/conversions, 12 pushes, 12 removals, 14 rollback
/// records, DT:387-397) times at most 4 live watches per handler give <= 200 wakeups, far
/// below 2^16 notifications; write-conflict retries change no key.
fn note_tag(h: u8, seq: u32) -> u32 { assert!(seq < 1 << 16); TAG_NOTE | ((h as u32) << 16) | seq }

// ---------------------------------------------------------------------------------------
// The model's helper operators.

/// find_readable_commit (DT:403-411): the latest commit record with ts <= read_ts.
fn find_readable_commit(writes: &[Write], read_ts: Ts) -> Option<&Write> {
    writes.iter().filter(|w| w.ty == WTy::Commit && w.ts <= read_ts).max_by_key(|w| w.ts)
}

/// keyCommitted (DT:783-786).
fn key_committed(writes: &[Write], start_ts: Ts) -> bool {
    writes.iter().any(|w| w.start_ts == start_ts && w.ty == WTy::Commit)
}

// ---------------------------------------------------------------------------------------
// Key process: key_data[k], key_lock[k], key_write[k] and the server actions on k.

struct Key {
    k: u8,
    data: BTreeMap<Ts, u8>, // key_data[k] (DT:42), with the written value of each version
    lock: Option<Lock>,     // key_lock[k] (DT:67), at most one (DT:197)
    writes: Vec<Write>,     // key_write[k] (DT:74)
    resps: Vec<(Ts, bool)>, // resp_msgs committed / commit_aborted sent by this key (DT:37)
    seen_ts: Vec<Ts>,       // every timestamp carried by a request evaluated here (MsgTsConsistency)
    // PLUS-C: the key's max_ts (tikv-src concurrency_manager lib.rs:235, `update_max_ts`), per
    // key as the owner fixed (INTERFACE section 6 has it per region). Read only by an async /
    // 1PC prewrite; not part of `state()`, so it changes no version and fires no watch (no
    // evaluation other than that min_commit_ts reads it).
    max_ts: Ts,
    keys: Vec<ThreadId>,
    ver: Ver,
    watchers: Vec<(u32, ThreadId)>,
    fired: BTreeSet<u32>,
}

impl Key {
    fn value_of(&self, w: Option<&Write>) -> (Ts, u8) {
        match w {
            None => (NONE_TS, 0),
            Some(w) => (w.ts, self.data.get(&w.start_ts).copied().unwrap_or(0)),
        }
    }

    fn state(&self) -> (Option<Lock>, Vec<Write>, BTreeMap<Ts, u8>) {
        (self.lock.clone(), self.writes.clone(), self.data.clone())
    }

    /// unlock_key (DT:361-362).
    fn unlock(&mut self) { self.lock = None; }

    /// commit (DT:364-370); false when no lock of start_ts exists (the operator is FALSE).
    fn commit(&mut self, start_ts: Ts, commit_ts: Ts) -> bool {
        match &self.lock {
            Some(l) if l.start_ts == start_ts => {
                self.unlock();
                self.writes.push(Write { ts: commit_ts, start_ts, ty: WTy::Commit, protected: false });
                true
            }
            _ => false,
        }
    }

    /// rollback (DT:373-399).
    fn rollback(&mut self, start_ts: Ts) {
        let k = self.k;
        let protected = matches!(&self.lock, Some(l) if l.start_ts == start_ts && l.primary == k
            && matches!(l.ty, LockTy::LockKey | LockTy::PrewritePessimistic));
        if matches!(&self.lock, Some(l) if l.start_ts == start_ts) { self.unlock(); }
        self.data.remove(&start_ts);
        if !self.writes.iter().any(|w| w.ts == start_ts) {
            self.writes.retain(|w| !(w.ty == WTy::Rollback && !w.protected && w.ts < start_ts));
            self.writes.push(Write { ts: start_ts, start_ts, ty: WTy::Rollback, protected });
        }
    }

    fn locked_by_other(&self, l: &Lock) -> HMsg {
        HMsg::KeyIsLocked { key: self.k, lock_primary: l.primary, lock_ts: l.start_ts, lock_ty: l.ty, ver: 0 }
    }

    /// ServerReadKey (DT:447-477).
    fn read_key(&mut self, start_ts: Ts) -> HMsg {
        let k = self.k;
        // PLUS-C: a point read raises max_ts to its read ts before it checks the lock
        // (tikv-src storage/mod.rs:3585, prepare_snap_ctx; the lock check follows, 3596).
        self.max_ts = self.max_ts.max(start_ts);
        match &self.lock {
            Some(l) if !(l.ty == LockTy::LockKey || l.start_ts == start_ts) => self.locked_by_other(l),
            _ => {
                let (value_ts, value) = self.value_of(find_readable_commit(&self.writes, start_ts));
                HMsg::ReadOptimisticSucceed { key: k, value_ts, value }
            }
        }
    }

    /// ServerLockKey (DT:479-537).
    fn lock_key(&mut self, start_ts: Ts, primary: u8, for_update_ts: Ts) -> HMsg {
        let k = self.k;
        match self.lock.clone() {
            None => {
                let latest = self.writes.iter().filter(|w| w.ty == WTy::Commit).map(|w| w.ts).max();
                if self.writes.iter().any(|w| w.start_ts == start_ts && w.ty == WTy::Rollback) {
                    HMsg::LockKeyAborted { key: k } // DT:494-500
                } else if !latest.is_some_and(|t| t > for_update_ts) {
                    // DT:507-513
                    self.lock = Some(Lock { start_ts, primary, min_commit_ts: NONE_TS, ty: LockTy::LockKey });
                    let (value_ts, value) = self.value_of(find_readable_commit(&self.writes, for_update_ts));
                    HMsg::LockKeySucceed { key: k, for_update_ts, value_ts, value }
                } else if !key_committed(&self.writes, start_ts) {
                    // DT:516-523
                    HMsg::LockKeyFailedWriteConflict { key: k, latest_commit_ts: latest.unwrap() }
                } else {
                    HMsg::Silent { key: k } // no disjunct of DT:489-523 is enabled
                }
            }
            Some(l) if l.start_ts == start_ts => {
                // DT:526-529
                let (value_ts, value) = self.value_of(find_readable_commit(&self.writes, for_update_ts));
                HMsg::LockKeySucceed { key: k, for_update_ts, value_ts, value }
            }
            Some(l) => self.locked_by_other(&l), // DT:530-537
        }
    }

    // PLUS-C: async_commit_timestamps (tikv-src actions/prewrite.rs:940-973):
    // max(max(max_ts, start_ts), for_update_ts) + 1, computed where the lock is written; no
    // oracle call. client-rust sets no min_commit_ts / max_commit_ts (client-src
    // transaction.rs:1442, FIXME), so neither the request's minimum nor the fallback applies.
    // 0 at a 2PC prewrite (`async_fut` None): S_dt's reply.
    fn min_commit_ts(&self, start_ts: Ts, async_fut: Option<Ts>) -> Ts {
        match async_fut {
            Some(fut) => self.max_ts.max(start_ts).max(fut) + 1,
            None => NONE_TS,
        }
    }

    /// ServerPrewriteOptimistic (DT:539-577).
    fn prewrite_optimistic(&mut self, start_ts: Ts, primary: u8, value: u8, async_fut: Option<Ts>) -> HMsg {
        let k = self.k;
        match self.lock.clone() {
            None => {
                // PLUS-C: with async commit or 1PC a commit_ts can equal another transaction's
                // start_ts; TiKV's optimistic write-conflict test is strict, `commit_ts >
                // start_ts` (tikv-src actions/prewrite.rs:584-592; equality "is ok to continue",
                // 569-571). Under 2PC every commit_ts comes from the oracle, no record has
                // ts = start_ts except the transaction's own rollback, and S_dt's `>=` is kept.
                let newer = |w: &Write| if async_fut.is_some() { w.ts > start_ts } else { w.ts >= start_ts };
                if key_committed(&self.writes, start_ts) {
                    HMsg::Silent { key: k } // DT:547-549: duplicated prewrite, UNCHANGED vars
                } else if self.writes.iter().any(|w| (w.start_ts == start_ts && w.ty == WTy::Rollback) || newer(w)) {
                    HMsg::PrewriteAborted { key: k } // DT:550-554
                } else {
                    // DT:556-563
                    self.lock = Some(Lock { start_ts, primary, ty: LockTy::PrewriteOptimistic, min_commit_ts: NONE_TS });
                    self.data.insert(start_ts, value);
                    HMsg::Prewrited { key: k, min_commit_ts: self.min_commit_ts(start_ts, async_fut) }
                }
            }
            Some(l) if l.start_ts == start_ts => HMsg::Prewrited { key: k, min_commit_ts: self.min_commit_ts(start_ts, async_fut) }, // DT:565-568
            Some(l) => self.locked_by_other(&l),                            // DT:569-577
        }
    }

    /// ServerPrewritePessimistic (DT:579-605).
    fn prewrite_pessimistic(&mut self, start_ts: Ts, primary: u8, value: u8, async_fut: Option<Ts>) -> HMsg {
        let k = self.k;
        let own_lock_key = matches!(&self.lock, Some(l) if l.start_ts == start_ts && l.ty == LockTy::LockKey);
        let free = self.lock.is_none() && !self.writes.iter().any(|w| w.ts >= start_ts);
        if own_lock_key || free {
            self.lock = Some(Lock { start_ts, primary, ty: LockTy::PrewritePessimistic, min_commit_ts: NONE_TS });
            self.data.insert(start_ts, value);
            HMsg::Prewrited { key: k, min_commit_ts: self.min_commit_ts(start_ts, async_fut) } // PLUS-C
        } else {
            HMsg::PrewriteAborted { key: k }
        }
    }

    /// ServerCommit (DT:607-640), at the primary key.
    fn server_commit(&mut self, start_ts: Ts, commit_ts: Ts) -> HMsg {
        if key_committed(&self.writes, start_ts) {
            self.resps.push((start_ts, true)); // DT:614-618
            HMsg::Committed
        } else {
            match self.lock.clone() {
                Some(l) if l.start_ts == start_ts && l.ty != LockTy::LockKey => {
                    if commit_ts >= l.min_commit_ts {
                        let ok = self.commit(start_ts, commit_ts); // DT:623-629
                        assert(ok);
                        self.resps.push((start_ts, true));
                        HMsg::Committed
                    } else {
                        HMsg::CommitTsExpired { min_commit_ts: l.min_commit_ts } // DT:630-634
                    }
                }
                _ => {
                    self.resps.push((start_ts, false)); // DT:635-640
                    HMsg::CommitAborted
                }
            }
        }
    }

    /// Sends a resolution message to every other key; the keys it was sent to.
    fn broadcast(&self, m: KeyMsg) -> Vec<u8> {
        let mut to = Vec::new();
        for (j, t) in self.keys.iter().enumerate() {
            let k2 = j as u8 + 1;
            if k2 != self.k { send_msg(*t, m.clone()); to.push(k2); }
        }
        to
    }

    /// check_txn_status_missing_lock (DT:669-686).
    fn check_missing_lock(&mut self, ack: ThreadId, cid: u32, start_ts: Ts, resolving_pessimistic_lock: bool) -> (CheckOutcome, Vec<u8>) {
        let pk = self.k;
        let committed: Vec<Ts> = self.writes.iter()
            .filter(|w| w.start_ts == start_ts && w.ty == WTy::Commit).map(|w| w.ts).collect();
        if !committed.is_empty() {
            let mut to = Vec::new();
            for commit_ts in committed { to.extend(self.broadcast(KeyMsg::ResolveCommitted { start_ts, primary: pk, commit_ts, ack, cid })); }
            (CheckOutcome::CommitFound, to)
        } else if resolving_pessimistic_lock {
            (CheckOutcome::Unchanged, Vec::new()) // DT:679-680
        } else {
            self.rollback(start_ts);
            (CheckOutcome::RolledBack, self.broadcast(KeyMsg::ResolveRollbacked { start_ts, primary: pk, ack, cid }))
        }
    }

    /// ServerCheckTxnStatus (DT:701-715) with check_txn_status_has_lock (DT:643-666).
    fn check_txn_status(&mut self, ack: ThreadId, job: u8, cid: u32, start_ts: Ts, caller_start_ts: Ts, resolving_pessimistic_lock: bool) -> HMsg {
        let pk = self.k;
        // PLUS-C: CheckTxnStatus raises max_ts to max(lock_ts, current_ts, caller_start_ts)
        // (tikv-src commands/check_txn_status.rs:88-104); DT's check has no current_ts and
        // caller_start_ts = NoneTs (DT:247).
        self.max_ts = self.max_ts.max(start_ts).max(caller_start_ts);
        let (outcome, resolving) = match self.lock.clone() {
            Some(l) if l.start_ts == start_ts => {
                // DT:659-666: the push disjunct, enabled when min_commit_ts <= caller_start_ts.
                let push_enabled = l.min_commit_ts <= caller_start_ts;
                if push_enabled && nondet() {
                    self.lock = Some(Lock { min_commit_ts: caller_start_ts + 1, ..l });
                    (CheckOutcome::Pushed, Vec::new())
                } else if l.ty == LockTy::LockKey && resolving_pessimistic_lock {
                    self.unlock(); // DT:648-652
                    (CheckOutcome::Unlocked, Vec::new())
                } else {
                    self.rollback(start_ts); // DT:653-658
                    (CheckOutcome::RolledBack, self.broadcast(KeyMsg::ResolveRollbacked { start_ts, primary: pk, ack, cid }))
                }
            }
            _ => self.check_missing_lock(ack, cid, start_ts, resolving_pessimistic_lock), // DT:712-715
        };
        HMsg::CheckDone { job, cid, key: pk, outcome, resolving, ver: 0, next: CheckNext::Settled }
    }

    /// The per-key conjuncts of the model's invariants, after every step of the key.
    fn assert_local_invariants(&self) {
        // WriteConsistency (DT:822-829)
        for w in &self.writes {
            match w.ty {
                WTy::Commit => assert(w.ts > w.start_ts && self.data.contains_key(&w.start_ts)),
                WTy::Rollback => assert(w.ts == w.start_ts),
            }
        }
        // UniqueLockOrWrite (DT:833-837)
        if let Some(l) = &self.lock {
            assert(self.writes.iter().all(|w| w.start_ts != l.start_ts));
        }
        // UniqueWrite (DT:840-843)
        for (a, w) in self.writes.iter().enumerate() {
            for w2 in &self.writes[a + 1..] { assert(w.start_ts != w2.start_ts); }
        }
    }

    fn see(&mut self, ts: Ts) { if !self.seen_ts.contains(&ts) { self.seen_ts.push(ts); } }

    fn run(k: u8) {
        let KeyMsg::Setup { keys, sup } = recv_tagged_msg_block::<_, KeyMsg>(|_, t| t == Some(TAG_SETUP))
            else { panic!("setup expected") };
        let mut s = Key { k, data: BTreeMap::new(), lock: None, writes: Vec::new(), resps: Vec::new(),
            seen_ts: Vec::new(), max_ts: NONE_TS, keys, ver: 0, watchers: Vec::new(), fired: BTreeSet::new() };
        loop {
            let before = s.state();
            let mut out: Option<(ThreadId, u32, HMsg)> = None;
            let mut wid: Option<u32> = None;          // watch of a tracked request
            let mut check_watch: Option<(u32, Ver)> = None; // (wid, vk) of a check
            let mut msg = recv_msg_block::<KeyMsg>();
            if let KeyMsg::Tracked { wid: w, req } = msg { wid = Some(w); msg = *req; }
            if let KeyMsg::CheckTxnStatus { wid: w, vk, .. } = &msg { check_watch = Some((*w, *vk)); }
            match msg {
                KeyMsg::Setup { .. } => panic!("second setup"),
                KeyMsg::ReadOptimistic { from, start_ts } => {
                    s.see(start_ts); out = Some((from, TAG_RESP, s.read_key(start_ts)));
                }
                KeyMsg::LockKey { from, start_ts, primary, for_update_ts, need_value } => {
                    s.see(start_ts); s.see(for_update_ts);
                    // PLUS-C: a lock with need_value is a read at for_update_ts and raises max_ts
                    // before the lock check (tikv-src actions/acquire_pessimistic_lock.rs:76-82).
                    if need_value { s.max_ts = s.max_ts.max(for_update_ts); }
                    out = Some((from, TAG_RESP, s.lock_key(start_ts, primary, for_update_ts)));
                }
                KeyMsg::PrewriteOptimistic { from, start_ts, primary, value, async_fut } => {
                    s.see(start_ts); out = Some((from, TAG_RESP, s.prewrite_optimistic(start_ts, primary, value, async_fut)));
                }
                KeyMsg::PrewritePessimistic { from, start_ts, primary, value, async_fut } => {
                    s.see(start_ts); out = Some((from, TAG_RESP, s.prewrite_pessimistic(start_ts, primary, value, async_fut)));
                }
                KeyMsg::Commit { from, start_ts, commit_ts } => {
                    s.see(start_ts); s.see(commit_ts); out = Some((from, TAG_RESP, s.server_commit(start_ts, commit_ts)));
                }
                KeyMsg::CheckTxnStatus { from, job, cid, start_ts, caller_start_ts, resolving_pessimistic_lock, .. } => {
                    s.see(start_ts);
                    out = Some((from, TAG_CHECK, s.check_txn_status(from, job, cid, start_ts, caller_start_ts, resolving_pessimistic_lock)));
                }
                KeyMsg::ResolveCommitted { start_ts, primary, commit_ts, ack, cid } => {
                    // ServerResolveCommitted (DT:717-728)
                    s.see(start_ts); s.see(commit_ts);
                    if matches!(&s.lock, Some(l) if l.primary == primary && l.start_ts == start_ts) {
                        let ok = s.commit(start_ts, commit_ts);
                        assert(ok);
                    }
                    out = Some((ack, ack_tag(cid, k), HMsg::ResolveDone { tag: ack_tag(cid, k) }));
                }
                KeyMsg::ResolveRollbacked { start_ts, primary, ack, cid } => {
                    // ServerResolveRollbacked (DT:730-741)
                    s.see(start_ts);
                    if matches!(&s.lock, Some(l) if l.primary == primary && l.start_ts == start_ts) {
                        s.rollback(start_ts);
                    }
                    out = Some((ack, ack_tag(cid, k), HMsg::ResolveDone { tag: ack_tag(cid, k) }));
                }
                KeyMsg::PessimisticRollback { start_ts, ack, cid } => {
                    // PLUS-B: tikv-src commands/pessimistic_rollback.rs:84-90: a pessimistic lock
                    // of start_ts (with for_update_ts <= the request's, vacuous here: a
                    // transaction creates at most one `lock_key` lock per key, NOTES.md) is
                    // unlocked; no record is written; any other lock is left alone.
                    s.see(start_ts);
                    if matches!(&s.lock, Some(l) if l.start_ts == start_ts && l.ty == LockTy::LockKey) {
                        s.unlock();
                    }
                    out = Some((ack, ack_tag(cid, k), HMsg::ResolveDone { tag: ack_tag(cid, k) }));
                }
                KeyMsg::Tracked { .. } => panic!("nested request"),
                KeyMsg::Unwatch { from, id } => {
                    // A watch is registered in the step whose reply tells the handler about it,
                    // so its Unwatch always comes after it (round 2, finding 1).
                    let fired = if let Some(p) = s.watchers.iter().position(|(i, _)| *i == id) {
                        s.watchers.remove(p);
                        false
                    } else {
                        assert!(s.fired.contains(&id), "Unwatch of an unknown watch");
                        true
                    };
                    send_tagged_msg(from, TAG_ACK | id, HMsg::UnwatchAck { id, fired });
                }
                KeyMsg::Stop => break,
            }
            let changed = s.state() != before;
            if changed {
                s.ver += 1;
                for (id, w) in std::mem::take(&mut s.watchers) {
                    send_tagged_msg(w, TAG_FIRED | id, HMsg::Fired { id });
                    s.fired.insert(id);
                }
            }
            if let Some((to, tag, mut m)) = out {
                match &mut m {
                    HMsg::KeyIsLocked { ver, .. } => {
                        *ver = s.ver;
                        if let Some(w) = wid { s.watchers.push((w, to)); }
                    }
                    HMsg::CheckDone { job, outcome, resolving, ver, next, .. } => {
                        *ver = s.ver;
                        let (w, _vk) = check_watch.unwrap();
                        *next = if *outcome == CheckOutcome::Pushed {
                            // DT:659-666 changed the primary: the check may be evaluated again
                            // now (its delivery can still be delayed to any later state), or
                            // only after the key's next change (or never).
                            if nondet() { CheckNext::ResendNow } else { CheckNext::Watching }
                        } else if resolving.contains(job) || (*job == k && changed) {
                            // A resolution is on its way to the request's key, or this step
                            // changed that key itself: any watch the request has there fires.
                            CheckNext::Settled
                        } else {
                            CheckNext::Watching
                        };
                        if *next == CheckNext::Watching { s.watchers.push((w, to)); }
                    }
                    _ => {}
                }
                send_tagged_msg(to, tag, m);
            }
            s.assert_local_invariants();
        }
        assert(s.watchers.is_empty());
        send_tagged_msg(sup, TAG_REPORT_K + k as u32,
            SupMsg::Key { lock: s.lock, writes: s.writes, resps: s.resps, seen_ts: s.seen_ts });
    }
}

// ---------------------------------------------------------------------------------------
// The timestamp process: next_ts (DT:91-94, Init DT:746).

fn ts_process() {
    let mut next_ts: Ts = 1;
    loop {
        match recv_msg_block::<TsMsg>() {
            TsMsg::Next(from) => { send_tagged_msg(from, TAG_TS, HMsg::Ts(next_ts)); next_ts += 1; }
            TsMsg::Stop(sup) => { send_tagged_msg(sup, TAG_REPORT_TS, SupMsg::NextTs(next_ts)); break; }
        }
    }
}

// ---------------------------------------------------------------------------------------
// Handler: one per client, the model's client actions (DT:211-356).

/// One outstanding model request of the handler (a "job", NOTES.md D5, D7), with the
/// check_txn_status of the lock it last met. The request and the check progress
/// independently (round 2, finding 2).
#[derive(Clone, Copy, Debug, PartialEq)]
enum RSt {
    /// The request is at its key; its reply is due.
    InFlight,
    /// It met a lock; it is sent again at the first change of its key (watch `kw`).
    Blocked,
    Done,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum CSt {
    None,
    /// The check is at the lock's primary; its CheckDone is due.
    InFlight,
    /// Its next evaluation can differ only after the primary changes (watch `pw`).
    Waiting,
    /// It decided and sent a resolution message to the request's key (or changed that key
    /// itself): evaluating it again is a stutter (D7), and the key changes for certain.
    Settled,
}

#[derive(Clone, Debug)]
struct Job {
    k: u8,
    req: KeyMsg,
    r: RSt,
    c: CSt,
    lock: (u8 /* primary */, Ts, LockTy),
    vk: Ver,
    kw: Option<u32>,
    pw: Option<u32>,
    cid: u32, // id of the job's current check
    rwid: u32, // watch id sent with the job's last request
    cwid: u32, // watch id sent with the job's current check
}

enum Step { Final(HMsg), Resend(KeyMsg), Abort }

fn reply_key(m: &HMsg) -> u8 {
    match m {
        HMsg::ReadOptimisticSucceed { key, .. } | HMsg::LockKeySucceed { key, .. } | HMsg::KeyIsLocked { key, .. }
        | HMsg::LockKeyFailedWriteConflict { key, .. } | HMsg::Prewrited { key, .. } | HMsg::LockKeyAborted { key }
        | HMsg::PrewriteAborted { key } | HMsg::Silent { key } => *key,
        m => panic!("not a key reply: {m:?}"),
    }
}

struct Handler {
    idx: u8,             // 0-based
    kind: Kind,
    primary: u8,         // CLIENT_PRIMARY[c] (0: the transaction writes nothing)
    write_keys: Vec<u8>, // CLIENT_WRITE_KEY[c], in script order
    keys: Vec<ThreadId>,
    ts: ThreadId,
    me: ThreadId,
    sup: ThreadId,
    a: u32,
    async_commit: bool,           // PLUS-C: async = 1 or onepc = 1 (INTERFACE 1)
    start_ts: Ts,                 // client_ts[c].start_ts
    for_update_ts: Ts,            // client_ts[c].for_update_ts
    read: Vec<(u8, Ts)>,          // client_read[c] (value_ts per key)
    buffer: BTreeMap<u8, u8>,     // the client's written values (not in the model)
    locked: Vec<u8>,              // pessimistic: keys whose lock_key succeeded
    jobs: Vec<Job>,
    out_resolve: Vec<u32>,        // tags of the acknowledgements due for this handler's checks' resolutions
    cancel_pending: Vec<u32>,     // cancelled watches whose UnwatchAck is due
    next_watch: u32,
    epoch: u32,
    idle_sent: bool,
    notes_sent: u32,              // number of Idle/Busy sent to sup (D11, round 3)
    quiesce_received: Vec<u32>,
    next_check: u32,
    checks_out: Vec<u32>,         // ids of the checks whose CheckDone is due (also of abandoned ones)
    check_wid: BTreeMap<u32, u32>, // check id -> its watch id
}

impl Handler {
    fn key(&self, k: u8) -> ThreadId { self.keys[k as usize - 1] }

    fn next_ts(&self) -> Ts {
        send_msg(self.ts, TsMsg::Next(self.me));
        let HMsg::Ts(t) = recv_tagged_msg_block::<_, HMsg>(|_, t| t == Some(TAG_TS)) else { panic!("ts expected") };
        t
    }

    fn cwid_of(&self, cid: u32) -> u32 { self.check_wid[&cid] }

    fn new_wid(&mut self) -> u32 {
        let id = ((self.idx as u32) << 16) | self.next_watch;
        self.next_watch += 1;
        id
    }

    fn unwatch(&mut self, k: u8, id: u32) {
        send_msg(self.key(k), KeyMsg::Unwatch { from: self.me, id });
        self.cancel_pending.push(id);
    }

    /// R1: the model's ClientResolveLock (DT:238-250) for the lock job j met; false when
    /// the client is answered `Aborted` while the check is pending (R2).
    fn send_check(&mut self, j: usize) -> bool {
        let (pk, lock_ts, ty) = self.jobs[j].lock;
        let cid = ((self.idx as u32) << 12) | self.next_check;
        self.next_check += 1;
        self.jobs[j].cid = cid;
        self.checks_out.push(cid);
        let wid = self.new_wid();
        self.jobs[j].cwid = wid;
        self.check_wid.insert(cid, wid);
        send_msg(self.key(pk), KeyMsg::CheckTxnStatus {
            from: self.me, job: self.jobs[j].k, cid, wid, vk: self.jobs[j].vk, start_ts: lock_ts, caller_start_ts: NONE_TS,
            resolving_pessimistic_lock: ty == LockTy::LockKey,
        });
        self.jobs[j].c = CSt::InFlight;
        !r2() // R2, D8 (b)
    }

    /// Stops following the job's check (its CheckDone, if due, is still taken).
    fn drop_check(&mut self, j: usize) {
        if let Some(id) = self.jobs[j].pw.take() { let pk = self.jobs[j].lock.0; self.unwatch(pk, id); }
        self.jobs[j].c = CSt::None;
    }

    fn send_req(&mut self, j: usize) {
        let wid = self.new_wid();
        self.jobs[j].rwid = wid;
        send_msg(self.key(self.jobs[j].k), KeyMsg::Tracked { wid, req: Box::new(self.jobs[j].req.clone()) });
        self.jobs[j].r = RSt::InFlight;
    }

    fn live(&self) -> impl Iterator<Item = &Job> { self.jobs.iter().filter(|j| j.r != RSt::Done) }

    /// Every live job's request and check wait for a change that may never come.
    fn all_full_wait(&self) -> bool {
        self.live().count() > 0 && self.checks_out.is_empty()
            && self.live().all(|j| j.r == RSt::Blocked && j.c == CSt::Waiting)
    }

    /// Tells `sup` when this handler can only be woken by a change that may never come.
    fn maybe_idle(&mut self) {
        if !self.idle_sent && self.out_resolve.is_empty() && self.all_full_wait() {
            self.epoch += 1;
            self.idle_sent = true;
            self.notes_sent += 1;
            send_tagged_msg(self.sup, note_tag(self.idx, self.notes_sent), SupMsg::Idle { h: self.idx, epoch: self.epoch, seq: self.notes_sent });
        }
    }

    fn wake(&mut self) {
        if self.idle_sent {
            self.idle_sent = false;
            self.notes_sent += 1;
            send_tagged_msg(self.sup, note_tag(self.idx, self.notes_sent), SupMsg::Busy { h: self.idx, epoch: self.epoch, seq: self.notes_sent });
        }
    }

    /// The next message the jobs wait for.
    fn recv_event(&self) -> HMsg {
        let mut tags: Vec<u32> = Vec::new();
        if self.live().any(|j| j.r == RSt::InFlight) { tags.push(TAG_RESP); }
        if !self.checks_out.is_empty() { tags.push(TAG_CHECK); }
        for j in self.live() {
            if j.r == RSt::Blocked { if let Some(id) = j.kw { tags.push(TAG_FIRED | id); } }
            if j.c == CSt::Waiting { if let Some(id) = j.pw { tags.push(TAG_FIRED | id); } }
        }
        if self.all_full_wait() { tags.extend(self.out_resolve.iter().copied()); }
        if self.idle_sent { tags.push(TAG_QUIESCE | self.epoch); }
        assert!(!tags.is_empty(), "handler with nothing to wait for");
        recv_tagged_msg_block::<_, HMsg>(move |_, t| t.is_some_and(|t| tags.contains(&t)))
    }

    /// The CheckDone of check `cid`: its acknowledgements are due; false if it is no
    /// longer the current check of a live job.
    /// The CheckDone of check `cid`: its acknowledgements are due. True if it is the
    /// current check of a live job; otherwise its watch, if the key registered one, is
    /// cancelled.
    fn take_check(&mut self, m: &HMsg) -> Option<usize> {
        let HMsg::CheckDone { job, cid, key, resolving, next, .. } = m else { unreachable!() };
        let p = self.checks_out.iter().position(|c| c == cid).expect("check due");
        self.checks_out.remove(p);
        self.out_resolve.extend(resolving.iter().map(|&k2| ack_tag(*cid, k2)));
        let j = self.jobs.iter().position(|x| x.k == *job && x.r != RSt::Done && x.c == CSt::InFlight && x.cid == *cid);
        if j.is_none() && *next == CheckNext::Watching {
            let id = self.cwid_of(*cid);
            self.unwatch(*key, id);
        }
        j
    }

    /// Runs the model requests `reqs` (key, request, R2 right after the send) to their
    /// final replies, with lock resolution (R1) and event-driven resending (NOTES.md D5,
    /// D7). `on_reply` maps every reply other than `key_is_locked`. None: answered
    /// `Aborted` (R2 or a model abort); the outstanding messages stay in `self.jobs`.
    fn run_jobs(&mut self, reqs: Vec<(u8, KeyMsg, bool)>, on_reply: &mut dyn FnMut(&mut Handler, HMsg) -> Step)
        -> Option<Vec<(u8, HMsg)>> {
        assert!(self.jobs.is_empty());
        self.jobs = reqs.iter().map(|(k, m, _)| Job { k: *k, req: m.clone(), r: RSt::InFlight, c: CSt::None,
            lock: (0, 0, LockTy::LockKey), vk: 0, kw: None, pw: None, cid: 0, rwid: 0, cwid: 0 }).collect();
        for j in 0..self.jobs.len() { self.send_req(j); }
        for (_, _, r) in &reqs { if *r && r2() { return None; } } // R2, D8 (d)
        let mut results = Vec::new();
        while self.live().count() > 0 {
            self.maybe_idle();
            match self.recv_event() {
                m @ HMsg::CheckDone { .. } => {
                    let Some(j) = self.take_check(&m) else { continue }; // abandoned check
                    let HMsg::CheckDone { next, outcome, cid, .. } = m else { unreachable!() };
                    // PLUS-B: the job's request met a `lock_key` lock on a key other than that
                    // lock's primary, and the check at the primary ended in DT's unlock
                    // (DT:648-652) or unchanged (DT:679-680) branch: send PessimisticRollback of
                    // that lock to the request's key (client-src lock.rs:423-434 ->
                    // `pessimistic_rollback_lock` 189-217; lock.key != primary, 194-199). The
                    // request is sent again when the key changes (its watch, D7), as before.
                    let (lpk, lts, lty) = self.jobs[j].lock;
                    let jk = self.jobs[j].k;
                    if lty == LockTy::LockKey && jk != lpk && matches!(outcome, CheckOutcome::Unlocked | CheckOutcome::Unchanged) {
                        send_msg(self.key(jk), KeyMsg::PessimisticRollback { start_ts: lts, ack: self.me, cid });
                        self.out_resolve.push(ack_tag(cid, jk));
                    }
                    match next {
                        CheckNext::ResendNow => { if !self.send_check(j) { return None; } }
                        CheckNext::Settled => self.jobs[j].c = CSt::Settled,
                        CheckNext::Watching => {
                            self.jobs[j].c = CSt::Waiting;
                            self.jobs[j].pw = Some(self.jobs[j].cwid);
                        }
                    }
                }
                HMsg::Fired { id } => {
                    self.wake();
                    if let Some(j) = self.jobs.iter().position(|x| x.r == RSt::Blocked && x.kw == Some(id)) {
                        // The request's key changed: send the request again. Its check goes on.
                        self.jobs[j].kw = None;
                        self.send_req(j);
                    } else {
                        // The primary changed: evaluate the check again.
                        let j = self.jobs.iter().position(|x| x.c == CSt::Waiting && x.pw == Some(id)).expect("watch of a job");
                        self.jobs[j].pw = None;
                        if !self.send_check(j) { return None; }
                    }
                }
                HMsg::ResolveDone { tag } => {
                    let p = self.out_resolve.iter().position(|t| *t == tag).expect("ack due");
                    self.out_resolve.remove(p);
                }
                HMsg::Quiesce { epoch } => {
                    self.quiesce_received.push(epoch);
                    // R2, D8 (c): see D7 "Quiescence".
                    if self.idle_sent && epoch == self.epoch { return None; }
                }
                HMsg::KeyIsLocked { key, lock_primary, lock_ts, lock_ty, ver } => {
                    let j = self.jobs.iter().position(|x| x.k == key && x.r == RSt::InFlight).expect("reply of a job");
                    self.jobs[j].r = RSt::Blocked;
                    self.jobs[j].kw = Some(self.jobs[j].rwid); // registered by the key (D7)
                    self.jobs[j].vk = ver;
                    if r2() { self.jobs[j].r = RSt::Done; return None; } // R2, D8 (a)
                    let lock = (lock_primary, lock_ts, lock_ty);
                    if !(self.jobs[j].c != CSt::None && self.jobs[j].lock == lock) {
                        // A lock the job's check is not about: follow a new check.
                        self.drop_check(j);
                        self.jobs[j].lock = lock;
                        if !self.send_check(j) { return None; }
                    }
                }
                r => {
                    let key = reply_key(&r);
                    let j = self.jobs.iter().position(|x| x.k == key && x.r == RSt::InFlight).expect("reply of a job");
                    self.drop_check(j);
                    match on_reply(self, r) {
                        Step::Final(m) => { self.jobs[j].r = RSt::Done; results.push((key, m)); }
                        Step::Resend(m) => { self.jobs[j].req = m; self.send_req(j); }
                        Step::Abort => { self.jobs[j].r = RSt::Done; return None; }
                    }
                }
            }
        }
        self.jobs.clear();
        Some(results)
    }

    /// Pessimistic lock of k: the model's lock_key request (ClientLockKey DT:252-264) and
    /// its responses (DT:266-297), `a` attempts on a write conflict (INTERFACE 5.4).
    /// Some(value) on success.
    fn lock_key(&mut self, k: u8, need_value: bool) -> Option<u8> {
        // PLUS-A: a fresh for_update_ts from the oracle at every lock_key, as client-rust's
        // `pessimistic_lock` takes one per call (client-src transaction.rs:887-888) and DT's own
        // write-conflict retry takes one per resend (DT:289-291); S_dt sent the first lock_key
        // of every key at start_ts (DT:255, 259-263). Resends after lock resolution keep it
        // (the request plan re-dispatches the same request, client-src request/plan.rs).
        self.for_update_ts = self.next_ts();
        let first_fut = self.for_update_ts;
        // DT:275 accepts a success only at the client's current for_update_ts: a success to a
        // request at an older one can never be accepted, while a write conflict newer than the
        // current for_update_ts can still enable the retry of DT:289-296 (D8 (d)).
        // PLUS-A: the first request is now always at the current for_update_ts (stale = false).
        let stale = first_fut != self.for_update_ts;
        let m = KeyMsg::LockKey { from: self.me, start_ts: self.start_ts, primary: self.primary, for_update_ts: first_fut, need_value };
        let mut attempts = 1;
        let a = self.a;
        let r = self.run_jobs(vec![(k, m, stale)], &mut |h, r| match r {
            HMsg::LockKeySucceed { for_update_ts, .. } => {
                // DT:275: only a response at the current for_update_ts is taken.
                if for_update_ts == h.for_update_ts { Step::Final(r) } else { Step::Abort }
            }
            HMsg::LockKeyFailedWriteConflict { latest_commit_ts, .. } => {
                // DT:289: the response is taken only if latest_commit_ts > for_update_ts.
                if !(latest_commit_ts > h.for_update_ts) { return Step::Abort; }
                if attempts == a { return Step::Abort; } // INTERFACE 5.4
                attempts += 1;
                h.for_update_ts = h.next_ts(); // DT:290-291
                Step::Resend(KeyMsg::LockKey { from: h.me, start_ts: h.start_ts, primary: h.primary, for_update_ts: h.for_update_ts, need_value })
            }
            HMsg::LockKeyAborted { .. } | HMsg::Silent { .. } => Step::Abort,
            r => panic!("unexpected lock_key response {r:?}"),
        })?;
        let HMsg::LockKeySucceed { value_ts, value, .. } = r[0].1 else { unreachable!() };
        self.read.retain(|(k2, _)| *k2 != k);
        self.read.push((k, value_ts)); // add_client_read_history (DT:279)
        self.locked.push(k);
        Some(value)
    }

    /// Commit: the model's ClientPrewriteOptimistic / ClientPrewritePessimistic
    /// (DT:299-319), ClientPrewrited (DT:321-329) and ClientCommit (DT:331-341).
    fn commit(&mut self) -> Rep {
        if self.write_keys.is_empty() { return Rep::Committed; } // NOTES.md D9
        // PLUS-C: an async-commit / 1PC prewrite carries the transaction's for_update_ts (0 for
        // an optimistic one, client-src transaction.rs:1425-1435; tikv-src actions/prewrite.rs:940-973).
        let async_fut = if !self.async_commit { None }
            else { Some(if self.kind == Kind::Pessimistic { self.for_update_ts } else { NONE_TS }) };
        let reqs: Vec<(u8, KeyMsg, bool)> = self.write_keys.iter().map(|&k| {
            let value = self.buffer[&k];
            let m = match self.kind {
                Kind::Optimistic => KeyMsg::PrewriteOptimistic { from: self.me, start_ts: self.start_ts, primary: self.primary, value, async_fut },
                Kind::Pessimistic => KeyMsg::PrewritePessimistic { from: self.me, start_ts: self.start_ts, primary: self.primary, value, async_fut },
            };
            (k, m, false)
        }).collect();
        let r = self.run_jobs(reqs, &mut |_, r| match r {
            HMsg::Prewrited { .. } => Step::Final(r),
            HMsg::PrewriteAborted { .. } | HMsg::Silent { .. } => Step::Abort,
            r => panic!("unexpected prewrite response {r:?}"),
        });
        let Some(r) = r else { return Rep::Aborted; };
        // ClientCommit (DT:331-341): commit_ts = next_ts, commit request to the primary.
        // ServerCommit (DT:607-640) answers it in every state: no R2 here.
        // PLUS-C: with async = 1 or onepc = 1 the commit_ts is the largest min_commit_ts of the
        // prewrites, max(max_ts of the keys, start_ts, for_update_ts) + 1, with no oracle call
        // (client-src transaction.rs:1386-1388, 1471-1481; tikv-src actions/prewrite.rs:940-973,
        // 1PC commands/prewrite.rs:953-969). With async = onepc = 0: next_ts, as S_dt.
        let commit_ts = if self.async_commit {
            r.iter().map(|(_, m)| match m { HMsg::Prewrited { min_commit_ts, .. } => *min_commit_ts, m => panic!("{m:?}") })
                .max().expect("a prewrite per write key")
        } else {
            self.next_ts()
        };
        send_msg(self.key(self.primary), KeyMsg::Commit { from: self.me, start_ts: self.start_ts, commit_ts });
        match recv_tagged_msg_block::<_, HMsg>(|_, t| t == Some(TAG_RESP)) {
            HMsg::Committed => Rep::Committed,
            HMsg::CommitAborted => Rep::Aborted,
            // ClientRetryCommit (DT:343-356) is never enabled (stage "commiting"): the request
            // stays unanswered, R2 (D4c).
            HMsg::CommitTsExpired { .. } => Rep::Aborted,
            r => panic!("unexpected commit response {r:?}"),
        }
    }

    /// One client request: its reply, and whether it is the last one.
    fn serve(&mut self, req: Req) -> (Rep, bool) {
        match req {
            Req::Begin => {
                // ClientReadOptimistic / ClientLockKey (DT:212-217, 252-257): start_ts = next_ts,
                // and for a pessimistic client for_update_ts = start_ts.
                self.start_ts = self.next_ts();
                self.for_update_ts = self.start_ts;
                (Rep::Begun, false)
            }
            Req::Get(k) => {
                assert!(self.kind == Kind::Optimistic, "Get from a pessimistic client");
                if let Some(v) = self.buffer.get(&k) { return (Rep::Value(*v), false); } // D10
                let m = KeyMsg::ReadOptimistic { from: self.me, start_ts: self.start_ts };
                let r = self.run_jobs(vec![(k, m, false)], &mut |_, r| match r {
                    HMsg::ReadOptimisticSucceed { .. } => Step::Final(r),
                    r => panic!("unexpected read response {r:?}"),
                });
                match r {
                    None => (Rep::Aborted, true),
                    Some(r) => {
                        let HMsg::ReadOptimisticSucceed { value_ts, value, .. } = r[0].1 else { unreachable!() };
                        self.read.retain(|(k2, _)| *k2 != k);
                        self.read.push((k, value_ts)); // ClientReadOptimisticSucceed (DT:227-236)
                        (Rep::Value(value), false)
                    }
                }
            }
            Req::GetForUpdate(k) => {
                assert!(self.kind == Kind::Pessimistic, "GetForUpdate from an optimistic client");
                match self.lock_key(k, true) { // PLUS-C: need_value
                    Some(v) => (Rep::Value(v), false),
                    None => (Rep::Aborted, true),
                }
            }
            Req::Put(k, v) => {
                self.buffer.insert(k, v);
                if self.kind == Kind::Pessimistic && !self.locked.contains(&k) {
                    if self.lock_key(k, false).is_none() { return (Rep::Aborted, true); }
                }
                (Rep::Done, false)
            }
            Req::Commit => (self.commit(), true),
        }
    }

    /// After the last reply: take every message this handler is still owed (every message
    /// is delivered exactly once, NOTES.md D11), then report to `sup`.
    fn finish(&mut self) {
        loop {
            let resp = self.live().filter(|j| j.r == RSt::InFlight).count();
            if resp == 0 && self.checks_out.is_empty() { break; }
            let mut tags = Vec::new();
            if resp > 0 { tags.push(TAG_RESP); }
            if !self.checks_out.is_empty() { tags.push(TAG_CHECK); }
            match recv_tagged_msg_block::<_, HMsg>(move |_, t| t.is_some_and(|t| tags.contains(&t))) {
                m @ HMsg::CheckDone { .. } => {
                    if let Some(j) = self.take_check(&m) {
                        // The current check of a job: its watch, if any, is cancelled below.
                        let HMsg::CheckDone { next, .. } = m else { unreachable!() };
                        if next == CheckNext::Watching { self.jobs[j].pw = Some(self.jobs[j].cwid); }
                        self.jobs[j].c = CSt::None;
                    }
                }
                r => {
                    let key = reply_key(&r);
                    let j = self.jobs.iter().position(|x| x.k == key && x.r == RSt::InFlight).unwrap();
                    self.jobs[j].r = RSt::Done;
                    // A `key_is_locked` registered the request's watch at the key (D7).
                    if let HMsg::KeyIsLocked { .. } = r { self.jobs[j].kw = Some(self.jobs[j].rwid); }
                }
            }
        }
        for j in 0..self.jobs.len() {
            if let Some(id) = self.jobs[j].kw.take() { let k = self.jobs[j].k; self.unwatch(k, id); }
            if let Some(id) = self.jobs[j].pw.take() { let pk = self.jobs[j].lock.0; self.unwatch(pk, id); }
        }
        self.jobs.clear();
        for id in std::mem::take(&mut self.cancel_pending) {
            let HMsg::UnwatchAck { fired, .. } = recv_tagged_msg_block::<_, HMsg>(move |_, t| t == Some(TAG_ACK | id))
                else { panic!("UnwatchAck expected") };
            if fired {
                let HMsg::Fired { .. } = recv_tagged_msg_block::<_, HMsg>(move |_, t| t == Some(TAG_FIRED | id))
                    else { panic!("Fired expected") };
            }
        }
        for tag in std::mem::take(&mut self.out_resolve) {
            let HMsg::ResolveDone { .. } = recv_tagged_msg_block::<_, HMsg>(move |_, t| t == Some(tag)) else { panic!() };
        }
        send_tagged_msg(self.sup, TAG_REPORT_H + self.idx as u32, SupMsg::Handler {
            h: self.idx, start_ts: self.start_ts, for_update_ts: self.for_update_ts, kind: self.kind,
            primary: self.primary, write_keys: self.write_keys.clone(), read: self.read.clone(),
            quiesce_received: self.quiesce_received.clone(), notes_sent: self.notes_sent,
        });
        let HMsg::Release { pending } = recv_tagged_msg_block::<_, HMsg>(|_, t| t == Some(TAG_RELEASE))
            else { panic!("Release expected") };
        for e in pending {
            let HMsg::Quiesce { .. } = recv_tagged_msg_block::<_, HMsg>(move |_, t| t == Some(TAG_QUIESCE | e)) else { panic!() };
        }
    }

    fn run(idx: u8, kind: Kind, primary: u8, write_keys: Vec<u8>, keys: Vec<ThreadId>, ts: ThreadId, a: u32, async_commit: bool) {
        let HMsg::Setup { client, sup } = recv_tagged_msg_block::<_, HMsg>(|_, t| t == Some(TAG_SETUP))
            else { panic!("setup expected") };
        let me = thread::current().id();
        let mut h = Handler { idx, kind, primary, write_keys, keys, ts, me, sup, a, async_commit, start_ts: NONE_TS,
            for_update_ts: NONE_TS, read: Vec::new(), buffer: BTreeMap::new(), locked: Vec::new(),
            jobs: Vec::new(), out_resolve: Vec::new(), cancel_pending: Vec::new(), next_watch: 0, epoch: 0,
            idle_sent: false, notes_sent: 0, quiesce_received: Vec::new(), next_check: 0, checks_out: Vec::new(), check_wid: BTreeMap::new() };
        loop {
            let req = recv_tagged_msg_block::<_, Req>(|_, t| t.is_none());
            let (rep, last) = h.serve(req);
            send_msg(client, rep);
            if last { break; }
        }
        h.finish();
    }
}

// ---------------------------------------------------------------------------------------
// sup: setup, quiescence, end of the run, and the global invariants on the final state.

#[derive(Clone, Copy, Debug, PartialEq)]
enum HState { Active, Idle(u32), Finished }

fn sup(keys: Vec<ThreadId>, ts: ThreadId, handlers: Vec<ThreadId>, clients: Vec<ThreadId>) {
    let me = thread::current().id();
    for &k in &keys { send_tagged_msg(k, TAG_SETUP, KeyMsg::Setup { keys: keys.clone(), sup: me }); }
    for (h, c) in handlers.iter().zip(&clients) { send_tagged_msg(*h, TAG_SETUP, HMsg::Setup { client: *c, sup: me }); }
    let m = handlers.len();
    let mut st = vec![HState::Active; m];
    let mut last_idle = vec![0u32; m];
    let mut busy_upto = vec![0u32; m];
    let mut sent: Vec<Vec<u32>> = vec![Vec::new(); m];
    let mut sent_for = vec![0u32; m];
    let mut txns: Vec<Option<SupMsg>> = vec![None; m];
    let mut notes_got: Vec<BTreeSet<u32>> = vec![BTreeSet::new(); m]; // seqs of the Idle/Busy taken (D11)
    let mut next = 0usize; // reports are taken in handler order, except while a handler is idle
    let mut finished = 0;
    while finished < m {
        let any_idle = st.iter().any(|s| matches!(s, HState::Idle(_)));
        let nx = next as u32;
        let mm = m as u32;
        let msg = recv_tagged_msg_block::<_, SupMsg>(move |_, t| match t {
            Some(t) if t & TAG_CLASS == TAG_NOTE => true,
            Some(t) if t == TAG_REPORT_H + nx => true,
            Some(t) if any_idle && (TAG_REPORT_H..TAG_REPORT_H + mm).contains(&t) => true,
            _ => false,
        });
        match msg {
            SupMsg::Idle { h, epoch, seq } => {
                let h = h as usize;
                notes_got[h].insert(seq);
                if st[h] != HState::Finished && epoch > busy_upto[h] && epoch > last_idle[h] {
                    st[h] = HState::Idle(epoch);
                    last_idle[h] = epoch;
                }
            }
            SupMsg::Busy { h, epoch, seq } => {
                let h = h as usize;
                notes_got[h].insert(seq);
                busy_upto[h] = busy_upto[h].max(epoch);
                if st[h] == HState::Idle(epoch) { st[h] = HState::Active; }
            }
            SupMsg::Handler { h, ref quiesce_received, .. } => {
                let hh = h as usize;
                st[hh] = HState::Finished;
                finished += 1;
                send_tagged_msg(handlers[hh], TAG_RELEASE, HMsg::Release { pending: sent[hh].iter().copied().filter(|e| !quiesce_received.contains(e)).collect() });
                txns[hh] = Some(msg);
                while next < m && txns[next].is_some() { next += 1; }
            }
            _ => unreachable!(),
        }
        // Every handler not finished has announced that it waits only for changes that may
        // never come. This is not a proof that none can come (a `Busy` or a `Fired` may be in
        // transit): the `Quiesce` abort rests on D7's summary-coverage argument (an earlier R2
        // point gives the same summaries; NOTES.md D7 "Quiescence", D8 (c)).
        if finished < m && st.iter().all(|s| matches!(s, HState::Idle(_) | HState::Finished)) {
            for h in 0..m {
                if let HState::Idle(e) = st[h] {
                    if sent_for[h] < e {
                        send_tagged_msg(handlers[h], TAG_QUIESCE | e, HMsg::Quiesce { epoch: e });
                        sent[h].push(e);
                        sent_for[h] = e;
                    }
                }
            }
        }
    }
    // Under `bag` a handler's report can overtake its own `Idle`/`Busy`: take every one still
    // in flight, by its tag, in sequence order (NOTES.md D11, round 3). All handlers have
    // finished, so each is stale and has no effect.
    for h in 0..m {
        let Some(SupMsg::Handler { notes_sent, .. }) = &txns[h] else { unreachable!() };
        for seq in 1..=*notes_sent {
            if notes_got[h].contains(&seq) { continue; }
            let tag = note_tag(h as u8, seq);
            match recv_tagged_msg_block::<_, SupMsg>(move |_, t| t == Some(tag)) {
                SupMsg::Idle { .. } | SupMsg::Busy { .. } => {}
                _ => unreachable!(),
            }
        }
    }
    send_msg(ts, TsMsg::Stop(me));
    // All handler-key work is drained (each handler took every reply, CheckDone, UnwatchAck,
    // Fired and ResolveDone it was owed) and `sup` has taken every notification, so the keys
    // can stop. `Release`, outstanding `Quiesce` and the final client reply may still await
    // receipt; they cannot affect keys or visible events, and their recipients take them
    // before ending (NOTES.md D11; round 4, note 4).
    for &k in &keys { send_msg(k, KeyMsg::Stop); }
    let mut finals = Vec::new();
    for j in 0..keys.len() {
        let tag = TAG_REPORT_K + j as u32 + 1;
        finals.push(recv_tagged_msg_block::<_, SupMsg>(move |_, t| t == Some(tag)));
    }
    let SupMsg::NextTs(next_ts) = recv_tagged_msg_block::<_, SupMsg>(|_, t| t == Some(TAG_REPORT_TS))
        else { panic!("next_ts expected") };
    let txns: Vec<SupMsg> = txns.into_iter().map(|t| t.unwrap()).collect();
    check_global(&txns, &finals, next_ts);
}

/// The model's global invariants (DT:789-878) on the final state; NOTES.md D12 gives, per
/// invariant, why a violation in any reachable state is still one in the final state.
fn check_global(txns: &[SupMsg], finals: &[SupMsg], next_ts: Ts) {
    let key = |k: u8| match &finals[k as usize - 1] {
        SupMsg::Key { lock, writes, .. } => (lock.clone(), writes.clone()),
        _ => unreachable!(),
    };
    let mut resps = Vec::new();
    let mut seen = Vec::new();
    for f in finals {
        if let SupMsg::Key { resps: r, seen_ts, .. } = f { resps.extend(r.iter().cloned()); seen.extend(seen_ts.iter().cloned()); }
    }
    // UniqueCommitOrAbort (DT:789-792)
    for (s, c) in &resps {
        if *c { assert(!resps.iter().any(|(s2, c2)| s2 == s && !*c2)); }
    }
    for t in txns {
        let SupMsg::Handler { start_ts, for_update_ts, kind, primary, write_keys, read, .. } = t else { unreachable!() };
        if *start_ts == NONE_TS { continue; } // never began: no message, no read
        seen.push(*start_ts);
        seen.push(*for_update_ts);
        let committed = resps.iter().any(|(s, c)| s == start_ts && *c);
        let aborted = resps.iter().any(|(s, c)| s == start_ts && !*c);
        if !write_keys.is_empty() {
            if committed {
                // CommitConsistency (DT:797-808)
                assert(key_committed(&key(*primary).1, *start_ts));
                for &k in write_keys {
                    let (lock, writes) = key(k);
                    let locked = matches!(&lock, Some(l) if l.start_ts == *start_ts);
                    assert(!locked == key_committed(&writes, *start_ts));
                }
            }
            if aborted {
                // AbortConsistency (DT:812-817)
                assert(!key_committed(&key(*primary).1, *start_ts));
            }
        }
        // Read-only transactions included (review round 1, finding 5).
        for (k, value_ts) in read {
            let read_ts = match kind {
                // OptimisticReadSnapshotIsolation (DT:855-865)
                Kind::Optimistic => Some(*start_ts),
                // PessimisticReadSnapshotIsolation (DT:867-878), for committed transactions
                Kind::Pessimistic => if committed { Some(*for_update_ts) } else { None },
            };
            if let Some(rt) = read_ts {
                let w = find_readable_commit(&key(*k).1, rt).map(|w| w.ts).unwrap_or(NONE_TS);
                assert(w == *value_ts);
            }
        }
    }
    // MsgTsConsistency (DT:848-853): every ts in a request or held by a client is <= next_ts.
    assert(seen.iter().all(|t| *t <= next_ts));
}

// ---------------------------------------------------------------------------------------
// Program entry (INTERFACE.md section 9).

struct Params { scenario: String, a: u32, async_commit: bool /* PLUS-C */ }

fn params(args: &[String]) -> Params {
    if args.len() < 4 || args.len() > 5 {
        panic!("arguments: <scenario> <async> <onepc> <a> [fifo|bag] (a specification accepts no variant)");
    }
    let bit = |s: &str| match s { "0" => false, "1" => true, _ => panic!("async/onepc is 0 or 1, got {s}") };
    let asy = bit(&args[1]);
    let onepc = bit(&args[2]);
    if asy && onepc { panic!("async = 1 together with onepc = 1 is not an instance"); }
    let a: u32 = args[3].parse().expect("a is a number");
    if a < 1 { panic!("a >= 1"); }
    if let Some(m) = args.get(4) {
        if m != "fifo" && m != "bag" { panic!("the delivery model is fifo or bag, got {m}"); }
    }
    let _ = scenario(&args[0]);
    // PLUS-C: async and onepc are no longer ignored (S_dt NOTES D14): either selects the
    // local commit_ts rule (async = onepc = 1 is refused above, as in S_dt).
    Params { scenario: args[0].clone(), a, async_commit: asy || onepc }
}

/// INTERFACE.md section 9: the positional `fifo|bag` must equal `--model`, and the
/// effective model is fifo or bag (review round 1, finding 6). Checked on the whole
/// command line before the runner starts; the runner's entries see only the program's
/// arguments. Mirrors program.rs `parse_opts`: options until `--` or the first
/// non-option, after the subcommand (and the side, for `count` and `export`).
fn check_model(argv: &[String]) -> Result<(), String> {
    let Some(cmd) = argv.get(1) else { return Ok(()) };
    let mut i = match cmd.as_str() { "count" | "export" => 3, _ => 2 };
    let valued = ["--model", "--seed", "--threshold", "--engine", "--threads", "--out", "--program", "--size", "--spec", "--budget"];
    let mut model = "fifo".to_string(); // main's default (program.rs `main`)
    let mut prog: &[String] = &[];
    while i < argv.len() {
        let a = argv[i].as_str();
        if a == "--" { prog = &argv[i + 1..]; break; }
        if a == "--model" { model = argv.get(i + 1).cloned().unwrap_or_default(); i += 2; continue; }
        if valued.contains(&a) { i += 2; continue; }
        if a.starts_with("--") { i += 1; continue; }
        prog = &argv[i..];
        break;
    }
    if model != "fifo" && model != "bag" {
        return Err(format!("the specification supports the delivery models fifo and bag, not `{model}`"));
    }
    if let Some(p) = prog.get(4) {
        if *p != model { return Err(format!("the positional delivery model `{p}` differs from --model {model}")); }
    }
    Ok(())
}

fn visible(args: &[String]) -> Vec<String> {
    let p = params(args);
    let (_, clients) = scenario(&p.scenario);
    (1..=clients.len()).map(|i| format!("c{i}")).collect()
}

fn spec_program(p: &Params) {
    let (n, clients) = scenario(&p.scenario);
    let mut keys = Vec::new();
    for k in 1..=n {
        let h = thread::Builder::new().name(format!("k{k}")).spawn(move || Key::run(k)).unwrap();
        keys.push(h.thread().id());
    }
    let ts = thread::Builder::new().name("ts".to_string()).spawn(ts_process).unwrap().thread().id();
    let mut handlers = Vec::new();
    for (kind, script) in clients.iter().cloned() {
        // CLIENT_WRITE_KEY in script order; CLIENT_PRIMARY = the first written key.
        let mut write_keys: Vec<u8> = Vec::new();
        for op in &script {
            if let Op::Put(k) = op { if !write_keys.contains(k) { write_keys.push(*k); } }
        }
        let primary = write_keys.first().copied().unwrap_or(0);
        let keys2 = keys.clone();
        let a = p.a;
        let async_commit = p.async_commit;
        let idx = handlers.len() as u8;
        let h = thread::Builder::new().name(format!("h{}", idx + 1))
            .spawn(move || Handler::run(idx, kind, primary, write_keys, keys2, ts, a, async_commit)).unwrap();
        handlers.push(h.thread().id());
    }
    let mut cids = Vec::new();
    for (j, (_, script)) in clients.into_iter().enumerate() {
        let t = handlers[j];
        let i = (j + 1) as u8;
        let c = thread::Builder::new().name(format!("c{i}")).spawn(move || client(i, script, t)).unwrap();
        cids.push(c.thread().id());
    }
    let keys2 = keys.clone();
    let handlers2 = handlers.clone();
    thread::Builder::new().name("sup".to_string()).spawn(move || sup(keys2, ts, handlers2, cids)).unwrap();
}

fn entry(args: &[String]) -> Entry {
    let p = Arc::new(params(args));
    Arc::new(move || spec_program(&p))
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().collect();
    if let Err(e) = check_model(&argv) {
        eprintln!("bench_tikv_tla_plus: {e}");
        return ExitCode::from(2);
    }
    traceforge::program::main(Program { visible: &visible, spec: &entry, imp: &entry })
}
