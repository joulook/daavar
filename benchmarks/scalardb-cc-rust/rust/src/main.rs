//! ScalarDB Consensus Commit (scalar-labs/scalardb at 416cdc2) ported to TraceForge's Must API:
//! client-coordinated two-phase commit over conditional storage mutations, with a coordinator-state
//! table and lazy recovery. Roles, names and visible messages follow INTERFACE-agents.md; the
//! source-to-function map and every decision are in NOTES.md.
//!
//! Arguments: `<pattern> <k> <iso> [<fifo|bag>]` (the delivery model is the `--model` option of
//! the shared main; a fourth argument is accepted and ignored).
use std::collections::BTreeMap;
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ---------------------------------------------------------------------------------------------
// Visible messages (INTERFACE-agents.md, verbatim) and the client loop (verbatim).
// ---------------------------------------------------------------------------------------------
#[derive(Clone, Debug, PartialEq)] enum Req { Begin, Read(u8), Write(u8, u8), Commit }
#[derive(Clone, Debug, PartialEq)] enum Rep { Begun, Value(u8, u8), Written(u8), Committed, Aborted, Unknown }

fn client(id: u8, keys: u8, writes: &[u8], h: ThreadId) {
    send_msg(h, Req::Begin); let Rep::Begun = recv_msg_block::<Rep>() else { panic!("Begun expected") };
    for k in 1..=keys {
        send_msg(h, Req::Read(k));
        match recv_msg_block::<Rep>() { Rep::Value(_, _) => {}, Rep::Aborted => return, _ => panic!("Value or Aborted expected") }
    }
    for &k in writes { send_msg(h, Req::Write(k, id)); let Rep::Written(_) = recv_msg_block::<Rep>() else { panic!("Written expected") }; }
    send_msg(h, Req::Commit);
    match recv_msg_block::<Rep>() { Rep::Committed | Rep::Aborted | Rep::Unknown => {}, _ => panic!("Committed, Aborted or Unknown expected") }
}

// ---------------------------------------------------------------------------------------------
// Records (Attribute.java l.4-17 plus `value` / `before_value`; replaced part 8).
// ---------------------------------------------------------------------------------------------
const VALUE: usize = 0;
const ID: usize = 1; // tx_id
const STATE: usize = 2; // tx_state
const VERSION: usize = 3; // tx_version
const PREPARED_AT: usize = 4; // tx_prepared_at
const COMMITTED_AT: usize = 5; // tx_committed_at
const BEFORE: usize = 6; // before_X = X + BEFORE
const AFTER_COLS: usize = 6; // after-image columns: 0..6
const NCOLS: usize = 12;

// api/TransactionState.java l.5-10
const PREPARED: i64 = 1;
const DELETED: i64 = 2;
const COMMITTED: i64 = 3;
const ABORTED: i64 = 4;

/// One record: every column possibly null. Transaction ids t0..t3 are 0..3.
type Rec = [Option<i64>; NCOLS];

/// A conjunct is `column == constant` (Some) or `column IS NULL` (None).
#[derive(Clone, Debug, PartialEq)]
enum Cond {
    Unconditional,
    PutIfNotExists,
    If(Vec<(usize, Option<i64>)>), // putIf / deleteIf
}

#[derive(Clone, Debug, PartialEq)]
enum Mutation {
    Put { cols: Vec<(usize, Option<i64>)>, cond: Cond },
    Delete { cond: Cond },
}

/// CoordinatorStateAccessor.State without child ids and write set (replaced part 2).
#[derive(Clone, Debug, PartialEq)]
struct CState {
    state: i64,
    created_at: i64,
}

/// A RecoveryExecutor background submission (RecoveryExecutor l.223 / l.326; replaced part 5).
#[derive(Clone, Debug, PartialEq)]
enum Job {
    /// `recovery.tryRecover(selection, recovered, state)` (l.223-228)
    TryRecover { rec: Rec, state: Option<CState> },
    /// `recovery.rollbackRecord(selection, recovered)` (l.326-331)
    Rollback { rec: Rec },
}

#[derive(Clone, Debug, PartialEq)]
struct Setup {
    s: Vec<ThreadId>,
    coord: ThreadId,
    x: Vec<Vec<ThreadId>>, // x[i-1][k-1]
    h: Vec<ThreadId>,
    c: Vec<ThreadId>,
    serializable: bool,
    read_only: Vec<bool>, // per handle
    tx_ids: Vec<i64>,     // per handle (t1, t2, t3)
}

/// Invisible messages. Requests are tagged REQ_TAG, replies are tagged with the request id `rid`
/// (unique per requesting process), the setup with SETUP_TAG.
#[derive(Clone, Debug, PartialEq)]
enum Msg {
    Setup(Setup),
    // requests
    Get { from: ThreadId, rid: u32 },
    Mutate { from: ThreadId, rid: u32, m: Mutation },
    GetState { from: ThreadId, rid: u32, id: i64 },
    PutState { from: ThreadId, rid: u32, id: i64, st: CState },
    Recover { from: ThreadId, rid: u32, job: Job },
    // replies
    Record { rid: u32, rec: Option<Rec> },
    Applied { rid: u32, ok: bool }, // Ok / NoMutation
    State { rid: u32, st: Option<CState> },
    /// The recovery future's completion. The future would hold an exception only for a storage
    /// or coordinator failure, which the shared store never raises (see NOTES.md).
    Done { rid: u32 },
}

const SETUP_TAG: u32 = u32::MAX;
const REQ_TAG: u32 = 0;

fn recv_setup() -> Setup {
    match recv_tagged_msg_block::<_, Msg>(|_, t| t == Some(SETUP_TAG)) {
        Msg::Setup(s) => s,
        m => panic!("setup expected, got {m:?}"),
    }
}

fn recv_request() -> Msg {
    recv_tagged_msg_block::<_, Msg>(|_, t| t == Some(REQ_TAG))
}

fn reply(to: ThreadId, rid: u32, m: Msg) {
    send_tagged_msg(to, rid, m);
}

/// The request/reply side of a process (replaced part 3).
struct Io {
    me: ThreadId,
    next: u32,
}

impl Io {
    fn new() -> Io {
        Io { me: thread::current_id(), next: 1 }
    }
    fn send(&mut self, to: ThreadId, mk: impl FnOnce(ThreadId, u32) -> Msg) -> u32 {
        let rid = self.next;
        self.next += 1;
        send_tagged_msg(to, REQ_TAG, mk(self.me, rid));
        rid
    }
    /// A typed receive of the reply to request `rid`.
    fn wait(&self, rid: u32) -> Msg {
        recv_tagged_msg_block::<_, Msg>(move |_, t| t == Some(rid))
    }
    /// A receive of the reply to any of `rids` (the arrival order of a parallel phase).
    fn wait_any(&self, rids: &[u32]) -> Msg {
        let rids = rids.to_vec();
        recv_tagged_msg_block::<_, Msg>(move |_, t| t.is_some_and(|t| rids.contains(&t)))
    }
    fn storage_get(&mut self, s: ThreadId) -> Option<Rec> {
        let rid = self.send(s, |from, rid| Msg::Get { from, rid });
        match self.wait(rid) {
            Msg::Record { rec, .. } => rec,
            m => panic!("record expected, got {m:?}"),
        }
    }
    fn storage_mutate(&mut self, s: ThreadId, m: Mutation) -> bool {
        let rid = self.send(s, |from, rid| Msg::Mutate { from, rid, m });
        applied(self.wait(rid))
    }
    /// CoordinatorStateAccessor.getState (l.163-169, non-group branch l.168)
    fn get_state(&mut self, coord: ThreadId, id: i64) -> Option<CState> {
        let rid = self.send(coord, |from, rid| Msg::GetState { from, rid, id });
        match self.wait(rid) {
            Msg::State { st, .. } => st,
            m => panic!("state expected, got {m:?}"),
        }
    }
    /// CoordinatorStateAccessor.putState (l.214-217) / put (l.400-437): Ok, or
    /// CoordinatorConflictException with getNumAttempts() == 1 (Err).
    fn put_state(&mut self, coord: ThreadId, id: i64, state: i64, created_at: i64) -> bool {
        let st = CState { state, created_at };
        let rid = self.send(coord, |from, rid| Msg::PutState { from, rid, id, st });
        applied(self.wait(rid))
    }
}

fn applied(m: Msg) -> bool {
    match m {
        Msg::Applied { ok, .. } => ok,
        m => panic!("mutation outcome expected, got {m:?}"),
    }
}

// ---------------------------------------------------------------------------------------------
// TransactionResult helpers (TransactionResult.java l.153-232) and the image builders.
// ---------------------------------------------------------------------------------------------
fn state_of(r: &Rec) -> i64 {
    r[STATE].unwrap_or(COMMITTED) // getState: NULL -> COMMITTED (l.167)
}
fn is_committed(r: &Rec) -> bool {
    state_of(r) == COMMITTED
}
fn has_before_image(r: &Rec) -> bool {
    r[BEFORE + ID].is_some() || r[BEFORE + VERSION].is_some()
}
fn null_before(cols: &mut Vec<(usize, Option<i64>)>) {
    for c in 0..AFTER_COLS {
        cols.push((BEFORE + c, None));
    }
}

/// RecoveryExecutor.createRecordFromBeforeImage (l.399-424)
fn record_from_before_image(r: &Rec) -> Option<Rec> {
    if !has_before_image(r) {
        return None;
    }
    let mut out: Rec = [None; NCOLS];
    for c in 0..AFTER_COLS {
        out[c] = r[BEFORE + c];
    }
    Some(out)
}

/// RecoveryExecutor.createResultFromAfterImage (l.426-461); getCommittedAt() -> 0.
fn result_from_after_image(r: &Rec) -> Option<Rec> {
    if state_of(r) == DELETED {
        return None;
    }
    assert_eq!(state_of(r), PREPARED);
    let mut out = *r;
    out[STATE] = Some(COMMITTED);
    out[COMMITTED_AT] = Some(0);
    for c in 0..AFTER_COLS {
        out[BEFORE + c] = None;
    }
    Some(out)
}

/// RollbackMutationComposer.composePut (l.106-140) / composeDelete (l.142-165) for `latest`,
/// written by transaction `id`.
fn rollback_mutation(id: i64, latest: &Rec) -> Mutation {
    let cond = Cond::If(vec![(ID, Some(id)), (STATE, latest[STATE])]);
    if has_before_image(latest) {
        let mut cols = Vec::new();
        for c in 0..AFTER_COLS {
            cols.push((c, latest[BEFORE + c]));
        }
        null_before(&mut cols);
        Mutation::Put { cols, cond }
    } else {
        Mutation::Delete { cond }
    }
}

/// CommitMutationComposer.composePut (l.79-108) / composeDelete (l.110-125) for a record of
/// state `state` written by `id`.
fn commit_mutation(id: i64, committed_at: i64, state: i64) -> Mutation {
    if state == DELETED {
        return Mutation::Delete { cond: Cond::If(vec![(ID, Some(id)), (STATE, Some(DELETED))]) };
    }
    let mut cols = vec![(COMMITTED_AT, Some(committed_at)), (STATE, Some(COMMITTED))];
    null_before(&mut cols);
    Mutation::Put { cols, cond: Cond::If(vec![(ID, Some(id)), (STATE, Some(PREPARED))]) }
}

// ---------------------------------------------------------------------------------------------
// Storage partition `sk` (replaced part 1) and the coordinator table `coord` (replaced part 2).
// ---------------------------------------------------------------------------------------------
fn initial_record() -> Rec {
    let mut r: Rec = [None; NCOLS];
    r[VALUE] = Some(0);
    r[ID] = Some(0); // t0
    r[STATE] = Some(COMMITTED);
    r[VERSION] = Some(1);
    r[PREPARED_AT] = Some(0);
    r[COMMITTED_AT] = Some(0);
    r
}

fn holds(rec: &Option<Rec>, cond: &Cond) -> bool {
    match cond {
        Cond::Unconditional => true,
        Cond::PutIfNotExists => rec.is_none(),
        Cond::If(cs) => match rec {
            None => false,
            Some(r) => cs.iter().all(|&(c, v)| r[c] == v),
        },
    }
}

fn storage() {
    let _setup = recv_setup();
    let mut rec: Option<Rec> = Some(initial_record());
    loop {
        match recv_request() {
            Msg::Get { from, rid } => reply(from, rid, Msg::Record { rid, rec }),
            Msg::Mutate { from, rid, m } => {
                let ok = match &m {
                    Mutation::Put { cols, cond } => {
                        let ok = holds(&rec, cond);
                        if ok {
                            let mut r = rec.unwrap_or([None; NCOLS]);
                            for &(c, v) in cols {
                                r[c] = v;
                            }
                            rec = Some(r);
                        }
                        ok
                    }
                    Mutation::Delete { cond } => {
                        let ok = holds(&rec, cond);
                        if ok {
                            rec = None;
                        }
                        ok
                    }
                };
                reply(from, rid, Msg::Applied { rid, ok });
            }
            m => panic!("storage: unexpected {m:?}"),
        }
    }
}

fn coordinator() {
    let _setup = recv_setup();
    let mut table: BTreeMap<i64, CState> = BTreeMap::new();
    loop {
        match recv_request() {
            Msg::GetState { from, rid, id } => {
                let st = table.get(&id).cloned();
                reply(from, rid, Msg::State { rid, st });
            }
            Msg::PutState { from, rid, id, st } => {
                let ok = !table.contains_key(&id); // put-if-not-exists (l.389)
                if ok {
                    table.insert(id, st);
                }
                reply(from, rid, Msg::Applied { rid, ok });
            }
            m => panic!("coord: unexpected {m:?}"),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Recovery task `xik`: RecoveryHandler (replaced part 5).
// ---------------------------------------------------------------------------------------------
struct Recovery {
    io: Io,
    s: ThreadId,
    coord: ThreadId,
}

impl Recovery {
    /// RecoveryHandler.tryRecover (l.73-81)
    fn try_recover(&mut self, rec: &Rec, state: &Option<CState>) {
        match state {
            Some(st) => self.roll_record_to_state(rec, st),
            None => {
                self.abort_if_expired(rec);
            }
        }
    }
    /// rollRecordToState (l.151-160)
    fn roll_record_to_state(&mut self, rec: &Rec, st: &CState) {
        if st.state == COMMITTED {
            self.rollforward_record(rec, st.created_at);
        } else {
            assert_eq!(st.state, ABORTED);
            self.rollback_record(rec);
        }
    }
    /// rollbackRecord (l.162-195) with RollbackMutationComposer.add for lazy recovery (l.62-89):
    /// result.getId() equals the composer's id, so the result itself is the latest record.
    fn rollback_record(&mut self, rec: &Rec) {
        let id = rec[ID].expect("uncommitted record has a tx_id");
        let m = rollback_mutation(id, rec);
        let _ = self.io.storage_mutate(self.s, m); // NoMutation ignored (l.183-194)
    }
    /// rollforwardRecord (l.207-241) with CommitMutationComposer.add(Selection) (l.60-77)
    fn rollforward_record(&mut self, rec: &Rec, committed_at: i64) {
        let id = rec[ID].expect("uncommitted record has a tx_id");
        let st = state_of(rec);
        if st == PREPARED || st == DELETED {
            let m = commit_mutation(id, committed_at, st);
            let _ = self.io.storage_mutate(self.s, m); // NoMutation ignored (l.229-240)
        }
    }
    /// abortIfExpired (l.295-378) with ensureRecordResolved = false (the tryRecover path)
    fn abort_if_expired(&mut self, rec: &Rec) -> bool {
        if !nondet() {
            // isTransactionExpired (l.429-432) -> a fresh nondeterministic boolean
            return false;
        }
        let tx = rec[ID].expect("uncommitted record has a tx_id");
        let latest = self.io.storage_get(self.s); // rereadRecord
        match latest {
            None => return true,
            Some(l) if is_committed(&l) || l[ID] != Some(tx) => return true,
            _ => {}
        }
        // forceAbort (l.219-228, non-group branch): putState(ABORTED, 0)
        if !self.io.put_state(self.coord, tx, ABORTED, 0) {
            return false; // conflict, best-effort path
        }
        self.rollback_record(rec);
        true
    }
}

fn recovery_task(i: usize, k: usize) {
    let setup = recv_setup();
    let mut r = Recovery { io: Io::new(), s: setup.s[k - 1], coord: setup.coord };
    let _ = i;
    // One recovery job per (handle, key): the submission, then the completion message.
    match recv_request() {
        Msg::Recover { from, rid, job } => {
            match job {
                Job::TryRecover { rec, state } => r.try_recover(&rec, &state),
                Job::Rollback { rec } => r.rollback_record(&rec),
            }
            reply(from, rid, Msg::Done { rid });
        }
        m => panic!("recovery task: unexpected {m:?}"),
    }
    // Idle forever (the process never terminates).
    loop {
        let m = recv_request();
        panic!("recovery task used twice: {m:?}");
    }
}

// ---------------------------------------------------------------------------------------------
// Transaction handle `hi`: ConsensusCommit, CrudHandler, RecoveryExecutor (synchronous part),
// Snapshot, CommitHandler, ParticipantCommitHandler, CoordinatorCommitHandler.
// ---------------------------------------------------------------------------------------------
/// CrudConflictException (UncommittedRecordException, retry limit) at a read.
struct Conflict;

struct Handle {
    io: Io,
    setup: Setup,
    i: usize,
    tx: i64,
    serializable: bool,
    read_only: bool,
    // Snapshot
    read_set: BTreeMap<u8, Option<Rec>>,
    get_set: BTreeMap<u8, Option<Rec>>,
    write_set: BTreeMap<u8, u8>,
    // TransactionContext.recoveryResults: (key, the completion's rid; None = immediate future)
    recovery_results: Vec<(u8, Option<u32>)>,
}

impl Handle {
    fn s(&self, k: u8) -> ThreadId {
        self.setup.s[k as usize - 1]
    }

    /// ConsensusCommitManager.begin (l.303-330): a fresh workspace and context.
    fn begin(&mut self) {
        self.read_set.clear();
        self.get_set.clear();
        self.write_set.clear();
        self.recovery_results.clear();
    }

    /// Snapshot.mergeResult (l.324-354), without conjunctions
    fn merge_result(&self, k: u8, r: Option<Rec>) -> Option<Rec> {
        match self.write_set.get(&k) {
            Some(&v) => {
                let mut m = r.unwrap_or([None; NCOLS]);
                m[VALUE] = Some(v as i64);
                Some(m)
            }
            None => r,
        }
    }

    /// ConsensusCommit.get (l.79-81) -> CrudHandler.get (l.97-124): isSnapshotReadRequired is
    /// true (SNAPSHOT / SERIALIZABLE), so readUnread then Snapshot.getResult(key, get).
    fn get(&mut self, k: u8) -> Result<Option<Rec>, Conflict> {
        if !self.get_set.contains_key(&k) {
            self.read(k)?;
        }
        let r = self.get_set.get(&k).cloned().flatten();
        Ok(self.merge_result(k, r))
    }

    /// CrudHandler.read (l.215-303), single-row Get branch (l.241-253)
    fn read(&mut self, k: u8) -> Result<Option<Rec>, Conflict> {
        let from_storage = self.io.storage_get(self.s(k)); // getFromStorage (l.838-851)
        let result = match from_storage {
            Some(r) => self.recover_and_filter(k, r)?,
            None => None,
        };
        // putIntoReadSetInSnapshot (l.598-604), putIntoGetSet
        if !self.read_only && !self.read_set.contains_key(&k) {
            self.read_set.insert(k, result);
        }
        self.get_set.insert(k, result);
        Ok(result)
    }

    /// CrudHandler.recoverAndFilter (l.396-436), no index, no conjunctions
    fn recover_and_filter(&mut self, k: u8, r: Rec) -> Result<Option<Rec>, Conflict> {
        if !is_committed(&r) {
            self.execute_recovery(k, r)
        } else {
            Ok(Some(r))
        }
    }

    /// CrudHandler.executeRecovery (l.458-483) -> RecoveryExecutor.execute (l.123-180) with
    /// RETURN_LATEST_RESULT_AND_RECOVER (l.133-134); the result is registered in recoveryResults.
    fn execute_recovery(&mut self, k: u8, r: Rec) -> Result<Option<Rec>, Conflict> {
        let (result, future) = self.resolve_latest_result_and_recover(k, r)?;
        self.recovery_results.push((k, future));
        Ok(result)
    }

    /// Submission to `xik` (RecoveryExecutor l.223, l.326): returns the completion's rid.
    fn submit(&mut self, k: u8, job: Job) -> Option<u32> {
        let x = self.setup.x[self.i - 1][k as usize - 1];
        Some(self.io.send(x, |from, rid| Msg::Recover { from, rid, job }))
    }

    /// RecoveryExecutor.checkReadRetryLimit (l.340-349), MAX_RESOLUTION_ATTEMPTS = 5
    fn check_read_retry_limit(attempt: u32) -> Result<(), Conflict> {
        if attempt >= 5 - 1 {
            Err(Conflict)
        } else {
            Ok(())
        }
    }

    /// RecoveryExecutor.resolveLatestResultAndRecover (l.205-338)
    fn resolve_latest_result_and_recover(
        &mut self,
        k: u8,
        result: Rec,
    ) -> Result<(Option<Rec>, Option<u32>), Conflict> {
        let mut current = result;
        let mut attempt: u32 = 0;
        loop {
            let tx = current[ID].expect("uncommitted record has a tx_id");
            let coord = self.setup.coord;
            let state = self.io.get_state(coord, tx); // getCoordinatorState (l.351-361)

            if let Some(st) = state {
                // createRecoveredResult (l.363-374)
                let recovered = if st.state == ABORTED {
                    record_from_before_image(&current)
                } else {
                    assert_eq!(st.state, COMMITTED);
                    result_from_after_image(&current)
                };
                let f = self.submit(k, Job::TryRecover { rec: current, state: Some(st) });
                return Ok((recovered, f));
            }

            // throwUncommittedRecordExceptionIfTransactionNotExpired (l.376-397):
            // isTransactionExpired -> a fresh nondeterministic boolean
            if !nondet() {
                return Err(Conflict); // UncommittedRecordException
            }

            // rereadRecord
            let fresh = self.io.storage_get(self.s(k));
            match fresh {
                None => return Ok((None, None)),
                Some(f) if is_committed(&f) => return Ok((Some(f), None)),
                Some(f) if f[ID] != Some(tx) => {
                    Self::check_read_retry_limit(attempt)?;
                    current = f;
                    attempt += 1;
                    continue;
                }
                _ => {}
            }

            // RecoveryHandler.tryAbortExpiredTransaction (l.268-280) -> forceAbort
            let aborted = self.io.put_state(coord, tx, ABORTED, 0);
            if aborted {
                let after_abort = self.io.storage_get(self.s(k));
                match after_abort {
                    None => return Ok((None, None)),
                    Some(a) if is_committed(&a) => return Ok((Some(a), None)),
                    Some(a) if a[ID] != Some(tx) => {
                        Self::check_read_retry_limit(attempt)?;
                        current = a;
                        attempt += 1;
                        continue;
                    }
                    _ => {}
                }
                let recovered = record_from_before_image(&current);
                let f = self.submit(k, Job::Rollback { rec: current });
                return Ok((recovered, f));
            }

            Self::check_read_retry_limit(attempt)?;
            attempt += 1;
        }
    }

    /// ConsensusCommit.upsert (l.198-204) -> CrudHandler.put (l.682-703) -> putIntoWriteSet
    fn upsert(&mut self, k: u8, v: u8) {
        self.write_set.insert(k, v);
    }

    /// ConsensusCommit.commit / commitInternal (l.227-270)
    fn commit(&mut self) -> Rep {
        // CrudHandler.waitForRecoveryCompletionIfNecessary (l.787-797)
        for (k, f) in self.recovery_results.clone() {
            if self.write_set.contains_key(&k) || self.serializable {
                if let Some(rid) = f {
                    match self.io.wait(rid) {
                        Msg::Done { .. } => {}
                        m => panic!("completion expected, got {m:?}"),
                    }
                }
            }
        }
        self.commit_handler_commit()
    }

    /// CommitHandler.commit (l.155-231)
    fn commit_handler_commit(&mut self) -> Rep {
        let has_writes = !self.read_only && !self.write_set.is_empty();

        if has_writes && !self.prepare_records() {
            // PreparationConflictException
            self.abort_state();
            self.rollback_records();
            return Rep::Aborted;
        }

        if !self.validate_records() {
            // ValidationConflictException -> abortStateAndRollbackRecordsIfNeeded (l.241-250);
            // isCoordinatorStateWriteRequired (l.256-258) = has_writes (write omission on)
            if has_writes {
                self.abort_state();
                self.rollback_records();
            }
            return Rep::Aborted;
        }

        if has_writes {
            match self.commit_state() {
                Ok(committed_at) => self.commit_records(committed_at),
                Err(()) => {
                    // CommitConflictException
                    self.rollback_records();
                    return Rep::Aborted;
                }
            }
        }
        Rep::Committed
    }

    /// ParticipantCommitHandler.prepareRecords (l.76-102) with PrepareMutationComposer.add(Put)
    /// (l.47-84); ParallelExecutor.prepareRecords: parallel, stopOnError = false.
    fn prepare_records(&mut self) -> bool {
        let mut mutations = Vec::new();
        for (&k, &v) in &self.write_set {
            let result = self.read_set.get(&k).cloned().flatten(); // Snapshot.to (l.371-382)
            let mut cols = vec![
                (VALUE, Some(v as i64)),
                (ID, Some(self.tx)),
                (STATE, Some(PREPARED)),
                (PREPARED_AT, Some(0)),
            ];
            let cond;
            match result {
                Some(r) => {
                    // createBeforeColumns (l.123-141)
                    for c in 0..AFTER_COLS {
                        if c == VERSION && r[c].is_none() {
                            cols.push((BEFORE + VERSION, Some(0)));
                        } else {
                            cols.push((BEFORE + c, r[c]));
                        }
                    }
                    cols.push((VERSION, Some(r[VERSION].unwrap_or(0) + 1)));
                    cond = match r[ID] {
                        None => Cond::If(vec![(ID, None)]),
                        Some(id) => Cond::If(vec![(ID, Some(id))]),
                    };
                }
                None => {
                    cols.push((VERSION, Some(1)));
                    cond = Cond::PutIfNotExists;
                }
            }
            mutations.push((k, Mutation::Put { cols, cond }));
        }
        let oks = self.run_all_mutations(mutations);
        oks.iter().all(|&ok| ok)
    }

    /// A phase that runs every task and collects every reply (stopOnError = false, or a phase
    /// that cannot fail): send all, then collect all. The outcome does not depend on the order
    /// the replies are collected in, so they are collected in send order.
    fn run_all_mutations(&mut self, mutations: Vec<(u8, Mutation)>) -> Vec<bool> {
        let mut rids = Vec::new();
        for (k, m) in mutations {
            let s = self.s(k);
            rids.push(self.io.send(s, |from, rid| Msg::Mutate { from, rid, m }));
        }
        rids.into_iter().map(|rid| applied(self.io.wait(rid))).collect()
    }

    /// TransactionContext.isValidationRequired (l.77-100)
    fn validation_keys(&self) -> Vec<u8> {
        if !self.serializable {
            return Vec::new();
        }
        self.get_set.keys().copied().filter(|k| !self.write_set.contains_key(k)).collect()
    }

    /// ParticipantCommitHandler.validateRecords (l.104-118) -> Snapshot.toSerializable
    /// (l.640-696, Get part l.670-693) with validateGetResult (l.872-898); ParallelExecutor
    /// validateRecords: parallel, stopOnError = true, one-task shortcut (l.255-259).
    /// Returns false on an anti-dependency.
    fn validate_records(&mut self) -> bool {
        let keys = self.validation_keys();
        if keys.is_empty() {
            return true;
        }
        let mut pending: Vec<(u32, u8)> = Vec::new();
        for &k in &keys {
            let s = self.s(k);
            pending.push((self.io.send(s, |from, rid| Msg::Get { from, rid }), k));
        }
        while !pending.is_empty() {
            let rids: Vec<u32> = pending.iter().map(|p| p.0).collect();
            let m = if rids.len() == 1 { self.io.wait(rids[0]) } else { self.io.wait_any(&rids) };
            let Msg::Record { rid, rec: latest } = m else { panic!("record expected") };
            let pos = pending.iter().position(|p| p.0 == rid).expect("pending rid");
            let (_, k) = pending.remove(pos);
            let original = self.get_set.get(&k).cloned().flatten();
            // isChanged (l.1004-1017)
            let changed = match (&latest, &original) {
                (Some(l), Some(o)) => l[ID] != o[ID],
                (None, None) => false,
                _ => true,
            };
            if changed {
                // throwExceptionDueToAntiDependency; the other replies stay unreceived
                return false;
            }
        }
        true
    }

    /// CommitHandler.commitState (l.303-307) -> CoordinatorCommitHandler.commitState (l.50-67)
    fn commit_state(&mut self) -> Result<i64, ()> {
        let coord = self.setup.coord;
        if self.io.put_state(coord, self.tx, COMMITTED, 0) {
            return Ok(0);
        }
        // handleCommitConflict (l.76-167) with getNumAttempts() == 1
        match self.io.get_state(coord, self.tx) {
            Some(st) if st.state == ABORTED => Err(()),
            Some(st) => Ok(st.created_at),
            None => Err(()),
        }
    }

    /// CommitHandler.abortState (l.309-313) -> CoordinatorCommitHandler.abortState (l.173-195);
    /// its result is not used by CommitHandler.commit.
    fn abort_state(&mut self) -> i64 {
        let coord = self.setup.coord;
        if self.io.put_state(coord, self.tx, ABORTED, 0) {
            return ABORTED;
        }
        // readCoordinatorStateAfterAbortConflict (l.241-252)
        self.io.get_state(coord, self.tx).map(|s| s.state).unwrap_or(ABORTED)
    }

    /// ParticipantCommitHandler.commitRecords / doCommitRecords (l.120-140), inline (async off),
    /// CommitMutationComposer.add(Put) (l.39-50); failures ignored.
    fn commit_records(&mut self, committed_at: i64) {
        let ms: Vec<(u8, Mutation)> = self
            .write_set
            .keys()
            .map(|&k| (k, commit_mutation(self.tx, committed_at, PREPARED)))
            .collect();
        let _ = self.run_all_mutations(ms);
    }

    /// ParticipantCommitHandler.rollbackRecords / doRollbackRecords (l.142-173), inline.
    fn rollback_records(&mut self) {
        // readRecordsForRollback (toReadTasks l.274-285): no read fails, so every reply is
        // collected; send order.
        let keys: Vec<u8> = self.write_set.keys().copied().collect();
        let mut rids = Vec::new();
        for &k in &keys {
            let s = self.s(k);
            rids.push((self.io.send(s, |from, rid| Msg::Get { from, rid }), k));
        }
        let mut latest_records: BTreeMap<u8, Rec> = BTreeMap::new();
        for (rid, k) in rids {
            match self.io.wait(rid) {
                Msg::Record { rec: Some(r), .. } => {
                    latest_records.insert(k, r);
                }
                Msg::Record { rec: None, .. } => {}
                m => panic!("record expected, got {m:?}"),
            }
        }
        // RollbackMutationComposer.add (l.62-89) via Snapshot.to
        let mut ms = Vec::new();
        for &k in &keys {
            let result = self.read_set.get(&k).cloned().flatten();
            let latest = match result {
                Some(r) if r[ID] == Some(self.tx) => r,
                _ => match latest_records.get(&k) {
                    Some(l) if l[ID] == Some(self.tx) => *l,
                    _ => continue,
                },
            };
            ms.push((k, rollback_mutation(self.tx, &latest)));
        }
        let _ = self.run_all_mutations(ms);
        // tryRecoverRecordsBlockingWrites (l.202-235) for getKeysOfWritesNeverRead (l.244-252):
        // every client reads every key before writing, so there is no such key.
        assert!(
            keys.iter().all(|k| self.read_set.contains_key(k)),
            "a write never read is outside the instance"
        );
    }
}

fn handle(i: usize) {
    let setup = recv_setup();
    let client = setup.c[i - 1];
    let mut h = Handle {
        io: Io::new(),
        i,
        tx: setup.tx_ids[i - 1],
        serializable: setup.serializable,
        read_only: setup.read_only[i - 1],
        setup,
        read_set: BTreeMap::new(),
        get_set: BTreeMap::new(),
        write_set: BTreeMap::new(),
        recovery_results: Vec::new(),
    };
    loop {
        let req = recv_tagged_msg_block::<_, Req>(move |tid, t| tid == client && t.is_none());
        let rep = match req {
            Req::Begin => {
                h.begin();
                Rep::Begun
            }
            Req::Read(k) => match h.get(k) {
                Ok(Some(r)) => Rep::Value(k, r[VALUE].expect("value present") as u8),
                Ok(None) => panic!("record of k{k} absent: outside the instance"),
                Err(Conflict) => Rep::Aborted,
            },
            Req::Write(k, v) => {
                h.upsert(k, v);
                Rep::Written(k)
            }
            Req::Commit => h.commit(),
        };
        send_msg(client, rep);
    }
}

// ---------------------------------------------------------------------------------------------
// The program entry.
// ---------------------------------------------------------------------------------------------
#[derive(Clone)]
struct Inst {
    keys: u8,
    serializable: bool,
    writes: Vec<Vec<u8>>, // per client; c3 (read-only) has none
    read_only: Vec<bool>,
}

fn instance(args: &[String]) -> Inst {
    let pattern = args.first().map(String::as_str).unwrap_or("lost");
    let keys: u8 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(1);
    let serializable = match args.get(2).map(String::as_str).unwrap_or("snapshot") {
        "snapshot" => false,
        "serializable" => true,
        o => panic!("iso is snapshot or serializable, not {o}"),
    };
    let writes: Vec<Vec<u8>> = match pattern {
        "lost" => vec![vec![1], vec![1]],
        "skew" => vec![vec![1], vec![2]],
        "both" => vec![vec![1, 2], vec![1, 2]],
        "mixed" => vec![vec![1, 2], vec![2]],
        "mixedro" => vec![vec![1, 2], vec![2], vec![]],
        o => panic!("unknown pattern {o}"),
    };
    assert!(keys == 1 || keys == 2, "k is 1 or 2");
    assert!(keys == 2 || pattern == "lost", "k = 1 only with lost");
    let read_only = (0..writes.len()).map(|i| i == 2).collect();
    Inst { keys, serializable, writes, read_only }
}

fn visible(args: &[String]) -> Vec<String> {
    let n = instance(args).writes.len();
    (1..=n).map(|i| format!("c{i}")).collect()
}

fn spawn_named<F: FnOnce() + Send + 'static>(name: String, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(name).spawn(f).expect("spawn")
}

fn imp_program(inst: Inst) {
    let n = inst.writes.len();
    let kk = inst.keys as usize;
    let mut s = Vec::new();
    for k in 1..=kk {
        s.push(spawn_named(format!("s{k}"), storage).thread().id());
    }
    let coord = spawn_named("coord".to_string(), coordinator).thread().id();
    let mut x = Vec::new();
    for i in 1..=n {
        let mut row = Vec::new();
        for k in 1..=kk {
            row.push(spawn_named(format!("x{i}{k}"), move || recovery_task(i, k)).thread().id());
        }
        x.push(row);
    }
    let mut h = Vec::new();
    for i in 1..=n {
        h.push(spawn_named(format!("h{i}"), move || handle(i)).thread().id());
    }
    let mut clients = Vec::new();
    let mut c = Vec::new();
    for i in 1..=n {
        let hi = h[i - 1];
        let w = inst.writes[i - 1].clone();
        let keys = inst.keys;
        let jh = spawn_named(format!("c{i}"), move || client(i as u8, keys, &w, hi));
        c.push(jh.thread().id());
        clients.push(jh);
    }
    let setup = Setup {
        s: s.clone(),
        coord,
        x: x.clone(),
        h: h.clone(),
        c,
        serializable: inst.serializable,
        read_only: inst.read_only.clone(),
        tx_ids: (1..=n as i64).collect(),
    };
    let init = spawn_named("init".to_string(), move || {
        let mut all: Vec<ThreadId> = Vec::new();
        all.extend(setup.s.iter().copied());
        all.push(setup.coord);
        for row in &setup.x {
            all.extend(row.iter().copied());
        }
        all.extend(setup.h.iter().copied());
        for t in all {
            send_tagged_msg(t, SETUP_TAG, Msg::Setup(setup.clone()));
        }
    });
    // The main thread sends and receives nothing; it waits for the clients and init.
    init.join().unwrap();
    for jh in clients {
        jh.join().unwrap();
    }
}

fn entry(args: &[String]) -> Entry {
    let inst = instance(args);
    Arc::new(move || imp_program(inst.clone()))
}

fn main() -> ExitCode {
    traceforge::program::main(Program { visible: &visible, spec: &entry, imp: &entry })
}
