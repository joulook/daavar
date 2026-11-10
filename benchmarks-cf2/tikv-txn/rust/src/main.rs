//! TiKV distributed transactions, transliterated to TraceForge's Must API (`bench_tikv_port`).
//!
//! Client: tikv/client-rust 1b883bf (`client-src/src/transaction`, `client-src/src/request`).
//! Server: tikv/tikv 96033a2 (`tikv-src/src/storage/txn`, `src/storage/mvcc`,
//! `components/txn_types`, `components/concurrency_manager`).
//! Interface: plan/case-studies/tikv/INTERFACE-agents.md; source cut and rules A1-A6, T1-T6, P1-P2:
//! plan/case-studies/tikv/cut/README.md. Every changed line carries `// PORT:` and its reason.
//!
//! Processes: `c1..cm` (visible, the application), `t1..tm` (client-rust library), `x1..xm` (the
//! `tokio::spawn`ed `commit_secondary`), `pd` (integer TSO), `r1..rR` (regions), `init` (setup).
//! Arguments: `<scenario> <async> <onepc> <a> [fifo|bag] [correct|f1|f2]`.
#![allow(dead_code, clippy::too_many_arguments, clippy::type_complexity)]

use std::collections::{BTreeMap, BTreeSet};
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{self, Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ============================================================================================
// INTERFACE.md sections 3-5 (verbatim)
// ============================================================================================

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

// ============================================================================================
// Shared: keys, values, timestamps, the setup message, the invisible messages
// ============================================================================================

/// PORT: `Key = Vec<u8>` (and its keyspace encoding) becomes the key index 1..n (INTERFACE.md 4).
type Key = u8;
/// PORT: `Value = Vec<u8>` becomes the writer's index 1..m (INTERFACE.md 4).
type Value = u8;
/// PORT: `TimeStamp` / `pdpb::Timestamp` is the integer the TSO hands out (rule T1).
type Ts = u64;
const TS_MAX: Ts = u64::MAX;

/// PORT: rule T2. Every TTL predicate of the source is kept and or-ed with this constant.
const EVERY_TTL_EXPIRED: bool = true;

/// PORT: rule T1, `TimeStamp::physical()` (txn_types/src/timestamp.rs:37-39) and
/// `Timestamp::from_version(..).physical` (client timestamp.rs): `ts >> 18`, 0 for every
/// timestamp of the integer TSO.
fn physical(ts: Ts) -> i64 { (ts >> 18) as i64 }

const TAG_SETUP: u32 = 1;
const TAG_NET: u32 = 2;
const TAG_TRIGGER: u32 = 3;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Variant { Correct, F1, F2 }

/// The setup message of `init` (INTERFACE.md 2).
#[derive(Clone, Debug, PartialEq)]
struct Setup {
    cs: Vec<ThreadId>,
    ts: Vec<ThreadId>,
    xs: Vec<ThreadId>,
    pd: ThreadId,
    regions: Vec<ThreadId>,
    /// `layout[k-1]` = index of the region holding key k.
    layout: Vec<usize>,
    kinds: Vec<Kind>,
    async_commit: bool,
    one_pc: bool,
    a: u32,
    variant: Variant,
}

/// Every invisible message. PORT: gRPC / protobuf (kvproto) become these variants.
#[derive(Clone, Debug, PartialEq)]
enum Net {
    Setup(Box<Setup>),
    /// TSO request (`PdClient::get_timestamp`), carrying the sender.
    Tso(ThreadId),
    TsoReply(Ts),
    /// A kvrpcpb request to a region, carrying the sender (the gRPC connection).
    Req(ThreadId, Request),
    /// A kvrpcpb response, carrying the region index (the shard's slot).
    Rep(usize, Response),
    /// PORT: transaction.rs:1401-1405 `tokio::spawn(self.commit_secondary(commit_ts))`
    /// becomes this trigger of `xi` (cut README section 4).
    Trigger(Box<Trigger>),
    /// PORT: `ProcessResult::NextCommand` (S:commands/resolve_lock_readphase.rs:100-110): the
    /// scheduler schedules the next command as a new command (scheduler.rs, cut by rule A1);
    /// here a message from the region to itself, carrying the original sender to reply to.
    NextCommand(ThreadId, NextCommand),
}

/// The commands a command can schedule next (S:storage/txn/mod.rs `ProcessResult::NextCommand`).
#[derive(Clone, Debug, PartialEq)]
enum NextCommand {
    /// S:commands/resolve_lock.rs:30-63 `ResolveLock` { txn_status, scan_key, key_locks }.
    /// PORT: scan_key is always None (RESOLVE_LOCK_BATCH_SIZE and MAX_TXN_WRITE_SIZE are never
    /// reached).
    ResolveLock { txn_status: BTreeMap<Ts, Ts>, key_locks: Vec<(Key, Lock)> },
}

#[derive(Clone, Debug, PartialEq)]
struct Trigger {
    primary_key: Option<Key>,
    mutations: Vec<Mutation>,
    start_version: Ts,
    options: TransactionOptions,
    write_size: u64,
    commit_ts: Ts,
}

// --------------------------------------------------------------------------------------------
// kvrpcpb (PORT: protobuf messages; only the fields the cut reads or writes)
// --------------------------------------------------------------------------------------------

/// kvrpcpb::Op. PORT: only Put, Lock (buffer.rs:398) and PessimisticLock exist (rule A2).
#[derive(Clone, Copy, Debug, PartialEq)]
enum PbOp { Put, Lock, PessimisticLock }

#[derive(Clone, Debug, PartialEq)]
struct Mutation { op: PbOp, key: Key, value: Value }

#[derive(Clone, Debug, PartialEq)]
struct LockInfo {
    primary_lock: Key,
    lock_version: Ts,
    key: Key,
    lock_ttl: u64,
    txn_size: u64,
    lock_type: PbOp,
    lock_for_update_ts: Ts,
    use_async_commit: bool,
    min_commit_ts: Ts,
    secondaries: Vec<Key>,
    is_txn_file: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum WriteConflictReason { Unknown, Optimistic, PessimisticRetry, SelfRolledBack, RcCheckTs, LazyUniquenessCheck, NotLockedKeyConflict }

#[derive(Clone, Debug, PartialEq)]
struct WriteConflict { start_ts: Ts, conflict_ts: Ts, conflict_commit_ts: Ts, key: Key, primary: Key, reason: WriteConflictReason }

#[derive(Clone, Debug, PartialEq)]
struct CommitTsExpired { start_ts: Ts, attempted_commit_ts: Ts, key: Key, min_commit_ts: Ts }

#[derive(Clone, Debug, PartialEq)]
struct TxnNotFound { start_ts: Ts, primary_key: Key }

#[derive(Clone, Debug, PartialEq, Default)]
struct KeyError {
    locked: Option<LockInfo>,
    retryable: Option<String>,
    abort: Option<String>,
    conflict: Option<WriteConflict>,
    already_exist: Option<Key>,
    commit_ts_expired: Option<CommitTsExpired>,
    txn_not_found: Option<TxnNotFound>,
    commit_ts_too_large: Option<Ts>,
    primary_mismatch: Option<LockInfo>,
    txn_lock_not_found: Option<Key>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Action { NoAction, TtlExpireRollback, LockNotExistRollback, MinCommitTsPushed, TtlExpirePessimisticRollback, LockNotExistDoNothing }

#[derive(Clone, Copy, Debug, PartialEq)]
enum PessimisticAction { SkipPessimisticCheck, DoPessimisticCheck, DoConstraintCheck }

#[derive(Clone, Debug, PartialEq)]
struct GetRequest { key: Key, version: Ts }
#[derive(Clone, Debug, PartialEq)]
struct GetResponse { error: Option<KeyError>, value: Value, not_found: bool }

#[derive(Clone, Debug, PartialEq)]
struct PessimisticLockRequest {
    mutations: Vec<Mutation>, primary_lock: Key, start_version: Ts, lock_ttl: u64, for_update_ts: Ts,
    is_first_lock: bool, wait_timeout: i64, return_values: bool, min_commit_ts: Ts,
}
#[derive(Clone, Debug, PartialEq)]
struct PessimisticLockResponse { errors: Vec<KeyError>, values: Vec<Value>, not_founds: Vec<bool> }

#[derive(Clone, Debug, PartialEq)]
struct PrewriteRequest {
    mutations: Vec<Mutation>, primary_lock: Key, start_version: Ts, lock_ttl: u64, txn_size: u64,
    for_update_ts: Ts, pessimistic_actions: Vec<PessimisticAction>, use_async_commit: bool,
    secondaries: Vec<Key>, try_one_pc: bool, min_commit_ts: Ts, max_commit_ts: Ts,
}
#[derive(Clone, Debug, PartialEq)]
struct PrewriteResponse { errors: Vec<KeyError>, min_commit_ts: Ts, one_pc_commit_ts: Ts }

#[derive(Clone, Debug, PartialEq)]
struct CommitRequest { keys: Vec<Key>, start_version: Ts, commit_version: Ts }
#[derive(Clone, Debug, PartialEq)]
struct CommitResponse { error: Option<KeyError>, commit_version: Ts }

#[derive(Clone, Debug, PartialEq)]
struct BatchRollbackRequest { keys: Vec<Key>, start_version: Ts }
#[derive(Clone, Debug, PartialEq)]
struct BatchRollbackResponse { error: Option<KeyError> }

#[derive(Clone, Debug, PartialEq)]
struct PessimisticRollbackRequest { keys: Vec<Key>, start_version: Ts, for_update_ts: Ts }
#[derive(Clone, Debug, PartialEq)]
struct PessimisticRollbackResponse { errors: Vec<KeyError> }

#[derive(Clone, Debug, PartialEq)]
struct CheckTxnStatusRequest {
    primary_key: Key, lock_ts: Ts, caller_start_ts: Ts, current_ts: Ts, rollback_if_not_exist: bool,
    force_sync_commit: bool, resolving_pessimistic_lock: bool, verify_is_primary: bool, is_txn_file: bool,
}
#[derive(Clone, Debug, PartialEq)]
struct CheckTxnStatusResponse { error: Option<KeyError>, lock_ttl: u64, commit_version: Ts, action: Action, lock_info: Option<LockInfo> }

#[derive(Clone, Debug, PartialEq)]
struct CheckSecondaryLocksRequest { keys: Vec<Key>, start_version: Ts }
#[derive(Clone, Debug, PartialEq)]
struct CheckSecondaryLocksResponse { error: Option<KeyError>, locks: Vec<LockInfo>, commit_ts: Ts }

#[derive(Clone, Debug, PartialEq)]
struct ResolveLockRequest { start_version: Ts, commit_version: Ts, is_txn_file: bool }
#[derive(Clone, Debug, PartialEq)]
struct ResolveLockResponse { error: Option<KeyError> }

#[derive(Clone, Debug, PartialEq)]
enum Request {
    Get(GetRequest),
    PessimisticLock(PessimisticLockRequest),
    Prewrite(PrewriteRequest),
    Commit(CommitRequest),
    BatchRollback(BatchRollbackRequest),
    PessimisticRollback(PessimisticRollbackRequest),
    CheckTxnStatus(CheckTxnStatusRequest),
    CheckSecondaryLocks(CheckSecondaryLocksRequest),
    ResolveLock(ResolveLockRequest),
}

#[derive(Clone, Debug, PartialEq)]
enum Response {
    Get(GetResponse),
    PessimisticLock(PessimisticLockResponse),
    Prewrite(PrewriteResponse),
    Commit(CommitResponse),
    BatchRollback(BatchRollbackResponse),
    PessimisticRollback(PessimisticRollbackResponse),
    CheckTxnStatus(CheckTxnStatusResponse),
    CheckSecondaryLocks(CheckSecondaryLocksResponse),
    ResolveLock(ResolveLockResponse),
}

// ============================================================================================
// CLIENT (tikv/client-rust 1b883bf)
// ============================================================================================

// --- common::Error (the variants the cut constructs) ----------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum Error {
    KeyError(Box<KeyError>),
    MultipleKeyErrors(Vec<Error>),
    PessimisticLockError { inner: Box<Error>, success_keys: Vec<Key> },
    ResolveLockError(Vec<LockInfo>),
    TxnNotFound(TxnNotFound),
    OnePcFailure,
    StringError(String),
    ProtocolViolation { message: String },
    OperationAfterCommitError,
    UndeterminedError(Box<Error>),
}
type Result<T> = std::result::Result<T, Error>;

/// plan.rs:144-146 `is_response_error`.
fn is_response_error(e: &Error) -> bool { matches!(e, Error::KeyError(_)) }

/// transaction.rs `is_commit_outcome_unknown`. PORT: gRPC errors and UndeterminedResult cannot
/// occur (rule A6), so an outcome is never unknown.
fn is_commit_outcome_unknown(_e: &Error) -> bool { false }

// --- backoff.rs ------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq)]
enum BackoffKind { None, NoJitter }

#[derive(Clone, Debug, PartialEq)]
struct Backoff { kind: BackoffKind, current_attempts: u32, max_attempts: u32 }

impl Backoff {
    const fn no_jitter_backoff(_base_delay_ms: u64, _max_delay_ms: u64, max_attempts: u32) -> Backoff {
        Backoff { kind: BackoffKind::NoJitter, current_attempts: 0, max_attempts }
    }
    const fn no_backoff() -> Backoff { Backoff { kind: BackoffKind::None, current_attempts: 0, max_attempts: 0 } }
    /// backoff.rs:30-50. PORT: rule T4, the attempt bound is kept, `Some(())` stands for the
    /// delay, nobody sleeps.
    fn next_delay_duration(&mut self) -> Option<()> {
        if self.current_attempts >= self.max_attempts {
            return None;
        }
        self.current_attempts += 1;
        match self.kind {
            BackoffKind::None => None,
            BackoffKind::NoJitter => Some(()),
        }
    }
    fn is_none(&self) -> bool { self.kind == BackoffKind::None }
}

const DEFAULT_REGION_BACKOFF: Backoff = Backoff::no_jitter_backoff(2, 500, 10);
const OPTIMISTIC_BACKOFF: Backoff = Backoff::no_jitter_backoff(2, 500, 10);
const PESSIMISTIC_BACKOFF: Backoff = Backoff::no_jitter_backoff(2, 500, 10);

// --- request/mod.rs RetryOptions ---------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
struct RetryOptions { region_backoff: Backoff, lock_backoff: Backoff }
impl RetryOptions {
    const fn default_optimistic() -> RetryOptions { RetryOptions { region_backoff: DEFAULT_REGION_BACKOFF, lock_backoff: OPTIMISTIC_BACKOFF } }
    const fn default_pessimistic() -> RetryOptions { RetryOptions { region_backoff: DEFAULT_REGION_BACKOFF, lock_backoff: PESSIMISTIC_BACKOFF } }
}

// --- the PD client and the request plans (request/plan.rs, shard.rs) ------------------------

/// `Arc<impl PdClient>`. PORT: the region cache, the store lookups and the gRPC KvClient are
/// replaced by the setup message's layout and thread ids (rule A5).
#[derive(Clone)]
struct PdClient { me: ThreadId, setup: Arc<Setup> }

impl PdClient {
    /// `PdClient::get_timestamp`. PORT: one message to `pd` and its reply (rule T1).
    fn get_timestamp(&self) -> Result<Ts> {
        send_tagged_msg(self.setup.pd, TAG_NET, Net::Tso(self.me));
        let pd = self.setup.pd;
        let Net::TsoReply(ts) = recv_tagged_msg_block::<_, Net>(move |tid, tag| tag == Some(TAG_NET) && tid == pd) else {
            panic!("TsoReply expected")
        };
        Ok(ts)
    }

    /// `store_for_key` / `region_for_key`. PORT: the layout of the setup message (rule A5).
    fn region_for_key(&self, key: Key) -> usize { self.setup.layout[(key - 1) as usize] }

    /// The send half of `Dispatch::execute` (plan.rs:69-84). PORT: gRPC becomes a message.
    fn send(&self, region: usize, request: Request) {
        send_tagged_msg(self.setup.regions[region], TAG_NET, Net::Req(self.me, request));
    }

    /// The receive half of `Dispatch::execute`, for any of `regions`.
    fn recv_any(&self, regions: &[usize]) -> (usize, Response) {
        let tids: Vec<ThreadId> = regions.iter().map(|r| self.setup.regions[*r]).collect();
        let Net::Rep(r, resp) = recv_tagged_msg_block::<_, Net>(move |tid, tag| tag == Some(TAG_NET) && tids.contains(&tid)) else {
            panic!("a region reply expected")
        };
        (r, resp)
    }

    /// `Dispatch::execute` (plan.rs:69-84) to one region.
    fn dispatch(&self, region: usize, request: Request) -> Response {
        self.send(region, request);
        self.recv_any(&[region]).1
    }

    /// `region_stream_for_keys` (store.rs): sorted keys grouped by consecutive region.
    /// PORT: from the layout (rule A5); `batches(.., TXN_COMMIT_BATCH_SIZE)` keeps one batch per
    /// region (16 KB is never reached by the scenarios).
    fn group_by_region<T: Clone>(&self, items: Vec<T>, key_of: impl Fn(&T) -> Key) -> Vec<(Vec<T>, usize)> {
        let mut out: Vec<(Vec<T>, usize)> = Vec::new();
        for it in items {
            let r = self.region_for_key(key_of(&it));
            match out.last_mut() {
                Some((v, lr)) if *lr == r => v.push(it),
                _ => out.push((vec![it], r)),
            }
        }
        out
    }
}

/// `KvRequest` + `Shardable` + the response conversions (request/mod.rs, shard.rs).
trait KvRequest: Clone + Send + 'static {
    type Resp: HasLocks + HasKeyErrors + Clone + PartialEq + std::fmt::Debug + Send + 'static;
    type Shard: Clone + PartialEq + std::fmt::Debug + Send + 'static;
    fn shards(&self, pd: &PdClient) -> Vec<(Self::Shard, usize)>;
    fn apply_shard(&mut self, shard: Self::Shard);
    fn to_request(&self) -> Request;
    fn from_response(r: Response) -> Self::Resp;
}

/// transaction/lock.rs:796-800 `HasLocks`.
trait HasLocks { fn take_locks(&mut self) -> Vec<LockInfo> { Vec::new() } }

/// `HasKeyErrors` (C:store/errors.rs:16-18; the impls `has_key_error!` 66-84, 145-161 and
/// `extract_errors` 205-215).
trait HasKeyErrors { fn key_errors(&mut self) -> Option<Vec<Error>>; }

fn has_key_error(error: &mut Option<KeyError>) -> Option<Vec<Error>> {
    error.take().map(|e| vec![Error::KeyError(Box::new(e))])
}
fn extract_errors(errors: &mut Vec<KeyError>) -> Option<Vec<Error>> {
    if errors.is_empty() { None } else { Some(std::mem::take(errors).into_iter().map(|e| Error::KeyError(Box::new(e))).collect()) }
}

/// The `ResolveLock` plan's parameters (plan.rs:1246-1252).
#[derive(Clone)]
struct ResolveLockOpts { timestamp: Ts, backoff: Backoff }

/// `ResponseWithShard` (plan.rs:1585).
#[derive(Clone, Debug, PartialEq)]
struct ResponseWithShard<R, S>(R, S);

/// One shard task's result (`<RetryableMultiRegion as Plan>::Result` of one shard).
type ShardResult<Q> = Result<Vec<Result<ResponseWithShard<<Q as KvRequest>::Resp, <Q as KvRequest>::Shard>>>>;

/// `RetryableMultiRegion::execute` / `single_plan_handler` (plan.rs:375-451, 1042-1060), with
/// `ResolveLock` (plan.rs:1267-1308) as its inner plan when `resolve` is given, and the shard
/// preserved (`PreserveShard`, plan.rs:1563-1580; carried always, read only by the plans that
/// preserve it). Returns the per-shard results in shard order.
fn retry_multi_region<Q: KvRequest>(
    pd: &PdClient,
    plan: &Q,
    resolve: Option<&ResolveLockOpts>,
    preserve_region_results: bool,
) -> Result<Vec<Result<ResponseWithShard<Q::Resp, Q::Shard>>>> {
    let shards = plan.shards(pd);
    let shards_len = shards.len();
    let mut tasks = Vec::with_capacity(shards_len);
    for (shard, region) in shards {
        let clone = {
            let mut c = plan.clone(); // clone_then_apply_shard
            c.apply_shard(shard.clone());
            c
        };
        tasks.push((clone, shard, region));
    }
    let results: Vec<ShardResult<Q>> = if shards_len == 1 {
        // PORT: with one shard the JoinSet holds one task and the caller only awaits it
        // (plan.rs:398, 408), so the task runs in the caller's process (NOTES.md D1).
        let (clone, shard, region) = tasks.pop().unwrap();
        vec![single_shard_handler(pd, clone, shard, region, resolve)]
    } else {
        // PORT: plan.rs:398 `join_set.spawn` per shard becomes one invisible thread per shard
        // task, with its own address for the replies (cut README section 4); every task
        // runs its dispatch, its lock resolution and its re-dispatches independently of the
        // others. `collect_join_set_results` (plan.rs:108-153) awaits every task and puts the
        // results in shard order: here the joins, in shard order (the caller does nothing
        // between them, so the join order is not observable).
        let mut handles = Vec::with_capacity(shards_len);
        for (clone, shard, region) in tasks {
            let setup = pd.setup.clone();
            let resolve = resolve.cloned();
            handles.push(thread::spawn(move || {
                let shard_pd = PdClient { me: thread::current_id(), setup };
                single_shard_handler(&shard_pd, clone, shard, region, resolve.as_ref())
            }));
        }
        handles.into_iter().map(|h| h.join().expect("a shard task ended")).collect()
    };
    if preserve_region_results {
        Ok(results
            .into_iter()
            .flat_map(|x| match x {
                Ok(v) => v,
                Err(e) => vec![Err(e)],
            })
            .collect())
    } else {
        let mut oks = Vec::with_capacity(results.len());
        let mut first_err: Option<Error> = None;
        for r in results {
            match r {
                Ok(v) => oks.push(v),
                Err(e) if first_err.is_none() => first_err = Some(e),
                Err(_) => {}
            }
        }
        if let Some(e) = first_err {
            return Err(e);
        }
        Ok(oks.into_iter().flatten().collect())
    }
}

/// `single_shard_handler` (plan.rs:679ff) of one shard: the candidate round executes the inner
/// plan (`ResolveLock` over `Dispatch`, or `Dispatch` alone), then `handle_candidate_response`
/// (plan.rs:806-812) turns key errors into the shard's `MultipleKeyErrors`. PORT: no region
/// error is ever produced (rule A1), so the region-error branches and retries are cut.
fn single_shard_handler<Q: KvRequest>(pd: &PdClient, clone: Q, shard: Q::Shard, region: usize, resolve: Option<&ResolveLockOpts>) -> ShardResult<Q> {
    let executed = match resolve {
        Some(opts) => resolve_lock_execute(pd, &clone, region, opts),
        None => Ok(Q::from_response(pd.dispatch(region, clone.to_request()))),
    };
    // CandidateRoundResult::OtherError(error) => return Err(error) (plan.rs:729).
    executed.map(|mut response| match response.key_errors() {
        Some(error) => vec![Err(Error::MultipleKeyErrors(error))],
        None => vec![Ok(ResponseWithShard(response, shard))],
    })
}

/// `ResolveLock::execute` (plan.rs:1273-1308).
fn resolve_lock_execute<Q: KvRequest>(pd: &PdClient, req: &Q, region: usize, opts: &ResolveLockOpts) -> Result<Q::Resp> {
    let mut result = Q::from_response(pd.dispatch(region, req.to_request()));
    let mut clone_backoff = opts.backoff.clone();
    loop {
        let locks = result.take_locks();
        if locks.is_empty() {
            return Ok(result);
        }
        if opts.backoff.is_none() {
            return Err(Error::ResolveLockError(locks));
        }
        let live_locks = resolve_locks(locks, opts.timestamp, pd)?;
        if live_locks.is_empty() {
            result = Q::from_response(pd.dispatch(region, req.to_request()));
        } else {
            match clone_backoff.next_delay_duration() {
                None => return Err(Error::ResolveLockError(live_locks)),
                Some(()) => {
                    // PORT: `sleep(delay_duration)` cut (rule T4).
                    result = Q::from_response(pd.dispatch(region, req.to_request()));
                }
            }
        }
    }
}

/// `ExtractError::execute` (plan.rs:1505-1540) over a `Vec<Result<_>>`; `HasKeyErrors for
/// Vec<T>` / `Result<T, Error>` (C:store/errors.rs:163-186) report the first shard's error.
fn extract_error<T>(result: Vec<Result<T>>) -> Result<Vec<T>> {
    let mut oks = Vec::with_capacity(result.len());
    for r in result {
        match r {
            Ok(t) => oks.push(t),
            Err(Error::MultipleKeyErrors(errors)) => return Err(Error::MultipleKeyErrors(errors)),
            Err(e) if is_response_error(&e) => return Err(Error::MultipleKeyErrors(vec![e])),
            Err(e) => return Err(e),
        }
    }
    Ok(oks)
}

/// `CollectSingle` (plan.rs:1194-1207).
fn collect_single<T>(mut input: Vec<Result<T>>) -> Result<T> {
    assert!(input.len() == 1);
    input.pop().unwrap()
}

/// `CollectError` (plan.rs:1216-1222).
fn collect_error<T>(input: Vec<Result<T>>) -> Result<Vec<T>> { input.into_iter().collect() }

/// `PlanBuilder::single_region_with_store(..).extract_error()` (plan_builder.rs:301-328) for a
/// non-shardable request (ResolveLock).
fn single_region_extract_error<R: HasKeyErrors>(pd: &PdClient, region: usize, request: Request, conv: impl Fn(Response) -> R) -> Result<R> {
    let mut r = conv(pd.dispatch(region, request));
    match r.key_errors() {
        Some(errors) => Err(Error::MultipleKeyErrors(errors)),
        None => Ok(r),
    }
}

// --- transaction/requests.rs (request types, sharding, HasLocks, merges) ---------------------

fn shard_single_key(pd: &PdClient, key: Key) -> Vec<(Vec<Key>, usize)> { vec![(vec![key], pd.region_for_key(key))] }
fn shard_keys(pd: &PdClient, keys: &[Key]) -> Vec<(Vec<Key>, usize)> {
    let mut keys = keys.to_vec();
    keys.sort();
    pd.group_by_region(keys, |k| *k)
}

macro_rules! unwrap_resp {
    ($r:expr, $v:ident) => {
        match $r { Response::$v(x) => x, other => panic!("unexpected response {other:?}") }
    };
}

/// requests.rs:95-100 `new_get_request`.
fn new_get_request(key: Key, timestamp: Ts) -> GetRequest { GetRequest { key, version: timestamp } }
impl KvRequest for GetRequest {
    type Resp = GetResponse;
    type Shard = Vec<Key>;
    fn shards(&self, pd: &PdClient) -> Vec<(Vec<Key>, usize)> { shard_single_key(pd, self.key) }
    fn apply_shard(&mut self, mut shard: Vec<Key>) { self.key = shard.pop().unwrap(); }
    fn to_request(&self) -> Request { Request::Get(self.clone()) }
    fn from_response(r: Response) -> GetResponse { unwrap_resp!(r, Get) }
}
/// requests.rs:110-120 `Process<GetResponse> for DefaultProcessor`.
fn process_get_response(input: Result<GetResponse>) -> Result<Option<Value>> {
    let input = input?;
    Ok(if input.not_found { None } else { Some(input.value) })
}

/// requests.rs:192-202.
fn new_resolve_lock_request(start_version: Ts, commit_version: Ts, is_txn_file: bool) -> ResolveLockRequest {
    ResolveLockRequest { start_version, commit_version, is_txn_file }
}

/// requests.rs:218-233.
fn new_prewrite_request(mutations: Vec<Mutation>, primary_lock: Key, start_version: Ts, lock_ttl: u64) -> PrewriteRequest {
    PrewriteRequest {
        mutations, primary_lock, start_version, lock_ttl,
        // FIXME: Lite resolve lock is currently disabled
        txn_size: u64::MAX,
        for_update_ts: 0, pessimistic_actions: vec![], use_async_commit: false, secondaries: vec![],
        try_one_pc: false, min_commit_ts: 0, max_commit_ts: 0,
    }
}
/// requests.rs:235-248.
fn new_pessimistic_prewrite_request(mutations: Vec<Mutation>, primary_lock: Key, start_version: Ts, lock_ttl: u64, for_update_ts: Ts) -> PrewriteRequest {
    let len = mutations.len();
    let mut req = new_prewrite_request(mutations, primary_lock, start_version, lock_ttl);
    req.for_update_ts = for_update_ts;
    req.pessimistic_actions = std::iter::repeat_n(PessimisticAction::DoPessimisticCheck, len).collect();
    req
}
/// requests.rs:254-294 `Shardable for PrewriteRequest`.
impl KvRequest for PrewriteRequest {
    type Resp = PrewriteResponse;
    type Shard = Vec<Mutation>;
    fn shards(&self, pd: &PdClient) -> Vec<(Vec<Mutation>, usize)> {
        let mut mutations = self.mutations.clone();
        mutations.sort_by(|a, b| a.key.cmp(&b.key));
        pd.group_by_region(mutations, |m| m.key)
    }
    fn apply_shard(&mut self, shard: Vec<Mutation>) {
        // Only need to set secondary keys if we're sending the primary key.
        if self.use_async_commit && !self.mutations.iter().any(|m| m.key == self.primary_lock) {
            self.secondaries = vec![];
        }
        // Only if there is only one request to send
        if self.try_one_pc && shard.len() != self.secondaries.len() + 1 {
            self.try_one_pc = false;
        }
        self.mutations = shard;
    }
    fn to_request(&self) -> Request { Request::Prewrite(self.clone()) }
    fn from_response(r: Response) -> PrewriteResponse { unwrap_resp!(r, Prewrite) }
}

/// requests.rs:306-317.
fn new_commit_request(keys: Vec<Key>, start_version: Ts, commit_version: Ts) -> CommitRequest {
    CommitRequest { keys, start_version, commit_version }
}
impl KvRequest for CommitRequest {
    type Resp = CommitResponse;
    type Shard = Vec<Key>;
    fn shards(&self, pd: &PdClient) -> Vec<(Vec<Key>, usize)> { shard_keys(pd, &self.keys) }
    fn apply_shard(&mut self, shard: Vec<Key>) { self.keys = shard; }
    fn to_request(&self) -> Request { Request::Commit(self.clone()) }
    fn from_response(r: Response) -> CommitResponse { unwrap_resp!(r, Commit) }
}

/// requests.rs:362-371.
fn new_batch_rollback_request(keys: Vec<Key>, start_version: Ts) -> BatchRollbackRequest { BatchRollbackRequest { keys, start_version } }
impl KvRequest for BatchRollbackRequest {
    type Resp = BatchRollbackResponse;
    type Shard = Vec<Key>;
    fn shards(&self, pd: &PdClient) -> Vec<(Vec<Key>, usize)> { shard_keys(pd, &self.keys) }
    fn apply_shard(&mut self, shard: Vec<Key>) { self.keys = shard; }
    fn to_request(&self) -> Request { Request::BatchRollback(self.clone()) }
    fn from_response(r: Response) -> BatchRollbackResponse { unwrap_resp!(r, BatchRollback) }
}

/// requests.rs:379-390.
fn new_pessimistic_rollback_request(keys: Vec<Key>, start_version: Ts, for_update_ts: Ts) -> PessimisticRollbackRequest {
    PessimisticRollbackRequest { keys, start_version, for_update_ts }
}
impl KvRequest for PessimisticRollbackRequest {
    type Resp = PessimisticRollbackResponse;
    type Shard = Vec<Key>;
    fn shards(&self, pd: &PdClient) -> Vec<(Vec<Key>, usize)> { shard_keys(pd, &self.keys) }
    fn apply_shard(&mut self, shard: Vec<Key>) { self.keys = shard; }
    fn to_request(&self) -> Request { Request::PessimisticRollback(self.clone()) }
    fn from_response(r: Response) -> PessimisticRollbackResponse { unwrap_resp!(r, PessimisticRollback) }
}

/// requests.rs:398-420 (via lowering.rs:140-161).
fn new_pessimistic_lock_request(mutations: Vec<Mutation>, primary_lock: Key, start_version: Ts, lock_ttl: u64, for_update_ts: Ts, need_value: bool) -> PessimisticLockRequest {
    PessimisticLockRequest {
        mutations, primary_lock, start_version, lock_ttl, for_update_ts,
        // FIXME: make them configurable
        is_first_lock: false,
        wait_timeout: 0,
        return_values: need_value,
        // FIXME: support large transaction
        min_commit_ts: 0,
    }
}
/// requests.rs:426-444.
impl KvRequest for PessimisticLockRequest {
    type Resp = PessimisticLockResponse;
    type Shard = Vec<Mutation>;
    fn shards(&self, pd: &PdClient) -> Vec<(Vec<Mutation>, usize)> {
        let mut mutations = self.mutations.clone();
        mutations.sort_by(|a, b| a.key.cmp(&b.key));
        pd.group_by_region(mutations, |m| m.key)
    }
    fn apply_shard(&mut self, shard: Vec<Mutation>) { self.mutations = shard; }
    fn to_request(&self) -> Request { Request::PessimisticLock(self.clone()) }
    fn from_response(r: Response) -> PessimisticLockResponse { unwrap_resp!(r, PessimisticLock) }
}

/// `KvPair`.
#[derive(Clone, Debug, PartialEq)]
struct KvPair(Key, Value);

/// requests.rs:448-500 `Merge<ResponseWithShard<PessimisticLockResponse, _>> for CollectWithShard`.
fn collect_with_shard(input: Vec<Result<ResponseWithShard<PessimisticLockResponse, Vec<Mutation>>>>) -> Result<Vec<KvPair>> {
    if input.iter().any(|r| r.is_err()) {
        let (success, mut errors): (Vec<_>, Vec<_>) = input.into_iter().partition(|r| r.is_ok());
        let first_err = errors.pop().unwrap();
        let success_keys = success
            .into_iter()
            .map(|r| r.unwrap())
            .flat_map(|ResponseWithShard(_resp, mutations)| mutations.into_iter().map(|m| m.key))
            .collect();
        Err(Error::PessimisticLockError { inner: Box::new(first_err.unwrap_err()), success_keys })
    } else {
        let mut out = Vec::new();
        for ResponseWithShard(resp, mutations) in input.into_iter().map(|r| r.unwrap()) {
            let values: Vec<Value> = resp.values;
            let values_len = values.len();
            let not_founds = resp.not_founds;
            let kvpairs: Vec<KvPair> = mutations.into_iter().map(|m| m.key).zip(values).map(|(k, v)| KvPair(k, v)).collect();
            assert_eq!(kvpairs.len(), values_len);
            if not_founds.is_empty() {
                // PORT: "empty value" of the legacy protocol: a value is never empty here (the
                // value 0 does not exist, INTERFACE.md 4), so every pair is kept.
                out.extend(kvpairs);
            } else {
                assert_eq!(kvpairs.len(), not_founds.len());
                out.extend(kvpairs.into_iter().zip(not_founds).filter_map(|(kvpair, not_found)| if not_found { None } else { Some(kvpair) }));
            }
        }
        Ok(out)
    }
}

/// requests.rs:631-652.
fn new_check_txn_status_request(primary_key: Key, lock_ts: Ts, caller_start_ts: Ts, current_ts: Ts, rollback_if_not_exist: bool, force_sync_commit: bool, resolving_pessimistic_lock: bool, is_txn_file: bool) -> CheckTxnStatusRequest {
    CheckTxnStatusRequest {
        primary_key, lock_ts, caller_start_ts, current_ts, rollback_if_not_exist, force_sync_commit,
        resolving_pessimistic_lock, verify_is_primary: true, is_txn_file,
    }
}
impl KvRequest for CheckTxnStatusRequest {
    type Resp = CheckTxnStatusResponse;
    type Shard = Vec<Key>;
    fn shards(&self, pd: &PdClient) -> Vec<(Vec<Key>, usize)> { shard_single_key(pd, self.primary_key) }
    fn apply_shard(&mut self, mut shard: Vec<Key>) {
        assert!(shard.len() == 1);
        self.primary_key = shard.pop().unwrap();
    }
    fn to_request(&self) -> Request { Request::CheckTxnStatus(self.clone()) }
    fn from_response(r: Response) -> CheckTxnStatusResponse { unwrap_resp!(r, CheckTxnStatus) }
}

/// requests.rs:701-795 `TransactionStatus`.
#[derive(Clone, Debug, PartialEq)]
struct TransactionStatus { kind: TransactionStatusKind, action: Action, is_expired: bool }

#[derive(Clone, Debug, PartialEq)]
enum TransactionStatusKind { Committed(Ts), RolledBack, Locked(u64, LockInfo) }

impl TransactionStatus {
    /// requests.rs:708-716 `From<CheckTxnStatusResponse>`.
    fn from_response(mut resp: CheckTxnStatusResponse) -> TransactionStatus {
        TransactionStatus { action: resp.action, kind: TransactionStatusKind::from_tuple(resp.commit_version, resp.lock_ttl, resp.lock_info.take()), is_expired: false }
    }
    fn from_commit_version(commit_version: Ts) -> Self {
        let kind = if commit_version == 0 { TransactionStatusKind::RolledBack } else { TransactionStatusKind::Committed(commit_version) };
        TransactionStatus { kind, action: Action::NoAction, is_expired: false }
    }
    /// requests.rs:732-741.
    fn check_ttl(&mut self, current: Ts) {
        if let TransactionStatusKind::Locked(ref ttl, ref lock_info) = self.kind {
            // PORT: rule T2 (the lock counts as expired).
            if physical(current) - physical(lock_info.lock_version) >= *ttl as i64 || EVERY_TTL_EXPIRED {
                self.is_expired = true
            }
        }
    }
    fn is_determined(&self) -> bool {
        match self.kind {
            TransactionStatusKind::Committed(..) => true,
            TransactionStatusKind::RolledBack => matches!(self.action, Action::NoAction | Action::LockNotExistRollback | Action::TtlExpireRollback),
            TransactionStatusKind::Locked(..) => false,
        }
    }
    fn is_cacheable(&self) -> bool { self.is_determined() }
}
impl TransactionStatusKind {
    /// requests.rs:782-791.
    fn from_tuple(ts: Ts, ttl: u64, info: Option<LockInfo>) -> TransactionStatusKind {
        match (ts, ttl, info) {
            (0, 0, None) => TransactionStatusKind::RolledBack,
            (ts, 0, None) => TransactionStatusKind::Committed(ts),
            (0, ttl, Some(info)) => TransactionStatusKind::Locked(ttl, info),
            _ => unreachable!(),
        }
    }
}

/// requests.rs:794-802.
fn new_check_secondary_locks_request(keys: Vec<Key>, start_version: Ts) -> CheckSecondaryLocksRequest { CheckSecondaryLocksRequest { keys, start_version } }
impl KvRequest for CheckSecondaryLocksRequest {
    type Resp = CheckSecondaryLocksResponse;
    type Shard = Vec<Key>;
    fn shards(&self, pd: &PdClient) -> Vec<(Vec<Key>, usize)> { shard_keys(pd, &self.keys) }
    fn apply_shard(&mut self, shard: Vec<Key>) { self.keys = shard; }
    fn to_request(&self) -> Request { Request::CheckSecondaryLocks(self.clone()) }
    fn from_response(r: Response) -> CheckSecondaryLocksResponse { unwrap_resp!(r, CheckSecondaryLocks) }
}

/// requests.rs:813-880 `CollectSecondaryLocks`.
fn collect_secondary_locks(start_version: Ts, input: Vec<Result<ResponseWithShard<CheckSecondaryLocksResponse, Vec<Key>>>>) -> Result<SecondaryLocksStatus> {
    let mut out = SecondaryLocksStatus { commit_ts: None, min_commit_ts: 0, fallback_2pc: false };
    for resp in input {
        let ResponseWithShard(resp, requested_keys) = resp?;
        if resp.locks.len() > requested_keys.len() {
            return Err(Error::ProtocolViolation { message: "CheckSecondaryLocks returned more locks than requested keys".to_string() });
        }
        for lock in &resp.locks {
            if lock.lock_version != start_version {
                return Err(Error::ProtocolViolation { message: "CheckSecondaryLocks returned a lock of another transaction".to_string() });
            }
            if !lock.use_async_commit {
                out.fallback_2pc = true;
            }
            out.min_commit_ts = std::cmp::max(out.min_commit_ts, lock.min_commit_ts);
        }
        if resp.locks.len() < requested_keys.len() {
            if out.commit_ts.is_some_and(|ts| ts != resp.commit_ts) {
                return Err(Error::ProtocolViolation { message: "CheckSecondaryLocks reported conflicting commit TS".to_string() });
            }
            out.commit_ts = Some(resp.commit_ts);
        }
    }
    Ok(out)
}

#[derive(Clone, Debug)]
struct SecondaryLocksStatus { commit_ts: Option<Ts>, min_commit_ts: Ts, fallback_2pc: bool }
impl SecondaryLocksStatus {
    /// requests.rs:906-924.
    fn resolved_commit_version(&self, primary_min_commit_ts: Ts) -> Result<Ts> {
        let min_commit_ts = std::cmp::max(primary_min_commit_ts, self.min_commit_ts);
        let commit_version = self.commit_ts.unwrap_or(min_commit_ts);
        if commit_version != 0 && commit_version < min_commit_ts {
            return Err(Error::ProtocolViolation { message: "CheckSecondaryLocks reported commit TS below a surviving lock's min_commit_ts".to_string() });
        }
        Ok(commit_version)
    }
}

// requests.rs:926-962: HasLocks (error_locks! / the `errors` impls); HasKeyErrors.
impl HasLocks for GetResponse { fn take_locks(&mut self) -> Vec<LockInfo> { self.error.as_mut().and_then(|e| e.locked.take()).into_iter().collect() } }
impl HasLocks for ResolveLockResponse { fn take_locks(&mut self) -> Vec<LockInfo> { self.error.as_mut().and_then(|e| e.locked.take()).into_iter().collect() } }
impl HasLocks for CommitResponse { fn take_locks(&mut self) -> Vec<LockInfo> { self.error.as_mut().and_then(|e| e.locked.take()).into_iter().collect() } }
impl HasLocks for BatchRollbackResponse { fn take_locks(&mut self) -> Vec<LockInfo> { self.error.as_mut().and_then(|e| e.locked.take()).into_iter().collect() } }
impl HasLocks for CheckTxnStatusResponse { fn take_locks(&mut self) -> Vec<LockInfo> { self.error.as_mut().and_then(|e| e.locked.take()).into_iter().collect() } }
impl HasLocks for CheckSecondaryLocksResponse { fn take_locks(&mut self) -> Vec<LockInfo> { self.error.as_mut().and_then(|e| e.locked.take()).into_iter().collect() } }
impl HasLocks for PessimisticRollbackResponse { fn take_locks(&mut self) -> Vec<LockInfo> { self.errors.iter_mut().filter_map(|e| e.locked.take()).collect() } }
impl HasLocks for PessimisticLockResponse { fn take_locks(&mut self) -> Vec<LockInfo> { self.errors.iter_mut().filter_map(|e| e.locked.take()).collect() } }
impl HasLocks for PrewriteResponse { fn take_locks(&mut self) -> Vec<LockInfo> { self.errors.iter_mut().filter_map(|e| e.locked.take()).collect() } }

impl HasKeyErrors for GetResponse { fn key_errors(&mut self) -> Option<Vec<Error>> { has_key_error(&mut self.error) } }
impl HasKeyErrors for ResolveLockResponse { fn key_errors(&mut self) -> Option<Vec<Error>> { has_key_error(&mut self.error) } }
impl HasKeyErrors for CommitResponse { fn key_errors(&mut self) -> Option<Vec<Error>> { has_key_error(&mut self.error) } }
impl HasKeyErrors for BatchRollbackResponse { fn key_errors(&mut self) -> Option<Vec<Error>> { has_key_error(&mut self.error) } }
impl HasKeyErrors for CheckTxnStatusResponse { fn key_errors(&mut self) -> Option<Vec<Error>> { has_key_error(&mut self.error) } }
impl HasKeyErrors for CheckSecondaryLocksResponse { fn key_errors(&mut self) -> Option<Vec<Error>> { has_key_error(&mut self.error) } }
impl HasKeyErrors for PessimisticRollbackResponse { fn key_errors(&mut self) -> Option<Vec<Error>> { extract_errors(&mut self.errors) } }
impl HasKeyErrors for PessimisticLockResponse { fn key_errors(&mut self) -> Option<Vec<Error>> { extract_errors(&mut self.errors) } }
impl HasKeyErrors for PrewriteResponse { fn key_errors(&mut self) -> Option<Vec<Error>> { extract_errors(&mut self.errors) } }

// --- transaction/lock.rs -------------------------------------------------------------------

/// lock.rs:47-67 `reject_shared_locks`. PORT: shared locks are cut (rule A2): no lock is shared.
fn reject_shared_locks(_locks: &[LockInfo]) -> Result<()> { Ok(()) }

/// lock.rs:86-151 `resolve_locks`.
fn resolve_locks(locks: Vec<LockInfo>, timestamp: Ts, pd_client: &PdClient) -> Result<Vec<LockInfo> /* live_locks */> {
    reject_shared_locks(&locks)?;
    let ts = pd_client.get_timestamp()?;
    let caller_start_ts = timestamp;
    let current_ts = ts;

    let mut live_locks = Vec::new();
    let mut lock_resolver = LockResolver::new(ResolveLocksContext::default());
    // PORT: `RegionVerId` becomes the region index (rule A1: no epochs).
    let mut clean_regions: BTreeMap<Ts, BTreeSet<usize>> = BTreeMap::new();
    for lock in locks {
        let (commit_version, keys) = match lock_resolver.action_for_lock(&lock, caller_start_ts, current_ts, pd_client)? {
            LockAction::Resolve { commit_version, keys } => (commit_version, keys),
            LockAction::Live(primary_lock) => {
                live_locks.push(primary_lock);
                continue;
            }
            LockAction::Done => continue,
        };
        let regions = clean_regions.entry(lock.lock_version).or_default();
        for key in keys.iter().chain([&lock.key]) {
            resolve_lock_with_retry(*key, lock.lock_version, commit_version, lock.is_txn_file, pd_client, OPTIMISTIC_BACKOFF, regions)?;
        }
    }
    Ok(live_locks)
}

enum LockAction {
    Resolve { commit_version: Ts, keys: Vec<Key> },
    Live(LockInfo),
    Done,
}

impl LockAction {
    fn from_status(status: &TransactionStatus) -> Self {
        match &status.kind {
            TransactionStatusKind::Committed(ts) => LockAction::Resolve { commit_version: *ts, keys: Vec::new() },
            TransactionStatusKind::RolledBack => LockAction::Resolve { commit_version: 0, keys: Vec::new() },
            TransactionStatusKind::Locked(_, primary_lock) => LockAction::Live(primary_lock.clone()),
        }
    }
}

/// lock.rs:189-217 `pessimistic_rollback_lock`.
fn pessimistic_rollback_lock(lock: &LockInfo, pd_client: &PdClient) -> Result<()> {
    if lock.key == lock.primary_lock {
        return Ok(());
    }
    let for_update_ts = if lock.lock_for_update_ts == 0 { u64::MAX } else { lock.lock_for_update_ts };
    let req = new_pessimistic_rollback_request(vec![lock.key], lock.lock_version, for_update_ts);
    // .retry_multi_region(DEFAULT_REGION_BACKOFF).extract_error()
    let r = retry_multi_region(pd_client, &req, None, false)?;
    extract_error(r)?;
    Ok(())
}

/// lock.rs:223-317 `resolve_lock_with_retry`.
fn resolve_lock_with_retry(key: Key, start_version: Ts, commit_version: Ts, is_txn_file: bool, pd_client: &PdClient, _backoff: Backoff, clean_regions: &mut BTreeSet<usize>) -> Result<()> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        let _ = attempt;
        let ver_id = pd_client.region_for_key(key); // store_for_key; PORT: rule A5
        if clean_regions.contains(&ver_id) {
            return Ok(());
        }
        let request = new_resolve_lock_request(start_version, commit_version, is_txn_file);
        // PORT: LeaderNotFound, region errors and gRPC errors cannot occur (rules A1, A6).
        match single_region_extract_error(pd_client, ver_id, Request::ResolveLock(request), |r| unwrap_resp!(r, ResolveLock)) {
            Ok(_) => {
                clean_regions.insert(ver_id);
                return Ok(());
            }
            Err(Error::MultipleKeyErrors(mut errors)) => match errors.pop() {
                Some(Error::KeyError(key_err)) => return Err(Error::KeyError(key_err)),
                Some(e) => return Err(e),
                None => unreachable!(),
            },
            Err(e) => return Err(e),
        }
    }
}

/// lock.rs:320-324.
#[derive(Default, Clone)]
struct ResolveLocksContext { resolved: BTreeMap<Ts, TransactionStatus> }
impl ResolveLocksContext {
    fn get_resolved(&self, txn_id: Ts) -> Option<TransactionStatus> { self.resolved.get(&txn_id).cloned() }
    fn save_resolved(&mut self, txn_id: Ts, txn_status: TransactionStatus) { self.resolved.insert(txn_id, txn_status); }
}

struct LockResolver { ctx: ResolveLocksContext }

impl LockResolver {
    fn new(ctx: ResolveLocksContext) -> Self { Self { ctx } }

    /// lock.rs:381-482.
    fn action_for_lock(&mut self, lock: &LockInfo, caller_start_ts: Ts, current_ts: Ts, pd_client: &PdClient) -> Result<LockAction> {
        let status = match self.get_txn_status_from_lock(OPTIMISTIC_BACKOFF, lock, caller_start_ts, current_ts, false, pd_client) {
            Ok(status) => status,
            Err(Error::KeyError(key_err)) if key_err.primary_mismatch.is_some() && lock.lock_type == PbOp::PessimisticLock => {
                pessimistic_rollback_lock(lock, pd_client)?;
                return Ok(LockAction::Done);
            }
            Err(err) => return Err(err),
        };

        let primary_lock = match &status.kind {
            TransactionStatusKind::Locked(_, primary_lock) if status.is_expired && primary_lock.use_async_commit => primary_lock.clone(),
            TransactionStatusKind::Locked(..) => return Ok(LockAction::from_status(&status)),
            _ if lock.lock_type == PbOp::PessimisticLock => {
                pessimistic_rollback_lock(lock, pd_client)?;
                return Ok(LockAction::Done);
            }
            _ => return Ok(LockAction::from_status(&status)),
        };

        let secondary_status = self.check_all_secondaries(pd_client, primary_lock.secondaries.clone(), lock.lock_version)?;
        if secondary_status.fallback_2pc {
            let status = self.get_txn_status_from_lock(OPTIMISTIC_BACKOFF, lock, caller_start_ts, current_ts, true, pd_client)?;
            return Ok(LockAction::from_status(&status));
        }

        let commit_version = secondary_status.resolved_commit_version(primary_lock.min_commit_ts)?;
        self.ctx.save_resolved(lock.lock_version, TransactionStatus::from_commit_version(commit_version));
        let mut keys = primary_lock.secondaries.clone();
        keys.push(primary_lock.key);
        Ok(LockAction::Resolve { commit_version, keys })
    }

    /// lock.rs:615-689.
    fn check_txn_status(&mut self, pd_client: &PdClient, txn_id: Ts, primary: Key, caller_start_ts: Ts, current_ts: Ts, rollback_if_not_exist: bool, force_sync_commit: bool, resolving_pessimistic_lock: bool, is_txn_file: bool) -> Result<TransactionStatus> {
        if let Some(txn_status) = self.ctx.get_resolved(txn_id) {
            return Ok(txn_status);
        }
        let req = new_check_txn_status_request(primary, txn_id, caller_start_ts, current_ts, rollback_if_not_exist, force_sync_commit, resolving_pessimistic_lock, is_txn_file);
        // .retry_multi_region(DEFAULT_REGION_BACKOFF).merge(CollectSingle).extract_error().post_process_default()
        let executed = retry_multi_region(pd_client, &req, None, false).and_then(|v| collect_single(v)).map(|ResponseWithShard(r, _)| r).map(TransactionStatus::from_response);
        let mut status: TransactionStatus = match executed {
            Ok(status) => status,
            Err(Error::MultipleKeyErrors(mut errors)) => match errors.pop() {
                Some(Error::KeyError(key_err)) => {
                    if let Some(txn_not_found) = key_err.txn_not_found {
                        return Err(Error::TxnNotFound(txn_not_found));
                    }
                    return Err(Error::KeyError(key_err));
                }
                Some(err) => return Err(err),
                None => unreachable!(),
            },
            Err(err) => return Err(err),
        };

        let current = pd_client.get_timestamp()?;
        status.check_ttl(current);
        let res = status;
        if res.is_cacheable() {
            self.ctx.save_resolved(txn_id, res.clone());
        }
        Ok(res)
    }

    /// lock.rs:691-708.
    fn check_all_secondaries(&mut self, pd_client: &PdClient, keys: Vec<Key>, txn_id: Ts) -> Result<SecondaryLocksStatus> {
        let req = new_check_secondary_locks_request(keys, txn_id);
        // .preserve_shard().retry_multi_region(DEFAULT_REGION_BACKOFF).extract_error().merge(CollectSecondaryLocks)
        let r = retry_multi_region(pd_client, &req, None, false)?;
        let r = extract_error(r)?;
        collect_secondary_locks(txn_id, r.into_iter().map(Ok).collect())
    }

    /// lock.rs:729-794.
    fn get_txn_status_from_lock(&mut self, mut backoff: Backoff, lock: &LockInfo, caller_start_ts: Ts, current_ts: Ts, force_sync_commit: bool, pd_client: &PdClient) -> Result<TransactionStatus> {
        let current_ts = if lock.lock_ttl == 0 {
            // NOTE: lock_ttl = 0 is a special protocol!!!
            u64::MAX
        } else {
            current_ts
        };

        let mut rollback_if_not_exist = false;
        loop {
            match self.check_txn_status(pd_client, lock.lock_version, lock.primary_lock, caller_start_ts, current_ts, rollback_if_not_exist, force_sync_commit, lock.lock_type == PbOp::PessimisticLock, lock.is_txn_file) {
                Ok(status) => return Ok(status),
                Err(Error::TxnNotFound(txn_not_found)) => {
                    let current = pd_client.get_timestamp()?;
                    // PORT: rule T2 (lock.rs:769: the lock counts as expired).
                    if lock_until_expired_ms(lock.lock_version, lock.lock_ttl, current) <= 0 || EVERY_TTL_EXPIRED {
                        rollback_if_not_exist = true;
                        continue;
                    } else if lock.lock_type == PbOp::PessimisticLock {
                        let status = TransactionStatus { kind: TransactionStatusKind::Locked(lock.lock_ttl, lock.clone()), action: Action::NoAction, is_expired: false };
                        return Ok(status);
                    }
                    if let Some(()) = backoff.next_delay_duration() {
                        // PORT: sleep cut (rule T4).
                        continue;
                    }
                    return Err(Error::TxnNotFound(txn_not_found));
                }
                Err(err) => return Err(err),
            }
        }
    }
}

/// lock.rs:805-807 (PORT: rule T1, `physical` is `ts >> 18`).
fn lock_until_expired_ms(lock_version: Ts, ttl: u64, current: Ts) -> i64 { physical(lock_version) + ttl as i64 - physical(current) }

// --- transaction/buffer.rs ------------------------------------------------------------------

/// buffer.rs:367-394 `BufferEntry`. PORT: Del, Insert, CheckNotExist cut (rule A2).
#[derive(Clone, Debug, PartialEq)]
enum BufferEntry { Cached(Option<Value>), Locked(Option<Option<Value>>), Put(Value) }

#[derive(Debug, PartialEq)]
enum MutationValue { Determined(Option<Value>), Undetermined }

impl BufferEntry {
    fn to_proto_with_key(&self, key: &Key) -> Option<Mutation> {
        let (op, value) = match self {
            BufferEntry::Cached(_) => return None,
            BufferEntry::Put(v) => (PbOp::Put, *v),
            BufferEntry::Locked(_) => (PbOp::Lock, 0),
        };
        Some(Mutation { op, key: *key, value })
    }
    fn get_value(&self) -> MutationValue {
        match self {
            BufferEntry::Cached(value) => MutationValue::Determined(*value),
            BufferEntry::Put(value) => MutationValue::Determined(Some(*value)),
            BufferEntry::Locked(None) => MutationValue::Undetermined,
            BufferEntry::Locked(Some(value)) => MutationValue::Determined(*value),
        }
    }
}

struct Buffer { primary_key: Option<Key>, entry_map: BTreeMap<Key, BufferEntry>, is_pessimistic: bool }

impl Buffer {
    fn new(is_pessimistic: bool) -> Buffer { Buffer { primary_key: None, entry_map: BTreeMap::new(), is_pessimistic } }
    fn get_primary_key(&self) -> Option<Key> { self.primary_key }
    fn primary_key_or(&mut self, key: &Key) { self.primary_key.get_or_insert(*key); }
    fn get(&self, key: &Key) -> Option<Value> {
        match self.get_from_mutations(key) { MutationValue::Determined(value) => value, MutationValue::Undetermined => None }
    }
    /// buffer.rs:55-73. PORT: async closure becomes a plain closure (blocking exchanges).
    fn get_or_else(&mut self, key: Key, f: impl FnOnce(Key) -> Result<Option<Value>>) -> Result<Option<Value>> {
        match self.get_from_mutations(&key) {
            MutationValue::Determined(value) => Ok(value),
            MutationValue::Undetermined => {
                let value = f(key)?;
                self.update_cache(key, value);
                Ok(value)
            }
        }
    }
    /// buffer.rs:201-214.
    fn lock(&mut self, key: Key) {
        self.primary_key.get_or_insert(key);
        let value = self.entry_map.entry(key).or_insert(BufferEntry::Locked(None));
        if let BufferEntry::Cached(v) = value {
            *value = BufferEntry::Locked(Some(v.take()))
        }
    }
    /// buffer.rs:216-226.
    fn unlock(&mut self, key: &Key) {
        if let Some(value) = self.entry_map.get_mut(key) {
            if let BufferEntry::Locked(v) = value {
                if let Some(v) = v {
                    *value = BufferEntry::Cached(v.take());
                } else {
                    self.entry_map.remove(key);
                }
            }
        }
    }
    /// buffer.rs:228-241. PORT: the Insert/CheckNotExist arm is cut (rule A2).
    fn put(&mut self, key: Key, value: Value) { self.insert_entry(key, BufferEntry::Put(value)) }
    /// buffer.rs:277-283.
    fn to_proto_mutations(&self) -> Vec<Mutation> { self.entry_map.iter().filter_map(|(key, m)| m.to_proto_with_key(key)).collect() }
    /// buffer.rs:285-294. PORT: one byte per key and per value.
    fn get_write_size(&self) -> usize { self.entry_map.values().map(|v| match v { BufferEntry::Put(_) => 2, _ => 0 }).sum() }
    fn get_from_mutations(&self, key: &Key) -> MutationValue {
        self.entry_map.get(key).map(BufferEntry::get_value).unwrap_or(MutationValue::Undetermined)
    }
    /// buffer.rs:303-322.
    fn update_cache(&mut self, key: Key, value: Option<Value>) {
        match self.entry_map.get(&key) {
            Some(BufferEntry::Locked(None)) => { self.entry_map.insert(key, BufferEntry::Locked(Some(value))); }
            None => { self.entry_map.insert(key, BufferEntry::Cached(value)); }
            Some(BufferEntry::Cached(v)) | Some(BufferEntry::Locked(Some(v))) => assert!(&value == v),
            Some(BufferEntry::Put(v)) => assert!(value.as_ref() == Some(v)),
        }
    }
    /// buffer.rs:324-330.
    fn insert_entry(&mut self, key: Key, entry: BufferEntry) {
        if !matches!(entry, BufferEntry::Cached(_)) {
            self.primary_key.get_or_insert(key);
        }
        self.entry_map.insert(key, entry);
    }
}

// --- transaction/transaction.rs ------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum TransactionKind { Optimistic, Pessimistic(Ts) }

/// transaction.rs:1160-1270 `TransactionOptions`. PORT: `check_level` (drop check, rule P1) and
/// `heartbeat_option` (rule T3) are cut.
#[derive(Clone, Debug, PartialEq)]
struct TransactionOptions { kind: TransactionKind, try_one_pc: bool, async_commit: bool, read_only: bool, retry_options: RetryOptions }

impl TransactionOptions {
    fn new_optimistic() -> TransactionOptions {
        TransactionOptions { kind: TransactionKind::Optimistic, try_one_pc: false, async_commit: false, read_only: false, retry_options: RetryOptions::default_optimistic() }
    }
    fn new_pessimistic() -> TransactionOptions {
        TransactionOptions { kind: TransactionKind::Pessimistic(0), try_one_pc: false, async_commit: false, read_only: false, retry_options: RetryOptions::default_pessimistic() }
    }
    fn use_async_commit(mut self) -> TransactionOptions { self.async_commit = true; self }
    fn try_one_pc(mut self) -> TransactionOptions { self.try_one_pc = true; self }
    fn push_for_update_ts(&mut self, for_update_ts: Ts) {
        match &mut self.kind {
            TransactionKind::Optimistic => unreachable!(),
            TransactionKind::Pessimistic(old_for_update_ts) => {
                self.kind = TransactionKind::Pessimistic(std::cmp::max(*old_for_update_ts, for_update_ts));
            }
        }
    }
    fn is_pessimistic(&self) -> bool { matches!(self.kind, TransactionKind::Pessimistic(_)) }
}

/// transaction.rs:1126-1134.
const MAX_TTL: u64 = 20000;
const DEFAULT_LOCK_TTL: u64 = 3000;
const TXN_COMMIT_BATCH_SIZE: u64 = 16 * 1024;
const TTL_FACTOR: f64 = 6000.0;

/// The transaction status (transaction.rs `TransactionStatus`, an AtomicU8). PORT: a plain field.
#[derive(Clone, Copy, Debug, PartialEq)]
enum TxnState { ReadOnly, Active, StartedCommit, Committed, StartedRollback, Rolledback, Dropped }

struct Transaction {
    status: TxnState,
    timestamp: Ts,
    buffer: Buffer,
    rpc: PdClient,
    options: TransactionOptions,
    prewritten: bool,
    /// PORT: rule P2, the attempt bound `a` of the application's WriteConflict retry.
    write_conflict_attempts: u32,
}

impl Transaction {
    /// transaction.rs:101-122. PORT: keyspace, heartbeat flag and `start_instant` cut (T3, T6).
    fn new(timestamp: Ts, rpc: PdClient, options: TransactionOptions, a: u32) -> Transaction {
        let status = if options.read_only { TxnState::ReadOnly } else { TxnState::Active };
        Transaction { status, timestamp, buffer: Buffer::new(options.is_pessimistic()), rpc, options, prewritten: false, write_conflict_attempts: a }
    }

    /// transaction.rs:143-164.
    fn get(&mut self, key: Key) -> Result<Option<Value>> {
        self.check_allow_operation()?;
        let timestamp = self.timestamp;
        let rpc = self.rpc.clone();
        let retry_options = self.options.retry_options.clone();
        self.buffer.get_or_else(key, |key| {
            let request = new_get_request(key, timestamp);
            // .resolve_lock(timestamp, lock_backoff).retry_multi_region(DEFAULT_REGION_BACKOFF)
            // .merge(CollectSingle).post_process_default()
            let opts = ResolveLockOpts { timestamp, backoff: retry_options.lock_backoff };
            let executed = retry_multi_region(&rpc, &request, Some(&opts), false).and_then(collect_single).map(|ResponseWithShard(r, _)| r);
            process_get_response(executed)
        })
    }

    /// transaction.rs:208-224.
    fn get_for_update(&mut self, key: Key) -> Result<Option<Value>> {
        self.check_allow_operation()?;
        if !self.is_pessimistic() {
            // PORT: `lock_keys` (optimistic) is not in the cut; no scenario issues it
            // (INTERFACE.md 3: GetForUpdate belongs to pessimistic clients).
            unreachable!("GetForUpdate on an optimistic transaction is not in the cut")
        } else {
            // PORT: rule P2, the WriteConflict retry of the application around pessimistic_lock.
            let mut attempt = 1;
            let mut pairs = loop {
                match self.pessimistic_lock(vec![key], true) {
                    Err(e) if is_write_conflict(&e) && attempt < self.write_conflict_attempts => attempt += 1, // PORT: P2
                    r => break r?,
                }
            };
            debug_assert!(pairs.len() <= 1);
            match pairs.pop() {
                Some(pair) => Ok(Some(pair.1)),
                None => Ok(None),
            }
        }
    }

    /// transaction.rs:470-480.
    fn put(&mut self, key: Key, value: Value) -> Result<()> {
        self.check_allow_operation()?;
        if self.is_pessimistic() {
            // PORT: rule P2, the WriteConflict retry of the application around pessimistic_lock.
            let mut attempt = 1;
            loop {
                match self.pessimistic_lock(vec![key], false) {
                    Err(e) if is_write_conflict(&e) && attempt < self.write_conflict_attempts => attempt += 1, // PORT: P2
                    r => { r?; break; }
                }
            }
        }
        self.buffer.put(key, value);
        Ok(())
    }

    /// transaction.rs:653-706.
    fn commit(&mut self) -> Result<Option<Ts>> {
        if !self.transit_status(|status| matches!(status, TxnState::StartedCommit | TxnState::Active), TxnState::StartedCommit) {
            return Err(Error::OperationAfterCommitError);
        }
        self.prewritten = true;

        let primary_key = self.buffer.get_primary_key();
        let mutations = self.buffer.to_proto_mutations();
        if mutations.is_empty() {
            assert!(primary_key.is_none());
            return Ok(None);
        }

        self.start_auto_heartbeat();

        let res = Committer::new(primary_key, mutations, self.timestamp, self.rpc.clone(), self.options.clone(), self.buffer.get_write_size() as u64).commit();

        if res.is_ok() {
            self.set_status(TxnState::Committed);
        }
        res
    }

    /// transaction.rs:859-940.
    fn pessimistic_lock(&mut self, keys: Vec<Key>, need_value: bool) -> Result<Vec<KvPair>> {
        assert!(matches!(self.options.kind, TransactionKind::Pessimistic(_)), "`pessimistic_lock` is only valid to use with pessimistic transactions");
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let first_key = keys[0];
        let primary_lock = self.buffer.get_primary_key().unwrap_or(first_key);
        let for_update_ts = self.rpc.get_timestamp()?;
        self.options.push_for_update_ts(for_update_ts);
        // lowering.rs:140-161: each key becomes an Op::PessimisticLock mutation (assertion None).
        let mutations = keys.iter().map(|k| Mutation { op: PbOp::PessimisticLock, key: *k, value: 0 }).collect();
        let request = new_pessimistic_lock_request(mutations, primary_lock, self.timestamp, MAX_TTL, for_update_ts, need_value);
        // .resolve_lock(..).preserve_shard().retry_multi_region_preserve_results(..).merge(CollectWithShard)
        let opts = ResolveLockOpts { timestamp: self.timestamp, backoff: self.options.retry_options.lock_backoff.clone() };
        let pairs = retry_multi_region(&self.rpc, &request, Some(&opts), true).and_then(collect_with_shard);

        if let Err(err) = pairs {
            match err {
                Error::PessimisticLockError { inner, success_keys } if !success_keys.is_empty() => {
                    self.pessimistic_lock_rollback(success_keys, self.timestamp, for_update_ts)?;
                    Err(*inner)
                }
                _ => Err(err),
            }
        } else {
            self.buffer.primary_key_or(&first_key);
            self.start_auto_heartbeat();
            for key in keys {
                self.buffer.lock(key);
            }
            pairs
        }
    }

    /// transaction.rs:943-980.
    fn pessimistic_lock_rollback(&mut self, keys: Vec<Key>, start_version: Ts, for_update_ts: Ts) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let req = new_pessimistic_rollback_request(keys.clone(), start_version, for_update_ts);
        let opts = ResolveLockOpts { timestamp: start_version, backoff: self.options.retry_options.lock_backoff.clone() };
        let r = retry_multi_region(&self.rpc, &req, Some(&opts), false)?;
        extract_error(r)?;
        for key in keys {
            self.buffer.unlock(&key);
        }
        Ok(())
    }

    /// transaction.rs:983-993.
    fn check_allow_operation(&self) -> Result<()> {
        match self.status {
            TxnState::ReadOnly | TxnState::Active => Ok(()),
            _ => Err(Error::OperationAfterCommitError),
        }
    }

    fn is_pessimistic(&self) -> bool { matches!(self.options.kind, TransactionKind::Pessimistic(_)) }

    /// transaction.rs:998-1062. PORT: rule T3, no heartbeats (the tokio::spawn of 1053 becomes
    /// nothing).
    fn start_auto_heartbeat(&mut self) {}

    fn set_status(&mut self, status: TxnState) { self.status = status; }
    fn transit_status(&mut self, check_status: impl Fn(TxnState) -> bool, next: TxnState) -> bool {
        // PORT: the compare_exchange loop of transaction.rs:1075-1093 on a plain field.
        if check_status(self.status) {
            self.status = next;
            true
        } else {
            false
        }
    }
}

/// PORT: rule P2, "a WriteConflict" returned by `pessimistic_lock` (INTERFACE.md 5.4).
fn is_write_conflict(e: &Error) -> bool {
    match e {
        Error::PessimisticLockError { inner, .. } => is_write_conflict(inner),
        Error::MultipleKeyErrors(v) => v.iter().any(is_write_conflict),
        Error::KeyError(k) => k.conflict.is_some(),
        _ => false,
    }
}

/// transaction.rs:1318-1331 `Committer`.
#[derive(Clone)]
struct Committer {
    primary_key: Option<Key>,
    mutations: Vec<Mutation>,
    start_version: Ts,
    rpc: PdClient,
    options: TransactionOptions,
    undetermined: bool,
    write_size: u64,
}

impl Committer {
    fn new(primary_key: Option<Key>, mutations: Vec<Mutation>, start_version: Ts, rpc: PdClient, options: TransactionOptions, write_size: u64) -> Committer {
        Committer { primary_key, mutations, start_version, rpc, options, undetermined: false, write_size }
    }

    /// transaction.rs:1340-1407.
    fn commit(mut self) -> Result<Option<Ts>> {
        let min_commit_ts = match self.prewrite() {
            Ok(ts) => ts,
            Err(e) => {
                return if (self.options.async_commit || self.options.try_one_pc) && is_commit_outcome_unknown(&e) {
                    Err(Error::UndeterminedError(Box::new(e)))
                } else {
                    Err(e)
                };
            }
        };

        // PORT: fail_point "after-prewrite" cut (rule A6).

        if self.options.try_one_pc {
            return Ok(min_commit_ts);
        }

        let commit_ts = if self.options.async_commit {
            // FIXME: min_commit_ts == 0 => fallback to normal 2PC
            min_commit_ts.unwrap()
        } else {
            match self.commit_primary_with_retry() {
                Ok(commit_ts) => commit_ts,
                Err(e) => {
                    return if self.undetermined { Err(Error::UndeterminedError(Box::new(e))) } else { Err(e) };
                }
            }
        };
        // PORT: transaction.rs:1401-1405 `tokio::spawn(self.commit_secondary(commit_ts))`
        // becomes the trigger of `xi` (cut README section 4); `xi` drops the result.
        let i = self.rpc.setup.ts.iter().position(|t| *t == self.rpc.me).expect("ti");
        send_tagged_msg(self.rpc.setup.xs[i], TAG_TRIGGER, Net::Trigger(Box::new(Trigger {
            primary_key: self.primary_key, mutations: self.mutations.clone(), start_version: self.start_version,
            options: self.options.clone(), write_size: self.write_size, commit_ts,
        })));
        Ok(Some(commit_ts))
    }

    /// transaction.rs:1409-1483.
    fn prewrite(&mut self) -> Result<Option<Ts>> {
        let primary_lock = self.primary_key.unwrap();
        let elapsed = 0; // PORT: rule T6, `start_instant.elapsed()` unused.
        let lock_ttl = self.calc_txn_lock_ttl();
        let mut request = match &self.options.kind {
            TransactionKind::Optimistic => new_prewrite_request(self.mutations.clone(), primary_lock, self.start_version, lock_ttl + elapsed),
            TransactionKind::Pessimistic(for_update_ts) => new_pessimistic_prewrite_request(self.mutations.clone(), primary_lock, self.start_version, lock_ttl + elapsed, *for_update_ts),
        };

        request.use_async_commit = self.options.async_commit;
        request.try_one_pc = self.options.try_one_pc;
        request.secondaries = self.mutations.iter().filter(|m| self.primary_key.unwrap() != m.key).map(|m| m.key).collect();
        // FIXME set max_commit_ts and min_commit_ts

        // .resolve_lock(..).retry_multi_region[_terminal_on_undetermined](..).merge(CollectError).extract_error()
        // PORT: terminal_on_undetermined has no effect, no UndeterminedResult occurs (rule A6).
        let opts = ResolveLockOpts { timestamp: self.start_version, backoff: self.options.retry_options.lock_backoff.clone() };
        let response: Vec<PrewriteResponse> = collect_error(retry_multi_region(&self.rpc, &request, Some(&opts), false)?)?.into_iter().map(|ResponseWithShard(r, _)| r).collect();

        if self.options.try_one_pc && response.len() == 1 {
            if response[0].one_pc_commit_ts == 0 {
                return Err(Error::OnePcFailure);
            }
            return Ok(Some(response[0].one_pc_commit_ts)); // Timestamp::try_from_version
        }

        self.options.try_one_pc = false;

        let min_commit_ts = response
            .iter()
            .map(|r| {
                assert_eq!(r.one_pc_commit_ts, 0);
                r.min_commit_ts
            })
            .max();

        Ok(min_commit_ts)
    }

    /// transaction.rs:1486-1542.
    fn commit_primary(&mut self) -> Result<Ts> {
        let primary_key: Vec<Key> = self.primary_key.into_iter().collect();
        let commit_version = self.rpc.get_timestamp()?;
        let req = new_commit_request(primary_key, self.start_version, commit_version);
        // .resolve_lock(..).retry_multi_region_terminal_on_undetermined(..).extract_error()
        let opts = ResolveLockOpts { timestamp: self.start_version, backoff: self.options.retry_options.lock_backoff.clone() };
        let r = retry_multi_region(&self.rpc, &req, Some(&opts), false).and_then(extract_error);
        if let Err(e) = &r {
            if is_commit_outcome_unknown(e) {
                self.undetermined = true;
            }
        }
        r?;
        Ok(commit_version)
    }

    /// transaction.rs:1544-1584.
    fn commit_primary_with_retry(&mut self) -> Result<Ts> {
        loop {
            match self.commit_primary() {
                Ok(commit_version) => return Ok(commit_version),
                Err(Error::MultipleKeyErrors(mut errors)) => match errors.pop() {
                    Some(Error::KeyError(key_err)) => {
                        if let Some(expired) = key_err.commit_ts_expired.clone() {
                            let primary_key = self.primary_key.unwrap();
                            if primary_key != expired.key {
                                return Err(Error::StringError("2PC commitTS rejected by TiKV, but the key is not the primary key".to_string()));
                            }
                            if expired.min_commit_ts.saturating_sub(expired.attempted_commit_ts) > 943718400000 {
                                return Err(Error::StringError("2PC min_commit_ts is too large".to_string()));
                            }
                            continue;
                        } else {
                            return Err(Error::KeyError(key_err));
                        }
                    }
                    Some(err) => return Err(err),
                    None => unreachable!(),
                },
                Err(err) => return Err(err),
            }
        }
    }

    /// transaction.rs:1586-1648 (the body of `xi`).
    fn commit_secondary(self, commit_version: Ts) -> Result<()> {
        let start_version = self.start_version;
        let mutations_len = self.mutations.len();
        let primary_only = mutations_len == 1;
        let mutations = self.mutations.into_iter();
        // PORT: the integration-tests fail point (1599-1626) cut (rule A6).

        let req = if self.options.async_commit {
            let keys = mutations.map(|m| m.key).collect();
            new_commit_request(keys, start_version, commit_version)
        } else if primary_only {
            return Ok(());
        } else {
            let primary_key = self.primary_key.unwrap();
            let keys = mutations.map(|m| m.key).filter(|key| primary_key != *key).collect();
            new_commit_request(keys, start_version, commit_version)
        };
        // .resolve_lock(start_version, lock_backoff).retry_multi_region(region_backoff).extract_error()
        let opts = ResolveLockOpts { timestamp: start_version, backoff: self.options.retry_options.lock_backoff.clone() };
        let r = retry_multi_region(&self.rpc, &req, Some(&opts), false)?;
        extract_error(r)?;
        Ok(())
    }

    /// transaction.rs:1662-1709 (not called by the application: rule P1).
    fn rollback(self, prewritten: bool) -> Result<()> {
        if self.options.kind == TransactionKind::Optimistic && self.mutations.is_empty() {
            return Ok(());
        }
        let keys: Vec<Key> = self.mutations.iter().map(|m| m.key).collect();
        let start_version = self.start_version;
        let opts = ResolveLockOpts { timestamp: start_version, backoff: self.options.retry_options.lock_backoff.clone() };
        match self.options.kind {
            TransactionKind::Pessimistic(for_update_ts) if !prewritten => {
                let req = new_pessimistic_rollback_request(keys, start_version, for_update_ts);
                extract_error(retry_multi_region(&self.rpc, &req, Some(&opts), false)?)?;
            }
            _ => {
                let req = new_batch_rollback_request(keys, start_version);
                extract_error(retry_multi_region(&self.rpc, &req, Some(&opts), false)?)?;
            }
        }
        Ok(())
    }

    /// transaction.rs:1711-1720.
    fn calc_txn_lock_ttl(&mut self) -> u64 {
        let mut lock_ttl = DEFAULT_LOCK_TTL;
        if self.write_size > TXN_COMMIT_BATCH_SIZE {
            let size_mb = self.write_size as f64 / 1024.0 / 1024.0;
            lock_ttl = (TTL_FACTOR * size_mb.sqrt()) as u64;
            lock_ttl = lock_ttl.clamp(DEFAULT_LOCK_TTL, MAX_TTL);
        }
        lock_ttl
    }
}

// --- the client processes -------------------------------------------------------------------

fn recv_setup() -> Arc<Setup> {
    let Net::Setup(s) = recv_tagged_msg_block::<_, Net>(|_, tag| tag == Some(TAG_SETUP)) else { panic!("Setup expected") };
    Arc::new(*s)
}

/// `ti`: client i's library (INTERFACE.md 5.2).
fn library(i: usize) {
    let setup = recv_setup();
    let me = setup.ts[i];
    let ci = setup.cs[i];
    let rpc = PdClient { me, setup: setup.clone() };
    let mut txn: Option<Transaction> = None;
    loop {
        let req: Req = recv_tagged_msg_block(|_, tag| tag.is_none());
        let rep = match req {
            Req::Begin => {
                // client.rs:174-181 / 226-230 `begin_with_options` (current_timestamp 250-252).
                let mut options = match setup.kinds[i] {
                    Kind::Optimistic => TransactionOptions::new_optimistic(),
                    Kind::Pessimistic => TransactionOptions::new_pessimistic(),
                };
                if setup.async_commit {
                    options = options.use_async_commit();
                }
                if setup.one_pc {
                    options = options.try_one_pc();
                }
                match rpc.get_timestamp() {
                    Ok(timestamp) => {
                        txn = Some(Transaction::new(timestamp, rpc.clone(), options, setup.a));
                        Rep::Begun
                    }
                    Err(_) => Rep::Aborted,
                }
            }
            Req::Get(k) => match txn.as_mut().expect("Begin first").get(k) {
                Ok(v) => Rep::Value(v.unwrap_or(0)),
                Err(_) => Rep::Aborted,
            },
            Req::GetForUpdate(k) => match txn.as_mut().expect("Begin first").get_for_update(k) {
                Ok(v) => Rep::Value(v.unwrap_or(0)),
                Err(_) => Rep::Aborted,
            },
            Req::Put(k, v) => match txn.as_mut().expect("Begin first").put(k, v) {
                Ok(()) => Rep::Done,
                Err(_) => Rep::Aborted,
            },
            Req::Commit => match txn.as_mut().expect("Begin first").commit() {
                Ok(_) => Rep::Committed,
                Err(_) => Rep::Aborted,
            },
        };
        send_msg(ci, rep);
    }
}

/// `xi`: the spawned `commit_secondary` (cut README section 4).
fn secondary_committer(i: usize) {
    let setup = recv_setup();
    let me = setup.xs[i];
    loop {
        let Net::Trigger(tr) = recv_tagged_msg_block::<_, Net>(|_, tag| tag == Some(TAG_TRIGGER)) else { panic!("Trigger expected") };
        let rpc = PdClient { me, setup: setup.clone() };
        let committer = Committer::new(tr.primary_key, tr.mutations, tr.start_version, rpc, tr.options, tr.write_size);
        // transaction.rs:1401-1405: the result is only logged.
        let _ = committer.commit_secondary(tr.commit_ts);
    }
}

/// `pd`: the timestamp oracle (rule T1).
fn pd_main() {
    let _setup = recv_setup();
    let mut last: Ts = 0;
    loop {
        let Net::Tso(from) = recv_tagged_msg_block::<_, Net>(|_, tag| tag == Some(TAG_NET)) else { panic!("Tso expected") };
        last += 1;
        send_tagged_msg(from, TAG_NET, Net::TsoReply(last));
    }
}

// ============================================================================================
// SERVER (tikv/tikv 96033a2)
// ============================================================================================

// --- components/txn_types: Lock, Write -----------------------------------------------------

/// txn_types/src/lock.rs:25ff `LockType`. PORT: Delete and Shared cut (rule A2); `Lock` exists
/// only to refuse it (a Lock mutation is cut, rule A2).
#[derive(Clone, Copy, Debug, PartialEq)]
enum LockType { Put, Lock, Pessimistic }

/// txn_types/src/lock.rs:87ff `Lock`: the cell's `lock` (rule A2). PORT: short_value, last_change,
/// txn_source, generation, is_txn_file cut (rule A2).
#[derive(Clone, Debug, PartialEq)]
struct Lock {
    lock_type: LockType,
    primary: Key,
    ts: Ts,
    ttl: u64,
    for_update_ts: Ts,
    txn_size: u64,
    min_commit_ts: Ts,
    use_async_commit: bool,
    use_one_pc: bool,
    secondaries: Vec<Key>,
    rollback_ts: Vec<Ts>,
    is_locked_with_conflict: bool,
}

impl Lock {
    fn new(lock_type: LockType, primary: Key, ts: Ts, ttl: u64, for_update_ts: Ts, txn_size: u64, min_commit_ts: Ts) -> Lock {
        Lock { lock_type, primary, ts, ttl, for_update_ts, txn_size, min_commit_ts, use_async_commit: false, use_one_pc: false, secondaries: vec![], rollback_ts: vec![], is_locked_with_conflict: false }
    }
    /// lock.rs:622-645.
    fn into_lock_info(self, raw_key: Key) -> LockInfo {
        LockInfo {
            primary_lock: self.primary,
            lock_version: self.ts,
            key: raw_key,
            lock_ttl: self.ttl,
            txn_size: self.txn_size,
            lock_type: match self.lock_type { LockType::Put => PbOp::Put, LockType::Lock => PbOp::Lock, LockType::Pessimistic => PbOp::PessimisticLock },
            lock_for_update_ts: self.for_update_ts,
            use_async_commit: self.use_async_commit,
            min_commit_ts: self.min_commit_ts,
            secondaries: self.secondaries,
            is_txn_file: false,
        }
    }
    fn is_pessimistic_txn(&self) -> bool { self.for_update_ts != 0 }
    fn is_pessimistic_lock(&self) -> bool { self.lock_type == LockType::Pessimistic }
    fn is_pessimistic_lock_with_conflict(&self) -> bool { self.is_pessimistic_lock() && self.is_locked_with_conflict }
}

/// txn_types `PessimisticLock`.
struct PessimisticLock { primary: Key, start_ts: Ts, ttl: u64, for_update_ts: Ts, min_commit_ts: Ts, is_locked_with_conflict: bool }
impl PessimisticLock {
    fn into_lock(self) -> Lock {
        let mut l = Lock::new(LockType::Pessimistic, self.primary, self.start_ts, self.ttl, self.for_update_ts, 0, self.min_commit_ts);
        l.is_locked_with_conflict = self.is_locked_with_conflict;
        l
    }
}

/// txn_types/src/write.rs:16ff `WriteType`. PORT: Delete and Lock cut (rule A2).
#[derive(Clone, Copy, Debug, PartialEq)]
enum WriteType { Put, Rollback }
impl WriteType {
    fn from_lock_type(tp: LockType) -> Option<WriteType> {
        match tp {
            LockType::Put => Some(WriteType::Put),
            LockType::Lock => panic!("PORT: Lock mutations are cut (rule A2)"),
            LockType::Pessimistic => None,
        }
    }
}

/// txn_types/src/write.rs:71ff `Write`: a `writes` entry of the cell (rule A2). PORT: the protected
/// marker short value is the flag `protected`; gc_fence, last_change, txn_source cut.
#[derive(Clone, Debug, PartialEq)]
struct Write { write_type: WriteType, start_ts: Ts, protected: bool, has_overlapped_rollback: bool }
impl Write {
    fn new(write_type: WriteType, start_ts: Ts) -> Write { Write { write_type, start_ts, protected: false, has_overlapped_rollback: false } }
    /// write.rs:204-219.
    fn new_rollback(start_ts: Ts, protected: bool) -> Write { Write { write_type: WriteType::Rollback, start_ts, protected, has_overlapped_rollback: false } }
    fn set_overlapped_rollback(mut self, has_overlapped_rollback: bool) -> Write { self.has_overlapped_rollback = has_overlapped_rollback; self }
    /// write.rs:459-466.
    fn is_protected(&self) -> bool { self.write_type == WriteType::Rollback && self.protected }
}

// --- the per-key cell (rule A2) --------------------------------------------------------------

#[derive(Clone, Debug, Default, PartialEq)]
struct Cell { lock: Option<Lock>, writes: BTreeMap<Ts, Write>, data: BTreeMap<Ts, Value> }
type Snapshot = BTreeMap<Key, Cell>;

// --- storage/mvcc errors (mvcc/mod.rs ErrorInner and txn/mod.rs ErrorInner, the kinds the cut raises)

#[derive(Clone, Debug, PartialEq)]
enum MvccError {
    KeyIsLocked(LockInfo),
    WriteConflict { start_ts: Ts, conflict_start_ts: Ts, conflict_commit_ts: Ts, key: Key, primary: Key, reason: WriteConflictReason },
    PessimisticLockNotFound { start_ts: Ts, key: Key, reason: &'static str },
    CommitTsTooLarge { start_ts: Ts, min_commit_ts: Ts, max_commit_ts: Ts },
    LockTypeNotMatch { start_ts: Ts, key: Key, pessimistic: bool },
    TxnLockNotFound { start_ts: Ts, commit_ts: Ts, key: Key },
    TxnNotFound { start_ts: Ts, key: Key },
    CommitTsExpired { start_ts: Ts, commit_ts: Ts, key: Key, min_commit_ts: Ts },
    PessimisticLockRolledBack { start_ts: Ts, key: Key },
    PrimaryMismatch(LockInfo),
    Committed { start_ts: Ts, commit_ts: Ts, key: Key },
    InvalidTxnTso { start_ts: Ts, commit_ts: Ts },
    Other(String),
}
type MResult<T> = std::result::Result<T, MvccError>;

/// storage/errors.rs:396-541 `extract_key_error`.
fn extract_key_error(err: &MvccError) -> KeyError {
    let mut key_error = KeyError::default();
    match err {
        MvccError::KeyIsLocked(info) => key_error.locked = Some(info.clone()),
        MvccError::WriteConflict { start_ts, conflict_start_ts, conflict_commit_ts, key, primary, reason } => {
            key_error.conflict = Some(WriteConflict { start_ts: *start_ts, conflict_ts: *conflict_start_ts, conflict_commit_ts: *conflict_commit_ts, key: *key, primary: *primary, reason: *reason });
            key_error.retryable = Some(format!("{err:?}"));
        }
        MvccError::TxnLockNotFound { key, .. } => {
            key_error.retryable = Some(format!("{err:?}"));
            key_error.txn_lock_not_found = Some(*key);
        }
        MvccError::TxnNotFound { start_ts, key } => key_error.txn_not_found = Some(TxnNotFound { start_ts: *start_ts, primary_key: *key }),
        MvccError::CommitTsExpired { start_ts, commit_ts, key, min_commit_ts } => {
            key_error.commit_ts_expired = Some(CommitTsExpired { start_ts: *start_ts, attempted_commit_ts: *commit_ts, key: *key, min_commit_ts: *min_commit_ts })
        }
        MvccError::CommitTsTooLarge { min_commit_ts, .. } => key_error.commit_ts_too_large = Some(*min_commit_ts),
        MvccError::PrimaryMismatch(lock_info) => key_error.primary_mismatch = Some(lock_info.clone()),
        _ => key_error.abort = Some(format!("{err:?}")),
    }
    key_error
}

// --- components/concurrency_manager (rule A3) -----------------------------------------------

struct ConcurrencyManager { max_ts: Ts }
impl ConcurrencyManager {
    /// lib.rs:221-223.
    fn max_ts(&self) -> Ts { self.max_ts }
    /// lib.rs:235-272. PORT: the limit checks (274-350) are cut, the update always succeeds.
    fn update_max_ts(&mut self, new_ts: Ts) {
        if new_ts == TS_MAX {
            return;
        }
        self.max_ts = std::cmp::max(self.max_ts, new_ts);
    }
}

// --- storage/mvcc: MvccTxn (txn.rs:66-274) --------------------------------------------------

#[derive(Clone, Debug)]
enum Modify { PutLock(Key, Lock), DeleteLock(Key), PutValue(Key, Ts, Value), DeleteValue(Key, Ts), PutWrite(Key, Ts, Write), DeleteWrite(Key, Ts) }

/// `ReleasedLock`. PORT: no waiter is ever woken (rule A4), the value only marks a release.
type ReleasedLock = ();

/// mvcc/txn.rs `MvccTxn`: writes collected during a command and applied to the cells at its
/// end (rule A2).
struct MvccTxn { start_ts: Ts, modifies: Vec<Modify>, locks_for_1pc: Vec<(Key, Lock, bool)> }

impl MvccTxn {
    fn new(start_ts: Ts) -> MvccTxn { MvccTxn { start_ts, modifies: vec![], locks_for_1pc: vec![] } }
    fn put_lock(&mut self, key: Key, lock: &Lock, _is_new: bool) { self.modifies.push(Modify::PutLock(key, lock.clone())); }
    fn put_locks_for_1pc(&mut self, key: Key, lock: Lock, remove_pessimistic_lock: bool) { self.locks_for_1pc.push((key, lock, remove_pessimistic_lock)); }
    fn put_pessimistic_lock(&mut self, key: Key, lock: PessimisticLock, is_new: bool) { self.put_lock(key, &lock.into_lock(), is_new); }
    fn unlock_key(&mut self, key: Key, _pessimistic: bool, _commit_ts: Ts) -> Option<ReleasedLock> {
        self.modifies.push(Modify::DeleteLock(key));
        Some(())
    }
    fn put_value(&mut self, key: Key, ts: Ts, value: Value) { self.modifies.push(Modify::PutValue(key, ts, value)); }
    fn delete_value(&mut self, key: Key, ts: Ts) { self.modifies.push(Modify::DeleteValue(key, ts)); }
    fn put_write(&mut self, key: Key, ts: Ts, write: Write) { self.modifies.push(Modify::PutWrite(key, ts, write)); }
    fn delete_write(&mut self, key: Key, ts: Ts) { self.modifies.push(Modify::DeleteWrite(key, ts)); }
    /// txn.rs:236-258.
    fn mark_rollback_on_mismatching_lock(&mut self, key: &Key, mut lock: Lock, is_protected: bool) {
        assert_ne!(lock.ts, self.start_ts);
        if !is_protected {
            return;
        }
        if self.start_ts < lock.min_commit_ts {
            return;
        }
        if !lock.use_async_commit {
            return;
        }
        lock.rollback_ts.push(self.start_ts);
        self.put_lock(*key, &lock, false);
    }
    fn clear(&mut self) {
        self.modifies.clear();
        self.locks_for_1pc.clear();
    }
    /// txn.rs:260-273 `get_pending_lock_bytes`.
    fn get_pending_lock(&self, key: &Key) -> Option<Option<Lock>> {
        for modify in self.modifies.iter().rev() {
            match modify {
                Modify::PutLock(k, l) if k == key => return Some(Some(l.clone())),
                Modify::DeleteLock(k) if k == key => return Some(None),
                _ => {}
            }
        }
        None
    }
    fn write_size(&self) -> usize { self.modifies.len() }
}

/// The write stage (the engine applying `WriteData`): the cell's writes at the end of the
/// command (rule A2).
fn apply_modifies(cells: &mut Snapshot, modifies: Vec<Modify>) {
    for m in modifies {
        match m {
            Modify::PutLock(k, l) => cells.entry(k).or_default().lock = Some(l),
            Modify::DeleteLock(k) => cells.entry(k).or_default().lock = None,
            Modify::PutValue(k, ts, v) => { cells.entry(k).or_default().data.insert(ts, v); }
            Modify::DeleteValue(k, ts) => { cells.entry(k).or_default().data.remove(&ts); }
            Modify::PutWrite(k, ts, w) => { cells.entry(k).or_default().writes.insert(ts, w); }
            Modify::DeleteWrite(k, ts) => { cells.entry(k).or_default().writes.remove(&ts); }
        }
    }
}

// --- storage/mvcc/reader (reader/mod.rs:30-96, reader.rs:40-124, 198-256, 476-653) -----------

#[derive(Clone, Debug)]
struct OverlappedWrite { write: Write, gc_fence: Ts }

#[derive(Clone, Debug)]
enum TxnCommitRecord {
    None { overlapped_write: Option<OverlappedWrite> },
    SingleRecord { commit_ts: Ts, write: Write },
    OverlappedRollback { commit_ts: Ts },
}
impl TxnCommitRecord {
    fn info(&self) -> Option<(Ts, WriteType)> {
        match self {
            Self::None { .. } => None,
            Self::SingleRecord { commit_ts, write } => Some((*commit_ts, write.write_type)),
            Self::OverlappedRollback { commit_ts } => Some((*commit_ts, WriteType::Rollback)),
        }
    }
    fn unwrap_none(self) -> Option<OverlappedWrite> {
        match self {
            Self::None { overlapped_write } => overlapped_write,
            _ => panic!("txn record found but not expected: {self:?}"),
        }
    }
}

/// `SnapshotReader` / `MvccReader` over the snapshot of the region's cells.
struct SnapshotReader<'a> { start_ts: Ts, snap: &'a Snapshot }

impl SnapshotReader<'_> {
    fn load_lock(&self, key: &Key) -> Option<Lock> { self.snap.get(key).and_then(|c| c.lock.clone()) }
    /// reader.rs:476-503: the newest write with commit_ts <= ts.
    fn seek_write(&self, key: &Key, ts: Ts) -> Option<(Ts, Write)> {
        self.snap.get(key).and_then(|c| c.writes.range(..=ts).next_back().map(|(t, w)| (*t, w.clone())))
    }
    /// reader.rs:500ff `load_data`: the value written at the write's start_ts.
    fn load_data(&self, key: &Key, write: Write) -> Value {
        *self.snap.get(key).and_then(|c| c.data.get(&write.start_ts)).expect("default not found")
    }
    /// reader.rs:516-525.
    fn get(&self, key: &Key, ts: Ts) -> Option<Value> { self.get_write(key, ts).map(|w| self.load_data(key, w)) }
    fn get_write(&self, key: &Key, ts: Ts) -> Option<Write> { self.get_write_with_commit_ts(key, ts).map(|(w, _)| w) }
    /// reader.rs:551-603. PORT: `last_change` is cut (rule A2): a Rollback is skipped by seeking
    /// below it (the `_` arm); gc_fence cut.
    fn get_write_with_commit_ts(&self, key: &Key, mut ts: Ts) -> Option<(Write, Ts)> {
        let mut seek_res = self.seek_write(key, ts);
        loop {
            match seek_res {
                Some((commit_ts, write)) => match write.write_type {
                    WriteType::Put => return Some((write, commit_ts)),
                    WriteType::Rollback => {
                        if ts == 0 {
                            return None;
                        }
                        if commit_ts == 0 {
                            return None;
                        }
                        ts = commit_ts - 1;
                    }
                },
                None => return None,
            }
            seek_res = self.seek_write(key, ts);
        }
    }
    /// reader.rs:605-631 (with the reader's start_ts).
    fn get_txn_commit_record(&self, key: &Key) -> TxnCommitRecord {
        let start_ts = self.start_ts;
        let mut seek_ts = TS_MAX;
        let mut gc_fence: Ts = 0;
        while let Some((commit_ts, write)) = self.seek_write(key, seek_ts) {
            if write.start_ts == start_ts {
                return TxnCommitRecord::SingleRecord { commit_ts, write };
            }
            if commit_ts == start_ts {
                if write.has_overlapped_rollback {
                    return TxnCommitRecord::OverlappedRollback { commit_ts };
                }
                return TxnCommitRecord::None { overlapped_write: Some(OverlappedWrite { write, gc_fence }) };
            }
            if write.write_type == WriteType::Put {
                gc_fence = commit_ts;
            }
            if commit_ts < start_ts {
                break;
            }
            if commit_ts == 0 {
                break;
            }
            seek_ts = commit_ts - 1;
        }
        TxnCommitRecord::None { overlapped_write: None }
    }
}

/// txn_types/src/lock.rs:363-422 `check_ts_conflict_si`. PORT: bypass_locks is empty and there is
/// no replica read; shared locks cut.
fn check_ts_conflict_si(lock: &Lock, key: &Key, ts: Ts) -> MResult<()> {
    if lock.ts > ts || lock.lock_type == LockType::Lock || lock.is_pessimistic_lock() {
        return Ok(());
    }
    if lock.min_commit_ts > ts {
        return Ok(());
    }
    let raw_key = *key;
    if ts == TS_MAX && raw_key == lock.primary && !lock.use_async_commit && !lock.use_one_pc {
        return Ok(());
    }
    Err(MvccError::KeyIsLocked(lock.clone().into_lock_info(raw_key)))
}

// --- txn/actions/check_txn_status.rs ---------------------------------------------------------

/// storage/types.rs:108-147 `TxnStatus`.
#[derive(Clone, Debug, PartialEq)]
enum TxnStatus {
    RolledBack,
    TtlExpire,
    LockNotExist,
    Uncommitted { lock: Lock, min_commit_ts_pushed: bool },
    Committed { commit_ts: Ts },
    PessimisticRollBack,
    LockNotExistDoNothing,
}

/// check_txn_status.rs:17-68.
fn check_txn_status_from_pessimistic_primary_lock(txn: &mut MvccTxn, reader: &SnapshotReader, primary_key: Key, lock: &Lock, current_ts: Ts, resolving_pessimistic_lock: bool, v: Variant) -> MResult<(Option<TxnStatus>, Option<ReleasedLock>)> {
    assert!(lock.is_pessimistic_lock());
    if lock.is_pessimistic_lock_with_conflict() {
        if let Some(txn_status) = check_determined_txn_status(reader, &primary_key) {
            let released = txn.unlock_key(primary_key, true, 0);
            return Ok((Some(txn_status), released));
        }
    }
    // PORT: rule T2, the lock counts as expired.
    if (physical(lock.ts) + lock.ttl as i64) < physical(current_ts) || EVERY_TTL_EXPIRED {
        return if resolving_pessimistic_lock {
            let released = txn.unlock_key(primary_key, true, 0);
            Ok((Some(TxnStatus::PessimisticRollBack), released))
        } else {
            let released = rollback_lock(txn, reader, primary_key, lock, true, true)?;
            Ok((Some(TxnStatus::TtlExpire), released))
        };
    }
    let _ = v;
    Ok((None, None))
}

/// check_txn_status.rs:92-217.
fn check_txn_status_lock_exists(txn: &mut MvccTxn, reader: &SnapshotReader, primary_key: Key, mut lock: Lock, current_ts: Ts, caller_start_ts: Ts, force_sync_commit: bool, resolving_pessimistic_lock: bool, verify_is_primary: bool, rollback_if_not_exist: bool, v: Variant) -> MResult<(TxnStatus, Option<ReleasedLock>)> {
    if verify_is_primary && primary_key != lock.primary {
        return match (resolving_pessimistic_lock, lock.is_pessimistic_lock()) {
            (false, true) => {
                let txn_status = check_txn_status_missing_lock(txn, reader, primary_key, None, MissingLockAction::rollback(rollback_if_not_exist), resolving_pessimistic_lock, v)?;
                let released = txn.unlock_key(primary_key, true, 0);
                Ok((txn_status, released))
            }
            _ => Err(MvccError::PrimaryMismatch(lock.into_lock_info(primary_key))),
        };
    }

    if lock.use_async_commit {
        if force_sync_commit {
            // fallback is set, check_txn_status treats it as a non-async-commit txn
        } else {
            return Ok((TxnStatus::Uncommitted { lock, min_commit_ts_pushed: false }, None));
        }
    }

    let is_pessimistic_txn = lock.for_update_ts != 0;
    if lock.is_pessimistic_lock() {
        let check_result = check_txn_status_from_pessimistic_primary_lock(txn, reader, primary_key, &lock, current_ts, resolving_pessimistic_lock, v)?;
        if let (Some(txn_status), Some(released_lock)) = check_result {
            return Ok((txn_status, Some(released_lock)));
        }
        assert!(check_result.0.is_none() && check_result.1.is_none());
    } else if (physical(lock.ts) + lock.ttl as i64) < physical(current_ts) || EVERY_TTL_EXPIRED {
        // PORT: rule T2, the lock counts as expired.
        let released = rollback_lock(txn, reader, primary_key, &lock, is_pessimistic_txn, true)?;
        return Ok((TxnStatus::TtlExpire, released));
    }

    if lock.min_commit_ts != 0 && caller_start_ts != TS_MAX && caller_start_ts >= lock.min_commit_ts {
        lock.min_commit_ts = caller_start_ts + 1;
        if lock.min_commit_ts < current_ts {
            lock.min_commit_ts = current_ts;
        }
        txn.put_lock(primary_key, &lock, false);
    }

    let min_commit_ts_pushed = (caller_start_ts != 0 && lock.min_commit_ts > caller_start_ts) || caller_start_ts == TS_MAX;
    Ok((TxnStatus::Uncommitted { lock, min_commit_ts_pushed }, None))
}

/// check_txn_status.rs:223-239.
fn check_determined_txn_status(reader: &SnapshotReader, primary_key: &Key) -> Option<TxnStatus> {
    match reader.get_txn_commit_record(primary_key) {
        TxnCommitRecord::SingleRecord { commit_ts, write } => {
            if write.write_type == WriteType::Rollback { Some(TxnStatus::RolledBack) } else { Some(TxnStatus::Committed { commit_ts }) }
        }
        TxnCommitRecord::OverlappedRollback { .. } => Some(TxnStatus::RolledBack),
        TxnCommitRecord::None { .. } => None,
    }
}

/// check_txn_status.rs:241-297.
fn check_txn_status_missing_lock(txn: &mut MvccTxn, reader: &SnapshotReader, primary_key: Key, mismatch_lock: Option<Lock>, action: MissingLockAction, resolving_pessimistic_lock: bool, v: Variant) -> MResult<TxnStatus> {
    match reader.get_txn_commit_record(&primary_key) {
        TxnCommitRecord::SingleRecord { commit_ts, write } => {
            if write.write_type == WriteType::Rollback { Ok(TxnStatus::RolledBack) } else { Ok(TxnStatus::Committed { commit_ts }) }
        }
        TxnCommitRecord::OverlappedRollback { .. } => Ok(TxnStatus::RolledBack),
        TxnCommitRecord::None { overlapped_write } => {
            if MissingLockAction::ReturnError == action {
                return Err(MvccError::TxnNotFound { start_ts: reader.start_ts, key: primary_key });
            }
            if resolving_pessimistic_lock {
                return Ok(TxnStatus::LockNotExistDoNothing);
            }
            let ts = reader.start_ts;
            if action.collapse_rollback() {
                collapse_prev_rollback(txn, reader, &primary_key);
            }
            if let (Some(l), None) = (mismatch_lock, overlapped_write.as_ref()) {
                txn.mark_rollback_on_mismatching_lock(&primary_key, l, action == MissingLockAction::ProtectedRollback);
            }
            // Insert a Rollback to Write CF in case that a stale prewrite
            // command is received after a cleanup command.
            if v != Variant::F1 {
                // PORT: variant f1 (INTERFACE.md 9) does not execute check_txn_status.rs:289-291.
                if let Some(write) = action.construct_write(ts, overlapped_write) {
                    txn.put_write(primary_key, ts, write);
                }
            }
            Ok(TxnStatus::LockNotExist)
        }
    }
}

/// check_txn_status.rs:299-357.
fn rollback_lock(txn: &mut MvccTxn, reader: &SnapshotReader, key: Key, lock: &Lock, is_pessimistic_txn: bool, collapse_rollback: bool) -> MResult<Option<ReleasedLock>> {
    let overlapped_write = match reader.get_txn_commit_record(&key) {
        TxnCommitRecord::None { overlapped_write } => overlapped_write,
        TxnCommitRecord::SingleRecord { write, commit_ts } if write.write_type != WriteType::Rollback => {
            panic!("txn record found but not expected: {write:?} {commit_ts} {lock:?}")
        }
        _ => return Ok(txn.unlock_key(key, is_pessimistic_txn, 0)),
    };
    // PORT: no short values (rule A2): a Put lock always has its value in `data`.
    if lock.lock_type == LockType::Put {
        txn.delete_value(key, lock.ts);
    }
    let protected: bool = key == lock.primary; // PORT: generation cut (rule A2)
    if let Some(write) = make_rollback(reader.start_ts, protected, overlapped_write) {
        txn.put_write(key, reader.start_ts, write);
    }
    if collapse_rollback {
        collapse_prev_rollback(txn, reader, &key);
    }
    Ok(txn.unlock_key(key, is_pessimistic_txn, 0))
}

/// check_txn_status.rs:423-434.
fn collapse_prev_rollback(txn: &mut MvccTxn, reader: &SnapshotReader, key: &Key) {
    if let Some((commit_ts, write)) = reader.seek_write(key, reader.start_ts) {
        if write.write_type == WriteType::Rollback && !write.is_protected() {
            txn.delete_write(*key, commit_ts);
        }
    }
}

/// check_txn_status.rs:438-455.
fn make_rollback(start_ts: Ts, protected: bool, overlapped_write: Option<OverlappedWrite>) -> Option<Write> {
    match overlapped_write {
        Some(OverlappedWrite { write, gc_fence: _ }) => {
            assert!(start_ts > write.start_ts);
            if protected { Some(write.set_overlapped_rollback(true)) } else { None }
        }
        None => Some(Write::new_rollback(start_ts, protected)),
    }
}

/// check_txn_status.rs:464-496.
#[derive(Debug, Copy, Clone, PartialEq)]
enum MissingLockAction { Rollback, ProtectedRollback, ReturnError }
impl MissingLockAction {
    fn rollback_protect(protect_rollback: bool) -> MissingLockAction { if protect_rollback { MissingLockAction::ProtectedRollback } else { MissingLockAction::Rollback } }
    fn rollback(rollback_if_not_exist: bool) -> MissingLockAction { if rollback_if_not_exist { MissingLockAction::ProtectedRollback } else { MissingLockAction::ReturnError } }
    fn collapse_rollback(&self) -> bool {
        match self {
            MissingLockAction::Rollback => true,
            MissingLockAction::ProtectedRollback => false,
            _ => unreachable!(),
        }
    }
    fn construct_write(&self, ts: Ts, overlapped_write: Option<OverlappedWrite>) -> Option<Write> { make_rollback(ts, !self.collapse_rollback(), overlapped_write) }
}

// --- txn/actions/cleanup.rs:24-135 ---------------------------------------------------------

fn cleanup(txn: &mut MvccTxn, reader: &SnapshotReader, key: Key, current_ts: Ts, protect_rollback: bool, v: Variant) -> MResult<Option<ReleasedLock>> {
    let lock_state = match txn.get_pending_lock(&key) {
        Some(None) => None,
        Some(Some(lock)) => Some(lock),
        None => reader.load_lock(&key),
    };
    match lock_state {
        Some(ref lock) if lock.ts == reader.start_ts => {
            if current_ts != 0 {
                let expire_time = physical(lock.ts) + lock.ttl as i64;
                // PORT: rule T2, the lock counts as expired.
                if expire_time >= physical(current_ts) && !EVERY_TTL_EXPIRED {
                    return Err(MvccError::KeyIsLocked(lock.clone().into_lock_info(key)));
                }
            }
            rollback_lock(txn, reader, key, lock, lock.is_pessimistic_txn(), !protect_rollback)
        }
        lock => match check_txn_status_missing_lock(txn, reader, key, lock, MissingLockAction::rollback_protect(protect_rollback), false, v)? {
            TxnStatus::Committed { commit_ts } => Err(MvccError::Committed { start_ts: reader.start_ts, commit_ts, key }),
            TxnStatus::RolledBack => Ok(None),
            TxnStatus::LockNotExist => Ok(None),
            _ => unreachable!(),
        },
    }
}

// --- txn/actions/commit.rs:19-245 ----------------------------------------------------------

fn handle_lock_not_found(reader: &SnapshotReader, key: Key, commit_ts: Ts) -> MResult<Option<ReleasedLock>> {
    match reader.get_txn_commit_record(&key).info() {
        Some((_, WriteType::Rollback)) | None => Err(MvccError::TxnLockNotFound { start_ts: reader.start_ts, commit_ts, key }),
        Some((_, WriteType::Put)) => Ok(None),
    }
}

fn commit(txn: &mut MvccTxn, reader: &SnapshotReader, key: Key, commit_ts: Ts) -> MResult<Option<ReleasedLock>> {
    let lock_state = match txn.get_pending_lock(&key) {
        Some(None) => None,
        Some(Some(lock)) => Some(lock),
        None => reader.load_lock(&key),
    };
    let (lock, commit) = match lock_state {
        Some(lock) if lock.ts == reader.start_ts => {
            if commit_ts < lock.min_commit_ts {
                return Err(MvccError::CommitTsExpired { start_ts: reader.start_ts, commit_ts, key, min_commit_ts: lock.min_commit_ts });
            }
            if lock.is_pessimistic_lock() { (lock, false) } else { (lock, true) }
        }
        Some(_) => return handle_lock_not_found(reader, key, commit_ts),
        None => return handle_lock_not_found(reader, key, commit_ts),
    };
    if !commit {
        assert!(lock.is_pessimistic_lock());
        return Ok(txn.unlock_key(key, lock.is_pessimistic_txn(), 0));
    }
    let mut write = Write::new(WriteType::from_lock_type(lock.lock_type).unwrap(), reader.start_ts);
    for ts in &lock.rollback_ts {
        if *ts == commit_ts {
            write = write.set_overlapped_rollback(true);
            break;
        }
    }
    txn.put_write(key, commit_ts, write);
    Ok(txn.unlock_key(key, lock.is_pessimistic_txn(), commit_ts))
}

// --- txn/actions/acquire_pessimistic_lock.rs:44-333, 358-464 -------------------------------

/// `PessimisticLockKeyResult`. PORT: locked-with-conflict results cut (allow_lock_with_conflict
/// is false: the client's wake_up_mode is Normal).
#[derive(Clone, Debug, PartialEq)]
enum PessimisticLockKeyResult { Empty, Value(Option<Value>), Waiting }

/// S:storage/types.rs:371-403 `PessimisticLockResults::into_legacy_values_and_not_founds`.
/// PORT: `Existence` is never produced (need_check_existence is false); a missing value is 0,
/// the source's empty value (INTERFACE.md 4: no value is 0).
fn into_legacy_values_and_not_founds(results: Vec<PessimisticLockKeyResult>) -> (Vec<Value>, Vec<bool>) {
    if results.is_empty() {
        return (vec![], vec![]);
    }
    match &results[0] {
        PessimisticLockKeyResult::Empty => {
            results.iter().for_each(|res| assert!(matches!(res, PessimisticLockKeyResult::Empty)));
            (vec![], vec![])
        }
        PessimisticLockKeyResult::Value(_) => {
            let mut not_founds = Vec::with_capacity(results.len());
            let mut values = Vec::with_capacity(results.len());
            for x in results {
                let PessimisticLockKeyResult::Value(v) = x else { panic!("unwrap_value") };
                match v {
                    Some(v) => {
                        not_founds.push(false);
                        values.push(v);
                    }
                    None => {
                        not_founds.push(true);
                        values.push(0);
                    }
                }
            }
            (values, not_founds)
        }
        PessimisticLockKeyResult::Waiting => unreachable!("no lock waiting (rule A4)"),
    }
}
impl PessimisticLockKeyResult {
    fn new_success(need_value: bool, _need_check_existence: bool, val: Option<Value>) -> Self {
        if need_value { PessimisticLockKeyResult::Value(val) } else { PessimisticLockKeyResult::Empty }
    }
}

fn acquire_pessimistic_lock(txn: &mut MvccTxn, reader: &SnapshotReader, cm: &mut ConcurrencyManager, key: Key, primary: Key, should_not_exist: bool, lock_ttl: u64, for_update_ts: Ts, need_value: bool, need_check_existence: bool, min_commit_ts: Ts) -> MResult<PessimisticLockKeyResult> {
    if should_not_exist || need_value || need_check_existence {
        cm.update_max_ts(for_update_ts);
    }
    let need_load_value = need_value;
    let mut val = None;
    if let Some(lock) = reader.load_lock(&key) {
        return handle_existing_exclusive_lock(txn, reader, key, primary, should_not_exist, lock_ttl, for_update_ts, need_value, need_check_existence, min_commit_ts, lock);
    }
    if let Some((commit_ts, write)) = reader.seek_write(&key, TS_MAX) {
        if commit_ts > for_update_ts {
            // PORT: allow_lock_with_conflict is false (WakeUpModeNormal).
            return Err(MvccError::WriteConflict { start_ts: reader.start_ts, conflict_start_ts: write.start_ts, conflict_commit_ts: commit_ts, key, primary, reason: WriteConflictReason::PessimisticRetry });
        }
        if commit_ts == reader.start_ts && (write.write_type == WriteType::Rollback || write.has_overlapped_rollback) {
            assert!(write.has_overlapped_rollback || write.start_ts == commit_ts);
            return Err(MvccError::PessimisticLockRolledBack { start_ts: reader.start_ts, key });
        }
        if commit_ts > reader.start_ts {
            if let Some((older_commit_ts, older_write)) = reader.seek_write(&key, reader.start_ts) {
                if older_commit_ts == reader.start_ts && (older_write.write_type == WriteType::Rollback || older_write.has_overlapped_rollback) {
                    return Err(MvccError::PessimisticLockRolledBack { start_ts: reader.start_ts, key });
                }
            }
        }
        // PORT: check_data_constraint has nothing to check (should_not_exist is false, rule A2);
        // last_change cut.
        if need_value || need_check_existence {
            val = match write.write_type {
                WriteType::Put => if need_load_value { Some(reader.load_data(&key, write)) } else { Some(0) },
                WriteType::Rollback => if need_load_value { reader.get(&key, commit_ts - 1) } else { reader.get_write(&key, commit_ts - 1).map(|_| 0) },
            };
        }
    }
    let lock = PessimisticLock { primary, start_ts: reader.start_ts, ttl: lock_ttl, for_update_ts, min_commit_ts, is_locked_with_conflict: false };
    // PORT: lock_only_if_exists is false and shared locks are cut.
    txn.put_pessimistic_lock(key, lock, true);
    Ok(PessimisticLockKeyResult::new_success(need_value, need_check_existence, val))
}

fn handle_existing_exclusive_lock(txn: &mut MvccTxn, reader: &SnapshotReader, key: Key, primary: Key, should_not_exist: bool, lock_ttl: u64, for_update_ts: Ts, need_value: bool, need_check_existence: bool, min_commit_ts: Ts, lock: Lock) -> MResult<PessimisticLockKeyResult> {
    if lock.ts != reader.start_ts {
        return Err(MvccError::KeyIsLocked(lock.into_lock_info(key)));
    }
    if !lock.is_pessimistic_lock() {
        return Err(MvccError::LockTypeNotMatch { start_ts: reader.start_ts, key, pessimistic: false });
    }
    let mut val = None;
    let need_load_value = need_value;
    if need_load_value || need_check_existence || should_not_exist {
        if let Some((write, _commit_ts)) = reader.get_write_with_commit_ts(&key, for_update_ts) {
            if need_load_value {
                val = Some(reader.load_data(&key, write));
            } else if need_check_existence {
                val = Some(0);
            }
        }
    }
    let lock_to_write = PessimisticLock { primary, start_ts: reader.start_ts, ttl: lock_ttl, for_update_ts, min_commit_ts, is_locked_with_conflict: lock.is_pessimistic_lock_with_conflict() };
    if for_update_ts > lock.for_update_ts {
        txn.put_pessimistic_lock(key, lock_to_write, false);
    }
    Ok(PessimisticLockKeyResult::new_success(need_value, need_check_existence, val))
}

// --- txn/actions/prewrite.rs ---------------------------------------------------------------

#[derive(Clone, Debug)]
enum CommitKind { TwoPc, OnePc(Ts), Async(Ts) }

#[derive(Clone, Debug)]
enum SrvTransactionKind { Optimistic(bool), Pessimistic(Ts) }

/// prewrite.rs:288-324.
#[derive(Clone, Debug)]
struct TransactionProperties { start_ts: Ts, kind: SrvTransactionKind, commit_kind: CommitKind, primary: Key, txn_size: u64, lock_ttl: u64, min_commit_ts: Ts, is_retry_request: bool }
impl TransactionProperties {
    fn max_commit_ts(&self) -> Ts {
        match &self.commit_kind { CommitKind::TwoPc => unreachable!(), CommitKind::OnePc(ts) => *ts, CommitKind::Async(ts) => *ts }
    }
    fn is_pessimistic(&self) -> bool { matches!(self.kind, SrvTransactionKind::Pessimistic(_)) }
    fn for_update_ts(&self) -> Ts { match &self.kind { SrvTransactionKind::Optimistic(_) => 0, SrvTransactionKind::Pessimistic(ts) => *ts } }
}

#[derive(Clone, Copy)]
enum LockStatus { Locked(Ts), Pessimistic(Ts), None }
impl LockStatus { fn has_pessimistic_lock(&self) -> bool { matches!(self, LockStatus::Pessimistic(_)) } }

/// prewrite.rs:358-434 `PrewriteMutation`. PORT: should_not_exist / should_not_write are false
/// (Put only), assertion None, last_change and shared locks cut (rule A2).
struct PrewriteMutation<'a> {
    key: Key,
    value: Option<Value>,
    secondary_keys: &'a Option<Vec<Key>>,
    min_commit_ts: Ts,
    pessimistic_action: PessimisticAction,
    lock_type: Option<LockType>,
    lock_ttl: u64,
    txn_props: &'a TransactionProperties,
}

impl<'a> PrewriteMutation<'a> {
    fn from_mutation(key: Key, value: Value, secondary_keys: &'a Option<Vec<Key>>, pessimistic_action: PessimisticAction, txn_props: &'a TransactionProperties) -> PrewriteMutation<'a> {
        PrewriteMutation { key, value: Some(value), secondary_keys, min_commit_ts: txn_props.min_commit_ts, pessimistic_action, lock_type: Some(LockType::Put), lock_ttl: txn_props.lock_ttl, txn_props }
    }

    /// prewrite.rs:427-434.
    fn lock_info(&self, lock: Lock) -> LockInfo {
        let mut info = lock.into_lock_info(self.key);
        if self.txn_props.is_pessimistic() {
            info.lock_ttl = 0;
        }
        info
    }

    /// prewrite.rs:435-561.
    fn check_lock(&mut self, lock: Lock, pessimistic_action: PessimisticAction, expected_for_update_ts: Option<Ts>) -> MResult<LockStatus> {
        if lock.ts != self.txn_props.start_ts {
            if matches!(pessimistic_action, PessimisticAction::DoPessimisticCheck) {
                return Err(MvccError::PessimisticLockNotFound { start_ts: self.txn_props.start_ts, key: self.key, reason: "LockTsMismatch" });
            }
            return Err(MvccError::KeyIsLocked(self.lock_info(lock)));
        }
        if lock.is_pessimistic_lock() {
            if !self.txn_props.is_pessimistic() {
                return Err(MvccError::LockTypeNotMatch { start_ts: self.txn_props.start_ts, key: self.key, pessimistic: true });
            }
            if let Some(ts) = expected_for_update_ts {
                if lock.for_update_ts != ts {
                    return Err(MvccError::PessimisticLockNotFound { start_ts: self.txn_props.start_ts, key: self.key, reason: "LockForUpdateTsMismatch" });
                }
            }
            self.lock_ttl = std::cmp::max(self.lock_ttl, lock.ttl);
            self.min_commit_ts = std::cmp::max(self.min_commit_ts, lock.min_commit_ts);
            return Ok(LockStatus::Pessimistic(lock.for_update_ts));
        }
        // PORT: generation checks (538-551) cut (rule A2).
        // Duplicated command. No need to overwrite the lock and data.
        let min_commit_ts = if lock.use_async_commit { lock.min_commit_ts } else { 0 };
        Ok(LockStatus::Locked(min_commit_ts))
    }

    /// prewrite.rs:563-648.
    fn check_for_newer_version(&mut self, reader: &SnapshotReader, v: Variant) -> MResult<Option<(Write, Ts)>> {
        let mut seek_ts = TS_MAX;
        while let Some((commit_ts, write)) = reader.seek_write(&self.key, seek_ts) {
            if commit_ts == self.txn_props.start_ts && (write.write_type == WriteType::Rollback || write.has_overlapped_rollback) {
                self.write_conflict_error(&write, commit_ts, WriteConflictReason::SelfRolledBack)?;
            }
            match self.txn_props.kind {
                SrvTransactionKind::Optimistic(_) => {
                    // PORT: variant f2 (INTERFACE.md 9) skips prewrite.rs:584-592.
                    if v != Variant::F2 && commit_ts > self.txn_props.start_ts {
                        self.write_conflict_error(&write, commit_ts, WriteConflictReason::Optimistic)?;
                    }
                }
                SrvTransactionKind::Pessimistic(for_update_ts) => {
                    if let PessimisticAction::DoConstraintCheck = self.pessimistic_action {
                        if commit_ts > self.txn_props.start_ts {
                            self.write_conflict_error(&write, commit_ts, WriteConflictReason::LazyUniquenessCheck)?;
                        }
                    }
                    if commit_ts > for_update_ts {
                        if write.write_type == WriteType::Rollback {
                            seek_ts = commit_ts - 1;
                            continue;
                        }
                        return Err(MvccError::PessimisticLockNotFound { start_ts: self.txn_props.start_ts, key: self.key, reason: "NonLockKeyConflict" });
                    }
                }
            }
            // PORT: check_data_constraint has nothing to check (Put only, rule A2).
            return Ok(Some((write, commit_ts)));
        }
        Ok(None)
    }

    /// prewrite.rs:650-770.
    fn write_lock(self, lock_status: LockStatus, txn: &mut MvccTxn, cm: &ConcurrencyManager, is_new_lock: bool) -> MResult<Ts> {
        let mut try_one_pc = self.try_one_pc();
        let for_update_ts_to_write = match (self.txn_props.for_update_ts(), lock_status) {
            (from_prewrite_req, LockStatus::Pessimistic(from_pessimistic_lock)) => std::cmp::max(from_prewrite_req, from_pessimistic_lock),
            (for_update_ts_from_req, _) => for_update_ts_from_req,
        };
        let mut lock = Lock::new(self.lock_type.unwrap(), self.txn_props.primary, self.txn_props.start_ts, self.lock_ttl, for_update_ts_to_write, self.txn_props.txn_size, self.min_commit_ts);
        if let Some(value) = self.value {
            // PORT: no short values (rule A2): the value goes to `data`.
            txn.put_value(self.key, self.txn_props.start_ts, value);
        }
        if let Some(secondary_keys) = self.secondary_keys {
            lock.use_async_commit = true;
            lock.secondaries = secondary_keys.to_owned();
        } else if try_one_pc {
            lock.use_one_pc = true;
        }
        let final_min_commit_ts = if lock.use_async_commit || try_one_pc {
            let res = async_commit_timestamps(&self.key, &mut lock, self.txn_props.start_ts, self.txn_props.for_update_ts(), self.txn_props.max_commit_ts(), cm);
            if let Err(MvccError::CommitTsTooLarge { .. }) = &res {
                try_one_pc = false;
                lock.use_one_pc = false;
                lock.use_async_commit = false;
                lock.secondaries = Vec::new();
            }
            res
        } else {
            Ok(0)
        };
        if try_one_pc {
            txn.put_locks_for_1pc(self.key, lock, lock_status.has_pessimistic_lock());
        } else {
            txn.put_lock(self.key, &lock, is_new_lock);
        }
        final_min_commit_ts
    }

    fn write_conflict_error(&self, write: &Write, commit_ts: Ts, reason: WriteConflictReason) -> MResult<()> {
        Err(MvccError::WriteConflict { start_ts: self.txn_props.start_ts, conflict_start_ts: write.start_ts, conflict_commit_ts: commit_ts, key: self.key, primary: self.txn_props.primary, reason })
    }

    /// prewrite.rs:896-912.
    fn skip_constraint_check(&self) -> bool {
        match &self.txn_props.kind {
            SrvTransactionKind::Optimistic(s) => *s,
            SrvTransactionKind::Pessimistic(_) => match self.pessimistic_action {
                PessimisticAction::DoPessimisticCheck => true,
                PessimisticAction::SkipPessimisticCheck => !self.txn_props.is_retry_request,
                PessimisticAction::DoConstraintCheck => false,
            },
        }
    }
    fn need_min_commit_ts(&self) -> bool { matches!(&self.txn_props.commit_kind, CommitKind::Async(_) | CommitKind::OnePc(_)) }
    fn try_one_pc(&self) -> bool { matches!(&self.txn_props.commit_kind, CommitKind::OnePc(_)) }
}

/// prewrite.rs:928-979. PORT: the in-memory lock of the key guard is the cell's lock (rule A3);
/// fail points cut.
fn async_commit_timestamps(_key: &Key, lock: &mut Lock, start_ts: Ts, for_update_ts: Ts, max_commit_ts: Ts, cm: &ConcurrencyManager) -> MResult<Ts> {
    let max_ts = cm.max_ts();
    let min_commit_ts = std::cmp::max(std::cmp::max(max_ts, start_ts), for_update_ts) + 1;
    let min_commit_ts = std::cmp::max(lock.min_commit_ts, min_commit_ts);
    if max_commit_ts != 0 && min_commit_ts > max_commit_ts {
        return Err(MvccError::CommitTsTooLarge { start_ts, min_commit_ts, max_commit_ts });
    }
    lock.min_commit_ts = min_commit_ts;
    Ok(min_commit_ts)
}

/// prewrite.rs:984-1035. PORT: assertions and last_change cut.
fn amend_pessimistic_lock(mutation: &mut PrewriteMutation, reader: &SnapshotReader) -> MResult<()> {
    let write = reader.seek_write(&mutation.key, TS_MAX);
    if let Some((commit_ts, _write)) = write.as_ref() {
        if *commit_ts >= reader.start_ts {
            return Err(MvccError::PessimisticLockNotFound { start_ts: reader.start_ts, key: mutation.key, reason: "LockMissingAmendFail" });
        }
    }
    Ok(())
}

/// prewrite.rs:36-56, 58-286 `prewrite` / `prewrite_with_generation` (generation 0). PORT: the
/// returned OldValue is cut (CDC old values, rule A6).
fn prewrite(txn: &mut MvccTxn, reader: &SnapshotReader, cm: &mut ConcurrencyManager, txn_props: &TransactionProperties, key: Key, value: Value, secondary_keys: &Option<Vec<Key>>, pessimistic_action: PessimisticAction, expected_for_update_ts: Option<Ts>, v: Variant) -> MResult<Ts> {
    let mut mutation = PrewriteMutation::from_mutation(key, value, secondary_keys, pessimistic_action, txn_props);
    // PORT: should_not_exist is false (no Insert, rule A2), so no update_max_ts here (72-77).
    let mut lock_amended = false;
    let lock_status = match reader.load_lock(&mutation.key) {
        Some(lock) => mutation.check_lock(lock, pessimistic_action, expected_for_update_ts)?,
        None if matches!(pessimistic_action, PessimisticAction::DoPessimisticCheck) => {
            amend_pessimistic_lock(&mut mutation, reader)?;
            lock_amended = true;
            LockStatus::None
        }
        None => LockStatus::None,
    };
    if let LockStatus::Locked(ts) = lock_status {
        return Ok(ts);
    }
    let _prev_write = if !mutation.skip_constraint_check() { mutation.check_for_newer_version(reader, v)? } else { None };
    // PORT: check_assertion returns at once (Assertion::None), should_not_write is false,
    // need_old_value is false (196-263).
    let is_new_lock = !matches!(pessimistic_action, PessimisticAction::DoPessimisticCheck) || lock_amended;
    let _ = &cm;
    let final_min_commit_ts = mutation.write_lock(lock_status, txn, cm, is_new_lock)?;
    Ok(final_min_commit_ts)
}

/// txn/actions/common.rs:70-91.
fn check_committed_record_on_err(prewrite_result: MResult<Ts>, txn: &mut MvccTxn, reader: &SnapshotReader, key: &Key) -> MResult<(Vec<MResult<()>>, Ts)> {
    match reader.get_txn_commit_record(key) {
        TxnCommitRecord::SingleRecord { commit_ts, write } if write.write_type != WriteType::Rollback => {
            txn.clear();
            Ok((vec![], commit_ts))
        }
        _ => Err(prewrite_result.unwrap_err()),
    }
}

// --- txn/commands ----------------------------------------------------------------------------

/// commands/prewrite.rs:44.
const FORWARD_MIN_MUTATIONS_NUM: usize = 12;

/// commands/prewrite.rs:482-500 `Prewriter`. PORT: Optimistic / Pessimistic `PrewriteKind`s are
/// the enum `SrvTransactionKind` with the per-mutation pessimistic action.
struct Prewriter {
    kind: SrvTransactionKind,
    mutations: Vec<(Key, Value, PessimisticAction)>,
    primary: Key,
    start_ts: Ts,
    lock_ttl: u64,
    txn_size: u64,
    min_commit_ts: Ts,
    max_commit_ts: Ts,
    secondary_keys: Option<Vec<Key>>,
    try_one_pc: bool,
    is_retry_request: bool,
}

/// `PrewriteResult`.
struct PrewriteResult { locks: Vec<MResult<()>>, min_commit_ts: Ts, one_pc_commit_ts: Ts }

impl Prewriter {
    /// commands/prewrite.rs:501-562.
    fn process_write(mut self, snap: &Snapshot, cm: &mut ConcurrencyManager, v: Variant) -> MResult<(PrewriteResult, Option<Vec<Modify>>)> {
        // PORT: the txn_status_cache check (507-533) is cut (rule A6).
        self.can_skip_constraint_check(snap);
        // PORT: check_max_ts_synced (568-579) cut (rule A6).
        let mut txn = MvccTxn::new(self.start_ts);
        let reader = SnapshotReader { start_ts: self.start_ts, snap };
        let res = self.prewrite(&mut txn, &reader, cm, v);
        let (locks, final_min_commit_ts) = res?;
        Ok(self.write_result(locks, txn, final_min_commit_ts))
    }

    /// commands/prewrite.rs:840-862 (Optimistic::can_skip_constraint_check).
    fn can_skip_constraint_check(&mut self, snap: &Snapshot) {
        if let SrvTransactionKind::Optimistic(_) = self.kind {
            if self.mutations.len() > FORWARD_MIN_MUTATIONS_NUM {
                self.mutations.sort_by(|a, b| a.0.cmp(&b.0));
                let left_key = self.mutations.first().unwrap().0;
                let right_key = self.mutations.last().unwrap().0;
                if !snap.range(left_key..=right_key).any(|(_, c)| !c.writes.is_empty()) {
                    self.kind = SrvTransactionKind::Optimistic(true);
                }
            }
        }
    }

    /// commands/prewrite.rs:585-719.
    fn prewrite(&mut self, txn: &mut MvccTxn, reader: &SnapshotReader, cm: &mut ConcurrencyManager, v: Variant) -> MResult<(Vec<MResult<()>>, Ts)> {
        let commit_kind = match (&self.secondary_keys, self.try_one_pc) {
            (_, true) => CommitKind::OnePc(self.max_commit_ts),
            (&Some(_), false) => CommitKind::Async(self.max_commit_ts),
            (&None, false) => CommitKind::TwoPc,
        };
        let mut props = TransactionProperties { start_ts: self.start_ts, kind: self.kind.clone(), commit_kind, primary: self.primary, txn_size: self.txn_size, lock_ttl: self.lock_ttl, min_commit_ts: self.min_commit_ts, is_retry_request: self.is_retry_request };
        let mut async_commit_pk: Option<Key> = self.secondary_keys.as_ref().filter(|keys| !keys.is_empty()).map(|_| self.primary);
        let mut final_min_commit_ts: Ts = 0;
        let mut locks = Vec::new();
        for (key, value, pessimistic_action) in std::mem::take(&mut self.mutations) {
            let expected_for_update_ts = None;
            let mut secondaries = self.secondary_keys.as_ref().map(|_| vec![]);
            if Some(key) == async_commit_pk {
                secondaries = self.secondary_keys.clone();
            }
            let need_min_commit_ts = secondaries.is_some() || self.try_one_pc;
            let prewrite_result = prewrite(txn, reader, cm, &props, key, value, &secondaries, pessimistic_action, expected_for_update_ts, v);
            match prewrite_result {
                Ok(ts) if !(need_min_commit_ts && ts == 0) => {
                    if need_min_commit_ts && final_min_commit_ts < ts {
                        final_min_commit_ts = ts;
                    }
                }
                Ok(..) => {
                    props.commit_kind = CommitKind::TwoPc;
                    async_commit_pk = None;
                    self.secondary_keys = None;
                    self.try_one_pc = false;
                    fallback_1pc_locks(txn);
                    final_min_commit_ts = 0;
                }
                Err(MvccError::WriteConflict { start_ts, conflict_commit_ts, .. }) if conflict_commit_ts > start_ts => {
                    return check_committed_record_on_err(prewrite_result, txn, reader, &key);
                }
                Err(MvccError::PessimisticLockNotFound { .. }) => {
                    return check_committed_record_on_err(prewrite_result, txn, reader, &key);
                }
                Err(MvccError::CommitTsTooLarge { .. }) => {
                    if let Ok(res) = check_committed_record_on_err(prewrite_result, txn, reader, &key) {
                        return Ok(res);
                    }
                    props.commit_kind = CommitKind::TwoPc;
                    async_commit_pk = None;
                    self.secondary_keys = None;
                    self.try_one_pc = false;
                    fallback_1pc_locks(txn);
                    final_min_commit_ts = 0;
                }
                Err(MvccError::KeyIsLocked(..)) => match check_committed_record_on_err(prewrite_result, txn, reader, &key) {
                    Ok(res) => return Ok(res),
                    Err(e) => locks.push(Err(e)),
                },
                Err(e) => return Err(e),
            }
        }
        Ok((locks, final_min_commit_ts))
    }

    /// commands/prewrite.rs:722-806.
    fn write_result(self, locks: Vec<MResult<()>>, mut txn: MvccTxn, final_min_commit_ts: Ts) -> (PrewriteResult, Option<Vec<Modify>>) {
        let async_commit_ts = if self.secondary_keys.is_some() { final_min_commit_ts } else { 0 };
        if locks.is_empty() {
            let one_pc_commit_ts = one_pc_commit(self.try_one_pc, &mut txn, final_min_commit_ts);
            (PrewriteResult { locks: vec![], min_commit_ts: async_commit_ts, one_pc_commit_ts }, Some(txn.modifies))
        } else {
            // Skip write stage if some keys are locked.
            (PrewriteResult { locks, min_commit_ts: async_commit_ts, one_pc_commit_ts: 0 }, None)
        }
    }
}

/// commands/prewrite.rs:953-969.
fn one_pc_commit(try_one_pc: bool, txn: &mut MvccTxn, final_min_commit_ts: Ts) -> Ts {
    if try_one_pc {
        assert_ne!(final_min_commit_ts, 0);
        handle_1pc_locks(txn, final_min_commit_ts);
        final_min_commit_ts
    } else {
        assert!(txn.locks_for_1pc.is_empty());
        0
    }
}

/// commands/prewrite.rs:971-991.
fn handle_1pc_locks(txn: &mut MvccTxn, commit_ts: Ts) {
    for (key, lock, delete_pessimistic_lock) in std::mem::take(&mut txn.locks_for_1pc) {
        let write = Write::new(WriteType::from_lock_type(lock.lock_type).unwrap(), txn.start_ts);
        txn.put_write(key, commit_ts, write);
        if delete_pessimistic_lock {
            txn.unlock_key(key, true, commit_ts);
        }
    }
}

/// commands/prewrite.rs:994-1000.
fn fallback_1pc_locks(txn: &mut MvccTxn) {
    for (key, mut lock, remove_pessimistic_lock) in std::mem::take(&mut txn.locks_for_1pc) {
        lock.use_one_pc = false;
        let is_new_lock = !remove_pessimistic_lock;
        txn.put_lock(key, &lock, is_new_lock);
    }
}

/// A region (rule A1): its cells, its `max_ts` (rule A3), the variant.
struct Region { cells: Snapshot, cm: ConcurrencyManager, variant: Variant }

/// What the scheduler does with a command's `ProcessResult` (S:storage/txn/mod.rs): reply to
/// the sender, or schedule the next command.
enum Handled { Reply(Response), NextCommand(NextCommand) }

impl Region {
    /// The scheduler: one command at a time, evaluated on a snapshot, its writes applied at the
    /// end (rule A1). The responses are built as S:server/service/kv.rs:2496-2613 and
    /// future_get 1645-1697 build them (NOTES.md D4); region errors cannot occur (rule A1).
    fn handle(&mut self, req: Request) -> Handled {
        let v = self.variant;
        let snap = self.cells.clone();
        let (resp, modifies) = match req {
            Request::Get(r) => (Response::Get(self.get(&snap, r)), None),
            Request::PessimisticLock(r) => {
                let (res, m) = process_acquire_pessimistic_lock(&snap, &mut self.cm, r.clone());
                let resp = match res {
                    // S:server/service/kv.rs:2503-2533, WakeUpModeNormal (the client's mode).
                    Ok(results) => {
                        let (values, not_founds) = into_legacy_values_and_not_founds(results);
                        PessimisticLockResponse { errors: vec![], values, not_founds }
                    }
                    Err(e) => PessimisticLockResponse { errors: vec![extract_key_error(&e)], values: vec![], not_founds: vec![] },
                };
                (Response::PessimisticLock(resp), m)
            }
            Request::Prewrite(r) => {
                let prewriter = prewriter_from_request(r);
                match prewriter.process_write(&snap, &mut self.cm, v) {
                    Ok((pr, m)) => {
                        let errors = pr.locks.iter().filter_map(|x| x.as_ref().err().map(extract_key_error)).collect();
                        (Response::Prewrite(PrewriteResponse { errors, min_commit_ts: pr.min_commit_ts, one_pc_commit_ts: pr.one_pc_commit_ts }), m)
                    }
                    Err(e) => (Response::Prewrite(PrewriteResponse { errors: vec![extract_key_error(&e)], min_commit_ts: 0, one_pc_commit_ts: 0 }), None),
                }
            }
            Request::Commit(r) => match process_commit(&snap, &r) {
                Ok(m) => (Response::Commit(CommitResponse { error: None, commit_version: r.commit_version }), Some(m)),
                Err(e) => (Response::Commit(CommitResponse { error: Some(extract_key_error(&e)), commit_version: 0 }), None),
            },
            Request::BatchRollback(r) => match process_rollback(&snap, &r, v) {
                Ok(m) => (Response::BatchRollback(BatchRollbackResponse { error: None }), Some(m)),
                Err(e) => (Response::BatchRollback(BatchRollbackResponse { error: Some(extract_key_error(&e)) }), None),
            },
            Request::PessimisticRollback(r) => match process_pessimistic_rollback(&snap, &r) {
                Ok(m) => (Response::PessimisticRollback(PessimisticRollbackResponse { errors: vec![] }), Some(m)),
                Err(e) => (Response::PessimisticRollback(PessimisticRollbackResponse { errors: vec![extract_key_error(&e)] }), None),
            },
            Request::CheckTxnStatus(r) => match process_check_txn_status(&snap, &mut self.cm, &r, v) {
                Ok((status, m)) => (Response::CheckTxnStatus(txn_status_response(status)), Some(m)),
                Err(e) => (Response::CheckTxnStatus(CheckTxnStatusResponse { error: Some(extract_key_error(&e)), lock_ttl: 0, commit_version: 0, action: Action::NoAction, lock_info: None }), None),
            },
            Request::CheckSecondaryLocks(r) => match process_check_secondary_locks(&snap, &mut self.cm, &r) {
                Ok((status, m)) => {
                    let resp = match status {
                        SecondaryLocksStatusSrv::Locked(locks) => CheckSecondaryLocksResponse { error: None, locks, commit_ts: 0 },
                        SecondaryLocksStatusSrv::Committed(ts) => CheckSecondaryLocksResponse { error: None, locks: vec![], commit_ts: ts },
                        SecondaryLocksStatusSrv::RolledBack => CheckSecondaryLocksResponse { error: None, locks: vec![], commit_ts: 0 },
                    };
                    (Response::CheckSecondaryLocks(resp), Some(m))
                }
                Err(e) => (Response::CheckSecondaryLocks(CheckSecondaryLocksResponse { error: Some(extract_key_error(&e)), locks: vec![], commit_ts: 0 }), None),
            },
            Request::ResolveLock(r) => {
                // commands/mod.rs:354-384: no keys -> ResolveLockReadPhase.
                let mut txn_status = BTreeMap::new();
                assert!(r.start_version > 0);
                txn_status.insert(r.start_version, r.commit_version);
                match process_resolve_lock_readphase(&snap, txn_status) {
                    // ProcessResult::Res: the reply.
                    None => (Response::ResolveLock(ResolveLockResponse { error: None }), None),
                    // ProcessResult::NextCommand: no reply yet; a read command writes nothing.
                    Some(next_cmd) => return Handled::NextCommand(next_cmd),
                }
            }
        };
        if let Some(m) = modifies {
            apply_modifies(&mut self.cells, m);
        }
        Handled::Reply(resp)
    }

    /// The scheduler running a command scheduled by `ProcessResult::NextCommand`, as a command
    /// of its own on a new snapshot (rule A1).
    fn handle_next(&mut self, cmd: NextCommand) -> Response {
        let v = self.variant;
        let snap = self.cells.clone();
        match cmd {
            NextCommand::ResolveLock { txn_status, key_locks } => match process_resolve_lock(&snap, txn_status, key_locks, v) {
                Ok(m) => {
                    apply_modifies(&mut self.cells, m);
                    Response::ResolveLock(ResolveLockResponse { error: None })
                }
                Err(e) => Response::ResolveLock(ResolveLockResponse { error: Some(extract_key_error(&e)) }),
            },
        }
    }

    /// storage/mod.rs:634-826 `get_entry` with `prepare_snap_ctx` 3573-3634 and
    /// point_getter.rs:173-421.
    fn get(&mut self, snap: &Snapshot, r: GetRequest) -> GetResponse {
        let key = r.key;
        let start_ts = r.version;
        // prepare_snap_ctx: update max_ts (3585) and check the in-memory lock (3596).
        self.cm.update_max_ts(start_ts);
        // PORT: the in-memory lock table and the persisted lock are the same cell lock (rule
        // A3): `read_key_check` and `load_and_check_lock` are one check.
        let reader = SnapshotReader { start_ts, snap };
        if let Some(lock) = reader.load_lock(&key) {
            if let Err(e) = check_ts_conflict_si(&lock, &key, start_ts) {
                return GetResponse { error: Some(extract_key_error(&e)), value: 0, not_found: false };
            }
        }
        // PointGetter::load_data: the newest Put with commit_ts <= start_ts.
        match reader.get(&key, start_ts) {
            Some(value) => GetResponse { error: None, value, not_found: false },
            None => GetResponse { error: None, value: 0, not_found: true },
        }
    }
}

/// commands/mod.rs:164-213 `From<PrewriteRequest>`.
fn prewriter_from_request(req: PrewriteRequest) -> Prewriter {
    let secondary_keys = if req.use_async_commit { Some(req.secondaries.clone()) } else { None };
    for m in &req.mutations {
        if m.op != PbOp::Put {
            panic!("PORT: only Put mutations are in the cut (rule A2), got {:?}", m.op);
        }
    }
    if req.for_update_ts == 0 {
        Prewriter {
            kind: SrvTransactionKind::Optimistic(false),
            mutations: req.mutations.iter().map(|m| (m.key, m.value, PessimisticAction::SkipPessimisticCheck)).collect(),
            primary: req.primary_lock, start_ts: req.start_version, lock_ttl: req.lock_ttl, txn_size: req.txn_size,
            min_commit_ts: req.min_commit_ts, max_commit_ts: req.max_commit_ts, secondary_keys, try_one_pc: req.try_one_pc, is_retry_request: false,
        }
    } else {
        Prewriter {
            kind: SrvTransactionKind::Pessimistic(req.for_update_ts),
            mutations: req.mutations.iter().zip(req.pessimistic_actions.iter()).map(|(m, a)| (m.key, m.value, *a)).collect(),
            primary: req.primary_lock, start_ts: req.start_version, lock_ttl: req.lock_ttl, txn_size: req.txn_size,
            min_commit_ts: req.min_commit_ts, max_commit_ts: req.max_commit_ts, secondary_keys, try_one_pc: req.try_one_pc, is_retry_request: false,
        }
    }
}

/// commands/acquire_pessimistic_lock.rs:82-220. PORT: wait_timeout 0 means no wait (rule A4):
/// KeyIsLocked is returned at once.
fn process_acquire_pessimistic_lock(snap: &Snapshot, cm: &mut ConcurrencyManager, req: PessimisticLockRequest) -> (MResult<Vec<PessimisticLockKeyResult>>, Option<Vec<Modify>>) {
    let start_ts = req.start_version;
    let mut txn = MvccTxn::new(start_ts);
    let reader = SnapshotReader { start_ts, snap };
    let mut res: Vec<PessimisticLockKeyResult> = Vec::new();
    let mut encountered_locks: Vec<LockInfo> = vec![];
    for m in &req.mutations {
        // commands/mod.rs:216-229: Op::PessimisticLock, assertion None (should_not_exist false).
        let k = m.key;
        match acquire_pessimistic_lock(&mut txn, &reader, cm, k, req.primary_lock, false, req.lock_ttl, req.for_update_ts, req.return_values, false, req.min_commit_ts) {
            Ok(key_res) => res.push(key_res),
            Err(MvccError::KeyIsLocked(lock_info)) => {
                encountered_locks.push(lock_info);
                txn.clear();
                res.clear();
                res.push(PessimisticLockKeyResult::Waiting);
                break;
            }
            Err(e) => return (Err(e), None),
        }
    }
    let modifies = txn.modifies;
    let wait_timeout_is_none = req.wait_timeout == 0; // WaitTimeout::from_encoded(0) = None
    if !encountered_locks.is_empty() && wait_timeout_is_none {
        let lock_info = encountered_locks.remove(0);
        return (Err(MvccError::KeyIsLocked(lock_info)), Some(modifies));
    }
    (Ok(res), Some(modifies))
}

/// commands/commit.rs:53-97.
fn process_commit(snap: &Snapshot, req: &CommitRequest) -> MResult<Vec<Modify>> {
    if req.commit_version <= req.start_version {
        return Err(MvccError::InvalidTxnTso { start_ts: req.start_version, commit_ts: req.commit_version });
    }
    let mut txn = MvccTxn::new(req.start_version);
    let reader = SnapshotReader { start_ts: req.start_version, snap };
    for k in &req.keys {
        commit(&mut txn, &reader, *k, req.commit_version)?;
    }
    Ok(txn.modifies)
}

/// commands/rollback.rs:50-81.
fn process_rollback(snap: &Snapshot, req: &BatchRollbackRequest, v: Variant) -> MResult<Vec<Modify>> {
    let mut txn = MvccTxn::new(req.start_version);
    let reader = SnapshotReader { start_ts: req.start_version, snap };
    for k in &req.keys {
        cleanup(&mut txn, &reader, *k, 0, false, v)?;
    }
    Ok(txn.modifies)
}

/// commands/pessimistic_rollback.rs:60-156. PORT: the read phase for an empty key list
/// (pessimistic_rollback_read_phase.rs) is never needed: the client never sends one.
fn process_pessimistic_rollback(snap: &Snapshot, req: &PessimisticRollbackRequest) -> MResult<Vec<Modify>> {
    assert!(!req.keys.is_empty(), "PessimisticRollbackReadPhase is not needed by the client");
    let mut txn = MvccTxn::new(req.start_version);
    let reader = SnapshotReader { start_ts: req.start_version, snap };
    for key in &req.keys {
        if let Some(lock) = reader.load_lock(key) {
            if lock.is_pessimistic_lock() && lock.ts == req.start_version && lock.for_update_ts <= req.for_update_ts {
                txn.unlock_key(*key, true, 0);
            }
        }
    }
    Ok(txn.modifies)
}

/// commands/check_txn_status.rs:87-189.
fn process_check_txn_status(snap: &Snapshot, cm: &mut ConcurrencyManager, req: &CheckTxnStatusRequest, v: Variant) -> MResult<(TxnStatus, Vec<Modify>)> {
    let mut new_max_ts = req.lock_ts;
    if req.current_ts != TS_MAX && req.current_ts > new_max_ts {
        new_max_ts = req.current_ts;
    }
    if req.caller_start_ts != TS_MAX && req.caller_start_ts > new_max_ts {
        new_max_ts = req.caller_start_ts;
    }
    cm.update_max_ts(new_max_ts);
    let mut txn = MvccTxn::new(req.lock_ts);
    let reader = SnapshotReader { start_ts: req.lock_ts, snap };
    let (txn_status, _released) = match reader.load_lock(&req.primary_key) {
        Some(lock) if lock.ts == req.lock_ts => check_txn_status_lock_exists(&mut txn, &reader, req.primary_key, lock, req.current_ts, req.caller_start_ts, req.force_sync_commit, req.resolving_pessimistic_lock, req.verify_is_primary, req.rollback_if_not_exist, v)?,
        l => (check_txn_status_missing_lock(&mut txn, &reader, req.primary_key, l, MissingLockAction::rollback(req.rollback_if_not_exist), req.resolving_pessimistic_lock, v)?, None),
    };
    Ok((txn_status, txn.modifies))
}

/// The CheckTxnStatusResponse built from a `TxnStatus` (S:server/service/kv.rs:2578-2601).
fn txn_status_response(status: TxnStatus) -> CheckTxnStatusResponse {
    let mut resp = CheckTxnStatusResponse { error: None, lock_ttl: 0, commit_version: 0, action: Action::NoAction, lock_info: None };
    match status {
        TxnStatus::RolledBack => resp.action = Action::NoAction,
        TxnStatus::TtlExpire => resp.action = Action::TtlExpireRollback,
        TxnStatus::LockNotExist => resp.action = Action::LockNotExistRollback,
        TxnStatus::Committed { commit_ts } => resp.commit_version = commit_ts,
        TxnStatus::Uncommitted { lock, min_commit_ts_pushed } => {
            if min_commit_ts_pushed {
                resp.action = Action::MinCommitTsPushed;
            }
            resp.lock_ttl = lock.ttl;
            let primary = lock.primary;
            resp.lock_info = Some(lock.into_lock_info(primary));
        }
        TxnStatus::PessimisticRollBack => resp.action = Action::TtlExpirePessimisticRollback,
        TxnStatus::LockNotExistDoNothing => resp.action = Action::LockNotExistDoNothing,
    }
    resp
}

/// commands/check_secondary_locks.rs:56-61 (server side `SecondaryLocksStatus`).
enum SecondaryLockStatus { Locked(Lock), Committed(Ts), RolledBack }
enum SecondaryLocksStatusSrv { Locked(Vec<LockInfo>), Committed(Ts), RolledBack }

/// check_secondary_locks.rs:67-92.
fn csl_check_determined_txn_status(reader: &SnapshotReader, key: &Key) -> (SecondaryLockStatus, bool, Option<OverlappedWrite>) {
    match reader.get_txn_commit_record(key) {
        TxnCommitRecord::SingleRecord { commit_ts, write } => {
            let status = if write.write_type != WriteType::Rollback { SecondaryLockStatus::Committed(commit_ts) } else { SecondaryLockStatus::RolledBack };
            (status, false, None)
        }
        TxnCommitRecord::OverlappedRollback { .. } => (SecondaryLockStatus::RolledBack, false, None),
        TxnCommitRecord::None { overlapped_write } => (SecondaryLockStatus::RolledBack, true, overlapped_write),
    }
}

/// check_secondary_locks.rs:94-145.
fn check_status_from_lock(txn: &mut MvccTxn, reader: &SnapshotReader, lock: Lock, key: &Key) -> (SecondaryLockStatus, bool, Option<OverlappedWrite>, Option<ReleasedLock>) {
    let mut overlapped_write = None;
    if lock.is_pessimistic_lock_with_conflict() {
        let (status, need_rollback, rollback_overlapped_write) = csl_check_determined_txn_status(reader, key);
        if !need_rollback {
            let released_lock = txn.unlock_key(*key, true, 0);
            return (status, need_rollback, rollback_overlapped_write, released_lock);
        }
        overlapped_write = rollback_overlapped_write;
    }
    if lock.is_pessimistic_lock() {
        let released_lock = txn.unlock_key(*key, true, 0);
        let overlapped_write_res = if lock.is_pessimistic_lock_with_conflict() { overlapped_write } else { reader.get_txn_commit_record(key).unwrap_none() };
        (SecondaryLockStatus::RolledBack, true, overlapped_write_res, released_lock)
    } else {
        (SecondaryLockStatus::Locked(lock), false, None, None)
    }
}

/// check_secondary_locks.rs:148-254.
fn process_check_secondary_locks(snap: &Snapshot, cm: &mut ConcurrencyManager, req: &CheckSecondaryLocksRequest) -> MResult<(SecondaryLocksStatusSrv, Vec<Modify>)> {
    cm.update_max_ts(req.start_version);
    let mut txn = MvccTxn::new(req.start_version);
    let reader = SnapshotReader { start_ts: req.start_version, snap };
    let mut result = SecondaryLocksStatusSrv::Locked(Vec::new());
    for key in &req.keys {
        let mut mismatch_lock = None;
        let (status, need_rollback, rollback_overlapped_write) = match reader.load_lock(key) {
            Some(lock) if lock.ts == req.start_version => {
                let (status, need_rollback, rollback_overlapped_write, _lock_released) = check_status_from_lock(&mut txn, &reader, lock, key);
                (status, need_rollback, rollback_overlapped_write)
            }
            l => {
                mismatch_lock = l;
                csl_check_determined_txn_status(&reader, key)
            }
        };
        if need_rollback {
            if let Some(l) = mismatch_lock {
                txn.mark_rollback_on_mismatching_lock(key, l, true);
            }
            if let Some(write) = make_rollback(req.start_version, true, rollback_overlapped_write) {
                txn.put_write(*key, req.start_version, write);
                collapse_prev_rollback(&mut txn, &reader, key);
            }
        }
        match status {
            SecondaryLockStatus::Locked(lock) => {
                if let SecondaryLocksStatusSrv::Locked(v) = &mut result {
                    v.push(lock.into_lock_info(*key));
                }
            }
            SecondaryLockStatus::Committed(commit_ts) => {
                result = SecondaryLocksStatusSrv::Committed(commit_ts);
                break;
            }
            SecondaryLockStatus::RolledBack => {
                result = SecondaryLocksStatusSrv::RolledBack;
                break;
            }
        }
    }
    Ok((result, txn.modifies))
}

/// commands/resolve_lock_readphase.rs:51-113 `process_read`: the region's locks whose ts is in
/// `txn_status` (`scan_locks_from_storage` over the cells). `None` is `ProcessResult::Res`,
/// `Some` the `NextCommand` ResolveLock. PORT: RESOLVE_LOCK_BATCH_SIZE is never reached
/// (`has_remain` false, scan_key None); shared locks cut (rule A2).
fn process_resolve_lock_readphase(snap: &Snapshot, txn_status: BTreeMap<Ts, Ts>) -> Option<NextCommand> {
    let flatten_pairs: Vec<(Key, Lock)> = snap.iter().filter_map(|(k, c)| c.lock.as_ref().filter(|l| txn_status.contains_key(&l.ts)).map(|l| (*k, l.clone()))).collect();
    if flatten_pairs.is_empty() {
        None
    } else {
        Some(NextCommand::ResolveLock { txn_status, key_locks: flatten_pairs })
    }
}

/// commands/resolve_lock.rs:82-173 `process_write`, a separate command on a new snapshot: the
/// locks of the read phase are `key_locks`; `cleanup` and `commit` load each lock again.
/// PORT: MAX_TXN_WRITE_SIZE is never reached (no further ResolveLockReadPhase);
/// known_txn_status and released_locks feed the txn_status_cache and the lock manager (cut,
/// rules A4, A6).
fn process_resolve_lock(snap: &Snapshot, txn_status: BTreeMap<Ts, Ts>, key_locks: Vec<(Key, Lock)>, v: Variant) -> MResult<Vec<Modify>> {
    let mut txn = MvccTxn::new(0);
    let mut reader = SnapshotReader { start_ts: 0, snap };
    for (current_key, current_lock) in key_locks {
        txn.start_ts = current_lock.ts;
        reader.start_ts = current_lock.ts;
        let commit_ts = *txn_status.get(&current_lock.ts).expect("txn status not found");
        if commit_ts == 0 {
            cleanup(&mut txn, &reader, current_key, 0, false, v)?;
        } else if commit_ts > current_lock.ts {
            match commit(&mut txn, &reader, current_key, commit_ts) {
                Ok(_) => {}
                Err(MvccError::TxnLockNotFound { .. }) if current_lock.is_pessimistic_lock() => {}
                Err(err) => return Err(err),
            }
        } else {
            return Err(MvccError::InvalidTxnTso { start_ts: current_lock.ts, commit_ts });
        }
    }
    Ok(txn.modifies)
}

/// `rj`: one region (rule A1).
fn region_main(idx: usize) {
    let setup = recv_setup();
    let mut cells = Snapshot::new();
    for (k0, r) in setup.layout.iter().enumerate() {
        if *r == idx {
            cells.insert((k0 + 1) as Key, Cell::default());
        }
    }
    let me = setup.regions[idx];
    let mut region = Region { cells, cm: ConcurrencyManager { max_ts: 0 }, variant: setup.variant };
    loop {
        match recv_tagged_msg_block::<_, Net>(|_, tag| tag == Some(TAG_NET)) {
            Net::Req(from, req) => match region.handle(req) {
                Handled::Reply(resp) => send_tagged_msg(from, TAG_NET, Net::Rep(idx, resp)),
                // PORT: the scheduler's NextCommand (S:commands/resolve_lock_readphase.rs:100-110)
                // becomes a message to the region itself (NOTES.md D5).
                Handled::NextCommand(cmd) => send_tagged_msg(me, TAG_NET, Net::NextCommand(from, cmd)),
            },
            Net::NextCommand(from, cmd) => {
                let resp = region.handle_next(cmd);
                send_tagged_msg(from, TAG_NET, Net::Rep(idx, resp));
            }
            other => panic!("a request expected, got {other:?}"),
        }
    }
}

// ============================================================================================
// Program entry (INTERFACE.md 2 start order, 9)
// ============================================================================================

struct Instance { scenario: String, async_commit: bool, one_pc: bool, a: u32, model: Option<String>, variant: Variant }

/// INTERFACE.md 1 and 9: `<scenario> <async> <onepc> <a> [<fifo|bag> [<variant>]]`, by position.
fn parse_instance(args: &[String]) -> Instance {
    assert!((4..=6).contains(&args.len()), "arguments: <scenario> <async> <onepc> <a> [fifo|bag [correct|f1|f2]], got {args:?}");
    let (_, clients) = scenario(&args[0]);
    let flag = |s: &str| match s { "0" => false, "1" => true, x => panic!("0 or 1 expected, got {x}") };
    let async_commit = flag(&args[1]);
    let one_pc = flag(&args[2]);
    let a: u32 = args[3].parse().expect("a >= 1");
    assert!(a >= 1, "a >= 1");
    if async_commit && one_pc {
        panic!("async = 1 together with onepc = 1 is not an instance");
    }
    let model = args.get(4).map(|m| match m.as_str() {
        "fifo" | "bag" => m.clone(),
        o => panic!("the fifth argument is the delivery model fifo|bag, got {o}"),
    });
    let variant = match args.get(5).map(String::as_str) {
        None | Some("correct") => Variant::Correct,
        Some("f1") => Variant::F1,
        Some("f2") => Variant::F2,
        Some(o) => panic!("the sixth argument is the variant correct|f1|f2, got {o}"),
    };
    // PORT: a key that is GetForUpdate-locked and never Put would commit an `Op::Lock`
    // mutation (C:transaction/buffer.rs:403), which INTERFACE.md 6 excludes (rule A2): such a
    // script is refused here (NOTES.md D7). No scenario of INTERFACE.md 3 has one.
    for (i, (_, script)) in clients.iter().enumerate() {
        for (j, op) in script.iter().enumerate() {
            if let Op::GetForUpdate(k) = op {
                assert!(script[j + 1..].contains(&Op::Put(*k)), "c{}: GetForUpdate({k}) without a later Put({k}) needs a Lock mutation, cut by INTERFACE.md 6", i + 1);
            }
        }
    }
    Instance { scenario: args[0].clone(), async_commit, one_pc, a, model, variant }
}

/// INTERFACE.md 9: the positional `fifo|bag`, if given, must equal `--model`. The entry
/// closure does not see `--model` (program.rs `parse_opts`), so `main` reads it from the
/// command line the same way `parse_opts` does (default `fifo`, the default of
/// `program::main`) and refuses a disagreement before running anything.
fn check_model_argument(argv: &[String]) -> std::result::Result<(), String> {
    let Some(cmd) = argv.get(1) else { return Ok(()) };
    let skip = match cmd.as_str() {
        "count" | "export" => 3,
        _ => 2,
    };
    let mut model = "fifo".to_string();
    let mut prog: &[String] = &[];
    let mut i = skip;
    while i < argv.len() {
        match argv[i].as_str() {
            "--" => {
                prog = &argv[i + 1..];
                break;
            }
            "--model" => {
                i += 1;
                if let Some(m) = argv.get(i) {
                    model = m.clone();
                }
            }
            "--seed" | "--threshold" | "--engine" | "--threads" | "--out" | "--program" | "--size" | "--spec" | "--budget" => i += 1,
            a if a.starts_with("--") => {}
            _ => {
                prog = &argv[i..];
                break;
            }
        }
        i += 1;
    }
    match prog.get(4) {
        Some(m) if *m != model => Err(format!("the positional model `{m}` differs from --model {model} (INTERFACE.md 9)")),
        _ => Ok(()),
    }
}

fn visible(args: &[String]) -> Vec<String> {
    let (_, clients) = scenario(&args[0]);
    (1..=clients.len()).map(|i| format!("c{i}")).collect()
}

fn run(inst: &Instance) {
    let (n, clients) = scenario(&inst.scenario);
    let m = clients.len();
    // INTERFACE.md 4 / 1: onepc = 1 -> layout `single`, else `split`.
    let layout: Vec<usize> = (0..n as usize).map(|k| if inst.one_pc { 0 } else { k }).collect();
    let r_count = if inst.one_pc { 1 } else { n as usize };
    let mut regions = Vec::new();
    for j in 0..r_count {
        let h = thread::Builder::new().name(format!("r{}", j + 1)).spawn_daemon(move || region_main(j)).unwrap();
        regions.push(h.thread().id());
    }
    let pd = thread::Builder::new().name("pd".to_string()).spawn_daemon(pd_main).unwrap().thread().id();
    let mut xs = Vec::new();
    for i in 0..m {
        xs.push(thread::Builder::new().name(format!("x{}", i + 1)).spawn_daemon(move || secondary_committer(i)).unwrap().thread().id());
    }
    let mut ts = Vec::new();
    for i in 0..m {
        ts.push(thread::Builder::new().name(format!("t{}", i + 1)).spawn_daemon(move || library(i)).unwrap().thread().id());
    }
    let mut cs = Vec::new();
    let mut handles = Vec::new();
    for (i, (_, script)) in clients.iter().enumerate() {
        let script = script.clone();
        let t = ts[i];
        let h = thread::Builder::new().name(format!("c{}", i + 1)).spawn(move || client((i + 1) as u8, script, t)).unwrap();
        cs.push(h.thread().id());
        handles.push(h);
    }
    let setup = Setup {
        cs, ts: ts.clone(), xs: xs.clone(), pd, regions: regions.clone(), layout,
        kinds: clients.iter().map(|(k, _)| *k).collect(),
        async_commit: inst.async_commit, one_pc: inst.one_pc, a: inst.a, variant: inst.variant,
    };
    let init = thread::Builder::new().name("init".to_string()).spawn(move || {
        let mut targets: Vec<ThreadId> = setup.regions.clone();
        targets.push(setup.pd);
        targets.extend(setup.xs.iter().copied());
        targets.extend(setup.ts.iter().copied());
        for t in targets {
            send_tagged_msg(t, TAG_SETUP, Net::Setup(Box::new(setup.clone())));
        }
    }).unwrap();
    for h in handles {
        h.join().unwrap();
    }
    init.join().unwrap();
}

fn entry(args: &[String]) -> Entry {
    let inst = Arc::new(parse_instance(args));
    Arc::new(move || run(&inst))
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().collect();
    if let Err(e) = check_model_argument(&argv) {
        eprintln!("bench_tikv_port: {e}");
        return ExitCode::from(2);
    }
    program::main(Program { visible: &visible, spec: &entry, imp: &entry })
}
