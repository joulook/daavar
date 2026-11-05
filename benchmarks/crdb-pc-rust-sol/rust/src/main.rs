use std::sync::Arc;
use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

#[derive(Clone, Debug, PartialEq)] enum Fate { Committed, Aborted }
#[derive(Clone, Debug, PartialEq)] enum Req { Put(u8), Commit(Vec<u8>), Recover }
#[derive(Clone, Debug, PartialEq)] enum Rep { Written(u8), Committed, Retry, Aborted, Recovered(Fate) }

fn committer(n: u8, p: u8, a: u32, tc: ThreadId) {
    for _ in 1..=a {
        for k in 1..=p { send_msg(tc, Req::Put(k)); let Rep::Written(_) = recv_msg_block::<Rep>() else { panic!("Written expected") }; }
        send_msg(tc, Req::Commit((p + 1..=n).collect()));
        match recv_msg_block::<Rep>() { Rep::Committed | Rep::Aborted => return, Rep::Retry => continue, _ => panic!("Committed, Retry or Aborted expected") }
    }
}
fn preventer(h: ThreadId) {
    send_msg(h, Req::Recover); let Rep::Recovered(_) = recv_msg_block::<Rep>() else { panic!("Recovered expected") };
}

// Must's generic payload parameter checks the selected value's type; tags
// implement the selective mailbox, rather than receiving and discarding values.
trait Wire: Clone + std::fmt::Debug + PartialEq + Send + 'static { const TAG: Option<u32>; }
macro_rules! wire { ($t:ty, $tag:expr) => { impl Wire for $t { const TAG: Option<u32> = $tag; } }; }
wire!(Req, None);
wire!(Setup, Some(1)); wire!(Batch, Some(2)); wire!(MakeExplicit, Some(3));
wire!(WriteReply, Some(4)); wire!(ParallelReply, Some(5)); wire!(CheckReply, Some(6));
wire!(ExplicitReply, Some(7)); wire!(PushReply, Some(8));
wire!(RecoveryReply, Some(9)); wire!(ResolveReply, Some(10));
fn wire_send<T: Wire>(to: ThreadId, value: T) {
    send_tagged_msg(to, T::TAG.expect("internal send must have a tag"), value);
}
fn receive<T: Wire>() -> T {
    recv_tagged_msg_block(move |_, tag| tag == T::TAG)
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Status {
    Pending,
    Staging,
    Committed,
    Aborted,
}
impl Status {
    fn finalized(self) -> bool {
        matches!(self, Self::Committed | Self::Aborted)
    }
    fn fate(self) -> Fate {
        match self {
            Self::Committed => Fate::Committed,
            Self::Aborted => Fate::Aborted,
            _ => panic!("not finalized"),
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
struct Write {
    key: u8,
    seq: u32,
}
#[derive(Clone, Debug, PartialEq)]
struct Txn {
    epoch: u32,
    ts: u32,
    read: u32,
    min: u32,
    status: Status,
    flight: Vec<Write>,
    locks: Vec<u8>,
}
impl Txn {
    fn initial() -> Self {
        Self {
            epoch: 0,
            ts: 1,
            read: 1,
            min: 1,
            status: Status::Pending,
            flight: vec![],
            locks: vec![],
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
struct Setup {
    ranges: Vec<ThreadId>,
    tcx: ThreadId,
    tc: ThreadId,
    handles: Vec<ThreadId>,
    c: ThreadId,
    preventers: Vec<ThreadId>,
    init: ThreadId,
    meta: Txn,
}
#[derive(Clone, Debug, PartialEq)]
struct Intent {
    epoch: u32,
    ts: u32,
    seq: u32,
}
#[derive(Clone, Debug, PartialEq, Default)]
struct Cell {
    intent: Option<Intent>,
    value: Option<u32>,
}
#[derive(Clone, Debug, PartialEq)]
enum RetryReason {
    Serializable,
    AsyncWriteFailure,
}
#[derive(Clone, Debug, PartialEq)]
enum AbortReason {
    RecordFound,
    Tombstone,
}
#[derive(Clone, Debug, PartialEq)]
enum Error {
    IntentMissing,
    TransactionRetry(RetryReason),
    TransactionAborted(AbortReason),
    AlreadyCommitted,
    IndeterminateCommit,
}
#[derive(Clone, Debug, PartialEq)]
enum Op {
    Put(Write),
    QueryIntent {
        write: Write,
        own: bool,
        error_if_missing: bool,
    },
    EndTxn {
        flight: Vec<Write>,
        locks: Vec<u8>,
    },
    QueryTxn,
    PushTxn,
    RecoverTxn {
        implicitly_committed: bool,
    },
    ResolveIntent(Fate),
}
#[derive(Clone, Copy, Debug, PartialEq)]
enum Phase {
    Write,
    Parallel,
    Check,
    Explicit,
    Push,
    Probe,
    Recovery,
    Resolve,
}
#[derive(Clone, Debug, PartialEq)]
struct Batch {
    from: ThreadId,
    phase: Phase,
    round: u32,
    txn: Txn,
    ops: Vec<Op>,
}
#[derive(Clone, Debug, PartialEq)]
struct Response {
    range: u8,
    txn: Txn,
    error: Option<Error>,
    staging: Option<u32>,
    queried: Option<(Txn, bool)>,
    intents: Vec<(u8, bool, bool)>,
}
// Distinct mailboxes for protocol phases. Probe rounds additionally have tags,
// so responses to a completed recovery attempt are never consumed by another.
#[derive(Clone, Debug, PartialEq)]
struct WriteReply(Response);
#[derive(Clone, Debug, PartialEq)]
struct ParallelReply(Response);
#[derive(Clone, Debug, PartialEq)]
struct CheckReply(Response);
#[derive(Clone, Debug, PartialEq)]
struct ExplicitReply(Response);
#[derive(Clone, Debug, PartialEq)]
struct PushReply(Response);
#[derive(Clone, Debug, PartialEq)]
struct ProbeReply(Response);
#[derive(Clone, Debug, PartialEq)]
struct RecoveryReply(Response);
#[derive(Clone, Debug, PartialEq)]
struct ResolveReply(Response);
#[derive(Clone, Debug, PartialEq)]
struct MakeExplicit(Txn, Vec<u8>);
fn reply(b: &Batch, r: Response) {
    match b.phase {
        Phase::Write => wire_send(b.from, WriteReply(r)),
        Phase::Parallel => wire_send(b.from, ParallelReply(r)),
        Phase::Check => wire_send(b.from, CheckReply(r)),
        Phase::Explicit => wire_send(b.from, ExplicitReply(r)),
        Phase::Push => wire_send(b.from, PushReply(r)),
        Phase::Probe => send_tagged_msg(b.from, 1000 + b.round, ProbeReply(r)),
        Phase::Recovery => wire_send(b.from, RecoveryReply(r)),
        Phase::Resolve => wire_send(b.from, ResolveReply(r)),
    }
}
fn batch(s: &Setup, key: u8, from: ThreadId, phase: Phase, round: u32, txn: &Txn, ops: Vec<Op>) {
    wire_send(
        s.ranges[key as usize - 1],
        Batch {
            from,
            phase,
            round,
            txn: txn.clone(),
            ops,
        },
    );
}
fn merge_lock(locks: &mut Vec<u8>, key: u8) {
    if !locks.contains(&key) {
        locks.push(key);
        locks.sort();
    }
}
fn resolve(cell: &mut Cell, fate: &Fate) {
    if let Some(i) = cell.intent.take() {
        if *fate == Fate::Committed {
            cell.value = Some(i.ts);
        }
    }
}

struct Range {
    key: u8,
    cell: Cell,
    cache: u32,
    record: Option<Txn>,
    tombstone: u32,
    push: u32,
}
impl Range {
    fn can_create_txn_record(&self, t: &Txn) -> bool {
        self.tombstone < t.min
    }
    fn synthesize_txn_from_meta(&self, t: &Txn) -> Txn {
        let mut t = t.clone();
        t.flight.clear();
        t.locks.clear();
        t.status = if self.can_create_txn_record(&t) {
            Status::Pending
        } else {
            Status::Aborted
        };
        t.ts = t.ts.max(self.push);
        t
    }
    fn query_txn(&self, t: &Txn) -> (Txn, bool) {
        match &self.record {
            Some(r) => (r.clone(), true),
            None => (self.synthesize_txn_from_meta(t), false),
        }
    }
    fn query_intent(
        &mut self,
        t: &mut Txn,
        w: &Write,
        own: bool,
        error_if_missing: bool,
    ) -> (bool, bool, Option<Error>) {
        let queried_ts = t.ts;
        let found = self
            .cell
            .intent
            .as_ref()
            .is_some_and(|i| i.epoch == t.epoch && i.seq >= w.seq);
        let unpushed = found && self.cell.intent.as_ref().unwrap().ts <= t.ts;
        if found && !unpushed && own {
            t.ts = t.ts.max(self.cell.intent.as_ref().unwrap().ts);
        }
        if !unpushed {
            self.cache = self.cache.max(queried_ts);
        }
        (
            found,
            unpushed,
            if !found && error_if_missing {
                Some(Error::IntentMissing)
            } else {
                None
            },
        )
    }
    fn push_txn(&mut self, t: &Txn) -> (Txn, Option<Error>) {
        let mut r = self.query_txn(t).0;
        if r.status.finalized() {
            if r.status == Status::Aborted {
                self.tombstone = self.tombstone.max(r.min);
            }
            return (r, None);
        }
        let known_higher = t.epoch > r.epoch || t.ts > r.ts;
        r.epoch = r.epoch.max(t.epoch);
        r.ts = r.ts.max(t.ts);
        if r.status == Status::Staging && !known_higher {
            return (r, Some(Error::IndeterminateCommit));
        }
        r.status = Status::Aborted;
        r.flight.clear();
        if self.record.is_some() {
            self.record = Some(r.clone());
        }
        self.tombstone = self.tombstone.max(r.min);
        (r, None)
    }
    fn recover_txn(&mut self, t: &Txn, implicit: bool) -> Txn {
        let mut r = self.query_txn(t).0;
        if self.record.is_none() {
            assert_eq!(r.status, Status::Aborted);
            self.tombstone = self.tombstone.max(t.ts);
            return r;
        }
        if implicit {
            assert!(matches!(r.status, Status::Staging | Status::Committed));
            assert_eq!((r.epoch, r.ts), (t.epoch, t.ts));
        } else if r.status.finalized() || r.epoch > t.epoch || r.ts > t.ts {
            // A resolved committed value may have looked like a missing intent (#37784).
            if r.status.finalized() {
                self.tombstone = self.tombstone.max(t.ts);
            }
            return r;
        } else {
            assert_eq!(r.status, Status::Staging);
        }
        if !r.status.finalized() {
            for w in &r.flight {
                merge_lock(&mut r.locks, w.key);
            }
            r.flight.clear();
            r.status = if implicit {
                Status::Committed
            } else {
                Status::Aborted
            };
            self.record = Some(r.clone());
        }
        self.tombstone = self.tombstone.max(t.ts);
        r
    }
    fn end_txn(
        &mut self,
        t: &Txn,
        flight: &[Write],
        locks: &[u8],
    ) -> (Txn, Option<Error>, Option<u32>) {
        let mut r = match &self.record {
            Some(r) => r.clone(),
            None => {
                if !self.can_create_txn_record(t) {
                    return (
                        t.clone(),
                        Some(Error::TransactionAborted(AbortReason::Tombstone)),
                        None,
                    );
                }
                t.clone()
            }
        };
        match r.status {
            Status::Committed => return (r, Some(Error::AlreadyCommitted), None),
            Status::Aborted => {
                return (
                    r,
                    Some(Error::TransactionAborted(AbortReason::RecordFound)),
                    None,
                )
            }
            _ => {}
        }
        assert!(t.epoch >= r.epoch);
        r.epoch = t.epoch;
        r.ts = r.ts.max(t.ts);
        r.read = r.read.max(t.read);
        if self
            .record
            .as_ref()
            .is_none_or(|x| x.status == Status::Pending)
        {
            r.ts = r.ts.max(self.push);
        }
        if r.read < r.ts {
            return (
                r,
                Some(Error::TransactionRetry(RetryReason::Serializable)),
                None,
            );
        }
        r.flight = flight.to_vec();
        r.locks = locks.to_vec();
        r.status = if flight.is_empty() {
            Status::Committed
        } else {
            Status::Staging
        };
        let staging = if r.status == Status::Staging {
            Some(r.ts)
        } else {
            None
        };
        self.record = Some(r.clone());
        self.tombstone = self.tombstone.max(t.ts);
        (r, None, staging)
    }
    fn evaluate(&mut self, s: &Setup, b: &Batch) -> Response {
        let mut t = b.txn.clone();
        // applyTimestampCache applies to the whole range batch before evaluation.
        if b.ops.iter().any(|o| matches!(o, Op::Put(_))) {
            t.ts = t.ts.max(self.cache + 1);
        }
        let old_cell = self.cell.clone();
        let mut r = Response {
            range: self.key,
            txn: t.clone(),
            error: None,
            staging: None,
            queried: None,
            intents: vec![],
        };
        for op in &b.ops {
            match op {
                Op::Put(w) => {
                    self.cell.intent = Some(Intent {
                        epoch: t.epoch,
                        ts: t.ts,
                        seq: w.seq,
                    })
                }
                Op::QueryIntent {
                    write,
                    own,
                    error_if_missing,
                } => {
                    let (found, unpushed, err) =
                        self.query_intent(&mut t, write, *own, *error_if_missing);
                    r.intents.push((write.key, found, unpushed));
                    r.error = err;
                }
                Op::QueryTxn => r.queried = Some(self.query_txn(&t)),
                Op::PushTxn => {
                    let (p, e) = self.push_txn(&t);
                    t = p;
                    r.error = e;
                }
                Op::RecoverTxn {
                    implicitly_committed,
                } => t = self.recover_txn(&t, *implicitly_committed),
                Op::ResolveIntent(f) => resolve(&mut self.cell, f),
                Op::EndTxn { flight, locks } => {
                    // maybeStripInFlightWrites: writes in this same atomic batch
                    // cannot still be in flight after it commits.
                    let mut flight = flight.clone();
                    let mut locks = locks.clone();
                    for op in &b.ops {
                        if let Op::Put(w) = op {
                            flight.retain(|x| x != w);
                            merge_lock(&mut locks, w.key);
                        }
                    }
                    let (et, err, staging) = self.end_txn(&t, &flight, &locks);
                    t = et;
                    r.error = err;
                    r.staging = staging;
                    if r.error.is_some() {
                        self.cell = old_cell.clone();
                    }
                    // An aborted existing record also returns its lock spans for
                    // cleanup, even though EndTxn itself returns an error.
                    if t.status.finalized()
                        && (r.error.is_none()
                            || matches!(r.error, Some(Error::TransactionAborted(_))))
                    {
                        if locks.contains(&self.key) {
                            resolve(&mut self.cell, &t.status.fate());
                        }
                        for key in locks.iter().copied().filter(|k| *k != self.key) {
                            batch(
                                s,
                                key,
                                s.ranges[0],
                                Phase::Resolve,
                                0,
                                &t,
                                vec![Op::ResolveIntent(t.status.fate())],
                            );
                        }
                    }
                }
            }
            if r.error.is_some() {
                break;
            }
        }
        r.txn = t;
        r
    }
}
fn range(key: u8) {
    let s = receive::<Setup>();
    let mut r = Range {
        key,
        cell: Cell::default(),
        cache: 0,
        record: None,
        tombstone: 0,
        push: 0,
    };
    loop {
        let b = receive::<Batch>();
        let out = r.evaluate(&s, &b);
        reply(&b, out);
    }
}

fn maybe_refresh_and_retry_send(t: &mut Txn) {
    t.read = t.ts;
    t.status = Status::Pending;
}
fn explicit_end(s: &Setup, from: ThreadId, t: &Txn, locks: Vec<u8>) -> Response {
    batch(
        s,
        1,
        from,
        Phase::Explicit,
        0,
        t,
        vec![Op::EndTxn {
            flight: vec![],
            locks,
        }],
    );
    receive::<ExplicitReply>().0
}
fn retry_txn_commit_after_failed_parallel_commit(
    s: &Setup,
    t: &mut Txn,
    locks: Vec<u8>,
) -> Response {
    // First send at the old read timestamp, as the committer does. The span
    // refresher receives the serialization retry and reissues EndTxn alone.
    let mut r = explicit_end(s, s.tc, t, locks.clone());
    if r.error == Some(Error::TransactionRetry(RetryReason::Serializable)) {
        t.ts = t.ts.max(r.txn.ts);
        maybe_refresh_and_retry_send(t);
        r = explicit_end(s, s.tc, t, locks);
    }
    r
}
fn adjust_error(error: &Option<Error>) -> Option<Error> {
    match error {
        Some(Error::IntentMissing) => Some(Error::TransactionRetry(RetryReason::AsyncWriteFailure)),
        e => e.clone(),
    }
}
fn outcome(r: &Response) -> Rep {
    match adjust_error(&r.error) {
        None => {
            assert_eq!(r.txn.status, Status::Committed);
            Rep::Committed
        }
        Some(Error::TransactionAborted(_)) => Rep::Aborted,
        Some(Error::AlreadyCommitted) => panic!("already committed outside async explicit task"),
        _ => Rep::Retry,
    }
}
fn divide_and_send_parallel_commit(
    s: &Setup,
    t: &Txn,
    flight: Vec<Write>,
    final_writes: Vec<Write>,
) -> (Response, u32, bool) {
    let mut queries = vec![];
    let mut finals = vec![];
    for key in 1..=s.ranges.len() as u8 {
        let qi: Vec<_> = flight
            .iter()
            .filter(|w| !final_writes.contains(w) && w.key == key)
            .map(|w| Op::QueryIntent {
                write: w.clone(),
                own: true,
                error_if_missing: true,
            })
            .collect();
        if !qi.is_empty() {
            queries.push((key, qi));
        }
        let mut ops: Vec<_> = final_writes
            .iter()
            .filter(|w| w.key == key)
            .cloned()
            .map(Op::Put)
            .collect();
        if key == 1 {
            ops.push(Op::EndTxn {
                flight: flight.clone(),
                locks: t.locks.clone(),
            });
        }
        if !ops.is_empty() {
            finals.push((key, ops));
        }
    }
    let count = queries.len() + finals.len();
    // The source's QueryIntent goroutine races the EndTxn sender. In particular,
    // their sends to r1 have no fixed order, even under FIFO.
    let qi_first = queries.is_empty() || nondet();
    let groups = if qi_first {
        vec![queries, finals]
    } else {
        vec![finals, queries]
    };
    for group in groups {
        for (key, ops) in group {
            batch(s, key, s.tc, Phase::Parallel, 0, t, ops);
        }
    }
    let mut end = None;
    let mut max_ts = t.ts;
    let mut missing = false;
    for _ in 0..count {
        let r = receive::<ParallelReply>().0;
        max_ts = max_ts.max(r.txn.ts);
        if r.error == Some(Error::IntentMissing) {
            missing = true;
        }
        if r.staging.is_some()
            || (r.range == 1
                && !matches!(r.error, Some(Error::IntentMissing))
                && r.intents.is_empty())
        {
            end = Some(r);
        }
    }
    (end.expect("EndTxn response"), max_ts, missing)
}
fn detect_intent_missing_due_to_intent_resolution(s: &Setup, t: &Txn) -> Result<bool, Error> {
    batch(s, 1, s.tc, Phase::Check, 0, t, vec![Op::QueryTxn]);
    let (r, exists) = receive::<CheckReply>().0.queried.unwrap();
    match r.status {
        Status::Committed => Ok(true),
        Status::Aborted if exists => Err(Error::TransactionAborted(AbortReason::RecordFound)),
        // With no GC, a successful staging EndTxn implies a real record exists.
        Status::Aborted => panic!("unreachable ambiguous missing record"),
        _ => Ok(false),
    }
}
// The source also retains the general refresh path for an EndTxn evaluation
// error. Unlike a successful-but-pushed parallel commit, this path retries the
// prefix, then sends EndTxn separately (span refresher 310-312). No fault is
// introduced to reach it in the blind-write instances here.
fn refresh_failed_final_batch(s: &Setup, t: &mut Txn, writes: &[Write], locks: Vec<u8>) -> Response {
    maybe_refresh_and_retry_send(t);
    let mut count = 0;
    for key in 1..=s.ranges.len() as u8 {
        let mut ops: Vec<_> = writes.iter().filter(|w| w.key == key).cloned().map(Op::Put).collect();
        for w in t.flight.iter().filter(|w| w.key == key) {
            ops.push(Op::QueryIntent { write: w.clone(), own: true, error_if_missing: true });
        }
        if !ops.is_empty() { batch(s, key, s.tc, Phase::Parallel, 0, t, ops); count += 1; }
    }
    let mut failure = None;
    for _ in 0..count {
        let r = receive::<ParallelReply>().0;
        t.ts = t.ts.max(r.txn.ts);
        if r.error.is_some() { failure = Some(r); }
    }
    if let Some(r) = failure { return r; }
    retry_txn_commit_after_failed_parallel_commit(s, t, locks)
}
fn coordinator() {
    let s = receive::<Setup>();
    let mut t = s.meta.clone();
    let mut seq = 0;
    loop {
        match receive::<Req>() {
            Req::Put(key) => {
                seq += 1;
                let w = Write { key, seq };
                batch(&s, key, s.tc, Phase::Write, 0, &t, vec![Op::Put(w.clone())]);
                let r = receive::<WriteReply>().0;
                assert!(r.error.is_none());
                t.ts = t.ts.max(r.txn.ts);
                t.flight.push(w);
                send_msg(s.c, Rep::Written(key));
            }
            Req::Commit(keys) => {
                let mut writes = vec![];
                for key in keys {
                    seq += 1;
                    writes.push(Write { key, seq });
                }
                // attachLocksToEndTxn; all requests qualify for canCommitInParallel.
                let mut flight = t.flight.clone();
                flight.extend(writes.clone());
                let locks: Vec<u8> = (1..=s.ranges.len() as u8).collect();
                let (mut end, ts, missing) =
                    divide_and_send_parallel_commit(&s, &t, flight, writes.clone());
                t.ts = ts;
                // EndTxn-half errors take priority over pre-commit query errors.
                if end.error.is_none() && missing {
                    match detect_intent_missing_due_to_intent_resolution(&s, &t) {
                        Ok(true) => {}
                        Ok(false) => end.error = Some(Error::IntentMissing),
                        Err(e) => end.error = Some(e),
                    }
                }
                let rep = if end.error.is_some() {
                    if end.error == Some(Error::TransactionRetry(RetryReason::Serializable)) {
                        outcome(&refresh_failed_final_batch(&s, &mut t, &writes, locks.clone()))
                    } else {
                        outcome(&end)
                    }
                } else if end.txn.status == Status::Committed {
                    Rep::Committed
                } else if end.staging.unwrap() >= ts {
                    // isTxnCommitImplicit. The interface orders the client reply
                    // before starting the explicit-commit task.
                    t.flight.clear();
                    t.status = Status::Committed;
                    send_msg(s.c, Rep::Committed);
                    let mut explicit = end.txn;
                    explicit.flight.clear();
                    wire_send(s.tcx, MakeExplicit(explicit, locks));
                    continue;
                } else {
                    let r =
                        retry_txn_commit_after_failed_parallel_commit(&s, &mut t, locks.clone());
                    outcome(&r)
                };
                t.flight.clear();
                t.locks = locks;
                if rep == Rep::Retry {
                    t.epoch += 1;
                    t.ts += 1;
                    t.read = t.ts;
                    t.status = Status::Pending;
                }
                send_msg(s.c, rep);
            }
            Req::Recover => unreachable!(),
        }
    }
}
fn explicit_task() {
    let s = receive::<Setup>();
    loop {
        let MakeExplicit(t, locks) = receive::<MakeExplicit>();
        let r = explicit_end(&s, s.tcx, &t, locks);
        // makeTxnCommitExplicitLocked tolerates another recovery winning.
        assert!(r.error.is_none() || r.error == Some(Error::AlreadyCommitted));
    }
}
fn resolve_indeterminate_commit(s: &Setup, me: ThreadId, t: &Txn, round: u32) {
    // One probe batch: QueryTxn and the local QueryIntent are atomic on r1.
    let mut count = 0;
    for key in 1..=s.ranges.len() as u8 {
        let mut ops = vec![];
        if key == 1 {
            ops.push(Op::QueryTxn);
        }
        for w in t.flight.iter().filter(|w| w.key == key) {
            ops.push(Op::QueryIntent {
                write: w.clone(),
                own: false,
                error_if_missing: false,
            });
        }
        if !ops.is_empty() {
            batch(s, key, me, Phase::Probe, round, t, ops);
            count += 1;
        }
    }
    let mut prevented = false;
    for _ in 0..count {
        let r = recv_tagged_msg_block::<_, ProbeReply>(move |_, tag| tag == Some(1000 + round)).0;
        if let Some((q, _)) = r.queried {
            if q.status.finalized() || q.epoch > t.epoch || q.ts > t.ts {
                // Do not receive late probe replies after this operation ends.
                return;
            }
        }
        prevented |= r.intents.iter().any(|(_, _, unpushed)| !unpushed);
    }
    batch(
        s,
        1,
        me,
        Phase::Recovery,
        0,
        t,
        vec![Op::RecoverTxn {
            implicitly_committed: !prevented,
        }],
    );
    let r = receive::<RecoveryReply>().0;
    assert!(r.error.is_none());
}
fn handle(index: usize) {
    let s = receive::<Setup>();
    let me = s.handles[index];
    let mut round = 0;
    loop {
        let Req::Recover = receive::<Req>() else {
            unreachable!()
        };
        let final_txn = loop {
            batch(&s, 1, me, Phase::Push, 0, &s.meta, vec![Op::PushTxn]);
            let r = receive::<PushReply>().0;
            if r.error == Some(Error::IndeterminateCommit) {
                round += 1;
                resolve_indeterminate_commit(&s, me, &r.txn, round);
                // replica_send retries the original push, with its original meta.
                continue;
            }
            assert!(r.error.is_none());
            assert!(r.txn.status.finalized());
            break r.txn;
        };
        let fate = final_txn.status.fate();
        for key in 1..=s.ranges.len() as u8 {
            batch(
                &s,
                key,
                me,
                Phase::Resolve,
                0,
                &final_txn,
                vec![Op::ResolveIntent(fate.clone())],
            );
        }
        for _ in &s.ranges {
            receive::<ResolveReply>();
        }
        send_msg(s.preventers[index], Rep::Recovered(fate));
    }
}
fn spawn(name: &str, f: impl FnOnce() + Send + 'static) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name(name.to_string())
        .spawn(f)
        .unwrap()
}
fn implementation(n: u8, p: u8, m: u8, a: u32) {
    let mut workers = vec![];
    let mut ranges = vec![];
    for k in 1..=n {
        let h = spawn(&format!("r{k}"), move || range(k));
        ranges.push(h.thread().id());
        workers.push(h);
    }
    let x = spawn("tcx", explicit_task);
    let tcx = x.thread().id();
    workers.push(x);
    let co = spawn("tc", coordinator);
    let tc = co.thread().id();
    workers.push(co);
    let mut handles = vec![];
    for i in 0..m as usize {
        let h = spawn(&format!("h{}", i + 1), move || handle(i));
        handles.push(h.thread().id());
        workers.push(h);
    }
    let client = spawn("c", move || committer(n, p, a, tc));
    let c = client.thread().id();
    let mut clients = vec![client];
    let mut preventers = vec![];
    for (i, h) in handles.iter().copied().enumerate() {
        let client = spawn(&format!("p{}", i + 1), move || preventer(h));
        preventers.push(client.thread().id());
        clients.push(client);
    }
    let init = spawn("init", move || {
        let s = Setup {
            ranges,
            tcx,
            tc,
            handles,
            c,
            preventers,
            init: thread::current().id(),
            meta: Txn::initial(),
        };
        for target in s
            .ranges
            .iter()
            .chain([&s.tcx, &s.tc])
            .chain(s.handles.iter())
        {
            wire_send(*target, s.clone());
        }
    });
    init.join().unwrap();
    for client in clients {
        client.join().unwrap();
    }
    for worker in workers {
        worker.join().unwrap();
    }
}
fn params(args: &[String]) -> (u8, u8, u8, u32) {
    assert!((4..=5).contains(&args.len()), "expected n p m a [fifo|bag]");
    let n = args[0].parse().unwrap();
    let p = args[1].parse().unwrap();
    let m = args[2].parse().unwrap();
    let a = args[3].parse().unwrap();
    assert!((n == 1 || n == 2) && p <= n && !(n == 1 && p == 0) && (m == 1 || m == 2) && a >= 1);
    if args.len() == 5 {
        assert!(matches!(args[4].as_str(), "fifo" | "bag"));
    }
    (n, p, m, a)
}
fn entry(args: &[String]) -> Entry {
    let (n, p, m, a) = params(args);
    Arc::new(move || implementation(n, p, m, a))
}
fn visible(args: &[String]) -> Vec<String> {
    let (_, _, m, _) = params(args);
    std::iter::once("c".into())
        .chain((1..=m).map(|i| format!("p{i}")))
        .collect()
}
fn main() -> std::process::ExitCode {
    // The shared runner owns --model; the optional positional model supplies
    // the default when that flag is absent.
    let model = if std::env::args().last().as_deref() == Some("bag") {
        ConsType::Bag
    } else {
        ConsType::FIFO
    };
    let program = Program {
        visible: &visible,
        spec: &entry,
        imp: &entry,
    };
    if model == ConsType::FIFO {
        traceforge::program::main(program)
    } else {
        traceforge::program::main_with_model(program, model)
    }
}
