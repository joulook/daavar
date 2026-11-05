//! Port of CockroachDB's parallel commits with transaction recovery (cockroachdb/cockroach
//! at d30c905) to TraceForge's Must API: one committing transaction T over n keys (n in
//! {1, 2}) with p pipelined writes, and m preventers that push T and recover it.
//! Program interface: plan/case-studies/cockroach/INTERFACE-agents.md. Decisions: NOTES.md.
//!
//! Arguments: `<n> <p> <m> <a> [<fifo|bag>]`.
use std::collections::BTreeMap;
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{self, Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ---------------------------------------------------------------------------
// The visible messages and the client loops (verbatim from the interface).
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum Fate {
    Committed,
    Aborted,
}
#[derive(Clone, Debug, PartialEq)]
enum Req {
    Put(u8),
    Commit(Vec<u8>),
    Recover,
}
#[derive(Clone, Debug, PartialEq)]
enum Rep {
    Written(u8),
    Committed,
    Retry,
    Aborted,
    Recovered(Fate),
}

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

// ---------------------------------------------------------------------------
// Shared abstractions: timestamps, transaction meta, records, requests.
// ---------------------------------------------------------------------------

type Ts = u64;
type Key = u8;

/// Message tags of the invisible messages (client messages are untagged).
const T_SETUP: u32 = 1;
/// A batch request to a range.
const T_BATCH: u32 = 2;
/// tc -> tcx: make the commit explicit.
const T_EXPLICIT: u32 = 3;
/// A batch response carries the tag `T_RESP + id` of its request.
const T_RESP: u32 = 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Status {
    Pending,
    Staging,
    Committed,
    Aborted,
}
impl Status {
    fn finalized(self) -> bool {
        matches!(self, Status::Committed | Status::Aborted)
    }
    fn rank(self) -> u8 {
        match self {
            Status::Pending => 0,
            Status::Staging => 1,
            Status::Committed | Status::Aborted => 2,
        }
    }
}

/// enginepb.TxnMeta of T (its id is fixed and its anchor key is k1, so neither is stored).
#[derive(Clone, Copy, Debug, PartialEq)]
struct Meta {
    epoch: u32,
    write_ts: Ts,
    min_ts: Ts,
    seq: u32,
}

/// roachpb.Transaction (the header transaction of T's batches).
#[derive(Clone, Debug, PartialEq)]
struct Txn {
    meta: Meta,
    read_ts: Ts,
    status: Status,
}
impl Txn {
    /// roachpb.Transaction.Update, reduced to the fields that exist here.
    fn update(&mut self, o: &Txn) {
        if o.meta.epoch > self.meta.epoch {
            self.meta.epoch = o.meta.epoch;
        }
        self.meta.write_ts = self.meta.write_ts.max(o.meta.write_ts);
        self.read_ts = self.read_ts.max(o.read_ts);
        self.meta.min_ts = self.meta.min_ts.min(o.meta.min_ts);
        self.meta.seq = self.meta.seq.max(o.meta.seq);
        if o.status.rank() > self.status.rank() {
            self.status = o.status;
        }
    }
}

/// roachpb.SequencedWrite (strength is always Intent).
#[derive(Clone, Copy, Debug, PartialEq)]
struct SeqWrite {
    key: Key,
    seq: u32,
}

/// roachpb.TransactionRecord of T, on r1.
#[derive(Clone, Debug, PartialEq)]
struct Record {
    meta: Meta,
    status: Status,
    in_flight: Vec<SeqWrite>,
    lock_spans: Vec<Key>,
}
impl Record {
    fn as_txn(&self) -> Txn {
        // A record has no read timestamp; Update with the header forwards it.
        Txn { meta: self.meta, read_ts: 0, status: self.status }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Request {
    Put { key: Key, seq: u32 },
    QueryIntent { key: Key, txn: Meta, error_if_missing: bool },
    QueryTxn { txn: Meta },
    EndTxn { commit: bool, lock_spans: Vec<Key>, in_flight: Vec<SeqWrite> },
    PushTxn { pushee: Meta },
    RecoverTxn { txn: Meta, implicitly_committed: bool },
    ResolveIntent { key: Key, txn: Meta, status: Status },
}
impl Request {
    /// The key that addresses the request (the record lives at k1).
    fn key(&self) -> Key {
        match self {
            Request::Put { key, .. }
            | Request::QueryIntent { key, .. }
            | Request::ResolveIntent { key, .. } => *key,
            _ => 1,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Response {
    Put,
    QueryIntent { found_intent: bool, found_unpushed: bool },
    QueryTxn { txn: Record, exists: bool },
    EndTxn { staging_ts: Option<Ts> },
    PushTxn { pushee: Record },
    RecoverTxn { recovered: Record },
    ResolveIntent,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum RetryReason {
    Serializable,
    AsyncWriteFailure,
}
#[derive(Clone, Copy, Debug, PartialEq)]
enum AbortReason {
    TimestampCacheRejected,
    AbortedRecordFound,
}

#[derive(Clone, Debug, PartialEq)]
enum ErrKind {
    IntentMissing(Key),
    TransactionRetry(RetryReason),
    TransactionAborted(AbortReason),
    /// TransactionStatusError with REASON_TXN_COMMITTED ("already committed").
    StatusCommitted,
    IndeterminateCommit(Record),
}

#[derive(Clone, Debug, PartialEq)]
struct Error {
    kind: ErrKind,
    index: usize,
    txn: Option<Txn>,
}

#[derive(Clone, Debug, PartialEq)]
struct Batch {
    id: u32,
    from: ThreadId,
    txn: Option<Txn>,
    ts: Ts,
    reqs: Vec<Request>,
    /// false for the ResolveIntents of r1's folded asynchronous intent resolver.
    reply: bool,
}

#[derive(Clone, Debug, PartialEq)]
struct BatchResp {
    id: u32,
    txn: Option<Txn>,
    result: Result<Vec<Response>, Error>,
}

#[derive(Clone, Debug, PartialEq)]
struct Setup {
    c: ThreadId,
    ps: Vec<ThreadId>,
    tc: ThreadId,
    tcx: ThreadId,
    hs: Vec<ThreadId>,
    rs: Vec<ThreadId>,
    meta: Meta,
}

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    Setup(Setup),
    Batch(Batch),
    Resp(BatchResp),
    Explicit { txn: Txn, lock_spans: Vec<Key> },
}

fn recv_tag(tag: u32) -> Msg {
    recv_tagged_msg_block::<_, Msg>(move |_, t| t == Some(tag))
}
fn recv_setup() -> Setup {
    let Msg::Setup(s) = recv_tag(T_SETUP) else { panic!("Setup expected") };
    s
}

fn merge_spans(a: &[Key], ws: &[SeqWrite]) -> Vec<Key> {
    let mut v: Vec<Key> = a.to_vec();
    v.extend(ws.iter().map(|w| w.key));
    v.sort();
    v.dedup();
    v
}

// ---------------------------------------------------------------------------
// DistSender (shared by tc, tcx and the handles): divides a batch per range,
// sends the sub-batches concurrently and waits for all their responses.
// ---------------------------------------------------------------------------

type SubResult = (Option<Txn>, Result<Vec<Response>, Error>);

struct Ds {
    me: ThreadId,
    rs: Vec<ThreadId>,
    next_id: u32,
    ordered: bool,
}

impl Ds {
    fn new(rs: Vec<ThreadId>, ordered: bool) -> Self {
        Ds { me: thread::current_id(), rs, next_id: 0, ordered }
    }

    /// Sends every group (a batch: header txn, timestamp, requests) divided per range, all
    /// concurrently, and returns each group's combined result in its original request order.
    /// Two sub-batches for the same range (only the parallel commit's EndTxn batch and its
    /// pre-commit QueryIntent batch) are sent in a nondet() order when channels are ordered.
    fn dispatch(&mut self, groups: Vec<(Option<Txn>, Ts, Vec<Request>)>) -> Vec<SubResult> {
        // (group, range, original indices, batch)
        let mut subs: Vec<(usize, usize, Vec<usize>, Batch)> = Vec::new();
        for (g, (txn, ts, reqs)) in groups.iter().enumerate() {
            let mut per: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
            for (i, r) in reqs.iter().enumerate() {
                per.entry(r.key() as usize - 1).or_default().push(i);
            }
            for (range, idxs) in per {
                self.next_id += 1;
                let b = Batch {
                    id: self.next_id,
                    from: self.me,
                    txn: txn.clone(),
                    ts: *ts,
                    reqs: idxs.iter().map(|&i| reqs[i].clone()).collect(),
                    reply: true,
                };
                subs.push((g, range, idxs, b));
            }
        }
        // Send order: by range; same-range pairs in either order.
        let mut order: Vec<usize> = (0..subs.len()).collect();
        order.sort_by_key(|&i| subs[i].1);
        let mut k = 0;
        while k + 1 < order.len() {
            if subs[order[k]].1 == subs[order[k + 1]].1 {
                if self.ordered && nondet() {
                    order.swap(k, k + 1);
                }
                k += 2;
            } else {
                k += 1;
            }
        }
        for &i in &order {
            let b = subs[i].3.clone();
            send_tagged_msg(self.rs[subs[i].1], T_BATCH, Msg::Batch(b));
        }
        // Wait for every response. They are received in a fixed order (group, then range):
        // nothing is sent between them and the combination does not depend on their order,
        // so receiving them in arrival order would only repeat the same outcomes (NOTES.md D3).
        let mut got: BTreeMap<u32, BatchResp> = BTreeMap::new();
        for sub in &subs {
            let id = sub.3.id;
            let Msg::Resp(r) = recv_tag(T_RESP + id) else { panic!("response expected") };
            got.insert(r.id, r);
        }
        // Combine per group.
        let mut out = Vec::new();
        for (g, (_, _, reqs)) in groups.iter().enumerate() {
            let mut txn: Option<Txn> = None;
            let mut resps: Vec<Option<Response>> = vec![None; reqs.len()];
            let mut err: Option<Error> = None;
            for (sg, _, idxs, b) in &subs {
                if *sg != g {
                    continue;
                }
                let r = &got[&b.id];
                if let Some(t) = &r.txn {
                    match &mut txn {
                        None => txn = Some(t.clone()),
                        Some(x) => x.update(t),
                    }
                }
                match &r.result {
                    Ok(rs) => {
                        for (j, resp) in rs.iter().enumerate() {
                            resps[idxs[j]] = Some(resp.clone());
                        }
                    }
                    Err(e) => {
                        let mut e = e.clone();
                        e.index = idxs[e.index];
                        // At most one sub-batch fails here (README "Errors"); keep the
                        // lowest index as mergeErrors would among equal priorities.
                        if err.as_ref().map_or(true, |o| e.index < o.index) {
                            err = Some(e);
                        }
                    }
                }
            }
            match err {
                Some(mut e) => {
                    if let (Some(et), Some(t)) = (&mut e.txn, &txn) {
                        et.update(t);
                    }
                    out.push((txn, Err(e)));
                }
                None => out.push((txn, Ok(resps.into_iter().map(|r| r.unwrap()).collect()))),
            }
        }
        out
    }

    fn send(&mut self, txn: Option<Txn>, ts: Ts, reqs: Vec<Request>) -> SubResult {
        self.dispatch(vec![(txn, ts, reqs)]).pop().unwrap()
    }

    /// DistSender.Send for a transactional batch: divideAndSendParallelCommit when the batch
    /// ends with an EndTxn preceded by pre-commit QueryIntents, else a plain divided send.
    fn send_txn(&mut self, txn: Txn, reqs: Vec<Request>) -> SubResult {
        let ts = txn.read_ts;
        let last = reqs.len() - 1;
        let has_et = matches!(reqs[last], Request::EndTxn { .. });
        let mut swap: Option<usize> = None;
        if has_et {
            let mut i = last;
            while i > 0 && matches!(reqs[i - 1], Request::QueryIntent { .. }) {
                i -= 1;
                swap = Some(i);
            }
        }
        let Some(swap_idx) = swap else {
            return self.send(Some(txn), ts, reqs);
        };
        // [puts.. qi(swap).. qi.. et] -> main [puts.. et], qi batch [qi(swap)..]
        let mut main: Vec<Request> = reqs[..swap_idx].to_vec();
        main.push(reqs[last].clone());
        let qi: Vec<Request> = reqs[swap_idx..last].to_vec();
        let qi_len = qi.len();
        let mut res = self.dispatch(vec![(Some(txn.clone()), ts, main), (Some(txn.clone()), ts, qi)]);
        let (qi_txn, qi_res) = res.pop().unwrap();
        let (br_txn, br_res) = res.pop().unwrap();
        // The batch with the EndTxn returned an error: it wins (dist_sender l.1675-1684).
        let main_resps = match br_res {
            Err(mut e) => {
                if let (Some(et), Some(q)) = (&mut e.txn, &qi_txn) {
                    et.update(q);
                }
                if e.index == swap_idx {
                    e.index = last;
                }
                return (br_txn, Err(e));
            }
            Ok(r) => r,
        };
        let mut br_txn = br_txn.expect("EndTxn batch response without txn");
        let qi_resps = match qi_res {
            Ok(r) => r,
            Err(e) => {
                let mut ignore_missing = false;
                if let ErrKind::IntentMissing(_) = e.kind {
                    match self.detect_intent_missing_due_to_intent_resolution(&br_txn) {
                        Ok(b) => ignore_missing = b,
                        Err(kind) => {
                            return (Some(br_txn.clone()), Err(Error { kind, index: last, txn: Some(br_txn) }));
                        }
                    }
                }
                if !ignore_missing {
                    let mut e = e.clone();
                    e.index += swap_idx;
                    let mut t = br_txn.clone();
                    if let Some(et) = &e.txn {
                        t.update(et);
                    }
                    e.txn = Some(t);
                    return (Some(br_txn), Err(e));
                }
                (0..qi_len)
                    .map(|_| Response::QueryIntent { found_intent: true, found_unpushed: true })
                    .collect()
            }
        };
        if let Some(q) = &qi_txn {
            br_txn.update(q);
        }
        // Stitch the responses back into the original order.
        let mut resps: Vec<Response> = main_resps[..swap_idx].to_vec();
        resps.extend(qi_resps);
        resps.push(main_resps[swap_idx].clone());
        (Some(br_txn), Ok(resps))
    }

    /// dist_sender.go detectIntentMissingDueToIntentResolution l.1746-1798: Ok(true) when the
    /// missing intent was resolved by a committed recovery.
    fn detect_intent_missing_due_to_intent_resolution(&mut self, txn: &Txn) -> Result<bool, ErrKind> {
        // ba.Timestamp = clock.Now(): the timestamp in hand, T's write timestamp.
        let (_, r) = self.send(None, txn.meta.write_ts, vec![Request::QueryTxn { txn: txn.meta }]);
        let resps = match r {
            Ok(r) => r,
            Err(e) => panic!("QueryTxn failed (ambiguous result, unreachable here): {:?}", e),
        };
        let Response::QueryTxn { txn: q, exists } = &resps[0] else { panic!("QueryTxn response expected") };
        match q.status {
            Status::Committed => Ok(true),
            Status::Aborted => {
                if *exists {
                    Err(ErrKind::TransactionAborted(AbortReason::AbortedRecordFound))
                } else {
                    // AmbiguousResultError "intent missing and record aborted": the record is
                    // written by the EndTxn of this very batch and never GCed.
                    panic!("ambiguous result: intent missing and no record (unreachable)")
                }
            }
            _ => Ok(false),
        }
    }
}

// ---------------------------------------------------------------------------
// Ranges r1, r2: per-key cell, timestamp cache, and (r1) T's record and markers.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq)]
struct Intent {
    epoch: u32,
    ts: Ts,
    seq: u32,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct Cell {
    intent: Option<Intent>,
    value: Option<Ts>,
}

#[derive(Clone, Debug)]
struct RangeState {
    /// Index of the range: key k(idx+1) lives here.
    idx: usize,
    cell: Cell,
    /// Timestamp cache entry of the range's key (initially 0).
    tscache: Ts,
    record: Option<Record>,
    tombstone: Ts,
    push_marker: Ts,
}

/// Effects after a batch: ResolveIntents of T's intents for r1's folded intent resolver.
#[derive(Default)]
struct Post {
    resolve: Vec<(Key, Meta, Status)>,
}

impl RangeState {
    fn local(&self, k: Key) -> bool {
        k as usize == self.idx + 1
    }

    /// MVCCResolveWriteIntent on the per-key cell.
    fn resolve(&mut self, txn: &Meta, status: Status) {
        if let Some(i) = self.cell.intent {
            if status == Status::Committed && i.epoch == txn.epoch {
                self.cell.value = Some(i.ts);
            }
            // ABORTED, or COMMITTED of an intent from an older epoch: removed.
            self.cell.intent = None;
        }
    }

    /// transaction.go SynthesizeTxnFromMeta l.167-203 with replica_tscache.go
    /// CanCreateTxnRecord l.598-656 (all tombstone cases fail).
    fn can_create(&self, min_ts: Ts) -> Result<(), AbortReason> {
        if min_ts <= self.tombstone {
            // The tombstone carries no txn id here; the reason is informational.
            return Err(AbortReason::TimestampCacheRejected);
        }
        Ok(())
    }
    fn synthesize(&self, meta: &Meta) -> Record {
        let mut r = Record { meta: *meta, status: Status::Pending, in_flight: vec![], lock_spans: vec![] };
        if self.can_create(meta.min_ts).is_ok() {
            r.meta.write_ts = r.meta.write_ts.max(self.push_marker);
        } else {
            r.status = Status::Aborted;
        }
        r
    }

    /// replica_batch_updates.go maybeStripInFlightWrites l.37-170.
    fn strip_in_flight_writes(reqs: &mut [Request]) {
        let last = reqs.len() - 1;
        let Request::EndTxn { in_flight, .. } = &reqs[last] else { return };
        if in_flight.is_empty() || last == 0 {
            return;
        }
        let orig = in_flight.clone();
        let others: Vec<Request> = reqs[..last].to_vec();
        let Request::EndTxn { lock_spans, in_flight, .. } = &mut reqs[last] else { unreachable!() };
        if others.len() >= orig.len() {
            let writes = others
                .iter()
                .filter(|r| matches!(r, Request::Put { .. } | Request::QueryIntent { .. }))
                .count();
            if orig.len() < writes {
                panic!("more write in batch with EndTxn than listed in in-flight writes");
            } else if orig.len() == writes {
                *lock_spans = merge_spans(lock_spans, &orig);
                in_flight.clear();
                return;
            }
        }
        let mut remaining = orig.clone();
        for r in &others {
            if let Request::Put { key, seq } = r {
                let Some(pos) = remaining.iter().position(|w| w.seq == *seq) else {
                    panic!("write in batch with EndTxn missing from in-flight writes")
                };
                remaining.remove(pos);
                lock_spans.push(*key);
            }
        }
        *in_flight = remaining;
        lock_spans.sort();
        lock_spans.dedup();
    }

    /// Evaluates one batch atomically; returns the response and the post-batch effects.
    fn eval_batch(&mut self, b: &Batch) -> (BatchResp, Post) {
        let mut reqs = b.reqs.clone();
        Self::strip_in_flight_writes(&mut reqs);
        let mut txn = b.txn.clone();
        // applyTimestampCache l.397-464: forward the write timestamp above the cache for
        // the keys the batch writes (all cache entries come from QueryIntents, with no txn id).
        for r in &reqs {
            if let Request::Put { .. } = r {
                if let Some(t) = &mut txn {
                    if t.meta.write_ts < self.tscache + 1 {
                        t.meta.write_ts = self.tscache + 1;
                    }
                }
            }
        }
        // The batch's timestamp (ba.Timestamp: the read timestamp of a txn batch).
        let bts = match &txn {
            Some(t) => t.read_ts,
            None => b.ts,
        };
        let mut st = self.clone();
        let mut post = Post::default();
        let mut resps = Vec::new();
        for (i, r) in reqs.iter().enumerate() {
            match st.eval_one(r, &mut txn, bts, &mut post) {
                Ok(resp) => resps.push(resp),
                Err(kind) => {
                    // Nothing of the batch is applied; only a QueryIntent at the error's
                    // index updates the timestamp cache on error (updateTimestampCache).
                    if let (Request::QueryIntent { txn: m, .. }, ErrKind::IntentMissing(_)) = (r, &kind) {
                        self.tscache = self.tscache.max(m.write_ts);
                    }
                    // No resolution on error: only a batch that finalizes T's record makes
                    // r1 resolve T's intents (interface; NOTES.md D9).
                    return (
                        BatchResp { id: b.id, txn: txn.clone(), result: Err(Error { kind, index: i, txn }) },
                        Post::default(),
                    );
                }
            }
        }
        // updateTimestampCache l.90-369 (cases EndTxn, RecoverTxn, PushTxn, QueryIntent).
        for (r, resp) in reqs.iter().zip(resps.iter()) {
            match (r, resp) {
                (Request::EndTxn { .. }, _) => st.tombstone = st.tombstone.max(bts),
                (Request::RecoverTxn { .. }, Response::RecoverTxn { recovered }) => {
                    if recovered.status.finalized() {
                        st.tombstone = st.tombstone.max(bts);
                    }
                }
                (Request::PushTxn { .. }, Response::PushTxn { pushee }) => match pushee.status {
                    Status::Aborted => st.tombstone = st.tombstone.max(pushee.meta.min_ts),
                    Status::Pending => st.push_marker = st.push_marker.max(pushee.meta.write_ts),
                    _ => {}
                },
                (Request::QueryIntent { txn: m, .. }, Response::QueryIntent { found_unpushed, .. }) => {
                    if !found_unpushed {
                        st.tscache = st.tscache.max(m.write_ts);
                    }
                }
                _ => {}
            }
        }
        *self = st;
        // Local resolutions were applied inside the batch; keep only the external ones.
        post.resolve.retain(|(k, _, _)| !self.local(*k));
        (BatchResp { id: b.id, txn: txn.clone(), result: Ok(resps) }, post)
    }

    fn eval_one(&mut self, r: &Request, txn: &mut Option<Txn>, bts: Ts, post: &mut Post) -> Result<Response, ErrKind> {
        match r {
            Request::Put { seq, .. } => {
                let t = txn.as_ref().expect("Put without txn");
                // The intent is laid (or rewritten) at the batch's (bumped) write timestamp.
                self.cell.intent = Some(Intent { epoch: t.meta.epoch, ts: t.meta.write_ts, seq: *seq });
                Ok(Response::Put)
            }
            Request::QueryIntent { txn: m, error_if_missing, .. } => self.query_intent(m, *error_if_missing, txn),
            Request::QueryTxn { txn: m } => {
                assert!(txn.is_none() && bts >= m.min_ts);
                Ok(match &self.record {
                    Some(rec) => Response::QueryTxn { txn: rec.clone(), exists: true },
                    None => Response::QueryTxn { txn: self.synthesize(m), exists: false },
                })
            }
            Request::EndTxn { commit, lock_spans, in_flight } => {
                let t = txn.as_mut().expect("EndTxn without txn");
                self.end_txn(t, *commit, lock_spans, in_flight, post)
            }
            Request::PushTxn { pushee } => self.push_txn(pushee, bts),
            Request::RecoverTxn { txn: m, implicitly_committed } => self.recover_txn(m, *implicitly_committed, bts),
            Request::ResolveIntent { txn: m, status, .. } => {
                self.resolve(m, *status);
                Ok(Response::ResolveIntent)
            }
        }
    }

    /// cmd_query_intent.go QueryIntent l.60-167 (intent strength only).
    fn query_intent(&mut self, m: &Meta, error_if_missing: bool, txn: &mut Option<Txn>) -> Result<Response, ErrKind> {
        let own = txn.is_some();
        let mut found_intent = false;
        let mut found_unpushed = false;
        if let Some(i) = self.cell.intent {
            found_intent = m.epoch == i.epoch && m.seq <= i.seq;
            if found_intent {
                let mut cmp = m.write_ts;
                if let Some(t) = txn.as_ref() {
                    cmp = cmp.max(t.meta.write_ts);
                }
                found_unpushed = i.ts <= cmp;
                if !found_unpushed && own {
                    let t = txn.as_mut().unwrap();
                    t.meta.write_ts = t.meta.write_ts.max(i.ts);
                }
            }
        }
        if !found_intent && error_if_missing {
            return Err(ErrKind::IntentMissing(self.idx as Key + 1));
        }
        Ok(Response::QueryIntent { found_intent, found_unpushed })
    }

    /// cmd_end_transaction.go EndTxn l.248-605 (commit path; no 1PC, triggers, prepare).
    fn end_txn(
        &mut self,
        h: &mut Txn,
        commit: bool,
        lock_spans: &[Key],
        in_flight: &[SeqWrite],
        post: &mut Post,
    ) -> Result<Response, ErrKind> {
        assert!(commit, "no rollback in this port");
        let existed = self.record.clone();
        let mut reply: Txn;
        match &existed {
            None => {
                reply = h.clone();
                if let Err(reason) = self.can_create(reply.meta.min_ts) {
                    return Err(ErrKind::TransactionAborted(reason));
                }
                assert!(reply.status == Status::Pending, "cannot create transaction record with non-PENDING transaction");
            }
            Some(rec) => {
                reply = rec.as_txn();
                match rec.status {
                    Status::Committed => return Err(ErrKind::StatusCommitted),
                    Status::Aborted => return Err(ErrKind::TransactionAborted(AbortReason::AbortedRecordFound)),
                    Status::Pending => assert!(h.meta.epoch >= rec.meta.epoch, "epoch regression"),
                    Status::Staging => {
                        assert!(h.meta.epoch >= rec.meta.epoch, "epoch regression");
                        if h.meta.epoch > rec.meta.epoch {
                            reply.status = Status::Pending;
                        }
                    }
                }
                reply.update(h);
            }
        }
        // BumpToMinTxnCommitTS for a missing or PENDING record (never for STAGING).
        if existed.as_ref().map_or(true, |r| r.status == Status::Pending) {
            reply.meta.write_ts = reply.meta.write_ts.max(self.push_marker);
        }
        // IsEndTxnTriggeringRetryError (serializable; no deadline).
        if reply.meta.write_ts > reply.read_ts {
            return Err(ErrKind::TransactionRetry(RetryReason::Serializable));
        }
        if !in_flight.is_empty() {
            // Parallel commit: stage the record.
            reply.status = Status::Staging;
            let staging_ts = reply.meta.write_ts;
            self.record = Some(Record {
                meta: reply.meta,
                status: Status::Staging,
                in_flight: in_flight.to_vec(),
                lock_spans: lock_spans.to_vec(),
            });
            *h = reply;
            return Ok(Response::EndTxn { staging_ts: Some(staging_ts) });
        }
        reply.status = Status::Committed;
        // resolveLocalLocks, then updateFinalizedTxn with the external locks.
        let mut external = Vec::new();
        for k in lock_spans {
            if self.local(*k) {
                let m = reply.meta;
                self.resolve(&m, Status::Committed);
            } else {
                external.push(*k);
                post.resolve.push((*k, reply.meta, Status::Committed));
            }
        }
        self.record = Some(Record { meta: reply.meta, status: Status::Committed, in_flight: vec![], lock_spans: external });
        *h = reply;
        Ok(Response::EndTxn { staging_ts: None })
    }

    /// cmd_push_txn.go PushTxn l.111-350 with pusherWins = true, PUSH_ABORT.
    fn push_txn(&mut self, args: &Meta, bts: Ts) -> Result<Response, ErrKind> {
        assert!(bts >= args.min_ts);
        let exists = self.record.is_some();
        let mut pushee = match &self.record {
            Some(r) => r.clone(),
            None => {
                let s = self.synthesize(args);
                if s.status == Status::Aborted {
                    return Ok(Response::PushTxn { pushee: s });
                }
                s
            }
        };
        if pushee.status.finalized() {
            return Ok(Response::PushTxn { pushee });
        }
        let mut known_higher = false;
        if pushee.meta.write_ts < args.write_ts {
            pushee.meta.write_ts = args.write_ts;
            known_higher = true;
        }
        if pushee.meta.epoch < args.epoch {
            pushee.meta.epoch = args.epoch;
            known_higher = true;
        }
        let staging = pushee.status == Status::Staging;
        if staging && !known_higher {
            return Err(ErrKind::IndeterminateCommit(pushee));
        }
        pushee.status = Status::Aborted;
        pushee.in_flight.clear();
        if exists {
            self.record = Some(pushee.clone());
        }
        Ok(Response::PushTxn { pushee })
    }

    /// cmd_recover_txn.go RecoverTxn l.52-234.
    fn recover_txn(&mut self, args: &Meta, implicit: bool, bts: Ts) -> Result<Response, ErrKind> {
        assert!(bts >= args.min_ts);
        let Some(rec) = self.record.clone() else {
            let s = self.synthesize(args);
            assert!(s.status == Status::Aborted, "txn record synthesized with non-ABORTED status");
            return Ok(Response::RecoverTxn { recovered: s });
        };
        let mut rec = rec;
        if implicit {
            match rec.status {
                Status::Pending | Status::Aborted => {
                    panic!("programming error: found {:?} record for implicitly committed transaction", rec.status)
                }
                Status::Staging | Status::Committed => {
                    assert!(args.epoch == rec.meta.epoch, "epoch change by implicitly committed transaction");
                    assert!(args.write_ts == rec.meta.write_ts, "timestamp change by implicitly committed transaction");
                    if rec.status == Status::Committed {
                        return Ok(Response::RecoverTxn { recovered: rec });
                    }
                }
            }
        } else {
            let legal = args.epoch < rec.meta.epoch || args.write_ts < rec.meta.write_ts;
            match rec.status {
                Status::Aborted | Status::Committed => return Ok(Response::RecoverTxn { recovered: rec }),
                Status::Pending => {
                    if args.epoch < rec.meta.epoch {
                        return Ok(Response::RecoverTxn { recovered: rec });
                    }
                    panic!("programming error: cannot recover PENDING transaction in same epoch");
                }
                Status::Staging => {
                    if legal {
                        return Ok(Response::RecoverTxn { recovered: rec });
                    }
                }
            }
        }
        rec.lock_spans = merge_spans(&rec.lock_spans, &rec.in_flight);
        rec.in_flight.clear();
        rec.status = if implicit { Status::Committed } else { Status::Aborted };
        self.record = Some(rec.clone());
        Ok(Response::RecoverTxn { recovered: rec })
    }
}

fn range_proc(idx: usize) {
    let s = recv_setup();
    let mut st = RangeState {
        idx,
        cell: Cell::default(),
        tscache: 0,
        record: None,
        tombstone: 0,
        push_marker: 0,
    };
    loop {
        let Msg::Batch(b) = recv_tag(T_BATCH) else { panic!("Batch expected") };
        let (resp, post) = st.eval_batch(&b);
        if b.reply {
            send_tagged_msg(b.from, T_RESP + b.id, Msg::Resp(resp));
        }
        // The folded asynchronous intent resolver: ResolveIntent to the key's range, no reply.
        for (k, m, status) in post.resolve {
            let rb = Batch {
                id: 0,
                from: thread::current_id(),
                txn: None,
                ts: m.write_ts,
                reqs: vec![Request::ResolveIntent { key: k, txn: m, status }],
                reply: false,
            };
            send_tagged_msg(s.rs[k as usize - 1], T_BATCH, Msg::Batch(rb));
        }
    }
}

// ---------------------------------------------------------------------------
// tc: TxnCoordSender interceptors (pipeliner, committer, span refresher) + DistSender.
// ---------------------------------------------------------------------------

struct Tc {
    ds: Ds,
    tcx: ThreadId,
    txn: Txn,
    seq: u32,
    /// The pipeliner's in-flight write set and lock footprint.
    ifw: Vec<SeqWrite>,
    footprint: Vec<Key>,
}

const MAX_TXN_REFRESH_ATTEMPTS: u32 = 5;

impl Tc {
    /// txnSpanRefresher.SendLocked: preemptive refresh (free: T has no refresh spans),
    /// then sendLockedWithRefreshAttempts.
    fn refresher_send(&mut self, mut txn: Txn, reqs: Vec<Request>) -> SubResult {
        if txn.meta.write_ts > txn.read_ts {
            txn.read_ts = txn.meta.write_ts;
        }
        self.send_with_refresh_attempts(txn, reqs, MAX_TXN_REFRESH_ATTEMPTS)
    }

    fn send_with_refresh_attempts(&mut self, txn: Txn, reqs: Vec<Request>, attempts: u32) -> SubResult {
        let (btxn, res) = self.ds.send_txn(txn, reqs.clone());
        match res {
            Err(e) if attempts > 0 => self.maybe_refresh_and_retry_send(reqs, e, btxn, attempts),
            r => (btxn, r),
        }
    }

    /// txn_interceptor_span_refresher.go maybeRefreshAndRetrySend l.249-324.
    fn maybe_refresh_and_retry_send(&mut self, reqs: Vec<Request>, e: Error, btxn: Option<Txn>, attempts: u32) -> SubResult {
        let Some(etxn) = e.txn.clone() else { return (btxn, Err(e)) };
        let refresh_ts = match e.kind {
            ErrKind::TransactionRetry(RetryReason::Serializable) => etxn.meta.write_ts,
            _ => return (btxn, Err(e)),
        };
        let mut to = etxn.clone();
        to.read_ts = to.read_ts.max(refresh_ts);
        match to.status {
            Status::Pending => {}
            Status::Staging => to.status = Status::Pending,
            s => panic!("unexpected txn status during refresh: {:?}", s),
        }
        // tryRefreshTxnSpans succeeds: T has no read spans.
        let has_et = matches!(reqs.last(), Some(Request::EndTxn { .. }));
        if reqs.len() > 1 && has_et {
            return self.split_end_txn_and_retry_send(to, reqs);
        }
        self.send_with_refresh_attempts(to, reqs, attempts - 1)
    }

    /// splitEndTxnAndRetrySend: the prefix, then the EndTxn alone with its in-flight writes
    /// moved to the lock spans.
    fn split_end_txn_and_retry_send(&mut self, txn: Txn, reqs: Vec<Request>) -> SubResult {
        let et_idx = reqs.len() - 1;
        let prefix = reqs[..et_idx].to_vec();
        let (ptxn, pres) = self.refresher_send(txn.clone(), prefix);
        let mut presps = match pres {
            Ok(r) => r,
            Err(e) => return (ptxn, Err(e)),
        };
        let mut stxn = txn;
        if let Some(p) = &ptxn {
            stxn.update(p);
        }
        let mut et = reqs[et_idx].clone();
        if let Request::EndTxn { lock_spans, in_flight, .. } = &mut et {
            if !in_flight.is_empty() {
                *lock_spans = merge_spans(lock_spans, in_flight);
                in_flight.clear();
            }
        }
        let (s_txn, sres) = self.refresher_send(stxn, vec![et]);
        match sres {
            Ok(r) => {
                presps.extend(r);
                let t = s_txn.expect("txn");
                assert!(t.status.finalized(), "txn status not finalized after successful retried EndTxn");
                (Some(t), Ok(presps))
            }
            Err(e) => (s_txn, Err(e)),
        }
    }

    /// txnPipeliner.SendLocked l.311-361 around the committer (for an EndTxn batch) or
    /// straight to the span refresher.
    fn pipeliner_send(&mut self, mut reqs: Vec<Request>) -> SubResult {
        let has_et = matches!(reqs.last(), Some(Request::EndTxn { .. }));
        // attachLocksToEndTxn l.437-501.
        if has_et {
            let last = reqs.len() - 1;
            let mut lock_spans = self.footprint.clone();
            let mut in_flight = self.ifw.clone();
            for r in &reqs[..last] {
                if let Request::Put { key, seq } = r {
                    in_flight.push(SeqWrite { key: *key, seq: *seq });
                }
            }
            lock_spans.sort();
            lock_spans.dedup();
            in_flight.sort_by_key(|w| w.seq);
            if let Request::EndTxn { lock_spans: l, in_flight: f, .. } = &mut reqs[last] {
                *l = lock_spans;
                *f = in_flight;
            }
        }
        // canUseAsyncConsensus: point writes without an EndTxn.
        let async_consensus = !has_et && reqs.iter().all(|r| matches!(r, Request::Put { .. }));
        // chainToInFlightWrites l.608-718.
        let reqs = self.chain_to_in_flight_writes(reqs);
        let (btxn, res) = if has_et { self.committer_send(reqs.clone()) } else { self.refresher_send(self.txn.clone(), reqs.clone()) };
        self.update_lock_tracking(&reqs, &btxn, &res, async_consensus);
        match res {
            // adjustError l.929-954: IntentMissing -> TransactionRetry(RETRY_ASYNC_WRITE_FAILURE).
            Err(mut e) => {
                if let ErrKind::IntentMissing(_) = e.kind {
                    e.kind = ErrKind::TransactionRetry(RetryReason::AsyncWriteFailure);
                }
                (btxn, Err(e))
            }
            ok => (btxn, ok),
        }
    }

    fn chain_to_in_flight_writes(&mut self, reqs: Vec<Request>) -> Vec<Request> {
        if self.ifw.is_empty() {
            return reqs;
        }
        let mut chained: Vec<Key> = Vec::new();
        let mut out = Vec::new();
        let mut ifw_by_key = self.ifw.clone();
        ifw_by_key.sort_by_key(|w| w.key);
        for r in reqs {
            let overlapping: Vec<SeqWrite> = match &r {
                Request::EndTxn { commit: true, .. } => ifw_by_key.clone(),
                Request::Put { key, .. } => ifw_by_key.iter().filter(|w| w.key == *key).cloned().collect(),
                _ => vec![],
            };
            for w in overlapping {
                if !chained.contains(&w.key) {
                    let mut meta = self.txn.meta;
                    meta.seq = w.seq;
                    out.push(Request::QueryIntent { key: w.key, txn: meta, error_if_missing: true });
                    chained.push(w.key);
                }
            }
            out.push(r);
        }
        out
    }

    /// updateLockTrackingInner l.796-899.
    fn update_lock_tracking(&mut self, reqs: &[Request], btxn: &Option<Txn>, res: &Result<Vec<Response>, Error>, async_consensus: bool) {
        let resps = match res {
            Err(_) => {
                for r in reqs {
                    if let Request::Put { key, .. } = r {
                        self.footprint.push(*key);
                    }
                }
                self.footprint.sort();
                self.footprint.dedup();
                return;
            }
            Ok(r) => r,
        };
        if let Some(t) = btxn {
            match t.status {
                Status::Aborted => {
                    for r in reqs {
                        if let Request::Put { key, .. } = r {
                            self.footprint.push(*key);
                        }
                    }
                    return;
                }
                Status::Committed => {
                    self.ifw.clear();
                    return;
                }
                _ => {}
            }
        }
        for (r, resp) in reqs.iter().zip(resps.iter()) {
            match (r, resp) {
                (Request::QueryIntent { key, txn, .. }, Response::QueryIntent { found_intent: true, .. }) => {
                    self.ifw.retain(|w| !(w.key == *key && w.seq == txn.seq));
                    self.footprint.push(*key);
                }
                (Request::Put { key, seq }, _) => {
                    if async_consensus {
                        self.ifw.push(SeqWrite { key: *key, seq: *seq });
                    } else {
                        self.footprint.push(*key);
                    }
                }
                _ => {}
            }
        }
        self.footprint.sort();
        self.footprint.dedup();
    }

    /// txn_interceptor_committer.go SendLocked l.138-261.
    fn committer_send(&mut self, reqs: Vec<Request>) -> SubResult {
        let last = reqs.len() - 1;
        let mut reqs = reqs;
        // canCommitInParallel l.349-396: every other request is a point write or a QueryIntent.
        let parallel = reqs[..last].iter().all(|r| matches!(r, Request::Put { .. } | Request::QueryIntent { .. }));
        if let Request::EndTxn { lock_spans, in_flight, .. } = &mut reqs[last] {
            if !in_flight.is_empty() && !parallel {
                *lock_spans = merge_spans(lock_spans, in_flight);
                in_flight.clear();
            }
        }
        let (btxn, res) = self.refresher_send(self.txn.clone(), reqs.clone());
        let resps = match res {
            Err(mut e) => {
                // maybeRemoveStagingStatusInErr.
                if let Some(t) = &mut e.txn {
                    if t.status == Status::Staging {
                        t.status = Status::Pending;
                    }
                }
                return (btxn, Err(e));
            }
            Ok(r) => r,
        };
        let mut br_txn = btxn.expect("response without txn");
        match br_txn.status {
            Status::Staging => {}
            Status::Committed => return (Some(br_txn), Ok(resps)),
            s => panic!("unexpected response status without error: {:?}", s),
        }
        // isTxnCommitImplicit l.413-441.
        let Some(Response::EndTxn { staging_ts: Some(staging_ts) }) = resps.last().cloned() else {
            panic!("empty StagingTimestamp in EndTxnResponse")
        };
        let implicit = !(staging_ts < br_txn.meta.write_ts);
        let Request::EndTxn { lock_spans, in_flight, .. } = reqs[last].clone() else { unreachable!() };
        if !implicit {
            // retryTxnCommitAfterFailedParallelCommit l.443-486.
            let mut t = br_txn.clone();
            t.status = Status::Pending;
            let et = Request::EndTxn { commit: true, lock_spans: merge_spans(&lock_spans, &in_flight), in_flight: vec![] };
            let (stxn, sres) = self.refresher_send(t, vec![et]);
            return match sres {
                Err(mut e) => {
                    if let Some(t) = &mut e.txn {
                        if t.status == Status::Staging {
                            t.status = Status::Pending;
                        }
                    }
                    (stxn, Err(e))
                }
                Ok(r) => {
                    let mut all = resps;
                    all[last] = r[0].clone();
                    let mut t = br_txn;
                    if let Some(s) = &stxn {
                        t.update(s);
                    }
                    assert!(t.status.finalized(), "txn status not finalized after successful retried EndTxn");
                    (Some(t), Ok(all))
                }
            };
        }
        // makeTxnCommitExplicitAsync l.492-527: the task tcx.
        send_tagged_msg(
            self.tcx,
            T_EXPLICIT,
            Msg::Explicit { txn: br_txn.clone(), lock_spans: merge_spans(&lock_spans, &in_flight) },
        );
        br_txn.status = Status::Committed;
        (Some(br_txn), Ok(resps))
    }

    /// TxnCoordSender: the result for the client, with the epoch restart on a retry error.
    fn finish(&mut self, btxn: Option<Txn>, res: Result<Vec<Response>, Error>, ok: Rep) -> Rep {
        match res {
            Ok(_) => {
                if let Some(t) = &btxn {
                    self.txn.update(t);
                }
                ok
            }
            Err(e) => match e.kind {
                ErrKind::TransactionRetry(_) => {
                    // Epoch restart: in-flight writes move to the lock footprint.
                    let mut ts = self.txn.meta.write_ts;
                    if let Some(t) = &e.txn {
                        ts = ts.max(t.meta.write_ts);
                    }
                    self.txn.meta.epoch += 1;
                    self.txn.meta.write_ts = ts;
                    self.txn.read_ts = ts;
                    self.txn.status = Status::Pending;
                    for w in self.ifw.drain(..) {
                        self.footprint.push(w.key);
                    }
                    self.footprint.sort();
                    self.footprint.dedup();
                    Rep::Retry
                }
                ErrKind::TransactionAborted(_) => Rep::Aborted,
                k => panic!("unexpected error at T's coordinator (unreachable here): {:?}", k),
            },
        }
    }
}

fn tc_proc(ordered: bool) {
    let s = recv_setup();
    let mut tc = Tc {
        ds: Ds::new(s.rs.clone(), ordered),
        tcx: s.tcx,
        txn: Txn { meta: s.meta, read_ts: s.meta.write_ts, status: Status::Pending },
        seq: 0,
        ifw: vec![],
        footprint: vec![],
    };
    let c = s.c;
    loop {
        let req: Req = recv_tagged_msg_block(move |tid, t| t.is_none() && tid == c);
        let rep = match req {
            Req::Put(k) => {
                tc.seq += 1;
                let (btxn, res) = tc.pipeliner_send(vec![Request::Put { key: k, seq: tc.seq }]);
                if let Err(e) = &res {
                    panic!("a pipelined Put never fails here: {:?}", e);
                }
                tc.finish(btxn, res, Rep::Written(k))
            }
            Req::Commit(keys) => {
                let mut reqs = Vec::new();
                for k in keys {
                    tc.seq += 1;
                    reqs.push(Request::Put { key: k, seq: tc.seq });
                }
                tc.seq += 1;
                reqs.push(Request::EndTxn { commit: true, lock_spans: vec![], in_flight: vec![] });
                let (btxn, res) = tc.pipeliner_send(reqs);
                tc.finish(btxn, res, Rep::Committed)
            }
            Req::Recover => panic!("Recover sent to tc"),
        };
        send_msg(c, rep);
    }
}

/// tcx: makeTxnCommitExplicitLocked l.529-594 (through the span refresher and DistSender).
fn tcx_proc(ordered: bool) {
    let s = recv_setup();
    let mut ds = Ds::new(s.rs.clone(), ordered);
    loop {
        let Msg::Explicit { txn, lock_spans } = recv_tag(T_EXPLICIT) else { panic!("Explicit expected") };
        let mut t = txn;
        if t.meta.write_ts > t.read_ts {
            t.read_ts = t.meta.write_ts;
        }
        let (_, res) = ds.send_txn(t, vec![Request::EndTxn { commit: true, lock_spans, in_flight: vec![] }]);
        match res {
            Ok(_) => {}
            // Someone else beat us to explicitly committing the record (l.565-571).
            Err(Error { kind: ErrKind::StatusCommitted, .. }) => {}
            Err(e) => panic!("making txn commit explicit failed: {:?}", e),
        }
    }
}

// ---------------------------------------------------------------------------
// h1..hm: the push path (PushTxn, handleIndeterminateCommitError, txnrecovery, retried push,
// resolution of T's intents).
// ---------------------------------------------------------------------------

/// txnrecovery/manager.go resolveIndeterminateCommitForTxn l.120-151 (probe and recover).
fn resolve_indeterminate_commit(ds: &mut Ds, txn: &Record) -> Record {
    assert!(txn.status == Status::Staging, "IndeterminateCommitError with non-STAGING transaction");
    // batchTimestamp l.306-310: the recovered transaction's write timestamp.
    let ts = txn.meta.write_ts;
    // Probe l.158-267: QueryTxn plus one QueryIntent per in-flight write, sorted by key.
    let mut writes = txn.in_flight.clone();
    writes.sort_by_key(|w| w.key);
    let mut reqs = vec![Request::QueryTxn { txn: txn.meta }];
    for w in &writes {
        let mut meta = txn.meta;
        meta.seq = w.seq;
        reqs.push(Request::QueryIntent { key: w.key, txn: meta, error_if_missing: false });
    }
    let mut prevented = false;
    if !writes.is_empty() {
        let (_, res) = ds.send(None, ts, reqs);
        let resps = res.unwrap_or_else(|e| panic!("recovery probe failed: {:?}", e));
        let Response::QueryTxn { txn: q, .. } = &resps[0] else { panic!("QueryTxn response expected") };
        if q.status.finalized() || txn.meta.epoch < q.meta.epoch || txn.meta.write_ts < q.meta.write_ts {
            return q.clone();
        }
        for r in &resps[1..] {
            if let Response::QueryIntent { found_unpushed: false, .. } = r {
                prevented = true;
                break;
            }
        }
    }
    // Recover l.279-299.
    let (_, res) = ds.send(None, ts, vec![Request::RecoverTxn { txn: txn.meta, implicitly_committed: !prevented }]);
    let resps = res.unwrap_or_else(|e| panic!("RecoverTxn failed: {:?}", e));
    let Response::RecoverTxn { recovered } = &resps[0] else { panic!("RecoverTxn response expected") };
    recovered.clone()
}

fn handle_proc(i: usize, n: u8, ordered: bool) {
    let s = recv_setup();
    let mut ds = Ds::new(s.rs.clone(), ordered);
    let p = s.ps[i];
    let meta = s.meta;
    loop {
        let Req::Recover = recv_tagged_msg_block::<_, Req>(move |tid, t| t.is_none() && tid == p) else {
            panic!("Recover expected")
        };
        // PushTxn at r1; on an IndeterminateCommitError recover, then retry the push
        // (replica_send.go l.593-600, handleIndeterminateCommitError l.818-838).
        let pushee = loop {
            let (_, res) = ds.send(None, meta.write_ts, vec![Request::PushTxn { pushee: meta }]);
            match res {
                Ok(r) => {
                    let Response::PushTxn { pushee } = &r[0] else { panic!("PushTxn response expected") };
                    break pushee.clone();
                }
                Err(Error { kind: ErrKind::IndeterminateCommit(staging), .. }) => {
                    let _ = resolve_indeterminate_commit(&mut ds, &staging);
                }
                Err(e) => panic!("push failed: {:?}", e),
            }
        };
        assert!(pushee.status.finalized(), "a winning push returns a finalized record");
        // Resolve T's intents on every key with the finalized status.
        let reqs: Vec<Request> =
            (1..=n).map(|k| Request::ResolveIntent { key: k, txn: pushee.meta, status: pushee.status }).collect();
        let (_, res) = ds.send(None, pushee.meta.write_ts, reqs);
        res.unwrap_or_else(|e| panic!("ResolveIntent failed: {:?}", e));
        let fate = if pushee.status == Status::Committed { Fate::Committed } else { Fate::Aborted };
        send_msg(p, Rep::Recovered(fate));
    }
}

// ---------------------------------------------------------------------------
// Program entry.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct Params {
    n: u8,
    p: u8,
    m: usize,
    a: u32,
    ordered: bool,
}

fn params(args: &[String]) -> Params {
    let num = |i: usize, d: u64| args.get(i).and_then(|s| s.parse::<u64>().ok()).unwrap_or(d);
    let n = num(0, 2) as u8;
    let p = num(1, 1) as u8;
    let m = num(2, 1) as usize;
    let a = num(3, 1) as u32;
    assert!((1..=2).contains(&n) && p <= n && !(n == 1 && p == 0), "n in {{1,2}}, 0 <= p <= n, (n,p) != (1,0)");
    assert!((1..=2).contains(&m) && a >= 1, "m in {{1,2}}, a >= 1");
    let ordered = args.get(4).map(String::as_str) != Some("bag");
    Params { n, p, m, a, ordered }
}

fn imp(pa: Params) {
    let Params { n, p, m, a, ordered } = pa;
    let mut rs = Vec::new();
    for r in 0..n as usize {
        let h = thread::Builder::new().name(format!("r{}", r + 1)).spawn_daemon(move || range_proc(r)).unwrap();
        rs.push(h.thread().id());
    }
    let tcx = thread::Builder::new().name("tcx".into()).spawn_daemon(move || tcx_proc(ordered)).unwrap();
    let tcx = tcx.thread().id();
    let tc = thread::Builder::new().name("tc".into()).spawn_daemon(move || tc_proc(ordered)).unwrap();
    let tc = tc.thread().id();
    let mut hs = Vec::new();
    for i in 0..m {
        let h = thread::Builder::new()
            .name(format!("h{}", i + 1))
            .spawn_daemon(move || handle_proc(i, n, ordered))
            .unwrap();
        hs.push(h.thread().id());
    }
    let c = thread::Builder::new().name("c".into()).spawn(move || committer(n, p, a, tc)).unwrap();
    let cid = c.thread().id();
    let mut ps = Vec::new();
    let mut pids = Vec::new();
    for i in 0..m {
        let h = hs[i];
        let jh = thread::Builder::new().name(format!("p{}", i + 1)).spawn(move || preventer(h)).unwrap();
        pids.push(jh.thread().id());
        ps.push(jh);
    }
    let setup = Setup {
        c: cid,
        ps: pids,
        tc,
        tcx,
        hs: hs.clone(),
        rs: rs.clone(),
        meta: Meta { epoch: 0, write_ts: 1, min_ts: 1, seq: 0 },
    };
    let init = thread::Builder::new()
        .name("init".into())
        .spawn(move || {
            let mut targets = setup.rs.clone();
            targets.push(setup.tcx);
            targets.push(setup.tc);
            targets.extend(setup.hs.iter().cloned());
            for t in targets {
                send_tagged_msg(t, T_SETUP, Msg::Setup(setup.clone()));
            }
        })
        .unwrap();
    c.join().unwrap();
    for jh in ps {
        jh.join().unwrap();
    }
    init.join().unwrap();
}

fn main() -> ExitCode {
    let visible = |args: &[String]| -> Vec<String> {
        let pa = params(args);
        let mut v = vec!["c".to_string()];
        v.extend((1..=pa.m).map(|i| format!("p{i}")));
        v
    };
    let entry = |args: &[String]| -> Entry {
        let pa = params(args);
        Arc::new(move || imp(pa))
    };
    program::main(Program { visible: &visible, spec: &entry, imp: &entry })
}
