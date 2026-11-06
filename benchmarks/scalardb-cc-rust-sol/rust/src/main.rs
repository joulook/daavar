use std::sync::Arc;
use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

#[derive(Clone, Debug, PartialEq)] enum Req { Begin, Read(u8), Write(u8, u8), Commit }
#[allow(dead_code)] // Unknown is required by the interface but unreachable with these stores.
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

// Six after-image columns followed by the corresponding six before-image columns.
const SETUP_TAG: u32 = u32::MAX;
const NEVER_TAG: u32 = u32::MAX - 1;
const RECOVERY_TAG: u32 = 1 << 30;
const VALUE: usize = 0;
const ID: usize = 1;
const STATE: usize = 2;
const VERSION: usize = 3;
const PREPARED_AT: usize = 4;
const COMMITTED_AT: usize = 5;
const PREPARED: u64 = 1;
const COMMITTED: u64 = 2;
const DELETED: u64 = 3;
#[derive(Clone, Debug, PartialEq)]
struct Record([Option<u64>; 12]);
impl Record {
    fn initial() -> Self {
        let mut r = Self([None; 12]);
        r.0[..6].copy_from_slice(&[Some(0), Some(0), Some(COMMITTED), Some(1), Some(0), Some(0)]);
        r
    }
    fn committed(&self) -> bool { self.0[STATE] == Some(COMMITTED) }
    fn before(&self) -> Option<Self> {
        if self.0[6 + ID].is_none() && self.0[6 + VERSION].is_none() { return None; }
        let mut r = Self([None; 12]);
        r.0[..6].copy_from_slice(&self.0[6..]);
        Some(r)
    }
    fn after(&self) -> Option<Self> {
        if self.0[STATE] == Some(DELETED) { return None; }
        let mut r = self.clone();
        r.0[STATE] = Some(COMMITTED);
        r.0[COMMITTED_AT] = Some(0);
        r.0[6..].fill(None);
        Some(r)
    }
}
#[derive(Clone, Debug, PartialEq)]
#[allow(dead_code)]
enum Condition { Always, Absent, All(Vec<(usize, Option<u64>)>) }
impl Condition {
    fn matches(&self, r: &Option<Record>) -> bool {
        match self {
            Self::Always => true,
            Self::Absent => r.is_none(),
            Self::All(cs) => r.as_ref().is_some_and(|r| cs.iter().all(|&(c, v)| r.0[c] == v)),
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
struct Mutation { condition: Condition, put: Option<Vec<(usize, Option<u64>)>> }
fn prepare_mutation(tx: u8, value: u8, old: Option<&Record>) -> Mutation {
    let mut columns = vec![(VALUE, Some(value.into())), (ID, Some(tx.into())),
        (STATE, Some(PREPARED)), (PREPARED_AT, Some(0)),
        (VERSION, Some(old.and_then(|r| r.0[VERSION]).unwrap_or(0) + 1))];
    let condition = if let Some(r) = old {
        columns.extend((0..6).map(|c| (c + 6, r.0[c])));
        Condition::All(vec![(ID, r.0[ID])])
    } else { Condition::Absent };
    Mutation { condition, put: Some(columns) }
}
fn commit_mutation(tx: u64, state: u64) -> Mutation {
    let put = if state == DELETED { None } else {
        let mut cs = vec![(STATE, Some(COMMITTED)), (COMMITTED_AT, Some(0))];
        cs.extend((6..12).map(|c| (c, None)));
        Some(cs)
    };
    Mutation { condition: Condition::All(vec![(ID, Some(tx)), (STATE, Some(state))]), put }
}
fn rollback_mutation(r: &Record) -> Mutation {
    Mutation {
        condition: Condition::All(vec![(ID, r.0[ID]), (STATE, r.0[STATE])]),
        put: r.before().map(|old| old.0.into_iter().enumerate().collect()),
    }
}
#[derive(Clone, Debug, PartialEq)]
enum Decision { Committed, Aborted }
#[derive(Clone, Debug, PartialEq)]
struct Setup {
    stores: Vec<ThreadId>, coord: ThreadId, recoveries: Vec<Vec<ThreadId>>,
    handles: Vec<ThreadId>, clients: Vec<ThreadId>, ids: Vec<u8>, readonly: Vec<bool>, serializable: bool,
}
#[derive(Clone, Debug, PartialEq)]
enum StorageRequest {
    Get { reply: ThreadId, id: u32, key: u8 },
    Mutate { reply: ThreadId, id: u32, key: u8, mutation: Mutation },
}
#[derive(Clone, Debug, PartialEq)] struct GetReply { id: u32, key: u8, record: Option<Record> }
#[derive(Clone, Debug, PartialEq)] struct MutateReply { id: u32, key: u8, applied: bool }
#[derive(Clone, Debug, PartialEq)]
enum CoordinatorRequest {
    Get { reply: ThreadId, id: u32, tx: u8 },
    Put { reply: ThreadId, id: u32, tx: u8, state: Decision, created_at: u64 },
}
#[derive(Clone, Debug, PartialEq)] struct StateReply { state: Option<(Decision, u64)> }
#[derive(Clone, Debug, PartialEq)] struct StatePutReply { applied: bool }
#[derive(Clone, Debug, PartialEq)] struct RecoveryJob { record: Record, state: Option<Decision> }
#[derive(Clone, Debug, PartialEq)] struct RecoveryDone { key: u8, outcome: Result<(), RecoveryError> }
#[derive(Clone, Debug, PartialEq)] enum RecoveryError {}

fn storage() {
    let _setup = recv_tagged_msg_block::<_, Setup>(|_, tag| tag == Some(SETUP_TAG));
    let mut record = Some(Record::initial());
    loop {
        match recv_msg_block::<StorageRequest>() {
            StorageRequest::Get { reply, id, key } => send_tagged_msg(reply, id, GetReply { id, key, record: record.clone() }),
            StorageRequest::Mutate { reply, id, key, mutation } => {
                let applied = mutation.condition.matches(&record);
                if applied {
                    if let Some(cs) = mutation.put {
                        let r = record.get_or_insert(Record([None; 12]));
                        for (c, v) in cs { r.0[c] = v; }
                    } else { record = None; }
                }
                send_tagged_msg(reply, id, MutateReply { id, key, applied });
            }
        }
    }
}
fn coordinator() {
    let _setup = recv_tagged_msg_block::<_, Setup>(|_, tag| tag == Some(SETUP_TAG));
    let mut table: Vec<Option<(Decision, u64)>> = vec![None; 4];
    loop {
        match recv_msg_block::<CoordinatorRequest>() {
            CoordinatorRequest::Get { reply, id, tx } => send_tagged_msg(reply, id, StateReply { state: table[tx as usize].clone() }),
            CoordinatorRequest::Put { reply, id, tx, state, created_at } => {
                let slot = &mut table[tx as usize];
                let applied = slot.is_none();
                if applied { *slot = Some((state, created_at)); }
                send_tagged_msg(reply, id, StatePutReply { applied });
            }
        }
    }
}
struct Io { setup: Setup, me: ThreadId, next: u32 }
impl Io {
    fn id(&mut self) -> u32 { self.next += 1; self.next }
    fn send_get(&mut self, key: u8) -> u32 {
        let id = self.id();
        send_msg(self.setup.stores[key as usize - 1], StorageRequest::Get { reply: self.me, id, key });
        id
    }
    fn get(&mut self, key: u8) -> Option<Record> {
        let id = self.send_get(key);
        recv_tagged_msg_block::<_, GetReply>(move |_, tag| tag == Some(id)).record
    }
    fn send_mutation(&mut self, key: u8, mutation: Mutation) -> u32 {
        let id = self.id();
        send_msg(self.setup.stores[key as usize - 1], StorageRequest::Mutate { reply: self.me, id, key, mutation });
        id
    }
    fn mutate(&mut self, key: u8, mutation: Mutation) -> bool {
        let id = self.send_mutation(key, mutation);
        recv_tagged_msg_block::<_, MutateReply>(move |_, tag| tag == Some(id)).applied
    }
    fn get_state(&mut self, tx: u8) -> Option<Decision> {
        let id = self.id();
        send_msg(self.setup.coord, CoordinatorRequest::Get { reply: self.me, id, tx });
        recv_tagged_msg_block::<_, StateReply>(move |_, tag| tag == Some(id)).state.map(|s| s.0)
    }
    fn put_state(&mut self, tx: u8, state: Decision) -> bool {
        let id = self.id();
        send_msg(self.setup.coord, CoordinatorRequest::Put { reply: self.me, id, tx, state, created_at: 0 });
        recv_tagged_msg_block::<_, StatePutReply>(move |_, tag| tag == Some(id)).applied
    }
}
// These receives select only outstanding requests of this phase. Abandoned replies stay queued.
fn collect_mutations(mut pending: Vec<u32>) -> bool {
    let mut success = true;
    while !pending.is_empty() {
        let accepted = pending.clone();
        let r = recv_tagged_msg_block::<_, MutateReply>(move |_, t| t.is_some_and(|id| accepted.contains(&id)));
        pending.retain(|&id| id != r.id);
        success &= r.applied;
    }
    success
}
fn collect_get(mut pending: Vec<u32>) -> Vec<GetReply> {
    let mut results = Vec::new();
    while !pending.is_empty() {
        let accepted = pending.clone();
        let r = recv_tagged_msg_block::<_, GetReply>(move |_, t| t.is_some_and(|id| accepted.contains(&id)));
        pending.retain(|&id| id != r.id);
        results.push(r);
    }
    results
}
fn is_transaction_expired() -> bool { nondet() }
fn abort_if_expired(io: &mut Io, key: u8, record: &Record) {
    if !is_transaction_expired() { return; }
    let fresh = io.get(key);
    if fresh.as_ref().is_none_or(|r| r.committed() || r.0[ID] != record.0[ID]) { return; }
    if io.put_state(record.0[ID].unwrap() as u8, Decision::Aborted) {
        io.mutate(key, rollback_mutation(record));
    }
    // tryRecover's best-effort conflict path intentionally does not re-read state.
}
fn try_recover(io: &mut Io, key: u8, job: RecoveryJob) {
    match job.state {
        Some(Decision::Committed) => { io.mutate(key, commit_mutation(job.record.0[ID].unwrap(), job.record.0[STATE].unwrap())); }
        Some(Decision::Aborted) => { io.mutate(key, rollback_mutation(&job.record)); }
        None => abort_if_expired(io, key, &job.record),
    }
}
fn recovery(index: usize, key: u8) {
    let setup = recv_tagged_msg_block::<_, Setup>(|_, tag| tag == Some(SETUP_TAG));
    let me = setup.recoveries[index][key as usize - 1];
    let h = setup.handles[index];
    let mut io = Io { setup, me, next: 0 };
    let job = recv_msg_block::<RecoveryJob>();
    try_recover(&mut io, key, job);
    send_tagged_msg(h, RECOVERY_TAG + u32::from(key), RecoveryDone { key, outcome: Ok(()) });
    // Each fixed recovery process runs at most one job and then remains alive.
    loop { let _: Never = recv_tagged_msg_block(|_, tag| tag == Some(NEVER_TAG)); }
}
#[derive(Clone, Debug, PartialEq)] struct Never;
#[derive(Clone, Debug, PartialEq)] enum ReadError { Conflict }
#[derive(Default)]
struct Snapshot {
    reads: Vec<Option<Record>>, gets: Vec<bool>, writes: Vec<Option<u8>>, jobs: Vec<bool>,
}
impl Snapshot {
    fn begin(keys: usize) -> Self {
        Self { reads: vec![None; keys], gets: vec![false; keys], writes: vec![None; keys], jobs: vec![false; keys] }
    }
    fn merge_result(&self, key: u8) -> u8 {
        let k = key as usize - 1;
        self.writes[k].unwrap_or_else(|| self.reads[k].as_ref().expect("initial records cannot disappear in this instance").0[VALUE].unwrap() as u8)
    }
}
fn submit_recovery(io: &Io, index: usize, key: u8, record: Record, state: Decision, snapshot: &mut Snapshot) {
    send_msg(io.setup.recoveries[index][key as usize - 1], RecoveryJob { record, state: Some(state) });
    snapshot.jobs[key as usize - 1] = true;
}
fn resolve_latest_result_and_recover(io: &mut Io, index: usize, key: u8, mut current: Record, snapshot: &mut Snapshot) -> Result<Option<Record>, ReadError> {
    for attempt in 0..5 {
        let tx = current.0[ID].unwrap() as u8;
        if let Some(state) = io.get_state(tx) {
            let result = match state { Decision::Committed => current.after(), Decision::Aborted => current.before() };
            submit_recovery(io, index, key, current, state, snapshot);
            return Ok(result);
        }
        if !is_transaction_expired() { return Err(ReadError::Conflict); }
        let Some(fresh) = io.get(key) else { return Ok(None); };
        if fresh.committed() { return Ok(Some(fresh)); }
        if fresh.0[ID] != current.0[ID] {
            if attempt == 4 { return Err(ReadError::Conflict); }
            current = fresh;
            continue;
        }
        if io.put_state(tx, Decision::Aborted) {
            let Some(after_abort) = io.get(key) else { return Ok(None); };
            if after_abort.committed() { return Ok(Some(after_abort)); }
            if after_abort.0[ID] != current.0[ID] {
                if attempt == 4 { return Err(ReadError::Conflict); }
                current = after_abort;
                continue;
            }
            let result = current.before();
            submit_recovery(io, index, key, current, Decision::Aborted, snapshot);
            return Ok(result);
        }
        if attempt == 4 { return Err(ReadError::Conflict); }
    }
    unreachable!()
}
fn get(io: &mut Io, index: usize, key: u8, snapshot: &mut Snapshot) -> Result<u8, ReadError> {
    let k = key as usize - 1;
    if !snapshot.gets[k] {
        let result = match io.get(key) {
            Some(r) if !r.committed() => resolve_latest_result_and_recover(io, index, key, r, snapshot)?,
            r => r,
        };
        snapshot.reads[k] = result;
        snapshot.gets[k] = true;
    }
    Ok(snapshot.merge_result(key))
}
fn wait_for_recovery_completion(io: &Io, snapshot: &mut Snapshot) -> Result<(), RecoveryError> {
    for k in 0..snapshot.jobs.len() {
        if snapshot.jobs[k] && (snapshot.writes[k].is_some() || io.setup.serializable) {
            let key = k as u8 + 1;
            let done = recv_tagged_msg_block::<_, RecoveryDone>(move |_, t| t == Some(RECOVERY_TAG + u32::from(key)));
            done.outcome?;
            snapshot.jobs[k] = false;
        }
    }
    Ok(())
}
fn prepare_records(io: &mut Io, tx: u8, snapshot: &Snapshot) -> bool {
    let mut pending = Vec::new();
    for (k, value) in snapshot.writes.iter().enumerate() {
        if let Some(value) = value {
            pending.push(io.send_mutation(k as u8 + 1, prepare_mutation(tx, *value, snapshot.reads[k].as_ref())));
        }
    }
    collect_mutations(pending)
}
fn validate_records(io: &mut Io, snapshot: &Snapshot) -> bool {
    if !io.setup.serializable { return true; }
    let mut pending = Vec::new();
    for k in 0..snapshot.gets.len() {
        if snapshot.gets[k] && snapshot.writes[k].is_none() { pending.push(io.send_get(k as u8 + 1)); }
    }
    while !pending.is_empty() {
        let accepted = pending.clone();
        let r = recv_tagged_msg_block::<_, GetReply>(move |_, t| t.is_some_and(|id| accepted.contains(&id)));
        pending.retain(|&id| id != r.id);
        // Snapshot.isChanged compares presence and transaction ID only, even for PREPARED records.
        if r.record.as_ref().map(|v| v.0[ID]) != snapshot.reads[r.key as usize - 1].as_ref().map(|v| v.0[ID]) { return false; }
    }
    true
}
fn commit_records(io: &mut Io, tx: u8, snapshot: &Snapshot) {
    let mut pending = Vec::new();
    for k in 0..snapshot.writes.len() {
        if snapshot.writes[k].is_some() { pending.push(io.send_mutation(k as u8 + 1, commit_mutation(tx.into(), PREPARED))); }
    }
    collect_mutations(pending);
}
fn rollback_records(io: &mut Io, tx: u8, snapshot: &Snapshot) {
    let mut pending = Vec::new();
    for k in 0..snapshot.writes.len() {
        if snapshot.writes[k].is_some() { pending.push(io.send_get(k as u8 + 1)); }
    }
    let latest = collect_get(pending);
    let mut pending = Vec::new();
    for r in latest {
        if let Some(record) = r.record {
            if record.0[ID] == Some(tx.into()) {
                pending.push(io.send_mutation(r.key, rollback_mutation(&record)));
            }
        }
    }
    collect_mutations(pending);
    // getKeysOfWritesNeverRead is empty: every client reads every key before writing.
}
fn abort_state(io: &mut Io, tx: u8) {
    if !io.put_state(tx, Decision::Aborted) { let _persisted = io.get_state(tx); }
}
fn commit_state(io: &mut Io, tx: u8) -> Result<(), ()> {
    if io.put_state(tx, Decision::Committed) { return Ok(()); }
    match io.get_state(tx) { Some(Decision::Committed) => Ok(()), _ => Err(()) }
}
fn commit(io: &mut Io, tx: u8, snapshot: &mut Snapshot) -> Rep {
    if wait_for_recovery_completion(io, snapshot).is_err() { return Rep::Aborted; }
    let has_writes = snapshot.writes.iter().any(Option::is_some);
    if has_writes && !prepare_records(io, tx, snapshot) {
        abort_state(io, tx);
        rollback_records(io, tx, snapshot);
        return Rep::Aborted;
    }
    if !validate_records(io, snapshot) {
        if has_writes { abort_state(io, tx); rollback_records(io, tx, snapshot); }
        return Rep::Aborted;
    }
    if has_writes {
        if commit_state(io, tx).is_err() { rollback_records(io, tx, snapshot); return Rep::Aborted; }
        commit_records(io, tx, snapshot);
    }
    Rep::Committed
}
fn handle(index: usize) {
    let setup = recv_tagged_msg_block::<_, Setup>(|_, tag| tag == Some(SETUP_TAG));
    let me = setup.handles[index];
    let client = setup.clients[index];
    let tx = setup.ids[index + 1];
    let readonly = setup.readonly[index];
    let keys = setup.stores.len();
    let mut io = Io { setup, me, next: 0 };
    let Req::Begin = recv_tagged_msg_block::<_, Req>(|_, tag| tag.is_none()) else { panic!("Begin expected") };
    let mut snapshot = Snapshot::begin(keys);
    send_msg(client, Rep::Begun);
    loop {
        match recv_tagged_msg_block::<_, Req>(|_, tag| tag.is_none()) {
            Req::Read(key) => match get(&mut io, index, key, &mut snapshot) {
                Ok(value) => send_msg(client, Rep::Value(key, value)),
                Err(_) => { send_msg(client, Rep::Aborted); break; }
            },
            Req::Write(key, value) => {
                assert!(!readonly);
                snapshot.writes[key as usize - 1] = Some(value);
                send_msg(client, Rep::Written(key));
            }
            Req::Commit => { let result = commit(&mut io, tx, &mut snapshot); send_msg(client, result); break; }
            Req::Begin => panic!("one transaction per handle"),
        }
    }
    // ConsensusCommit.rollback/releaseResources has no storage work in this cut.
    // Do not receive late storage replies or unawaited recovery completions.
    loop { let _: Never = recv_tagged_msg_block(|_, tag| tag == Some(NEVER_TAG)); }
}
fn spawn(name: String, f: impl FnOnce() + Send + 'static) -> thread::JoinHandle<()> {
    thread::Builder::new().name(name).spawn(f).unwrap()
}
fn run(pattern: &str, keys: u8, serializable: bool) {
    let n = if pattern == "mixedro" { 3 } else { 2 };
    let mut workers = Vec::new();
    let mut stores = Vec::new();
    for k in 1..=keys { let t = spawn(format!("s{k}"), storage); stores.push(t.thread().id()); workers.push(t); }
    let c = spawn("coord".into(), coordinator);
    let coord = c.thread().id(); workers.push(c);
    let mut recoveries = Vec::new();
    for i in 0..n {
        let mut row = Vec::new();
        for k in 1..=keys { let t = spawn(format!("x{}{k}", i + 1), move || recovery(i, k)); row.push(t.thread().id()); workers.push(t); }
        recoveries.push(row);
    }
    let mut handles = Vec::new();
    for i in 0..n { let t = spawn(format!("h{}", i + 1), move || handle(i)); handles.push(t.thread().id()); workers.push(t); }
    let mut clients = Vec::new();
    let mut client_threads = Vec::new();
    for (i, &h) in handles.iter().enumerate() {
        let writes = if i == 2 { vec![] } else { match pattern {
            "lost" => vec![1], "skew" => vec![i as u8 + 1], "both" => vec![1, 2],
            "mixed" | "mixedro" => if i == 0 { vec![1, 2] } else { vec![2] }, _ => unreachable!(),
        }};
        let t = spawn(format!("c{}", i + 1), move || client(i as u8 + 1, keys, &writes, h));
        clients.push(t.thread().id()); client_threads.push(t);
    }
    let setup = Setup { stores, coord, recoveries, handles, clients, ids: (0..=n as u8).collect(),
        readonly: (0..n).map(|i| i == 2).collect(), serializable };
    let destinations: Vec<_> = workers.iter().map(|t| t.thread().id()).collect();
    let init = spawn("init".into(), move || { for t in destinations { send_tagged_msg(t, SETUP_TAG, setup.clone()); } });
    init.join().unwrap();
    for t in client_threads { t.join().unwrap(); }
    for t in workers { t.join().unwrap(); }
}
fn entry(args: &[String]) -> Entry {
    assert!((3..=4).contains(&args.len()), "arguments: <pattern> <k> <snapshot|serializable> [fifo|bag]");
    let pattern = args[0].clone();
    assert!(["lost", "skew", "both", "mixed", "mixedro"].contains(&pattern.as_str()));
    let keys: u8 = args[1].parse().expect("k is 1 or 2");
    assert!((1..=2).contains(&keys) && (keys == 2 || pattern == "lost"));
    let serializable = match args[2].as_str() { "snapshot" => false, "serializable" => true, _ => panic!("unknown isolation") };
    if let Some(model) = args.get(3) { assert!(["fifo", "bag"].contains(&model.as_str())); }
    Arc::new(move || run(&pattern, keys, serializable))
}
fn main() -> std::process::ExitCode {
    let visible = |args: &[String]| (1..=if args.first().is_some_and(|a| a == "mixedro") { 3 } else { 2 }).map(|i| format!("c{i}")).collect();
    let program = Program { visible: &visible, spec: &entry, imp: &entry };
    // The explicit runner --model still overrides this positional default.
    if std::env::args().last().as_deref() == Some("bag") {
        traceforge::program::main_with_model(program, ConsType::Bag)
    } else {
        traceforge::program::main(program)
    }
}
