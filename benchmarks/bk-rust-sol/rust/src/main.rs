use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;
use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::{nondet, recv_msg_block, recv_tagged_msg_block, send_msg, send_tagged_msg, send_tagged_msg_as, recv_tagged_msg_block_as, send_vec_tagged_msg, recv_vec_tagged_msg_block, Visibility};

#[derive(Clone, Debug, PartialEq)] enum Req { Add(u64), Recover }
#[derive(Clone, Debug, PartialEq)] enum Rep { Acked(u64), Rejected(u64), Closed(i64) }

fn writer(e: u64, h: ThreadId) {
    for i in 1..=e { send_msg(h, Req::Add(i)); match recv_msg_block::<Rep>() { Rep::Acked(_) | Rep::Rejected(_) => {}, _ => panic!("Acked or Rejected expected") } }
}
fn recoverer(h: ThreadId) {
    send_msg(h, Req::Recover); let Rep::Closed(_) = recv_msg_block::<Rep>() else { panic!("Closed expected") };
}

const SETUP: u32 = 1;
const START_AFTER: u32 = 2;
const START: u32 = 3;
const ADD_RESPONSE: u32 = 4;
const LAC_RESPONSE: u32 = 5;
const READ_RESPONSE: u32 = 6;
const METADATA_RESPONSE: u32 = 7;
const METADATA_WRITTEN: u32 = 8;
#[derive(Clone, Debug, PartialEq)]
struct Addresses { b: [ThreadId; 3], zk: ThreadId, w1h: ThreadId, w2h: ThreadId, w1: ThreadId, w2: ThreadId }
fn setup() -> Addresses {
    recv_tagged_msg_block(|_, tag| tag == Some(SETUP))
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum State { Open, InRecovery, Closed }
#[derive(Clone, Debug, PartialEq)]
struct Metadata { version: u64, state: State, last: i64, ensemble: [ThreadId; 3] }
#[derive(Clone, Copy, Debug, PartialEq)]
enum Status { Ok, Fenced }
#[derive(Clone, Debug, PartialEq)]
enum Wire {
    Write { entry: u64, lac: i64, recovery: bool },
    ReadLac { fence: bool },
    Read { entry: u64, fence: bool },
    GetMetadata,
    PutMetadata { expected: u64, state: State, last: i64 },
}

// Reply types select the active protocol phase. Entry tags exclude replies
// from completed operations without receiving or discarding them.
#[derive(Clone, Debug, PartialEq)]
struct AddResponse { bookie: usize, status: Status }
#[derive(Clone, Debug, PartialEq)]
struct ReadLacResponse { bookie: usize, lac: i64 }
#[derive(Clone, Debug, PartialEq)]
struct ReadResponse { found: bool }
#[derive(Clone, Debug, PartialEq)]
struct MetadataResponse(Metadata);
#[derive(Clone, Debug, PartialEq)]
struct MetadataWritten { applied: bool, metadata: Metadata }
#[derive(Clone, Debug, PartialEq)]
struct StartAfter { completed: u64 }
#[derive(Clone, Debug, PartialEq)]
struct Start;
fn entry_tag(entry: u64) -> Vec<u32> { vec![ADD_RESPONSE, entry as u32, (entry >> 32) as u32] }
fn add_response(entry: u64) -> AddResponse {
    recv_vec_tagged_msg_block(move |_, tag| tag == Some(entry_tag(entry)))
}

#[derive(Default)]
struct AckSet(u8);
impl AckSet {
    fn complete_bookie_and_check(&mut self, bookie: usize) -> bool {
        self.0 |= 1 << bookie;
        self.0.count_ones() >= 2
    }
}

struct Storage { fenced: bool, entries: BTreeSet<u64>, lac: i64 }
impl Storage {
    fn fence_ledger(&mut self) { self.fenced = true; }
    fn add_entry(&mut self, entry: u64, lac: i64) -> Status {
        if self.fenced { Status::Fenced } else { self.recovery_add_entry(entry, lac) }
    }
    fn recovery_add_entry(&mut self, entry: u64, lac: i64) -> Status {
        self.entries.insert(entry);
        self.lac = self.lac.max(lac);
        Status::Ok
    }
    fn read_entry(&self, entry: u64) -> bool { self.entries.contains(&entry) }
    fn read_last_add_confirmed(&self) -> i64 { self.lac }
}
fn bookie(index: usize) {
    let a = setup();
    let mut storage = Storage { fenced: false, entries: BTreeSet::new(), lac: -1 };
    loop {
        match recv_msg_block::<Wire>() {
            Wire::Write { entry, lac, recovery } => {
                let status = if recovery { storage.recovery_add_entry(entry, lac) } else { storage.add_entry(entry, lac) };
                send_vec_tagged_msg(if recovery { a.w2h } else { a.w1h }, entry_tag(entry), AddResponse { bookie: index, status });
            }
            Wire::ReadLac { fence } => {
                if fence { storage.fence_ledger(); }
                send_tagged_msg(a.w2h, LAC_RESPONSE, ReadLacResponse { bookie: index, lac: storage.read_last_add_confirmed() });
            }
            Wire::Read { entry, fence } => {
                if fence { storage.fence_ledger(); }
                send_tagged_msg(a.w2h, READ_RESPONSE, ReadResponse { found: storage.read_entry(entry) });
            }
            _ => panic!("bookie request expected"),
        }
    }
}
fn metadata_store() {
    let a = setup();
    let mut metadata = Metadata { version: 0, state: State::Open, last: -1, ensemble: a.b };
    loop {
        match recv_msg_block::<Wire>() {
            Wire::GetMetadata => send_tagged_msg(a.w2h, METADATA_RESPONSE, MetadataResponse(metadata.clone())),
            Wire::PutMetadata { expected, state, last } => {
                let applied = expected == metadata.version;
                if applied {
                    metadata.version += 1;
                    metadata.state = state;
                    metadata.last = last;
                }
                send_tagged_msg(a.w2h, METADATA_WRITTEN, MetadataWritten { applied, metadata: metadata.clone() });
            }
            _ => panic!("metadata request expected"),
        }
    }
}

fn send_writes(a: &Addresses, entry: u64, lac: i64, recovery: bool) {
    // RoundRobinDistributionSchedule.getWriteSetBookieIndex.
    for offset in 0..3 {
        let index = ((entry % 3) as usize + offset) % 3;
        send_msg(a.b[index], Wire::Write { entry, lac, recovery });
    }
}
struct PendingAdd { entry: u64, acks: AckSet }
fn error_out_pending_adds(a: &Addresses, pending: &mut VecDeque<PendingAdd>) {
    for op in pending.drain(..) { send_msg(a.w1, Rep::Rejected(op.entry)); }
}
fn send_add_success_callbacks(a: &Addresses, pending: &mut VecDeque<PendingAdd>, lac: &mut i64) {
    while pending.front().is_some_and(|op| op.acks.0.count_ones() >= 2) {
        let op = pending.pop_front().unwrap();
        assert!(op.entry == 1 && *lac == -1 || op.entry as i64 == *lac + 1);
        *lac = op.entry as i64;
        send_msg(a.w1, Rep::Acked(op.entry));
    }
}
fn writer_handle() {
    let a = setup();
    // Receive the invisible timing choice once, before serving the client.
    // Prefix zero permits recovery before any writer request is processed.
    let StartAfter { completed: prefix } = recv_tagged_msg_block::<_, StartAfter>(|_, tag| tag == Some(START_AFTER));
    if prefix == 0 { send_tagged_msg_as(a.w2, Visibility::Invisible, START, Start); }
    let mut closed = false;
    let mut lac = -1;
    let mut completed = 0;
    let mut pending = VecDeque::<PendingAdd>::new();
    loop {
        let Req::Add(entry) = recv_tagged_msg_block::<_, Req>(|_, tag| tag.is_none()) else { panic!("Add expected") };
        if closed { send_msg(a.w1, Rep::Rejected(entry)); }
        else {
            pending.push_back(PendingAdd { entry, acks: AckSet::default() });
            send_writes(&a, entry, lac, false);
            loop {
                let AddResponse { bookie, status } = add_response(entry);
                if status == Status::Fenced {
                    closed = true;
                    error_out_pending_adds(&a, &mut pending);
                    break;
                }
                if pending.front_mut().unwrap().acks.complete_bookie_and_check(bookie) {
                    send_add_success_callbacks(&a, &mut pending, &mut lac);
                    break;
                }
            }
        }
        completed += 1;
        if prefix != 0 && completed == prefix {
            send_tagged_msg_as(a.w2, Visibility::Invisible, START, Start);
        }
    }
}

fn read_metadata(a: &Addresses) -> Metadata {
    send_msg(a.zk, Wire::GetMetadata);
    recv_tagged_msg_block::<_, MetadataResponse>(|_, tag| tag == Some(METADATA_RESPONSE)).0
}
fn update_metadata(a: &Addresses, state: State, last: i64, mut m: Metadata) -> Metadata {
    loop {
        send_msg(a.zk, Wire::PutMetadata { expected: m.version, state, last });
        let MetadataWritten { applied, metadata } = recv_tagged_msg_block::<_, MetadataWritten>(|_, tag| tag == Some(METADATA_WRITTEN));
        if applied { return metadata; }
        m = read_metadata(a);
    }
}
fn read_last_confirmed_with_fencing(a: &Addresses) -> i64 {
    for b in a.b { send_msg(b, Wire::ReadLac { fence: true }); }
    let mut responses = AckSet::default();
    let mut maximum = -1;
    loop {
        let ReadLacResponse { bookie, lac } = recv_tagged_msg_block::<_, ReadLacResponse>(|_, tag| tag == Some(LAC_RESPONSE));
        maximum = maximum.max(lac);
        if responses.complete_bookie_and_check(bookie) { return maximum; }
    }
}
fn recovery_read(a: &Addresses, entry: u64) -> bool {
    // SequenceReadRequest: one outstanding request, first valid entry wins;
    // two NoSuchEntry answers terminate recovery without querying the third.
    for offset in 0..2 {
        let index = ((entry % 3) as usize + offset) % 3;
        send_msg(a.b[index], Wire::Read { entry, fence: true });
        if recv_tagged_msg_block::<_, ReadResponse>(|_, tag| tag == Some(READ_RESPONSE)).found { return true; }
    }
    false
}
fn recovery_add(a: &Addresses, entry: u64, lac: i64) {
    send_writes(a, entry, lac, true);
    let mut acks = AckSet::default();
    loop {
        let AddResponse { bookie, status } = add_response(entry);
        assert_eq!(status, Status::Ok);
        if acks.complete_bookie_and_check(bookie) { return; }
    }
}
fn recovery_handle() {
    let a = setup();
    let Req::Recover = recv_msg_block::<Req>() else { panic!("Recover expected"); };
    let metadata = read_metadata(&a);
    let metadata = update_metadata(&a, State::InRecovery, -1, metadata);
    let mut last = read_last_confirmed_with_fencing(&a);
    loop {
        // Entry ids start at 1 in this interface; 0 is not a ledger entry.
        let entry = if last == -1 { 1 } else { last as u64 + 1 };
        if !recovery_read(&a, entry) { break; }
        recovery_add(&a, entry, last);
        last = entry as i64;
    }
    update_metadata(&a, State::Closed, last, metadata);
    send_msg(a.w2, Rep::Closed(last));
    loop { let _: Req = recv_tagged_msg_block(|_, tag| tag.is_none()); }
}

fn implementation(e: u64) {
    let b1 = thread::Builder::new().name("b1".into()).spawn(|| bookie(0)).unwrap();
    let b2 = thread::Builder::new().name("b2".into()).spawn(|| bookie(1)).unwrap();
    let b3 = thread::Builder::new().name("b3".into()).spawn(|| bookie(2)).unwrap();
    let zk = thread::Builder::new().name("zk".into()).spawn(metadata_store).unwrap();
    let w1h = thread::Builder::new().name("w1h".into()).spawn(writer_handle).unwrap();
    let w2h = thread::Builder::new().name("w2h".into()).spawn(recovery_handle).unwrap();
    let h1 = w1h.thread().id();
    let h2 = w2h.thread().id();
    let w1 = thread::Builder::new().name("w1".into()).spawn(move || writer(e, h1)).unwrap();
    let w2 = thread::Builder::new().name("w2".into()).spawn(move || {
        // Choose an acknowledged prefix before starting recovery. Zero also
        // permits every concurrent arrival order, including recovery first.
        let mut prefix = 0;
        while prefix < e && nondet() { prefix += 1; }
        send_tagged_msg_as(h1, Visibility::Invisible, START_AFTER, StartAfter { completed: prefix });
        let Start = recv_tagged_msg_block_as::<_, Start>(Visibility::Invisible, |_, tag| tag == Some(START));
        recoverer(h2);
    }).unwrap();
    let a = Addresses { b: [b1.thread().id(), b2.thread().id(), b3.thread().id()], zk: zk.thread().id(), w1h: h1, w2h: h2, w1: w1.thread().id(), w2: w2.thread().id() };
    let init = thread::Builder::new().name("init".into()).spawn(move || {
        for destination in [a.b[0], a.b[1], a.b[2], a.zk, a.w1h, a.w2h] {
            send_tagged_msg(destination, SETUP, a.clone());
        }
    }).unwrap();
    init.join().unwrap();
    w1.join().unwrap();
    w2.join().unwrap();
    // Persistent services finish each execution blocked on their inboxes.
    b1.join().unwrap();
    b2.join().unwrap();
    b3.join().unwrap();
    zk.join().unwrap();
    w1h.join().unwrap();
    w2h.join().unwrap();
}
fn entry(args: &[String]) -> Entry {
    assert!((1..=2).contains(&args.len()), "expected <e> [fifo|bag]");
    let e: u64 = args[0].parse().expect("e must be an integer");
    assert!(e >= 1 && e < i64::MAX as u64, "e must be positive and fit entry ids");
    if let Some(model) = args.get(1) { assert!(model == "fifo" || model == "bag", "expected fifo|bag"); }
    Arc::new(move || implementation(e))
}
fn main() -> std::process::ExitCode {
    traceforge::program::main(Program { visible: &|_| vec!["w1".into(), "w2".into()], spec: &entry, imp: &entry })
}
