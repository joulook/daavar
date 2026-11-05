//! S_bk: the client-visible content of BookKeeper's ledger replication with fencing (the TLA+ model's invariants, as the
//! writer w1 and the recovery client w2 see them), written by the coordinator (2026-11-05) as the hand-written
//! specification of the BookKeeper study; the interface is plan/case-studies/bookkeeper/INTERFACE.md. One invisible
//! object `ledger`: w1's entries are acknowledged in entry order (a prefix 1..k); an entry may stay in flight; the recovery
//! closes the ledger at some `last` >= k among the entries added so far, then no entry above `last` is ever acknowledged
//! and an entry below or at `last` that was in flight may still be acknowledged or rejected; every Add after a rejection or
//! after the close is rejected. Entries: `spec` = this object; `imp` = the same. ARGS `<e> [fifo|bag]`.
use std::collections::VecDeque;
use std::sync::Arc;
use traceforge::thread::{self, ThreadId};
use traceforge::{nondet, recv_msg_block, recv_tagged_msg_block, send_msg, send_tagged_msg};

#[derive(Clone, Debug, PartialEq)]
enum Req { Add(u64), Recover }
#[derive(Clone, Debug, PartialEq)]
enum Rep { Acked(u64), Rejected(u64), Closed(i64) }
#[derive(Clone, Debug, PartialEq)]
struct Setup { w1: ThreadId, w2: ThreadId }
const SETUP_TAG: u32 = 1;

fn writer(e: u64, h: ThreadId) {
    for i in 1..=e { send_msg(h, Req::Add(i)); match recv_msg_block::<Rep>() { Rep::Acked(_) | Rep::Rejected(_) => {}, _ => panic!("Acked or Rejected expected") } }
}
fn recoverer(h: ThreadId) {
    send_msg(h, Req::Recover); let Rep::Closed(_) = recv_msg_block::<Rep>() else { panic!("Closed expected") };
}

/// The ledger object: `e` entries of w1 and one Recover of w2. State: `acked` (the acknowledged prefix 1..acked), `added`
/// (the highest entry w1 has added), `pending` (added, not yet answered, in entry order), `closed` (the last entry id once
/// the recovery decided it; the reply to w2 may be sent later, so that w1 may see a rejection before w2 sees the close),
/// `rejecting` (w1's handle is closed by a fenced answer: every later Add is rejected). At every step the enabled actions
/// are: acknowledge the front entry (in order; only if it is part of the closed ledger once closed; never once rejecting),
/// reject the front entry (once the recovery started: a fenced answer may reach the handle), send the Closed reply, or
/// receive the next request (only when one can still arrive: w1 sends only after its answer, w2 only once).
fn ledger(e: u64) {
    let Setup { w1, w2 } = recv_tagged_msg_block::<_, Setup>(|_, tag| tag == Some(SETUP_TAG));
    let (mut acked, mut added) = (0u64, 0u64);
    let mut pending: VecDeque<u64> = VecDeque::new();
    let mut closed: Option<u64> = None;
    let mut closed_sent = false;
    let mut rejecting = false;
    let mut recover_served = false;
    loop {
        let front = pending.front().copied();
        let may_ack = front.map_or(false, |i| !rejecting && i == acked + 1 && closed.map_or(true, |l| i <= l));
        let may_reject = front.is_some() && (closed.is_some() || recover_served);
        let may_close_reply = closed.is_some() && !closed_sent;
        let may_recv = (pending.is_empty() && added < e) || !recover_served;
        let mut acts: Vec<u8> = Vec::new();
        if may_ack { acts.push(1); }
        if may_reject { acts.push(2); }
        if may_close_reply { acts.push(3); }
        if may_recv { acts.push(4); }
        if acts.is_empty() { break; }   // everything answered and nothing can arrive
        let mut chosen = acts[acts.len() - 1];
        for &a in &acts[..acts.len() - 1] { if nondet() { chosen = a; break; } }
        match chosen {
            1 => { let i = pending.pop_front().unwrap(); acked = i; send_msg(w1, Rep::Acked(i)); }
            2 => { let i = pending.pop_front().unwrap(); rejecting = true; send_msg(w1, Rep::Rejected(i)); }
            3 => { closed_sent = true; let l = closed.unwrap(); send_msg(w2, Rep::Closed(if l == 0 { -1 } else { l as i64 })); }
            _ => match recv_tagged_msg_block::<_, Req>(|_, tag| tag.is_none()) {
                Req::Add(i) => {
                    if rejecting || closed.is_some() { added = i; send_msg(w1, Rep::Rejected(i)); rejecting = true; }
                    else { added = i; pending.push_back(i); }
                }
                Req::Recover => {
                    recover_served = true;
                    // the close: some last >= acked among the entries added so far (an in-flight entry may have reached the ack quorum)
                    let mut last = acked;
                    while last < added && nondet() { last += 1; }
                    closed = Some(last);
                }
            },
        }
    }
}

type Prog = traceforge::program::Entry;
fn prog<F: Fn() + Send + Sync + 'static>(f: F) -> Prog { Arc::new(f) }
fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> { thread::Builder::new().name(n.to_string()).spawn(f).unwrap() }

fn op_spec(e: u64) -> Prog {
    prog(move || {
        let obj = named("ledger", move || ledger(e)).thread().id();
        let w1 = named("w1", move || writer(e, obj)).thread().id();
        let w2 = named("w2", move || recoverer(obj)).thread().id();
        named("init", move || send_tagged_msg(obj, SETUP_TAG, Setup { w1, w2 }));
    })
}
fn parse(a: &[String]) -> u64 {
    let usage = || -> ! { panic!("bk-ledger: ARGS are `<e> [fifo|bag]`, got {a:?}") };
    if a.is_empty() { usage(); }
    a[0].parse().unwrap_or_else(|_| usage())
}
fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &|_| vec!["w1".to_string(), "w2".to_string()],
        spec: &|a| op_spec(parse(a)),
        imp: &|a| op_spec(parse(a)),
    })
}
