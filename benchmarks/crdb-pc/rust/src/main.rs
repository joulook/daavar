//! S_pc: the client-visible content of CockroachDB's parallel commits with transaction recovery (the TLA+ model
//! ParallelCommits.tla: AckImpliesCommit and the record/commit stability properties, as the committer's client `c` and
//! the preventers' clients `p1`..`pm` see them), the hand-written specification of the CockroachDB study; the interface
//! is plan/case-studies/cockroach/INTERFACE.md, the reply rules are in NOTES.md (revision 2: no Retry, an abort needs a
//! pending preventer). One invisible object `pc` with
//! `fate in {Undecided, Committed, Aborted}` and `outstanding` (c's Commit received and not yet answered). Entries:
//! `spec` = this object; `imp` = the same. ARGS `<n> <p> <m> <a> [fifo|bag]`.
use std::sync::Arc;
use traceforge::thread::{self, ThreadId};
use traceforge::{nondet, recv_msg_block, recv_tagged_msg_block, send_msg, send_tagged_msg};

#[derive(Clone, Debug, PartialEq)] enum Fate { Committed, Aborted }
#[derive(Clone, Debug, PartialEq)] enum Req { Put(u8), Commit(Vec<u8>), Recover }
#[allow(dead_code)] // Retry: kept in the verbatim interface type, never sent by S_pc
#[derive(Clone, Debug, PartialEq)] enum Rep { Written(u8), Committed, Retry, Aborted, Recovered(Fate) }
#[derive(Clone, Debug, PartialEq)]
struct Setup { c: ThreadId, ps: Vec<ThreadId> }
const SETUP_TAG: u32 = 1;

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

#[derive(Clone, Copy, Debug, PartialEq)]
enum F { Undecided, Committed, Aborted }

#[derive(Clone, Copy, Debug, PartialEq)]
enum Act { RecvC, AckC, AbortC, RecvP(usize), Serve(usize) }

/// Receive the next untagged request of `from`.
fn recv_from(from: ThreadId) -> Req { recv_tagged_msg_block::<_, Req>(move |s, tag| s == from && tag.is_none()) }

/// The object. Enabled actions at each step (one is chosen by nondet; NOTES.md gives the clause of each rule):
/// - RecvC: receive c's next request (c has not ended and no Commit is outstanding): `Put(k)` is answered `Written(k)`
///   at once; `Commit` makes the commit outstanding.
/// - AckC: reply `Committed` to the outstanding Commit, if fate != Aborted; fate := Committed.
/// - AbortC: reply `Aborted` to the outstanding Commit, if fate == Aborted, or fate == Undecided and some preventer's
///   Recover has been received and not yet answered (an abort needs an actor: that preventer's push or recovery);
///   fate := Aborted.
/// - No Retry: in this interface no path produces a retry error; S4 is "Retry never occurs" (the client loop still
///   accepts Retry syntactically; an implementation that sends it violates S_pc).
/// - RecvP(i): receive p_i's Recover; it is pending from then on.
/// - Serve(i): reply `Recovered(f)` to a pending p_i: f = the fate if decided; if Undecided, Committed or Aborted by
///   nondet while the Commit is outstanding (fixing the fate), Aborted (the preventer prevents) otherwise.
/// The loop ends when c has ended and every preventer has its reply.
fn object(p: u8, m: usize) {
    let Setup { c, ps } = recv_tagged_msg_block::<_, Setup>(|_, tag| tag == Some(SETUP_TAG));
    assert!(ps.len() == m);
    let mut fate = F::Undecided;
    let mut outstanding = false;
    let mut c_done = false;
    let mut puts_done = 0u8;      // Puts received and answered
    let mut received = vec![false; m];
    let mut served = vec![false; m];
    loop {
        let pending_any = (0..m).any(|i| received[i] && !served[i]);
        let mut acts: Vec<Act> = Vec::new();
        if !c_done && !outstanding { acts.push(Act::RecvC); }
        if outstanding && fate != F::Aborted { acts.push(Act::AckC); }
        if outstanding && (fate == F::Aborted || (fate == F::Undecided && pending_any)) { acts.push(Act::AbortC); }
        for i in 0..m { if !received[i] { acts.push(Act::RecvP(i)); } }
        for i in 0..m { if received[i] && !served[i] { acts.push(Act::Serve(i)); } }
        if acts.is_empty() { break; }
        let mut chosen = acts[acts.len() - 1];
        for &x in &acts[..acts.len() - 1] { if nondet() { chosen = x; break; } }
        match chosen {
            Act::RecvC => match recv_from(c) {
                Req::Put(k) => { assert!(k == puts_done + 1 && k <= p); puts_done = k; send_msg(c, Rep::Written(k)); }
                Req::Commit(_) => { assert!(puts_done == p); outstanding = true; }
                Req::Recover => panic!("Recover from c"),
            },
            Act::AckC => { fate = F::Committed; outstanding = false; c_done = true; send_msg(c, Rep::Committed); }
            Act::AbortC => { fate = F::Aborted; outstanding = false; c_done = true; send_msg(c, Rep::Aborted); }
            Act::RecvP(i) => {
                let Req::Recover = recv_from(ps[i]) else { panic!("Recover expected from p{}", i + 1) };
                received[i] = true;
            }
            Act::Serve(i) => {
                served[i] = true;
                if fate == F::Undecided { fate = if outstanding && nondet() { F::Committed } else { F::Aborted }; }
                let f = if fate == F::Committed { Fate::Committed } else { Fate::Aborted };
                send_msg(ps[i], Rep::Recovered(f));
            }
        }
    }
}

type Prog = traceforge::program::Entry;
fn prog<G: Fn() + Send + Sync + 'static>(f: G) -> Prog { Arc::new(f) }
fn named<G: FnOnce() + Send + 'static>(n: &str, f: G) -> thread::JoinHandle<()> { thread::Builder::new().name(n.to_string()).spawn(f).unwrap() }

fn op_spec(n: u8, p: u8, m: usize, a: u32) -> Prog {
    prog(move || {
        let obj = named("pc", move || object(p, m)).thread().id();
        let c = named("c", move || committer(n, p, a, obj)).thread().id();
        let ps: Vec<ThreadId> = (1..=m).map(|i| named(&format!("p{i}"), move || preventer(obj)).thread().id()).collect();
        named("init", move || send_tagged_msg(obj, SETUP_TAG, Setup { c, ps: ps.clone() }));
    })
}
fn parse(a: &[String]) -> (u8, u8, usize, u32) {
    let fail = |why: String| -> ! {
        eprintln!("crdb-pc: {why}; ARGS are `<n> <p> <m> <a> [fifo|bag]` with n in 1..2, p in 0..n, (n,p) != (1,0), m in 1..2, a >= 1; got {a:?}");
        std::process::exit(2)
    };
    if a.len() < 4 { fail("four ARGS expected".to_string()); }
    let num = |i: usize, what: &str| -> u32 { a[i].parse().unwrap_or_else(|_| fail(format!("{what} is not a number"))) };
    let (n, p, m, e) = (num(0, "n"), num(1, "p"), num(2, "m"), num(3, "a"));
    if !(1..=2).contains(&n) { fail(format!("n = {n} is not in 1..2")); }
    if p > n { fail(format!("p = {p} exceeds n = {n}")); }
    if n == 1 && p == 0 { fail("(n,p) = (1,0) is a one-phase commit, which the interface excludes".to_string()); }
    if !(1..=2).contains(&m) { fail(format!("m = {m} is not in 1..2")); }
    if e < 1 { fail("a must be at least 1".to_string()); }
    (n as u8, p as u8, m as usize, e)
}
fn visible(a: &[String]) -> Vec<String> {
    let (_, _, m, _) = parse(a);
    let mut v = vec!["c".to_string()];
    for i in 1..=m { v.push(format!("p{i}")); }
    v
}
fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &visible,
        spec: &|a| { let (n, p, m, e) = parse(a); op_spec(n, p, m, e) },
        imp: &|a| { let (n, p, m, e) = parse(a); op_spec(n, p, m, e) },
    })
}
