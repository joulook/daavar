//! CockroachDB parallel commits with status resolution: the specification derived from the
//! design document (docs/20180324_parallel_commit.md) alone. One object serves T's client `c`
//! and the preventers `p1..pm`; T's fate (undecided, committed, aborted) is its only state that
//! clients can observe. See NOTES.md for the sentences each rule follows.
//! Usage: `bench_crdb_derived count spec [--model fifo|bag] -- <n> <p> <m> <a>`.
use std::process::ExitCode;
use std::sync::Arc;
use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

#[derive(Clone, Debug, PartialEq)] enum Fate { Committed, Aborted }
#[derive(Clone, Debug, PartialEq)] enum Req { Put(u8), Commit(Vec<u8>), Recover }
#[derive(Clone, Debug, PartialEq)] enum Rep { Written(u8), Committed, Retry, Aborted, Recovered(Fate) }

/// The object's setup message (invisible): the clients' addresses.
#[derive(Clone, Debug, PartialEq)]
struct Setup { c: ThreadId, ps: Vec<ThreadId> }
const SETUP: u32 = 1;

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

/// One step of the object.
#[derive(Clone, Debug)]
enum Act {
    /// take c's next request (c is not waiting for a reply)
    RecvC,
    /// take preventer i's Recover
    RecvP(usize),
    /// answer c's pending Put / Commit
    ReplyC(Rep),
    /// answer preventer i with this fate
    ReplyP(usize, Fate),
}

/// A choice among `k >= 1` alternatives, made with `k - 1` binary `nondet()`s.
fn choose(k: usize) -> usize {
    for i in 0..k - 1 {
        if nondet() { return i; }
    }
    k - 1
}

fn object(m: usize) {
    let Setup { c, ps } = recv_tagged_msg_block::<_, Setup>(|_, t| t == Some(SETUP));
    // T's fate: None while undecided; decided once, never changed (NOTES §2, §4).
    let mut fate: Option<Fate> = None;
    let mut c_done = false;
    // c's request awaiting its reply; a Commit pending = the epoch's commit window is open.
    let mut c_pend: Option<Req> = None;
    let mut received = vec![false; m];
    let mut pending = vec![false; m];
    loop {
        let any_pending = pending.iter().any(|&b| b);
        let window = matches!(c_pend, Some(Req::Commit(_)));
        let mut acts: Vec<Act> = Vec::new();
        if !c_done && c_pend.is_none() { acts.push(Act::RecvC); }
        for i in 0..m { if !received[i] { acts.push(Act::RecvP(i)); } }
        match &c_pend {
            Some(Req::Put(k)) => acts.push(Act::ReplyC(Rep::Written(*k))),
            Some(Req::Commit(_)) => {
                // Committed: T is (or now becomes) implicitly committed (NOTES §3).
                if fate != Some(Fate::Aborted) { acts.push(Act::ReplyC(Rep::Committed)); }
                // Aborted: a preventer's push or recovery has aborted T (its own report may be
                // delayed). No Retry: this setting has no path to a visible retry (NOTES §3).
                if fate == Some(Fate::Aborted) || (fate.is_none() && any_pending) { acts.push(Act::ReplyC(Rep::Aborted)); }
            }
            _ => {}
        }
        for i in 0..m {
            if !pending[i] { continue; }
            // Committed: the recovery found the staged record with all writes present (or the
            // record already committed); only while c's Commit is outstanding or after it.
            if fate == Some(Fate::Committed) || (fate.is_none() && window) { acts.push(Act::ReplyP(i, Fate::Committed)); }
            // Aborted: only while no one has decided Committed; the undecided branch stands for a
            // push that beat the staging or a recovery that prevented a final-batch write, placed
            // at or after this preventer's Recover and before any implicit commit (NOTES §4).
            if fate != Some(Fate::Committed) { acts.push(Act::ReplyP(i, Fate::Aborted)); }
        }
        if acts.is_empty() { break; }
        let act = acts[choose(acts.len())].clone();
        match act {
            Act::RecvC => {
                let cc = c;
                let r: Req = recv_tagged_msg_block(move |t, tag| t == cc && tag.is_none());
                match r {
                    Req::Put(_) | Req::Commit(_) => {}
                    Req::Recover => panic!("c sends no Recover"),
                }
                c_pend = Some(r);
            }
            Act::RecvP(i) => {
                let pi = ps[i];
                let r: Req = recv_tagged_msg_block(move |t, tag| t == pi && tag.is_none());
                assert!(r == Req::Recover);
                received[i] = true;
                pending[i] = true;
            }
            Act::ReplyC(rep) => {
                match rep {
                    Rep::Written(_) => {}
                    Rep::Committed => { fate = Some(Fate::Committed); c_done = true; }
                    Rep::Aborted => { fate = Some(Fate::Aborted); c_done = true; }
                    Rep::Retry | Rep::Recovered(_) => unreachable!(),
                }
                c_pend = None;
                send_msg(c, rep);
            }
            Act::ReplyP(i, f) => {
                fate = Some(f.clone());
                pending[i] = false;
                send_msg(ps[i], Rep::Recovered(f));
            }
        }
    }
    assert!(c_done && c_pend.is_none() && received.iter().all(|&b| b) && !pending.iter().any(|&b| b));
}

fn params(args: &[String]) -> (u8, u8, usize, u32) {
    let g = |i: usize, d: &str| args.get(i).map(String::as_str).unwrap_or(d).to_string();
    let n: u8 = g(0, "2").parse().expect("n");
    let p: u8 = g(1, "1").parse().expect("p");
    let m: usize = g(2, "1").parse().expect("m");
    let a: u32 = g(3, "1").parse().expect("a");
    assert!((1..=2).contains(&n) && p <= n && (1..=2).contains(&m) && a >= 1, "n in 1..2, p <= n, m in 1..2, a >= 1");
    assert!(!(n == 1 && p == 0), "n = 1, p = 0 is a one-phase commit and not an instance");
    (n, p, m, a)
}

fn spec(n: u8, p: u8, m: usize, a: u32) {
    let obj = thread::Builder::new().name("obj".to_string()).spawn(move || object(m)).unwrap();
    let oid = obj.thread().id();
    let ch = thread::Builder::new().name("c".to_string()).spawn(move || committer(n, p, a, oid)).unwrap();
    let cid = ch.thread().id();
    let mut phs = Vec::new();
    for i in 1..=m {
        phs.push(thread::Builder::new().name(format!("p{i}")).spawn(move || preventer(oid)).unwrap());
    }
    let ps: Vec<ThreadId> = phs.iter().map(|h| h.thread().id()).collect();
    let init = thread::Builder::new().name("init".to_string())
        .spawn(move || send_tagged_msg(oid, SETUP, Setup { c: cid, ps })).unwrap();
    init.join().unwrap();
    ch.join().unwrap();
    for h in phs { h.join().unwrap(); }
    obj.join().unwrap();
}

fn entry(args: &[String]) -> Entry {
    let (n, p, m, a) = params(args);
    Arc::new(move || spec(n, p, m, a))
}

fn main() -> ExitCode {
    traceforge::program::main(Program {
        visible: &|args: &[String]| {
            let (_, _, m, _) = params(args);
            let mut v = vec!["c".to_string()];
            for i in 1..=m { v.push(format!("p{i}")); }
            v
        },
        spec: &entry,
        imp: &entry,
    })
}
