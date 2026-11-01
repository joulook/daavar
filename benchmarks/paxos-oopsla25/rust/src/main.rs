//! Benchmark paxos-oopsla25: single-decree Paxos of Bílý, Pereira, Müller (OOPSLA 2025, §5.3,
//! `refinement-examples/src/bin/ex05_paxos.rs`, after Lamport's TLA+ Phase1a/1b/2a/2b) against
//! a consensus oracle (`RELATED-BENCHMARKS.tex` §B1).
//!
//! Usage: `<bin> count|export|conf|online … --model M -- <L> [variant]`; `L` = learners (1..=3);
//! 2 clients, 2 proposers (ballots 1, 2), 3 acceptors. Model `--model bag` (asyn: the source's
//! message soup; the default this program declares), default variant `correct`. Variants
//! (impl side): `correct`, `ignore-max-vote`,
//! `skip-2a-check`, `half-majority`, `1b-none`. `count` prints
//! `execs=<complete> blocked=<blocked>` of plain Must; `conf` runs TraceForge's conformance check
//! of `impl <variant>` against `spec` and prints the verdict (`--engine enumerator` for the
//! diagnosing engine).
//!
//! Visible threads: `c0`, `c1` (clients, one proposal each), `l0..l{L-1}` (learners, one
//! decision each). Invisible: `oracle` (spec); `p1`, `p2`, `a0`, `a1`, `a2` (impl). Every thread
//! receives only on its own named channel.
use traceforge::channel::{self, Receiver, Sender};
use traceforge::thread::{self, JoinHandle};
use traceforge::*;

const ACCEPTORS: usize = 3;
const PROPOSERS: usize = 2; // ballots 1..=PROPOSERS; proposer b serves client b-1

type Ballot = u8;
type Value = u8;

/// A vote an acceptor reports in `Msg1b`: the ballot and the value, in that order.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Vote(Ballot, Value);

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// client -> proposer (spec: client -> oracle)
    Propose(Value),
    /// `Msg1a(Ballot)`, proposer -> acceptor
    Msg1a(Ballot),
    /// `Msg1b(Acceptor, Ballot, Option<Vote>)`, acceptor -> proposer of the ballot
    Msg1b(u8, Ballot, Option<Vote>),
    /// `Msg2a(Ballot, Value)`, proposer -> acceptor
    Msg2a(Ballot, Value),
    /// `Msg2b(Acceptor, Ballot, Value)`, acceptor -> proposer of the ballot
    Msg2b(u8, Ballot, Value),
    /// proposer (spec: oracle) -> learner
    Decide(Value),
}

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn inbox(role: &'static str, i: usize) -> (Sender<Msg>, Receiver<Msg>) {
    channel::Builder::<Msg>::new().with_name((role, i)).build()
}

fn learner(j: usize) {
    let Msg::Decide(_v) = inbox("l", j).1.recv_msg_block() else { unreachable!() };
}

/// Client `i` proposes value `i + 1` to `to`.
fn client(i: usize, to: Sender<Msg>) {
    to.send_msg(Msg::Propose(i as u8 + 1));
}

/// The consensus oracle: reads each proposal non-blocking (it may leave any unread, so the
/// decision depends only on what it read), then decides one value read, or nothing, and
/// tells every learner the same value.
fn spec(l: usize) {
    let o = named("oracle", move || {
        let rx = inbox("o", 0).1;
        let mut vals: Vec<Value> = Vec::new();
        for _ in 0..PROPOSERS {
            if let Some(Msg::Propose(v)) = rx.recv_msg() {
                vals.push(v);
            }
        }
        if !vals.is_empty() && nondet() {
            let v = vals[(0..vals.len()).nondet()];
            for j in 0..l {
                inbox("l", j).0.send_msg(Msg::Decide(v));
            }
        }
    });
    let ls: Vec<_> = (0..l).map(|j| named(&format!("l{j}"), move || learner(j))).collect();
    let cs: Vec<_> = (0..2).map(|i| named(&format!("c{i}"), move || client(i, inbox("o", 0).0))).collect();
    for h in cs.into_iter().chain(ls) {
        h.join().unwrap();
    }
    o.join().unwrap();
}

#[derive(Clone, Copy)]
struct Faults {
    ignore_max_vote: bool,
    skip_2a_check: bool,
    half_majority: bool,
    report_none: bool,
}

/// `run_leader_ballot` for ballot `b`, plus the decision announcement: collect 1b's of ballot
/// `b` until a majority, propose the highest-ballot vote or the own value (`choose_value()`),
/// collect 2b's until a majority, announce to every learner. Receives are blocking: a ballot
/// that never gets its majority waits forever (the source's `decide()` abandonment and the
/// soup's loss, both invisible: an abandoned proposer sends nothing more, as a waiting one).
/// Each loop is bounded by the acceptor count: phase 1 reads only 1b's; phase 2 reads at most
/// `ACCEPTORS - majority` late 1b's before its `majority` 2b's.
fn proposer(b: Ballot, l: usize, f: Faults) {
    let rx = inbox("p", b as usize).1;
    let majority = if f.half_majority { ACCEPTORS / 2 } else { ACCEPTORS / 2 + 1 };
    let Msg::Propose(mine) = rx.recv_msg_block() else { unreachable!() };
    for a in 0..ACCEPTORS {
        inbox("a", a).0.send_msg(Msg::Msg1a(b));
    }
    let mut q: Vec<u8> = Vec::new();
    let mut max_vote: Option<Vote> = None;
    for _ in 0..ACCEPTORS {
        if q.len() >= majority {
            break;
        }
        if let Msg::Msg1b(a, mb, vote) = rx.recv_msg_block() {
            if mb == b && !q.contains(&a) {
                q.push(a);
                if let Some(Vote(vb, vv)) = vote {
                    if max_vote.map_or(true, |Vote(x, _)| vb > x) {
                        max_vote = Some(Vote(vb, vv));
                    }
                }
            }
        }
    }
    let v = match max_vote {
        Some(Vote(_, vv)) if !f.ignore_max_vote => vv,
        _ => mine,
    };
    for a in 0..ACCEPTORS {
        inbox("a", a).0.send_msg(Msg::Msg2a(b, v));
    }
    let mut acc: Vec<u8> = Vec::new();
    for _ in 0..ACCEPTORS {
        if acc.len() >= majority {
            break;
        }
        if let Msg::Msg2b(a, mb, _) = rx.recv_msg_block() {
            if mb == b && !acc.contains(&a) {
                acc.push(a);
            }
        }
    }
    if acc.len() >= majority {
        for j in 0..l {
            inbox("l", j).0.send_msg(Msg::Decide(v));
        }
    }
}

/// `run_acceptor`, bounded to 2 prepares + 2 accepts: 4 blocking receives (the A4 idiom: an
/// acceptor ends blocked, unobserved, when a proposer sent it fewer).
fn acceptor(me: usize, f: Faults) {
    let rx = inbox("a", me).1;
    let mut max_bal: Option<Ballot> = None;
    let mut max_vote: Option<Vote> = None;
    for _ in 0..2 * PROPOSERS {
        match rx.recv_msg_block() {
            Msg::Msg1a(b) => {
                if max_bal.map_or(true, |mb| b > mb) {
                    max_bal = Some(b);
                    let vote = if f.report_none { None } else { max_vote };
                    inbox("p", b as usize).0.send_msg(Msg::Msg1b(me as u8, b, vote));
                }
            }
            Msg::Msg2a(b, v) => {
                if f.skip_2a_check || max_bal.map_or(true, |mb| b >= mb) {
                    max_bal = Some(b);
                    max_vote = Some(Vote(b, v));
                    inbox("p", b as usize).0.send_msg(Msg::Msg2b(me as u8, b, v));
                }
            }
            _ => unreachable!(),
        }
    }
}

fn imp(l: usize, f: Faults) {
    let mut hs = Vec::new();
    for b in 1..=PROPOSERS as u8 {
        hs.push(named(&format!("p{b}"), move || proposer(b, l, f)));
    }
    for a in 0..ACCEPTORS {
        hs.push(named(&format!("a{a}"), move || acceptor(a, f)));
    }
    let ls: Vec<_> = (0..l).map(|j| named(&format!("l{j}"), move || learner(j))).collect();
    let cs: Vec<_> = (0..2).map(|i| named(&format!("c{i}"), move || client(i, inbox("p", i + 1).0))).collect();
    for h in cs.into_iter().chain(ls) {
        h.join().unwrap();
    }
    for h in hs {
        h.join().unwrap();
    }
}

fn faults(variant: &str) -> Faults {
    let mut f = Faults { ignore_max_vote: false, skip_2a_check: false, half_majority: false, report_none: false };
    match variant {
        "correct" => {}
        "ignore-max-vote" => f.ignore_max_vote = true,
        "skip-2a-check" => f.skip_2a_check = true,
        "half-majority" => f.half_majority = true,
        "1b-none" => f.report_none = true,
        v => panic!("unknown variant {v}; one of correct, ignore-max-vote, skip-2a-check, half-majority, 1b-none"),
    }
    f
}

fn parse(a: &[String]) -> (usize, Faults) {
    let l: usize = a.first().and_then(|s| s.parse().ok()).unwrap_or(1);
    assert!((1..=3).contains(&l), "L must be 1..=3");
    let f = faults(a.get(1).map(String::as_str).unwrap_or("correct"));
    (l, f)
}

fn main() -> std::process::ExitCode {
    let program = traceforge::program::Program {
        visible: &|a| {
            let mut visible: Vec<String> = vec!["c0".into(), "c1".into()];
            visible.extend((0..parse(a).0).map(|j| format!("l{j}")));
            visible
        },
        spec: &|a| {
            let (l, _) = parse(a);
            std::sync::Arc::new(move || spec(l))
        },
        imp: &|a| {
            let (l, f) = parse(a);
            std::sync::Arc::new(move || imp(l, f))
        },
    };
    traceforge::program::main_with_model(program, traceforge::ConsType::Bag)
}
