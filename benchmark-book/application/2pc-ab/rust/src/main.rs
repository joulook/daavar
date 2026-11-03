//! Benchmark two-pc-abstain-kragl (candidate N21): the one-transaction two-phase commit of Kragl
//! et al., PLDI 2020 (`2PC.bpl`), with early abort and abstaining participants, against the
//! source's top-level atomic action `MAIN5`. Run by TraceForge's shared main
//! (`traceforge::program`): `count`, `export`, `conf`, `online`.
//!
//! ARGS: `<n> [variant]`; `n >= 1` participants; variant (impl side) `correct` (default) or
//! `off-by-one` (the coordinator's vote loop runs to `n - 1`: it commits after `n - 1` `Yes`
//! votes; the seeded fault, `2PC.bpl` ships none). Model: `--model bag` (multiset channels).
//!
//! Visible threads: `p1 .. pn`, the same code on both sides (`participant1` + `participant2`):
//! `if (*)` { receive `Req`; vote `Yes` or `No` (`havoc v`); send `Vote(v)` to the coordinator };
//! then receive `Dec(d)`. An abstaining participant never receives its request.
//! * spec: an invisible `coord` running `MAIN5`: it sends `Req` to every participant; then, before
//!   each of up to `n` vote receives, it may abort (`nondet()`); after `n` votes it commits only if
//!   all were `Yes` (and may still abort); the one decision goes to every participant.
//! * impl: `coord` running `coordinator1` + `coordinator2`: `Req` to everyone, then receive votes
//!   until `n` `Yes` or the first `No` (early abort, the rest not drained), decision to everyone.
//! Every thread has one inbox; receives select by message tag (the Java twin's type tests).
use traceforge::channel::{Builder, Receiver, Sender};
use traceforge::thread;
use traceforge::*;

const T_REQ: u32 = 1;
const T_VOTE: u32 = 2;
const T_DEC: u32 = 3;

#[derive(Clone, Debug, PartialEq)]
enum Ballot {
    Yes,
    No,
}

#[derive(Clone, Debug, PartialEq)]
enum Decision {
    Commit,
    Abort,
}

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// coordinator -> participant: `send_req(i)`.
    Req,
    /// participant -> coordinator: `send_vote(v)` (no sender identity, as in `VoteCH: [vote]int`).
    Vote(Ballot),
    /// coordinator -> participant: `send_dec(i, d)`.
    Dec(Decision),
}

type Tx = Sender<Msg>;
type Rx = Receiver<Msg>;

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn recv(rx: &Rx, tag: u32) -> Msg {
    rx.recv_tagged_msg_block(move |t| t == Some(tag))
}

fn send(tx: &Tx, m: Msg) {
    let tag = match m {
        Msg::Req => T_REQ,
        Msg::Vote(_) => T_VOTE,
        Msg::Dec(_) => T_DEC,
    };
    tx.send_tagged_msg(tag, m);
}

/// `participant1` then `participant2`; `t[0]` is the coordinator.
fn participant(rx: Rx, t: Vec<Tx>) {
    if nondet() {
        let _ = recv(&rx, T_REQ);
        let v = if nondet() { Ballot::Yes } else { Ballot::No };
        send(&t[0], Msg::Vote(v));
    }
    let _ = recv(&rx, T_DEC);
}

fn broadcast(n: usize, t: &[Tx], d: Decision) {
    for i in 1..=n {
        send(&t[i], Msg::Dec(d.clone()));
    }
}

/// `MAIN5`: any decision, `Commit` only if every participant voted `Yes`; one decision for all.
fn spec_coord(n: usize, rx: Rx, t: Vec<Tx>) {
    for i in 1..=n {
        send(&t[i], Msg::Req);
    }
    let mut all_yes = true;
    for _ in 0..n {
        if nondet() {
            broadcast(n, &t, Decision::Abort);
            return;
        }
        if let Msg::Vote(Ballot::No) = recv(&rx, T_VOTE) {
            all_yes = false;
        }
    }
    let d = if all_yes && nondet() { Decision::Commit } else { Decision::Abort };
    broadcast(n, &t, d);
}

/// `coordinator1` + `coordinator2`; `quorum` is `n` (correct) or `n - 1` (off-by-one).
fn impl_coord(n: usize, quorum: usize, rx: Rx, t: Vec<Tx>) {
    for i in 1..=n {
        send(&t[i], Msg::Req);
    }
    let mut d = Decision::Commit;
    for _ in 0..quorum {
        if let Msg::Vote(Ballot::No) = recv(&rx, T_VOTE) {
            d = Decision::Abort;
            break;
        }
    }
    broadcast(n, &t, d);
}

fn program(n: usize, coord: Box<dyn FnOnce(Rx, Vec<Tx>) + Send>) {
    // inboxes: 0 coord, 1..=n participants
    let (tx, rxs): (Vec<Tx>, Vec<Rx>) = (0..=n).map(|_| Builder::<Msg>::new().build()).unzip();
    let mut rxs: Vec<Option<Rx>> = rxs.into_iter().map(Some).collect();
    let mut hs = Vec::new();
    let crx = rxs[0].take().unwrap();
    let t = tx.clone();
    hs.push(named("coord", move || coord(crx, t)));
    for i in 1..=n {
        let rx = rxs[i].take().unwrap();
        let t = tx.clone();
        hs.push(named(&format!("p{i}"), move || participant(rx, t)));
    }
    for h in hs {
        h.join().unwrap();
    }
}

const PROGRAM: &str = "two-pc-abstain-kragl";
const USAGE: &str = "`<n> [correct|off-by-one]`, n >= 1";

struct Args {
    n: usize,
    off_by_one: bool,
}

fn parse(a: &[String]) -> Args {
    if a.is_empty() || a.len() > 2 {
        panic!("{PROGRAM}: ARGS are {USAGE}, got {a:?}");
    }
    let n: usize = a[0]
        .parse()
        .unwrap_or_else(|_| panic!("{PROGRAM}: <n> must be a number; ARGS are {USAGE}"));
    if n < 1 {
        panic!("{PROGRAM}: n must be at least 1 (the source's axiom n > 0)");
    }
    let off_by_one = match a.get(1).map(String::as_str).unwrap_or("correct") {
        "correct" => false,
        "off-by-one" => true,
        v => panic!("{PROGRAM}: unknown variant `{v}`; ARGS are {USAGE}"),
    };
    Args { n, off_by_one }
}

fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &|a| (1..=parse(a).n).map(|i| format!("p{i}")).collect(),
        spec: &|a| {
            let n = parse(a).n;
            std::sync::Arc::new(move || program(n, Box::new(move |rx, t| spec_coord(n, rx, t))))
        },
        imp: &|a| {
            let p = parse(a);
            let n = p.n;
            let q = if p.off_by_one { n - 1 } else { n };
            std::sync::Arc::new(move || program(n, Box::new(move |rx, t| impl_coord(n, q, rx, t))))
        },
    })
}
