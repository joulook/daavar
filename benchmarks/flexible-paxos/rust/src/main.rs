//! Flexible Paxos (Howard, Malkhi, Spiegelman, OPODIS 2016), single decree, with the
//! majority quorums of section 4.1: Q1 = floor(A/2)+1, Q2 = A - Q1 + 1 (= A/2 for even A).
//! Usage: `bench_fpaxos count impl --model bag -- <A> <K>` (P = K proposers, R = 1 retry).
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::channel::{Builder as ChanBuilder, Receiver, Sender};
mod spec; // the coordinator's hand-written specification (plan/twins-contextfree/flexible-paxos/spec.rs); the agent's program is `imp`
use traceforge::program::{main_with_model, Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

/// Number of retries with a higher proposal number after a failed phase.
const R: u32 = 1;

/// A proposal number: (round, proposer index), compared by round first
/// (the lexicographic order of Rust tuples).
type Ballot = (i32, i32);

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    // client <-> proposer (the externally observable messages)
    Propose { from: i32, value: i32 },
    Decided { value: i32 },
    Fail {},
    // proposer -> acceptor
    /// `prepare(p)`; `reply_to` is the proposer's address.
    Prepare { p: Ballot, reply_to: ThreadId },
    /// The paper's phase-2 `propose(p,v)` (renamed: `Propose` is the client's message).
    Propose2 { p: Ballot, v: i32, reply_to: ThreadId },
    /// Shutdown: the proposer will send nothing more.
    Done {},
    // acceptor -> proposer (tagged with `tag_of(p, phase)`)
    /// `promise(p', v')` for prepare `p`: the acceptor's last accepted proposal, if any.
    Promise { p: Ballot, last: Option<(Ballot, i32)> },
    /// `accept(p)`.
    Accept { p: Ballot },
    /// Conflict reply: the request with number `p` was refused; the acceptor promised `promised`.
    Nack { p: Ballot, promised: Ballot },
}

/// The tag of the acceptors' replies to the phase-`phase` request numbered `p`. A
/// proposer's rounds strictly increase, so this tag names one attempt and phase.
fn tag_of(p: Ballot, phase: u32) -> u32 {
    (p.0 as u32) * 2 + phase
}

/// Client `i`'s inbox, known to everyone from the start (see NOTES.md, D1).
fn client_inbox(i: usize) -> (Sender<Msg>, Receiver<Msg>) {
    ChanBuilder::<Msg>::new()
        .with_name(("fpaxos-client", i))
        .build()
}

fn acceptor(proposers: usize) {
    let mut promised: Option<Ballot> = None;
    let mut accepted: Option<(Ballot, i32)> = None;
    let mut done = 0;
    while done < proposers {
        match recv_msg_block::<Msg>() {
            // Phase 1 ii
            Msg::Prepare { p, reply_to } => {
                if promised.map_or(true, |q| p > q) {
                    promised = Some(p);
                    send_tagged_msg(reply_to, tag_of(p, 1), Msg::Promise { p, last: accepted });
                } else {
                    let promised = promised.unwrap();
                    send_tagged_msg(reply_to, tag_of(p, 1), Msg::Nack { p, promised });
                }
            }
            // Phase 2 ii
            Msg::Propose2 { p, v, reply_to } => {
                if promised.map_or(true, |q| p >= q) {
                    promised = Some(p);
                    accepted = Some((p, v));
                    send_tagged_msg(reply_to, tag_of(p, 2), Msg::Accept { p });
                } else {
                    let promised = promised.unwrap();
                    send_tagged_msg(reply_to, tag_of(p, 2), Msg::Nack { p, promised });
                }
            }
            Msg::Done {} => done += 1,
            m => panic!("acceptor: unexpected {m:?}"),
        }
    }
}

/// Outcome of one phase: `Ok(replies)` with a quorum of positive replies, or
/// `Err(highest round seen in a Nack)` once a quorum can no longer be reached.
fn collect(p: Ballot, phase: u32, n: usize, quorum: usize) -> Result<Vec<Msg>, i32> {
    let tag = tag_of(p, phase);
    let mut oks = Vec::new();
    let mut nacks = 0;
    let mut highest = p.0;
    loop {
        let m: Msg = recv_tagged_msg_block(move |_, t| t == Some(tag));
        match m {
            Msg::Nack { p: q, promised } if q == p => {
                nacks += 1;
                highest = highest.max(promised.0);
            }
            Msg::Promise { p: q, .. } | Msg::Accept { p: q } if q == p => oks.push(m),
            m => panic!("proposer: unexpected {m:?}"),
        }
        if oks.len() >= quorum {
            return Ok(oks);
        }
        if nacks > n - quorum {
            return Err(highest);
        }
    }
}

fn proposer(me: i32, acceptors: Vec<ThreadId>, q1: usize, q2: usize) {
    let self_id = thread::current_id();
    let n = acceptors.len();
    let (from, value) = match recv_tagged_msg_block::<_, Msg>(|_, t| t.is_none()) {
        Msg::Propose { from, value } => (from, value),
        m => panic!("proposer: unexpected {m:?}"),
    };
    let (reply, _) = client_inbox(from as usize);
    let mut round = 1;
    let mut outcome = Msg::Fail {};
    for _attempt in 0..=R {
        // Phase 1 i
        let p: Ballot = (round, me);
        for &a in &acceptors {
            send_msg(a, Msg::Prepare { p, reply_to: self_id });
        }
        // Phase 1 iii
        let promises = match collect(p, 1, n, q1) {
            Ok(ps) => ps,
            Err(highest) => {
                round = highest + 1;
                continue;
            }
        };
        // Phase 2 i: the value of the highest-numbered accepted proposal, else our own
        let mut best: Option<(Ballot, i32)> = None;
        for m in &promises {
            if let Msg::Promise { last: Some((b, v)), .. } = m {
                if best.map_or(true, |(bb, _)| *b > bb) {
                    best = Some((*b, *v));
                }
            }
        }
        let v = best.map_or(value, |(_, v)| v);
        for &a in &acceptors {
            send_msg(a, Msg::Propose2 { p, v, reply_to: self_id });
        }
        // Phase 2 iii
        match collect(p, 2, n, q2) {
            Ok(_) => {
                outcome = Msg::Decided { value: v };
                break;
            }
            Err(highest) => round = highest + 1,
        }
    }
    for &a in &acceptors {
        send_msg(a, Msg::Done {});
    }
    reply.send_msg(outcome);
}

fn client(i: usize, proposer: ThreadId) {
    let (_, inbox) = client_inbox(i);
    send_msg(proposer, Msg::Propose { from: i as i32, value: i as i32 + 1 });
    match inbox.recv_msg_block() {
        Msg::Decided { .. } | Msg::Fail {} => {}
        m => panic!("client: unexpected {m:?}"),
    }
}

fn fpaxos(a: usize, k: usize) {
    let q1 = a / 2 + 1;
    let q2 = a - q1 + 1;
    let mut acc_h = Vec::new();
    for j in 0..a {
        acc_h.push(
            thread::Builder::new()
                .name(format!("a{j}"))
                .spawn(move || acceptor(k))
                .unwrap(),
        );
    }
    let acc: Vec<ThreadId> = acc_h.iter().map(|h| h.thread().id()).collect();
    let mut prop_h = Vec::new();
    for i in 0..k {
        let acc = acc.clone();
        prop_h.push(
            thread::Builder::new()
                .name(format!("p{i}"))
                .spawn(move || proposer(i as i32, acc, q1, q2))
                .unwrap(),
        );
    }
    let mut cli_h = Vec::new();
    for i in 0..k {
        let p = prop_h[i].thread().id();
        cli_h.push(
            thread::Builder::new()
                .name(format!("c{i}"))
                .spawn(move || client(i, p))
                .unwrap(),
        );
    }
    for h in acc_h.into_iter().chain(prop_h).chain(cli_h) {
        h.join().unwrap();
    }
}

fn sizes(args: &[String]) -> (usize, usize) {
    let a = args.first().and_then(|s| s.parse().ok()).unwrap_or(4);
    let k = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(2);
    (a, k)
}

fn visible(args: &[String]) -> Vec<String> {
    let (_, k) = sizes(args);
    (0..k).map(|i| format!("c{i}")).collect()
}

fn spec_entry(args: &[String]) -> Entry {
    let (a, k) = sizes(args);
    Arc::new(move || spec::spec(a, k))
}

fn imp(args: &[String]) -> Entry {
    let (a, k) = sizes(args);
    Arc::new(move || fpaxos(a, k))
}

fn main() -> ExitCode {
    main_with_model(
        Program { visible: &visible, spec: &spec_entry, imp: &imp },
        ConsType::Bag,
    )
}
