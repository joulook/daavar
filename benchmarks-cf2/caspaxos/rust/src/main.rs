//! CASPaxos (Rystsov, arXiv:1802.07000v9, sections 2.1-2.2) as a Must-API message-passing program.
//!
//! Roles: clients `c0..c{K-1}` (visible), proposers `p0..p{P-1}` and acceptors `a0..a{A-1}`
//! (internal). Arguments: `<A> <K>`. P = 2, R = 1 are constants. See NOTES.md.
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use traceforge::program::{main_with_model, Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

/// Number of proposers (INTERFACE.md).
const P: usize = 2;
/// Number of times a proposer may retry one request after a conflict (INTERFACE.md).
const R: usize = 1;
/// Tags below this value are client requests (tag = client index); tags at or above it
/// address acceptor replies to one phase of one ballot.
const REPLY_TAG_BASE: u32 = 1000;

/// A ballot number: the paper's (counter, ID) tuple, compared on the counter first and
/// on the proposer ID as a tiebreaker (the derived `Ord` is lexicographic in field order).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Ballot {
    counter: u32,
    id: u32,
}

/// The bottom ballot: smaller than every ballot a proposer generates (counters start at 1).
const ZERO: Ballot = Ballot { counter: 0, id: 0 };

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    // client <-> proposer (fixed by INTERFACE.md)
    Cas { expected: i32, value: i32 },
    Done { value: i32 },
    Fail {},
    // proposer -> acceptor
    Prepare { ballot: Ballot },
    Accept { ballot: Ballot, value: i32 },
    /// The proposer has no more requests to serve (termination of the acceptors).
    Stop {},
    // acceptor -> proposer
    /// Confirmation of a prepare: empty (`None`) or the accepted (ballot, value) tuple.
    Promise { ballot: Ballot, accepted: Option<(Ballot, i32)> },
    /// Confirmation of an accept.
    Accepted { ballot: Ballot },
    /// The acceptor already saw `seen`, which is greater than `ballot`.
    Conflict { ballot: Ballot, seen: Ballot },
}

/// The tag of the acceptors' replies to `phase` (1 or 2) of `b`; unique per (ballot, phase).
fn reply_tag(b: Ballot, phase: u32) -> u32 {
    REPLY_TAG_BASE + (b.counter * P as u32 + b.id) * 2 + (phase - 1)
}

/// Thread ids published by their owners before any message that leads to their use.
type Registry = Arc<Mutex<Vec<Option<ThreadId>>>>;

fn lookup(r: &Registry, i: usize) -> ThreadId {
    r.lock().unwrap()[i].expect("thread id read before it was published")
}

/// A choice of one of `n` alternatives with the API's choice operation (only when n > 1).
fn choose(n: usize) -> usize {
    for k in 0..n.saturating_sub(1) {
        if nondet() {
            return k;
        }
    }
    n - 1
}

fn acceptor(proposers: Vec<ThreadId>) {
    let mut promise: Ballot = ZERO; // the persisted promise (ZERO = none)
    let mut accepted: Option<(Ballot, i32)> = None; // the accepted (ballot, value) tuple
    let mut stops = 0;
    while stops < proposers.len() {
        // the greatest ballot number this acceptor has seen
        let seen = promise.max(accepted.map(|(b, _)| b).unwrap_or(ZERO));
        match recv_msg_block::<Msg>() {
            Msg::Prepare { ballot } => {
                let to = proposers[ballot.id as usize];
                if seen > ballot {
                    send_tagged_msg(to, reply_tag(ballot, 1), Msg::Conflict { ballot, seen });
                } else {
                    promise = ballot;
                    send_tagged_msg(to, reply_tag(ballot, 1), Msg::Promise { ballot, accepted });
                }
            }
            Msg::Accept { ballot, value } => {
                let to = proposers[ballot.id as usize];
                if seen > ballot {
                    send_tagged_msg(to, reply_tag(ballot, 2), Msg::Conflict { ballot, seen });
                } else {
                    promise = ZERO; // erases the promise
                    accepted = Some((ballot, value));
                    send_tagged_msg(to, reply_tag(ballot, 2), Msg::Accepted { ballot });
                }
            }
            Msg::Stop {} => stops += 1,
            m => panic!("acceptor received an unexpected message {m:?}"),
        }
    }
}

/// Waits for replies to `phase` of `b` until F + 1 confirmations arrived (`Ok`, with the
/// confirmations) or a quorum can no longer be reached (`Err`, with the greatest ballot
/// reported in a conflict).
fn await_quorum(b: Ballot, phase: u32, a: usize) -> Result<Vec<Msg>, Ballot> {
    let f = (a - 1) / 2;
    let tag = reply_tag(b, phase);
    let mut oks = Vec::new();
    let mut conflicts = 0;
    let mut greatest = ZERO;
    loop {
        let m: Msg = recv_tagged_msg_block(move |_, t| t == Some(tag));
        match m {
            Msg::Conflict { seen, .. } => {
                conflicts += 1;
                greatest = greatest.max(seen);
                if conflicts > a - (f + 1) {
                    return Err(greatest);
                }
            }
            ok => {
                oks.push(ok);
                if oks.len() == f + 1 {
                    return Ok(oks);
                }
            }
        }
    }
}

fn proposer(id: usize, a: usize, k: usize, acceptors: Registry, clients: Registry) {
    let mut counter: u32 = 0;
    // the clients this proposer serves: ci with i mod P == id
    let mut pending: Vec<usize> = (0..k).filter(|i| i % P == id).collect();
    while !pending.is_empty() {
        let i = pending.remove(choose(pending.len()));
        let tag = i as u32;
        let Msg::Cas { expected, value } = recv_tagged_msg_block::<_, Msg>(move |_, t| t == Some(tag))
        else {
            panic!("proposer p{id} expected a Cas from c{i}");
        };
        let accs: Vec<ThreadId> = (0..a).map(|j| lookup(&acceptors, j)).collect();
        let mut reply = Msg::Fail {};
        for _attempt in 0..=R {
            counter += 1;
            let b = Ballot { counter, id: id as u32 };
            // phase 1: prepare
            for &t in &accs {
                send_msg(t, Msg::Prepare { ballot: b });
            }
            let promises = match await_quorum(b, 1, a) {
                Ok(p) => p,
                Err(seen) => {
                    counter = counter.max(seen.counter);
                    continue;
                }
            };
            let mut best: Option<(Ballot, i32)> = None;
            for p in promises {
                if let Msg::Promise { accepted: Some((ab, v)), .. } = p {
                    if best.map_or(true, |(bb, _)| ab > bb) {
                        best = Some((ab, v));
                    }
                }
            }
            let current = best.map(|(_, v)| v).unwrap_or(0); // 0 is the paper's empty value
            let new = if current == expected { value } else { current };
            // phase 2: accept
            for &t in &accs {
                send_msg(t, Msg::Accept { ballot: b, value: new });
            }
            match await_quorum(b, 2, a) {
                Ok(_) => {
                    reply = Msg::Done { value: new };
                    break;
                }
                Err(seen) => {
                    counter = counter.max(seen.counter);
                }
            }
        }
        send_msg(lookup(&clients, i), reply);
    }
    for j in 0..a {
        send_msg(lookup(&acceptors, j), Msg::Stop {});
    }
}

fn client(i: usize, proposer: ThreadId, me: Registry) {
    me.lock().unwrap()[i] = Some(thread::current_id());
    send_tagged_msg(proposer, i as u32, Msg::Cas { expected: i as i32, value: i as i32 + 1 });
    match recv_msg_block::<Msg>() {
        Msg::Done { .. } | Msg::Fail {} => {}
        m => panic!("client c{i} received an unexpected message {m:?}"),
    }
}

fn register(a: usize, k: usize) {
    assert!(a >= 1 && k <= REPLY_TAG_BASE as usize);
    let acceptors: Registry = Arc::new(Mutex::new(vec![None; a]));
    let clients: Registry = Arc::new(Mutex::new(vec![None; k]));
    let mut handles = Vec::new();
    let mut proposers = Vec::new();
    for id in 0..P {
        let (accs, cls) = (acceptors.clone(), clients.clone());
        let h = thread::Builder::new()
            .name(format!("p{id}"))
            .spawn(move || proposer(id, a, k, accs, cls))
            .unwrap();
        proposers.push(h.thread().id());
        handles.push(h);
    }
    for j in 0..a {
        let ps = proposers.clone();
        let h = thread::Builder::new()
            .name(format!("a{j}"))
            .spawn(move || acceptor(ps))
            .unwrap();
        acceptors.lock().unwrap()[j] = Some(h.thread().id());
        handles.push(h);
    }
    for i in 0..k {
        let (p, cls) = (proposers[i % P], clients.clone());
        let h = thread::Builder::new()
            .name(format!("c{i}"))
            .spawn(move || client(i, p, cls))
            .unwrap();
        handles.push(h);
    }
    for h in handles {
        h.join().unwrap();
    }
}

fn parse(args: &[String]) -> (usize, usize) {
    let a = args.first().and_then(|s| s.parse().ok()).unwrap_or(3);
    let k = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(2);
    (a, k)
}

fn visible(args: &[String]) -> Vec<String> {
    let (_, k) = parse(args);
    (0..k).map(|i| format!("c{i}")).collect()
}

fn entry(args: &[String]) -> Entry {
    let (a, k) = parse(args);
    Arc::new(move || register(a, k))
}

fn main() -> ExitCode {
    main_with_model(
        Program {
            visible: &visible,
            spec: &entry,
            imp: &entry,
        },
        ConsType::Bag,
    )
}
