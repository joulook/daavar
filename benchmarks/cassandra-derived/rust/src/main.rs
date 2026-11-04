//! Cassandra LWT (Paxos v1) on one partition, register `x`: the client-observable
//! specification, derived from `service/paxos/Paxos.md` (see NOTES.md).
//!
//! One invisible object thread `lwt` holds the decided register `x` and serves both
//! clients; it models no replica, ballot, quorum or replica message. Every freedom of
//! the scheme is a `nondet()` choice; everything else is the arrival order of requests.
//!
//! Usage (through `traceforge::program`): `<bin> count spec|impl [--model fifo|bag] -- <q> <v1|v1nolin> [fifo|bag]`.
use std::sync::Arc;
use std::process::ExitCode;

use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ---- message types, verbatim from INTERFACE.md ----
#[derive(Clone, Debug, PartialEq)] enum Req { Cas(usize, u64, u64), Read(usize) }                    // client -> replica: (client, expected, value) / (client)
#[derive(Clone, Debug, PartialEq)] enum Rep { CasResult(usize, u64, u64), Value(usize, u64), Failed(usize) }  // replica -> client: (client, applied 0/1, value) / (client, value) / (client)

/// Setup sent by the invisible `init` thread (INTERFACE.md); `nodes` holds the object.
#[derive(Clone, Debug, PartialEq)]
struct Setup {
    nodes: Vec<ThreadId>,
    clients: Vec<ThreadId>,
}

const SETUP_TAG: u32 = 1;

// ---- client code, verbatim from INTERFACE.md ----
const CLIENTS: usize = 2;
fn value(i: usize, j: usize) -> u64 { (1 + i + CLIENTS * j) as u64 }
fn client(i: usize, q: usize, node: ThreadId) {
    let mut known: u64 = 0;
    for j in 0..q {
        if (i + j) % 2 == 0 { send_msg(node, Req::Cas(i, known, value(i, j))); } else { send_msg(node, Req::Read(i)); }
        match recv_msg_block::<Rep>() {
            Rep::CasResult(_, _, v) => known = v,
            Rep::Value(_, v) => known = v,
            Rep::Failed(_) => {}
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Variant {
    /// `PaxosVariant.v1`: reads and rejected CAS propose an empty update.
    V1,
    /// `v1_without_linearizable_reads_or_rejected_writes`: they propose nothing.
    V1NoLin,
}

/// The abstract state of the partition as its clients can observe it.
struct Lwt {
    variant: Variant,
    /// The value of the last decided non-empty proposal (0: row absent).
    x: u64,
    /// Updates `x := v` of CAS operations that were not decided but whose proposal may
    /// still be completed by a later coordinator (accepted by a minority). Each was
    /// computed against the current `x` (any later decided write removes them all).
    pending: Vec<u64>,
    clients: Vec<ThreadId>,
}

impl Lwt {
    /// 4(i): a coordinator that finds an in-progress (non-empty, uncommitted) proposal
    /// completes it before its own operation. At most one pending update is decided;
    /// its decision supersedes every other (Proposition 1).
    fn maybe_complete_in_progress(&mut self) {
        for k in 0..self.pending.len() {
            if nondet() {
                let v = self.pending[k];
                self.write(v);
                return;
            }
        }
    }

    /// A decided non-empty proposal: every earlier undecided proposal is dead.
    fn write(&mut self, v: u64) {
        self.x = v;
        self.pending.clear();
    }

    /// A decided empty proposal (read or rejected CAS). In v1 it supersedes every
    /// earlier undecided proposal; in v1nolin no proposal is made, so nothing changes.
    fn empty_decision(&mut self) {
        if self.variant == Variant::V1 {
            self.pending.clear();
        }
    }

    fn reply(&self, c: usize, r: Rep) {
        send_msg(self.clients[c], r);
    }

    /// One request, taken at its arrival. Returns `Some(expected)` when the CAS is
    /// applied now and its reply deferred.
    fn serve(&mut self, r: Req) -> Option<(usize, u64)> {
        self.maybe_complete_in_progress();
        let c = match r { Req::Cas(c, _, _) | Req::Read(c) => c };
        if nondet() {
            // the coordinator gives up after A attempts (unknown outcome)
            if let Req::Cas(_, expected, v) = r {
                if self.x == expected {
                    // its proposal may have reached a minority: it may be decided later
                    self.pending.push(v);
                }
            }
            self.reply(c, Rep::Failed(c));
            return None;
        }
        match r {
            Req::Read(_) => {
                let x = self.x;
                self.empty_decision();
                self.reply(c, Rep::Value(c, x));
                None
            }
            Req::Cas(_, expected, v) => {
                if self.x == expected {
                    self.write(v);
                    if nondet() {
                        // decided by a reproposal (own retry or another coordinator):
                        // the coordinator restarts and its final attempt reads later
                        Some((c, expected))
                    } else {
                        self.reply(c, Rep::CasResult(c, 1, v));
                        None
                    }
                } else {
                    let x = self.x;
                    self.empty_decision();
                    self.reply(c, Rep::CasResult(c, 0, x));
                    None
                }
            }
        }
    }

    /// The final attempt of a CAS whose update was already decided: the condition
    /// no longer holds (values are unique), so it is rejected with the current value.
    fn finish(&mut self, c: usize, expected: u64) {
        self.maybe_complete_in_progress();
        let x = self.x;
        debug_assert!(x != expected);
        self.empty_decision();
        self.reply(c, Rep::CasResult(c, 0, x));
    }
}

fn object(q: usize, variant: Variant) {
    // a request that arrives before Setup stays in the mailbox (buffered)
    let setup: Setup = recv_tagged_msg_block(|_, t| t == Some(SETUP_TAG));
    let mut s = Lwt { variant, x: 0, pending: Vec::new(), clients: setup.clients };
    let mut remaining = vec![q; CLIENTS];
    let mut deferred: Vec<(usize, u64)> = Vec::new();
    let mut served = 0;
    while served < CLIENTS * q {
        let receivable = (0..CLIENTS)
            .any(|c| remaining[c] > 0 && !deferred.iter().any(|&(d, _)| d == c));
        if !deferred.is_empty() && (!receivable || nondet()) {
            let k = if deferred.len() > 1 && nondet() { 1 } else { 0 };
            let (c, expected) = deferred.remove(k);
            s.finish(c, expected);
            served += 1;
            continue;
        }
        let r: Req = recv_msg_block();
        let c = match r { Req::Cas(c, _, _) | Req::Read(c) => c };
        remaining[c] -= 1;
        match s.serve(r) {
            Some(d) => deferred.push(d),
            None => served += 1,
        }
    }
}

fn spec(q: usize, variant: Variant) {
    let obj = thread::Builder::new()
        .name("lwt".to_string())
        .spawn(move || object(q, variant))
        .unwrap();
    let oid = obj.thread().id();
    let mut cs = Vec::new();
    for i in 0..CLIENTS {
        cs.push(
            thread::Builder::new()
                .name(format!("c{i}"))
                .spawn(move || client(i, q, oid))
                .unwrap(),
        );
    }
    let cids: Vec<ThreadId> = cs.iter().map(|h| h.thread().id()).collect();
    let init = thread::Builder::new()
        .name("init".to_string())
        .spawn(move || {
            send_tagged_msg(oid, SETUP_TAG, Setup { nodes: vec![oid], clients: cids });
        })
        .unwrap();
    init.join().unwrap();
    for h in cs {
        h.join().unwrap();
    }
    obj.join().unwrap();
}

fn parse(args: &[String]) -> (usize, Variant) {
    let q: usize = args.first().and_then(|s| s.parse().ok()).expect("<q> >= 1");
    assert!(q >= 1, "<q> >= 1");
    let variant = match args.get(1).map(String::as_str) {
        Some("v1") => Variant::V1,
        Some("v1nolin") => Variant::V1NoLin,
        other => panic!("variant v1 or v1nolin, got {other:?}"),
    };
    (q, variant)
}

fn entry(args: &[String]) -> Entry {
    let (q, variant) = parse(args);
    Arc::new(move || spec(q, variant))
}

fn visible(_args: &[String]) -> Vec<String> {
    (0..CLIENTS).map(|i| format!("c{i}")).collect()
}

fn main() -> ExitCode {
    traceforge::program::main(Program { visible: &visible, spec: &entry, imp: &entry })
}
