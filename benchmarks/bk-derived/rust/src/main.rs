//! BookKeeper ledger replication with fencing (master): the derived specification.
//!
//! One ledger, one writer `w1` (Add 1..e, each answered Acked or Rejected), one recovery
//! client `w2` (Recover, answered Closed(last)). Three bookies, Qw = 3, Qa = 2, no ensemble
//! change, every message delivered, no timeouts. The specification is one object, `ledger`,
//! with the two clients attached directly to it; bookies, quorums, fencing flags and
//! metadata versions are not data here, only their visible consequences (see NOTES.md).
//!
//! Usage: `bench_bk_derived count spec|impl [--model fifo|bag] -- <e> [<fifo|bag>]`.
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

#[derive(Clone, Debug, PartialEq)]
enum Req {
    Add(u64),
    Recover,
} // w1 -> w1h: Add(entry); w2 -> w2h: Recover
#[derive(Clone, Debug, PartialEq)]
enum Rep {
    Acked(u64),
    Rejected(u64),
    Closed(i64),
} // handle -> client

/// The invisible setup message from `init`: the addresses of the two clients.
#[derive(Clone, Debug, PartialEq)]
struct Setup {
    w1: ThreadId,
    w2: ThreadId,
}

const SETUP_TAG: u32 = 1;

// ---- the client code, verbatim from INTERFACE.md ----
fn writer(e: u64, h: ThreadId) {
    for i in 1..=e { send_msg(h, Req::Add(i)); match recv_msg_block::<Rep>() { Rep::Acked(_) | Rep::Rejected(_) => {}, _ => panic!("Acked or Rejected expected") } }
}
fn recoverer(h: ThreadId) {
    send_msg(h, Req::Recover); let Rep::Closed(_) = recv_msg_block::<Rep>() else { panic!("Closed expected") };
}

/// A choice among `n >= 1` options, one `nondet()` per option but the last.
fn choose(n: usize) -> usize {
    for i in 0..n - 1 {
        if nondet() {
            return i;
        }
    }
    n - 1
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Step {
    /// Receive the next client request (w1's next Add or w2's Recover).
    Recv,
    /// w1's pending entry reached the ack quorum (all lower entries are acked): Acked.
    Ack,
    /// A Fenced answer reached w1's handle: the pending entry is Rejected, the handle closed.
    Reject,
    /// The recovery reads entry `x` and finds it (some bookie has it): written back.
    ReadPos,
    /// The recovery reads entry `x` and gets NoSuchEntry from 2 bookies: close at `x - 1`.
    ReadNeg,
}

/// The specification object: the ledger as the two clients can observe it.
fn ledger(e: u64) {
    let Setup { w1, w2 } =
        recv_tagged_msg_block::<_, Setup>(|_, t| t == Some(SETUP_TAG));

    // w1's side
    let mut acked: u64 = 0; // w1 got Acked(1..=acked)
    let mut replied: u64 = 0; // w1's Adds answered so far
    let mut pend: Option<u64> = None; // w1's Add being served (sent to the bookies)
    let mut may_ack = false; // the pending entry can still reach the ack quorum
    let mut handle_closed = false; // w1's handle got Fenced: every later Add is Rejected
    let mut issued: u64 = 0; // highest entry w1's handle sent to the bookies
    // w2's side
    let mut recover = false; // Recover received (the ledger is InRecovery, fencing sent)
    let mut covered = false; // fencing reached one bookie of every ack quorum
    let mut recovered: u64 = 0; // entries 1..=recovered read and written back
    let mut closed = false;

    loop {
        // w1 has an Add on its way exactly when its last request was answered and it has more.
        let w1_due = pend.is_none() && replied < e;
        let recv_due = w1_due || !recover;
        let x = recovered.max(acked) + 1; // the entry the recovery reads next
        let mut steps: Vec<Step> = Vec::new();
        if recv_due {
            steps.push(Step::Recv);
        }
        if pend.is_some() && may_ack {
            steps.push(Step::Ack);
        }
        if pend.is_some() && recover {
            steps.push(Step::Reject);
        }
        if recover && !closed {
            if x <= issued {
                steps.push(Step::ReadPos);
            }
            steps.push(Step::ReadNeg);
        }
        if steps.is_empty() {
            // every request served: the object ends (the clients never wait)
            assert(replied == e && closed);
            return;
        }
        match steps[choose(steps.len())] {
            Step::Recv => match recv_msg_block::<Req>() {
                Req::Add(i) => {
                    if handle_closed {
                        send_msg(w1, Rep::Rejected(i));
                        replied += 1;
                    } else {
                        pend = Some(i);
                        issued = i;
                        // sent after fencing coverage: at most one bookie can store it
                        may_ack = !covered;
                    }
                }
                Req::Recover => recover = true,
            },
            Step::Ack => {
                let i = pend.take().unwrap();
                send_msg(w1, Rep::Acked(i));
                acked = i;
                replied += 1;
            }
            Step::Reject => {
                let i = pend.take().unwrap();
                send_msg(w1, Rep::Rejected(i));
                handle_closed = true;
                replied += 1;
            }
            Step::ReadPos => {
                covered = true;
                recovered = x;
            }
            Step::ReadNeg => {
                covered = true;
                if pend == Some(x) {
                    // the bookies that answered NoSuchEntry are fenced: x never reaches Qa
                    may_ack = false;
                }
                let last: i64 = if x == 1 { -1 } else { (x - 1) as i64 };
                send_msg(w2, Rep::Closed(last));
                closed = true;
            }
        }
    }
}

fn spec_prog(e: u64) {
    let obj = thread::Builder::new()
        .name("ledger".to_string())
        .spawn(move || ledger(e))
        .unwrap();
    let h = obj.thread().id();
    let c1 = thread::Builder::new()
        .name("w1".to_string())
        .spawn(move || writer(e, h))
        .unwrap();
    let c2 = thread::Builder::new()
        .name("w2".to_string())
        .spawn(move || recoverer(h))
        .unwrap();
    let setup = Setup { w1: c1.thread().id(), w2: c2.thread().id() };
    let init = thread::Builder::new()
        .name("init".to_string())
        .spawn(move || send_tagged_msg(h, SETUP_TAG, setup))
        .unwrap();
    init.join().unwrap();
    c1.join().unwrap();
    c2.join().unwrap();
    obj.join().unwrap();
}

fn size_of(args: &[String]) -> u64 {
    let e: u64 = args.first().and_then(|s| s.parse().ok()).unwrap_or(1);
    assert!(e >= 1, "e >= 1");
    e
}

fn main() -> ExitCode {
    let visible = |_: &[String]| vec!["w1".to_string(), "w2".to_string()];
    let spec = |a: &[String]| -> Entry {
        let e = size_of(a);
        Arc::new(move || spec_prog(e))
    };
    // Both entries return the specification program.
    let imp = |a: &[String]| -> Entry {
        let e = size_of(a);
        Arc::new(move || spec_prog(e))
    };
    traceforge::program::main(Program { visible: &visible, spec: &spec, imp: &imp })
}
