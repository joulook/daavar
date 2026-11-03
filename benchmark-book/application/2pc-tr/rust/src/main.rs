//! Benchmark two-pc-trillium (candidate N7): the AnerisLang two-phase commit of Trillium
//! (`aneris/ml_sources/examples/transaction_commit/two_phase_code.ml`), whose refinement target
//! is the Coq `TC_model` (`aneris/examples/transaction_commit/tc_model.v`, TLA+ `TCommit`).
//!
//! Usage: `<bin> count|export|conf|online … --model M -- <N> [correct|ignore-abort-vote]`. `N` =
//! resource managers. Model `--model bag` (asyn: UDP; the default this program declares),
//! default variant `correct`. `count` prints
//! `execs=<complete> blocked=<blocked>` of plain Must; `conf` runs TraceForge's conformance check
//! of `impl <variant>` against `spec` and prints the verdict; `--engine enumerator`
//! selects the directed enumerator (which prints diagnostics) instead of the
//! default stateful engine.
//!
//! Visible threads: `obs0..obs{N-1}`: the observer of RM `i`'s final `TC_model` state, which
//! receives one `Outcome` (the RM's `COMMITTED` / `ABORTED`). Invisible: `tc` (spec oracle), `tm`
//! and `rm0..rm{N-1}` (impl).
//!
//! Plumbing: the observers and `tm` receive on their thread mailboxes; an RM's inbox is the
//! named channel `("rm", i)`, so that `tm` can reach an RM spawned after it. Each mailbox and
//! channel carries one message type.
use traceforge::channel::{self, Receiver, Sender};
use traceforge::thread::{self, JoinHandle, ThreadId};
use traceforge::*;

/// TM to RM: `"PREPARE" | "COMMIT" | "ABORT"`.
#[derive(Clone, Debug, PartialEq)]
enum ToRm {
    Prepare,
    Commit,
    Abort,
}

/// RM to TM: `"PREPARED" | "ABORTED" | "COMMITTED"`, with the sender (UDP `snd msg`), an RM index.
#[derive(Clone, Debug, PartialEq)]
enum ToTm {
    Prepared(usize),
    Aborted(usize),
    Committed(usize),
}

/// RM `i`'s final `TC_model` state, reported to its observer.
#[derive(Clone, Debug, PartialEq)]
enum Outcome {
    Committed,
    Aborted,
}

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn rm_inbox(i: usize) -> (Sender<ToRm>, Receiver<ToRm>) {
    channel::Builder::<ToRm>::new().with_name(("rm", i)).build()
}

/// `obs{i}`: one blocking receive of RM `i`'s outcome (identical on both sides).
fn spawn_observers(n: usize) -> (Vec<JoinHandle<()>>, Vec<ThreadId>) {
    let hs: Vec<JoinHandle<()>> = (0..n)
        .map(|i| named(&format!("obs{i}"), move || { let _o = recv_msg_block::<Outcome>(); }))
        .collect();
    let ids = hs.iter().map(|h| h.thread().id()).collect();
    (hs, ids)
}

/// The specification: `TC_model` with RM-local coin votes, as one oracle `tc`. It draws each RM's
/// `coin_flip ()` (true = local abort), decides COMMITTED iff no RM aborted (`tc_decide_commit`
/// needs `canCommit`: every RM PREPARED), else ABORTED (`tc_decide_abort`), and reports the same
/// final state to every observer (`TCConsistent`).
fn spec(n: usize) {
    let (obs, oids) = spawn_observers(n);
    let tc = named("tc", move || {
        let mut all_prepared = true;
        for _ in 0..n {
            if nondet() {
                all_prepared = false;
            }
        }
        let o = if all_prepared { Outcome::Committed } else { Outcome::Aborted };
        for id in &oids {
            send_msg(*id, o.clone());
        }
    });
    for h in obs {
        h.join().unwrap();
    }
    tc.join().unwrap();
}

/// `resource_manager rm tm`, plus the report of its final state to `obs{i}`.
fn resource_manager(i: usize, tm: ThreadId, obs: ThreadId) {
    let inbox = rm_inbox(i).1;
    let value = inbox.recv_msg_block();
    if value == ToRm::Abort {
        // someone already aborted
        send_msg(tm, ToTm::Aborted(i));
        send_msg(obs, Outcome::Aborted);
    } else {
        let local_abort = nondet(); // coin_flip ()
        if local_abort {
            send_msg(tm, ToTm::Aborted(i));
            send_msg(obs, Outcome::Aborted);
        } else {
            send_msg(tm, ToTm::Prepared(i));
            // wait_receivefrom skt (COMMIT || ABORT): only those can arrive now
            let decision = inbox.recv_msg_block();
            if decision == ToRm::Commit {
                send_msg(tm, ToTm::Committed(i));
                send_msg(obs, Outcome::Committed);
            } else {
                send_msg(tm, ToTm::Aborted(i));
                send_msg(obs, Outcome::Aborted);
            }
        }
    }
}

/// `transaction_manager tm rms`. `ignore-abort-vote` drops the `(fst msg = "PREPARED") &&` test
/// of `recv_responses`, so every response, `"ABORTED"` included, adds its sender to `prepared`.
fn transaction_manager(n: usize, ignore_abort_vote: bool) {
    for i in 0..n {
        rm_inbox(i).0.send_msg(ToRm::Prepare); // sendto_all_set skt rms "PREPARE"
    }
    // recv_responses
    let mut prepared: Vec<usize> = Vec::new();
    let mut all_prepared = true;
    while prepared.len() != n {
        match recv_msg_block::<ToTm>() {
            ToTm::Prepared(j) => {
                if !prepared.contains(&j) {
                    prepared.push(j);
                }
            }
            ToTm::Aborted(j) if ignore_abort_vote => {
                if !prepared.contains(&j) {
                    prepared.push(j);
                }
            }
            _ => {
                all_prepared = false;
                break;
            }
        }
    }
    if all_prepared {
        for i in 0..n {
            rm_inbox(i).0.send_msg(ToRm::Commit);
        }
        // receivefrom_nodup_set skt recv rms; map_iter (assert (m = "COMMITTED"))
        let mut from: Vec<usize> = Vec::new();
        while from.len() != n {
            let m = recv_msg_block::<ToTm>();
            let j = match m {
                ToTm::Prepared(j) | ToTm::Aborted(j) | ToTm::Committed(j) => j,
            };
            if !from.contains(&j) {
                from.push(j);
                assert_eq!(m, ToTm::Committed(j), "assert (m = \"COMMITTED\")");
            }
        }
    } else {
        for i in 0..n {
            rm_inbox(i).0.send_msg(ToRm::Abort);
        }
    }
}

fn imp(n: usize, variant: &str) {
    let ignore_abort_vote = match variant {
        "correct" => false,
        "ignore-abort-vote" => true,
        v => panic!("unknown variant {v}; one of correct, ignore-abort-vote"),
    };
    let (obs, oids) = spawn_observers(n);
    let tm = named("tm", move || transaction_manager(n, ignore_abort_vote));
    let tid = tm.thread().id();
    let rms: Vec<JoinHandle<()>> = (0..n)
        .map(|i| { let o = oids[i]; named(&format!("rm{i}"), move || resource_manager(i, tid, o)) })
        .collect();
    for h in obs {
        h.join().unwrap();
    }
    for h in rms {
        h.join().unwrap();
    }
    tm.join().unwrap();
}

fn parse(a: &[String]) -> (usize, String) {
    let n: usize = a.first().and_then(|s| s.parse().ok()).unwrap_or(2);
    let variant = a.get(1).cloned().unwrap_or_else(|| "correct".to_string());
    (n, variant)
}

fn main() -> std::process::ExitCode {
    let program = traceforge::program::Program {
        visible: &|a| (0..parse(a).0).map(|i| format!("obs{i}")).collect(),
        spec: &|a| {
            let (n, _) = parse(a);
            std::sync::Arc::new(move || spec(n))
        },
        imp: &|a| {
            let (n, variant) = parse(a);
            std::sync::Arc::new(move || imp(n, &variant))
        },
    };
    traceforge::program::main_with_model(program, traceforge::ConsType::Bag)
}
