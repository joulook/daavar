//! Benchmark two-pc-lbinria (candidate N1): the Java two-phase commit of Cirstea et al.
//! (`lbinria-twophase/`, `TransactionManager.java`, `ResourceManager.java`, spec `TwoPhase.tla`).
//!
//! Usage: `<spec|impl|conf> <N> [fifo|bag|causal] [correct|counting-bug]`. `N` = resource
//! managers; the resend bound is `R = 2`. Default model `bag` (asyn: the paper runs the mailbox
//! server with `"unordered"` boxes), default variant `correct`. `spec`/`impl` print
//! `execs=<complete> blocked=<blocked>` of plain Must; `conf` runs TraceForge's conformance check
//! of `impl <variant>` against `spec` and prints the verdict; a fifth argument
//! `enumerator` selects the directed enumerator (which prints diagnostics) instead of the
//! default stateful engine; an argument `all` keeps every report (`ConfBuilder::all_reports`).
//!
//! Visible threads: `rm0..rm{N-1}`. Invisible: `tm` (both sides), `p0..p{N-1}` (impl only, the
//! retransmitting transport of each RM).
//!
//! Plumbing: servers are spawned first and receive on their thread mailbox; an RM's decision
//! inbox is the named channel `("rm", i)`, so that `tm` can reach an RM spawned after it (main
//! performs no communication). Each mailbox and channel carries one message type.
use traceforge::channel::{self, Receiver, Sender};
use traceforge::conformance::{self, ConfBuilder, Engine};
use traceforge::thread::{self, JoinHandle, ThreadId};
use traceforge::*;

/// Resends per RM: the proxy forwards `Prepared` `1..=R` times.
const R: usize = 2;

/// `TwoPhaseMessage.Prepared`, with the sender (`Message.from`), an RM index.
#[derive(Clone, Debug, PartialEq)]
struct Prepared(usize);

/// `TwoPhaseMessage.Commit` / `TwoPhaseMessage.Abort`, TM to RM.
#[derive(Clone, Debug, PartialEq)]
enum Decision {
    Commit,
    Abort,
}

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn rm_inbox(i: usize) -> (Sender<Decision>, Receiver<Decision>) {
    channel::Builder::<Decision>::new().with_name(("rm", i)).build()
}

fn send_all(n: usize, d: Decision) {
    for i in 0..n {
        rm_inbox(i).0.send_msg(d.clone());
    }
}

/// `ResourceManager.run`, identical on both sides: crash before preparing (the source's
/// `Helper.next(PROBABILITY_TO_ABORT) == 1` return), or send `Prepared` once and wait for the
/// decision. The resends are not the RM's events here: they live in the invisible proxy (rule 3).
fn rm(i: usize, server: ThreadId) {
    if nondet() {
        return; // crash: no message at all
    }
    send_msg(server, Prepared(i));
    let _d: Decision = rm_inbox(i).1.recv_msg_block(); // RMRcvCommitMsg / RMRcvAbortMsg
}

fn spawn_rms(n: usize, server: ThreadId) -> Vec<JoinHandle<()>> {
    (0..n).map(|i| named(&format!("rm{i}"), move || rm(i, server))).collect()
}

/// The specification: `TwoPhase.tla` with one TM oracle. `TMAbort` is enabled whenever the TM is
/// in `init`, so before each receive the oracle may abort; it receives each RM's `Prepared` once
/// (`TMRcvPrepared`), and once all `N` arrived it commits (`TMCommit`, `tmPrepared = RM`) or still
/// aborts. Commit therefore happens iff every RM's `Prepared` was received, each once.
fn spec(n: usize) {
    let tm = named("tm", move || {
        for _ in 0..n {
            if nondet() {
                send_all(n, Decision::Abort);
                return;
            }
            let Prepared(_) = recv_msg_block::<Prepared>();
        }
        let d = if nondet() { Decision::Commit } else { Decision::Abort };
        send_all(n, d);
    });
    let tid = tm.thread().id();
    let rms = spawn_rms(n, tid);
    for h in rms {
        h.join().unwrap();
    }
    tm.join().unwrap();
}

/// `TransactionManager.run`: before every receive the abort timeout may fire (`nondet()`); a
/// received `Prepared(from)` is added to `preparedRMs`; commit once `preparedRMs.size() >= N`.
/// `correct`: `preparedRMs` is a `HashSet` (the source as shipped). `counting-bug`: an
/// `ArrayList` (the commented-out line), so a resent `Prepared` is counted twice.
fn tm(n: usize, counting_bug: bool) {
    let mut prepared: Vec<usize> = Vec::new();
    for _ in 0..n * R {
        if nondet() {
            send_all(n, Decision::Abort); // TMAbort (timeout)
            return;
        }
        let Prepared(from) = recv_msg_block::<Prepared>();
        if counting_bug || !prepared.contains(&from) {
            prepared.push(from);
        }
        if prepared.len() >= n {
            send_all(n, Decision::Commit); // TMCommit
            return;
        }
    }
}

/// The transport of RM `i` (the RM's resend loop, `sendPrepared` on every receive timeout):
/// receive the RM's one `Prepared` and forward it `1..=R` times. A crashed RM's proxy forwards
/// nothing (it ends blocked; it is invisible).
fn proxy(tm: ThreadId) {
    let m = recv_msg_block::<Prepared>();
    let k = (1..=R).nondet();
    for _ in 0..k {
        send_msg(tm, m.clone());
    }
}

fn imp(n: usize, variant: &str) {
    let counting_bug = match variant {
        "correct" => false,
        "counting-bug" => true,
        v => panic!("unknown variant {v}; one of correct, counting-bug"),
    };
    let t = named("tm", move || tm(n, counting_bug));
    let tid = t.thread().id();
    let ps: Vec<JoinHandle<()>> = (0..n).map(|i| named(&format!("p{i}"), move || proxy(tid))).collect();
    let pids: Vec<ThreadId> = ps.iter().map(|h| h.thread().id()).collect();
    let rms: Vec<JoinHandle<()>> =
        (0..n).map(|i| { let p = pids[i]; named(&format!("rm{i}"), move || rm(i, p)) }).collect();
    for h in rms {
        h.join().unwrap();
    }
    for h in ps {
        h.join().unwrap();
    }
    t.join().unwrap();
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let side = a.get(1).map(String::as_str).unwrap_or("spec").to_string();
    let n: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(2);
    let model = match a.get(3).map(String::as_str).unwrap_or("bag") {
        "fifo" => ConsType::FIFO,
        "causal" => ConsType::Causal,
        _ => ConsType::Bag,
    };
    let variant = a.get(4).cloned().unwrap_or_else(|| "correct".to_string());
    if side == "conf" {
        let visible: Vec<String> = (0..n).map(|i| format!("rm{i}")).collect();
        let cfg = ConfBuilder::new()
            .config(Config::builder().with_cons_type(model).with_verbose(0).build())
            .cons_type(model)
            .visible_threads(visible)
            .engine(match a.get(5).map(String::as_str).unwrap_or("stateful") {
                "enumerator" => Engine::Enumerator,
                _ => Engine::Stateful,
            })
            .all_reports(a.iter().any(|x| x == "all"))
            .build()
            .expect("conformance config");
        let v = conformance::verify(cfg, move || imp(n, &variant), move || spec(n)).expect("conformance run");
        println!("{v}");
        return;
    }
    let conf = Config::builder().with_cons_type(model).with_verbose(0).build();
    let stats = match side.as_str() {
        "impl" => verify(conf, move || imp(n, &variant)),
        _ => verify(conf, move || spec(n)),
    };
    println!("execs={} blocked={}", stats.execs, stats.block);
}
