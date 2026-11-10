//! Specification program of Neon's safekeeper consensus, derived from
//! `safekeeper/spec/ProposerAcceptorStatic.tla` (PAS) at fa50421, against the
//! interface `plan/case-studies/neon/INTERFACE-agents.md`.
//!
//! Visible: the writers `w0..w{p-1}` (verbatim writer code). Invisible: `lg`,
//! one object holding the log (entries = records, tagged with the term that
//! wrote them), the committed length and the current leader; `init`, which
//! hands `lg` the writers' thread ids. Acceptors, votes, term histories and
//! LSNs are not data: only their consequences for the log are.
//!
//! `lg` works in two steps. (1) The abstract run: every freedom of the
//! protocol is a `nondet()` choice (elections, donor prefixes, commits); it
//! fixes the log and every writer's replies. (2) The communication: `lg`
//! receives an Append only when a reply needs its record, i.e. just before the
//! first `Committed(k)` whose log prefix 1..k holds it, and sends every reply
//! as early as that allows. So a reply is causally after exactly the Appends
//! of the records it depends on (positions 1..k for `Committed(k)`; none from
//! other writers for `Ended`), never after an unrelated writer's Append.
//!
//! Arguments: `<variant> <n> <p> <r>`; the variant is ignored, `n` is checked
//! to be a number and not otherwise read.
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ---- Interface section 4, verbatim ----
#[derive(Clone, Debug, PartialEq)] enum WReq { Append(u8) }
#[derive(Clone, Debug, PartialEq)] enum WRep { Committed(u8), Ended }
fn value(i: u8, j: u8, p: u8) -> u8 { 1 + i + p * j }

// ---- Interface section 5.1, verbatim ----
fn writer(i: u8, p: u8, r: u8, prop: ThreadId) {
    for j in 0..r {
        send_msg(prop, WReq::Append(value(i, j, p)));
    }
    for _ in 0..r {
        let _: WRep = recv_msg_block::<WRep>();
    }
}

/// Invisible setup message from `init` to `lg`: the writers' thread ids.
#[derive(Clone, Debug, PartialEq)]
struct Setup(Vec<ThreadId>);
const TAG_SETUP: u32 = 1;

/// One log entry: record j of writer w, and the term of the leader that wrote
/// it (PAS's wal entries are terms, PAS l.355-362; (w, j) identifies the record).
#[derive(Clone, Copy, Debug, PartialEq)]
struct Ent {
    w: u8,
    j: u8,
    term: u8,
}

/// The one invisible object: log, committed length, current leader.
fn log_object(p: u8, r: u8) {
    let Setup(w) = recv_tagged_msg_block::<_, Setup>(|_, t| t == Some(TAG_SETUP));
    assert_eq!(w.len(), p as usize);

    // ---------------- (1) the abstract run ----------------
    let mut log: Vec<Ent> = Vec::new(); // the newest leader's log
    let mut c: usize = 0; // committed length (PAS `committed`, as a prefix)
    let mut committed: Vec<Ent> = Vec::new(); // entries at committed positions 1..c
    let mut remaining: Vec<u8> = (0..p).collect(); // proposers that have not led
    let mut term: u8 = 0; // term of the current leader (0: none yet, PAS InitAcc)
    let mut elected_terms: Vec<u8> = Vec::new(); // PAS elected_history
    // Per writer: how many of its records it is told are committed, and at
    // which position the first one is (its leader's term start + 1).
    let mut ncommit: Vec<usize> = vec![0; p as usize];
    let mut first_pos: Vec<usize> = vec![0; p as usize];

    loop {
        if remaining.is_empty() {
            break;
        }
        // Next election: one of the proposers that has not led, or none ever
        // again (R3: a proposer may fail to become leader).
        let ch = (0..remaining.len() + 1).nondet();
        if ch == remaining.len() {
            break;
        }
        let i = remaining.remove(ch);
        // BecomeLeader (PAS l.269-306, Vote l.245-253). Abstract terms are the
        // order of the abstract elections; the previous abstract leader can no
        // longer commit (see NOTES, observational normalization).
        term += 1;
        traceforge::assert(!elected_terms.contains(&term)); // ElectionSafety(Full)
        elected_terms.push(term);
        // The donor's log: a prefix of the newest log that holds every
        // committed entry (normalization of MaxVoteAcc/TruncateWal, NOTES).
        let m = (c..log.len() + 1).nondet();
        log.truncate(m);
        // LogSafety: the committed prefix survives in the new leader's log.
        traceforge::assert(log.len() >= c && log[..c] == committed[..]);
        // NewEntry (PAS l.355-362): the writer's records, in its order, after
        // the term start position m (R1).
        let start = m;
        for j in 0..r {
            log.push(Ent { w: i, j, term });
        }
        // LogIsMonotonic (PAS l.463-466).
        traceforge::assert(log.windows(2).all(|x| x[0].term <= x[1].term));
        // CommitEntries (PAS l.381-430): commit a prefix that reaches the term
        // start (the prev-term rule, l.402-408); k = own records committed.
        let k = (0..r as usize + 1).nondet();
        if k > 0 {
            let nc = start + k;
            traceforge::assert(nc >= start && nc >= c); // prev-term rule, monotonic commit
            traceforge::assert(log[..c] == committed[..]); // LogSafety
            c = nc;
            committed = log[..c].to_vec();
        }
        ncommit[i as usize] = k;
        first_pos[i as usize] = start + 1;
        // R3: the term ends here; the rest of the writer's records get Ended.
    }

    // ---------------- (2) the communication ----------------
    // Replies of writer i: Committed(first_pos+j) for j < ncommit, then Ended.
    let mut received: Vec<u8> = vec![0; p as usize]; // Appends taken per writer
    let take = |i: u8, upto: u8, received: &mut Vec<u8>| {
        // Receive writer i's Appends up to and including record `upto`.
        let wi = w[i as usize];
        while received[i as usize] <= upto {
            let j = received[i as usize];
            let WReq::Append(x) =
                recv_tagged_msg_block::<_, WReq>(move |tid, t| tid == wi && t.is_none());
            traceforge::assert(x == value(i, j, p));
            received[i as usize] += 1;
        }
    };
    // Writers told nothing is committed: Ended for every record, at once
    // (R3/R4; an Ended depends on no other writer's record).
    for i in 0..p {
        if ncommit[i as usize] == 0 {
            for _ in 0..r {
                send_msg(w[i as usize], WRep::Ended);
            }
        }
    }
    // Committed replies in log order: before Committed(q), take the Appends of
    // the records at positions 1..q (R2: entry q's prefix exists only once its
    // records were appended).
    for q in 1..=c {
        let e = committed[q - 1];
        take(e.w, e.j, &mut received);
        let i = e.w as usize;
        let k = ncommit[i];
        if k > 0 && q >= first_pos[i] && q < first_pos[i] + k {
            traceforge::assert(q - first_pos[i] == e.j as usize); // own records, in order
            send_msg(w[i], WRep::Committed(q as u8));
            if q == first_pos[i] + k - 1 {
                // R3: the term ended after the last commit.
                for _ in k..r as usize {
                    send_msg(w[i], WRep::Ended);
                }
            }
        }
    }
    // Every reported commit lies inside the final committed prefix.
    for i in 0..p as usize {
        traceforge::assert(ncommit[i] == 0 || first_pos[i] + ncommit[i] - 1 <= c);
    }
    // Every message is delivered once: take the Appends no reply needed.
    for i in 0..p {
        if r > 0 {
            take(i, r - 1, &mut received);
        }
    }
}

struct Params {
    p: u8,
    r: u8,
}

fn params(args: &[String]) -> Params {
    if args.len() != 4 {
        panic!("arguments: <variant> <n> <p> <r>, got {args:?}");
    }
    // args[0]: the variant, accepted and ignored.
    // args[1]: n, checked to be a number (any width) and not otherwise read.
    let n = &args[1];
    assert!(
        !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()),
        "n must be a number, got {n:?}"
    );
    let p: u8 = args[2].parse().expect("p must be a number");
    let r: u8 = args[3].parse().expect("r must be a number");
    assert!(p >= 1 && r >= 1, "p >= 1 and r >= 1");
    assert!((p as u32) * (r as u32) <= 255, "values and positions must fit in u8");
    Params { p, r }
}

fn visible(args: &[String]) -> Vec<String> {
    let p = params(args).p;
    (0..p).map(|i| format!("w{i}")).collect()
}

fn run(p: u8, r: u8) {
    let lg = thread::Builder::new()
        .name("lg".to_string())
        .spawn(move || log_object(p, r))
        .unwrap();
    let lg_id = lg.thread().id();
    let mut ws = Vec::new();
    for i in 0..p {
        ws.push(
            thread::Builder::new()
                .name(format!("w{i}"))
                .spawn(move || writer(i, p, r, lg_id))
                .unwrap(),
        );
    }
    let wids: Vec<ThreadId> = ws.iter().map(|h| h.thread().id()).collect();
    let init = thread::Builder::new()
        .name("init".to_string())
        .spawn(move || send_tagged_msg(lg_id, TAG_SETUP, Setup(wids)))
        .unwrap();
    for h in ws {
        h.join().unwrap();
    }
    init.join().unwrap();
    lg.join().unwrap();
}

fn spec(args: &[String]) -> Entry {
    let Params { p, r } = params(args);
    Arc::new(move || run(p, r))
}

fn main() -> ExitCode {
    traceforge::program::main(Program {
        visible: &visible,
        spec: &spec,
        imp: &spec,
    })
}
