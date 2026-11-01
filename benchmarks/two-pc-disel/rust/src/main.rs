//! Benchmark two-pc-disel (candidate N8): Disel's multi-round two-phase commit with
//! acknowledgements (`disel/theories/Examples/TwoPhaseCommit/`: `TwoPhaseProtocol.v`,
//! `TwoPhaseCoordinator.v`, `TwoPhaseParticipant.v`, `SimpleTPCApp.v`).
//!
//! Usage: `<bin> count|export|conf|online … --model M -- <N> [correct|skip-ack-loop]`. `N` =
//! participants; the round count is `K = 2` (data `[1;2]`, `[3;4]`, the first two items of
//! `data_seq`). Model `--model bag` (asyn, Disel's message soup; the default this program
//! declares), default variant `correct`. `count` prints
//! `execs=<complete> blocked=<blocked>` of plain Must; `conf` runs TraceForge's conformance check
//! of `impl <variant>` against `spec` and prints the verdict; `--engine enumerator`
//! selects the directed enumerator (which prints diagnostics) instead of the
//! default stateful engine.
//!
//! Visible threads: the participants `p0..p{N-1}`. Invisible: the coordinator `cn` (both sides).
//!
//! Plumbing: `cn` is spawned first and receives on its thread mailbox; a participant's inbox is
//! the named channel `("p", i)`, so that `cn` can reach a participant spawned after it. Disel's
//! tag-filtered `tryrecv` actions (`tryrecv_prep_resp`, `tryrecv_commit_ack`, ...) are tag-guarded
//! blocking receives (a `tryrecv` spin loop until the message arrives is a blocking receive).
use traceforge::channel::{self, Receiver, Sender};
use traceforge::thread::{self, JoinHandle, ThreadId};
use traceforge::*;

/// Rounds.
const K: usize = 2;
/// `data_seq` of `SimpleTPCApp.v`, first `K` items.
const DATA: [[u8; 2]; 2] = [[1, 2], [3, 4]];

/// Tags of `TwoPhaseProtocol.v` grouped as the receive filters group them.
const TAG_PREP: u32 = 0; // prep_req
const TAG_DEC: u32 = 3; // commit_req | abort_req
const TAG_VOTE: u32 = 1; // prep_yes | prep_no
const TAG_ACK: u32 = 5; // commit_ack | abort_ack

/// Coordinator to participant.
#[derive(Clone, Debug, PartialEq)]
enum ToP {
    /// `prep_req`, contents `e :: data`.
    Prep(usize, [u8; 2]),
    /// `commit_req` / `abort_req`, contents `[e]`.
    Commit(usize),
    Abort(usize),
}

/// Participant to coordinator: the sender (Disel's `from`, a participant index) and the round.
#[derive(Clone, Debug, PartialEq)]
enum ToCn {
    /// `prep_yes` / `prep_no`.
    Yes(usize, usize),
    No(usize, usize),
    /// `commit_ack` / `abort_ack`.
    CommitAck(usize, usize),
    AbortAck(usize, usize),
}

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn p_inbox(i: usize) -> (Sender<ToP>, Receiver<ToP>) {
    channel::Builder::<ToP>::new().with_name(("p", i)).build()
}

/// `participant_with_choices`, one round per data item, the vote `doCommit` a free choice (the
/// proof's free `bool`; the app's `choice_seq` lists are instances of it).
fn participant(i: usize, cn: ThreadId) {
    let inbox = p_inbox(i).1;
    for e in 0..K {
        // receive_prep_req_loop e. The source discards a prep_req of another round
        // (`head 0 body == e`); none can arrive (a Prep(e+1) needs this participant's round-e
        // vote, and in `correct` its ack), so the filter is one receive and an assertion.
        let ToP::Prep(e2, _d) = inbox.recv_tagged_msg_block(|t| t == Some(TAG_PREP)) else {
            unreachable!()
        };
        assert_eq!(e2, e, "prep_req of another round");
        // resp_to_req e doCommit
        let do_commit = nondet();
        if do_commit {
            send_tagged_msg(cn, TAG_VOTE, ToCn::Yes(i, e));
        } else {
            send_tagged_msg(cn, TAG_VOTE, ToCn::No(i, e));
        }
        // receive_commabrt_loop e doCommit: likewise one receive (a round-(e+1) decision needs
        // this participant's round-(e+1) vote).
        let committed = match inbox.recv_tagged_msg_block(|t| t == Some(TAG_DEC)) {
            ToP::Commit(e2) => { assert_eq!(e2, e, "commit_req of another round"); true }
            ToP::Abort(e2) => { assert_eq!(e2, e, "abort_req of another round"); false }
            ToP::Prep(..) => unreachable!(),
        };
        // send_ack e hasCommitted
        if committed {
            send_tagged_msg(cn, TAG_ACK, ToCn::CommitAck(i, e));
        } else {
            send_tagged_msg(cn, TAG_ACK, ToCn::AbortAck(i, e));
        }
    }
}

fn send_to_all(n: usize, tag: u32, m: ToP) {
    for i in 0..n {
        p_inbox(i).0.send_tagged_msg(tag, m.clone());
    }
}

/// `coordinator_loop` over the first `K` data items; each round is `coordinator_round d`:
/// `send_prep_loop; receive_prep_loop; read_resp_result; send_commits|send_aborts;
/// receive_commit_loop|receive_abort_loop`. The specification (`free_abort`) is the same
/// round structure with the TCommit decision rule: commit only if every vote is yes, abort
/// always allowed. `skip_acks` is fault F1: the ack loops are skipped, the next round starts
/// at once.
fn coordinator(n: usize, free_abort: bool, skip_acks: bool) {
    for e in 0..K {
        send_to_all(n, TAG_PREP, ToP::Prep(e, DATA[e]));
        // receive_prep_loop e: one response per participant of this round. The source skips a
        // response of another round or a duplicate sender; neither can arrive (each participant
        // votes once per round, and only after this round's prep_req), so: N receives and an
        // assertion.
        let mut acc: Vec<(usize, bool)> = Vec::new();
        for _ in 0..n {
            let (from, e2, yes) = match recv_tagged_msg_block::<_, ToCn>(|_, t| t == Some(TAG_VOTE)) {
                ToCn::Yes(f, e2) => (f, e2, true),
                ToCn::No(f, e2) => (f, e2, false),
                _ => unreachable!(),
            };
            assert!(e2 == e && !acc.iter().any(|(f, _)| *f == from), "vote of another round or duplicate");
            acc.push((from, yes));
        }
        // read_resp_result: all yes
        let all_yes = acc.iter().all(|(_, y)| *y);
        let commit = if free_abort { all_yes && nondet() } else { all_yes };
        if commit {
            send_to_all(n, TAG_DEC, ToP::Commit(e));
        } else {
            send_to_all(n, TAG_DEC, ToP::Abort(e));
        }
        if skip_acks {
            continue;
        }
        // receive_commit_loop e | receive_abort_loop e: N receives (each participant acks once
        // per round, the kind matching the decision); the source's filter is an assertion.
        let mut acks: Vec<usize> = Vec::new();
        for _ in 0..n {
            let (from, e2, ok) = match recv_tagged_msg_block::<_, ToCn>(|_, t| t == Some(TAG_ACK)) {
                ToCn::CommitAck(f, e2) => (f, e2, commit),
                ToCn::AbortAck(f, e2) => (f, e2, !commit),
                _ => unreachable!(),
            };
            assert!(ok && e2 == e && !acks.contains(&from), "ack of another round, kind or duplicate");
            acks.push(from);
        }
    }
}

fn run(n: usize, free_abort: bool, skip_acks: bool) {
    let cn = named("cn", move || coordinator(n, free_abort, skip_acks));
    let cid = cn.thread().id();
    let ps: Vec<JoinHandle<()>> = (0..n).map(|i| named(&format!("p{i}"), move || participant(i, cid))).collect();
    for h in ps {
        h.join().unwrap();
    }
    cn.join().unwrap();
}

fn spec(n: usize) {
    run(n, true, false)
}

fn imp(n: usize, variant: &str) {
    let skip = match variant {
        "correct" => false,
        "skip-ack-loop" => true,
        v => panic!("unknown variant {v}; one of correct, skip-ack-loop"),
    };
    run(n, false, skip)
}

fn parse(a: &[String]) -> (usize, String) {
    let n: usize = a.first().and_then(|s| s.parse().ok()).unwrap_or(2);
    let variant = a.get(1).cloned().unwrap_or_else(|| "correct".to_string());
    (n, variant)
}

fn main() -> std::process::ExitCode {
    let program = traceforge::program::Program {
        visible: &|a| (0..parse(a).0).map(|i| format!("p{i}")).collect(),
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
