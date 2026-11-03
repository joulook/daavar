//! TraceForge's leader election + two-phase commit ("A5": `2pc/leader` and `2pc/ring`, ruling
//! Q11), as an ordinary Must program run by TraceForge's shared main (`traceforge::program`).
//!
//! Ported for H (`criteria/H.md` H-0.3; `log/dev/H.derived.md` §1.2–§1.4) from
//! `traceforge/src/conformance/demo.rs` at `a3a7e1b`, lines 601–739 and 744–872 (`EIn`, `PIn`,
//! `Setup`, `le_participant`, `le_announce`, `le_elector_with`, `le_oracle`, `le_impl`,
//! `le_impl_silent`, `le_impl_with`, `le_spec`, `le_visible`; the unused wrapper `le_elector`,
//! 740–743, is left out and its doc comment now heads `le_elector_with`) and 977–1105 (`RIn`,
//! `ring_elector`, `ring_impl`). The program code is copied unchanged apart from `crate::` →
//! `traceforge::` and the dropped `pub(super)`; `named` is `demo.rs:45-47`, `prog`/`Prog` are
//! `grid.rs:60-64` (the registry wraps each entry point in `prog`).
//!
//! ARGS: `leader {conf|split|silent} <n>` or `ring {conf|split} <n>`
//! * spec: `le_spec(n)` for every variant;
//! * impl: `leader conf` = `le_impl(n, false)`, `leader split` = `le_impl(n, true)`,
//!   `leader silent` = `le_impl_silent(n)`, `ring conf` = `ring_impl(n, false)`,
//!   `ring split` = `ring_impl(n, true)` (`grid.rs:1132-1225`, APPTAB `grid.rs:5659-5674`).
//!
//! Visible threads: `p0 .. p{n-1}` (`le_visible`).
//!
//! Fixture names: `2pc/leader/<v>/n<n>` → `election leader <v> <n>`;
//! `2pc/ring/<v>/n<n>` → `election ring <v> <n>`.
//!
//! Options every row spells (never set here; they are the shared main's `--model`, `--seed`,
//! `--threshold`): `--model fifo --threshold 65536 --seed 0` (`seeded(FIFO, None)` = `cfg(FIFO)`,
//! `grid.rs:88-107`), **except** the two recorded seeds of `2pc/ring/conf` (`grid.rs:1172-1176`):
//! * `2pc/ring/conf/n2`: `--seed 8694399807746060002`;
//! * `2pc/ring/conf/n3`: `--seed 1992948037369921880`;
//! * (`2pc/ring/conf/n4`, in no row: `--seed 13733943097982699720`.)
//!
//! Example: `<bin> row --model fifo --threshold 65536 --seed 1992948037369921880 -- ring conf 3`.

use std::sync::Arc;

use traceforge::{nondet, recv_msg_block, send_msg, thread};

type Prog = traceforge::program::Entry;

fn prog<F: Fn() + Send + Sync + 'static>(f: F) -> Prog {
    Arc::new(f)
}

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

// ===========================================================================
// Leader election + two-phase commit (2026-09-29, owner's design).
//
// **The abstraction is a whole sub-protocol.** Both sides run `n` participants
// and the *same* participant code. The only difference:
//
//   - the **specification** has one invisible oracle that picks the leader with
//     a free `nondet()` choice;
//   - the **implementation** has `n` invisible electors running a real
//     election — all-to-all exchange, highest index wins.
//
// The refinement question: *can the election produce an outcome the free choice
// could not?*
//
// **Why the election is on separate threads.** Visibility is declared **per
// thread** (`visible_threads`), not per event, so "the election is invisible"
// must be realised by running it on threads that are not declared visible. Were
// those messages exchanged between the participants, they would be visible
// observations and §6.1 would require the specification to reproduce them one
// for one — putting the election *into* the specification and abstracting
// nothing.
//
// **Why one message type.** `recv_msg_block::<T>()` type-checks the *next*
// message rather than filtering for `T`. A participant's inbound messages come
// from two sources — its invisible boss and its peers — so their arrival order
// is not fixed, and a first draft with separate `Role`/`Tpc` types panicked with
// "expecting Role but got Tpc" when a peer's message overtook the role. One
// enum makes every arrival order well typed, and the loop below tolerates them.
//
// **F41.** Participants are spawned **first** on both sides, so their
// `ThreadId`s agree. Every id travelling inside a message a participant
// observes is a *participant* id; no elector or oracle id is ever observed.
// ===========================================================================

/// Everything an **elector** can receive: its setup from main, and its peers'
/// election messages — in one type, for the same reason [`PIn`] is one type.
#[derive(Clone, Debug, PartialEq)]
enum EIn {
    Setup(Setup),
    Elect(usize),
}

/// Everything a participant can receive, in one type.
#[derive(Clone, Debug, PartialEq)]
enum PIn {
    /// You lead; coordinate these peers.
    BeLeader(Vec<traceforge::thread::ThreadId>),
    /// You follow; vote to this leader.
    BeFollower(traceforge::thread::ThreadId),
    Vote(bool),
    Decision(bool),
}

#[derive(Clone, Debug, PartialEq)]
struct Setup {
    me: usize,
    electors: Vec<traceforge::thread::ThreadId>,
    participants: Vec<traceforge::thread::ThreadId>,
}

/// **Shared by both sides, byte for byte.** Its visible word is the whole of
/// what conformance compares: a role, a vote or a tally, and one decision.
///
/// Written as a tolerant loop rather than a straight line because a peer's vote
/// may overtake this thread's own role message; the counting is unaffected.
fn le_participant(n: usize) {
    let mut peers: Option<Vec<traceforge::thread::ThreadId>> = None;
    let mut voted = false;
    let mut votes = 0usize;
    let mut all_yes = true;
    // **A hard bound rather than `loop`.** A leader needs its role plus `n-1`
    // votes; a follower needs its role plus one decision. `n` covers both.
    //
    // It is a clarity choice, and nothing more. An earlier comment here claimed
    // the bound was load-bearing for the specification search, on the theory
    // that the prober may offer a receive reading **⊥** so an unbounded loop
    // gives the specification an unbounded space. **That is false**:
    // `Offer::may_read_nothing` is set only for a *non-blocking* receive —
    // `prober.rs`'s own tests assert both directions — and `recv_msg_block`
    // simply blocks, contributing no offer at all. The claim was invented to
    // explain a slow run that, measured properly, was never slow: `n = 2`
    // completes in 0.025 s and `n = 3` in 1.633 s. The apparent hang was `n = 4`
    // throughout, whose cost is the election's own state space (1 839 744
    // executions under plain checking), and the missing output was a block-
    // buffered pipe losing a finished row when the process was killed.
    for _ in 0..n {
        match recv_msg_block::<PIn>() {
            PIn::BeLeader(ps) => peers = Some(ps),
            PIn::BeFollower(leader) => {
                if !voted {
                    voted = true;
                    // A fixed vote on purpose. This example exists to abstract
                    // the **election**, and a `nondet()` per participant
                    // multiplies the specification's space by 2^(n-1) for a
                    // dimension the abstraction does not touch — measured: with
                    // nondet votes, `n = 2` did not finish in ten minutes.
                    send_msg(leader, PIn::Vote(true));
                }
            }
            PIn::Vote(v) => {
                votes += 1;
                all_yes &= v;
            }
            PIn::Decision(_) => return,
        }
        if let Some(ps) = peers.as_ref() {
            if votes == ps.len() {
                for p in ps {
                    send_msg(*p, PIn::Decision(all_yes));
                }
                return;
            }
        }
    }
}

/// Announce participant `i`'s role.
fn le_announce(i: usize, leader: usize, ps: &[traceforge::thread::ThreadId]) {
    if i == leader {
        let peers = ps
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != leader)
            .map(|(_, t)| *t)
            .collect();
        send_msg(ps[i], PIn::BeLeader(peers));
    } else {
        send_msg(ps[i], PIn::BeFollower(ps[leader]));
    }
}

/// The implementation's election, on an **invisible** thread.
///
/// Correct: the leader is the highest index, *including my own*, so every
/// elector computes the same answer from the same set.
///
/// `buggy = true` omits its own index — a one-word omission of the kind that
/// survives review. Each elector then takes the maximum of what it *received*,
/// so at `n = 2` the two disagree at once (e0 elects 1, e1 elects 0).
///
/// `P5-APPTAB` T3 (β): with `silent_winner` the elector that computes
/// **itself** as the leader announces nothing — the winner never learns it won;
/// every other elector announces as before. Under the correct rule that
/// elector is `e_{n−1}`; the fault stays in the invisible part.
fn le_elector_with(buggy: bool, silent_winner: bool) {
    let mut setup: Option<Setup> = None;
    let mut heard: Vec<usize> = Vec::new();
    let mut announced = false;
    loop {
        match recv_msg_block::<EIn>() {
            EIn::Setup(s) => setup = Some(s),
            EIn::Elect(j) => heard.push(j),
        }
        let Some(s) = setup.as_ref() else { continue };
        if !announced {
            announced = true;
            for (i, e) in s.electors.iter().enumerate() {
                if i != s.me {
                    send_msg(*e, EIn::Elect(s.me));
                }
            }
        }
        if heard.len() == s.electors.len() - 1 {
            // Correct: seed with my own index. Buggy: start from nothing, so
            // the maximum is taken over what I *received* only.
            let mut best: Option<usize> = if buggy { None } else { Some(s.me) };
            for j in &heard {
                best = Some(match best {
                    Some(b) => b.max(*j),
                    None => *j,
                });
            }
            let leader = best.unwrap_or(s.me);
            if silent_winner && leader == s.me {
                return;
            }
            le_announce(s.me, leader, &s.participants);
            return;
        }
    }
}

/// The specification's oracle: a free choice among the `n` participants,
/// encoded as a ladder of boolean `nondet()`s — the only choice primitive
/// TraceForge offers.
fn le_oracle() {
    let s: Setup = recv_msg_block();
    let n = s.participants.len();
    let mut leader = n - 1;
    for i in 0..n - 1 {
        if nondet() {
            leader = i;
            break;
        }
    }
    for i in 0..n {
        le_announce(i, leader, &s.participants);
    }
}

fn le_impl(n: usize, buggy: bool) -> impl Fn() + Send + Sync + Clone + 'static {
    le_impl_with(n, buggy, false)
}

/// `P5-APPTAB`'s blocked-class mutant (criteria T3, definition (β)): the correct
/// election, but the winner's own elector never announces `BeLeader`.
fn le_impl_silent(n: usize) -> impl Fn() + Send + Sync + Clone + 'static {
    le_impl_with(n, false, true)
}

fn le_impl_with(
    n: usize,
    buggy: bool,
    silent_winner: bool,
) -> impl Fn() + Send + Sync + Clone + 'static {
    move || {
        // Participants first: this is what keeps their ids equal to the
        // specification's (F41).
        let ps: Vec<traceforge::thread::ThreadId> = (0..n)
            .map(|i| {
                named(&format!("p{i}"), move || le_participant(n))
                    .thread()
                    .id()
            })
            .collect();
        let es: Vec<traceforge::thread::ThreadId> = (0..n)
            .map(|i| {
                named(&format!("e{i}"), move || le_elector_with(buggy, silent_winner))
                    .thread()
                    .id()
            })
            .collect();
        for (i, e) in es.iter().enumerate() {
            send_msg(
                *e,
                EIn::Setup(Setup {
                    me: i,
                    electors: es.clone(),
                    participants: ps.clone(),
                }),
            );
        }
    }
}

fn le_spec(n: usize) -> impl Fn() + Send + Sync + Clone + 'static {
    move || {
        let ps: Vec<traceforge::thread::ThreadId> = (0..n)
            .map(|i| {
                named(&format!("p{i}"), move || le_participant(n))
                    .thread()
                    .id()
            })
            .collect();
        let oracle = named("oracle", le_oracle).thread().id();
        send_msg(
            oracle,
            Setup {
                me: 0,
                electors: vec![],
                participants: ps.clone(),
            },
        );
    }
}

fn le_visible(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("p{i}")).collect()
}

// ===========================================================================
// Ring leader election + 2PC (2026-09-29). A *separate* example from the
// all-to-all one above, written because that one does not scale: its election
// gives the implementation 1 839 744 executions at `n = 4` under plain checking,
// before conformance looks at anything.
//
// **The structural difference.** In a ring each elector has exactly **one**
// inbound channel — its predecessor — so its receive order is FIFO and fixed,
// and each message is *caused* by the previous one. The election is a chain
// rather than a cloud, so there is almost nothing to interleave.
//
// **The specification is unchanged**: `le_spec`, one invisible oracle making a
// free `nondet()` choice. Only the implementation's election differs, which is
// the whole point of the example.
//
// **Correct**: a token carrying the running maximum goes round once, `e0` reads
// the winner off the returning token, then an announcement goes round so every
// elector uses *the same* answer.
//
// **Buggy**: each elector decides alone, from the token value it happened to
// see, without considering its own index — "deciding on partial information",
// and it skips the announcement. Every elector then names a different leader, so
// nobody leads.
// ===========================================================================

/// What a **ring** elector can receive. One inbound channel, but three kinds of
/// message, so one type for the reason [`PIn`] is one type.
#[derive(Clone, Debug, PartialEq)]
enum RIn {
    Setup(Setup),
    /// The circulating token, carrying the running maximum index.
    Tok(usize),
    /// The winner, announced once the token has completed its loop.
    Ann(usize),
}

fn ring_elector(buggy: bool) {
    // **Order-tolerant, and this is not optional.** An elector's inbound
    // messages come from two senders — `main` (its `Setup`) and its predecessor
    // (the ring traffic) — so their arrival order is not fixed. A first draft
    // read `Setup` with `match … { RIn::Setup(s) => s, _ => return }` and `e1`
    // received `Tok(0)` first, took the `_` arm and **exited silently**; the
    // whole ring then blocked behind it, and conformance correctly reported the
    // resulting blocked execution as a violation. The lesson is the `_ => return`
    // as much as the race: it discarded a message and left no trace.
    let mut setup: Option<Setup> = None;
    let mut tok: Option<usize> = None;
    let mut ann: Option<usize> = None;
    let mut started = false;
    // At most three inbound messages: `Setup`, `Tok`, and (correct mode, i != 0)
    // `Ann`.
    for _ in 0..3 {
        match recv_msg_block::<RIn>() {
            RIn::Setup(s) => setup = Some(s),
            RIn::Tok(m) => tok = Some(m),
            RIn::Ann(l) => ann = Some(l),
        }
        let Some(s) = setup.as_ref() else { continue };
        let n = s.electors.len();
        let succ = s.electors[(s.me + 1) % n];

        if !started {
            started = true;
            if s.me == 0 {
                send_msg(succ, RIn::Tok(0));
            }
        }

        if let Some(m) = tok.take() {
            if buggy {
                // **The bug.** Decide alone, from the token value seen so far,
                // ignoring my own index, and never wait for an announcement.
                le_announce(s.me, m, &s.participants);
                if s.me != 0 {
                    send_msg(succ, RIn::Tok(m.max(s.me)));
                }
                return;
            }
            if s.me == 0 {
                // The token has been all the way round, so `m` is the maximum.
                le_announce(0, m, &s.participants);
                if n > 1 {
                    send_msg(succ, RIn::Ann(m));
                }
                return;
            }
            send_msg(succ, RIn::Tok(m.max(s.me)));
        }

        if let Some(l) = ann.take() {
            le_announce(s.me, l, &s.participants);
            if s.me + 1 < n {
                send_msg(succ, RIn::Ann(l));
            }
            return;
        }
    }
}

/// Implementation: `n` visible participants, `n` invisible electors in a ring.
fn ring_impl(n: usize, buggy: bool) -> impl Fn() + Send + Sync + Clone + 'static {
    move || {
        // Participants first, so their ids match the specification's (F41).
        let ps: Vec<traceforge::thread::ThreadId> = (0..n)
            .map(|i| {
                named(&format!("p{i}"), move || le_participant(n))
                    .thread()
                    .id()
            })
            .collect();
        let es: Vec<traceforge::thread::ThreadId> = (0..n)
            .map(|i| {
                named(&format!("e{i}"), move || ring_elector(buggy))
                    .thread()
                    .id()
            })
            .collect();
        for (i, e) in es.iter().enumerate() {
            send_msg(
                *e,
                RIn::Setup(Setup {
                    me: i,
                    electors: es.clone(),
                    participants: ps.clone(),
                }),
            );
        }
    }
}

// ------------------------------------------------------ the arguments ----

const PROGRAM: &str = "election";
const USAGE: &str = "`leader {conf|split|silent} <n>` or `ring {conf|split} <n>`";

fn arity(args: &[String], n: usize) {
    if args.len() != n {
        panic!(
            "{PROGRAM}: expected {n} arguments, got {} ({args:?}); ARGS are {USAGE}",
            args.len()
        );
    }
}

fn num(args: &[String], i: usize, what: &str) -> usize {
    args[i].parse().unwrap_or_else(|_| {
        panic!(
            "{PROGRAM}: <{what}> must be a number, got `{}`; ARGS are {USAGE}",
            args[i]
        )
    })
}

#[derive(Clone, Copy)]
enum Variant {
    LeaderConf,
    LeaderSplit,
    LeaderSilent,
    RingConf,
    RingSplit,
}

struct Args {
    variant: Variant,
    n: usize,
}

fn parse(args: &[String]) -> Args {
    arity(args, 3);
    let variant = match (args[0].as_str(), args[1].as_str()) {
        ("leader", "conf") => Variant::LeaderConf,
        ("leader", "split") => Variant::LeaderSplit,
        ("leader", "silent") => Variant::LeaderSilent,
        ("ring", "conf") => Variant::RingConf,
        ("ring", "split") => Variant::RingSplit,
        (e, v) => panic!("{PROGRAM}: unknown variant `{e} {v}`; ARGS are {USAGE}"),
    };
    Args {
        variant,
        n: num(args, 2, "n"),
    }
}

fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &|a| le_visible(parse(a).n),
        spec: &|a| prog(le_spec(parse(a).n)),
        imp: &|a| {
            let p = parse(a);
            match p.variant {
                Variant::LeaderConf => prog(le_impl(p.n, false)),
                Variant::LeaderSplit => prog(le_impl(p.n, true)),
                Variant::LeaderSilent => prog(le_impl_silent(p.n)),
                Variant::RingConf => prog(ring_impl(p.n, false)),
                Variant::RingSplit => prog(ring_impl(p.n, true)),
            }
        },
    })
}
