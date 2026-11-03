//! TraceForge's A1 (`P5-APPS`): two-phase commit deepened to `r` rounds, as an ordinary Must
//! program run by TraceForge's shared main (`traceforge::program`).
//!
//! Ported for H (`criteria/H.md` H-0.3; `log/dev/H.derived.md` §1.2–§1.4) from
//! `traceforge/src/conformance/grid.rs` at `a3a7e1b`, lines 3106–3266 (`A1ToCoord`, `A1ToPart`,
//! `A1Coord`, `a1_participant`, `a1_coordinator`, `a1_impl`, `a1_spec`). The program code is
//! copied unchanged apart from `crate::` → `traceforge::` and the dropped `pub(super)`;
//! `prog`/`Prog` and `named` are `grid.rs`'s helpers (lines 60–70).
//!
//! ARGS: `<variant> <n> <r>`
//! * variant: `correct`, `eager`, `early-abort`, `silent`, `control-reverse` (`A1Coord`);
//! * impl: `a1_impl(n, r, variant)`; spec: `a1_spec(r)` (two participants whatever `n`; the
//!   spec reads only `r`, but every argument is checked on both sides).
//!
//! Visible threads: `p0`, `p1`, whatever the arguments.
//!
//! Fixture names: `apps/a1/<variant>/spec/n<n>r<r>` → `a1-commit <variant> <n> <r>`.
//!
//! Options every row spells (never set here; they are the shared main's `--model`, `--seed`,
//! `--threshold`): `--model fifo --seed 0`, and **no** `--threshold` — the engine's default
//! thread bound (1 000), as `apps_config` (`grid.rs:3087-3092`) built the fixtures' `Config`.
//! Example: `<bin> row --model fifo --seed 0 -- correct 2 2`.

use std::sync::Arc;

use traceforge::thread;

type Prog = traceforge::program::Entry;

fn prog<F: Fn() + Send + Sync + 'static>(f: F) -> Prog {
    Arc::new(f)
}

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

// ---------------------------------------------------------------- A1 ----

#[derive(Clone, PartialEq, Debug)]
enum A1ToCoord {
    Init(Vec<traceforge::thread::ThreadId>),
    Yes,
    No,
}

#[derive(Clone, PartialEq, Debug)]
enum A1ToPart {
    Prepare(traceforge::thread::ThreadId),
    Commit,
    Abort,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum A1Coord {
    /// Real 2PC: every vote read, commit iff all yes, the same decision to all.
    Correct,
    /// `bench.rs`'s bug: decide as each vote arrives (value and order faults).
    Eager,
    /// Broadcast `Abort` on the first `No`, then drain the round's votes (order).
    EarlyAbort,
    /// The decision to `p1` is never sent in the last round (status).
    Silent,
    /// Decisions sent in reverse participant order (the still-conforming control).
    ControlReverse,
}

/// A participant: `r` rounds of `recv Prepare; vote; send; recv decision`.
fn a1_participant(r: usize) {
    for _ in 0..r {
        let cid = match traceforge::recv_msg_block::<A1ToPart>() {
            A1ToPart::Prepare(id) => id,
            _ => return,
        };
        let vote = traceforge::nondet();
        traceforge::send_msg(cid, if vote { A1ToCoord::Yes } else { A1ToCoord::No });
        let _decision: A1ToPart = traceforge::recv_msg_block();
    }
}

fn a1_coordinator(kind: A1Coord, r: usize) {
    let ps = match traceforge::recv_msg_block::<A1ToCoord>() {
        A1ToCoord::Init(ps) => ps,
        _ => return,
    };
    let me = thread::current().id();
    for round in 0..r {
        for p in &ps {
            traceforge::send_msg(*p, A1ToPart::Prepare(me));
        }
        match kind {
            A1Coord::Correct | A1Coord::Silent | A1Coord::ControlReverse => {
                let mut yes = 0usize;
                for _ in 0..ps.len() {
                    if let A1ToCoord::Yes = traceforge::recv_msg_block::<A1ToCoord>() {
                        yes += 1;
                    }
                }
                let d = if yes == ps.len() {
                    A1ToPart::Commit
                } else {
                    A1ToPart::Abort
                };
                let order: Vec<&traceforge::thread::ThreadId> = if kind == A1Coord::ControlReverse {
                    ps.iter().rev().collect()
                } else {
                    ps.iter().collect()
                };
                for (i, p) in order.into_iter().enumerate() {
                    let silent = kind == A1Coord::Silent && round + 1 == r && i == 1;
                    if !silent {
                        traceforge::send_msg(*p, d.clone());
                    }
                }
            }
            A1Coord::Eager => {
                let mut seen_no = false;
                for p in &ps {
                    if let A1ToCoord::No = traceforge::recv_msg_block::<A1ToCoord>() {
                        seen_no = true;
                    }
                    traceforge::send_msg(
                        *p,
                        if seen_no {
                            A1ToPart::Abort
                        } else {
                            A1ToPart::Commit
                        },
                    );
                }
            }
            A1Coord::EarlyAbort => {
                let mut read = 0usize;
                let mut aborted = false;
                while read < ps.len() {
                    let v: A1ToCoord = traceforge::recv_msg_block();
                    read += 1;
                    if !aborted && v == A1ToCoord::No {
                        aborted = true;
                        for p in &ps {
                            traceforge::send_msg(*p, A1ToPart::Abort);
                        }
                    }
                }
                if !aborted {
                    for p in &ps {
                        traceforge::send_msg(*p, A1ToPart::Commit);
                    }
                }
            }
        }
    }
}

/// The Impl: coordinator first (`t1`), then `p0..p{n-1}`; `main` sends `Init`.
fn a1_impl(n: usize, r: usize, kind: A1Coord) -> Prog {
    prog(move || {
        let coord = named("coord", move || a1_coordinator(kind, r));
        let mut ids = Vec::with_capacity(n);
        for i in 0..n {
            ids.push(named(&format!("p{i}"), move || a1_participant(r)).thread().id());
        }
        traceforge::send_msg(coord.thread().id(), A1ToCoord::Init(ids));
    })
}

/// The Spec: two participants whatever `n`; the oracle reads every vote, then
/// chooses by `nondet()`, the same decision to both (no early abort).
fn a1_spec(r: usize) -> Prog {
    prog(move || {
        let coord = named("coord", move || {
            let ps = match traceforge::recv_msg_block::<A1ToCoord>() {
                A1ToCoord::Init(ps) => ps,
                _ => return,
            };
            let me = thread::current().id();
            for _ in 0..r {
                for p in &ps {
                    traceforge::send_msg(*p, A1ToPart::Prepare(me));
                }
                for _ in 0..ps.len() {
                    let _vote: A1ToCoord = traceforge::recv_msg_block();
                }
                let d = if traceforge::nondet() {
                    A1ToPart::Commit
                } else {
                    A1ToPart::Abort
                };
                for p in &ps {
                    traceforge::send_msg(*p, d.clone());
                }
            }
        });
        let mut ids = Vec::with_capacity(2);
        for i in 0..2 {
            ids.push(named(&format!("p{i}"), move || a1_participant(r)).thread().id());
        }
        traceforge::send_msg(coord.thread().id(), A1ToCoord::Init(ids));
    })
}

// ------------------------------------------------------ the arguments ----

const PROGRAM: &str = "a1-commit";
const USAGE: &str =
    "`<variant> <n> <r>`, variant one of correct, eager, early-abort, silent, control-reverse";

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

struct Args {
    kind: A1Coord,
    n: usize,
    r: usize,
}

fn parse(args: &[String]) -> Args {
    arity(args, 3);
    let kind = match args[0].as_str() {
        "correct" => A1Coord::Correct,
        "eager" => A1Coord::Eager,
        "early-abort" => A1Coord::EarlyAbort,
        "silent" => A1Coord::Silent,
        "control-reverse" => A1Coord::ControlReverse,
        v => panic!("{PROGRAM}: unknown variant `{v}`; ARGS are {USAGE}"),
    };
    Args {
        kind,
        n: num(args, 1, "n"),
        r: num(args, 2, "r"),
    }
}

fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &|a| {
            let _ = parse(a);
            vec!["p0".to_string(), "p1".to_string()]
        },
        spec: &|a| a1_spec(parse(a).r),
        imp: &|a| {
            let p = parse(a);
            a1_impl(p.n, p.r, p.kind)
        },
    })
}
