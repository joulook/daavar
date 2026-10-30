//! TraceForge's S3 (`P5-SYNTH`): `commit(n, j)`, the commitment dial, as an ordinary Must
//! program run by TraceForge's shared main (`traceforge::program`).
//!
//! Ported for H (`criteria/H.md` H-0.3; `log/dev/H.derived.md` §1.2–§1.4) from
//! `traceforge/src/conformance/grid.rs` at `a3a7e1b`, lines 4233–4268 (`commit_prog`). The
//! program code is copied unchanged apart from `crate::` → `traceforge::` and the dropped
//! `pub(super)`. `fifo_chan` is `grid.rs:590-596` with the crate-internal
//! `cons_to_model(ConsType::FIFO)` written as its value, `CommunicationModel::LocalOrder`
//! (derivation §1.3); `prog`/`Prog` and `named` are `grid.rs:60-70`.
//!
//! ARGS: `<n> <j>`
//! * impl: `commit_prog(n, None)`; spec: `commit_prog(n, Some(j))` (`commit_fixture`,
//!   `grid.rs:4272-4280`). `j <= n` is asserted by `commit_prog` on the spec side only (the
//!   impl reads only `n`), as in the source.
//!
//! Visible threads: `p`, `c`.
//!
//! Fixture names: `synth/commit/n<n>j<j>` → `s3-commit <n> <j>`.
//!
//! Options every row spells (never set here; they are the shared main's `--model`, `--seed`,
//! `--threshold`): `--model fifo --seed 0 --threshold 65536` (`cfg(ConsType::FIFO)`,
//! `grid.rs:88-99`). Example: `<bin> row --model fifo --seed 0 --threshold 65536 -- 8 4`.

use std::sync::Arc;

use traceforge::thread;
use traceforge::Nondet;

type Prog = traceforge::program::Entry;

fn prog<F: Fn() + Send + Sync + 'static>(f: F) -> Prog {
    Arc::new(f)
}

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

/// A channel of `i32` under the p2p model (`ConsType::FIFO`'s communication
/// model), created by the calling thread — the server pairs' mailboxes.
fn fifo_chan() -> (traceforge::channel::Sender<i32>, traceforge::channel::Receiver<i32>) {
    traceforge::channel::Builder::<i32>::new()
        .with_comm(traceforge::loc::CommunicationModel::LocalOrder)
        .build()
}

// ---------------------------------------------------------------- S3 ----

/// **S3 `commit(n, j)`**, FIFO (N7), spawned `c, p`: visible `p` sends the
/// common prefix `−1`, then `n` dependent sends of bits `b_i :=
/// (0..=1usize).nondet()` (a `Choice`: invisible, order pinned); visible `c`
/// receives `n + 1` times. The Impl (`j = None`) chooses each bit
/// immediately before its send; the Spec (`j = Some(j)`) chooses `b_1..b_j`
/// **before the prefix send** and the rest each before its send. Knob `n` is
/// the size (`2^n` graphs and signatures on both sides, the `2^n` bit strings
/// behind the prefix); knob `j` is the commitment dial — `j(j+1)/2` visible
/// events of commitment distance, `j = 0` the aligned endpoint.
fn commit_prog(n: usize, j: Option<usize>) -> Prog {
    if let Some(j) = j {
        assert!(j <= n, "conformance: commit(n, j) needs j <= n");
    }
    prog(move || {
        let (tx, rx) = fifo_chan();
        let _c = named("c", move || {
            for _ in 0..=n {
                let _x: i32 = rx.recv_msg_block();
            }
        });
        let _p = named("p", move || {
            let early = j.unwrap_or(0);
            let mut pre: Vec<i32> = Vec::with_capacity(early);
            for _ in 0..early {
                pre.push((0..=1usize).nondet() as i32);
            }
            tx.send_msg(-1);
            for b in pre {
                tx.send_msg(b);
            }
            for _ in early..n {
                tx.send_msg((0..=1usize).nondet() as i32);
            }
        });
    })
}

// ------------------------------------------------------ the arguments ----

const PROGRAM: &str = "s3-commit";
const USAGE: &str = "`<n> <j>`";

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
    n: usize,
    j: usize,
}

fn parse(args: &[String]) -> Args {
    arity(args, 2);
    Args {
        n: num(args, 0, "n"),
        j: num(args, 1, "j"),
    }
}

fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &|a| {
            let _ = parse(a);
            vec!["p".to_string(), "c".to_string()]
        },
        spec: &|a| {
            let p = parse(a);
            commit_prog(p.n, Some(p.j))
        },
        imp: &|a| commit_prog(parse(a).n, None),
    })
}
