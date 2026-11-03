//! TraceForge's S6 (`P5-SYNTH`): `width(w)`, one visible `w`-way choice, self-paired, as an
//! ordinary Must program run by TraceForge's shared main (`traceforge::program`).
//!
//! Ported for H (`criteria/H.md` H-0.3; `log/dev/H.derived.md` §1.2–§1.4) from
//! `traceforge/src/conformance/grid.rs` at `a3a7e1b`, lines 4486–4502 (`width_prog`). The
//! program code is copied unchanged apart from `crate::` → `traceforge::` and the dropped
//! `pub(super)`. `fifo_chan` is `grid.rs:590-596` with the crate-internal
//! `cons_to_model(ConsType::FIFO)` written as its value, `CommunicationModel::LocalOrder`
//! (derivation §1.3); `prog`/`Prog` and `named` are `grid.rs:60-70`.
//!
//! ARGS: `<w>`
//! * `width_prog(w)` on both sides (`width_fixture`, `grid.rs:4505-4515`). `w >= 1` is
//!   asserted by `width_prog`.
//!
//! Visible threads: `v`, `c`.
//!
//! Fixture names: `synth/width/w<w>` → `s6-width <w>`.
//!
//! Options every row spells (never set here; they are the shared main's `--model`, `--seed`,
//! `--threshold`): `--model fifo --seed 0 --threshold 65536` (`cfg(ConsType::FIFO)`,
//! `grid.rs:88-99`). Example: `<bin> row --model fifo --seed 0 --threshold 65536 -- 32`.

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

// ---------------------------------------------------------------- S6 ----

/// **S6 `width(w)`**, FIFO, spawned `c, v`: visible `v` chooses `x :=
/// (0..w).nondet()` (a `Choice`) and sends it to visible `c`, who receives
/// once; the same program on both sides. Knob `w` is the size: `w` graphs,
/// `w` signatures, `|W| = w` on the sweeping engines.
fn width_prog(w: usize) -> Prog {
    assert!(w >= 1, "conformance: width(w) needs w >= 1");
    prog(move || {
        let (tx, rx) = fifo_chan();
        let _c = named("c", move || {
            let _x: i32 = rx.recv_msg_block();
        });
        let _v = named("v", move || {
            let x = (0..w).nondet();
            tx.send_msg(x as i32);
        });
    })
}

// ------------------------------------------------------ the arguments ----

const PROGRAM: &str = "s6-width";
const USAGE: &str = "`<w>`";

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

fn parse(args: &[String]) -> usize {
    arity(args, 1);
    num(args, 0, "w")
}

fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &|a| {
            let _ = parse(a);
            vec!["v".to_string(), "c".to_string()]
        },
        spec: &|a| width_prog(parse(a)),
        imp: &|a| width_prog(parse(a)),
    })
}
