//! TraceForge's S1 (`P5-SYNTH`): the paper's `ex:naive` and its conforming twin `naive-self`,
//! as an ordinary Must program run by TraceForge's shared main (`traceforge::program`).
//!
//! Ported for H (`criteria/H.md` H-0.3; `log/dev/H.derived.md` §1.2–§1.4) from
//! `traceforge/src/conformance/grid.rs` at `a3a7e1b`, lines 227–267 (`naive_d`) and 291–296
//! (`naive_visible`). The program code is copied unchanged apart from `crate::` →
//! `traceforge::` and the dropped `pub(super)`. `chan` is `grid.rs:70-76` (its
//! `CommunicationModel::NoOrder` is the public `traceforge::loc::CommunicationModel`, imported
//! below); `prog`/`Prog` and `named` are `grid.rs:60-70`.
//!
//! ARGS: `{naive|self} <k> <enc>`
//! * `enc`: 1 spawns `c, b1..bk, a` (`c_first`), 2 spawns `b1..bk, a, c` — `naive_fixture`'s
//!   and `synth_c_first`'s mapping (`grid.rs:857-875`, 4079-4085); any other value panics;
//! * spec: `naive_d(k, 1, c_first, None)` for both variants;
//! * impl: `naive` = `naive_d(k, 0, c_first, None)` (the violating `ex:naive`),
//!   `self` = `naive_d(k, 1, c_first, None)` (`naive-self`, spec against itself).
//!
//! Visible threads: `a`, `c`, `b1 .. bk` (`naive_visible`).
//!
//! Fixture names: `ex:naive/k<k>/enc<e>` → `s1-naive naive <k> <e>`;
//! `synth/naive-self/k<k>enc<e>` → `s1-naive self <k> <e>`.
//!
//! Options every row spells (never set here; they are the shared main's `--model`, `--seed`,
//! `--threshold`): `--model bag --seed 0 --threshold 65536` (`cfg(ConsType::Bag)`,
//! `grid.rs:88-99`). Example: `<bin> row --model bag --seed 0 --threshold 65536 -- self 3 2`.
//! Declared default model, used when `--model` is left out: `bag`.

use std::sync::Arc;

use traceforge::loc::CommunicationModel;
use traceforge::thread;

type Prog = traceforge::program::Entry;

fn prog<F: Fn() + Send + Sync + 'static>(f: F) -> Prog {
    Arc::new(f)
}

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

/// An unnamed `asyn` channel of `i32`, created by the calling thread (the
/// tester files' `chan`).
fn chan() -> (traceforge::channel::Sender<i32>, traceforge::channel::Receiver<i32>) {
    traceforge::channel::Builder::<i32>::new()
        .with_comm(CommunicationModel::NoOrder)
        .build()
}

// ---------------------------------------------------------------- S1 ----

/// **Copy of `enumerator_tests.rs::naive_d`** (also `gated_tests.rs::naive_enc`
/// without `d`): `ex:naive`'s `Impl_k` (`v = 0`) or `Spec_k` (`v = 1`), every
/// mailbox a channel `main` creates before any spawn. Encoding 1 (`c_first`)
/// spawns `c, b1..bk, a`; encoding 2 spawns `b1..bk, a, c`. With `d_asserts`,
/// a visible `d` spawned last fails an assertion (Impl side only).
fn naive_d(k: usize, v: i32, c_first: bool, d_asserts: Option<bool>) -> Prog {
    prog(move || {
        let (tx_a, _rx_a) = chan();
        let (tx_c, rx_c) = chan();
        let spawn_c = |tx_a: traceforge::channel::Sender<i32>, rx_c: traceforge::channel::Receiver<i32>| {
            named("c", move || {
                tx_a.send_msg(v);
                for _ in 0..k {
                    let _x: i32 = rx_c.recv_msg_block();
                }
            })
        };
        let spawn_bs = |tx_c: &traceforge::channel::Sender<i32>| {
            for i in 1..=k {
                let t = tx_c.clone();
                let _b = named(&format!("b{i}"), move || t.send_msg(1));
            }
        };
        if c_first {
            let _c = spawn_c(tx_a, rx_c);
            spawn_bs(&tx_c);
            let _a = named("a", || {});
        } else {
            spawn_bs(&tx_c);
            let _a = named("a", || {});
            let _c = spawn_c(tx_a, rx_c);
        }
        if let Some(fails) = d_asserts {
            let _d = named("d", move || {
                if fails {
                    traceforge::assert(false);
                }
            });
        }
    })
}

/// `ex:naive`'s visible set: `a`, `c`, `b1..bk` (`enumerator_tests.rs`).
fn naive_visible(k: usize) -> Vec<String> {
    let mut v = vec!["a".to_string(), "c".to_string()];
    v.extend((1..=k).map(|i| format!("b{i}")));
    v
}

// ------------------------------------------------------ the arguments ----

const PROGRAM: &str = "s1-naive";
const USAGE: &str = "`{naive|self} <k> <enc>`, enc 1 or 2";

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
    /// The impl's `v`: 0 for `naive`, 1 for `self`.
    impl_v: i32,
    k: usize,
    c_first: bool,
}

fn parse(args: &[String]) -> Args {
    arity(args, 3);
    let impl_v = match args[0].as_str() {
        "naive" => 0,
        "self" => 1,
        v => panic!("{PROGRAM}: unknown variant `{v}`; ARGS are {USAGE}"),
    };
    let k = num(args, 1, "k");
    let c_first = match num(args, 2, "enc") {
        1 => true,
        2 => false,
        e => panic!("{PROGRAM}: ex:naive has encodings 1 and 2, got {e}; ARGS are {USAGE}"),
    };
    Args { impl_v, k, c_first }
}

fn main() -> std::process::ExitCode {
    let program = traceforge::program::Program {
        visible: &|a| naive_visible(parse(a).k),
        spec: &|a| {
            let p = parse(a);
            naive_d(p.k, 1, p.c_first, None)
        },
        imp: &|a| {
            let p = parse(a);
            naive_d(p.k, p.impl_v, p.c_first, None)
        },
    };
    traceforge::program::main_with_model(program, traceforge::ConsType::Bag)
}
