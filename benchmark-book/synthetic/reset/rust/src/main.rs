//! TraceForge's S5 (`P5-SYNTH`): `reset(k, s)`, backward revisits under `Bag`, with its
//! low-revisit control and its conforming twin, as an ordinary Must program run by
//! TraceForge's shared main (`traceforge::program`).
//!
//! Ported for H (`criteria/H.md` H-0.3; `log/dev/H.derived.md` §1.2–§1.4) from
//! `traceforge/src/conformance/grid.rs` at `a3a7e1b`, lines 4360–4446 (`ResetOrder`,
//! `reset_prog`, `reset_visible`). The program code is copied unchanged apart from `crate::` →
//! `traceforge::` and the dropped `pub(super)`. `chan` is `grid.rs:70-76` (its
//! `CommunicationModel::NoOrder` is the public `traceforge::loc::CommunicationModel`, imported
//! below); `prog`/`Prog` and `named` are `grid.rs:60-70`.
//!
//! ARGS: `{reset|ctl|twin} <k> <s>`
//! * `reset` (`synth/reset`, `grid.rs:4450-4459`): impl `reset_prog(k, false, Acb, None)`,
//!   spec `reset_prog(k, true, Acb, Some(s))`;
//! * `ctl` (`synth/reset-ctl`, `grid.rs:4462-4471`): impl `reset_prog(k, false, Abc, None)`,
//!   spec `reset_prog(k, true, Abc, Some(s))`;
//! * `twin` (`synth/reset-twin`, `grid.rs:4474-4486`): `reset_prog(k, true, Acb, Some(s))` on
//!   both sides.
//! * `k >= 1` is asserted by `reset_prog`; `s >= 1` only where `s` is used (`pad = Some(s)`),
//!   as in the source.
//!
//! Visible threads: `a{i}, b{i}, c{i}` for `i < k`, interleaved (`reset_visible`).
//!
//! Fixture names: `synth/reset/k<k>s<s>` → `s5-reset reset <k> <s>`;
//! `synth/reset-ctl/k<k>s<s>` → `s5-reset ctl <k> <s>`;
//! `synth/reset-twin/k<k>s<s>` → `s5-reset twin <k> <s>`.
//!
//! Options every row spells (never set here; they are the shared main's `--model`, `--seed`,
//! `--threshold`): `--model bag --seed 0 --threshold 65536` (`cfg(ConsType::Bag)`,
//! `grid.rs:88-99`). Example: `<bin> row --model bag --seed 0 --threshold 65536 -- ctl 4 3`.
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

// ---------------------------------------------------------------- S5 ----

/// The spawn order of one copy of N4's pair.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ResetOrder {
    /// `a, c, b` — the family: `b`'s send backward-revisits `c`'s receive.
    Acb,
    /// `a, b, c` — the control: both sends deliverable at `c`'s receive, the
    /// alternative source is a forward pop.
    Abc,
}

/// **S5 `reset(k, s)`**, `Bag`: `k` copies of N4's pair on disjoint channels,
/// copy `i` with visible `a{i}` (sends 1, tag 1), `c{i}` (receives once:
/// any message on the Impl, `only_tag_2` false; only tag 2 on the Spec) and
/// `b{i}` (sends 2, tag 2), in `order`, copies in index order. With `pad =
/// Some(s)` — the **Spec side only** — an invisible padding module follows
/// every copy: `s` invisible senders `s0..s{s-1}` of distinct values to one
/// invisible receiver `d` that receives `s` times, spawned `s0..s{s-1}, d`:
/// `s!` Spec graphs with one visible projection. Knob `k` is the size (Impl
/// `2^k`, Spec `s!`, uncovered `2^k − 1`); knob `s` multiplies the work of
/// every failing sweep after a certificate (`s!` Spec graphs each) and
/// nothing else — it is not a size axis.
fn reset_prog(
    k: usize,
    only_tag_2: bool,
    order: ResetOrder,
    pad: Option<usize>,
) -> Prog {
    assert!(k >= 1, "conformance: reset(k, s) needs k >= 1");
    if let Some(s) = pad {
        assert!(s >= 1, "conformance: reset(k, s) needs s >= 1");
    }
    prog(move || {
        for i in 0..k {
            let (tx, rx) = chan();
            let ta = tx.clone();
            let spawn_a = move || {
                let _a = named(&format!("a{i}"), move || ta.send_tagged_msg(1, 1));
            };
            let spawn_b = move || {
                let _b = named(&format!("b{i}"), move || tx.send_tagged_msg(2, 2));
            };
            let spawn_c = move || {
                let _c = named(&format!("c{i}"), move || {
                    let _x: i32 = if only_tag_2 {
                        rx.recv_tagged_msg_block(|t| t == Some(2))
                    } else {
                        rx.recv_msg_block()
                    };
                });
            };
            match order {
                ResetOrder::Acb => {
                    spawn_a();
                    spawn_c();
                    spawn_b();
                }
                ResetOrder::Abc => {
                    spawn_a();
                    spawn_b();
                    spawn_c();
                }
            }
        }
        if let Some(s) = pad {
            let (tx_d, rx_d) = chan();
            for j in 0..s {
                let t = tx_d.clone();
                let _s = named(&format!("s{j}"), move || t.send_msg(100 + j as i32));
            }
            let _d = named("d", move || {
                for _ in 0..s {
                    let _x: i32 = rx_d.recv_msg_block();
                }
            });
        }
    })
}

fn reset_visible(k: usize) -> Vec<String> {
    let mut v = Vec::with_capacity(3 * k);
    for i in 0..k {
        v.push(format!("a{i}"));
        v.push(format!("b{i}"));
        v.push(format!("c{i}"));
    }
    v
}

// ------------------------------------------------------ the arguments ----

const PROGRAM: &str = "s5-reset";
const USAGE: &str = "`{reset|ctl|twin} <k> <s>`";

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
    Reset,
    Ctl,
    Twin,
}

struct Args {
    variant: Variant,
    k: usize,
    s: usize,
}

fn parse(args: &[String]) -> Args {
    arity(args, 3);
    let variant = match args[0].as_str() {
        "reset" => Variant::Reset,
        "ctl" => Variant::Ctl,
        "twin" => Variant::Twin,
        v => panic!("{PROGRAM}: unknown variant `{v}`; ARGS are {USAGE}"),
    };
    Args {
        variant,
        k: num(args, 1, "k"),
        s: num(args, 2, "s"),
    }
}

fn main() -> std::process::ExitCode {
    let program = traceforge::program::Program {
        visible: &|a| reset_visible(parse(a).k),
        spec: &|a| {
            let p = parse(a);
            match p.variant {
                Variant::Reset | Variant::Twin => {
                    reset_prog(p.k, true, ResetOrder::Acb, Some(p.s))
                }
                Variant::Ctl => reset_prog(p.k, true, ResetOrder::Abc, Some(p.s)),
            }
        },
        imp: &|a| {
            let p = parse(a);
            match p.variant {
                Variant::Reset => reset_prog(p.k, false, ResetOrder::Acb, None),
                Variant::Ctl => reset_prog(p.k, false, ResetOrder::Abc, None),
                Variant::Twin => reset_prog(p.k, true, ResetOrder::Acb, Some(p.s)),
            }
        },
    };
    traceforge::program::main_with_model(program, traceforge::ConsType::Bag)
}
