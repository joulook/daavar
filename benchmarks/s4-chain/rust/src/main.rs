//! TraceForge's S4 (`P5-SYNTH`): `chain(d)`, a relayed chain against a direct one, as an
//! ordinary Must program run by TraceForge's shared main (`traceforge::program`).
//!
//! Ported for H (`criteria/H.md` H-0.3; `log/dev/H.derived.md` §1.2–§1.4) from
//! `traceforge/src/conformance/grid.rs` at `a3a7e1b`, lines 4284–4344 (`chain_prog`) and
//! 4098–4100 (`synth_names`). The program code is copied unchanged apart from `crate::` →
//! `traceforge::` and the dropped `pub(super)`. `fifo_chan` is `grid.rs:590-596` with the
//! crate-internal `cons_to_model(ConsType::FIFO)` written as its value,
//! `CommunicationModel::LocalOrder` (derivation §1.3); `prog`/`Prog` and `named` are
//! `grid.rs:60-70`.
//!
//! ARGS: `<d>`
//! * impl: `chain_prog(d, true)` (relayed); spec: `chain_prog(d, false)` (`chain_fixture`,
//!   `grid.rs:4348-4356`). `d >= 1` is asserted by `chain_prog`.
//!
//! Visible threads: `v0 .. v{d}` (`synth_names("v", d + 1)`).
//!
//! Fixture names: `synth/chain/d<d>` → `s4-chain <d>`.
//!
//! Options every row spells (never set here; they are the shared main's `--model`, `--seed`,
//! `--threshold`): `--model fifo --seed 0 --threshold 65536` (`cfg(ConsType::FIFO)`,
//! `grid.rs:88-99`). The threshold matters here: without it `chain` at `d >= 512` stops early
//! (1 025 spawns against the default bound of 1 000, `grid.rs:81-88`).
//! Example: `<bin> row --model fifo --seed 0 --threshold 65536 -- 512`.

use std::sync::Arc;

use traceforge::thread;

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

fn synth_names(prefix: &str, n: usize) -> Vec<String> {
    (0..n).map(|i| format!("{prefix}{i}")).collect()
}

// ---------------------------------------------------------------- S4 ----

/// **S4 `chain(d)`**, FIFO: visible `v0 … v{d}`; `v0` sends 1 along hop 1,
/// `v{i}` (`1 ≤ i < d`) receives then sends `i + 1` along hop `i + 1`, `v{d}`
/// receives. With `relayed`, hop `i` goes through an invisible `r{i}` that
/// forwards what it read (the Impl, spawned `v{d}, r{d}, …, v1, r1, v0`);
/// without, `v{i-1}` sends to `v{i}` directly (the Spec, spawned `v{d}, …,
/// v0`). Knob `d` is the size: 1 graph, 1 signature either side; `ord` a
/// chain of `2d` visible events over `d` hops; `4d` Impl paper events, `2d`
/// Spec.
fn chain_prog(d: usize, relayed: bool) -> Prog {
    assert!(d >= 1, "conformance: chain(d) needs d >= 1");
    prog(move || {
        // Hop i (1..=d) is index i - 1: the channel into v{i}, and, relayed,
        // the channel into r{i}; the sender side of hop i belongs to v{i-1}
        // (direct) or to r{i} (relayed).
        let mut tx_v = Vec::with_capacity(d);
        let mut rx_v = Vec::with_capacity(d);
        let mut tx_r = Vec::with_capacity(d);
        let mut rx_r = Vec::with_capacity(d);
        for _ in 0..d {
            let (t, r) = fifo_chan();
            tx_v.push(Some(t));
            rx_v.push(Some(r));
            if relayed {
                let (t, r) = fifo_chan();
                tx_r.push(Some(t));
                rx_r.push(Some(r));
            } else {
                tx_r.push(None);
                rx_r.push(None);
            }
        }
        for i in (0..=d).rev() {
            let rx = if i >= 1 { rx_v[i - 1].take() } else { None };
            let tx = if i < d {
                if relayed {
                    tx_r[i].take()
                } else {
                    tx_v[i].take()
                }
            } else {
                None
            };
            let _v = named(&format!("v{i}"), move || {
                if let Some(rx) = rx {
                    let _x: i32 = rx.recv_msg_block();
                }
                if let Some(tx) = tx {
                    tx.send_msg(i as i32 + 1);
                }
            });
            if relayed && i >= 1 {
                let rx_r = rx_r[i - 1].take().expect("conformance: relay in");
                let tx_v = tx_v[i - 1].take().expect("conformance: relay out");
                let _r = named(&format!("r{i}"), move || {
                    let x: i32 = rx_r.recv_msg_block();
                    tx_v.send_msg(x);
                });
            }
        }
    })
}

// ------------------------------------------------------ the arguments ----

const PROGRAM: &str = "s4-chain";
const USAGE: &str = "`<d>`";

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
    num(args, 0, "d")
}

fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &|a| synth_names("v", parse(a) + 1),
        spec: &|a| chain_prog(parse(a), false),
        imp: &|a| chain_prog(parse(a), true),
    })
}
