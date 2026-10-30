//! TraceForge's S2 (`P5-SYNTH`): `share(m, c)`, invisible choice behind relays, and its matched
//! control `share-ctl(m, c)`, as an ordinary Must program run by TraceForge's shared main
//! (`traceforge::program`).
//!
//! Ported for H (`criteria/H.md` H-0.3; `log/dev/H.derived.md` §1.2–§1.4) from
//! `traceforge/src/conformance/grid.rs` at `a3a7e1b`, lines 4125–4207 (`share_impl`,
//! `share_spec`, `share_ctl`, `share_visible`) and 4098–4100 (`synth_names`). The program code
//! is copied unchanged apart from `crate::` → `traceforge::` and the dropped `pub(super)`.
//! `fifo_chan` is `grid.rs:590-596` with the crate-internal `cons_to_model(ConsType::FIFO)`
//! written as its value, `CommunicationModel::LocalOrder` (derivation §1.3); `prog`/`Prog` and
//! `named` are `grid.rs:60-70`.
//!
//! ARGS: `{share|ctl} <m> <c>`
//! * `share`: impl `share_impl(m, c)`, spec `share_spec(m)` (`synth/share`, `grid.rs:4210-4218`);
//! * `ctl`: `share_ctl(m, c)` on both sides (`synth/share-ctl`, `grid.rs:4221-4230`).
//! * `c <= m` is asserted by `share_impl` and `share_ctl` (on the `share` spec side, which
//!   reads only `m`, it is not checked, as in the source).
//!
//! Visible threads: `a0 .. a{m-1}`, then `c0 .. c{m-1}` (`share_visible`).
//!
//! Fixture names: `synth/share/m<m>c<c>` → `s2-share share <m> <c>`;
//! `synth/share-ctl/m<m>c<c>` → `s2-share ctl <m> <c>`.
//!
//! Options every row spells (never set here; they are the shared main's `--model`, `--seed`,
//! `--threshold`): `--model fifo --seed 0 --threshold 65536` (`cfg(ConsType::FIFO)`,
//! `grid.rs:88-99`). Example: `<bin> row --model fifo --seed 0 --threshold 65536 -- share 4 2`.

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

fn synth_names(prefix: &str, n: usize) -> Vec<String> {
    (0..n).map(|i| format!("{prefix}{i}")).collect()
}

// ---------------------------------------------------------------- S2 ----

/// **S2 `share(m, c)`, the Impl**: `m` relay modules on disjoint FIFO
/// channels, module `i` spawned `c{i}, r{i}, a{i}`. Visible `a{i}` sends 9
/// to invisible `r{i}`; `r{i}` reads it — for `i < c` by a **non-blocking**
/// receive followed, when it read ⊥, by a blocking one; for `i ≥ c` by one
/// blocking receive — and sends what it read to visible `c{i}`, who receives.
/// The choice is invisible (N3): every Impl graph has one visible word and
/// one `vo`, so the Spec's single graph covers all `2^c`. Knob `m` is the
/// size (modules); knob `c` is the number of modules with the choice —
/// at fixed `m`, `c` varies sharing (`impl_graphs / |W| = 2^c`) at
/// near-constant size; scaling `m` at fixed `c/m` is size scaling.
fn share_impl(m: usize, c: usize) -> Prog {
    assert!(c <= m, "conformance: share(m, c) needs c <= m");
    prog(move || {
        for i in 0..m {
            let (tx_r, rx_r) = fifo_chan();
            let (tx_c, rx_c) = fifo_chan();
            let _c = named(&format!("c{i}"), move || {
                let _x: i32 = rx_c.recv_msg_block();
            });
            let choose = i < c;
            let _r = named(&format!("r{i}"), move || {
                let x: i32 = if choose {
                    match rx_r.recv_msg() {
                        Some(x) => x,
                        None => rx_r.recv_msg_block(),
                    }
                } else {
                    rx_r.recv_msg_block()
                };
                tx_c.send_msg(x);
            });
            let _a = named(&format!("a{i}"), move || tx_r.send_msg(9));
        }
    })
}

/// **S2's Spec `share-spec(m)`**: per module the relay pair's Spec, `a{i}:
/// send(c{i}, 9) ‖ c{i}: recv()`, spawned `c{i}, a{i}`; one graph.
fn share_spec(m: usize) -> Prog {
    prog(move || {
        for i in 0..m {
            let (tx_c, rx_c) = fifo_chan();
            let _c = named(&format!("c{i}"), move || {
                let _x: i32 = rx_c.recv_msg_block();
            });
            let _a = named(&format!("a{i}"), move || tx_c.send_msg(9));
        }
    })
}

/// **S2's matched control `share-ctl(m, c)`**, self-paired: the same shape
/// with a blocking receive in every `r{i}`; the first `c` modules' `r{i}`
/// choose `b := (0..=1usize).nondet()` (a `Choice`, N2) after the receive and
/// send `10·i + b` to `c{i}`, the others send the fixed `10·i`. The
/// branching is at the same point as the family's but **visible** in
/// `c{i}`'s value: Impl = Spec = `2^c` graphs, `2^c` signatures, `|W| =
/// 2^c`, `cache_hits = 0`, sharing 1.
fn share_ctl(m: usize, c: usize) -> Prog {
    assert!(c <= m, "conformance: share-ctl(m, c) needs c <= m");
    prog(move || {
        for i in 0..m {
            let (tx_r, rx_r) = fifo_chan();
            let (tx_c, rx_c) = fifo_chan();
            let _c = named(&format!("c{i}"), move || {
                let _x: i32 = rx_c.recv_msg_block();
            });
            let choose = i < c;
            let base = 10 * i as i32;
            let _r = named(&format!("r{i}"), move || {
                let _x: i32 = rx_r.recv_msg_block();
                let b = if choose { (0..=1usize).nondet() as i32 } else { 0 };
                tx_c.send_msg(base + b);
            });
            let _a = named(&format!("a{i}"), move || tx_r.send_msg(9));
        }
    })
}

fn share_visible(m: usize) -> Vec<String> {
    let mut v = synth_names("a", m);
    v.extend(synth_names("c", m));
    v
}

// ------------------------------------------------------ the arguments ----

const PROGRAM: &str = "s2-share";
const USAGE: &str = "`{share|ctl} <m> <c>`";

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
    /// `ctl` (`share-ctl`, self-paired) or `share`.
    ctl: bool,
    m: usize,
    c: usize,
}

fn parse(args: &[String]) -> Args {
    arity(args, 3);
    let ctl = match args[0].as_str() {
        "share" => false,
        "ctl" => true,
        v => panic!("{PROGRAM}: unknown variant `{v}`; ARGS are {USAGE}"),
    };
    Args {
        ctl,
        m: num(args, 1, "m"),
        c: num(args, 2, "c"),
    }
}

fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &|a| share_visible(parse(a).m),
        spec: &|a| {
            let p = parse(a);
            if p.ctl {
                share_ctl(p.m, p.c)
            } else {
                share_spec(p.m)
            }
        },
        imp: &|a| {
            let p = parse(a);
            if p.ctl {
                share_ctl(p.m, p.c)
            } else {
                share_impl(p.m, p.c)
            }
        },
    })
}
