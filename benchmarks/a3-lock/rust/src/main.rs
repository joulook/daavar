//! TraceForge's A3 (`P5-APPS`): a lock service, as an ordinary Must program run by TraceForge's
//! shared main (`traceforge::program`).
//!
//! Ported for H (`criteria/H.md` H-0.3; `log/dev/H.derived.md` §1.2–§1.4) from
//! `traceforge/src/conformance/grid.rs` at `a3a7e1b`, lines 3565–3573 (`Msg`, `Grant`) and
//! 3626–3744 (`a3_spawn_clients`, `a3_reply_channels`, `a3_spec`, `A3Coord`, `a3_impl`); the
//! J3 `SummaryEncode` impls (3574–3624) are left out, as G1 stage B removed them (the generic
//! encoder needs only `#[derive(Debug)]`). The program code is copied unchanged apart from
//! `crate::` → `traceforge::` and the dropped `pub(super)`. `typed_chan` is
//! `grid.rs:3080-3085` with the crate-internal `cons_to_model(ConsType::FIFO)` written as its
//! value, `CommunicationModel::LocalOrder` (derivation §1.3); `prog`/`Prog` and `named` are
//! `grid.rs:60-70`.
//!
//! ARGS: `<variant> <k> <q>`
//! * variant: `fifo`, `double-grant`, `never-grant`, `wrong-round`, `control-lifo` (`A3Coord`
//!   `Fifo`, `DoubleGrant`, `NeverGrant`, `WrongRound`, `Lifo`);
//! * impl: `a3_impl(k, q, variant)`; spec: `a3_spec(k, q)`.
//!
//! Visible threads: `c0 .. c{k-1}`.
//!
//! Fixture names: `apps/a3/<variant>/spec/k<k>q<q>` → `a3-lock <variant> <k> <q>`.
//!
//! Options every row spells (never set here; they are the shared main's `--model`, `--seed`,
//! `--threshold`): `--model fifo --seed 0`, and **no** `--threshold` — the engine's default
//! thread bound (1 000), as `apps_config` (`grid.rs:3087-3092`) built the fixtures' `Config`.
//! Example: `<bin> row --model fifo --seed 0 -- double-grant 3 3`.

use std::sync::Arc;

use traceforge::thread;

type Prog = traceforge::program::Entry;

fn prog<F: Fn() + Send + Sync + 'static>(f: F) -> Prog {
    Arc::new(f)
}

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

/// A typed channel under `ConsType::FIFO`'s communication model (`LocalOrder`),
/// created by the calling thread (criterion 2; `chan()`/`fifo_chan()` untouched).
fn typed_chan<T: Send + PartialEq + Clone + std::fmt::Debug + 'static>(
) -> (traceforge::channel::Sender<T>, traceforge::channel::Receiver<T>) {
    traceforge::channel::Builder::<T>::new()
        .with_comm(traceforge::loc::CommunicationModel::LocalOrder)
        .build()
}

// ---------------------------------------------------------------- A3 ----

#[derive(Clone, PartialEq, Debug)]
enum Msg {
    Acq(usize),
    Rel(usize),
}

#[derive(Clone, PartialEq, Debug)]
struct Grant(usize, usize);

fn a3_spawn_clients(
    q: usize,
    reply_rx: Vec<traceforge::channel::Receiver<Grant>>,
    tx_acq: traceforge::channel::Sender<Msg>,
    tx_rel: traceforge::channel::Sender<Msg>,
) {
    for (i, rx) in reply_rx.into_iter().enumerate() {
        let tx_acq = tx_acq.clone();
        let tx_rel = tx_rel.clone();
        named(&format!("c{i}"), move || {
            for _ in 0..q {
                tx_acq.send_msg(Msg::Acq(i));
                let _g: Grant = rx.recv_msg_block();
                tx_rel.send_msg(Msg::Rel(i));
            }
        });
    }
}

fn a3_reply_channels(
    k: usize,
) -> (
    Vec<traceforge::channel::Sender<Grant>>,
    Vec<traceforge::channel::Receiver<Grant>>,
) {
    let mut txs = Vec::with_capacity(k);
    let mut rxs = Vec::with_capacity(k);
    for _ in 0..k {
        let (tx, rx) = typed_chan::<Grant>();
        txs.push(tx);
        rxs.push(rx);
    }
    (txs, rxs)
}

/// The Spec: an invisible `lock` with an acquire channel and a release
/// channel — one holder at a time, any grant order.
fn a3_spec(k: usize, q: usize) -> Prog {
    prog(move || {
        let (tx_acq, rx_acq) = typed_chan::<Msg>();
        let (tx_rel, rx_rel) = typed_chan::<Msg>();
        let (reply_tx, reply_rx) = a3_reply_channels(k);
        named("lock", move || {
            let mut round = vec![0usize; k];
            for _ in 0..k * q {
                if let Msg::Acq(i) = rx_acq.recv_msg_block() {
                    reply_tx[i].send_msg(Grant(i, round[i]));
                    round[i] += 1;
                    let _r: Msg = rx_rel.recv_msg_block();
                }
            }
        });
        a3_spawn_clients(q, reply_rx, tx_acq, tx_rel);
    })
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum A3Coord {
    Fifo,
    /// An `Acq` is granted at once even when held (order).
    DoubleGrant,
    /// `c_{k-1}` is never granted from the queue (status): a direct grant when
    /// the lock is free still happens, and a waiter queued behind `c_{k-1}` is
    /// stranded too (the lock is left free, nothing is granted) — round 01 n3.
    NeverGrant,
    /// Every grant to `c_{k-1}` carries `j + 1` (value).
    WrongRound,
    /// A LIFO waiter stack (the control; identical to FIFO at `k = 2`).
    Lifo,
}

/// The Impl: a coordinator with one mailbox and a waiter queue.
fn a3_impl(k: usize, q: usize, kind: A3Coord) -> Prog {
    prog(move || {
        let (tx, rx) = typed_chan::<Msg>();
        let (reply_tx, reply_rx) = a3_reply_channels(k);
        named("coordinator", move || {
            let mut round = vec![0usize; k];
            let mut held = false;
            let mut queue: std::collections::VecDeque<usize> = std::collections::VecDeque::new();
            let grant = |i: usize, round: &mut Vec<usize>| {
                let j = if kind == A3Coord::WrongRound && i == k - 1 {
                    round[i] + 1
                } else {
                    round[i]
                };
                reply_tx[i].send_msg(Grant(i, j));
                round[i] += 1;
            };
            for _ in 0..2 * k * q {
                match rx.recv_msg_block() {
                    Msg::Acq(i) => {
                        if !held || kind == A3Coord::DoubleGrant {
                            held = true;
                            grant(i, &mut round);
                        } else {
                            queue.push_back(i);
                        }
                    }
                    Msg::Rel(_) => {
                        let next = if kind == A3Coord::Lifo {
                            queue.pop_back()
                        } else {
                            queue.pop_front()
                        };
                        match next {
                            Some(n) if kind == A3Coord::NeverGrant && n == k - 1 => {
                                held = false;
                            }
                            Some(n) => grant(n, &mut round),
                            None => held = false,
                        }
                    }
                }
            }
        });
        a3_spawn_clients(q, reply_rx, tx.clone(), tx);
    })
}

// ------------------------------------------------------ the arguments ----

const PROGRAM: &str = "a3-lock";
const USAGE: &str = "`<variant> <k> <q>`, variant one of fifo, double-grant, never-grant, \
    wrong-round, control-lifo";

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
    kind: A3Coord,
    k: usize,
    q: usize,
}

fn parse(args: &[String]) -> Args {
    arity(args, 3);
    let kind = match args[0].as_str() {
        "fifo" => A3Coord::Fifo,
        "double-grant" => A3Coord::DoubleGrant,
        "never-grant" => A3Coord::NeverGrant,
        "wrong-round" => A3Coord::WrongRound,
        "control-lifo" => A3Coord::Lifo,
        v => panic!("{PROGRAM}: unknown variant `{v}`; ARGS are {USAGE}"),
    };
    Args {
        kind,
        k: num(args, 1, "k"),
        q: num(args, 2, "q"),
    }
}

fn clients(k: usize) -> Vec<String> {
    (0..k).map(|i| format!("c{i}")).collect()
}

fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &|a| clients(parse(a).k),
        spec: &|a| {
            let p = parse(a);
            a3_spec(p.k, p.q)
        },
        imp: &|a| {
            let p = parse(a);
            a3_impl(p.k, p.q, p.kind)
        },
    })
}
