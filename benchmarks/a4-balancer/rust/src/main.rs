//! TraceForge's A4 (`P5-APPS`): a load-balancing server with two Specs, as an ordinary Must
//! program run by TraceForge's shared main (`traceforge::program`).
//!
//! Ported for H (`criteria/H.md` H-0.3; `log/dev/H.derived.md` §1.2–§1.4) from
//! `traceforge/src/conformance/grid.rs` at `a3a7e1b`, lines 3748–3912 (`A4Req`, `A4Reply`, the
//! channel and spawn helpers, `A4Spec`, `a4_spec`, `A4Policy`, `A4Fault`, `a4_impl`). The
//! program code is copied unchanged apart from `crate::` → `traceforge::` and the dropped
//! `pub(super)`. `typed_chan` is `grid.rs:3080-3085` with the crate-internal
//! `cons_to_model(ConsType::FIFO)` written as its value, `CommunicationModel::LocalOrder`
//! (derivation §1.3); `prog`/`Prog` and `named` are `grid.rs:60-70`.
//!
//! ARGS: `<impl> <spec> <k> <q> <m>`
//! * impl, as `(A4Policy, A4Fault)`: `hash` = (Hash, None), `rr` = (RoundRobin, None),
//!   `pair-swap-hash` = (Hash, PairSwap), `pair-swap-rr` = (RoundRobin, PairSwap),
//!   `drop` = (RoundRobin, Drop), `hash-drop` = (Hash, Drop) — the registry's mapping
//!   (`grid.rs:4014-4068`) and APPTAB's (`grid.rs:5644-5648`);
//! * spec: `a4a` = `A4Spec::A4a`, `a4b` = `A4Spec::A4b`;
//! * impl side: `a4_impl(k, q, m, policy, fault)`; spec side: `a4_spec(k, q, m, spec)`.
//!   The registry builds the pair-swap impls at `k = 2` only (`PairSwap` is "`k = 2` only");
//!   no combination is refused here.
//!
//! Visible threads: `c0 .. c{k-1}`.
//!
//! Fixture names: `apps/a4/<impl>/<spec>/k<k>q<q>m<m>` → `a4-balancer <impl> <spec> <k> <q> <m>`.
//!
//! Options every row spells (never set here; they are the shared main's `--model`, `--seed`,
//! `--threshold`): `--model fifo --seed 0`, and **no** `--threshold` — the engine's default
//! thread bound (1 000), as `apps_config` (`grid.rs:3087-3092`) built the fixtures' `Config`.
//! Example: `<bin> row --model fifo --seed 0 -- pair-swap-hash a4b 2 2 3`.

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

/// A typed channel under `ConsType::FIFO`'s communication model (`LocalOrder`),
/// created by the calling thread (criterion 2; `chan()`/`fifo_chan()` untouched).
fn typed_chan<T: Send + PartialEq + Clone + std::fmt::Debug + 'static>(
) -> (traceforge::channel::Sender<T>, traceforge::channel::Receiver<T>) {
    traceforge::channel::Builder::<T>::new()
        .with_comm(traceforge::loc::CommunicationModel::LocalOrder)
        .build()
}

// ---------------------------------------------------------------- A4 ----

#[derive(Clone, PartialEq, Debug)]
struct A4Req(usize, usize);

#[derive(Clone, PartialEq, Debug)]
struct A4Reply(usize, usize);

fn a4_reply_channels(
    k: usize,
) -> (
    Vec<traceforge::channel::Sender<A4Reply>>,
    Vec<traceforge::channel::Receiver<A4Reply>>,
) {
    let mut txs = Vec::with_capacity(k);
    let mut rxs = Vec::with_capacity(k);
    for _ in 0..k {
        let (tx, rx) = typed_chan::<A4Reply>();
        txs.push(tx);
        rxs.push(rx);
    }
    (txs, rxs)
}

/// The `m` servers' request channels, created by `main` before any spawn.
fn a4_server_channels(
    m: usize,
) -> (
    Vec<traceforge::channel::Sender<A4Req>>,
    Vec<traceforge::channel::Receiver<A4Req>>,
) {
    let mut txs = Vec::with_capacity(m);
    let mut rxs = Vec::with_capacity(m);
    for _ in 0..m {
        let (tx, rx) = typed_chan::<A4Req>();
        txs.push(tx);
        rxs.push(rx);
    }
    (txs, rxs)
}

/// `m` servers, each on its own request channel, replying `Reply(x, j)` to
/// the client directly; they loop until the run ends (their request count
/// depends on the routing). Spawned **after** the balancer or dispatcher
/// (the encoding's order; gate 3 T1).
fn a4_spawn_servers(
    rxs: Vec<traceforge::channel::Receiver<A4Req>>,
    reply_tx: &[traceforge::channel::Sender<A4Reply>],
) {
    for (x, rx) in rxs.into_iter().enumerate() {
        let reply_tx = reply_tx.to_vec();
        named(&format!("s{x}"), move || loop {
            let A4Req(i, j) = rx.recv_msg_block();
            reply_tx[i].send_msg(A4Reply(x, j));
        });
    }
}

fn a4_spawn_clients(
    q: usize,
    reply_rx: Vec<traceforge::channel::Receiver<A4Reply>>,
    tx_req: traceforge::channel::Sender<A4Req>,
) {
    for (i, rx) in reply_rx.into_iter().enumerate() {
        let tx = tx_req.clone();
        named(&format!("c{i}"), move || {
            for j in 0..q {
                tx.send_msg(A4Req(i, j));
                let _r: A4Reply = rx.recv_msg_block();
            }
        });
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum A4Spec {
    /// Affinity: one server per client, chosen by `nondet` on its first request.
    A4a,
    /// Relay: a server chosen by `nondet` per request.
    A4b,
}

fn a4_spec(k: usize, q: usize, m: usize, kind: A4Spec) -> Prog {
    prog(move || {
        let (tx_req, rx_req) = typed_chan::<A4Req>();
        let (reply_tx, reply_rx) = a4_reply_channels(k);
        let (servers, server_rx) = a4_server_channels(m);
        named("dispatcher", move || {
            let mut affinity: Vec<Option<usize>> = vec![None; k];
            for _ in 0..k * q {
                let r = rx_req.recv_msg_block();
                let x = match kind {
                    A4Spec::A4b => (0..m).nondet(),
                    A4Spec::A4a => match affinity[r.0] {
                        Some(x) => x,
                        None => {
                            let x = (0..m).nondet();
                            affinity[r.0] = Some(x);
                            x
                        }
                    },
                };
                servers[x].send_msg(r);
            }
        });
        a4_spawn_servers(server_rx, &reply_tx);
        a4_spawn_clients(q, reply_rx, tx_req);
    })
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum A4Policy {
    /// Request number `p` to `s_{p mod m}`.
    RoundRobin,
    /// `c_i` to `s_{i mod m}`.
    Hash,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum A4Fault {
    None,
    /// `c_{k-1}`'s last request is dropped (status).
    Drop,
    /// `k = 2` only: the balancer takes both clients' `j`-th requests and
    /// forwards the later-arrived first (the control).
    PairSwap,
}

fn a4_impl(k: usize, q: usize, m: usize, policy: A4Policy, fault: A4Fault) -> Prog {
    prog(move || {
        let (tx_req, rx_req) = typed_chan::<A4Req>();
        let (reply_tx, reply_rx) = a4_reply_channels(k);
        let (servers, server_rx) = a4_server_channels(m);
        named("balancer", move || {
            let mut p = 0usize;
            let mut forward = |r: A4Req, servers: &Vec<traceforge::channel::Sender<A4Req>>| {
                let x = match policy {
                    A4Policy::RoundRobin => p % m,
                    A4Policy::Hash => r.0 % m,
                };
                p += 1;
                servers[x].send_msg(r);
            };
            match fault {
                A4Fault::PairSwap => {
                    for _ in 0..q {
                        let a = rx_req.recv_msg_block();
                        let b = rx_req.recv_msg_block();
                        forward(b, &servers);
                        forward(a, &servers);
                    }
                }
                _ => {
                    for _ in 0..k * q {
                        let r = rx_req.recv_msg_block();
                        let dropped = fault == A4Fault::Drop && r.0 == k - 1 && r.1 == q - 1;
                        if !dropped {
                            forward(r, &servers);
                        }
                    }
                }
            }
        });
        a4_spawn_servers(server_rx, &reply_tx);
        a4_spawn_clients(q, reply_rx, tx_req);
    })
}

// ------------------------------------------------------ the arguments ----

const PROGRAM: &str = "a4-balancer";
const USAGE: &str = "`<impl> <spec> <k> <q> <m>`, impl one of hash, rr, pair-swap-hash, \
    pair-swap-rr, drop, hash-drop; spec one of a4a, a4b";

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
    policy: A4Policy,
    fault: A4Fault,
    spec: A4Spec,
    k: usize,
    q: usize,
    m: usize,
}

fn parse(args: &[String]) -> Args {
    arity(args, 5);
    let (policy, fault) = match args[0].as_str() {
        "hash" => (A4Policy::Hash, A4Fault::None),
        "rr" => (A4Policy::RoundRobin, A4Fault::None),
        "pair-swap-hash" => (A4Policy::Hash, A4Fault::PairSwap),
        "pair-swap-rr" => (A4Policy::RoundRobin, A4Fault::PairSwap),
        "drop" => (A4Policy::RoundRobin, A4Fault::Drop),
        "hash-drop" => (A4Policy::Hash, A4Fault::Drop),
        v => panic!("{PROGRAM}: unknown impl `{v}`; ARGS are {USAGE}"),
    };
    let spec = match args[1].as_str() {
        "a4a" => A4Spec::A4a,
        "a4b" => A4Spec::A4b,
        v => panic!("{PROGRAM}: unknown spec `{v}`; ARGS are {USAGE}"),
    };
    Args {
        policy,
        fault,
        spec,
        k: num(args, 2, "k"),
        q: num(args, 3, "q"),
        m: num(args, 4, "m"),
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
            a4_spec(p.k, p.q, p.m, p.spec)
        },
        imp: &|a| {
            let p = parse(a);
            a4_impl(p.k, p.q, p.m, p.policy, p.fault)
        },
    })
}
