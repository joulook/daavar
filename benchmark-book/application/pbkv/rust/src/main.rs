//! TraceForge's A2 (`P5-APPS`): a key-value store, one Spec and three Impls (`pb`, `pb-br`,
//! `sh`) with their mutants and controls, as an ordinary Must program run by TraceForge's shared
//! main (`traceforge::program`).
//!
//! Ported for H (`criteria/H.md` H-0.3; `log/dev/H.derived.md` §1.2–§1.4) from
//! `traceforge/src/conformance/grid.rs` at `a3a7e1b`, lines 3270–3561 (`Req`, `Rep`,
//! `Internal`, `a2_op` and the helpers, `a2_spec`, `A2Pb`, `a2_pb`, `A2Br`, `a2_pb_br`, `a2_sh`;
//! the unused `a2_client` is left out). The program code is copied unchanged apart from
//! `crate::` → `traceforge::` and the dropped `pub(super)`. `typed_chan` is `grid.rs:3080-3085`
//! with the crate-internal `cons_to_model(ConsType::FIFO)` written as its value,
//! `CommunicationModel::LocalOrder` (derivation §1.3); `prog`/`Prog` and `named` are
//! `grid.rs:60-70`.
//!
//! ARGS: `<variant> <k> <q> [<s>]`, `s` present exactly for `sh` and `misroute`
//! * spec: `a2_spec(k, q)` for every variant;
//! * impl, by variant (the registry's mapping, `grid.rs:3944-3986`):
//!   `pb` = `a2_pb(k, q, Correct)`, `pb-br` = `a2_pb_br(k, q, Correct)`,
//!   `early-ack` = `a2_pb_br(k, q, EarlyAck)`, `stale-get` = `a2_pb(k, q, StaleGet)`,
//!   `silent-put` = `a2_pb(k, q, SilentPut)`,
//!   `control-early-ack-primary` = `a2_pb(k, q, A2Pb::EarlyAck)`,
//!   `control-early-ack-fwd` = `a2_pb_br(k, q, ControlForwardedEarlyAck)`,
//!   `sh` = `a2_sh(k, q, s, false)`, `misroute` = `a2_sh(k, q, s, true)`.
//!
//! Visible threads: `c0 .. c{k-1}`.
//!
//! Fixture names: `apps/a2/<variant>/spec/k<k>q<q>` → `a2-store <variant> <k> <q>`;
//! `apps/a2/{sh,misroute}/spec/k<k>q<q>s<s>` → `a2-store {sh|misroute} <k> <q> <s>`.
//!
//! Options every row spells (never set here; they are the shared main's `--model`, `--seed`,
//! `--threshold`): `--model fifo --seed 0`, and **no** `--threshold` — the engine's default
//! thread bound (1 000), as `apps_config` (`grid.rs:3087-3092`) built the fixtures' `Config`.
//! Example: `<bin> row --model fifo --seed 0 -- sh 2 2 1`.

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

// ---------------------------------------------------------------- A2 ----

#[derive(Clone, PartialEq, Debug)]
enum Req {
    Put { c: usize, k: usize, v: i32 },
    Get { c: usize, k: usize },
}

#[derive(Clone, PartialEq, Debug)]
enum Rep {
    Ack,
    Val(i32),
    None,
}

#[derive(Clone, PartialEq, Debug)]
enum Internal {
    Fwd(Req),
    AckPut,
}

/// Client `c_i`'s op `j`: a put iff `i+j` is even, key `(⌊j/2⌋ + ⌊i/2⌋) mod 2`,
/// value `10i+j`.
fn a2_op(i: usize, j: usize) -> Req {
    let k = (j / 2 + i / 2) % 2;
    if (i + j).is_multiple_of(2) {
        Req::Put {
            c: i,
            k,
            v: (10 * i + j) as i32,
        }
    } else {
        Req::Get { c: i, k }
    }
}

fn a2_is_put(r: &Req) -> bool {
    matches!(r, Req::Put { .. })
}

fn a2_key(r: &Req) -> usize {
    match r {
        Req::Put { k, .. } | Req::Get { k, .. } => *k,
    }
}

type RepTx = traceforge::channel::Sender<Rep>;

fn a2_lookup(map: &std::collections::HashMap<usize, i32>, k: usize) -> Rep {
    map.get(&k).map_or(Rep::None, |v| Rep::Val(*v))
}

/// The closed-loop clients: `c_i` sends its script to `send_to(req)` and blocks
/// on its reply channel after each request.
fn a2_spawn_clients(
    q: usize,
    reply_rx: Vec<traceforge::channel::Receiver<Rep>>,
    route: impl Fn(&Req) -> traceforge::channel::Sender<Req> + Send + Sync + 'static,
) {
    let route = Arc::new(route);
    for (i, rx) in reply_rx.into_iter().enumerate() {
        let route = Arc::clone(&route);
        named(&format!("c{i}"), move || {
            for j in 0..q {
                let r = a2_op(i, j);
                route(&r).send_msg(r.clone());
                let _rep: Rep = rx.recv_msg_block();
            }
        });
    }
}

fn a2_reply_channels(k: usize) -> (Vec<RepTx>, Vec<traceforge::channel::Receiver<Rep>>) {
    let mut txs = Vec::with_capacity(k);
    let mut rxs = Vec::with_capacity(k);
    for _ in 0..k {
        let (tx, rx) = typed_chan::<Rep>();
        txs.push(tx);
        rxs.push(rx);
    }
    (txs, rxs)
}

/// The Spec: one invisible `store`, serving in arrival order.
fn a2_spec(k: usize, q: usize) -> Prog {
    prog(move || {
        let (tx_req, rx_req) = typed_chan::<Req>();
        let (reply_tx, reply_rx) = a2_reply_channels(k);
        named("store", move || {
            let mut map = std::collections::HashMap::new();
            for _ in 0..k * q {
                match rx_req.recv_msg_block() {
                    Req::Put { c, k, v } => {
                        map.insert(k, v);
                        reply_tx[c].send_msg(Rep::Ack);
                    }
                    Req::Get { c, k } => reply_tx[c].send_msg(a2_lookup(&map, k)),
                }
            }
        });
        a2_spawn_clients(q, reply_rx, move |_| tx_req.clone());
    })
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum A2Pb {
    /// Primary-backup: forward, wait for the ack, reply.
    Correct,
    /// Reply `Ack` right after forwarding; the ack is read before the next
    /// request (the control with primary-served gets: unobservable).
    EarlyAck,
    /// The primary drops every put from its map (wrong value at a later get).
    StaleGet,
    /// The first put the primary receives is never answered (status).
    SilentPut,
}

/// `pb`: `primary` (request channel), `backup` (channel from the primary, a
/// dedicated ack channel back), gets served by the primary.
fn a2_pb(k: usize, q: usize, kind: A2Pb) -> Prog {
    prog(move || {
        let (tx_req, rx_req) = typed_chan::<Req>();
        let (tx_b, rx_b) = typed_chan::<Internal>();
        let (tx_ack, rx_ack) = typed_chan::<Internal>();
        let (reply_tx, reply_rx) = a2_reply_channels(k);
        let n_puts = (0..k)
            .flat_map(|i| (0..q).map(move |j| a2_op(i, j)))
            .filter(a2_is_put)
            .count();
        named("primary", move || {
            let mut map = std::collections::HashMap::new();
            let mut first_put = true;
            for _ in 0..k * q {
                match rx_req.recv_msg_block() {
                    r @ Req::Put { c, k, v } => {
                        if kind != A2Pb::StaleGet {
                            map.insert(k, v);
                        }
                        tx_b.send_msg(Internal::Fwd(r));
                        if kind == A2Pb::EarlyAck {
                            reply_tx[c].send_msg(Rep::Ack);
                            let _ack: Internal = rx_ack.recv_msg_block();
                        } else {
                            let _ack: Internal = rx_ack.recv_msg_block();
                            let silent = kind == A2Pb::SilentPut && first_put;
                            if !silent {
                                reply_tx[c].send_msg(Rep::Ack);
                            }
                        }
                        first_put = false;
                    }
                    Req::Get { c, k } => reply_tx[c].send_msg(a2_lookup(&map, k)),
                }
            }
        });
        named("backup", move || {
            let mut map = std::collections::HashMap::new();
            for _ in 0..n_puts {
                if let Internal::Fwd(Req::Put { k, v, .. }) = rx_b.recv_msg_block() {
                    map.insert(k, v);
                }
                tx_ack.send_msg(Internal::AckPut);
            }
        });
        a2_spawn_clients(q, reply_rx, move |_| tx_req.clone());
    })
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum A2Br {
    /// Gets go directly to the backup; the primary waits for the ack.
    Correct,
    /// Gets go directly to the backup; the primary replies `Ack` before the ack
    /// (the ack-before-commit mutant: (O) at (2,1), (V) at (1,2)).
    EarlyAck,
    /// Gets are forwarded through the primary, early ack (the second control:
    /// the backup reads in the primary's order — unobservable).
    ControlForwardedEarlyAck,
}

/// `pb-br`: the backup's request channel is `typed_chan::<Req>()` fed by the
/// primary (forwarded puts, unwrapped) and by every client (gets); the backup
/// replies to clients directly and acks puts on `Internal::AckPut`.
fn a2_pb_br(k: usize, q: usize, kind: A2Br) -> Prog {
    prog(move || {
        let (tx_req, rx_req) = typed_chan::<Req>();
        let (tx_bk, rx_bk) = typed_chan::<Req>();
        let (tx_ack, rx_ack) = typed_chan::<Internal>();
        let (reply_tx, reply_rx) = a2_reply_channels(k);
        let reply_tx_b = reply_tx.clone();
        let tx_bk_primary = tx_bk.clone();
        named("primary", move || {
            let n = (0..k)
                .flat_map(|i| (0..q).map(move |j| a2_op(i, j)))
                .filter(|r| a2_is_put(r) || kind == A2Br::ControlForwardedEarlyAck)
                .count();
            for _ in 0..n {
                match rx_req.recv_msg_block() {
                    r @ Req::Put { c, .. } => {
                        tx_bk_primary.send_msg(r);
                        if kind == A2Br::Correct {
                            let _ack: Internal = rx_ack.recv_msg_block();
                            reply_tx[c].send_msg(Rep::Ack);
                        } else {
                            reply_tx[c].send_msg(Rep::Ack);
                            let _ack: Internal = rx_ack.recv_msg_block();
                        }
                    }
                    r @ Req::Get { .. } => tx_bk_primary.send_msg(r),
                }
            }
        });
        named("backup", move || {
            let mut map = std::collections::HashMap::new();
            for _ in 0..k * q {
                match rx_bk.recv_msg_block() {
                    Req::Put { k, v, .. } => {
                        map.insert(k, v);
                        tx_ack.send_msg(Internal::AckPut);
                    }
                    Req::Get { c, k } => reply_tx_b[c].send_msg(a2_lookup(&map, k)),
                }
            }
        });
        a2_spawn_clients(q, reply_rx, move |r| {
            if a2_is_put(r) || kind == A2Br::ControlForwardedEarlyAck {
                tx_req.clone()
            } else {
                tx_bk.clone()
            }
        });
    })
}

/// `sh`: a router forwards each request to shard `key mod s` (`misroute`: a
/// get to `(key + 1) mod s`); shards reply to clients directly.
fn a2_sh(k: usize, q: usize, s: usize, misroute: bool) -> Prog {
    prog(move || {
        let (tx_req, rx_req) = typed_chan::<Req>();
        let (reply_tx, reply_rx) = a2_reply_channels(k);
        let mut shard_tx = Vec::with_capacity(s);
        let mut shard_rx = Vec::with_capacity(s);
        for _ in 0..s {
            let (tx, rx) = typed_chan::<Req>();
            shard_tx.push(tx);
            shard_rx.push(rx);
        }
        let route = move |r: &Req| -> usize {
            let key = a2_key(r);
            if misroute && !a2_is_put(r) {
                (key + 1) % s
            } else {
                key % s
            }
        };
        let per_shard: Vec<usize> = (0..s)
            .map(|x| {
                (0..k)
                    .flat_map(|i| (0..q).map(move |j| a2_op(i, j)))
                    .filter(|r| route(r) == x)
                    .count()
            })
            .collect();
        named("router", move || {
            for _ in 0..k * q {
                let r = rx_req.recv_msg_block();
                shard_tx[route(&r)].send_msg(r);
            }
        });
        for (x, rx) in shard_rx.into_iter().enumerate() {
            let reply_tx = reply_tx.clone();
            let n = per_shard[x];
            named(&format!("shard{x}"), move || {
                let mut map = std::collections::HashMap::new();
                for _ in 0..n {
                    match rx.recv_msg_block() {
                        Req::Put { c, k, v } => {
                            map.insert(k, v);
                            reply_tx[c].send_msg(Rep::Ack);
                        }
                        Req::Get { c, k } => reply_tx[c].send_msg(a2_lookup(&map, k)),
                    }
                }
            });
        }
        a2_spawn_clients(q, reply_rx, move |_| tx_req.clone());
    })
}

// ------------------------------------------------------ the arguments ----

const PROGRAM: &str = "a2-store";
const USAGE: &str = "`<variant> <k> <q> [<s>]` (`<s>` exactly for sh and misroute), variant one \
    of pb, pb-br, early-ack, stale-get, silent-put, control-early-ack-primary, \
    control-early-ack-fwd, sh, misroute";

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
    Pb(A2Pb),
    PbBr(A2Br),
    Sh { misroute: bool },
}

struct Args {
    variant: Variant,
    k: usize,
    q: usize,
    /// The shard count, for `sh` and `misroute` only.
    s: usize,
}

fn parse(args: &[String]) -> Args {
    let Some(v) = args.first() else {
        panic!("{PROGRAM}: no arguments; ARGS are {USAGE}");
    };
    let variant = match v.as_str() {
        "pb" => Variant::Pb(A2Pb::Correct),
        "pb-br" => Variant::PbBr(A2Br::Correct),
        "early-ack" => Variant::PbBr(A2Br::EarlyAck),
        "stale-get" => Variant::Pb(A2Pb::StaleGet),
        "silent-put" => Variant::Pb(A2Pb::SilentPut),
        "control-early-ack-primary" => Variant::Pb(A2Pb::EarlyAck),
        "control-early-ack-fwd" => Variant::PbBr(A2Br::ControlForwardedEarlyAck),
        "sh" => Variant::Sh { misroute: false },
        "misroute" => Variant::Sh { misroute: true },
        v => panic!("{PROGRAM}: unknown variant `{v}`; ARGS are {USAGE}"),
    };
    let sharded = matches!(variant, Variant::Sh { .. });
    arity(args, if sharded { 4 } else { 3 });
    Args {
        variant,
        k: num(args, 1, "k"),
        q: num(args, 2, "q"),
        s: if sharded { num(args, 3, "s") } else { 0 },
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
            a2_spec(p.k, p.q)
        },
        imp: &|a| {
            let p = parse(a);
            match p.variant {
                Variant::Pb(kind) => a2_pb(p.k, p.q, kind),
                Variant::PbBr(kind) => a2_pb_br(p.k, p.q, kind),
                Variant::Sh { misroute } => a2_sh(p.k, p.q, p.s, misroute),
            }
        },
    })
}
