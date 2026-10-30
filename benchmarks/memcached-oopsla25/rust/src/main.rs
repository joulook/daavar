//! Benchmark memcached-oopsla25: the Memcached case study of Bílý, Pereira, Müller (OOPSLA 2025,
//! §5.1, `refinement-lib/case-study-memcached/`). Specification: the model's one cache
//! (`MemcacheModel`, `cache_process`) served per key (rule 2: one thread per independently served
//! unit), each client's commands and responses in FIFO order. Implementation: v3, one connection
//! handler per client (`handle_client`) and one storage thread per bucket (`Storage::process`
//! locks only the key's bucket; the thread plays the bucket's lock).
//!
//! Usage: `<spec|impl|conf> <Q> [fifo|bag|causal] [v3|shared-bucket|lost-set]`; `Q` = commands
//! per client (1..=3), 2 clients, 2 keys. Default model `fifo` (p2p: TCP), default variant `v3`.
//! `spec`/`impl` print `execs=<complete> blocked=<blocked>` of plain Must; `conf` runs
//! TraceForge's conformance check of `impl <variant>` against `spec` and prints the verdict.
//!
//! Visible threads: `c0`, `c1`. Invisible: `k0`, `k1` (spec); `b0`, `b1` (or `b0` alone for
//! `shared-bucket`), `w0`, `w1` (impl). Every thread receives only on its own named channel.
use traceforge::channel::{self, Receiver, Sender};
use traceforge::conformance::{self, ConfBuilder, Engine};
use traceforge::thread::{self, JoinHandle};
use traceforge::*;

const N: usize = 2; // clients (connections)
const KEYS: usize = 2;

/// `AbsCommand` without expiry; keys and values are a finite domain (`u8`).
#[derive(Clone, Copy, Debug, PartialEq)]
enum Op {
    Get { key: u8 },
    Set { key: u8, value: u8 },
}

/// `AbsResponse`: `Set` (`STORED`) and `Get { key, value }`.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Res {
    Stored,
    Got { key: u8, value: Option<u8> },
}

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// A command of connection `c` (the connection id the model's actions carry).
    Cmd { c: u8, op: Op },
    Res(Res),
}

/// Client `i`'s commands; the first `Q` are issued.
const SCRIPT: [[Op; 3]; N] = [
    [Op::Set { key: 0, value: 1 }, Op::Get { key: 1 }, Op::Get { key: 0 }],
    [Op::Set { key: 1, value: 2 }, Op::Get { key: 1 }, Op::Set { key: 0, value: 2 }],
];

fn key_of(op: Op) -> u8 {
    match op {
        Op::Get { key } | Op::Set { key, .. } => key,
    }
}

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn inbox(role: &'static str, i: usize) -> (Sender<Msg>, Receiver<Msg>) {
    channel::Builder::<Msg>::new().with_name((role, i)).build()
}

/// `cache_process` for Set and Get on a store holding every key (no expiry, so Get never
/// removes).
fn process(store: &mut [Option<u8>; KEYS], op: Op) -> Res {
    match op {
        Op::Set { key, value } => {
            store[key as usize] = Some(value);
            Res::Stored
        }
        Op::Get { key } => Res::Got { key, value: store[key as usize] },
    }
}

/// A server thread body: `n` commands, each read blocking from `("role", me)`, processed, and
/// answered to `("reply_to", c)`.
fn serve(role: &'static str, me: usize, reply_to: &'static str, n: usize) {
    let rx = inbox(role, me).1;
    let mut store: [Option<u8>; KEYS] = [None; KEYS];
    for _ in 0..n {
        let Msg::Cmd { c, op } = rx.recv_msg_block() else { unreachable!() };
        let res = process(&mut store, op);
        inbox(reply_to, c as usize).0.send_msg(Msg::Res(res));
    }
}

/// A client: `SendCommand` then `ReceiveResponse`, `q` times. In the implementation the command
/// goes to its connection handler `w{i}`; in the specification to the server of its key.
fn client(i: usize, q: usize, spec: bool) {
    let rx = inbox("c", i).1;
    for j in 0..q {
        let op = SCRIPT[i][j];
        let to = if spec { inbox("k", key_of(op) as usize).0 } else { inbox("w", i).0 };
        to.send_msg(Msg::Cmd { c: i as u8, op });
        let Msg::Res(_) = rx.recv_msg_block() else { unreachable!() };
    }
}

fn count<F: Fn(Op) -> bool>(q: usize, f: F) -> usize {
    (0..N).map(|i| (0..q).filter(|&j| f(SCRIPT[i][j])).count()).sum()
}

/// The model: one cache, served per key. Server `k{key}` serves every command on its key.
fn spec(q: usize) {
    let mut hs = Vec::new();
    for k in 0..KEYS {
        let n = count(q, |op| key_of(op) as usize == k);
        hs.push(named(&format!("k{k}"), move || serve("k", k, "c", n)));
    }
    let cs: Vec<_> = (0..N).map(|i| named(&format!("c{i}"), move || client(i, q, true))).collect();
    for c in cs {
        c.join().unwrap();
    }
    for h in hs {
        h.join().unwrap();
    }
}

/// The bucket of a command: `hash` (identity on the two keys, `B = 2`) for `v3`; one bucket
/// for `shared-bucket`; `lost-set` hashes a Set to the other bucket (wrong hash on one command
/// kind).
fn route(variant: &str, op: Op) -> usize {
    match (variant, op) {
        ("shared-bucket", _) => 0,
        ("lost-set", Op::Set { key, .. }) => (key as usize + 1) % KEYS,
        _ => key_of(op) as usize,
    }
}

fn buckets(variant: &str) -> usize {
    if variant == "shared-bucket" { 1 } else { KEYS }
}

fn imp(q: usize, variant: &'static str) {
    let mut hs = Vec::new();
    // Storage: one thread per bucket (the bucket's MutexWrap and StorageBucket guard).
    for b in 0..buckets(variant) {
        let n = count(q, |op| route(variant, op) == b);
        hs.push(named(&format!("b{b}"), move || serve("b", b, "w", n)));
    }
    // handle_client: read a command, Storage::process on its bucket, write the response.
    for w in 0..N {
        hs.push(named(&format!("w{w}"), move || {
            let rx = inbox("w", w).1;
            for _ in 0..q {
                let Msg::Cmd { c, op } = rx.recv_msg_block() else { unreachable!() };
                inbox("b", route(variant, op)).0.send_msg(Msg::Cmd { c, op });
                let Msg::Res(res) = rx.recv_msg_block() else { unreachable!() };
                inbox("c", w).0.send_msg(Msg::Res(res));
            }
        }));
    }
    let cs: Vec<_> = (0..N).map(|i| named(&format!("c{i}"), move || client(i, q, false))).collect();
    for c in cs {
        c.join().unwrap();
    }
    for h in hs {
        h.join().unwrap();
    }
}

fn variant_of(s: &str) -> &'static str {
    match s {
        "v3" => "v3",
        "shared-bucket" => "shared-bucket",
        "lost-set" => "lost-set",
        v => panic!("unknown variant {v}; one of v3, shared-bucket, lost-set"),
    }
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let side = a.get(1).map(String::as_str).unwrap_or("spec").to_string();
    let q: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(1);
    assert!((1..=3).contains(&q), "Q must be 1..=3");
    let model = match a.get(3).map(String::as_str).unwrap_or("fifo") {
        "bag" => ConsType::Bag,
        "causal" => ConsType::Causal,
        _ => ConsType::FIFO,
    };
    let variant = variant_of(a.get(4).map(String::as_str).unwrap_or("v3"));
    if side == "conf" {
        let cfg = ConfBuilder::new()
            .config(Config::builder().with_cons_type(model).with_verbose(0).build())
            .cons_type(model)
            .visible_threads(["c0", "c1"])
            .engine(match a.get(5).map(String::as_str).unwrap_or("stateful") {
                "enumerator" => Engine::Enumerator,
                _ => Engine::Stateful,
            })
            .all_reports(a.iter().any(|x| x == "all"))
            .build()
            .expect("conformance config");
        let v = conformance::verify(cfg, move || imp(q, variant), move || spec(q)).expect("conformance run");
        println!("{v}");
        return;
    }
    let conf = Config::builder().with_cons_type(model).with_verbose(0).build();
    let stats = match side.as_str() {
        "impl" => verify(conf, move || imp(q, variant)),
        _ => verify(conf, move || spec(q)),
    };
    println!("execs={} blocked={}", stats.execs, stats.block);
}
