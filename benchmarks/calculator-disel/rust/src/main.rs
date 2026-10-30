//! Benchmark calculator-disel (candidate N2): Disel's calculator. One specification (a server that
//! answers `Resp(f(args), args)` to each `Req(args)`, `f [x; y] = x + y`) and three
//! implementations (batched, memoising, delegating), each with a seeded fault.
//!
//! Usage: `<spec|impl> <K> [fifo|bag|causal] [variant]`; `K` = requests per client (2 clients),
//! default model `bag` (asyn, Disel's soup), default variant `batched`. Variants (impl side):
//! `batched`, `batched-swap`, `memo`, `memo-ignore-args`, `delegating`, `delegating-prev`.
//! Prints `execs=<complete> blocked=<blocked>`.
use traceforge::thread::{self, ThreadId};
use traceforge::*;

const N: usize = 2; // clients
const TAG_REQ: u32 = 1;
const TAG_RESP: u32 = 2;
/// The argument domain: client `i`'s `j`-th request has `ARGS[(i + j) % 2]`.
const ARGS: [(u8, u8); 2] = [(1, 2), (2, 2)];

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// Disel `req` (tag 0): the requester and `args = [x; y]`.
    Req { from: ThreadId, x: u8, y: u8 },
    /// Disel `resp` (tag 1): `ans :: args`.
    Resp { ans: u8, x: u8, y: u8 },
}

fn f(a: (u8, u8)) -> u8 {
    a.0 + a.1
}

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn recv_req() -> (ThreadId, (u8, u8)) {
    match recv_tagged_msg_block::<_, Msg>(|_, t| t == Some(TAG_REQ)) {
        Msg::Req { from, x, y } => (from, (x, y)),
        _ => unreachable!(),
    }
}

fn recv_resp() -> (u8, (u8, u8)) {
    match recv_tagged_msg_block::<_, Msg>(|_, t| t == Some(TAG_RESP)) {
        Msg::Resp { ans, x, y } => (ans, (x, y)),
        _ => unreachable!(),
    }
}

fn send_answer(to: ThreadId, ans: u8, args: (u8, u8)) {
    send_tagged_msg(to, TAG_RESP, Msg::Resp { ans, x: args.0, y: args.1 });
}

/// `compute_list_f`: `K` times `compute_f server args` = send `Req`, blocking receive `Resp`.
fn spawn_clients(k: usize, server: ThreadId) -> Vec<thread::JoinHandle<()>> {
    (0..N)
        .map(|i| {
            named(&format!("c{i}"), move || {
                let me = thread::current_id();
                for j in 0..k {
                    let args = ARGS[(i + j) % 2];
                    send_tagged_msg(server, TAG_REQ, Msg::Req { from: me, x: args.0, y: args.1 });
                    let _ = recv_resp();
                }
            })
        })
        .collect()
}

/// The memoising server body (`memoizing_server`), `n` requests; `ignore_args` is fault F1.
fn memo_loop(n: usize, ignore_args: bool) {
    let mut table: Vec<((u8, u8), u8)> = Vec::new();
    for _ in 0..n {
        let (from, args) = recv_req();
        let hit = if ignore_args {
            table.first().map(|e| e.1)
        } else {
            table.iter().find(|e| e.0 == args).map(|e| e.1)
        };
        let ans = match hit {
            Some(v) => v,
            None => {
                let v = f(args);
                table.push((args, v));
                v
            }
        };
        send_answer(from, ans, args);
    }
}

fn spec(k: usize) {
    let server = named("server", move || {
        for _ in 0..N * k {
            let (from, args) = recv_req();
            send_answer(from, f(args), args);
        }
    });
    let cs = spawn_clients(k, server.thread().id());
    for c in cs {
        c.join().unwrap();
    }
    server.join().unwrap();
}

fn imp(k: usize, variant: &str) {
    let total = N * k;
    let mut servers = Vec::new();
    let front: ThreadId = match variant {
        "batched" | "batched-swap" => {
            let swap = variant == "batched-swap";
            let h = named("batcher", move || {
                const BSIZE: usize = 2;
                for _ in 0..total / BSIZE {
                    // receive_req_loop: prepend each request (Disel's `acc`)
                    let mut acc: Vec<(ThreadId, (u8, u8))> = Vec::new();
                    for _ in 0..BSIZE {
                        let r = recv_req();
                        acc.insert(0, r);
                    }
                    // send_ans_loop; fault F2 sends entry e's answer to entry e+1's requester
                    for e in 0..BSIZE {
                        let (from, args) = acc[e];
                        let to = if swap { acc[(e + 1) % BSIZE].0 } else { from };
                        send_answer(to, f(args), args);
                    }
                }
            });
            let id = h.thread().id();
            servers.push(h);
            id
        }
        "memo" | "memo-ignore-args" => {
            let bad = variant == "memo-ignore-args";
            let h = named("memo", move || memo_loop(total, bad));
            let id = h.thread().id();
            servers.push(h);
            id
        }
        "delegating" | "delegating-prev" => {
            let prev = variant == "delegating-prev";
            let m = named("memo", move || memo_loop(total, false));
            let mid = m.thread().id();
            let d = named("delegator", move || {
                let me = thread::current_id();
                let mut last: Option<u8> = None;
                for _ in 0..total {
                    let (from, args) = recv_req();
                    // compute_f on the delegate (instance l2)
                    send_tagged_msg(mid, TAG_REQ, Msg::Req { from: me, x: args.0, y: args.1 });
                    let (ans, _) = recv_resp();
                    // fault F3: a one-slot cache answers with the previous request's result
                    let out = if prev { last.unwrap_or(ans) } else { ans };
                    last = Some(ans);
                    send_answer(from, out, args);
                }
            });
            let did = d.thread().id();
            servers.push(m);
            servers.push(d);
            did
        }
        _ => panic!("unknown variant {variant}"),
    };
    let cs = spawn_clients(k, front);
    for c in cs {
        c.join().unwrap();
    }
    for s in servers {
        s.join().unwrap();
    }
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let side = a.get(1).map(String::as_str).unwrap_or("spec").to_string();
    let n: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(1);
    let model = match a.get(3).map(String::as_str).unwrap_or("bag") {
        "fifo" => ConsType::FIFO,
        "causal" => ConsType::Causal,
        _ => ConsType::Bag,
    };
    let variant = a.get(4).cloned().unwrap_or_else(|| "batched".to_string());
    let conf = Config::builder().with_cons_type(model).with_verbose(0).build();
    let stats = match side.as_str() {
        "impl" => verify(conf, move || imp(n, &variant)),
        _ => verify(conf, move || spec(n)),
    };
    println!("execs={} blocked={}", stats.execs, stats.block);
}
