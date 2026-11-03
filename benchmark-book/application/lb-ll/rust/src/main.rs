//! Benchmark balancer-load-aware: the three load-aware load-balancer variants of
//! `plan/traceForge/LB-VARIANTS.md` (L1, L2, L2-stale) against the relay specification of the
//! A4 family (`benchmarks/a4-balancer`, `A4b`: a dispatcher that picks a server by `nondet` for
//! every request, the server replying at once), run by TraceForge's shared main
//! (`traceforge::program`).
//!
//! ARGS: `<variant> <k> <q> <m> [<c>]`
//! * variant: `l1` (least-loaded by polling, unbounded servers), `l2` (capacity `c`, the balancer
//!   re-polls while every server reports full), `l2-stale` (the `l2` balancer, two of them
//!   sharing the servers: the poll's snapshot is stale);
//! * `k` clients, `q` requests per client, `m` servers, `c` the capacity (`l2`, `l2-stale` only).
//! * `b`, the number of balancers (impl) and dispatchers (spec): 1, and 2 for `l2-stale`;
//!   client `c_i` talks to balancer / dispatcher `i mod b`.
//!
//! Visible threads: `c0 .. c{k-1}`. A client sends `Req { c, j }` and receives `Reply { s, j }`
//! or (impl only) `Reject { s, j }`, for `j` in `0..q`, one request at a time.
//!
//! * spec: `dispatcher{d}` receives its clients' requests, forwards each to server `(0..m).nondet()`
//!   and ends with `Stop` to every server; `s{x}` replies to every request at once and ends after
//!   `b` `Stop`s.
//! * impl: `balancer{d}`, per request, sends `Poll { b: d, again }` to every server, receives the
//!   `m` `Load { s, n }` replies (any order), keeps the servers with space (all of them for `l1`,
//!   `n < c` otherwise), and if none has space polls again with `again: true`; it sends the
//!   request to a server of least load among them (ties by `nondet`); at the end `Stop` to every
//!   server. `s{x}` keeps a FIFO queue; on each turn, if its queue is non-empty, it either serves
//!   the oldest request (its non-blocking receive reads nothing: `Reply` to its client) or takes
//!   its next message (the receive reads one; with an empty queue the receive blocks): a
//!   `Poll` is answered with its queue length (a re-poll while full is answered only after one
//!   serve: the fairness bound that keeps the re-poll loop finite), a `Req` is queued, or rejected
//!   with `Reject { s, j }` when the queue holds `c`, a `Stop` is counted; after `b` `Stop`s it
//!   drains its queue and ends.
//!
//! Channels: one inbox per server and per client, two per balancer (requests, loads), all under
//! the run's model; the twin is meaningful under `--model fifo` (JMC `p2p`) only.
use std::collections::VecDeque;
use std::sync::Arc;

use traceforge::channel::{Builder, Receiver, Sender};
use traceforge::thread;
use traceforge::*;

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// client -> balancer / dispatcher -> server: request `j` of client `c`.
    Req { c: u8, j: u8 },
    /// balancer `b` -> server: report your queue length; `again` on a re-poll.
    Poll { b: u8, again: bool },
    /// server `s` -> balancer: its queue length `n`.
    Load { s: u8, n: u8 },
    /// server `s` -> client: request `j` served.
    Reply { s: u8, j: u8 },
    /// server `s` -> client: request `j` arrived while the queue was full.
    Reject { s: u8, j: u8 },
    /// balancer / dispatcher -> server: no more messages from me.
    Stop,
}

type Tx = Sender<Msg>;
type Rx = Receiver<Msg>;

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn chans(n: usize) -> (Vec<Tx>, Vec<Rx>) {
    (0..n).map(|_| Builder::<Msg>::new().build()).unzip()
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Variant {
    L1,
    L2,
    L2Stale,
}

#[derive(Clone, Copy, Debug)]
struct Args {
    k: usize,
    q: usize,
    m: usize,
    /// `None` for `l1` (unbounded queues).
    cap: Option<usize>,
    b: usize,
}

/// Requests balancer / dispatcher `d` receives: `q` from every client `i` with `i mod b = d`.
fn requests_of(a: &Args, d: usize) -> usize {
    a.q * (0..a.k).filter(|i| i % a.b == d).count()
}

fn client(i: usize, q: usize, to: Tx, rx: Rx) {
    for j in 0..q {
        to.send_msg(Msg::Req { c: i as u8, j: j as u8 });
        let _ = rx.recv_msg_block();
    }
}

fn spawn_clients(a: &Args, to: &[Tx], rxs: Vec<Rx>) -> Vec<thread::JoinHandle<()>> {
    let (b, q) = (a.b, a.q);
    rxs.into_iter()
        .enumerate()
        .map(|(i, rx)| {
            let to = to[i % b].clone();
            named(&format!("c{i}"), move || client(i, q, to, rx))
        })
        .collect()
}

// ------------------------------------------------------------ spec ----

fn spec(a: Args) {
    let (rtx, rrx) = chans(a.k); // client inboxes
    let (dtx, drx) = chans(a.b); // dispatcher inboxes
    let (stx, srx) = chans(a.m); // server inboxes
    let mut hs = Vec::new();
    for (d, rx) in drx.into_iter().enumerate() {
        let n = requests_of(&a, d);
        let (m, srv) = (a.m, stx.clone());
        hs.push(named(&format!("dispatcher{d}"), move || {
            for _ in 0..n {
                let r = rx.recv_msg_block();
                let x = if m > 1 { (0..m).nondet() } else { 0 };
                srv[x].send_msg(r);
            }
            for s in &srv {
                s.send_msg(Msg::Stop);
            }
        }));
    }
    for (x, rx) in srx.into_iter().enumerate() {
        let (b, reply) = (a.b, rtx.clone());
        hs.push(named(&format!("s{x}"), move || {
            let mut stops = 0;
            while stops < b {
                match rx.recv_msg_block() {
                    Msg::Req { c, j } => reply[c as usize].send_msg(Msg::Reply { s: x as u8, j }),
                    Msg::Stop => stops += 1,
                    v => panic!("spec s{x} got {v:?}"),
                }
            }
        }));
    }
    hs.extend(spawn_clients(&a, &dtx, rrx));
    for h in hs {
        h.join().unwrap();
    }
}

// ------------------------------------------------------------ impl ----

fn balancer(d: usize, n: usize, m: usize, cap: Option<usize>, reqs: Rx, loads: Rx, srv: Vec<Tx>) {
    for _ in 0..n {
        let r = reqs.recv_msg_block();
        let mut again = false;
        // At most two rounds: a re-polled full server serves once before it answers.
        let x = loop {
            for s in &srv {
                s.send_msg(Msg::Poll { b: d as u8, again });
            }
            let mut load = vec![0usize; m];
            for _ in 0..m {
                match loads.recv_msg_block() {
                    Msg::Load { s, n } => load[s as usize] = n as usize,
                    v => panic!("balancer{d} got {v:?}"),
                }
            }
            let space: Vec<usize> = (0..m).filter(|&x| cap.map_or(true, |c| load[x] < c)).collect();
            if space.is_empty() {
                again = true;
                continue;
            }
            let min = space.iter().map(|&x| load[x]).min().unwrap();
            let best: Vec<usize> = space.into_iter().filter(|&x| load[x] == min).collect();
            break if best.len() == 1 { best[0] } else { best[(0..best.len()).nondet()] };
        };
        srv[x].send_msg(r);
    }
    for s in &srv {
        s.send_msg(Msg::Stop);
    }
}

fn server(x: usize, b: usize, cap: Option<usize>, rx: Rx, loads: Vec<Tx>, reply: Vec<Tx>) {
    let mut queue: VecDeque<(u8, u8)> = VecDeque::new();
    let full = |q: &VecDeque<(u8, u8)>| cap.map_or(false, |c| q.len() >= c);
    let serve = |q: &mut VecDeque<(u8, u8)>| {
        let (c, j) = q.pop_front().unwrap();
        reply[c as usize].send_msg(Msg::Reply { s: x as u8, j });
    };
    let mut stops = 0;
    loop {
        if stops == b {
            while !queue.is_empty() {
                serve(&mut queue);
            }
            return;
        }
        // Serve now or receive: with work queued the receive is non-blocking and its ⊥ is the
        // serve, so a server holding requests never blocks.
        let msg = if queue.is_empty() { Some(rx.recv_msg_block()) } else { rx.recv_msg() };
        let Some(msg) = msg else {
            serve(&mut queue);
            continue;
        };
        match msg {
            Msg::Poll { b: d, again } => {
                if again && full(&queue) {
                    serve(&mut queue);
                }
                loads[d as usize].send_msg(Msg::Load { s: x as u8, n: queue.len() as u8 });
            }
            Msg::Req { c, j } => {
                if full(&queue) {
                    reply[c as usize].send_msg(Msg::Reject { s: x as u8, j });
                } else {
                    queue.push_back((c, j));
                }
            }
            Msg::Stop => stops += 1,
            v => panic!("s{x} got {v:?}"),
        }
    }
}

fn imp(a: Args) {
    let (rtx, rrx) = chans(a.k); // client inboxes
    let (btx, brx) = chans(a.b); // balancer request channels
    let (ltx, lrx) = chans(a.b); // balancer load channels
    let (stx, srx) = chans(a.m); // server inboxes
    let mut hs = Vec::new();
    for (d, (reqs, loads)) in brx.into_iter().zip(lrx).enumerate() {
        let (n, m, cap, srv) = (requests_of(&a, d), a.m, a.cap, stx.clone());
        hs.push(named(&format!("balancer{d}"), move || balancer(d, n, m, cap, reqs, loads, srv)));
    }
    for (x, rx) in srx.into_iter().enumerate() {
        let (b, cap, loads, reply) = (a.b, a.cap, ltx.clone(), rtx.clone());
        hs.push(named(&format!("s{x}"), move || server(x, b, cap, rx, loads, reply)));
    }
    hs.extend(spawn_clients(&a, &btx, rrx));
    for h in hs {
        h.join().unwrap();
    }
}

// ------------------------------------------------------- the arguments ----

const USAGE: &str = "`<variant> <k> <q> <m> [<c>]`, variant l1 | l2 | l2-stale; c for l2, l2-stale";

fn num(args: &[String], i: usize, what: &str) -> usize {
    let v: usize = args
        .get(i)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("balancer-load-aware: <{what}> missing or not a number; ARGS are {USAGE}"));
    assert!(v >= 1 && v <= 100, "balancer-load-aware: <{what}> must be in 1..=100");
    v
}

fn parse(args: &[String]) -> Args {
    let variant = match args.first().map(String::as_str) {
        Some("l1") => Variant::L1,
        Some("l2") => Variant::L2,
        Some("l2-stale") => Variant::L2Stale,
        v => panic!("balancer-load-aware: unknown variant {v:?}; ARGS are {USAGE}"),
    };
    let n = if variant == Variant::L1 { 4 } else { 5 };
    assert!(args.len() == n, "balancer-load-aware: expected {n} arguments, got {args:?}; ARGS are {USAGE}");
    Args {
        k: num(args, 1, "k"),
        q: num(args, 2, "q"),
        m: num(args, 3, "m"),
        cap: if variant == Variant::L1 { None } else { Some(num(args, 4, "c")) },
        b: if variant == Variant::L2Stale { 2 } else { 1 },
    }
}

fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &|a| (0..parse(a).k).map(|i| format!("c{i}")).collect(),
        spec: &|a| {
            let p = parse(a);
            Arc::new(move || spec(p))
        },
        imp: &|a| {
            let p = parse(a);
            Arc::new(move || imp(p))
        },
    })
}
