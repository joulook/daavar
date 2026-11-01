//! Benchmark ewd998-termination (candidate N14): Safra's termination detection (EWD 998) on a
//! ring of N nodes, against an oracle that announces termination only when every node is idle
//! and no payload is in flight (`AsyncTerminationDetection.tla`, `Safe`).
//!
//! Every node is split into a visible application thread `app<i>` and an invisible detector
//! `det<i>`. The apps are the same program on both sides: an app starts active, may send its one
//! payload `Pl { from }` to the detector of another node, reports `Idle { node, sent, got }`
//! (`sent`: the node it sent its payload to, if any; `got`: the bitmask of the nodes whose
//! payload it has received, cumulative), and then blocks on `Pl | Trm`: a `Pl` reactivates it, a
//! `Trm` ends it. Payloads travel `app<i> -> det<j> -> app<j>` on both sides.
//!
//! * `spec`: `det<j>` is a forwarding stub (a `Pl` goes on to `app<j>`, a `Stop` ends it); the
//!   apps report `Idle` to one invisible `oracle`, which keeps the latest report of each node and,
//!   when those reports form a consistent cut in which every payload sent was received
//!   (`terminated`), may (`nondet()`) send `Trm` to every app and `Stop` to every stub; either
//!   way it then ends.
//! * `impl`: the apps report `Idle` to their own detector; `det<i>` runs Safra's rules as
//!   `EWD998.java` does: `counter` (sends minus receives), `color`, the token `Tok { q, black }`
//!   passed `i -> i-1`, `InitiateProbe` at node 0 (at most `R` probes); on a conclusive round
//!   `det0` sends `Trm` to every app and `Stop` to `det1..`.
//!
//! Every thread has one inbox channel, created by main before any thread is spawned (the
//! threads address each other circularly; the Java twin mints the thread ids first instead).
//!
//! Usage: `<bin> count|export|conf|online … --model M -- <N> [variant]`; `N` nodes (K = 1 payload
//! per node, R = 2 probes), model `--model bag` (asyn; `EWD998Chan` receives from any inbox
//! index; the default this program declares),
//! default variant `correct`. Variants (impl side): `correct`, `no-black-on-receive` (a detector
//! does not turn black when it receives a payload: Rule 3 / `RecvMsg`'s `color' = black` dropped).
//! `count` prints `execs=<complete> blocked=<blocked>`.
use traceforge::channel::{Builder, Receiver, Sender};
use traceforge::thread;
use traceforge::*;

/// Probes `det0` may initiate.
const R: usize = 2;

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// A payload (`pl`); `from` is the sending node.
    Pl { from: u8 },
    /// An app's report on going idle: the node its payload went to, the nodes it heard from.
    Idle { node: u8, sent: Option<u8>, got: u8 },
    /// Impl only: the token `tok(q, color)`.
    Tok { q: i8, black: bool },
    /// The termination announcement (`trm`).
    Trm,
    /// Ends a detector or a stub.
    Stop,
}

type Tx = Sender<Msg>;
type Rx = Receiver<Msg>;

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn chans(k: usize) -> (Vec<Tx>, Vec<Rx>) {
    (0..k).map(|_| Builder::<Msg>::new().build()).unzip()
}

/// The application of node `i` (the same on both sides); `det` are the n detectors (or stubs).
fn app(i: usize, n: usize, rx: Rx, det: Vec<Tx>, report: Tx) {
    let mut sent: Option<u8> = None;
    let mut got: u8 = 0;
    // one active phase: SendMsg (at most once per node, K = 1), then Deactivate
    let phase = |sent: &mut Option<u8>, got: u8| {
        if sent.is_none() {
            // 0: no send; c in 1..n: send to node (i + c) mod n (never to itself)
            let c: usize = (0..n).nondet();
            if c > 0 {
                let j = (i + c) % n;
                det[j].send_msg(Msg::Pl { from: i as u8 });
                *sent = Some(j as u8);
            }
        }
        report.send_msg(Msg::Idle { node: i as u8, sent: *sent, got });
    };
    phase(&mut sent, got);
    // at most n-1 payloads, then the announcement
    for _ in 0..n {
        match rx.recv_msg_block() {
            Msg::Trm => return,
            Msg::Pl { from } => {
                got |= 1 << from;
                phase(&mut sent, got);
            }
            m => panic!("app{i} got {m:?}"),
        }
    }
}

/// The oracle's test: every node has reported; the latest reports form a consistent cut (every
/// receive reported was reported sent, to that node) in which every payload reported sent was
/// reported received.
fn terminated(view: &[Option<(Option<u8>, u8)>]) -> bool {
    if view.iter().any(|v| v.is_none()) {
        return false;
    }
    let n = view.len();
    for i in 0..n {
        let (sent, got) = view[i].unwrap();
        if let Some(j) = sent {
            if view[j as usize].unwrap().1 & (1 << i) == 0 {
                return false;
            }
        }
        for k in 0..n {
            if got & (1 << k) != 0 && view[k].unwrap().0 != Some(i as u8) {
                return false;
            }
        }
    }
    true
}

/// Spec. Inboxes: apps 0..n, stubs n..2n, oracle 2n. Spawn order: oracle, det0.., app0...
fn spec(n: usize) {
    let (tx, mut rxs) = chans(2 * n + 1);
    let mut hs = Vec::new();
    let orx = rxs.pop().unwrap();
    let t = tx.clone();
    hs.push(named("oracle", move || {
        let mut view: Vec<Option<(Option<u8>, u8)>> = vec![None; n];
        loop {
            match orx.recv_msg_block() {
                Msg::Idle { node, sent, got } => {
                    let newer = match view[node as usize] {
                        None => true,
                        Some((_, g)) => got.count_ones() > g.count_ones(),
                    };
                    if newer {
                        view[node as usize] = Some((sent, got));
                    }
                    if terminated(&view) {
                        if nondet() {
                            for a in 0..n {
                                t[a].send_msg(Msg::Trm);
                            }
                            for d in 0..n {
                                t[n + d].send_msg(Msg::Stop);
                            }
                        }
                        return;
                    }
                }
                m => panic!("oracle got {m:?}"),
            }
        }
    }));
    let mut rxs: Vec<Option<Rx>> = rxs.into_iter().map(Some).collect();
    for j in 0..n {
        let rx = rxs[n + j].take().unwrap();
        let t = tx.clone();
        hs.push(named(&format!("det{j}"), move || loop {
            match rx.recv_msg_block() {
                Msg::Pl { from } => t[j].send_msg(Msg::Pl { from }),
                Msg::Stop => return,
                m => panic!("det{j} (stub) got {m:?}"),
            }
        }));
    }
    for i in 0..n {
        let rx = rxs[i].take().unwrap();
        let det = tx[n..2 * n].to_vec();
        let report = tx[2 * n].clone();
        hs.push(named(&format!("app{i}"), move || app(i, n, rx, det, report)));
    }
    for h in hs {
        h.join().unwrap();
    }
}

/// Impl. Inboxes: apps 0..n, detectors n..2n. Spawn order: det0.., app0...
fn imp(n: usize, black_on_receive: bool) {
    let (tx, rxs) = chans(2 * n);
    let mut rxs: Vec<Option<Rx>> = rxs.into_iter().map(Some).collect();
    let mut hs = Vec::new();
    for i in 0..n {
        let rx = rxs[n + i].take().unwrap();
        let t = tx.clone();
        hs.push(named(&format!("det{i}"), move || detector(i, n, black_on_receive, rx, t)));
    }
    for i in 0..n {
        let rx = rxs[i].take().unwrap();
        let det = tx[n..2 * n].to_vec();
        let report = tx[n + i].clone();
        hs.push(named(&format!("app{i}"), move || app(i, n, rx, det, report)));
    }
    for h in hs {
        h.join().unwrap();
    }
}

/// Safra's detector of node `i` (`EWD998.java`'s node loop, minus the application part).
/// `t`: apps 0..n, detectors n..2n.
fn detector(i: usize, n: usize, black_on_receive: bool, rx: Rx, t: Vec<Tx>) {
    let mut counter: i8 = 0; // sends minus receives
    let mut black = false; // color (Java's initial color is white)
    let mut outstanding: u8 = 1; // the app is active until it reports Idle for every Pl it got
    let mut sent_counted = false;
    let mut token: Option<(i8, bool)> = None;
    let mut rounds = 0;
    if i == 0 {
        // the initial token at node 0 is black, so the first round is initiated at once
        t[n + n - 1].send_msg(Msg::Tok { q: 0, black: false });
        rounds = 1;
    }
    loop {
        match rx.recv_msg_block() {
            Msg::Pl { from } => {
                // RecvMsg: counter - 1, color black (dropped by the fault), active
                counter -= 1;
                if black_on_receive {
                    black = true;
                }
                outstanding += 1;
                t[i].send_msg(Msg::Pl { from });
            }
            Msg::Idle { sent, .. } => {
                // SendMsg (counter + 1, once per node) and Deactivate
                if sent.is_some() && !sent_counted {
                    counter += 1;
                    sent_counted = true;
                }
                outstanding -= 1;
            }
            Msg::Tok { q, black: tb } => token = Some((q, tb)),
            Msg::Stop => return,
            m => panic!("det{i} got {m:?}"),
        }
        if let Some((q, tb)) = token {
            if i != 0 {
                // PassToken: only when inactive
                if outstanding == 0 {
                    t[n + i - 1].send_msg(Msg::Tok { q: q + counter, black: tb || black });
                    black = false;
                    token = None;
                }
            } else {
                let conclusive = !tb && !black && q + counter == 0;
                if !conclusive {
                    // InitiateProbe (Java: regardless of activity); after R probes, no more
                    if rounds < R {
                        t[n + n - 1].send_msg(Msg::Tok { q: 0, black: false });
                        black = false;
                        rounds += 1;
                    }
                    token = None;
                } else if outstanding == 0 {
                    // termination detected: trm to every node
                    for a in 0..n {
                        t[a].send_msg(Msg::Trm);
                    }
                    for d in 1..n {
                        t[n + d].send_msg(Msg::Stop);
                    }
                    return;
                }
                // conclusive but active: keep the token (Java re-enqueues it)
            }
        }
    }
}

fn parse(a: &[String]) -> (usize, bool) {
    let n: usize = a.first().and_then(|s| s.parse().ok()).unwrap_or(3);
    let black_on_receive = match a.get(1).map(String::as_str).unwrap_or("correct") {
        "correct" => true,
        "no-black-on-receive" => false,
        v => panic!("unknown variant {v}"),
    };
    (n, black_on_receive)
}

fn main() -> std::process::ExitCode {
    let program = traceforge::program::Program {
        visible: &|a| (0..parse(a).0).map(|i| format!("app{i}")).collect(),
        spec: &|a| {
            let (n, _) = parse(a);
            std::sync::Arc::new(move || spec(n))
        },
        imp: &|a| {
            let (n, black_on_receive) = parse(a);
            std::sync::Arc::new(move || imp(n, black_on_receive))
        },
    };
    traceforge::program::main_with_model(program, traceforge::ConsType::Bag)
}
