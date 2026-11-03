//! Benchmark ironlock (N13): IronFleet's IronLock token ring
//! (`Protocol/Lock/Node.i.dfy`, `Impl/Lock/NodeImpl.i.dfy`, spec `Services/Lock/AbstractService.s.dfy`)
//! as Must-API programs, each explored on its own under plain Must.
//! Usage: `<bin> count|export|conf|online … --model M -- <n> [variant] [spec-kind]`;
//! variant `correct`, `double-hold` or `grant-without-token`; spec-kind `any` (default, the
//! specification `spec` below) or `ring` (`spec-ring` below); `imp` ignores it.
//! K = 2 transfers. `count` prints `execs=<complete> blocked=<blocked>`.
//!
//! Visible threads `obs0 .. obs{n-1}`: `obs_i` stands for the wire out of node i. It receives the
//! `Locked(e)` packets node i emits (the packets `Service_Correspondence` constrains: a `Locked(e)`
//! from node i requires `history[e-1] == i`), then one `End` marking that node i has stopped.
//! It reads at most K+2 messages and stops at the first `End`.
//! * spec (IronFleet's, any holder): invisible `oracle`; for every epoch `e = 1 ..= K+1` it picks
//!   `c = (0..n+1).nondet()`: `c == n` means no `Locked(e)` packet exists, otherwise history[e-1]
//!   = node c and it sends `Locked(e)` to `obs_c`. Then `End` to every observer. Every epoch has at
//!   most one holder; holders are in any order.
//! * spec-ring (stricter than IronFleet's): invisible `oracle`; `history[e-1] = (e-1) mod n`; for
//!   `e = 2 ..= K+1` it sends `Locked(e)` to `obs_{(e-1) mod n}`, then `End` to every observer.
//! * impl (invisible `node0 .. node{n-1}`): `NodeInit` (node 0 holds epoch 1), `NodeGrant` (the
//!   holder sends `Transfer(epoch+1)` to node `(i+1) mod n` and releases), `NodeAccept` (a
//!   non-holder receiving `Transfer(e)` with `e > epoch` takes the lock and emits `Locked(e)`).
//!   Stop rule (schedule-independent): a holder grants only while `epoch <= K`; a node that holds
//!   epoch K+1 does not grant, it sends `Stop` to every node (its own mailbox included) and ends.
//!   A non-holder waiting on its mailbox ends at the first `Stop` it reads (a later `Stop` stays
//!   unread). On ending a node sends `End` to `obs_i`.
//! * variant `double-hold`: node 1, right after a `NodeGrant` of `Transfer(e+1)`, may
//!   (`nondet()`) also take epoch e+1 itself (keep the token it gave away) and emit `Locked(e+1)`.
//! * variant `grant-without-token`: node 1 may (`nondet()`) run `NodeGrant` once at start without
//!   holding the token (the `s.held` guard dropped): it sends `Transfer(0 + 1)` to node 2 mod n.
//! Declared default model, used when `--model` is left out: `fifo`.
use traceforge::channel::{Builder, Receiver, Sender};
use traceforge::thread;
use traceforge::*;

const K: i32 = 2;

/// What goes node to node: `LockMessage::Transfer(transfer_epoch)`, and `Stop` (ours: some node
/// holds epoch K+1, so the bounded run is over).
#[derive(Clone, Debug, PartialEq)]
enum NodeMsg {
    Transfer(i32),
    Stop,
}
/// What goes on the wire out of node i to `obs_i`: `LockMessage::Locked(locked_epoch)`, and `End`
/// (ours: node i has stopped).
#[derive(Clone, Debug, PartialEq)]
enum Wire {
    Locked(i32),
    End,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Variant {
    Correct,
    DoubleHold,
    GrantWithoutToken,
}

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn chan<T: Clone + std::fmt::Debug + PartialEq + Send + 'static>() -> (Sender<T>, Receiver<T>) {
    Builder::<T>::new().build()
}

/// The visible `obs_i`, identical on every side: at most K+2 receives, stop at `End`.
fn observer(rx: Receiver<Wire>) {
    for _ in 0..(K as usize + 2) {
        if let Wire::End = rx.recv_msg_block() {
            return;
        }
    }
}

fn spawn_observers(rx_obs: Vec<Receiver<Wire>>, hs: &mut Vec<thread::JoinHandle<()>>) {
    for (i, rx) in rx_obs.into_iter().enumerate() {
        hs.push(named(&format!("obs{i}"), move || observer(rx)));
    }
}

fn spec(n: usize, ring: bool) {
    let (tx_obs, rx_obs): (Vec<_>, Vec<_>) = (0..n).map(|_| chan::<Wire>()).unzip();
    let mut hs = Vec::new();
    hs.push(named("oracle", move || {
        if ring {
            for e in 2..=K + 1 {
                tx_obs[((e - 1) as usize) % n].send_msg(Wire::Locked(e));
            }
        } else {
            for e in 1..=K + 1 {
                let c: usize = (0..n + 1).nondet();
                if c < n {
                    tx_obs[c].send_msg(Wire::Locked(e));
                }
            }
        }
        for tx in &tx_obs {
            tx.send_msg(Wire::End);
        }
    }));
    spawn_observers(rx_obs, &mut hs);
    for h in hs {
        h.join().unwrap();
    }
}

fn node(i: usize, variant: Variant, rx: Receiver<NodeMsg>, all: Vec<Sender<NodeMsg>>, obs: Sender<Wire>) {
    let n = all.len();
    let next = &all[(i + 1) % n];
    let mut held = i == 0;
    let mut epoch: i32 = if i == 0 { 1 } else { 0 };
    if variant == Variant::GrantWithoutToken && i == 1 && nondet() {
        next.send_msg(NodeMsg::Transfer(epoch + 1));
    }
    for _ in 0..(2 * K as usize + 2) {
        if held {
            if epoch <= K {
                next.send_msg(NodeMsg::Transfer(epoch + 1));
                held = false;
                if variant == Variant::DoubleHold && i == 1 && nondet() {
                    held = true;
                    epoch += 1;
                    obs.send_msg(Wire::Locked(epoch));
                }
            } else {
                for tx in &all {
                    tx.send_msg(NodeMsg::Stop);
                }
                break;
            }
        } else {
            match rx.recv_msg_block() {
                NodeMsg::Stop => break,
                NodeMsg::Transfer(e) => {
                    if e > epoch {
                        held = true;
                        epoch = e;
                        obs.send_msg(Wire::Locked(e));
                    }
                }
            }
        }
    }
    obs.send_msg(Wire::End);
}

fn imp(n: usize, variant: Variant) {
    let (tx_obs, rx_obs): (Vec<_>, Vec<_>) = (0..n).map(|_| chan::<Wire>()).unzip();
    let (tx_node, rx_node): (Vec<_>, Vec<_>) = (0..n).map(|_| chan::<NodeMsg>()).unzip();
    let mut hs = Vec::new();
    for (i, rx) in rx_node.into_iter().enumerate() {
        let all = tx_node.clone();
        let obs = tx_obs[i].clone();
        hs.push(named(&format!("node{i}"), move || node(i, variant, rx, all, obs)));
    }
    spawn_observers(rx_obs, &mut hs);
    for h in hs {
        h.join().unwrap();
    }
}

fn parse(a: &[String]) -> (usize, Variant, bool) {
    let n: usize = a.first().and_then(|s| s.parse().ok()).unwrap_or(2);
    let variant = match a.get(1).map(String::as_str).unwrap_or("correct") {
        "correct" => Variant::Correct,
        "double-hold" => Variant::DoubleHold,
        "grant-without-token" => Variant::GrantWithoutToken,
        v => panic!("unknown variant {v}; one of correct, double-hold, grant-without-token"),
    };
    let ring = match a.get(2).map(String::as_str).unwrap_or("any") {
        "any" => false,
        "ring" => true,
        s => panic!("unknown spec-kind {s}; one of any, ring"),
    };
    (n, variant, ring)
}

fn main() -> std::process::ExitCode {
    let program = traceforge::program::Program {
        visible: &|a| (0..parse(a).0).map(|i| format!("obs{i}")).collect(),
        spec: &|a| {
            let (n, _, ring) = parse(a);
            std::sync::Arc::new(move || spec(n, ring))
        },
        imp: &|a| {
            let (n, variant, _) = parse(a);
            std::sync::Arc::new(move || imp(n, variant))
        },
    };
    traceforge::program::main_with_model(program, traceforge::ConsType::FIFO)
}
