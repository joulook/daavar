//! Benchmark ring-election-igloo (candidate N23): the leader election of Igloo (Sprenger, Klenze
//! et al., OOPSLA 2020), a Chang–Roberts variant over UDP that retransmits on every receive
//! timeout, under an environment that loses, duplicates and reorders messages
//! (`Leader_Election_3.thy`'s `tr3e`), against the abstract models' property: only the top node
//! can be elected (`Leader_Election_1.thy`, `inv_leader_top`), so no two different nodes are
//! (`Leader_Election_0.thy`, `U_0`). Run by TraceForge's shared main (`traceforge::program`).
//!
//! ARGS: `<n> [variant]`; `n >= 2` nodes `node0 .. node{n-1}` with ids `0 .. n-1` on the ring
//! `i -> i + 1 mod n` (the `example012` instance at n = 3; the top is `n - 1`). Every node runs
//! `R = 1` iteration of `mainLoop` after its initial send, and may duplicate one of its sends
//! (`D = 1` per node). Variant (impl side): `correct` (default) or `no-compare` (a node adopts every
//! id it receives that is not its own: `elif msg > to_send` dropped; the seeded fault).
//! Model: `--model bag` (UDP: reordering; loss is a message never read; duplication explicit).
//!
//! Visible thread: `observer` (added: the source prints "I am the leader"): `n` blocking receives
//! of `Status { node, leader }`, one from every node when it stops.
//! * spec: one invisible `oracle`: `elected := nondet()`; `Status { node: i, leader: elected &&
//!   i == n - 1 }` for every i.
//! * impl: `node0 .. node{n-1}`, `election.py`'s `mainLoop`: `to_send := my_id`; send it; then `R`
//!   times: `msg := try_receive_int` (non-blocking: `None` is the timeout); `msg == my_id`: elected,
//!   stop; `msg > to_send`: `to_send := msg`; send `to_send`. Each send may be sent twice while the
//!   node's duplication budget lasts. Then `Status` to the observer.
use traceforge::channel::{Builder, Receiver, Sender};
use traceforge::nondet;
use traceforge::thread;

/// Iterations of `mainLoop` (receive, then resend) after the initial send.
const R: usize = 1;
/// Duplicated sends per node.
const D: usize = 1;

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// node -> successor: `send_int(to_send)`.
    Id(i32),
    /// node -> observer (added): the node's final `leader` flag (`tr0_state_local`).
    Status { node: i32, leader: bool },
}

type Tx = Sender<Msg>;
type Rx = Receiver<Msg>;

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn observer(n: usize, rx: Rx) {
    for _ in 0..n {
        let _ = rx.recv_msg_block();
    }
}

/// `try_send_int`, with the environment's duplication (`tr3e_receive` does not remove a message).
fn udp_send(tx: &Tx, v: i32, budget: &mut usize) {
    tx.send_msg(Msg::Id(v));
    if *budget > 0 && nondet() {
        *budget -= 1;
        tx.send_msg(Msg::Id(v));
    }
}

fn node(i: usize, compare: bool, rx: Rx, next: Tx, obs: Tx) {
    let my_id = i as i32;
    let mut budget = D;
    let mut to_send = my_id;
    let mut leader = false;
    udp_send(&next, to_send, &mut budget);
    for _ in 0..R {
        if let Some(Msg::Id(msg)) = rx.recv_msg() {
            if msg == my_id {
                leader = true;
                break;
            } else if !compare || msg > to_send {
                to_send = msg;
            }
        }
        udp_send(&next, to_send, &mut budget);
    }
    obs.send_msg(Msg::Status { node: my_id, leader });
}

fn spec(n: usize) {
    let (otx, orx) = Builder::<Msg>::new().build();
    let o = named("observer", move || observer(n, orx));
    let s = named("oracle", move || {
        let elected = nondet();
        for i in 0..n {
            otx.send_msg(Msg::Status { node: i as i32, leader: elected && i == n - 1 });
        }
    });
    o.join().unwrap();
    s.join().unwrap();
}

fn imp(n: usize, compare: bool) {
    let (otx, orx) = Builder::<Msg>::new().build();
    let (tx, rxs): (Vec<Tx>, Vec<Rx>) = (0..n).map(|_| Builder::<Msg>::new().build()).unzip();
    let mut hs = vec![named("observer", move || observer(n, orx))];
    for (i, rx) in rxs.into_iter().enumerate() {
        let next = tx[(i + 1) % n].clone();
        let obs = otx.clone();
        hs.push(named(&format!("node{i}"), move || node(i, compare, rx, next, obs)));
    }
    for h in hs {
        h.join().unwrap();
    }
}

const PROGRAM: &str = "ring-election-igloo";
const USAGE: &str = "`<n> [correct|no-compare]`, n >= 2";

struct Args {
    n: usize,
    compare: bool,
}

fn parse(a: &[String]) -> Args {
    if a.is_empty() || a.len() > 2 {
        panic!("{PROGRAM}: ARGS are {USAGE}, got {a:?}");
    }
    let n: usize = a[0]
        .parse()
        .unwrap_or_else(|_| panic!("{PROGRAM}: <n> must be a number; ARGS are {USAGE}"));
    if n < 2 {
        panic!("{PROGRAM}: n must be at least 2");
    }
    let compare = match a.get(1).map(String::as_str).unwrap_or("correct") {
        "correct" => true,
        "no-compare" => false,
        v => panic!("{PROGRAM}: unknown variant `{v}`; ARGS are {USAGE}"),
    };
    Args { n, compare }
}

fn main() -> std::process::ExitCode {
    traceforge::program::main(traceforge::program::Program {
        visible: &|_| vec!["observer".to_string()],
        spec: &|a| {
            let n = parse(a).n;
            std::sync::Arc::new(move || spec(n))
        },
        imp: &|a| {
            let p = parse(a);
            std::sync::Arc::new(move || imp(p.n, p.compare))
        },
    })
}
