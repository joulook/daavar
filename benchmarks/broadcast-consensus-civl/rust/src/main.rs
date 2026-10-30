//! Benchmark broadcast-consensus-civl (N3): CIVL's `samples/BroadcastConsensus.bpl` (each of n
//! processes broadcasts its value to all, self included, collects n values and decides the max)
//! as two Must-API programs, each explored on its own under plain Must.
//! Usage: `<spec|impl> <n> [fifo|bag|causal] [correct|decide-early]`.
//! Prints `execs=<complete> blocked=<blocked>` so the Java side can be compared line for line.
//!
//! Visible threads `c0 .. c{n-1}`, one per process: `c_i` chooses the process's value
//! `v_i in {0, 1}` by `nondet()`, sends `Input(v_i)`, and receives `Decide(d)`.
//! * spec (invisible `oracle`): receives the n inputs, sends `Decide(max)` to every `c_i`.
//! * impl (invisible `node0 .. node{n-1}`): `node_i` receives `Input(v_i)`, sends `Val(v_i)` to
//!   every node (self included, the source's `Broadcast`), receives n `Val`s and sends the max to
//!   `c_i` (the source's `Collect` + `set_decision`).
//! * variant `decide-early`: `node_i` decides after n - 1 values.
use traceforge::channel::{Builder, Receiver, Sender};
use traceforge::thread;
use traceforge::*;

/// `c_i -> oracle` (spec) or `c_i -> node_i` (impl): the process's value.
#[derive(Clone, Debug, PartialEq)]
struct Input(i32);
/// `node_j -> node_i`: the source's `send(v, i)`.
#[derive(Clone, Debug, PartialEq)]
struct Val(i32);
/// `oracle -> c_i` (spec) or `node_i -> c_i` (impl): the decision.
#[derive(Clone, Debug, PartialEq)]
struct Decide(i32);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Variant {
    Correct,
    DecideEarly,
}

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn chan<T: Clone + std::fmt::Debug + PartialEq + Send + 'static>() -> (Sender<T>, Receiver<T>) {
    Builder::<T>::new().build()
}

fn client(to: Sender<Input>, rx: Receiver<Decide>) {
    let v = if nondet() { 1 } else { 0 };
    to.send_msg(Input(v));
    let Decide(_d) = rx.recv_msg_block();
}

fn spec(n: usize) {
    let (tx_o, rx_o) = chan::<Input>();
    let (tx_d, rx_d): (Vec<_>, Vec<_>) = (0..n).map(|_| chan::<Decide>()).unzip();
    let mut hs = Vec::new();
    hs.push(named("oracle", move || {
        let mut d = i32::MIN;
        for _ in 0..n {
            let Input(v) = rx_o.recv_msg_block();
            d = d.max(v);
        }
        for tx in &tx_d {
            tx.send_msg(Decide(d));
        }
    }));
    for (i, rx) in rx_d.into_iter().enumerate() {
        let to = tx_o.clone();
        hs.push(named(&format!("c{i}"), move || client(to, rx)));
    }
    for h in hs {
        h.join().unwrap();
    }
}

fn imp(n: usize, variant: Variant) {
    let (tx_in, rx_in): (Vec<_>, Vec<_>) = (0..n).map(|_| chan::<Input>()).unzip();
    let (tx_val, rx_val): (Vec<_>, Vec<_>) = (0..n).map(|_| chan::<Val>()).unzip();
    let (tx_d, rx_d): (Vec<_>, Vec<_>) = (0..n).map(|_| chan::<Decide>()).unzip();
    let collect = if variant == Variant::DecideEarly { n - 1 } else { n };
    let mut hs = Vec::new();
    let mut rx_in = rx_in.into_iter();
    let mut rx_val = rx_val.into_iter();
    for i in 0..n {
        let my_in = rx_in.next().unwrap();
        let my_val = rx_val.next().unwrap();
        let peers = tx_val.clone();
        let to_c = tx_d[i].clone();
        hs.push(named(&format!("node{i}"), move || {
            let Input(v) = my_in.recv_msg_block();
            for p in &peers {
                p.send_msg(Val(v));
            }
            let mut d = i32::MIN;
            for _ in 0..collect {
                let Val(w) = my_val.recv_msg_block();
                d = d.max(w);
            }
            to_c.send_msg(Decide(d));
        }));
    }
    for (i, rx) in rx_d.into_iter().enumerate() {
        let to = tx_in[i].clone();
        hs.push(named(&format!("c{i}"), move || client(to, rx)));
    }
    for h in hs {
        h.join().unwrap();
    }
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let side = a.get(1).map(String::as_str).unwrap_or("spec");
    let n: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(2);
    let model = match a.get(3).map(String::as_str).unwrap_or("fifo") {
        "bag" => ConsType::Bag,
        "causal" => ConsType::Causal,
        _ => ConsType::FIFO,
    };
    let variant = match a.get(4).map(String::as_str).unwrap_or("correct") {
        "correct" => Variant::Correct,
        "decide-early" => Variant::DecideEarly,
        v => panic!("unknown variant {v}; one of correct, decide-early"),
    };
    let conf = Config::builder().with_cons_type(model).with_verbose(0).build();
    let stats = match side {
        "impl" => verify(conf, move || imp(n, variant)),
        _ => verify(conf, move || spec(n)),
    };
    println!("execs={} blocked={}", stats.execs, stats.block);
}
