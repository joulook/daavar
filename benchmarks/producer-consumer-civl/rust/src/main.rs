//! Benchmark producer-consumer-civl (N4): CIVL's `samples/ProducerConsumer.bpl` as two Must-API
//! programs, each explored on its own under plain Must.
//! Usage: `<spec|impl> <k> [fifo|bag|causal]`.
//! Prints `execs=<complete> blocked=<blocked>` so the Java side can be compared line for line.
//!
//! Visible threads: `producer`, `consumer`. No invisible thread on either side.
//! The producer sends `Item(1) .. Item(j)` and then `Item(0)` (the source's stop value), choosing
//! `j` in `0..=k` by one `nondet()` per step as the source's `if (*)`; the consumer receives until
//! it reads `Item(0)`, at most `k + 1` receives.
//! * spec: the channel is FIFO by construction (`LocalOrder`, whatever the run's model), as the
//!   source's `Channel(C, head, tail)` is a FIFO queue.
//! * impl: the same two threads over a channel that takes the run's model.
//! Under `fifo` the two programs coincide; under `bag` the implementation's consumer can read the
//! items out of order, which the specification never does.
use traceforge::channel::{Builder, Receiver, Sender};
use traceforge::loc::CommunicationModel;
use traceforge::thread;
use traceforge::*;

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// `Item(x)`: `x >= 1` is a payload, `x == 0` ends the stream.
    Item(i32),
}

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn producer(k: usize, tx: Sender<Msg>) {
    for x in 1..=k as i32 {
        if nondet() {
            tx.send_msg(Msg::Item(x));
        } else {
            tx.send_msg(Msg::Item(0));
            return;
        }
    }
    tx.send_msg(Msg::Item(0));
}

fn consumer(k: usize, rx: Receiver<Msg>) {
    for _ in 0..=k {
        let Msg::Item(x) = rx.recv_msg_block();
        if x == 0 {
            return;
        }
    }
}

fn program(k: usize, fifo_by_construction: bool) {
    let (tx, rx) = if fifo_by_construction {
        Builder::<Msg>::new().with_comm(CommunicationModel::LocalOrder).build()
    } else {
        Builder::<Msg>::new().build()
    };
    let c = named("consumer", move || consumer(k, rx));
    let p = named("producer", move || producer(k, tx));
    c.join().unwrap();
    p.join().unwrap();
}

fn spec(k: usize) {
    program(k, true)
}

fn imp(k: usize) {
    program(k, false)
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
    let conf = Config::builder().with_cons_type(model).with_verbose(0).build();
    let stats = match side {
        "impl" => verify(conf, move || imp(n)),
        _ => verify(conf, move || spec(n)),
    };
    println!("execs={} blocked={}", stats.execs, stats.block);
}
