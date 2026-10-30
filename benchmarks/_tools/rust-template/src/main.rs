//! Benchmark TEMPLATE: the specification and the implementation as two Must-API programs,
//! each explored on its own under plain Must. Usage: `<spec|impl> <size> [fifo|bag|causal]`.
//! Prints `execs=<complete> blocked=<blocked>` so the Java side can be compared line for line.
use traceforge::thread::{self, ThreadId};
use traceforge::*;

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    Req(i32),
    Rep(i32),
}

fn spec(n: usize) {
    // the specification program: one server thread orders everything it serves
    let server = thread::spawn(move || {
        for _ in 0..n {
            let Msg::Req(v) = recv_msg_block::<Msg>() else { unreachable!() };
            let _ = v;
        }
    });
    let sid: ThreadId = server.thread().id();
    let mut clients = Vec::new();
    for i in 0..n {
        clients.push(thread::spawn(move || {
            send_msg(sid, Msg::Req(i as i32));
        }));
    }
    for c in clients { c.join().unwrap(); }
    server.join().unwrap();
}

fn imp(n: usize) {
    // the implementation program (here identical; a real benchmark differs)
    spec(n)
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
