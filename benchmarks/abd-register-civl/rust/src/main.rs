//! Benchmark abd-register-civl (N15): the ABD atomic register of CIVL's `samples/ABD.bpl`
//! (Attiya, Bar-Noy, Dolev) as two Must-API programs, each explored on its own under plain Must.
//! Usage: `<spec|impl> <replicas> [fifo|bag|causal] [correct|read-no-writeback]`.
//! Prints `execs=<complete> blocked=<blocked>` so the Java side can be compared line for line.
//!
//! Visible threads `w`, `r1`, `r2`. `w` sends `Op::Write(1)` and receives `Reply::WriteAck`;
//! `r1` sends `Op::Read(1)`, receives `Reply::ReadReply(v)`, then sends `Done` to `r2`; `r2`
//! receives `Done`, sends `Op::Read(2)`, receives `Reply::ReadReply(v)`. The register's initial
//! value is 0. The `Done` message puts `r1`'s read before `r2`'s in real time (in happens-before),
//! which is what a new-old inversion needs.
//! * spec (invisible `register`): one thread serving the three operations atomically, in any
//!   order (the layer-4 atomic `Read`/`Write` of the source, over one `value_store`).
//! * impl (invisible `rep0 .. rep{n-1}`, `wp`, `rp1`, `rp2`): each client talks to its proxy,
//!   which runs the source's `WriteClient` / `ReadClient`: `QueryPhase` (`Query` to every replica,
//!   keep the max `(ts, value)` of the first majority of replies) and `UpdatePhase` (`Update(ts, v)`
//!   to every replica, wait for a majority of acks); the writer uses `ts = (max.t + 1, 1)`, a
//!   reader writes back the max it read. Replicas: the source's `Query#0` and `Update#0` (store
//!   `(ts, v)` if `ts` is larger), each answered by a reply; a replica serves every request sent to
//!   it (6 in `correct`, 4 in `read-no-writeback`) and then ends.
//! * variant `read-no-writeback`: the readers skip `UpdatePhase`.
use traceforge::channel::{Builder, Receiver, Sender};
use traceforge::thread;
use traceforge::*;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};

/// Diagnostic only (no events): the value r1's read returned in the current execution, and the
/// number of executions in which r2 then read 0 after r1 read 1 (the new-old inversion). Printed
/// when the fifth argument is `witness`.
static R1_READ: AtomicI32 = AtomicI32::new(-1);
static INVERSIONS: AtomicUsize = AtomicUsize::new(0);

/// The writer's pid in `TimeStamp(t, pid)`.
const WPID: i32 = 1;

/// Client -> register (spec) or client -> its proxy (impl). `Read(i)` names the reader `r<i>`.
#[derive(Clone, Debug, PartialEq)]
enum Op {
    Write(i32),
    Read(usize),
}
/// Register or proxy -> client.
#[derive(Clone, Debug, PartialEq)]
enum Reply {
    WriteAck,
    ReadReply(i32),
}
/// `r1 -> r2`: r1's read has returned.
#[derive(Clone, Debug, PartialEq)]
struct Done;
/// Proxy -> replica; the `usize` is the proxy (0 = `wp`, 1 = `rp1`, 2 = `rp2`); a timestamp is
/// `(t, pid)`, ordered lexicographically as the source's `lt`.
#[derive(Clone, Debug, PartialEq)]
enum ToReplica {
    Query(usize),
    Update(usize, (i32, i32), i32),
}
/// Replica -> proxy: the source's `Query#0` result.
#[derive(Clone, Debug, PartialEq)]
struct QueryReply((i32, i32), i32);
/// Replica -> proxy: `Update#0` done.
#[derive(Clone, Debug, PartialEq)]
struct UpdateAck;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Variant {
    Correct,
    ReadNoWriteback,
}

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn chan<T: Clone + std::fmt::Debug + PartialEq + Send + 'static>() -> (Sender<T>, Receiver<T>) {
    Builder::<T>::new().build()
}

/// The three visible clients, identical on both sides. `to[0]` is w's front end, `to[1]` r1's,
/// `to[2]` r2's.
fn spawn_clients(to: [Sender<Op>; 3], replies: Vec<Receiver<Reply>>, hs: &mut Vec<thread::JoinHandle<()>>) {
    let [tw, t1, t2] = to;
    let mut replies = replies.into_iter();
    let (rw, rr1, rr2) = (replies.next().unwrap(), replies.next().unwrap(), replies.next().unwrap());
    let (tx_done, rx_done) = chan::<Done>();
    hs.push(named("w", move || {
        tw.send_msg(Op::Write(1));
        let _a = rw.recv_msg_block();
    }));
    hs.push(named("r1", move || {
        t1.send_msg(Op::Read(1));
        if let Reply::ReadReply(v) = rr1.recv_msg_block() {
            R1_READ.store(v, Ordering::SeqCst);
        }
        tx_done.send_msg(Done);
    }));
    hs.push(named("r2", move || {
        let Done = rx_done.recv_msg_block();
        t2.send_msg(Op::Read(2));
        if let Reply::ReadReply(v) = rr2.recv_msg_block() {
            if v == 0 && R1_READ.load(Ordering::SeqCst) == 1 {
                INVERSIONS.fetch_add(1, Ordering::SeqCst);
            }
        }
    }));
}

fn spec(_n: usize) {
    let (tx_op, rx_op) = chan::<Op>();
    let (tx_rep, rx_rep): (Vec<_>, Vec<_>) = (0..3).map(|_| chan::<Reply>()).unzip();
    let mut hs = Vec::new();
    hs.push(named("register", move || {
        let mut value = 0;
        for _ in 0..3 {
            match rx_op.recv_msg_block() {
                Op::Write(v) => {
                    value = v;
                    tx_rep[0].send_msg(Reply::WriteAck);
                }
                Op::Read(i) => tx_rep[i].send_msg(Reply::ReadReply(value)),
            }
        }
    }));
    spawn_clients([tx_op.clone(), tx_op.clone(), tx_op], rx_rep, &mut hs);
    for h in hs {
        h.join().unwrap();
    }
}

/// `QueryPhase`: query every replica, keep the max of the first `maj` replies.
fn query_phase(me: usize, reps: &[Sender<ToReplica>], rx: &Receiver<QueryReply>, maj: usize) -> ((i32, i32), i32) {
    for r in reps {
        r.send_msg(ToReplica::Query(me));
    }
    let mut best = ((0, 0), 0);
    for _ in 0..maj {
        let QueryReply(ts, v) = rx.recv_msg_block();
        if ts > best.0 {
            best = (ts, v);
        }
    }
    best
}

/// `UpdatePhase`: send `(ts, v)` to every replica, wait for `maj` acks.
fn update_phase(me: usize, reps: &[Sender<ToReplica>], rx: &Receiver<UpdateAck>, maj: usize, ts: (i32, i32), v: i32) {
    for r in reps {
        r.send_msg(ToReplica::Update(me, ts, v));
    }
    for _ in 0..maj {
        let UpdateAck = rx.recv_msg_block();
    }
}

fn imp(n: usize, variant: Variant) {
    let maj = n / 2 + 1;
    let per_replica = if variant == Variant::Correct { 6 } else { 4 };
    let (tx_r, rx_r): (Vec<_>, Vec<_>) = (0..n).map(|_| chan::<ToReplica>()).unzip();
    let (tx_q, rx_q): (Vec<_>, Vec<_>) = (0..3).map(|_| chan::<QueryReply>()).unzip();
    let (tx_a, rx_a): (Vec<_>, Vec<_>) = (0..3).map(|_| chan::<UpdateAck>()).unzip();
    let (tx_op, rx_op): (Vec<_>, Vec<_>) = (0..3).map(|_| chan::<Op>()).unzip();
    let (tx_rep, rx_rep): (Vec<_>, Vec<_>) = (0..3).map(|_| chan::<Reply>()).unzip();
    let mut hs = Vec::new();
    for (i, rx) in rx_r.into_iter().enumerate() {
        let (tq, ta) = (tx_q.clone(), tx_a.clone());
        hs.push(named(&format!("rep{i}"), move || {
            let mut store: ((i32, i32), i32) = ((0, 0), 0);
            for _ in 0..per_replica {
                match rx.recv_msg_block() {
                    ToReplica::Query(p) => tq[p].send_msg(QueryReply(store.0, store.1)),
                    ToReplica::Update(p, ts, v) => {
                        if store.0 < ts {
                            store = (ts, v);
                        }
                        ta[p].send_msg(UpdateAck);
                    }
                }
            }
        }));
    }
    let mut rx_q = rx_q.into_iter();
    let mut rx_a = rx_a.into_iter();
    let mut rx_op = rx_op.into_iter();
    for p in 0..3 {
        let (rq, ra, ro) = (rx_q.next().unwrap(), rx_a.next().unwrap(), rx_op.next().unwrap());
        let reps = tx_r.clone();
        let to_c = tx_rep[p].clone();
        let name = ["wp", "rp1", "rp2"][p];
        hs.push(named(name, move || match ro.recv_msg_block() {
            Op::Write(v) => {
                let (ts, _) = query_phase(p, &reps, &rq, maj);
                update_phase(p, &reps, &ra, maj, (ts.0 + 1, WPID), v);
                to_c.send_msg(Reply::WriteAck);
            }
            Op::Read(_) => {
                let (ts, v) = query_phase(p, &reps, &rq, maj);
                if variant == Variant::Correct {
                    update_phase(p, &reps, &ra, maj, ts, v);
                }
                to_c.send_msg(Reply::ReadReply(v));
            }
        }));
    }
    let mut tx_op = tx_op.into_iter();
    let to = [tx_op.next().unwrap(), tx_op.next().unwrap(), tx_op.next().unwrap()];
    spawn_clients(to, rx_rep, &mut hs);
    for h in hs {
        h.join().unwrap();
    }
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let side = a.get(1).map(String::as_str).unwrap_or("spec");
    let n: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(3);
    let model = match a.get(3).map(String::as_str).unwrap_or("fifo") {
        "bag" => ConsType::Bag,
        "causal" => ConsType::Causal,
        _ => ConsType::FIFO,
    };
    let variant = match a.get(4).map(String::as_str).unwrap_or("correct") {
        "correct" => Variant::Correct,
        "read-no-writeback" => Variant::ReadNoWriteback,
        v => panic!("unknown variant {v}; one of correct, read-no-writeback"),
    };
    let conf = Config::builder().with_cons_type(model).with_verbose(0).build();
    let stats = match side {
        "impl" => verify(conf, move || imp(n, variant)),
        _ => verify(conf, move || spec(n)),
    };
    println!("execs={} blocked={}", stats.execs, stats.block);
    if a.get(5).map(String::as_str) == Some("witness") {
        println!("witness: executions with r1 reading 1 and then r2 reading 0: {}", INVERSIONS.load(Ordering::SeqCst));
    }
}
