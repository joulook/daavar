//! Benchmark reliable-channel-aneris (candidate N12): Aneris's reliable FIFO channel over UDP
//! (`client_server_code.ml`), against a FIFO relay (our p2p channel as the specification).
//!
//! Visible: `sender` and `receiver`, the same program on both sides (Aneris's
//! `messages_in_order`): `sender` does K times `send c v` (v = 1..K) as a call to its endpoint,
//! `Send { v }` answered by `Queued`; `receiver` does K times `recv c` as a call to its endpoint,
//! `Recv` answered by `Deliver { v }`.
//!
//! * `spec`: two invisible stubs, `ep_s` (answers `Queued`, forwards `Data { seq, v }` once to
//!   `ep_r`) and `ep_r` (keeps the data by `seq` and answers each `Recv` with the next value in
//!   order): a lossless FIFO channel.
//! * `impl`: `ep_s` keeps the send buffer `sbuf` and transmits its head with sequence id
//!   `sidLB` (window 1) over a lossy, duplicating link: a transmission may be lost (`nondet()`,
//!   at most L = 1 loss; the retransmission timer then fires and the message is sent again) and
//!   may be duplicated (`nondet()`, at most D = 1 duplicate: a second send). `prune_sendbuf_at_ack`
//!   drops the acknowledged prefix. `ep_r` runs `process_data_on_chan`: a `Data` whose id is the
//!   next expected one (`ackId`) is enqueued in `rbuf`, any other is dropped, and every `Data`
//!   is answered with `Ack { ack: ackId }`.
//!
//! Usage: `<spec|impl> <K> [fifo|bag|causal] [variant]`; K messages, default model `bag`
//! (asyn: UDP reorders), default variant `correct`. Variants (impl side): `correct`, `no-seqno`
//! (`ep_r` ignores the sequence id: every `Data` is enqueued and counted).
//! Prints `execs=<complete> blocked=<blocked>`.
use traceforge::channel::{Builder, Receiver, Sender};
use traceforge::thread;
use traceforge::*;

/// Losses in all.
const L: usize = 1;
/// Duplications in all.
const D: usize = 1;

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// `send c v`: the sender's call.
    Send { v: u8 },
    /// Its return.
    Queued,
    /// `recv c`: the receiver's call.
    Recv,
    /// Its return.
    Deliver { v: u8 },
    /// A data packet `InjR (InjR (seq, v))`.
    Data { seq: u8, v: u8 },
    /// An acknowledgement `InjR (InjL ack)`: the next sequence id expected.
    Ack { ack: u8 },
}

type Tx = Sender<Msg>;
type Rx = Receiver<Msg>;

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

/// Inboxes: 0 sender, 1 receiver, 2 ep_s, 3 ep_r.
const SND: usize = 0;
const RCV: usize = 1;
const EPS: usize = 2;
const EPR: usize = 3;

fn sender(k: usize, rx: Rx, t: Vec<Tx>) {
    for v in 1..=k {
        t[EPS].send_msg(Msg::Send { v: v as u8 });
        let Msg::Queued = rx.recv_msg_block() else { panic!("sender") };
    }
}

fn receiver(k: usize, rx: Rx, t: Vec<Tx>) {
    for _ in 0..k {
        t[EPR].send_msg(Msg::Recv);
        let Msg::Deliver { .. } = rx.recv_msg_block() else { panic!("receiver") };
    }
}

/// Spawns `ep_s`, `ep_r` (given), then `sender`, `receiver`, and joins them all.
fn run(k: usize, ep_s: fn(usize, Rx, Vec<Tx>), ep_r: fn(usize, Rx, Vec<Tx>)) {
    let (tx, rxs): (Vec<Tx>, Vec<Rx>) = (0..4).map(|_| Builder::<Msg>::new().build()).unzip();
    let mut rxs: Vec<Option<Rx>> = rxs.into_iter().map(Some).collect();
    let mut hs = Vec::new();
    let (r, t) = (rxs[EPS].take().unwrap(), tx.clone());
    hs.push(named("ep_s", move || ep_s(k, r, t)));
    let (r, t) = (rxs[EPR].take().unwrap(), tx.clone());
    hs.push(named("ep_r", move || ep_r(k, r, t)));
    let (r, t) = (rxs[SND].take().unwrap(), tx.clone());
    hs.push(named("sender", move || sender(k, r, t)));
    let (r, t) = (rxs[RCV].take().unwrap(), tx.clone());
    hs.push(named("receiver", move || receiver(k, r, t)));
    for h in hs {
        h.join().unwrap();
    }
}

/// Spec `ep_s`: answer each call, forward the value once with its position.
fn spec_ep_s(k: usize, rx: Rx, t: Vec<Tx>) {
    for seq in 0..k {
        let Msg::Send { v } = rx.recv_msg_block() else { panic!("spec ep_s") };
        t[SND].send_msg(Msg::Queued);
        t[EPR].send_msg(Msg::Data { seq: seq as u8, v });
    }
}

/// Spec `ep_r`: deliver the values in position order, one per call.
fn spec_ep_r(k: usize, rx: Rx, t: Vec<Tx>) {
    let mut buf: Vec<Option<u8>> = vec![None; k];
    let mut next = 0; // next position to deliver
    let mut calls = 0; // calls not yet answered
    while next < k {
        match rx.recv_msg_block() {
            Msg::Data { seq, v } => buf[seq as usize] = Some(v),
            Msg::Recv => calls += 1,
            m => panic!("spec ep_r got {m:?}"),
        }
        while calls > 0 && next < k && buf[next].is_some() {
            t[RCV].send_msg(Msg::Deliver { v: buf[next].unwrap() });
            next += 1;
            calls -= 1;
        }
    }
}

/// Impl `ep_s`: `send` enqueues into `sbuf`; the head of `sbuf` is transmitted with id `sidLB`
/// (window 1); an `Ack` prunes the acknowledged prefix (`prune_sendbuf_at_ack`).
fn impl_ep_s(k: usize, rx: Rx, t: Vec<Tx>) {
    let mut sbuf: Vec<u8> = Vec::new(); // unacknowledged, from id sid_lb on
    let mut sid_lb: usize = 0;
    let mut queued = 0;
    let mut in_flight = false; // the head of sbuf has been transmitted
    let mut losses = L;
    let mut dups = D;
    let xmit = |seq: usize, v: u8, losses: &mut usize, dups: &mut usize| {
        // a lost transmission is retransmitted when the timer fires (at most L losses)
        while *losses > 0 && nondet() {
            *losses -= 1;
        }
        t[EPR].send_msg(Msg::Data { seq: seq as u8, v });
        if *dups > 0 && nondet() {
            *dups -= 1;
            t[EPR].send_msg(Msg::Data { seq: seq as u8, v });
        }
    };
    while queued < k || !sbuf.is_empty() {
        match rx.recv_msg_block() {
            Msg::Send { v } => {
                t[SND].send_msg(Msg::Queued);
                sbuf.push(v);
                queued += 1;
            }
            Msg::Ack { ack } => {
                let ack = ack as usize;
                if ack > sid_lb {
                    sbuf.drain(0..(ack - sid_lb).min(sbuf.len()));
                    sid_lb = ack;
                    in_flight = false;
                }
            }
            m => panic!("ep_s got {m:?}"),
        }
        if !in_flight && !sbuf.is_empty() {
            xmit(sid_lb, sbuf[0], &mut losses, &mut dups);
            in_flight = true;
        }
    }
}

/// Impl `ep_r`, `process_data_on_chan` (+ `recv`): the next expected id is enqueued, every
/// `Data` is acknowledged with the current `ackId`. `seqno == false` is the fault: the id is
/// ignored. Ends after K deliveries.
fn ep_r_with(k: usize, rx: Rx, t: Vec<Tx>, seqno: bool) {
    let mut rbuf: std::collections::VecDeque<u8> = Default::default();
    let mut ack_id: u8 = 0;
    let mut calls = 0;
    let mut delivered = 0;
    while delivered < k {
        match rx.recv_msg_block() {
            Msg::Data { seq, v } => {
                if !seqno || seq == ack_id {
                    rbuf.push_back(v);
                    ack_id += 1;
                }
                t[EPS].send_msg(Msg::Ack { ack: ack_id });
            }
            Msg::Recv => calls += 1,
            m => panic!("ep_r got {m:?}"),
        }
        while calls > 0 && !rbuf.is_empty() && delivered < k {
            t[RCV].send_msg(Msg::Deliver { v: rbuf.pop_front().unwrap() });
            calls -= 1;
            delivered += 1;
        }
    }
}

fn impl_ep_r(k: usize, rx: Rx, t: Vec<Tx>) {
    ep_r_with(k, rx, t, true)
}

fn impl_ep_r_no_seqno(k: usize, rx: Rx, t: Vec<Tx>) {
    ep_r_with(k, rx, t, false)
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let side = a.get(1).map(String::as_str).unwrap_or("spec").to_string();
    let k: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(2);
    let model = match a.get(3).map(String::as_str).unwrap_or("bag") {
        "fifo" => ConsType::FIFO,
        "causal" => ConsType::Causal,
        _ => ConsType::Bag,
    };
    let variant = a.get(4).cloned().unwrap_or_else(|| "correct".to_string());
    let ep_r: fn(usize, Rx, Vec<Tx>) = match variant.as_str() {
        "correct" => impl_ep_r,
        "no-seqno" => impl_ep_r_no_seqno,
        v => panic!("unknown variant {v}"),
    };
    let conf = Config::builder().with_cons_type(model).with_verbose(0).build();
    let stats = match side.as_str() {
        "impl" => verify(conf, move || run(k, impl_ep_s, ep_r)),
        _ => verify(conf, move || run(k, spec_ep_s, spec_ep_r)),
    };
    println!("execs={} blocked={}", stats.execs, stats.block);
}
