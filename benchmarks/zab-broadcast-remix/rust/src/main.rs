//! Benchmark zab-broadcast-remix (candidate N18): the broadcast phase of Zab (`Zab.tla`, the
//! protocol spec vendored in the Remix artifact), with the one-shot join of a late follower
//! through which all three bugs of `issues.md` are reached, against one sequencer server.
//!
//! Visible: clients `c0`, `c1`, each doing Q times `Write { c, v }` and waiting for
//! `Committed { zxid }` (the same program on both sides; `v = 10 c + j + 1` for the j-th write).
//! `c0` talks to the leader, `c1` to the late follower (ZooKeeper forwards a follower's writes
//! to the leader; `Zab.tla`'s `LeaderProcessRequest` abstracts this away).
//!
//! * `spec`: one invisible `server` reads the 2Q writes in some order, gives the n-th the zxid n
//!   and answers `Committed { zxid: n }` to its client.
//! * `impl`: `s0` the leader (epoch fixed, in BROADCAST with `s1` in `ackeRecv` and `ackldRecv`
//!   from the start, empty history), `s1` a synchronised follower, `s2` a late follower that joins
//!   with `ACKEPOCH -> NEWLEADER(history) -> ACKLD -> COMMITLD(zxid)` and serves `c1`'s writes
//!   once it has processed `COMMITLD`. Broadcast: the leader appends a write to `history` and
//!   sends `PROPOSE` to `ackeRecv` (`Broadcast`, the fix of Issue 1); a follower appends the next
//!   zxid and answers `ACK` (`FollowerProcessPROPOSE`); the leader commits the next index on a
//!   quorum (2 of 3, itself included) and sends `COMMIT` to `ackldRecv`
//!   (`LeaderTryToCommit`/`DiscardAndBroadcast`); a late `ACKLD` is answered with
//!   `COMMITLD(lastCommitted.zxid)` (`LeaderProcessACKLD` case 2, the fix of Issue 3). A server
//!   answers `Committed` to its own client when it commits that client's entry.
//!
//! Usage: `<spec|impl> <Q> [fifo|bag|causal] [variant]`; Q writes per client, 2 clients, 3
//! servers, default model `fifo` (p2p: `Zab.tla`'s `msgs[i][j]` are FIFO queues), default
//! variant `correct`. Variants (impl side): `correct`, `issue1-propose-to-ackld` (Issue 1:
//! `PROPOSE` goes to `ackldRecv` instead of `ackeRecv`).
//! Prints `execs=<complete> blocked=<blocked>`.
use traceforge::channel::{Builder, Receiver, Sender};
use traceforge::thread;
use traceforge::*;

const QUORUM: u32 = 2;

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// A client write (also a follower's forward of it to the leader).
    Write { c: u8, v: u8 },
    /// The answer to a client: its write's zxid.
    Committed { zxid: u8 },
    /// `ACKEPOCH` of the late follower.
    AckEpoch,
    /// `NEWLEADER` with the leader's history `(zxid, client, value)`.
    NewLeader { hist: Vec<(u8, u8, u8)> },
    /// `ACKLD` with the last zxid of the history received.
    AckLd { zxid: u8 },
    /// `COMMITLD`.
    CommitLd { zxid: u8 },
    /// `PROPOSE`.
    Propose { zxid: u8, c: u8, v: u8 },
    /// `ACK` from follower `from`.
    Ack { zxid: u8, from: u8 },
    /// `COMMIT`.
    Commit { zxid: u8 },
}

type Tx = Sender<Msg>;
type Rx = Receiver<Msg>;

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

/// Inboxes: 0 c0, 1 c1, 2 server (spec) / s0 (impl), 3 s1, 4 s2.
const C0: usize = 0;
const S0: usize = 2;

fn client(c: usize, q: usize, rx: Rx, to: Tx) {
    for j in 0..q {
        to.send_msg(Msg::Write { c: c as u8, v: (10 * c + j + 1) as u8 });
        let Msg::Committed { .. } = rx.recv_msg_block() else { panic!("c{c}") };
    }
}

fn chans(k: usize) -> (Vec<Tx>, Vec<Option<Rx>>) {
    let (tx, rx): (Vec<Tx>, Vec<Rx>) = (0..k).map(|_| Builder::<Msg>::new().build()).unzip();
    (tx, rx.into_iter().map(Some).collect())
}

fn spec(q: usize) {
    let (tx, mut rx) = chans(3);
    let mut hs = Vec::new();
    let (r, t) = (rx[S0].take().unwrap(), tx.clone());
    hs.push(named("server", move || {
        for n in 1..=2 * q {
            let Msg::Write { c, .. } = r.recv_msg_block() else { panic!("server") };
            t[c as usize].send_msg(Msg::Committed { zxid: n as u8 });
        }
    }));
    for c in 0..2 {
        let (r, to) = (rx[c].take().unwrap(), tx[S0].clone());
        hs.push(named(&format!("c{c}"), move || client(c, q, r, to)));
    }
    for h in hs {
        h.join().unwrap();
    }
}

struct Txn {
    zxid: u8,
    c: u8,
    v: u8,
    acks: u32, // ackSid, bit per server
}

/// The leader `s0`. `t`: c0, c1, s0, s1, s2. Servers are numbered 0, 1, 2 (inbox 2 + sid).
fn leader(rx: Rx, t: Vec<Tx>, propose_to_ackld: bool) {
    let mut history: Vec<Txn> = Vec::new();
    let mut last_committed: usize = 0; // index (= zxid, one epoch)
    let mut ackerecv: u32 = 1 << 1; // followers in ackeRecv (s1 synchronised before broadcast)
    let mut ackldrecv: u32 = 1 << 1; // followers in ackldRecv
    loop {
        match rx.recv_msg_block() {
            Msg::Write { c, v } => {
                // LeaderProcessRequest + LeaderBroadcastPROPOSE (one step)
                let zxid = history.len() as u8 + 1;
                history.push(Txn { zxid, c, v, acks: 1 << 0 });
                let q = if propose_to_ackld { ackldrecv } else { ackerecv };
                for f in 1..3 {
                    if q & (1 << f) != 0 {
                        t[S0 + f].send_msg(Msg::Propose { zxid, c, v });
                    }
                }
            }
            Msg::Ack { zxid, from } => {
                // LeaderProcessACK + LeaderTryToCommit
                let index = zxid as usize;
                history[index - 1].acks |= 1 << from;
                if last_committed + 1 == index && history[index - 1].acks.count_ones() >= QUORUM {
                    last_committed = index;
                    for f in 1..3 {
                        if ackldrecv & (1 << f) != 0 {
                            t[S0 + f].send_msg(Msg::Commit { zxid });
                        }
                    }
                    if history[index - 1].c as usize == C0 {
                        t[C0].send_msg(Msg::Committed { zxid });
                    }
                }
            }
            Msg::AckEpoch => {
                // LeaderProcessACKEPOCH, case 1 (NEWLEADER already broadcast): reply NEWLEADER
                ackerecv |= 1 << 2;
                let hist = history.iter().map(|x| (x.zxid, x.c, x.v)).collect();
                t[S0 + 2].send_msg(Msg::NewLeader { hist });
            }
            Msg::AckLd { zxid } => {
                // LeaderProcessACKLD, case 2 (COMMITLD already broadcast)
                ackldrecv |= 1 << 2;
                for x in history.iter_mut() {
                    if x.zxid <= zxid {
                        x.acks |= 1 << 2;
                    }
                }
                t[S0 + 2].send_msg(Msg::CommitLd { zxid: last_committed as u8 });
            }
            m => panic!("s0 got {m:?}"),
        }
    }
}

/// A follower `s<sid>`; `client`: the client it serves (`c1` for `s2`), `late`: joins late.
fn follower(sid: u8, client: Option<usize>, late: bool, rx: Rx, t: Vec<Tx>) {
    let mut log: Vec<(u8, u8, u8)> = Vec::new(); // (zxid, client, value)
    let mut committed: usize = 0;
    let mut synced = !late;
    let mut waiting: Vec<Msg> = Vec::new(); // client writes before the follower is synchronised
    let reply = |e: &(u8, u8, u8)| {
        if Some(e.1 as usize) == client {
            t[e.1 as usize].send_msg(Msg::Committed { zxid: e.0 });
        }
    };
    if late {
        t[S0].send_msg(Msg::AckEpoch);
    }
    loop {
        match rx.recv_msg_block() {
            w @ Msg::Write { .. } => {
                if synced {
                    t[S0].send_msg(w);
                } else {
                    waiting.push(w);
                }
            }
            Msg::NewLeader { hist } => {
                // FollowerProcessNEWLEADER: take the history, ACKLD its last zxid
                log = hist;
                let last = log.last().map(|e| e.0).unwrap_or(0);
                t[S0].send_msg(Msg::AckLd { zxid: last });
            }
            Msg::CommitLd { zxid } => {
                // FollowerProcessCOMMITLD: commit every txn up to zxid that is in the log
                while committed < log.len() && log[committed].0 <= zxid {
                    reply(&log[committed]);
                    committed += 1;
                }
                synced = true;
                for w in waiting.drain(..) {
                    t[S0].send_msg(w);
                }
            }
            Msg::Propose { zxid, c, v } => {
                // FollowerProcessPROPOSE: append and ACK the next zxid; otherwise discard
                let last = log.last().map(|e| e.0).unwrap_or(0);
                if zxid == last + 1 {
                    log.push((zxid, c, v));
                    t[S0].send_msg(Msg::Ack { zxid, from: sid });
                }
            }
            Msg::Commit { zxid } => {
                // FollowerProcessCOMMIT: commit the next pending txn if it is this zxid
                if committed < log.len() && log[committed].0 == zxid {
                    reply(&log[committed]);
                    committed += 1;
                }
            }
            m => panic!("s{sid} got {m:?}"),
        }
    }
}

fn imp(q: usize, propose_to_ackld: bool) {
    let (tx, mut rx) = chans(5);
    let mut hs = Vec::new();
    let (r, t) = (rx[S0].take().unwrap(), tx.clone());
    hs.push(named("s0", move || leader(r, t, propose_to_ackld)));
    let (r, t) = (rx[S0 + 1].take().unwrap(), tx.clone());
    hs.push(named("s1", move || follower(1, None, false, r, t)));
    let (r, t) = (rx[S0 + 2].take().unwrap(), tx.clone());
    hs.push(named("s2", move || follower(2, Some(1), true, r, t)));
    // c0 writes to the leader, c1 to the late follower
    for (c, srv) in [(0usize, S0), (1usize, S0 + 2)] {
        let (r, to) = (rx[c].take().unwrap(), tx[srv].clone());
        hs.push(named(&format!("c{c}"), move || client(c, q, r, to)));
    }
    for h in hs {
        h.join().unwrap();
    }
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let side = a.get(1).map(String::as_str).unwrap_or("spec").to_string();
    let q: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(1);
    let model = match a.get(3).map(String::as_str).unwrap_or("fifo") {
        "bag" => ConsType::Bag,
        "causal" => ConsType::Causal,
        _ => ConsType::FIFO,
    };
    let variant = a.get(4).cloned().unwrap_or_else(|| "correct".to_string());
    let propose_to_ackld = match variant.as_str() {
        "correct" => false,
        "issue1-propose-to-ackld" => true,
        v => panic!("unknown variant {v}"),
    };
    let conf = Config::builder().with_cons_type(model).with_verbose(0).build();
    let stats = match side.as_str() {
        "impl" => verify(conf, move || imp(q, propose_to_ackld)),
        _ => verify(conf, move || spec(q)),
    };
    println!("execs={} blocked={}", stats.execs, stats.block);
}
