//! ZooKeeper atomic broadcast (Zab), fixed leader n0 of epoch 1, no failure:
//! the client-observable specification derived from Zab.tla (protocol-spec).
//!
//! What a client can observe (Zab.tla invariants, see NOTES.md):
//! - there is one broadcast order T of all proposals: the order in which the
//!   leader appends client requests to its history (LeaderProcessRequest,
//!   l.886) and broadcasts them (LeaderBroadcastPROPOSE, l.905);
//! - every server's delivered sequence is a prefix of T (PrefixConsistency
//!   l.1160, TotalOrder l.1190, LocalPrimaryOrder l.1206, Integrity l.1168);
//! - with no failure and a fixed leader nothing is lost: every server delivers
//!   all of T (decision D1 in NOTES.md).
//! So the specification is one sequencer (`n0`) that fixes T by the arrival
//! order of the proposals, and one delivery point per client (`n0` for c0,
//! `n1` for c1) that sends `Committed` for all of T in T's order.
//! No servers, epochs, zxids, quorums or server-to-server packets are modelled.
//!
//! Usage: `<bin> count|export|conf … -- <e> <late> [fifo|bag]` (program.rs).
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

#[derive(Clone, Debug, PartialEq)]
enum Req {
    Propose(u64),
} // client -> server
#[derive(Clone, Debug, PartialEq)]
enum Rep {
    Committed(u64),
} // server -> client

/// The servers' setup, sent by the invisible `init` thread (INTERFACE.md).
#[derive(Clone, Debug, PartialEq)]
struct Setup {
    nodes: Vec<ThreadId>,
    clients: Vec<ThreadId>,
}

/// Tags of the invisible messages (client messages are untagged).
const SETUP: u32 = 1; // init -> n0, n1: Setup
const FWD: u32 = 2; // n1 -> n0: the follower's client proposal (REQUEST), a Req
const DELIVER: u32 = 3; // n0 -> n1: the next transaction of T (a Rep)

const CLIENTS: usize = 2;
fn value(i: usize, j: usize) -> u64 {
    (1 + i + CLIENTS * j) as u64
}
fn client(i: usize, e: usize, server: ThreadId) {
    let mine: Vec<u64> = (0..e).map(|j| value(i, j)).collect();
    for v in &mine {
        send_msg(server, Req::Propose(*v));
    }
    let mut seen = 0usize;
    while seen < e {
        let Rep::Committed(v) = recv_msg_block::<Rep>();
        if mine.contains(&v) {
            seen += 1;
        }
    }
}

fn recv_setup() -> Setup {
    recv_tagged_msg_block::<_, Setup>(|_, t| t == Some(SETUP))
}

/// `n0`: the abstract broadcast. It fixes T as the order in which proposals
/// reach it (its own client's directly, the follower's forwarded), and
/// delivers every transaction of T, in T's order, to its client and to `n1`.
fn sequencer(e: usize) {
    let s = recv_setup();
    let (c0, n1) = (s.clients[0], s.nodes[1]);
    for _ in 0..CLIENTS * e {
        // untagged from c0, FWD from n1; never the (tagged) Setup
        let Req::Propose(v) = recv_tagged_msg_block::<_, Req>(|_, t| t.is_none() || t == Some(FWD));
        send_msg(c0, Rep::Committed(v));
        send_tagged_msg(n1, DELIVER, Rep::Committed(v));
    }
}

/// `n1`: the delivery point of c1. It hands its client's proposals, in their
/// arrival order, to the sequencer, then delivers all of T to its client in
/// the order `n0` hands it over.
fn follower(e: usize) {
    let s = recv_setup();
    let (c1, n0) = (s.clients[1], s.nodes[0]);
    for _ in 0..e {
        let Req::Propose(v) = recv_tagged_msg_block::<_, Req>(|_, t| t.is_none());
        send_tagged_msg(n0, FWD, Req::Propose(v));
    }
    for _ in 0..CLIENTS * e {
        let Rep::Committed(v) = recv_tagged_msg_block::<_, Rep>(|_, t| t == Some(DELIVER));
        send_msg(c1, Rep::Committed(v));
    }
}

fn spec(e: usize, _late: usize) {
    let named = |n: &str| thread::Builder::new().name(n.to_string());
    let n0 = named("n0").spawn(move || sequencer(e)).unwrap();
    let n1 = named("n1").spawn(move || follower(e)).unwrap();
    // n2 has no client and is not needed for a quorum: invisible, nothing to do (D3)
    let n2 = named("n2").spawn(|| {}).unwrap();
    let nodes: Vec<ThreadId> = vec![n0.thread().id(), n1.thread().id(), n2.thread().id()];
    let c0 = {
        let s = nodes[0];
        named("c0").spawn(move || client(0, e, s)).unwrap()
    };
    let c1 = {
        let s = nodes[1];
        named("c1").spawn(move || client(1, e, s)).unwrap()
    };
    let clients = vec![c0.thread().id(), c1.thread().id()];
    let init = named("init")
        .spawn(move || {
            for k in 0..2 {
                send_tagged_msg(nodes[k], SETUP, Setup { nodes: nodes.clone(), clients: clients.clone() });
            }
        })
        .unwrap();
    for h in [n0, n1, n2, c0, c1, init] {
        h.join().unwrap();
    }
}

fn parse(a: &[String]) -> (usize, usize) {
    let e = a.first().and_then(|s| s.parse().ok()).unwrap_or(1);
    let late = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    assert!(e >= 1 && late <= 1, "arguments: <e >= 1> <late in 0|1>");
    (e, late)
}

fn entry(a: &[String]) -> Entry {
    let (e, late) = parse(a);
    Arc::new(move || spec(e, late))
}

fn main() -> ExitCode {
    traceforge::program::main(Program {
        visible: &|_| vec!["c0".to_string(), "c1".to_string()],
        spec: &entry,
        imp: &entry,
    })
}
