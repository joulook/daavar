//! Pineapple (NSDI 2025), one key, common case (stable leader n0, no failure):
//! the derived *specification* of the single-key interface.
//!
//! The paper proves (Appendix B) that every execution of Algorithms 1 and 2 has
//! a total order S of its completed operations that respects real-time order
//! (Lemma 1) and is legal for a register with RMW (Lemma 2). The specification is
//! therefore one atomic object holding the key: it serves the clients' requests
//! one at a time, in the order they reach it (that order is the linearization
//! order S, the protocol's only freedom), applying each to its current value.
//! No nodes, tags, pstamps, quorums or node-to-node messages are modelled.
//!
//! Usage (shared main): `<bin> count spec|impl [--model fifo|bag] -- <q>`.
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ---- message types, verbatim from INTERFACE.md --------------------------------
#[derive(Clone, Debug, PartialEq)]
enum Req {
    Write(usize, u64),
    Read(usize),
    Rmw(usize),
} // client -> node: (client index, ...)
#[derive(Clone, Debug, PartialEq)]
enum Rep {
    Ack,
    Value(u64),
    Result(u64),
} // node -> client

/// The setup the invisible `init` thread sends to the specification's object.
#[derive(Clone, Debug, PartialEq)]
struct Setup {
    nodes: Vec<ThreadId>,
    clients: Vec<ThreadId>,
}

/// Tag carried only by the Setup message, so the object takes Setup first and
/// leaves any request that arrived earlier in its queue (buffered).
const SETUP_TAG: u32 = 1;

// ---- client code, verbatim from INTERFACE.md ----------------------------------
const CLIENTS: usize = 2;
fn value(i: usize, j: usize) -> u64 { (1 + i + CLIENTS * j) as u64 }
fn client(i: usize, q: usize, node: ThreadId) {
    for j in 0..q {
        match (i + j) % 3 {
            0 => { send_msg(node, Req::Write(i, value(i, j))); let Rep::Ack = recv_msg_block::<Rep>() else { panic!("Ack expected") }; }
            1 => { send_msg(node, Req::Read(i)); let Rep::Value(_) = recv_msg_block::<Rep>() else { panic!("Value expected") }; }
            _ => { send_msg(node, Req::Rmw(i)); let Rep::Result(_) = recv_msg_block::<Rep>() else { panic!("Result expected") }; }
        }
    }
}

/// The one-shot transaction of the interface: f(v) = v + 10.
fn f(v: u64) -> u64 {
    v + 10
}

/// The atomic single-key object. It serves exactly `total` requests (all the
/// clients' operations) and then ends.
fn object(total: usize) {
    let Setup { clients, .. } =
        recv_tagged_msg_block::<_, Setup>(|_, tag| tag == Some(SETUP_TAG));
    // The key's value, initially 0 (the paper's "null value").
    let mut key: u64 = 0;
    for _ in 0..total {
        // Which pending request is received next is the linearization point:
        // the order of arrival is the order S of Appendix B.
        let req = recv_tagged_msg_block::<_, Req>(|_, tag| tag.is_none());
        match req {
            Req::Write(i, v) => {
                key = v;
                send_msg(clients[i], Rep::Ack);
            }
            Req::Read(i) => {
                send_msg(clients[i], Rep::Value(key));
            }
            Req::Rmw(i) => {
                key = f(key);
                send_msg(clients[i], Rep::Result(key));
            }
        }
    }
}

fn spec(q: usize) {
    let obj = thread::Builder::new()
        .name("object".to_string())
        .spawn(move || object(CLIENTS * q))
        .unwrap();
    let oid = obj.thread().id();
    let mut cs = Vec::new();
    for i in 0..CLIENTS {
        cs.push(
            thread::Builder::new()
                .name(format!("c{i}"))
                .spawn(move || client(i, q, oid))
                .unwrap(),
        );
    }
    let cids: Vec<ThreadId> = cs.iter().map(|c| c.thread().id()).collect();
    let init = thread::Builder::new()
        .name("init".to_string())
        .spawn(move || {
            send_tagged_msg(oid, SETUP_TAG, Setup { nodes: vec![oid], clients: cids });
        })
        .unwrap();
    init.join().unwrap();
    for c in cs {
        c.join().unwrap();
    }
    obj.join().unwrap();
}

fn q_of(args: &[String]) -> usize {
    args.first().and_then(|s| s.parse().ok()).unwrap_or(1).max(1)
}

fn entry(args: &[String]) -> Entry {
    let q = q_of(args);
    Arc::new(move || spec(q))
}

fn main() -> ExitCode {
    traceforge::program::main(Program {
        visible: &|_| (0..CLIENTS).map(|i| format!("c{i}")).collect(),
        spec: &entry,
        imp: &entry,
    })
}
