//! SwiftPaxos on one key (N = 3, K = 2 clients, q commands each, ballot 0, leader n0, no
//! failure): the client-observable *specification*, derived from the paper's SMR
//! properties (Validity, Integrity, Ordering, Nontriviality, Liveness), Invariants 1-3 and
//! the leader's optimistic execution (section 4.4, Appendix A.5). See NOTES.md.
//!
//! Abstraction: one state-machine thread `sm` stands for "the order in which the leader
//! receives (and optimistically executes) the commands". It applies each command to the
//! register `x` in the order the commands reach it; that arrival order is the only freedom
//! (any interleaving of the two clients' program orders). No replicas, ballots, quorums,
//! dependencies or replica-to-replica messages are modelled.
use std::sync::Arc;

use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ---- interface (verbatim) -------------------------------------------------------------
#[derive(Clone, Debug, PartialEq)]
enum Req {
    Write(usize, u64),
    Read(usize),
    Rmw(usize),
}
#[derive(Clone, Debug, PartialEq)]
enum Rep {
    Ack,
    Value(u64),
    Result(u64),
}

#[derive(Clone, Debug, PartialEq)]
struct Setup {
    nodes: Vec<ThreadId>,
    libs: Vec<ThreadId>,
    clients: Vec<ThreadId>,
}

const CLIENTS: usize = 2;
const SETUP_TAG: u32 = 1;

fn value(i: usize, j: usize) -> u64 {
    (1 + i + CLIENTS * j) as u64
}
fn client(i: usize, q: usize, node: ThreadId) {
    for j in 0..q {
        match (i + j) % 3 {
            0 => { send_msg(node, Req::Write(i, value(i, j))); let Rep::Ack = recv_msg_block::<Rep>() else { panic!("Ack expected") }; }
            1 => { send_msg(node, Req::Read(i)); let Rep::Value(_) = recv_msg_block::<Rep>() else { panic!("Value expected") }; }
            _ => { send_msg(node, Req::Rmw(i)); let Rep::Result(_) = recv_msg_block::<Rep>() else { panic!("Result expected") }; }
        }
    }
}

// ---- the specification's internal processes --------------------------------------------

/// Receives this thread's Setup (tagged), whatever else is already buffered.
fn recv_setup() -> Setup {
    recv_tagged_msg_block::<_, Setup>(|_, tag| tag == Some(SETUP_TAG))
}

/// The abstract replicated state machine: the register x, initially 0. It serves the
/// 2q commands one at a time, in the order they arrive (= the leader's pending_log order),
/// and answers each with the result of that command applied after all earlier ones.
/// Ends once all 2q commands are served.
fn state_machine(q: usize) {
    let setup = recv_setup();
    let libs = setup.libs.clone();
    let mut x: u64 = 0;
    for _ in 0..CLIENTS * q {
        let lib_ids = libs.clone();
        let req: Req = recv_tagged_msg_block(move |tid, tag| tag.is_none() && lib_ids.contains(&tid));
        let (i, rep) = match req {
            Req::Write(i, v) => {
                x = v;
                (i, Rep::Ack)
            }
            Req::Read(i) => (i, Rep::Value(x)),
            Req::Rmw(i) => {
                x += 10;
                (i, Rep::Result(x))
            }
        };
        send_msg(libs[i], rep);
    }
}

/// Client library li: forwards each request of ci to the state machine, waits for its
/// result and returns it to ci. Ends after ci's q commands.
fn library(i: usize, q: usize) {
    let setup = recv_setup();
    let me_client = setup.clients[i];
    let sm = setup.nodes[0];
    for _ in 0..q {
        let req: Req = recv_tagged_msg_block(move |tid, tag| tag.is_none() && tid == me_client);
        send_msg(sm, req);
        let rep: Rep = recv_tagged_msg_block(move |tid, tag| tag.is_none() && tid == sm);
        send_msg(me_client, rep);
    }
}

fn spawn_named<F: FnOnce() + Send + 'static>(name: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name(name.to_string())
        .spawn(f)
        .expect("spawn")
}

fn spec(q: usize) {
    // processes in the interface's order: (abstract) replica side, libraries, clients
    let sm = spawn_named("sm", move || state_machine(q));
    let mut libs = Vec::new();
    for i in 0..CLIENTS {
        libs.push(spawn_named(&format!("l{i}"), move || library(i, q)));
    }
    let lib_ids: Vec<ThreadId> = libs.iter().map(|h| h.thread().id()).collect();
    let mut clients = Vec::new();
    for i in 0..CLIENTS {
        let node = lib_ids[i];
        clients.push(spawn_named(&format!("c{i}"), move || client(i, q, node)));
    }
    let setup = Setup {
        nodes: vec![sm.thread().id()],
        libs: lib_ids,
        clients: clients.iter().map(|h| h.thread().id()).collect(),
    };
    let init = spawn_named("init", move || {
        for &t in setup.nodes.iter().chain(setup.libs.iter()) {
            send_tagged_msg(t, SETUP_TAG, setup.clone());
        }
    });
    for c in clients {
        c.join().unwrap();
    }
    for l in libs {
        l.join().unwrap();
    }
    sm.join().unwrap();
    init.join().unwrap();
}

fn q_of(args: &[String]) -> usize {
    args.first().and_then(|s| s.parse().ok()).unwrap_or(1)
}

fn main() -> std::process::ExitCode {
    let visible = |_: &[String]| vec!["c0".to_string(), "c1".to_string()];
    let entry = |args: &[String]| -> Entry {
        let q = q_of(args);
        Arc::new(move || spec(q))
    };
    traceforge::program::main(Program {
        visible: &visible,
        spec: &entry,
        imp: &entry,
    })
}
