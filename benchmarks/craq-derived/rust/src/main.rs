//! 3FS chain replication (CRAQ) of one chunk on one chain of three targets: the specification
//! derived from 3FS's documentation ("Strong Consistency ... CRAQ") and its P model
//! (specs/DataStorage). What the two clients can observe is exactly what one atomic register,
//! initially 0, shows: every Write and every Read takes effect at one instant between its request
//! and its reply, in one total order consistent with each client's program order. The register is
//! one object thread that serves requests one at a time, in the order they reach it; that arrival
//! order is the only freedom, so no `nondet()` choice is needed (see NOTES.md).
//! Usage: `bench_craq_derived count spec|impl [--model fifo|bag] -- <q>`.
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{self, Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

#[derive(Clone, Debug, PartialEq)]
enum Req {
    Write(usize, u64),
    Read(usize),
} // client -> node: (client, value) / (client)
#[derive(Clone, Debug, PartialEq)]
enum Rep {
    Ack(usize),
    Value(usize, u64),
} // node -> client: (client) / (client, value)

/// The invisible setup message: the clients' addresses, sent once by `init` to the object.
#[derive(Clone, Debug, PartialEq)]
struct Setup(Vec<ThreadId>);
const SETUP_TAG: u32 = 1;

const CLIENTS: usize = 2;
fn value(i: usize, j: usize) -> u64 {
    (1 + i + CLIENTS * j) as u64
}

/// The interface's client loop; in the specification the read goes directly to the one object
/// (`head` and the only element of `targets` are the object), so there is no not-committed answer
/// and no retry.
fn client(i: usize, q: usize, head: ThreadId, targets: Vec<ThreadId>) {
    for j in 0..q {
        match (i + j) % 2 {
            0 => {
                send_msg(head, Req::Write(i, value(i, j)));
                let Rep::Ack(_) = recv_msg_block::<Rep>() else { panic!("Ack expected") };
            }
            _ => {
                send_msg(targets[0], Req::Read(i));
                let Rep::Value(..) = recv_msg_block::<Rep>() else { panic!("Value expected") };
            }
        }
    }
}

/// The chunk as the clients see it: one register, initially 0 (the chunk committed at version 0
/// at every target). Writes of the chunk are applied and committed one at a time, in one order
/// (the head's version order); a read returns the content of the last committed write at the
/// instant it is served. Ends after serving all 2*q requests.
fn object(q: usize) {
    let Setup(clients) = recv_tagged_msg_block::<_, Setup>(|_, t| t == Some(SETUP_TAG));
    let mut chunk: u64 = 0;
    for _ in 0..CLIENTS * q {
        match recv_tagged_msg_block::<_, Req>(|_, t| t.is_none()) {
            Req::Write(c, v) => {
                chunk = v;
                send_msg(clients[c], Rep::Ack(c));
            }
            Req::Read(c) => send_msg(clients[c], Rep::Value(c, chunk)),
        }
    }
}

fn spec_program(q: usize) {
    let obj = thread::Builder::new()
        .name("obj".to_string())
        .spawn(move || object(q))
        .unwrap();
    let oid = obj.thread().id();
    let mut cs = Vec::new();
    for i in 0..CLIENTS {
        cs.push(
            thread::Builder::new()
                .name(format!("c{i}"))
                .spawn(move || client(i, q, oid, vec![oid]))
                .unwrap(),
        );
    }
    let cids: Vec<ThreadId> = cs.iter().map(|h| h.thread().id()).collect();
    let init = thread::Builder::new()
        .name("init".to_string())
        .spawn(move || send_tagged_msg(oid, SETUP_TAG, Setup(cids)))
        .unwrap();
    init.join().unwrap();
    for c in cs {
        c.join().unwrap();
    }
    obj.join().unwrap();
}

fn q_of(a: &[String]) -> usize {
    a.first().and_then(|s| s.parse().ok()).unwrap_or(1)
}

fn entry(a: &[String]) -> Entry {
    let q = q_of(a);
    Arc::new(move || spec_program(q))
}

fn visible(_a: &[String]) -> Vec<String> {
    (0..CLIENTS).map(|i| format!("c{i}")).collect()
}

fn main() -> ExitCode {
    program::main(Program { visible: &visible, spec: &entry, imp: &entry })
}
