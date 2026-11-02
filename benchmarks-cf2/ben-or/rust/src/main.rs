//! Ben-Or's consensus protocol (PODC 1983, section 3, step 3 (a)-(c), first variant) as a
//! TraceForge Must program, following INTERFACE.md.
//!
//! Usage: `bench_ben_or count impl [--model bag] -- <N>` (t = 1, R = 2, K = N clients).
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use traceforge::program::{main_with_model, Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

/// The protocol's fault bound: a process waits for N - t messages of a round.
const T: usize = 1;
/// The maximum number of rounds: a process that has not decided after round R reports so.
const R: i32 = 2;

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// client -> process: the process's initial value; the reply goes to `c{from}`.
    Input { from: i32, value: i32 },
    /// process -> client: the process decided `value`.
    Decide { value: i32 },
    /// process -> client: round R ended without a decision.
    Undecided {},
    /// The paper's type-1 message (1, r, x_p).
    Type1 { r: i32, x: i32 },
    /// The paper's type-2 message: `v = Some(v)` is (2, r, v, D), `v = None` is (2, r, ?).
    Type2 { r: i32, v: Option<i32> },
}

/// Tag of the type-1 messages of round r.
fn tag1(r: i32) -> u32 {
    2 * r as u32
}
/// Tag of the type-2 messages of round r.
fn tag2(r: i32) -> u32 {
    2 * r as u32 + 1
}

/// Process `pi`: runs the protocol with the initial value its client gives it.
fn process(n: usize, peers: Arc<Mutex<Vec<ThreadId>>>, clients: Arc<Mutex<Vec<Option<ThreadId>>>>) {
    // Initial value x_p from the client (the client's message is the only untagged one).
    let Msg::Input { from, value } = recv_tagged_msg_block::<_, Msg>(|_, tag| tag.is_none()) else {
        unreachable!()
    };
    let peers: Vec<ThreadId> = peers.lock().unwrap().clone();
    let client: ThreadId = clients.lock().unwrap()[from as usize].expect("client registered");
    let mut x: i32 = value;
    let mut decided: Option<i32> = None; // the write-once output register
    // step 0: r := 1
    for r in 1..=R {
        // step 1: send (1, r, x_p) to all processes
        for &q in &peers {
            send_tagged_msg(q, tag1(r), Msg::Type1 { r, x });
        }
        // step 2: wait till N - t messages of type (1, r, *) are received
        let mut count = [0usize; 2];
        for _ in 0..n - T {
            let Msg::Type1 { x: v, .. } =
                recv_tagged_msg_block::<_, Msg>(move |_, tag| tag == Some(tag1(r)))
            else {
                unreachable!()
            };
            count[v as usize] += 1;
        }
        // more than N/2 messages with the same value v: (2, r, v, D); else (2, r, ?)
        let maj: Option<i32> = (0..2).find(|&v| 2 * count[v as usize] > n);
        for &q in &peers {
            send_tagged_msg(q, tag2(r), Msg::Type2 { r, v: maj });
        }
        // step 3: wait till N - t messages of type (2, r, *) arrive
        let mut d_count = 0usize;
        let mut d_val: Option<i32> = None;
        for _ in 0..n - T {
            let Msg::Type2 { v, .. } =
                recv_tagged_msg_block::<_, Msg>(move |_, tag| tag == Some(tag2(r)))
            else {
                unreachable!()
            };
            if let Some(v) = v {
                d_count += 1;
                d_val = Some(v);
            }
        }
        match d_val {
            Some(v) => {
                // (a) there is a D-message (2, r, v, D): x_p := v
                x = v;
                // (b) more than t D-messages: decide v (write-once)
                if d_count > T && decided.is_none() {
                    decided = Some(v);
                    send_msg(client, Msg::Decide { value: v });
                }
            }
            // (c) else x_p := 1 or 0 each with probability 1/2
            None => x = if nondet() { 1 } else { 0 },
        }
        // step 4: r := r + 1, go to step 1
    }
    if decided.is_none() {
        send_msg(client, Msg::Undecided {});
    }
}

/// Client `ci`: sends Input { from: i, value: i mod 2 } to `pi` and waits for the reply.
fn client(i: usize, p: ThreadId, clients: Arc<Mutex<Vec<Option<ThreadId>>>>) {
    clients.lock().unwrap()[i] = Some(thread::current_id());
    send_msg(p, Msg::Input { from: i as i32, value: (i % 2) as i32 });
    let _reply: Msg = recv_msg_block();
}

fn ben_or(n: usize) {
    let peers: Arc<Mutex<Vec<ThreadId>>> = Arc::new(Mutex::new(Vec::new()));
    let clients: Arc<Mutex<Vec<Option<ThreadId>>>> = Arc::new(Mutex::new(vec![None; n]));
    let mut ps = Vec::new();
    for i in 0..n {
        let (pe, cl) = (peers.clone(), clients.clone());
        ps.push(
            thread::Builder::new()
                .name(format!("p{i}"))
                .spawn(move || process(n, pe, cl))
                .unwrap(),
        );
    }
    let ids: Vec<ThreadId> = ps.iter().map(|h| h.thread().id()).collect();
    *peers.lock().unwrap() = ids.clone();
    let mut cs = Vec::new();
    for i in 0..n {
        let (p, cl) = (ids[i], clients.clone());
        cs.push(
            thread::Builder::new()
                .name(format!("c{i}"))
                .spawn(move || client(i, p, cl))
                .unwrap(),
        );
    }
    for h in ps {
        h.join().unwrap();
    }
    for h in cs {
        h.join().unwrap();
    }
}

fn size(args: &[String]) -> usize {
    args.first().and_then(|s| s.parse().ok()).unwrap_or(3)
}

fn visible(args: &[String]) -> Vec<String> {
    (0..size(args)).map(|i| format!("c{i}")).collect()
}

fn entry(args: &[String]) -> Entry {
    let n = size(args);
    Arc::new(move || ben_or(n))
}

fn main() -> ExitCode {
    main_with_model(
        Program { visible: &visible, spec: &entry, imp: &entry },
        ConsType::Bag,
    )
}
