//! Ricart–Agrawala mutual exclusion (CACM 24(1), 1981, section 2) on TraceForge's Must API.
//! Usage: `bench_ricart_agrawala count impl --model bag -- <N>`.
//! Processes: nodes n0..n{N-1} (the paper's algorithm, one process per node handling its
//! messages in arrival order) and clients c0..c{N-1} (visible); main only spawns and joins.
use std::process::ExitCode;
use std::sync::Arc;

mod spec; // the coordinator's hand-written specification (plan/twins-contextfree/ricart-agrawala/spec.rs); the agent's program is `entry`
use traceforge::program::{main_with_model, Entry, Program};
use traceforge::thread::{self, construct_thread_id, ThreadId};
use traceforge::*;

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    // client -> node
    Enter { from: i32 },
    Exit { from: i32 },
    // node -> client
    Granted {},
    Released {},
    // node -> node: the paper's REQUEST(k, j) and REPLY
    Request { seq: i32, from: i32 },
    Reply {},
}

/// Thread ids: main is t0; main spawns n0..n{N-1} then c0..c{N-1}, so ni = t(1+i), ci = t(1+N+i).
/// `run` asserts that the spawned handles carry exactly these ids.
fn node_id(i: usize) -> ThreadId {
    construct_thread_id(1 + i as u32)
}
fn client_id(n: usize, i: usize) -> ThreadId {
    construct_thread_id(1 + (n + i) as u32)
}

fn node(me: usize, n: usize) {
    let me_i = me as i32;
    // SHARED DATABASE of the paper (one process, so no semaphore is needed)
    let mut our_sequence_number: i32 = 0;
    let mut highest_sequence_number: i32 = 0;
    let mut outstanding_reply_count: usize = 0;
    let mut requesting_critical_section = false;
    let mut reply_deferred = vec![false; n];
    // termination bookkeeping
    let mut left_cs = false;
    let mut replies_sent: usize = 0; // REPLYs sent to other nodes' REQUESTs
    let client = client_id(n, me);

    while !(left_cs && replies_sent == n - 1) {
        match recv_msg_block::<Msg>() {
            Msg::Enter { from } => {
                assert_eq!(from as usize, me);
                // Request entry: choose a sequence number
                requesting_critical_section = true;
                our_sequence_number = highest_sequence_number + 1;
                outstanding_reply_count = n - 1;
                for j in 0..n {
                    if j != me {
                        send_msg(
                            node_id(j),
                            Msg::Request { seq: our_sequence_number, from: me_i },
                        );
                    }
                }
                // WAITFOR (Outstanding_Reply_Count = 0)
                if outstanding_reply_count == 0 {
                    send_msg(client, Msg::Granted {});
                }
            }
            Msg::Reply {} => {
                // PROCESS WHICH RECEIVES REPLY MESSAGES
                outstanding_reply_count -= 1;
                if outstanding_reply_count == 0 && requesting_critical_section {
                    // the WAITFOR is satisfied: enter the critical section
                    send_msg(client, Msg::Granted {});
                }
            }
            Msg::Request { seq: k, from: j } => {
                // PROCESS WHICH RECEIVES REQUEST(k, j) MESSAGES
                highest_sequence_number = highest_sequence_number.max(k);
                let defer_it = requesting_critical_section
                    && (k > our_sequence_number || (k == our_sequence_number && j > me_i));
                if defer_it {
                    reply_deferred[j as usize] = true;
                } else {
                    send_msg(node_id(j as usize), Msg::Reply {});
                    replies_sent += 1;
                }
            }
            Msg::Exit { from } => {
                assert_eq!(from as usize, me);
                // Release the critical section
                requesting_critical_section = false;
                for j in 0..n {
                    if reply_deferred[j] {
                        reply_deferred[j] = false;
                        send_msg(node_id(j), Msg::Reply {});
                        replies_sent += 1;
                    }
                }
                send_msg(client, Msg::Released {});
                left_cs = true;
            }
            m => panic!("node n{me}: unexpected message {m:?}"),
        }
    }
}

fn client(i: usize) {
    let to = node_id(i);
    send_msg(to, Msg::Enter { from: i as i32 });
    match recv_msg_block::<Msg>() {
        Msg::Granted {} => {}
        m => panic!("client c{i}: expected Granted, got {m:?}"),
    }
    send_msg(to, Msg::Exit { from: i as i32 });
    match recv_msg_block::<Msg>() {
        Msg::Released {} => {}
        m => panic!("client c{i}: expected Released, got {m:?}"),
    }
}

fn run(n: usize) {
    let mut handles = Vec::new();
    for i in 0..n {
        let h = thread::Builder::new()
            .name(format!("n{i}"))
            .spawn(move || node(i, n))
            .unwrap();
        traceforge::assert(h.thread().id() == node_id(i));
        handles.push(h);
    }
    for i in 0..n {
        let h = thread::Builder::new()
            .name(format!("c{i}"))
            .spawn(move || client(i))
            .unwrap();
        traceforge::assert(h.thread().id() == client_id(n, i));
        handles.push(h);
    }
    for h in handles {
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
    Arc::new(move || run(n))
}

fn spec_entry(args: &[String]) -> Entry {
    let n = size(args);
    Arc::new(move || spec::spec(n))
}

fn main() -> ExitCode {
    main_with_model(
        Program { visible: &visible, spec: &spec_entry, imp: &entry },
        ConsType::Bag,
    )
}
