//! Chain replication (van Renesse & Schneider, OSDI 2004, §2-§3) as a Must-API program.
//! One object (an integer, initially 0), N servers s0 (head) -> ... -> s{N-1} (tail), K clients.
//! No failures, so no master. Usage: see `traceforge::program` (`count impl -- <N> <K>`).
use std::process::ExitCode;
use std::sync::{Arc, Mutex, OnceLock};

mod spec; // the coordinator's hand-written specification (plan/twins-contextfree/chain-replication/spec.rs); the agent's program is `imp`
use traceforge::program::{main_with_model, Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    // client <-> chain (fixed by INTERFACE.md)
    Update { from: u32, value: i32 },
    UpdateReply { value: i32 },
    Query { from: u32 },
    QueryReply { value: i32 },
    // server <-> server (ours)
    /// A state change forwarded down the chain: the update's sequence number (assigned by the
    /// head), the new value computed once by the head, and the client to which the tail replies.
    Fwd { sn: u32, value: i32, client: ThreadId },
    /// ack(r): the tail has completed update `sn`; travels up the chain.
    Ack { sn: u32 },
}

/// Every process's address, known "from the start" (INTERFACE.md): the servers' addresses are
/// written by main after it has spawned all servers and before it spawns any client; client `ci`
/// writes its own address as its first step, before its first send. A server reads an address only
/// after receiving a message, which is always causally after both writes.
#[derive(Default)]
struct Directory {
    servers: OnceLock<Vec<ThreadId>>,
    clients: Mutex<Vec<Option<ThreadId>>>,
}
type Dir = Arc<Directory>;

fn client_addr(dir: &Dir, from: u32) -> ThreadId {
    dir.clients.lock().unwrap()[from as usize].expect("client registered before sending")
}

fn server(i: usize, n: usize, k: usize, pred: Option<ThreadId>, dir: Dir) {
    let is_head = i == 0;
    let is_tail = i == n - 1;
    let succ = || -> ThreadId { dir.servers.get().expect("directory set")[i + 1] };
    // Hist^i reduced to the object's current value (each replica only performs writes).
    let mut value: i32 = 0;
    // Sent_i: updates forwarded to the successor but not yet acknowledged by the tail.
    let mut sent: Vec<u32> = Vec::new();
    let mut next_sn: u32 = 0; // head only
    // Every server receives exactly 2K messages in a failure-free run:
    // head: K Update + K Ack (N>1); middle: K Fwd + K Ack; tail: K Fwd (or K Update when N=1) + K Query.
    for _ in 0..2 * k {
        match recv_msg_block::<Msg>() {
            Msg::Update { from, value: v } => {
                assert!(is_head, "updates are directed to the head");
                // The head processes the update atomically: V := newVal (a write), assign a sn.
                next_sn += 1;
                value = v;
                let client = client_addr(&dir, from);
                if is_tail {
                    send_msg(client, Msg::UpdateReply { value });
                } else {
                    send_msg(succ(), Msg::Fwd { sn: next_sn, value, client });
                    sent.push(next_sn);
                }
            }
            Msg::Fwd { sn, value: v, client } => {
                value = v;
                if is_tail {
                    send_msg(client, Msg::UpdateReply { value });
                    send_msg(pred.expect("tail of a chain of length > 1 has a predecessor"), Msg::Ack { sn });
                } else {
                    send_msg(succ(), Msg::Fwd { sn, value, client });
                    sent.push(sn);
                }
            }
            Msg::Ack { sn } => {
                sent.retain(|&s| s != sn);
                if let Some(p) = pred {
                    send_msg(p, Msg::Ack { sn });
                }
            }
            Msg::Query { from } => {
                assert!(is_tail, "queries are directed to the tail");
                let client = client_addr(&dir, from);
                send_msg(client, Msg::QueryReply { value });
            }
            m => panic!("server s{i} got an unexpected message {m:?}"),
        }
    }
    let _ = sent;
}

fn chain(n: usize, k: usize) {
    let dir: Dir = Arc::new(Directory::default());
    *dir.clients.lock().unwrap() = vec![None; k];
    let mut servers = Vec::new();
    let mut sids: Vec<ThreadId> = Vec::new();
    for i in 0..n {
        let pred = if i == 0 { None } else { Some(sids[i - 1]) };
        let d = dir.clone();
        let h = thread::Builder::new()
            .name(format!("s{i}"))
            .spawn(move || server(i, n, k, pred, d))
            .unwrap();
        sids.push(h.thread().id());
        servers.push(h);
    }
    dir.servers.set(sids.clone()).unwrap();
    let head = sids[0];
    let tail = sids[n - 1];
    let mut clients = Vec::new();
    for i in 0..k {
        let d = dir.clone();
        let h = thread::Builder::new()
            .name(format!("c{i}"))
            .spawn(move || {
                d.clients.lock().unwrap()[i] = Some(thread::current_id());
                send_msg(head, Msg::Update { from: i as u32, value: i as i32 + 1 });
                let Msg::UpdateReply { .. } = recv_msg_block::<Msg>() else { panic!("expected UpdateReply") };
                send_msg(tail, Msg::Query { from: i as u32 });
                let Msg::QueryReply { .. } = recv_msg_block::<Msg>() else { panic!("expected QueryReply") };
            })
            .unwrap();
        clients.push(h);
    }
    for s in servers {
        s.join().unwrap();
    }
    for c in clients {
        c.join().unwrap();
    }
}

fn nk(a: &[String]) -> (usize, usize) {
    let n = a.first().and_then(|s| s.parse().ok()).unwrap_or(3);
    let k = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(2);
    assert!(n >= 1 && k >= 1, "N >= 1 and K >= 1");
    (n, k)
}

fn visible(a: &[String]) -> Vec<String> {
    let (_, k) = nk(a);
    (0..k).map(|i| format!("c{i}")).collect()
}

fn spec_entry(a: &[String]) -> Entry {
    let (n, k) = nk(a);
    Arc::new(move || spec::spec(n, k))
}

fn imp(a: &[String]) -> Entry {
    let (n, k) = nk(a);
    Arc::new(move || chain(n, k))
}

fn main() -> ExitCode {
    main_with_model(
        Program { visible: &visible, spec: &spec_entry, imp: &imp },
        ConsType::FIFO,
    )
}
