//! Benchmark replicated-log-aneris (candidate N11): Aneris's leader-followers replicated
//! database (`repdb_code.ml`) driven by the causality example (`causality_example_code.ml`).
//! Specification (the Iris spec's structure: the leader's log is the history, a follower's state
//! is a growing prefix of it): a `leader` server that appends, acknowledges and pushes each write
//! to a `follower` server, which serves reads from the longest prefix it has received.
//! Implementation: `leader` (client writes, `follower_request_handler` with `log_wait_until`)
//! and one `follower` (`sync_loop` + `client_request_handler_at_follower`).
//!
//! Usage: `<bin> count|export|conf|online … --model M -- <W> [variant]`; `W` writes by `writer`
//! at the leader
//! (key `i` := `VAL(i)`, so `x = 37`, `y = 1` at W = 2), then `W` reads by `reader` at the
//! follower in reverse key order (`y` first, then `x`). Default model `fifo` (p2p), default
//! variant `correct`. Variants (impl side): `correct` (the source's `sync_loop`: request entry
//! `i`, wait for it, `assert (i = j)`, apply, ask for `i+1`) and `asyn-fault` (a pipelined sync:
//! the follower asks for every entry at once and applies the entries in arrival order).
//! `count` prints `execs=<complete> blocked=<blocked>`.
//! Declared default model, used when `--model` is left out: `fifo`.
use traceforge::thread::{self, ThreadId};
use traceforge::*;

const TAG_WRITE: u32 = 1;
const TAG_ACK: u32 = 2;
const TAG_READ: u32 = 3;
const TAG_VAL: u32 = 4;
const TAG_SYNC: u32 = 5;
const TAG_ENTRY: u32 = 6;

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// client -> leader: `InjL (k, v)`.
    Write { from: ThreadId, k: u8, v: u8 },
    /// leader -> client: `InjL ()`.
    Ack,
    /// client -> follower: the key.
    Read { from: ThreadId, k: u8 },
    /// follower -> client: `val option` (0 = `None`).
    Val(u8),
    /// follower -> leader: the requested log index.
    Sync { from: ThreadId, i: u8 },
    /// leader -> follower: `((k, v), j)`.
    Entry { k: u8, v: u8, j: u8 },
}

/// The value written to key `i`: `x = 37`, `y = 1`, then `i`.
fn val(i: usize) -> u8 {
    match i {
        0 => 37,
        1 => 1,
        _ => i as u8,
    }
}

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn recv_tag(tag: u32) -> Msg {
    recv_tagged_msg_block::<_, Msg>(move |_, t| t == Some(tag))
}

fn recv_tags(a: u32, b: u32) -> Msg {
    recv_tagged_msg_block::<_, Msg>(move |_, t| t == Some(a) || t == Some(b))
}

/// `writer` (node0, `do_writes`) and `reader` (node1, `do_reads` with the wait loop bounded to
/// one read per key).
fn spawn_clients(w: usize, wto: ThreadId, rto: ThreadId) -> Vec<thread::JoinHandle<()>> {
    let wr = named("writer", move || {
        let me = thread::current_id();
        for i in 0..w {
            send_tagged_msg(wto, TAG_WRITE, Msg::Write { from: me, k: i as u8, v: val(i) });
            let _ = recv_tag(TAG_ACK);
        }
    });
    let rd = named("reader", move || {
        let me = thread::current_id();
        for i in (0..w).rev() {
            send_tagged_msg(rto, TAG_READ, Msg::Read { from: me, k: i as u8 });
            let _ = recv_tag(TAG_VAL);
        }
    });
    vec![wr, rd]
}

fn spec(w: usize) {
    // The leader appends each write to the log, acknowledges it, and pushes the entry to the
    // follower; when the follower receives it is nondeterministic (its blocking receive takes a
    // read or an entry, whichever Must offers).
    let follower = named("follower", move || {
        // the follower's state: the longest prefix of the log it has received contiguously
        let mut db = vec![0u8; w];
        let mut next = 0u8;
        let mut early: Vec<(u8, u8, u8)> = Vec::new(); // entries received ahead of `next`
        for _ in 0..2 * w {
            match recv_tags(TAG_ENTRY, TAG_READ) {
                Msg::Entry { k, v, j } => {
                    early.push((k, v, j));
                    while let Some(p) = early.iter().position(|e| e.2 == next) {
                        let (k, v, _) = early.remove(p);
                        db[k as usize] = v;
                        next += 1;
                    }
                }
                Msg::Read { from, k } => send_tagged_msg(from, TAG_VAL, Msg::Val(db[k as usize])),
                _ => unreachable!(),
            }
        }
    });
    let fid = follower.thread().id();
    let leader = named("leader", move || {
        for j in 0..w {
            let Msg::Write { from, k, v } = recv_tag(TAG_WRITE) else { unreachable!() };
            send_tagged_msg(from, TAG_ACK, Msg::Ack);
            send_tagged_msg(fid, TAG_ENTRY, Msg::Entry { k, v, j: j as u8 });
        }
    });
    let lid = leader.thread().id();
    let cs = spawn_clients(w, lid, fid);
    for c in cs {
        c.join().unwrap();
    }
    leader.join().unwrap();
    follower.join().unwrap();
}

fn imp(w: usize, variant: &str) {
    let pipelined = match variant {
        "correct" => false,
        "asyn-fault" => true,
        _ => panic!("unknown variant {variant}"),
    };
    let leader = named("leader", move || {
        let mut log: Vec<(u8, u8)> = Vec::new();
        let mut db = vec![0u8; w];
        // follower requests waiting in log_wait_until
        let mut waiting: Vec<(ThreadId, u8)> = Vec::new();
        for _ in 0..2 * w {
            match recv_tags(TAG_WRITE, TAG_SYNC) {
                Msg::Write { from, k, v } => {
                    db[k as usize] = v;
                    log.push((k, v));
                    send_tagged_msg(from, TAG_ACK, Msg::Ack);
                    // monitor_signal: answer the waiting requests the new entry satisfies
                    waiting.sort_by_key(|e| e.1);
                    while let Some(&(f, i)) = waiting.first() {
                        if (i as usize) < log.len() {
                            let (k, v) = log[i as usize];
                            send_tagged_msg(f, TAG_ENTRY, Msg::Entry { k, v, j: i });
                            waiting.remove(0);
                        } else {
                            break;
                        }
                    }
                }
                Msg::Sync { from, i } => {
                    if (i as usize) < log.len() {
                        let (k, v) = log[i as usize];
                        send_tagged_msg(from, TAG_ENTRY, Msg::Entry { k, v, j: i });
                    } else {
                        waiting.push((from, i));
                    }
                }
                _ => unreachable!(),
            }
        }
    });
    let lid = leader.thread().id();
    let follower = named("follower", move || {
        let me = thread::current_id();
        let mut db = vec![0u8; w];
        let mut next = 0u8; // sync_loop's i
        if pipelined {
            for i in 0..w {
                send_tagged_msg(lid, TAG_SYNC, Msg::Sync { from: me, i: i as u8 });
            }
        } else {
            send_tagged_msg(lid, TAG_SYNC, Msg::Sync { from: me, i: 0 });
        }
        for _ in 0..2 * w {
            match recv_tags(TAG_ENTRY, TAG_READ) {
                Msg::Entry { k, v, j } => {
                    if !pipelined {
                        assert!(next == j);
                    }
                    db[k as usize] = v;
                    next += 1;
                    if !pipelined && (next as usize) < w {
                        send_tagged_msg(lid, TAG_SYNC, Msg::Sync { from: me, i: next });
                    }
                }
                Msg::Read { from, k } => {
                    send_tagged_msg(from, TAG_VAL, Msg::Val(db[k as usize]));
                }
                _ => unreachable!(),
            }
        }
    });
    let fid = follower.thread().id();
    let cs = spawn_clients(w, lid, fid);
    for c in cs {
        c.join().unwrap();
    }
    leader.join().unwrap();
    follower.join().unwrap();
}

fn parse(a: &[String]) -> (usize, String) {
    let w: usize = a.first().and_then(|s| s.parse().ok()).unwrap_or(2);
    let variant = a.get(1).cloned().unwrap_or_else(|| "correct".to_string());
    (w, variant)
}

fn main() -> std::process::ExitCode {
    let program = traceforge::program::Program {
        visible: &|_| vec!["writer".to_string(), "reader".to_string()],
        spec: &|a| {
            let (w, _) = parse(a);
            std::sync::Arc::new(move || spec(w))
        },
        imp: &|a| {
            let (w, variant) = parse(a);
            std::sync::Arc::new(move || imp(w, &variant))
        },
    };
    traceforge::program::main_with_model(program, traceforge::ConsType::FIFO)
}
