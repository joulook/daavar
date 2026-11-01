//! Benchmark kv-snapshot-isolation-tla (candidate N9): the TLA+ examples' key-value store.
//! Specification: a serializable server (one thread executing each transaction atomically, in
//! the order it receives them, or rolling it back). Implementation: KVsnap's snapshot-isolation
//! store (`store`, the PlusCal globals `store`, `tx`, `missed`) driven by one transaction process
//! per client (`t0..`, the PlusCal `process (t \in TxId)`: START, READ, UPDATE, COMMIT).
//!
//! Usage: `<bin> count|export|conf|online … --model M -- <n> [variant]`; `n` = clients = keys
//! (client `i` reads
//! key `(i+1) mod n` and writes key `i` with value `i+1`; `NoVal` = 0), default model `fifo`
//! (p2p), default variant `si`. Variants (impl side): `si` (KVsnap: first committer wins on the
//! write set) and `serializable` (the control: the commit also validates the read set).
//! `count` prints `execs=<complete> blocked=<blocked>`.
//! Declared default model, used when `--model` is left out: `fifo`.
use traceforge::thread::{self, ThreadId};
use traceforge::*;

const TAG_TXN: u32 = 1;
const TAG_RESULT: u32 = 2;
const TAG_BEGIN: u32 = 3;
const TAG_SNAP: u32 = 4;
const TAG_COMMIT: u32 = 5;
const TAG_CRES: u32 = 6;

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// client -> server / transaction process: one transaction (read `rk`, write `wk := v`).
    Txn { from: ThreadId, rk: u8, wk: u8, v: u8 },
    /// -> client: `ok` (committed) and the value read (0 when aborted).
    Result { ok: bool, read: u8 },
    /// transaction process -> store: START (open the transaction, take the snapshot).
    Begin { from: ThreadId },
    /// store -> transaction process: the snapshot (`snapshotStore := store`).
    Snap(Vec<u8>),
    /// transaction process -> store: COMMIT with the read and write sets.
    Commit { from: ThreadId, rk: u8, wk: u8, v: u8 },
    /// store -> transaction process: committed (true) or aborted (false).
    CRes(bool),
}

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn recv_tag(tag: u32) -> Msg {
    recv_tagged_msg_block::<_, Msg>(move |_, t| t == Some(tag))
}

/// Client `i`: one transaction, read key `(i+1) mod n`, write key `i` := `i+1`.
fn spawn_clients(n: usize, front: Vec<ThreadId>) -> Vec<thread::JoinHandle<()>> {
    (0..n)
        .map(|i| {
            let to = front[i];
            named(&format!("c{i}"), move || {
                let me = thread::current_id();
                let (rk, wk, v) = (((i + 1) % n) as u8, i as u8, (i + 1) as u8);
                send_tagged_msg(to, TAG_TXN, Msg::Txn { from: me, rk, wk, v });
                let _ = recv_tag(TAG_RESULT);
            })
        })
        .collect()
}

fn spec(n: usize) {
    let server = named("server", move || {
        let mut store = vec![0u8; n];
        for _ in 0..n {
            let Msg::Txn { from, rk, wk, v } = recv_tag(TAG_TXN) else { unreachable!() };
            // RollbackTx is always enabled for an open transaction
            if nondet() {
                send_tagged_msg(from, TAG_RESULT, Msg::Result { ok: false, read: 0 });
            } else {
                let r = store[rk as usize];
                store[wk as usize] = v;
                send_tagged_msg(from, TAG_RESULT, Msg::Result { ok: true, read: r });
            }
        }
    });
    let sid = server.thread().id();
    let cs = spawn_clients(n, vec![sid; n]);
    for c in cs {
        c.join().unwrap();
    }
    server.join().unwrap();
}

fn imp(n: usize, variant: &str) {
    let validate_reads = match variant {
        "si" => false,
        "serializable" => true,
        _ => panic!("unknown variant {variant}"),
    };
    let store = named("store", move || {
        let mut store = vec![0u8; n];
        // open transactions and their missed write sets
        let mut tx: Vec<ThreadId> = Vec::new();
        let mut missed: Vec<(ThreadId, Vec<bool>)> = Vec::new();
        for _ in 0..2 * n {
            match recv_tagged_msg_block::<_, Msg>(|_, t| t == Some(TAG_BEGIN) || t == Some(TAG_COMMIT)) {
                Msg::Begin { from } => {
                    tx.push(from);
                    missed.push((from, vec![false; n]));
                    send_tagged_msg(from, TAG_SNAP, Msg::Snap(store.clone()));
                }
                Msg::Commit { from, rk, wk, v } => {
                    let m = missed.iter().position(|e| e.0 == from).unwrap();
                    let conflict =
                        missed[m].1[wk as usize] || (validate_reads && missed[m].1[rk as usize]);
                    tx.retain(|t| *t != from);
                    missed.remove(m);
                    if !conflict {
                        for e in missed.iter_mut() {
                            if tx.contains(&e.0) {
                                e.1[wk as usize] = true;
                            }
                        }
                        store[wk as usize] = v;
                    }
                    send_tagged_msg(from, TAG_CRES, Msg::CRes(!conflict));
                }
                _ => unreachable!(),
            }
        }
    });
    let sid = store.thread().id();
    let ts: Vec<thread::JoinHandle<()>> = (0..n)
        .map(|i| {
            named(&format!("t{i}"), move || {
                let me = thread::current_id();
                let Msg::Txn { from, rk, wk, v } = recv_tag(TAG_TXN) else { unreachable!() };
                // START
                send_tagged_msg(sid, TAG_BEGIN, Msg::Begin { from: me });
                let Msg::Snap(mut snap) = recv_tag(TAG_SNAP) else { unreachable!() };
                // READ
                let r = snap[rk as usize];
                // UPDATE (local to the snapshot)
                snap[wk as usize] = v;
                // COMMIT
                send_tagged_msg(sid, TAG_COMMIT, Msg::Commit { from: me, rk, wk, v });
                let Msg::CRes(ok) = recv_tag(TAG_CRES) else { unreachable!() };
                send_tagged_msg(from, TAG_RESULT, Msg::Result { ok, read: if ok { r } else { 0 } });
            })
        })
        .collect();
    let front: Vec<ThreadId> = ts.iter().map(|h| h.thread().id()).collect();
    let cs = spawn_clients(n, front);
    for c in cs {
        c.join().unwrap();
    }
    for t in ts {
        t.join().unwrap();
    }
    store.join().unwrap();
}

fn parse(a: &[String]) -> (usize, String) {
    let n: usize = a.first().and_then(|s| s.parse().ok()).unwrap_or(2);
    let variant = a.get(1).cloned().unwrap_or_else(|| "si".to_string());
    (n, variant)
}

fn main() -> std::process::ExitCode {
    let program = traceforge::program::Program {
        visible: &|a| (0..parse(a).0).map(|i| format!("c{i}")).collect(),
        spec: &|a| {
            let (n, _) = parse(a);
            std::sync::Arc::new(move || spec(n))
        },
        imp: &|a| {
            let (n, variant) = parse(a);
            std::sync::Arc::new(move || imp(n, &variant))
        },
    };
    traceforge::program::main_with_model(program, traceforge::ConsType::FIFO)
}
