//! Benchmark kv-exactly-once-grove (candidate N6): Grove's tutorial exactly-once KV service
//! (`gokv/tutorial/kvservice`): fresh operation ids, `lastReplies`, a clerk that retries until
//! a reply arrives; against one KV server applying each operation once.
//!
//! Usage: `<spec|impl> <keys> [fifo|bag|causal] [variant]`; 2 clients: `c0` runs
//! `ConditionalPut(k, "", "1")` and `c1` runs `Get(k)` for every key `k` in order; default
//! model `bag` (asyn), default variant `exactly-once`. Variants (impl side): `exactly-once`,
//! `no-request-id`. One drop in all: clerk `p0` may lose one reply (`if nondet()`) and retry.
//! Values: 0 is "" and 1 is "1". Prints `execs=<complete> blocked=<blocked>`.
use traceforge::thread::{self, ThreadId};
use traceforge::*;

const N: usize = 2; // clients
const DROPS: usize = 1; // lost replies in the whole run (all at p0)

#[derive(Clone, Copy, Debug, PartialEq)]
enum KvOp {
    /// `conditionalPut{key, expectedVal, newVal}`
    CondPut { key: u8, expected: u8, new: u8 },
    /// `get{key}`
    Get { key: u8 },
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum KvRes {
    /// `ConditionalPut`'s result: `reply == "ok"`
    Ok(bool),
    /// `Get`'s result
    Val(u8),
}

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// A client's operation; `from` is the client (a visible role).
    Op { from: ThreadId, op: KvOp },
    /// The answer to a client.
    Res(KvRes),
    /// `getFreshNumRpc`
    Fresh { from: ThreadId },
    /// its reply
    FreshId(u8),
    /// `putRpc`/`conditionalPutRpc`/`getRpc` with `OpId` (`None`: the no-request-id clerk)
    Rpc { from: ThreadId, op_id: Option<u8>, op: KvOp },
    /// its reply
    Reply(KvRes),
}

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn script(i: usize, key: u8) -> KvOp {
    if i == 0 {
        KvOp::CondPut { key, expected: 0, new: 1 }
    } else {
        KvOp::Get { key }
    }
}

/// The server's operation semantics (`kvs[key]`, default "").
fn execute(kvs: &mut [u8], op: KvOp) -> KvRes {
    match op {
        KvOp::CondPut { key, expected, new } => {
            if kvs[key as usize] == expected {
                kvs[key as usize] = new;
                KvRes::Ok(true)
            } else {
                KvRes::Ok(false)
            }
        }
        KvOp::Get { key } => KvRes::Val(kvs[key as usize]),
    }
}

fn spawn_clients(keys: usize, front: &[ThreadId]) -> Vec<thread::JoinHandle<()>> {
    (0..N)
        .map(|i| {
            let to = front[i];
            named(&format!("c{i}"), move || {
                let me = thread::current_id();
                for key in 0..keys as u8 {
                    send_msg(to, Msg::Op { from: me, op: script(i, key) });
                    let _: Msg = recv_msg_block();
                }
            })
        })
        .collect()
}

fn spec(keys: usize) {
    let server = named("server", move || {
        let mut kvs = vec![0u8; keys];
        for _ in 0..N * keys {
            let Msg::Op { from, op } = recv_msg_block() else { unreachable!() };
            let r = execute(&mut kvs, op);
            send_msg(from, Msg::Res(r));
        }
    });
    let sid = server.thread().id();
    let cs = spawn_clients(keys, &[sid; N]);
    for c in cs {
        c.join().unwrap();
    }
    server.join().unwrap();
}

fn imp(keys: usize, variant: &str) {
    let with_ids = match variant {
        "exactly-once" => true,
        "no-request-id" => false,
        _ => panic!("unknown variant {variant}"),
    };
    // the Server: getFreshNum, conditionalPut, get; ends blocked when the clerks are done
    let server = named("server", move || {
        let mut kvs = vec![0u8; keys];
        let mut next_fresh_id = 0u8;
        let mut last_replies: Vec<(u8, KvRes)> = Vec::new();
        loop {
            match recv_msg_block::<Msg>() {
                Msg::Fresh { from } => {
                    let n = next_fresh_id;
                    next_fresh_id += 1;
                    send_msg(from, Msg::FreshId(n));
                }
                Msg::Rpc { from, op_id, op } => {
                    let cached = op_id.and_then(|id| last_replies.iter().find(|e| e.0 == id).map(|e| e.1));
                    let r = match cached {
                        Some(r) => r,
                        None => {
                            let r = execute(&mut kvs, op);
                            if let Some(id) = op_id {
                                last_replies.push((id, r));
                            }
                            r
                        }
                    };
                    send_msg(from, Msg::Reply(r));
                }
                _ => unreachable!(),
            }
        }
    });
    let sid = server.thread().id();
    // the Clerks: one per client, invisible
    let clerks: Vec<_> = (0..N)
        .map(|i| {
            named(&format!("p{i}"), move || {
                let me = thread::current_id();
                let mut drops = if i == 0 { DROPS } else { 0 };
                for _ in 0..keys {
                    let Msg::Op { from: client, op } = recv_msg_block() else { unreachable!() };
                    let op_id = if with_ids {
                        send_msg(sid, Msg::Fresh { from: me });
                        let Msg::FreshId(n) = recv_msg_block() else { unreachable!() };
                        Some(n)
                    } else {
                        None
                    };
                    send_msg(sid, Msg::Rpc { from: me, op_id, op });
                    let Msg::Reply(mut r) = recv_msg_block() else { unreachable!() };
                    if drops > 0 && nondet() {
                        // the reply is lost (urpc error): retry the same request
                        drops -= 1;
                        send_msg(sid, Msg::Rpc { from: me, op_id, op });
                        let Msg::Reply(r2) = recv_msg_block() else { unreachable!() };
                        r = r2;
                    }
                    send_msg(client, Msg::Res(r));
                }
            })
        })
        .collect();
    let front: Vec<ThreadId> = clerks.iter().map(|p| p.thread().id()).collect();
    let cs = spawn_clients(keys, &front);
    for c in cs {
        c.join().unwrap();
    }
    for p in clerks {
        p.join().unwrap();
    }
    server.join().unwrap();
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let side = a.get(1).map(String::as_str).unwrap_or("spec").to_string();
    let n: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(1);
    let model = match a.get(3).map(String::as_str).unwrap_or("bag") {
        "fifo" => ConsType::FIFO,
        "causal" => ConsType::Causal,
        _ => ConsType::Bag,
    };
    let variant = a.get(4).cloned().unwrap_or_else(|| "exactly-once".to_string());
    let conf = Config::builder().with_cons_type(model).with_verbose(0).build();
    let stats = match side.as_str() {
        "impl" => verify(conf, move || imp(n, &variant)),
        _ => verify(conf, move || spec(n)),
    };
    println!("execs={} blocked={}", stats.execs, stats.block);
}
