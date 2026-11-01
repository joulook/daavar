//! Benchmark rpc-at-most-once-aneris (candidate N5): the Aneris dscm one-server memory with
//! request ids, retransmission and duplicate suppression (`ctable`), against a sequentially
//! consistent memory server.
//!
//! Usage: `<bin> count|export|conf|online … --model M -- <K> [variant]`; `K` = operations per
//! client (2
//! clients; client `i`'s operation `j` is `Wr(x, i+1)` for even `j`, `Rd(x)` for odd `j`),
//! model `--model bag` (asyn; the default this program declares), default variant `dedup`.
//! Variants (impl side): `dedup`,
//! `no-dedup`. One duplicate in all: proxy `p0` may resend one request once (`if nondet()`).
//! `count` prints `execs=<complete> blocked=<blocked>`.
use traceforge::thread::{self, ThreadId};
use traceforge::*;

const N: usize = 2; // clients
const DUPS: usize = 1; // retransmissions in the whole run (all at p0)
const TAG_OP: u32 = 1; // client -> server (spec) / proxy (impl)
const TAG_RES: u32 = 2; // server (spec) / proxy (impl) -> client
const TAG_REQ: u32 = 3; // proxy -> server
const TAG_REP: u32 = 4; // server -> proxy

#[derive(Clone, Copy, Debug, PartialEq)]
enum Op {
    /// `InjL (k, v)`
    Wr { key: u8, val: u8 },
    /// `InjR k`
    Rd { key: u8 },
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Res {
    /// `InjL ()`
    Ack,
    /// `InjR (map_lookup k db)`; 0 is `None` (written values are 1 and 2)
    Val(u8),
}

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// A client's operation; `from` is the client (a visible role).
    Op { from: ThreadId, op: Op },
    /// The answer to a client.
    Res(Res),
    /// `((op, rid), caddr)`: the proxy's request.
    Req { op: Op, rid: u8, from: ThreadId },
    /// `((res, rid), caddr)`: the server's reply.
    Rep { res: Res, rid: u8 },
}

fn named<F: FnOnce() + Send + 'static>(n: &str, f: F) -> thread::JoinHandle<()> {
    thread::Builder::new().name(n.to_string()).spawn(f).unwrap()
}

fn recv_tag(tag: u32) -> Msg {
    recv_tagged_msg_block::<_, Msg>(move |_, t| t == Some(tag))
}

fn script(i: usize, j: usize) -> Op {
    if j % 2 == 0 {
        Op::Wr { key: 0, val: (i + 1) as u8 }
    } else {
        Op::Rd { key: 0 }
    }
}

/// `commit_op` on a one-key-per-index map.
fn apply(db: &mut [u8], op: Op) -> Res {
    match op {
        Op::Wr { key, val } => {
            db[key as usize] = val;
            Res::Ack
        }
        Op::Rd { key } => Res::Val(db[key as usize]),
    }
}

fn spawn_clients(k: usize, front: &[ThreadId]) -> Vec<thread::JoinHandle<()>> {
    (0..N)
        .map(|i| {
            let to = front[i];
            named(&format!("c{i}"), move || {
                let me = thread::current_id();
                for j in 0..k {
                    send_tagged_msg(to, TAG_OP, Msg::Op { from: me, op: script(i, j) });
                    let _ = recv_tag(TAG_RES);
                }
            })
        })
        .collect()
}

fn spec(k: usize) {
    let server = named("server", move || {
        let mut db = vec![0u8; 1];
        for _ in 0..N * k {
            let Msg::Op { from, op } = recv_tag(TAG_OP) else { unreachable!() };
            let r = apply(&mut db, op);
            send_tagged_msg(from, TAG_RES, Msg::Res(r));
        }
    });
    let sid = server.thread().id();
    let cs = spawn_clients(k, &[sid; N]);
    for c in cs {
        c.join().unwrap();
    }
    server.join().unwrap();
}

fn imp(k: usize, variant: &str) {
    let dedup = match variant {
        "dedup" => true,
        "no-dedup" => false,
        _ => panic!("unknown variant {variant}"),
    };
    // the server: request_loop over process_request; it ends blocked when the clients are done
    let server = named("server", move || {
        let mut db = vec![0u8; 1];
        // ctable: caddr -> (rid, result); results are always present (one atomic step)
        let mut ctable: Vec<(ThreadId, u8, Res)> = Vec::new();
        loop {
            let Msg::Req { op, rid, from } = recv_tag(TAG_REQ) else { unreachable!() };
            if dedup {
                if let Some(e) = ctable.iter().position(|e| e.0 == from) {
                    let (_, t, res) = ctable[e];
                    if rid < t {
                        continue; // ignore stale requests
                    }
                    if rid == t {
                        send_tagged_msg(from, TAG_REP, Msg::Rep { res, rid });
                        continue; // resend the cached reply
                    }
                    assert!(rid == t + 1);
                    ctable.remove(e);
                }
                let res = apply(&mut db, op);
                ctable.push((from, rid, res));
                send_tagged_msg(from, TAG_REP, Msg::Rep { res, rid });
            } else {
                let res = apply(&mut db, op);
                send_tagged_msg(from, TAG_REP, Msg::Rep { res, rid });
            }
        }
    });
    let sid = server.thread().id();
    // the client proxies (install_proxy / request / wait_for_reply)
    let proxies: Vec<_> = (0..N)
        .map(|i| {
            named(&format!("p{i}"), move || {
                let me = thread::current_id();
                let mut dups = if i == 0 { DUPS } else { 0 };
                let mut sent_dups = 0usize;
                for rid in 0..k as u8 {
                    let Msg::Op { from: client, op } = recv_tag(TAG_OP) else { unreachable!() };
                    send_tagged_msg(sid, TAG_REQ, Msg::Req { op, rid, from: me });
                    if dups > 0 && nondet() {
                        // receive timeout: retransmit the same request
                        dups -= 1;
                        sent_dups += 1;
                        send_tagged_msg(sid, TAG_REQ, Msg::Req { op, rid, from: me });
                    }
                    // accept only resId = rid; at most `sent_dups` stale replies precede it
                    let mut res = None;
                    for _ in 0..=sent_dups {
                        let Msg::Rep { res: r, rid: id } = recv_tag(TAG_REP) else { unreachable!() };
                        assert!(id <= rid);
                        if id == rid {
                            res = Some(r);
                            break;
                        }
                    }
                    send_tagged_msg(client, TAG_RES, Msg::Res(res.unwrap()));
                }
            })
        })
        .collect();
    let front: Vec<ThreadId> = proxies.iter().map(|p| p.thread().id()).collect();
    let cs = spawn_clients(k, &front);
    for c in cs {
        c.join().unwrap();
    }
    for p in proxies {
        p.join().unwrap();
    }
    server.join().unwrap();
}

fn parse(a: &[String]) -> (usize, String) {
    let k: usize = a.first().and_then(|s| s.parse().ok()).unwrap_or(2);
    let variant = a.get(1).cloned().unwrap_or_else(|| "dedup".to_string());
    (k, variant)
}

fn main() -> std::process::ExitCode {
    let program = traceforge::program::Program {
        visible: &|_| vec!["c0".to_string(), "c1".to_string()],
        spec: &|a| {
            let (k, _) = parse(a);
            std::sync::Arc::new(move || spec(k))
        },
        imp: &|a| {
            let (k, variant) = parse(a);
            std::sync::Arc::new(move || imp(k, &variant))
        },
    };
    traceforge::program::main_with_model(program, traceforge::ConsType::Bag)
}
