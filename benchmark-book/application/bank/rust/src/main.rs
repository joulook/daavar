//! Benchmark bank-grove (candidate N10): gokv's bank over a lock service and a KV store.
//! Specification: one `bank` thread holding the accounts, serving each `Transfer` atomically and
//! `Audit` with the sum of the balances. Implementation: one bank clerk per visible client
//! (`clerk0`, `clerk1`, `aclerk` for the auditor) running `transfer_internal` / `get_total`
//! over a lock service (`lock`, the KV behind `LockClerk`: `Lock` = `ConditionalPut(key, "", "1")`
//! retried until "ok", `Unlock` = `Put(key, "")`) and an account KV (`kv`: `Get`, `Put`).
//!
//! Usage: `<bin> count|export|conf|online … --model M -- <K> [variant]`; `K` = transfers per
//! client; `c0` moves 1
//! from account 0 to account 1, `c1` from 1 to 0; `auditor` audits once. `BAL_TOTAL` = 1, all of
//! it in account 0 initially. Default model `fifo` (p2p), default variant `ordered`. Variants
//! (impl side): `ordered` (`acquire_two_good`) and `unordered` (`acquire_two`, the source's).
//! `count` prints `execs=<complete> blocked=<blocked>`.
//! Declared default model, used when `--model` is left out: `fifo`.
use traceforge::thread::{self, ThreadId};
use traceforge::*;

const ACCTS: usize = 2;
const BAL_TOTAL: u8 = 1;
const AMOUNT: u8 = 1;

const TAG_TRANSFER: u32 = 1;
const TAG_DONE: u32 = 2;
const TAG_AUDIT: u32 = 3;
const TAG_TOTAL: u32 = 4;
const TAG_CONDPUT: u32 = 5;
const TAG_CONDOK: u32 = 6;
const TAG_UNLOCK: u32 = 7;
const TAG_GET: u32 = 8;
const TAG_VAL: u32 = 9;
const TAG_PUT: u32 = 10;
const TAG_PUTOK: u32 = 11;

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// client -> bank / clerk: `transfer_internal(src, dst, amt)`.
    Transfer { from: ThreadId, src: u8, dst: u8, amt: u8 },
    /// -> client: the transfer returned.
    Done,
    /// auditor -> bank / auditor's clerk: `get_total()`.
    Audit { from: ThreadId },
    /// -> auditor: the total.
    Total(u8),
    /// clerk -> lock: `ConditionalPut(key, "", "1")`, answered once it succeeds.
    CondPut { from: ThreadId, key: u8 },
    /// lock -> clerk: "ok".
    CondOk,
    /// clerk -> lock: `Put(key, "")`.
    Unlock { from: ThreadId, key: u8 },
    /// clerk -> kv: `Get(key)`.
    Get { from: ThreadId, key: u8 },
    /// kv -> clerk: the value.
    Val(u8),
    /// clerk -> kv: `Put(key, val)`.
    Put { from: ThreadId, key: u8, val: u8 },
    /// kv / lock -> clerk: the `Put` returned.
    PutOk,
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

/// The visible clients: `c0`, `c1` (K transfers each) and `auditor` (one audit).
fn spawn_clients(k: usize, to: [ThreadId; 3]) -> Vec<thread::JoinHandle<()>> {
    let mut hs = Vec::new();
    for i in 0..2usize {
        let t = to[i];
        hs.push(named(&format!("c{i}"), move || {
            let me = thread::current_id();
            let (src, dst) = (i as u8, (1 - i) as u8);
            for _ in 0..k {
                send_tagged_msg(t, TAG_TRANSFER, Msg::Transfer { from: me, src, dst, amt: AMOUNT });
                let _ = recv_tag(TAG_DONE);
            }
        }));
    }
    let t = to[2];
    hs.push(named("auditor", move || {
        let me = thread::current_id();
        send_tagged_msg(t, TAG_AUDIT, Msg::Audit { from: me });
        let _ = recv_tag(TAG_TOTAL);
    }));
    hs
}

fn spec(k: usize) {
    let bank = named("bank", move || {
        let mut bal = [BAL_TOTAL, 0];
        for _ in 0..2 * k + 1 {
            match recv_tags(TAG_TRANSFER, TAG_AUDIT) {
                Msg::Transfer { from, src, dst, amt } => {
                    if bal[src as usize] >= amt {
                        bal[src as usize] -= amt;
                        bal[dst as usize] += amt;
                    }
                    send_tagged_msg(from, TAG_DONE, Msg::Done);
                }
                Msg::Audit { from } => {
                    send_tagged_msg(from, TAG_TOTAL, Msg::Total(bal.iter().sum()));
                }
                _ => unreachable!(),
            }
        }
    });
    let b = bank.thread().id();
    let cs = spawn_clients(k, [b, b, b]);
    for c in cs {
        c.join().unwrap();
    }
    bank.join().unwrap();
}

/// The lock clerk's RPCs.
fn lock(l: ThreadId, me: ThreadId, key: u8) {
    send_tagged_msg(l, TAG_CONDPUT, Msg::CondPut { from: me, key });
    let _ = recv_tag(TAG_CONDOK);
}
fn unlock(l: ThreadId, me: ThreadId, key: u8) {
    send_tagged_msg(l, TAG_UNLOCK, Msg::Unlock { from: me, key });
    let _ = recv_tag(TAG_PUTOK);
}
/// The KV clerk's RPCs.
fn get(kv: ThreadId, me: ThreadId, key: u8) -> u8 {
    send_tagged_msg(kv, TAG_GET, Msg::Get { from: me, key });
    let Msg::Val(v) = recv_tag(TAG_VAL) else { unreachable!() };
    v
}
fn put(kv: ThreadId, me: ThreadId, key: u8, val: u8) {
    send_tagged_msg(kv, TAG_PUT, Msg::Put { from: me, key, val });
    let _ = recv_tag(TAG_PUTOK);
}

fn imp(k: usize, variant: &str) {
    let ordered = match variant {
        "ordered" => true,
        "unordered" => false,
        _ => panic!("unknown variant {variant}"),
    };
    // Lock and unlock requests: 4 per transfer (2 locks, 2 unlocks) and 4 for the audit.
    let lock_msgs = 4 * (2 * k) + 4;
    let lk = named("lock", move || {
        let mut held = [false; ACCTS];
        let mut waiting: Vec<Vec<ThreadId>> = vec![Vec::new(); ACCTS];
        for _ in 0..lock_msgs {
            match recv_tags(TAG_CONDPUT, TAG_UNLOCK) {
                Msg::CondPut { from, key } => {
                    let key = key as usize;
                    if !held[key] {
                        held[key] = true;
                        send_tagged_msg(from, TAG_CONDOK, Msg::CondOk);
                    } else {
                        // the clerk keeps retrying; it succeeds once the key is free
                        waiting[key].push(from);
                    }
                }
                Msg::Unlock { from, key } => {
                    let key = key as usize;
                    send_tagged_msg(from, TAG_PUTOK, Msg::PutOk);
                    let w = waiting[key].len();
                    if w == 0 {
                        held[key] = false;
                    } else {
                        // any retrying clerk may win the next ConditionalPut
                        let j = if w == 1 { 0 } else { (0..w).nondet() };
                        let next = waiting[key].remove(j);
                        send_tagged_msg(next, TAG_CONDOK, Msg::CondOk);
                    }
                }
                _ => unreachable!(),
            }
        }
    });
    let kvh = named("kv", move || {
        let mut store = [BAL_TOTAL, 0];
        // a blocking server loop (A4 idiom): it ends blocked once the clerks are done
        loop {
            match recv_tagged_msg_block::<_, Msg>(|_, t| {
                t == Some(TAG_GET) || t == Some(TAG_PUT)
            }) {
                Msg::Get { from, key } => send_tagged_msg(from, TAG_VAL, Msg::Val(store[key as usize])),
                Msg::Put { from, key, val } => {
                    store[key as usize] = val;
                    send_tagged_msg(from, TAG_PUTOK, Msg::PutOk);
                }
                _ => unreachable!(),
            }
        }
    });
    let (l, kv) = (lk.thread().id(), kvh.thread().id());
    let mut clerks = Vec::new();
    for i in 0..2usize {
        clerks.push(named(&format!("clerk{i}"), move || {
            let me = thread::current_id();
            for _ in 0..k {
                let Msg::Transfer { from, src, dst, amt } = recv_tag(TAG_TRANSFER) else { unreachable!() };
                // acquire_two / acquire_two_good
                if ordered && dst < src {
                    lock(l, me, dst);
                    lock(l, me, src);
                } else {
                    lock(l, me, src);
                    lock(l, me, dst);
                }
                let old = get(kv, me, src);
                if old >= amt {
                    put(kv, me, src, old - amt);
                    let t = get(kv, me, dst);
                    put(kv, me, dst, t + amt);
                }
                // release_two
                unlock(l, me, src);
                unlock(l, me, dst);
                send_tagged_msg(from, TAG_DONE, Msg::Done);
            }
        }));
    }
    clerks.push(named("aclerk", move || {
        let me = thread::current_id();
        let Msg::Audit { from } = recv_tag(TAG_AUDIT) else { unreachable!() };
        // get_total: the accounts in sorted order
        let mut sum = 0;
        for a in 0..ACCTS as u8 {
            lock(l, me, a);
            sum += get(kv, me, a);
        }
        for a in 0..ACCTS as u8 {
            unlock(l, me, a);
        }
        send_tagged_msg(from, TAG_TOTAL, Msg::Total(sum));
    }));
    let to = [clerks[0].thread().id(), clerks[1].thread().id(), clerks[2].thread().id()];
    let cs = spawn_clients(k, to);
    for c in cs {
        c.join().unwrap();
    }
    for c in clerks {
        c.join().unwrap();
    }
    lk.join().unwrap();
    kvh.join().unwrap();
}

fn parse(a: &[String]) -> (usize, String) {
    let k: usize = a.first().and_then(|s| s.parse().ok()).unwrap_or(1);
    let variant = a.get(1).cloned().unwrap_or_else(|| "ordered".to_string());
    (k, variant)
}

fn main() -> std::process::ExitCode {
    let program = traceforge::program::Program {
        visible: &|_| vec!["c0".to_string(), "c1".to_string(), "auditor".to_string()],
        spec: &|a| {
            let (k, _) = parse(a);
            std::sync::Arc::new(move || spec(k))
        },
        imp: &|a| {
            let (k, variant) = parse(a);
            std::sync::Arc::new(move || imp(k, &variant))
        },
    };
    traceforge::program::main_with_model(program, traceforge::ConsType::FIFO)
}
