//! Benchmark fencing-grove (candidate N16): gokv's fencing-token front ends over a counter
//! server. Specification: one `server` holding the counter of key 0, answering each
//! `FetchAndIncrement` atomically. Implementation: the `config` server (`AcquireEpoch`, which
//! waits while the current holder is active, `Get`; the heartbeat expiry is a `nondet()` step),
//! two front ends `fe1`, `fe2` (each acquires an epoch, then serves `FetchAndIncrement` by
//! `ctr.Get(epoch)` and `ctr.Put(v+1, epoch)`), the counter server `ctr` (rejects
//! `epoch < lastEpoch` with `EStale`), and one client clerk per client (`p0`, `p1`: call the
//! current front end, on an error ask `config` for the current one and retry).
//!
//! Usage: `<spec|impl> <K> [fifo|bag|causal] [variant]`; `K` = `FetchAndIncrement(0)` calls per
//! client (`c0`, `c1`). Default model `fifo` (p2p), default variant `fenced`. Variants (impl
//! side): `fenced` (the source: a front end that sees `EStale` exits, so its callers get an
//! error) and `stale-frontend` (a front end ignores `EStale` and keeps serving with its old
//! epoch). Prints `execs=<complete> blocked=<blocked>`.
use traceforge::thread::{self, ThreadId};
use traceforge::*;

const TAG_FAI: u32 = 1;
const TAG_VAL: u32 = 2;
const TAG_CFGGET: u32 = 3;
const TAG_CURRENT: u32 = 4;
const TAG_ACQUIRE: u32 = 5;
const TAG_EPOCH: u32 = 6;
const TAG_FEFAI: u32 = 7;
const TAG_FEVAL: u32 = 8;
const TAG_FEERR: u32 = 9;
const TAG_CTRGET: u32 = 10;
const TAG_GETREP: u32 = 11;
const TAG_CTRPUT: u32 = 12;
const TAG_PUTREP: u32 = 13;

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// client -> server / clerk: `FetchAndIncrement(key)`.
    Fai { from: ThreadId, key: u8 },
    /// -> client: the value fetched.
    Val(u8),
    /// clerk -> config: `Get()`.
    CfgGet { from: ThreadId },
    /// config -> clerk: the current front end.
    Current(ThreadId),
    /// front end -> config: `AcquireEpoch(me)`.
    Acquire { from: ThreadId },
    /// config -> front end: the epoch.
    Epoch(u8),
    /// clerk -> front end: `RPC_FAI(key)`.
    FeFai { from: ThreadId, key: u8 },
    /// front end -> clerk: the value.
    FeVal(u8),
    /// front end -> clerk: the call failed (the front end has exited).
    FeErr,
    /// front end -> ctr: `Get(epoch)`.
    CtrGet { from: ThreadId, epoch: u8 },
    /// ctr -> front end: `GetReply { err, val }`.
    GetRep { stale: bool, val: u8 },
    /// front end -> ctr: `Put(v, epoch)`.
    CtrPut { from: ThreadId, v: u8, epoch: u8 },
    /// ctr -> front end: the error code.
    PutRep { stale: bool },
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

/// `c0`, `c1`: K times `FetchAndIncrement(0)`.
fn spawn_clients(k: usize, to: [ThreadId; 2]) -> Vec<thread::JoinHandle<()>> {
    (0..2usize)
        .map(|i| {
            let t = to[i];
            named(&format!("c{i}"), move || {
                let me = thread::current_id();
                for _ in 0..k {
                    send_tagged_msg(t, TAG_FAI, Msg::Fai { from: me, key: 0 });
                    let _ = recv_tag(TAG_VAL);
                }
            })
        })
        .collect()
}

fn spec(k: usize) {
    let server = named("server", move || {
        let mut v = 0u8;
        for _ in 0..2 * k {
            let Msg::Fai { from, .. } = recv_tag(TAG_FAI) else { unreachable!() };
            send_tagged_msg(from, TAG_VAL, Msg::Val(v));
            v += 1;
        }
    });
    let s = server.thread().id();
    let cs = spawn_clients(k, [s, s]);
    for c in cs {
        c.join().unwrap();
    }
    server.join().unwrap();
}

fn imp(k: usize, variant: &str) {
    let fenced = match variant {
        "fenced" => true,
        "stale-frontend" => false,
        _ => panic!("unknown variant {variant}"),
    };
    let config = named("config", move || {
        let mut epoch = 0u8;
        let mut active = false;
        let mut data: Option<ThreadId> = None;
        let mut acquirers: Vec<ThreadId> = Vec::new();
        let mut getters: Vec<ThreadId> = Vec::new();
        // a blocking server loop (A4 idiom): it ends blocked
        loop {
            let mut grant: Option<ThreadId> = None;
            match recv_tags(TAG_ACQUIRE, TAG_CFGGET) {
                Msg::Acquire { from } => {
                    if active {
                        acquirers.push(from); // AcquireEpoch waits while currHolderActive
                    } else {
                        grant = Some(from);
                    }
                }
                Msg::CfgGet { from } => match data {
                    Some(fe) => send_tagged_msg(from, TAG_CURRENT, Msg::Current(fe)),
                    None => getters.push(from), // no front end yet
                },
                _ => unreachable!(),
            }
            // HeartbeatListener: the holder's heartbeat may expire while another front end waits
            if grant.is_none() && active && !acquirers.is_empty() && nondet() {
                active = false;
                grant = Some(acquirers.remove(0));
            }
            if let Some(fe) = grant {
                active = true;
                data = Some(fe);
                epoch += 1;
                send_tagged_msg(fe, TAG_EPOCH, Msg::Epoch(epoch));
                for g in getters.drain(..) {
                    send_tagged_msg(g, TAG_CURRENT, Msg::Current(fe));
                }
            }
        }
    });
    let cfg = config.thread().id();
    let ctr = named("ctr", move || {
        let mut v = 0u8;
        let mut last = 0u8;
        loop {
            match recv_tags(TAG_CTRGET, TAG_CTRPUT) {
                Msg::CtrGet { from, epoch } => {
                    if epoch < last {
                        send_tagged_msg(from, TAG_GETREP, Msg::GetRep { stale: true, val: 0 });
                    } else {
                        last = epoch;
                        send_tagged_msg(from, TAG_GETREP, Msg::GetRep { stale: false, val: v });
                    }
                }
                Msg::CtrPut { from, v: nv, epoch } => {
                    if epoch < last {
                        send_tagged_msg(from, TAG_PUTREP, Msg::PutRep { stale: true });
                    } else {
                        last = epoch;
                        v = nv;
                        send_tagged_msg(from, TAG_PUTREP, Msg::PutRep { stale: false });
                    }
                }
                _ => unreachable!(),
            }
        }
    });
    let cid = ctr.thread().id();
    let mut fes = Vec::new();
    for name in ["fe1", "fe2"] {
        fes.push(named(name, move || {
            let me = thread::current_id();
            send_tagged_msg(cfg, TAG_ACQUIRE, Msg::Acquire { from: me });
            let Msg::Epoch(e) = recv_tag(TAG_EPOCH) else { unreachable!() };
            let mut exited = false;
            loop {
                let Msg::FeFai { from, .. } = recv_tag(TAG_FEFAI) else { unreachable!() };
                if exited {
                    send_tagged_msg(from, TAG_FEERR, Msg::FeErr);
                    continue;
                }
                send_tagged_msg(cid, TAG_CTRGET, Msg::CtrGet { from: me, epoch: e });
                let Msg::GetRep { stale, val } = recv_tag(TAG_GETREP) else { unreachable!() };
                if stale && fenced {
                    exited = true; // "ctr: get() stale epoch number"; primitive.Exit(1)
                    send_tagged_msg(from, TAG_FEERR, Msg::FeErr);
                    continue;
                }
                send_tagged_msg(cid, TAG_CTRPUT, Msg::CtrPut { from: me, v: val + 1, epoch: e });
                let Msg::PutRep { stale } = recv_tag(TAG_PUTREP) else { unreachable!() };
                if stale && fenced {
                    exited = true;
                    send_tagged_msg(from, TAG_FEERR, Msg::FeErr);
                    continue;
                }
                send_tagged_msg(from, TAG_FEVAL, Msg::FeVal(val));
            }
        }));
    }
    let mut clerks = Vec::new();
    for i in 0..2usize {
        clerks.push(named(&format!("p{i}"), move || {
            let me = thread::current_id();
            // MakeClerk: the current front end
            send_tagged_msg(cfg, TAG_CFGGET, Msg::CfgGet { from: me });
            let Msg::Current(mut fe) = recv_tag(TAG_CURRENT) else { unreachable!() };
            for _ in 0..k {
                let Msg::Fai { from, key } = recv_tag(TAG_FAI) else { unreachable!() };
                let mut done = false;
                for _attempt in 0..2 {
                    send_tagged_msg(fe, TAG_FEFAI, Msg::FeFai { from: me, key });
                    match recv_tags(TAG_FEVAL, TAG_FEERR) {
                        Msg::FeVal(v) => {
                            send_tagged_msg(from, TAG_VAL, Msg::Val(v));
                            done = true;
                            break;
                        }
                        Msg::FeErr => {
                            send_tagged_msg(cfg, TAG_CFGGET, Msg::CfgGet { from: me });
                            let Msg::Current(f) = recv_tag(TAG_CURRENT) else { unreachable!() };
                            fe = f;
                        }
                        _ => unreachable!(),
                    }
                }
                assert!(done, "the second front end failed too");
            }
        }));
    }
    let to = [clerks[0].thread().id(), clerks[1].thread().id()];
    let cs = spawn_clients(k, to);
    for c in cs {
        c.join().unwrap();
    }
    for p in clerks {
        p.join().unwrap();
    }
    for f in fes {
        f.join().unwrap();
    }
    ctr.join().unwrap();
    config.join().unwrap();
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let side = a.get(1).map(String::as_str).unwrap_or("spec").to_string();
    let k: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(1);
    let model = match a.get(3).map(String::as_str).unwrap_or("fifo") {
        "bag" => ConsType::Bag,
        "causal" => ConsType::Causal,
        _ => ConsType::FIFO,
    };
    let variant = a.get(4).cloned().unwrap_or_else(|| "fenced".to_string());
    let conf = Config::builder().with_cons_type(model).with_verbose(0).build();
    let stats = match side.as_str() {
        "impl" => verify(conf, move || imp(k, &variant)),
        _ => verify(conf, move || spec(k)),
    };
    println!("execs={} blocked={}", stats.execs, stats.block);
}
