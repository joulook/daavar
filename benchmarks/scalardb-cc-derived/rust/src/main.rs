//! ScalarDB Consensus Commit, derived specification (from docs/consensus-commit.mdx and
//! docs/design.mdx only). One specification object `spec` serves the clients `c1`, `c2`
//! (`c3` for mixedro) through invisible per-client ports `h1`, `h2` (`h3`); the ports only
//! relay (TraceForge's receive does not name the sender) and pace the commit: a commit is
//! a sequence of atomic steps of the object (one per prepare, one per validation, one for
//! the commit-state decision), so other clients' requests interleave with them.
//! Abstract state (visible consequences only, see NOTES.md): the committed value of each
//! key (values name their writer, so it is also the writer's id), the transaction holding a
//! pending (prepared, not yet committed or rolled back) write on each key, each
//! transaction's decision, and the pending lazy-recovery roll-back jobs.
//! Usage: `count spec|impl [--model fifo|bag] -- <pattern> <k> <iso> [<fifo|bag>]`.
use std::process::ExitCode;
use std::sync::Arc;
use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

#[derive(Clone, Debug, PartialEq)] enum Req { Begin, Read(u8), Write(u8, u8), Commit }
#[derive(Clone, Debug, PartialEq)] enum Rep { Begun, Value(u8, u8), Written(u8), Committed, Aborted, Unknown }

/// Port -> object: a client request of transaction `i`, or "advance my commit by one step".
#[derive(Clone, Debug, PartialEq)]
enum ToObj { Op(u8, Req), Step(u8) }
/// Object -> port: the reply to forward, or "send the next step".
#[derive(Clone, Debug, PartialEq)]
enum FromObj { Reply(Rep), More }
/// init -> object (the ports' ids) and init -> port (its client's id).
#[derive(Clone, Debug, PartialEq)]
enum Setup { Ports(Vec<ThreadId>), Client(ThreadId) }
const SETUP: u32 = 99;

fn client(id: u8, keys: u8, writes: &[u8], h: ThreadId) {
    send_msg(h, Req::Begin); let Rep::Begun = recv_msg_block::<Rep>() else { panic!("Begun expected") };
    for k in 1..=keys {
        send_msg(h, Req::Read(k));
        match recv_msg_block::<Rep>() { Rep::Value(_, _) => {}, Rep::Aborted => return, _ => panic!("Value or Aborted expected") }
    }
    for &k in writes { send_msg(h, Req::Write(k, id)); let Rep::Written(_) = recv_msg_block::<Rep>() else { panic!("Written expected") }; }
    send_msg(h, Req::Commit);
    match recv_msg_block::<Rep>() { Rep::Committed | Rep::Aborted | Rep::Unknown => {}, _ => panic!("Committed, Aborted or Unknown expected") }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Phase { Active, Prepare, Validate, CommitPoint, Done }
#[derive(Clone, Copy, Debug, PartialEq)]
enum Decision { Undecided, Aborted, Committed }

struct Tx {
    phase: Phase,
    dec: Decision,
    read: Vec<Option<u8>>, // value read of key k (index k-1); it names its writer
    writes: Vec<u8>,
    pend: Vec<u8>,         // keys still to prepare / validate in the current phase
    failed: bool,          // some prepare found a conflicting write
}

struct Obj {
    ser: bool,
    cur: Vec<u8>,           // committed value of each key
    held: Vec<Option<u8>>,  // the transaction whose prepared write is pending on each key
    jobs: Vec<(u8, u8, u8)>, // pending roll-back jobs (key, aborted writer, owner)
    tx: Vec<Tx>,
    done: usize,
}

/// A free choice among the elements of `v` (parallel phases: any order).
fn pick(v: &mut Vec<u8>) -> u8 {
    for idx in 0..v.len().saturating_sub(1) {
        if nondet() { return v.remove(idx); }
    }
    v.pop().unwrap()
}

impl Obj {
    fn t(&mut self, i: u8) -> &mut Tx { &mut self.tx[(i - 1) as usize] }
    fn dec(&self, w: u8) -> Decision { self.tx[(w - 1) as usize].dec }

    /// Roll back key `k`'s pending write of the aborted `w`, if it is still there.
    fn run_job(&mut self, k: u8, w: u8) {
        let ki = (k - 1) as usize;
        if self.held[ki] == Some(w) { self.held[ki] = None; }
        self.jobs.retain(|&(jk, jw, _)| !(jk == k && jw == w));
    }
    /// A background roll-back job for key `k` may have run by now (free choice).
    fn lazy_job(&mut self, k: u8) {
        let ki = (k - 1) as usize;
        if let Some(w) = self.held[ki] {
            if self.dec(w) == Decision::Aborted
                && self.jobs.iter().any(|&(jk, jw, _)| jk == k && jw == w)
                && nondet()
            {
                self.run_job(k, w);
            }
        }
    }
    fn finish(&mut self, i: u8) -> Rep { self.t(i).phase = Phase::Done; self.done += 1; Rep::Aborted }
    /// Abort `i`: decision ABORTED, its own pending writes rolled back, reply Aborted.
    fn abort(&mut self, i: u8) -> Rep {
        self.t(i).dec = Decision::Aborted;
        for h in self.held.iter_mut() { if *h == Some(i) { *h = None; } }
        self.jobs.retain(|&(_, jw, _)| jw != i);
        self.finish(i)
    }
    fn commit(&mut self, i: u8) -> Rep {
        if self.dec(i) == Decision::Aborted { return self.abort(i); }
        if !self.tx[(i - 1) as usize].writes.is_empty() {
            self.t(i).dec = Decision::Committed;
            for ki in 0..self.cur.len() {
                if self.held[ki] == Some(i) { self.held[ki] = None; self.cur[ki] = i; }
            }
        }
        self.t(i).phase = Phase::Done; self.done += 1;
        Rep::Committed
    }
    fn prepare(&mut self, i: u8, k: u8) {
        self.lazy_job(k);
        let ki = (k - 1) as usize;
        let r = self.tx[(i - 1) as usize].read[ki];
        match self.held[ki] {
            Some(_) => self.t(i).failed = true,
            None if Some(self.cur[ki]) == r => self.held[ki] = Some(i),
            None => self.t(i).failed = true,
        }
    }
    /// One validation step (SERIALIZABLE); `Some(reply)` when the transaction ends.
    fn validate(&mut self, i: u8) -> Option<FromObj> {
        let k = pick(&mut self.t(i).pend);
        self.lazy_job(k);
        let ki = (k - 1) as usize;
        let now = self.held[ki].unwrap_or(self.cur[ki]);
        if Some(now) != self.tx[(i - 1) as usize].read[ki] {
            return Some(FromObj::Reply(self.abort(i)));
        }
        if self.tx[(i - 1) as usize].pend.is_empty() { self.t(i).phase = Phase::CommitPoint; }
        Some(FromObj::More)
    }
    fn read_only_keys(&self, i: u8) -> Vec<u8> {
        let t = &self.tx[(i - 1) as usize];
        (1..=self.cur.len() as u8).filter(|k| t.read[(*k - 1) as usize].is_some() && !t.writes.contains(k)).collect()
    }

    fn op(&mut self, i: u8, req: Req) -> FromObj {
        match req {
            Req::Begin => FromObj::Reply(Rep::Begun),
            Req::Read(k) => {
                let ki = (k - 1) as usize;
                let v = match self.held[ki] {
                    Some(w) if self.dec(w) == Decision::Aborted => { self.jobs.push((k, w, i)); self.cur[ki] }
                    Some(w) => {
                        if nondet() {
                            // w expired: the reader aborts it and returns the before image
                            self.t(w).dec = Decision::Aborted;
                            self.jobs.push((k, w, i));
                            self.cur[ki]
                        } else {
                            return FromObj::Reply(self.finish(i));
                        }
                    }
                    None => self.cur[ki],
                };
                self.t(i).read[ki] = Some(v);
                FromObj::Reply(Rep::Value(k, v))
            }
            Req::Write(k, _v) => { self.t(i).writes.push(k); FromObj::Reply(Rep::Written(k)) }
            Req::Commit => {
                // wait for own recovery jobs: written keys (SNAPSHOT), all read keys (SERIALIZABLE)
                let ser = self.ser;
                let mine: Vec<(u8, u8)> = self.jobs.iter()
                    .filter(|&&(k, _, o)| o == i && (ser || self.tx[(i - 1) as usize].writes.contains(&k)))
                    .map(|&(k, w, _)| (k, w)).collect();
                for (k, w) in mine { self.run_job(k, w); }
                let writes = self.tx[(i - 1) as usize].writes.clone();
                if writes.is_empty() {
                    let ro = self.read_only_keys(i);
                    if !ser || ro.is_empty() { return FromObj::Reply(self.commit(i)); }
                    let t = self.t(i); t.phase = Phase::Validate; t.pend = ro;
                } else {
                    let t = self.t(i); t.phase = Phase::Prepare; t.pend = writes;
                }
                FromObj::More
            }
        }
    }

    fn step(&mut self, i: u8) -> FromObj {
        match self.tx[(i - 1) as usize].phase {
            Phase::Prepare => {
                if !self.tx[(i - 1) as usize].pend.is_empty() {
                    let k = pick(&mut self.t(i).pend);
                    self.prepare(i, k);
                    return FromObj::More;
                }
                // all prepare replies collected
                if self.tx[(i - 1) as usize].failed { return FromObj::Reply(self.abort(i)); }
                if !self.ser { return FromObj::Reply(self.commit(i)); }
                let ro = self.read_only_keys(i);
                if ro.is_empty() { return FromObj::Reply(self.commit(i)); }
                let t = self.t(i); t.phase = Phase::Validate; t.pend = ro;
                self.validate(i).unwrap()
            }
            Phase::Validate => self.validate(i).unwrap(),
            Phase::CommitPoint => FromObj::Reply(self.commit(i)),
            p => panic!("step in phase {p:?}"),
        }
    }
}

fn object(n: usize, keys: u8, ser: bool) {
    let Setup::Ports(ports) = recv_tagged_msg_block::<_, Setup>(|_, t| t == Some(SETUP)) else { panic!("setup") };
    let mut o = Obj {
        ser,
        cur: vec![0; keys as usize],
        held: vec![None; keys as usize],
        jobs: Vec::new(),
        tx: (0..n).map(|_| Tx { phase: Phase::Active, dec: Decision::Undecided, read: vec![None; keys as usize],
                                writes: Vec::new(), pend: Vec::new(), failed: false }).collect(),
        done: 0,
    };
    while o.done < n {
        let m: ToObj = recv_tagged_msg_block(|_, t| t.is_none());
        let (i, r) = match m {
            ToObj::Op(i, req) => (i, o.op(i, req)),
            ToObj::Step(i) => (i, o.step(i)),
        };
        send_msg(ports[(i - 1) as usize], r);
    }
}

fn port(i: u8, obj: ThreadId) {
    let Setup::Client(c) = recv_tagged_msg_block::<_, Setup>(|_, t| t == Some(SETUP)) else { panic!("setup") };
    loop {
        let req: Req = recv_tagged_msg_block(move |t, tag| t == c && tag.is_none());
        send_msg(obj, ToObj::Op(i, req));
        loop {
            match recv_tagged_msg_block::<_, FromObj>(move |t, _| t == obj) {
                FromObj::More => send_msg(obj, ToObj::Step(i)),
                FromObj::Reply(r) => {
                    let end = matches!(r, Rep::Aborted | Rep::Committed | Rep::Unknown);
                    send_msg(c, r);
                    if end { return; }
                    break;
                }
            }
        }
    }
}

struct Inst { keys: u8, ser: bool, writes: Vec<Vec<u8>> }

fn parse(a: &[String]) -> Inst {
    let pattern = a.first().map(String::as_str).unwrap_or("lost");
    let keys: u8 = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(if pattern == "lost" { 1 } else { 2 });
    let ser = match a.get(2).map(String::as_str).unwrap_or("snapshot") {
        "snapshot" => false,
        "serializable" => true,
        x => panic!("iso is snapshot or serializable, not {x}"),
    };
    let writes = match pattern {
        "lost" => vec![vec![1], vec![1]],
        "skew" => vec![vec![1], vec![2]],
        "both" => vec![vec![1, 2], vec![1, 2]],
        "mixed" => vec![vec![1, 2], vec![2]],
        "mixedro" => vec![vec![1, 2], vec![2], vec![]],
        x => panic!("unknown pattern {x}"),
    };
    assert!(keys == 1 || keys == 2);
    assert!(keys == 2 || pattern == "lost", "k = 1 only with lost");
    Inst { keys, ser, writes }
}

fn spec_program(inst: &Inst) {
    let n = inst.writes.len();
    let (keys, ser) = (inst.keys, inst.ser);
    let obj = thread::Builder::new().name("spec".to_string()).spawn(move || object(n, keys, ser)).unwrap();
    let oid = obj.thread().id();
    let mut ports = Vec::new();
    for i in 1..=n as u8 {
        ports.push(thread::Builder::new().name(format!("h{i}")).spawn(move || port(i, oid)).unwrap());
    }
    let pids: Vec<ThreadId> = ports.iter().map(|p| p.thread().id()).collect();
    let mut clients = Vec::new();
    for i in 1..=n as u8 {
        let w = inst.writes[(i - 1) as usize].clone();
        let h = pids[(i - 1) as usize];
        clients.push(thread::Builder::new().name(format!("c{i}")).spawn(move || client(i, keys, &w, h)).unwrap());
    }
    let cids: Vec<ThreadId> = clients.iter().map(|c| c.thread().id()).collect();
    let pids2 = pids.clone();
    let init = thread::Builder::new().name("init".to_string()).spawn(move || {
        send_tagged_msg(oid, SETUP, Setup::Ports(pids2.clone()));
        for (p, c) in pids2.iter().zip(cids.iter()) { send_tagged_msg(*p, SETUP, Setup::Client(*c)); }
    }).unwrap();
    init.join().unwrap();
    for c in clients { c.join().unwrap(); }
    for p in ports { p.join().unwrap(); }
    obj.join().unwrap();
}

fn entry(a: &[String]) -> Entry {
    let inst = Arc::new(parse(a));
    Arc::new(move || spec_program(&inst))
}

fn visible(a: &[String]) -> Vec<String> {
    (1..=parse(a).writes.len()).map(|i| format!("c{i}")).collect()
}

fn main() -> ExitCode {
    traceforge::program::main(Program { visible: &visible, spec: &entry, imp: &entry })
}
