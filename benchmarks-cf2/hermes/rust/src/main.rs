//! Hermes-replicated key (Katsarakis et al., ASPLOS 2020, §3.2: reads and writes only, no
//! faults, no replays) as a Must-API message-passing program.
//! Roles: replicas r0..r{N-1}, clients c0..c{K-1}; the main thread only spawns and joins.
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{main_with_model, Entry, Program};
use traceforge::thread::{self, construct_thread_id, ThreadId};
use traceforge::*;

/// The paper's logical timestamp [v, cid], ordered lexicographically (footnote 5).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Ts {
    version: i32,
    cid: i32,
}

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    // client <-> replica (fixed by INTERFACE.md, with the addendum's `from`)
    Write { from: i32, value: i32 },
    WriteAck {},
    Read { from: i32 },
    ReadReply { value: i32 },
    // replica <-> replica (Figure 3)
    Inv { ts: Ts, value: i32 },
    Ack { ts: Ts },
    Val { ts: Ts },
}

/// The per-key state of a replica (§3.2; Replay is never entered: no faults, no mlt timer).
#[derive(Clone, Copy, Debug, PartialEq)]
enum KeyState {
    Valid,
    Invalid,
    Write,
    Trans,
}

/// A client request that could not be served yet (stalled, §3.2 Reads / Coordinator).
enum Stalled {
    Read { from: usize },
    Write { from: usize, value: i32 },
}

/// The coordinator's write in flight: the client to answer, its timestamp and the ACKs so far.
struct Pending {
    client: usize,
    ts: Ts,
    acks: usize,
}

/// The process addresses, fixed before any process starts (INTERFACE: every process knows every
/// address from the start). Ids follow the spawn order: main 0, r0..r{N-1}, then c0..c{K-1};
/// main checks every prediction against the id the spawn returns.
#[derive(Clone)]
struct Addrs {
    replicas: Vec<ThreadId>,
    clients: Vec<ThreadId>,
}

fn addrs(n: usize, k: usize) -> Addrs {
    Addrs {
        replicas: (0..n).map(|j| construct_thread_id(1 + j as u32)).collect(),
        clients: (0..k).map(|i| construct_thread_id(1 + (n + i) as u32)).collect(),
    }
}

/// Number of messages replica `j` receives in a run: its client writes W and reads R, an INV and
/// a VAL for every write coordinated elsewhere, N-1 ACKs for every write it coordinates.
fn expected_msgs(j: usize, n: usize, k: usize) -> usize {
    let w = (0..k).filter(|i| i % n == j).count();
    let r = (0..k).filter(|i| (i + 1) % n == j).count();
    w + r + 2 * (k - w) + (n - 1) * w
}

struct Replica {
    id: usize,
    a: Addrs,
    state: KeyState,
    ts: Ts,
    value: i32,
    pending: Option<Pending>,
    stalled: Vec<Stalled>,
}

impl Replica {
    fn followers(&self) -> Vec<ThreadId> {
        (0..self.a.replicas.len())
            .filter(|&r| r != self.id)
            .map(|r| self.a.replicas[r])
            .collect()
    }

    /// Reads: served locally iff the key is Valid, otherwise stalled.
    fn read(&mut self, from: usize) {
        if self.state == KeyState::Valid {
            send_msg(self.a.clients[from], Msg::ReadReply { value: self.value });
        } else {
            self.stalled.push(Stalled::Read { from });
        }
    }

    /// Coordinator: issue a write only in the Valid state (and with no own write in flight).
    fn write(&mut self, from: usize, value: i32) {
        if self.state != KeyState::Valid || self.pending.is_some() {
            self.stalled.push(Stalled::Write { from, value });
            return;
        }
        // CTS
        let ts = Ts {
            version: self.ts.version + 1,
            cid: self.id as i32,
        };
        // CINV: broadcast INV, go to Write, apply the value locally
        self.ts = ts;
        self.value = value;
        self.state = KeyState::Write;
        self.pending = Some(Pending {
            client: from,
            ts,
            acks: 0,
        });
        for f in self.followers() {
            send_msg(f, Msg::Inv { ts, value });
        }
        self.try_complete();
    }

    /// CACK + CVAL: once all ACKs are in, complete, broadcast VAL and answer the client.
    fn try_complete(&mut self) {
        let done = matches!(&self.pending, Some(p) if p.acks == self.a.replicas.len() - 1);
        if !done {
            return;
        }
        let p = self.pending.take().unwrap();
        match self.state {
            KeyState::Write => self.state = KeyState::Valid,
            KeyState::Trans => self.state = KeyState::Invalid,
            _ => {}
        }
        for f in self.followers() {
            send_msg(f, Msg::Val { ts: p.ts });
        }
        send_msg(self.a.clients[p.client], Msg::WriteAck {});
        self.retry_stalled();
    }

    /// Stalled requests are retried, in arrival order, whenever the key may have become Valid.
    fn retry_stalled(&mut self) {
        if self.state != KeyState::Valid {
            return;
        }
        let stalled = std::mem::take(&mut self.stalled);
        for s in stalled {
            match s {
                Stalled::Read { from } => self.read(from),
                Stalled::Write { from, value } => self.write(from, value),
            }
        }
    }

    fn handle(&mut self, from_replica: Option<usize>, m: Msg) {
        match m {
            Msg::Write { from, value } => self.write(from as usize, value),
            Msg::Read { from } => self.read(from as usize),
            // FINV + FACK
            Msg::Inv { ts, value } => {
                if ts > self.ts {
                    self.state = match self.state {
                        KeyState::Write | KeyState::Trans => KeyState::Trans,
                        _ => KeyState::Invalid,
                    };
                    self.ts = ts;
                    self.value = value;
                }
                let coord = from_replica.expect("INV carries its coordinator in ts.cid");
                send_msg(self.a.replicas[coord], Msg::Ack { ts });
            }
            Msg::Ack { ts } => {
                if let Some(p) = self.pending.as_mut() {
                    if p.ts == ts {
                        p.acks += 1;
                    }
                }
                self.try_complete();
            }
            // FVAL
            Msg::Val { ts } => {
                if ts == self.ts {
                    self.state = KeyState::Valid;
                    self.retry_stalled();
                }
            }
            Msg::WriteAck {} | Msg::ReadReply { .. } => unreachable!("reply sent to a replica"),
        }
    }
}

fn replica(id: usize, a: Addrs, k: usize) {
    let n = a.replicas.len();
    let mut r = Replica {
        id,
        a,
        state: KeyState::Valid,
        ts: Ts { version: 0, cid: 0 },
        value: 0,
        pending: None,
        stalled: Vec::new(),
    };
    for _ in 0..expected_msgs(id, n, k) {
        let m: Msg = recv_msg_block();
        // The sender of an INV is its coordinator, ts.cid (no replays: the cid is the sender).
        let from_replica = match &m {
            Msg::Inv { ts, .. } => Some(ts.cid as usize),
            _ => None,
        };
        r.handle(from_replica, m);
    }
    assert(r.pending.is_none() && r.stalled.is_empty());
}

fn client(i: usize, a: Addrs) {
    let n = a.replicas.len();
    send_msg(
        a.replicas[i % n],
        Msg::Write {
            from: i as i32,
            value: i as i32 + 1,
        },
    );
    let m: Msg = recv_msg_block();
    assert(matches!(m, Msg::WriteAck {}));
    send_msg(a.replicas[(i + 1) % n], Msg::Read { from: i as i32 });
    let m: Msg = recv_msg_block();
    assert(matches!(m, Msg::ReadReply { .. }));
}

fn hermes(n: usize, k: usize) {
    let a = addrs(n, k);
    let mut hs = Vec::new();
    for j in 0..n {
        let aj = a.clone();
        let h = thread::Builder::new()
            .name(format!("r{j}"))
            .spawn(move || replica(j, aj, k))
            .unwrap();
        assert(h.thread().id() == a.replicas[j]);
        hs.push(h);
    }
    for i in 0..k {
        let ai = a.clone();
        let h = thread::Builder::new()
            .name(format!("c{i}"))
            .spawn(move || client(i, ai))
            .unwrap();
        assert(h.thread().id() == a.clients[i]);
        hs.push(h);
    }
    for h in hs {
        h.join().unwrap();
    }
}

fn size(args: &[String]) -> (usize, usize) {
    let n = args.first().and_then(|s| s.parse().ok()).unwrap_or(3);
    let k = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(2);
    (n, k)
}

fn visible(args: &[String]) -> Vec<String> {
    let (_, k) = size(args);
    (0..k).map(|i| format!("c{i}")).collect()
}

fn entry(args: &[String]) -> Entry {
    let (n, k) = size(args);
    Arc::new(move || hermes(n, k))
}

fn main() -> ExitCode {
    main_with_model(
        Program {
            visible: &visible,
            spec: &entry,
            imp: &entry,
        },
        ConsType::Bag,
    )
}
