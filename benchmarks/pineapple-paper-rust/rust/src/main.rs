//! Pineapple (Bantikyan et al., NSDI 2025), section 3: the single-key protocol in the common
//! case (stable leader n0, no failure, no leader change, no reconfiguration, one key).
//! ABD component (Algorithm 1) for Write and Read, Multi-Paxos component (Algorithm 2) for
//! Rmw, connected by pstamps ((ts, id), slot) compared lexicographically.
//! Processes: nodes n0, n1, n2 (internal), clients c0, c1 (visible), init (internal).
//! Usage: see traceforge::program; program arguments `<q> [<fifo|bag>]`.
use std::process::ExitCode;
use std::sync::Arc;

use traceforge::program::{Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

const NODES: usize = 3;
const CLIENTS: usize = 2;
const LEADER: usize = 0;
/// Majority quorum, floor(n/2) + 1 (section 3.1, Analysis).
const QUORUM: usize = NODES / 2 + 1;
/// Tags on node-bound messages: Setup from init, and node-to-node protocol messages.
/// Requests (from a client, or an Rmw forwarded to the leader) are untagged.
const TAG_SETUP: u32 = 1;
const TAG_NODE: u32 = 2;

/// ABD tag (ts, id): logical timestamp, writer proxy id.
type Tag = (u64, usize);
/// pstamp (tag, slot); the derived tuple order is exactly the paper's lexicographic order.
type PStamp = (Tag, u64);
/// Operation identifier (proxy node index, per-node sequence number), used to discard
/// late replies of an earlier operation.
type OpId = (usize, u64);

const PS0: PStamp = ((0, 0), 0);

// ---- interface messages (verbatim) ----
#[derive(Clone, Debug, PartialEq)] enum Req { Write(usize, u64), Read(usize), Rmw(usize) }   // client -> node: (client index, ...)
#[derive(Clone, Debug, PartialEq)] enum Rep { Ack, Value(u64), Result(u64) } // node -> client

#[derive(Clone, Debug, PartialEq)]
struct Setup {
    nodes: Vec<ThreadId>,
    clients: Vec<ThreadId>,
}

/// Node-to-node messages: the paper's GET-TAG / ACK-TAG / PUT / ACK-PUT / GET-VALUE /
/// ACK-VALUE (each carries the operation id and the sender's node index), and the
/// leader's answer to an Rmw forwarded by a non-leader proxy.
#[derive(Clone, Debug, PartialEq)]
enum NodeMsg {
    GetTag { op: OpId, from: usize },
    AckTag { op: OpId, from: usize, ps: PStamp },
    Put { op: OpId, from: usize, ps: PStamp, value: u64 },
    AckPut { op: OpId, from: usize },
    GetValue { op: OpId, from: usize },
    AckValue { op: OpId, from: usize, value: u64, ps: PStamp },
    RmwReply { client: usize, value: u64 },
}

// ---- clients (verbatim) ----
fn value(i: usize, j: usize) -> u64 { (1 + i + CLIENTS * j) as u64 }
fn client(i: usize, q: usize, node: ThreadId) {
    for j in 0..q {
        match (i + j) % 3 {
            0 => { send_msg(node, Req::Write(i, value(i, j))); let Rep::Ack = recv_msg_block::<Rep>() else { panic!("Ack expected") }; }
            1 => { send_msg(node, Req::Read(i)); let Rep::Value(_) = recv_msg_block::<Rep>() else { panic!("Value expected") }; }
            _ => { send_msg(node, Req::Rmw(i)); let Rep::Result(_) = recv_msg_block::<Rep>() else { panic!("Result expected") }; }
        }
    }
}

/// The one-shot transaction of the interface: f(v) = v + 10.
fn f(v: u64) -> u64 {
    v + 10
}

// ---- nodes ----
struct Node {
    me: usize,
    nodes: Vec<ThreadId>,
    clients: Vec<ThreadId>,
    /// Storage_i[key].value and Storage_i[key].ps (one key).
    value: u64,
    ps: PStamp,
    /// slot_i: the leader's Multi-Paxos slot index (only the leader uses it).
    slot: u64,
    seq: u64,
}

impl Node {
    fn send_node(&self, j: usize, m: NodeMsg) {
        send_tagged_msg(self.nodes[j], TAG_NODE, m);
    }

    /// "send ... to all": every node, this one included.
    fn send_all(&self, m: NodeMsg) {
        for j in 0..NODES {
            self.send_node(j, m.clone());
        }
    }

    fn new_op(&mut self) -> OpId {
        self.seq += 1;
        (self.me, self.seq)
    }

    /// Background handlers (Algorithm 1, lines 18-26). Returns the message back when it is
    /// not a request (a reply, for the caller to inspect).
    fn handle(&mut self, m: NodeMsg) -> Option<NodeMsg> {
        match m {
            NodeMsg::GetTag { op, from } => {
                self.send_node(from, NodeMsg::AckTag { op, from: self.me, ps: self.ps });
                None
            }
            NodeMsg::Put { op, from, ps, value } => {
                if self.ps < ps {
                    self.ps = ps;
                    self.value = value;
                }
                self.send_node(from, NodeMsg::AckPut { op, from: self.me });
                None
            }
            NodeMsg::GetValue { op, from } => {
                self.send_node(
                    from,
                    NodeMsg::AckValue { op, from: self.me, value: self.value, ps: self.ps },
                );
                None
            }
            other => Some(other),
        }
    }

    /// Receives node messages, serving requests, until a reply arrives.
    fn next_reply(&mut self) -> NodeMsg {
        loop {
            let m: NodeMsg = recv_tagged_msg_block(|_, t| t == Some(TAG_NODE));
            if let Some(r) = self.handle(m) {
                return r;
            }
        }
    }

    /// Get phase of a write (Algorithm 1, lines 2-4): the largest pstamp of a quorum.
    fn get_tag(&mut self, op: OpId) -> PStamp {
        self.send_all(NodeMsg::GetTag { op, from: self.me });
        let mut seen = [false; NODES];
        let mut count = 0;
        let mut max = PS0;
        while count < QUORUM {
            if let NodeMsg::AckTag { op: o, from, ps } = self.next_reply() {
                if o == op && !seen[from] {
                    seen[from] = true;
                    count += 1;
                    if ps > max {
                        max = ps;
                    }
                }
            }
        }
        max
    }

    /// Get phase of a read (Algorithm 1, lines 10-13) or of an RMW (Algorithm 2, lines 2-4):
    /// the value with the largest pstamp of a quorum; `with_self` requires this node in it.
    fn get_value(&mut self, op: OpId, with_self: bool) -> (u64, PStamp) {
        self.send_all(NodeMsg::GetValue { op, from: self.me });
        let mut seen = [false; NODES];
        let mut count = 0;
        let mut max = PS0;
        let mut vmax = 0;
        while count < QUORUM || (with_self && !seen[self.me]) {
            if let NodeMsg::AckValue { op: o, from, value, ps } = self.next_reply() {
                if o == op && !seen[from] {
                    seen[from] = true;
                    count += 1;
                    if ps > max {
                        max = ps;
                        vmax = value;
                    }
                }
            }
        }
        (vmax, max)
    }

    /// Put phase (Algorithm 1, lines 7-8 and 15-16; Algorithm 2, lines 8-9).
    fn put(&mut self, op: OpId, ps: PStamp, value: u64) {
        self.send_all(NodeMsg::Put { op, from: self.me, ps, value });
        let mut seen = [false; NODES];
        let mut count = 0;
        while count < QUORUM {
            if let NodeMsg::AckPut { op: o, from } = self.next_reply() {
                if o == op && !seen[from] {
                    seen[from] = true;
                    count += 1;
                }
            }
        }
    }

    /// Algorithm 1, Write at writer proxy N_i.
    fn write(&mut self, v: u64) {
        let op = self.new_op();
        let (tag_max, slot_max) = self.get_tag(op);
        let ps_new = ((tag_max.0 + 1, self.me), slot_max);
        let op = self.new_op();
        self.put(op, ps_new, v);
    }

    /// Algorithm 1, Read at reader proxy N_i.
    fn read(&mut self) -> u64 {
        let op = self.new_op();
        let (vmax, ps_max) = self.get_value(op, false);
        let op = self.new_op();
        self.put(op, ps_max, vmax);
        vmax
    }

    /// Algorithm 2, RMW at the leader N_l.
    fn rmw(&mut self) -> u64 {
        let op = self.new_op();
        let (vmax, ps_max) = self.get_value(op, true);
        let v_new = f(vmax);
        let ps_new = (ps_max.0, self.slot + 1);
        self.slot += 1;
        let op = self.new_op();
        self.put(op, ps_new, v_new);
        v_new
    }

    /// Serves one request; `c` is the client index, and node `c` is that client's proxy.
    fn serve(&mut self, r: Req) {
        match r {
            Req::Write(c, v) => {
                self.write(v);
                send_msg(self.clients[c], Rep::Ack);
            }
            Req::Read(c) => {
                let v = self.read();
                send_msg(self.clients[c], Rep::Value(v));
            }
            Req::Rmw(c) if self.me == LEADER => {
                let v = self.rmw();
                if c == self.me {
                    send_msg(self.clients[c], Rep::Result(v));
                } else {
                    self.send_node(c, NodeMsg::RmwReply { client: c, value: v });
                }
            }
            Req::Rmw(c) => {
                // Not the leader: forward the request to the leader and wait for its result,
                // serving the background handlers meanwhile.
                send_msg(self.nodes[LEADER], Req::Rmw(c));
                loop {
                    if let NodeMsg::RmwReply { client, value } = self.next_reply() {
                        if client == c {
                            send_msg(self.clients[c], Rep::Result(value));
                            break;
                        }
                    }
                }
            }
        }
    }
}

fn node(me: usize) {
    let Setup { nodes, clients } = recv_tagged_msg_block::<_, Setup>(|_, t| t == Some(TAG_SETUP));
    let mut n = Node { me, nodes, clients, value: 0, ps: PS0, slot: 0, seq: 0 };
    if me < CLIENTS || me == LEADER {
        // A proxy (and the leader): between operations it waits for the next request
        // (from its client, or, at the leader, an Rmw forwarded by a non-leader proxy).
        loop {
            let r: Req = recv_tagged_msg_block(|_, t| t.is_none());
            n.serve(r);
        }
    } else {
        // A node without a client: only the background handlers.
        loop {
            let _ = n.next_reply();
        }
    }
}

fn imp(q: usize) {
    let mut node_ids = Vec::new();
    for i in 0..NODES {
        let h = thread::Builder::new()
            .name(format!("n{i}"))
            .spawn(move || node(i))
            .unwrap();
        node_ids.push(h.thread().id());
    }
    let mut client_hs = Vec::new();
    for i in 0..CLIENTS {
        let nid = node_ids[i];
        client_hs.push(
            thread::Builder::new()
                .name(format!("c{i}"))
                .spawn(move || client(i, q, nid))
                .unwrap(),
        );
    }
    let client_ids: Vec<ThreadId> = client_hs.iter().map(|h| h.thread().id()).collect();
    let nodes = node_ids.clone();
    let init = thread::Builder::new()
        .name("init".to_string())
        .spawn(move || {
            for &n in &nodes {
                send_tagged_msg(n, TAG_SETUP, Setup { nodes: nodes.clone(), clients: client_ids.clone() });
            }
        })
        .unwrap();
    init.join().unwrap();
    for h in client_hs {
        h.join().unwrap();
    }
}

fn entry(args: &[String]) -> Entry {
    let q: usize = args.first().and_then(|s| s.parse().ok()).unwrap_or(1);
    Arc::new(move || imp(q))
}

fn visible(_: &[String]) -> Vec<String> {
    (0..CLIENTS).map(|i| format!("c{i}")).collect()
}

fn main() -> ExitCode {
    traceforge::program::main(Program { visible: &visible, spec: &entry, imp: &entry })
}
