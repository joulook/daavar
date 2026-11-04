//! ChainPaxos on one register (USENIX ATC 2022), the derived *specification*: what the two
//! clients can observe, for the variants `mixed` (reads ordered through the chain) and `local`
//! (section 4's linearizable local reads), at the interface's instance (chain n0 -> n1 -> n2,
//! n0 the leader for the whole run, no failure). Both `spec` and `imp` return this program.
//!
//! The chain, the Paxos instances, the accept acks and the messages among replicas are not
//! modelled. What is kept (NOTES.md gives the paper's sentence behind each part):
//! - n0 (the leader) is the single point of order: it receives every client command, c0's
//!   directly and c1's redirected by n2, and applies them to `x` in the order they arrive.
//!   The arrival order is the protocol's freedom of ordering concurrent requests.
//! - n2 is c1's front: it redirects c1's request to n0 and relays the reply to c1.
//! - n1 only receives its Setup.
//! - `local`: a read may be left unanswered forever (the leader declines its no-op and no
//!   further write or Rmw instance is ever started). This is a `nondet()` choice, taken only
//!   where the rest of the run can still avoid any further Write/Rmw instance.
use std::sync::Arc;

use traceforge::program::{self, Entry, Program};
use traceforge::thread::{self, ThreadId};
use traceforge::*;

// ---- the interface's message types and client code, verbatim ----
#[derive(Clone, Debug, PartialEq)]
enum Req {
    Write(usize, u64),
    Read(usize),
    Rmw(usize),
}
#[derive(Clone, Debug, PartialEq)]
enum Rep {
    Ack,
    Value(u64),
    Result(u64),
}

const CLIENTS: usize = 2;
fn value(i: usize, j: usize) -> u64 {
    (1 + i + CLIENTS * j) as u64
}
fn client(i: usize, q: usize, node: ThreadId) {
    for j in 0..q {
        match (i + j) % 3 {
            0 => { send_msg(node, Req::Write(i, value(i, j))); let Rep::Ack = recv_msg_block::<Rep>() else { panic!("Ack expected") }; }
            1 => { send_msg(node, Req::Read(i)); let Rep::Value(_) = recv_msg_block::<Rep>() else { panic!("Value expected") }; }
            _ => { send_msg(node, Req::Rmw(i)); let Rep::Result(_) = recv_msg_block::<Rep>() else { panic!("Result expected") }; }
        }
    }
}

// ---- internal messages ----
#[derive(Clone, Debug, PartialEq)]
struct Setup {
    nodes: Vec<ThreadId>,
    clients: Vec<ThreadId>,
}
const SETUP_TAG: u32 = 1;

/// n0 -> n2: the reply to relay to c1, or the notice that c1's read is never answered.
#[derive(Clone, Debug, PartialEq)]
enum ToFront {
    Reply(Rep),
    Park,
}

#[derive(Clone, Copy, PartialEq)]
enum Variant {
    Mixed,
    Local,
}

fn is_read(i: usize, j: usize) -> bool {
    (i + j) % 3 == 1
}

fn recv_setup() -> Setup {
    recv_tagged_msg_block::<_, Setup>(|_, tag| tag == Some(SETUP_TAG))
}

/// n0: the leader. Orders every command in arrival order and executes it on `x`.
fn leader(q: usize, variant: Variant) {
    let s = recv_setup();
    let n2 = s.nodes[2];
    let c0 = s.clients[0];
    let mut x: u64 = 0;
    let mut served = [0usize; CLIENTS]; // commands of each client answered so far
    let mut parked = [false; CLIENTS]; // the client's current read is never answered
    let finished = |served: &[usize; CLIENTS], parked: &[bool; CLIENTS]| {
        (0..CLIENTS).all(|i| served[i] == q || parked[i])
    };
    while !finished(&served, &parked) {
        let req = recv_msg_block::<Req>();
        let (i, rep) = match req {
            Req::Write(i, v) => {
                x = v;
                (i, Some(Rep::Ack))
            }
            Req::Rmw(i) => {
                x += 10;
                (i, Some(Rep::Result(x)))
            }
            Req::Read(i) => {
                let park = variant == Variant::Local && may_park(i, q, &served, &parked);
                if park {
                    (i, None)
                } else {
                    (i, Some(Rep::Value(x)))
                }
            }
        };
        let to = |m: Rep| {
            if i == 0 {
                send_msg(c0, m)
            } else {
                send_msg(n2, ToFront::Reply(m))
            }
        };
        match rep {
            Some(m) => {
                served[i] += 1;
                to(m);
            }
            None => {
                parked[i] = true;
                if i == 1 {
                    send_msg(n2, ToFront::Park);
                }
            }
        }
    }
}

/// `local` only: whether the read of client `i` (its command `served[i]`) is left unanswered.
/// A read stays unanswered only if no Write/Rmw instance is ever started after it, so:
/// - if the other client already has a parked read, `i` must never send a Write/Rmw again:
///   it must park here when its next command is a Write/Rmw, and may park otherwise;
/// - otherwise `i` may park only when the other client's next command is not a Write/Rmw
///   (it is done, or its next command is a read, which the rule above then constrains).
fn may_park(i: usize, q: usize, served: &[usize; CLIENTS], parked: &[bool; CLIENTS]) -> bool {
    let k = 1 - i;
    let j = served[i];
    if parked[k] {
        let next_is_instance = j + 1 < q && !is_read(i, j + 1);
        if next_is_instance {
            return true;
        }
        return nondet();
    }
    let k_next = served[k];
    let k_quiet = k_next >= q || is_read(k, k_next);
    if k_quiet {
        nondet()
    } else {
        false
    }
}

/// n2: c1's front. Redirects c1's command to the leader and relays the reply.
fn front(q: usize) {
    let s = recv_setup();
    let n0 = s.nodes[0];
    let c1 = s.clients[1];
    for _ in 0..q {
        let r = recv_tagged_msg_block::<_, Req>(move |t, _| t == c1);
        send_msg(n0, r);
        match recv_tagged_msg_block::<_, ToFront>(move |t, _| t == n0) {
            ToFront::Reply(m) => send_msg(c1, m),
            ToFront::Park => return,
        }
    }
}

/// n1: not involved in what the clients observe.
fn middle() {
    let _ = recv_setup();
}

fn spec_program(q: usize, variant: Variant) {
    let spawn = |name: &str, f: Box<dyn FnOnce() + Send + 'static>| {
        thread::Builder::new()
            .name(name.to_string())
            .spawn(f)
            .unwrap()
    };
    let h0 = spawn("n0", Box::new(move || leader(q, variant)));
    let h1 = spawn("n1", Box::new(middle));
    let h2 = spawn("n2", Box::new(move || front(q)));
    let nodes: Vec<ThreadId> = vec![h0.thread().id(), h1.thread().id(), h2.thread().id()];
    let (n0, n2) = (nodes[0], nodes[2]);
    let hc0 = spawn("c0", Box::new(move || client(0, q, n0)));
    let hc1 = spawn("c1", Box::new(move || client(1, q, n2)));
    let clients: Vec<ThreadId> = vec![hc0.thread().id(), hc1.thread().id()];
    let hi = spawn(
        "init",
        Box::new(move || {
            let s = Setup { nodes: nodes.clone(), clients };
            for n in nodes {
                send_tagged_msg(n, SETUP_TAG, s.clone());
            }
        }),
    );
    for h in [h0, h1, h2, hc0, hc1, hi] {
        h.join().unwrap();
    }
}

fn parse(args: &[String]) -> (usize, Variant) {
    let q: usize = args
        .first()
        .and_then(|s| s.parse().ok())
        .filter(|&q| q >= 1)
        .expect("usage: <q> <mixed|local> [<fifo|bag>], q >= 1");
    let variant = match args.get(1).map(String::as_str) {
        Some("mixed") => Variant::Mixed,
        Some("local") => Variant::Local,
        _ => panic!("usage: <q> <mixed|local> [<fifo|bag>]"),
    };
    (q, variant)
}

fn entry(args: &[String]) -> Entry {
    let (q, variant) = parse(args);
    Arc::new(move || spec_program(q, variant))
}

fn main() -> std::process::ExitCode {
    program::main(Program {
        visible: &|_| vec!["c0".to_string(), "c1".to_string()],
        spec: &entry,
        imp: &entry,
    })
}
