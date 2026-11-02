//! Viewstamped Replication, normal operation (Liskov & Cowling 2012, section 4.1), on
//! TraceForge's Must API. N = 2f + 1 replicas `r0 … r{N-1}` (view 0, `r0` primary), K clients
//! `c0 … c{K-1}`, one request each; the replicated service is a counter with `add(n)`.
//! Usage: `bench_vrr count impl --model bag -- <N> <K>` (see `traceforge::program`).
use std::collections::{BTreeMap, BTreeSet};
use std::process::ExitCode;
use std::sync::Arc;

mod spec; // the coordinator's hand-written specification (plan/twins-contextfree/vrr/spec.rs); the agent's program is `entry`
use traceforge::program::{main_with_model, Entry, Program};
use traceforge::thread::{self, construct_thread_id, ThreadId};
use traceforge::*;

/// The view-number: the group is fixed and there is no view change.
const VIEW: i32 = 0;
/// Message tag of a COMMIT (a PREPARE is tagged with its op-number, which is at least 1).
const COMMIT_TAG: u32 = 0;

/// A client request as it sits in the log (the paper's `m`): client-id, request-number, op.
#[derive(Clone, Debug, PartialEq)]
struct Req {
    client: i32,
    s: i32,
    op: i32,
}

#[derive(Clone, Debug, PartialEq)]
enum Msg {
    /// client -> primary: <REQUEST op, c, s> with c = `from`, s = 1, op = add(`op`).
    Request { from: i32, op: i32 },
    /// primary -> client: <REPLY v, s, x> with only x kept.
    Reply { result: i32 },
    /// primary -> backups: <PREPARE v, m, n, k>.
    Prepare { v: i32, m: Req, n: i32, k: i32 },
    /// backup -> primary: <PREPAREOK v, n, i>.
    PrepareOk { v: i32, n: i32, i: i32 },
    /// primary -> backups: <COMMIT v, k>.
    Commit { v: i32, k: i32 },
}

/// Every process's address, computed from the spawn order (replicas, then clients): the
/// process at spawn position `p` has id `base + p`. `main` checks this after spawning.
#[derive(Clone, Copy)]
struct Addrs {
    base: u32,
    n: usize,
}

impl Addrs {
    fn of_self(pos: usize, n: usize) -> Addrs {
        Addrs { base: u32::from(thread::current_id()) - pos as u32, n }
    }
    fn replica(&self, j: usize) -> ThreadId {
        construct_thread_id(self.base + j as u32)
    }
    fn client(&self, c: usize) -> ThreadId {
        construct_thread_id(self.base + (self.n + c) as u32)
    }
}

/// Client-table entry: the most recent request-number and, once executed, its result.
#[derive(Clone, Copy, Default)]
struct ClientEntry {
    s: i32,
    result: Option<i32>,
}

/// The up-call to the service code: the counter's `add(op)`.
fn upcall(counter: &mut i32, op: i32) -> i32 {
    *counter += op;
    *counter
}

fn primary(n: usize, k: usize) {
    let addrs = Addrs::of_self(0, n);
    let f = (n - 1) / 2;
    let mut op_number: i32 = 0;
    let mut log: Vec<Req> = Vec::new();
    let mut commit_number: i32 = 0;
    let mut client_table: BTreeMap<i32, ClientEntry> = BTreeMap::new();
    let mut counter: i32 = 0;
    // PREPAREOKs received, per op-number, by backup number.
    let mut oks: BTreeMap<i32, BTreeSet<i32>> = BTreeMap::new();
    // The highest commit-number the backups have been told (in a PREPARE or a COMMIT).
    let mut announced: i32 = 0;
    let mut requests = 0usize;
    let mut replies = 0usize;

    while replies < k {
        match recv_msg_block::<Msg>() {
            // Step 2: compare with the client table.
            Msg::Request { from, op } => {
                let s = 1; // the one request of each client
                let e = client_table.get(&from).copied().unwrap_or_default();
                if s <= e.s {
                    if s == e.s {
                        if let Some(x) = e.result {
                            send_msg(addrs.client(from as usize), Msg::Reply { result: x });
                        }
                    }
                    continue;
                }
                // Step 3: advance op-number, append, update table, PREPARE to the others.
                requests += 1;
                op_number += 1;
                let m = Req { client: from, s, op };
                log.push(m.clone());
                client_table.insert(from, ClientEntry { s, result: None });
                for j in 1..n {
                    send_tagged_msg(
                        addrs.replica(j),
                        op_number as u32,
                        Msg::Prepare { v: VIEW, m: m.clone(), n: op_number, k: commit_number },
                    );
                }
                announced = commit_number;
            }
            // Step 5: wait for f PREPAREOKs from different backups.
            Msg::PrepareOk { v, n: on, i } => {
                if v != VIEW {
                    continue;
                }
                // A PREPAREOK for `on` means `on` and all earlier ops prepared at backup `i`.
                for x in 1..=on {
                    oks.entry(x).or_default().insert(i);
                }
                while commit_number < op_number
                    && oks.get(&(commit_number + 1)).map_or(0, |b| b.len()) >= f
                {
                    let req = log[commit_number as usize].clone();
                    let x = upcall(&mut counter, req.op);
                    commit_number += 1;
                    send_msg(addrs.client(req.client as usize), Msg::Reply { result: x });
                    client_table.insert(req.client, ClientEntry { s: req.s, result: Some(x) });
                    replies += 1;
                }
                // Step 6: no new client request in a timely way -> COMMIT. While more
                // requests may still come, whether the wait times out is a choice.
                if commit_number == op_number
                    && commit_number > announced
                    && requests < k
                    && nondet()
                {
                    for j in 1..n {
                        send_tagged_msg(
                            addrs.replica(j),
                            COMMIT_TAG,
                            Msg::Commit { v: VIEW, k: commit_number },
                        );
                    }
                    announced = commit_number;
                }
            }
            _ => {}
        }
    }
    // All K requests are executed: no new request will come, so the wait times out for sure.
    if commit_number > announced {
        for j in 1..n {
            send_tagged_msg(addrs.replica(j), COMMIT_TAG, Msg::Commit { v: VIEW, k: commit_number });
        }
    }
}

fn backup(i: usize, n: usize, k: usize) {
    let addrs = Addrs::of_self(i, n);
    let me = i as i32;
    let mut op_number: i32 = 0;
    let mut log: Vec<Req> = Vec::new();
    let mut commit_number: i32 = 0;
    let mut known_commit: i32 = 0;
    let mut client_table: BTreeMap<i32, ClientEntry> = BTreeMap::new();
    let mut counter: i32 = 0;

    while (commit_number as usize) < k {
        // Step 4: accept a PREPARE with op-number n only after entries 1..n-1 (receive only
        // the next op-number's PREPARE); a COMMIT can be received at any time.
        let next = (op_number + 1) as u32;
        let msg: Msg = recv_tagged_msg_block(move |_, tag| tag == Some(next) || tag == Some(COMMIT_TAG));
        match msg {
            Msg::Prepare { v, m, n: pn, k: pk } => {
                if v != VIEW {
                    continue;
                }
                assert_eq!(pn, op_number + 1);
                op_number += 1;
                log.push(m.clone());
                client_table.insert(m.client, ClientEntry { s: m.s, result: None });
                send_msg(addrs.replica(0), Msg::PrepareOk { v: VIEW, n: op_number, i: me });
                known_commit = known_commit.max(pk);
            }
            Msg::Commit { v, k: ck } => {
                if v != VIEW {
                    continue;
                }
                known_commit = known_commit.max(ck);
            }
            _ => {}
        }
        // Step 7: execute committed ops that are in the log, in order; no reply.
        while commit_number < known_commit && commit_number < op_number {
            let req = log[commit_number as usize].clone();
            let x = upcall(&mut counter, req.op);
            commit_number += 1;
            client_table.insert(req.client, ClientEntry { s: req.s, result: Some(x) });
        }
    }
}

fn client(c: usize, n: usize) {
    let addrs = Addrs::of_self(n + c, n);
    // Step 1: REQUEST to the primary, then wait for the REPLY.
    send_msg(addrs.replica(0), Msg::Request { from: c as i32, op: c as i32 + 1 });
    match recv_msg_block::<Msg>() {
        Msg::Reply { result } => {
            let _ = result;
        }
        other => panic!("client c{c} received {other:?}"),
    }
}

fn vrr(n: usize, k: usize) {
    let mut handles = Vec::new();
    for j in 0..n {
        let h = thread::Builder::new()
            .name(format!("r{j}"))
            .spawn(move || if j == 0 { primary(n, k) } else { backup(j, n, k) })
            .unwrap();
        handles.push(h);
    }
    for c in 0..k {
        let h = thread::Builder::new()
            .name(format!("c{c}"))
            .spawn(move || client(c, n))
            .unwrap();
        handles.push(h);
    }
    // The address computation of `Addrs` holds: ids are consecutive in spawn order.
    let base = u32::from(handles[0].thread().id());
    for (p, h) in handles.iter().enumerate() {
        assert_eq!(h.thread().id(), construct_thread_id(base + p as u32));
    }
    for h in handles {
        h.join().unwrap();
    }
}

fn parse(args: &[String]) -> (usize, usize) {
    let n = args.first().and_then(|s| s.parse().ok()).unwrap_or(3);
    let k = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(2);
    (n, k)
}

fn entry(args: &[String]) -> Entry {
    let (n, k) = parse(args);
    Arc::new(move || vrr(n, k))
}

fn visible(args: &[String]) -> Vec<String> {
    let (_, k) = parse(args);
    (0..k).map(|c| format!("c{c}")).collect()
}

fn spec_entry(args: &[String]) -> Entry {
    let (n, k) = parse(args);
    Arc::new(move || spec::spec(n, k))
}

fn main() -> ExitCode {
    main_with_model(
        Program { visible: &visible, spec: &spec_entry, imp: &entry },
        ConsType::Bag,
    )
}
