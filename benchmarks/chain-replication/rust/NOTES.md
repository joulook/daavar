# bench_chainrep: chain replication (van Renesse & Schneider, OSDI 2004) on TraceForge's Must API

Source: `src/main.rs`; build file `Cargo.toml` (crate `bench_chainrep`, `traceforge` by path, `[workspace]`).

## Processes

- main: spawns `s0 … s{N-1}` (named via `thread::Builder::name`) in index order, then `c0 … c{K-1}`,
  then joins all; sends and receives nothing.
- client `ci`: `send_msg(s0, Update { from: i, value: i+1 })`, `recv_msg_block` → `UpdateReply`;
  then `send_msg(s{N-1}, Query { from: i })`, `recv_msg_block` → `QueryReply`.
- server `si`: one loop, `recv_msg_block::<Msg>()`, dispatch on the message (order = arrival order;
  no `nondet` is needed: the paper leaves nothing open that is a choice inside one server).

## Paper step → code

| Paper | Code |
|---|---|
| "Each update request is directed to the head" | client → `s0`: `Update { from, value }` |
| "processed there atomically using replica of objID at the head" | head arm `Msg::Update`: `value = v`, assigns sequence number `sn` |
| "the new value is computed once by the head and then forwarded … each replica has only to perform a write" | `Fwd { sn, value, client }` to successor; each server sets `value = v` |
| "forwarded along a reliable FIFO link to the next element … until handled by the tail" | arm `Msg::Fwd`: non-tail forwards to `s{i+1}`; FIFO per link is the harness model (`--model fifo`, default) |
| "Whenever server i forwards an update request r to its successor, server i also appends r to Sent_i" | `sent.push(sn)` after every forward |
| "Reply Generation. The reply for every request is generated and sent by the tail." | tail sends `UpdateReply { value }` (value right after applying the update) |
| "The tail sends an acknowledgement ack(r) to its predecessor when it completes … update request r" | tail sends `Ack { sn }` to `s{N-2}` |
| "upon receipt ack(r), a server i deletes r from Sent_i and forwards ack(r) to its predecessor" | arm `Msg::Ack`: `sent.retain(≠ sn)`, forward to predecessor if any (head stops it) |
| "Each query request is directed to the tail … processed there atomically using the replica stored at the tail" | arm `Msg::Query`: tail replies `QueryReply { value }` |

Internal messages (ours): `Fwd { sn: u32, value: i32, client: ThreadId }`, `Ack { sn: u32 }`.
All messages are one enum `Msg`.

## Decisions where the paper is silent

1. Update semantics. Paper: "can be a value V produced in some nondeterministic pre-programmed way involving
   the current value of objId and/or value newVal". Chosen: V = newVal (a plain write); no nondeterminism.
2. Hist kept as the current value only. Paper: "we are free to implement them in any way that is convenient."
   Each replica stores the integer value; `Sent_i` stores sequence numbers.
3. Sequence numbers. The paper mentions "the sequence number sn of the last update request" (Figure 3) without
   saying who assigns it. The head assigns 1, 2, … in processing order; carried in `Fwd`/`Ack`.
4. Which client to reply to. Paper: "The reply for every request is generated and sent by the tail" (does not
   say how the tail finds the client). The head puts the client's address into `Fwd`; for `Query` the tail uses
   `from`. Address of `c{from}` is looked up in a directory (INTERFACE: "assume every process knows every other
   process's address from the start").
5. How addresses are known. Paper: (INTERFACE) "leaves open how a server learns the next server's address and how
   a client learns the head's and the tail's". Implemented as a shared, write-once directory (`Directory`):
   server addresses written by main after spawning all servers and before spawning any client; client `ci`
   writes its own address as its first step, before its first send. Every read happens after receiving a
   message, hence causally after the write. This shared structure carries no protocol data; all protocol
   exchanges are messages.
6. Termination. The paper's servers run forever. Here every server stops after exactly 2K receives
   (head: K `Update` + K `Ack`; middle: K `Fwd` + K `Ack`; tail: K `Fwd` + K `Query`; N = 1: K `Update`
   + K `Query`), which is the exact number of messages each receives in a failure-free run.
7. The head's `Ack`s are consumed (Sent emptied) and not forwarded further; there is no one before the head.
8. Failures, the master, chain extension, T2 (ignored requests), client re-issue: not implemented (INTERFACE:
   no server fails). `Sent_i` is maintained but never read, since it serves only failure recovery.
9. N = 1 is handled (head = tail: replies directly, no `Fwd`/`Ack`).

## Commands

Build (from this directory):

    CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/cf2-chainrep-target cargo build --release -j 2

Run (plain exploration, N=3 K=2):

    /local/home/mkhoshechin/tmp/cf2-chainrep-target/release/bench_chainrep count impl --model fifo -- 3 2 </dev/null

Result line: `execs=18 blocked=0`. The last line printed (the measurement block after it) is `cache_hits: `.
