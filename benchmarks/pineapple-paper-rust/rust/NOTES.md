# Pineapple, single key, common case: Rust (TraceForge Must API) implementation

Source: PAPER-EXCERPT.md, section 3 (Algorithms 1 and 2, pstamps), common case only (stable leader n0,
no failure, no leader change, no reconfiguration, one key). Interface: INTERFACE.md. Code: src/main.rs.

## Processes
- `main`: spawns n0, n1, n2, then c0, c1, then `init`; joins `init` and the clients; no message event.
- `init` (internal): sends each node `Setup { nodes, clients }`, tagged TAG_SETUP = 1.
- `c0`, `c1` (visible): the interface's client code, verbatim. Client ci's proxy is node ni.
- `n0` (leader, proxy of c0), `n1` (proxy of c1), `n2` (no client): internal, never terminate.

## State per node
`Storage[key].value` (u64, initially 0), `Storage[key].ps` (pstamp, initially ((0,0),0)), and at the
leader `slot_i` (initially 0). A pstamp is `((ts, id), slot)`; Rust's tuple order is the paper's
lexicographic order (ts, then id, then slot). Quorum = floor(3/2)+1 = 2.

## Messages
Client -> node (untagged): `Req::Write(c, v)`, `Req::Read(c)`, `Req::Rmw(c)`.
Node -> client: `Rep::Ack`, `Rep::Value(v)`, `Rep::Result(v)` (always sent by the client's own proxy).
Node -> node (type `NodeMsg`, tagged TAG_NODE = 2; each carries an op id `(proxy, seq)` and the sender index):
- `GetTag` / `AckTag{ps}`  (Alg. 1 GET-TAG / ACK-TAG)
- `Put{ps, value}` / `AckPut`  (PUT / ACK-PUT; used by writes, reads and RMWs)
- `GetValue` / `AckValue{value, ps}`  (GET-VALUE / ACK-VALUE; reads and RMWs)
- `RmwReply{client, value}`: leader -> the non-leader proxy that forwarded an Rmw.
Non-leader proxy -> leader: the client's `Req::Rmw(c)` forwarded unchanged (untagged).

## Roles
Handlers (Alg. 1, lines 18-26), at every node whenever it receives a NodeMsg request:
GetTag -> AckTag(Storage.ps); Put(ps_j, v_j) -> if Storage.ps < ps_j then store (ps_j, v_j); AckPut;
GetValue -> AckValue(Storage.value, Storage.ps).

Write(v) at proxy Ni (Alg. 1, 1-8): GetTag to all (itself included); wait for AckTag from 2 distinct
nodes; ps_max = (tag_max, slot_max) largest; ps_new = ((tag_max.ts+1, i), slot_max); Put(ps_new, v) to
all; wait for 2 AckPut; reply Ack.

Read at proxy Ni (Alg. 1, 9-17): GetValue to all; wait for 2 AckValue; (v_max, ps_max) with the largest
pstamp; Put(ps_max, v_max) to all; wait for 2 AckPut; reply Value(v_max).

Rmw at the leader n0 (Alg. 2): GetValue to all; wait until AckValue from >= 2 distinct nodes AND n0
itself among them ("the quorum Q must include the leader itself"); v_max with the largest pstamp
ps_max over all collected replies; v_new = f(v_max) = v_max + 10; ps_new = (ps_max.tag, slot_i + 1);
slot_i += 1; Put(ps_new, v_new) to all; wait for 2 AckPut; then Result(v_new) to c0 if the request came
from c0, else RmwReply{client, v_new} to the client's proxy.

Rmw at a non-leader proxy (n1): forwards `Req::Rmw(c)` to n0, then serves handlers until the
`RmwReply` for c arrives, then sends `Rep::Result(v)` to its client.

## Decisions (where the paper is silent or the API forces a choice)
1. Forwarding of Rmw from a non-leader and the route of the result. Paper: "RMW takes 3-RTT if the
   client is not co-located with the leader." It does not say how the request reaches the leader.
   Decision: the proxy forwards the request to the leader and relays the leader's reply back to its
   client (proxy<->leader is the third RTT next to Get and Put).
2. Leader's Get quorum must contain the leader: "Contact a quorum Q, which includes the leader itself".
   Decision: wait until 2 replies are in and one is the leader's own; the max is taken over every
   reply received by then (possibly all 3).
3. "send ... to all" includes the sender itself, as a real message to its own mailbox, handled by the
   same handlers; so the proxy's own reply counts towards the quorum like any other.
4. "wait until receiving ... from quorum": the first 2 replies from distinct nodes for the current
   phase; replies carry an op id, and late replies of an earlier phase/operation are ignored.
5. slot_i: "the leader can update the pstamp by incrementing the local slot index slot_i directly."
   Decision: slot_i starts at 0 (initial pstamp ((0,0),0), from Appendix B "tag_r is (0,0) and
   slot_r = 1 ... first non-read operation") and is incremented after each RMW; it is not merged with
   the observed ps_max.slot (with a stable leader every slot seen is <= slot_i).
6. Writer id in the tag: the node index (n0 = 0, n1 = 1, n2 = 2). Initial value 0.
7. One operation at a time per node: each node is one sequential thread. A request arriving while
   the node is busy (e.g. a forwarded Rmw at n0 while n0 serves c0) waits in the mailbox; the leader
   therefore runs RMWs one after another, in slot order.
8. Optimizations omitted. Paper: "Pineapple also adopts various optimizations mentioned in [10, 25,
   42]. As a result, Pineapple is able to always complete reads in 1 RTT when n = 3." The
   optimizations are not given; reads always do both phases of Algorithm 1.
9. API-forced (tradeoff to review): a Must receive returns one message type, and the client's
   `Req` and the node-to-node `NodeMsg` are different types, so a single thread cannot wait for
   "either". Decision: a node with a request source (n0, n1) waits, between operations, only for the
   next untagged `Req` (its client's, or at n0 a forwarded Rmw); while it runs an operation it waits
   only for tagged `NodeMsg` and serves every handler request it receives. n2 only serves handlers.
   Effect: handler requests to an idle n0 or n1 are delayed until that node next runs an operation,
   which is the same as the network delaying their delivery; quorums never need n0 or n1 while idle
   (n2 always answers, and the leader's own Get reply comes from itself while busy), so no deadlock.
   Rejected alternatives: a second thread per node (changes the process set), `nondet()` choosing
   the type (spurious blocked executions), non-blocking polling or `inbox` (unbounded/other events).
10. `main` joins `init` and the clients, not the nodes (nodes never end).
11. Program arguments: `<q> [<fifo|bag>]`; only q is read (the model comes from `--model`); q
    defaults to 1. `spec` and `imp` both return this program.

## Compile (inside this directory)
    cargo build --release -j 2
Result: `Finished `release` profile [optimized] target(s) in 1m 12s`, no warnings.

## The one run (inside this directory)
    timeout 600 target/release/bench_pineapple_paper count impl --model fifo -- 1
Stopped by the 600 s timeout I put on it, before the final `execs=... blocked=...` line. Last line:
    Executions attempted so far: 5000000 total 0 finished normally 5000000 blocked (8474.58/sec).
(Every execution ends blocked, as expected: the nodes never terminate.)
