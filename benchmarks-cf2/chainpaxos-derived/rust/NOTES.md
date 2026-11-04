# ChainPaxos on one register: the derived specification (`bench_chainpaxos_derived`)

Sources read: `plan/case-studies/chainpaxos/PAPER-EXCERPT.md` (sections 3, 4, Annex A),
`plan/case-studies/chainpaxos/INTERFACE.md`, TraceForge `lib.rs` and `program.rs`, the template
crate. Nothing else. `spec` and `imp` of `Program` both return the program in `src/main.rs`.

## 1. What the clients can observe (the claim the program encodes)

The client commands are fixed by the interface, so a behaviour is the sequence of reply values each
client receives (plus, for a blocked run, which request is never answered).

- `mixed`: every command (Write, Read, Rmw) is one instance; the complete behaviours are exactly
  those of an atomic register under some interleaving of the two clients' command sequences that
  respects each client's own order. No blocked behaviour.
- `local`: the complete behaviours are the same set as `mixed`. In addition there are blocked
  behaviours: a run of the above, cut off so that each client is either done or waiting forever on
  a Read, at least one waiting, and no Write/Rmw of either client is ordered after a waiting Read.

## 2. Paper sentences behind each part

| Part of the specification | Paper sentence(s) |
|---|---|
| One total order of commands, executed in that order at every replica (n0 as single sequencer applying commands to `x`) | "ChainPaxos is used to order the execution of operations." "Replicas start in the same initial state and apply the same sequence of operations, thus guaranteeing that all replicas transition through the same sequence of states and output the same results." Annex A: "no replica considers an instance as decided before knowing the operations that have been ordered in all previous instances". |
| The order is the order in which the leader receives the requests (arrival order at n0 = the freedom) | "Requests from clients can be received by any replica, and are redirected to the leader (Alg. 2, line 1) ... The leader, upon receiving a new request, starts a new instance by increasing the instance number" (Alg. 2 lines 5-11). |
| c1's request goes n2 -> n0, the reply comes back through n2 | Alg. 2 line 3 `SEND(csleader, <REDIRECT_REQUEST, req>)`; interface: the front role "receiving a client's request, redirecting it to the leader and replying to the client when the request is executed belongs to the replica the client is connected to". |
| Every Write/Rmw (and every `mixed` Read) is eventually answered | "the leader sends the accept message ... When the accept message reaches the tail ... it sends a message directly to the head with the accept ack of all replicas, guaranteeing that the head learns about the decided value"; "The nodes in the second half of the chain ... can decide instances as soon as they receive the accept message, while the first n/2 nodes only decide (and execute an operation) after receiving an acknowledgement (the leader via an accept ack message ...)"; interface: every message is eventually delivered, no failure. |
| `mixed`: reads are commands like writes | Interface: "`mixed` (reads are ordered through the chain like writes: ChainPaxosMixed)"; section 4: "most SMR protocols only support linearizable reads by executing them as normal consensus operations". |
| `local`: a read returns the state at a point between its receipt and its reply (so complete behaviours = atomic register, i.e. the `mixed` set) | "Upon receiving the operation, the replica locally registers that the operation depends on the lowest unseen consensus instance ... Upon receiving the accept ack message to the consensus instance for which the operation depends on, the read operation is performed locally in the local committed state and the reply is sent to the client." "i) a read r returns a value that is at least as recent as any value outputted by the protocol at the moment the read was received." "the result of a read will also reflect the result of any committed write operations, at the moment the read was received". |
| `local`: a read may be answered with no later client write, via a no-op | "the head of the chain issues periodic NoOP operations if no write is received ... the maximum latency of reads in scenarios with a low load will be controlled by the frequency of these NoOP operations"; interface: the leader "may start a no-op instance, at most one per outstanding client read". |
| `local`: a read may never be answered (the `nondet()` park) | Same sentences read the other way: a read waits for instance (highest seen + 1); if the leader declines every no-op (the interface's "may") and no Write/Rmw instance is started afterwards, that instance never exists. "Under low load, write operations may be less frequent, which could delay read operations." |
| No replica-to-replica structure (chain, instances, acks) in the program | The prompt; Annex A: in a fault-free run an instance of ChainPaxos "is equivalent to the execution of an instance in Multi-Paxos". |

## 3. The program, role by role and message by message

Threads, spawned by `main` in this order: `n0`, `n1`, `n2`, `c0`, `c1`, then the invisible `init`;
`main` joins them and sends/receives nothing.

- `init`: sends `Setup { nodes: [n0,n1,n2], clients: [c0,c1] }` to each replica with
  `send_tagged_msg(_, SETUP_TAG = 1, _)`, then ends.
- `c0`, `c1`: the interface's client code verbatim; `c0`'s node is `n0`, `c1`'s is `n2`.
- `n0` (leader / sequencer): receives `Setup` (tagged receive, so a client request that arrives
  first stays buffered). Then loops until every client is done or parked: `recv_msg_block::<Req>()`
  (from `c0` directly or from `n2`; which one comes first is the protocol's ordering freedom);
  `Write(i, v)`: `x := v`, reply `Ack`; `Rmw(i)`: `x := x + 10`, reply `Result(x)`; `Read(i)`:
  reply `Value(x)`, except in `local` where it may park (below). A reply to client 0 is sent to `c0`;
  a reply to client 1 is sent to `n2` as `ToFront::Reply(rep)`; a parked read of client 1 is
  announced to `n2` as `ToFront::Park`. Ends.
- `n2` (c1's front): receives `Setup`, then `q` times: receives `Req` from `c1` (tagged receive
  filtered on sender `c1`), forwards it to `n0` (the REDIRECT_REQUEST), receives `ToFront` from `n0`
  (filtered on sender `n0`); `Reply(rep)` is sent to `c1`, `Park` ends `n2`. Ends.
- `n1`: receives `Setup` and ends.

Internal messages: `Setup` (init -> n0, n1, n2, tagged), `Req` (n2 -> n0, the redirect; the
interface's type reused since it carries the client index), `ToFront::{Reply, Park}` (n0 -> n2).

The park rule (`may_park`, `local` only, at a Read of client `i`; `k` the other client): a read may
stay unanswered only if no Write/Rmw instance is ever started after it.
- If `k` already has a parked read: `i` must park now if its next command is a Write/Rmw;
  otherwise (next is a Read, or this is its last command) `nondet()` decides.
- Else `i` may park (`nondet()`) only if `k` is done or `k`'s next command (the one `n0` will
  receive from `k` next) is a Read; if `k`'s next command is a Write/Rmw, the read is answered.
The internal threads end in every execution; a blocked execution is blocked only on the parked
client(s) (and `main`'s join).

## 4. Decisions where the paper (or the interface) was silent

1. **Blocked behaviours are part of the `local` specification.** The interface's no-op is a "may";
   a run in which the leader never starts it and no Write/Rmw follows leaves a read waiting forever.
   I read "exactly the set of behaviours the protocol can show" as including these, so the `local`
   spec has blocked executions. If the comparison should consider complete executions only, the
   park branch can be removed (set `may_park` to `false`) and `local` becomes identical to `mixed`.
2. **The exact condition for a read to stay unanswered** was derived, not stated: it needs no
   instance after the read's dependency, hence no Write/Rmw of either client after it. I argued that
   a parked read can always be placed last in the order (it returns nothing), so the only visible
   constraint is that neither client issues a Write/Rmw after it. The tail's lag (instances started
   but not yet seen by `n2`) can let c1's later reads be answered after c0's read parks; those reads
   then return a value of some earlier state, which the interleaving already covers. Not checked by
   running `local` (only one run was allowed).
3. **Local reads give no visible behaviour beyond `mixed`.** Since only per-client order is visible
   and timing is free, a read linearized at its execution point gives the same value sets as a read
   ordered as a command. In particular I assume the leader can always start a no-op right after a
   read's receipt (when no request is pending) so the read can see exactly the current state.
4. **The tail and local reads.** The paper says the read is answered "upon receiving the accept ack"
   for its instance; the tail receives no accept ack. I took the tail's event to be its decision of
   that instance (when the accept arrives). Either reading leaves the visible set unchanged.
5. **Out-of-order acks under `bag`** (an ack for d+1 before d): the paper is silent; it does not
   change the visible set under my reasoning (the read still returns a decided prefix taken between
   receipt and reply).
6. **n1** has no role in the specification; it only receives its Setup so that it ends.
7. **Program arguments**: `<q> <mixed|local> [<fifo|bag>]`; the third argument is accepted and
   ignored (the model is `--model`). `q < 1` or an unknown variant panics.
8. **Cargo.toml** has `version = "0.1.0"` in addition to the prescribed `name` and `edition`.
9. The `nondet()` in `may_park` is asked only where parking is allowed but not forced; when forced
   no choice is made (a choice with one outcome is not a freedom).

## 5. Build and run

Compile (inside this directory): `cargo build --release -j 2` -> `Finished \`release\` profile
[optimized] target(s) in 2m 02s`, no warnings.

The one run (inside this directory):
`timeout 600 target/release/bench_chainpaxos_derived count spec --model fifo -- 1 mixed`
Result line: `execs=2 blocked=0` (c0 writes 1, c1 reads 0 or 1). The last line printed is the end
of the measurement block, `cache_hits: ` (empty value); the line before the block is
`execs=2 blocked=0`.
