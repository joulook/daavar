# ChainPaxos (paper version), Rust / TraceForge Must API

Source: PAPER-EXCERPT.md (section 3: Alg. 1 state, Alg. 2 message flow, Alg. 3 auxiliary
functions; section 4: local linearizable reads) and INTERFACE.md. Fault-free only: there is no
PREPARE, no RemoveNode/AddNode, no keep-alive and no leader change. The chain is
`n0 -> n1 -> n2`, `n0` is the head and leader, `npleader = 0` throughout, the quorum is 2 of 3.

## Processes

- `main`: spawns `n0`, `n1`, `n2`, then `c0`, `c1`, then the invisible `init` (all with
  `thread::Builder::new().name(..)`), and joins `c0`, `c1` and `init`. It sends and receives nothing.
- `init`: sends `Setup { nodes, clients }` to each replica with `send_tagged_msg(.., SETUP_TAG, ..)`.
- `c0` (connected to `n0`) and `c1` (connected to `n2`): the client code from INTERFACE.md, verbatim.
- `n0`, `n1`, `n2`: a replica first takes its Setup with
  `recv_tagged_msg_block(|_, t| t == Some(SETUP_TAG))`. Anything that arrived earlier stays in the
  mailbox, which is the buffering the interface asks for. The replica then loops forever on
  `recv_msg_block::<Req>()`.

## Replica state (Alg. 1, fault-free subset)

`chain`, `self`, `cnextok` (the successor in the chain; the tail's successor is the leader),
`csleader = n0`, `npleader = 0`, `inst: ni -> (na, val, nacpts, decided)`, `submitted` (the
front's requests not yet answered), `pending` (leader), `maxack`, `maxacpt`, `amLeader`.
The fault-only variable `marked` is left out. The replica also has these:
- the register `x`, the highest executed instance `executed`, and the decided-but-not-yet-executed values `to_exec`;
- `client_reqs`, the number of requests received from the replica's own client (this numbers the requests);
- for `local`: `highest_seen`, `acked` (the highest instance known to be accepted by every node) and `local_reads`, a list of (client, instance the read depends on);
- leader only, for `local`: `noops_left`;
- the FIFO layer: `out_seq`, `in_next`, `in_buf` (see D2).

## Messages

Client <-> front: `Req::{Write, Read, Rmw}` and `Rep::{Ack, Value, Result}`, as in INTERFACE.md.
Among the replicas (`Wire`, sent with `send_tagged_msg(.., INTERNAL_TAG, ..)` and carried as
explained in D1):
- `Redirect { op }`: <REDIRECT_REQUEST, req>, front -> csleader.
- `Accept { ni, ldr, na, val, nacpts, mack }`: <ACCEPT, ...>, from the leader to itself, then along the chain.
- `AcceptAck { ni }`: <ACCEPT_ACK, ni>, tail -> leader.

`op = Op { kind: Write|Read|Rmw|NoOp, client, seq, value }`, where `seq` is the client's j.

## Message flow, role by role

- Front (`n0` for `c0`, `n2` for `c1`), Alg. 2 line 1: on a client request it builds `op`, adds it to
  `submitted` and sends `Redirect{op}` to `csleader`. For `n0` this is a send to itself.
  In `local`, a Read is not submitted: it is registered in `local_reads` and depends on instance
  `highest_seen + 1` (section 4). Nothing is sent.
- Leader, Alg. 2 line 5: on `Redirect{op}` it adds `op` to `pending` and calls StartInstance
  (Alg. 2 line 9): `maxacpt += 1`, then it sends `Accept{maxacpt, self, npleader, op, 0, maxack}`
  to itself and clears `pending`.
- Every replica, Alg. 2 line 13: on `Accept` with `npleader <= na`, it runs UpdateLeaderInfo
  (never fires here). It stores or updates `inst[ni]` with `nacpts + 1`. If that count is a
  quorum and `ni` is not yet decided, it calls Decide(ni). Then it runs DecideAndGCUpTo(mack) and
  Forward(ni).
  - Leader: count 1, so the instance is not decided yet. It forwards `Accept` with nacpts 1 to `n1`.
  - `n1`: count 2, so it decides and executes (the second half of the chain). It forwards to `n2` with nacpts 2.
  - `n2` (tail): count 3, so it decides and executes, and replies to `c1` if the op is one of
    its own. Its `cnextok` is the leader, so Forward sends `AcceptAck{ni}` to `n0`.
- Leader, Alg. 2 line 27: on `AcceptAck{ni}` it runs DecideAndGCUpTo(ni). This decides and
  executes every instance up to `ni`, replies to `c0` for its own ops and raises `maxack`. The
  next `Accept` carries the new `maxack` as `mack`, so `n1` and `n2` decide and GC up to it.
- Decide (Alg. 3 line 22): marks the instance decided and queues its value for execution.
  Execution runs in instance order (D6). The front whose `submitted` contains the op replies:
  `Ack` for Write, `Value(x)` for Read (`mixed`), `Result(x)` for Rmw.
- `local` reads (section 4): after each handled message, a replica answers `Value(x)` to each read
  whose dependency `dep` satisfies `dep <= min(acked, executed)`. `acked` is `maxack` (on the leader,
  this is what ACCEPT_ACK raises), and on the tail it is also every `ni` for which the tail sent
  ACCEPT_ACK (D7).
- `local` no-ops: at the top of each loop iteration, the leader may start a NoOp instance by
  `nondet()` (D10). This happens only when `pending` is empty, which it always is at that point.

## Decisions (each with the sentence that left it open)

- **D1 (one mailbox, one Rust type: API, not paper).** A Must receive returns one Rust type and
  panics on any other, and `Req` is fixed verbatim. The fronts `n0` and `n2` must take client
  requests and replica messages in arrival order. So the replicas' messages are encoded as
  `Req::Write(a, b)` with `a >= MAGIC = 1<<60`, which is never a client index (`encode`/`decode`
  in main.rs, with bounds asserted on every field). They also carry `INTERNAL_TAG`. They go only
  between invisible replica threads; a client never sees them. I rejected two alternatives.
  The first was a `nondet()` choice between "receive client" and "receive replica message": it
  creates spurious blocked runs, or forces the client first and loses behaviour, e.g. c1's Read
  ordered before c0's Write. The second was helper threads, which break "one process each".
  Paper: none. INTERFACE: "Messages among the replicas ... are yours to define".
- **D2 (FIFO channels).** Each replica message carries a per-(sender, receiver) sequence number,
  and the receiver delivers in that order, buffering early arrivals. Under `--model fifo` this
  never buffers. Under `bag` it gives back the paper's channel assumption, on which Alg. 2/3 rely
  (e.g. DecideAndGCUpTo(mack) assumes the accepts up to mack were received). Paper: "connected by
  a network that can lose, duplicate, and deliver messages out of order ... a fair loss model that
  allows the creation of FIFO channels between any pair of nodes."
- **D3 (when the leader starts an instance).** The leader starts it immediately on receiving
  REDIRECT_REQUEST, one instance per request (batching off), and several instances may be in
  flight. Alg. 2 shows no trigger for StartInstance; the text says "The leader, upon receiving a new
  request, starts a new instance by increasing the instance number".
- **D4 (IsQuorum argument).** The quorum test uses the incremented count `inst[ni].nacpts`, not the
  received `nacpts`. Alg. 2 line 22 writes "IsQuorum(nacpts)", while the text says "increasing the
  value of nacpts to indicate the node itself is accepting the value. If nacpts is greater than n/2
  ... its value can be decided" and "nodes in the second half of the chain (starting from the n/2th
  node) can decide instances as soon as they receive the accept message". The incremented count is
  the only reading under which `n1` decides (Fig. 3: "the leader and replica 2 form a quorum").
- **D5 (maxack monotone).** `maxack <- max(maxack, ni)`. Alg. 3 line 40 says "maxack ← ni". The
  leader's self-ACCEPT carries the `maxack` from when the instance started, and with that line as
  written it would lower `maxack` after an ACCEPT_ACK.
- **D6 (execution order).** Decide queues the value and SMRExecute runs over the contiguous
  decided prefix. Alg. 3 line 33 executes inside Decide, while Annex A.5 says "replicas only consider
  an operation as decided after learning the decided values for all previous instances". With D2
  the two coincide.
- **D7 (the tail's "accept ack" for local reads).** The tail counts the instance as acknowledged
  when it sends ACCEPT_ACK(ni) itself, because its own accept completes the set of accepts. Section 4
  says "Upon receiving the accept ack message to the consensus instance for which the operation
  depends on". The tail never receives that message, and waiting for a later `mack` would need a
  second instance per read, beyond the one no-op per read that INTERFACE allows. Other replicas
  count an instance as acknowledged through `maxack` (the leader by ACCEPT_ACK, `n1` by
  piggybacked `mack`).
- **D8 (the leader's "highest instance seen").** For the leader, the read's dependency is
  `max(highest_seen, maxacpt) + 1`, so instances it started but has not yet received from itself
  also count. Paper: "if the highest instance that the replica has seen so far is 6 (regardless of
  it being decided or not), the read operation will depend on instance 7."
- **D9 (who replies, and when).** The front replies when it executes the op itself. `n0` executes
  on ACCEPT_ACK; `n2` executes on the ACCEPT that reaches quorum. The reply is sent inside Decide,
  before Forward (line order of Alg. 2). Paper: "the replica knows that the received request has
  been decided, and can execute the request and return the result to the client" (there, the
  middle replica). INTERFACE gives the reply to the front.
- **D10 (no-op choice).** A no-op is offered only in `local`, by the leader, at the top of every
  loop iteration (before each blocking receive) while `pending` is empty. The budget is the number
  of Read commands in the whole run (`reads_in_run(q)`). The leader cannot know which reads are
  outstanding, because a local read sends nothing. `mixed` has no no-op. INTERFACE: "whenever the
  leader has no pending request it may start a no-op instance, at most one per outstanding client
  read in the whole run". Executions where the leader never takes the choice can leave a local
  read unanswered (the client stays blocked). This follows from the interface's "may".
- **D11 (self-sends).** The leader's REDIRECT to itself (Alg. 2 line 3 with csleader = self) and its
  ACCEPT to itself (line 11, "SEND(self, ...)") are real messages, not inline calls.
- **D12 (request identity).** A request is (kind, client, j, value), where j is counted by the
  front. A reply goes out when the executed op is in that front's `submitted`. Alg. 1 says
  "submitted : set of requests" and does not say how a decided value is matched to a request.
- **D13 (Alg. 3 line 38).** I read "DECIDE(ni)" inside the loop over instances i as DECIDE(i).
- **D14 (GC).** DecideAndGCUpTo removes `inst` entries up to mack after deciding them. Execution
  uses its own queue, so GC never drops an unexecuted value.
- **D15 (program edges).** `main` joins only the clients and `init`, because replicas never end.
  The optional third argument `fifo|bag` is accepted and ignored: the harness's `--model` selects
  the delivery model, and D2 makes the protocol independent of it. `spec` and `imp` both return
  this implementation.

## Build and the one run

Compile (inside this directory): `cargo build --release -j 2` finished with
`Finished \`release\` profile [optimized] target(s) in 1m 59s`, with no errors and no warnings.

Run (inside this directory): `timeout 600 target/release/bench_chainpaxos_paper count impl --model fifo -- 1 mixed`
exited 0. Its result line was `execs=0 blocked=14`. Every execution ends with the replicas
blocked on receive, by design. The last output line was `cache_hits: `, the end of the
measurement block.
