# ChainPaxos, fixed chain and one register

Source: `java/bench/chainpaxos_paper_sol/ChainpaxosPaperSol.java`.
The implementation uses only the provided JMC message-passing API for process
communication and nondeterministic choices. There are five named processes:
`n0`, `n1`, `n2`, `c0`, `c1`. Only the clients are visible. All sends and receives
use P2P for `fifo`, ASYN for `bag`. There are no failures, elections,
reconfigurations, retries, or application-level duplicate messages.

## Roles and state

Each replica has Algorithm 1's fixed `chain`, `self`, `cnextok`, `csleader`, empty
`marked`, prepare number `npleader = 0`, instance map `inst`, `submitted`,
`pending`, `maxack`, `maxacpt`, and `amLeader`. Instance state contains `na`,
`val`, `nacpts`, and `decided`. Instance numbers start at 1; watermarks and the
register start at 0. Extra state records the received-instance high watermark,
the contiguous accepted and executed prefixes, reordered incoming ACCEPTs,
outstanding local reads, and per-client request sequence numbers. All mutable
protocol state belongs to one replica process.

`n0` is the permanent leader and c0's front. It redirects even its own client's
ordered requests through a self-message, starts each instance by sending itself
ACCEPT, accepts as the first replica, and forwards to n1. Its own acceptance is
not a decision. It decides and executes on the tail's ACCEPT_ACK, or on the
equivalent information carried in maxack. It replies to c0 only after its own
execution. It also runs the bounded optional no-op timer in `local`.

`n1` accepts second. Its own acceptance brings the accumulated count to two,
so it decides and executes the instance and forwards ACCEPT to n2. It has no
connected client and sends no application reply. It retains decided instance
state until a later ACCEPT carries a sufficiently large maxack.

`n2` is the tail and c1's front. It accepts third, decides and executes, and
returns ACCEPT_ACK directly to n0. It replies to c1 on its own execution,
without waiting for n0 to receive the tail acknowledgment. It retains instance
state until the piggybacked maxack permits GC.

Each client issues exactly the q sequential commands specified by INTERFACE.md,
sending one Write, Read, or Rmw and receiving one Ack, Value, or Result before
its next command. Request and reply records have exactly the required tags and
integer fields. Each replica executes every ordered command against its own
register, in increasing instance order. Write assigns x, Rmw adds 10 and returns
the new x, ordered Read returns x, and NoOp changes nothing.

Main constructs all five MpThreads in replica-then-client order, fills their
address configuration before starting any of them, then starts them in that
order. Addresses come from the constructed API threads' task IDs; no task-ID
offset is assumed. The configuration is never mutated after startup. Main
performs no send, receive, or nondeterministic choice. It awaits both clients
and then the replicas with `join1`. Replica bodies are infinite receive loops;
JMC's blocked-receive control flow is allowed to propagate.

## Messages and handlers

* `Write(client,value)`, `Read(client)`, `Rmw(client)` travel only from a client
  to its connected replica. The front gives each command a monotonically
  increasing per-client sequence number and records the internal Request in
  `submitted`. This keeps repeated Read/Rmw commands distinct without changing
  any external message. For ordered commands it sends REDIRECT_REQUEST to n0.
* `REDIRECT_REQUEST(Request)` adds the request to the leader's pending set.
  Batching is disabled, so the leader immediately removes that single request
  and starts one instance for it. Arrival order determines instance assignment.
* `ACCEPT(instance,leader,prepare,value,accepts,maxack)` is initially sent by
  n0 to itself with accepts zero. A receiver stores the value and increments
  the count to include its own acceptance. At count two or three it decides
  and executes the instance. It processes the carried maxack via
  DecideAndGCUpTo and forwards the value, accumulated count, and its current
  maxack. No replica sends a separate decision broadcast.
* `ACCEPT_ACK(instance)` travels only from n2 to n0. Because the tail handles
  instances in increasing order, it certifies acceptance by all three replicas
  of the entire prefix through that instance. The leader calls DecideAndGCUpTo.
* `DecideAndGCUpTo(k)` monotonically records the all-replica acknowledgment
  watermark. It visits accepted instances through k in order, decides and
  executes any not already decided, and removes only executed instance state.
  Its `maxack` is piggybacked on future ACCEPTs. A replica never invents a
  separate maxack dissemination message to flush the last instances.
* `Ack()`, `Value(value)`, and `Result(value)` travel only from a front to its
  connected client. For an ordered command, only the replica whose submitted
  set contains that Request replies, after locally executing it. Other replicas
  still execute the same command, including Rmw, but discard its reply.

## Reads and timer

In `mixed`, Read follows exactly the same redirect, ACCEPT, decision, execution,
and front-reply path as Write and Rmw. It consumes one instance.

In `local`, the front keeps Read local, with dependency `highestSeen + 1`, where
highestSeen includes ACCEPTs received but not yet decided or processed out of
order. It neither redirects the read nor allocates an instance to it. A read
waits for the dependency's full-chain acceptance evidence and local execution
through that dependency, then returns the current local committed x. At n0
this evidence is the tail's ACCEPT_ACK; at n2 it is the incoming ACCEPT together
with the tail's own acceptance, which yields all three accept acks. At n1,
were there a connected client, it would be the piggybacked maxack. A majority
decision alone is insufficient to release a local read. Completed reads leave
the submitted and waiting-read sets.

Timer-budget interpretation: the implementation uses a whole-run budget equal
to the number of Read commands in the fixed client workload. Each such command
provides one possible no-op credit; credits are never replenished. Whenever the
leader has no pending request and has credit, `Nondet.chooseBoolean()` chooses
between starting one NoOp instance and proceeding to receive. It may choose
again after a subsequent message, or after starting a no-op. In `mixed` the
budget is zero. All no-ops use the ordinary self-ACCEPT/chain/ACK path.

This interpretation preserves the paper's message-free read registration, but
does not test whether a remote Read is currently outstanding when a credit is
spent. A credit can be spent before that Read arrives, and bounded executions
can end with an unsatisfied read. A stricter dynamic interpretation of “at most
one per outstanding client read” needs remote read bookkeeping or a harness
oracle, neither present in the specified API/protocol. This ambiguity was raised
to the user; absent an answer, the static whole-workload interpretation is used.
The implementation does not claim to enforce the stronger dynamic bound.

## Decisions and the source text leaving them open

1. **Request identity.** The paper says “submitted stores requests received from
   clients and not yet decided,” but does not define request identity. Internal
   identity is `(client, sequence, operation)`; the front assigns the sequence.
   NoOp uses client -1 and its instance number as sequence. NoOp is never sent
   to a client.
2. **Initial values.** “The leader is the first to handle the accept message of
   each instance, as it starts a new instance by sending the accept to itself.”
   The excerpt does not give numeric initial instance or prepare values. Use
   zero watermarks and prepare, start instances at one, and preinstall the fixed
   leader without a Phase 1 exchange, as required by the task.
3. **Start scheduling and batching.** “The leader, upon receiving a new request,
   starts a new instance by increasing the instance number and generating a new
   accept message.” Algorithm 2 does not otherwise schedule StartInstance.
   With batching off, perform it immediately in the redirect handler; no
   arbitrary pending-set ordering choice remains. Replica event ordering comes
   from the API receive operation.
4. **Quorum counter.** “In the example of Figure 3, with three replicas, the
   leader and replica 2 form a quorum, with replica 2 replying to the client.”
   Algorithm 2 line 17 stores nacpts + 1 but line 22 tests nacpts. Test the
   updated stored count, so n1 and n2 decide on ACCEPT, consistent with this
   sentence and Figure 3. Testing the incoming count would delay the quorum
   decision by one replica.
5. **DecideAndGCUpTo loop typo.** Algorithm 3 says “for i ∈ inst ∧ i ≤ ni” and
   calls “DECIDE(ni)” inside that loop. Its prose says it traverses the instances,
   “marking them as decided (if they were not yet), and garbage-collecting the
   information about them after their execution.” Therefore call Decide(i)
   for each iterated instance. Keep maxack monotonic if an older message arrives.
6. **Ordered execution and bag transport.** “Replicas start in the same initial
   state and apply the same sequence of operations.” Also, “Nodes communicate
   by exchanging messages over a network with a fair loss model that allows the
   creation of FIFO channels between any pair of nodes.” The excerpt gives no
   implementation of FIFO or execution buffering. Store out-of-order ACCEPTs
   and process them in consecutive instance order even under `bag`; update
   highestSeen as soon as an ACCEPT arrives. Execute only the decided prefix.
   Defer acknowledgment-driven GC until the corresponding accepts are present.
   No retries or sequence-repair messages are needed with eventual delivery.
7. **Who replies.** Section 3's “the replica knows that the received request has
   been decided, and can execute the request and return the result to the client”
   illustrates quorum-replica latency but does not specify front ownership.
   The later sentence is explicit: “Each replica includes: an application ...
   which receives client requests, submits them for ordering, and replies to the
   client when the operation is executed.” Follow that sentence and INTERFACE.md:
   all replicas execute on their own decision; only the connected front replies
   after its own execution. No quorum-replica result is relayed to a lagging front.
8. **Meaning of local-read acknowledgment away from n0.** Section 4 says “Upon
   receiving the accept ack message to the consensus instance for which the
   operation depends on, the read operation is performed locally in the local
   committed state and the reply is sent to the client.” Algorithm 2 only sends
   an explicit ACCEPT_ACK to the leader. The bridge is section 3: “It also
   encodes the accept ack messages of all nodes in the chain for all instances
   up to mack.” Use all-replica evidence encoded in ACCEPT at the tail, explicit
   ACCEPT_ACK at the head, and mack at an intermediate replica. There is no
   added read-roundtrip message. The tail does not advance maxack merely because
   it sent ACCEPT_ACK; its GC still waits for the piggybacked watermark.
9. **Read release point.** Section 4 specifies “the read operation is performed
   locally in the local committed state.” Execute the instance establishing the
   dependency before replying to waiting reads; return the current x rather
   than a saved value from read arrival. Multiple eligible reads, if any, are
   served in arrival order during the same handler.
10. **Timer abstraction and bound.** “The head of the chain issues periodic NoOP
    operations if no write is received.” The paper supplies neither timer code
    nor finite bounds, and says read registration sends “no information ... to
    other nodes.” INTERFACE.md replaces the timer with a bounded nondeterministic
    choice. The static interpretation and its limitation are stated above;
    there is no wall clock, hidden shared read counter, or extra process.
11. **No timeout fallback.** The paper says “Alternatively ... a replica ... can
    forward the read to the leader to be executed as a normal operation.” Use
    the primary local-read algorithm, without this optional timeout alternative.
12. **No final flush.** “The leader executes DecideAndGCUpTo, increasing its
    maxack which leads subsequent accept messages to trigger DecideAndGCUpTo in
    every node across the chain.” There need not be a subsequent ACCEPT after
    the workload. Leave the final uncollected states at replicas; do not add
    DECIDED broadcasts, mixed-variant no-ops, or termination messages.
13. **Process setup.** INTERFACE.md says “Assume every process knows every other
    process's address and the chain order from the start.” The paper leaves
    initialization outside the algorithm. Construct API threads and publish
    immutable address lists before starting them, avoiding a setup process or
    any main-thread messages.

## Validation

Compiled inside this directory, exit status 0:

```sh
javac -d java/out -cp "$(cat cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

Ran exactly once, inside this directory, exit status 0:

```sh
java -Xss4m -cp java/out:$(cat cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.chainpaxos_paper_sol.ChainpaxosPaperSol:impl,1,mixed,fifo' must 0 1 report - 600 </dev/null
```

The run explored 14 iterations and reported
`graphs:     0 distinct complete, 14 distinct blocked; stack exhausted`.
The permanent replica receive loops make blocked executions expected. This
run does not exercise `local`, larger q, or `bag`; these paths were inspected,
not run. The launcher emitted a logging-configuration diagnostic at startup
(`Console contains an invalid element or attribute "level"`) and still exited 0.

The run's last output line was:

```text
max_porf_count: 0
```
