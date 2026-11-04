# Pineapple single-key implementation

Source: `java/bench/pineapple_paper_sol/PineapplePaperSol.java`.
Only the supplied `PAPER-EXCERPT.md`, `INTERFACE.md`, and local API sources
were consulted. This implements section 3, Algorithms 1 and 2, with N=3,
majority=2, stable leader n0, no failures, and one integer key initially 0.
No leader election, reconfiguration, or multi-key extension is included.

## Roles and lifecycle

- `c0` and `c1` are the only visible processes. Client i sends to ni,
  sequentially, for j=0..q-1. Kind `(i+j)%3` chooses Write, Read, Rmw;
  writes contain `1+i+2*j`. Each client receives and checks its reply's
  type before continuing, then terminates after q replies.
- `n0`, `n1`, `n2` each own a replica value and pstamp, and act as proxies
  for their attached clients. n0 also coordinates all RMWs. A node's event
  loop always receives, including after clients finish. Waiting operations
  are local state machines; replica handlers keep running during waits.
- Main constructs all five API `MpThread`s and their address configuration
  before starting them, starts nodes in index order and then clients in
  index order, and waits for clients followed by nodes with `join1`.
  A blocked node wait blocks main; the harness ends that execution.
  Main sends/receives no messages. Configuration arrays are filled before
  any thread starts and never mutated afterwards; no protocol state is
  shared between processes. Each Node is created inside its own thread.
- Entry: public `PineapplePaperSol(String[] args)`, as required by the local
  launcher's reflection interface. It accepts exactly `impl,<q>,<fifo|bag>`
  with q>=1. `fifo` is API P2P; `bag` is API ASYN. All sends and receives use
  that selected model. Metadata name is `pineapple`, size is q in decimal.

## Client messages

All are immutable public records implementing `MpMessage`, annotated with
the exact tags and field order required by INTERFACE.md:

| Message | Fields | Route and meaning |
| --- | --- | --- |
| Write | int client, int value | ci -> ni; start ABD write |
| Read | int client | ci -> ni; start ABD read |
| Rmw | int client | ci -> ni; request f(v)=v+10 |
| Ack | none | ni -> ci after write put majority |
| Value | int value | ni -> ci after read writeback majority |
| Result | int value | ni -> ci, new value produced by its RMW |

## Replica and proxy messages

The single key is implicit. `request` is a monotonically increasing local
long at the coordinating node; `coordinator` and `replica` are node indices.
Transport does not supply the sender to the receive caller, so return
addresses and replica identities are explicit payload fields. All internal
records also carry matching `@Tag` annotations.

| Message | Fields | Handling |
| --- | --- | --- |
| GetTag | coordinator, request | Recipient sends its stored pstamp to coordinator |
| AckTag | request, replica, ps | Write coordinator collects get responses |
| GetValue | coordinator, request | Recipient sends stored value and pstamp |
| AckValue | request, replica, ps, value | Read or RMW coordinator collects get responses |
| Put | coordinator, request, ps, value | Replace storage only if ps is strictly greater; always acknowledge |
| AckPut | request, replica | Count distinct replicas for this request's put phase |
| ForwardRmw | proxy, client, request | Non-leader proxy -> n0; enqueue client RMW |
| RmwDone | request, client, value | n0 -> originating proxy after put majority; proxy sends Result to client |

`Pstamp(long ts, int writer, long slot)` flattens ((ts,writer),slot).
Comparison is lexicographic in precisely that order. All replica updates,
including read writeback and RMW proposals, use Algorithm 1's strict
comparison. Equal or older PUTs still receive acknowledgements.

An ABD write broadcasts GetTag to all three nodes, including itself. After
the first two distinct replies, it chooses the maximum full pstamp,
constructs ((max.ts+1,proxyID),max.slot), broadcasts Put with the client's
value, and returns Ack after two distinct AckPut replies. It does not skip
the get phase or shortcut the put phase.

An ABD read broadcasts GetValue to all three. After two distinct replies,
it selects the value accompanying the maximum full pstamp, broadcasts that
exact (value,pstamp) with Put, and returns Value after two AckPut replies.

n0 queues RMWs in receive order and runs one at a time. It broadcasts
GetValue to all three. Its quorum is exactly n0 plus the first remote
response received. A second remote response is ignored for quorum selection.
After both quorum replies, it computes vnew=vmax+10, increments its local
RMW slot counter, creates (psmax.tag,newSlot), and broadcasts Put to all.
It replies after the first two distinct AckPut replies, directly to c0 for
a local request or through RmwDone and the attached proxy for a remote one.
It waits for its own AckPut as well before starting the next queued RMW.
The latter wait ensures that the leader's next self get observes the prior
RMW or a higher pstamp, even when the completion quorum excludes n0.
ABD operations and background messages can progress throughout this wait.

## Decisions where the excerpt leaves details open

1. **Initial metadata.** The paper says storage keeps “the current value for
   key key” and “the associated pstamp,” but gives no initial pstamp.
   Use value 0 (fixed by INTERFACE.md), tag (0,-1), and slot 0. Writer IDs
   are node indices 0..2. Slot 0 is an initialization sentinel; generated
   RMW slots are positive, consistent with “slot is a positive integer.”

2. **Selecting quorums.** Algorithm 1 says “wait until receiving ... from
   quorum Q,” without a selection rule. Use the first two distinct
   responses received, independently in each phase. Algorithm 2 requires
   “Contact a quorum Q, which includes the leader itself”; use n0 plus
   the first remote response. No fixed remote node is preferred. All
   broadcasts include the sender using actual send/receive operations.

3. **RMW scheduling and local acceptance.** The paper says “First, RMWs go
   through the leader; hence the ordering is stable” and “the leader has
   the most recent information about prior RMWs,” but omits event-loop,
   pipelining, and self-message timing details. Serialize RMWs in arrival
   order through put completion and self acceptance. Client completion
   still uses any majority, exactly as “wait for an acknowledgement from
   a quorum” specifies. Before starting another RMW, additionally wait
   for the previous self AckPut. Self delivery thus implements local
   acceptance without a shared-memory shortcut or a stronger client
   completion quorum. Increment the leader-local slot once per RMW;
   ABD write/read handlers never change that counter. With one stable
   leader, every observed slot originated at this leader and is at most
   its counter, so the new RMW pstamp is strictly above psmax.

4. **Forwarding and returning RMWs.** “RMW takes 3-RTT if the client is not
   co-located with the leader” does not specify forwarding payloads or
   the return route. The non-leader retains its client's identity under
   a local request ID and sends ForwardRmw. The leader retains the proxy,
   client, and original ID separately from its coordinator ID, sends
   RmwDone to the proxy, and the proxy sends Result to its own client.
   No client is contacted by an unattached node.

5. **Correlation and late replies.** The algorithm's messages include a key
   but no invocation identifier: “send ... GET-TAG, key ... to all.”
   Since this is one fixed key, omit the key and add coordinator-local
   request IDs, replica IDs, and return coordinator indices. Track
   distinct responders separately in each phase. Ignore late get replies
   after starting put, and replies to completed operations. IDs are never
   reused within a run; forwarding IDs share the originating node's
   counter, so they cannot collide with its coordinator operations.

6. **Equal maximum pstamps.** “vmax <- value associated with psmax” does
   not define tie breaking. Equal pstamps have equal values: writes have
   unique tags (one sequential client per proxy), RMWs have unique slots,
   and reads only copy pairs. Select the lowest replica index on an equal
   maximum; there is consequently no choice of different values here.

7. **Fast paths.** “ABD can complete on the fast path when the quorum
   returns the same response” and “Pineapple also adopts various
   optimizations” do not give a complete fast-path algorithm. Implement
   the explicit two phases of Algorithm 1 for every read and write, as
   requested. No optimization or additional condition is inferred.

8. **Process setup, serialization, and choices.** The excerpt presents
   “Background Handlers at node Ni” without a threading or wire format.
   Use five API processes with immutable record messages and one receive
   loop per node. All state changes are process local. Construction-time
   address configuration implements the interface's assumption that all
   addresses are known initially. Fixed broadcast order is node index
   order, as a local loop over “send ... to all.” Operation and quorum
   selection follow message arrival order. There is no residual protocol
   alternative requiring a `Nondet` choice; no JVM randomness, time,
   polling, or shared-state communication is used. Internal numeric
   metadata uses long; client values use the interface's required int.
   Arithmetic uses Java's integer arithmetic; the supplied one-run
   workload has no arithmetic boundary issue.

## Compilation and requested single run

Commands executed from this directory:

```sh
javac -d java/out -cp "$(cat cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
java -Xss4m -cp java/out:$(cat cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.pineapple_paper_sol.PineapplePaperSol:impl,1,fifo' must 0 1 report - 600 </dev/null
```

Compilation succeeded with exit status 0. The launcher is invoked only once.
The specified q=1 workload contains c0's Write(1) and c1's Read, and contains
no RMW; this run therefore does not dynamically validate the RMW path.

The single run exited with status 0 after 600.054 seconds of exploration.
It stopped at the configured timeout, with 88,423 iterations, all classified
as blocked, 0 completed, and the exploration stack not exhausted. Blocked
executions are expected because the nodes never terminate and main waits
for them. No protocol assertion or process exception was reported; timeout
is not an exhaustive correctness result. The launcher printed
`error: Halting execution: 88423 due to timeout` and
`stop_reason: timeout`. Startup also printed the logging configuration
diagnostic `Console contains an invalid element or attribute "level"`;
exploration continued normally afterwards.

The run's last output line, verbatim:

```text
max_porf_count: 0
```
