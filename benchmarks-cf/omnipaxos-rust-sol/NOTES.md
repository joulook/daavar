# Rust Sequence Paxos port

The implementation is `java/bench/omnipaxos_rust_sol/OmniPaxosRustSol.java`.
It ports the fixed-membership protocol in `omnipaxos-src/sequence_paxos/`,
`ballot_leader_election.rs`, `messages.rs`, `omni_paxos.rs`, and `util.rs`.
Reconfiguration, StopSigns, snapshots, compaction, UniCache, and flexible quorums
are omitted. Both prepare and accept quorums are two of three nodes.

## Roles and interface

- Main constructs all five `MpThread`s and their `Tid` address table before
  starting any of them. It starts `n0`, `n1`, `n2`, `c0`, `c1`, in that order,
  then calls `join1` on each in that order. It sends and receives nothing.
  Construction before starting makes every address available without a bootstrap
  protocol or shared protocol state. Joining a blocked node blocks main while
  the harness continues the other processes.
- `c0` and `c1` are the visible roles. Client `ci` sends all `e` values
  `1 + i + 2*j`, in increasing `j`, to `ni`, each in a separate
  `Propose(int value)`. It then receives `Committed(int value)` until every own
  value has appeared once, ignoring other values and duplicate own values for
  this count. It terminates at that point.
- `n0`, `n1`, `n2` are internal, never terminate, and each owns its protocol
  state. Their Rust node identifiers are respectively 1, 2, 3; 0 means no
  promised leader. These identifiers are protocol ballot/configuration numbers,
  not raw JMC task identifiers. Transport addresses are always `Tid`s.
- Nodes 1 and 2 send their respective client a `Committed` for every newly
  learned decided log position, including values proposed by the other client.
  Node 3 advances the same local delivery cursor without a client send. A cursor
  prevents repeat delivery of a position, rather than suppressing equal values.
  Delivery polls the decided suffix after every incoming-message handler or
  timeout, and sends positions in increasing order.

The public constructor accepts `impl,<e>,<t>,<cand>,<fifo|bag>`; `e,t >= 1` and
`1 <= cand <= 3`. All five arguments are explicit in this launcher entry (use
`cand=3` for the interface's default). `fifo` maps to `P2P`, `bag` to `ASYN`.
Every send and receive, including client traffic, uses this selected model.
`programName()` is `omnipaxos`; `size()` records `e`, `t`, and `cand`.
All transmitted records and nested record values have `@Tag` annotations.
Messages and their lists are immutable value objects.

## Ballot Leader Election

A ballot stores `(configId,n,priority,pid)`. The fixed configuration ID is 1;
priority is 0, matching the library default. Ordering compares `(n,priority,pid)`
lexicographically, excluding configuration ID as in the Rust comparator. The
initial BLE ballot is `(1,1,0,pid)`, the BLE leader is initially that ballot,
and happiness is true. Sequence Paxos initially has the all-zero promise and
accepted round, follower role, no phase, and an empty log.

Every node starts heartbeat round 1 by sending `HeartbeatRequest(from,round)`
to each peer, even if that node has no timeout budget. Each request receives a
`HeartbeatReply(round,ballot,leader,happy)` reporting the receiver's current BLE
state. A reply is retained only when its round equals the local heartbeat round
and its ballot's configuration ID is 1. As in Rust, replies are a vector, not a
set; transport does not duplicate messages.

Only nodes `n0` through `n(cand-1)` receive `t` timeout units. Before each receive,
a node with budget and no promised leader uses `Nondet.chooseBoolean()` to
choose between firing a timeout now and doing a blocking receive. Firing spends
one unit, and performs exactly one BLE `hb_timeout` (the library default election
timer period is one). Receiving preserves the budget. Choosing receive may
block even with unused budget; there is no implicit forced timeout, wall clock,
self-sent timer, or fairness assumption about the choice. After any receive the
choice is available again if its guard still holds. "No leader" means
`promise.pid == 0`, corresponding to `OmniPaxos::get_current_leader`, rather than
the BLE component's initial speculative self leader or happiness flag.

A BLE timeout performs these operations in the Rust order:

1. Raise the BLE leader to the largest ballot in this round's replies, if larger.
2. Recompute happiness. A self leader needs a majority counting itself and
   replies whose advertised leader is no larger, or evidence of a larger happy
   leader. A follower needs a happy reply whose ballot is its BLE leader.
3. If unhappy, quorum connected, and every replying neighbor is unhappy,
   raise the local ballot number to `bleLeader.n + 1`, become BLE leader, and
   become happy. The empty-reply case fails quorum connectivity.
4. Clear replies, increment the heartbeat round, and send fresh requests.
5. If the Paxos promise exceeds the BLE leader, adopt that promise, update the
   own ballot when its PID is local, and become happy.
6. If BLE leader equals the own ballot, invoke the Sequence Paxos leader handler.
   There is no extra majority gate on this final return: Rust may return its
   own ballot even when unhappy or no heartbeat replies arrived.

Heartbeat reception alone never elects a leader. A timeout can therefore fail
to elect anyone, or return the initial own ballot without increasing its number.
The interface restricts timeouts to nodes with no promised leader; this is the
explicit harness restriction even though an unconstrained Rust application
normally calls `tick` periodically forever.

## Sequence Paxos messages and transitions

Each integer is an entry; batching threshold is one, the library default.
Accepted index is log length; decided index is an exclusive prefix length.
The accepted round is updated by log synchronization. Logs and delivery cursors
are local, never shared across processes.

- **Leader notification:** Ignore a ballot no greater than either the saved
  leader-state ballot or the durable-promise abstraction. A locally owned new
  ballot resets leader metadata, promises it locally, inserts a self Promise,
  enters leader/Prepare, and sends Prepare to both peers. BLE in this library
  notifies only the node whose own ballot won. Followers discover leaders via
  Prepare; there is no invented leader-announcement broadcast.
- **Prepare `(n,decidedIdx,nAccepted,acceptedIdx)`:** Accept a greater ballot,
  or an equal one while recovering. Set the promise, enter follower/Prepare,
  and reset the receive sequence number. Reply with Promise metadata. If the
  follower's accepted round is greater, include its suffix starting at the
  sender's decided index; if equal but its log is longer, include the suffix
  starting at the sender's accepted index; otherwise include no suffix.
  Old/equal ordinary Prepares are ignored, exactly as in Rust.
- **Promise `(from,n,nAccepted,decidedIdx,acceptedIdx,logSync)`:** Only a leader
  in Prepare or Accept with matching ballot handles it. During Prepare, save
  metadata per sender, select the greatest accepted round and then longest log
  (ties keep the existing maximum), and count distinct promised senders including
  self. At two promises, apply the selected optional suffix, adopt the maximum
  promised decided index and new accepted round, append buffered proposals,
  enter leader/Accept, and send AcceptSync to each promised follower. In Accept,
  a late Promise updates that follower's metadata and triggers its AcceptSync;
  it does not replace the Prepare quorum's selected maximum.
- **AcceptSync `(n,seqNum,decidedIdx,logSync)`:** Send only to promised followers.
  Determine their common prefix from Promise metadata: their accepted index if
  they already accepted the current ballot; the minimum of their index and the
  selected maximum's index if their accepted round matches that maximum;
  otherwise their decided index. Send the leader's suffix from this prefix.
  Start a new per-follower sequence session and use counter 1. A receiver with
  matching promise in follower/Prepare replaces that suffix, adopts accepted
  round and decided index, forwards buffered proposals, enters follower/Accept,
  installs the supplied sequence number, and sends Accepted for its log length.
  AcceptSync itself does not require an expected-next sequence number, matching
  the Rust handler.
- **Propose / ProposalForward `(entries)`:** In leader/Prepare, buffer entries;
  in leader/Accept, append entries locally, update the own accepted index, and
  send AcceptDecide to every promised follower. Otherwise forward entries to
  the promised leader if it is another node, or buffer if no such node exists.
  Forwarded proposals use the same routing rule and may be forwarded again.
  Buffering is not deduplicated by value, client, or request ID. A follower
  drains its buffer only upon AcceptSync; a leader drains it when its Prepare
  quorum synchronizes. Each client sends one entry; a forwarded buffered vector
  or an AcceptSync suffix can contain multiple entries, each retaining its own
  log position.
- **AcceptDecide `(n,seqNum,decidedIdx,entries)`:** For a valid ballot in
  follower/Accept with the expected sequence number, append the entries, advance
  the decided prefix if greater (bounded by accepted length), and reply Accepted
  with the new accepted length. A merely accepted entry is not delivered.
- **Accepted `(from,n,acceptedIdx)`:** Only leader/Accept in the matching ballot
  handles it. Assign the sender's accepted index, including a lower reordered
  acknowledgement as Rust does. If this message's index exceeds the local
  decided index and at least two accepted indices cover it, decide that prefix
  locally and send Decide to every currently promised follower.
- **Decide `(n,seqNum,decidedIdx)`:** With valid ballot, Accept phase, and expected
  sequence number, monotonically advance the decided index, bounded by local
  accepted length. No Accepted reply is needed for a pure Decide with batching
  disabled. This is how a follower learns decisions not already piggybacked on
  AcceptDecide or carried by AcceptSync.
- **NotAccepted `(from,n)`:** A leader receiving a ballot greater than its leader
  ballot removes that sender from the promised follower set. Existing accepted
  indices remain, as in Rust. Internally null denotes both non-promised and
  promised-higher metadata because the resend timer (which distinguishes these
  cases) is outside the interface; neither is counted or sent accept traffic.
- **PrepareReq `(from,n)`:** A leader whose ballot is at least the request ballot
  clears the sender's Promise metadata and sends a fresh Prepare. Accepted
  indices and sequence sessions are retained. This supports sequence recovery
  even without node crashes.

## Sequence ordering and integration decisions

Each leader has independent `(session,counter)` sequence numbers for each
follower. AcceptSync increments session and starts at counter 1. Each later
AcceptDecide or Decide increments that follower's counter. An expected message
has the same session and the next counter; an older/equal pair is ignored;
a greater unexpected pair triggers `reconnected`: enter follower/Recover when
that sender is the promised leader, and send it PrepareReq with the current
promise. The gap-causing message is discarded, not buffered or applied out of
order. In particular `bag` delivery uses Rust's recovery, not an invented
reordering buffer. Messages received before Accept phase are handled according
to their phase guards, not selectively held until their phase becomes suitable.

For accept-family messages, compare the message ballot with the promise before
phase and sequence checks. Equality passes. A smaller message ballot receives
NotAccepted with the current promise; a larger one triggers `reconnected` and
PrepareReq. This preserves Rust's rejection and recovery behavior.

The library expects an application to drain outgoing buffers and read decided
entries. Here outgoing messages are sent immediately during each handler, in
Rust iteration order (peers in increasing PID), and the application polls after
every completed handler/timeout. Thus no outgoing message remains queued across
calls for the library's optional AcceptDecide/Accepted coalescing optimization.
Separate Decide messages are sent when decisions advance; decisions are also
piggybacked on subsequent AcceptDecide messages. Every exchange uses
`Channel.send` and `Channel.recv`. Incoming-message selection is delegated to
the API receive operation and chosen communication model; there is no extra
priority among heartbeat, client, and Paxos messages. The only additional open
local choice is timeout versus receive, implemented with `Nondet`.

Storage code is not included in the supplied source subset. With no failures,
compaction, or batching, storage is represented by a local list, promise,
accepted round, and indices. A LogSync replaces the suffix at its `syncIdx`
(including truncation for an empty suffix), preserves the prefix, sets accepted
round and decided index, and checks that the decided prefix exists and has not
fallen behind delivered entries. An optional LogSync is encoded as an immutable
zero/one-element list rather than a null payload. Java long counters represent
the Rust unsigned counters over the finite explored executions; wraparound is
not modeled. No client completion acknowledgements, monitor processes, quorum
shortcuts, proposal retries, or direct client success replies are added.

The interface supplies only bounded BLE election ticks. Independent periodic
resend and batch-flush timers are not introduced: messages are reliable and
batching is disabled. Promise retransmission caches and the UI-only previous
heartbeat vector are therefore unnecessary. Explicit PrepareReq/Prepare
recovery is retained. A finite timeout budget can leave an execution without
a leader or unfinished clients, and asynchronous sequence recovery can delay
progress; no extra liveness mechanism is added. Nodes always continue their
receive loops, even after clients terminate or their timeout budgets run out.

## Validation

Compile, from this directory:

```sh
javac -d java/out -cp "$(cat cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

Compilation succeeded (exit 0). The JVM printed an incidental hsperfdata lock
warning; there were no compiler errors.

One launcher invocation, from this directory:

```sh
java -Xss4m -cp java/out:$(cat cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.omnipaxos_rust_sol.OmniPaxosRustSol:impl,1,1,1,fifo' must 0 1 report - 600 </dev/null
```

The single invocation exited with status 0 after the checker's 600-second
limit. It reported 22,355 iterations, all counted as blocked, zero complete
graphs, and 22,355 distinct blocked graphs; the exploration stack was not
exhausted. Its measurement block says `stop_reason: timeout`. Nodes are
intentionally perpetual receivers, so blocked executions are expected; the
bounded run is not an exhaustive verification result. The launcher also
reported startup hostname-resolution and logging-configuration diagnostics,
then continued model checking.

Exact last output line:

```text
max_porf_count: 0
```
