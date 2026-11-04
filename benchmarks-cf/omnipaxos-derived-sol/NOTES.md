# Replicated log implementation

Source: `java/bench/omnipaxos_derived_sol/OmniPaxosDerivedSol.java`.
The design uses only the supplied interface, Rust specification, and local API
sources. No paper or external source was consulted.

## Interface and execution

The public entry implements `MpProgram`, with a public `String...` constructor.
It accepts `impl,e,t,cand,fifo|bag`, or `impl,e,t,fifo|bag` with `cand=3`.
It validates positive `e` and `t`, `0 <= cand <= 3`, and bounds `e` so that
all client values fit in an `int`. There are exactly three nodes and two clients.
The program metadata name is `omnipaxos`; the size records `e,t,cand`.
Only `c0,c1` are visible. `fifo` maps to `P2P`; `bag` maps to `ASYN`.
Every send and receive uses that selected model.

Main constructs all five `MpThread`s, obtains their `Tid`s, and fills the address
directory before starting any thread. It starts `n0,n1,n2,c0,c1` in that order
and calls `join1` on each in that order. Construction before starting makes
addresses available without setup messages, polling, synchronization, or assuming
numeric task IDs. The directory is never mutated after starting; each process
copies it. Everything else that can change belongs to one process. Main performs
no sends or receives. Awaiting the first perpetual node can block main; JMC still
executes the other processes and ends an execution when no process can move.
Nodes have unconditional receive/election loops and no completion condition.

The only client message types are the immutable, tagged, single-field records
`Propose(int value)` and `Committed(int value)`. All internal records are tagged
too. List payloads defensively copy their contents. Process references in messages
are `Tid`s; ballots and slot indices are ordinary scalar protocol data.

## Ordering decision and clients

This implementation selects the fixed legal merge `1,2,...,2e`. This is a
deliberate restriction of the specification's permitted interleavings, not an
attempt to generate every allowed trace. Each client's values remain in its
sending order. A missing earlier value prevents later values being appended.
Waiting for the other client is permitted: the specification imposes no progress
guarantee and permits arbitrary proposal loss. The implementation does not
deliberately discard proposals or add a proposal-loss choice.

This choice also resolves an ambiguity of ordered external delivery over `bag`:
the unadorned `Committed` messages have no slot field or acknowledgement, and bag
delivery can otherwise reverse two sends. Since the chosen merge is known to
clients, each client selectively receives precisely the next integer. This uses
the supplied API's receive guard, rather than sharing the decided sequence or
introducing extra client messages. Out-of-order pending commits remain pending.
It preserves receive order as well as send order in both communication models.

Client `ci` first sends all `e` values `1+i+2j` to `ni`, in increasing `j`.
It then receives commits in the fixed log order, counting only its own parity,
and exits as soon as all its own values have been received once. Consequently
`c0` can exit before receiving the final even value; its gateway still sends that
commit. Client delivery traces are prefixes, as in the Rust client.

## Node state and elections

Every node combines proposer/candidate, acceptor, and learner roles. Initially it
has no leader, promise 0, no accepted slots, no learned prefix, and no offers.
Node `ni` has timeout budget `t` if `i < cand`, otherwise zero.

At each loop boundary a leaderless node with budget uses
`Nondet.chooseBoolean()` to choose timeout now or receive. Timeout spends exactly
one unit. Choosing receive performs a blocking, unrestricted receive; it does
not busy-wait or force an eventual timeout. A candidate still awaiting election
has no leader and may spend another timeout. With no budget, or with a known
leader, the node only receives. There is no heartbeat or periodic leader failure
detection: nodes never fail, and interface timeouts apply only without a leader.
In particular, declining the initial timeout may leave clients and nodes blocked.
No election or delivery fairness stronger than the interface is assumed.

A timeout abandons any old campaign and chooses
`ballot = 3*(floor(maxSeen/3)+1)+nodeIndex`. It immediately promises this ballot
locally, and sends `Prepare` to all three nodes, including itself. Ballots are
positive, globally unique across candidates, and increase beyond all observed
ballots. `long` arithmetic accommodates the finite budgets even at maximum `int`
parameter values. Two distinct acceptor identities constitute a majority.

Observing a strictly higher promise raises the local promise and maximum seen,
clears the known leader, and abandons any lower campaign, its quorum state and
its in-flight proposal. Accepted and learned data survive. A candidate activates
only after collecting two promises for its exact current ballot. Activation
sets itself as leader and announces `Elected`. A follower knows a leader only
from a nonstale `Elected` or `Accept`. A stale announcement never undoes a higher
promise. No extra election tie-breaking choice is needed: ballots totally order
candidates. False timeout choices can block; finite budgets provide no unconditional
liveness guarantee.

## Internal messages

* `Offer(value)`: a gateway receiving a client proposal remembers it locally and
  sends it once to each of the other two nodes. Receivers remember it in a set
  and do not forward it. Broadcasting availability lets any elected leader find
  client values, without retry/redirect cycles or reliance on the old leader.
  Gateway values are checked for range and owner. Bag arrival order is immaterial.
* `Prepare(ballot,candidate)`: an acceptor rejects a ballot below its promise.
  Otherwise it raises its promise if necessary and replies with a snapshot of
  every accepted slot. Self preparation uses the same message path as peer
  preparation, although the candidate has already promised its own ballot locally.
* `Promise(ballot,voter,slots)`: `slots` is an immutable list of
  `AcceptedSlot(index,ballot,value)` entries in index order. A campaign retains
  one response per voter. Stale responses, and additional responses after
  activation, are ignored. On a majority it selects the highest accepted ballot
  separately at every reported index. An equal-ballot conflict is an assertion
  failure. The merged indices must have no hole. The leader re-proposes this
  entire recovered sequence from index zero before extending it, even if some
  of these slots were already decided. It never treats a promise as an acceptance.
* `Elected(ballot,leader)`: sent to all three nodes after phase one; records the
  leader when its ballot is at least the local promise. A higher ballot also
  preempts a local campaign. This message alone never changes the log.
* `Accept(ballot,leader,index,value)`: the active leader has at most one index
  in flight. It first replays recovered slots; thereafter it proposes the next
  integer only if its offer is present. It sends to all three nodes including
  itself. An acceptor rejects ballots below its promise, otherwise raises its
  promise as needed, records this slot's accepted ballot and value, and replies.
  Acceptance checks that the fixed merge is respected and that an acceptor
  never accepts different values at one slot in one ballot. Slots are stored
  sparsely because bag delivery may bring later accepts before earlier ones.
* `Accepted(ballot,voter,index,value)`: counts once per distinct voter only for
  the current active ballot and exact in-flight slot/value. Two votes make that
  value chosen. The leader extends its campaign's contiguous chosen prefix,
  broadcasts `Decide`, and may then propose the next slot. Stale votes do nothing.
* `Nack(rejected,promised)`: replies to an obsolete prepare or accept. It raises
  maximum seen. If it rejects the current campaign with a higher promise, the
  node abandons the campaign and becomes leaderless. Other stale nacks cannot
  depose a new leader. There is no automatic retry outside timeout choices.
* `Decide(prefix)`: an immutable full prefix, sent by a leader only after quorum
  acceptance of its last slot. A learner checks overlap with its learned prefix,
  ignores an entirely redundant/shorter prefix, and learns each new entry in
  increasing index order. A longer message overtaking earlier decisions thus
  fills all missing entries. Decisions remain valid even from older ballots:
  learners do not filter them by their current promise. `n0` sends each newly
  learned value once to `c0`; `n1` sends it once to `c1`; `n2` has no client.
  Every node learns through decision messages, including a leader learning its
  own decisions, with a learner prefix distinct from campaign/quorum state.

After processing a message an active leader starts its next eligible slot if
none is outstanding. There is no application queue arbitration: the ascending
merge uniquely determines eligibility. Unrestricted node receives leave delivery
selection to JMC's receive operation under the selected model; there is no local
polling or hand-written scheduling choice. The only explicit nondeterministic
choice needed by this design is timeout versus receive. All other actions follow
the received message and deterministic protocol state.

## Safety and refinement reasoning

Promises are monotone. Each chosen slot has two distinct acceptors; every new
phase-one quorum intersects every old choosing quorum. Selecting the highest
accepted ballot at each index and reaccepting it therefore preserves chosen
values. Serial proposal/decision also ensures recovery has no hole: before any
accept for index k is sent, all indices below k have already been chosen, and
phase-one quorum intersection recovers those predecessors. Counting acknowledgments
by ballot, slot, value, and distinct voter prevents stale traffic from constructing
a quorum. Separate immutable snapshots prevent message payload mutation.

Only received offers or recovered accepted proposals can enter an accept round.
Offers originate only in actual client proposals. Recovery never invents a value.
Only quorum-accepted contiguous prefixes are announced, and learners never repeat
an index. Consequently every delivered value was proposed, every learned log is
a prefix of the same chosen sequence, and both visible clients receive prefixes
of that sequence. The chosen merge preserves each client's send order. The
specification can retain and decide exactly this sequence, or retain only the
observable prefix when an implementation execution blocks. Refinement here is a
safety argument; the requested run does not invoke a Rust/Java trace comparator.

## Compilation and the single requested run

Working directory for both commands is this directory. Compilation succeeded
with exit status 0 and no output:

```sh
javac -d java/out -cp "$(cat cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

The launcher was invoked exactly once, with stdout and stderr saved locally to
`run-output.txt`:

```sh
java -Xss4m -cp java/out:$(cat cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.omnipaxos_derived_sol.OmniPaxosDerivedSol:impl,1,1,1,fifo' must 0 1 report - 600 </dev/null >run-output.txt 2>&1
```

The single run exited with status 0 after 600.035 seconds of exploration.
It reported 60,913 iterations, all counted as blocked, and 60,913 distinct
blocked graphs; the exploration stack was **not exhausted**. Perpetual nodes
make even a served execution end blocked. The reported stop reason was
`timeout`, and the report's error field was
`Halting execution: 60913 due to timeout`. This is a bounded exploration, not
an exhaustive verification result. No protocol assertion failure was reported.
The counting invocation does not export summaries or perform specification
lookups; its zero visible-event/summary measurements are not a trace comparison.

Startup logging also printed a local-hostname `UnknownHostException` and a
logging configuration diagnostic (`Console contains an invalid element or
attribute "level"`); execution continued and produced the report above.

The last output line, copied verbatim from `run-output.txt`, was:

```text
max_porf_count: 0
```
