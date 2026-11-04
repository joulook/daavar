# Replicated log decisions

The implementation is `java/bench/omnipaxos_spec_sol/OmniPaxosSpecSol.java`.
It was designed from SPEC.md, INTERFACE.md, and the supplied API sources. No paper,
web source, or source outside this directory was consulted.

## Entry, processes, and transport

The public `(String...)` constructor accepts `impl,e,t,cand,fifo|bag`;
`cand` defaults to 3 and the delivery model to fifo when omitted. It rejects
invalid parameters, permits cand=0, and bounds e so all integer client values fit.
The program declares n0,n1,n2,c0,c1 in that order and only c0,c1 as visible.
The main task constructs all five MpThreads and their Tid addresses before
starting any thread, starts them in the required order, and awaits them with
join1. It never sends or receives. Each node owns its mutable state; the only
shared object is the address directory, populated before any thread starts.
The directory is never subsequently changed. No daemon, timer thread, shared
protocol state, monitor process, or extra initialization message is used.

`fifo` means P2P; `bag` means ASYN for proposals and all inter-node traffic.
Committed sends and client receives always use P2P. This resolves a transport
ambiguity in favor of SPEC.md's explicit FIFO client notification stream:
unrestricted bag delivery of the fixed, unnumbered Committed messages cannot
preserve the required observed prefix order. The MpProgram.model() reports the
selected replication transport; the API permits mixed communication models.
With bag proposal delivery, nodes explicitly restore each client's proposal
order using the integer value's encoded ordinal. All node receives use the
selected transport, with an unrestricted guard; message selection belongs to
the API receive operation. There is no application queue-selection choice.

Clients send all e Propose(value) records to their designated nodes, in order,
then receive Committed(value) records. They count each of their own values once,
ignore the other client's values for termination, and terminate after all their
values have been seen. The message records have exactly the required single
integer field and the tags Propose and Committed. Nodes never terminate: their
outer loop always continues to an election choice or a blocking receive.

## Election and acceptor role (every node)

The group is fixed at three and every quorum is two distinct nodes. State is
in-memory because no node fails. Ballot 0 is the initial sentinel. A timeout
chooses a ballot `(promised / 3 + 1) * 3 + id + 1`, strictly above the node's
promise, with a unique residue for its owner. Ballots use long arithmetic.
The timeout budget is t only at nodes with id < cand; all other budgets are zero.
At the beginning of each loop iteration, a node with no known leader and budget
remaining calls Nondet.chooseBoolean(). True spends one timeout and starts or
restarts an election; false proceeds to blocking receive. Consecutive timeouts
are permitted while candidacy has not established a leader. Declining a timeout
and blocking with unused budget is permitted; this is a finite-budget,
asynchronous safety implementation, not a guarantee that an election occurs.
There are no heartbeats or timeouts while a node knows a leader.

Starting an election raises the local promise, clears old leader/candidate
activity, counts the local promise, snapshots the local accepted state, and
sends Prepare to the other two nodes. Self votes are local transitions, not
messages between processes. Higher Prepare messages preempt candidates and
leaders and make the leader unknown. Lower requests receive Reject. A node
promises a ballot before replying and never lowers its promise.

An acceptor stores an accepted ballot and an entire immutable sequence. Accept
at or above its promise updates the promise if necessary, records the sequence,
and replies Accepted. Within one ballot, sequences must extend each other;
a delayed shorter Accept is acknowledged with the current longer length and
cannot truncate state. A higher ballot may replace an unchosen suffix.
Acceptor state and learned state are separate: a delayed lower-ballot Accept
may arrive after a later decision was learned, before the corresponding higher
Prepare has arrived. Learning does not manufacture an election promise.

## Candidate and leader role

A candidate collects one Promise per distinct responder for its current ballot.
The local promise counts toward the majority. On the first majority it selects
the response with greatest accepted ballot, breaking equal-ballot ties by
longest sequence. All sequences accepted at one ballot are prefix-comparable.
The candidate becomes leader and reaccepts that entire recovered sequence at
its new ballot, even if empty. This recovery round must finish before any fresh
append. A later Promise is ignored. A higher Reject preempts the campaign;
old replies cannot reactivate it.

The leader counts its own acceptance and sends the full sequence in Accept to
the other nodes. It permits only one outstanding replication round. Two distinct
Accepted replies for the current ballot and exact current length decide the
whole proposed prefix. The leader learns locally, broadcasts Decide, and then
tries the next append. Full snapshots favor clarity over bandwidth efficiency;
there is no pipelining, compaction, retransmission timer, or per-index hole.
An empty recovery can establish leadership without generating client output.

Each node retains received requests in insertion order. A direct Propose is
forwarded immediately if a remote leader is known. Accept doubles as the leader
announcement: discovering a different leader forwards all locally retained,
unlearned requests to it. Forward receivers retain requests but do not forward
them again, preventing cycles among obsolete leaders. Origins retain requests
across leadership changes. Forward has no ballot because the unique client
value remains a valid request under any leader. Duplicates are suppressed by
value, which is a globally unique request identifier for this interface.

The active leader scans its insertion-ordered pending values and selects the
first eligible value not already in its proposed sequence. Values 1 and 2 are
eligible immediately; every subsequent value v requires v-2 earlier in the
sequence. Thus reordered proposals and forwards cannot reverse one client's
values. Ineligible requests wait without a busy loop. The leader appends exactly
one fresh value at the next index and runs phase two. Recovered values already
occupy their original indices. Requests absent from the phase-one recovery can
be proposed again through the retained pending set, but never duplicated within
a sequence. Arrival order plus this deterministic eligibility rule resolves
request ordering; there is no additional free ordering choice.

## Learner role (every node)

Decide carries the full chosen sequence, so it can arrive before Accept or
Prepare and still supply every missing value. It remains valid after a higher
promise. A learner checks prefix compatibility, ignores an equal/shorter
already-known decision, and extends its learned prefix once. n0 sends one
Committed per new entry to c0; n1 does so to c1; n2 learns without client output.
All three learn the complete prefix in index order. Assertions check conflicting
learned prefixes, conflicting same-ballot accepts, duplicate values, and reversed
client proposal order. There is no early return from a node after client work.

## Inter-node message inventory

All messages are immutable, value-equality, tagged Java records. Sequence fields
are defensively copied with List.copyOf. Addresses in payloads use Tid.

- Prepare(from, ballot): request a promise and an accepted-state snapshot.
- Promise(from, ballot, acceptedBallot, log): a phase-one vote and full snapshot;
  the candidate accepts only its current campaign's distinct responders.
- Accept(from, ballot, log): propose a full prefix, synchronize an acceptor, and
  announce a leader. A stale ballot elicits Reject; a current ballot elicits
  Accepted and may trigger pending-request forwarding.
- Accepted(from, ballot, length): acceptance evidence for an exact prefix length;
  only the current outstanding round and distinct voters count.
- Reject(ballot): report a greater promise to a stale proposer; receiving a
  strictly higher promise clears local leadership/candidacy without replying.
- Forward(value): carry a retained client request to a known leader; receivers
  deduplicate and retain it even if they have since stepped down.
- Decide(log): certify a majority-accepted prefix and transmit its contents to
  every learner; no reply is needed.

Send loops use fixed node-index order. There are no failures, fabricated client
values, message loss choices, random generators, sleeps, or external I/O in the
protocol. All interprocess exchanges use Channel.send and Channel.recv. The
only explicit protocol nondeterminism is the API election-timeout choice.

## Safety argument and progress limits

Within a ballot there is one owner and one extending sequence. A majority's
accepted prefix intersects every later prepare majority. Selecting the greatest
accepted ballot and the longest sequence at that ballot preserves every prefix
already chosen; reaccepting recovery before appending maintains this invariant
across elections. Preempted leaders may send old proposals, but cannot obtain a
conflicting majority after a higher prepare quorum. Decide is sent only after
a majority acceptance, so all learned sequences are prefix-comparable. Full
prefix delivery and per-node cursors make notifications ordered and unique.
The predecessor rule preserves each client's proposal order. Every notified
value originates in a proposal. These properties establish the externally
visible behavior required by SPEC.md; unused proposals may remain uncommitted,
as that specification permits loss.

With a stable elected leader, eventual message delivery carries the learned
prefix to every node and retained client requests to the leader. Finite budgets
and the allowed choice to block instead of timing out can leave executions
without a leader. No stronger election liveness is assumed. Bag delivery is
handled by ballot/length checks, immutable full snapshots, and predecessor gating.

## Compilation and the single requested run

Working directory for both commands: this directory.

Compile command (exit status 0):

```sh
javac -d java/out -cp "$(cat cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

Run command (executed exactly once; stdout/stderr captured in run-output.txt):

```sh
java -Xss4m -cp java/out:$(cat cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.omnipaxos_spec_sol.OmniPaxosSpecSol:impl,1,1,1,fifo' must 0 1 report - 600 </dev/null
```

The run exited with status 0 after 52.652 seconds of checking. It explored
4,387 iterations, all blocked, and exhausted its exploration stack:
`graphs:     0 distinct complete, 4387 distinct blocked; stack exhausted`.
`stop_reason: exhausted`. No protocol assertion failure was reported. This was
a counting run, not an online refinement comparison (no --spec was supplied),
and it checked only the requested e=1,t=1,cand=1,fifo instance. The safety argument
above covers the intended general protocol; this run is not an exhaustive
check of other parameter values or bag delivery. The launcher emitted host-name
and logging-configuration diagnostics before checking, retained in run-output.txt.

Exact last output line:

```text
max_porf_count: 0
```
