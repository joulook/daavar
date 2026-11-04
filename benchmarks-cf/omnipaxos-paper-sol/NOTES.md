# Omni-Paxos implementation notes

Source: `java/bench/omnipaxos_paper_sol/OmniPaxosPaperSol.java`.
Only `PAPER-EXCERPT.md`, `INTERFACE.md`, and the supplied local API sources were consulted. No reconfiguration, crashes, recovery, disconnections, real clocks, or extra processes are modeled.

## Roles and interface

The entry implements `MpProgram` and accepts `impl,e,t,cand,fifo|bag`; omitting `cand` defaults to 3. Values must satisfy e >= 1, t >= 1, and 0 <= cand <= 3. e is bounded to keep client values within Java int. Nodes n0, n1, n2 are internal; c0 and c1 are the only visible roles.

Main constructs five named `MpThread` instances and all their `Tid` addresses before starting them. It starts n0, n1, n2, c0, c1, in that order, then awaits them using `join1`. Using the API's named `MpThread` constructor supplies the same process/role lifecycle as `MpProcess.spawn`, while making all addresses available before any process runs. Main performs no send or receive. The address array is initialized before starting any thread and never subsequently modified; protocol state belongs exclusively to its node.

Client ci sends e `Propose(value)` records to ni in increasing j, with value = 1 + i + 2*j, then receives `Committed(value)` records until each own value has been seen. Other-client values are consumed without counting them. Clients end; nodes loop forever receiving, even after all proposals have been learned. No stop message or daemon is used.

`Propose` and `Committed` each have exactly one int field, `value`, and matching summary tags. n0 sends Committed to c0 and n1 to c1 for every newly learned log entry, including the other client's entries, in prefix order. n2 maintains the same decided index without a client. On bag links, notification sends still occur in decided order, but receipt order is governed by the selected communication model.

## Communication and choices

Every process exchange uses `Channel.send` and `Channel.recv` with P2P for fifo and ASYN for bag. All message records are immutable and tagged; lists in payloads are defensive immutable copies. Node identities in messages are `Tid`, not numeric runtime task IDs.

The excerpt explicitly requires FIFO perfect links for its suffix/append protocol. Node traffic therefore uses `Wire(from,sequence,body)`, with independent monotonically increasing counters for each directed link. The receiver buffers an out-of-order envelope in a per-sender ordered map and dispatches consecutive envelopes once their missing predecessors arrive. This is a lossless FIFO link implementation over bag delivery, not a change to the API's delivery model. It is also used in fifo mode, where envelopes already arrive in order. There are no acknowledgments or retransmissions at this transport layer because the interface guarantees delivery without failures. Heartbeats and replication share each directed node link. Client messages are sent directly, without envelopes.

A node with no known leader and positive budget calls `Nondet.chooseBoolean()` before its next receive: true spends one timeout, false proceeds to a blocking receive. Deferral consumes no budget and may leave a node blocked awaiting a message. The budget belongs only to n0 through n(cand-1). No wall-clock timer, polling loop, random generator, or deterministic forced election substitutes for this choice. Which available message is received is the API scheduler's choice subject to the selected model. Dispatch of consecutive envelopes is deterministic link-order restoration.

Finite budgets and the option to defer do not guarantee completion of clients in every execution: all timeouts can occur before useful replies, or nodes can block while deferring a timeout. This is an intentional consequence of the prescribed bounded-timeout interface, not an unbounded eventual-leader guarantee. Blocking executions are left to the harness.

## Ballot Leader Election (Figure 4)

Ballots compare lexicographically by `(sequence,pid)`. Stable unique process identifiers are assigned pid(n0)=3, pid(n1)=2, pid(n2)=1; the interface does not prescribe numeric identifiers. This makes n0 the highest initial priority, including when it is the only permitted timeout candidate. This ordering is fixed independently of cand. Each node starts with ballot `(0,pid)`, qc=true, heartbeat round 0, and no elected leader. The sentinel ballot `(-1,0)` is below every real ballot and means no ballot, rather than an extra server.

Each node sends `HBRequest(round=0)` to both peers at startup. A heartbeat timeout evaluates the replies collected for the current round, includes its own ballot and qc flag, and then clears replies, increments the round, and sends requests for the next round. Thus a timeout is Figure 4's timer boundary, not an immediate self-election. A reply alone never triggers an election. Even the final budgeted timeout starts the next round; its replies may be served/received without another timeout evaluation.

* `HBRequest(round)`: reply to the requester with `HBReply(round,ballot,qc)`. Responses advertise only this node's own ballot and QC flag, never its elected leader's ballot.
* `HBReply(round,ballot,qc)`: retain one reply per peer only when its round matches the current heartbeat round; late replies are ignored.
* Timeout with fewer than two replies including self: set qc=false. Otherwise inspect the maximum ballot among qc=true responders. If it exceeds the previously elected ballot, remember it and deliver a local Leader event to Sequence Paxos. If it is lower, increment the node's own sequence sufficiently to exceed the previous elected ballot and set qc=true; election awaits a subsequent round. An empty QC candidate set is treated as requiring the same increment. Matching maximum/elected ballots do nothing, as in Figure 4. Successful collection alone does not set qc=true; this follows the figure's explicit assignments.

Only a Leader event naming self with a ballot higher than the replication promise starts Prepare. A non-self election supplies a forwarding destination; learning a valid Prepare also supplies a leader and disables further timeout choices. BLE's elected ballot, its own advertised ballot, and Sequence Paxos's promised/accepted ballots are separate variables. Receiving Prepare never gossips or increments the advertised BLE ballot. There is no failure detector or leader clearing in this fixed, reliable, failure-free group.

## Sequence Paxos (Figure 3)

Each node stores a log, a monotonically increasing promised ballot, an accepted ballot, and a decided index. An index is a prefix length: index d means positions 0 through d-1 have been learned. Initial logs are empty and indices are zero. Leader state additionally contains its current ballot, Prepare/Accept phase, a promise per node, an accepted prefix length per node, and buffered proposals.

The implementation uses full immutable log snapshots in Promise and AcceptSync rather than the excerpt's suffix optimization. Equivalently, every synchronization starts at syncIdx=0. Consequently Prepare needs only the new ballot: accepted-round, length, and decided-index hints used solely to select a smaller Promise suffix are omitted. Promise's log length is implicit in the snapshot. This retains adoption by accepted ballot first and length second, and preserves the chosen prefix invariant described in the excerpt.

* Local self Leader event: reset leader bookkeeping, promise the new ballot, add the node's own Promise, retain pending requests in the Prepare buffer, and send `Prepare(ballot)` to both peers.
* `Prepare(ballot)`: ignore a ballot below the local promise. Otherwise promise it, enter follower/Prepare, remember the sender as leader, and send `Promise(ballot,acceptedRound,log,decided)`. Equal Prepare is permitted, as in the figure; with reliable, nonduplicating messages the leader sends one Prepare per peer per ballot.
* `Promise(ballot,acceptedRound,log,decided)`: only the leader of that ballot acts. During Prepare, collect a majority including self. Select the greatest acceptedRound, breaking a tie by longest log; identical ties use node index order. Adopt that sequence, append buffered requests not already present, set acceptedRound to the current ballot, count self's full log as accepted, and enter Accept. Send AcceptSync to each promised follower. A Promise arriving later gets a snapshot of the current log and any newer decision, allowing the third node to catch up.
* `AcceptSync(ballot,log)`: only a follower in Prepare with exactly that promise accepts it. Replace the log with the leader's snapshot, record the accepted ballot, enter Accept, and reply `Accepted(ballot,length)`. Every replacement checks that the already learned prefix remains unchanged.
* `Propose(value)` at a leader: retain in the Prepare buffer, or append in Accept and send `Accept(ballot,value)` to every promised follower. Update self's accepted length. A newly promising follower instead receives the full AcceptSync containing all entries so far.
* `Accept(ballot,value)`: only a follower in Accept with exactly that promise appends the value, then replies `Accepted(ballot,length)`. FIFO node links ensure AcceptSync precedes subsequent Accept messages and preserve append order.
* `Accepted(ballot,length)`: only the leader in Accept for that ballot acts. Keep the maximum acknowledgement per follower. The median of the three accepted lengths is the longest prefix accepted by a majority. If it exceeds the local decided index, learn it locally and send `Decide(ballot,length)` to all promised followers.
* `Decide(ballot,length)`: only a follower in Accept with exactly that promise learns the newly decided prefix. The decided index never decreases, and only newly learned positions produce client notifications. A late Promise receives a Decide after its AcceptSync if its decision lags the leader. FIFO links guarantee the corresponding accepted entries are present before Decide is processed.
* `Forward(value)`: followers forward proposals to their known leader. Before any leader is known they retain proposals. They resend retained proposals whenever their leader changes. A leading receiver processes Forward like Propose. A nonleading receiver retains an in-flight Forward rather than bouncing it among stale routing destinations; the original ingress also retains and resubmits it on learning the new leader. A candidate that becomes leader includes retained proposals in its buffer. Receiving Prepare is sufficient to flush an ingress node's pending requests, so a follower need not independently elect the leader in BLE.

The excerpt does not prescribe request identity, retries, or duplicate handling. Here client values are globally unique by the fixed interface, so value serves as request identity. Nodes retain requests until locally learned; leader buffers are insertion-ordered sets and leaders skip values already in their adopted/current log. This prevents resubmission from appending a client value twice. Requests retained by an old leader survive its role change; the new leader adopts accepted requests and/or receives resubmitted requests. Forward carries no client address because notifications are always sent by each client's fixed ingress node.

Leader changes and ballot checks are local; no shared log, global coordinator, shared monitor, or direct method calls between nodes exist. Updating an index/log before sending notifications is local sequential execution; API sends may yield, but another process cannot mutate that node's state. Assertions check decided-prefix preservation and that decisions refer to locally accepted entries. No extra safety monitor process is added because the interface fixes the five roles.

## Compilation and the single run

Compilation inside this directory succeeded (exit 0, no diagnostics):

```sh
javac -d java/out -cp "$(cat cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

Exactly one launcher invocation was started, inside this directory, with the requested command (stdout and stderr captured in `run-output.txt`):

```sh
java -Xss4m -cp java/out:$(cat cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.omnipaxos_paper_sol.OmniPaxosPaperSol:impl,1,1,1,fifo' must 0 1 report - 600 </dev/null
```

The launcher exited with status 0 after the requested 600-second timeout. It explored 101686 iterations and reported `0 distinct complete, 101686 distinct blocked; stack not exhausted`. The source deliberately keeps nodes receiving forever, so blocked graphs are expected; these counts do not establish that clients completed in every graph. Exploration was bounded by time and is not an exhaustive verification. The measurement block reports `stop_reason: timeout` and `wall_ms: 600514`.

The captured startup output includes a hostname-resolution stack trace and a logging configuration diagnostic (`Console contains an invalid element or attribute "level"`). The checker nevertheless ran and produced the measurements above. Full output is in `run-output.txt`; the launcher also wrote its requested `report` artifacts.

The exact last output line was:

```text
max_porf_count: 0
```
