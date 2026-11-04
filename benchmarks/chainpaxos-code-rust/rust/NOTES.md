# bench_chainpaxos_code: ChainPaxos (pfouto/chain) on one register, TraceForge Must API

Source: `plan/case-studies/chainpaxos/code-src` (M = chainpaxos/ChainPaxosMixedProto.java,
D = chainpaxos/ChainPaxosDelayedProto.java, MF/DF = the two *Front.java, App = app/HashMapApp.java).
Interface: `plan/case-studies/chainpaxos/INTERFACE.md`. Code: `src/main.rs`.

## Processes

| thread | role |
|---|---|
| `n0` | head of the chain, leader for the whole run; front of `c0` |
| `n1` | middle of the chain; no client |
| `n2` | tail of the chain; front of `c1` |
| `c0`, `c1` | the interface's client code, verbatim; `c0` -> `n0`, `c1` -> `n2` |
| `init` | invisible; sends `Setup { nodes, clients }` to n0, n1, n2 (tag `SETUP_TAG`) |
| main | spawns n0, n1, n2, then c0, c1, then init; joins init and the clients; sends/receives nothing |

Each replica thread is protocol and front at once (the source runs the front, the protocol and
the app as Babel protocols of one process; their requests, replies and notifications become local
calls). Constants: N = 3, quorum 2, leader 0, `nextOk` = next index mod 3 (M 197
`membership.nextLivingInChain`).

## Messages

Client <-> replica: `Req`, `Rep` verbatim (untagged, `send_msg`).

Replica <-> replica: `Wire { from, msg: Peer }`, sent with `send_tagged_msg(dest, PEER_BASE + seq, _)`.

| Peer | source | sent by -> to | content |
|---|---|---|---|
| `Redirect { issuer, op }` | PeerBatchMessage, MF 103-110/153-156 | n2 -> n0 | one client op (no batching) and the issuing replica (OpBatch.issuer) |
| `Accept { inst, counter, value, ack }` | AcceptMsg, M 575-577 | n0 -> n1, n1 -> n2 | instance, nodeCounter, PaxosValue, sender's highestAcknowledgedInstance; sN omitted (one ballot) |
| `AcceptAck { inst }` | AcceptAckMsg, M 573 | n2 -> n0 | instance number |

PaxosValue = `PVal::App { issuer, op }` (AppOpBatch with one op) or `PVal::NoOp`.

## Protocol, role by role

- **Front, client request** (`on_client_request`; App 319-331, MF 72-77, DF 91-106).
  Write and Rmw are WRITE ops. Read is a STRONG_READ op. In `mixed` every op goes to
  `writesTo` = `supportedLeader()` (M 827-831: the 3rd MembershipChange argument is writesTo).
  At n0 that is itself, so it calls onSubmitBatch (MF 153-154, 84-85). At n2 it sends `Redirect` to n0.
  In `local`, a Read calls `onSubmitRead` at the client's own replica (DF 142-147 -> D 657-660). Write and
  Rmw go as in `mixed`.
- **Leader, onSubmitBatch / Redirect** (M 629-636, MF 84-85). The leader is always
  amQuorumLeader, so it calls `sendNextAccept` (M 638-669). The value goes to instance `lastAcceptSent+1`
  through a local call of uponAcceptMsg with counter 0 and ack = highestAcknowledgedInstance.
- **uponAcceptMsg** (M 490-536). Accept the value with counter := msg.counter+1, raise
  highestAccepted (no gaps), `forward`, decide when counter >= quorum, then `ackInstance(msg.ack)`.
  So n0 has counter 1 and decides only at the AcceptAck. n1 (counter 2) and n2 (counter 3) decide at the accept.
- **forward** (M 549-580). The targets are `nextNodesUntil(self, nextOk)`, here the successor only. If the
  target `isAfterLeader` (n2 -> n0), send AcceptAck. Otherwise send Accept with the forwarder's own
  highestAcknowledgedInstance. That value is taken *before* this accept's `ackInstance(msg.ack)` (M 530 before 535),
  so n2 learns acks one hop late. This is kept.
- **uponAcceptAckMsg** (M 582-600). Ignore old acks, otherwise `ackInstance(inst)` at the leader.
- **ackInstance** (M 602-618, D 619-636). Decide and execute every undecided instance <= n (the first
  half of the chain), then acknowledge and remove every instance <= n. In D, also answer the reads
  attached to the removed instance (D 631).
- **decideAndExecute** (M 722-741) + Front.onExecuteBatch (MF 161-173) + App.executeOperation
  (App 221-248). Every replica applies Write (x := v) and Rmw (x := x+10) to its own register. Only the
  issuer replica (`local == true`: issuer == self) replies to its client: `Ack`, `Result(x)`, or in
  `mixed` `Value(x)` for a Read. A replica that is not the issuer does nothing for a Read (App 246).
- **local reads** (D 657-660, 631, DF 220-231). The read is attached to instance
  `highestAcceptedInstance + 1` of the client's replica. When that replica *acknowledges* that instance,
  it answers `Value(x)` with x at that moment.
- **No-op** (`local` only, interface rule). This replaces M/D onNoOpTimer (M 325-334). At the top of each loop
  iteration of the leader, when no instance is in flight (`lastAcceptSent == highestAcknowledged`) and the budget is
  not used up, `nondet()` may start `sendNextAccept(NoOp)`.

## Decisions (with the source lines that left them open)

1. **Fixed leader without a Prepare phase.** n0 starts as amQuorumLeader with one constant ballot.
   tryTakeLeadership, Prepare, PrepareOk, Decided-on-prepare and becomeLeader (M 314-460) are dropped. So are
   sN in messages and the sN checks in uponAcceptMsg (M 499-513), which cannot fail with one ballot
   and no duplicates. Membership ops, joins, state transfer, Unaffiliated and connection events
   (M 205-312, 538-547, 620-627, 671-720, 745-768) are dropped.
2. **Which message a replica receives next.** This is the main modelling decision. The API's receive is typed, so a
   replica cannot block on "a `Req` or a `Wire`" in one receive. A Babel handler runs for whichever
   event arrives. The replica therefore picks the kind first:
   - If its client's next request is certainly coming (the previous reply was sent and fewer than q requests
     were received) **and** a replica message can still come without this replica first taking that request,
     the replica makes a `nondet()` choice.
   - If only one of the two can come, it receives that one.
   - When the client is waiting or done, it blocks on replica messages.

   "Can still come" is counted from static facts and local counters:
   - n0: an instance is unacknowledged, or fewer redirects were received than c1 creates instances.
   - n2: fewer accepts for c0's instances were seen than c0 creates, or more redirects were sent than accepts were
     seen for them, or (`local`) fewer no-ops were seen than the budget.
   - n1: always replica messages.

   In `mixed` these conditions are exact: a message they promise always comes, so the guard creates no spurious
   deadlock and loses no arrival order. In `local` they over-approximate. A read can wait on a no-op that the
   leader never chooses, so some `nondet` branches can block with a client request left unreceived. Those
   executions end blocked, with the client waiting.
3. **TCP FIFO between replicas under `bag`.** The source assumes per-connection FIFO (Babel TCPChannel).
   The gap assertion in uponAcceptMsg (M 521-524) and the in-order loops of ackInstance would
   break on reordering. So every replica message carries a per-channel sequence number in its tag, and the receive
   accepts only the next number from each sender. Under `fifo` this has no effect. Client channels are
   not sequenced (at most one request in flight per client).
4. **Setup is received first.** It has its own tag. Messages that arrive earlier stay in the mailbox, which is the
   interface's buffering.
5. **Rmw** does not exist in the source. It is treated as a WRITE op (ordered through the chain in both variants)
   and answered with the new x by the issuer at execution.
6. **No batching.** BATCH_SIZE = 1 and no BatchTimer (MF 72-77, 92-101). One request is one instance. A local read
   is one SubmitReadRequest. The pendingBatches / batch-id checks (MF 161-170, DF 220-231) are kept implicitly:
   the issuer replica in the value identifies its own ops, and the client has one op in flight.
7. **No-op budget and "no pending request".** The interface says "at most one per outstanding client read
   in the whole run". The leader cannot know about reads at n2 (D 657-660 sends nothing), so the budget is
   static: the total number of Read commands of both clients. "No pending request" is read as "no
   instance in flight". The source's forced no-op after an AcceptAck (M 596-597, `lastAcceptTime = 0`)
   is not modelled separately. Consequence (from M 575-576, 530 before 535): a read at the tail is answered only
   when an Accept that carries an ack >= its instance reaches n2. Without other traffic that can need three
   further instances, so with this budget some tail reads in `local` are never answered.
8. **The value of a local read** is x when the replica acknowledges the attached instance. The front
   executes the ExecuteReadReply after the notifications already queued (D 631 runs after the decide loop of
   the same ackInstance).
9. **No time.** lastLeaderOp, lastAcceptTime, LeaderTimer, Join/StateTransfer/Reconnect timers are dropped.
10. **main** joins init and the clients only. The replicas never end, so the execution ends with them blocked on
    receive. The optional third program argument (`fifo|bag`) is accepted and ignored. The model is the
    harness's `--model`.

## Build and run

Compile (in this directory): `cargo build --release -j 2`. It finished without errors or warnings.

Run: `timeout 600 target/release/bench_chainpaxos_code count impl --model fifo -- 1 mixed`

Result line: `execs=0 blocked=5`. All 5 executions end blocked because the replicas never terminate. The last line
of stdout is the measurement block's last line, `cache_hits: `.
