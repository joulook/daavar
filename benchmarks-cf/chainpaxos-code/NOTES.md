# ChainPaxos (code port), Java / JMC message-passing API

Source: `plan/case-studies/chainpaxos/code-src` (pfouto/chain cut). Interface: `plan/case-studies/chainpaxos/INTERFACE.md`.
Code: `java/bench/chainpaxos_code/ChainpaxosCode.java` (one file). Argument string: `impl,<q>,<mixed|local>,<fifo|bag>`.
Line numbers below are from `chainpaxos/ChainPaxosMixedProto.java` (MP), `chainpaxos/ChainPaxosDelayedProto.java` (DP),
`chainpaxos/ChainPaxosMixedFront.java` (MF), `chainpaxos/ChainPaxosDelayedFront.java` (DF), `app/HashMapApp.java` (APP),
`chainpaxos/utils/InstanceState.java` (IS), `chainpaxos/utils/Membership.java` (MEM).

## Processes

Main creates five `MpThread`s in the order n0, n1, n2, c0, c1 (roles = names), then starts them in that order, then
awaits c0, c1, n0, n1, n2. Main sends and receives nothing. `rolesInSpawnOrder` = [n0,n1,n2,c0,c1]; `visibleRoles` =
[c0,c1]; `model()` = `P2P` for `fifo`, `ASYN` for `bag`; every send and receive uses it. `programName` = `chainpaxos`,
`size` = `<q>,<variant>`.

- `c0`, `c1`: the interface's client, verbatim (command j of client i: kind (i+j) mod 3; Write v = 1+i+2j; one request,
  one blocking receive of any message, `AssertionError("Ack expected")` etc. on a wrong reply). c0 talks to n0, c1 to n2.
- `n0`, `n1`, `n2`: one process = Proto + Front + application (one register `x`, initially 0). Loop forever: (leader, local
  variant only) optional no-op; then one blocking receive and its handler. Replicas never end; each execution ends with
  them blocked on receive, so the launcher counts every execution as blocked (Must's B).

## Messages

Visible (interface): `Write{client,value}`, `Read{client}`, `Rmw{client}` (client -> its replica); `Ack{}`,
`Value{value}`, `Result{value}` (replica -> client).

Internal (all `@Tag`ged records):
- `Op{kind,origin,id,client,arg}` (payload only): a Paxos value. kind NOOP = NoOpValue; WRITE/READ/RMW = an AppOpBatch of
  one operation. `origin`,`id` = OpBatch issuer and batch id (FrontendProto.nextId); `client` replaces APP's opMapper
  (APP 326-330, 231); `arg` = written value.
- `Redirect{op}` = PeerBatchMessage, front -> writesTo (MF 103-111, 153-156; DF 150-157, 198-201).
- `Accept{iN,counter,value,ack}` = AcceptMsg without sN (MP 575-577).
- `AcceptAck{iN}` = AcceptAckMsg, last in chain -> leader (MP 564-573).

## Handlers, role by role

Fixed membership [n0,n1,n2], leader n0 from the start (`amQuorumLeader` true at n0, `supportedLeader()` = n0 everywhere).

1. Client request at its replica r (APP `channelRead` 319-331 + Front `submitOperation`): a fresh id, the id is appended to
   `pendingWrites` (MF 106 `pendingBatches`), and the op goes to writesTo = the leader: at n0 directly to
   `onSubmitBatch` (MF 154 local call), elsewhere as `Redirect` to n0. In `local`, a Read instead goes to
   `onSubmitRead` (DF 93-98, 142-148).
2. `Redirect` at the leader (MF 84-86 -> MP 629-636): `sendNextAccept`.
3. `sendNextAccept(v)` (MP 638-669): instance lastAcceptSent+1, local `uponAcceptMsg(Accept(iN, 0, v, highestAck))`,
   lastAcceptSent := iN.
4. `uponAcceptMsg` (MP 490-536): discard if decided; discard if already accepted with counter >= msg counter; accept with
   counter+1; highestAccepted++; `forward`; decide if counter >= 2 (quorum); `ackInstance(msg.ack)`.
5. `forward` (MP 549-580 with MEM 99-109, 172-187): the target is the next node in the ring (nobody pending removal). If
   the target is the leader (the sender is the tail n2), send `AcceptAck(iN)` to n0 (error if counter < quorum);
   else send `Accept(iN, counter, value, own highestAck)`. So n0 -> n1 -> n2 -> AcceptAck to n0. Counters: n0 1, n1 2
   (decides), n2 3 (decides); n0 decides on the AcceptAck.
6. `AcceptAck` at n0 (MP 582-600): ignore if <= highestAck; `ackInstance(iN)`.
7. `ackInstance(k)` (MP 602-618, DP adds line 631): decide (and execute) every undecided instance <= k, then acknowledge
   every instance <= k, removing it; in `local` each removed instance answers its attached reads.
8. `decideAndExecute` (MP 722-741) -> Front `onExecuteBatch` (MF 161-173) -> APP `executeOperation` (APP 221-248): every
   replica executes in instance order; Write x := v, Rmw x := x+10, Read nothing. Only at the origin replica
   (`local == true` in APP) the head of `pendingWrites` is checked against the op id (AssertionError on mismatch, MF
   164-168) and the reply is sent: `Ack`, `Result(x)` or (mixed) `Value(x)`.
9. `local` reads (DP 657-661, IS 254-257, DP 631, DF 220-230): a Read at replica r is attached to instance
   highestAccepted+1 of r; when r acknowledges that instance (step 7), r replies `Value(x)` with its current x (all
   instances <= the acknowledged one are executed by then). At n0 one later instance suffices (the AcceptAck); at n1/n2
   the ack only arrives piggybacked on a later Accept (Accept.ack), i.e. it needs an instance proposed after n0
   acknowledged the read's instance.
10. No-op (local only; MP/DP 325-334 replaced per the interface): at the top of each loop iteration n0, while its no-op
    budget is positive, makes `Nondet.chooseIndex(2)`; 1 = `sendNextAccept(NoOp)` and budget--, 0 = receive.

## Decisions (with the source lines that left them open or that were simplified)

D1. Replica-to-replica order. The source relies on TCP FIFO (MP 521-524 asserts no gap; MP 604-616 expects instances
    to exist). Under `bag` I keep that order with a selective receive: an `Accept` is received only when its iN equals
    highestAccepted+1 (guard captures that int); every other message is received in any order. Under `fifo` the guard
    never excludes a message (n1 and n2 each receive Accepts only from their predecessor, in order).
D2. `sN` dropped from Accept/AcceptAck and every sN comparison (MP 499, 503-508, 510) reduced to "same ballot": fixed
    leader, no Prepare. PREPARE/PREPARE_OK/DECIDED/JOIN/STATE/MEMBERSHIP/Unaffiliated messages and connection events
    dropped (MP 205-334, 336-488, 620-627, 675-720, 745-784). DECIDED (MP 379, 462-488) is only sent in reply to a
    Prepare, so it has no role with a fixed leader; decisions reach n0 by AcceptAck and the others by counter or
    piggybacked ack.
D3. Rmw is not in the source (APP handles WRITE/READ only, APP 232-247). It is routed and ordered like a write
    (`OpType.WRITE`, APP 329-330; in `local` it is not a local read) and replies `Result(new x)` at the origin.
D4. Mixed-variant reads go through the chain exactly like writes (MF 72-78 does not distinguish types) and reply
    `Value(x)` at the origin when executed (APP 240-246).
D5. Read reply value in `local`: the source sends ExecuteReadReply to the front (DP 631) and the front executes the read
    on its app state (DF 228); Babel delivers the earlier ExecuteBatch notifications first, so the read sees every
    instance this replica has executed. I reply immediately with the replica's current x at the ack.
D6. No-op choice point and budget. "No pending request" (interface; MP 327 asserts waitingAppOps empty) always holds in
    this model because a request is proposed in the same step it is received. The bound "at most one per outstanding
    client read in the whole run" cannot be computed from local knowledge (n0 does not see reads at n2), so the budget is
    the static number of Read commands in the run (#{(i,j) : (i+j) mod 3 = 1}), offered before every receive of n0
    while positive. Consequence: a tail read needs up to two later instances (step 9), so with few writes after it some
    executions end with c1 blocked; that is the interface's bound, not a protocol fault.
D7. Mixed variant has no no-ops at all (the interface replaces the timer by the choice only in `local`); `lastAcceptTime`
    (MP 596-597, 668) is dropped with the timer. Without no-ops n1/n2 may keep instances unacknowledged; this affects
    nothing visible in `mixed`.
D8. Source `assert`s that matter for ordering (MP 523, 606, 614, 723-724; IS 255) are explicit checks throwing
    AssertionError / IllegalStateException, so a violation fails the run instead of being silently skipped.
D9. Process ids: main constructs all five MpThreads first (minting their task ids) and only then starts them, so every
    body knows every Tid from the start (the interface: "every process knows every other process's address"), with no
    setup message. Handles are awaited through `MpThread.join1()`.
D10. Redirect to a non-leader is ignored as in MP 635 (cannot happen). One operation per batch and per read batch
    (batching off); ids come from one per-replica counter (FrontendProto.nextId), shared by writes and reads.

## Compile (inside OUT_DIR)

    javac -d java/out -cp "$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')

Result: exit 0, no output.

## Run (inside OUT_DIR, once)

    java -Xss4m -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.chainpaxos_code.ChainpaxosCode:impl,1,mixed,fifo' must 0 1 report - 600 </dev/null

Exit 0. Key lines: `ran: 4 iterations`, `blocked: 4`, `graphs: 0 distinct complete, 4 distinct blocked; stack
exhausted`, `stop_reason: exhausted` (all blocked because the replicas end blocked on receive). Last output line:

    max_porf_count: 0
