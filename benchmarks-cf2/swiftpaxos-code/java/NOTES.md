# SwiftPaxos (code port) on JMC's message-passing API

Source file: `java/bench/swiftpaxos_code/SwiftpaxosCode.java`, class `bench.swiftpaxos_code.SwiftpaxosCode`. It
implements `org.mpi_sws.jmc.mp.launch.MpProgram` and is built from `impl,<q>,<fifo|bag>`.

Ported from the prototype cut in `plan/case-studies/swiftpaxos/code-src`. Line numbers below are the lines of those
files. The scope is ballot 0, leader n0, no recovery, the quorum configuration (C2) of INTERFACE.md, and one
integer register x with Write/Read/Rmw.

## Processes

| process | Java | started as | ends |
|---|---|---|---|
| `n0`, `n1`, `n2` | `Replica` (swift.go) | daemon `MpThread` | never (blocked on receive) |
| `l0`, `l1` | `Library` (swift/client.go + client/) | daemon `MpThread` | never (blocked on receive) |
| `c0`, `c1` | `client(i, q, ...)` (the interface's client) | `MpThread(body, "ci")` | after q replies |

Main does the following and sends and receives nothing:

1. It creates all seven `MpThread`s, which mints their task ids, in the order n0, n1, n2, l0, l1, c0, c1.
2. It starts them in the same order.
3. It awaits c0 and c1 (`join1`).

`MpProcess.runMain`, which the launcher calls, then lets the daemons run until they block.

## Messages

All messages are `@Tag` records. Command id = `CmdId(client, seq)`, with `seq` counting from 0 (client.go:75, 199).

| message | from → to | source |
|---|---|---|
| `Write{client,value}`, `Read{client}`, `Rmw{client}` | ci → li | interface |
| `Ack{}`, `Value{value}`, `Result{value}` | li → ci | interface |
| `Propagate(client, seq, kind, value)` | li → n0, n1, n2 (in index order) | GPropose (defs/defs.go:116-122), SendProposal with Fast (client/client.go:175-196) |
| `FastAck(replica, ballot, cmdId, dep, checksum, seqnum)` | replica → every other replica | MFastAck. The batcher sends it as a one-element MOptAcks (batcher.go 26-142). |
| `LightSlowAck(replica, ballot, cmdId)` | follower → every other replica, and → the command's library | MLightSlowAck. To replicas the batcher sends it as an MOptAcks entry with the nil dep `[cmdId]`, which the receiver turns back into a FastAck with `Dep = nil` (swift.go:330-334). A separate message type carries the same information. |
| `FastAckClient(replica, ballot, cmdId, checksum)` | fast-quorum follower (n1) → the command's library | MFastAckClient (batcher.go 52-58, 122-124) |
| `Reply(replica, ballot, cmdId, checksum, rep)` | leader n0 → the command's library | MReply (replychan.go 49-57) |

`Hash(chain)` stands for SHash, and a checksum is a `List<Hash>` with one element, because there is one key.

## Replica (swift.go)

Each replica keeps the following state:

- `delivered`: executed commands.
- `cmdDescs`: per-command descriptor. A descriptor holds the phase, cmd, dep, hs, propose, proposeDep, slowPathH,
  fastPathH, the afterPropagate slot, slowPath and successors.
- The light key info of the one key: `lastCmd` and `lastWrite` (key.go:113-136).
- The hash log of the one key (dpath.go).
- The leader's `seqnum`.
- `pendingHashUpds` and `proposes`.

The loop (`run`, swift.go:227-353) receives one message at a time:

- **Propagate.** The replica records the proposal and computes dep and hash (`getDepAndHashes`, 767-792).
  - Read takes the conflicts in `lastWrite`. Write and Rmw take the conflicts in `lastCmd`.
  - The command becomes `lastCmd`. Write and Rmw also become `lastWrite`.
  - The command's id is appended to the hash log.

  At a follower, a leader hash that arrived earlier is applied next (279-283). Then `getCmdDescSeq` (589-632)
  sets `proposeDep`/`hs` once and calls `handlePropose`.
- **handlePropose** (355-407):
  - **n2 (not in FQ):** it runs the stored leader FastAck, if there is one, and stops. n2 never sends a FastAck.
  - **n0 and n1:** dep := proposeDep and phase PRE_ACCEPT. If the stored leader FastAck runs and sets slowPath,
    no FastAck is sent. Otherwise the replica builds `FastAck(dep, hs, seqnum)`, where seqnum is the leader's
    counter (incremented) and 0 at a follower.
    - **Leader (optExec):** it sends FastAck to n1 and n2, then `deliver`s, which executes the command at once and
      sends `Reply` to the library.
    - **n1:** it sends `FastAckClient` to the library, then FastAck to n0 and n2 (the batcher's order: client
      messages first, then SendToAll).

    Both then handle their own ack.
- **FastAck.** If it comes from the leader and this replica is a follower, `recordLeaderHash` (794-804) applies it:
  if the proposal is already known it calls `HashLog.Update`, otherwise the hash waits in `pendingHashUpds`.
  Then: from the leader → `fastAckFromLeader`, from others → `commonCaseFastAck`. Nothing is done if the command is
  already delivered (591-593).
- **fastAckFromLeader** (417-482) runs only once the proposal is known (afterPropagate). It works in this order:
  1. phase ACCEPT, and `neq := dep ≠ leader dep`.
  2. `sendSlowAck := leader ≠ me ∧ (SQ.Contains(me) ∨ (FQ.Contains(me) ∧ neq))`.
  3. It adds the leader ack to slowPathH and, if the command is not yet delivered, to fastPathH.
  4. If sendSlowAck: dep := leader dep (if neq and not delivered), slowPath := true, then it sends `LightSlowAck`
     to the library and to the other replicas, and handles its own slow ack if the command is not delivered.
  5. The deferred LightSlowAck to the client (438-451) can never fire for a follower here, because sendSlowAck is
     always true. It is ported anyway.
- **commonCaseFastAck** (484-497). A slow ack (`dep == null`) goes into slowPathH and, unless the command is
  delivered, also into fastPathH. A FastAck goes into fastPathH only.
- **MsgSet** (mset.go:45-87) is ported unchanged.
  - The replica's accept is `leader == nil ∨ dep == nil ∨ dep equals the leader's dep` (665-673).
  - The handler (499-522) runs when there are ≥ q.size messages, or ≥ q.size−1 messages plus the leader's. It sets
    phase COMMIT, registers the command as a successor of each of its deps, and calls `deliver`.
  - slowPathH uses SQ = Majority(3) (size 2, contains all). fastPathH uses FQ = {n0, n1}.
- **deliver** (533-583): it needs the proposal, the command not yet delivered, phase COMMIT (the leader under
  optExec skips this), and every dep delivered. It then executes on x and queues the successors on `deliverChan`.
  Only the leader replies (`Reply` with its hs and the value, replychan.go 41-67). Followers reply nothing.
- **Execution** (state.go:115-146, mapped by the interface): Write: x := v and the result is 0 (NIL). Read: x
  (initially 0). Rmw: x := x+10 and the result is x.

## Client library (swift/client.go)

The library holds `seqnum` (starts at −1), the pending kind, `val`, `ballot` (−1), `delivered`, and per-command
slowPathH and fastPathH. It runs the following loop:

- **Write/Read/Rmw from ci.** seqnum++, then `Propagate` to n0, n1, n2.
- **Reply** (243-260). It ignores the message if the command is delivered. Otherwise val := rep, the reply goes into
  fastPathH as the leader message (with its checksum), and, if the command is still not delivered, into slowPathH as
  the leader message with a nil checksum.
- **FastAckClient** (132-139) → `handleFastAck` (186-202). There is a ballot check, then the message goes into
  fastPathH as a non-leader message.
- **LightSlowAck** (204-226). It goes into fastPathH (nil checksum) and, if the command is not delivered, into
  slowPathH.
- **accept** (80-100) is `leader == nil ∨ SHashesEq(leader checksum, checksum)`.
- **Handler** (228-241). Once the leader's Reply is in, the first set to reach its quorum marks the command delivered
  and answers ci: `Ack` for a Write, `Value(val)` for a Read, `Result(val)` for an Rmw.
  - **Fast path:** Reply plus n1's FastAckClient with the leader's checksum.
  - **Slow path:** Reply plus any follower's LightSlowAck.

## Decisions (and the source lines that left them open)

1. **optExec = true, fastRead = false, Exec = true, Dreply = true, Proxy = false, slowAddrs empty, Thrifty
   irrelevant.** These are command-line or configuration flags that the cut does not fix (swift.go:106-107,
   replica.go:91-95, replica.go clientListener `isProxy`). optExec is the mode the interface describes: the leader
   executes and sends Reply, and followers send FastAckClient. With optExec false the client side would accept on
   MAccept (replychan.go 41-48), which the interface does not mention. With fastRead off, `isRead` (swift.go:392) is
   always false.
2. **Command mapping** (key.go:113-136, swift.go:392). Write and Rmw are PUT-class: they depend on `lastCmd` and
   become `lastWrite`. Read is GET-class: it depends on `lastWrite`. This gives "every two commands conflict except
   two Reads". lightKeyInfo, not fullKeyInfo, is used, as in `getDepAndHashes` (swift.go:778). A Write's reply value
   is NIL, encoded as 0 and never read.
3. **Hashes** (dpath.go). An SHash is kept as the chain of command ids it hashes (the zero hash is the empty chain),
   and `hash(h, id)` = h followed by id. Equal chains ⇔ equal sha256 values, assuming no collisions. The pending
   linked list is a list. `Append`, `Update` and `update` are otherwise unchanged, including the `pendingUpd` path.
4. **hook.OptCondF (afterPropagate)** is not in the cut (swift.go:76, 364, 370, 418, 661-663). It is modelled as
   one slot: `Call(f)` runs f when `propose != nil` and otherwise stores f (only the leader's single FastAck ever
   stores one). `Recall()` runs and clears the stored f when `propose != nil` and returns whether it ran one.
   Returning the condition instead gives the same behaviour, because slowPath is set only inside f.
5. **Client fast quorum.** The prototype's client uses `c.FQ = NewMajorityOf(N)` (client.go:57-60, "TODO: it has to
   be the correct majority"). Here it is {n0, n1}, as INTERFACE.md says. This is equivalent: n2 sends the client only
   LightSlowAcks with a nil checksum. With the leader's Reply present (checksum of length 1) those are never
   accepted, and the filter removes them when the Reply arrives later. Before the Reply, the handler returns at
   once. The client SQ stays Majority(3).
6. **Sequential replica.** In the prototype, a follower's descriptors are handled by one goroutine per command
   (swift.go:613-616, 702-722) while the run loop computes deps and hashes. The batcher, sender and replyChan are
   goroutines too. Here a replica handles each received message completely, in arrival order. Sends happen in
   program order (to the library first, then the replicas in index order, as in the batcher). Successors of an
   executed command (`deliverChan`, swift.go:558-566) are handled after the current message and before the next
   receive. The history/slot bookkeeping (swift.go:572-582, 745-757; replychan.go 61-62) affects nothing and is
   left out.
7. **No batching** (batcher.go). Every FastAck and LightSlowAck is sent on its own, so the MOptAcks merge of a
   FastAck and a LightSlowAck for the same command into one slow entry (batcher.go 98-104) never happens.
8. **SendToAll excludes the sender**, because `Alive[self]` is never set (replica.go:144-160, 395-411; sender.go
   149-159).
9. **"Fatal"** (a proposal for a delivered command, swift.go:287-289, or a hash count mismatch, 809-811) is an
   `IllegalStateException`, which fails the execution.
10. **No `Nondet` choice is needed.** All nondeterminism the code has comes from message arrival order (the
    `select` in `run` and `handleMsgs`).
11. **Process names.** The API gives daemons no role (`MpProcess.spawnDaemon`, `MpThread(body, true)`), so n0-n2 and
    l0-l1 are unnamed daemons. Only c0 and c1 carry roles, and `rolesInSpawnOrder` = `visibleRoles` = [c0, c1].
    Daemons are used because the API defines "never terminates, blocked on receive at the end" for them only. A
    non-daemon replica would have to be awaited, and awaiting a blocked process blocks main.
12. **Tid distribution.** All processes are created (ids minted) before any is started. This avoids a library
    replying to a client whose handle main has not yet stored. The bodies read the address arrays only after
    `start()`.
13. **Program name and size.** `programName()` = `swiftpaxos_code` and `size()` = `q`. INTERFACE.md does not fix
    them.
14. **Stats left out:** `slowPaths`/`alreadySlow` on the client (client.go:89-92, 168-171, 209-212) and `history`.
15. **Client failure.** A wrong reply kind is an `AssertionError` ("Ack expected", ...), as the Rust client panics.

## Observations on the code's behaviour (ported unchanged)

- `r.SQ = NewMajorityOf(N)` contains every replica (quorum.go 16-29), so `slow` is always true at a follower. Every
  follower therefore sends a LightSlowAck after the leader's FastAck, even when its deps agree (swift.go:433-434).
- At a follower, the commit handler can run inside `slowPathH.Add(leader)` or `fastPathH.Add(leader)` (453-456),
  before `desc.dep` is replaced by the leader's dep (461-463). n2's `desc.dep` is nil there (n2 skips line 368), so
  n2 may execute a command before its dependency. Only followers are affected, and they reply nothing, so no client
  sees it.

## Compile and run

Compile, inside this directory:

```
javac -d java/out -cp "$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

Result: exit 0, no output.

Run, inside this directory:

```
java -Xss4m -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.swiftpaxos_code.SwiftpaxosCode:impl,1,fifo' must 0 1 report - 600 </dev/null
```

Last output line: `max_porf_count: 0` (the last line of the measurement block). The run stopped on the 600 s timeout
(`stop_reason: timeout`, `stack not exhausted`). Before it stopped it ran 38308 iterations: 38308 distinct complete
graphs, 0 blocked, no failure, 63.8 iterations/s, max_graph_events 67, peak RSS 489556 kB. Exit status 0.
