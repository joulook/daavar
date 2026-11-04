# PineappleCode: a Java (JMC message-passing API) port of the Go Pineapple replica

Source: `plan/twins-contextfree/pineapple/code-src/{pineapple.go, pineappleproto.go, genericsmr.go}`.
Interface: `plan/twins-contextfree/pineapple/INTERFACE.md`. Line numbers below refer to `pineapple.go` unless they name another file.
Program: `java/bench/pineapple_code/PineappleCode.java`. Class `bench.pineapple_code.PineappleCode`, which implements `MpProgram` and is constructed from `impl,<q>,<fifo|bag>`.

## Processes

- **main**: creates and starts `n0, n1, n2` (in index order), then `c0, c1`, and awaits `c0, c1`. It sends and receives nothing.
- **nodes `n0, n1, n2`** (replica ids 0, 1, 2; `n0` has `IsLeader = true`, the others `false`): each is one loop that does a blocking `recv` of any message and dispatches on the message type. This is the `select` of `Run` (l. 711-785). A node never terminates.
- **clients `c0, c1`**: these follow INTERFACE.md exactly. `ci` sends its j-th request to `ni` and blocks for the reply. The kinds are `(i+j) mod 3`: 0 is `Write(i, 1+i+2j)`, 1 is `Read(i)` and 2 is `Rmw(i)`. Client `ci` checks that the reply is `Ack`, `Value` or `Result` respectively and otherwise throws `AssertionError`.

## Messages

Client to node: `Write{client,value}`, `Read{client}` and `Rmw{client}`. Node to client: `Ack{}`, `Value{value}` and `Result{value}`.
Each record carries `@Tag` with its name.

A client request becomes a `genericsmr.Propose` with op `PUT`, `GET` or RMW respectively. Here the `Reply` writer of the Go code (genericsmr.go l. 27-30, 294) is the client index. The request is handled by `handlePropose` (l. 610-665):
- **Write (PUT)**: create an instance with a fresh number (skipping used numbers, l. 611-613), then `bcastGet(write=true)`: send `Get{replicaId, instance, write=1, key, Payload{}}` to `(id+1)%3` and `(id+2)%3`.
- **Read (GET)**: set `initialTag` to own data's tag. If the key is absent, first store `{Tag{0,id},0}`. Then `bcastGet(write=false)`, which carries own data.
- **Rmw**: `rmwId = crtRmwId++`, create the instance, then `bcastRMWGet`: send `RMWGet{leaderId=id, instance, ballot=0, key}` to the two other replicas. The node that receives the Rmw coordinates it, as in the code (l. 637-647). The request is not forwarded to `n0`.

Among the nodes (all ported line by line):
- `Get` goes to `handleGet` (l. 233-258). For a read, if the key is absent or the received tag is larger, the node adopts the received payload and replies with it; otherwise it replies with its own data. For a write it replies with `Payload{}`. The reply is `GetReply{replicaId, instance, OK=1, write, key, payload}`, sent to the coordinator.
- `GetReply` goes to `handleGetReply` (l. 261-323). If `getDone` is set, the reply is ignored. Otherwise the node records the reply and adopts its payload if the tag is larger. With `getOKs+1 > N>>1` (with N = 3 that is the first reply), the node computes identicalCount and hasMaxTag and runs the initialTag check:
  - For an optimized read, it calls `replyClient`.
  - Otherwise, for a write, it sets `data = {Tag{ts+1,id}, data.Value}`.
  - In both non-optimized cases it then calls `bcastSet` with own data. For a read, `bcastSet` skips replicas in `hasMaxTag`.
- `Set` goes to `handleSet` (l. 368-378). The node adopts the payload if its tag is larger and replies `SetReply{instance}`.
- `SetReply` goes to `handleSetReply` (l. 381-391). The node runs the quorum test exactly as written. With N = 3 the first reply passes. It then calls `replyClient`, which replies once because of `completed`.
- `RMWGet` goes to `handleRMWGet` (l. 422-452). On an unknown instance, the node creates it and replies `RMWGetReply{instance, defaultBallot, key, Payload{}}`, an empty payload (l. 438). On a known instance, it updates cmds and status and replies with its own data. Replies go to `leaderId`.
- `RMWGetReply` goes to `handleRMWGetReply` (l. 456-492). If `rmwGetDone` is set, the reply is ignored. Otherwise the node collects the reply. At `rmwGetOKs+1 > N>>1` (the first reply), it runs the "largest tag" loop as written. Then it sets `data = {Tag{ts+1,id}, f(data.Value)}` and calls `bcastRMWSet(instance, reply.ballot, key)` with own data.
- `RMWSet` goes to `handleRMWSet` (l. 526-565). The node runs the ballot cases (all ballots are 0), stores `receivedRMW` and adopts it if its tag is larger. It replies `RMWSetReply{instance, OK=1, defaultBallot}` to `leaderId`.
- `RMWSetReply` goes to `handleRMWSetReply` (l. 568-581). If `rmwId <= rmwDoneUpTo`, the reply is ignored. Otherwise `rmwSetOKs++`. At a quorum (the first reply), the node sets `pendingRMWs[rmwId] = inst` and `rmwDoneUpTo++`, and the RMW is answered (see D2, D4).

`isLargerTag` (l. 150-165) is ported verbatim, including the leader rule (`IsLeader && currentTag.ID == r.Id` gives true).

## Decisions

- **D1. Nodes are daemons.** "Nodes never terminate" and "the harness ends executions blocked on receive" match `MpThread(body, daemon=true)`: a daemon ending blocked on a receive does not make the execution blocked. A daemon cannot carry a role in this API, because `spawnDaemon` and `MpThread(Runnable, boolean)` take no role. So `n0..n2` exist only as replica indices 0..2, not as role names, and `rolesInSpawnOrder() = visibleRoles() = [c0, c1]`. Main awaits only the clients. Awaiting a node would block forever, and the interface's "waits for them" cannot hold for processes that never end.
- **D1b. The `MpThread` constructor is used directly.** Main creates the thread, records its `Tid` (`getTaskId`) and only then calls `start()`. `MpProcess.spawn` returns the Tid only after the start. Under that ordering a started client could send a request, and its node could reply to it, before main had stored the client's Tid. Every process therefore knows every address from the start, as the interface asks, without a Setup message. Main raises no send or receive.
- **D2. RMW modification and reply value.**
  - The code computes `newValue := r.data[key].Value + 1 // TODO: update RMW modify` (l. 483). The port uses the interface's f: `data.Value + 10`.
  - The code's reply to the client carries `Value: state.NIL` (l. 595, in `executeRMWs`). The port replies `Result(data.value)`, taking the replica's data at the point where the operation completes: the `RMWSetReply` quorum in `handleRMWSetReply` (l. 576-580).
  - In the code, `executeRMWs` is a separate goroutine that polls `rmwDoneUpTo` (l. 584-606, 718-719). The port runs its inner loop synchronously at that point. This is the clock and scheduling I was told to ignore.
  - Consequence: if a `Set` or `RMWSet` with a larger tag reached the coordinator between its `RMWGetReply` quorum and its `RMWSetReply` quorum, `Result` reports the coordinator's data at completion. That need not equal f applied to the value it read. The rejected alternative was to reply with the payload sent in `RMWSet`.
- **D3. Read and Write replies.** `replyClient` (l. 168-179) replies `Value: state.NIL` for every ABD operation. The port sends `Ack` for a PUT. For a GET it sends `Value(data.value)`, the coordinator's data at the `replyClient` call. That call comes either on the optimized path in `handleGetReply` (l. 303-306) or at the `SetReply` quorum (l. 386-390).
- **D4. An Rmw at `n1` is never answered by the code.** `executeRMWs`, the only place that answers an RMW, is started only on replica 0 (l. 718-719: `if r.Id == 0 { go r.executeRMWs() }`). An Rmw proposed at `n1` (client `c1`) completes its quorum in `handleRMWSetReply`, but no reply is ever sent. The port answers `Result(data.value)` at that completion point (`rmwDoneUpTo++`, l. 579) on every replica other than 0. On replica 0 the port runs the `executeRMWs` loop at that point.
- **D5. `Dreply` is taken to be true.** Both reply sites require `r.Dreply` (l. 170, 591; genericsmr.go l. 55, set from the command line). With `Dreply` false no client is ever answered.
- **D6. A Write does not write the client's value. This is kept from the code and should be noted.** `handlePropose` never reads `propose.Command.V` (l. 650-651). `bcastGet(write=true)` sends `Payload{}` (l. 206). The write path sets `r.data[key] = Payload{Tag: newTag, Value: r.data[key].Value}` (l. 317): a new tag with the old value. The port does the same, so `Write.value` is received and ignored. The prompt's overrides cover reply values, f and unanswered clients, not this, so I kept the code's behaviour. The consequence is that a key's value changes only through Rmw (+10).
- **D7. Shared instance numbering is kept.** `handleRMWGet` and `handleRMWSet` index the receiver's own `instanceSpace` by the sender's instance number (l. 423, 527). They create the entry when it is absent (with `lb = nil`) and update it when it exists, even when the existing entry is the receiver's own ABD or RMW instance. `handlePropose` skips occupied numbers (l. 611-613). Ported as is, as a per-node `Map<Integer, Instance>`.
- **D8. Code oddities kept verbatim.**
  - `handleRMWGetReply` assigns `rmwGetReply.Payload`, not the loop's `data` (l. 473).
  - An `RMWGet` on an unknown instance replies with an empty payload (l. 438).
  - The read's `identicalCount` and initialTag check (l. 279-296).
  - `handleSetReply`'s compound quorum test (l. 386-387).
  - `rmwDoneUpTo++` (rather than being set to `rmwId`).
- **D9. The quorum is one reply plus self**, as written: `x+1 > r.N>>1` with N = 3 (l. 275, 386-387, 465, 577).
- **D10. Paxos parts that are absent are not added.** The interface mentions Multi-Paxos for RMWs. The code has no Prepare, Commit or leader forwarding on this path, only RMWGet and RMWSet with ballot 0, so none are added. The `panic("outdated ballot received")` branches become `AssertionError`. They cannot fire, since every ballot and `defaultBallot` is 0.
- **D11. Nondeterminism comes only from message arrival.** The Go `select` over channels (l. 731) becomes one receive of any message. Which message a node handles next is the receive's choice, made by the strategy under the delivery model. The `onOffProposeChan` gating (l. 726, 733-735, 761) belongs to the clock and batching, which I was told to ignore. Each node has at most one pending client request anyway, because clients are sequential. No other choice exists, so `Nondet` is not used.
- **D12. Representation choices.**
  - One key (`KEY = 0`). `map[int]Payload` becomes a presence flag plus a `Payload`, with the zero `Payload` when absent, because `handleGet` (l. 236) and the GET case of `handlePropose` (l. 653) test presence.
  - `Command []state.Command` in `RMWGet` and `RMWSet` is carried as its only used field, the key (`cmdKey`).
  - `ReplicaID` and `LeaderId` are replica indices 0..2, mapped to the nodes' Tids.
  - Ignored: persistence (`recordInstanceMetadata`, `recordCommands`, `sync`), `Alive`, the master RPC, networking and `CommitShort`/`Prepare`/`Commit` (unused here).
- **D13. Program metadata.**
  - `fifo` maps to `CommunicationModel.P2P` and `bag` to `ASYN`. Every send and receive uses that model.
  - `programName() = "pineapple"` and `size() = "<q>"`. The interface fixes neither, so these are my choices.

## Compile (inside OUT_DIR)

    javac -d java/out -cp "$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')

Result: exit 0, with no output.

## The one run (inside OUT_DIR)

    java -Xss4m -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.pineapple_code.PineappleCode:impl,1,fifo' must 0 1 report - 600 </dev/null

Exit 0. Summary lines: `ran: 325 iterations`, `blocked: 0`, `graphs: 325 distinct complete, 0 distinct blocked; stack exhausted`, `stop_reason: exhausted`, `elapsed: 2.426 s`.
Last output line: `max_porf_count: 0`
