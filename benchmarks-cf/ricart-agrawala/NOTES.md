# Ricart–Agrawala (Java, JMC message-passing API)

Source: `java/bench/ricart_agrawala/RicartAgrawala.java`, class `bench.ricart_agrawala.RicartAgrawala`
(implements `org.mpi_sws.jmc.mp.launch.MpProgram`, constructed from `impl,<N>,<fifo|bag>`).

## Roles and processes

- Main: creates the 2N processes as `MpThread`s with roles `n0..n{N-1}`, then `c0..c{N-1}` (this is the
  spawn order), records each process's `Tid` from `getTaskId()`, then starts them in the same order and
  awaits them all (`join1`). It sends and receives nothing.
- Client `ci`: `send(n_i, Enter(i))`; `recv(Granted)`; `send(n_i, Exit(i))`; `recv(Released)`.
- Node `ni`: one process running the paper's three processes as one loop that receives any message
  (`Channel.recv(model)`) and handles it atomically (which replaces the semaphore `Shared_vars`).

## Messages

| Message | Direction | Fields | Tag |
|---|---|---|---|
| `Enter` | client -> node | `from: int` | `Enter` |
| `Exit` | client -> node | `from: int` | `Exit` |
| `Granted` | node -> client | none | `Granted` |
| `Released` | node -> client | none | `Released` |
| `Request` | node -> node | `seq: int` (k), `node: int` (j) | `Request` |
| `Reply` | node -> node | none | `Reply` |

## Paper step -> code (in `node(...)`)

Shared database: `ourSequenceNumber`, `highestSequenceNumber` (initial 0), `outstandingReplyCount`,
`requestingCriticalSection` (initial false), `replyDeferred[N]` (initial false); `me` = node index, `N` = n.

- Process which invokes mutual exclusion (triggered by `Enter` from the client):
  `Requesting_Critical_Section := TRUE; Our_Sequence_Number := Highest_Sequence_Number + 1;
  Outstanding_Reply_Count := N - 1;` then `Send_Message(REQUEST(Our_Sequence_Number, me), j)` for every
  `j != me`, in order j = 0..N-1.
- `WAITFOR (Outstanding_Reply_Count = 0)` / "Critical Section Processing": when the count reaches 0
  (in the REPLY handler, or immediately when N = 1) the node sends `Granted` to its client; the node is
  in the critical section until the client's `Exit`.
- Process which receives REQUEST(k, j): `Highest := max(Highest, k)`;
  `Defer_it := Requesting AND ((k > Our_Sequence_Number) OR (k = Our_Sequence_Number AND j > me))`;
  if `Defer_it` then `Reply_Deferred[j] := TRUE` else `Send_Message(REPLY, j)`.
- Process which receives REPLY: `Outstanding_Reply_Count := Outstanding_Reply_Count - 1` (then the
  WAITFOR check above).
- Release (triggered by `Exit` from the client): `Requesting_Critical_Section := FALSE`; for j = 0..N-1,
  if `Reply_Deferred[j]` then `Reply_Deferred[j] := FALSE; Send_Message(REPLY, j)`; then `Released` to the
  client (after the deferred replies, as INTERFACE.md requires).

## Decisions where the paper is silent

1. "The three processes run asynchronously but operate on a set of common variables. A semaphore is used
   to serialize access to the common variables when necessary." -> one process per node (allowed by
   INTERFACE.md); each message is handled to completion before the next receive, so every handler is
   atomic and no semaphore is needed. Messages are handled in arrival order (one receive accepting any
   message); no `Nondet` choice is used, since the only open choice (which message first) is the
   receive's, made by the strategy.
2. "Highest_Sequence_Number ... ! The highest sequence number seen in any REQUEST message sent or
   received" -> the paper's code updates it only on receipt (`Maximum(Highest_Sequence_Number, k)`), not
   when the node sends its own REQUEST. I follow the code. This is unobservable here: K = 1, so a node
   chooses its number once.
3. REPLY carries no fields ("Send_Message (REPLY, j)"); the receiver only counts replies.
4. "Comment Critical Section Processing can be performed at this point;" -> the node's critical section
   is the interval between sending `Granted` and receiving `Exit`. `Requesting_Critical_Section` stays
   TRUE throughout (the paper resets it only at release), so a REQUEST arriving then is decided by the
   paper's rule as written; nothing is added to the rule.
5. When a node ends (the paper's processes run forever): after `Released` is sent and it has sent a
   REPLY to each of the N-1 other nodes' REQUESTs (K = 1, so each other node requests exactly once).
   At that point all N-1 REPLYs to its own REQUEST were received, so no message to it is left pending.
6. Order of sends in loops: REQUESTs and deferred REPLYs in increasing node index (the paper's
   `FOR j := 1 STEP 1 UNTIL N`).
7. Process addresses: main constructs all `MpThread`s first (task ids minted at construction) and only
   then starts them, so every process body has every address before any process runs. (With
   `MpProcess.spawn` a node could need a client's address before main had stored it.)
8. Program metadata (not fixed by INTERFACE.md): `programName()` = `ricart_agrawala`, `size()` = `N`
   (e.g. `3`); `fifo` -> `CommunicationModel.P2P`, `bag` -> `CommunicationModel.ASYN`; only the kind
   `impl` is accepted.
9. Unexpected message at a node -> `IllegalStateException` (cannot happen in this protocol).

## Commands

Compile (from this directory, i.e. OUT_DIR):

    javac -d java/out -cp "$(cat ../../../jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')

Result: compiled without errors or warnings.

One run (plain exploration, smallest parameters, from this directory):

    java -Xss4m -cp java/out:$(cat ../../../jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.ricart_agrawala.RicartAgrawala:impl,3,bag' must 0 1 report - 600 </dev/null

Exit status 0; `graphs: 32800 distinct complete, 0 distinct blocked; stack exhausted`
(32800 iterations, 320 s, stop_reason exhausted). Last output line:

    max_porf_count: 0

`java/out` and `report/` were removed afterwards.
