# PineapplePaper: Pineapple single-key protocol, implemented from the paper (JMC message-passing API)

Source: `java/bench/pineapple_paper/PineapplePaper.java` (package `bench.pineapple_paper`, class
`PineapplePaper`, implements `org.mpi_sws.jmc.mp.launch.MpProgram`, constructed from `impl,<q>,<fifo|bag>`).
Scope: section 3 of the paper, single key, common case: stable leader n0, no failure, no leader change, no
reconfiguration, no multi-key (Appendix A not implemented).

## Processes

| role | spawned | ends? | visible |
|---|---|---|---|
| `n0` | 1st | never (loops on a blocking receive) | no |
| `n1` | 2nd | never | no |
| `n2` | 3rd | never | no |
| `c0` | 4th | after q replies | yes |
| `c1` | 5th | after q replies | yes |

Main spawns n0, n1, n2, c0, c1 with `MpProcess.spawn(role, body)`, then awaits c0, c1, n0, n1, n2. It sends
and receives nothing. The nodes never finish, so every execution ends with the nodes, and main awaiting n0,
blocked.

Communication model: `fifo` gives `CommunicationModel.P2P`, `bag` gives `CommunicationModel.ASYN`. Every send and
receive uses that model.

## Client ci (i = 0, 1), attached to node ni

For j = 0 … q-1, kind = (i + j) mod 3:
- kind 0: send `Write{client: i, value: 1 + i + 2j}` to ni and receive; the reply must be `Ack{}`.
- kind 1: send `Read{client: i}` to ni and receive; the reply must be `Value{value}`.
- kind 2: send `Rmw{client: i}` to ni and receive; the reply must be `Result{value}`.

Any other reply raises `AssertionError` (the Rust client's `panic!`).

## Node state (node ni)

- `Storage_i[key]`: `storedValue` (int, initially 0) and `storedPs` (pstamp, initially ((0,0),0)).
- `ops`: operations this node is proxying, by a node-local operation id.
- n0 only: `slot` (the leader's slot index slot_i, initially 0), a FIFO queue of pending RMWs, and a busy flag.

Pstamp = ((ts, id), slot). Comparison is lexicographic on (ts, id, slot), as in section 3.1. Quorum = ⌊3/2⌋ + 1 = 2.

## Messages among nodes

`op` is the proxy's operation id. `from` is the sender's node index, because a receiver cannot see who sent a
message.

| message | paper | sent by → to | fields |
|---|---|---|---|
| `GetTag` | ⟨GET-TAG, key⟩ | writer proxy → all 3 nodes (including itself) | op, from |
| `AckTag` | ⟨ACK-TAG, key, ps_j⟩ | node → proxy | op, from, ts, id, slot |
| `GetValue` | ⟨GET-VALUE, key⟩ | reader proxy or leader (RMW) → all 3 nodes | op, from |
| `AckValue` | ⟨ACK-VALUE, key, value_j, ps_j⟩ | node → proxy | op, from, value, ts, id, slot |
| `Put` | ⟨PUT, key, ps, value⟩ | proxy or leader → all 3 nodes | op, from, ts, id, slot, value |
| `AckPut` | ⟨ACK-PUT, key⟩ | node → proxy | op, from |
| `RmwFwd` | (not in the paper; D5) | n1 or n2 → n0 | node, client |
| `RmwDone` | (not in the paper; D5) | n0 → the attached node | client, value |

## Background handlers at every node (Algorithm 1, lines 18–26)

- `GetTag{op, from}`: reply `AckTag{op, i, storedPs}` to n_from.
- `Put{op, from, ps, v}`: if `storedPs < ps`, set `storedPs = ps` and `storedValue = v`. Then always reply
  `AckPut{op, i}` to n_from.
- `GetValue{op, from}`: reply `AckValue{op, i, storedValue, storedPs}` to n_from.

## ABD write at proxy ni (Algorithm 1, lines 1–8), on `Write{client, v}`

1. Send `GetTag` to n0, n1, n2.
2. On the 2nd `AckTag`, Q is the first two repliers. Let psmax = ((ts, id), slot) be the largest pstamp in Q.
   Set psnew = ((ts + 1, i), slot).
3. Send `Put{psnew, v}` to n0, n1, n2.
4. On the 2nd `AckPut`, send `Ack{}` to c_client.

## ABD read at proxy ni (Algorithm 1, lines 9–17), on `Read{client}`

1. Send `GetValue` to n0, n1, n2.
2. On the 2nd `AckValue`, Q is the first two repliers. Take psmax, the largest pstamp in Q, and vmax, the value
   that came with it.
3. Send `Put{psmax, vmax}` to n0, n1, n2. This is the write-back.
4. On the 2nd `AckPut`, send `Value{vmax}` to c_client.

## RMW at the leader n0 (Algorithm 2), f(v) = v + 10

The RMW comes from `Rmw{client}` received by n0 (origin 0), or from `RmwFwd{node, client}` (origin = node). It is
appended to the leader's queue. One RMW runs at a time (D4).

1. Send `GetValue` to n0, n1, n2.
2. Wait until at least 2 `AckValue` have arrived and one of them is n0's own (l ∈ Q). Q is every reply received
   by then, so 2 or 3 replies.
3. From Q take vmax, the value with the largest pstamp psmax. Set vnew = vmax + 10.
4. Set slot = slot + 1 and psnew = (psmax.tag, slot).
5. Send `Put{psnew, vnew}` to n0, n1, n2.
6. On the 2nd `AckPut`: if origin = 0, send `Result{vnew}` to c0. Otherwise send `RmwDone{client, vnew}` to
   n_origin. Then start the next queued RMW.

## Rmw from a client attached to a non-leader node

- n1 (or n2) receives `Rmw{client}` and sends `RmwFwd{node: i, client}` to n0.
- n0 runs the RMW as above and sends `RmwDone{client, vnew}` to n_i.
- n_i sends `Result{vnew}` to c_client.

## Late replies

A reply that arrives after its phase has reached its quorum is received and dropped. This covers the 3rd
`AckTag` or `AckValue`, and the 3rd `AckPut`. The operation is identified by `op`.

## Decisions where the paper is silent

- **D1. Messages to self.** Paper: "send ⟨GET-TAG, key⟩ to all" / "send ⟨PUT, key, psnew, value⟩ to all".
  "All" is read as all three nodes, the proxy included. The proxy's own storage is reached by a message to
  itself and answered by its own background handler, never read directly. As a result the ABD quorum Q is the
  first two replies in arrival order, and it may leave out the proxy ("the leader might not be in the quorum
  for an ABD write").
- **D2. Which replies form Q.** Paper: "wait until receiving … from quorum Q". For ABD, Q is the first 2
  replies, and later ones are dropped. For an RMW, the paper says "Contact a quorum Q, which includes the leader
  itself" and "l ∈ Q". The leader waits until it has at least 2 replies, its own among them. Q is then every
  reply received so far (2 or 3). A third reply that arrives before the leader's own is therefore kept in Q.
- **D3. What slot_i is.** Paper: "psnew ← (psmax.tag, slot_i + 1)" and "the leader can update the pstamp by
  incrementing the local slot index slot_i directly". slot_i is a counter at the leader that starts at 0 and
  goes up by 1 for each RMW it starts, at line 6. It is not taken from psmax.slot. Only the leader creates slot
  values, so slot_i is never smaller than any slot in the system, and the first RMW gets slot 1, as Appendix
  B.2 assumes.
- **D4. RMWs at the leader run one at a time.** The paper does not say whether the leader may run RMWs
  concurrently. Paper: "in the common case with a stable leader, the leader has the most recent information
  about prior RMWs". The leader starts an RMW's get phase only after the previous RMW's put phase has 2
  `AckPut`. Waiting requests sit in a FIFO queue in order of receipt. Writes and reads at n0 do not wait for
  RMWs, and RMWs do not wait for them.
- **D5. How an Rmw at a non-leader reaches the leader and how the result gets back.** Paper: "RMW takes 3-RTT if
  the client is not co-located with the leader". The paper names no message for this. The attached node
  forwards (`RmwFwd`) to n0. n0 returns the result to the attached node (`RmwDone`), which replies `Result` to
  the client. The leader never sends to a client it is not attached to.
- **D6. No fast path.** Paper: "Pineapple also adopts various optimizations mentioned in [10, 25, 42] … ABD can
  complete on the fast path when the quorum returns the same response." Reads always run both rounds of
  Algorithm 1: get, then a write-back put. This follows the task's "the rounds of an ABD read" and Algorithm 1
  as printed.
- **D7. Writer id.** Paper: "psnew ← ((tagmax.ts + 1, i), slotmax)". id is the node index i (0, 1 or 2). Every
  written tag has ts ≥ 1, so tag (1, 0) from n0 is never confused with the initial tag (0, 0).
- **D8. Initial state.** Paper: "tag_r is (0, 0) and slot_r = 1, meaning that this RMW is the first non-read
  operation". The initial value is 0 (INTERFACE.md) and the initial pstamp is ((0, 0), 0).
- **D9. Ties.** When several replies carry the same largest pstamp, the first one received is used. A pstamp
  identifies one value, so the choice does not change the result.
- **D10. Nondet.** No choice is left to `Nondet`. Every open choice (which replies form Q, the order of RMWs in
  the leader's queue) is settled by the order in which messages arrive. The checker explores that order.
- **D11. Addresses.** INTERFACE.md: "Assume every process knows every other process's address from the start."
  Main writes each node's `Tid` into a shared array right after that node's spawn returns, and every node is
  spawned before any client. Each client writes its own `Tid` (`MpProcess.self()`) into a second array before its
  first send. Neither write is a message event.
- **D12. Main's await order.** INTERFACE.md says main "waits for them" without an order. Main awaits c0, c1,
  then n0, n1, n2.
- **D13. `programName()` and `size()`.** The interface fixes neither. They return `"pineapple_paper"` and the
  text of q.

## Compile (inside this directory)

    javac -d java/out -cp "$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')

Result: exit 0, no output.

## The one run (inside this directory)

    java -Xss4m -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.pineapple_paper.PineapplePaper:impl,1,fifo' must 0 1 report - 600 </dev/null

Last output line: `max_porf_count: 0`

Summary of that run's output: `ran: 90179 iterations`, `blocked: 90179`, `completed: 0`,
`graphs: 0 distinct complete, 90179 distinct blocked; stack not exhausted`,
`error: Halting execution: 90179 due to timeout`, `stop_reason: timeout`, `elapsed: 600.022 s`, exit status 0.
Every execution ends blocked, as expected, because the nodes never terminate. The enumeration did not finish
within the 600 s timeout.
