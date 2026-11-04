# OmniPaxosPaper: notes

Source: `java/bench/omnipaxos_paper/OmniPaxosPaper.java` (one file). It implements Sequence Paxos (§4, Figure 3b)
and Ballot Leader Election (§5, Figure 4) from PAPER-EXCERPT.md. It does not implement reconfiguration, recovery
(⑩ ⑪) or reconnection (⑫). The program interface is the one in INTERFACE.md. Circled numbers below are the
handler numbers in the figures.

## Processes and the API

| Interface | Code |
|---|---|
| main | `run()`: `MpProcess.spawn` with roles n0, n1, n2, c0, c1 in that order. It checks that the task ids are consecutive, awaits the clients, then awaits the nodes. Awaiting a node blocks main, so every execution ends blocked with the nodes in `recv`. |
| client `ci` | `client(i)`: `Channel.send(Propose(1+i+2j))` to `n_i` for j = 0..e−1. Then it calls `Channel.recv(Committed.class)` until each of its own values has been seen once. Other values are read and not counted. |
| node `n_i` | `Node.run()` loops forever. When the node can time out (budget > 0, BLE leader `l` still ⊥, no heartbeat round running), `Nondet.chooseBoolean()` decides between timing out (`startRound()`) and receiving. Otherwise the node receives the next message with `Channel.recv` and handles it in arrival order. |
| model | `fifo` → `CommunicationModel.P2P`, `bag` → `ASYN`. Every send and receive uses this one model. |
| MpProgram | `programName()` = `omnipaxos_paper`, `size()` = `e=<e>,t=<t>,cand=<cand>`. Roles are n0 n1 n2 c0 c1; the visible roles are c0 and c1. Every message is a record with a `@Tag`. |

Node-to-node message records all start with `(from, seq)`: the sender's index and a sequence number per link
(D7). They are `HBRequest(rnd)`, `HBReply(rnd, ballot, qc)`, `Prepare(n, accRnd, logIdx, decIdx)`,
`Promise(n, accRnd, logIdx, decIdx, sfx)`, `AcceptSync(n, sfx, syncIdx)`, `Accept(n, c)`,
`Accepted(n, logIdx)`, `Decide(n, decIdx)` and `Forward(value)`. `Ballot(n, pid)` is ordered lexicographically
and is also the Sequence Paxos round.

## Mapping, role by role and message by message

### Ballot Leader Election (Figure 4), every node
- State: `l` (initially ⊥ = (0,0)), `r` = 0, `b` = (0, pid), `qc` = true, `ballots` (keyed by sender).
  `inRound` is added to mark that a round is open.
- "When starting: send ⟨HBRequest, r⟩ to all peers and startTimer(delay)" and ② steps 3–4 are both
  `startRound()`. It spends one budget unit, clears `ballots`, inserts the node's own (b, qc) (D3) and sends
  `HBRequest(r)` to both peers.
- ③ ⟨HBRequest⟩ is `onHBRequest`: it replies `HBReply(rnd, b, qc)`. Every node answers, including nodes
  with budget 0.
- ④ ⟨HBReply⟩ is `onHBReply`: if `rnd = r` (and the round is open), it inserts (ballot, q). When the round
  holds a majority, the round's timer fires (D4).
- ② "Upon timeout of startTimer" steps 1–3 are `timerFires()`. If |ballots| ≥ majority it calls
  checkLeader(), else it sets qc ← false. Then it clears `ballots` and sets r ← r+1. Step 4 (the next round)
  only happens as a new budget unit (D2).
- ① checkLeader() is `checkLeader()`. It takes the maximum ballot among those with qc = true. If max < l it
  sets b = (l.n+1, pid) and qc ← true. If max > l it sets l ← max and triggers ⟨Leader, max.pid, max⟩, which is
  a local call to `onLeader`.

### Sequence Paxos leader
- ② ⟨Leader⟩ from BLE is `onLeader(s, n)`. It acts only if s = self and n > promisedRnd. It then resets the
  leader's volatile state, sets state ← (LEADER, PREPARE), currentRnd ← n and promisedRnd ← n, inserts its own
  promise (acceptedRnd, |log|, self, decidedIdx, suffix(decidedIdx)), and sends
  `Prepare(currentRnd, acceptedRnd, |log|, decidedIdx)` to both peers.
- ④ ⟨Promise⟩ is `onPromise`. It returns if n ≠ currentRnd and otherwise inserts the promise.
  - In (LEADER, PREPARE) it runs P1–P7. P1 is the majority check. P2 picks maxProm by (accRnd, logIdx), with
    ties as in D5. P3 truncates to prefix(decidedIdx) if maxProm.accRnd ≠ acceptedRnd. P4 appends maxProm.sfx
    (D6). P5 appends the buffer, because stopped() is always false. P6 sets acceptedRnd ← currentRnd,
    accepted[self] ← |log| and state ← (LEADER, ACCEPT). P7 sends `AcceptSync(currentRnd, suffix(syncIdx),
    syncIdx)` to each promised follower, with syncIdx = p.logIdx if p.accRnd = maxProm.accRnd and p.decIdx
    otherwise.
  - In (LEADER, ACCEPT) it runs A1–A3 for a late promise: syncIdx (D8), then AcceptSync, then
    `Decide(currentRnd, decidedIdx)` if decidedIdx > decIdx.
- ⑥ Proposal is `onProposal(c)`, used for both a client `Propose` and a forwarded `Forward`. In
  (LEADER, PREPARE) it puts C in the buffer. In (LEADER, ACCEPT) it appends C, sets accepted[self] ← |log| and
  sends `Accept(currentRnd, C)` to every promised follower.
- ⑧ ⟨Accepted⟩ is `onAccepted`. It returns unless currentRnd = n and the state is (LEADER, ACCEPT). It sets
  accepted[f] ← logIdx. If logIdx > decidedIdx and a majority (counting the leader) has accepted ≥ logIdx, it
  sets decidedIdx ← logIdx and sends `Decide(currentRnd, decidedIdx)` to every promised follower.

### Sequence Paxos follower
- ③ ⟨Prepare⟩ is `onPrepare`. It returns if promisedRnd > n. Otherwise it sets state ← (FOLLOWER, PREPARE)
  and promisedRnd ← n, and computes sfx: suffix(decIdx) if acceptedRnd > accRnd, suffix(logIdx) if they are
  equal, [] otherwise. It sends `Promise(n, acceptedRnd, |log|, decidedIdx, sfx)`. Then D9 and D10 apply
  (forwarding).
- ⑤ ⟨AcceptSync⟩ is `onAcceptSync`. It returns unless promisedRnd = n and the state is (FOLLOWER, PREPARE).
  It sets acceptedRnd ← n and state ← (FOLLOWER, ACCEPT), sets log ← prefix(syncIdx) ++ sfx, and sends
  `Accepted(n, |log|)`.
- ⑦ ⟨Accept⟩ is `onAccept`. It returns unless promisedRnd = n and the state is (FOLLOWER, ACCEPT). It
  appends C and sends `Accepted(n, |log|)`.
- ⑨ ⟨Decide⟩ is `onDecide`. If promisedRnd = n and the state is (FOLLOWER, ACCEPT), it sets
  decidedIdx ← decIdx (D11).
- ⑥ at a follower forwards the proposal to the leader (D9).

### Learning (every node)
- `learn(idx)` is the only place decidedIdx changes. n0 and n1 send `Committed(log[i])` to their client for
  every newly decided index, in log order. n2 has no client.

## Decisions where the paper is silent

- **D1, pid assignment.** Paper: "b ballot number. Initially set to (0, pid)" and "Every ballot b = (s, pid)
  is unique as pid is the unique identifier of each server". The paper does not say which server gets which
  pid. Here pid(n_i) = 3 − i, so n0 has pid 3, n1 pid 2, n2 pid 1. The ⊥ ballot (0,0) lies below every real
  ballot, so every node can win against the initial promisedRnd. Consequence: with cand = 1, n0 (the only
  candidate) always elects itself. With pid(n_i) = i+1, n0 would elect n1 or n2, which never run BLE, and
  nothing would ever be decided.
- **D2, timeout = one heartbeat round.** Paper: "In BLE, all servers periodically exchange heartbeats" and
  "Upon timeout of startTimer … send ⟨HBRequest, r⟩ to all peers, startTimer(delay)". The rounds are periodic
  and unbounded in the paper. The interface bounds them. One budget unit opens one round. The node has no
  timer and starts no further round except by spending another unit. It may time out only while it "has no
  leader", which here means its BLE leader `l` is still ⊥. Sequence Paxos promises do not count as having a
  leader.
- **D3, own ballot.** Paper: "send ⟨HBRequest, r⟩ to all peers" and "if |ballots| ≥ majority then
  checkLeader()". The paper does not say whether the server's own ballot is in `ballots`. Here it is,
  inserted when the round starts, so a majority is the node itself plus one reply.
- **D4, when the round's timer fires.** Paper: "delay: the duration a server waits for heartbeat replies
  within a single round" and "we assume that there are some stable periods such that connected servers will
  learn a period T_delay where no late heartbeat is received". There is no time in the model. The timer fires
  as soon as the round holds a majority of ballots (the earliest point at which ② reaches checkLeader). A
  later reply of that round has rnd ≠ r and is ignored, as in ④ ("A late heartbeat is simply ignored"). In
  effect, the peer whose reply arrives first decides what the candidate sees. As a result, the qc ← false
  branch and increment(b) are written but never run in this setting.
- **D5, ties in maxProm.** Paper: "maxProm ← the value with highest accRnd in promises (and highest logIdx if
  equal)". The paper does not say how to break a tie on both. Here a tie goes to the leader's own promise,
  then to the lowest node index. Equal accRnd and equal logIdx mean equal logs, so the choice does not change
  the log.
- **D6, P4 when maxProm is the leader's own promise.** Paper: own promise "(acceptedRnd, |log|, self,
  decidedIdx, suffix(decidedIdx))", then "P3. if maxProm.accRnd ≠ acceptedRnd then log ← prefix(decidedIdx)
  P4. append maxProm.sfx to the log". If the leader's own promise is maxProm, then accRnd = acceptedRnd, P3
  does nothing, and a literal P4 would append suffix(decidedIdx) a second time. Here P4 is skipped when
  maxProm is the leader's own promise, which is the follower rule "suffix(logIdx) if acceptedRnd = accRnd"
  applied to the leader itself (an empty suffix).
- **D7, FIFO links.** Paper: "By using session-based FIFO perfect links and log synchronization, the
  correctness is preserved even by only sending suffixes" and "replicated to the promised followers in FIFO
  order". The protocol assumes FIFO links between servers. Every node-to-node message therefore carries a
  sequence number per link, and the receive guard accepts only the next expected number from each sender.
  Under `fifo` (P2P) this changes nothing. Under `bag` (ASYN) it rebuilds the paper's FIFO link on top of the
  bag. The order across senders, and the order of the clients' `Propose`/`Committed` messages (fixed shape, no
  sequence number), stay free under `bag`.
- **D8, A1's syncIdx for a late promise.** Paper: "let syncIdx ← if accRnd = maxProm.accRnd then
  maxProm.logIdx else decIdx", then the follower does "log ← prefix(syncIdx)". prefix is defined only for idx
  ≤ |log| ("the entries in the log with index {0..idx-1}"). A late follower with the same accRnd can have a
  log shorter than maxProm.logIdx, which would leave a gap. Here syncIdx = min(logIdx, maxProm.logIdx) in
  that case. That is the follower's own length when it is shorter, as in P7, and maxProm.logIdx when it is
  longer, so that its stale entries from that round are cut off.
- **D9, forwarding.** The task names "forwarding of a follower's proposals to the leader". Figure 3b ⑥ has no
  follower case, and the paper's leader notification is "trigger ⟨Leader, max.pid, max⟩", which SP uses only
  when s = self. Here a follower forwards a client proposal (`Forward(value)`) to the leader of its
  promisedRnd. If it has promised no one yet (promisedRnd = ⊥), it holds the proposal in `pending` and
  forwards it when it promises a leader (in ③). If it becomes leader itself, `pending` joins its buffer (in
  ②). A forwarded proposal is handled at its receiver by ⑥ like a client proposal, so it is forwarded again
  if that node is no longer leader. Rounds only increase, so this cannot loop.
- **D10, a demoted leader's buffer.** Paper: "buffer: buffer for client requests received during the prepare
  phase" (volatile state of leader). ③ does not say what happens to it. Here a leader that promises a higher
  round forwards its buffer to the new leader. Entries already in its log are not re-forwarded: they survive
  only if the new leader adopts them in the Prepare phase. If they are lost, the client waits forever and
  that execution ends blocked. There are no client retries, and the paper describes none.
- **D11, monotone decidedIdx.** Paper ⑨: "decidedIdx ← decIdx". `learn` only raises decidedIdx. Under FIFO
  links (D7) decIdx never decreases within a round, so this changes nothing there. A node with a client
  reports each newly decided entry once, in log order (INTERFACE.md).
- **D12, addresses.** INTERFACE.md: "Assume every process knows every other process's address from the
  start". The API hands out a `Tid` only at spawn. The runtime's task ids are consecutive for consecutive
  spawns but do not start at a known number. A first attempt with task id = position + 1, which is what the
  MpProgram javadoc says, failed: n0 was task 2. So every process computes base = self().raw() − its spawn
  position, and main checks that the five ids are consecutive. Messages name nodes by index, never by `Tid`.
- **D13, not implemented:** stopped() (always false), reconfiguration, PrepareReq/Recovery (⑩ ⑪) and
  Reconnected (⑫). No node fails or disconnects in this setting.
- **D14, Promise outside the leader role.** Paper ④: "return if n ≠ currentRnd", then insert. A promise
  for an old round of a demoted leader is still inserted, as written, and has no further effect.

## Compile and run (inside daavar/benchmarks-cf2/omnipaxos-paper/java)

```
javac -d java/out -cp "$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
java -Xss4m -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.omnipaxos_paper.OmniPaxosPaper:impl,1,1,1,fifo' must 0 1 report - 600 </dev/null
```

The program compiled cleanly with javac 17. The run, with the fixed version (D12), took 120.5 s, exited 0 and
printed:
`graphs:     0 distinct complete, 10945 distinct blocked; stack exhausted` (10945 iterations, all blocked as
expected, because the nodes never end).

Last output line: `max_porf_count: 0`

The run left an empty `report/` directory here.
