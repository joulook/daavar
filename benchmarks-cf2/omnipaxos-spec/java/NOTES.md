# OmniPaxosSpec: a replicated log over three nodes, written against JMC's message-passing API

Source: `java/bench/omnipaxos_spec/OmniPaxosSpec.java`, a single file. The entry is `impl,<e>,<t>,<cand>,<fifo|bag>`.

## Processes

| role | count | body |
|---|---|---|
| `n0`, `n1`, `n2` | 3 | the protocol node loop below; it never ends |
| `c0`, `c1` | 2 | the client, exactly as in INTERFACE.md (`c0` -> `n0`, `c1` -> `n1`) |
| main | 1 | creates the five threads, starts them (nodes in index order, then clients), awaits clients, then awaits nodes; sends and receives nothing |

Every send and receive uses one communication model: `fifo` = `CommunicationModel.P2P`, `bag` = `CommunicationModel.ASYN`.

## Node state

- `prom`: the highest ballot promised (0 = none).
- `acc`: the ballot at which `log` was accepted (0 = none).
- `log`: the accepted log.
- `decided`: the decided prefix learned so far.
- `leaderBallot`: the ballot of the leader this node knows of (0 = no leader).
- `role`: one of FOLLOWER, CANDIDATE, LEADER.
- `pending`: client values held while no leader is known.
- `budget`: the remaining election timeouts. It is `t` for `n0 … n(cand−1)` and 0 for the other nodes.
- `acceptedLen[j]`: leader only. The longest log length that follower `j` has acknowledged at the current ballot.

Ballots: node `i` in round `r ≥ 1` uses ballot `b = 3r + i`. The owner of ballot `b` is `b mod 3`. On a timeout a node takes the smallest round whose ballot is larger than its `prom`: `b = (prom/3 + 1)·3 + i`.

## Node loop

```
loop:
  if leaderBallot = 0 ∧ budget > 0 ∧ Nondet.chooseBoolean():   // true = time out now
      budget--; timeout(); continue
  handle(recv())
```

"Has no leader" means `leaderBallot = 0`. This holds initially, while the node is a candidate, and after the node has promised a higher Prepare. Apart from this timeout choice, the node handles messages in the order the harness delivers them. It uses no other choice.

## Messages (records, with `@Tag` equal to the record name)

| message | fields | from -> to |
|---|---|---|
| `Propose` | `value:int` | client -> its node (shape fixed by INTERFACE.md) |
| `Committed` | `value:int` | `n0` -> `c0`, `n1` -> `c1` (shape fixed) |
| `Prepare` | `ballot` | candidate -> the two other nodes |
| `Promise` | `ballot, accBallot, log` | node -> owner of `ballot` |
| `Accept` | `ballot, log, decidedLen` | leader -> the two followers (the whole log each time) |
| `Accepted` | `ballot, from, len` | follower -> leader |
| `Decide` | `prefix` | leader -> the two followers (the whole decided prefix) |
| `Forward` | `ballot, value` | node -> the leader of `ballot` |

## Handlers

- **timeout()**: set `prom := b` (the new ballot), `role := CANDIDATE`, `leaderBallot := 0`. Send `Prepare(b)` to both other nodes. The candidate's own state counts as its own promise.
- **Prepare(b)**: if `b > prom`, set `prom := b`, `role := FOLLOWER`, `leaderBallot := 0`, and send `Promise(b, acc, log)` to the owner of `b`. Otherwise ignore it. There is no NACK.
- **Promise(b, a, l)**: if `role = CANDIDATE` and `b = prom`, the node itself and the first promiser form a majority (2 of 3). The node becomes leader:
  1. Adopt `l` if `(a, |l|) > (acc, |log|)` in lexicographic order.
  2. Check that the adopted log extends `decided` (an `AssertionError` otherwise).
  3. Set `role := LEADER`, `acc := leaderBallot := b`, and clear `acceptedLen`.
  4. Append `pending` in order.
  5. Send `Accept(b, log, |decided|)` to both other nodes.

  Promises that arrive later, or for a stale ballot, are ignored.
- **Propose(v)** from the node's own client:
  - leader: append `v` (see below);
  - a leader is known: send `Forward(leaderBallot, v)` to that leader;
  - otherwise: add `v` to `pending`.
- **Forward(b, v)**: append `v` only if `role = LEADER` and `b = prom`. Otherwise drop it (see decision D4).
- **append(v)** (leader): add `v` to `log` and send `Accept(prom, log, |decided|)` to both other nodes.
- **Accept(b, l, d)**: if `b < prom`, or `b = prom` while this node is not a FOLLOWER (that is, `b` is this node's own ballot), ignore it. Otherwise:
  1. Set `prom := b`, `role := FOLLOWER`, `leaderBallot := b`.
  2. Replace `log` with `l` and set `acc := b`, but only if `acc ≠ b` or `|l| > |log|`. Logs accepted at one ballot are prefixes of one another, so a late, shorter Accept never shortens the log.
  3. Send `Accepted(b, self, |log|)` to the owner of `b`.
  4. Learn `log[0..d)`.
  5. If `b` is a newly learned leader, send every pending value to it as `Forward(b, v)`, in order, and clear `pending`.
- **Accepted(b, j, n)**: only the leader of ballot `b` handles it. It sets `acceptedLen[j] := max(acceptedLen[j], n)` and computes `dec = max over followers j of acceptedLen[j]`. The leader plus one follower is a majority, so `log[0..dec)` is chosen. If `dec > |decided|`, the leader learns `log[0..dec)` and sends `Decide(log[0..dec))` to both other nodes.
- **Decide(p)**: learn `p`.
- **learn(p)**: check that `p` and `decided` are prefix-consistent (an `AssertionError` otherwise). Append each new entry to `decided`. For each new entry, `n0` sends `Committed(v)` to `c0` and `n1` sends `Committed(v)` to `c1`, in decided order. `n2` has no client.

## Decisions (where the requirements are silent)

- **D1. Lifecycle.** The nodes are ordinary (non-daemon) processes with roles `n0`, `n1`, `n2`. Main awaits the clients first and then the nodes. A node never ends, so every execution ends blocked. This matches the spec side, where `log` never ends.
- **D2. Addresses.** A node must know its client's address from the start, but the clients are spawned after the nodes. So main first constructs all five `MpThread`s; construction mints the task ids and raises the creation events in the order n0, n1, n2, c0, c1. Main then fills the address arrays, and only after that starts the threads in the same order. I did not use `MpProcess.spawn`, because it constructs and starts a thread in one step.
- **D3. One message model.** All messages use the model the argument selects, the internal ones included. The program has a single `model()`.
- **D4. Forward carries the target ballot and is never re-routed.** A forward that reaches a node which is no longer the leader of that ballot is dropped. Re-routing it, or accepting it at a later ballot of the same node, could commit a client's values out of order. The spec allows a proposal to be lost.
- **D5. Every log message carries the whole log.** `Accept` carries the whole log and `Decide` carries the whole decided prefix, instead of a single entry at an index. As a result the protocol is correct under `bag` (any reordering) without gap handling. A stale Accept is harmless (see the Accept handler, step 2).
- **D6. No NACK, no leader lease, no heartbeats.** A node that has a leader never times out. A node keeps promising strictly higher ballots, so a leader can be preempted by a candidate that still has budget.
- **D7. Majority of two.** A candidate becomes leader on its first Promise, because it counts itself. A leader decides as soon as one follower has acknowledged a length.
- **D8. Values that arrive while there is no leader.** Client values that arrive while a node knows no leader are buffered in `pending`. A candidate that wins appends them. A follower forwards them to the first leader it learns of.
- **D9. Timeout choice.** Before every receive, a node with no leader and a positive budget makes `Nondet.chooseBoolean()`. `true` (the min option) means it times out now.
- **D10. Safety assertions.** The two safety checks in learn and in step 2 of becoming leader throw `AssertionError`. They are protocol sanity checks; they should never fire, and none fired in the run below.
- **D11. Program name and size.** `programName() = "omnipaxos-spec"` and `size() = "e=<e>,t=<t>,cand=<cand>"`. The interface does not fix either value, so I chose them; they may need to match the spec side's.
- **D12. Visible and declared roles.** `visibleRoles() = [c0, c1]` and `rolesInSpawnOrder() = [n0, n1, n2, c0, c1]`.

## Compile (run inside this directory, `daavar/benchmarks-cf2/omnipaxos-spec/java`)

```
javac -d java/out -cp "$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

It compiled without errors or warnings.

## The one run (plain exploration, e = 1, t = 1, cand = 1, fifo)

```
java -Xss4m -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.omnipaxos_spec.OmniPaxosSpec:impl,1,1,1,fifo' must 0 1 report - 600 </dev/null
```

The run exited with status 0. Its result lines:

```
ran:        2701 iterations
blocked:    2701 (counted by the strategy)
graphs:     0 distinct complete, 2701 distinct blocked; stack exhausted
```

There were no assertion failures and no exceptions. Every execution ends blocked, as intended (D1). The run created an empty `report/` directory, which I removed afterwards. The last output line, the end of the measurements block, was:

```
max_porf_count: 0
```
