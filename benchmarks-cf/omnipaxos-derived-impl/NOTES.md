# OmniPaxosDerivedImpl: replicated log over three nodes (Java, JMC message-passing API)

Source: `java/bench/omnipaxos_derived_impl/OmniPaxosDerivedImpl.java` (one file).
Interface followed: INTERFACE.md (roles n0 n1 n2 c0 c1, `Propose { value }`, `Committed { value }`,
entry `OmniPaxosDerivedImpl(String...)` from `impl,<e>,<t>,<cand>,<fifo|bag>`).
Refinement target: `spec_log` of DERIVED-SPEC.rs.

## Notation

- Ballot `b = (round, node)`, ordered by round, then node index. `ZERO = (0, -1)`.
- Majority of 3 = 2 = the leader itself plus one other node.
- Per node state: `promised` (highest ballot promised, started or accepted), `accRnd` (ballot under
  which `log` was accepted), `log` (list of ints), `decided` (decided prefix learnt and delivered),
  `leader` (index or -1), `candidate` flag, `budget` (timeouts left), `pending` (own client's values
  received while no leader is known); leader only: `accLen[k]` (longest log length node k holds
  under the leader's ballot), `leaderCommit`.

## Roles

### main
Spawns n0, n1, n2, then c0, c1 (`MpProcess.spawn(role, body)`), checks that their task ids are
consecutive, awaits c0, c1, then n0, n1, n2. It sends and receives nothing. Awaiting a node never
returns (nodes end blocked on receive), so every execution ends blocked; that is the intended end.

### client ci (i = 0, 1)
Sends `Propose(v(i,j))`, `v(i,j) = 1 + i + 2j`, j = 0..e-1, in order, to `n_i`; then receives
`Committed` (typed receive) until each of its own values has been seen once; ends.

### node n_k (k = 0, 1, 2), never ends
Loop: if `leader == -1` and `budget > 0`, one `Nondet.chooseIndex(2)`: option 0 = receive,
option 1 = time out. Otherwise (or on option 0) one blocking `Channel.recv(model)` of any message,
handled as below. Budget: `t` for nodes n0..n(cand-1), 0 for the others.

- **Timeout**: `budget--`; `promised := (promised.round + 1, k)`; `candidate := true`;
  `leader := -1`; send `Prepare(promised)` to the two other nodes.
- **Propose(v)** (own client): leader → append `v`, broadcast Accept; leader known → send
  `Forward(v)` to it; no leader → append to `pending`.
- **Forward(v)**: if this node is the leader, append `v` and broadcast Accept; otherwise drop it.
- **Prepare(b)**: if `b > promised`: `promised := b`, `candidate := false`, `leader := -1`, send
  `Promise(b, accRnd, log, |decided|)` to `n_{b.node}`. Otherwise ignored (no nack).
- **Promise(b, accRnd', log', d')**: only if `candidate` and `b == promised` (else ignored). The
  first such promise completes a majority: adopt `log'` if `(accRnd', |log'|)` is greater than
  own `(accRnd, |log|)`; become leader (`leader := k`, `accRnd := b`), `leaderCommit :=
  min(max(|decided|, d'), |log|)`, append all `pending` values in order, send `Accept(b, log)` to
  both other nodes. Later promises for b are ignored (those nodes get the Accepts anyway).
- **Accept(b, log')**: if `b > promised`, `promised := b` (implicit promise); if `b < promised`
  ignored. Then adopt `log'` if `accRnd != b` or `log'` is longer; `accRnd := b`;
  `leader := b.node`; send `Accepted(b, k, |log|)` to the leader; if the leader just became known,
  flush `pending` (Forward each, in order).
- **Accepted(b, from, len)**: only at the leader of `b == promised`. `accLen[from] := max(...)`.
  If the largest `accLen` of another node exceeds `leaderCommit`, that prefix is decided:
  `leaderCommit := it`, learn `log[0..it)` locally and send `Decide(log[0..it))` to both others.
- **Decide(d)**: learn `d`.
- **learn(d)**: if `d` is longer than `decided` (an assertion checks that `decided` is a prefix
  of `d`), append the new entries to `decided` and, if this node has a client (n0 → c0, n1 → c1),
  send `Committed(v)` for each new entry in order. n2 has no client.

## Messages among nodes (all `@Tag` records, tag = record name)

| message | from → to | payload |
|---|---|---|
| `Prepare` | candidate → other nodes | `Ballot b` |
| `Promise` | node → candidate of b | `b, accRnd, log, decidedLen` |
| `Accept` | leader → other nodes | `b`, the leader's whole log under b |
| `Accepted` | node → leader of b | `b, from, len` |
| `Decide` | leader → other nodes | the decided prefix (whole list) |
| `Forward` | node → leader | one value of the sender's own client |

Client messages: `@Tag("Propose") record Propose(int value)`, `@Tag("Committed") record Committed(int value)`.

## Decisions (where the requirements are silent)

1. **Addresses**: each process computes the others' `Tid`s from its own id (`MpProcess.self()`)
   and its spawn position, since main spawns the five processes consecutively and task ids are
   minted consecutively; main checks this after spawning and throws if not.
2. **Model**: `fifo` → `CommunicationModel.P2P`, `bag` → `CommunicationModel.ASYN`; every send
   and receive uses it.
3. **"Has no leader"**: a node has no leader from the start, after it times out (until it wins),
   and after promising a higher ballot (until it receives that ballot's Accept). Only then may it
   time out (one choice per loop step, before a receive). So a node that followed a leader can time
   out again only after a higher ballot pre-empted it.
4. **Timeout choice placement**: the choice is made before each receive while the node is
   leaderless with budget; option 0 (min) is "receive". A node that chooses to receive and gets no
   message stays blocked (allowed: no message loss is required, and the spec allows lost proposals).
5. **Majority = leader + first promise**; no nack messages; late promises are ignored.
6. **Accept carries the whole log** and Decide the whole decided prefix, so that both are
   order-insensitive under `bag` (a longer log of the same ballot extends a shorter one; a longer
   decided prefix extends a shorter one). Under `fifo` this costs message size only.
7. **Proposals at a node without a leader** are buffered (own client only) and handed to the first
   leader learnt, in order. **Forward at a non-leader is dropped** (the value is lost), never
   re-forwarded: re-forwarding could let a client's later value be decided before an earlier one.
   Each value is forwarded at most once; this keeps each client's decided values in sending order.
8. **No reconfiguration, no failures, no leader lease/heartbeat**: once a node has a leader it never
   times out again unless a higher Prepare/Accept arrives.
9. **Program metadata**: `programName()` = `omnipaxos-derived-impl`, `size()` = `e=<e>,t=<t>,cand=<cand>`,
   `rolesInSpawnOrder()` = n0 n1 n2 c0 c1, `visibleRoles()` = c0 c1. These are my choices; the
   interface does not fix them.
10. **Safety check**: `learn` asserts that a node's decided prefix only grows by extension
    (an AssertionError in a node fails the iteration).

## Compile (inside OUT_DIR = daavar/benchmarks-cf2/omnipaxos-derived-impl/java)

    javac -d java/out -cp "$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')

Compiled without errors or warnings.

## Run (once, 2026-11-04)

    java -Xss4m -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.omnipaxos_derived_impl.OmniPaxosDerivedImpl:impl,1,1,1,fifo' must 0 1 report - 600 </dev/null

Exit 0. Key lines: `ran: 2701 iterations`, `blocked: 2701`, `graphs: 0 distinct complete, 2701
distinct blocked; stack exhausted`, `stop_reason: exhausted`, `elapsed: 27.737 s`. All iterations
are blocked by design (nodes never end). No assertion failed.

Last output line:

    max_porf_count: 0
