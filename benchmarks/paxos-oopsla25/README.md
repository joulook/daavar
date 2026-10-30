# Single-decree Paxos (OOPSLA 2025) against a consensus oracle

**Family:** related-work  **Source:** Bílý, Pereira, Müller, OOPSLA 2025 §5.3,
`related-work-papers/sources/oopsla25-artifact/oopsla552-artefact-amd64/refinement-lib/refinement-examples/src/bin/ex05_paxos.rs`
(record in CATALOGUE-oopsla25.md; planned pair RELATED-BENCHMARKS.tex §B1)  **Expected verdict:**
`correct`: **CONFORMS** (Paxos agreement: every learner receives the same value, and that value
causally descends from the proposal of the client that supplied it). The four faults: **DOES NOT
REFINE at L = 2** (two learners receive different values); **conforming controls at L = 1** (one
learner receives one proposed value, which the oracle allows). Status: all ten TraceForge conformance
runs are done (`out/conf.log`, 2026-10-30): the six expected CONFORMS cases were silent with both
state spaces exhausted, and each of the four faults at L = 2 was reported with the uncovered word
below. Every verdict is observed.

## 1. The protocol

Single-decree Paxos after Lamport's TLA+ `Paxos` (`ex05_paxos.rs`). Proposers send 1a with their
ballot; an acceptor promises (1b, reporting its `max_vote`) if the ballot is greater than its
`max_bal`; a proposer with a majority of 1b's proposes (2a) the value of the highest-ballot vote
reported, or its own value; an acceptor votes (2b) if the ballot is ≥ its `max_bal`. The source
has no learners and no clients; as in B1 we add both.

Roles:

- Visible: `c0`, `c1` (clients, one `Propose` each: values 1, 2), `l0..l{L-1}` (learners, one
  blocking receive of `Decide` each).
- Invisible: `oracle` (specification); `p1`, `p2` (proposers, ballots 1, 2; `p{i+1}` serves `c{i}`)
  and `a0`, `a1`, `a2` (acceptors) (implementation).

Messages (source names): `Propose(v)`, `Msg1a(b)`, `Msg1b(a, b, Option<(b, v)>)`, `Msg2a(b, v)`,
`Msg2b(a, b, v)`, `Decide(v)`. A 1b/2b goes to the proposer of its ballot (the source broadcasts
into a soup; only that proposer reads them). Ballot = proposer index (the source's `(counter,
leader)` bounded to one ballot per leader). Parameters: 2 clients, 2 proposers, 3 acceptors,
L learners (the size argument; 1 and 2 run). Model `bag` (asyn: the source's message soup without
duplication; duplication is not needed: an acceptor ignores a non-greater 1a, a proposer ignores a
duplicate acceptor). Smallest interesting size: L = 2 (a disagreement needs two learners).

## 2. The specification program

One `oracle` (rule 2: one oracle for a decision every learner observes). It performs two
non-blocking receives of `Propose`, keeping what it read (it may leave any proposal unread, so the
decision depends causally only on the proposals read, as in the implementation, where the
decision depends only on the proposals that reached the deciding ballot); then, if it read
something, it nondeterministically decides nothing or one value read, and sends `Decide(v)` to
every learner. A learner blocks when nothing is decided. Graphs: 8 complete, 7 blocked, for both L.

## 3. The implementation program

- `p{b}` (`run_leader_ballot`): blocking receive of its client's `Propose(mine)`; `Msg1a(b)` to
  every acceptor; up to 3 blocking receives until a majority (`3/2+1 = 2`) of distinct 1b's of
  ballot b, tracking the highest-ballot vote; `v` = that vote's value, else `mine`
  (`choose_value()`); `Msg2a(b, v)` to every acceptor; up to 3 blocking receives until a majority of
  2b's (late 1b's are read and ignored); then `Decide(v)` to every learner.
- `a{j}` (`run_acceptor`): 4 blocking receives (2 prepares + 2 accepts); 1a: promise if
  `b > max_bal`; 2a: vote if `b >= max_bal`.
- Receives are blocking (the A4 idiom, rule 1): a ballot that never reaches a majority waits forever,
  an acceptor sent fewer than 4 messages ends blocked. These are invisible statuses. The source's
  `decide()` abandonment and the soup's loss are represented by this waiting (an abandoned proposer
  sends nothing more, as a waiting one); abandonment while a quorum is available is not explored,
  which only removes implementation behaviours. A first version with non-blocking receives
  (B1's draft) did not finish L = 1 in 600 s on TraceForge and was replaced.
- With two deciding proposers a learner receives two `Decide`s and reads one; the other stays pending.

## 4. Where they differ, and why the verdict is what it is

Notation: `S(m)` a send, `R(m)` a receive, per visible thread.

- **correct: CONFORMS.** Paxos agreement: if ballot 1 decides `v`, a majority voted `(1, v)`; ballot
  2's quorum intersects it, so p2 receives a 1b reporting `(1, v)` and proposes `v`. Every learner
  receives the same value `v ∈ {1, 2}`, and `v`'s client's `Propose` precedes (causally) every
  learner's receive: the value travels from `c{v-1}` through its proposer, possibly an acceptor's
  vote and the second proposer. The oracle graph that reads exactly that client's proposal and
  decides `v` has the same words and statuses and an order contained in the implementation's.
  TraceForge's check: silence at L = 1 and at L = 2 (2026-10-30), both state spaces exhausted.
- **ignore-max-vote** (catalogue (a): `v = mine` even when a vote was reported). At L = 2: `p1`
  decides 1 through `a0, a1`; `a1` then promises ballot 2 reporting `(1, 1)`; `p2` with `a1, a2`
  proposes its own 2, `a1, a2` vote, `p2` decides 2; `l0` reads `p1`'s `Decide(1)`, `l1` reads
  `p2`'s `Decide(2)`. Uncovered word: **`l0: R(Decide(1))`, `l1: R(Decide(2))`** (with `c0: S(Propose(1))`,
  `c1: S(Propose(2))`); the oracle sends one value to all. Component: **signature value**.
- **skip-2a-check** (catalogue (b): an acceptor votes for a 2a below its `max_bal`). At L = 2:
  `p1` gets promises from `a0, a1`, its 2a's are delayed; `p2` gets promises from `a1, a2`
  (no votes), proposes 2, `a1, a2` vote, `p2` decides 2; then `a0, a1` vote for the delayed
  `Msg2a(1, 1)` (`a1` had promised 2), `p1` decides 1. Same uncovered word. **Signature value.**
- **half-majority** (catalogue (c): `majority = 3/2 = 1`). At L = 2: `p1` decides 1 with `a0` alone,
  `p2` decides 2 with `a2` alone (quorums of one do not intersect). Same uncovered word. **Signature value.**
- **1b-none** (catalogue (d): a 1b reports `None` instead of `max_vote`). At L = 2: as
  ignore-max-vote, `a1`'s 1b to ballot 2 hides its vote `(1, 1)`. Same uncovered word. **Signature value.**
- **All four at L = 1: conforming controls.** One learner reads one `Decide(v)`; `v` is a proposed
  value that causally descends from its client's `Propose` (no fault invents a value), so the
  oracle graph reading that proposal covers it.

All verdicts above are observed (`out/conf.log`): the five L = 1 runs and `correct` at L = 2 were
silent; each fault at L = 2 was reported, the first kept report being the graph in which `l0`
receives `Decide(1)` from `p1` and `l1` receives `Decide(2)` from `p2`, a signature-value miss
(no specification graph has that word pair). The checker counted the further uncovered graphs
without keeping them: 186623 (ignore-max-vote), 870911 (skip-2a-check), 168479 (half-majority),
186623 (1b-none).

## 5. Variants and faults

| variant | how it differs (source line) | expected verdict | reason |
|---|---|---|---|
| `correct` | the source's leader and acceptor | CONFORMS (observed, L=1 and 2) | agreement |
| `ignore-max-vote` | `run_leader_ballot` uses `mine`, not `max_vote` | DOES NOT REFINE at L=2; control at L=1 | `l0: R(Decide(1))`, `l1: R(Decide(2))` |
| `skip-2a-check` | `run_acceptor` drops `ballot_greater_equal(b, mb)` on 2a | DOES NOT REFINE at L=2; control at L=1 | same |
| `half-majority` | `majority = acceptor_count / 2` | DOES NOT REFINE at L=2; control at L=1 | same |
| `1b-none` | `Msg1b(my_id, b, None)` instead of `max_vote` | DOES NOT REFINE at L=2; control at L=1 | same |

## 6. Files

- `source/`: `ex05_paxos.rs`, copied verbatim, with provenance in `source/PROVENANCE.md`.
- `rust/`: crate `bench_paxos_oopsla25`. CLI: `<spec|impl|conf> <L> [bag|fifo|causal] [variant]`
  (defaults `bag`, `correct`); `conf` runs TraceForge's conformance check with visible threads
  `c0, c1, l0..l{L-1}` (fifth argument `enumerator`, `all` to keep every report).
- `java/`: package `bench.paxos_oopsla25`: `Paxos` (args `<spec|impl>,<L>[,<bag|fifo>[,<variant>]]`)
  and `Support` (model argument and construct-first spawning). Spawn order: spec `oracle, l*, c0, c1`;
  impl `p1, p2, a0, a1, a2, l*, c0, c1`. Same visible roles on both sides and languages. Receives
  untyped (`Channel.recv(m)` / `tryRecv(m)`), as in Rust.
- `out/runs.log`: every plain-Must command and its (summary-line) output; `out/conf.log`: the
  conformance runs.

## 7. Plain-Must exploration counts (both tools, one run each)

Model `bag` throughout. Size = L (learners); 2 clients, 2 proposers, 3 acceptors.

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | L=1 | bag | 8 / 7 | 8 / 7 | yes |
| spec | L=2 | bag | 8 / 7 | 8 / 7 | yes |
| impl `correct` | L=1 | bag | 186624 / 82368 | not exhausted: 100000-iteration budget (75472 / 24528) | — |
| impl `correct` | L=2 | bag | 373248 / 82368 | not exhausted at 600 s (82002 / 17856) | — |
| impl `ignore-max-vote` | L=1 | bag | 186624 / 82368 | not exhausted: budget (75472 / 24528) | — |
| impl `ignore-max-vote` | L=2 | bag | 373248 / 82368 | not exhausted at 600 s (81216 / 17685) | — |
| impl `skip-2a-check` | L=1 | bag | 1959552 / 2880 | not exhausted: budget (100000 / 0) | — |
| impl `skip-2a-check` | L=2 | bag | 3919104 / 2880 | not exhausted: budget (100000 / 0) | — |
| impl `half-majority` | L=1 | bag | 379080 / 37440 | not exhausted: budget (95444 / 4556) | — |
| impl `half-majority` | L=2 | bag | 758160 / 37440 | not exhausted: budget (95444 / 4556) | — |
| impl `1b-none` | L=1 | bag | 186624 / 82368 | not exhausted: budget (75472 / 24528) | — |
| impl `1b-none` | L=2 | bag | 373248 / 82368 | not exhausted: budget (82144 / 17856) | — |

The rows marked — are not disagreements: every implementation graph count exceeds the prescribed
`must 100000` budget, so JMC stopped ("stack not exhausted"), and each partial count is below
TraceForge's total. Every spec run exhausted on both tools and agrees. L = 3 was not run (L = 2 took
more than a minute on JMC). `ignore-max-vote` and `1b-none` change only values, so they have
`correct`'s TraceForge counts; `skip-2a-check` and `half-majority` change control flow (more votes,
fewer waiting proposers). Blocked counts are equal at L = 1 and 2: a blocked execution is one in which
no proposer decides, and the learner count does not change those.

Commands, run from `B=/local/home/mkhoshechin/Documents/github-jmc/plan/benchmarks`, for
`L ∈ {1, 2}`, `v ∈ {correct, ignore-max-vote, skip-2a-check, half-majority, 1b-none}`:

```
$B/_tools/run-rust.sh $B/paxos-oopsla25 spec <L> bag
$B/_tools/run-rust.sh $B/paxos-oopsla25 impl <L> bag <v>
$B/_tools/run-java.sh $B/paxos-oopsla25 'bench.paxos_oopsla25.Paxos:spec,<L>,bag' must 100000 600
$B/_tools/run-java.sh $B/paxos-oopsla25 'bench.paxos_oopsla25.Paxos:impl,<L>,bag,<v>' must 100000 600
$B/_tools/run-rust.sh $B/paxos-oopsla25 conf <L> bag <v>        # verdict check, out/conf.log
```
