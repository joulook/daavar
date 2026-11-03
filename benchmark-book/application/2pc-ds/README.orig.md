# Multi-round two-phase commit with acknowledgements (Disel)

**Family:** related-work  **Source:** Disel's two-phase commit,
`related-work-papers/sources/disel/theories/Examples/TwoPhaseCommit/` (`TwoPhaseProtocol.v`,
`TwoPhaseCoordinator.v`, `TwoPhaseParticipant.v`, `SimpleTPCApp.v`; CANDIDATES.md N8,
CATALOGUE-verdi-disel.md)  **Expected verdict:** `correct`: **CONFORMS** under `bag` and `fifo`
(the coordinator's decision rule is the oracle's, and it collects every ack before the next round);
`skip-ack-loop`: **DOES NOT REFINE** under `bag` **and under `fifo`** (a participant receives the
next round's `Prep` before another participant has acked the current round; the catalogue's
"conforms under p2p" is refuted by the run, see §4)

## 1. The protocol

A coordinator `cn` and participants `pts` run one two-phase commit per data item, in rounds
`e = 0, 1, …` (`coordinator_loop`, `participant_with_choices`). One round
(`coordinator_round d`): `send_prep_loop` sends `prep_req` (contents `e :: d`) to every
participant; `receive_prep_loop` collects one `prep_yes`/`prep_no` (contents `[e]`) per
participant; `read_resp_result` is "all yes"; then `send_commits`/`send_aborts` (`commit_req` /
`abort_req`, `[e]`) and `receive_commit_loop`/`receive_abort_loop` collects one
`commit_ack`/`abort_ack` per participant before the round counter advances. A participant
(`participant_round doCommit`) receives the `prep_req` of its round, answers yes iff `doCommit`,
receives the decision of its round, appends it to its log and sends the matching ack. Every
receive is a `tryrecv` spin loop with a filter on the tag and, after reception, on the sender and
the round (`head 0 body == e`); messages of another round are dropped. The network is Disel's
message soup: reliable, unordered. The app (`SimpleTPCApp.v`) has `cn = 0`, `pts = [1;2;3]`,
`data_seq = [[1;2];[3;4];…]` and fixed choice lists; the proof has a free `doCommit`.

Roles: visible participants `p0..p{N-1}`; invisible coordinator `cn`. Per round a participant's
visible events are `recv Prep(e, d)`, `send Yes(i,e) | No(i,e)`, `recv Commit(e) | Abort(e)`,
`send CommitAck(i,e) | AbortAck(i,e)`. Messages: `ToP ∈ {Prep(e, d), Commit(e), Abort(e)}` and
`ToCn ∈ {Yes, No, CommitAck, AbortAck}(i, e)` in Rust, with Disel's tag filters as message tags
(prep 0, vote 1, decision 3, ack 5); in Java the tag filters are the types `Prep` (a `@Tag` record
`Prep(e, d)`, `d` a list) and the sealed interfaces `Dec` (`Commit(e)`, `Abort(e)`), `Vote` (`Yes(i, e)`,
`No(i, e)`), `Ack` (`CommitAck(i, e)`, `AbortAck(i, e)`) of `@Tag` records named as the Rust variants,
and the filtered receives are type-guarded `Channel.recv(M, T.class)`.

Parameters: N participants, K = 2 rounds with data `[1;2]`, `[3;4]` (the first two items of
`data_seq`); each vote is a `nondet()`. Smallest interesting size N = 2, K = 2 (K = 1 is one-round
2PC plus acks). Runs: N = 2, 3 with K = 2, models `bag` (asyn, the soup) and `fifo` (p2p).

Loop-freedom: the source's round filters never discard a message here. A participant's next
`prep_req` needs its own previous vote, a decision of round e+1 needs its own round-(e+1) vote,
each participant votes and acks once per round, and the coordinator consumes every vote and ack
of its round before the next. So each spin loop is one receive, and the filter becomes an
assertion (Rust `assert!`, Java `AssertionError`) that no run violates.

## 2. The specification program

The same round structure with the TCommit decision rule, as one oracle `cn` (rule 2: one
thread orders all rounds, as the implementation's coordinator does): per round it sends
`Prep(e, d)` to every participant, receives one vote from each, decides `Commit` only if every vote
is yes and then still by `nondet()` (abort is always allowed, `TCommit`'s `notCommitted`), sends
the decision to all and receives one matching ack from each before the next round. The
participants are the implementation's.

## 3. The implementation program

The source's coordinator: the same as §2 with the decision exactly "all yes"
(`read_resp_result`). Variant `skip-ack-loop` (fault F1 of the catalogue, ours) drops
`receive_commit_loop`/`receive_abort_loop`: after sending the decision of round e the
coordinator starts round e+1 at once and never reads an ack. Spawn order (both tools): `cn`, then
`p0..p{N-1}`. In Rust a participant's inbox is the named channel `("p", i)` so that `cn` can
address it; Java constructs every thread first. Main performs no communication and joins every
thread last.

## 4. Where they differ, and why the verdict is what it is

- `correct`: the implementation's coordinator is the oracle with its free abort fixed to "commit
  iff all yes", so every implementation graph is a specification graph with the same visible
  events, values and causal order (both collect every vote and every ack of round e before
  sending `Prep(e+1)`). **CONFORMS** under both models (TraceForge's check: silence at N = 2 and
  N = 3, `bag` and `fifo`).
- `skip-ack-loop`: every participant still sees, per round, `recv Prep(e, d)`, its vote, the
  decision of round e and its ack, with the specification's values; nothing blocks. But the
  coordinator sends `Prep(1, [3;4])` right after the round-0 decision, without reading an ack.
  At N = 2, K = 2: `p0` receives its `Abort(0)`/`Commit(0)`, sends its ack and receives
  `Prep(1, [3;4])` while `p1` has only received `Prep(0, [1;2])` and sent its vote. Uncovered
  behaviour: **`p0`: `recv Prep(0,[1,2]) · send vote · recv Dec(0) · send Ack(0) · recv
  Prep(1,[3,4])` with `p1` at `recv Prep(0,[1,2]) · send vote`** — in every specification graph
  `p1`'s `send Ack(0)` happens before `p0`'s `recv Prep(1, …)` (the oracle reads every ack of
  round 0 before sending any `Prep(1)`). The values and statuses are the specification's, so it is
  caught by the **order**: in TraceForge's stateful check every complete graph is uncovered (64
  of 64 at N = 2, 2304 of 2304 at N = 3), since in the implementation another participant's
  round-0 ack need not precede this participant's round-1 `Prep` receive (each participant's own
  ack does precede its own next receive, by program order); the enumerator engine
  (`conf --engine enumerator --model fifo -- 2 skip-ack-loop`) names the first
  failing obligation as "observation mismatch on `p0` at position 4: the specification has
  nothing, the implementation has receive Prep(1, [3, 4])" (8 reports). **DOES NOT REFINE.**

  This holds under `fifo` too, and the reason is independent of the model: with Disel's
  tag-filtered receives (`tryrecv_prep_req`, `tryrecv_commabrt_req`; here tags and type guards) a
  `Prep(e+1)` that overtakes `Commit(e)` is not consumed by the decision receive, so the
  catalogue's mechanism (a participant receiving the two out of order, which only `bag` allows)
  does not arise; what remains is the missing ack → next-prepare order, which `fifo` does not
  restore. CATALOGUE-verdi-disel.md's "reports under asyn, conforms under p2p" assumed untagged
  receives; the runs (both models, N = 2 and 3) report under both.

## 5. Variants and faults

| variant | model | difference | expected verdict | reason |
|---|---|---|---|---|
| `correct` | `bag`, `fifo` | the source's coordinator | CONFORMS | decision rule within the oracle's; acks read before the next round |
| `skip-ack-loop` | `bag` | `receive_commit_loop`/`receive_abort_loop` dropped (F1, ours) | DOES NOT REFINE | `Prep(1)` received before another participant's `Ack(0)` (order), N = 2, K = 2 |
| `skip-ack-loop` | `fifo` | same | DOES NOT REFINE (catalogue said CONFORMS) | same; per-pair FIFO does not order an ack before the next `Prep` |

The catalogue's F2 (commit when some vote is yes) and F3 (participant ignores the round check,
reachable only with F1 and untagged receives) are not built.

## 6. Files

- `source/`: the whole `TwoPhaseCommit/` directory (seven `.v` files), copied verbatim, with
  provenance in `source/PROVENANCE.md`.
- `rust/`: crate `bench_two_pc_disel`. CLI, the shared main: `<bin> count|export|conf|online …
  --model M -- <N> [correct|skip-ack-loop]`; defaults: N 2, variant `correct`. `--model` left out
  means `bag`, this benchmark's model, which the crate declares. `conf` runs TraceForge's
  conformance check with visible threads `p0..p{N-1}` (`--engine enumerator` for the diagnosing
  engine; the old `all` suffix, which kept every report, is gone: the shared `conf` has no flag for
  it). See `_tools/run-rust.sh`.
- `java/`: package `bench.two_pc_disel`: `TwoPcDisel` (args `<spec|impl>,N[,model[,variant]]`) and
  `Support`. See `_tools/run-java.sh`.
- `out/logs/`: the JMC logs, `java-<side>-<N>-<model>.log`.

## 7. Plain-Must exploration counts (both tools, one run each)

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | N=2, K=2 | bag | 400 / 0 | 400 / 0 | yes |
| impl `correct` | N=2, K=2 | bag | 256 / 0 | 256 / 0 | yes |
| impl `skip-ack-loop` | N=2, K=2 | bag | 64 / 0 | 64 / 0 | yes |
| spec | N=2, K=2 | fifo | 400 / 0 | 400 / 0 | yes |
| impl `correct` | N=2, K=2 | fifo | 256 / 0 | 256 / 0 | yes |
| impl `skip-ack-loop` | N=2, K=2 | fifo | 64 / 0 | 64 / 0 | yes |
| spec | N=3, K=2 | bag | 104976 / 0 | not exhausted: stopped at the 100000-iteration budget (100000 / 0) | — |
| impl `correct` | N=3, K=2 | bag | 82944 / 0 | not exhausted at 600 s (76399 / 0) | — |
| impl `skip-ack-loop` | N=3, K=2 | bag | 2304 / 0 | 2304 / 0 | yes |
| spec | N=3, K=2 | fifo | 104976 / 0 | not exhausted at 600 s (63259 / 0) | — |
| impl `correct` | N=3, K=2 | fifo | 82944 / 0 | 82944 / 0 | yes |
| impl `skip-ack-loop` | N=3, K=2 | fifo | 2304 / 0 | 2304 / 0 | yes |

The rows marked — are not disagreements: JMC stopped early ("stack not exhausted"), and its
partial counts are below TraceForge's totals. The `spec`, N = 3, `bag` run hit the
`must 100000` iteration budget of the prescribed command (104976 graphs exceed it); it was not
rerun with a larger budget (one run per row). Every JMC run except the three rows marked
unfinished (—) ended with "stack exhausted", and on those rows the two tools agree. Size 4 was
not run: size 3 took longer than a minute on JMC.

The specification has more graphs than the implementation (400 vs 256 at N = 2): its free
abort adds a choice whenever every vote is yes. `skip-ack-loop` has fewer (64) because no ack is
ever read. The `fifo` counts equal the `bag` counts on every exhausted row (observed, not
derived here).

Commands, run from `B=/local/home/mkhoshechin/Documents/github-jmc/plan/benchmarks`, for
`N ∈ {2, 3}`, `m ∈ {bag, fifo}`, `v ∈ {correct, skip-ack-loop}`:

```
$B/_tools/run-rust.sh $B/two-pc-disel count spec --model <m> -- <N> correct
$B/_tools/run-rust.sh $B/two-pc-disel count impl --model <m> -- <N> <v>
$B/_tools/run-java.sh $B/two-pc-disel 'bench.two_pc_disel.TwoPcDisel:spec,<N>,<m>,correct' must 100000 600
$B/_tools/run-java.sh $B/two-pc-disel 'bench.two_pc_disel.TwoPcDisel:impl,<N>,<m>,<v>' must 100000 600
```

Verdict check (TraceForge conformance), `N ∈ {2, 3}`, `m ∈ {bag, fifo}`:

```
$B/_tools/run-rust.sh $B/two-pc-disel conf --model <m> -- <N> correct         # silence (all four runs)
$B/_tools/run-rust.sh $B/two-pc-disel conf --model <m> -- <N> skip-ack-loop   # 64 (N=2) / 2304 (N=3) uncovered, both models
$B/_tools/run-rust.sh $B/two-pc-disel conf --engine enumerator --model fifo -- 2 skip-ack-loop   # diagnostics quoted in §4
```
