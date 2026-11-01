# Two-phase commit refining TCommit (Trillium / AnerisLang)

**Family:** related-work  **Source:** Trillium's AnerisLang two-phase commit,
`related-work-papers/sources/aneris/ml_sources/examples/transaction_commit/two_phase_code.ml`
(= `aneris/aneris/examples/transaction_commit/two_phase_code.v`), refinement target the Coq
`TC_model` in `aneris/aneris/examples/transaction_commit/tc_model.v` (TLA+ `TCommit`;
CANDIDATES.md N7, CATALOGUE-coq-iris.md)  **Expected verdict:** `correct`: **CONFORMS** (every
RM ends in the one state the TM decided, COMMITTED only if every coin said prepare);
`ignore-abort-vote`: **DOES NOT REFINE** (the TM counts an `ABORTED` vote as prepared and commits,
so one RM ends COMMITTED and another ABORTED, which `TCConsistent` forbids).

## 1. The protocol

`transaction_manager tm rms` sends `"PREPARE"` to every RM (`sendto_all_set`), then
`recv_responses`: it receives until the set of `"PREPARED"` senders equals `rms`, returning false
at the first other response. If all prepared it sends `"COMMIT"` to all, collects one reply per RM
(`receivefrom_nodup_set`) and asserts each is `"COMMITTED"`; else it sends `"ABORT"` to all.
`resource_manager rm tm` receives one message; if it is `"ABORT"` it replies `"ABORTED"`;
otherwise it flips a coin (`coin_flip ()`, true = local abort): on abort it replies `"ABORTED"`,
else it replies `"PREPARED"`, waits for `"COMMIT"` or `"ABORT"` (`wait_receivefrom`) and replies
`"COMMITTED"` or `"ABORTED"`. Transport: UDP (reordering, duplication; the TM's `nodup` receive
removes duplicates). The runner has one TM `tm` and three RMs `rm.01`–`rm.03`.

The vote is RM-local and the program has no client, so the observable outcome is the `TC_model`
state of each RM (`WORKING | PREPARED | COMMITTED | ABORTED`), whose final value is COMMITTED or
ABORTED. We make it visible by one observer per RM: visible threads `obs0..obs{N-1}`, each with
exactly one event, a blocking receive of `Outcome ∈ {Committed, Aborted}`, the final state of
RM `i`, which the RM reports as its last step (rule 3: the RMs and the TM stay invisible).
Invisible: `tc` (specification) or `tm` and `rm0..rm{N-1}` (implementation).

Messages: `ToRm ∈ {Prepare, Commit, Abort}`; `ToTm ∈ {Prepared(i), Aborted(i), Committed(i)}`
(`i` = the sender, UDP's `snd msg`); `Outcome`. Parameter: N RMs. Smallest size N = 2 (the
runner has 3). Runs: N = 2, 3, 4, model `bag` (asyn: UDP reorders; we do not model duplication,
so the `nodup` filtering is a set of senders that never sees a repeat).

## 2. The specification program

`TC_model` with RM-local coins, as one oracle `tc`: it draws one `nondet()` per RM (that RM's
`coin_flip ()`), decides COMMITTED iff no coin said abort (`tc_decide_commit` needs `canCommit`,
every RM PREPARED; otherwise `tc_decide_abort`), and sends that one outcome to every observer
(`TCConsistent`). One oracle serves every observer because the decision is one value every RM
observes (rule 2). Spawn order: `obs0..obs{N-1}`, `tc`.

## 3. The implementation program

The source program, line for line: `tm` as `transaction_manager`, `rm{i}` as `resource_manager`
plus the final `Outcome` to `obs{i}` after each reply (`"ABORTED"` → `Aborted`, `"COMMITTED"` →
`Committed`). `wait_receivefrom` is a plain blocking receive (only `COMMIT` or `ABORT` can arrive
then). The TM's commit-phase assertion is kept (Rust `assert_eq!`, Java `AssertionError`); it
never fires. Spawn order: `obs0..obs{N-1}`, `tm`, `rm0..rm{N-1}`. In Rust each RM's inbox is the
named channel `("rm", i)` so that `tm`, spawned before the RMs, can address it; Java constructs
every thread first. Main performs no communication and joins everything last.

## 4. Where they differ, and why the verdict is what it is

- `correct`: if every RM's first message is `Prepare` and every coin says prepare, the TM commits
  and every RM reports `Committed`. If some coin says abort, the TM breaks at the first `Aborted`
  and sends `Abort` to all; every RM then reports `Aborted` (one that prepared receives `Abort`;
  one that had not yet read anything may read `Abort` before `Prepare` under `bag` and abort
  without a coin). So every implementation graph has all observers receiving the same outcome,
  `Committed` only when all coins said prepare, and nothing blocks. The observers' receives are
  causally unordered with each other on both sides (each has only its own receive). The
  specification has exactly these two visible words. **CONFORMS** (TraceForge's check: silence at
  N = 2 and N = 3).
- `ignore-abort-vote`: drop the `(fst msg = "PREPARED") &&` test of `recv_responses`, so an
  `"ABORTED"` response also adds its sender to `prepared`. At N = 2: `rm1`'s coin says abort, it
  replies `Aborted(1)` and reports `Aborted`; `rm0` prepares; the TM's set is full and it sends
  `Commit`; `rm0` reports `Committed`. Uncovered visible word: **`obs0`: `recv Committed`; `obs1`:
  `recv Aborted`** (and the mirror image). No specification graph gives two observers different
  outcomes. Caught by the **signature value**. The observers finish; the TM ends blocked in its
  commit-phase collection (the aborted RM never replies again), which is invisible, so TraceForge
  counts the graph as a complete graph of the check (4 uncovered at N = 2, 54 at N = 3) while plain
  Must counts it blocked (§7). **DOES NOT REFINE.**

## 5. Variants and faults

| variant | difference | expected verdict | reason |
|---|---|---|---|
| `correct` | the source program | CONFORMS | one outcome for all observers, commit only if all coins prepare |
| `ignore-abort-vote` | `recv_responses` without `(fst msg = "PREPARED") &&` (an `ABORTED` vote counts as prepared) | DOES NOT REFINE | `obs0` Committed, `obs1` Aborted (signature value), N = 2 |

The source ships no faulty variant; `ignore-abort-vote` is ours, derived by deleting one conjunct
of the source (CATALOGUE-coq-iris.md lists TM-side early commits as the natural seeded faults).

## 6. Files

- `source/`: `two_phase_code.ml`, `two_phase_code.v`, `two_phase_runner_code.v`, `tc_model.v`,
  `two_phase_adequacy.v` and `Readme.md`, copied verbatim in the repository's layout, with
  provenance in `source/PROVENANCE.md`.
- `rust/`: crate `bench_two_pc_trillium`. CLI, the shared main: `<bin> count|export|conf|online …
  --model M -- <N> [correct|ignore-abort-vote]`; defaults: N 2, variant `correct`. `--model` left
  out means `bag`, this benchmark's model, which the crate declares. `conf` runs TraceForge's
  conformance check with visible threads `obs0..obs{N-1}` (`--engine enumerator` for the diagnosing
  engine; the old `all` suffix, which kept every report, is gone: the shared `conf` has no flag for
  it). See `_tools/run-rust.sh`.
- `java/`: package `bench.two_pc_trillium`: `TwoPcTrillium` (args `<spec|impl>,N[,model[,variant]]`)
  and `Support`. See `_tools/run-java.sh`.
- `out/logs/`: the JMC logs, `java-<side>-<N>-bag.log`.

## 7. Plain-Must exploration counts (both tools, one run each)

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | N=2 | bag | 4 / 0 | 4 / 0 | yes |
| impl `correct` | N=2 | bag | 12 / 0 | 12 / 0 | yes |
| impl `ignore-abort-vote` | N=2 | bag | 4 / 6 | 4 / 6 | yes |
| spec | N=3 | bag | 8 / 0 | 8 / 0 | yes |
| impl `correct` | N=3 | bag | 87 / 0 | 87 / 0 | yes |
| impl `ignore-abort-vote` | N=3 | bag | 36 / 60 | 36 / 60 | yes |
| spec | N=4 | bag | 16 / 0 | 16 / 0 | yes |
| impl `correct` | N=4 | bag | 888 / 0 | 888 / 0 | yes |
| impl `ignore-abort-vote` | N=4 | bag | 576 / 984 | 576 / 984 | yes |

Every JMC run ended with "stack exhausted". Size 4 was run because size 3 finished in under a
minute on both tools. The two tools agree on every row.

The specification has `2^N` graphs (one per coin vector; two visible words). The
`ignore-abort-vote` blocked graphs are exactly those with some coin saying abort: the TM, having
committed, waits forever for a `COMMITTED` from the aborted RM. Plain Must counts them blocked;
the conformance check treats a graph whose visible threads have all finished as complete, which
is why its reports (§4) are drawn from these graphs.

Commands, run from `B=/local/home/mkhoshechin/Documents/github-jmc/plan/benchmarks`, for
`N ∈ {2, 3, 4}`, `v ∈ {correct, ignore-abort-vote}`:

```
$B/_tools/run-rust.sh $B/two-pc-trillium count spec --model bag -- <N> correct
$B/_tools/run-rust.sh $B/two-pc-trillium count impl --model bag -- <N> <v>
$B/_tools/run-java.sh $B/two-pc-trillium 'bench.two_pc_trillium.TwoPcTrillium:spec,<N>,bag,correct' must 100000 600
$B/_tools/run-java.sh $B/two-pc-trillium 'bench.two_pc_trillium.TwoPcTrillium:impl,<N>,bag,<v>' must 100000 600
```

Verdict check (TraceForge conformance, default stateful engine), `N ∈ {2, 3}`:

```
$B/_tools/run-rust.sh $B/two-pc-trillium conf --model bag -- <N> correct            # silence at N=2 and N=3
$B/_tools/run-rust.sh $B/two-pc-trillium conf --model bag -- <N> ignore-abort-vote  # 4 (N=2) / 54 (N=3) uncovered graphs
```
