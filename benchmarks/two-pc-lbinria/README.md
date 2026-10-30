# Two-phase commit with retransmission and the counting bug (Cirstea et al., lbinria)

**Family:** related-work  **Source:** the Java 2PC of Cirstea et al. (trace validation),
`related-work-papers/sources/lbinria-twophase/` (`protocol/TransactionManager.java`,
`protocol/ResourceManager.java`, spec `spec/TwoPhase.tla`; CANDIDATES.md N1, CATALOGUE-tla.md)
**Expected verdict:** `correct`: **CONFORMS** (the TM commits only after a `Prepared` of every RM,
which is the oracle's rule); `counting-bug`: **DOES NOT REFINE** (the TM counts a resent
`Prepared` twice and commits while another RM never prepared).

## 1. The protocol

One transaction manager (TM) and N resource managers (RMs). In the source each RM either crashes
before preparing (`Helper.next(PROBABILITY_TO_ABORT) == 1`: it returns and sends nothing) or sends
`Prepared` to the TM and resends it on every receive timeout until it gets `Commit` or `Abort`.
The TM loops: before each receive it aborts if `ABORT_TIMEOUT` has passed (it sends `Abort` to
every RM), else it receives one `Prepared(from)` and adds `from` to `preparedRMs`; once
`preparedRMs.size() >= N` it sends `Commit` to every RM. `preparedRMs` is a `HashSet` as shipped.
The paper's bug is the commented-out line `// this.preparedRMs = new ArrayList<>();`: with a list,
a resent `Prepared` is counted twice. Delivery is reliable; `run_impl.py` starts the mailbox
server with `"unordered"` boxes, so messages are reordered.

Roles: visible `rm0..rm{N-1}`; invisible `tm` (both sides) and `p0..p{N-1}` (implementation only:
the transport of each RM, which performs its resends). Messages: `Prepared(i)` (RM index `i`, the
source's `Message.from`) and `Decision ∈ {Commit, Abort}` (Rust `enum Decision`, Java
`record Decision(boolean commit)`).

Each RM's visible events: nothing (crash), or one send `Prepared(i)` followed by one blocking
receive of the decision. The RM is the same code on both sides.

Parameters: N RMs, resend bound R = 2 (a transport forwards its RM's `Prepared` 1 or 2 times).
Smallest interesting size: N = 2, R = 2 (the bug needs one resend and one crashed RM). Runs:
N = 2, 3, 4 with R = 2, model `bag` (asyn, as `"unordered"`).

Abstractions: the timeouts become `nondet()` choices (the TM's abort timeout before each
receive; the RM's resend timeout becomes the transport's forward count). The RM's own give-up
(`ABORT_TIMEOUT` after preparing, return without a decision) is not modelled; it would be the same
code on both sides. The TM's unbounded loop is bounded by `N·R`, the number of `Prepared` messages
the transports can send, so the bound never cuts a run short.

## 2. The specification program

`spec/TwoPhase.tla` with one TM oracle `tm` and the same RMs, which send `Prepared(i)` directly to
`tm`. The oracle performs at most N iterations; in each it may abort (`nondet()`: `TMAbort` is
enabled whenever `tmState = "init"`, it sends `Abort` to every RM and stops) or does one blocking
receive of a `Prepared` (`TMRcvPrepared`). After N receives it sends `Commit` or, by `nondet()`,
`Abort` to every RM (`TMCommit` needs `tmPrepared = RM`; `TMAbort` stays enabled). So it commits
only when every RM's `Prepared` was received, each exactly once. An RM that crashed leaves the
oracle short of N receives: it aborts or ends blocked (invisible). A non-crashed RM receives the
decision, or ends blocked if the oracle did.

## 3. The implementation program

`tm` is `TransactionManager.run`: at most `N·R` iterations, each first a `nondet()` abort (the
timeout; sends `Abort` to all) and then a blocking receive of `Prepared(from)`; `from` is added to
`prepared` if absent (`correct`, the `HashSet`) or always (`counting-bug`, the `ArrayList`); when
`prepared.len() >= N` it sends `Commit` to all and stops. Transport `pi` receives RM `i`'s one
`Prepared` and forwards it to `tm` `(1..=R).nondet()` times (Java `1 + chooseIndex(R)`); the
retransmission is invisible (rule 3). A crashed RM's transport ends blocked (invisible).

Spawn order (both tools): `tm`, then `p0..p{N-1}` (impl only), then `rm0..rm{N-1}`. In Rust each
RM's decision inbox is the named channel `("rm", i)`, so that `tm`, spawned first, can address it;
Java constructs every thread (minting its id) before starting any. Main performs no
communication and joins every thread last.

## 4. Where they differ, and why the verdict is what it is

- `correct`: `tm` commits only when its set holds N distinct RMs, that is, after receiving a
  `Prepared` from every RM, which every RM sent once, visibly. Duplicates are invisible
  (transport sends, TM receives). The specification oracle has the same decision points (abort
  before any receive, commit after all N), so every visible word and order of the implementation
  is one of the specification's. **CONFORMS** (TraceForge's check reports silence at N = 2 and
  N = 3, `bag`).
- `counting-bug`: at N = 2, `rm0` crashes (no events), `rm1` sends `Prepared(1)`, `p1` forwards it
  twice, `tm` receives both and its list has length 2 = N, so it sends `Commit`; `rm1` receives
  it. Uncovered visible word: **`rm0`: ε (done); `rm1`: `send Prepared(1) · recv Commit` (done)**
  (and symmetrically with the roles of `rm0`/`rm1` swapped). Every specification graph in which
  an RM receives `Commit` has both `Prepared` sends; with `rm0` crashed the oracle can only abort
  or block. Caught by the **signature value** (the decision `rm1` receives: `Commit` where the
  specification offers only `Abort` or a blocked `rm1`). The same early commit without a crash
  is caught by the **order**: both RMs prepare, `p0` forwards twice, `tm` commits on those two
  receives and `p1`'s forward is never read (or the mirror image); both RMs receive `Commit`, the specification's word,
  but `rm1`'s `send Prepared(1)` is not causally before `rm0`'s `recv Commit`, while in every
  specification graph with a `Commit` the oracle received both `Prepared` before deciding. With
  every report kept (`conf 2 bag counting-bug all`, log `out/logs/conf-counting-bug-2-bag-all.log`)
  TraceForge's 12 uncovered complete graphs at N = 2 are 4 with a crashed RM (value) and 8 with
  both RMs prepared (order); 324 at N = 3.
  **DOES NOT REFINE.**

## 5. Variants and faults

| variant | difference | expected verdict | reason |
|---|---|---|---|
| `correct` | `prepared` is a set (`HashSet`, as shipped) | CONFORMS | commit only after every RM's `Prepared` |
| `counting-bug` | `prepared` is a list (the commented-out `ArrayList` line) | DOES NOT REFINE | `rm0` crashed, `rm1` receives `Commit` (signature value); or both prepared and `Commit` before `rm1`'s `Prepared` was read (order); N = 2 |

The source's alternative "alt1" (the TM does not send the final commit) and the
`handleMessageAndCommit` logging variant are not built.

## 6. Files

- `source/`: the Java program (all 16 files under `src/main/java/org/lbee/`), the TLA+ specs
  `TwoPhase.tla`, `TCommit.tla`, `TwoPhaseTrace.tla`, `run_impl.py`, `README.md` and the bug trace
  `TracesCompact/trace-4RM-BUG-E.ndjson`, copied verbatim, with provenance in `source/PROVENANCE.md`.
- `rust/`: crate `bench_two_pc_lbinria`. CLI: `<spec|impl|conf> <N> [fifo|bag|causal]
  [correct|counting-bug]` (defaults `bag`, `correct`); `conf` runs TraceForge's conformance check of
  `impl <variant>` against `spec` with visible threads `rm0..rm{N-1}`. See `_tools/run-rust.sh`.
- `java/`: package `bench.two_pc_lbinria`: `TwoPcLbinria` (args `<spec|impl>,N[,model[,variant]]`)
  and `Support` (model parsing, construct-first spawning). See `_tools/run-java.sh`.
- `out/logs/`: the JMC logs, `java-<side>-<N>-bag.log`, and the all-reports conformance log.

## 7. Plain-Must exploration counts (both tools, one run each)

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | N=2, R=2 | bag | 12 / 3 | 12 / 3 | yes |
| impl `correct` | N=2, R=2 | bag | 54 / 22 | 54 / 22 | yes |
| impl `counting-bug` | N=2, R=2 | bag | 42 / 18 | 42 / 18 | yes |
| spec | N=3, R=2 | bag | 44 / 10 | 44 / 10 | yes |
| impl `correct` | N=3, R=2 | bag | 1238 / 452 | 1238 / 452 | yes |
| impl `counting-bug` | N=3, R=2 | bag | 554 / 272 | 554 / 272 | yes |
| spec | N=4, R=2 | bag | 192 / 41 | 192 / 41 | yes |
| impl `correct` | N=4, R=2 | bag | 54248 / 18098 | 54248 / 18098 | yes |
| impl `counting-bug` | N=4, R=2 | bag | 10472 / 6146 | 10472 / 6146 | yes |

Every JMC run ended with "stack exhausted" (the exploration finished). Size 4 was run because
size 3 finished in under a minute on both tools. The two tools agree on every row.

The blocked graphs have two sources. (a) An RM ends blocked on its decision receive: a crashed
RM's `Prepared` never comes, the TM or oracle chose not to abort, and so the invisible server ends
blocked and sends no decision. (b) Implementation only: a crashed RM's invisible proxy `p<i>` ends
blocked even when every visible RM finishes. Example at N = 2: `rm0` crashes, `rm1` prepares, the
TM aborts; `rm1` receives `Abort` and finishes, while `p0` remains blocked.

The counts above classify whole-program completion: a graph is blocked when any thread, visible or
invisible, ends blocked, so graphs of kind (b) count as blocked. The conformance check compares
visible-thread completion (threads `rm0..rm{N-1}`): in a graph of kind (b) every visible thread has
finished although the program as a whole is blocked.

Commands, run from `B=/local/home/mkhoshechin/Documents/github-jmc/plan/benchmarks`, for
`N ∈ {2, 3, 4}`, `v ∈ {correct, counting-bug}`:

```
$B/_tools/run-rust.sh $B/two-pc-lbinria spec <N> bag correct
$B/_tools/run-rust.sh $B/two-pc-lbinria impl <N> bag <v>
$B/_tools/run-java.sh $B/two-pc-lbinria 'bench.two_pc_lbinria.TwoPcLbinria:spec,<N>,bag,correct' must 100000 600
$B/_tools/run-java.sh $B/two-pc-lbinria 'bench.two_pc_lbinria.TwoPcLbinria:impl,<N>,bag,<v>' must 100000 600
```

Verdict check (TraceForge conformance, default stateful engine), `N ∈ {2, 3}`:

```
$B/_tools/run-rust.sh $B/two-pc-lbinria conf <N> bag correct        # silence at N=2 and N=3
$B/_tools/run-rust.sh $B/two-pc-lbinria conf <N> bag counting-bug   # 12 (N=2) / 324 (N=3) uncovered complete graphs
$B/_tools/run-rust.sh $B/two-pc-lbinria conf 2 bag counting-bug all  # every report kept: 4 crash (value) + 8 early commit (order)
```
