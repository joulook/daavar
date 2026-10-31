# A1: two-phase commit over r rounds

**Family:** popl-conf application  **Source:** the TraceForge fixture A1 (`P5-APPS`),
`traceforge/src/conformance/grid.rs` at `a3a7e1b`, lines 3106–3266 (`A1ToCoord`, `A1ToPart`,
`A1Coord`, `a1_participant`, `a1_coordinator`, `a1_impl`, `a1_spec`), ported to `rust/`.
**Expected verdict:** `correct`, `control-reverse`: **CONFORMS** (one decision, sent to every
participant after every vote is read); `eager`, `early-abort`, `silent`: **DOES NOT REFINE**
(split decision; a decision before another participant's vote; a participant never decided).

## 1. The protocol

Roles: visible `p0`, `p1`; invisible `coord`, and in the implementation `p2..p{n-1}` (invisible
participants). Each of `r` rounds: `coord` sends `Prepare(coord)` to every participant; a
participant reads it, votes by a free choice (`Yes` or `No`) to `coord`, then reads the round's
decision (`Commit` or `Abort`). `coord` learns the participants from `Init(ids)`, sent by `main`
in Rust (by the invisible `init` in Java, §8). Messages: `A1ToCoord::{Init(Vec<ThreadId>), Yes,
No}`, `A1ToPart::{Prepare(ThreadId), Commit, Abort}`; a thread id in a payload is exported as its
role name (`Prepare("coord")`). Parameters: `n` participants, `r` rounds. Smallest interesting
size: `n = 2, r = 1`.

## 2. The specification program

`a1_spec(r)`: two participants whatever `n`. `coord` sends `Prepare` to both, reads both votes,
then chooses `Commit` or `Abort` freely (`nondet()`) and sends that one decision to both, in
participant order. So every decision is the same for both participants and causally after both
votes; the votes do not constrain the decision.

## 3. The implementation program

`a1_impl(n, r, variant)`: `coord`, then `p0..p{n-1}`, each running the same participant code.
`correct`: read all `n` votes, `Commit` iff all are `Yes`, send it to every participant in order.
The variants change only the coordinator (§5).

## 4. Where they differ, and why the verdict is what it is

`correct`: the decision is one value, sent to `p0` and `p1` after every vote is read; the spec
can choose that value. Invisible participants (`n > 2`) only change which value is chosen, which
the free choice covers. **CONFORMS**. `control-reverse`: the same, decisions sent to `p{n-1}`
first; the order of two sends to different threads is not observed. **CONFORMS**.

`eager` (`n = 2, r = 1`): the coordinator reads one vote and at once sends `p0` the decision so
far, then reads the next and decides for `p1`. With `p0: S(Yes)` and `p1: S(No)` read in that
order, `p0: R(Prepare("coord")) S(Yes) R(Commit)`, `p1: R(Prepare("coord")) S(No) R(Abort)`: two
decisions. The spec sends one value to both. Component: **signature value** (also order: `p0`'s
decision is not after `p1`'s vote).

`early-abort` (`n = 2, r = 1`): on the first `No` read, `Abort` goes to both before the other
vote is read. `p0: S(No)` read first, `p0: R(Abort)` is not causally after `p1: S(vote)`; in the
spec every decision is after both votes. Component: **order**.

`silent` (`n = 2, r = 1`): in the last round the decision to the second participant (`p1`) is
not sent; `p1` ends blocked in every graph. Component: **signature status**.

## 5. Variants and faults

| variant | change (coordinator) | expected verdict | uncovered behaviour |
|---|---|---|---|
| `correct` | real 2PC | CONFORMS | none |
| `eager` | decide as each vote arrives | DOES NOT REFINE (n ≥ 2, r ≥ 1) | split decision (value) |
| `early-abort` | broadcast `Abort` on the first `No`, then drain | DOES NOT REFINE | decision before the other vote (order) |
| `silent` | last-round decision to `p1` never sent | DOES NOT REFINE | `p1` blocked (status) |
| `control-reverse` | decisions in reverse participant order | CONFORMS (control) | none |

Rust `conf` at `n = 2, r = 1` (`out/runs.log`): `correct` silence, `eager` 5 candidate
violations, `early-abort` 4, `silent` 8, `control-reverse` silence: as expected.

## 6. Files

- `source/`: none; the source is the TraceForge fixture named above (see `rust/src/main.rs`).
- `rust/`: the Must-API Rust crate on TraceForge's shared main; ARGS `<variant> <n> <r>`.
- `java/`: the Java twin, package `bench.a1_commit` (§8).
- `out/runs.log`: every command of the Java-twin round and its output; `out/export/`: the summary files.

## 7. Plain-Must exploration counts (both tools, one run each)

Model fifo (JMC p2p). TraceForge `count` prints `execs=<complete> blocked=<blocked>`; JMC
`graphs: C distinct complete, B distinct blocked`.

| side | size (n, r) | TraceForge ARGS | JMC args | TraceForge complete / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|---|
| spec | 2, 1 | `correct 2 1` | `spec,2,1,fifo` | 16 / 0 | 16 / 0 | yes |
| impl | 2, 1 | `correct 2 1` | `impl,2,1,fifo,correct` | 8 / 0 | 8 / 0 | yes |
| impl | 2, 1 | `eager 2 1` | `impl,2,1,fifo,eager` | 8 / 0 | 8 / 0 | yes |
| impl | 2, 1 | `early-abort 2 1` | `impl,2,1,fifo,early-abort` | 8 / 0 | 8 / 0 | yes |
| impl | 2, 1 | `silent 2 1` | `impl,2,1,fifo,silent` | 0 / 8 | 0 / 8 | yes |
| impl | 2, 1 | `control-reverse 2 1` | `impl,2,1,fifo,control-reverse` | 8 / 0 | 8 / 0 | yes |
| spec | 2, 2 | `correct 2 2` | `spec,2,2,fifo` | 256 / 0 | 256 / 0 | yes |
| impl | 2, 2 | `correct 2 2` | `impl,2,2,fifo,correct` | 64 / 0 | 64 / 0 | yes |
| impl | 2, 2 | `eager 2 2` | `impl,2,2,fifo,eager` | 64 / 0 | 64 / 0 | yes |
| impl | 2, 2 | `early-abort 2 2` | `impl,2,2,fifo,early-abort` | 64 / 0 | 64 / 0 | yes |
| impl | 2, 2 | `silent 2 2` | `impl,2,2,fifo,silent` | 0 / 64 | 0 / 64 | yes |
| impl | 2, 2 | `control-reverse 2 2` | `impl,2,2,fifo,control-reverse` | 64 / 0 | 64 / 0 | yes |
| spec | 3, 1 | `correct 3 1` | `spec,3,1,fifo` | 16 / 0 | 16 / 0 | yes |
| impl | 3, 1 | `correct 3 1` | `impl,3,1,fifo,correct` | 48 / 0 | 48 / 0 | yes |
| impl | 3, 1 | `eager 3 1` | `impl,3,1,fifo,eager` | 48 / 0 | 48 / 0 | yes |
| impl | 3, 1 | `early-abort 3 1` | `impl,3,1,fifo,early-abort` | 48 / 0 | 48 / 0 | yes |
| impl | 3, 1 | `silent 3 1` | `impl,3,1,fifo,silent` | 0 / 48 | 0 / 48 | yes |
| impl | 3, 1 | `control-reverse 3 1` | `impl,3,1,fifo,control-reverse` | 48 / 0 | 48 / 0 | yes |

Commands (exactly, `$R` = `/local/home/mkhoshechin/tmp/bench-target/release`, `$D` = the daavar tree):

```
$R/tf_a1_commit count spec --model fifo -- <variant> <n> <r>
$R/tf_a1_commit count impl --model fifo -- <variant> <n> <r>
java -Xss4m -cp $D/benchmarks/a1-commit/java/out:$(cat $D/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher \
     'bench.a1_commit.A1Commit:spec,<n>,<r>,fifo' must 0 1 <fresh report dir> - 600 </dev/null
java ... 'bench.a1_commit.A1Commit:impl,<n>,<r>,fifo,<variant>' must 0 1 <fresh report dir> - 600 </dev/null
```

## 8. Java twin

`java/bench/a1_commit/A1Commit.java` (an `MpProgram`) and `Support.java` (the model argument and
the start-then-await-all joiner, copied as in the other twins). Arguments
`A1Commit:spec,<n>,<r>,<fifo|bag>[,<variant>]` and `A1Commit:impl,<n>,<r>,<fifo|bag>,<variant>`;
the spec ignores the variant, as `a1_spec(r)` does, and has two participants whatever `n`.
Visible roles `p0`, `p1`; invisible `coord`, `p2..`, `init`. Messages are `@Tag` records with
the Rust variant names and field order: `Init(List<Tid>)`, `Yes`, `No`, `Prepare(Tid)`,
`Commit`, `Abort`; the vote and the spec's decision are `Nondet.chooseBoolean()`.

One departure of form: Rust's `main` sends `Init(ids)` to `coord` after spawning; a JMC main may
perform no message-passing event, so the invisible thread `init`, spawned last, sends the same
`Init`. `coord`'s single receive of it is unchanged, and the counts are equal (§7).

Cross-language check at `n = 2, r = 1` (exports in `out/export/`, joined with
`daavar/traceforge/target/release/conf-join <spec file> <impl file>`):

| variant | Rust spec vs Java spec, both ways | Rust impl vs Java impl, both ways | Rust spec ⊒ Java impl | Java spec ⊒ Rust impl | hand verdict |
|---|---|---|---|---|---|
| `correct` | conforms | conforms | conforms | conforms | CONFORMS |
| `eager` | conforms | conforms | violation, 5 uncovered | violation, 5 uncovered | DOES NOT REFINE |
| `early-abort` | conforms | conforms | violation, 4 uncovered | violation, 4 uncovered | DOES NOT REFINE |
| `silent` | conforms | conforms | violation, 4 uncovered | violation, 4 uncovered | DOES NOT REFINE |
| `control-reverse` | conforms | conforms | conforms | conforms | CONFORMS |

Not done: model `bag` was not run (the fixture is a fifo row); sizes beyond (3, 1).
