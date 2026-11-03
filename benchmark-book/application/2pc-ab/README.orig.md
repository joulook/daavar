# One-transaction 2PC with early abort and abstaining participants (Kragl et al., PLDI 2020)

**Family:** related-work  **Source:** Kragl, Enea, Henzinger, Mutluergil, Qadeer, "Inductive
Sequentialization of Asynchronous Programs", PLDI 2020, artifact file
`related-work-papers/sources/kragl-pldi20-artifact/inductive-sequentialization-pldi2020/boogie/Test/civl/inductive-sequentialization/2PC.bpl`
(candidate N21, record "KIS/2PC.bpl" in `CATALOGUE-artifacts-2.md`)  **Expected verdict:**
`correct`: **CONFORMS** (the coordinator commits only after `n` `Yes` votes and sends one decision
to everyone, which `MAIN5` allows); `off-by-one`: **DOES NOT REFINE** (it commits after `n − 1`
votes: at n = 1 a participant that voted `No`, or did not vote, receives `Commit`).

## 1. The protocol

`2PC.bpl`, layer 1: a coordinator (pid 0) and `n > 0` participants (pids 1..n), one transaction.
`coordinator1` sends a request to every participant (`send_req(i)`); `coordinator2` receives
votes (`receive_vote`, from the multiset `VoteCH`, no sender identity), **breaks at the first
`NO()`** with `ABORT()`, else decides `COMMIT()` after `n` `YES()`; the votes after a `NO` are not
drained; then it sends the decision to every participant (`send_dec(i, d)`). `participant1`
either (`if (*)`) receives its request, picks a vote (`havoc v`) and sends it, or **abstains**:
it neither receives the request nor votes; either way `participant2` receives the decision. If a
participant abstains and every other votes `Yes`, the coordinator waits for a vote that never
comes and everyone blocks (the source proves safety only).

Roles:

- Visible: `p1 .. pn` (the participants; the same code on both sides).
- Invisible: `coord` (different code: §2, §3).

Messages: `Req` (coord → participant), `Vote(Yes | No)` (participant → coord), `Dec(Commit |
Abort)` (coord → participant). Each participant's request and decision are separate message
types, so the abstaining participant's receive of the decision selects `Dec` (the source's separate
`ReqCH`/`DecCH`).

Parameter: `n` participants (the size), from 1 (the source's `axiom n > 0`). Model `bag` (the
source's channels are multisets).

## 2. The specification program

`coord` runs a message-level specification derived from `MAIN5`. `MAIN5` is a ghost-state predicate:
`dec ∈ {COMMIT, ABORT}` with `dec == COMMIT ==> forall i :: votes[i] == YES()`, where `votes` is
havoced by `MAIN5` itself, not initialised by `Init`, and never written by an abstainer; taken
literally it does not say that every participant *sent* `Yes`, nor that those sends precede the
commit. The specification program adds that commit evidence on purpose: `COMMIT` is decided only
after a `Yes` vote has been received from every participant, so every `Yes` send causally precedes
every `Commit` receive; `ABORT` is allowed at any point, as `MAIN5` places no precondition on it.
As a program: send `Req` to every
participant (they must be able to vote); then, before each of up to `n` blocking vote receives,
it may abort (`nondet()`: send `Abort` to everyone and stop); after `n` votes, `Commit` if all
were `Yes` and a `nondet()` says so, else `Abort`; the decision goes to every participant. One
coordinator thread orders every decision (rule 2). The abort-at-any-point choice is `MAIN5`'s
freedom to abort whatever the votes; a commit is causally after all `n` `Yes` sends (the oracle
received them), which is how "every participant voted `Yes`" becomes observable.

## 3. The implementation program

`coord` runs `coordinator1` + `coordinator2` as one thread: `Req` to everyone; `d := Commit`; up
to `n` vote receives, `Abort` and stop receiving at the first `No`; `Dec(d)` to everyone. The
participants are the shared code: `if nondet() { receive Req; v := Yes or No (nondet()); send
Vote(v) }; receive Dec`.

Departures: `set_vote`/`set_decision` (ghost state) are dropped, the decision being the `Dec`
message the participant receives; `participant1`/`participant2` and `coordinator1`/`coordinator2`
(pending asyncs) are one thread each, in program order, the only order the source's spawns allow.

## 4. Where they differ, and why the verdict is what it is

**`correct`, CONFORMS.** Each implementation graph is a specification graph with the same vote
receives: the oracle receives the same votes from the same participants in the same order, takes
the "receive" branch each time, and then: if the implementation broke at a `No`, the oracle aborts
at its next abort choice (or, at the last vote, by `all_yes = false`), sending `Abort` to everyone,
with the same causal predecessors; if the implementation received `n` `Yes`, the oracle chooses
`Commit`; if the implementation blocked (an abstainer, no `No` received), the oracle blocks on the
same receive. Participants' words and statuses are identical, and the decision's causal past (the
votes received) is the same.

**`off-by-one`, DOES NOT REFINE**, smallest size n = 1 (`quorum = n − 1 = 0`: the coordinator
commits without receiving a vote). Uncovered words (2 of the implementation's 3 graphs):

```
p1: R(Req) S(Vote(No)) R(Dec(Commit))     -- voted No, told Commit
p1: R(Dec(Commit))                         -- abstained, told Commit
```

The specification commits only after receiving `n` `Yes` votes, so neither word is possible
(the second would need a vote from p1). The third graph, p1 votes `Yes` and receives `Commit`,
is covered (one visible thread: no causal counts). Component: **signature value** (`Dec(Commit)`
where every specification graph with that prefix has `Dec(Abort)` or blocks). At n = 2 (6 of 13
graphs uncovered) the fault also shows in the order: with both voting `Yes` and the coordinator
reading only one vote, `Dec(Commit)` at either participant has the other participant's
`S(Vote(Yes))` outside its causal past, while every specification `Commit` follows both votes
(**order**); the other four are value faults (an abstainer and a `Yes`: `Commit`; a `Yes` and a
`No`, the `Yes` read: `Commit`).

## 5. Variants and faults

| variant | model | how it differs | expected verdict | reason |
|---|---|---|---|---|
| `correct` | bag | the source's coordinator (early abort, no drain) | CONFORMS | §4 |
| `off-by-one` | bag | the vote loop runs `n − 1` times (`while (i < n - 1)`) | DOES NOT REFINE (n ≥ 1) | `Commit` after a `No` or without every vote |

The source ships no fault (its `.expect` is "0 errors") and the abstain path is the protocol's own
behaviour, not a fault. The seeded fault is the catalogue's (c), a quorum off-by-one: it breaks
the derived commit-evidence requirement of §2 (a `Commit` decided without a `Yes` received from
every participant), which is what `MAIN5`'s clause `dec == COMMIT ==> forall i :: votes[i] ==
YES()` expresses for the participants that voted; the abstention witness (a `Commit` reaching a
participant that never voted) and the order witness at n = 2 (a `Commit` not causally after one
participant's `Yes`) are violations of the derived requirement, not of the literal ghost predicate.
With abstaining participants a missing vote is an ordinary run, so the fault is reachable at the
smallest size.

## 6. Files

- `source/`: `2PC.bpl`, `2PC.bpl.expect`, copied verbatim, provenance in `source/PROVENANCE.md`.
- `rust/`: crate `bench_two_pc_abstain_kragl` on TraceForge's shared main; ARGS `<n> [correct|off-by-one]`,
  model by `--model bag`. Visible `p1 .. pn`.
- `java/`: package `bench.two_pc_abstain_kragl`, class `TwoPc` (args `<spec|impl>,<n>,<fifo|bag>[,<variant>]`),
  `Support` (model argument and thread joiner, copied from `H1Support`). Spawn order `coord, p1 .. pn`
  on both sides and in both languages. `Ballot` and `Decision` are sealed interfaces of `@Tag`
  records (`Yes`, `No`, `Commit`, `Abort`), the Rust unit variants. Compiled to `java/out/`.
- `out/runs.log`: every command and its output; `out/export/`: the five summary files of §7.

## 7. Plain-Must exploration counts (both tools, one run each)

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | n=1 | bag | 6 / 1 | 6 / 1 | yes |
| impl `correct` | n=1 | bag | 2 / 1 | 2 / 1 | yes |
| impl `off-by-one` | n=1 | bag | 3 / 0 | 3 / 0 | yes |
| spec | n=2 | bag | 31 / 5 | 31 / 5 | yes |
| impl `correct` | n=2 | bag | 10 / 3 | 10 / 3 | yes |
| impl `off-by-one` | n=2 | bag | 12 / 1 | 12 / 1 | yes |
| spec | n=3 | bag | 207 / 31 | 207 / 31 | yes |
| impl `correct` | n=3 | bag | 57 / 10 | 57 / 10 | yes |
| impl `off-by-one` | n=3 | bag | 63 / 4 | 63 / 4 | yes |
| spec | n=4 | bag | 1713 / 249 | 1713 / 249 | yes |
| impl `correct` | n=4 | bag | 336 / 41 | 336 / 41 | yes |
| impl `off-by-one` | n=4 | bag | 360 / 17 | 360 / 17 | yes |
| spec | n=5 | bag | 17253 / 2491 | 17253 / 2491 | yes |
| impl `correct` | n=5 | bag | 2085 / 206 | 2085 / 206 | yes |
| impl `off-by-one` | n=5 | bag | 2205 / 86 | 2205 / 86 | yes |

All runs in the table exhausted; no disagreement. n = 5 is the first size over a minute (JMC
spec), so the table stops there. Four n = 6 runs were started by mistake after that point (three
finished; the JMC `impl,6,bag,correct` run was terminated by hand, exit 143, and has no final
counts); they are in
`out/runs.log`, marked, and not in the table (TraceForge spec 207045 / 29893, JMC spec not
exhausted at 600 s, TraceForge impl `correct` 13968 / 1237).

Hand counts at n = 1 and 2 (participant choices A = abstain, Y, N): impl `correct` n = 1: A blocks,
Y commits, N aborts (2 / 1); n = 2: AA, AY, YA block (3), AN, NA (2), YY (2: which vote is read
first), YN, NY (4: the `Yes` read first or not), NN (2): 10 complete. Spec n = 1: A: abort first
(1) or block (1); Y: abort first, or receive and then commit or abort (3); N: abort first, or
receive and abort (2): 6 / 1; n = 2 by the same rule: AA 1 / 1, AY and YA 2 / 1 each, AN and NA
2 / 1 each, YY 7, YN and NY 5 each, NN 5: 31 / 5.

Conformance and the cross-language join, model bag:

| check | spec | impl | verdict |
|---|---|---|---|
| `conf` n=1 | Rust spec | Rust `correct` | conforms (silence) |
| `conf` n=1 | Rust spec | Rust `off-by-one` | 2 candidate violations (exit 1) |
| `conf` n=2 | Rust spec | Rust `correct` | conforms (silence) |
| `conf` n=2 | Rust spec | Rust `off-by-one` | 6 candidate violations (exit 1) |
| `conf-join` n=1 | Rust spec export | Java `correct` export | conforms, 0 uncovered |
| `conf-join` n=1 | Rust spec export | Java `off-by-one` export | violation, 2 uncovered (`no-slot`: the two words of §4) |
| `conf-join` n=1 | Java spec export | Rust `correct` export | conforms, 0 uncovered |

Commands (X = `/local/home/mkhoshechin/tmp/bench-target/release/bench_two_pc_abstain_kragl`, built with
`CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/bench-target cargo build --release -j 4` in `rust/`;
J = `java -Xss4m -cp java/out:$(cat daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher`,
Java compiled with `javac -d java/out -cp $(cat daavar/jmc/cp.txt) java/bench/two_pc_abstain_kragl/*.java`),
for `n ∈ {1..5}`, `v ∈ {correct, off-by-one}`, all under `timeout 600`:

```
X count spec --model bag <n>
X count impl --model bag <n> <v>
J 'bench.two_pc_abstain_kragl.TwoPc:spec,<n>,bag' must 0 1 <fresh report dir> - 600 </dev/null
J 'bench.two_pc_abstain_kragl.TwoPc:impl,<n>,bag,<v>' must 0 1 <fresh report dir> - 600 </dev/null
X conf --model bag <1|2> <v>
X export spec --out out/export/rust-spec-n1.jsonl --model bag --program two-pc-abstain-kragl --size n1 1
X export impl --out out/export/rust-impl-correct-n1.jsonl --model bag --program two-pc-abstain-kragl --size n1 1 correct
J 'bench.two_pc_abstain_kragl.TwoPc:<side>,1,bag[,<v>]' must 0 1 <fresh report dir> out/export/java-<side>[-<v>]-n1.jsonl 600 </dev/null
daavar/traceforge/target/release/conf-join <spec file> <impl file>
```
