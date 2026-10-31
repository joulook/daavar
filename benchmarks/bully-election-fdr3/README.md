# Bully leader election, buggy and fixed (FDR3 inputs): not modelled

**Family:** related-work  **Source:** Roscoe's CSP model of the bully algorithm, the FDR3 TACAS 2014
input `related-work-papers/sources/fdr3-inputs/tacas14/Samples/bully.7.csp` (buggy) and the
corrected `fdr3-inputs/cloud14/Paper Scripts/bully.8.csp` (candidate N24, record "FDR/bully.7.csp"
in `CATALOGUE-artifacts-2.md`)  **Expected verdict:** none: **no Must-API pair was built**. The
buggy/fixed distinction of the source is a timing property, and the Must API has no time (§4).

## 1. The protocol

`bully.7.csp`: nodes `Proc = {0..N-1}` (N = 7; `bully.8.csp`: N = 8). Channels `election.i.j`,
`answer.i.j`, `coordinator.i.j`, `test.i.j`, `ok.i.j` (node to node), `fail.i`, `revive.i`
(the environment), `leader.i.j` (node i's opinion of the leader, the observed event) and `tock`
(discrete time, in every node's alphabet). A node starting an election sends `election` to every
higher node (`SendElections`) and waits `T1` tocks for an `answer` (`AwaitAnswers`); without one it
becomes coordinator and sends `coordinator` to every lower node, one at a time (`SendCoords`);
with one it waits `T2` tocks for a `coordinator` (`AwaitCoordinator`) and else starts again. A
running node tests its coordinator (`test`), and starts an election if no `ok` arrives before the
next tock (`Testing`). `fail.n` may happen in any state (`Failed`, absorbing every message);
`revive.n` after at least one tock. Communication is CSP handshake: `Network = || n:Proc @
[Alpha(n)] Node(n)`.

The specification `AGREEBY(0, alive)`: once `TS` tocks have passed since the last `fail`/`revive`
(and a `test`), every `leader.m.k` names `max(alive)`. In `bully.7.csp` the check `AGREEBY(0,Proc)
[T= Network\diff(Events,AlphaAgree)` is asserted **not** to hold (the comments: "making the
distribution of the co-ordinator messages non-atomic with the possibility of a process failing
mid-way through a distribution creates havoc with the algorithm"). `bully.8.csp` fixes it: a node
that has just received `coordinator` waits one tock and then tests its coordinator
(`Running''` → `Testing1` → `Testing2`), the revive delay becomes `TR`, and `T2 = 2`, `T3 = 5`;
there the same assertion holds.

## 2. The specification program

Not written. A specification program would be an oracle naming `max(alive)` in every visible
`Leader(i, k)`, but `AGREEBY` only constrains opinions expressed after `TS` (`T3`) tocks of
stability; before that any opinion is allowed (§4, reason 2).

## 3. The implementation program

Not written. The node state machine itself is expressible: every CSP state is an external choice
over receive events and at most one send, which is a loop over one mailbox with a type-guarded
blocking receive, `fail`/`revive` being messages from an invisible injector and `Failed` a state
that receives and drops everything. What is not expressible is what drives the transitions that
matter, the `tock`s (§4).

## 4. Where they differ, and why the verdict is what it is

The source cannot be modelled faithfully in the Must API (one mailbox per thread, blocking
receives with type-test guards, non-blocking receives that may read ⊥ at any time), for three
reasons, each sufficient to change the verdicts:

1. **Every timeout is a count of global `tock`s under maximal progress.** `tock` is in every
   node's alphabet, so all nodes advance time together, and the events `election`, `answer`,
   `coordinator`, `ok` are urgent (`Urgent = diff(Events, union({tock}, Delayable))`, the timing
   consistency check `TOCKS`): a live higher node's `answer` always happens before the `T1`-th
   tock. In the Must API a timeout can only be a non-blocking receive that reads ⊥, and rule 1
   (`RELATED-BENCHMARKS.tex`) lets it read ⊥ whether or not a message is pending. A timeout can then
   fire before an answer that a live node is about to send. Already at N = 2 without any failure:
   node 0's test of node 1 times out, node 0 starts an election, its `AwaitAnswers` times out before
   node 1's `answer`, and node 0 becomes coordinator and says `leader.0.0` while node 1 says
   `leader.1.1`. This run exists in both `bully.7` and `bully.8` once time is gone, so both
   versions violate any agreement specification and the buggy/fixed pair no longer separates. The
   fix of `bully.8` is itself a timing fix ("test their coordinator a small time after being sent
   the coordinator message", "in any case enough for the coordinator to have finished
   distribution"), and it has no untimed counterpart.
2. **The specification is a timed trace property.** `AGREEBY(n, alive)` allows any `leader`
   event until `n` tocks have passed, and is reset by every `fail`, `revive` and `test`. Without
   `tock` there is no stability point. The catalogue's substitute (compare final opinions, status
   done) needs the run to end, but every node of the source runs forever (`RunAsCoord`,
   `Running` loop on `tock`), so "final" would be an artefact of the bound chosen for elections
   and tests, not a property of the source.
3. **Communication is synchronous.** `coordinator.n.j` in `SendCoords` happens only together with
   node j's receive; the bug the comments describe is about which lower nodes have *received* the
   coordinator message when `fail.n` happens mid-distribution. An asynchronous send separates
   sending from receiving and adds interleavings in which a node has been sent the message but not
   received it, so even the failure scenario differs from the source's.

The candidate record already rated this pair `maybe` for the same reason ("the bug's mechanism
depends on timing (`tock`) and the settle window"). A Must program that replaced the timeouts by a
perfect failure detector (a timeout fires only if the awaited node has failed) would be a
different, synchronous-system abstraction of the bully algorithm, not a translation of these two
files, and whether the seeded mid-distribution failure still separates the two versions under that
abstraction is not something the source answers; it was not built.

## 5. Variants and faults

| variant | model | how it differs | expected verdict | reason |
|---|---|---|---|---|
| (`bully.7.csp`, buggy) | — | the source's own fault: non-atomic coordinator distribution + failure mid-way | not run | §4 |
| (`bully.8.csp`, fixed) | — | test the coordinator one tock after being bullied | not run | §4 |

## 6. Files

- `source/`: `bully.7.csp`, `bully.8.csp`, copied verbatim, provenance in `source/PROVENANCE.md`.
- `rust/`, `java/`: not present (no faithful program, §4).

## 7. Plain-Must exploration counts (both tools, one run each)

None: no program was built, nothing was run.
