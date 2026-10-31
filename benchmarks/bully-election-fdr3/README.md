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

**Status: deferred, not impossible.** No faithful *distributed* port of these two files was built,
for the three reasons below, and a direct translation of the nodes to Must threads (asynchronous
sends, timeouts as non-blocking receives) changes the source: with that encoding both `bully.7` and
`bully.8` mis-elect already at N = 2 without any failure, so the buggy/fixed pair no longer
separates. That is a property of the naive encoding, not an expressiveness limit of the API.
Discrete time can be represented explicitly in finite state and messages: an invisible driver can
hold the CSP control states of all nodes, choose an enabled joint event, update both endpoints of a
rendezvous atomically, and advance every node on an enabled `tock`, with one visible observer
receiving the retained alphabet (`tock`, `fail`, `revive`, `test`, `leader`) and the specification
generating the bounded `AGREEBY` traces through the same interface. Such a central interpreter is a
bounded *trace encoding* of the CSP scripts: it would decide the pair, but its evaluation value
differs from a distributed port (one driver thread, no message-level concurrency), and whether a
small bound separates the two versions has not been shown by a witness. The directory therefore
keeps the sources and this record; the encoding is deferred.

Why the direct translation does not work:

1. **Every timeout counts global `tock`s.** `tock` is in every node's alphabet, so all nodes advance
   time together; a live higher node's `answer` is meant to precede the `T1`-th tock. (The scripts
   name a set `Urgent = diff(Events, union({tock}, Delayable))`, which by itself installs no priority
   operator, and the timing-consistency `TOCKS` assertions are commented out; the timing
   restrictions follow from the process transitions and the `tock` synchronisation, not from the
   set name.) Translated as a non-blocking receive, a timeout can read ⊥ whether or not a message is
   pending (rule 1 of `RELATED-BENCHMARKS.tex`), so it can fire before an answer a live node is
   about to send: node 0's test of node 1 times out, node 0 starts an election, its `AwaitAnswers`
   times out before node 1's `answer`, and the two nodes announce different leaders. The `bully.8`
   fix is itself a timing fix ("test their coordinator a small time after being sent the
   coordinator message") with no untimed counterpart.
2. **The specification is a timed trace property.** `AGREEBY(n, alive)` allows any `leader` event
   until `n` tocks have passed; it is reset by `fail` and `revive`, and only the post-failure state
   `AGREEBY'` starts its countdown on `test` (`bully.7.csp` lines 226 and 235 preserve the counter on
   `test`; line 244 starts it), with the analogous distinction in `bully.8.csp`. Without `tock` there
   is no stability point; the catalogue's substitute (compare final opinions) needs the run to end,
   but every node runs forever (`RunAsCoord`, the `Running` loop on `tock`).
3. **Communication is synchronous.** `coordinator.n.j` happens only together with node j's receive;
   the described bug is about which lower nodes have *received* the coordinator message when
   `fail.n` happens mid-distribution. An asynchronous send separates sending from receiving and adds
   interleavings the source does not have.

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
