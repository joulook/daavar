# Zab broadcast phase (with a late follower) against one sequencer

**Family:** related-work  **Source:** the Zab protocol spec vendored in the Remix artifact
(EuroSys 2025), `related-work-papers/sources/remix/Remix-artifact/apache-zookeeper-3.9.1/zookeeper-specifications/protocol-spec/{Zab.tla, issues.md}`
(CANDIDATES.md N18, record in CATALOGUE-mocket-remix-oortwijn.md)  **Expected verdict:**
`correct`: **CONFORMS** (every write is committed once, in one total order of zxids, and its
client is answered with its zxid); `issue1-propose-to-ackld`: **DOES NOT REFINE** (Issue 1 of
`issues.md`: the late follower misses a `PROPOSE` and can never commit its own client's write,
so `c1` ends blocked where the specification always answers it).

## 1. The protocol

Zab's broadcast phase (`Zab.tla`, lines 886–1077): the leader turns a client request into a
transaction with zxid `<<epoch, counter>>` and appends it to `history`
(`LeaderProcessRequest`), sends `PROPOSE` to the followers in `ackeRecv`
(`LeaderBroadcastPROPOSE`, `Broadcast`); a follower appends the next zxid and answers `ACK`
(`FollowerProcessPROPOSE`; a zxid that is neither next nor in its log is discarded); the leader
commits the next index once a quorum has acknowledged it and sends `COMMIT` to `ackldRecv`
(`LeaderProcessACK`, `LeaderTryToCommit`, `DiscardAndBroadcast`); a follower commits the next
pending transaction if it carries that zxid (`FollowerProcessCOMMIT`). All three issues of
`issues.md` involve a follower that joins while the leader is already broadcasting, so the
benchmark keeps exactly that part of synchronisation: the late follower's `ACKEPOCH` is answered
with `NEWLEADER(history)` and the follower is added to `ackeRecv` (`LeaderProcessACKEPOCH`,
case 1); the follower adopts the history and answers `ACKLD(last zxid)`
(`FollowerProcessNEWLEADER`); the leader adds it to `ackldRecv`, adds it to the acks of the
history up to that zxid, and answers `COMMITLD(lastCommitted.zxid)` (`LeaderProcessACKLD`,
case 2, with the fix of Issue 3); the follower commits its log up to that zxid
(`FollowerProcessCOMMITLD`). No election (the leader is fixed), no `CEPOCH`/`NEWEPOCH`, one
epoch (a zxid is its counter), no timeout, no restart.

Roles:

- Visible: `c0`, `c1` (clients; Q times: send `Write { c, v }`, block on `Committed { zxid }`;
  `v = 10 c + j + 1` for the j-th write).
- Invisible: `server` (specification); `s0` (leader), `s1` (follower, synchronised before the
  broadcast phase), `s2` (late follower) (implementation).

`c0` writes to the leader; `c1` writes to `s2`, which forwards its writes to the leader once it
has processed `COMMITLD` (ZooKeeper's `FollowerRequestProcessor`; a learner serves clients only
after synchronising; `Zab.tla` lets only the leader take requests, see the comment above `LeaderProcessRequest`). A server answers
its own client when it commits that client's transaction. Messages between servers (invisible):
`AckEpoch`, `NewLeader { hist }`, `AckLd { zxid }`, `CommitLd { zxid }`, `Propose { zxid, c, v }`,
`Ack { zxid, from }`, `Commit { zxid }`. Parameters: 3 servers, 2 clients (fixed), Q writes per
client (the size), quorum 2. Smallest interesting size: Q = 1.

## 2. The specification program

One invisible `server` (rule 2: one replicated log is one unit; every write of either client is
ordered by it). It performs 2Q blocking receives of `Write { c, v }`; the n-th one read gets zxid
n and is answered with `Committed { zxid: n }` to client `c`. Visible behaviour: each client's
j-th write is answered with a zxid; the zxids of all writes are 1..2Q, each once, in an order
consistent with every client's own order; an answer happens after its write and after every write
the server read before it. Every specification execution ends with both clients done.

## 3. The implementation program

`s0` starts in BROADCAST with an empty history, `ackeRecv = ackldRecv = {s1}`. On `Write`: append
`(zxid, c, v)` with acks `{s0}` and send `Propose` to `ackeRecv` (request and broadcast are one
step here, the first repair scheme of Issue 2, so the redundant `PROPOSE` of Issue 2 does not
arise). On `Ack { zxid, from }`: add `from` to the acks; if the index is the next to commit and
has a quorum (2 of 3), commit it, send `Commit` to `ackldRecv`, and answer `c0` if it is `c0`'s.
On `AckEpoch`: add `s2` to `ackeRecv`, send `NewLeader(history)`. On `AckLd { zxid }`: add `s2` to
`ackldRecv` and to the acks up to `zxid`, send `CommitLd(lastCommitted)`. `s1` and `s2` run
`FollowerProcessPROPOSE` / `FollowerProcessCOMMIT`; `s2` first sends `AckEpoch` and runs
`FollowerProcessNEWLEADER` / `FollowerProcessCOMMITLD`, holds `c1`'s writes until it is
synchronised and then forwards them, and answers `c1` when it commits `c1`'s transaction. The
servers are blocking loops that end blocked when nothing more comes (the A4 idiom; their status is
invisible), so every implementation execution counts as blocked. Model: p2p (`Zab.tla`'s
`msgs[i][j]` are FIFO queues, `Send = Append`).

## 4. Where they differ, and why the verdict is what it is

Notation: `S(m)` a send, `R(m)` a receive, per visible thread in program order.

**`correct` conforms.** `s2` is in `ackeRecv` from the moment the leader sends it `NEWLEADER`,
so every transaction created later reaches it as a `PROPOSE` after the `NEWLEADER` (FIFO), and
every earlier one is in the `NEWLEADER` history: its log is the leader's history. A transaction
committed before the leader processes `ACKLD` is covered by `COMMITLD(lastCommitted)`, one
committed after it by a `COMMIT` (`s2` is then in `ackldRecv`, and the `COMMIT` follows the
`COMMITLD` on the FIFO link). `c1`'s writes are forwarded only after `COMMITLD`, so each is
proposed to and committed at `s2`, and `c1` is answered; `c0` is answered by the leader. The
leader commits in zxid order, so the zxids answered are 1..2Q in the leader's order of writes,
which is the specification's order of reads; each answer is sent after the commit of its zxid,
which is after every write with a smaller zxid reached the leader. So every implementation word
is a specification word whose order the implementation contains. **CONFORMS.**

**`issue1-propose-to-ackld` does not refine** (`issues.md`, Issue 1: the leader broadcasts
`PROPOSE` to `ackldRecv`). Smallest size Q = 1, schedule: `s0` processes `s2`'s `AckEpoch`
(sends `NewLeader([])`), then `c0`'s `Write(0, 1)`: zxid 1, `PROPOSE` to `ackldRecv = {s1}` only;
then `s2`'s `AckLd(0)` (`s2` joins `ackldRecv`, `CommitLd`); `s1`'s `Ack(1)` commits zxid 1
(`c0` gets `Committed(1)`). `s2`'s log lacks zxid 1 for ever. `s2`, synchronised, forwards
`c1`'s `Write(1, 11)`, which becomes zxid 2; `s2` discards `PROPOSE(2)` (neither next nor in its
log: `Zab.tla`'s `proposalInconsistent`) and ignores `COMMIT(2)` ("Committing zxid without
seeing txn"), so it never commits `c1`'s transaction. Uncovered behaviour: `c0: S(Write(0,1))
R(Committed(1))`, `c1: S(Write(1,11))` with `c1` **blocked**; in every specification execution
both clients end done. Component: **signature status**.

Reachability was checked with `out/fault-check/` (the same crate with an assertion in the
followers: a server never discards a `PROPOSE` of its own client's transaction for want of an
earlier one). Executed results (2026-10-30), one run each, from
`$B/zab-broadcast-remix/out/fault-check` with
`export CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/bench-target; export TMPDIR=/local/home/mkhoshechin/tmp`:

| claim | command | result |
|---|---|---|
| Issue 1 loses `c1`'s write, Q=1 | `cargo run -q -j 2 --offline --release -- impl 1 fifo issue1-propose-to-ackld` | panic `s2 discards PROPOSE(2, c1, 11): its log [] lacks an earlier txn` |
| Issue 1 loses `c1`'s write, Q=2 | `cargo run -q -j 2 --offline --release -- impl 2 fifo issue1-propose-to-ackld` | panic `s2 discards PROPOSE(2, c1, 11): its log [] lacks an earlier txn` |
| `correct` never loses it, Q=1 / Q=2 | `cargo run -q -j 2 --offline --release -- impl <Q> fifo correct` | exit 0, `execs=0 blocked=161` / `execs=0 blocked=33161` |

Every claim reproduced; the holding runs explore as many executions as the plain-Must counts in
§7. Every command and its output is in `out/fault-check.log`. The check shows the lost entry; that
`c1` then blocks follows from §1 (only `s2` answers `c1`, and only on committing `c1`'s entry).

## 5. Variants and faults

| variant | how it differs | expected verdict | reason |
|---|---|---|---|
| `correct` | `PROPOSE` to `ackeRecv`, `COMMIT` to `ackldRecv`, `COMMITLD(lastCommitted)` (the spec as shipped) | CONFORMS | §4 |
| `issue1-propose-to-ackld` | `PROPOSE` to `ackldRecv` (Issue 1 reverted) | DOES NOT REFINE (Q ≥ 1) | `c1` blocked |

Issue 2 (redundant `PROPOSE`) cannot arise: request and broadcast are one leader step, and the
follower discards a redundant `PROPOSE` anyway (the scheme the authors chose). Issue 3
(`COMMITLD` carrying the `ACKLD` zxid) is not built as a variant: with `COMMIT` to `ackldRecv`,
reverting it lets `s2` miss the commit of a transaction it holds, after which every later
`COMMIT` mismatches; that would be a second status fault of the same kind (reasoned, not run).

## 6. Files

- `source/`: `Zab.tla`, `issues.md`, `doc.md`, copied verbatim, with provenance in
  `source/PROVENANCE.md`.
- `rust/`: crate `bench_zab_broadcast_remix`. CLI, the shared main: `<bin> count|export|conf|online
  … --model M -- <Q> [variant]`; default Q 1, default model `fifo`, default variant `correct`. The
  `out/fault-check` crate is a separate copy, not on the shared main: it keeps the old positional
  CLI `<spec|impl> <size> [fifo|bag|causal] [variant]`, so its commands stand as run. Each thread
  has an inbox channel built by main before any spawn (the servers address each other). See
  `_tools/run-rust.sh`.
- `java/`: package `bench.zab_broadcast_remix`, class `ZabBroadcast` (args
  `<spec|impl>,<Q>,<fifo|bag>,<variant>`), `Support` (the model argument and a thread registry
  that mints every id before any thread starts, as in `ironlock/`). Spawn order: spec
  `server, c0, c1`; impl `s0, s1, s2, c0, c1`. Visible roles `c0`, `c1` on both sides and in both
  languages. See `_tools/run-java.sh`.
- `out/runs.log`: every plain-Must command and its output; `out/fault-check/`: the reachability
  crate; `out/fault-check.log`: every reachability command and its output.

## 7. Plain-Must exploration counts (both tools, one run each)

Model `fifo` (p2p) throughout. Size = Q (writes per client), 2 clients, 3 servers.

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | Q=1 | fifo | 2 / 0 | 2 / 0 | yes |
| spec | Q=2 | fifo | 6 / 0 | 6 / 0 | yes |
| impl `correct` | Q=1 | fifo | 0 / 161 | 0 / 161 | yes |
| impl `correct` | Q=2 | fifo | 0 / 33161 | 0 / 33161 | yes |
| impl `issue1-propose-to-ackld` | Q=1 | fifo | 0 / 128 | 0 / 128 | yes |
| impl `issue1-propose-to-ackld` | Q=2 | fifo | 0 / 24215 | 0 / 24215 | yes |

All JMC runs ended with "stack exhausted". No disagreement. Every implementation execution is
blocked because the three server loops end blocked (§3); the clients' statuses are not visible in
these counts. Sizes are Q = 1, 2, the lead's range ("1–2 writes"), not the brief's default 2, 3:
Q = 3 was not run, because Q = 2 already took more than a minute on JMC (`correct`).

Commands, run from `B=/local/home/mkhoshechin/Documents/github-jmc/plan/benchmarks`, for
`Q ∈ {1, 2}`, `v ∈ {correct, issue1-propose-to-ackld}`:

```
$B/_tools/run-rust.sh $B/zab-broadcast-remix count spec --model fifo -- <Q>
$B/_tools/run-rust.sh $B/zab-broadcast-remix count impl --model fifo -- <Q> <v>
$B/_tools/run-java.sh $B/zab-broadcast-remix bench.zab_broadcast_remix.ZabBroadcast:spec,<Q>,fifo must 100000 600
$B/_tools/run-java.sh $B/zab-broadcast-remix bench.zab_broadcast_remix.ZabBroadcast:impl,<Q>,fifo,<v> must 100000 600
```

Every count command and its output is in `out/runs.log`; every reachability command and its
output is in `out/fault-check.log`.
