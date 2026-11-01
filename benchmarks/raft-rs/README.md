# raft-rs: tikv/raft-rs, unmodified, under the Must API

**Program:** `raft`$(e,t)$: three Raft nodes (tikv/raft-rs core, `RawNode<MemStorage>`, commit
ad13f3d, unmodified), two visible clients `c0 c1`, client `c_i` attached to node `n_i`. Each client
proposes `e` values to its node, then receives `Committed(v)` from it until all of its own values
have been seen. `t` bounds the election timeouts each candidate node may fire (an invisible
`nondet()` choice while no leader is known); `cand` (default 3) is the number of nodes allowed to
time out. Model: `fifo` (per-pair order; raft-rs assumes an ordered transport per peer).

**Expected verdict:** `correct`: **CONFORMS** (certified by the spike of 2026-10-31 at
`e=1 t=1 cand=1`, in-process stateful, sequential; `source/spike-runs.log`). `reorder`: **no hand-derived
verdict** (the run decides; §2 gives the analysis: under per-pair FIFO delivery the two clients
appear to see the same application batches, so the fault may be unobservable in this harness).

**No Java side.** This directory has no Java twin; the Java side of the case study is
`benchmarks/microraft` (a real Java Raft core under JMC's Must API, in preparation), with the same
visible interface and specification. Rule 3 (identical visible interface) holds between the Rust
specification and implementation here.

## 1. The protocol (implementation side)

Each invisible node thread owns one `RawNode`. Its loop: either fire an election timeout
(`tick()` ELECTION_TICK = 2 times; only while `leader_id` is unknown, at most `t` times per
candidate node) or receive one message: a raft-rs wire message (`step`), or a client's
`Propose(v)` (queued until a leader is known, then `propose`). After each step the node drains
`ready()`: sends raft-rs's messages to the peers, appends entries to its `MemStorage`, persists the
hard state, and hands **committed** entries to its client as `Committed(v)` (`apply`). No snapshots,
no configuration changes, no crashes. The harness is `rust/src/main.rs`; raft-rs itself is a git
dependency pinned at its commit, with no patch.

The spec is the client-visible content of Raft's TLA+ specification (`raft.tla`, Ongaro; a copy
is in `plan/benchmarks/related-work-papers/sources/mocket-raft-tla`): one totally ordered committed
log. One invisible `log` thread receives every proposal; each is either appended, and then
delivered as `Committed(v)` to every client in log order, or lost (never delivered). Clients are the
same program on both sides.

## 2. Variants

- `correct`: raft-rs as is.
- `reorder`: a fault of this harness's delivery layer, not of raft-rs: `apply` hands a batch of
  committed entries to the client in reverse log order. A violation needs two clients to observe
  incompatible orders of the same values, e.g. `c0: R(Committed(3)) R(Committed(1))` against
  `c1: R(Committed(1)) R(Committed(3))` at `e=2` (`c0` proposes 1 and 3, `c1` proposes 2 and 4; at
  `e=1` each client stops after its single own value, so no client can observe two values in an
  order the specification lacks). A batch forms where a node's commit index jumps over two data
  entries in one `Ready`: at the leader, when one acknowledgement covers two entries (a follower in
  raft-rs's probe state receives the entries it missed in one `AppendEntries` and acknowledges them
  together); at a follower, when one message raises its commit index over two entries it holds.
  Whether the two clients can see *different* batchings is not established: the leader broadcasts
  its commit index after every commit step, per-pair FIFO delivery hands a follower those
  broadcasts in order and after the entries they cover, so the follower's commit-index jumps repeat
  the leader's and both clients see the same batches, reversed alike, which some single
  specification order matches. This analysis is hand-derived and not a proof; the campaign rows
  (`reorder` at `1 1 1` and, on the 48 h tier, `2 1 1`) decide, and the expectation file carries no
  verdict for `reorder`. raft-rs requires an ordered transport, so the variant is not run under
  `bag`, where out-of-order commit broadcasts would separate the batchings. Component, if observed:
  **signature word order**.

## 3. Counts (plain Must, `count`)

Historical, from the spike of 2026-10-31 (`source/spike-runs.log`, crate without the `<variant>`
argument, one thread, 600 s cap, `--model fifo`; every execution ends blocked because the nodes
loop forever, so "complete" is 0 and the count is the blocked graphs):

| command (spike crate) | graphs (complete / blocked) |
|---|---|
| `count spec --model fifo -- 1 0` | 0 / 24 |
| `count spec --model fifo -- 2 0` | 0 / 480 |
| `count spec --model fifo -- 3 0` | 0 / 8960 |
| `count impl --model fifo -- 1 1 1` | 0 / 1,323,550 |
| `count impl --model fifo -- 1 1 2` | not finished in 600 s |
| `count impl --model fifo -- 1 1 3` | not finished in 600 s |
| `count impl --model fifo -- 2 1 1` | not finished in 600 s |
| `conf --model fifo -- 1 1 1` (stateful, in-process) | conforms, exhausted |
| `export spec` + `export impl` at `1 1 1`, `conf-join` | conforms, 0 uncovered |
| `conf --model fifo -- 1 1 2` | not finished in 600 s |

(Wall times of these runs are in `source/spike-runs.log`: the in-process check 7m48s, the
implementation export 5m35s, the implementation count 4m10s, one thread.)

Measurements of this crate (with the `<variant>` argument and the full-`u64` value encoding), from
`out/runs.log` (workstation, one thread, 2026-11-01/02):

| command (this crate) | graphs (complete / blocked) |
|---|---|
| `count spec --model fifo -- correct 1 1 1` | 0 / 24 |
| `count spec --model fifo -- correct 2 1 1` | 0 / 480 |
| `count impl --model fifo -- correct 1 1 1` | 0 / 1,323,550 |
| `count impl --model fifo -- reorder 1 1 1` | 0 / 1,323,550 |

Observed: the spec counts and the `correct` implementation count equal the spike's, and `reorder`
and `correct` have the same count at `1 1 1` (an observation at this size, not derived). Wall
times are in `out/runs.log`. Conformance of this crate (`conf`, the exports and the join,
`reorder`'s verdict) is measured by the campaign rows (§4), not run yet.

## 4. Sizes

`<variant> <e> <t> [<cand>]`. The spike: `correct 1 1 1` finished (certified); `correct 1 1 2` and
larger did not finish in 10 min at one thread (every execution ends blocked: the nodes loop
forever, so every graph counts as blocked, as in `a4-balancer`). Campaign rows: `1 1 1` in the
three TraceForge modes at 16 threads; `1 1 2`, `1 2 1` and `2 1 1` on the 48 h tier.

## 5. What this benchmark is for

A refinement check of real software: the implementation side is an unmodified Raft library used
in production (TiKV), wired to the Must API by a 300-line harness. The paper reports it as a case
study with the sizes reached, not as a scaling row.

## 6. Sources and runs

`source/PROVENANCE.md` (raft-rs commit, the TLA+ source, the spike log); `out/runs.log` (every run of
this crate on the workstation, commands and full output).
