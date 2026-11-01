# raft-rs: tikv/raft-rs, unmodified, under the Must API

**Program:** `raft`$(e,t)$: three Raft nodes (tikv/raft-rs core, `RawNode<MemStorage>`, commit
ad13f3d, unmodified), two visible clients `c0 c1`, client `c_i` attached to node `n_i`. Each client
proposes `e` values to its node, then receives `Committed(v)` from it until all of its own values
have been seen. `t` bounds the election timeouts each candidate node may fire (an invisible
`nondet()` choice while no leader is known); `cand` (default 3) is the number of nodes allowed to
time out. Model: `fifo` (per-pair order; raft-rs assumes an ordered transport per peer).

**Expected verdict:** `correct`: **CONFORMS** (certified by the spike of 2026-10-31 at
`e=1 t=1 cand=1`, in-process stateful, sequential; `source/spike-runs.log`). `reorder`: **DOES NOT REFINE at
`e = 257, t = 1, cand = 1`** by a hand-derived schedule (§2, from the pinned raft-rs source:
a follower whose inflight window is full misses a commit broadcast and later commits two entries
in one append); no verdict is derived for smaller sizes, where the campaign rows decide.

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
  committed entries to the client in reverse log order. A violation needs the two clients to observe
  incompatible orders of the same values; compatibility is a joint property of both words, so a
  single client's stopping rule says nothing by itself. A batch forms where a node's commit index
  jumps over two data entries in one `Ready`. In this harness (raft-rs defaults inherited by the
  `Config`: `max_size_per_msg = 0`, at most one entry per `AppendEntries`; `batch_append = false`;
  `max_inflight_msgs = 256`) the leader commits data entries one at a time, and a follower's
  commit-index jumps repeat the leader's commit steps as long as every commit broadcast reaches
  it: `bcast_append` skips a peer whose progress is paused (probe state after one send, replicate
  state with a full inflight window), so a paused follower misses intermediate commit
  notifications and later receives one append whose commit index covers two entries it holds.
  Realizable schedule under per-pair FIFO (derived from the pinned raft-rs source, not measured):
  `reorder 257 1 1`. `n0` is elected with `n2`'s vote; the initial empty entry is replicated and
  acknowledged by both followers (both in replicate state, empty inflight windows). `c0`'s 257
  values `1, 3, ..., 513` are proposed one at a time and each is committed through `n2`'s
  acknowledgement before the next; `n1`'s handling of `c1`'s proposals is delayed, and all of
  `n1`'s acknowledgements after the first are withheld. The appends to `n1` arrive in FIFO order,
  each with the preceding commit index, so for the first 255 values both clients receive the
  values one by one. The append carrying the 256th data entry (`511`) fills `n1`'s inflight
  window; when `n2`'s acknowledgement commits `511`, the broadcast to `n1` is skipped (paused);
  `513` is then appended and committed through `n2` while sends to `n1` stay paused. Delivering
  one withheld acknowledgement from `n1` frees a slot: the leader sends the missing entry `513`
  with the current commit index, `n1` holds `511` already, so this one append commits `511` and
  `513` together, and `apply` reverses the pair. `c0` observed `..., 511, 513`; `c1` observed
  `..., 513, 511` while waiting for its own values. No single specification log order produces
  both words. 257 is a sufficient size, not a smallest one; whether a smaller size separates the
  two clients' batchings is not established, so the expectation file carries no verdict for
  `reorder` at the campaign sizes and the runs decide there. raft-rs requires an ordered
  transport, so the variant is not run under `bag`. Component, when observed: **signature word
  order**.

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

(Wall times of these runs are in `source/spike-runs.log`, one thread.)

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
