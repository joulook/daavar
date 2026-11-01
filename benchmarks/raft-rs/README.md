# raft-rs: tikv/raft-rs, unmodified, under the Must API

**Program:** `raft`$(e,t)$: three Raft nodes (tikv/raft-rs core, `RawNode<MemStorage>`, commit
ad13f3d, unmodified), two visible clients `c0 c1`, client `c_i` attached to node `n_i`. Each client
proposes `e` values to its node, then receives `Committed(v)` from it until all of its own values
have been seen. `t` bounds the election timeouts each candidate node may fire (an invisible
`nondet()` choice while no leader is known); `cand` (default 3) is the number of nodes allowed to
time out. Model: `fifo` (per-pair order; raft-rs assumes an ordered transport per peer).

**Expected verdict:** `correct`: **CONFORMS** (certified by the spike of 2026-10-31 at
`e=1 t=1 cand=1`, in-process stateful, 7m48s, sequential; `source/spike-runs.log`). `reorder`:
**DOES NOT REFINE** when a batch of two committed entries is delivered in reverse order (two clients
observe different delivery orders of the same log); hand-derived, a run in which no batch of two
entries commits together conforms.

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
  committed entries to the client in reverse log order. Visible effect: with two entries committed in
  one batch at one node and separately at another, `c0` observes `v1` before `v0` while `c1`
  observes `v0` before `v1`; the specification has one order. Component: **signature word order**.

## 3. Sizes

`<variant> <e> <t> [<cand>]`. The spike: `correct 1 1 1` finished (certified); `correct 1 1 2` and
larger did not finish in 10 min at one thread (every execution ends blocked: the nodes loop
forever, so every graph counts as blocked, as in `a4-balancer`). Campaign rows: `1 1 1` in the
three TraceForge modes at 16 threads; `1 1 2`, `1 2 1` and `2 1 1` on the 48 h tier.

## 4. What this benchmark is for

A refinement check of real software: the implementation side is an unmodified Raft library used
in production (TiKV), wired to the Must API by a 300-line harness. The paper reports it as a case
study with the sizes reached, not as a scaling row.

## 5. Sources

`source/PROVENANCE.md` (raft-rs commit, the TLA+ source, the spike log).
