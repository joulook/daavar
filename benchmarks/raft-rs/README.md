# raft-rs: tikv/raft-rs, unmodified, under the Must API

**Program:** `raft`$(e,t)$: three Raft nodes (tikv/raft-rs core, `RawNode<MemStorage>`, commit
ad13f3d, unmodified), two visible clients `c0 c1`, client `c_i` attached to node `n_i`. Each client
proposes `e` values to its node, then receives `Committed(v)` from it until all of its own values
have been seen. `t` bounds the election timeouts each candidate node may fire (an invisible
`nondet()` choice while no leader is known); `cand` (default 3) is the number of nodes allowed to
time out. Model: `fifo` (per-pair order; raft-rs assumes an ordered transport per peer).

**Expected verdict:** `correct`: **CONFORMS** (certified by the spike of 2026-10-31 at
`e=1 t=1 cand=1`, in-process stateful, 7m48s, sequential; `source/spike-runs.log`). `reorder`:
**DOES NOT REFINE** when the two clients observe incompatible delivery orders of the same log
(hand-derived, not measured; §2 gives the witness and the schedule that produces it; a run whose
schedules never apply a reversed batch of two values behaves as `correct`).

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
  committed entries to the client in reverse log order. It is visible only through incompatible
  prefixes at two clients, because each client's word ends when it has seen its own values and
  empty leader entries are not delivered. Smallest arguments: `e=1` (values `1` from `c0`, `2`
  from `c1`), `t>=1`, `cand>=1`. Witness: `c0: R(Committed(2)) R(Committed(1))` and
  `c1: R(Committed(1)) R(Committed(2))`; the specification delivers one log order to every client,
  so no specification graph has both words. Schedule: the leader appends `1` then `2`; node `n0`
  (client `c0`) receives both entries in one `AppendEntries`, learns the commit index covering both
  in one `Ready`, and `apply` reverses that batch; node `n1` (client `c1`) receives the two entries
  in two messages, or learns the two commit indices in two `Ready`s, and applies them one at a
  time in log order. Whether the enumeration contains this schedule at a given size is what the
  run decides; the expectation is hand-derived. Component: **signature word order**.

## 3. Counts (plain Must, `count`)

Historical, from the spike of 2026-10-31 (`source/spike-runs.log`, crate without the `<variant>`
argument, one thread, 600 s cap, `--model fifo`; every execution ends blocked because the nodes
loop forever, so "complete" is 0 and the count is the blocked graphs):

| command (spike crate) | graphs (complete / blocked) | wall |
|---|---|---|
| `count spec --model fifo -- 1 0` | 0 / 24 | 0.004 s |
| `count spec --model fifo -- 2 0` | 0 / 480 | 0.03 s |
| `count spec --model fifo -- 3 0` | 0 / 8960 | 0.54 s |
| `count impl --model fifo -- 1 1 1` | 0 / 1,323,550 | 4m10s |
| `count impl --model fifo -- 1 1 2` | not finished in 600 s | — |
| `count impl --model fifo -- 1 1 3` | not finished in 600 s | — |
| `count impl --model fifo -- 2 1 1` | not finished in 600 s | — |
| `conf --model fifo -- 1 1 1` (stateful, in-process) | conforms, exhausted | 7m48s |
| `export spec` + `export impl` at `1 1 1`, `conf-join` | conforms, 0 uncovered | 5m35s (impl export) |
| `conf --model fifo -- 1 1 2` | not finished in 600 s | — |

Measurements of this crate (with the `<variant>` argument and the full-`u64` value encoding), from
`out/runs.log` (workstation, one thread, 2026-11-01/02):

| command (this crate) | graphs (complete / blocked) | wall |
|---|---|---|
| `count spec --model fifo -- correct 1 1 1` | 0 / 24 | 0.004 s |
| `count spec --model fifo -- correct 2 1 1` | 0 / 480 | 0.04 s |
| `count impl --model fifo -- correct 1 1 1` | 0 / 1,323,550 | 5m57s |
| `count impl --model fifo -- reorder 1 1 1` | 0 / 1,323,550 | 5m25s |

The spec counts equal the spike's (the value encoding does not change the graphs), the `correct`
implementation count equals the spike's, and `reorder` has the same number of graphs as `correct`
at this size (the fault changes delivered values, not the graph count). Conformance of this crate
(`conf`, the exports and the join, `reorder`'s verdict) is measured by the campaign rows (§4), not
run yet.

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
