# Snapshot-isolation KV store against a serializable server

**Family:** related-work  **Source:** TLA+ examples, `KeyValueStore/` (Demirbas's PlusCal
`KVsnap.tla`, Helwer's `KeyValueStore.tla`), `related-work-papers/sources/tlaplus-examples/specifications/KeyValueStore/`
(CANDIDATES.md N9, record in CATALOGUE-tla.md, pair (i))  **Expected verdict:** `si`:
**DOES NOT REFINE** (write skew: both transactions commit having read the initial values, which
no serial order gives); `serializable`: **CONFORMS** (the commit also validates the read key, so
the committed transactions are serializable in commit order).

## 1. The protocol

`KVsnap.tla`: one process per transaction id runs `START` (join `tx`, take the snapshot
`snapshotStore := store`), `READ` (read its read keys from the snapshot), `UPDATE` (write its
write keys in the snapshot), `COMMIT` (if `missed[self] ∩ write_keys = {}`: leave `tx`, add the
write keys to `missed[o]` of every open `o`, install the writes in `store`; otherwise abort,
first committer wins). The source's own anomaly: "Serializability would not be satisfied due to
write-skew" (the `Serialization` invariant at the end of `KVsnap.tla`, and the README).

Roles:

- Visible: `c0`, …, `c{n-1}` (clients; each submits one transaction and receives its result).
- Invisible: `server` (specification); `store` (the PlusCal globals `store`, `tx`, `missed`) and
  `t0`, …, `t{n-1}` (the PlusCal processes, one per client) (implementation). The transaction
  processes are invisible by rule 3: `Begin`, the snapshot and `Commit` have no counterpart in the
  specification.

Messages: `Txn { from, rk, wk, v }` (client → server / `t_i`: one transaction with one read of
`rk` and one write `wk := v`), `Result { ok, read }` (→ client: committed or aborted, the value
read, 0 when aborted); implementation only: `Begin { from }` / `Snap(store)` (START),
`Commit { from, rk, wk, v }` / `CRes(ok)` (COMMIT). Values: 0 = `NoVal`; client `i` writes
`i+1` (KVsnap writes the transaction's own id).

Script and parameters: `n` clients and `n` keys (the size argument); client `i` reads key
`(i+1) mod n` and writes key `i`, so the read set of each transaction is another's write set and
the write sets are disjoint (the classic write skew at n = 2, a cycle of length n beyond).
Smallest interesting size: n = 2 (2 keys, 2 clients, 1 transaction of 2 operations each). Model
`fifo` (p2p).

## 2. The specification program

One invisible `server` (rule 2: the implementation's `store` orders every commit, and a
serializable store is one serial order). It performs n blocking receives of `Txn`; for each it
either rolls the transaction back (`nondet()`: `Result { ok: false, read: 0 }`;
`KeyValueStore.tla`'s `RollbackTx` is always enabled, and a serializable store may abort any
transaction) or executes it atomically on its `store` (initially all 0): `read = store[rk]`,
`store[wk] = v`, `Result { ok: true, read }`. Visible behaviour: the committed transactions are
executed one after the other in the server's receive order, so when all n transactions of the
cycle commit, at least one of them reads a value written by another (at n = 2: whenever both
commit). With aborts in between this need not hold: at n = 3, commit c0, then c1, abort c2, and
both committed reads are 0.
Graphs: n!·2^n (8, 48, 384 for n = 2, 3, 4).

## 3. The implementation program

- `t_i` (KVsnap's `process (t \in TxId)`): receives `Txn` from `c_i`; `START`: sends `Begin`,
  receives `Snap(store)`; `READ`: `r = snap[rk]`; `UPDATE`: `snap[wk] = v` (local);
  `COMMIT`: sends `Commit { rk, wk, v }`, receives `CRes(ok)`; answers `Result { ok, read: r }`
  (`read: 0` when aborted).
- `store`: 2n blocking receives. `Begin`: add the sender to `tx` with an empty `missed` set and
  send a copy of `store`. `Commit`: conflict iff `wk ∈ missed[t]` (`si`, KVsnap's test), or
  `wk ∈ missed[t] ∨ rk ∈ missed[t]` (`serializable`); remove `t` from `tx`; without a conflict add
  `wk` to `missed[o]` for every open `o` and install `store[wk] = v`; answer `CRes(!conflict)`.

Graphs: the store's receive orders with each `Begin` before its `Commit`, (2n)!/2^n (6, 90, 2520).
Deviations from KVsnap: the read and write key sets are fixed by the script instead of chosen
from `SUBSET Key \ {{}}` at `START`; the transaction is submitted by a client in one message and
answered in one (KVsnap has no clients; the transactions are its processes); an aborted
transaction leaves `tx` (KVsnap leaves it in `tx` with `pc = "Done"`; `KeyValueStore.tla`'s
`RollbackTx` removes it).

## 4. Where they differ, and why the verdict is what it is

Notation: `S(m)` a send, `R(m)` a receive, per visible thread in program order.

**`si`, DOES NOT REFINE**, smallest size n = 2. Schedule: `store` receives `Begin(t0)`,
`Begin(t1)` (both snapshots are all 0), `Commit(t0)` (no conflict: `missed[t0] = {}`; `missed[t1]
= {0}`), `Commit(t1)` (`wk = 1 ∉ missed[t1] = {0}`, no conflict). Uncovered word:

```
c0: S(Txn(c0, rk 1, wk 0, v 1)) R(Result(ok true, read 0))
c1: S(Txn(c1, rk 0, wk 1, v 2)) R(Result(ok true, read 0))
```

Each word alone is a specification word (the client served first reads 0), but not both in one
graph: when both commit, the server executes one of them second, and that one reads the other's
write (`c1` reads 1, or `c0` reads 2). Component: **signature value** (the joint observation of
`c0` and `c1`). At n = 3 the same schedule (all `Begin`s, then all `Commit`s) gives every client
`Result(true, 0)`. The write sets are disjoint, so first-committer-wins never aborts here.

**`serializable`, CONFORMS.** A transaction commits only if no transaction that committed after
its `Begin` wrote its read key or its write key, so its snapshot value of `rk` equals `store[rk]`
at its `Commit`. Put the specification's service of transaction i at the store's receipt of
`Commit(t_i)`: those points are totally ordered by the `store` thread, each lies after `c_i`'s
`S(Txn)` and before its `R(Result)`, a committed transaction reads in the specification what it
read in the implementation, and an aborted one is a specification rollback. So every
implementation graph's visible words are words of the specification graph that serves the
transactions in commit order with those rollback choices. In the write-skew schedule `t1`'s
commit aborts (`rk = 0 ∈ missed[t1]`) and `c1` receives `Result(false, 0)`.

Reachability was checked with `out/fault-check/` (the same program plus an observer thread `obs`
to which each client sends its result, asserting that some order of the committed transactions
explains every committed read): the assertion fails for `si` at n = 2 (`not serializable:
[(true, 0), (true, 0)]`) and n = 3 (`[(true, 0), (true, 0), (true, 0)]`), and holds for `spec` and
`serializable` at n = 2 and n = 3.

## 5. Variants and faults

| variant | how it differs | expected verdict | reason |
|---|---|---|---|
| `si` | KVsnap: the commit tests the write key only (first committer wins) | DOES NOT REFINE (n ≥ 2) | write skew: both commit with `read 0` |
| `serializable` | the commit also tests the read key (SSI-style read validation; the catalogue's conforming control) | CONFORMS | §4 |

Two pairs against the one `spec`. The source's fault is `si` itself (the commented-out
`Serialization` invariant); `serializable` is ours.

## 6. Files

- `source/`: `KVsnap.tla`, `KeyValueStore.tla`, `ClientCentric.tla`, `MCKVsnap.{tla,cfg}`,
  `Util.tla`, `README.md`, `LICENSE`, copied verbatim, with provenance in `source/PROVENANCE.md`.
  `KeyValueStore.tla` is itself a snapshot-isolation specification; the serializable
  specification program here is ours (catalogue pair (i)).
- `rust/`: crate `bench_kv_snapshot_isolation_tla`. CLI: `<spec|impl> <n> [fifo|bag|causal]
  [si|serializable]`, default model `fifo`, default variant `si`. See `_tools/run-rust.sh`.
- `java/`: package `bench.kv_snapshot_isolation_tla`, class `KvSnap` (args
  `<spec|impl>,<n>,<fifo|bag>,<variant>`), `Support` (the model argument, copied from
  `H1Support`). Spawn order: spec `server, c0, …`; impl `store, t0, …, c0, …`. Visible roles
  `c0`, …, `c{n-1}` on both sides. Rust's tagged receives are type-test receives in Java.
- `out/runs.log`: every command and its output; `out/fault-check/`: the reachability crate.

## 7. Plain-Must exploration counts (both tools, one run each)

Model `fifo` (p2p) throughout. Size = n (clients = keys). n = 4 was run for every side because n = 3 finished in under a minute on both tools.

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | n=2 | fifo | 8 / 0 | 8 / 0 | yes |
| spec | n=3 | fifo | 48 / 0 | 48 / 0 | yes |
| spec | n=4 | fifo | 384 / 0 | 384 / 0 | yes |
| impl `si` | n=2 | fifo | 6 / 0 | 6 / 0 | yes |
| impl `si` | n=3 | fifo | 90 / 0 | 90 / 0 | yes |
| impl `si` | n=4 | fifo | 2520 / 0 | 2520 / 0 | yes |
| impl `serializable` | n=2 | fifo | 6 / 0 | 6 / 0 | yes |
| impl `serializable` | n=3 | fifo | 90 / 0 | 90 / 0 | yes |
| impl `serializable` | n=4 | fifo | 2520 / 0 | 2520 / 0 | yes |

All runs exhausted on both tools (JMC: "stack exhausted"). No disagreement. The two implementation variants differ only in the commit test (a value), so they have equal counts.

Commands, run from `B=/local/home/mkhoshechin/Documents/github-jmc/plan/benchmarks`, for `n ∈ {2, 3, 4}`, `v ∈ {si, serializable}`:

```
timeout 600 $B/_tools/run-rust.sh $B/kv-snapshot-isolation-tla spec <n> fifo
timeout 600 $B/_tools/run-rust.sh $B/kv-snapshot-isolation-tla impl <n> fifo <v>
$B/_tools/run-java.sh $B/kv-snapshot-isolation-tla 'bench.kv_snapshot_isolation_tla.KvSnap:spec,<n>,fifo' must 100000 600
$B/_tools/run-java.sh $B/kv-snapshot-isolation-tla 'bench.kv_snapshot_isolation_tla.KvSnap:impl,<n>,fifo,<v>' must 100000 600
```

The fault checks of §4 (not counts) ran as `cd $B/kv-snapshot-isolation-tla/out/fault-check && cargo run -q -j 2
--offline --release -- <side> <size> <model> [variant]` with
`CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/bench-target`. Every command and its output is in
`out/runs.log`.
