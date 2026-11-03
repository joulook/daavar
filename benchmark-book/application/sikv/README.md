# sikv(n) — snapshot-isolation store (`kv-snapshot-isolation-tla`)

**In the paper:** not in Tables 2 and 3 (reported in the appendix tables and in EVAL.pdf here only).

**Communication model:** fifo (`p2p`: per-channel order), the default of both the Rust program (`--model` left out)
and the Java twin; the campaign ran fifo only. **Languages:** Rust (TraceForge's Must API), in `rust/`; Java twin in
`java/` (`bench.kv_snapshot_isolation_tla.KvSnap`); context-free Java port in `java-cf/`
(`bench.kv_snapshot_isolation_tla.KvSnapshotIsolation`). **Visible threads:** `c0 … c(n−1)` (the clients, one
transaction each); the specification's `server` and the implementation's `store` and transaction processes
`t0 … t(n−1)` are invisible. **Parameters:** `2 ≤ n ≤ 255` = the number of clients and of keys (default 2; client
`i` writes the byte `i + 1`); the verdicts below need `n ≥ 2` (at `n = 1` there is one transaction and nothing to
skew). ARGS `<n> [si|serializable]`, variant `si` (default) or `serializable`; the specification ignores the
variant. **Source:** the TLA+ examples repository, `specifications/KeyValueStore/` (Demirbas's PlusCal `KVsnap.tla`,
Helwer's `KeyValueStore.tla`); the serializable specification program is the benchmark's own.

Notation: as in `../../synthetic/naive/README.md`. No loop ends blocked here (`store` performs `2n` receives), so
every graph is complete on both sides. Messages are `Txn(c, rk, wk, v)`, `Result(ok, r)`, `Begin(t)`, `Snap(s)`,
`Commit(t, rk, wk, v)`, `CRes(ok)`; value `0` = `NoVal`. A receive written `M(…) := recv^fifo()` takes a message of
kind `M` (the Rust program's tagged receives), other kinds stay pending; `store` takes `Begin` and `Commit` in any
order. The clients are the same on both sides except for the thread they send to (`F_i`), which the comparison
does not see:

```
Clients(n, F) ≜ ‖_{i<n} c_i: send^fifo(F_i, Txn(c_i, (i + 1) mod n, i, i + 1));  m := recv^fifo()    (m = Result(ok, r))
```

Client `i` submits one transaction reading key `(i + 1) mod n` and writing key `i` with value `i + 1`: each
transaction's read key is another's write key and the write keys are disjoint (a cycle of length `n`).

The specification (both pairs), a serializable server with rollbacks:

```
Spec(n) ≜ Clients(n, server)
        ‖ server: store := [0, …, 0]
                  for j := 1..n:
                     Txn(c, rk, wk, v) := recv^fifo()
                     if nondet({true, false}):  send^fifo(c, Result(false, 0))                        (RollbackTx)
                     else:  r := store[rk];  store[wk] := v;  send^fifo(c, Result(true, r))
```

`server` takes the `n` transactions one at a time in some order and either rolls one back (`KeyValueStore.tla`'s
`RollbackTx`, always enabled) or executes it atomically. The committed transactions are serial in the receive order:
when all `n` commit, the last served reads the value written by its predecessor on the cycle (≠ 0); at `n = 2`,
whenever both commit the one served second reads the other's write. Graphs `n!·2^n` (8, 48, 384 at `n = 2, 3, 4`).

The implementation, `KVsnap.tla`'s store and one transaction process per client:

```
Impl_v(n) ≜ Clients(n, t) ‖ store ‖ ‖_{i<n} t_i

t_i ≜ Txn(c, rk, wk, v) := recv^fifo()
      send^fifo(store, Begin(t_i));  Snap(s) := recv^fifo()                         (START)
      r := s[rk]                                                                   (READ)
      s[wk] := v                                                                   (UPDATE, local)
      send^fifo(store, Commit(t_i, rk, wk, v));  CRes(ok) := recv^fifo()            (COMMIT)
      send^fifo(c, Result(ok, if ok then r else 0))

store ≜ store := [0, …, 0];  tx := ∅;  missed := ∅
        for j := 1..2n:
           m := recv^fifo()
           if m = Begin(t):              tx := tx ∪ {t};  missed[t] := ∅;  send^fifo(t, Snap(store))
           if m = Commit(t, rk, wk, v):  conflict := ⟨conflict test⟩
                                         tx := tx \ {t}
                                         if ¬conflict:  for o ∈ tx:  missed[o] := missed[o] ∪ {wk}
                                                        store[wk] := v
                                         send^fifo(t, CRes(¬conflict))
```

`t_i` is KVsnap's `process (t ∈ TxId)` driven by `c_i`'s transaction: `START` opens it at the store and takes the
snapshot, `READ` and `UPDATE` act on the snapshot, `COMMIT` asks the store and the answer goes to the client (`read`
is `0` when aborted). `store` holds the PlusCal globals `store`, `tx`, `missed`: a `Begin` opens the transaction
with an empty missed set; a `Commit` tests the conflict (`⟨conflict test⟩`, the variant's hole), closes the
transaction and, without a conflict, records `wk` as missed by every open transaction and installs the write (first
committer wins). Graphs: the store's receive orders with each `Begin` before its `Commit`, `(2n)!/2^n`
(6, 90, 2520 at `n = 2, 3, 4`).

## The pairs

### serializable — expected: **conforms**

```
⟨conflict test⟩ ≜ wk ∈ missed[t] ∨ rk ∈ missed[t]
```

`missed[t]` holds the write keys of every transaction that committed between `t`'s `Begin` and its `Commit`, so a
committing `t` saw no committed write to `rk` after its snapshot: `s[rk] = store[rk]` at its `Commit`, where its own
write is installed. Put the specification `server`'s service of transaction `i` at the store's receipt of
`Commit(t_i)`, with the rollback coin `true` iff that commit conflicts. These points are totally ordered by the
`store` thread, each lies after `c_i`'s send of `Txn` and before its receive of `Result`, and the specification's
`store` at each point equals the implementation's (the committed writes before it, in the same order), so a committed
transaction reads in the specification what it read from its snapshot and an aborted one is a rollback with
`Result(false, 0)`. Every order of the `n` `Txn`s is a specification receive order, so each implementation graph's
visible words are those of one specification graph. No thread blocks on either side.

### si — expected: **violation** (`n ≥ 2`)

```
⟨conflict test⟩ ≜ wk ∈ missed[t]        ◀ changed: KVsnap's test, the write key only; the read key is not validated
```

The source's own anomaly ("Serializability would not be satisfied due to write-skew", the `Serialization`
invariant at the end of `KVsnap.tla`). Schedule at `n = 2`: `store` receives `Begin(t0)`, `Begin(t1)` (both snapshots
all `0`), `Commit(t0)` (`missed[t0] = ∅`: no conflict; `missed[t1] := {0}`, `store[0] := 1`), `Commit(t1)`
(`wk = 1 ∉ missed[t1] = {0}`: no conflict). The uncovered behaviour is

```
c0: S(Txn(c0, 1, 0, 1)) R(Result(true, 0))
c1: S(Txn(c1, 0, 1, 2)) R(Result(true, 0))
```

Each word alone is a specification word (the client served first reads `0`), but no specification graph has both:
when both commit, the one served second reads the other's write (`c1` reads `1`, or `c0` reads `2`). Signature
value, the joint observation of `c0` and `c1`. At every `n ≥ 2` the schedule "all `Begin`s, then all `Commit`s"
commits every transaction (the write keys are disjoint, so `wk ∉ missed[t]`) with every read `0`, which the
specification never does when all `n` commit.

## What it is for

A TLA+ model's own anomaly as the violation: the implementation is the PlusCal `KVsnap` snapshot-isolation store with
its first-committer-wins test, the specification a serializable server with rollbacks, and `serializable` adds the
read validation that makes the store conform. The uncovered behaviour is a joint observation of two clients, each of
whose words alone is a specification word. Both sides complete in every graph, and the two implementation variants
differ only in a value, so they have equal counts (6, 90, 2520 against the specification's 8, 48, 384 at
`n = 2, 3, 4`).
