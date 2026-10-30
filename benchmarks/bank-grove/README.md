# Bank over a lock service and a KV store (Grove) against one bank server

**Family:** related-work  **Source:** Grove (Sharma et al., SOSP 2023) / gokv,
`related-work-papers/sources/gokv/bank/`, `lockservice/`, `kv/` (CANDIDATES.md N10, record in
CATALOGUE-grove-civl.md)  **Expected verdict:** `ordered`: **CONFORMS** (every clerk and the
auditor take account 0's lock first, so the operations are serialised and the total is always
`BAL_TOTAL`); `unordered`: **DOES NOT REFINE** (the source's `acquire_two`: two opposite transfers
each hold one lock and wait for the other, and `c0`, `c1` and `auditor` end blocked where the
specification's end done).

## 1. The protocol

A `BankClerk` moves money between accounts kept in a KV service and guards each account with a
lock: `transfer_internal(from, to, amount)` = `acquire_two(from, to)`; `old = Get(from)`; if
`old >= amount`: `Put(from, old - amount)`, `Put(to, Get(to) + amount)`; `release_two(from, to)`.
`get_total` locks and reads every account in sorted order, then unlocks all; `SimpleAudit`
panics unless the total is `BAL_TOTAL`. The lock service is a clerk over a KV: `Lock(key)` spins
on `ConditionalPut(key, "", "1") != "ok"`, `Unlock(key)` is `Put(key, "")`. The source's
`transfer_internal` calls `acquire_two` (`Lock(l1); Lock(l2)`, unordered, "FIXME: need to add
string lt to operational semantics"); `acquire_two_good` (by `l1 < l2`) is unused.

Roles:

- Visible: `c0`, `c1` (transfer clients), `auditor` (one audit).
- Invisible: `bank` (specification); `clerk0`, `clerk1` (the transfer clerks), `aclerk` (the
  auditor's clerk running `get_total`), `lock` (the KV behind the `LockClerk`), `kv` (the account
  KV) (implementation). The clerks are invisible by rule 3: their lock and KV RPCs have no
  counterpart in the specification, so a visible client hands each operation to its clerk.

Messages: `Transfer { from, src, dst, amt }` / `Done` (client ↔ bank / clerk), `Audit { from }`
/ `Total(sum)` (auditor ↔ bank / `aclerk`); implementation only: `CondPut { from, key }` /
`CondOk` (`Lock`), `Unlock { from, key }` / `PutOk` (`Unlock`), `Get { from, key }` / `Val(v)`,
`Put { from, key, val }` / `PutOk`.

Script and parameters: 2 accounts, `BAL_TOTAL = 1`, initially all in account 0 (the source's
`MakeBankClerkSlice` initialisation, done here by starting the KV in that state); `c0` makes K
transfers of 1 from account 0 to account 1, `c1` K transfers of 1 from 1 to 0 (the size argument
K; the source draws `src`, `dst`, `amount` at random forever); `auditor` audits once. Smallest
interesting size: K = 1 (2 accounts, 2 clerks, 1 auditor). Model `fifo` (p2p).

## 2. The specification program

One invisible `bank` holding `bal = [1, 0]`, 2K + 1 blocking receives. `Transfer`: if
`bal[src] >= amt`, move `amt`; answer `Done`. `Audit`: answer `Total(bal[0] + bal[1])`, which is
always 1. Visible behaviour: every client and the auditor finish (status done), the auditor
receives `Total(1)`, and the operations are served one at a time in the bank's receive order.
Graphs: (2K+1)!/(K!·K!) (6, 30, 140, 630 for K = 1..4).

## 3. The implementation program

- `clerk_i`: K times: receive `Transfer`; lock both accounts (`unordered`: `src` then `dst`, the
  source's `acquire_two`; `ordered`: the smaller account first, `acquire_two_good`); `Get(src)`;
  if `old >= amt`: `Put(src, old - amt)`, `Get(dst)`, `Put(dst, t + amt)`; `Unlock(src)`,
  `Unlock(dst)` (`release_two`); answer `Done`.
- `aclerk`: receive `Audit`; for account 0 then 1: `Lock`, `Get`; unlock both; answer `Total`.
- `lock`: 8K + 4 blocking receives (every transfer and the audit send two `CondPut` and two
  `Unlock`). `CondPut(key)` succeeds at once if the key is free; otherwise the requester is
  recorded as retrying. `Unlock(key)` answers `PutOk` and, if clerks are retrying on `key`, hands
  the lock to one of them chosen by `(0..w).nondet()` (any spinning clerk may win the next
  `ConditionalPut`); otherwise frees the key. This replaces the unbounded spin by a blocking
  reply (the catalogue's mapping).
- `kv`: a blocking server loop (A4 idiom) over `Get` and `Put`. Its number of requests depends on
  which transfers find a sufficient balance, so it ends blocked; that status is invisible and
  unobserved. Every implementation graph is therefore counted blocked in §7.

Deviations: the lock retry is a queued reply, the unlocking clerk cannot win the race against a
retrying one; the initialisation under `init_flag` is the KV's initial state; urpc's loss and
retry are not modelled (p2p is reliable).

## 4. Where they differ, and why the verdict is what it is

Notation: `S(m)` a send, `R(m)` a receive, per visible thread in program order.

**`unordered`, DOES NOT REFINE**, smallest size K = 1. Schedule: `lock` grants `clerk0`'s
`CondPut(0)` and `clerk1`'s `CondPut(1)`; then `clerk0`'s `CondPut(1)` and `clerk1`'s
`CondPut(0)` both wait. Neither clerk ever unlocks, and `aclerk`'s `CondPut(0)` waits too.
Uncovered visible behaviour:

```
c0:      S(Transfer(c0, 0, 1, 1))   -- blocked in R(Done)
c1:      S(Transfer(c1, 1, 0, 1))   -- blocked in R(Done)
auditor: S(Audit(auditor))          -- blocked in R(Total)
```

In every specification graph the three visible threads end done. Component: **signature
status**. A second deadlock of the source has the same shape: `aclerk` holds lock 0 and waits for
1 while `clerk1` holds 1 and waits for 0 (the auditor's sorted order against `c1`'s unordered
transfer).

**`ordered`, CONFORMS.** Every transfer and the audit take lock 0 first, and hold it until
their last KV operation, so the lock server hands lock 0 to one operation at a time and the KV
operations of different operations never interleave. Put the specification's service of each
operation at the moment `lock` grants it lock 0: those points are ordered by the `lock` thread,
each lies after the client's `S` and before its `R`, the balances evolve as in the
specification's serial execution, and `aclerk`, holding both locks while it reads both accounts,
always sums to 1. No cycle of lock waits is possible (all take 0, then 1), so no visible thread
blocks. `Done` carries no value, so the visible values are `Total(1)` on both sides.

Status check (not a count), with `out/fault-check/`: the same program in which `kv` stops after
a `Bye` from each of the three clerks, so that a graph ends blocked only if some clerk, and with
it its visible client, is blocked. At K = 1: `ordered` 18294 graphs, 0 blocked; `unordered`
7311 graphs, **140 blocked** (the deadlocks). The plain counts in §7 cannot show this because
the `kv` loop of the benchmark program ends blocked in every graph.

## 5. Variants and faults

| variant | how it differs | expected verdict | reason |
|---|---|---|---|
| `ordered` | `acquire_two_good`: lock the smaller account first | CONFORMS | §4 |
| `unordered` | `acquire_two`, the source's (what `transfer_internal` calls) | DOES NOT REFINE (K ≥ 1) | opposite transfers deadlock; visible threads blocked |

Two pairs against the one `spec`. The catalogue's further faults (an auditor reading without
locks, unlocking `from` before the second `Put`) are not built.

## 6. Files

- `source/`: `bank/bank.go`, `lockservice/lock_clerk.go`, `kv/interface.go` (156 lines Go),
  copied verbatim, with provenance in `source/PROVENANCE.md`. The Perennial proof is not on disk.
- `rust/`: crate `bench_bank_grove`. CLI: `<spec|impl> <K> [fifo|bag|causal]
  [ordered|unordered]`, default model `fifo`, default variant `ordered`. See `_tools/run-rust.sh`.
- `java/`: package `bench.bank_grove`, class `Bank` (args `<spec|impl>,<K>,<fifo|bag>,<variant>`),
  `Support` (the model argument, copied from `H1Support`). Spawn order: spec `bank, c0, c1,
  auditor`; impl `lock, kv, clerk0, clerk1, aclerk, c0, c1, auditor`. Visible roles `c0`, `c1`,
  `auditor` on both sides. Rust's tagged receives are type-test receives in Java.
- `out/runs.log`: every command and its output; `out/fault-check/`: the status-check crate.

## 7. Plain-Must exploration counts (both tools, one run each)

Model `fifo` (p2p) throughout. Size = K (transfers per client; 2 clients, 1 audit). Every implementation graph is counted blocked: the `kv` loop ends blocked (A4 idiom, invisible), see §3; the deadlocks are shown by the status check in §4, not by these counts. K = 4 was run for `spec` only (K = 3 under a minute); for the implementation K = 3 already exceeded 600 s.

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | K=1 | fifo | 6 / 0 | 6 / 0 | yes |
| spec | K=2 | fifo | 30 / 0 | 30 / 0 | yes |
| spec | K=3 | fifo | 140 / 0 | 140 / 0 | yes |
| spec | K=4 | fifo | 630 / 0 | 630 / 0 | yes |
| impl `ordered` | K=1 | fifo | 0 / 366 | 0 / 366 | yes |
| impl `ordered` | K=2 | fifo | 0 / 43383 | not exhausted at 600 s (partial 0 / 7254) | — (not both exhausted) |
| impl `ordered` | K=3 | fifo | not exhausted at 600 s (≥ 500000 executions, all blocked, at the last progress line) | not exhausted at 600 s (partial 0 / 1992) | — (not both exhausted) |
| impl `unordered` | K=1 | fifo | 0 / 258 | 0 / 258 | yes |
| impl `unordered` | K=2 | fifo | 0 / 11072 | 0 / 11072 | yes |
| impl `unordered` | K=3 | fifo | 0 / 247764 | not exhausted at 600 s (partial 0 / 5831) | — (not both exhausted) |

No disagreement where both tools finished (K = 1 for both variants, K = 2 for `unordered`). Not done: JMC did not exhaust `ordered` K = 2 (7254 of TraceForge's 43383 graphs in 600 s), `ordered` K = 3 or `unordered` K = 3; TraceForge did not exhaust `ordered` K = 3 (its last progress line before the timeout reports 500000 executions, all blocked). These are timeouts, not disagreements; the partial counts are recorded.

Commands, run from `B=/local/home/mkhoshechin/Documents/github-jmc/plan/benchmarks`, for `K ∈ {1, 2, 3}` (and `spec` 4), `v ∈ {ordered, unordered}`:

```
timeout 600 $B/_tools/run-rust.sh $B/bank-grove spec <K> fifo
timeout 600 $B/_tools/run-rust.sh $B/bank-grove impl <K> fifo <v>
$B/_tools/run-java.sh $B/bank-grove 'bench.bank_grove.Bank:spec,<K>,fifo' must 100000 600
$B/_tools/run-java.sh $B/bank-grove 'bench.bank_grove.Bank:impl,<K>,fifo,<v>' must 100000 600
```

The fault checks of §4 (not counts) ran as `cd $B/bank-grove/out/fault-check && cargo run -q -j 2
--offline --release -- <side> <size> <model> [variant]` with
`CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/bench-target`. Every command and its output is in
`out/runs.log`.
