# Fencing-token front ends (Grove) against one counter server

**Family:** related-work  **Source:** Grove (Sharma et al., SOSP 2023) / gokv,
`related-work-papers/sources/gokv/fencing/` (CANDIDATES.md N16, record in CATALOGUE-grove-civl.md)
**Expected verdict:** `fenced`: **CONFORMS** (once the second front end has used epoch 2 at the
counter server, every operation of the first front end is rejected with `EStale` and the first
front end exits before answering, so the client retries at the second); `stale-frontend`:
**DOES NOT REFINE** (a front end that ignores `EStale` answers with a value the other front end
also returned: `c0` and `c1` both receive 0).

## 1. The protocol

`fencing/`: a config server hands out epochs (`AcquireEpoch(newFrontend)` waits while
`currHolderActive`, then increments `currentEpoch` and records the front end; the
`HeartbeatListener` clears `currHolderActive` once the holder's heartbeat expires; `Get()`
returns the current front end). A front end acquires an epoch at start and serves
`FetchAndIncrement(key)` under its mutex by `ret = ctr.Get(epoch)`, `ctr.Put(ret + 1, epoch)`.
A counter server rejects `Get` and `Put` with `epoch < lastEpoch` (`EStale`) and otherwise sets
`lastEpoch = epoch`; the front end's counter clerk, on `EStale`, logs "stale epoch number" and
calls `primitive.Exit(1)`. A client clerk calls its front end and, on an error, fetches the
current front end from the config server and retries (`client/client.go`).

Roles:

- Visible: `c0`, `c1` (the clients, each calling `FetchAndIncrement(0)`).
- Invisible: `server` (specification); `config`, `ctr` (the counter server of key 0), `fe1`,
  `fe2` (the front ends), `p0`, `p1` (the client clerks) (implementation). The clerks are
  invisible by rule 3: the config lookups, the errors and the retries have no counterpart in the
  specification.

Messages: `Fai { from, key }` / `Val(v)` (client ↔ server / clerk); implementation only:
`CfgGet { from }` / `Current(fe)` (config `Get`), `Acquire { from }` / `Epoch(e)`
(`AcquireEpoch`), `FeFai { from, key }` / `FeVal(v)` / `FeErr` (the front end's `RPC_FAI`; `FeErr`
is the failed call to an exited front end), `CtrGet { from, epoch }` / `GetRep { stale, val }`,
`CtrPut { from, v, epoch }` / `PutRep { stale }`.

Script and parameters: 1 key (key 0, counter server 1; the front end's key-1 branch and the
second counter server are unused), 2 front ends, 2 clients with K calls each (the size argument;
the source's `loopclient` loops forever). Smallest interesting size: K = 1 (2 operations). Model
`fifo` (p2p).

## 2. The specification program

One invisible `server` with the counter (rule 2: one unit, the key; one thread). 2K blocking
receives of `Fai`; each is answered `Val(v)` and `v` incremented. Visible behaviour: the 2K
values the clients receive are 0, …, 2K−1, each once, in the server's receive order. Graphs:
(2K)!/(K!·K!) (2, 6, 20, 70 for K = 1..4).

## 3. The implementation program

- `config`: a blocking server loop (A4 idiom). `Acquire`: granted at once if no holder is
  active, otherwise queued (`AcquireEpoch` waits). `CfgGet`: answered with the current front end;
  before the first grant it waits (the source would return address 0). After each receive, if a
  holder is active and a front end is waiting, `nondet()` decides whether the holder's heartbeat
  has expired now (the real-time `HeartbeatListener` and `Heartbeat` RPCs become this choice); if
  so the next waiting front end is granted. A grant: `active`, `data = fe`, `epoch += 1`, `Epoch(epoch)`
  to `fe`, and the waiting `CfgGet`s are answered.
- `fe1`, `fe2`: send `Acquire`, receive `Epoch(e)`; then a blocking loop over `FeFai`: `CtrGet(e)`,
  then `CtrPut(val + 1, e)`, then `FeVal(val)`. `fenced`: on `stale` from either, the front end
  exits (it answers this and every later call with `FeErr`). `stale-frontend`: `stale` is ignored
  (on a stale `Get` the value is the reply's zero `val`, as Go's `new(GetReply)` leaves it).
- `ctr`: a blocking loop; `CtrGet`/`CtrPut` with `epoch < last` answer `stale`; otherwise
  `last = epoch` and the counter is read or written.
- `p_i`: `CfgGet` once (`MakeClerk`); then K times: receive `Fai` from `c_i`, call `FeFai` on the
  current front end; on `FeVal(v)` answer `Val(v)`; on `FeErr` `CfgGet` again and retry (at most
  one retry is ever needed: `fe2` holds the last epoch and never sees `stale`).

Every implementation graph ends with `config`, `ctr` and the front ends blocked in their loops
(and `fe2` blocked in its `Acquire` when the holder never expired); those statuses are invisible
and unobserved, so every implementation graph is counted blocked in §7. Deviations: heartbeats
and expiry by a `nondet()` step; urpc loss, timeouts and erpc's duplicate suppression are not
modelled (p2p is reliable); the front end's exit is a front end that answers `FeErr`.

## 4. Where they differ, and why the verdict is what it is

Notation: `S(m)` a send, `R(m)` a receive, per visible thread in program order.

**`stale-frontend`, DOES NOT REFINE**, smallest size K = 1. Schedule: `config` grants `fe1`
epoch 1 and answers `p0`'s `CfgGet` with `fe1`; `fe2`'s `Acquire` waits; the heartbeat expires
(`nondet()`), `fe2` gets epoch 2, and `p1`'s `CfgGet` is answered with `fe2`. `fe1` reads
`CtrGet(1)` → 0; `fe2` reads `CtrGet(2)` → 0 (`last = 2`) and writes `CtrPut(1, 2)`, `p1` forwards
0 to `c1`; `fe1`'s `CtrPut(1, 1)` is rejected (`stale`), `fe1` ignores it and `p0` forwards 0 to
`c0`. Uncovered word:

```
c0: S(Fai(c0, 0)) R(Val(0))
c1: S(Fai(c1, 0)) R(Val(0))
```

In every specification graph the two clients receive 0 and 1. Component: **signature value**
(the joint observation of `c0` and `c1`).

**`fenced`, CONFORMS.** The `ctr` thread orders every `CtrGet`/`CtrPut`. A front end answers a
value only if its `Get` and its `Put` both passed the epoch check. If `fe2` (epoch 2) touched
`ctr` between `fe1`'s `Get` and `Put`, `last = 2` and `fe1`'s `Put` fails, so `fe1` exits without
answering and its clerk retries at `fe2` (the config's current front end is `fe2` from the grant
on); after that every `fe1` call fails. So the successful calls' Get–Put pairs do not interleave
at `ctr`, each returns the counter value its `Get` read, and placing the specification's service
of each call at its successful `Get` (ordered by `ctr`, after the client's `S`, before its `R`)
gives a specification graph with the same values: 0, 1, … in that order.

Reachability was checked with `out/fault-check/` (the same program plus an observer thread `obs`
to which each client sends the value it received, asserting that no value is received twice): it
fails for `stale-frontend` at K = 1 (`c1 got 0, already given: [(0, 0)]`) and holds for `spec`
(K = 1, 2) and `fenced` (K = 1, 2).

## 5. Variants and faults

| variant | how it differs | expected verdict | reason |
|---|---|---|---|
| `fenced` | the source: a front end that sees `EStale` exits | CONFORMS | §4 |
| `stale-frontend` | a front end ignores `EStale` and keeps serving with its old epoch | DOES NOT REFINE (K ≥ 1) | `c0` and `c1` both receive 0 |

Two pairs against the one `spec`. The source ships no fault; the catalogue's fault (counter
servers that ignore the epoch) gives the same uncovered word through a different schedule
(`fe1`'s stale `Put(1, 1)` lands); `stale-frontend` is the task's wording.

## 6. Files

- `source/fencing/`: the whole `gokv/fencing/` directory (Go, `notes.txt`, `test.py`), copied
  verbatim, with provenance in `source/PROVENANCE.md`. The Perennial proof is not on disk.
- `rust/`: crate `bench_fencing_grove`. CLI: `<spec|impl> <K> [fifo|bag|causal]
  [fenced|stale-frontend]`, default model `fifo`, default variant `fenced`. See `_tools/run-rust.sh`.
- `java/`: package `bench.fencing_grove`, class `Fencing` (args
  `<spec|impl>,<K>,<fifo|bag>,<variant>`), `Support` (the model argument, copied from
  `H1Support`). Spawn order: spec `server, c0, c1`; impl `config, ctr, fe1, fe2, p0, p1, c0, c1`.
  Visible roles `c0`, `c1` on both sides. Rust's tagged receives are type-test receives in Java.
- `out/runs.log`: every command and its output; `out/fault-check/`: the reachability crate.

## 7. Plain-Must exploration counts (both tools, one run each)

Model `fifo` (p2p) throughout. Size = K (calls per client; 2 clients). Every implementation graph is counted blocked (`config`, `ctr` and the front ends end in their blocking loops, invisible), see §3. K = 4 was run for `spec` (K = 3 under a minute) but not for the implementation (K = 3 took over a minute on JMC).

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | K=1 | fifo | 2 / 0 | 2 / 0 | yes |
| spec | K=2 | fifo | 6 / 0 | 6 / 0 | yes |
| spec | K=3 | fifo | 20 / 0 | 20 / 0 | yes |
| spec | K=4 | fifo | 70 / 0 | 70 / 0 | yes |
| impl `fenced` | K=1 | fifo | 0 / 164 | 0 / 164 | yes |
| impl `fenced` | K=2 | fifo | 0 / 660 | 0 / 660 | yes |
| impl `fenced` | K=3 | fifo | 0 / 2540 | 0 / 2540 | yes |
| impl `stale-frontend` | K=1 | fifo | 0 / 176 | 0 / 176 | yes |
| impl `stale-frontend` | K=2 | fifo | 0 / 1152 | 0 / 1152 | yes |
| impl `stale-frontend` | K=3 | fifo | 0 / 12128 | 0 / 12128 | yes |

All runs exhausted on both tools. No disagreement. `stale-frontend` has more graphs than `fenced`: a front end that does not exit keeps serving and receiving after a stale reply.

Commands, run from `B=/local/home/mkhoshechin/Documents/github-jmc/plan/benchmarks`, for `K ∈ {1, 2, 3}` (and `spec` 4), `v ∈ {fenced, stale-frontend}`:

```
timeout 600 $B/_tools/run-rust.sh $B/fencing-grove spec <K> fifo
timeout 600 $B/_tools/run-rust.sh $B/fencing-grove impl <K> fifo <v>
$B/_tools/run-java.sh $B/fencing-grove 'bench.fencing_grove.Fencing:spec,<K>,fifo' must 100000 600
$B/_tools/run-java.sh $B/fencing-grove 'bench.fencing_grove.Fencing:impl,<K>,fifo,<v>' must 100000 600
```

The fault checks of §4 (not counts) ran as `cd $B/fencing-grove/out/fault-check && cargo run -q -j 2
--offline --release -- <side> <size> <model> [variant]` with
`CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/bench-target`. Every command and its output is in
`out/runs.log`.
