# At-most-once RPC (Aneris dscm, one server) against a sequentially consistent memory

**Family:** related-work  **Source:** Aneris, distributed sequentially consistent memory,
one-server implementation: `related-work-papers/sources/aneris/ml_sources/examples/dscm/implementations/one_server/`
and `aneris/aneris/examples/dscm/{spec,clients}/` (CANDIDATES.md N5, record in
CATALOGUE-coq-iris.md)  **Expected verdict:** `dedup`: **CONFORMS** (a retransmitted request is
answered from `ctable` or ignored, so every operation is applied once, in the server's order);
`no-dedup`: **DOES NOT REFINE** at K ≥ 2 (a retransmitted `Wr` is applied twice, and
`c0` reads 2 while `c1` reads 1, which no single-server order gives).

## 1. The protocol

The source's server (`one_server_server_code.ml`) holds a map `db` and a client table
`ctable: caddr → (rid, result)`. A client proxy (`one_server_client_proxy_code.ml`) sends
`((op, rid), caddr)` with `op = InjL (k, v)` (write) or `InjR k` (read), retransmits on a
receive timeout, and accepts only a reply with `resId = rid`. The server ignores `rid < t`,
re-sends the cached reply for `rid = t`, and applies a new request (`rid = t + 1`).

Roles:

- Visible: `c0`, `c1` (the client programs; each sends `Op` and receives `Res`).
- Invisible: `server` (both sides); `p0`, `p1` (the client proxies, implementation only; rule 3:
  retransmission and stale replies stay between proxy and server).

Messages: `Op { from, op }` (client → server/proxy), `Res(res)` (→ client), `Req { op, rid,
from }` (proxy → server), `Rep { res, rid }` (server → proxy); `op ∈ {Wr{key,val}, Rd{key}}`,
`res ∈ {Ack, Val(v)}`, one key `x` (key 0), `Val(0)` encodes `None` (written values are 1, 2).
Script: client i's operation j is `Wr(x, i+1)` for even j and `Rd(x)` for odd j, so at K = 2
`c0: Wr(x,1); Rd(x)` and `c1: Wr(x,2); Rd(x)` (the dscm `example2` shape, `wr k n; rd k`).
Parameters: N = 2 clients (fixed), K operations per client (the size argument), D = 1
duplicate in the whole run (at `p0`). Smallest interesting size: K = 2.

## 2. The specification program

One invisible `server` (rule 2: one memory, one unit). It performs N·K blocking receives of
`Op`, applies each atomically to `db` (`commit_op`), and sends `Res` to `from`. This is
sequential consistency for one location: every visible history is an interleaving of the two
clients' program orders in which each read returns the last write before it. Graphs: the
server's receive orders, (2K)!/(K!·K!) (2, 6, 20, 70).

## 3. The implementation program

- `p_i` (proxy): for rid = 0..K−1, receives `Op` from `c_i`, sends `Req(op, rid, p_i)` to
  `server`; `p0` may, once in the run, `if nondet()` send the same `Req` a second time (the
  timeout retransmission; Must does not duplicate, so the duplicate is a second send). It then
  receives `Rep`s until one has `rid` (at most `sent_dups` stale ones precede it), and forwards
  `Res` to `c_i`.
- `server`: a blocking receive loop (A4 idiom, rule 1) that ends blocked when the proxies are
  done; with `dedup`, the `ctable` logic of `process_request` (ignore stale, re-send cached,
  apply new); with `no-dedup`, every `Req` is applied and answered.

Every implementation graph ends with `server` blocked in its receive, so all are counted
blocked; that status is invisible and unobserved.

## 4. Where they differ, and why the verdict is what it is

**`dedup`.** Each `(p_i, rid)` is applied once: the first copy is applied and recorded; a
duplicate read while `t = rid` gets the cached reply, and one read after `p0`'s next request
(`rid < t`) is ignored. A proxy forwards only the reply with its own `rid`, whose result is the
one computed when the operation was applied. So the visible history is the server's order of
first copies, which is one of the specification's receive orders, with the same results; the
duplicate only adds order between invisible events. **CONFORMS.**

**`no-dedup`**, smallest size K = 2. Schedule: `p0` sends `Req(Wr(x,1),0)` twice; `server`
reads the first (x = 1, `Ack` to `c0`), then `p1`'s `Req(Wr(x,2),0)` (x = 2), then `p0`'s
`Req(Rd(x),1)` (`Val(2)`), then the duplicate `Req(Wr(x,1),0)` (x = 1 again), then `p1`'s
`Req(Rd(x),1)` (`Val(1)`). Uncovered word:

```
c0: S(Op(c0,Wr(0,1))) R(Res(Ack)) S(Op(c0,Rd(0))) R(Res(Val(2)))
c1: S(Op(c1,Wr(0,2))) R(Res(Ack)) S(Op(c1,Rd(0))) R(Res(Val(1)))
```

In the specification, `c0` reading 2 puts `Wr(x,2)` after `Wr(x,1)`, and `c1` reading 1 puts
`Wr(x,1)` after `Wr(x,2)`; with each write applied once, no order has both. Component:
**signature value**. At K = 1 (writes only, all results `Ack`) the double application is not
observable, so `no-dedup` at K = 1 is a conforming control.

Reachability was checked with `out/fault-check/` (the same program with an assertion in the
clients that `c0` read 2 and `c1` read 1 never both hold): at K = 2 it holds for `spec` and
`dedup` and fails for `no-dedup`; at K = 1 it holds for `no-dedup`. The assertion is a valid
witness only at K = 2: at K = 3 (`c_i`'s third operation is a second write) it also fails on
`spec`, because `Wr(x,1); Wr(x,2); Rd→2 (c0); Wr(x,1) (c0's third); Rd→1 (c1)` is sequentially
consistent.

These are executed results (2026-10-30), one run each, from
`$B/rpc-at-most-once-aneris/out/fault-check` with `export
CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/bench-target; export TMPDIR=/local/home/mkhoshechin/tmp`:

| claim | command | result |
|---|---|---|
| `no-dedup` fails, K=2 | `cargo run -q -j 2 --offline --release -- impl 2 bag no-dedup` | panic `non-SC: c0 read 2 and c1 read 1` |
| `no-dedup` holds, K=1 (control) | `cargo run -q -j 2 --offline --release -- impl 1 bag no-dedup` | exit 0, `execs=0 blocked=14` |
| `spec` holds, K=2 | `cargo run -q -j 2 --offline --release -- spec 2 bag` | exit 0, `execs=6 blocked=0` |
| `dedup` holds, K=2 | `cargo run -q -j 2 --offline --release -- impl 2 bag dedup` | exit 0, `execs=0 blocked=146` |

Every claim reproduced. The holding runs explore the same numbers of executions as the
plain-Must counts in §7 (implementation graphs are all blocked, §3). Every command and its full
output is in `out/fault-check.log`.

## 5. Variants and faults

| variant | how it differs | expected verdict | reason |
|---|---|---|---|
| `dedup` | the source's `ctable` (stale ignored, cached reply re-sent) | CONFORMS | §4 |
| `no-dedup` | `ctable` removed: every `Req` applied (catalogue fault a) | DOES NOT REFINE (K ≥ 2); conforms at K = 1 | duplicate `Wr(x,1)` re-applied: `c0` reads 2, `c1` reads 1 |

The source ships no faults. Catalogue fault (b) (the proxy accepts any `resId`) is not built.
Rust receives are tag-filtered (`TAG_OP`, `TAG_RES`, `TAG_REQ`, `TAG_REP`), one tag per message
variant, i.e. type tests; the Java side uses `Channel.recv(M, OpMsg.class)` etc.

## 6. Files

- `source/`: `ml_sources-one_server/` (the OCaml server, client proxy, network, serialization,
  dune), `coq-spec/` (the SC memory specification `.v` files), `coq-clients/` (`example1_code.v`,
  `example2_code.v`), copied verbatim, with provenance in `source/PROVENANCE.md`.
- `rust/`: crate `bench_rpc_at_most_once_aneris`. CLI, the shared main: `<bin>
  count|export|conf|online … --model M -- <K> [dedup|no-dedup]`; default K 2, default variant
  `dedup`. `--model` left out means `bag`, this benchmark's model, which the crate declares. `conf`
  is not this crate's conformance check: visible values carry a `ThreadId`, the two sides spawn in
  different orders, and `conf` compares typed values, so it reports every implementation graph; use
  `online` or `conf-join`, which compare role names. The `out/fault-check` crate is a separate copy,
  not on the shared main: it keeps the old positional CLI `<spec|impl> <size> [fifo|bag|causal]
  [variant]`, so its commands stand as run. See `_tools/run-rust.sh`.
- `java/`: package `bench.rpc_at_most_once_aneris`, class `AtMostOnceRpc` (args
  `<spec|impl>,<K>,<fifo|bag>,<variant>`), `Support` (the model argument, copied from
  `H1Support`). Spawn order: spec `server, c0, c1`; impl `server, p0, p1, c0, c1`. Visible roles
  `c0`, `c1` on both sides. See `_tools/run-java.sh`.
- `out/runs.log`: every plain-Must command and its output; `out/fault-check/`: the reachability
  crate; `out/fault-check.log`: every reachability command and its output.

## 7. Plain-Must exploration counts (both tools, one run each)

Model `bag` (asyn; the duplicate must be able to overtake later requests). Size = K
(operations per client), 2 clients, 1 duplicate.

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | K=1 | bag | 2 / 0 | 2 / 0 | yes |
| spec | K=2 | bag | 6 / 0 | 6 / 0 | yes |
| spec | K=3 | bag | 20 / 0 | 20 / 0 | yes |
| spec | K=4 | bag | 70 / 0 | 70 / 0 | yes |
| impl `dedup` | K=1 | bag | 0 / 14 | 0 / 14 | yes |
| impl `dedup` | K=2 | bag | 0 / 146 | 0 / 146 | yes |
| impl `dedup` | K=3 | bag | 0 / 1070 | 0 / 1070 | yes |
| impl `dedup` | K=4 | bag | 0 / 6622 | 0 / 6622 | yes |
| impl `no-dedup` | K=1 | bag | 0 / 14 | 0 / 14 | yes |
| impl `no-dedup` | K=2 | bag | 0 / 166 | 0 / 166 | yes |
| impl `no-dedup` | K=3 | bag | 0 / 1350 | 0 / 1350 | yes |
| impl `no-dedup` | K=4 | bag | 0 / 9142 | 0 / 9142 | yes |

All JMC runs ended with "stack exhausted". No disagreement. Every implementation graph is
blocked (the server's final receive, §3). K = 4 was run because K = 3 finished in under a
minute on both tools. `no-dedup` has more graphs than `dedup` from K = 2 on, because a
duplicate always produces a second `Rep`, which `p0` may read in either order.

Commands, run from `B=/local/home/mkhoshechin/Documents/github-jmc/plan/benchmarks`, for
`K ∈ {1, 2, 3, 4}`, `v ∈ {dedup, no-dedup}`:

```
$B/_tools/run-rust.sh $B/rpc-at-most-once-aneris count spec --model bag -- <K>
$B/_tools/run-rust.sh $B/rpc-at-most-once-aneris count impl --model bag -- <K> <v>
$B/_tools/run-java.sh $B/rpc-at-most-once-aneris 'bench.rpc_at_most_once_aneris.AtMostOnceRpc:spec,<K>,bag' must 100000 600
$B/_tools/run-java.sh $B/rpc-at-most-once-aneris 'bench.rpc_at_most_once_aneris.AtMostOnceRpc:impl,<K>,bag,<v>' must 100000 600
```

Reachability check (not a count): `cd $B/rpc-at-most-once-aneris/out/fault-check && cargo run
-q -j 2 --offline --release -- <spec|impl> <K> bag [<v>]` with
`CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/bench-target` and
`TMPDIR=/local/home/mkhoshechin/tmp`; the exact cases are listed in §4. Every count command and
its output is in `out/runs.log`; every reachability command and its output is in
`out/fault-check.log`.
