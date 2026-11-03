# Disel calculator: one specification, three servers

**Family:** related-work  **Source:** Disel (Sergey, Wilcox, Tatlock, POPL 2018),
`related-work-papers/sources/disel/theories/Examples/Calculator/` (CANDIDATES.md N2, record in
CATALOGUE-verdi-disel.md)  **Expected verdict:** `batched`, `memo`, `delegating`: **CONFORMS**
(each answers every `Req(args)` with `Resp(f(args), args)` to its requester, from one serving
thread); `batched-swap`, `memo-ignore-args`, `delegating-prev`: **DOES NOT REFINE** (a client
receives a `Resp` whose value or arguments are not `f` of its own request).

## 1. The protocol

Disel's calculator: a client sends `req` (tag 0, contents `args`), a server answers `resp`
(tag 1, contents `ans :: args`) with `f args = Some ans` (`CalculatorProtocol.v`, `ss_safe`).
`SimpleCalculatorApp.v` fixes `f [x; y] = x + y`.

Roles:

- Visible: `c0`, `c1` (clients, `compute_list_f`: K times `compute_f server args` = send
  `Req`, blocking receive `Resp`).
- Invisible: `server` (specification); `batcher`, or `memo`, or `memo` + `delegator`
  (implementations).

Messages: `Req { from, x, y }` (`from` is the requester: a client, or the delegator on the
`l2` instance), `Resp { ans, x, y }`. Argument domain `ARGS = [(1,2), (2,2)]`; client i's j-th
request has `ARGS[(i + j) mod 2]`, so at K = 1, `c0` asks `(1,2)` (f = 3) and `c1` asks `(2,2)`
(f = 4). Parameters: N = 2 clients (fixed), K requests per client (the size argument).
Smallest interesting size: K = 1 (every fault is visible there).

## 2. The specification program

One invisible `server` (rule 2: every implementation serves from one thread at a time). It
performs N·K blocking receives of `Req` and answers each with `Resp(f(x,y), x, y)` to `from`.
Visible behaviour: each `c_i` receives, for its j-th request, exactly `Resp(f(ARGS[(i+j) mod 2]),
ARGS[(i+j) mod 2])`; the answer to a request happens after that request and after every
request the server read before it. Graphs: the server's receive orders, (2K)!/(K!·K!) (2, 6,
20, 70 for K = 1..4).

## 3. The implementation programs

- **batched** (`batched_server`, `bsize = 2`): `batcher` receives two `Req`s, prepending each to
  `acc` (Disel's `receive_req_loop`), then answers both in `acc` order (`send_ans_loop`); N·K/2
  batches.
- **memo** (`memoizing_server`): `memo` keeps a table `(args, ans)`; on a hit it answers the
  cached value, on a miss it computes `f args` and records it.
- **delegating** (`delegating_server` + `SimpleCalculatorApp.v`'s wiring): `delegator` receives
  a client `Req`, sends `Req { from: delegator, args }` to `memo` (protocol instance `l2`),
  blocks on its `Resp`, and answers the client with that `ans`. `memo` is the memoising server.

## 4. Where they differ, and why the verdict is what it is

Notation: `S(m)` a send, `R(m)` a receive, per visible thread in program order.

**Conforming variants.** Each answers every request with `Resp(f(args), args)` to the request's
`from`; the answer depends on the request (it is sent after the request is read). `memo`'s table
returns `f(args)` for the same `args`, so its values equal the specification's; `memo` and
`delegating` read and answer requests one at a time in one thread (`delegating`'s `delegator` is
that thread; `memo` behind it is invisible), so their graphs correspond one-to-one to the
specification's receive orders (counts 2, 6, 20, 70 on both sides). `batched` reads both
requests of a batch before answering either, so both answers of a batch depend on both requests.
That order contains the order of the specification graph that reads the same two requests in
the same order; the implementation adds order, it never removes any (fewer graphs: 2, 4, 8, 16).
**CONFORMS.**

**Non-refining variants**, smallest size K = 1, every implementation graph:

- `batched-swap` (F2: the batch answers entry e's result to entry e+1's requester). Uncovered
  word: `c0: S(Req(c0,1,2)) R(Resp(4,2,2))`, `c1: S(Req(c1,2,2)) R(Resp(3,1,2))`. The
  specification gives `c0` only `Resp(3,1,2)`. Component: **signature value**.
- `memo-ignore-args` (F1: the lookup returns the first table entry, whatever `args`). The second
  served request is answered with the first one's value. Uncovered words: `c1: S(Req(c1,2,2))
  R(Resp(3,2,2))` (`c0` served first) or `c0: S(Req(c0,1,2)) R(Resp(4,1,2))` (`c1` first).
  Component: **signature value**.
- `delegating-prev` (F3: a one-slot cache in the delegator answers with the previous request's
  result). Same uncovered words as `memo-ignore-args`. Component: **signature value**.

Reachability was checked with `out/fault-check/` (the same program with an assertion
`ans == f(args) && args == own args` in each client): at K = 1 the assertion fails for the three
faulty variants (`c0 got Resp(4, (2, 2)) for (1, 2)`; `c1 got Resp(3, (2, 2)) for (2, 2)` twice)
and holds for `spec`, `batched`, `memo`, `delegating` at K = 1 and K = 2.
These are executed results (2026-10-30), one run each, from `$B/calculator-disel/out/fault-check`
with `export CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/bench-target; export
TMPDIR=/local/home/mkhoshechin/tmp`:

| claim | command | result |
|---|---|---|
| `batched-swap` fails, K=1 | `cargo run -q -j 2 --offline --release -- impl 1 bag batched-swap` | panic `c0 got Resp(4, (2, 2)) for (1, 2)` |
| `memo-ignore-args` fails, K=1 | `cargo run -q -j 2 --offline --release -- impl 1 bag memo-ignore-args` | panic `c1 got Resp(3, (2, 2)) for (2, 2)` |
| `delegating-prev` fails, K=1 | `cargo run -q -j 2 --offline --release -- impl 1 bag delegating-prev` | panic `c1 got Resp(3, (2, 2)) for (2, 2)` |
| `spec` holds, K=1 / K=2 | `cargo run -q -j 2 --offline --release -- spec <K> bag` | exit 0, `execs=2` / `execs=6` |
| `batched` holds, K=1 / K=2 | `cargo run -q -j 2 --offline --release -- impl <K> bag batched` | exit 0, `execs=2` / `execs=4` |
| `memo` holds, K=1 / K=2 | `cargo run -q -j 2 --offline --release -- impl <K> bag memo` | exit 0, `execs=2` / `execs=6` |
| `delegating` holds, K=1 / K=2 | `cargo run -q -j 2 --offline --release -- impl <K> bag delegating` | exit 0, `execs=2` / `execs=6` |

Every claim reproduced. The holding runs explore the same number of executions as the plain-Must
counts in §7. Every command and its full output is in `out/fault-check.log`.

## 5. Variants and faults

| variant | how it differs | expected verdict | reason |
|---|---|---|---|
| `batched` | `batched_server`, bsize 2 | CONFORMS | §4 |
| `batched-swap` | answer sent to the next entry's requester (catalogue F2) | DOES NOT REFINE (K ≥ 1) | `c0` receives `Resp(4,2,2)` |
| `memo` | `memoizing_server` | CONFORMS | §4 |
| `memo-ignore-args` | lookup ignores `args` (catalogue F1) | DOES NOT REFINE (K ≥ 1) | second-served client receives the first's value |
| `delegating` | `delegator` + memoising delegate | CONFORMS | §4 |
| `delegating-prev` | delegator answers the previous result (catalogue F3) | DOES NOT REFINE (K ≥ 1) | second-served client receives the first's value |

Six pairs, all against the one `spec`. The source ships no faults; F1–F3 are the catalogue's.
Rust receives are tag-filtered (`TAG_REQ`, `TAG_RESP`); each tag belongs to exactly one message
variant, so the filter is a type test, and the Java side uses `Channel.recv(M, Req.class)` /
`Resp.class`. The delegator is the only thread receiving both kinds.

## 6. Files

- `source/`: the seven `.v` files of `theories/Examples/Calculator/`, copied verbatim, with
  provenance in `source/PROVENANCE.md`.
- `rust/`: crate `bench_calculator_disel`. CLI, the shared main: `<bin> count|export|conf|online …
  --model M -- <K> [variant]`; default K 1, default variant `batched`. `--model` left out means
  `bag`, this benchmark's model, which the crate declares. The `out/fault-check` crate is a separate
  copy, not on the shared main: it keeps the old positional CLI `<spec|impl> <size>
  [fifo|bag|causal] [variant]`, so its commands stand as run. See `_tools/run-rust.sh`.
- `java/`: package `bench.calculator_disel`, class `Calculator` (args
  `<spec|impl>,<K>,<fifo|bag>,<variant>`), `Support` (the model argument, copied from
  `H1Support`). Spawn order: spec `server, c0, c1`; impl `batcher | memo | memo, delegator`, then
  `c0, c1`. Visible roles `c0`, `c1` on both sides. See `_tools/run-java.sh`.
- `out/runs.log`: every plain-Must command and its output; `out/fault-check/`: the reachability
  crate; `out/fault-check.log`: every reachability command and its output.

## 7. Plain-Must exploration counts (both tools, one run each)

Model `bag` (asyn, Disel's unordered soup) throughout. Size = K (requests per client), 2 clients.

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | K=1 | bag | 2 / 0 | 2 / 0 | yes |
| spec | K=2 | bag | 6 / 0 | 6 / 0 | yes |
| spec | K=3 | bag | 20 / 0 | 20 / 0 | yes |
| spec | K=4 | bag | 70 / 0 | 70 / 0 | yes |
| impl `batched` | K=1 | bag | 2 / 0 | 2 / 0 | yes |
| impl `batched` | K=2 | bag | 4 / 0 | 4 / 0 | yes |
| impl `batched` | K=3 | bag | 8 / 0 | 8 / 0 | yes |
| impl `batched` | K=4 | bag | 16 / 0 | 16 / 0 | yes |
| impl `batched-swap` | K=1 | bag | 2 / 0 | 2 / 0 | yes |
| impl `batched-swap` | K=2 | bag | 4 / 0 | 4 / 0 | yes |
| impl `batched-swap` | K=3 | bag | 8 / 0 | 8 / 0 | yes |
| impl `batched-swap` | K=4 | bag | 16 / 0 | 16 / 0 | yes |
| impl `memo` | K=1 | bag | 2 / 0 | 2 / 0 | yes |
| impl `memo` | K=2 | bag | 6 / 0 | 6 / 0 | yes |
| impl `memo` | K=3 | bag | 20 / 0 | 20 / 0 | yes |
| impl `memo` | K=4 | bag | 70 / 0 | 70 / 0 | yes |
| impl `memo-ignore-args` | K=1 | bag | 2 / 0 | 2 / 0 | yes |
| impl `memo-ignore-args` | K=2 | bag | 6 / 0 | 6 / 0 | yes |
| impl `memo-ignore-args` | K=3 | bag | 20 / 0 | 20 / 0 | yes |
| impl `memo-ignore-args` | K=4 | bag | 70 / 0 | 70 / 0 | yes |
| impl `delegating` | K=1 | bag | 2 / 0 | 2 / 0 | yes |
| impl `delegating` | K=2 | bag | 6 / 0 | 6 / 0 | yes |
| impl `delegating` | K=3 | bag | 20 / 0 | 20 / 0 | yes |
| impl `delegating` | K=4 | bag | 70 / 0 | 70 / 0 | yes |
| impl `delegating-prev` | K=1 | bag | 2 / 0 | 2 / 0 | yes |
| impl `delegating-prev` | K=2 | bag | 6 / 0 | 6 / 0 | yes |
| impl `delegating-prev` | K=3 | bag | 20 / 0 | 20 / 0 | yes |
| impl `delegating-prev` | K=4 | bag | 70 / 0 | 70 / 0 | yes |

All JMC runs ended with "stack exhausted". No disagreement. K = 4 was run because K = 3
finished in under a minute on both tools. A fault changes values only, never the control flow,
so each faulty variant has its correct twin's counts.

Commands, run from `B=/local/home/mkhoshechin/Documents/github-jmc/plan/benchmarks`, for
`K ∈ {1, 2, 3, 4}`, `v ∈ {batched, batched-swap, memo, memo-ignore-args, delegating, delegating-prev}`:

```
$B/_tools/run-rust.sh $B/calculator-disel count spec --model bag -- <K>
$B/_tools/run-rust.sh $B/calculator-disel count impl --model bag -- <K> <v>
$B/_tools/run-java.sh $B/calculator-disel 'bench.calculator_disel.Calculator:spec,<K>,bag' must 100000 600
$B/_tools/run-java.sh $B/calculator-disel 'bench.calculator_disel.Calculator:impl,<K>,bag,<v>' must 100000 600
```

Reachability check (not a count): `cd $B/calculator-disel/out/fault-check && cargo run -q -j 2
--offline --release -- <spec|impl> <K> bag [<v>]` with `CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/bench-target`
and `TMPDIR=/local/home/mkhoshechin/tmp`; the exact cases are listed in §4.
Every count command and its output is in `out/runs.log`; every reachability command and its
output is in `out/fault-check.log`.
