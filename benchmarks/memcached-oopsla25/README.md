# Memcached (OOPSLA 2025): per-key cache against v3 per-bucket storage

**Family:** related-work  **Source:** Bílý, Pereira, Müller, "A Refinement Methodology for
Distributed Programs in Rust", OOPSLA 2025 §5.1,
`related-work-papers/sources/oopsla25-artifact/oopsla552-artefact-amd64/refinement-lib/case-study-memcached/`
(record in CATALOGUE-oopsla25.md; planned pair RELATED-BENCHMARKS.tex §B5)  **Expected verdict:**
`v3`: **CONFORMS** (identity hash: a bucket serves exactly one key, so each bucket thread is that key's
server, with an invisible relay in front); `shared-bucket`: **CONFORMS** (one bucket for both keys
orders more than the per-key specification, and extra order is accepted; TraceForge's check is silent
at Q = 1, 2, 3); `lost-set`: **DOES NOT REFINE** from Q = 2 (a client's `Get` after its own
acknowledged `Set` answers `None`); a conforming control at Q = 1, where no client issues a `Get`.

## 1. The protocol

An in-memory key-value cache served over connections (`src/bin/memcached.rs`). For each connection
the server runs `handle_client`: read a command (`ReceiveCommand`), `Storage::process` it, write the
response (`SendResponse`). `Storage::process` (`src/storage/mod.rs`, v3, fine-grained locking) hashes
the key, locks only that bucket, and runs `set` / `get` on the bucket's item list. The model
(`src/model/mod.rs`) is one `cache` and, per connection, a FIFO command queue `network_cmd[con]` and a
FIFO response queue `network_res[con]`; `cache_process` (`src/model/message.rs`) gives the
semantics: `Set{key,value}` stores and answers `Set` (`STORED`); `Get{key}` answers
`Get{key, value}` with the stored value or `None`.

Roles:

- Visible: `c0`, `c1`, the clients, one connection each (the model's environment actions
  `SendCommand` / `ReceiveResponse`).
- Invisible: `k0`, `k1` (specification); `w0`, `w1` (connection handlers) and `b0`, `b1` (bucket
  storage, one thread per bucket: the thread plays the bucket's `MutexWrap` and its
  `StorageBucket(hash)` guard) (implementation; `b0` alone for `shared-bucket`).

Messages: `Cmd { c, op }` with `op = Get { key } | Set { key, value }` (`c` is the connection id the
model's actions carry: a client index), `Res(Stored | Got { key, value: Option })` (the source's
`Get` response echoes the key; kept). Keys {0, 1}, values {1, 2}. Dropped: expiry and time
(`AdvanceTime`, `Instant`, `expiry: i64`), `Delete`, `Evict` (no implementation code performs it),
connection accept/close (connections are static threads), the text protocol (trusted in the source).

Parameters: N = 2 clients, 2 keys, Q commands per client (the size argument, 1..3). Client
scripts (closed loop: send a command, block on its response), the first Q of:

- `c0`: `Set(0,1)`, `Get(1)`, `Get(0)`
- `c1`: `Set(1,2)`, `Get(1)`, `Set(0,2)`

At Q = 1 the two `Set`s are on different keys (pure cross-key concurrency); at Q = 2 `c0`'s
`Get(1)` races `c1`'s `Set(1,2)` and `c1` reads its own write; at Q = 3 `c0`'s `Get(0)` races
`c1`'s `Set(0,2)`. Smallest interesting size: Q = 1 for `shared-bucket` (the bucket orders the two
`Set`s), Q = 2 for `lost-set`. Model `fifo` (p2p: per-connection TCP). The caller fixed 1–2
commands per client; Q = 3 is the brief's next size, run because Q = 2 took seconds.

## 2. The specification program

One server thread per key (rule 2: v3 serves different buckets independently, and with the
identity hash a bucket is a key): `k0`, `k1`. Server `k{key}` performs exactly as many blocking
receives as the scripts issue commands on its key (static, `k0`+`k1`: 1+1, 1+3, 3+3 over Q = 1, 2, 3), applies
`cache_process` to its cell and answers `Res` to client `c`. A client sends each command straight to
the server of its key and blocks on the response; responses reach it in FIFO order, as
`network_res[con]`. Visible behaviour: per key, the commands are served in one order consistent
with each client's program order, and every `Get` returns the value of the last `Set` before it in
that order; commands on different keys are unordered. Graphs: 1, 3, 7 (Q = 1, 2, 3).

## 3. The implementation program

- **v3**: `w{i}` (`handle_client`) Q times: blocking receive of `c{i}`'s `Cmd`, forward it to bucket
  `b{hash(key)}` (`hash` = identity on the two keys, so `B = 2` buckets, one key each), blocking
  receive of the bucket's `Res`, forward it to `c{i}`. `b{h}` (`Storage::process` under the bucket's
  lock) serves exactly the commands routed to it (a static count), holding a store for every key
  (the source's bucket list holds items of any key that hashes to it).
- **shared-bucket**: `hash` maps both keys to bucket 0: one storage thread `b0` serves all 2Q
  commands. This is the source's design with `TABLE_SIZE` collisions; the bucket lock orders
  commands on different keys.
- **lost-set** (seeded fault, catalogue fault (a): wrong hash on one command kind): the worker
  routes a `Set{key}` to bucket `(key+1) mod 2`, a `Get{key}` to bucket `key`. The wrong bucket
  stores the item; the `Get` looks in the right bucket and finds nothing.

All receives are blocking and untyped; each mailbox holds one kind of message at a time (a worker's
next `Cmd` is sent only after its previous `Res` was delivered). Every thread receives on its own
named channel `("c"|"w"|"b"|"k", i)`.

## 4. Where they differ, and why the verdict is what it is

Notation: `S(m)` a send, `R(m)` a receive, per visible thread in program order.

- **v3: CONFORMS.** With the identity hash, bucket `b{k}` serves exactly the commands on key `k`,
  in one order, with the same store semantics as `k{k}`; the workers are invisible relays, one per
  closed-loop client, so they add no choice (same counts: 1, 3, 7). Every implementation graph maps
  to the specification graph with the same per-key service order, same values, and an order that
  contains the specification's (the relay adds hops, never removes an edge). TraceForge's check:
  silence at Q = 1, 2, 3.
- **shared-bucket: CONFORMS** (the catalogue left it open). One bucket serves both keys, so in
  every implementation graph a `Set(0)` and a `Set(1)` are ordered by `b0`'s program order, which no
  per-key specification graph has. That is extra order in the implementation: per key, `b0`'s
  service order restricted to that key is a valid key-server order with the same values, so each
  implementation graph has a specification graph with the same signature whose order is contained
  in its own. The check is a containment of the specification's order in the implementation's
  (`lem:sig`: "no order stored under its signature is contained in its own"), so more order in the
  implementation is accepted. TraceForge's check: silence at Q = 1 (2 implementation graphs
  against 1 specification graph), Q = 2 (6 vs 3), Q = 3 (20 vs 7). The converse pair (v3 against a
  single-store specification) would be reported, by order; it is not built here.
- **lost-set: DOES NOT REFINE** from Q = 2. Every `Set` lands in the other key's bucket, every
  `Get` reads the right bucket, so every `Get` answers `None`. At Q = 2, in every implementation
  graph, `c1` reads its own key-1 write as absent. Uncovered word:
  **`c1: S(Cmd{c:1, Set(1,2)}) R(Res(Stored)) S(Cmd{c:1, Get(1)}) R(Res(Got{key:1, value:None}))`**;
  in every specification graph `k1` serves `c1`'s `Set(1,2)` before its `Get(1)` (program order and
  FIFO), and no command deletes, so the answer is `Some(2)` (or `Some` of a later `Set`).
  Component: **signature value**. TraceForge's stateful check: 3 of 3 implementation graphs
  uncovered at Q = 2, 7 of 7 at Q = 3; the enumerator (`conf 2 fifo lost-set enumerator`) names the
  first failing obligation "observation mismatch on `c1` at position 3: … another attempt of the
  specification observed receive Res(Got { key: 1, value: Some(2) }) at this position, the
  implementation has receive Res(Got { key: 1, value: None })". At Q = 1 the scripts issue only two
  `Set`s, both answered `Stored`: the misrouting is invisible, and the check is silent (a conforming
  control, as rule 4 requires to be said).

## 5. Variants and faults

| variant | how it differs | expected verdict | reason |
|---|---|---|---|
| `v3` | the artefact's v3: one storage thread per bucket, identity hash on two keys | CONFORMS | §4: a bucket is a key server |
| `shared-bucket` | both keys hash to one bucket | CONFORMS (observed: silence, Q = 1..3) | extra cross-key order in the implementation is accepted |
| `lost-set` | worker routes `Set{key}` to bucket `(key+1) mod 2` (catalogue fault (a)) | DOES NOT REFINE (Q ≥ 2); conforming control at Q = 1 | `c1` receives `Got{1, None}` after its own `Stored` |

One specification for all three. The source ships no faults; `lost-set` is the catalogue's (a).
Not built: the catalogue's (b) (needs `Delete`), (c) (a v2 snapshot), (d) (pipelining), and the v1/v2
implementations (only v3 is in the artefact).

## 6. Files

- `source/`: the artefact's `case-study-memcached/src/` (18 files) and `Cargo.toml`, copied verbatim,
  with provenance (artefact doi, zip md5, file md5s) in `source/PROVENANCE.md`.
- `rust/`: crate `bench_memcached_oopsla25`. CLI: `<spec|impl|conf> <Q> [fifo|bag|causal]
  [v3|shared-bucket|lost-set]` (defaults `fifo`, `v3`); `conf` runs TraceForge's conformance check
  with visible threads `c0`, `c1` (fifth argument `enumerator` for the diagnosing engine, `all` to
  keep every report). See `_tools/run-rust.sh`.
- `java/`: package `bench.memcached_oopsla25`: `Memcached` (args `<spec|impl>,<Q>[,<fifo|bag>[,<variant>]]`)
  and `Support` (the model argument and construct-first spawning, copied from the H1 idiom). Spawn
  order: spec `k0, k1, c0, c1`; impl `b0[, b1], w0, w1, c0, c1`. Visible roles `c0`, `c1` on both
  sides and in both languages. Receives are untyped `Channel.recv(m)`, as in Rust.
- `out/runs.log`: every plain-Must command and its (summary-line) output; `out/conf.log`: every
  conformance command and its output.

## 7. Plain-Must exploration counts (both tools, one run each)

Model `fifo` (p2p) throughout. Size = Q (commands per client), 2 clients, 2 keys.

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | Q=1 | fifo | 1 / 0 | 1 / 0 | yes |
| spec | Q=2 | fifo | 3 / 0 | 3 / 0 | yes |
| spec | Q=3 | fifo | 7 / 0 | 7 / 0 | yes |
| impl `v3` | Q=1 | fifo | 1 / 0 | 1 / 0 | yes |
| impl `v3` | Q=2 | fifo | 3 / 0 | 3 / 0 | yes |
| impl `v3` | Q=3 | fifo | 7 / 0 | 7 / 0 | yes |
| impl `shared-bucket` | Q=1 | fifo | 2 / 0 | 2 / 0 | yes |
| impl `shared-bucket` | Q=2 | fifo | 6 / 0 | 6 / 0 | yes |
| impl `shared-bucket` | Q=3 | fifo | 20 / 0 | 20 / 0 | yes |
| impl `lost-set` | Q=1 | fifo | 1 / 0 | 1 / 0 | yes |
| impl `lost-set` | Q=2 | fifo | 3 / 0 | 3 / 0 | yes |
| impl `lost-set` | Q=3 | fifo | 7 / 0 | 7 / 0 | yes |

All JMC runs ended with "stack exhausted". No disagreement. `shared-bucket`'s counts are the
interleavings of two closed-loop clients at one server, C(2Q, Q) = 2, 6, 20. Q = 4 does not exist
(the scripts have three commands).

Commands, run from `B=/local/home/mkhoshechin/Documents/github-jmc/plan/benchmarks`, for
`Q ∈ {1, 2, 3}`, `v ∈ {v3, shared-bucket, lost-set}`:

```
$B/_tools/run-rust.sh $B/memcached-oopsla25 spec <Q> fifo
$B/_tools/run-rust.sh $B/memcached-oopsla25 impl <Q> fifo <v>
$B/_tools/run-java.sh $B/memcached-oopsla25 'bench.memcached_oopsla25.Memcached:spec,<Q>,fifo' must 100000 600
$B/_tools/run-java.sh $B/memcached-oopsla25 'bench.memcached_oopsla25.Memcached:impl,<Q>,fifo,<v>' must 100000 600
```

Verdict check (TraceForge conformance, not a count), same `Q` and `v`:

```
$B/_tools/run-rust.sh $B/memcached-oopsla25 conf <Q> fifo <v>     # silence except lost-set Q=2 (3 uncovered), Q=3 (7)
$B/_tools/run-rust.sh $B/memcached-oopsla25 conf 2 fifo lost-set enumerator   # diagnostics quoted in §4
```

Every count command and its output is in `out/runs.log`; every conformance command and its output
in `out/conf.log`.
