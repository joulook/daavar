# Leader–follower replicated log (Aneris repdb) against a prefix-replica specification

**Family:** related-work  **Source:** Aneris (Krogh-Jespersen et al., ESOP 2020; the
reliable-communication library), `related-work-papers/sources/aneris/ml_sources/examples/reliable_communication/lib/repdb/`
and `.../examples/repdb_leader_followers/causality_example_code.ml`, Iris spec in
`aneris/aneris/examples/reliable_communication/lib/repdb/spec/` (CANDIDATES.md N11, record in
CATALOGUE-coq-iris.md)  **Expected verdict:** `correct` under `fifo` and under `bag`:
**CONFORMS** (the follower applies the leader's log in index order, so a reader sees a growing
prefix of the log); `asyn-fault` under `bag`: **DOES NOT REFINE** (the pipelined follower applies
entry 1 before entry 0, and `reader` sees `y = 1` but then `x = None`); `asyn-fault` under `fifo`:
**CONFORMS** (a control: FIFO delivers the entries in order).

## 1. The protocol

`repdb_code.ml`: clients write `(k, v)` and read `k` at the leader (`InjL (k, v)` → `InjL ()`,
`InjR k` → `InjR (val option)`); the leader applies a write to its DB, appends `((k, v), n)` to
its log and signals. A follower's `sync_loop` requests log index `i` from the leader
(`follower_request_handler`: `log_wait_until log mon i`, then entry `i`), checks
`assert (i = j)`, appends the entry, writes `k := v` to its DB, and asks for `i + 1`; its client
handler answers a read of `k` from its DB (`val option`). The example
(`causality_example_code.ml`): node 0 writes `x = 37`, then `y = 1` at the leader; node 1 reads
`y` at the follower until it is `Some 1`, then asserts `rd "x" = Some 37`.

Roles:

- Visible: `writer` (node 0, at the leader), `reader` (node 1, at the follower).
- Invisible: `leader`, `follower` on both sides (different programs: §2 and §3).

Messages: `Write { from, k, v }` / `Ack` (writer ↔ leader, both sides), `Read { from, k }` /
`Val(v)` (reader ↔ follower, both sides; `v = 0` is `None`); `Entry { k, v, j }` (leader →
follower, `((k, v), j)`, both sides: pushed unprompted in the specification, in reply to a `Sync`
in the implementation); implementation only: `Sync { from, i }` (follower → leader, the requested
index).

Script and parameters: `W` writes (the size argument): key `i` := 37, 1, 2, … (`x = 37`, `y = 1`
at W = 2); then `W` reads by `reader` in reverse key order (`y`, then `x`). The source's
read-until loop on `y` is bounded to one read. 1 follower. Smallest interesting size: W = 2.
Models: `fifo` (p2p, the source's reliable FIFO channels) and `bag` (asyn).

## 2. The specification program

Two invisible servers, the structure of the Iris spec (the leader's log is the global history,
a follower's state is a prefix of it) and of rule 2 (the leader and the follower serve their
clients independently, so the specification has one server each).

- `leader`: W blocking receives of `Write`; for the j-th: answer `Ack`, then push
  `Entry { k, v, j }` to `follower`.
- `follower`: 2W blocking receives of `Entry` or `Read` (when an entry arrives relative to the
  reads is the receive's nondeterminism). An entry is kept until every earlier index has
  arrived, and the contiguous prefix is applied to `db`; `Read(k)` answers `Val(db[k])`.

Visible behaviour: a read may lag the writes (a follower that has received no entry answers
0), but the reader's view is a prefix of the log and grows, so once `reader` sees a later write
it sees every earlier one. The only causal order between `writer` and `reader` is leader →
follower entry delivery: a read that returns write i's value is after `S(Write i)`; nothing
orders a read before a write or an ack. Graphs: C(2W, W) under `fifo` (6, 20, 70 for W = 2, 3,
4: the follower's interleaving of entries and reads); under `bag` the entries also arrive in any
order (12, 120, 1680).

## 3. The implementation program

- `leader`: 2W blocking receives (W `Write`s, W `Sync`s). `Write`: DB and log updated, `Ack`,
  then every waiting `Sync` whose index now exists is answered, in index order
  (`monitor_signal` / `log_wait_until`). `Sync(i)`: answered with `Entry(log[i], j = i)` if the
  entry exists, otherwise kept waiting.
- `follower`: `correct` (the source's `sync_loop`): sends `Sync(0)`; then 2W blocking receives:
  `Entry(k, v, j)`: `assert(next == j)`, `db[k] = v`, `next += 1`, send `Sync(next)` while
  `next < W`; `Read(k)`: answer `Val(db[k])`. `asyn-fault` (catalogue fault (a), a pipelined,
  non-blocking sync): sends `Sync(0)`, …, `Sync(W−1)` at once and applies each `Entry` in arrival
  order, without the index check.

Deviations: the leader's `update_log_copy_loop` (which publishes the log to the followers' copy
every 3 s) is not modelled (the follower reads the leader's log directly; the copy only adds
lag, which the follower already has); the follower's sync loop and client handler, two threads
sharing the DB under a monitor in the source, are one thread receiving both kinds of message.

## 4. Where they differ, and why the verdict is what it is

Notation: `S(m)` a send, `R(m)` a receive, per visible thread in program order.

**`correct`, CONFORMS (fifo and bag).** The follower holds one outstanding `Sync` and applies
entry `i` only after entries `0..i−1`, so its DB is always the first `p` entries of the leader's
log, and `p` grows. That holds under `bag` too: a request–reply chain has nothing to reorder.
Take the specification graph in which the specification follower receives entry `i` at the
position (among its reads) where the implementation follower applies it, in index order. Its
causal order between visible events is contained in the implementation graph's: in both,
`S(Write i)` reaches the follower through the leader's receipt of write `i` (in the
implementation, `Entry i` is sent only after that receipt), and the specification adds no other
cross edge (its follower never sends to its leader). The implementation's extra order (the
`Sync` requests to the leader) only removes linearisations. Values agree read by read, since
both followers hold the same prefix at each read. So every visible word of the implementation
graph is a word of that specification graph. In particular the graph in which the follower
answers both reads with 0 before any entry arrives, and the leader acknowledges both writes,
has the specification counterpart in which the follower receives both reads first.

**`asyn-fault` under `bag`, DOES NOT REFINE**, smallest size W = 2. Schedule: `follower` sends
`Sync(0)`, `Sync(1)`; `leader` keeps both waiting, receives `Write(x, 37)` and sends `Entry(x, 37,
0)`, receives `Write(y, 1)` and sends `Entry(y, 1, 1)`; `follower` receives `Entry(y, 1, 1)` first
(bag), serves `Read(y)` → 1 and `Read(x)` → `None`, then receives `Entry(x, 37, 0)`. Uncovered
word:

```
reader: S(Read(reader, y)) R(Val(1)) S(Read(reader, x)) R(Val(0))
```

(`writer`: `S(Write(writer, x, 37)) R(Ack) S(Write(writer, y, 1)) R(Ack)`.) In the specification
the follower applies entry 1 only after entry 0, so `Val(1)` for `y` means `x = 37` is already
in its `db` and the later read of `x` returns 37. This is the
failure of the causality example's `assert (vx = Some 37)`. Component: **signature value**.

**`asyn-fault` under `fifo`, CONFORMS (control).** The leader answers the pipelined requests in
index order (it receives them in order on the FIFO channel and drains the waiting ones in index
order), and FIFO delivers them in that order, so the follower applies a prefix as in `correct`,
and the same choice of specification graph covers it.

Reachability was checked with `out/fault-check/` (the same program with an assertion in `reader`:
once a read returned a key's value, every later read of an earlier key returns its value): it
fails for `asyn-fault` under `bag` at W = 2 (`reader saw a later write but key 0 = 0`) and holds
for `spec` (W = 2 and 3, fifo and bag), `correct` (W = 2 fifo and bag, W = 3 bag), and `asyn-fault`
under `fifo` (W = 2 and 3).

The task's wording for `asyn-fault` was "the same [program] under asyn". The source's sync is a
request–reply loop with one outstanding request, so running `correct` under `bag` cannot reorder
entries; the rows `correct`/`bag` show it conforms. The out-of-order application therefore needs
the catalogue's pipelined sync, which is what `asyn-fault` is.

## 5. Variants and faults

| variant | model | how it differs | expected verdict | reason |
|---|---|---|---|---|
| `correct` | fifo | the source's `sync_loop` | CONFORMS | §4 |
| `correct` | bag | the same, asyn | CONFORMS | request–reply sync cannot reorder |
| `asyn-fault` | bag | pipelined sync, entries applied in arrival order (catalogue fault (a)) | DOES NOT REFINE (W ≥ 2) | `reader` sees `y = 1`, then `x = None` |
| `asyn-fault` | fifo | the same, p2p | CONFORMS (control) | FIFO keeps the entries in order |

Each against `spec` under the same model. The source ships no fault.

## 6. Files

- `source/`: `ml/repdb_code.ml`, `ml/log_code.ml`, `ml/causality_example_code.ml`, and
  `coq-spec/*.v` (the Iris specification), copied verbatim, with provenance in
  `source/PROVENANCE.md`. The path `aneris/aneris/examples/repdb/` named in the task does not
  exist; these are the repdb files.
- `rust/`: crate `bench_replicated_log_aneris`. CLI: `<spec|impl> <W> [fifo|bag|causal]
  [correct|asyn-fault]`, default model `fifo`, default variant `correct`. See `_tools/run-rust.sh`.
- `java/`: package `bench.replicated_log_aneris`, class `RepDb` (args
  `<spec|impl>,<W>,<fifo|bag>,<variant>`), `Support` (the model argument, copied from
  `H1Support`). Spawn order: spec `follower, leader, writer, reader`; impl `leader, follower, writer, reader`.
  Visible roles `writer`, `reader` on both sides. Rust's tagged receives are type-test receives
  in Java.
- `out/runs.log`: every command and its output; `out/fault-check/`: the reachability crate.

## 7. Plain-Must exploration counts (both tools, one run each)

Size = W (writes, and as many reads). Both models for every side. W = 4 was run wherever W = 3 finished in under a minute on both tools, i.e. everywhere except `asyn-fault` under `bag` (W = 3 took longer on JMC).

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | W=2 | fifo | 6 / 0 | 6 / 0 | yes |
| spec | W=3 | fifo | 20 / 0 | 20 / 0 | yes |
| spec | W=4 | fifo | 70 / 0 | 70 / 0 | yes |
| impl `correct` | W=2 | fifo | 30 / 0 | 30 / 0 | yes |
| impl `correct` | W=3 | fifo | 280 / 0 | 280 / 0 | yes |
| impl `correct` | W=4 | fifo | 2940 / 0 | 2940 / 0 | yes |
| impl `asyn-fault` | W=2 | fifo | 36 / 0 | 36 / 0 | yes |
| impl `asyn-fault` | W=3 | fifo | 400 / 0 | 400 / 0 | yes |
| impl `asyn-fault` | W=4 | fifo | 4900 / 0 | 4900 / 0 | yes |
| spec | W=2 | bag | 12 / 0 | 12 / 0 | yes |
| spec | W=3 | bag | 120 / 0 | 120 / 0 | yes |
| spec | W=4 | bag | 1680 / 0 | 1680 / 0 | yes |
| impl `correct` | W=2 | bag | 30 / 0 | 30 / 0 | yes |
| impl `correct` | W=3 | bag | 280 / 0 | 280 / 0 | yes |
| impl `correct` | W=4 | bag | 2940 / 0 | 2940 / 0 | yes |
| impl `asyn-fault` | W=2 | bag | 144 / 0 | 144 / 0 | yes |
| impl `asyn-fault` | W=3 | bag | 14400 / 0 | 14400 / 0 | yes |

The spec rows are from the second specification (leader + follower, after the review); the first specification's rows remain in `out/runs.log`, marked superseded. All runs exhausted on both tools. No disagreement. `correct` has the same counts under both models (one outstanding request per channel leaves nothing to reorder).

Commands, run from `B=/local/home/mkhoshechin/Documents/github-jmc/plan/benchmarks`, for `W ∈ {2, 3, 4}`, `m ∈ {fifo, bag}`, `v ∈ {correct, asyn-fault}`:

```
timeout 600 $B/_tools/run-rust.sh $B/replicated-log-aneris spec <W> <m>
timeout 600 $B/_tools/run-rust.sh $B/replicated-log-aneris impl <W> <m> <v>
$B/_tools/run-java.sh $B/replicated-log-aneris 'bench.replicated_log_aneris.RepDb:spec,<W>,<m>' must 100000 600
$B/_tools/run-java.sh $B/replicated-log-aneris 'bench.replicated_log_aneris.RepDb:impl,<W>,<m>,<v>' must 100000 600
```

The fault checks of §4 (not counts) ran as `cd $B/replicated-log-aneris/out/fault-check && cargo run -q -j 2
--offline --release -- <side> <size> <model> [variant]` with
`CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/bench-target`. Every command and its output is in
`out/runs.log`.
