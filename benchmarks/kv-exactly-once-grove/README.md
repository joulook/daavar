# Exactly-once KV service (Grove tutorial) against one KV server

**Family:** related-work  **Source:** Grove (Sharma et al., SOSP 2023) / gokv,
`related-work-papers/sources/gokv/tutorial/kvservice/` (CANDIDATES.md N6, record in
CATALOGUE-grove-civl.md)  **Expected verdict:** `exactly-once`: **CONFORMS** (a retried RPC
carries the same fresh `OpId` and is answered from `lastReplies`, so every operation is applied
once); `no-request-id`: **DOES NOT REFINE** at 1 key (a retried `ConditionalPut` is executed
again and `c0` receives `Ok(false)`, while the specification always gives `Ok(true)`).

## 1. The protocol

The source's `Server` (`server.go`) holds `kvs`, `lastReplies: OpId → reply` and `nextFreshId`.
A `Clerk` (`client.go`) obtains an op id (`getFreshNum`), then sends `put`/`conditionalPut`/`get`
with that `OpId`, retrying while the urpc call returns an error; the server answers a repeated
`OpId` from `lastReplies` without re-executing.

Roles:

- Visible: `c0`, `c1` (the clients; each sends `Op` and receives `Res`).
- Invisible: `server` (both sides); `p0`, `p1` (the clerks, implementation only; rule 3: the id
  request, the lost reply and the retry stay between clerk and server).

Messages: `Op { from, op }` (client → server/clerk), `Res(res)` (→ client), `Fresh { from }` /
`FreshId(n)` (`getFreshNum`), `Rpc { from, op_id, op }` / `Reply(res)` (the operation RPC;
`op_id = None` in `no-request-id`). `op ∈ {CondPut{key, expected, new}, Get{key}}`,
`res ∈ {Ok(bool), Val(v)}`; values 0 = `""`, 1 = `"1"`. Script: for every key k in order, `c0`
runs `ConditionalPut(k, "", "1")` and `c1` runs `Get(k)`. Parameters: N = 2 clients (fixed),
`keys` (the size argument), 1 dropped reply in the whole run (at `p0`). Smallest interesting
size: 1 key.

## 2. The specification program

One invisible `server` (rule 2: the implementation has one server for all keys, so the
specification has one too). It performs N·keys blocking receives of `Op`, executes each once
on `kvs` (initially all `""`), and sends `Res` to `from`. Since only `c0` writes, `c0`'s
`ConditionalPut(k, "", "1")` returns `Ok(true)` in every graph, and `c1`'s `Get(k)` returns
`Val(1)` iff it is served after it. Graphs: (2·keys)!/(keys!·keys!) (2, 6, 20).

## 3. The implementation program

- `p_i` (clerk): for each key, receives `Op` from `c_i`; with ids, sends `Fresh` and receives
  `FreshId(n)`; sends `Rpc(op_id, op)` and receives `Reply(r)`; `p0` may, once in the run,
  `if nondet()` treat that reply as lost (urpc error), resend the same `Rpc` and take the second
  `Reply`. It forwards `Res(r)` to `c_i`. The drop is modelled after the reply's receipt: the
  server has executed the operation, and the clerk does not use its reply.
- `server`: a blocking receive loop (A4 idiom, rule 1), ending blocked when the clerks are done:
  `Fresh` → `FreshId(nextFreshId++)`; `Rpc` → the cached reply if `op_id` is in `lastReplies`,
  else execute, record `(op_id, reply)` when `op_id` is present, and answer.

Every implementation graph ends with `server` blocked in its receive, so all are counted
blocked; that status is invisible and unobserved. Receives are unguarded in both languages (one
message type, `Msg`); at each receive only the expected kind can be pending.

## 4. Where they differ, and why the verdict is what it is

**`exactly-once`.** Each operation gets a distinct `OpId`; the retry carries the same one, so the
server executes it once and answers the retry with the recorded reply. The clerk forwards a reply
computed at the single execution, so the visible history is the server's order of first
executions, one of the specification's orders, with the same results. The `Fresh` exchanges and
the retry only add order through the one server thread. **CONFORMS.**

**`no-request-id`**, smallest size 1 key. Schedule: `p0` sends `Rpc(None, CondPut(0,"","1"))`;
`server` executes it (`kvs[0] = "1"`, `Ok(true)`); `p0` drops that reply and resends; `server`
executes it again (`kvs[0] = "1" ≠ ""`, `Ok(false)`); `p0` forwards it. Uncovered word:

```
c0: S(Op(c0,CondPut(0,0,1))) R(Res(Ok(false)))
```

(`c1`'s word is arbitrary.) The specification gives `c0` only `R(Res(Ok(true)))`. Component:
**signature value**. It needs only `c0` and the one drop.

Reachability was checked with `out/fault-check/` (the same program with the assertion
`c0 receives Ok(true)`): it fails for `no-request-id` at 1 key (`c0 got Res(Ok(false)) for
CondPut(0)`), and holds for `spec` (1 key) and `exactly-once` (1 and 2 keys).

These are executed results (2026-10-30), one run each, from
`$B/kv-exactly-once-grove/out/fault-check` with `export
CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/bench-target; export TMPDIR=/local/home/mkhoshechin/tmp`:

| claim | command | result |
|---|---|---|
| `no-request-id` fails, 1 key | `cargo run -q -j 2 --offline --release -- impl 1 bag no-request-id` | panic `c0 got Res(Ok(false)) for CondPut(0)` |
| `spec` holds, 1 key | `cargo run -q -j 2 --offline --release -- spec 1 bag` | exit 0, `execs=2 blocked=0` |
| `exactly-once` holds, 1 key | `cargo run -q -j 2 --offline --release -- impl 1 bag exactly-once` | exit 0, `execs=0 blocked=16` |
| `exactly-once` holds, 2 keys | `cargo run -q -j 2 --offline --release -- impl 2 bag exactly-once` | exit 0, `execs=0 blocked=322` |

Every claim reproduced. The holding runs explore the same numbers of executions as the
plain-Must counts in §7 (implementation graphs are all blocked, §3). Every command and its full
output is in `out/fault-check.log`.

## 5. Variants and faults

| variant | how it differs | expected verdict | reason |
|---|---|---|---|
| `exactly-once` | the source: fresh `OpId`, `lastReplies` | CONFORMS | §4 |
| `no-request-id` | the clerk takes no `OpId` (`op_id = None`), so the server cannot recognise the retry | DOES NOT REFINE (keys ≥ 1) | re-executed `CondPut` returns `Ok(false)` to `c0` |

The source ships no faults. The catalogue's fault ("delete the `lastReplies` check in
`conditionalPut`") has the same visible effect; `no-request-id` removes the id altogether,
which also removes the `Fresh` round trip, hence its smaller counts.

## 6. Files

- `source/`: `tutorial/kvservice/` (server, client, proto, generated marshalling, `*_gk/`,
  `cmd/`), copied verbatim, with provenance in `source/PROVENANCE.md`. The Perennial proof is
  not on disk; the specification program is the centralised one.
- `rust/`: crate `bench_kv_exactly_once_grove`. CLI, the shared main: `<bin>
  count|export|conf|online … --model M -- <keys> [exactly-once|no-request-id]`; default keys 1,
  default variant `exactly-once`. `--model` left out means `bag`, this benchmark's model, which the
  crate declares. `conf` is not this crate's conformance check: visible values carry a `ThreadId`,
  the two sides spawn in different orders, and `conf` compares typed values, so it reports every
  implementation graph; use `online` or `conf-join`, which compare role names. The `out/fault-check`
  crate is a separate copy, not on the shared main: it keeps the old positional CLI `<spec|impl>
  <size> [fifo|bag|causal] [variant]`, so its commands stand as run. See `_tools/run-rust.sh`.
- `java/`: package `bench.kv_exactly_once_grove`, class `ExactlyOnceKv` (args
  `<spec|impl>,<keys>,<fifo|bag>,<variant>`), `Support` (the model argument, copied from
  `H1Support`). Spawn order: spec `server, c0, c1`; impl `server, p0, p1, c0, c1`. Visible roles
  `c0`, `c1` on both sides. Rust `op_id: Option<u8>` is Java `opId`, −1 for `None`. See
  `_tools/run-java.sh`.
- `out/runs.log`: every plain-Must command and its output; `out/fault-check/`: the reachability
  crate; `out/fault-check.log`: every reachability command and its output.

## 7. Plain-Must exploration counts (both tools, one run each)

Model `bag` (asyn) throughout. The catalogue suggests asyn between clerk and server and p2p on
the client legs; a client leg carries at most one message at a time, so `bag` there equals
`fifo`, and one model per run is used. Size = keys, 2 clients, 1 drop.

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | keys=1 | bag | 2 / 0 | 2 / 0 | yes |
| spec | keys=2 | bag | 6 / 0 | 6 / 0 | yes |
| spec | keys=3 | bag | 20 / 0 | 20 / 0 | yes |
| impl `exactly-once` | keys=1 | bag | 0 / 16 | 0 / 16 | yes |
| impl `exactly-once` | keys=2 | bag | 0 / 322 | 0 / 322 | yes |
| impl `exactly-once` | keys=3 | bag | 0 / 6072 | 0 / 6072 | yes |
| impl `no-request-id` | keys=1 | bag | 0 / 5 | 0 / 5 | yes |
| impl `no-request-id` | keys=2 | bag | 0 / 26 | 0 / 26 | yes |
| impl `no-request-id` | keys=3 | bag | 0 / 125 | 0 / 125 | yes |

All JMC runs ended with "stack exhausted". No disagreement. Every implementation graph is
blocked (the server's final receive, §3). keys = 4 was not run: the JMC run of
`exactly-once` at keys = 3 did not finish in under a minute.

Commands, run from `B=/local/home/mkhoshechin/Documents/github-jmc/plan/benchmarks`, for
`k ∈ {1, 2, 3}`, `v ∈ {exactly-once, no-request-id}`:

```
$B/_tools/run-rust.sh $B/kv-exactly-once-grove count spec --model bag -- <k>
$B/_tools/run-rust.sh $B/kv-exactly-once-grove count impl --model bag -- <k> <v>
$B/_tools/run-java.sh $B/kv-exactly-once-grove 'bench.kv_exactly_once_grove.ExactlyOnceKv:spec,<k>,bag' must 100000 600
$B/_tools/run-java.sh $B/kv-exactly-once-grove 'bench.kv_exactly_once_grove.ExactlyOnceKv:impl,<k>,bag,<v>' must 100000 600
```

Reachability check (not a count): `cd $B/kv-exactly-once-grove/out/fault-check && cargo run -q
-j 2 --offline --release -- <spec|impl> <k> bag [<v>]` with
`CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/bench-target` and
`TMPDIR=/local/home/mkhoshechin/tmp`; the exact cases are listed in §4. Every count command and
its output is in `out/runs.log`; every reachability command and its output is in
`out/fault-check.log`.
