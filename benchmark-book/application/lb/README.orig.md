# A4: load balancer with two specifications

**Family:** popl-conf application  **Source:** the TraceForge fixture A4 (`P5-APPS`),
`traceforge/src/conformance/grid.rs` at `a3a7e1b`, lines 3748–3912 (`A4Req`, `A4Reply`, `A4Spec`,
`a4_spec`, `A4Policy`, `A4Fault`, `a4_impl`), ported to `rust/`.
**Expected verdict:** `hash`, `pair-swap-*` against both specs, `rr` against `a4b`: **CONFORMS**;
`rr` against `a4a`: **DOES NOT REFINE** iff `q ≥ 2` and `m ≥ 2` (a client's requests land on two
servers); `hash-drop`, `drop`: **DOES NOT REFINE** (`c{k-1}` is never answered).

## 1. The protocol

Roles: visible clients `c0..c{k-1}`; invisible `dispatcher` (spec) or `balancer` (impl) and
servers `s0..s{m-1}`. Client `c_i` sends `q` requests `A4Req(i, j)`, each followed by a blocking
read of its reply `A4Reply(x, j)`, `x` the server that served it. The front thread forwards each
request to a server; server `s_x` loops forever, replying `A4Reply(x, j)` to client `i` directly
(so the servers end blocked in every graph, and every graph counts as blocked). All channels are
TraceForge typed channels with `LocalOrder` (per-sender FIFO) whatever `--model`. Parameters: `k`
clients, `q` requests each, `m` servers. Smallest interesting size: `(k, q, m) = (2, 1, 2)`;
`(1, 2, 2)` or `(2, 2, 2)` for `rr` against `a4a`.

## 2. The specification program

`a4_spec(k, q, m, spec)`: the `dispatcher` reads `k·q` requests in arrival order and forwards
each to a server chosen by `(0..m).nondet()`: `a4b` (relay) chooses per request; `a4a` (affinity)
chooses on a client's first request and sends all its later requests to the same server.

## 3. The implementation program

`a4_impl(k, q, m, policy, fault)`: the `balancer` reads `k·q` requests and routes request number
`p` (in arrival order) to `s_{p mod m}` (`RoundRobin`) or client `i`'s requests to `s_{i mod m}`
(`Hash`). Faults: `Drop` drops `c{k-1}`'s last request; `PairSwap` (`k = 2` only) reads two
requests and forwards the later-arrived one first, `q` times.

## 4. Where they differ, and why the verdict is what it is

`hash`: every client gets one fixed server, which `a4a` can choose on its first request and `a4b`
on every request; the balancer and the dispatcher both read in arrival order, so the causal order
of replies after requests is the same. **CONFORMS** against both.

`rr` against `a4b`: any server per request is allowed. **CONFORMS**. Against `a4a` at
`(k, q, m) = (1, 2, 2)`: `c0: S(A4Req(0,0)) R(A4Reply(0,0)) S(A4Req(0,1)) R(A4Reply(1,1))`; the
affinity spec serves both from one server. Component: **signature value**. With `q = 1` or `m = 1`
the two coincide (vacuous); at `(2, 1, 2)` conforms, at `(2, 2, 2)` 4 uncovered summaries.

`hash-drop`, `drop`: `c{k-1}`'s last request is never forwarded, so `c{k-1}` ends blocked on its
reply in every graph, while in the spec every client is answered. Component: **signature status**.
Smallest size `(1, 1, 1)`.

`pair-swap-hash` (both specs), `pair-swap-rr` (`a4b`): the servers are unchanged (hash) or
arbitrary (`a4b`), and forwarding a pair only after both arrived adds causal order (each reply is
after both requests), which the specification's graphs allow. **CONFORMS** (controls).
`pair-swap-rr` against `a4a` is not a row (it violates, as `rr` does).

## 5. Variants and faults

| impl variant | policy, fault | against `a4a` | against `a4b` |
|---|---|---|---|
| `hash` | Hash, None | CONFORMS | CONFORMS |
| `rr` | RoundRobin, None | DOES NOT REFINE iff q ≥ 2, m ≥ 2 (value) | CONFORMS |
| `hash-drop` | Hash, Drop | DOES NOT REFINE (status) | not a row |
| `drop` | RoundRobin, Drop | not a row | DOES NOT REFINE (status) |
| `pair-swap-hash` | Hash, PairSwap (k = 2) | CONFORMS (control) | CONFORMS (control) |
| `pair-swap-rr` | RoundRobin, PairSwap (k = 2) | not a row | CONFORMS (control) |

Rust `conf` (`out/runs.log`) at `(1, 2, 2)` for `rr/a4a`: 1 candidate violation; at `(2, 1, 2)`: silence for every row except `hash-drop/a4a` and
`drop/a4b` (2 candidate violations each); at `(2, 2, 2)`: silence except `rr/a4a` (4),
`hash-drop/a4a` (6), `drop/a4b` (6). As expected.

## 6. Files

- `source/`: none; the source is the TraceForge fixture named above (see `rust/src/main.rs`).
- `rust/`: the Must-API Rust crate on TraceForge's shared main; ARGS `<impl> <spec> <k> <q> <m>`.
- `java/`: the Java twin, package `bench.a4_balancer` (§8).
- `out/runs.log`: every command of the Java-twin round and its output; `out/export/`: the summary files.

## 7. Plain-Must exploration counts (both tools, one run each)

Model fifo (the channels are `LocalOrder` regardless; JMC p2p). Every graph is blocked (the
servers loop). The impl side ignores the spec argument and the spec side the impl argument.

| side | size (k, q, m) | TraceForge ARGS | JMC args | TraceForge complete / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|---|
| spec | 1, 1, 1 | `hash a4a 1 1 1` | `spec,1,1,1,fifo,a4a` | 0 / 1 | 0 / 1 | yes |
| spec | 1, 1, 1 | `hash a4b 1 1 1` | `spec,1,1,1,fifo,a4b` | 0 / 1 | 0 / 1 | yes |
| impl | 1, 1, 1 | `hash a4a 1 1 1` | `impl,1,1,1,fifo,hash` | 0 / 1 | 0 / 1 | yes |
| impl | 1, 1, 1 | `rr a4a 1 1 1` | `impl,1,1,1,fifo,rr` | 0 / 1 | 0 / 1 | yes |
| impl | 1, 1, 1 | `hash-drop a4a 1 1 1` | `impl,1,1,1,fifo,hash-drop` | 0 / 1 | 0 / 1 | yes |
| impl | 1, 1, 1 | `drop a4a 1 1 1` | `impl,1,1,1,fifo,drop` | 0 / 1 | 0 / 1 | yes |
| impl | 1, 1, 1 | `pair-swap-hash a4a 1 1 1` | `impl,1,1,1,fifo,pair-swap-hash` | 0 / 1 | 0 / 1 | yes |
| impl | 1, 1, 1 | `pair-swap-rr a4a 1 1 1` | `impl,1,1,1,fifo,pair-swap-rr` | 0 / 1 | 0 / 1 | yes |
| spec | 1, 2, 2 | `rr a4a 1 2 2` | `spec,1,2,2,fifo,a4a` | 0 / 2 | 0 / 2 | yes |
| impl | 1, 2, 2 | `rr a4a 1 2 2` | `impl,1,2,2,fifo,rr` | 0 / 1 | 0 / 1 | yes |
| spec | 2, 1, 2 | `hash a4a 2 1 2` | `spec,2,1,2,fifo,a4a` | 0 / 8 | 0 / 8 | yes |
| spec | 2, 1, 2 | `hash a4b 2 1 2` | `spec,2,1,2,fifo,a4b` | 0 / 8 | 0 / 8 | yes |
| impl | 2, 1, 2 | `hash a4a 2 1 2` | `impl,2,1,2,fifo,hash` | 0 / 2 | 0 / 2 | yes |
| impl | 2, 1, 2 | `rr a4a 2 1 2` | `impl,2,1,2,fifo,rr` | 0 / 2 | 0 / 2 | yes |
| impl | 2, 1, 2 | `hash-drop a4a 2 1 2` | `impl,2,1,2,fifo,hash-drop` | 0 / 2 | 0 / 2 | yes |
| impl | 2, 1, 2 | `drop a4a 2 1 2` | `impl,2,1,2,fifo,drop` | 0 / 2 | 0 / 2 | yes |
| impl | 2, 1, 2 | `pair-swap-hash a4a 2 1 2` | `impl,2,1,2,fifo,pair-swap-hash` | 0 / 2 | 0 / 2 | yes |
| impl | 2, 1, 2 | `pair-swap-rr a4a 2 1 2` | `impl,2,1,2,fifo,pair-swap-rr` | 0 / 2 | 0 / 2 | yes |
| spec | 2, 2, 2 | `hash a4a 2 2 2` | `spec,2,2,2,fifo,a4a` | 0 / 24 | 0 / 24 | yes |
| spec | 2, 2, 2 | `hash a4b 2 2 2` | `spec,2,2,2,fifo,a4b` | 0 / 96 | 0 / 96 | yes |
| impl | 2, 2, 2 | `hash a4a 2 2 2` | `impl,2,2,2,fifo,hash` | 0 / 6 | 0 / 6 | yes |
| impl | 2, 2, 2 | `rr a4a 2 2 2` | `impl,2,2,2,fifo,rr` | 0 / 6 | 0 / 6 | yes |
| impl | 2, 2, 2 | `hash-drop a4a 2 2 2` | `impl,2,2,2,fifo,hash-drop` | 0 / 6 | 0 / 6 | yes |
| impl | 2, 2, 2 | `drop a4a 2 2 2` | `impl,2,2,2,fifo,drop` | 0 / 6 | 0 / 6 | yes |
| impl | 2, 2, 2 | `pair-swap-hash a4a 2 2 2` | `impl,2,2,2,fifo,pair-swap-hash` | 0 / 4 | 0 / 4 | yes |
| impl | 2, 2, 2 | `pair-swap-rr a4a 2 2 2` | `impl,2,2,2,fifo,pair-swap-rr` | 0 / 4 | 0 / 4 | yes |

At `k = 1` the pair-swap balancer waits for a second request that never comes (not a row).
Commands (exactly, `$R` = `/local/home/mkhoshechin/tmp/bench-target/release`, `$D` = the daavar tree):

```
$R/tf_a4_balancer count spec --model fifo -- hash <a4a|a4b> <k> <q> <m>
$R/tf_a4_balancer count impl --model fifo -- <impl> a4a <k> <q> <m>
java -Xss4m -cp $D/benchmarks/a4-balancer/java/out:$(cat $D/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher \
     'bench.a4_balancer.A4Balancer:spec,<k>,<q>,<m>,fifo,<a4a|a4b>' must 0 1 <fresh report dir> - 600 </dev/null
java ... 'bench.a4_balancer.A4Balancer:impl,<k>,<q>,<m>,fifo,<impl>' must 0 1 <fresh report dir> - 600 </dev/null
```

## 8. Java twin

`java/bench/a4_balancer/A4Balancer.java` (an `MpProgram`) and `Support.java`. Arguments
`A4Balancer:spec,<k>,<q>,<m>,fifo,<a4a|a4b>` and
`A4Balancer:impl,<k>,<q>,<m>,fifo,<hash|rr|pair-swap-hash|pair-swap-rr|drop|hash-drop>` (the
model must be `fifo`: the Rust channels ignore `--model`). Visible `c0..c{k-1}`; spawn order as in
Rust: `dispatcher`/`balancer`, `s0..`, `c0..`. Messages: `@Tag("A4Req") A4Req(int i, int j)`,
`@Tag("A4Reply") A4Reply(int x, int j)`, the Rust tuple structs' names and field order; the
dispatcher's choice is `Nondet.chooseIndex(m)`.

Departure of form: Rust's typed channels (one request channel read by the front thread, one per
server, one reply channel per client) become the reading thread's mailbox under p2p. Each thread
reads exactly one channel, so the per-sender FIFO relation is the same; the counts are equal (§7).

Cross-language check (exports in `out/export/`, `conf-join <spec file> <impl file>`):

| impl / spec | size | Rust spec vs Java spec, both ways | Rust impl vs Java impl, both ways | Rust spec ⊒ Java impl | Java spec ⊒ Rust impl | hand verdict |
|---|---|---|---|---|---|---|
| `rr` / `a4a` | 1,2,2 | conforms | conforms | violation, 1 | violation, 1 | DOES NOT REFINE (the smallest witness) |
| `hash` / `a4a` | 2,1,2 | conforms | conforms | conforms | conforms | CONFORMS |
| `rr` / `a4a` | 2,1,2 | conforms | conforms | conforms | conforms | CONFORMS (vacuous, q = 1) |
| `hash-drop` / `a4a` | 2,1,2 | conforms | conforms | violation, 2 | violation, 2 | DOES NOT REFINE |
| `hash` / `a4b` | 2,1,2 | conforms | conforms | conforms | conforms | CONFORMS |
| `rr` / `a4b` | 2,1,2 | conforms | conforms | conforms | conforms | CONFORMS |
| `drop` / `a4b` | 2,1,2 | conforms | conforms | violation, 2 | violation, 2 | DOES NOT REFINE |
| `pair-swap-hash` / `a4a` | 2,1,2 | conforms | conforms | conforms | conforms | CONFORMS |
| `pair-swap-hash` / `a4b` | 2,1,2 | conforms | conforms | conforms | conforms | CONFORMS |
| `pair-swap-rr` / `a4b` | 2,1,2 | conforms | conforms | conforms | conforms | CONFORMS |
| `hash` / `a4a` | 2,2,2 | conforms | conforms | conforms | conforms | CONFORMS |
| `rr` / `a4a` | 2,2,2 | conforms | conforms | violation, 4 | violation, 4 | DOES NOT REFINE |
| `hash-drop` / `a4a` | 2,2,2 | conforms | conforms | violation, 6 | violation, 6 | DOES NOT REFINE |
| `hash` / `a4b` | 2,2,2 | conforms | conforms | conforms | conforms | CONFORMS |
| `rr` / `a4b` | 2,2,2 | conforms | conforms | conforms | conforms | CONFORMS |
| `drop` / `a4b` | 2,2,2 | conforms | conforms | violation, 6 | violation, 6 | DOES NOT REFINE |
| `pair-swap-hash` / `a4a` | 2,2,2 | conforms | conforms | conforms | conforms | CONFORMS |
| `pair-swap-hash` / `a4b` | 2,2,2 | conforms | conforms | conforms | conforms | CONFORMS |
| `pair-swap-rr` / `a4b` | 2,2,2 | conforms | conforms | conforms | conforms | CONFORMS |

("violation, U": U uncovered summary lines.) Rust `conf` at `(1, 2, 2)` for `rr`/`a4a`: 1 candidate
violation. Not done: sizes beyond `(2, 2, 2)` (the registry's `k 1..3 × q 1..2 × m 1..3`).
