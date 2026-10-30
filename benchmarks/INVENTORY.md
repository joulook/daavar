# Benchmark inventory (built and planned), 2026-10-28

One row per specification/implementation pair or variant, each with a proposed directory under
`plan/benchmarks/`. Compiled read-only from:

- TraceForge, private copy `/local/home/mkhoshechin/tmp/tf-bench/traceforge` (detached at `a1c4796`),
  `src/conformance/{grid.rs, bench.rs, demo.rs, paper_examples.rs, generator.rs}` and their tests.
  Line numbers below are for that checkout. `grid.rs` is abbreviated `g`, `bench.rs` `b`, `demo.rs` `d`.
- JMC, private copy `/local/home/mkhoshechin/tmp/jmc-bench` (HEAD `94dbfd25`, plus the working tree),
  `core/src/test/java/org/mpi_sws/jmc/mp/`.
- Plans: `plan/traceForge/{APPTAB-PLAN.md, DEMO-2PC*.md, DEMO-1PM.md, EVAL-PLAN-algorithms.md,
  ANSWERS-apptab.md, LB-VARIANTS.md, BENCHMARKS-FROM-RELATED-WORK.md, P5-DISCUSS.md, P5-OWNER-ITEMS.md,
  criteria/P5-APPS.md, log/dev/P5-APPS.expected.md}`, `popl-conf/tex/eval.tex`,
  `plan/benchmarks/RELATED-BENCHMARKS.tex`, `plan/simulation/BENCHMARKS.tex` (headings only).

Two things to know before reading the tables:

1. **No standalone Rust version exists for any pair.** `plan/benchmarks/` holds only `_tools/`,
   `related-work-papers/`, `BENCH-BRIEF.md` and `RELATED-BENCHMARKS.tex`. Every built pair exists only
   as a fixture registered in `grid.rs`, or as an in-tree `#[test]` in `bench.rs` or `demo.rs`. The
   "Rust" column therefore says where the fixture lives.
2. **Java versions exist for A2 and A3 only.** They are `programs/j4/A2Store.java` and
   `programs/j4/A3Lock.java`, one class per model, with the variant passed as the first argument
   (`A3Lock:<kind>,<k>,<q>`, `A2Store:<kind>,<k>,<q>[,<s>]`). The `h1/` ports are single Must
   programs from TraceForge's `tests/*.rs`, not specification/implementation pairs (§5).

Verdict vocabulary: **conforms**; **reports** means a value or order violation (an uncovered visible
word, (V) or (O)); **blocked-report** means a status violation (S): a visible thread ends `Blocked`
where every matching Spec thread ends `Done`. **control** marks a still-conforming mutation.

---

## 0. Registry naming scheme and communication models

### 0.1 Fixture names

Names are `/`-separated: `<group>/<model>/<impl-or-mutant>/<spec>/<knobs>`. Knob strings concatenate
letters and values with no separator.

| prefix | builder (file:line) | name pattern | knobs → function arguments |
|---|---|---|---|
| A1 | `apps_fixtures` g:3729, `apps_series_fixtures` g:5168, `apptab_build` g:5421 | `apps/a1/<v>/spec/n{n}r{r}` | `a1_impl(n, r, A1Coord::<v>)` g:3085 vs `a1_spec(r)` g:3098. `v` ∈ `correct, eager, early-abort, silent, control-reverse` |
| A2 | same | `apps/a2/<v>/spec/k{k}q{q}`, plus `…s{s}` for `sh` and `misroute` | `v` → `a2_pb(k,q,A2Pb::*)` g:3256, `a2_pb_br(k,q,A2Br::*)` g:3320 or `a2_sh(k,q,s,misroute)` g:3373, vs `a2_spec(k,q)` g:3221 |
| A3 | same | `apps/a3/<v>/spec/k{k}q{q}` | `a3_impl(k,q,A3Coord::<v>)` g:3509 vs `a3_spec(k,q)` g:3474 |
| A4 | same | `apps/a4/<impl>/<a4a\|a4b>/k{k}q{q}m{m}` | `a4_impl(k,q,m,A4Policy,A4Fault)` g:3685 vs `a4_spec(k,q,m,A4Spec)` g:3639. `impl` ∈ `hash, rr, drop, pair-swap-hash, pair-swap-rr`, plus the APPTAB names `hash-drop` and `rr` vs A4a |
| leader (A5) | `two_pc_fixtures` g:1077, `apptab_build` | `2pc/leader/<conf\|split\|silent>/n{n}` | `d::le_impl(n, buggy)` d:804 or `le_impl_silent(n)` d:810 vs `le_spec(n)` d:849 |
| 2PC coordinator | `two_pc_fixtures` | `2pc/coord/<conf\|eager>/n{n}` | `b::two_pc(n, eager)` b:170 vs `two_pc_spec()` b:191 |
| ring | `two_pc_fixtures` | `2pc/ring/<conf\|split>/n{n}` | `d::ring_impl(n, buggy)` d:1077 vs `le_spec(n)` |
| S1 | `naive_fixture` g:857, `naive_self_fixture` g:3920, `naive_e2_fixture` g:878 | `ex:naive/k{k}/enc{e}`, `synth/naive-self/k{k}enc{e}`, `ex:naive/e2/k{k}` | `naive_d(k, v, c_first, None)` g:232. Impl `v = 0`, Spec `v = 1`. `enc 1` gives `c_first = true` |
| S2–S6 | `synth_fixtures` g:4488 | `synth/share/m{m}c{c}`, `synth/share-ctl/m{m}c{c}`, `synth/commit/n{n}j{j}`, `synth/chain/d{d}`, `synth/reset/k{k}s{s}`, `synth/reset-ctl/k{k}s{s}`, `synth/reset-twin/k{k}s{s}`, `synth/width/w{w}` | see §2 |
| other | `paper_fixtures` g:895, `ndk_fixtures` g:1052, `mixed_fixtures` g:771, `corpus_fixtures` g:1228 | `ex:<name>`, `relay/…`, `R1`, `ndk3/bad_A`, `mixed/server/r{r}/annotated`, `corpus/<Mode>/<seed hex>` | see §3 |

`registry(k_naive, n_max, per_mode)` (g:1249) is the union of S1 (`ex:naive` encodings 1 and 2 and
`e2` at every `k`), the paper fixtures, `ndk`, the 2PC demos up to `n_max`, and the corpus. The
application and synthetic fixtures are separate builders:

- `apps_fixtures`: registry sizes A1 `n ≤ 4, r ≤ 3`; A2 `k, q ≤ 3`; A3 `k ≤ 3, q ≤ 2`;
  A4 `k ≤ 3, q ≤ 2, m ≤ 3`.
- `apps_series_fixtures`: the series points beyond those sizes.
- `apptab_fixtures` (g:5492): the 15 diagonal pairs at `s = 2..=8` (`APPTAB_MAX_SIZE`) wherever the
  registry lacks the name.

The predeclared grids `synth_grid` (g:4529) and `apps_grid` (g:4811) attach three labels to each
point:

- a **variant**: `conforming`, `violating`, `mutant:<v>` or `control:<v>`;
- a **line**, for example `a1/correct/r=1`, `share/c=m/2/family` or `reset/s=3/control`;
- a **twin**, the conforming fixture that a mutant row is compared with.

In the APPTAB rows (`ApptabPair`, g:5249), `family` ∈ `a1..a4, le` and `variant` is
`conforming | mutant:<label>`, with the A4 mutants as `mutant:rr-a4a` and `mutant:hash-drop-a4a`.
The A4 grid labels each variant as `<impl>-<spec>` (`hash-a4a`, `rr-a4b`, `pair-swap-hash-a4b`);
`a4_name` (g:4750) splits the label back into the fixture path, and `drop` always means A4b.

### 0.2 Communication model per family

| family | model | how |
|---|---|---|
| A1–A4 | p2p (`ConsType::FIFO`, seed 0, `apps_config` g:2950) | `typed_chan` FIFO channels created by `main` (A2–A4). A1 uses thread-addressed `send_msg` with `ThreadId`s, coordinator spawned first (F41) |
| A5 leader, ring, 2PC coordinator | p2p (`ConsType::FIFO`) | thread-addressed `send_msg`/`recv_msg_block`. Leader and ring fixtures carry the raised thread bound `CONF_THREAD_THRESHOLD` = 65 536 |
| S1 naive, S5 reset | asyn (`ConsType::Bag`) | `chan()` |
| S2 share, S3 commit, S4 chain, S6 width | p2p (`ConsType::FIFO`) | `fifo_chan()` |
| `ndk` | p2p, seed 0, 64 MiB stack | thread-addressed |
| paper fixtures | per fixture: Bag for `ex:cone`, `relay/apparatus`, `ex:nogate`, `ex:sched`, `ex:rebuild`, `traces/self`, `blocking/*`, `reset-pair`, `forward-pop`, `a27`, `R1`, `R2`; FIFO for `relay/paper*` and `ex:restart` | |
| mixed | FIFO, except `mixed/branch`, which is Bag | |
| corpus | the generated pair's own model | |
| Java A2/A3 | `CommunicationModel.P2P` | TraceForge's typed channels become type-test receives on one mailbox (per-sender, per-type FIFO) |

---

## 1. Built: application models (A1–A5)

All run under p2p. "Paper sizes" are what `eval.tex` §RQ4 prints: Tables `tab:eval:appc` and
`tab:eval:appv` use the diagonal size `s` and stop at the first 600 s timeout. The campaign series is
`apps_grid`. Unless the row says otherwise, Java = none and Rust = grid fixture only.

### A1: two-phase commit

Visible `p0, p1`. The Spec keeps two participants whatever `n` is; its coordinator reads every vote
and then chooses Commit or Abort freely by `nondet()`.

| proposed dir | what | spec | impl | params; paper sizes | expected verdict (reason) | Java | Rust | source doc |
|---|---|---|---|---|---|---|---|---|
| `a1-two-pc/correct` | real 2PC: commit iff all vote yes, same decision to all, over `r` rounds | `a1_spec(r)` g:3098 | `a1_impl(n,r,Correct)` g:3085, `a1_coordinator` g:3010 | `n` participants, `r` rounds. Registry `n 2..4 × r 1..3`, series adds (5,1), (6,1), (2,4). Paper `s = n = r` ∈ {2,3,4}; vanilla timeout at 3, Must alone at 4 | conforms (the Spec admits any agreed decision) | none (h1 `TwoPc` is TraceForge's plain `tests/two_pc.rs`, not this pair) | grid only | APPTAB-PLAN §1; criteria/P5-APPS; eval.tex appc |
| `a1-two-pc/eager` | decides as each vote arrives (`bench.rs` bug) | `a1_spec` | `a1_impl(…,Eager)` | paper `s` ∈ {2,3} (vanilla timeout at 3); grid `n 2..4 @ r=1`, `r 2..3 @ n=2` | reports, value and order: at (2,1), 5 of 8 graphs are uncovered (3 (O), 1 (O)+(V), 1 (V)); `p0` is answered before `p1`'s vote is read | none | grid only | P5-APPS catalogue; eval.tex appv |
| `a1-two-pc/silent` | the decision to `p1` is never sent in the last round | `a1_spec` | `a1_impl(…,Silent)` | paper `s` ∈ {2,3} | blocked-report: `p1` ends `Blocked` at its last decision receive in every graph | none | grid only | APPTAB-PLAN §1, §8 |
| `a1-two-pc/early-abort` | broadcasts Abort on the first `No`, then drains the round's votes | `a1_spec` | `a1_impl(…,EarlyAbort)` | campaign grid only (not in the paper tables) | reports (O): 4 of 8 graphs at (2,1) | none | grid only | P5-APPS catalogue |
| `a1-two-pc/control-reverse` | decisions sent in reverse participant order | `a1_spec` | `a1_impl(…,ControlReverse)` | campaign grid `n 2..4`, `r 2..3` | conforms, control (every vote is still read before any decision) | none | grid only | P5-APPS catalogue |
| `two-pc-coord/correct` | the original single-round 2PC (DEMO-2PC version 1); same protocol as A1 at `r = 1`, with separate types | `two_pc_spec()` b:191 | `two_pc(n,false)` b:170, `coordinator_correct` b:108 | `2pc/coord/conf/n{2,3,4}`, seeds recorded; DEMO tables `N = 2,3,4` (8/48/384 graphs) | conforms (agreement only) | none | `#[test]` in bench.rs + grid | DEMO-2PC-ALL v1, DEMO-1PM |
| `two-pc-coord/eager` | the eager coordinator (`coordinator_eager` b:139) | `two_pc_spec()` | `two_pc(n,true)` | `2pc/coord/eager/n{2..4}`; budget 10 000 | reports (loss of agreement; naive oracle: 40/132/144 uncovered words at N = 2/3/4) | none | `#[test]` + grid | DEMO-2PC-ALL v1 |

`two-pc-coord/*` and `a1-two-pc/*` at `r = 1` are the same protocol. The test
`c05_the_eager_row_reproduces_bench_rs` asserts that the eager rows agree. Either keep both
directories (two code bases, two provenances) or make `two-pc-coord` an alias of `a1-two-pc` at
`r = 1`. This needs your decision.

### A2: key-value store (primary-backup and sharded; A5 merged in)

Visible clients `c0..c{k-1}`. Client `i`'s operation `j` is a Put iff `i + j` is even, on key
`(⌊j/2⌋ + ⌊i/2⌋) mod 2` with value `10i + j` (`a2_op`, g:3154). The Spec is one invisible `store`
that serves requests in arrival order.

| proposed dir | what | spec | impl | params; paper sizes | expected verdict (reason) | Java | Rust | source doc |
|---|---|---|---|---|---|---|---|---|
| `a2-kv-store/pb` | primary forwards a put to the backup, waits for the ack, replies; gets served by the primary | `a2_spec(k,q)` g:3221 | `a2_pb(k,q,Correct)` g:3256 | `k` clients, `q` requests. Registry `k,q ≤ 3`, series (4,1), (5,1), (1,4), (1,5). Paper `s` ∈ {2,3,4}; vanilla 1680 graphs in 165 s at 3, timeout at 4 | conforms (the primary serialises) | `A2Store` kind `pb` | grid only | APPTAB-PLAN §1; eval.tex appc |
| `a2-kv-store/stale-get` | the primary drops every put from its map | `a2_spec` | `a2_pb(…,StaleGet)` | paper `s` ∈ {2,3,4}; at 3, 1680/1680 uncovered in 335 s | reports, (V), mixed (V\|O) at some graphs: a get returns `None` where the Spec returns `Val`; vacuous at (1,1) | `A2Store` `stale-get` | grid only | APPTAB-PLAN §8; eval.tex appv |
| `a2-kv-store/silent-put` | the first put the primary receives is never answered | `a2_spec` | `a2_pb(…,SilentPut)` | paper `s` ∈ {2,3,4} | blocked-report: exactly one client ends `Blocked` in every graph, at every size | `A2Store` `silent-put` | grid only | APPTAB-PLAN §8 |
| `a2-kv-store/pb-br` | gets go directly to the backup; the primary waits for the ack | `a2_spec` | `a2_pb_br(k,q,Correct)` g:3320 | campaign series `k 1..5 @ q=1`, `q 2..5 @ k=1` | conforms | `A2Store` `pb-br` | grid only | P5-APPS |
| `a2-kv-store/early-ack` | `pb-br` with the ack sent before the commit | `a2_spec` | `a2_pb_br(…,EarlyAck)` | grid (2,1), (3,1), (1,2), (1,3) | reports: (O) at (2,1), (V) at (1,2); some graphs are uncovered only as candidates | `A2Store` `early-ack` | grid only | P5-APPS catalogue |
| `a2-kv-store/control-early-ack-primary` | `pb` with early ack and primary-served gets | `a2_spec` | `a2_pb(…,EarlyAck)` | series as `pb` | conforms, control (unobservable: the primary still serialises) | `A2Store` `control-early-ack-primary` | grid only | P5-APPS |
| `a2-kv-store/control-early-ack-fwd` | early ack on `pb-br` with gets forwarded through the primary | `a2_spec` | `a2_pb_br(…,ControlForwardedEarlyAck)` | series as `pb-br` | conforms, control (the backup reads in the primary's order) | `A2Store` `control-early-ack-fwd` | grid only | P5-APPS |
| `a2-kv-store/sh` | router forwards to shard `key mod s`; shards reply directly (the former A5) | `a2_spec` | `a2_sh(k,q,s,false)` g:3373 | `s ∈ {1,2}` (registry), `s = 2` in the series; twin line (2,2,2), (3,2,2) | conforms (correct routing is serialisable) | `A2Store` `sh` | grid only | P5-DISCUSS D1; P5-APPS |
| `a2-kv-store/misroute` | `sh`, but a get goes to shard `(key+1) mod s` | `a2_spec` | `a2_sh(…,true)` | `s = 2`; grid `k 1..3 @ q=2`, (1,3) | reports (V): the get reads `None` | `A2Store` `misroute` | grid only | P5-APPS catalogue |

### A3: lock service

Visible clients `c0..c{k-1}`, each doing `q` rounds of `Acq`, `Grant`, `Rel`. The Spec is one
invisible lock with an acquire channel and a release channel.

| proposed dir | what | spec | impl | params; paper sizes | expected verdict (reason) | Java | Rust | source doc |
|---|---|---|---|---|---|---|---|---|
| `a3-lock/fifo` | coordinator with one mailbox and a FIFO waiter queue | `a3_spec(k,q)` g:3474 | `a3_impl(k,q,Fifo)` g:3509 | registry `k 2..3 × q 1..2`; series (4,1), (5,1), (2,3). Paper `s` ∈ {2,3}; at 3, 330 252 graphs, the check completes only with `c_W = ∞` (582 s) | conforms (`holder.rel < next.grant` through the coordinator's program order) | `A3Lock` kind `fifo` | grid only | APPTAB-PLAN §1; eval.tex |
| `a3-lock/double-grant` | `Acq` granted at once even when the lock is held | `a3_spec` | `a3_impl(…,DoubleGrant)` | paper `s` ∈ {2,3}; Must alone also times out at 3 | reports (O): Impl 6, 4 uncovered at (2,1) | `A3Lock` `double-grant` | grid only | P5-APPS; eval.tex appv |
| `a3-lock/never-grant` | `c_{k-1}` is never granted from the queue | `a3_spec` | `a3_impl(…,NeverGrant)` | paper `s` ∈ {2,3} | blocked-report, existential: 8 of 21 graphs at k2q2, wherever `c_{k-1}`'s `Acq` is enqueued | `A3Lock` `never-grant` | grid only | APPTAB-PLAN §8 |
| `a3-lock/wrong-round` | grants to `c_{k-1}` carry round `j+1` | `a3_spec` | `a3_impl(…,WrongRound)` | campaign grid only | reports (V) | `A3Lock` `wrong-round` | grid only | P5-APPS catalogue |
| `a3-lock/control-lifo` | LIFO waiter stack | `a3_spec` | `a3_impl(…,Lifo)` | series as `fifo` | conforms, control; vacuous at `k = 2` (at most one waiter), non-vacuous at `k ≥ 3` | `A3Lock` `control-lifo` | grid only | P5-APPS.expected |

### A4: load balancer

Visible clients `c0..c{k-1}`, each sending `q` requests to an invisible balancer, which forwards them
to `m` invisible servers that reply directly. There are two Specs:

- **A4a (affinity):** one server per client, chosen by `nondet` on the client's first request.
- **A4b (relay):** a server chosen by `nondet` for every request.

The directory name carries both sides. Registry `k 1..3 × q 1..2 × m 1..3`; series adds (4,1,2),
(2,3,2), (2,1,4).

| proposed dir | what | spec | impl | params; paper sizes | expected verdict (reason) | Java | Rust | source doc |
|---|---|---|---|---|---|---|---|---|
| `a4-load-balancer/hash-vs-a4a` | client `i` goes to server `i mod m` | `a4_spec(k,q,m,A4a)` g:3639 | `a4_impl(k,q,m,Hash,None)` g:3685 | paper `s = k = q = m` ∈ {2,3}; timeout at 3 | conforms by construction (D3) | none | grid only | APPTAB-PLAN §1; eval.tex appc |
| `a4-load-balancer/rr-vs-a4a` | round-robin: request `p` goes to server `p mod m` | A4a | `a4_impl(…,RoundRobin,None)` | paper `s` ∈ {2,3}; grid (1..3,2,2), (1,2,3) | reports (V) iff some client has `q ≥ 2` and `m ≥ 2` (its consecutive requests land on different servers); vacuous otherwise | none | grid only | P5-DISCUSS D3; APPTAB-PLAN §8 |
| `a4-load-balancer/hash-drop-vs-a4a` | `hash` with `c_{k-1}`'s last request dropped | A4a | `a4_impl(…,Hash,Drop)` | paper `s` ∈ {2,3}; APPTAB only (`apptab_build`) | blocked-report: `c_{k-1}` ends `Blocked` at every size | none | grid (apptab) only | APPTAB-PLAN §1, §8 |
| `a4-load-balancer/hash-vs-a4b` | hash against the relay Spec | `a4_spec(…,A4b)` | `a4_impl(…,Hash,None)` | campaign series; corner `k3q2m3` | conforms | none | grid only | P5-APPS |
| `a4-load-balancer/rr-vs-a4b` | round-robin against relay | A4b | `a4_impl(…,RoundRobin,None)` | campaign series | conforms | none | grid only | P5-APPS |
| `a4-load-balancer/drop-vs-a4b` | round-robin with `c_{k-1}`'s last request dropped (registry `drop`) | A4b | `a4_impl(…,RoundRobin,Drop)` | grid (1..3,1,2), (2,2,2), (2,1,3); smallest (1,1,1) | blocked-report | none | grid only | P5-APPS catalogue |
| `a4-load-balancer/pair-swap-hash-vs-a4a` | `k = 2`: the balancer forwards each pair of `j`-th requests in reverse arrival order | A4a | `a4_impl(…,Hash,PairSwap)` | `k = 2`; grid (2,1..2,2), (2,1,3) | conforms, control | none | grid only | P5-APPS catalogue |
| `a4-load-balancer/pair-swap-hash-vs-a4b` | same, against A4b | A4b | `a4_impl(…,Hash,PairSwap)` | `k = 2` | conforms, control | none | grid only | P5-APPS |
| `a4-load-balancer/pair-swap-rr-vs-a4b` | round-robin pair-swap against A4b; the pair against A4a is never built, because it violates | A4b | `a4_impl(…,RoundRobin,PairSwap)` | `k = 2` | conforms, control | none | grid only | P5-APPS (round 02 D4) |

### A5: 2PC with leader election (all-to-all), and the ring version

Visible `p0..p{n-1}`, running the same participant code on both sides (`le_participant`, d:666). The
Spec's oracle picks a leader freely (`le_oracle`, d:789). Raised thread bound.

| proposed dir | what | spec | impl | params; paper sizes | expected verdict (reason) | Java | Rust | source doc |
|---|---|---|---|---|---|---|---|---|
| `a5-leader-2pc/conf` | `n` invisible electors run an all-to-all election; highest index wins | `le_spec(n)` d:849 | `le_impl(n,false)` d:804 | `n`; registry `n ≤ 3`, APPTAB to 8. Paper `n` ∈ {2,3,4,5}: vanilla 2.45 s at 3, timeout at 4; Must alone 1 839 744 graphs at 4, timeout at 5 | conforms (a real election refines a free choice; hand argument, not the 132-fixture derivation) | none | `#[test]` in demo.rs + grid | DEMO-2PC-ALL v2; APPTAB-PLAN §1 |
| `a5-leader-2pc/split` | the elector omits its own index (split brain) | `le_spec` | `le_impl(n,true)` | registry `n ≤ 4`, budget 100 000; paper `n` ∈ {2,3,4} | reports, trace: every elector `e_i` with `i < n−1` elects `n−1` and `e_{n−1}` elects `n−2`, so nobody receives `BeLeader` and every graph is uncovered (9/9, 882/882) | none | `#[test]` + grid | APPTAB-PLAN §8 |
| `a5-leader-2pc/silent` | the winner's elector never announces `BeLeader` (β) | `le_spec` | `le_impl_silent(n)` d:810 | APPTAB only; paper `n` ∈ {2,3,4} | blocked-report: every participant ends `Blocked` (each word a proper prefix of a Spec word); expected counts 3 and 196 | none | grid (apptab) only | APPTAB-PLAN §8; ANSWERS-apptab §1 |
| `a5-ring-2pc/conf` | ring election (token carries the running maximum); scales (4/24/192 graphs) | `le_spec(n)` | `ring_impl(n,false)` d:1077, `ring_elector` d:1013 | `2pc/ring/conf/n{2..4}`, seeds recorded; DEMO `n` = 2,3,4 | conforms | none | `#[test]` + grid | DEMO-2PC-ALL v3, DEMO-2PC-RING |
| `a5-ring-2pc/split` | the ring elector decides alone from the token value | `le_spec` | `ring_impl(n,true)` | `2pc/ring/split/n{2..4}` | reports (both participants receive `BeFollower`; 14/14 uncovered at `n = 2`) | none | `#[test]` + grid | DEMO-2PC-ALL v3 |

---

## 2. Built: synthetic families (S1–S6)

Grids from `g:4404–4410` (`SYNTH_S*`), frozen before measurement. What each dial isolates is quoted
from eval.tex §Benchmark Families. Java = none and Rust = grid only for every row.

| proposed dir | family | what (dial) | spec | impl | params; paper sizes | expected verdict (reason) | source doc |
|---|---|---|---|---|---|---|---|
| `s1-naive/violating` | S1 | `ex:naive`: `k` senders `b_i` to `c`; Impl `c` sends 0, Spec sends 1; `k!` graphs on each side. Isolates failure certification at worst case | `naive_d(k,1,enc,None)` g:232 | `naive_d(k,0,enc,None)` | `ex:naive/k{k}/enc{1,2}`, `k` 2..7 (both spawn orders; the paper shows one) | reports (every Impl graph is uncovered; a sweep costs `(k!)^2`) | eval.tex; EVAL-PLAN §2.1 |
| `s1-naive/self` | S1 | conforming twin: Spec against itself | `naive_d(k,1,enc,None)` | same | `synth/naive-self/k{k}enc{e}`, `k` 2..7 | conforms | eval.tex |
| `s1-naive/e2` | S1 (validation) | encoding E2 (`b1, c, b2..bk, a`), the P4-GATED revisit fixture | `naive_e2(k,1)` g:271 | `naive_e2(k,0)` | `ex:naive/e2/k{k}`, registry range (2..4 in the flat table) | reports | grid.rs doc; P4-GATED |
| `s2-share/family` | S2 | `m` invisible relay modules, `c` of which make the non-blocking ⊥ choice. Dial `c` varies sharing (`2^c` Impl graphs per witness, minimum cover 1) at near-constant size | `share_spec(m)` g:3974 | `share_impl(m,c)` g:3946 | `m` ∈ {2,4,8,12,16,24}, `c` ∈ {0, m/2, m} | conforms | eval.tex; EVAL-PLAN |
| `s2-share/control` | S2 | matched control: the choice is made visible in the sent value (`10i + b`); self-paired, sharing 1 | `share_ctl(m,c)` g:3993 | same | same grid | conforms | eval.tex |
| `s3-commit` | S3 | `n` hidden bits; the Spec commits `j` of them before the common visible prefix, the Impl commits each at its dependent send. A pure alignment dial; behaviours identical for every `j` | `commit_prog(n,Some(j))` g:4053 | `commit_prog(n,None)` | `n` ∈ {2,4,8,12,16,20}, `j` ∈ {0, n/2, n} | conforms | eval.tex |
| `s4-chain` | S4 | visible order chain through `d` invisible relays; pure size scaling in one signature class | `chain_prog(d,false)` g:4103 | `chain_prog(d,true)` | `d` ∈ {2,8,32,128,512,1024,2048} | conforms | eval.tex |
| `s5-reset/family` | S5 | `k` copies of the N4 pair (spawn order `a, c, b`); uncovered branches reached across backward revisits that re-enter certified states; the Spec is padded with `s!` graphs | `reset_prog(k,true,Acb,Some(s))` g:4192 | `reset_prog(k,false,Acb,None)` | `k` 1..10, `s` ∈ {1,3,4}; validation (1,2) | reports: `2^k − 1` of `2^k` uncovered; `2^k − 1` failing gate sweeps | eval.tex RQ2 |
| `s5-reset/control` | S5 | same visible behaviours with spawn order `a, b, c` (forward pops instead of backward revisits) | `reset_prog(k,true,Abc,Some(s))` | `reset_prog(k,false,Abc,None)` | same | reports (also violating); `k` failing gate sweeps | eval.tex |
| `s5-reset/twin` | S5 | conforming twin: the padded Spec against itself | `reset_prog(k,true,Acb,Some(s))` | same | same | conforms | eval.tex |
| `s6-width` | S6 | one `nondet` of width `w` sent to a visible receiver; pure value branching | `width_prog(w)` g:4301 | same | `w` ∈ {2,8,32,128,512,1024,2048} | conforms | eval.tex |

---

## 3. Built: validation, regression and demonstration pairs

These are not in the paper's benchmark tables, but they are spec/impl pairs that the registry runs,
so each still gets a row.

| proposed dir | group | what | spec | impl | params | expected verdict | Java | Rust | source |
|---|---|---|---|---|---|---|---|---|---|
| `paper/ex-cone` | paper | `ex:cone` | `ex_cone_spec` g:318 | `ex_cone_impl` g:328 | Bag, {a,c} | reports | none | grid | grid_tests `expected_paper_class` |
| `paper/relay-apparatus` | paper | cone Impl vs relay Spec | `relay_spec` g:339 | `ex_cone_impl` | Bag | reports | none | grid | same |
| `paper/relay` | paper | `ex:relay` | `paper_examples::p1_direct` :81 | `p2_relay` :68 | FIFO, {main,c} | conforms | none | grid | same |
| `paper/relay-rev` | paper | `ex:relay`, reversed orientation | `p2_relay` | `p1_direct` | FIFO | conforms | none | grid | same |
| `paper/ex-nogate` | paper | `ex:nogate`, self | `nogate()` g:393 | same | Bag | conforms | none | grid | same |
| `paper/ex-sched` | paper | `ex:sched`, self | `ex_sched()` g:302 | same | Bag | conforms | none | grid | same |
| `paper/ex-restart` | paper | `ex:restart` | `restart(true)` g:423 | `restart(false)` | FIFO | conforms | none | grid | same |
| `paper/ex-rebuild` | paper | `ex:traces`, Plain vs OnlyFromA | `traces(OnlyFromA)` g:460 | `traces(Plain)` | Bag | reports | none | grid | same |
| `paper/traces-self` | paper | `ex:traces`, self | `traces(Plain)` | same | Bag | conforms | none | grid | same |
| `paper/blocking-apparatus` | paper | `c` receives twice vs once | `direct_of(1)` g:364 | `blocking_impl` g:354 | Bag | reports | none | grid | same |
| `paper/blocking-cfirst` | paper | the same, `main/c` form | `blocking_mc(false)` g:376 | `blocking_mc(true)` | Bag | reports | none | grid | same |
| `paper/reset-pair` | paper | certificate-reset pair `abc` | `abc("acb",Some(2))` g:492 | `abc("acb",None)` | Bag | reports | none | grid | same |
| `paper/forward-pop` | paper | forward-pop pair | `forward_pair(true)` g:525 | `forward_pair(false)` | Bag | reports | none | grid | same |
| `paper/a27` | paper | A27 witness pair | `a27(false)` g:543 | `a27(true)` | Bag | reports | none | grid | same |
| `regression/r1` | regression | visible `assert(false)` vs skip | `r1(false)` g:561 | `r1(true)` | Bag | reports | none | grid | same |
| `regression/r2` | regression | `growing`: assert plus an unread send | `r2(false)` g:574 | `r2(true)` | Bag | reports | none | grid | same |
| `ndk/conf` | ndk | `n` senders of `{1,2}` to `c`, self | `b::ndk(ndk_conf(n))` b:533 | same | `n` ∈ {2,3}, FIFO, seed 0; minimum budgets 33 and 981 | conforms | none | `#[test]` + grid | bench.rs F63/F69/F70 |
| `ndk/bad-a` | ndk | `ndk3`, the Spec pins `s1, s2` to 1 | `ndk(ndk3_bad_a())` b:564 | `ndk(ndk_conf(3))` | `n = 3` | reports (exhausts at the default budget) | none | `#[test]` + grid | bench.rs |
| `mixed/server-annotated` | mixed | `ex:server` with `rounds` storage round-trips, annotated | `server_spec` g:638 | `server_impl(r,true,2)` g:604 | `r` 1..3, FIFO, {k,s} | conforms | none | grid | mixed_tests `c10_expected` |
| `mixed/server-unannotated` | mixed | same, unannotated | `server_spec` | `server_impl(r,false,2)` | `r` 1..3 | reports | none | grid | same |
| `mixed/server-reply3` | mixed | the server replies 3 | `server_spec` | `server_impl(r,true,3)` | `r` 1..3 | reports | none | grid | same |
| `mixed/serverblock` | mixed | the server blocks | `server_spec` | `serverblock_impl` g:656 | FIFO | reports | none | grid | same |
| `mixed/branch` | mixed | branch-dependent, self | `branch_dependent` g:680 | same | Bag | conforms | none | grid | same |
| `mixed/cut` | mixed | criterion 7′ pair | `cut_pair(false)` g:699 | `cut_pair(true)` | FIFO, {v} | reports | none | grid | same |
| `mixed/corpus-invisible-refactor` | mixed | storage round-trip through a relay vs a direct send | `corpus_mixed(None).1` g:720 | `.0` | FIFO | conforms | none | grid | same |
| `mixed/corpus-visible-mutation` | mixed | same, the Spec sends 2 | `corpus_mixed(Some(2)).1` | `.0` | FIFO | reports | none | grid | same |
| `demo/c01-identical` … `demo/c13-visible-logging` (13 dirs) | demo | the 13 pairs in the `demo.rs` header table (d:16–30): identical, invisible relay, wrong value, Spec blocks, Impl blocks, Spec or Impl orders, visible or invisible assert, Spec allows more, Impl does more, extra invisible work, visible logging | per test: `direct_1/2`, `via_relay`, `two_free_sends`, `two_ordered_sends` d:224–270 and inline | per test | default (`conf`) model | c1, c2, c7, c9, c10, c12 conform; c3, c4, c5, c6, c8, c11, c13 report | none | `#[test]` in demo.rs | demo.rs module doc |
| `corpus/<mode>` (6 dirs) | generator | random pairs per `generator::Mode` (g. `generator.rs:245`): Identity, InvisibleRefactor, DecoupleSpec (conform), VisibleMutation, DecoupleImpl, SpecBlocks (report) | `generator::pair(mode, seed)` :440 | same | `corpus(7, per_mode)`, 10 per mode (full), 3 (subset) | per mode as listed | none | grid | generator.rs |

---

## 4. Counts of built pairs

| part | directories |
|---|---|
| A1 (5) + 2PC coordinator (2) | 7 |
| A2 | 9 |
| A3 | 5 |
| A4 | 9 |
| A5 leader (3) + ring (2) | 5 |
| synthetic S1–S6 | 11 |
| paper and regression | 16 |
| `ndk` | 2 |
| mixed | 8 |
| demo c1–c13 | 13 |
| generator corpus modes | 6 |
| **total built** | **91** |

The benchmark subset (application plus synthetic, §1–§2) is **46**. The paper's application tables
use 15 of those pairs (three per model). The other application variants run only in the campaign
series (`apps_grid`).

---

## 5. Java ports that are not pairs (h1)

`core/src/test/java/org/mpi_sws/jmc/mp/h1/` ports TraceForge's own `tests/*.rs` programs as single
Must programs, for the H1 count-agreement gate (`H1CountsTest`: TraceForge's complete and blocked
counts). None is a specification/implementation pair, so none appears in the tables above. Each
could become a single-program benchmark. One row per file:

`Actors` (tests/bank.rs), `ActorChannels` (mpsctest.rs: `bounded_channel,{1..4}`, `locked_bank`),
`ChannelSemantics`, `Estimation`, `ExecutionOrder`, `JobQueue`, `Litmus`, `LitmusChannels`,
`Micro` (microbenchmarks.rs), `Minibus`, `RuPaxos` (rupaxos.rs, 1299 complete + 201 blocked),
`SemanticsComparison`, `SmallTests` (coverage.rs), `Tags`, `VecTags`, `TwoPc` (two_pc.rs and
two_pc_demo.rs, `N` participants), `TwoPcActor` (two_pc_actor.rs), `WorkTerminate` (book.rs),
`H1Repro` (repro cases f2–f34).

The J4 case studies `programs/j4/A2Store.java` and `A3Lock.java` are the Java sides recorded in §1.
`J4LauncherTest` checks them under `must` against counts; for example, `A3Lock:fifo,3,1` gives
30 complete graphs and 12 graphs with a distinct summary.

---

## 6. Planned but not built

None of these has code in TraceForge or JMC; a grep of `src/` for their names returns nothing. Java
and Rust are none throughout.

| proposed dir | family | what | spec | impl | params / sizes planned | expected verdict (reason) | status | source doc |
|---|---|---|---|---|---|---|---|---|
| `b1-paxos/conf` | B1 | single-decree Paxos: 2 proposers (ballots 1, 2), 3 acceptors, 2 clients, 2 learners | consensus oracle (decides a proposed value or nothing; the same value to every learner) | proposers and acceptors | acceptors' loop bound 4; NoOrder for acceptor/proposer channels, FIFO for clients/learners | conforms (reviewed by Opus 2026-10-26) | first in the value order | RELATED-BENCHMARKS §B1; BENCHMARKS-FROM-RELATED-WORK §1 |
| `b1-paxos/ignore-best` | B1 | the proposer ignores the highest-ballot promised value | oracle | mutant | same | reports: two learners see different values | planned | §B1 Faults 1 |
| `b1-paxos/accept-below-promise` | B1 | an acceptor accepts below its promised ballot | oracle | mutant | same | reports: disagreement | planned | §B1 Faults 2 |
| `b1-paxos/silent-learner` | B1 | the winner tells one learner only | oracle | mutant | same | blocked-report | planned | §B1 Faults 3 |
| `b2-raft/conf` | B2 | one term of Raft, 3 replicas (`r0`, `r1` candidates), 2 clients with front-ends, 1 reader | atomic log that may stop serving | Raft replicas | 1 command per client; p2p | conforms | planned | §B2 |
| `b2-raft/commit-without-quorum` | B2 | the leader replies on its own append | log | mutant | same | conforms, control (unobservable without failures) | planned | §B2 Faults 1 |
| `b2-raft/out-of-order-commit` | B2 | index 2 committed before index 1 | log | mutant | same | blocked-report (`c0` blocks while `c1` gets `Committed(2)`) | planned | §B2 Faults 2 |
| `b2-raft/follower-serves-read` | B2 | a follower answers `Read` from its own log | log | mutant | same | reports (value or missing order) | planned | §B2 Faults 3 |
| `b2-raft/conflicting-suffix` | B2 (alt.) | a follower keeps a conflicting suffix after a term change; needs terms {1,2}, superseded by §B2's one-term design | log | mutant | terms {1,2}, ≤ 2 entries | reports | draft only | BENCHMARKS-FROM-RELATED-WORK §2 |
| `b3-sharded-kv-migration/conf` | B3 | 2 shards, 2 clients with proxies, a controller moves key 0 | one store per key | shards + proxies + controller | `Q = 3`, A2's script; p2p | conforms | planned | §B3 |
| `b3-sharded-kv-migration/stale-after-move` | B3 | `s0` keeps serving key 0 after the move | per-key stores | mutant | same | reports (V) | planned | §B3 Faults 1 |
| `b3-sharded-kv-migration/lost-transfer` | B3 | `s1` takes ownership without waiting for the transfer | per-key stores | mutant | same | reports (V) | planned | §B3 Faults 2 |
| `b3-sharded-kv-migration/double-apply` | B3 | a forwarded put is applied and acked by both shards | per-key stores | mutant | same | reports (V at the next reply) | planned | §B3 Faults 3 |
| `b4-causal-kv/conf` | B4 | 2 replicas, 2 clients, 3 operations each, dependency-checked remote updates | one causal server per replica | replicas | update channels NoOrder; client channels FIFO | conforms | planned (last) | §B4 |
| `b4-causal-kv/apply-eagerly` | B4 | applies an update without checking its dependencies | causal servers | mutant | same | reports (V): `y = 2` then `x = None` | planned | §B4 Faults 1 |
| `b4-causal-kv/lost-update-with-clock` | B4 | advances the clock without writing the value | causal servers | mutant | same | reports (V) | planned | §B4 Faults 2 |
| `b4-causal-kv/drop-without-clock` | B4 | drops the update without advancing the clock | causal servers | mutant | same | conforms, control (the Spec may delay forever) | planned | §B4 Faults 2 note |
| `b5-memcached/conf` | B5 | one worker per connection plus a storage thread; Get, Set, Delete | one map (as `a2_spec` plus Delete) | workers + storage | 2 clients, `Q = 3` on key 0; p2p | conforms | planned | §B5 |
| `b5-memcached/cached-get` | B5 | the worker answers a Get from its connection's last Set | map | mutant | same | reports | planned | §B5 Faults 1 |
| `b5-memcached/delete-ack-early` | B5 | the worker replies `Deleted` without asking the storage | map | mutant | same | reports | planned | §B5 Faults 2 |
| `b6-ring-election/conf` | B6 | Chang–Roberts ring with visible observers | oracle sending `Leader(max id)` | ring nodes | `n` ∈ {3,4,5}; p2p | conforms | planned (second) | §B6 |
| `b6-ring-election/forward-smaller` | B6 | the max-holder forwards a smaller id | oracle | mutant | same | reports (V); a control if placed at another node | planned | §B6 Faults 1 |
| `b6-ring-election/drop-announcement` | B6 | a node (not the leader's predecessor) drops `Leader` | oracle | mutant | same | blocked-report; a control at the predecessor | planned | §B6 Faults 2 |
| `b6-ring-election/announce-twice` | B6 | the leader forwards its `Leader` again | oracle | mutant | same | conforms, control at this size | planned | §B6 Faults 3 |
| `b6-ring-election/tie` | B6 | growth variant with two equal ids | oracle | variant | `n = 6` growth | not stated | growth note only | §B6 Parameters |
| `causal-broadcast/conf` | (draft 3rd) | endpoints broadcast and deliver through dissemination workers, run under asyn | causal-delivery oracle | workers | not sized | conforms | draft, not an order | BENCHMARKS-FROM-RELATED-WORK §3 |
| `causal-broadcast/premature-delivery` | same | | oracle | mutant | | reports | draft | same |
| `causal-broadcast/duplicate-delivery` | same | | oracle | mutant | | reports | draft | same |
| `a4-load-balancer/l1-least-loaded-vs-a4b` | A4 L1 | polls every server, sends to the least loaded (ties by `nondet`); servers queue | A4b | new balancer + queueing servers | `k = q = m = s` from 2; `apps/a4/ll/a4b/k{s}q{s}m{s}` | conforms | not released (owner, 2026-10-15: "LB-VARIANTS not released") | LB-VARIANTS §1 |
| `a4-load-balancer/l2-capacity-vs-a4c` | A4 L2 | bounded capacity `c`; dispatch waits for space | new **A4c** capacity oracle | balancer that re-polls | `c = 1` at `s = 2`, `c = s − 1` above; `apps/a4/cap/a4c/…c{c}` | conforms | not released | LB-VARIANTS §2 |
| `a4-load-balancer/l2-stale-vs-a4c` | A4 L2-stale | two balancers dispatch on a stale poll; a full server sends `Reject` | A4c with `b` dispatchers | 2 balancers | first at `k = 2, b = 2, m = 1, c = 1, q = 1`; `apps/a4/cap-stale/a4c/b2…` | reports (a visible `Reject`) | not released | LB-VARIANTS §3 |
| `s7-hstable/random` | S7 | Hitting Set instance with `n` candidates and targets, `d = ⌈log² n⌉` (pilot `d = 4`); conforming and violating chosen by precheck | Spec_H | Impl_H | random density 1/2 | both variants | `pendingdata` in eval.tex | EVAL-PLAN §7.1 |
| `s7-hstable/nested` | S7 | nested (chain) targets, an easy control | Spec_H | Impl_H | | conforms | pending | same |
| `s8-grid` | S8 | `ex:grid` scaled: choices in opposite index orders, identical visible emission order | chooser | chooser (reversed) | `k ≥ 2` | conforms (`2^k` signatures) | pending | EVAL-PLAN §7.1; eval.tex |
| `s9-antichain/violating` | S9 | `ex:naive` (Impl_k, Spec_k), instrumented for width (`w = 0`) | `naive_d(k,1,…)` | `naive_d(k,0,…)` | capped where `k!` exceeds the budget | reports | pending (programs exist; the instrumentation does not) | same |
| `s9-antichain/identity` | S9 | (Spec_k, Spec_k); online basis width `k!` | Spec_k | Spec_k | same | conforms | pending | same |
| `s10-relaymin/min-first` | S10 | optional-relay self-pair under the minimum-first policy | relay pair | same | `m` | conforms (expected online width 1) | pending; needs a selector surface | same |
| `s10-relaymin/reversed` | S10 | same, with the source order reversed | | | `m` | conforms (width measured) | pending | same |

**Planned count: 38 rows.** By group: B1–B6, 25 (4 + 4 + 4 + 4 + 3 + 6); the Raft draft
alternative, 1; causal broadcast, 3; A4 L-variants, 3; S7–S10, 6.

---

## 7. Dropped, merged and changed variants (history)

| item | what happened | why | source |
|---|---|---|---|
| A5 (distributed storage) | merged into A2 as the `sh` Impl (and `misroute`) | one Spec, one client interface, two Impls makes the "same Spec, different distribution" point and halves the fault-catalogue work; owner decision D1, 2026-10-08 | P5-DISCUSS D1, P5-OWNER-ITEMS |
| A4 least-loaded policy | omitted from A4 | load is not observable without extra protocol (m4); reappears as LB-VARIANTS L1/L2, not released | criteria/P5-APPS; LB-VARIANTS §0 |
| A4 `rr` vs A4a | turned from a conforming series into a violating pair | by construction it violates iff `q ≥ 2` and `m ≥ 2` (D3) | P5-DISCUSS D3 |
| A4 pair-swap with `rr` vs A4a | not built | it violates at `q = 2`, so it cannot be a control (round 02 D4) | P5-APPS catalogue; g:3844 |
| A4 conforming pair in the paper tables | `hash` vs **A4a** replaced the campaign's corner `hash` vs A4b | all three pairs of a benchmark must share one Spec | APPTAB-PLAN §1 |
| A4 blocked mutant | new `hash-drop` vs A4a instead of the registry's `drop` (`rr` vs A4b) | `drop` would change the Spec | APPTAB-PLAN §1 |
| Leader blocked mutant (α) | rejected in favour of (β), `le_impl_silent` | (α) (the leader never sends `p_{n−1}` its decision) needs a change to the visible `le_participant`, so its violation would not be of the blocked class alone | APPTAB-PLAN §8; ANSWERS-apptab §1 |
| Leader participant with `nondet` votes and an unbounded loop | replaced by a fixed vote and a bounded `for` | `n = 2` did not finish in ten minutes with `nondet` votes; the listing in DEMO-2PC-LEADER is stale | d:666 comments; DEMO-2PC-LEADER banner |
| Leader all-to-all at `n ≥ 4` | ring version added (DEMO v3) | all-to-all has 1 839 744 Impl executions at `n = 4`; the ring has 192 | DEMO-2PC-ALL "Why the ring scales" |
| DEMO-2PC, -LEADER, -RING | superseded by DEMO-2PC-ALL | consolidation; the older files are kept as records | each file's banner |
| DEMO-1PM storage example | "do not promise"; later built as A2 | it did not exist at the 1 PM demo | DEMO-1PM §4 |
| P3-F61 `ndk3_bad` | lost; `ndk3_bad_a` is the closest of ten candidates | no artefact recorded the original | b:559–566 |
| Application tables: mutants beyond two per model | `early-abort`, `wrong-round`, `early-ack`, `misroute` and the controls appear only in the campaign series, not in the paper tables | owner, 2026-10-13: three pairs per benchmark (conforming, value/order, blocked) | APPTAB-PLAN §0 D-T3 |
| S1 two encodings | both run; the paper shows one | they behave alike | eval.tex |
| B4 `wrong-origin-clock` | dropped | with two replicas it only delays updates, which the Spec allows | RELATED-BENCHMARKS §B4 |
| B5 `reorder-connection` | dropped | the clients are closed-loop, so a worker never holds two commands | RELATED-BENCHMARKS §B5 |
| B1–B6, first version | rewritten | it broke three of the four rules (review, Opus, 2026-10-26) | RELATED-BENCHMARKS "Four rules" |
| Generator `UnionCovered` mode | declared but not constructed | the only mode that could produce a false alarm; no construction exists yet | generator.rs:285–305 |
| Not to add (related-work draft) | bank transfer, reliable broadcast with crashes, PBFT, shared-memory structures | they add little beyond 2PC, need a failure model or atomics, or are too large | BENCHMARKS-FROM-RELATED-WORK "Not to add" |

---

## 8. Open points

- `two-pc-coord/*` vs `a1-two-pc/*` at `r = 1`: separate directories or an alias (§1, A1).
- Directory granularity for parameterised fixtures: this inventory gives one directory per variant,
  with size knobs as CLI arguments. Examples: `s1-naive/violating` covers both encodings;
  `mixed/server-*` covers rounds 1..3. Splitting encodings or rounds into their own directories would
  raise the count.
- The demo `c1..c13` pairs and the generator corpus are counted as built pairs. If only benchmarks
  count, the built total is 46, not 91.
- No expected verdict is stated in the plans for B6's `tie` variant.
