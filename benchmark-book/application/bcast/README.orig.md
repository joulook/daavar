# Broadcast consensus (CIVL): decide the max of n values

**Family:** related-work  **Source:** CIVL (Boogie) `samples/BroadcastConsensus.bpl`, under
`related-work-papers/sources/boogie/Test/civl/samples/` (CANDIDATES.md N3)  **Expected verdict:**
`correct`: **CONFORMS** (every node decides the max of all n values, after all n inputs, which is
what the oracle announces); `decide-early`: **DOES NOT REFINE** (a node decides after n − 1
values and can miss the max, so its decision differs from the oracle's and does not depend on
every input).

## 1. The protocol

Each process `i ∈ 1..n` of the source runs two procedures:

- `Broadcast(i)`: `send(value[i], j)` for every `j ∈ 1..n`, self included.
- `Collect(i)`: `receive(i)` n times, keep the max, `set_decision`.

`channels[i]` is a multiset, so a receive takes any pending value (our `bag`/asyn). The
property, `Main`'s `ensures`, is `decision[j] == max(values)` for every j.

Roles (0-based here):

- Visible: `c0 .. c{n-1}`, one per process. `c_i` is process i's environment. It picks the
  process's value `v_i ∈ {0, 1}` by `nondet()` (Java: `Nondet.chooseBoolean()`), sends
  `Input(v_i)`, and receives `Decide(d)`. In the source `value` is an unconstrained global; the
  nondeterministic input stands for "any value".
- Invisible: `node0 .. node{n-1}` (implementation) and `oracle` (specification).

Messages: `Input(i32)` (client → node / oracle), `Val(i32)` (node → node, the source's
`send(v, i)`), `Decide(i32)` (node / oracle → client). Parameter n, the number of processes.
Smallest interesting size: n = 2.

**Interpretation, stated.** The brief says "visible roles: the n nodes' decisions". The nodes
themselves cannot be visible (rule 3), because in the implementation they send and receive
`Val`s, which the specification has no counterpart for. So each process gets a visible endpoint
`c_i` that both supplies the value and receives the decision. Without the visible input, the
specification could only say "everyone decides the same value in {0, 1}", not "the max of the
inputs". The catalogue (CATALOGUE-grove-civl.md) proposes the same mapping.

## 2. The specification program

One invisible `oracle`, as rule 2 requires for one decision every client observes. It performs n
blocking receives of `Input`, computes the max, and sends `Decide(max)` to `c0, …, c{n-1}` in
that order. Visible behaviour: every `c_i`'s `Decide` comes after every client's `Input` (in
happens-before), and they all carry max(v). Graphs: 2^n value choices × n! receive orders.

## 3. The implementation program

`node_i` does three things:

1. A blocking receive of `Input(v)` from `c_i`.
2. `Val(v)` sent to every node, self included (the source's `Broadcast`).
3. n blocking receives of `Val` (the source's `Collect`), then `Decide(max)` sent to `c_i`
   (`set_decision`).

Graphs: 2^n × (n!)^n (each node reads the n values in any order).

## 4. Where they differ, and why the verdict is what it is

`correct`: node i decides only after receiving all n `Val`s. Each `Val(v_j)` was sent after
node j received `Input(v_j)`. So every client's `Input` happens before every `Decide`, as in the
specification, and every decision is max(v). The specification orders nothing else among visible
events: the oracle's `Decide` sends are invisible, so the clients' receives are mutually
unordered on both sides. **CONFORMS.**

`decide-early`: node i decides after n − 1 values. At n = 2 (smallest size), take `v0 = 0`,
`v1 = 1` and node 0 reading its own `Val(0)` first. Then `c0` receives `Decide(0)`, while every
specification graph with these inputs gives `Decide(1)`. That is a **signature value**
difference. The **order** component can also fail, on some graphs: with `v0 = v1 = 0`, node 0
collects its own value and decides before receiving anything from node 1, so `c0`'s `Decide(0)`
is not causally after `c1`'s `Input(0)`, which the specification requires. (Not every
correct-valued early decision loses an input dependency: a node that collects every other
node's value and omits only its own broadcast still depends on all inputs.) **DOES NOT REFINE.**

## 5. Variants and faults

| variant | how it differs | expected verdict | reason |
|---|---|---|---|
| `correct` | — | CONFORMS | §4 |
| `decide-early` | `Collect` stops after n − 1 receives | DOES NOT REFINE (n ≥ 2) | wrong decision value; decision not after every input |

The source ships no faults; `decide-early` is the catalogue's fault (a). The catalogue's fault (b)
(a node decides on its own value plus the first received) was not built. Numerical coincidence:
the `decide-early` counts equal `correct`'s, because n!/1! = n! orders per node.

## 6. Files

- `source/`: `BroadcastConsensus.bpl` and its `.expect`, copied verbatim, with provenance in
  `source/PROVENANCE.md`.
- `rust/`: the Must-API Rust crate `bench_broadcast_consensus_civl`. CLI, the shared main: `<bin>
  count|export|conf|online … --model M -- <n> [correct|decide-early]`; defaults: n 2, model `bag`,
  variant `correct`. Each node has one `Input` channel and one `Val` channel. See
  `_tools/run-rust.sh`.
- `java/`: package `bench.broadcast_consensus_civl`, with `Spec` (args `n,model`) and `Impl` (args
  `n,model,variant`). A node's two Rust channels are two type guards (`Input.class`, `Val.class`)
  on its mailbox. Spawn order: spec `oracle, c0..`; impl `node0.., c0..`. See
  `_tools/run-java.sh`.

## 7. Plain-Must exploration counts (both tools, one run each)

Model `bag` (asyn) throughout, the source's model.

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | n=2 | bag | 8 / 0 | 8 / 0 | yes |
| spec | n=3 | bag | 48 / 0 | 48 / 0 | yes |
| spec | n=4 | bag | 384 / 0 | 384 / 0 | yes |
| impl `correct` | n=2 | bag | 16 / 0 | 16 / 0 | yes |
| impl `correct` | n=3 | bag | 1728 / 0 | 1728 / 0 | yes |
| impl `correct` | n=4 | bag | 5308416 / 0 | not exhausted at 600 s (60868 / 0 when cut) | not comparable |
| impl `decide-early` | n=2 | bag | 16 / 0 | 16 / 0 | yes |
| impl `decide-early` | n=3 | bag | 1728 / 0 | 1728 / 0 | yes |
| impl `decide-early` | n=4 | bag | 5308416 / 0 | not exhausted: stopped at the 100000-iteration bound (100000 / 0), before 600 s | not comparable |

All JMC runs at n ≤ 3 ended with "stack exhausted". Size 4 was run because size 3 finished in
under a minute on both tools. At n = 4 TraceForge exhausts both
implementation variants (5308416 = 2^4 × (4!)^4 graphs, as §3 predicts) and JMC does not, with
the brief's arguments `100000 600`: `correct` stopped on the 600 s timeout, `decide-early` on the
100000-iteration bound. That is an exhaustion difference, not a count disagreement; the two tools
agree on every row both finished.

Commands, run from `B=/local/home/mkhoshechin/Documents/github-jmc/plan/benchmarks`, for
`n ∈ {2, 3, 4}`, `v ∈ {correct, decide-early}`:

```
$B/_tools/run-rust.sh $B/broadcast-consensus-civl count spec --model bag -- <n>
$B/_tools/run-rust.sh $B/broadcast-consensus-civl count impl --model bag -- <n> <v>
$B/_tools/run-java.sh $B/broadcast-consensus-civl 'bench.broadcast_consensus_civl.Spec:<n>,bag' must 100000 600
$B/_tools/run-java.sh $B/broadcast-consensus-civl 'bench.broadcast_consensus_civl.Impl:<n>,bag,<v>' must 100000 600
```

The size-4 Rust runs were wrapped in `timeout 600`. Each row's log is in `out/logs/`, named
`rust-spec-<n>-bag.log`, `rust-impl-<v>-<n>-bag.log`, `java-spec-<n>-bag.log` and
`java-impl-<v>-<n>-bag.log` for the four commands above, in that order.
