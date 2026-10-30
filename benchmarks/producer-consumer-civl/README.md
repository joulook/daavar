# Producer–consumer over a FIFO channel (CIVL), a model-sensitivity control

**Family:** related-work  **Source:** CIVL (Boogie) `samples/ProducerConsumer.bpl`, under
`related-work-papers/sources/boogie/Test/civl/samples/` (CANDIDATES.md N4)  **Expected verdict:**
**CONFORMS** under `fifo` (p2p) (the implementation is the specification's program over a channel
that is FIFO in that run); **DOES NOT REFINE** under `bag` (asyn) (the consumer can read the items
out of order, or the stop value before an item, which the FIFO specification never does).

## 1. The protocol

The source has two processes over one FIFO queue `Channel(C, head, tail)`. The `producer` sends
`1, 2, …` and, at a nondeterministic step (`if (*)`), the stop value `0`. The `consumer` receives
until it reads `0` and asserts that each value it reads is one more than the previous one
(`assert {:layer 1} x' == x`). The queue is FIFO, so the assertion holds.

Roles, both visible: `producer` and `consumer`. There is no invisible thread. Message: one type,
`Item(x)` (`Msg::Item(i32)` in Rust, record `Item(int x)` in Java). `x ≥ 1` is a payload and
`x = 0` ends the stream.

Parameter `k`: the producer sends at most `k` items. At each step `x = 1..k` it chooses by one
`nondet()` (Java: `Nondet.chooseBoolean()`) between sending `Item(x)` and sending `Item(0)` and
stopping. After `k` items it sends `Item(0)`. The consumer performs at most `k + 1` blocking
receives and stops at `Item(0)`. The source has no bound; `k` is ours. Smallest interesting size:
`k = 1` (`Item(1), Item(0)`, which `bag` can reorder). The runs use `k = 2, 3, 4`.

## 2. The specification program

The specification is the source program itself. `producer` and `consumer` share one channel whose
order is FIFO by construction, whatever the run's model. In Rust the channel is built with
`Builder::with_comm(CommunicationModel::LocalOrder)`. In Java every send and receive of `Spec`
uses `CommunicationModel.P2P`, and `Spec` ignores its model argument: its `model()` declares P2P
whatever the argument. A JMC conformance run must therefore take each side's own model (the
spec P2P, the impl ASYN under `bag`), not one model for both. So the consumer reads
`Item(1), …, Item(j), Item(0)` in order, for each `j ∈ 0..k` (k + 1 graphs under either model).

## 3. The implementation program

The implementation has the same two threads and the same code. The only difference is that its
channel takes the run's model (Rust `Builder::new()` without `with_comm`; Java `Support.model(arg)`).
Under `fifo` it is the specification program. Under `bag` a receive reads any unread item.

## 4. Where they differ, and why the verdict is what it is

- `fifo`: the two programs are the same program, so every implementation graph is a
  specification graph. **CONFORMS.**
- `bag`: the implementation has graphs the specification lacks. At `k = 1` the producer sends
  `Item(1), Item(0)` and the consumer reads `Item(0)` first. Its visible receive returns `0` where
  every specification graph with that producer choice returns `1`, so the **signature value** of
  the consumer's first receive differs (0 against 1). At `k = 2`, the consumer can also read
  `Item(2)` before `Item(1)`. The smallest size is `k = 1`; every size we run (`k ≥ 2`) contains it.
  **DOES NOT REFINE.** This is the one-line check that a tool honours the run's communication
  model: a tool that treated `bag` as FIFO would report CONFORMS here.

## 5. Variants and faults

There is one pair, run under two models (the CLI's model argument):

| model | expected verdict | reason |
|---|---|---|
| `fifo` (p2p) | CONFORMS | identical programs |
| `bag` (asyn) | DOES NOT REFINE | early `Item(0)`: the consumer's first receive returns 0 against 1 (value) |

The source ships no faults. The model is the fault.

## 6. Files

- `source/`: `ProducerConsumer.bpl` and `ProducerConsumer.bpl.expect` copied verbatim, with
  provenance in `source/PROVENANCE.md`.
- `rust/`: the Must-API Rust crate `bench_producer_consumer_civl`. CLI: `<spec|impl> <k> [fifo|bag]`
  (the spec ignores the model). See `_tools/run-rust.sh`.
- `java/`: package `bench.producer_consumer_civl`, with `Spec` (args `k[,model]`, model ignored;
  it declares P2P whatever its argument, so a JMC conformance run takes each side's own model:
  the spec P2P, the impl ASYN) and `Impl` (args `k,model`), both on the shared `ProducerConsumer` base. The spawn order is
  consumer, then producer. See `_tools/run-java.sh`.

## 7. Plain-Must exploration counts (both tools, one run each)

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | k=2 | fifo | 3 / 0 | 3 / 0 | yes |
| spec | k=2 | bag | 3 / 0 | 3 / 0 | yes |
| spec | k=3 | fifo | 4 / 0 | 4 / 0 | yes |
| spec | k=3 | bag | 4 / 0 | 4 / 0 | yes |
| spec | k=4 | fifo | 5 / 0 | 5 / 0 | yes |
| spec | k=4 | bag | 5 / 0 | 5 / 0 | yes |
| impl | k=2 | fifo | 3 / 0 | 3 / 0 | yes |
| impl | k=2 | bag | 8 / 0 | 8 / 0 | yes |
| impl | k=3 | fifo | 4 / 0 | 4 / 0 | yes |
| impl | k=3 | bag | 24 / 0 | 24 / 0 | yes |
| impl | k=4 | fifo | 5 / 0 | 5 / 0 | yes |
| impl | k=4 | bag | 89 / 0 | 89 / 0 | yes |

Every JMC run ended with "stack exhausted", meaning the exploration finished. Size 4 was run
because size 3 finished in well under a minute on both tools. The two tools agree on every row.

The `spec` rows under `bag` equal those under `fifo` because the specification's channel is FIFO
by construction (§2).

Commands, run from `B=/local/home/mkhoshechin/Documents/github-jmc/plan/benchmarks`, for
`side ∈ {spec, impl}`, `k ∈ {2, 3, 4}`, `m ∈ {fifo, bag}`:

```
$B/_tools/run-rust.sh $B/producer-consumer-civl <side> <k> <m>
$B/_tools/run-java.sh $B/producer-consumer-civl 'bench.producer_consumer_civl.Spec:<k>,<m>' must 100000 600
$B/_tools/run-java.sh $B/producer-consumer-civl 'bench.producer_consumer_civl.Impl:<k>,<m>' must 100000 600
```

Each row's logs are `out/logs/rust-<side>-<k>-<m>.log` (the first command) and
`out/logs/java-<side>-<k>-<m>.log` (the second or third).
