# prodcons(k) — producer–consumer over a FIFO channel, a model-sensitivity control (`producer-consumer-civl`)

**Communication model:** the specification's channel is fifo (`p2p`) by construction, whatever the run's model; the
implementation's channel takes the run's model, fifo (`p2p`) or bag (`asyn`). The pair is run under both.
**Languages:** Rust (TraceForge's Must API), in `rust/`; Java twin, in `java/` (classes `Spec` and `Impl`);
context-free Java port, in `java-cf/`. **Visible threads:** `producer`, `consumer`; there is no invisible thread.
**Parameters:** `k` = the most items the producer sends (default 2; the runs use `k = 2, 3, 4`; smallest interesting
size `k = 1`). ARGS `<k>`, model by `--model`. **Source:** CIVL (Boogie), `Test/civl/samples/ProducerConsumer.bpl`.

Notation: as in `../../synthetic/naive/README.md`. `M` is the channel's model. `Item(0)` is the stop value; `Item(x)`
with `x ≥ 1` is a payload.

The program, the same code on both sides (spawn order `consumer, producer`):

```
PC^M(k) ≜ producer: for x := 1..k:
                       if nondet({true, false}) then send^M(consumer, Item(x))
                       else send^M(consumer, Item(0)); stop
                    send^M(consumer, Item(0))
        ‖ consumer: for t := 1..k+1:
                       Item(x) := recv^M()
                       if x = 0 then stop
```

The producer sends `Item(1), …, Item(j)` and then `Item(0)`, choosing `j ∈ {0, …, k}` by one choice per step (the
source's `if (*)`). The consumer receives until it reads `Item(0)`, at most `k + 1` times. (The source's consumer also
asserts that each value is one more than the previous; the Rust and Java programs have no assertion, the order shows
in the received values.)

## The pairs

The specification of both pairs:

```
Spec(k) ≜ PC^fifo(k)
```

The channel is FIFO by construction, so the consumer reads `Item(1), …, Item(j), Item(0)` in order: `k + 1` graphs,
one per producer choice, under either run model.

### fifo — expected: **conforms**

```
Impl(k) ≜ PC^fifo(k)                     -- run with --model fifo
```

The implementation is the specification's program: every implementation graph is a specification graph.

### bag — expected: **violation** (`k ≥ 1`; at `k = 0` the producer sends only `Item(0)`)

```
Impl(k) ≜ PC^asyn(k)                     ◀ changed: the channel takes the run's model, bag
```

Under bag a receive reads any unread item. At `k = 1`, with the producer sending `Item(1), Item(0)`:
`producer: S(Item(1)) S(Item(0))`, `consumer: R(Item(0))`. Every specification graph with that producer word has the
consumer's first receive return `1` (value). From `k = 2` the consumer can also read `Item(2)` before `Item(1)`. The
implementation has 8 graphs at `k = 2`, 24 at `k = 3`, 89 at `k = 4` against the specification's 3, 4, 5.

## What it is for

The one-line check that a tool honours the run's communication model: the two sides are the same code, and only the
channel's model separates them. A tool that treated bag as FIFO would report `bag` as conforming. A JMC run must
take each side's own model (the specification P2P, the implementation ASYN under bag), not one model for both.
