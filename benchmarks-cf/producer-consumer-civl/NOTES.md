# producer-consumer-civl — context-free Java twin

Class: `bench.producer_consumer_civl.ProducerConsumerCivl` (constructor `(String[] args)`), in
`java/bench/producer_consumer_civl/ProducerConsumerCivl.java`.

Arguments: `<spec|impl>,<k>,<fifo|bag>[,<variant>]`. `k >= 0` (Rust default 2 when absent; here it is
required). The program has no variants; a fourth argument is accepted only if empty.

## Mapping

Threads (spawned by the invisible main in Rust's order, awaited in Rust's join order):
1. `consumer` (visible): `for i in 0..=k`: blocking receive of an `Item` (`Channel.recv(model, Item.class)`,
   a type-test guard = Rust `recv_msg_block`); return on `Item(0)`.
2. `producer` (visible): `for x in 1..=k`: `Nondet.chooseBoolean()` (true = Rust `nondet()` true, also
   `min(S)`): true sends `Item(x)`, false sends `Item(0)` and returns; after the loop sends `Item(0)`.
Main: spawn consumer, spawn producer (with the consumer's Tid captured), await consumer, await producer.
No message-passing event on main.

Messages: Rust `enum Msg { Item(i32) }` -> `sealed interface Msg extends MpMessage permits Item`,
`@Tag("Item") record Item(int x)`.

Channel: the Rust program has one channel, producer -> consumer. JMC has no channel objects; sends are
addressed to the consumer's Tid (its mailbox). Since the consumer receives only from this one channel
and has only one sender, this is the same.

Models: `fifo` -> `P2P`, `bag` -> `ASYN`. spec: always `P2P` (Rust `LocalOrder`, FIFO by construction
whatever the run's model). impl: the run's model.

## Doubts / assumptions

- Rust `LocalOrder` taken as JMC `P2P` (per-sender FIFO; there is only one sender, so it is FIFO).
- `model()` for spec returns `P2P` (the model every send/receive actually uses), not the run's `--model`.
  If the harness expects the header model to echo the run argument (e.g. `asyn` for `spec,k,bag`),
  this differs; the format says the model is printed, never compared.
- `programName()` = `producer-consumer-civl` (directory name; the Rust program declares no name).
  `size()` = `k` as decimal text.
- Smallest size taken as `k = 0` (the Rust loop admits it); also measured k = 1, 2.

## Counts measured (`must 0 1 report - 600`, all ended "stack exhausted", 0 blocked)

| args | complete | blocked |
|---|---|---|
| spec,0,fifo | 1 | 0 |
| impl,0,fifo | 1 | 0 |
| spec,0,bag | 1 | 0 |
| impl,0,bag | 1 | 0 |
| spec,1,fifo | 2 | 0 |
| impl,1,fifo | 2 | 0 |
| spec,1,bag | 2 | 0 |
| impl,1,bag | 3 | 0 |
| spec,2,fifo | 3 | 0 |
| impl,2,fifo | 3 | 0 |
| spec,2,bag | 3 | 0 |
| impl,2,bag | 8 | 0 |

Hand check (not tuned): impl/bag at k sums over j = 0..k of the orders in which the consumer can read
`Item(0)` among the j+1 pending items: k=1: 1+2 = 3; k=2: 1+2+5 = 8. FIFO/spec: k+1.
