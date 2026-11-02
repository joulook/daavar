# two-pc-trillium: Java twin (written without context, 2026-11-02)

Source of truth: `daavar/benchmarks/two-pc-trillium/rust/src/main.rs`.

- Class: `bench.two_pc_trillium.TwoPcTrillium` (`java/bench/two_pc_trillium/TwoPcTrillium.java`).
- Arguments: `<spec|impl>,<N>,<fifo|bag>[,<correct|ignore-abort-vote>]`. N = number of resource
  managers. `bag` -> `CommunicationModel.ASYN`, `fifo` -> `CommunicationModel.P2P`. The variant
  defaults to `correct` and is ignored on the spec side (as in Rust); an unknown variant is refused
  for impl only (Rust panics in `imp`).
- `programName()` = `two-pc-trillium`, `size()` = `"<N>"`.

## Threads (spawn order)

| Rust | Java role | visible | body |
|---|---|---|---|
| `obs{i}` (i = 0..N-1), spawned first on both sides | `obs{i}` | yes | one blocking `Channel.recv(model, Outcome.class)` |
| spec: `tc` | `tc` | no | N times `Nondet.chooseBoolean()` (true = local abort, as Rust `nondet()` true); Committed iff no true; sends the same `Outcome` to obs0..obs{N-1} in order |
| impl: `tm` (spawned after the observers) | `tm` | no | `transaction_manager`, line by line |
| impl: `rm{i}` (after `tm`, i ascending) | `rm{i}` | no | `resource_manager`, line by line, then report to `obs{i}` |

Main is invisible, spawns everything, then awaits obs*, then rm* (impl), then tm / tc, as the
Rust joins. Main performs no message event.

## Messages

| Rust | Java record | `@Tag` | fields |
|---|---|---|---|
| `ToRm::Prepare` / `Commit` / `Abort` | `ToRm.Prepare()` / `Commit()` / `Abort()` | `Prepare` / `Commit` / `Abort` | none |
| `ToTm::Prepared(usize)` / `Aborted(usize)` / `Committed(usize)` | `ToTm.Prepared(int rm)` / `Aborted(int rm)` / `Committed(int rm)` | `Prepared` / `Aborted` / `Committed` | RM index |
| `Outcome::Committed` / `Aborted` | `Outcome.Committed()` / `Aborted()` | `Committed` / `Aborted` | none |

Each enum is a sealed interface extending `MpMessage`. All receives are blocking and typed by the
sealed interface (the Rust side's type of the mailbox / channel); no value guards.

## Places where I was unsure / what I assumed

1. **RM inbox = named channel `("rm", i)` in Rust; no channels in the Java API.** I mapped the
   channel to rm i's own process mailbox (in Rust rm i receives nothing else on its thread
   mailbox, and the channel carries only `ToRm`). The Rust program uses the named channel so that
   `tm` can address an RM spawned after it. In Java `tm` cannot hold rm i's `Tid` from a handle,
   so it builds `Tid.of(b + N + 1 + i, "rm"+i)`, where b = obs0's task id. This relies on task
   ids being minted consecutively at spawn (observed: main is task 1, obs0 is task 2; the
   `MpProgram` comment "k-th process is task k+1" is off by one in my reading, so I do not
   hard-code it). Main checks after spawning that obs i, tm and rm i got exactly these ids and
   throws `IllegalStateException` otherwise (this check caught my first, hard-coded guess).
   Doubt: whether the twin should instead differ in event shape here (it does not: the sends go
   to the same receivers under the same model). Whether a send to a channel vs. to a thread
   mailbox is compared differently by TraceForge's `bag`/`fifo` model is unknown to me; rm is
   invisible anyway.
2. Tag clash: `ToTm::Committed`/`Aborted` and `Outcome::Committed`/`Aborted` share the tag names
   `Committed`/`Aborted` (they are distinct Rust enums with the same variant names). I kept the
   variant names as tags, per the brief. Only `Outcome` reaches visible threads.
3. `usize` -> `int`.
4. The Rust `assert_eq!` in tm is `throw new AssertionError(...)` (not the `assert` keyword, so it
   holds without `-ea`). Note it fires in a spawned process (tm); its failure reaches the checker
   when main awaits tm.
5. Program name `two-pc-trillium` and size text `N` are guesses at what the Rust header writes.
6. `Nondet.chooseBoolean()` lists `true` first; Rust's `nondet()` order unknown. The set of outcomes
   is the same.
7. N = 0 is accepted (Rust would accept it too); not tested.

## Counts measured (`must 0 1 report - 600`, plain enumeration)

| args | complete | blocked | end |
|---|---|---|---|
| `spec,1,bag` | 2 | 0 | stack exhausted |
| `impl,1,bag` | 2 | 0 | stack exhausted |
| `impl,1,bag,ignore-abort-vote` | 1 | 1 | stack exhausted |
| `spec,1,fifo` | 2 | 0 | stack exhausted |
| `impl,1,fifo` | 2 | 0 | stack exhausted |
| `impl,1,fifo,ignore-abort-vote` | 1 | 1 | stack exhausted |
| `spec,2,bag` | 4 | 0 | stack exhausted |
| `impl,2,bag` | 12 | 0 | stack exhausted |
| `impl,2,bag,ignore-abort-vote` | 4 | 6 | stack exhausted |

All exit 0. The ignore-abort-vote blocked graph at N=1: rm0 votes Aborted, tm counts it as
prepared, sends Commit and waits for a `Committed` that never comes (rm0 has ended), so tm blocks.
I did not look up the Rust counts.
