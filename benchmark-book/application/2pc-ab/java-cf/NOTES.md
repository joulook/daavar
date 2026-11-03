# two-pc-abstain-kragl: context-free Java twin

Class: `bench.two_pc_abstain_kragl.TwoPcAbstainKragl` (`java/bench/two_pc_abstain_kragl/TwoPcAbstainKragl.java`).

Arguments: `<spec|impl>,<n>,<fifo|bag>[,<correct|off-by-one>]`, n >= 1. `fifo` maps to `P2P`, `bag` to `ASYN`.
The Rust program says to use `--model bag`. The variant is accepted on both sides, as Rust's `parse` accepts it. Only the impl side uses it: with `off-by-one` the quorum is `n - 1`.

## Mapping

Threads (spawn order as in Rust: `coord`, then `p1 .. pn`; main is invisible, spawns all, awaits all):
- `coord` (role `coord`, invisible: in `rolesInSpawnOrder`, not in `visibleRoles`):
  - spec: `specCoord` = Rust `spec_coord` (MAIN5). It sends `Req` to p1..pn. For each of the n rounds it first makes a `Nondet.chooseBoolean()` choice: `true` broadcasts `Dec(Abort)` and returns, `false` does a blocking receive of `Vote`. At the end it commits only when every vote was Yes and a final `chooseBoolean()` returns true. The decision is broadcast to p1..pn in order.
  - impl: `implCoord` = Rust `impl_coord`. It sends `Req` to everyone and then does up to `quorum` blocking `Vote` receives, breaking at the first `No` (decision Abort; otherwise Commit). It then broadcasts the decision.
- `p1 .. pn` (visible): `participant` = Rust `participant`. `chooseBoolean()`: when true, it does a blocking receive of `Req`, then `chooseBoolean()` picks Yes (true) or No (false) and it sends `Vote(v)` to coord. It then does a blocking receive of `Dec`.
- Every Rust `nondet()` is `Nondet.chooseBoolean()` at the same point, with the outcome set {true, false}.
- Each thread has one inbox: Rust's per-thread channel is the process's own Tid. Rust's tagged receives (`recv_tagged_msg_block` on the tag) become blocking type-test receives `Channel.recv(model, Req.class / Vote.class / Dec.class)`. There are no value guards and no non-blocking receives.

Messages (`sealed interface Msg extends MpMessage`):
- `Req` -> `@Tag("Req") record Req()`
- `Vote(Ballot)` -> `@Tag("Vote") record Vote(Ballot ballot)`
- `Dec(Decision)` -> `@Tag("Dec") record Dec(Decision decision)`
- `Ballot::{Yes,No}` -> `sealed interface Ballot` with `@Tag("Yes") record Yes()` and `@Tag("No") record No()`. These are records with no fields, not a Java enum, because a Java enum would encode as a bare string and the Rust side encodes `{"t":"Yes","f":[]}`.
- `Decision::{Commit,Abort}` -> the same pattern: `@Tag("Commit")` and `@Tag("Abort")`.

## Doubts / assumptions

1. **How addresses exist before spawn.** In Rust all channels exist before any thread is spawned, so `coord` can send to `p1` before `p1` is spawned. `MpProcess.spawn` returns a Tid only after it starts the thread, and a started thread may run before main spawns the next one. Instead, I construct every `MpThread(body, role)` first; the constructor mints the task id and raises TASK_CREATED. Then I start them in order and await each with `join1()`. So the program uses `MpThread` directly, not `MpProcess.spawn`/`Handle.await`. All creation events therefore come before all start events. I assumed this is acceptable for the exporter and recorder; I tested only the counting run.
2. **`size()`.** I return `n` alone (e.g. `"1"`), without the variant. I did not know the Rust side's header size text.
3. **Name of the invisible role.** `coord` is spawned with role `coord` (the Rust thread name) and listed in `rolesInSpawnOrder`. I assumed an invisible thread must still be declared there, because the launcher checks spawned roles against that list.
4. **Order of choices.** `chooseBoolean` has min(S) = true. That is the "abort now", "participate" and "Yes" branch. Whether Rust's `nondet()` orders true first is unknown to me; the outcome set is the same.
5. Spec with `off-by-one` is accepted and ignored, as in Rust.

## Counts measured (`must 0 1 report - 600`, n = 1, bag)

- `spec,1,bag`: `graphs: 6 distinct complete, 1 distinct blocked; stack exhausted`
- `impl,1,bag`: `graphs: 2 distinct complete, 1 distinct blocked; stack exhausted`
- `impl,1,bag,off-by-one`: `graphs: 3 distinct complete, 0 distinct blocked; stack exhausted`

A hand check (not tuning) agrees for n = 1. In the spec, the participant abstains, votes Yes or votes No. An early abort gives 3 complete graphs. No early abort gives Yes -> commit or abort (2), No -> abort (1), and abstain -> coord blocked (1).
