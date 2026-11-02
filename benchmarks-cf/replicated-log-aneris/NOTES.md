# replicated-log-aneris: context-free Java twin

Class: `bench.replicated_log_aneris.ReplicatedLogAneris` (implements `MpProgram`, public `(String...)` constructor).
File: `java/bench/replicated_log_aneris/ReplicatedLogAneris.java`.

Arguments: `<spec|impl>,<W>,<fifo|bag>[,<variant>]`. `W` = number of writes (and reads); variant
`correct` (default) or `asyn-fault`, used by impl only (spec ignores it, as the Rust spec does).
`fifo` -> `CommunicationModel.P2P`, `bag` -> `CommunicationModel.ASYN`.

## Mapping

Roles (all spawned with `MpProcess.spawn(role, ...)` by main, which then awaits them):
- spec spawn order: `follower`, `leader`, `writer`, `reader` (as Rust `spec`).
- impl spawn order: `leader`, `follower`, `writer`, `reader` (as Rust `imp`).
- visible: `writer`, `reader` (Rust `visible`). Await order: writer, reader, leader, follower (as Rust joins).

Threads:
- `writer`: for i in 0..W: send Write(self, i, val(i)) to leader; blocking recv of Ack.
- `reader`: for i in W-1..0: send Read(self, i) to follower; blocking recv of Val.
- spec `follower`: 2W blocking receives of Entry-or-Read; Entry goes to an `early` list and the
  contiguous prefix from `next` is applied (same first-position search and removal); Read is answered
  with Val(db[k]).
- spec `leader`: W times: blocking recv Write, send Ack to its `from`, send Entry(k, v, j) to follower.
- impl `leader`: 2W blocking receives of Write-or-Sync; Write: store, append to log, Ack, then stable
  sort `waiting` by index and answer the satisfiable prefix with Entry; Sync: answer immediately if
  the index is in the log, else park in `waiting`.
- impl `follower`: correct: send Sync(self, 0); asyn-fault: send Sync(self, i) for all i in 0..W first.
  Then 2W blocking receives of Entry-or-Read; on Entry (correct only) `assert next == j` (thrown as an
  explicit `AssertionError`, so it does not depend on `-ea`), apply, `next++`, and (correct only) send
  Sync(self, next) if next < W; Read is answered with Val(db[k]).

Messages (sealed interface `Msg extends MpMessage`, one `@Tag` record per Rust variant, same field order):
- `Write{from: ThreadId, k: u8, v: u8}` -> `@Tag("Write") record Write(Tid from, int k, int v)`
- `Ack` -> `@Tag("Ack") record Ack()`
- `Read{from, k}` -> `@Tag("Read") record Read(Tid from, int k)`
- `Val(u8)` -> `@Tag("Val") record Val(int v)`
- `Sync{from, i}` -> `@Tag("Sync") record Sync(Tid from, int i)`
- `Entry{k, v, j}` -> `@Tag("Entry") record Entry(int k, int v, int j)`

Receives: all blocking (Rust uses only `recv_tagged_msg_block`). Rust tag guards become type tests
(`Channel.recv(model, Ack.class)` etc., and `a.isInstance(m) || b.isInstance(m)` for two tags).
Nondet: none (the Rust program has no `nondet` choice). No non-blocking receives, no daemons.

## Doubts / assumptions

1. The Rust file has no `ARGS:` comment; I used its `Usage:` doc line (`-- <W> [variant]`) and
   `parse` (W defaults to 2, variant to `correct`). In Java W is required by the argument format.
2. `programName()` = `replicated-log-aneris` and `size()` = `W` as decimal text: guesses; the Rust side
   (TraceForge's `program::main_with_model`) chooses the header's name/size and I did not read it.
3. `Val(u8)` is a Rust tuple variant; I named the single field `v`. Field names do not appear in the
   encoding, only order, so this should not matter.
4. u8 -> int. Values stay small (val(i) <= W, indices < W), so no u8 wrap-around differences unless
   W > 255.
5. The Rust tag numbers (TAG_WRITE=1, ...) are only used for receive filtering; I assumed that the
   Rust variant name is the summary tag (as the brief says), not the numeric tag.
6. Non-visible roles `leader`/`follower` are declared in `rolesInSpawnOrder` (the launcher requires every
   spawned role there) but not in `visibleRoles`.
7. Spec side has no assertion; impl `correct` has the one assertion (`next == j`); the `unreachable!()`
   arms are `IllegalStateException`s that cannot fire because guards admit only the two types.

## Measured (plain must enumeration, `must 0 1 report - 600`)

| args | complete | blocked | end |
|---|---|---|---|
| spec,1,fifo | 2 | 0 | stack exhausted |
| impl,1,fifo | 4 | 0 | stack exhausted |
| impl,1,fifo,asyn-fault | 4 | 0 | stack exhausted |
| spec,1,bag | 2 | 0 | stack exhausted |
| impl,1,bag | 4 | 0 | stack exhausted |
| impl,1,bag,asyn-fault | 4 | 0 | stack exhausted |
| spec,2,fifo | 6 | 0 | stack exhausted |
| impl,2,fifo | 30 | 0 | stack exhausted |
| impl,2,fifo,asyn-fault | 36 | 0 | stack exhausted |

All exited 0 (no assertion failure). Not compared with any Rust count.
