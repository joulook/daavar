# a1-commit: context-free Java twin

Class: `bench.a1_commit.A1Commit` (file `java/bench/a1_commit/A1Commit.java`), implements `MpProgram`,
public `(String...)` constructor.

Read: `rust/src/main.rs`, `rust/Cargo.toml`, the listed JMC API sources (api/mp, mp/events, mp/summary,
MpProgram, MpLauncher) and SUMMARY-FORMAT.md sections "Values" and "What both programs must satisfy".
Nothing else.

## Arguments

`<spec|impl>,<n>,<r>,<fifo|bag>,<variant>`, variant one of `correct`, `eager`, `early-abort`, `silent`,
`control-reverse`. Example: `bench.a1_commit.A1Commit:impl,2,1,fifo,correct`.

- The Rust ARGS are `<variant> <n> <r>` (variant first, and mandatory on both sides). The brief's
  template is `<spec|impl>,<size...>,<fifo|bag>[,<variant>]`. I followed the brief (variant last,
  required, validated on the spec side too as in Rust). The Rust order `<spec|impl>,<variant>,<n>,<r>,<fifo|bag>`
  is also accepted: a non-numeric second argument is read as the variant.
- `fifo` -> `CommunicationModel.P2P`, `bag` -> `CommunicationModel.ASYN`.
- `programName()` = `a1-commit`; `size()` = `"<n>,<r>"` (assumption: I did not read the header
  section of the format; the variant is not in the size).

## Threads (spawn order as in Rust)

| Rust | Java |
|---|---|
| main: spawn `coord`, then `p0..p{n-1}` (spec: always `p0,p1`), then `send(coord, Init(ids))` | `run()`: `MpProcess.spawn("coord", ...)`, `spawn("p"+i, participant)`, **no send** (see Change 1), then `await()` on every handle (Rust main does not join; JMC requires main to await all) |
| `a1_coordinator(kind, r)` | `coordinator()` - same loops, branch per variant, same send order |
| spec coordinator closure | `specCoordinator()` |
| `a1_participant(r)` | `participant()` |

Roles: `rolesInSpawnOrder()` = `coord, p0..p{k-1}` (k = n impl, 2 spec); `visibleRoles()` = `p0, p1`
always (Rust `visible` returns them whatever the arguments). `coord` is given a role (invisible) because
its Tid travels inside `Prepare` and the encoder needs a role name for every Tid in a payload.

## Messages

| Rust | Java (`@Tag`) |
|---|---|
| `A1ToCoord::Init(Vec<ThreadId>)` | `record Init(List<Tid> ps)` `@Tag("Init")`, declared but never sent (Change 1) |
| `A1ToCoord::Yes` | `record Yes()` `@Tag("Yes")` |
| `A1ToCoord::No` | `record No()` `@Tag("No")` |
| `A1ToPart::Prepare(ThreadId)` | `record Prepare(Tid coord)` `@Tag("Prepare")` |
| `A1ToPart::Commit` | `record Commit()` `@Tag("Commit")` |
| `A1ToPart::Abort` | `record Abort()` `@Tag("Abort")` |

Sealed interfaces `ToCoord` (Init, Yes, No) and `ToPart` (Prepare, Commit, Abort). Every
`recv_msg_block::<A1ToCoord>()` / `::<A1ToPart>()` is `Channel.recv(model, ToCoord.class)` /
`Channel.recv(model, ToPart.class)`: blocking, guard = type test of the Rust enum. No non-blocking
receives (Rust has none). The coordinator's own id is `MpProcess.self()` (tagged `coord`).

## Nondeterminism

- Participant vote: `traceforge::nondet()` -> `Nondet.chooseBoolean()` (true = Yes).
- Spec decision: `traceforge::nondet()` -> `Nondet.chooseBoolean()` (true = Commit).
Same two places as Rust, outcome set {true, false}. Option order: JMC's min(S) is `true`; I do not
know TraceForge's order for `nondet()` (doubt; it affects only exploration order, not the set).

## Measured before Change 1 (plain `must` enumeration, `must 0 1 report - 600`)

| args | result |
|---|---|
| `spec,2,1,fifo,correct` | 16 distinct complete, 0 distinct blocked; stack exhausted |
| `impl,2,1,fifo,correct` | 8 complete, 0 blocked; stack exhausted |
| `impl,2,1,fifo,eager` | 8 complete, 0 blocked; stack exhausted |
| `impl,2,1,fifo,early-abort` | 8 complete, 0 blocked; stack exhausted |
| `impl,2,1,fifo,silent` | 0 complete, 8 blocked; stack exhausted |
| `impl,2,1,fifo,control-reverse` | 8 complete, 0 blocked; stack exhausted |
| `spec,2,1,bag,correct` | 16 complete, 0 blocked; stack exhausted |
| `impl,2,1,bag,correct` | 8 complete, 0 blocked; stack exhausted |

Smallest size taken as n=2, r=1 (visible roles need p0 and p1).

## Change 1 (coordinator's order, 2026-11-02): main sends nothing

- Main no longer sends `Init`. Both coordinators (impl and spec) skip the `Init` receive and build
  the participant list themselves with `participantIds()`: `p_i = Tid.of(i + 3, "p" + i)`.
- The order said `Tid.of(i + 2)`, which was my own proposal, and it was wrong by one. MpProgram's
  rule "the k-th process main spawns is task k + 1" counts k from 1. Coord is spawned first, so it is
  task 2, and p0 is task 3. A check in main (each spawned handle's Tid must equal
  `participantIds().get(i)`, otherwise IllegalStateException) caught it: the first run failed with
  `spawn-order rule broken: p0 is Tid(3:p0), expected Tid(2:p0)`. I changed the offset to `i + 3`
  and kept the check.
- Effect: one invisible receive with a single candidate (coord's `Init`) and main's one send are gone.
  The participants' events are unchanged. The coordinator no longer matches Rust line for line.
- Counts after the change (`must 0 1 report - 600`):
  - `spec,2,1,fifo,correct`: 16 distinct complete, 0 distinct blocked; stack exhausted
  - `impl,2,1,fifo,correct`: 8 distinct complete, 0 distinct blocked; stack exhausted

  Both are the same as before the change. Export mode was not run again after the change.

## Open doubts

1. (Resolved by Change 1.) Main used to send `Init`, which the format forbids; export refused it.
2. Argument order: the variant is last (brief template), not first (Rust ARGS). See above.
3. `n < 2` for impl: Rust still declares p0,p1 visible, and so do I. With n<2 a visible role is
   never spawned. I did not refuse it and did not test it.
4. `size()` text `"n,r"` is a guess.
5. Rust main never joins; JMC main awaits every handle after the send. Required by the JMC API.
