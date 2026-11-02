# two-pc-disel: context-free Java twin

Class: `bench.two_pc_disel.TwoPcDisel` (implements `MpProgram`, constructor `(String...)`).
Arguments: `<spec|impl>,<N>,<fifo|bag>[,<variant>]`, variant `correct` (default) or `skip-ack-loop`.
`fifo` -> `CommunicationModel.P2P`, `bag` -> `CommunicationModel.ASYN`. `N` = participants; K = 2 rounds,
data `[1,2]`, `[3,4]`. programName `two-pc-disel`, size `N`.

## Mapping

Threads (spawn order as in Rust: `cn`, then `p0..p{N-1}`; main awaits `p0..p{N-1}`, then `cn`):
- `cn` (role "cn", invisible): `coordinator(freeAbort, skipAcks)`; spec = (true, false),
  impl correct = (false, false), impl skip-ack-loop = (false, true).
- `p<i>` (role "p<i>", visible): `participant(i, cnTid)`.
- main: invisible, spawns and awaits only, no message events.

Messages (two sealed interfaces, one per Rust enum; tags = Rust variant names, fields in Rust order):
- `ToP`: `@Tag("Prep") Prep(int e, List<Integer> d)`, `@Tag("Commit") Commit(int e)`, `@Tag("Abort") Abort(int e)`.
- `ToCn`: `@Tag("Yes") Yes(int from, int e)`, `@Tag("No") No(int from, int e)`,
  `@Tag("CommitAck") CommitAck(int from, int e)`, `@Tag("AbortAck") AbortAck(int from, int e)`.

Receives (all blocking, as in Rust; guards are type tests matching the Rust tag groups):
- participant: `recv(Prep.class)` (TAG_PREP), then `Commit|Abort` (TAG_DEC), on its own mailbox.
- coordinator: N x `Yes|No` (TAG_VOTE), then (unless skip) N x `CommitAck|AbortAck` (TAG_ACK).

Nondet (`Nondet.chooseBoolean()`, outcomes {true,false}):
- participant, once per round: the vote (`doCommit`).
- coordinator, spec only, once per round and only when all votes are yes (`all_yes && nondet()`
  short-circuits as in Rust).

Assertions: the Rust `assert!`/`assert_eq!` are kept as `throw new AssertionError` (not Java
`assert`, so they hold without `-ea`), on both sides as in Rust.

## Doubts and assumptions

1. Participant addressing. Rust gives each participant a named channel `("p", i)` so that `cn`,
   spawned first, can reach participants spawned later. The JMC API has only per-thread mailboxes
   (`Channel.send(model, Tid, msg)`), so I send to the participant's thread mailbox. `cn` cannot
   capture handles that do not exist yet when it is spawned, so it computes `p_i` as
   `Tid.of(self.raw() + 1 + i, "p"+i)` (consecutive task ids, cn spawned first). Main checks after
   each spawn that the handle's raw id is that value and throws `IllegalStateException` otherwise.
   This relies on task ids being consecutive; a mailbox per thread vs a named channel per
   participant is the same set of receivers here (each participant reads only its own inbox), but
   I am assuming TraceForge's named-channel semantics under bag/fifo match per-thread mailboxes.
2. `[u8; 2]` data payload mapped to `List<Integer>`; I assume TraceForge's encoder renders a
   fixed array as a JSON list of integers. `usize` mapped to `int`.
3. Field names (`from`, `e`, `d`) are mine; only order matters for the encoding.
4. `spec` accepts and validates an optional variant but ignores it (Rust `spec(n)` ignores it).
   Unknown variants are rejected on both sides (Rust only rejects for impl).
5. The Rust `nondet()` order of outcomes is not known to me; Java's `chooseBoolean` is
   {true, false} with min = true. Only the outcome set should matter.
6. `rolesInSpawnOrder` = `cn, p0..p{N-1}`; `visibleRoles` = `p0..p{N-1}`.

## Measured (plain enumeration: `must 0 1 report - 600`)

| args | complete | blocked | end |
|---|---|---|---|
| spec,1,bag | 9 | 0 | stack exhausted |
| impl,1,bag | 4 | 0 | stack exhausted |
| impl,1,bag,skip-ack-loop | 4 | 0 | stack exhausted |
| spec,1,fifo | 9 | 0 | stack exhausted |
| impl,1,fifo | 4 | 0 | stack exhausted |
| spec,2,bag | 400 | 0 | stack exhausted |
| impl,2,bag | 256 | 0 | stack exhausted |

Not compared against any Rust count (per the brief).
