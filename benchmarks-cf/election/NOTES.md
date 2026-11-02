# election: context-free Java twin (notes)

Class: `bench.election.Election` (`java/bench/election/Election.java`), implements `MpProgram`,
public `(String...)` constructor.

Arguments: `<spec|impl>,<n>,<fifo|bag>,<leader|ring>,<conf|split|silent>`
(the Rust `{spec|impl} ... --model {fifo|bag} -- leader conf <n>`). The variant can also be one
token (`leader-conf`, `leader conf`). `fifo` -> `P2P`, `bag` -> `ASYN`. The variant is required on
both sides (Rust `arity(args, 3)`); the spec ignores it except for `size()`. Valid variants:
`leader conf|split|silent`, `ring conf|split`.

## Mapping

Threads (spawn order, as in Rust; participants first on both sides):
- `p0..p{n-1}` (visible): `le_participant(n)` -> `participant()`. Same bounded loop of `n`
  blocking receives, same fixed `Vote(true)`, same decision broadcast.
- impl `leader *`: `e0..e{n-1}` (invisible) -> `elector(buggy, silentWinner)` =
  `le_elector_with`; `conf` = (false,false), `split` = (true,false), `silent` = (false,true).
- impl `ring *`: `e0..e{n-1}` -> `ringElector(buggy)` = `ring_elector`; `conf` false, `split` true.
  Same 3-receive bound and same order-tolerant handling (`take()` modelled with nullable locals).
- spec: `oracle` (invisible) -> `oracle()` = `le_oracle`: receives a bare `Setup`, then the ladder
  of `n-1` boolean choices, each `Nondet.chooseBoolean()` exactly where Rust calls `nondet()`
  (outcomes {true,false}; first true picks `i`, none picks `n-1`).
- `boot` (invisible, NEW, spawned last): performs the sends that the Rust main performs after
  spawning (`Setup` to each elector in index order, or the bare `Setup{me:0,electors:[],participants}`
  to the oracle). See doubt 1.
- main: spawns all of the above, then awaits all of them in spawn order; no message event.

Messages (tags = Rust variant names, field order kept; `usize` -> `int`, `ThreadId` -> `Tid`,
`Vec` -> `List`):
- `struct Setup{me, electors, participants}` -> record `Setup(int, List<Tid>, List<Tid>)`, `@Tag("Setup")`
  (sent bare to the oracle, as in Rust).
- `EIn::Setup(Setup)` -> `EIn.SetupE(Setup)` `@Tag("Setup")`; `EIn::Elect(usize)` -> `EIn.Elect(int)` `@Tag("Elect")`.
- `PIn::BeLeader(Vec<ThreadId>)` -> `PIn.BeLeader(List<Tid>)`; `BeFollower(ThreadId)` -> `PIn.BeFollower(Tid)`;
  `Vote(bool)` -> `PIn.Vote(boolean)`; `Decision(bool)` -> `PIn.Decision(boolean)`; tags = variant names.
- `RIn::Setup(Setup)` -> `RIn.SetupR(Setup)` `@Tag("Setup")`; `Tok(usize)` -> `RIn.Tok(int)`; `Ann(usize)` -> `RIn.Ann(int)`.
- Receives: all blocking (`recv_msg_block`), typed by class (`Channel.recv(model, PIn.class)` etc.),
  i.e. a type-test guard only. Each thread receives only one message type, as in Rust.

## Doubts / assumptions

1. **Extra `boot` process.** Rust main sends the `Setup` messages itself. SUMMARY-FORMAT says main
   performs no message-passing event, so those sends moved to an invisible process `boot` spawned
   after all others (it gets the Tids from the spawn handles). Each elector still receives exactly one
   `Setup` from a single sender, causally after all spawns, so I expect the same behaviours; but the
   thread count is one higher than Rust's, and `boot` is a role in `rolesInSpawnOrder()`.
2. **Argument format.** The Rust variant is two words before `<n>`; I put it after the model as two
   comma tokens (`impl,2,fifo,leader,conf`). Not sure this is the harness's expected spelling.
3. **`size()`** returns the Rust ARGS words, e.g. `leader conf 2`. The spec run therefore reports
   the variant words it was called with. If the harness wants only `n`, change `size()`.
4. `fifo` = P2P and `bag` = ASYN is my reading of the model names.
5. `Nondet.chooseBoolean()` has `min(S) = true`; I did not check TraceForge's order of `nondet()`
   outcomes. The outcome set is the same.
6. Rust main never joins; Java main awaits every process (required). A blocked participant (e.g.
   `leader silent`) makes main's await block, so the execution is blocked, as expected.
7. n = 1 is accepted (Rust accepts it too); fixtures start at n = 2.

## Measured counts (`must 0 1 report - 600`, plain enumeration, all `stack exhausted`)

| args | complete | blocked |
|---|---|---|
| spec,1,fifo,leader,conf | 1 | 0 |
| spec,1,fifo,ring,conf | 1 | 0 |
| impl,1,fifo,leader,conf | 1 | 0 |
| impl,1,fifo,leader,split | 1 | 0 |
| impl,1,fifo,leader,silent | 0 | 1 |
| impl,1,fifo,ring,conf | 1 | 0 |
| impl,1,fifo,ring,split | 1 | 0 |
| spec,2,fifo,leader,conf | 4 | 0 |
| spec,2,fifo,ring,conf | 4 | 0 |
| impl,2,fifo,leader,conf | 6 | 0 |
| impl,2,fifo,leader,split | 9 | 0 |
| impl,2,fifo,leader,silent | 0 | 3 |
| impl,2,fifo,ring,conf | 4 | 0 |
| impl,2,fifo,ring,split | 6 | 0 |
| spec,2,bag,leader,conf | 4 | 0 |
| impl,2,bag,leader,conf | 6 | 0 |
| impl,2,bag,ring,split | 6 | 0 |

An export smoke run (`must 0 1 report <file> 600`) of `spec,2,fifo,leader,conf` and
`impl,2,fifo,leader,silent` was accepted by the recorder (roles check passed); the file was deleted.
`report/` is the launcher's report directory.
