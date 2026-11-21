# Daavar: quick reference for agents (draft 1, 2026-11-20)

Load this before writing or porting a program for a conformance check; `docs/` has the detail. A check reports the
implementation behaviours, over every delivery order and `nondet` choice, that the specification does not allow.

## What a program is
- Fixed **processes** (roles), each a sequential loop `receive -> local step -> send*`; no threads, locks or shared
  memory inside a process.
- **Messages**: a tag with fields of signed-64-bit integers, booleans, strings, enums, optionals, lists, sets and
  process identities (encoded as the role name: main, a visible or a payload-nameable role). No floats, maps or
  null payloads; Rust arrays encode as lists, Java arrays are refused.
- **Visible roles** are compared: their sends and receives, their status (`done`, `blocked`, `err`), the causal order.

## API: Rust (TraceForge, `traceforge::`)
| Need | Call |
|---|---|
| start, daemon | `thread::Builder::new().name("server".to_string()).spawn(f)`; `thread::spawn_daemon(f)` |
| identity, wait | `thread::current().id()`, `handle.thread().id()`; `handle.join()` |
| send | `send_msg(tid, v)`; `send_tagged_msg(tid, tag, v)` |
| receive | `recv_msg_block::<T>()` (another type panics); `recv_tagged_msg_block(\|from, tag\| ...)` |
| non-blocking | `recv_msg::<T>()`, `recv_tagged_msg(f)`: `None` is ⊥ |
| choice | `nondet()`; `(0..n).nondet()` (`Nondet` trait) |
| entry | `program::main_with_model(Program { visible, spec, imp }, ConsType::FIFO)` |

## API: Java (JMC; `MpProgram`, `MpLauncher` in `org.mpi_sws.jmc.mp.launch`, test sources at this pin)
| Need | Call (`org.mpi_sws.jmc.api.mp`) |
|---|---|
| start | `MpProcess.create("server")` gives `MpProcess.Pending` (`.tid()`, `.start(body)`); `MpProcess.spawn("server", body)` |
| wait | `MpProcess.Handle.await()` |
| send, receive | `Channel.send(model, tid, msg)`; `Channel.recv(model)`, `Channel.recv(model, Put.class)` |
| non-blocking | `Channel.tryRecv(model[, Put.class])`: empty `Optional` is ⊥ |
| choice | `Nondet.choose(list)`, `Nondet.chooseBoolean()`, `Nondet.chooseIndex(n)` |
| message | `@Tag("Put") record Put(int req) implements MpMessage {}` (`mp.summary.Tag`, `mp.events.MpMessage`) |
| entry | `implements MpProgram`; no-argument constructor when no colon follows the class, `String[]` constructor when arguments follow |

Guards in paired programs are type tests only; ⊥ may be read with a message pending.

## The ten rules of a faithful port
1. One sequential process per real node or actor; one step per handled message.
2. Reproduce the source's behaviour, including what looks wrong. Never repair, reorder or "improve".
3. Abstract, do not delete: internal concurrency, timers and model calls become sequences or `nondet` over the
   outcomes the code distinguishes; external I/O becomes a scripted environment process.
4. Map first: a row per abstraction or deviation with source file:line, what the model does instead, and why.
5. Identifiers deterministic and correlated (`t1`, `req1`); opaque codes one constant; no timestamps.
6. One vocabulary (`msgs.rs`), mirrored field for field in Java.
7. JMC refuses: main sending or receiving; a non-main process starting a non-daemon or awaiting; main starting a
   non-daemon process after its first completed await (daemons may start later), or not awaiting every non-daemon (unless blocked on an await or assume). Any
   process may spawn daemons.
8. We recommend: main starts all, then awaits all; a client shutdown every process receives; nothing unread; no
   daemons (never visible). A blocked end is a status, an error only where the protocol does not block.
9. Send what the protocol compares, not what the SDK serialises.
10. Argument errors exit 2 with a `usage:` line; a panic or failed assertion is a program failure.

## Required files
`MAP`; `SPEC-DESIGN` (quoted sentences, readings, labelled restrictions, mutants, sizes); shared messages and
shared environment.

## Deriving the specification
Quote each sentence used; MUST restricts, SHOULD, MAY and silence become `nondet`. Two readings become an argument
(`s0`, `s1`). Unstated restrictions are labelled (`R3`) and listed; a behaviour rejected only by one is not a
violation. Rust is recommended (stateful engine); the join with merge and Decide reads either tool's exports.

## Running (`bin/daavar check <project>`; every command and flag in running.md)

## Reading the result
Each uncovered behaviour gets every class that applies: **T** violates a quoted sentence; **D** a twin difference
the text permits, or an exclusion by a labelled restriction; **A** spec too narrow (widen, record); **C** harness
artefact. Decide: violation, false alarm or unresolved. A graph may be missing (limit, false alarm, no store, no
graph, refused); replay needs the producing tool, commit and sources. An independent reader reproduces every result.

## Forbidden moves
Repairing the source; changing a program so the tools agree; a silent reading; a number not from a run; a finding
without its map row and witness.
