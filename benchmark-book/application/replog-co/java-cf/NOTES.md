# replicated-log-esop20: context-free Java twin

Class: `bench.replicated_log_esop20.ReplicatedLog` (`java/bench/replicated_log_esop20/ReplicatedLog.java`),
public `(String...)` constructor.

Arguments: `<spec|impl>,<c>,<fifo|bag>[,<correct|early-reply>]`, `1 <= c <= 6` (as the Rust
`ARGS: <c> [variant]`); the variant is accepted and ignored on the spec side, as the Rust `parse`
does. `fifo` -> `CommunicationModel.P2P`, `bag` -> `CommunicationModel.ASYN`.
`programName()` = `replicated-log-esop20`, `size()` = `c` as text.

## Threads (spawn order as in Rust `program`)

| Rust | Java role | body |
|---|---|---|
| `coordinator` (impl) / `log` (spec) | same | `coordinator()` / `specLog()` |
| `db1`, `db2` (impl only) | same | `db()` |
| `auditor` | `auditor` (visible) | `auditor(dbs)`; dbs = [db1, db2] impl, [log, log] spec |
| `client1..client<c>` | same (visible) | `client(i)` |

Main is invisible, sends/receives nothing, starts every process and awaits all of them (in spawn
order). Visible roles: `client1..client<c>`, `auditor`. Each Rust per-thread inbox is the
process's own JMC receive queue; Rust's inbox index (0 coord/log, 1-2 db, 3 auditor, 3+i client i)
is a `Tid[]` with the same indices.

Receives: every Rust `recv_tagged_msg_block` with a tag set is a blocking `Channel.recv` whose guard
is an `instanceof` test over the same record classes (no value guards). No non-blocking receives,
no `Nondet` (the Rust program has no nondeterministic choice).

## Messages (sealed interface `Msg`, records with `@Tag` = Rust variant name, same field order)

`Event(int from, String ev)`, `Result(Verdict)`, `Request(String)`, `Vote(Verdict)`, `Dec(Verdict)`,
`Ack()`, `Read()`, `Log(String)`, `Done()`.
`Verdict` (Rust enum `Commit | Abort`) is a sealed interface with unit records `@Tag("Commit")`,
`@Tag("Abort")`, so it encodes as `{"t":"Commit","f":[]}` (a Java enum would encode as the bare
string `"Commit"`, which the format forbids for a variant). Strings concatenated as in Rust
(`log.push_str`). Rust's `unreachable!()` -> `IllegalStateException` (never reached).

## Doubts and assumptions

1. **How processes learn each other's addresses.** In Rust all channels exist before any spawn, so
   the coordinator/log can reply to clients spawned after it. With `MpProcess.spawn` a Tid is
   known only after the spawn returns, and the spawned process may run (and a client's `Event` be
   received) before main stores it. I therefore create every `MpThread(body, role)` first (this
   mints the task ids and raises the creation events in spawn order), fill the `Tid[]` with
   `Tid.of(taskId, role)`, then `start()` all in the same order and `join1()` all. This is exactly
   what `MpProcess.spawn(role, body)` does (new `MpThread`, `start`, `Tid.of(taskId, role)`),
   only split into create-all/start-all. Assumed equivalent for the checker; the export run below
   accepted the roles. Alternative not taken: `MpProcess.spawn` plus `Tid.of(k+1)` from the
   documented "k-th spawned process is task k+1".
2. `fifo` mapped to P2P (per-sender FIFO) and `bag` to ASYN; assumed this is the intended
   correspondence with TraceForge's `--model`.
3. `Vote`/`Ack` are both accepted by both coordinator collection loops (as in Rust's `[T_VOTE,
   T_ACK]` guard), so a late Vote can be consumed in the ACK loop and vice versa; reproduced
   as-is. A `Vote(Abort)` is never sent, so the verdict is always `Commit`.
4. Field names of the records (`from`, `ev`, `v`, `log`) are my choice; only tag and field order
   matter for the encoding.

## Measured (must, plain enumeration, `must 0 1 report - 600`)

| args | complete | blocked | end |
|---|---|---|---|
| `spec,1,fifo` | 1 | 0 | stack exhausted |
| `impl,1,fifo` | 8 | 0 | stack exhausted |
| `spec,1,bag` | 4 | 0 | stack exhausted |
| `impl,1,bag` | 8 | 0 | stack exhausted |
| `impl,1,fifo,early-reply` | 32 | 0 | stack exhausted |
| `impl,1,bag,early-reply` | 32 | 0 | stack exhausted |

Additional sanity check: `impl,1,fifo` export under must (`enumeration:"complete"`, visible
`["auditor","client1"]`, model `p2p`) was produced without refusal; the export file was deleted.
Not compared with any Rust count.
