# The compact summary interchange format, version 1 (draft 2026-10-26)

One file per enumeration of one program, JSON Lines. Both tools write it; `conf-join` reads two.

## Header (first line)

```
{"format":1,"tool":"jmc"|"traceforge","program":"<name>","size":"<params>",
 "visible":["c0","c1","coord"],            // sorted ASCII role names, identical on both sides
 "model":"p2p"|"asyn"|"cd",
 "enumeration":"complete"|"sampled"|"bounded"|"aborted",
 "seed":<int>, "iterations":<int>,       // sampled files only
 "encoding":1}
```

An `online --out` file's header also carries `first_miss_iteration`, `first_miss_wall_ms` and
`uncovered_total`, after the enumeration fields, in both tools (TraceForge `online.rs`, JMC
`SummaryExporter.OnlineFields`). Readers ignore all three. `first_miss_iteration` is 1-based and 0
means none. TraceForge's `online --threads N` with N > 1 (from the PAR-ONLINE commit) measures no
iteration and always writes 0, even when a miss occurred; `uncovered_total` above 0 shows the miss
(ruling Q29, 2026-11-01).

`conf-join` requires the two files' `visible` lists to be equal and prints both files' `program`
and `size` without requiring them to agree (ruling of 2026-10-27).

A file whose enumeration is not `complete` is never accepted as a specification. A `sampled`
file (JMC under `mp-random`, L0) is accepted as the implementation side only: an uncovered
summary in it is a real violation of a complete specification; coverage of all its lines is no
verdict. `bounded` and `aborted` files yield verdicts labelled partial.

## One line per distinct (signature, counts)

```
{"statuses":{"c0":"done","c1":"blocked","coord":"done"},
 "words":{"c0":[{"s":V},{"r":V}],"c1":[{"r":null}],"coord":[]},
 "counts":[0,1,0,1, ...],
 "n":3}
```

- `words`: per visible thread, in program order; `{"s":V}` a send observed with its value, no
  destination; `{"r":V}` a receive observed with the value read; `{"r":null}` a non-blocking
  receive that read nothing (⊥).
- `statuses`: `done`, `blocked` or `err`, read from the graph's last event of the thread, never
  from the runtime's task state. In JMC a receive still parked when the strategy blocks it is
  recorded as a Block label (not as an unread receive) and gives `blocked`.
- `counts`: a flat integer array, event-major: threads in sorted-name order, each thread's
  visible events in program order, and for each event the other threads in sorted-name order
  skipping the event's own thread; the entry is the number of that other thread's visible events
  causally before the event (porf, transitive, through invisible events; spawn and join edges
  count in porf on both sides, as TraceForge's `in_porf` and JMC's recorder do). TraceForge's
  `OrdCounts::of` exactly, without the byte-width distinction. JMC computes it with
  `ExecutionGraph.porfBefore` over the visible events, not from its clocks.
- `n`: optional; the number of complete graphs of the enumeration with this summary (information
  only). JMC writes it; TraceForge omits it (its Set index keeps no counts); readers accept both
  and never compare it (ruling of 2026-10-27).
- The order of lines is not significant; a reader compares the lines as a set.

Event positions are per visible thread, counting message-passing events only (sends and
receives, in program order), on both sides: TraceForge's index 0 (Begin) and its Choice labels
do not count, JMC's send index does not serve, and a one-option nondet (no event in JMC, a Choice
in TraceForge) does not count either.

Nothing else: no native hashes, no thread identifiers, no insertion order, no spawn/join edges. The reader recomputes
the bucket from the decoded words and statuses and compares signatures by the decoded values.

## Values

Canonical, compared by the bytes of this encoding on both sides:
- integers as JSON integers within signed 64 bits; booleans; strings;
- records, tuples and every enum variant as `{"t":"<declared tag>","f":[v1,v2,...]}`, the tag
  declared identically in both programs (not the Java class name, not the Rust type name); a
  unit variant is `{"t":"<variant>","f":[]}` (ruling of 2026-10-27);
- lists in order; sets as lists sorted by the encoded bytes of their elements; maps are not in
  version 1 (the encoder refuses them; ruling of 2026-10-26);
- a thread identity inside a payload as the role name string;
- refused: floating-point values, arrays compared by identity, `null` other than ⊥.

## What both programs must satisfy

- The same visible role names, ASCII, declared once; main invisible, spawning every process
  first and awaiting all of them; no visible thread spawns or joins another thread.
- Per-thread visibility only. A daemon process (TraceForge `spawn_daemon`, JMC
  `MpProcess.spawnDaemon`) is never a visible role (ruling of 2026-10-27).
- The specification side raises no assertion; the implementation side's assertion failures are
  reported as verdicts (status `err`), and JMC's truncated graph after an error is not exported.
- Non-blocking receives may read ⊥ whether or not a message is pending, on both sides.
- Guards are type tests (the Rust side's tag matching); no value guards.
- No spawned process awaits another; main performs no message-passing event at all (it spawns
  every process first and awaits all of them afterwards); JMC's recorder refuses a program that
  breaks this (ruling of 2026-10-26).
- JMC exports no iteration with a fault, a recorded error or an abandoned process.
- Communication models `p2p` or `asyn`; `cd` later; no mailbox.
