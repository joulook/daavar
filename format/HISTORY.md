# Summary format: history

`SPEC.md` is the normative specification of the summary format, the join, the witness files, the measurements
and the exit codes. This file keeps the decision record from which it was written, verbatim; where the two differ,
`SPEC.md` holds.

## Decision record

# The compact summary interchange format, version 1, minor 1 (draft 2026-10-26; P5 2026-11-16)

One file per enumeration of one program, JSON Lines. Both tools write it; `conf-join` reads two.

## Header (first line)

```
{"format":1,"minor":1,"tool":"jmc"|"traceforge","program":"<name>","size":"<params>",
 "visible":["c0","c1","coord"],            // sorted ASCII role names, identical on both sides
 "roles":["c0","c1","coord","sched"],      // sorted declared names (minor 1), identical on both sides
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

`roles` (minor 1, P5 rulings 7c, 11.4 and 12.1, 2026-11-16): the visible names and the declared names of
the invisible processes whose identity can appear in a visible payload, sorted by their bytes; main
and daemons are never listed. Both tools declare such invisible processes in an optional list, empty
by default (TraceForge: a `Program` field; JMC: an `MpProgram` method), so a program that puts no
identity in a payload has `roles` equal to `visible`. A header without `minor` is minor 0 and has no
`roles`; it is read as before, with a warning that the roles were not compared.

`conf-join` requires the two files' `visible` lists to be equal, refuses two minor-1 files whose
`roles` lists differ (P5 ruling 11.5: an online check against an exported reference compares
the header's `roles`; one whose two programs run in one process compares their declared roles),
and prints both files' `program` and `size` without requiring them to agree
(ruling of 2026-10-27; P5 ruling 2: no canonical spelling, the pairing of two programs is recorded
in daavar's rows). `conf-join` (plain and merge) and the online checks (TraceForge `online`,
sequential and partitioned; JMC's online check) refuse a mismatch of `model` (P5 N1): a join
compares its two files' headers, an online check compares the specification's header with the
running implementation's model.

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
  receive that read nothing (⊥). A word holds at most 2^32-1 events (TraceForge's 32-bit counts;
  P5 ruling 11.1); `--decide` refuses a line whose counts it uses exceed 65535. JMC's practical
  limit is 2^31-1 observations per word (a Java list; P5 ruling 12.2).
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
  JOIN-TOTAL (JT-0.4, 2026-11-16): on a successful `conf-join`, `uncovered_total` is Σ `n` over the
  uncovered implementation lines when each carries a JSON integer `n` in 1 ..= `u64::MAX`; otherwise
  it is empty, and with no uncovered line it is 0. A refused join leaves it empty. Current JMC
  exports supply `n`; current TraceForge exports omit it, so they give 0 when no line is uncovered
  and empty otherwise. For a `sampled` or partial file the total counts the represented enumeration
  only; 0 does not establish conformance.
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
  unit variant is `{"t":"<variant>","f":[]}` (ruling of 2026-10-27), a Java `enum` constant
  included, with the constant's name as its tag (P5 V1). A tag is any string, the same in both
  programs; the Rust identifier is a convention, not a check (P5 ruling 4);
- lists in order; sets as lists sorted by the encoded bytes of their elements; maps are not in
  version 1 (the encoders refuse them; ruling of 2026-10-26); a map encoding (format version 2)
  comes when a visible payload needs one (P5 ruling 6). Known gap until then (P5 ruling 11.2): an
  empty Rust map prints like an empty set, and TraceForge exports it as `[]`; JMC refuses every map;
- a thread identity inside a visible payload as the name of that process, main as `"main"`; an
  identity of a daemon, of an unnamed process, or of a process other than main that is neither
  visible nor declared as payload-nameable is refused by both exporters (P5 V5, rulings 7a-7b, 11.4).
  Invisible messages are not encoded and may carry any identity, daemons included (P5 ruling 11.8);
- refused: floating-point values, arrays compared by identity, `null` other than ⊥.

## What both programs must satisfy

- The same visible role names: non-empty ASCII, not `main`, not starting with `~`, each declared
  once in the role list and once in the visible list; both exporters refuse otherwise (P5 R1).
- A daemon is never a visible name; both exporters refuse one (R-d; TraceForge from P5).
- JMC's export shapes (JMC runtime constraints, not format rules; P5 ruling 11.3, replacing the
  rulings of 2026-10-26 on shape): main is invisible and performs no message-passing event (R-a); it
  starts every non-daemon process before its first completed await (R-b) and awaits all of them,
  unless it ends blocked at an await of a blocked process or at a failed assume (R-e); no process
  other than main awaits (R-c) or starts a non-daemon process (R-f); a process other than main may
  start daemons, also after main's first completed await (P5 ruling 10; needed by `bk-sol` and
  `bk-413`). TraceForge does not adopt these: it computes each thread's status whether main awaits
  it or not, so a Rust program need not follow them as long as its Java twin uses a JMC-legal one.
  TraceForge's own rule stays: no send or receive may be causally before the creation of a declared
  visible thread (it panics otherwise).
- Per-thread visibility only. A daemon process (TraceForge `spawn_daemon`, JMC
  `MpProcess.spawnDaemon`) is never a visible role (ruling of 2026-10-27).
- The specification side raises no assertion. Neither tool exports an implementation execution
  with a failed assertion: the run fails and no line is written (TraceForge `export impl`,
  sequential and parallel, and `online --out`, sequential and partitioned; JMC). The status `err` is reserved for that case; exporting it, the
  errored status, is the known gap K1 and the first follow-up after P5 (P5 ruling 8). JMC's
  truncated graph after an error is not exported.
- Non-blocking receives may read ⊥ whether or not a message is pending, on both sides.
- Guards are type tests (the Rust side's tag matching); no value guards.
- JMC exports no iteration with a fault, a recorded error or an abandoned process.
- Communication models `p2p` or `asyn`; `cd` later; no mailbox.

## Witness files (WITNESS, 2026-11-05)

An uncovered execution of the online check is written as text by `--witness <file | dir/>` (JMC: `WitnessWriter`,
MpLauncher; TraceForge the same format): line 1 `uncovered summary (iteration <i>): <summary line>`, then per thread in
creation order (main first, `t` = position, main 0) `thread "<name>"[tid=<t>]:` and one tab-indented line per event in
program order, `k` from 0: `(<t>, <k>): BEGIN | TCREATE(<child t>:"<name>") | SEND(<destination name>, <value as in
the summaries>) | RECV() [(<sender t>, <k>)] | RECV() [TIMEOUT] (reads ⊥) | NONDET <choice> | END | BLK`; joins are not
lines. JMC also writes `ERROR(<message>)` for a last event that is an error. Names are printed verbatim; JMC refuses to
write a witness whose names hold a control character or a line separator (the format defines no escaping).

From P5 (W1-W5, ruling 11, 2026-11-16) this replaces the event list above, in both tools: a daemon's thread line is `thread "<name>"[tid=<t>, daemon]:`;
the events are `BEGIN`, `TCREATE(<child t>:"<name>")`, `SEND(<destination name>, <value>)`,
`RECV() [(<sender t>, <k>) <sender name>]`, `RECV() [TIMEOUT]`, `NONDET <option index>` (a one-option choice is not
printed), `TJOIN(<t>)` for a completed join, `BLK`, `END` for every thread that ends normally, main included;
`ERROR(<message>)` only with K1. A name or message with a control character or U+2028/U+2029 is refused. `k` is a
position within the witness: every RECV's `(<sender t>, <k>)` names a SEND line of the same file; positions are not the
other tool's event indices. `--witness <file>` writes the first uncovered execution, `--witness <dir/>` one
`witness-<n>.txt` per distinct uncovered summary in order of first occurrence; refused under JMC `--split` and
TraceForge `--threads N>1`. TraceForge writes witnesses from `online` and `conf --engine stateful`. The `--decide`
witness of `conf-join` is a different output and keeps its form.

## Measurements and run ends (P5, rulings 12-14, 2026-11-16)

Both tools print `--- measurements` and `key: value` lines; the 23 shared keys keep their names and order, tool-specific
keys follow, readers ignore unknown keys, and a value a tool cannot measure under the definition is empty. From the P5
pins (rows before and after are never mixed in one table):
- `graphs_complete`, `graphs_blocked`: distinct canonical graphs of the enumeration that end complete or blocked.
- `max_graph_events`, `mean_graph_events`: per summarised execution, the number of witness event lines it would have
  (BEGIN, TCREATE, SEND, RECV, NONDET with two or more options, TJOIN, BLK, ERROR, END) over all threads; maximum and
  mean over summarised executions.
- `max_message_events`: per summarised execution, its sends and receives over all threads; maximum.
- `max_visible_events`, `mean_visible_events`: per summarised execution, the events of its visible words; maximum and
  mean over summarised executions (not over distinct lines).
- An execution refused at summarisation counts in none of these.
- `uncovered_total` in `conf-join`: JOIN-TOTAL above.
- `stop_reason`: `exhausted`, `budget`, `first-miss`, `error`, `timeout` for a tool's own wall-clock limit, or
  `threshold` (TraceForge) when a thread passes `--threshold N` events; `export` refuses a threshold run. A run
  whose budget equals the state space stops with `exhausted` and exports `complete` (P5 M3); in
  TraceForge only when no revisit or saved state is left at the stop (a queue of infeasible or
  pruned entries still gives `budget`).
- Exit codes: a checker that reached a verdict exits 0, a failed run 1, a refusal or bad arguments 2; documented
  exceptions: `conf-join` exits 1 on a violation, `conf` exits 1 when it reports and 2 when inconclusive (P5 M4).
