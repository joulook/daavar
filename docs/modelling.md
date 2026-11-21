# Modelling guide: what to rewrite, what to keep, what to abstract (draft 1, 2026-11-20)

This guide is for porting an existing implementation (an SDK, a server, a protocol library) into a program the
tools can explore. The rules are the ones the A2A and MCP case studies converged on; the examples come from them.

## 1. The shape of a port
A port is a set of sequential processes exchanging immutable messages. The real system's threads, executors, event
buses, channels and queues inside one node collapse into one process whose loop is: receive a message, update local
state, send zero or more messages. The order in which the real node would interleave its internal threads is not
modelled; the order in which messages from different processes arrive is explored exhaustively.

Keep: the control flow that decides which messages are sent in reply to which, the state machine of each object
(a task, a request, a session), the store semantics that later requests observe, and every decision point the source
makes on the message content.

Rewrite as a sequence: handler chains, publishers and subscribers inside one node, result aggregators, event queues.
If the real code hands an event to a background consumer that may run before or after the next handler, the
visible difference is an ordering of sends; model it as a `nondet` over the orders the code can produce, and
write the map row that says so.

## 2. What must be abstracted, and how
- **Intra-process concurrency.** Thread pools, futures, locks and condition variables are not visible to the tools.
  Replace each by the sequence it produces when it runs to completion, or by a `nondet` over its distinguishable
  outcomes (the event reached the stream before or after the status was stored). The A2A Java map's part on
  collapsed concurrency (one row per collapsed construct, each with the public behaviour lost) is the worked example.
- **Timers and timeouts.** A timer either fires or not before the next message: a `nondet` with two branches, placed
  where the code checks it. Wall-clock values never appear in messages.
- **Model and LLM calls.** The code distinguishes a finite set of outcomes of a model call (answer with a Message, open
  a Task and report Working first, complete at once, keep working); the port is a procedure that chooses among
  exactly those outcomes with `nondet`. The checker then explores every outcome; no model is run. See the A2A
  benchmark's `call_llm`.
- **External I/O.** Sockets, files and remote services become environment processes with a script: they answer the
  messages the port sends, with `nondet` where the real environment could answer differently.
- **Identifiers.** Generated identifiers (UUIDs, counters that depend on timing) break the comparison between
  programs. Use deterministic identifiers that preserve correlation (`t1` for the first task, `req1` for the first
  request) and the same ones in every program. Error codes that the protocol treats as opaque become one constant.
- **Payloads.** Send what the protocol compares. A message that carries a whole object graph is a sign that the
  port copied the SDK's serialisation instead of the protocol's content.

## 3. Faithfulness
The port reproduces the source's behaviour, including what looks wrong: a status sent before the object it refers
to, a stream that stays open after its final item, an error that the code swallows. Fixing it in the port hides the
finding. Every deviation forced by the tools (a value kind that cannot be encoded, a loop that cannot be
modelled) is written in the map with the source file:line and what the port does instead.

The map is a table per source file: source element, lines, model element, kind of correspondence (kept, collapsed,
abstracted, not modelled), and a reason for everything that is not "kept". Reviewers read the map before the code.

## 4. The harness
- What JMC refuses: main sends or receives a message; a process other than main starts a non-daemon; main starts a
  non-daemon process after its first completed await (daemons may start later); a process other than main awaits; main does not await every non-daemon
  (unless it is blocked on an await or an assume). Any process may spawn daemons. TraceForge's export and online
  check impose no shape rules.
- What we recommend, beyond that: main starts every process before it waits for any and awaits them all; ordinary
  processes start no processes; and the harness contract:
  - One client process drives the scenario: its sequence of requests is the scenario; its last message is a
    shutdown that reaches every process, so that no process waits forever.
  - Nothing is left unread: a message left in a mailbox at the end is behaviour the harness did not account for.
  - No daemons: a background loop with no termination is not an abstraction. Daemons exist in both tools, but a
    daemon can never be a visible role.
  An execution that ends blocked is reported as a status; it is a modelling error only when the protocol does not
  block there.
- Scenarios are small: one or two requests, two or three processes. The exploration grows exponentially with the
  number of concurrent sends; count before exporting, and grow one parameter at a time.

## 5. Two programs, one vocabulary
The specification program and every implementation program share the message vocabulary: one `msgs.rs` for the Rust
programs, mirrored field for field by the Java records. A comparison between programs with different vocabularies
compares nothing. When the port needs a message the specification does not have (an internal callback), make the
role that uses it invisible or keep the message out of the compared roles.

## 6. Checklist before the first export
1. Map written, every non-"kept" row with a reason.
2. Identifiers deterministic; no timestamps; opaque codes constant.
3. Vocabulary identical on both sides.
4. Main spawns then awaits; client ends with a shutdown; no daemon.
5. `count` at the smallest size finishes; blocked executions are zero or explained.
6. Argument errors exit 2 with a usage line; the program asserts nothing it has not modelled.
