# Pineapple, one key, common case: derived specification (`bench_pineapple_derived`)

Sources read: PAPER-EXCERPT.md (sections 2.2, 3, Appendices A and B as excerpted), INTERFACE.md,
TraceForge `lib.rs` and `program.rs`, the rust template crate. Nothing else.

## What is specified

The externally visible behaviour of Pineapple on one key with a stable leader and no failure: per client, in
order, the requests `Write`/`Read`/`Rmw` it sends and the replies `Ack`/`Value`/`Result` it receives.

The specification is one atomic object holding the key (value initially 0). It serves the clients' requests one
at a time, in the order in which it receives them, and applies each atomically:

| request         | effect on the key | reply                          |
|-----------------|-------------------|--------------------------------|
| `Write(i, v)`   | key := v          | `Ack` to client i              |
| `Read(i)`       | none              | `Value(key)` to client i       |
| `Rmw(i)`        | key := key + 10   | `Result(key)` (the new value)  |

## Roles and messages (spec program)

- `object` (internal): receives `Setup` (tagged `SETUP_TAG` = 1) first, then exactly `2q` untagged `Req`s, each
  answered as in the table to `clients[i]` taken from `Setup`; then it ends.
- `c0`, `c1` (visible): the interface's client code verbatim; they send to the object's `ThreadId`.
- `init` (internal): sends `Setup { nodes: vec![object], clients: vec![c0, c1] }` to the object with tag 1, ends.
- `main`: spawns `object`, `c0`, `c1`, `init` and joins them; it sends and receives nothing.
- Messages: `Req` and `Rep` exactly as in INTERFACE.md; `Setup` as in INTERFACE.md.
- Program entry: `traceforge::program::main(Program { visible: ["c0","c1"], spec, imp })`, both `spec` and `imp`
  return the specification program; args `<q>`.

## Paper sentences each part follows

1. *One atomic object, linearization order = receive order.* Appendix B: "To prove linearizability, we need to show
   that under any execution of the algorithm, we can find a total ordering S which satisfies both the real-time
   constraint order and the semantic of the operations (i.e., the ordering is legal)." and "identify an ordering
   (or a sequence) S in which operations do not overlap with each other, i.e., the ordering that could occur on a
   single machine." The object is that single machine; the order in which it receives requests is S.
2. *Real-time order.* Lemma 1: "If an operation o1 occurs before operation o2, then o2 must have the same pstamp
   (this only happens when o2 is a read) or a larger pstamp." Section 2.2: the put phase "ensures real-time
   ordering". In the spec, an operation that completed (the client got its reply) was received by the object
   before any operation that starts later, so it precedes it in S.
3. *Read returns the most recent value in S (or the initial value).* Appendix B: a read "must return a null value
   (the default value), a value either from a write, or a value from a RMW" and Lemma 2: "any read d returns the
   most recent value by a write w or a RMW r in S." The object replies `Value(key)` with its current value;
   initial value 0 is INTERFACE.md's "initially 0".
4. *Writes are ordered among themselves by pstamps (non-stable, any order the tags allow).* Section 2.2: "Writes
   and reads can be ordered lexicographically using the associated tags, to obtain the desired total ordering
   for linearizability"; Appendix B "Order the writes using pstamps." Section 3 "Finally, the reason that writes
   can be ordered in a non-stable manner ...". Two concurrent writes may get either tag order (equal ts broken by
   proxy id, or different ts depending on what each get phase saw), so the spec lets either be received first.
   A write completely defines the state: "each write completely defines the state of the single object without
   any reliance on a previous state" -> key := v.
5. *RMWs are totally ordered by the leader and see the most recent value.* Section 3 correctness (ii) "the
   correctness of Multi-Paxos to order RMWs"; Algorithm 2 lines 4-5 "vmax <- value w/ largest psmax ...;
   vnew <- f(vmax)"; Lemma 2: "the vnew is the computation after applying f(.) on the value of w2", i.e. on the
   most recent write/RMW in S. The object computes key + 10 on its current value and returns the new value
   (INTERFACE.md: "the value after f was applied").
6. *Writes vs RMWs.* Section 3 Case I / Case II: a concurrent write w is ordered before r (leader observed it, or
   w' supersedes it) or after r (w's pstamp larger, "The total ordering is r, w"). Both orders are in S; in the
   spec both receive orders are possible.
7. *No other observable freedom.* Section 3: "All the operations have non-blocking execution"; in the common case
   with no failure every request completes (INTERFACE.md: every message delivered), so every request gets
   exactly one reply.

## Decisions where the paper is silent (or where I chose)

- D1. **Completeness direction (protocol can show every interleaving).** The paper proves only that executions are
  linearizable (protocol behaviours are contained in atomic behaviours). That the protocol can produce *every*
  atomic interleaving is my argument, not the paper's: run the operations one at a time in the chosen order; each
  completes in isolation (put phase reaches a quorum, so the next get phase sees it, Lemma 1's argument) and
  returns the sequential result. Since the two clients never communicate, the only constraint linking them is
  per-client program order, and every merge of the two sequences is realisable. Hence the visible behaviours are
  exactly the interleavings of the two clients' sequential operations on an atomic register-with-RMW.
- D2. **No `nondet()` call.** The only freedom is the linearization order, which is the order in which the object
  receives the two clients' pending requests (at most one per client at a time). The prompt allows "otherwise the
  order in which messages arrive", so no explicit choice is needed. Every other result is determined.
- D3. **No separate model of the leader / proxies.** Which node serves a request, Rmw forwarding to `n0`, tags,
  slots, quorums, fast paths are invisible and not modelled (prompt: "do not model nodes ..."). The clients'
  `node` argument is the object's `ThreadId` for both clients. Assumption: the conformance comparison does not
  depend on the name of a visible send's destination thread (the spec sends to `object`, an implementation to
  `n0`/`n1`). If it does, the object would have to be split under the names `n0`/`n1`, which models nodes; I did
  not do that.
- D4. **Internal thread name `object`, setup thread name `init`.** INTERFACE.md fixes only `c0`, `c1`, `n0..n2`
  and `init`; the object's name is mine.
- D5. **The object ends after `2q` requests** (prompt: internal threads end when all requests are served), unlike
  the interface's nodes, which never end. Hence the spec's executions are complete, not blocked.
- D6. **Setup buffering.** Setup carries tag 1 and the object's first receive accepts only tag 1, so a request that
  arrives before Setup stays in the queue; requests are received with the "no tag" predicate. `Setup.nodes` is
  `[object]` (there are no nodes in the spec); only `clients` is used.
- D7. **Main's spawn order** is object, c0, c1, init (the interface's "nodes in index order, then clients" with
  the object in the nodes' place; init last because it needs the clients' ids). Main joins every thread.
- D8. **Leader-change extensions (Appendix A.2) and multi-key generalisation (A.1)** are out of scope (common case,
  one key) and not reflected.
- D9. **`q` argument**: first program argument, default 1 if missing or unparsable, at least 1. The delivery model
  is the harness's `--model` option (the shared main parses it), not a program argument.
- D10. **Values are u64**; f(v) = v + 10 never overflows for the instance sizes.

## Compile and run

- Compile (inside this directory): `cargo build --release -j 2` -> `Finished \`release\` profile [optimized]
  target(s) in 35.75s` (no errors, no warnings shown).
- Run once (inside this directory): `target/release/bench_pineapple_derived count spec --model fifo -- 1`
  Last output line: `cache_hits: ` (the final line of the measurement block; the run's output was captured with
  `tail -5`, so the `execs=... blocked=...` line was not kept, and the run was not repeated, as the task allows
  only one run). Hand-derived expectation (not a run result): q = 1 gives c0 `Write(0,1)` and c1 `Read(1)`, two
  receive orders, so execs=2 blocked=0 (c1 reads 0 or 1).
