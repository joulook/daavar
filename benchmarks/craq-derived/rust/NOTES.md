# bench_craq_derived: the specification of 3FS's CRAQ (one chunk, one chain of three targets, two clients)

Derived only from: `craq-src/README-3fs.md` (the "Strong Consistency" line), `craq-src/specs/README.md`,
`specs/DataStorage/PSrc/{StorageClient,StorageService,MgmtService,Common}.p`, `specs/DataStorage/PSpec/SystemSpec.p`,
`craq/INTERFACE-agents.md`, and TraceForge's `lib.rs`, `program.rs` and the template crate (plus a signature grep
of `thread.rs` for `thread::Builder::name`/`spawn`, which lib.rs re-exports as `thread`).

## The specification in one sentence

The clients of the chunk observe one **atomic register**, initially 0: every Write and every Read takes effect at
one instant between its request and its reply, all operations in one total order consistent with each client's
program order; a Read returns the value of the last Write before it in that order (0 if none).

The program: one object thread `obj` holds `chunk: u64 = 0`, receives the 2*q requests one at a time in the order
they reach it, applies a Write (`chunk = v`, reply `Ack(c)`) or answers a Read (`Value(c, chunk)`), and ends after
the 2*q-th request. The clients `c0`, `c1` run the interface's client loop verbatim, with `head` = `obj` and
`targets` = `[obj]` (a direct `Req::Read(i)` to the object, as INTERFACE.md prescribes for a specification).
An invisible thread `init` sends `obj` one tagged setup message (`Setup`, tag 1) with the clients' thread ids;
`obj` receives it first (tagged receive) and the requests with an untagged receive. The main thread spawns
`obj`, `c0`, `c1`, `init` and joins them; it sends and receives nothing. Both `spec` and `imp` of `Program`
return this program; `visible` = `c0`, `c1`; the argument is `<q>` (the model comes from `--model`).

## Part by part, with the sources

1. **Strong consistency is the contract.** README-3fs.md: "Implements Chain Replication with Apportioned Queries
   (CRAQ) for strong consistency, making application code simple and easy to reason about." Taken as
   linearizability of the chunk (an atomic register), the standard guarantee of chain replication / CRAQ.
2. **Writes are applied and committed one at a time, in one order.** StorageService.p `createWriteProcess`:
   `if (sizeof(runningWriteProcs[chunkId]) > 0) { blockedWriteReqs[...] = writeReq; return ErrorCode_CHUNK_BUSY; }`
   (one running write per chunk per node; the next starts only in `eWriteReqCompleted` -> `startBlockedWriteReqs`,
   i.e. after the local commit). ChunkReplica `eWriteOpBegin`: the head allocates `metadata.updateVer + 1`
   (`writeBegin.updateVer == 0`); the successors accept only `updateVer == metadata.updateVer + 1`
   ("normal update"), anything else is STALE/COMMITTED/MISSING_UPDATE, and `MISSING_UPDATE` is asserted
   impossible ("since one write request is running over the entire replication chain, there should not exist any
   out-of-order update request along the chain"). SystemSpec.p `MonotoneIncreasingVersionNumber`: commit versions
   per chunk and target strictly increase. -> The writes have one total order (the head's version order), the
   order in which the object applies them.
3. **A write is acknowledged only after it is committed along the whole chain.** WriteProcess
   `ForwardWriteRequest`: the tail goes to `CommitLocalChunk` directly; every other target forwards and commits on
   the successor's `eCommitMsg`; `postWriteCommitted` sends `eWriteResp` to the client only at the head
   (`chainInfo.isHeadTarget && writeReq.fromClient`), after the head's local commit. SystemSpec.p `WriteComplete`
   (every client write is eventually answered with SUCCESS). StorageClient.p `eWriteResp` -> `eWriteComplete`.
   -> `Ack` comes after the write took effect; commit order is tail, t1, head.
4. **A target never returns uncommitted data; the client retries.** ChunkReplica `eReadOp`:
   `if (!readUncommitted && metadata.updateVer != metadata.commitVer) -> ErrorCode_CHUNK_NOT_COMMIT`;
   ReadProcess calls `ReadLocalChunk(..., false /*readUncommitted*/)`. StorageClient.p `eReadResp`:
   `CHAIN_VERION_MISMATCH || CHUNK_NOT_COMMIT -> reissueReadReq`. -> A visible `Value` is always a committed
   content of a clean replica.
5. **What a read returns (derived).** By 2 and 3, at any instant the replicas differ by at most the one running
   write w_{k+1}, applied on a prefix of the chain (head first) and committed on a suffix (tail first). A replica
   that answers a read is clean (4) at some version k. If it is the head, w_{k+1} has not started, so the tail is
   at k; if it is t1, w_{k+1} has not reached t1, so the tail is at k; if it is the tail, it is at k. So every
   answered read returns the tail's current committed value: the value of the last write committed at the tail.
   Taking a write's instant as its commit at the tail (between its request and its Ack, by 3) and a read's instant
   as the moment its answering replica is read, every execution is an atomic-register execution.
   - after a write is acknowledged, every later read (by any client) returns it or a later write;
   - a read overlapping a write returns either the old or the new value (old: tail not yet committed; new: committed);
   - two reads never return values in an order different from the writes' order (the tail's value only advances),
     whether by the same client or by different clients.
6. **Every atomic-register behaviour is possible.** The clients do not communicate with each other, so what they
   can observe is the pair of per-client sequences; the atomic-register behaviours are those explainable by some
   interleaving of the two programs. Each such interleaving is realized by the documented system when every
   operation completes (write committed and acked, read answered) before the next one in the interleaving is
   sent; every message is eventually delivered and the chain is fixed (INTERFACE.md), so nothing prevents it.
   The object realizes exactly these interleavings by its arrival order.
7. **Version, forwarding, commit messages, target choice and retries are not modelled** (prompt: only visible
   consequences). The read target choice of the client (`LoadBalance`) does not change the set of visible
   behaviours by 5, so the specification has none.

## Decisions where the documents are silent

- **Strong consistency = linearizability** of the single chunk (README says only "strong consistency"); the P
  model's checked properties (SystemSpec.p) are liveness and version monotonicity, not a read-value property, so
  the read semantics in 5 is derived from the model's code, not stated by it.
- **No `nondet()`.** The only freedom left by the documented behaviour is the order of the operations of the two
  clients; in the program it is the order in which the requests reach the object (message order). A read never
  has a choice of value: in the order chosen, it returns the current register value.
- **Failures, membership changes, sync, chain-version mismatch** are out of scope (INTERFACE.md: fixed chain,
  every target serving); their retries are not modelled.
- **Read retry termination.** A read in the documented system can be answered `kChunkNotCommit` repeatedly while
  writes are in progress; with finitely many writes it is eventually answered, so every read eventually returns.
- **Write values** are full-chunk contents (the chunk is one integer); a write fully replaces the value.
- **`init` and the setup message** are invisible plumbing so that `obj` knows the clients' addresses; the setup
  is tagged and the requests untagged, so the object never confuses them.
- **Termination:** `obj` serves exactly 2*q requests (each client sends q, one at a time) and ends; no client is
  left waiting, no thread ends blocked.

## Compile and runs (inside OUT_DIR, TMPDIR=/local/home/mkhoshechin/tmp)

    cargo build --release -j 2
    -> Finished `release` profile [optimized] target(s) in 1m 41s (no warnings)

    timeout 600 target/release/bench_craq_derived count spec --model fifo -- 1   -> execs=2 blocked=0   (exit 0)
    timeout 600 target/release/bench_craq_derived count spec --model fifo -- 2   -> execs=6 blocked=0   (exit 0)
    timeout 600 target/release/bench_craq_derived count spec --model fifo -- 3   -> execs=20 blocked=0  (exit 0)

The counts are the binomials C(2q, q): the interleavings of the two clients' q operations (each served
atomically by the object). Each run's output ends with the measurement block (wall_ms 0, 1, 2 respectively;
its last lines are `uncovered_distinct: 0`, `spec_graphs_explored:`, `sweeps:`, `cache_tests:`, `cache_hits:`).

## Coordinator harness change (2026-11-06)

This is a coordinator harness change made on the owner's decision (plan/case-studies/3fs/craq/INTERFACE.md, revision 2026-11-06), not part of this program's own design; its author did not make this change.

The object may now answer any read with `Rep::Unavailable(c)` instead of `Rep::Value(c, chunk)` (one `nondet()` choice per read); such a read leaves the chunk unchanged and constrains no later read, and the client loop accepts `Value` or `Unavailable` as the end of a read. The derived object is otherwise unchanged; the state counts above (C(2q, q)) predate the change (now 4 and 24 executions at q=1 and q=2).

## Coordinator change: process structure (owner decision, 2026-11-06 19:55)

This is a coordinator change made on the owner's decision (plan/case-studies/3fs/craq/PROMPT-spec-fix.md; design
and argument in plan/case-studies/3fs/craq/SPEC-FIX-DESIGN.md), not part of this program's own derivation; its
author did not make this change. It changes only the process structure and hence the causal shape of the
specification; the rules taken from the sources (parts 1-6 above: one total write order, the head's version order;
a write acknowledged only after its commit along the chain; a read answers only committed content, the last
committed write) are unchanged, and so is the owner's harness revision of 2026-11-06 (`Unavailable` for any
read, with no effect on the state), which is not from the sources.

Why: the one object served every request in one receive loop, so any two operations it served were causally
ordered (op A -> obj -> op B), also two reads of one committed version, which the atomic register does not order.
The conformance check compares causal orders (a specification summary covers an implementation summary when its
per-event counts are coordinatewise <=), so the object's extra edge made a linearizable implementation history
(two concurrent reads both returning 1) uncovered at q=2 (plan/case-studies/3fs/craq/cut-reply-review/REVIEW-astra.md).

The program now (supersedes the "The program:" paragraph, the last sentence of part 6, the "No `nondet()`" and
"Termination" decisions, and the execution counts above):
- `obj` (write serialiser; the clients' `head`): takes the pending `Write`s one at a time (the order taken is the
  write order); for each, sends `Prepare` to `p0` and `p1`, waits for both `Released`, sends `Install(v)` to both,
  then `Ack(c)` to the writer.
- `p<i>` (client i's committed-state holder): holds the last installed value (initially 0); takes a forwarded read
  or a `Prepare`, in either order; answers a read `Value(c, chunk)`; on `Prepare` answers `Released` and serves no
  read until `Install`.
- `f<i>` (client i's front end; the only element of client i's `targets`): receives client i's reads only; answers
  `Unavailable(c)` itself or forwards the read to `p<i>` (one `nondet()` per read).
- `init` sends each of `obj`, `p<i>`, `f<i>` one tagged `Setup` with the addresses. `obj`, `p<i>`, `f<i>` never end
  (like the implementations' nodes), so TraceForge classes every graph as blocked; both clients end `done` in every
  graph. The main thread joins `init` and the clients.

Causal shape: the cross-client causal order produced is exactly program order plus, for operations A before B in the
linearization, A.inv -> B.resp unless A and B are two reads of one version; an `Unavailable` read is causally
independent of the other client. Runs (fifo): q=1 6 graphs, 3 distinct summaries; q=2 52 graphs, 13 distinct
summaries; the summaries are identical to the hand-written S_lin's (same change).

Note (coordinator, 2026-11-06 23:00, Astra spec-fix review): `obj` sends both `Install`s and then the `Ack` without waiting for the installs to be received; a read after the Ack cannot see the old value because each holder serves no read between its `Released` and its `Install`. The internal processes now end blocked (the clients end `done`), so TraceForge classes every graph as blocked; the old object terminated. Conformance is decided against the causal contract of plan/case-studies/3fs/craq/INTERFACE.md (revision 2026-11-06 23:00).
