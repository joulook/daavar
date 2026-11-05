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
