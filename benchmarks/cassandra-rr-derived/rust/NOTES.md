# cassandra-rr-derived: the derived specification (Rust, TraceForge Must API)

Source read: `plan/case-studies/cassandra/read-repair-doc.adoc` (Cassandra 4.1 read-repair page),
`plan/case-studies/cassandra/rr/INTERFACE-agents.md`, TraceForge `lib.rs` and `program.rs`, and the
rust template. Nothing else.

## Shape

- Threads: `n0`, `n1`, `n2` (in that order), then `c0`, `c1`, spawned by the entry closure, which
  then sends one invisible `Int::Setup(ids)` to `n0..n2` and joins everyone. Client code, `Req`, `Rep`,
  `value`, `CLIENTS` are verbatim from INTERFACE.md.
- `n0`, `n1`: coordinators of `c0`, `c1`. They turn each request into steps on the abstract state
  and send the reply (`Ack`, `ValueA`, `ValueAB`) to their client.
- `n2`: holds the abstract partition state. No replica, digest, repair message or timestamp is
  modelled. The state is:
  - `lo_a`, `lo_b`: per cell, the floor: the value every later QUORUM read of that cell sees at least;
  - `pending`: values of writes that have started but have not yet reached a quorum.
- Every thread ends: `n0`/`n1` after their client's q requests, `n2` after exactly
  sum over clients of (2·writes + reads) steps. No execution ends blocked (`blocked=0` in all runs).
- `spec` and `imp` both return this program; `visible` = `c0`, `c1`; args `<q> <blocking|none>`.

## Parts and the sentences they follow

1. **A write is pending, then committed, then acked.** Coordinator sends `Begin(v)`, then
   `Commit(v)`, waits for `Committed`, then sends `Ack`. Between `Begin` and `Commit` the write is in
   `pending`; `Commit` removes it and raises both floors to at least v (both cells are written together).
   The position of `Begin`/`Commit` relative to the other client's reads is not a `nondet()`: it is the
   arrival order at `n2`, which the checker explores.
   - Follows: "a failed quorum write made a write of the most up to date value only to a minority of
     replicas"; "When ... a write fails to reach a quorum of replicas, it may be visible in one read".
     With every message eventually delivered (INTERFACE), "fails to reach a quorum" can only mean:
     a read ran while the write was still on a minority. That is the `pending` window.
   - Ack after commit: INTERFACE ("`Ack` once a quorum of replicas has applied the mutation").
2. **What a quorum read of one cell returns**: the floor, or the value of any pending write newer than
   the floor (`candidates`; one `nondet()`-chain choice `choose` among them).
   - Floor: "'Quorum' means majority of nodes among replicas", "The most up-to-date data is returned to
     the client" (reconciliation of the replicas read): a quorum read intersects the write quorum of every
     acknowledged (committed) write, so it never returns less than it.
   - Pending values may or may not be seen: "it may be visible in one read".
   - Never an older pending value below the floor: reconciliation returns the most recent of the replicas
     read ("based on the timestamps the most recent replica is selected").
3. **`blocking`: monotonic quorum reads.** After a read returns x for a cell, that cell's floor becomes x
   (before the reply is sent).
   - Follows: "in 2 successive quorum reads, it's guaranteed the 2nd one won't get something older than
     the 1st one, and this even if a failed quorum write made a write ... only to a minority";
     "the read will block on writes sent to other replicas until the CL is reached by the writes"
     (so the returned value is on a quorum when the client gets it).
   - Only the cells read are raised: a `ReadA` raises `lo_a` only, a `ReadAB` both.
     Follows: "only the data covered by a SELECT statement is repaired by a read repair, read repair can
     break write atomicity when data is read at a more granular level than it is written";
     "Provides monotonic quorum reads, but not partition level write atomicity".
   - So after `ReadA` returns a pending v, a later `ReadAB` can return `(v, older)`.
4. **`blocking` `ReadAB`** (revised after review, round 2): the candidates are *pairs*, built jointly:
   the floor pair `(lo_a, lo_b)` and, for each pending write v, `(max(lo_a, v), max(lo_b, v))`;
   deduplicated; one pair is chosen (`nondet()` chain) and both floors are raised to it
   (`pair_candidates`). A whole-pair write that is newly observed is therefore seen on both cells or on
   neither; a split `(a > b)` is returned only when it already exists in the floors, i.e. when an
   earlier `ReadA` repaired cell a alone (`lo_a > lo_b`).
   - Follows: "read repair can break write atomicity when data is read at a more granular level than it
     is written" — the page's only cause of a partially applied write is a repair of fewer cells than
     the write covered; without such a repair every replica holds both cells of one write.
   - Round 1 chose `a` and `b` independently (subject to floors and `b <= a`); the reviewer showed this
     admits `c1: A -> 0; AB -> (1,0)` with no preceding `ReadA` that propagated a=1 alone, which the
     documented mechanism cannot produce. That history is now excluded; the legitimate
     `c1: A -> 1; AB -> (1,0)` stays admitted.
5. **`none`**: no floor is raised by reads (only by `Commit`), so `lo_a = lo_b` always. A read picks one
   value x from the candidates; `ReadA` returns x, `ReadAB` returns `(x, x)`.
   - Follows: "the coordinator will reconcile any differences between replicas, but will not attempt to
     repair them. Provides partition level write atomicity, but not monotonic quorum reads";
     "it may be visible in one read, and then disappear in a subsequent read" (a pending v seen, then
     the floor seen again).
   - `a = b` in a `ReadAB`: "Write atomicity prevents reads from returning partially applied writes";
     both cells are always written together (INTERFACE).

## Decisions where the document is silent

- D1. Reads are atomic steps at `n2` (a single point between request and reply). The order of two
  overlapping reads' steps at `n2` is independent of the order of their replies, so overlapping reads may
  reply in decreasing value order (the successive-read guarantee does not apply to them). This does not
  mean any combination of values: each read's value is constrained by the floors at its step.
- D2. A write becomes visible-possibly only once its coordinator has received it (`Begin`), and is
  guaranteed visible from its `Commit`, which precedes its `Ack`. A read overlapping a write whose
  `Commit` has been processed sees it, even if the `Ack` has not reached the client yet.
- D3. "Two successive quorum reads" is taken across clients too (any read whose step at `n2` comes after
  another read's): the blocking repair makes the value durable on a quorum, which any later reader sees.
- D4. Under `blocking`, `ReadAB` never returns `b` newer than `a`, and its pair is chosen jointly (part 4),
  not per cell. Reason: initially a=b=0; writes set both cells with one timestamp; a `ReadA` repair advances
  only a; a `ReadAB` reconciles and repairs both cells from the same replicas; reconciliation never moves
  a cell back. So every replica keeps a >= b, and a split exists only after an a-only repair. This is an
  inference from the documented mechanism plus the interface, not a sentence of the page. `b <= a` now
  holds by construction (`lo_b <= lo_a` is invariant) and is checked by a `debug_assert!`.
  Possible residual strictness, not shown to matter: with two pending writes v1 < v2 the joint rule offers
  no pair mixing them on top of the floors other than through `max`; for q <= 2 there is only one write
  (`c0`'s value 1), so this cannot arise in the four required runs.
- D5. Under `blocking`, a `ReadAB` raises `lo_b` to the returned `b`, i.e. it repairs b as well
  ("only the data covered by a SELECT statement is repaired").
- D6. Under `none`, a `ReadAB` returns `a = b` even across several concurrent pending writes (one
  candidate for both cells). Under `none` the page gives no monotonicity at all, so successive reads
  may return any candidate, newer or older, as long as it is at or above the committed floor.
- D7. Writes of two different clients are ordered by value (INTERFACE: the timestamp is the value, last
  writer wins), so a commit raises the floor to `max(floor, v)`; a pending value below the floor is never
  returned.
- D8. Not modelled: the CL TWO / 5-node example, digest vs full reads, speculative retry, diagnostic
  events, background repair. Argument about observable histories (not just "invisible"): digest reads and
  the full reads after a mismatch only decide *how* the coordinator obtains the reconciled value of the
  replicas it read, which the page says is "the most up-to-date data" of those replicas — this is what
  `candidates`/`pair_candidates` return; speculative retry and timeouts never fire under the interface (a
  reply always arrives), and background repair is removed in 4.0; diagnostic events send nothing to
  clients. I do not claim this is a proof of correspondence; the execution counts below are not counts
  of distinct client histories either.
- D9. Setup: INTERFACE says the main thread sends nothing but also that the replicas get one setup
  message; I send the `Setup` from the entry closure (the main thread) after spawning, invisible. This
  deviates from INTERFACE's literal structure; it has no bearing on the client values (reviewer agrees).
  Coordinator-to-`n2` messages are `(coordinator index, Int)` tuples; every receive of a different-typed
  message is a tagged receive filtered by sender, so no receive can take a message of the wrong type.
- D10. Internal threads end (the prompt's rule, which I followed over INTERFACE's "replicas never end").
  Also a deviation from INTERFACE's literal structure, without bearing on the client values.
  For q <= 2 there is a single write (`c0`'s value 1); larger q has more and is supported.

## Revision history

- Round 2 (2026-11-05, after the independent review `plan/case-studies/cassandra/rr/derived-review/REVIEW.md`,
  verdict TOO LOOSE under `blocking`): ReadAB under `blocking` chooses one jointly built pair (part 4)
  instead of two independent cells. `none` unchanged; zero as a candidate, cross-client per-cell floors,
  `b <= a` and the asynchronous ack path kept. NONE's source strategy is `ReadOnlyReadRepair`
  (reconciles, issues no repair mutations), per the review; this matches part 5.

## Compile and runs (inside OUT_DIR)

    cargo build --release -j 2

    timeout 600 target/release/bench_cassandra_rr_derived count spec --model fifo -- 1 blocking
    timeout 600 target/release/bench_cassandra_rr_derived count spec --model fifo -- 1 none
    timeout 600 target/release/bench_cassandra_rr_derived count spec --model fifo -- 2 blocking
    timeout 600 target/release/bench_cassandra_rr_derived count spec --model fifo -- 2 none

All exit 0. The result line of each run (`execs=… blocked=…`; stdout's literal last line is the empty
measurement field `cache_hits: ` of program.rs's block in every run):

| args | result line |
|---|---|
| 1 blocking | `execs=4 blocked=0` |
| 1 none | `execs=4 blocked=0` |
| 2 blocking | `execs=16 blocked=0` |
| 2 none | `execs=16 blocked=0` |

Also run (revision round): q=3 blocking `execs=212 blocked=0`, q=3 none `execs=212 blocked=0`.
Round 1 (independent ReadAB choice) had 2 blocking = 18 and 3 blocking = 291. Equal counts for the two
strategies after the revision are a coincidence of branch counts (one choice per read among the same
number of candidates), not equal client histories: the values differ (below).

For q=2 the visible difference: under `none`, `c1` may see `ValueA(1,1)` then `ValueAB(1,0,0)`
(non-monotonic); under `blocking` that is impossible, but `ValueA(1,1)` then `ValueAB(1,1,0)` is
possible (non-atomic), which `none` forbids.
