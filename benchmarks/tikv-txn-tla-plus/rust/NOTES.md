# bench_tikv_tla_plus: S_dt+, the second reference of the TiKV study

S_dt+ = S_dt (`../../tikv-txn-tla/rust`, `src/main.rs` md5 `360590b7d87f15f6b14bf3cc49ee4d80`, Astra r4
ACCEPTED; identical to `benchmarks-cf2/tikv-txn-tla/rust/src/main.rs`) with exactly three changes A, B, C
(owner's decision 2026-11-10 18:40; causes A, B, C of `plan/case-studies/tikv/VERDICT-correct-r1.md`).
S_dt stays the paper's reference; S_dt+ only tests the attribution. This program: `src/main.rs` md5
`f6f6dad5ca894a79763e6029cd490472`. Every change is marked `// PLUS-A:`, `// PLUS-B:` or `// PLUS-C:` in
the source. S_dt's NOTES.md (D1-D17) applies to everything else.

Citations: **DT:n** = `DistributedTransaction.tla` (pingcap/tla-plus 5649f86); **C:** =
`plan/case-studies/tikv/client-src/src/transaction/` (client-rust 1b883bf); **T:** =
`plan/case-studies/tikv/tikv-src/src/storage/txn/` (tikv 96033a2).

## 1. The three changes

### A. A fresh for_update_ts at every lock_key

- S_dt: the first `lock_key` of every key carries `for_update_ts = start_ts` (S_dt main.rs:842-846, 914-915;
  DT:255, 259-263; S_dt NOTES D15). Only the resend after a write conflict takes a new timestamp (DT:289-291).
- S_dt+ (`Handler::lock_key`): `self.for_update_ts = self.next_ts()` before the first request of each key.
  The resend after a write conflict is unchanged (DT:289-291). A resend after lock resolution keeps the
  timestamp (the request is re-sent as it was, as client-rust's plan re-dispatches the same request).
  `for_update_ts = start_ts` at `Begin` stays; it is now only the initial value. It is still used by
  optimistic clients and by the report to `sup`. The first request is never stale, so R2 point (d) of
  S_dt D8 is never taken on it.
- TiKV behaviour admitted: client-rust's `pessimistic_lock` takes one timestamp per call (C:transaction.rs:887-888)
  and pushes it into the transaction's for_update_ts, which is the maximum (C:transaction.rs:1253-1262).
  TiKV grants the lock iff no commit record is newer than for_update_ts (T:actions/acquire_pessimistic_lock.rs:167-194).
  DT's `ServerLockKey` (DT:507-523) makes the same test and is unchanged.
- Nothing more: the timestamp comes from the same oracle, after the `Put` arrives at the handler, as in
  client-rust. The server test, the retry bound `a` and DT:275 (a success counts only at the current
  for_update_ts) are S_dt's. The only new freedom is the timestamp's value, which is now taken after the
  request.

### B. Removable pessimistic secondary locks

- S_dt: a `lock_key` lock on a key other than its transaction's primary leaves only by its owner's prewrite,
  or by a `resolve_rollbacked` after the primary has been prewritten and rolled back. `check_txn_status`
  with `resolving_pessimistic_lock` unlocks only the primary (DT:648-652). When the primary is missing it
  does nothing (DT:679-680). (S_dt NOTES D7 "Quiescence".)
- S_dt+ (`Handler::run_jobs`, `CheckDone` branch) has one new key message, `KeyMsg::PessimisticRollback
  { start_ts, ack, cid }`. The handler sends it when all of the following hold:
  - the `CheckDone` is the current check of a live job;
  - the lock the job's request met is a `lock_key` lock, on a key `k` other than that lock's primary;
  - the check ended with outcome `Unlocked` (DT:648-652) or `Unchanged` (DT:679-680).

  The message goes to `k`. At `k`, if the lock is a `lock_key` lock of `start_ts`, it is removed with no
  record (otherwise nothing changes). The key acknowledges with `ResolveDone { ack_tag(cid, k) }`. That tag
  is free, because those two outcomes send no resolution message. The acknowledgement goes through the
  handler's `out_resolve`, as resolution acknowledgements do (S_dt D11). The request is re-sent through its
  existing watch on `k`: when the removal changes `k`, the watch fires (S_dt D7). The send is
  deterministic, as client-rust's is.
- TiKV behaviour admitted: client-rust handles a pessimistic lock whose CheckTxnStatus answer is not
  `Locked` (C:lock.rs:423-434). It sends `pessimistic_rollback_lock` for that one lock, unless the lock is
  its transaction's primary (C:lock.rs:189-217, 194-199), and then retries the request. TiKV's
  PessimisticRollback unlocks a pessimistic lock of that start_ts and writes nothing
  (T:commands/pessimistic_rollback.rs:60-90). The server unlocks the expired pessimistic primary without a
  rollback record (T:actions/check_txn_status.rs:46-64), which is DT's unlock branch. When the primary is
  missing it answers `LockNotExistDoNothing` (T:actions/check_txn_status.rs:260-270), which is DT's
  unchanged branch.
- Nothing more:
  - The removal is restricted to `lock_key` locks of that start_ts. A prewrite lock is never removed.
  - TiKV's guard `lock.for_update_ts <= request.for_update_ts` is vacuous here. A transaction creates at
    most one `lock_key` lock per key: one job per key, one request in flight per job, and the job ends at
    its success (S_dt D10). So a lock of that start_ts at `k` is the lock the request met.
  - No record is written, as in TiKV.
  - The other client-rust branches are not added. With the outcome `RolledBack` or `CommitFound`, DT's own
    resolution message is unchanged; see section 3, item 5.

### C. Async-commit and 1PC commit timestamps

- S_dt: `async` and `onepc` are ignored (S_dt D14, DECISIONS D-3). commit_ts is always `next_ts` (DT:335).
  The optimistic prewrite rejects any write with `ts >= start_ts` (DT:550-554).
- S_dt+, when `async = 1` or `onepc = 1` (`Params.async_commit`, `Handler.async_commit`):
  - Each prewrite message carries `async_fut = Some(for_update_ts)`. That is the transaction's
    for_update_ts for a pessimistic client and 0 for an optimistic one (C:transaction.rs:1425-1435).
  - The key computes `min_commit_ts = max(max(max_ts, start_ts), for_update_ts) + 1` where it writes the
    lock (`Key::min_commit_ts`; T:actions/prewrite.rs:940-973) and returns it in `Prewrited`.
  - The handler's commit_ts is the largest `min_commit_ts` of the prewrites, with no oracle call
    (C:transaction.rs:1386-1388, 1471-1481; 1PC: T:commands/prewrite.rs:953-969). It then sends DT's
    commit to the primary, as S_dt does.
  - The optimistic conflict test is strict: `w.ts > start_ts` (T:actions/prewrite.rs:584-592; equality "is
    ok to continue", 569-571).
- Per-key `max_ts` (`Key.max_ts`, initially 0). It is raised where TiKV calls `update_max_ts` among the
  requests S_dt has:
  - a read, to its start_ts, before the lock check (T:../mod.rs:3585);
  - `check_txn_status`, to max(lock_ts, caller_start_ts) (T:commands/check_txn_status.rs:88-104; DT's
    check has no current_ts, and caller_start_ts = NoneTs, DT:247);
  - a `lock_key` with `need_value`, i.e. a GetForUpdate, to its for_update_ts
    (T:actions/acquire_pessimistic_lock.rs:76-82). No scenario has a GetForUpdate.

  A prewrite of a `Put` reads max_ts and does not raise it. TiKV raises it only for Insert and
  CheckNotExists (T:actions/prewrite.rs:72-77, 224-230), which this interface does not have.
- `max_ts` is not part of a key's `state()`, so it changes no version and fires no watch. Only the
  `min_commit_ts` computation reads it, and a `key_is_locked` answer does not depend on it (S_dt D7, Lemma).
- With `async = onepc = 0`:
  - Every prewrite carries `async_fut = None` and the reply carries `min_commit_ts = 0`.
  - commit_ts = `next_ts`, and the test keeps S_dt's `>=`. The two tests coincide there: every commit_ts
    comes from the oracle, so no record other than the transaction's own rollback has ts = start_ts.
  - max_ts is written but never read.

  The behaviour is S_dt's. It is confirmed by the identical exports of L1, X2, X2r and A3 below.
- TiKV behaviour admitted: the async/1PC commit_ts (T:actions/prewrite.rs:943) can equal a later
  transaction's start_ts (T:components/txn_types/src/write.rs:75-83). TiKV's optimistic test lets that
  transaction continue.
- Nothing more, with two documented approximations:
  - S_dt+'s max_ts is never above TiKV's in the same history. It is kept per key (the owner's choice;
    INTERFACE 6 and the port keep it per region, so one region at 1PC), and DT's check carries no
    current_ts. So a commit_ts in S_dt+ can be smaller than the port's in a corresponding execution.
  - TiKV forbids a prewrite after the transaction's own rollback collided with another commit record at
    ts = start_ts (`has_overlapped_rollback`, T:actions/prewrite.rs:571-578; write.rs:75-83). DT has no such
    flag; its `rollback` writes no record when a write with ts = start_ts exists (DT:383-399), so the strict
    test could admit that prewrite.

  Checked unreachable at the async/1PC sizes of this round: a scratch copy (build directory only) asserted
  false in `rollback` whenever a commit record with ts = start_ts exists. Each `count spec --model fifo`
  ran 202497 executions, exhausted, with 0 failure lines: `X2 1 0 1` in 108 s, `X2 0 1 1` in 116 s. The
  same copy counted 40384 prewrite evaluations at each size that pass only through the strict test (the
  equal-timestamp case).
  - The DT lock's `min_commit_ts` (the push field, DT:659-666) is not set from the async min_commit_ts;
    DT's push and `ServerCommit` are unchanged.

## 2. Unchanged

- Visible messages, client code, scenarios, CLI, positional `fifo|bag` check, R1, R2.
- The watch, quiescence and termination bookkeeping (D5, D7, D8, D11).
- Invariants (D12), variants (refused), and the summary program name: export with `--program tikv_txn`.

Non-behavioural edits:
- the header doc comment;
- the error prefix `bench_tikv_tla_plus:` in `main`;
- the package name `bench_tikv_tla_plus` (Cargo.toml, Cargo.lock; same traceforge path dependency).

## 3. Interpretations and remaining differences (stated, not resolved)

1. B is deterministic, as client-rust, not a `nondet()` choice. The owner's "may" is read as a rule of
   the specification, as R1's "may" is.
2. C's max_ts raise sites follow "as TiKV does": reads, CheckTxnStatus and GetForUpdate. A `Put` prewrite
   does not raise max_ts (section 1 C). The owner's wording "every read and prewrite raises" was read this
   way, so that S_dt+ does not exclude TiKV histories.
3. C's strict test applies only to async/1PC prewrites. Under 2PC it is equivalent to `>=` (section 1 C).
4. A is not monotone: it removes S_dt behaviours (section 5).
5. Not changed (not among A, B, C), and possibly relevant at M2: after a check at a primary that was
   already prewritten, DT's `ResolveRollbacked` rolls back the pessimistic secondary with a rollback
   record. Client-rust sends only PessimisticRollback (no record) there too, because the encountered lock
   is pessimistic (C:lock.rs:423-434). A record ts = start_ts(T) > start_ts(U) aborts a later optimistic
   prewrite of U at that key (DT:550-554). None of the sizes run here showed an effect.

## 4. Build and runs (this workstation, 2026-11-10)

    TMPDIR=/local/home/mkhoshechin/tmp CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/claude-build/tikv-plus \
      cargo build --release -j 2 --offline

The build prints S_dt's one warning (`Op::GetForUpdate` never constructed). Export, one size at a time,
under `timeout 3600`:

    bench_tikv_tla_plus export spec --out <dir>/export.jsonl --threads 16 --program tikv_txn \
      --size <sc>,<as>,<op>,<a> --model fifo -- <sc> <as> <op> <a>

Join, under `timeout 3600`: `conf-join --merge --decide <spec export> <impl export>`.
- conf-join: `/local/home/mkhoshechin/tmp/claude-build/daavar-tf/release/conf-join`.
- The port's and S_dt's exports are the cluster ones,
  `/home/mkhoshechin/.claude/jobs/28580aa5/tmp/tikv-b09/exports/C3E.tikv-txn-{rust.impl,tla.spec}_<size>_fifo[_f1|_f2].t16.cinf/export.jsonl`.

Outputs:
- exports: `/local/home/mkhoshechin/tmp/claude-build/tikv-plus/exports/`;
- joins: `/local/home/mkhoshechin/tmp/claude-build/tikv-plus/joins/`.

## 5. Results

In every export, `stop_reason: exhausted`, blocked 0, and no assertion line. Join entries are written
"uncovered (real / false alarms)", with real and false alarms from `--decide`. "conforms" means 0 uncovered.

| size (fifo) | S_dt+ execs | S_dt+ summaries (S_dt) | wall | port correct vs S_dt+ | port control vs S_dt+ | S_dt as impl, S_dt+ as spec | S_dt+ as impl, S_dt as spec | port vs S_dt (for comparison) |
|---|---|---|---|---|---|---|---|---|
| S1 0 0 1 | 3682 | 88 (62) | 0.3 s | conforms | - | 15 (2 / 13) | 4 (4 / 0) | 4 (4 / 0) |
| X2 0 0 1 | 267909 | 22 (22, identical) | 16.4 s | conforms | - | conforms | conforms | conforms |
| X2 1 0 1 | 202497 | 14 (22) | 11.5 s | conforms | - | conforms | 6 (4 / 2) | 2 (2 / 0) |
| X2 0 1 1 | 202497 | 14 (22) | 10.6 s | conforms | - | conforms | 6 (4 / 2) | 2 (2 / 0) |
| X2r 0 0 1 | 660573 | 136 (136, identical) | 40.5 s | conforms | f2: 50 (50 / 0) | conforms | conforms | conforms; f2 50 (50 / 0) |
| P2 0 0 1 | 2771690 | 820 (96) | 170.5 s | conforms | - | 64 (4 / 60) | 240 (240 / 0) | 142 (142 / 0) |
| L1 0 0 1 | 563 | 56 (56, identical) | 0.1 s | conforms | f2: 22 (22 / 0) | conforms | conforms | conforms; f2 22 (22 / 0) |
| A3 0 0 1 | 5612572 | 2648 (2648, identical) | 293.5 s | 24 (0 / 24) | f1: 34 (10 / 24) | conforms | conforms | 24 (0 / 24); f1 34 (10 / 24) |

"identical" = the same set of summary lines as S_dt's cluster export, header included. These are the
optimistic 2PC sizes, where A, B and C do not apply. A3's wall time (S_dt on the cluster: 114 s) ran
beside the scratch counts of section 1 C.

Not run here (left to the cluster): M2 0 0 1 (S_dt's export took 3735 s) and X2 0 0 1 under `bag`
(13221 s).

Reading:
- **Attribution.** The correct port conforms to S_dt+ at every size run here. A3 has 24 uncovered
  summaries, all false alarms by Decide, as against S_dt. The 4 (S1), 2 + 2 (X2 async/1PC) and 142 (P2)
  real violations against S_dt are all covered. The controls still violate, with the same counts as
  against S_dt: A3 f1 10 real, L1 f2 22, X2r f2 50. So S_dt+ does not admit the injected faults.
- **S_dt+ adds behaviours.** At S1, X2 async/1PC and P2, S_dt+ has real summaries outside S_dt
  (4, 4, 4, 240). These include the port's previously uncovered ones.
- **S_dt does not refine S_dt+ at S1 and P2** (the owner's expectation was that it does). Decide finds 2
  (S1) and 4 (P2) S_dt summaries that are real visible traces outside S_dt+. Each has the same shape: a
  pessimistic client gets `Aborted` at a `Put` from a write conflict, and the conflicting transaction's
  `Committed` is not causally after that `Put`.
  - Example: S1, c1 counts `[0,0,0,7]`, c2's `Committed` count 1. P2: c2's `Committed` count 3, i.e.
    after c1's first `Put` but not after its second.
  - In S_dt the conflict is with `for_update_ts = start_ts` (D15), which was taken at `Begin`.
  - In S_dt+, as in client-rust, the for_update_ts is taken after the `Put`. A conflict then needs the
    other commit_ts to come later from the oracle, which orders that commit after the `Put`.
  - The port has none of these 6 summaries: a join with the port's export as spec and these 6 lines as
    impl gives 2 / 2 and 4 / 4 real.

  So A replaces S_dt behaviours that are artefacts of D15; it does not only add behaviours. At L1, X2 (all
  three), X2r and A3 S_dt refines S_dt+. At X2 async/1PC it does so with fewer summaries in S_dt+ (14
  against 22), because no commit_ts there comes from the oracle any more.
- **Supplementary, with the port's export as spec** (how much more than the port each reference admits;
  uncovered, real):

  | size | S_dt | S_dt+ |
  |---|---|---|
  | S1 | 30, 22 | 28, 28 |
  | P2 | 76, 48 | 364, 324 |
  | X2 1 0 1 | 4, 4 | 2, 2 |
  | X2 0 1 1 | 12, 12 | 6, 6 |

  The other sizes are equal for S_dt and S_dt+. In the not-found lines conf-join lists, every summary of
  S_dt+ that the port lacks has at least one `Aborted` client: the R2 aborts of S_dt, now after more lock
  removals. None has every client `Committed`.
