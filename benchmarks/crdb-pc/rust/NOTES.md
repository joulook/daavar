# crdb-pc: S_pc, the hand-written specification (2026-11-05)

Interface: plan/case-studies/cockroach/INTERFACE.md (message types and client loops verbatim). Plan: STUDY-PLAN.md,
section "Specification S_pc". Properties: ParallelCommits.tla, `AckImpliesCommit` l.139 and the stability clauses of
`TemporalTxnRecordProperties` l.109-120 (`[](RecordCommitted => []RecordCommitted)`, the aborted twin,
`[](Committed => []Committed)`). Liveness (`<>[]RecordFinalized`, `ImplicitCommitLeadsToExplicitCommit`,
`AckLeadsToExplicitCommit`) is not checked.

## Revision 2 (2026-11-06, after Astra's review plan/case-studies/cockroach/derived-review/REVIEW.md)
Coordinator's three changes, all applied:
1. No Retry. With no failures, blind Puts only and refreshes that always succeed, the code has no path to a retry
   error (REVIEW.md item 3: RFC l.194 and l.249 name the general retry causes; l.245 makes a refresh an internal
   restart of the TxnCoordSender; l.390 maps a prevented/pushed pipelined write to a failure/push; l.377 belongs to
   ambiguous results, excluded here). S4 is now "Retry never occurs": S_pc never sends `Retry`, so a `Retry` from an
   implementation is a violation to investigate. The client loop stays verbatim (it still accepts `Retry`
   syntactically); `Rep::Retry` carries `#[allow(dead_code)]` on its own line above the verbatim type.
2. An abort needs an actor. The only actor that can abort T here is a preventer's push or recovery (no coordinator
   rollback, no failed replication). `Aborted` to c is allowed only if the fate is already Aborted, or the fate is
   Undecided and some preventer's Recover has been received by `pc` and not yet answered. That pending preventer
   stands for a handle whose push aborted the missing/PENDING record before T's EndTxn, or whose recovery prevented a
   final-batch write and aborted the record (RFC l.308 "if an intent was prevented, abort the transaction"; l.346
   "the abort would have beaten the Stage(k2)"; REVIEW.md item 2). The reply fixes the fate Aborted. `Aborted` with no
   Recover received is forbidden. For this rule the Recover receipt is now its own action (`RecvP(i)`), separate from
   the reply (`Serve(i)`). That also gives the causal shapes REVIEW.md lists under "exact causal graphs" (a Recover
   received before `Written`, answered `Committed` after the Commit; several Recovers received before any is answered).
3. (n,p) = (1,0) is rejected: exit 2 with "crdb-pc: (n,p) = (1,0) is a one-phase commit, which the interface
   excludes; ...". Every other bad argument also gets a named error and exit 2.
Kept: `Committed` to c whenever the fate is not Aborted (it stands for the writes succeeding and the record staged,
RFC l.91-93, l.98, including after a write was prevented at an old timestamp, when the refresh and the re-sent EndTxn
beat the recovery's abort, RFC l.308, l.334; REVIEW.md item 1). A preventer served while the Commit is outstanding
chooses Committed or Aborted by nondet: an existential abstraction of "all writes present and staged" (RFC l.93,
l.296) versus "a write prevented, or the record pushed, first". A preventer served while no Commit is outstanding and
the fate is Undecided replies Aborted. A preventer served after the fate is decided reports the fate (agreement, RFC
l.350).

## Shape
One invisible object thread `pc`; visible `c`, `p1`..`pm` (the interface's client code, attached directly to `pc`);
invisible `init` sends `pc` the client addresses (a tagged setup message). State: `fate in {Undecided, Committed,
Aborted}`, `outstanding` (c's Commit received by `pc`, not yet answered), c's Puts, and for each preventer `received` /
`served`. At each step `pc` picks one enabled action by `nondet()`. It ends when `c` has ended and every preventer has
its reply (no client is left waiting; `blocked=0` in every run below). `a` is parsed but irrelevant: without Retry,
c ends at its first Commit reply.

## Reply rules (each with its clause)
| Action | Enabled when | Effect / reply | Follows |
|---|---|---|---|
| receive c's `Put(k)` | c not ended, no Commit outstanding | reply `Written(k)` at once | interface (a pipelined write is answered after evaluation); no S-clause constrains it |
| receive c's `Commit(keys)` | same | `outstanding := true` | defines the outstanding interval of S3 |
| reply `Committed` to c | outstanding, fate != Aborted | fate := Committed; c ends | S1, S3; TLA+ `AckImpliesCommit` l.139, `[](Committed => []Committed)`; RFC l.91-93, l.98, l.308, l.334 |
| reply `Aborted` to c | outstanding, and (fate = Aborted, or fate = Undecided and some Recover received and not answered) | fate := Aborted; c ends | S1, S2; TLA+ `[](RecordAborted => []RecordAborted)`; RFC l.308, l.346 (an abort needs a preventer) |
| reply `Retry` to c | never | none | S4 "Retry never occurs" (RFC l.194, l.245, l.249, l.390 under this interface; REVIEW.md item 3) |
| receive p_i's `Recover` | p_i not yet received | p_i pending | enables the abort rule; no reply |
| reply `Recovered(f)` to pending p_i | p_i pending | fate decided: f = fate. Undecided and outstanding: nondet Committed (fate := Committed) or Aborted (fate := Aborted). Undecided and not outstanding: Aborted (fate := Aborted) | S1 (agreement, RFC l.350), S2, S3; TLA+ `AckImpliesCommit`, record stability l.113-114; RFC l.93, l.296, l.308 |

## Decisions taken without asking (recorded here)
- D-a Crate/package name `bench_crdb_pc` (the prompt) rather than `crdb_pc` (INTERFACE.md "crate `crdb_pc`"); the
  program entry is otherwise as INTERFACE.md says.
- D-b (revision 1; superseded by revision 2) `Retry` was allowed. It is now forbidden: see Revision 2, item 1.
- D-c The outstanding interval is taken at `pc` (receipt of Commit to the reply), a sub-interval of c's send-to-receive;
  since `pc` may delay every other action, every placement of a preventer's reply relative to c's Commit is still
  reachable.
- D-d (revision 1; superseded by revision 2) `Aborted` to c with an Undecided fate needed no preventer. It now needs
  a pending preventer: see Revision 2, item 2.
- D-g (revision 2) Preventers are received in any order (no symmetry reduction). Fixing the order p1 before p2 would
  cut executions, but it changes which Recover can causally precede c's `Aborted`, so it is not applied.
- D-e The Commit's key list is not inspected (only that it arrives after the p Puts of the epoch; asserted).
- D-f The optional `[fifo|bag]` trailing ARG is ignored by the parser (the model comes from `--model`), as in bk-ledger.

## Compile
`TMPDIR=/local/home/mkhoshechin/tmp cargo build --release -j 2`: Finished, no warnings, no errors (revision 2).

## Counts, revision 2 (`target/release/bench_crdb_pc count spec --model fifo -- ARGS`, all exhaust, exit 0)
| ARGS (n p m a) | revision 2 | revision 1 |
|---|---|---|
| 1 1 1 1 | execs=16 blocked=0 | execs=10 |
| 2 1 1 1 | execs=16 blocked=0 | execs=10 |
| 2 2 1 1 | execs=23 blocked=0 | execs=12 |
| 2 0 1 1 | execs=10 blocked=0 | execs=8 |
| 2 1 2 1 | execs=318 blocked=0 | execs=46 |
| 2 2 2 1 | execs=588 blocked=0 | execs=66 |
| 1 1 1 2 | execs=16 blocked=0 (= 1 1 1 1) | execs=22 |
| bag: 1 1 1 1 / 2 2 2 1 | execs=16 / execs=588, blocked=0 | 10 / 66 |
| 1 0 1 1 | exit 2, "(n,p) = (1,0) is a one-phase commit, which the interface excludes" | panic |
The counts rose although the spec is tighter. The cause is the separate Recover-receipt action: the receipt is placed
anywhere before its reply. These are executions, not distinct visible histories (count does not deduplicate). At m = 2
they exceed the "tens at most" of the first prompt (318, 588); every run is still exhaustive and takes milliseconds.

## Hand check at (1,1,1,1), revision 2
Object actions: P = receive Put(1) and reply W = Written(1); K = receive Commit([]); R = receive p1's Recover; S = reply
to p1; X = reply to c. Executions (16 = the count):
- p1 served before K (S before K): the fate becomes Aborted, then X = Aborted (the only enabled reply). R,S placed
  around P: (R S P), (R P S), (P R S), then K, X. 3 executions.
- p1 served while the Commit is outstanding (K < S < X): S = Committed then X = Committed, or S = Aborted then
  X = Aborted. R before P, between P and K, or after K: 3 x 2 = 6 executions.
- c answered first (X before S): X = Committed: R before P, between P and K, between K and X, or after X (4), then
  S = Committed. X = Aborted needs R before X: 3 placements, then S = Aborted. 7 executions.
Client-visible histories (c's Commit interval vs p1's reply; outcomes):
1. p1 Recovered(Aborted) before c's Put is answered; c Aborted.
2. p1 Recovered(Aborted) after Written(1), before the Commit is received; c Aborted.
3. p1 Recovered(Committed) during the Commit; c Committed.
4. p1 Recovered(Aborted) during the Commit; c Aborted.
5. c Committed, then p1 Recovered(Committed).
6. c Aborted (p1's Recover received, still pending), then p1 Recovered(Aborted).
That is 6 histories, with only the outcome pairs (Committed, Committed) and (Aborted, Aborted). Pinned down as
"p1 before / during / after the Commit": before has only Aborted, and splits into 1 and 2 by its order against
Written(1); during and after each have Committed/Committed and Aborted/Aborted. As global reply orders, 2 and 4
coincide (W, p1:A, c:A) and differ only in where c's Commit falls. Forbidden now: c Aborted with no Recover received;
any Retry; (Committed, Aborted) and (Aborted, Committed).
