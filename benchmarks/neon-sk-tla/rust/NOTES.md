# bench_neon_sk_tla: specification program from ProposerAcceptorStatic.tla

Source: `safekeeper/spec/ProposerAcceptorStatic.tla` (PAS) and `MCProposerAcceptorStatic.tla`, neon fa50421.
Interface: `plan/case-studies/neon/INTERFACE-agents.md` (sections 4, 5.1, 8 R1-R4, 9).
Files: `Cargo.toml`, `src/main.rs`. Revision: review round 1 applied (section at the end).

## Shape

- Visible: `w0..w{p-1}`, writer code verbatim; each writer's `prop` is the thread `lg`.
- Invisible: `lg` holds one object: `log: Vec<Ent{w, j, term}>` (the newest abstract leader's log; (w, j) = record j of
  writer w), `c` (committed length), `committed` (entries 1..c), the current leader (the iteration of the election loop),
  `term`, `elected_terms`. `init` sends `lg` the writers' ids (tagged `Setup`). Main spawns `lg`, `w0..`, `init`, joins all.
- Acceptors, votes, term histories, LSNs are not data.
- Arguments `<variant> <n> <p> <r>`: variant ignored; n checked to be a string of decimal digits (any width), not read.
- `lg` works in two steps:
  1. Abstract run: every protocol freedom is a `nondet()` choice; it fixes the log, the committed prefix and each
     writer's replies (`ncommit[i]` committed records starting at position `first_pos[i]`, then Ended).
  2. Communication: first every writer with `ncommit = 0` gets `Ended` for all its records; then, for q = 1..c in log
     order, `lg` receives the Appends of the record at position q (and earlier records of its writer) if not yet received,
     and sends `Committed(q)` if that record is a reported commit, followed by its writer's `Ended` replies after its last
     commit; finally it receives every Append no reply needed.
  So each reply is causally after exactly the Appends it depends on: `Committed(q)` after the Appends of the records at
  positions 1..q, `Ended` after no other writer's Append (only after its own writer's earlier `Committed`, FIFO).

## Parts, the PAS lines followed, and decisions

| Part | PAS | Program |
|---|---|---|
| Init | l.181-206 | `log = []`, `c = 0`, `term = 0` (term 0 has no leader). |
| Records (R1) | NewEntry l.355-362 needs `state = "leader"` | A leader's records enter the log after its term start, in writer order; non-leaders' records never do. |
| Election | Vote l.245-253, BecomeLeader l.301-306, DoBecomeLeader l.279-297 | Choice `ch ∈ 0..=|remaining|`: next abstract leader among proposers that have not led, or no further election. |
| Donor log | MaxVoteAcc l.269-275, DoBecomeLeader l.284-295, TruncateWal/FindHighestCommonPoint l.317-353 | Choice `m ∈ c..=len(log)`: the new log is `log[..m]`. |
| Appending | NewEntry l.355-362, AppendEntry l.365-378 | The leader's `r` records go after position `m`, tagged with its abstract term. |
| Commit | QuorumCommitLsn l.386-411 (prev-term rule l.402-408), DoCommitEntries l.413-415, CommitEntries l.420-430 | Choice `k ∈ 0..=r`: own records committed; `c := m + k` if `k > 0`. |
| Replies (R2-R4) | none (PAS has no clients) | `Committed(m+j+1)` for `j < k`, then `Ended`; non-leaders `Ended` for all. Sent in step 2. |
| Invariants | ElectionSafety l.452-456, ElectionSafetyFull l.459, LogIsMonotonic l.463-466, LogSafety l.470-471 | `traceforge::assert`: each abstract term elected once; log terms non-decreasing; committed prefix unchanged at each election and commit; each `Committed(q)` names its writer's record j at q with j = q - first_pos; every reported commit lies in the final committed prefix; Appends carry `value(i, j, p)`. These are consistency checks of the program, not a completeness proof. |

### Observational normalization (why the abstract run covers PAS's writer-visible behaviours)

PAS does not order `BecomeLeader` by term: votes are saved (l.245-252) and election checks only their quorum and the
donor's current term (l.292, l.301-304), so a lower-term proposer holding old votes can become leader after a higher-term
election, as long as its donor is still in its term. Such a leader cannot then commit anything of its own term (no quorum
of acceptors is still at its term, QuorumCommitLsn l.386-398), so for its writer it is a leader that reports nothing, and
for the log it adds nothing to the committed prefix. The program therefore normalizes PAS runs as follows:
- Abstract leaders are the elections in the order of the abstract run; an abstract leader's term ends when the next one
  is elected (R3). A PAS leader that reports no commit can be placed anywhere (or omitted: its writer sees only Ended).
- What every later leader must keep is the committed prefix (LogSafety l.470-471 together with the quorum-intersection
  of votes and commits; the code asserts it, the assertion does not prove it). The program keeps exactly this obligation:
  `m >= c`.
- Single log: a new leader keeps a prefix of the newest abstract leader's log, not of an older branch. In PAS a donor can
  hold an older branch that a later leader dropped. If every leader since that branch reported no commit, their start
  positions are not observed, so the run where they kept that branch (larger `m`) shows the same replies. If one of them
  reported a commit, every later committed history extends its committed prefix, which the newest log holds. With p = 2
  there is no third election that could return to an older branch; at p = 3 this is the argument above (not a proof).
- Decisions where PAS is silent or where the program abstracts:
  1. Commit and term end are one step (commit `k`, then Ended). A record committed in PAS but answered `Ended` (the term
     ended before the proposer learned it) is visibly a record a later leader keeps (the `m` choice).
  2. Committing inherited entries only (PAS's zero-length record, l.230-240, `>=` at l.405) is allowed by PAS; it shows
     no reply and only narrows later `m`, so it is subsumed by `k = 0` with a later `m` that keeps them (an existential
     abstraction; it does not claim PAS needs an own-term entry before a commit).
  3. Decide, then communicate (step 2). The choices are invisible; the communication realises, for each abstract run,
     the weakest causal order its replies need. A real run with the same replies has at least these orders (a
     `Committed(q)` needs records 1..q appended; an `Ended` needs nothing from other writers), so under order inclusion
     the weakest order covers it. An `Ended` may be sent before `lg` received that writer's own Append: the writer's
     sends precede its receives anyway, so the visible order is the same as sending it right after.
  4. Abstract terms are consecutive (1, 2, ...): terms are not visible.
  5. Positions are u8 (1-based); `p*r <= 255` is asserted (values are u8 by the interface).
  6. Not modelled: RestartProposer (each proposer campaigns once, interface section 1), UpdateTerm (no visible effect).
  7. Invisible threads end when every writer is answered and every Append received; nothing waits forever (R4).

## Hand check: writer-visible histories

C k = Committed(k), E = Ended. Values: w0 sends 1, w1 sends 2, (w2 sends 3). Cross-writer orders: `Ai → Rj` = writer
i's Append causally precedes writer j's reply (writer program order Ai → Ri always holds).

(3,1,1): `[]` → (E); elect p0, m=0, k=0 → (E); k=1 → (C1). Executions 3, histories {(E), (C1)} = 2, no cross order.

(3,2,1): abstract runs by leader order `[]`, `[p0]`, `[p1]`, `[p0,p1]`, `[p1,p0]`:
- `[]`: (E,E). `[p0]`: (E,E), (C1,E). `[p1]`: (E,E), (E,C1).
- `[p0,p1]`: p0 k=0 (log [1], c=0): p1 m=0 → (E,E),(E,C1); m=1 → (E,E),(E,C2). p0 k=1 (c=1): p1 m=1 → (C1,E),(C1,C2).
- `[p1,p0]`: symmetric: (E,E),(C1,E),(E,E),(C2,E),(E,C1),(C2,C1).
- Executions: 1 + 8 + 8 = 17.
- Histories with their cross orders (each tuple has exactly one weakest order):

| w0 | w1 | cross-writer order | export `counts` |
|---|---|---|---|
| E | E | none | [0,0,0,0] |
| C1 | E | none | [0,0,0,0] |
| E | C1 | none | [0,0,0,0] |
| C2 | E | A1 → R0 (entry 1 is w1's) | [0,1,0,0] |
| E | C2 | A0 → R1 | [0,0,0,1] |
| C1 | C2 | A0 → R1 only (R0 is sent before A1 is received) | [0,0,0,1] |
| C2 | C1 | A1 → R0 only | [0,1,0,0] |

  The review's history (w0 gets C1 before w1 sends; w1 gets C2) is the row (C1, C2) with order A0 → R1 only:
  `counts [0,0,0,1]`, now produced. The 7 exported lines of (3,2,1) are exactly these rows. Never (C1,C1) or (C2,C2).

At (3,3,1) the 34 reply tuples are unchanged; 3 tuples now have two orders each ((C2,E,E), (E,C2,E), (E,E,C2): entry 1
may be either other writer's record, so the dependency is on one of two Appends), hence 37 summaries.

## Build and runs (workstation, 2026-11-10, review round 1)

Build (in this directory):
`TMPDIR=/local/home/mkhoshechin/tmp CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/claude-build/neon-tla cargo build --release -j 2`
→ `Finished release profile [optimized]`, no warnings. `target/release/bench_neon_sk_tla` below stands for
`$CARGO_TARGET_DIR/release/bench_neon_sk_tla`.

| Args (`timeout 600 target/release/bench_neon_sk_tla count spec --model fifo -- …`) | Last output lines | execs | summaries (export) | count wall_ms | export wall_ms |
|---|---|---|---|---|---|
| `correct 3 1 1` | `execs=3 blocked=0`, `stop_reason: exhausted` | 3 | 2 | 0 | 2 |
| `correct 3 2 1` | `execs=17 blocked=0`, `stop_reason: exhausted` | 17 | 7 | 0 | 2 |
| `correct 3 2 2` | `execs=43 blocked=0`, `stop_reason: exhausted` | 43 | 25 | 1 | 4 |
| `correct 3 3 1` | `execs=163 blocked=0`, `stop_reason: exhausted` | 163 | 37 | 3 | 10 |

Export: `timeout 600 target/release/bench_neon_sk_tla export spec --out /local/home/mkhoshechin/tmp/neon-tla-out-r2/<n_p_r>.sum --program neon_sk --size <n>,<p>,<r> --model fifo -- correct <n> <p> <r>`
→ `exported: <lines> lines from <execs> graphs`. No assertion failure, no blocked execution.
Also `count spec --model fifo -- correct 256 1 1` → `execs=3 blocked=0` (n wider than u8 accepted).
Round-0 results (before the review): 3/2, 17/7, 43/25, 163/34 (execs/summaries), all with the receive barrier.

## Review round 1 (Astra, `plan/case-studies/neon/review/REVIEW-astra-tla.md`)

| Finding | Change |
|---|---|
| MUST-FIX: `lg` received every writer's Appends before answering anyone (old main.rs:53-67); every summary at 3 2 1 had `counts [0,1,0,1]`, so histories such as w0 `Committed(1)` before w1's Append were missing at 3 2 1, 3 2 2, 3 3 1. | Removed the up-front receive. `lg` now decides the abstract run first, then receives an Append only just before the first `Committed(q)` that needs its record (positions 1..q) and sends every reply as early as that allows; `Ended`-only writers are answered before any receive; remaining Appends are received last. Writer code unchanged. Records are now identified as (w, j) in the log. The 3 2 1 summaries now carry the orders of the table above (the review's history appears); 3 3 1 went from 34 to 37 summaries (same 34 tuples). |
| SHOULD-FIX (a): NOTES claimed elections happen in increasing term order and "every later donor has its term". | Replaced by the observational normalization section: PAS can elect a lower term later, but such a leader reports nothing; the obligation kept is preservation of the committed prefix; the single-log argument is stated as an argument, assertions as consistency checks only. Code comments at the election and donor choice changed accordingly. |
| SHOULD-FIX (b): `n` parsed as u8 (`correct 256 1 1` rejected). | `n` is checked to be a non-empty string of decimal digits and not otherwise read; `correct 256 1 1` runs. |
| NOTE: inherited-only commit omitted. | Kept; decision 2 now states it as an existential abstraction, citing l.230-240 and l.405. |
| NOTE: commit and term end in one step. | Kept; decision 1; its justification no longer relies on the receive barrier. |
| Other NOTEs (counts reproduced, interface mechanics conform). | No change needed. |
