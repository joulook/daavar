# scalardb-cc: S_cc, the hand-written specification (2026-11-06)

Interface: plan/case-studies/scalardb/INTERFACE.md (message types and client loop verbatim; contract S1-S7 at its
end). Plan: plan/case-studies/scalardb/STUDY-PLAN.md, section "Specification S_cc". Sources of the clauses:
- the model: code/tla+/consensus-commit/CC.toolbox/Model_1/CCSpec.tla, `Consistent` l.47-48 (in this snapshot; l.46-47
  in the top-level copy the plan cites) and `THEOREM Spec => [](TypeOK /\ Consistent)` l.52; README.md l.11-14;
- the isolation levels: cc-src/.../ConsensusCommit.java class comment l.41-48, and docs/consensus-commit.mdx
  "Isolation levels" l.166-176 and the note l.182.
Not checked: liveness (shows only as `blocked=0`), the refinement CC => CCSpec (internal states), READ_COMMITTED.

## Shape
One invisible object thread `cc`; visible `c1`, `c2` (`c3` for mixedro) run the interface's client code attached
directly to `cc`; invisible `init` sends `cc` the client addresses (tagged setup message). Spawn order: `cc`, `c1`,
`c2` (`c3`), `init`. Transaction t_i belongs to c_i; t0 is the setup transaction. State of `cc`:
- `chain[k]`: the committed version chain of key k, writers oldest first, starting `[t0]` (value 0). A version is
  named by its writer (c_i writes the value i).
- per transaction: status Idle / Active / Outstanding (Commit received, not answered) / Committed / Aborted; the writer
  of the version it read on each key; its write set (from its Writes).
At each step `cc` picks one enabled action by `nondet()`; the loop ends when every client has ended, so no client is
left waiting and the object thread ends (`blocked=0` everywhere below).

## Reply rules (each with its clause and its document sentence)
| Action | Enabled when | Reply / effect | Follows |
|---|---|---|---|
| receive `Begin` | c_i Idle (handled in the same step as the `Read(1)` after it, D-r) | `Begun`; Active | S7 "`Begun` ... always answer[s] `Begin`" |
| receive `Read(k)`, value | c_i Active | `Value(k, v)`, v = the latest entry of `chain[k]` at this step; recorded as t_i's read of k | S3 "returns the latest value of the key's committed version chain at the moment it is decided"; S1 "every `Value(k, v)` is the value of the committed version chain"; model: `Consistent` second disjunct (no committed record under an aborted coordinator), README l.13 "If this succeeds then the transaction is committed"; docs l.182 "requires each underlying database to provide linearizable operations" |
| receive `Read(k)`, abort | c_i Active and some other t_j that wrote k is Outstanding; chosen by `nondet()` against the value reply | `Aborted`; t_i Aborted; c_i ends | S6 "`Aborted` to a `Read(k)` only while another transaction that writes k has an outstanding commit" (D6; the recorded loosening "any read may be answered `Aborted`" is NOT applied); README l.18 (prepared record, no status) |
| receive `Write(k, v)` | c_i Active, all reads done (handled in the same step as the `Commit` after it, D-r) | `Written(k)`; k joins t_i's write set | S7 "`Written` always answer[s] `Write`" |
| receive `Commit` | c_i Active, all writes received | no reply; t_i Outstanding | defines the outstanding interval of S4/S6 |
| decide t_i, Committed | t_i Outstanding, and S4: for every key t_i wrote, the version t_i read is the latest of its chain; and, iso = serializable only, S5: the serialization graph of t0, the committed transactions and t_i is acyclic. Chosen by `nondet()` against Aborted | t_i's writes are appended to the chains in one step; `Committed` | S4 first-updater-wins "T is committed only if, for every key T writes, the version T read is still the latest committed one at T's commit decision"; S5; S2 atomic visibility (the whole write set joins the chains at one step); model: `Consistent` first disjunct (coordinator committed => no record initialized or aborted), README l.11-13; ConsensusCommit l.41-44 "When SERIALIZABLE is specified ... it makes schedule strict serializable"; docs l.171-172 "Serializable ... Possible anomalies: None" |
| decide t_i, Aborted | t_i Outstanding (always) | `Aborted`; nothing joins the chains | S6 "`Aborted` to a `Commit` is always allowed"; S1 (an aborted transaction's value is never in a chain, so never read); README l.11, l.13 "If this fails then the transaction fails, rollback" |
| `Unknown` | never | none | S7 "`Unknown` is never sent" |

Serialization graph (S5), nodes t0..t3, built from the chains with t_i's writes appended and the reads of t0, the
committed transactions and t_i: ww a->b for consecutive versions a, b of a key; wr w->r when r read w's version; rw
r->n when r read a version whose successor in the chain is n (n != r). Under snapshot S5 is off: ConsensusCommit l.46-48
"a weaker variant of snapshot isolation (SI). This snapshot isolation could cause read skew anomalies in addition to
write skew and read-only anomalies"; docs l.168-169 "Read-committed snapshot isolation (SNAPSHOT - default) / Possible
anomalies: read skew, write skew, read only". S4 stays on under snapshot (both levels, INTERFACE.md S4).
Read-only c3 has an empty write set: S4 holds vacuously; under serializable S5 applies to it (INTERFACE.md S5
"read-only transactions included").

## Decisions taken without asking (recorded here)
- D-a Package name `bench_scalardb_cc` (the prompt) rather than INTERFACE.md's crate `scalardb_cc`; the object thread
  is named `cc`. Both entries `spec` and `imp` return S_cc.
- D-b The commit decision and the reply to `Commit` are one step (STUDY-PLAN: "a later ... Decide(t) decides and
  replies"), done by action choice as in crdb-pc, not by a self-message. A decision strictly inside the interval
  (as the code's PutState COMMITTED before the records are committed) adds no observation: a read placed between
  the code's decision and its reply is placed after the reply in S_cc; it returns the same value, and this placement
  gives S_cc fewer (not more) orders into the writer's `Committed` receipt.
- D-c The read decision and its reply are one step (the read's moment is the object's step).
- D-d The trailing `[fifo|bag]` ARG is validated and ignored (the model comes from `--model`), as in crdb-pc.
- D-e A bad argument exits 2 with a named reason (e.g. "pattern `skew` needs k = 2").
- D-f "Another transaction that writes k" (S6) = another transaction whose received Writes include k; when it is
  Outstanding all its Writes have been received, so this is its write set.
- D-r (state-space reduction) One Recv step of c_i handles a `Begin` together with the `Read(1)` after it, and the
  Writes together with the `Commit` after them. `Begun` and `Written` carry no information and in the source do no
  I/O (STUDY-PLAN "Begin ... and Write ... do no I/O"). Without it, mixedro does not exhaust in 600 s (6.6 million
  executions at ~11 000/s and still running); with it, it exhausts at 229 981. Cost: S_cc no longer has the
  executions in which `cc` processes another client's step between c_i's `Begun` and its `Read(1)`, or between its
  last read and its Writes/Commit. Those differ from the kept ones only in which visible orders run through `cc` around
  `Begun`/`Written` (neither choice orders less in every pair). Unreduced counts for comparison (same rules otherwise):
  lost 1 snapshot 788, lost 2 snapshot 2810, skew 2 snapshot 3750, skew 2 serializable 2861, mixed 2 snapshot 5324,
  both 2 snapshot/serializable 10452, mixed 2 serializable 5296, mixedro: not exhausted in 600 s.
- D-g Clients are processed in any order (no symmetry reduction).

## For the coordinator: an order concern with any one-object specification
The conformance morphism (traceforge/src/conformance/morphism.rs l.6-15, l.159-184) requires (M2) that every `vo`
(= porf) pair of the specification graph is a `vo` pair of the implementation graph. In a one-object specification,
`cc` processes one client's request before or after every other client's reply, so for any two clients some send of
one is porf-before a receive of the other (e.g. c1's `Begin` before c2's `Begun`, or the reverse). In the
implementation, h1 answers `Begin` without I/O, so no event of c2 is porf-before c1's `Begun` and c1's `Begin` is
porf-before nothing of c2. If `vo` is plain porf over all threads, as morphism.rs reads, no S_cc graph can cover
such an implementation graph, with or without D-r. crdb-pc has the same shape. I did not check how the engines handle
this. It is recorded here, not resolved.

## Compile
`TMPDIR=/local/home/mkhoshechin/tmp cargo build --release -j 2`: Finished, no warnings, no errors.

## Counts (`timeout 600 target/release/bench_scalardb_cc count spec --model fifo -- ARGS`; all exhaust, exit 0, blocked=0)
| ARGS | execs | wall |
|---|---|---|
| lost 1 snapshot | 66 | 8 ms |
| lost 2 snapshot | 216 | 26 ms |
| skew 2 snapshot | 290 | 28 ms |
| skew 2 serializable | 226 | 88 ms |
| mixed 2 snapshot | 238 | 82 ms |
| mixedro 2 snapshot | 229981 | 29.7 s |
| (extra) lost 1 serializable / lost 2 serializable | 66 / 216 | |
| (extra) both 2 snapshot / both 2 serializable | 232 / 232 | |
| (extra) mixed 2 serializable | 234 | |
| (extra) mixedro 2 serializable | 223771 | 29.8 s |
`count` counts executions (object orders x choices), not distinct visible histories. mixedro is not small: the third
client multiplies the interleavings of the Read/Commit/Decide steps (12 steps, 4 per client).

Outcome check (from a temporary trace print, removed afterwards): skew snapshot has executions where both commit having
read (0, 0) (64 of them); skew serializable has none. mixed snapshot has c2 reading (k1 = 0, k2 = 1) and committing
(read skew); under serializable that read is always followed by `Aborted`. In mixedro serializable c3 never commits
after reading (0, 1).

## Hand check at (lost, 1, snapshot): the client-visible histories
Object steps per client: R_i (Begin+Read(1) and the reply), K_i (Write(1, i)+Commit received, outstanding), D_i (the
decision and the reply). 20 interleavings of R1 K1 D1 with R2 K2 D2. A read in the other's K..D window may reply
`Aborted`. A decision may be `Committed` only if no other commit happened since the read. Histories (c1's
observations ; c2's observations), with the object orders that give them:
1. c1 V(1,0) Aborted ; c2 V(1,0) Aborted. All 20 interleavings (both decide Aborted). 20 executions.
2. c1 V(1,0) Aborted ; c2 V(1,0) Committed. Every interleaving except R2 K2 D2 R1 K1 D1, where c1 would read 2. 19.
3. c1 V(1,0) Committed ; c2 V(1,0) Aborted. Symmetric. 19.
4. c1 V(1,0) Committed ; c2 V(1,1) Committed. Only R1 K1 D1 R2 K2 D2 (c2 reads after c1's commit). 1.
5. c1 V(1,0) Committed ; c2 V(1,1) Aborted. Same order. 1.
6. c1 V(1,2) Committed ; c2 V(1,0) Committed. Symmetric to 4. 1.
7. c1 V(1,2) Aborted ; c2 V(1,0) Committed. Symmetric to 5. 1.
8. c1 V(1,0) Committed ; c2 read Aborted. Only R1 K1 R2 D1 (c2 reads while c1's commit is outstanding). 1.
9. c1 V(1,0) Aborted ; c2 read Aborted. Same order. 1.
10. c1 read Aborted ; c2 V(1,0) Committed. Only R2 K2 R1 D2. 1.
11. c1 read Aborted ; c2 V(1,0) Aborted. Same order. 1.
Total 20 + 19 + 19 + 8 = 66, which equals the count. An independent re-derivation (a small Python enumeration of the
same rules, not kept) also gives 66 with the same 11 outcome classes. Forbidden, and absent: both clients read 0 and both
commit (S4, lost update); a read of 1 (or 2) when c1 (c2) aborts (S1); a read of 1 before c1's decision (S3); a read
answered `Aborted` outside the other's outstanding window (S6); a `Committed` after reading a version that was
overwritten since; `Unknown` (S7); c1 reads 2 while c2 reads 1 (each would have to commit before the other's read).
