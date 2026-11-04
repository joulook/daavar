# bench_cassandra_derived: the client-observable specification of Cassandra LWT (Paxos v1)

Source: `plan/case-studies/cassandra/paxos-v1-src/service/paxos/Paxos.md` (sections "Basic scheme",
the propositions and proofs, "Insubstantial differences", "Skipping commit for empty proposals"; the
version-2-only sections are ignored except where noted) and `INTERFACE.md`. No Cassandra code was read.

## Roles and messages

| Process | Visible | Role |
|---|---|---|
| `c0`, `c1` | yes | the client code of INTERFACE.md, verbatim. `ci` sends `Req` to its node and waits for a `Rep`. Both clients get the same node id: the object `lwt`. |
| `lwt` | no | the specification's object: the decided value of `x`, the set of undecided updates that can still be decided (`pending`), and the CAS operations whose update is decided and whose reply is still due (`deferred`). Ends after serving 2q requests. |
| `init` | no | sends `Setup { nodes: [lwt], clients: [c0, c1] }` (tag 1) to `lwt`. |
| main | no | spawns `lwt`, `c0`, `c1`, `init`, joins them; sends and receives nothing. |

Messages:
1. `init -> lwt`: `Setup` (tagged `SETUP_TAG = 1`). `lwt` receives it first with `recv_tagged_msg_block`; requests sent earlier wait in its mailbox (buffering).
2. `ci -> lwt`: `Req::Cas(i, expected, v)` or `Req::Read(i)` (untagged, `send_msg`).
3. `lwt -> ci`: `Rep::CasResult(i, applied, value)`, `Rep::Value(i, value)` or `Rep::Failed(i)`.

There are no other messages. The order in which `lwt` receives the requests of the two clients is the
linearization order; the choices below are `nondet()`.

## What `lwt` does (one request at a time)

On each request (`Lwt::serve`), and before each deferred reply (`Lwt::finish`):
1. **Complete an in-progress proposal** (`maybe_complete_in_progress`): for each pending update in turn,
   `nondet()` may decide it (`x := v`, all pending updates removed); at most one is decided.
2. **Give up** (`nondet()`): reply `Failed`. If the request is a CAS and `x == expected`, its update is
   added to `pending` (its proposal may have reached a minority). A failed Read changes nothing.
3. Otherwise:
   - `Read`: reply `Value(x)`. v1: all pending updates removed (empty proposal decided).
   - `Cas`, `x != expected`: reply `CasResult(0, x)`. v1: all pending removed.
   - `Cas`, `x == expected`: `x := v`, all pending removed; then `nondet()`:
     reply `CasResult(1, v)` now, or **defer**: the update was decided by a reproposal and the
     coordinator restarts; its final attempt (`finish`) happens later, after any number of the other
     client's operations, and replies `CasResult(0, x)` with the then-current `x` (never `expected`).
4. The scheduling loop (`object`): when a CAS is deferred, `nondet()` chooses between finishing it now
   and receiving the next request; it must finish when no other request can come (so no execution blocks).

v1nolin differs from v1 only in step 3: a Read or a rejected CAS does not remove the pending updates.

## The document's sentences behind each part

| Part of the specification | Paxos.md |
|---|---|
| one register `x`, decided values form one sequence; each operation acts atomically on the latest decided value | Prop. 1 "Once a value (possibly empty) has been decided ... no earlier proposal can be reproposed"; "Proposition 1 ensures that we can only commit one value in any concurrent operation. Proposition 2 means that any accepted proposal must have started its promise after the previous value was committed ... and hence must be able to see its effects in its read responses." |
| a CAS's update is computed from the value read; applied iff the condition held; the reply's value | 4(iii) "a partition update which is the result of applying the operation, using a read result obtained by resolving the quorum of read responses ... is empty if the operation was a read or the CAS failed" |
| a successful CAS replies only after it is decided | 6 "On receipt of `accepted` messages from a quorum of nodes, the Paxos session has reached a decision"; v1 TTL section "Version 1 ... only reports success on writes after the commit stage has reached a requested consistency level" |
| Read and rejected CAS see the latest decided value (linearizable in v1) | "Skipping commit": "no update made concurrently with any operation (including a read or a failing CAS) can resurface after that operation is decided, and that any operation will see the results of applying any previous"; "no undecided writes can be reflected in read responses" |
| `pending`: an undecided update may be decided later by any coordinator (step 1) | 4(i) "If the `MRA` is not null or empty, and its committed flag is not true, there is an in-progress Paxos session that needs to be completed ... Controller reproposes the value with the new ballot ... and then restarts the process"; proof case "If the proposal reaches a minority of nodes ..." |
| at most one pending update is decided; deciding a write removes the others | Prop. 1 (a later decision has a higher ballot, so the earlier proposals cannot be reproposed); "both `propose` and `commit` actions can only replace the `accepted` value with one with a higher or equal ballot number" |
| v1: a decided Read / rejected CAS removes the pending updates | "Skipping commit": the combination "ensures that no update made concurrently with any operation (including a read or a failing CAS) can resurface after that operation is decided"; v2 section: "To ensure correct ordering of reads and unsuccessful CAS operations, the algorithm above forces invalidation of any concurrent operation by issuing an empty update." |
| v1nolin: a Read / rejected CAS removes nothing | the same sentence read contrapositively: without the empty update (v1nolin, by INTERFACE.md) nothing is invalidated; v2 section "this read will not invalidate an incomplete concurrent write, and thus an undecided entry could be decided after the read executes" |
| `Failed`, and a failed CAS's update possibly decided later | 8 "If ... the quorum is not reached, the coordinator backs off and then restarts"; introduction "repeated clashes between coordinators"; INTERFACE.md (A = 3, "CAS contention/write timeout with an unknown outcome") |
| deferred CAS: decided update, reply `CasResult(0, x)` after later operations | 4(i) "reproposes the value ... and then restarts the process" applied to the coordinator's own earlier proposal (or another coordinator completing it): the restarted attempt reads `x != expected` |
| no non-LWT writes | "The descriptions and proofs below assume no non-LWT modifications to the partition" (INTERFACE.md has none) |

## Decisions where the document is silent (and simplifications)

1. **When `Failed` is possible.** The document gives no condition for giving up. I let every request end
   `Failed` (`nondet()`). Contention needs the other coordinator's ballots, but clients see no real time
   and messages may be delayed arbitrarily, so I assumed every Failed can be explained by an overlapping
   operation (or a delayed prepare) of the other client. *Least sure:* there may be per-client sequences
   (e.g. many consecutive `Failed` against an other client with few attempts, A = 3) that the
   implementation cannot produce; the spec is then a superset.
2. **Effect of a failed CAS.** Its update can be decided only if the condition held when it read
   (otherwise its proposal is empty), at any point after its invocation until the next decided operation
   (v1) / the next decided write (v1nolin). I model it by adding it to `pending` at the point the request is
   taken; deciding it "inside the interval" is equivalent, because between taking the request and the next
   decision nothing observable happens.
3. **Deferred CAS (`CasResult(0, x)` after the own update was decided).** Not stated in the document;
   derived from 4(i) "reproposes ... and then restarts": a coordinator whose own earlier proposal is
   completed (by itself or by the other coordinator) restarts and its condition fails. I allow it for every
   successful CAS (`nondet()`), with any number of the other client's operations between the decision and
   the reply. Whether three attempts always suffice for this path is not checked (A = 3 bounds attempts;
   the path needs 2 or 3).
4. **Uniqueness of values.** `v(i, j)` are distinct and never 0, so after a CAS's update is decided its
   condition can never hold again; a CAS has at most one effect.
5. **v1nolin.** The document describes the variant only implicitly (the role of the empty update). I took
   it as: everything as v1, except that a Read and a rejected CAS do not invalidate pending updates. Reads
   still complete in-progress proposals (step 1) and still see the latest decided value.
6. **TTL expiry (v1 only)** is ignored: there is no time in the model.
7. **Object.** One object `lwt` instead of per-node fronts; both clients address it. `Setup.nodes` = `[lwt]`.
   The object ends after the 2q replies (INTERFACE's "replicas never end" is for the implementation; the
   prompt asks the internal threads to end).
8. Pending updates are tried in insertion order with one `nondet()` each, and with two deferred CAS one
   `nondet()` picks which to finish: some behaviours are reached by several executions (the count is of
   executions, not behaviours).
9. The optional third program argument (`fifo|bag`) is accepted and ignored; the model is `--model`.

## Ported methods

No Cassandra source code was available or read (only Paxos.md). Map from the document's steps to functions:

| Paxos.md step | Function |
|---|---|
| 1-3 prepare/promise, 8 back off and retry (abstracted to a choice) | `Lwt::serve` (`nondet()` → `Failed`) |
| 4(i) complete an in-progress proposal | `Lwt::maybe_complete_in_progress` |
| 4(iii)-(iv), 5, 6 propose and decide a non-empty update | `Lwt::serve` (`Cas` with `x == expected`) → `Lwt::write` |
| 4(iii) empty update (read, failed CAS) decided; v1 invalidation | `Lwt::empty_decision` |
| 4(i) "then restarts the process" for the own update | `Lwt::finish` |
| 7 commit, 4(ii) repair of commits | not modelled (only decided values are visible) |
| coordinator loop / request arrival | `object` |
| program entry | `spec`, `entry`, `visible`, `main` |

## Build and run

Compile (inside this directory): `cargo build --release -j 2` → `Finished release profile [optimized]`, 0 warnings.

Run: `timeout 600 target/release/bench_cassandra_derived count spec --model fifo -- 1 v1`
→ `execs=16 blocked=0` (exit 0), followed by the measurement block whose last line is `cache_hits: `.
