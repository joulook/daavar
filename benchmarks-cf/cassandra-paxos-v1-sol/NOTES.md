# Cassandra 4.1 Paxos v1 port

The implementation is `java/bench/cassandra_paxos_v1_sol/CassandraPaxosV1Sol.java`.
All source references below are to the supplied `paxos-v1-src` cut, with its original line numbers. `service/StorageProxy.java` is abbreviated SP; `service/paxos/PaxosState.java` is PS. The supplied README was read first. No files outside this directory were read and no web sources were used.

## Roles and entry

`CassandraPaxosV1Sol(String... args)` implements `MpProgram` and accepts exactly `impl,<q>,<v1|v1nolin>,<fifo|bag>`. `fifo` selects `CommunicationModel.P2P`; `bag` selects `ASYN`. The program declares only `c0,c1` visible. `n0,n1,n2` are internal processes and each owns both its replica registers and its coordinator's state.

Main allocates five `MpThread`s and their addresses before starting any of them, starts replicas in index order and then clients in index order, and waits for the clients and replicas. It performs no sends, receives, or protocol choices. Addresses are immutable lists initialized before any thread starts. There is no shared mutable protocol state. Constructing all threads before starting them avoids inventing an extra setup process or assuming numerical task IDs.

Client `ci` sends to `ni` and performs exactly q sequential operations, with `known=0` initially. Operation j uses `(i+j)%2`: zero sends `Cas(i,known,1+i+2*j)`, one sends `Read(i)`. `CasResult` and `Value` replace known with the returned value; `Failed` leaves it unchanged. Clients terminate. Replica processes stay in an unconditional blocking receive loop forever, including after clients finish. A replica handles peer requests while its coordinator waits for a callback; thus two coordinators waiting on each other do not prevent replica service.

## State and ballots

Each replica starts with promised `(0,-1)`, accepted NONE, committed NONE, and stored NONE. NONE is an empty commit with ballot `(0,-1)` and value 0. NONE represents the absence of accepted state as well as the initial committed state; its ballot cannot supersede a real ballot. Stored represents the base-table integer with its write timestamp, separately from Paxos metadata.

`Ballot.compareTo` compares `(counter,replica)` lexicographically as required by the interface. `seen` is the largest counter witnessed by that process in protocol requests or replies (including late replies, negative replies, and base-table read timestamps). Each fresh attempt uses `(++seen,id)`. A process also remembers ballots from its own preceding attempts. Real ballots start at counter 1. The model uses Java ints, like the interface's integer register and operation values; it assumes input sizes that do not overflow integer arithmetic.

The replica's latest witnessed ballot is max(promised, accepted.ballot, committed.ballot). A successful prepare requires STRICTLY greater than that maximum. It changes promised only and returns the previous accepted and committed registers. A rejected prepare returns an empty commit at the current promised ballot plus committed state, exactly as PS 748-752; it does not return the maximum instead of promised. A proposal requires greater than OR EQUAL to the witnessed maximum. It installs accepted unless its ballot is already committed. Neither acceptance nor commit changes promised. A commit unconditionally reaches the mutation application path: stored retains the greatest nonempty write timestamp, committed retains the greatest ballot, and accepted is cleared when its ballot is no greater than committed. These distinctions follow PS 158-246, 664-805 and `service/paxos/Paxos.md` 139-154.

## Messages and processing

All records are immutable, value-equal `MpMessage`s with `@Tag`; all interprocess traffic, including a replica's local request to itself, uses `Channel.send` and `Channel.recv`. No Java queues, locks, shared registers, or direct cross-process handler calls carry protocol information.

| Message | Sender -> receiver | Payload and action |
| --- | --- | --- |
| `Cas` | client -> its coordinator | Exactly int fields `client,expected,value`; start a CAS request. |
| `Read` | client -> its coordinator | Exactly int `client`; start a SERIAL read. |
| `CasResult` | coordinator -> client | Exactly int `client,applied,value`; applied is 0 or 1; return the proposal value on success or the condition-read value on rejection. |
| `Value` | coordinator -> client | Exactly int `client,value`; return the final quorum read. |
| `Failed` | coordinator -> client | Exactly int `client`; attempt exhaustion or an unknown partial-proposal outcome. |
| `Prepare` | coordinator -> all replicas | `from,round,ballot`; test and persist the promise register. |
| `Promise` | replica -> coordinator | `from,round,promised,accepted,committed`; on rejection accepted is the empty commit at promised, as in `PrepareResponse`. |
| `Propose` | coordinator -> all replicas | `from,round,proposal`; apply legacyPropose's acceptance test. Proposal is `Commit(ballot,empty,value)`; value is ignored when empty. |
| `Accepted` | replica -> coordinator | `from,round,ok,witness`; ok models Cassandra's Boolean response. Witness supplies the interface's observed-ballot rule. |
| `Learn` | coordinator -> replicas | `from,round,proposal,acknowledge`; apply the commit. Ordinary/replay commits go to all three with acknowledge=true. Missing-commit repair targets only the mismatching promise respondents with acknowledge=false. |
| `Ack` | replica -> coordinator | `from,round`; commit has been applied; two replies complete commitPaxos. |
| `Fetch` | coordinator -> two chosen replicas | `from,round`; independently read the base register, without changing Paxos state. |
| `Data` | replica -> coordinator | `from,round,stored`; includes the base value's ballot so read reconciliation chooses the newest value. |

`round` is a monotonically increasing local callback identifier, separate from the ballot and attempt budget. Reply `from` identifies its replica. Requests carry the coordinator's replica index, which resolves to a known `Tid`; numerical replica indices are not raw API task IDs. Every request receives exactly one reply when requested. Unique round IDs prevent late replies from satisfying a later quorum. Unmatched replies are consumed and update seen where applicable; they cannot mutate a completed callback. The loop receives every message without a selective guard, preserving the selected communication model's ordering. Only one client uses each coordinator and waits for its reply, so a second client request cannot arrive during an active request.

## Coordinator flow

Each request gets at most three fresh-ballot attempts in total. There is one budget around preparation, recovery, and proposal, rather than nested budgets. Missing-commit repair and successful replay each consume their attempt and require a fresh prepare on the next attempt. Third-attempt repair still sends/applies the recovery work; the client then gets Failed because its own operation has not completed.

Prepare broadcasts to three replicas. `preparePaxos` stops after the first two positive responses or the first negative response, matching PrepareCallback's quorum latch and immediate release on rejection. The received promises determine the highest accepted proposal and highest committed proposal independently.

`beginAndRepairPaxos` first checks whether the highest accepted proposal is nonempty and newer than the highest committed proposal. If so it reproposes that value with the NEW ballot. Recovery proposal callbacks stop at two accepts or as soon as two rejects make a quorum impossible. Successful recovery commits to all three, waits for two acknowledgments, and then retries preparation. Failed recovery also retries. Empty accepted proposals are skipped, even if newer than committed; they are not committed or replayed.

Otherwise the coordinator checks whether every promise respondent reports the highest committed ballot. It sends that commit only to respondents missing it, without waiting for acknowledgments, then retries with a fresh ballot. It does not silently proceed under the old promise. Only when no such repair is required does preparation succeed.

For CAS, the coordinator now performs an independent QUORUM read of the base integer, tests expected against that result, and proposes the desired value if the condition holds, or an empty update if it fails. A client proposal callback returns success on two accepts. If no quorum forms, it waits for all three responses to distinguish complete refusal from partial acceptance. Complete refusal retries within the budget. One accept and two rejects is an unknown outcome and returns Failed immediately, retaining the accepted proposal for possible replay by another coordinator. This follows SP 734-769 and ProposeCallback 33-43, 60-92; it is not safe to convert that source exception into a successful rejected CAS. Nonempty successful proposals commit to all replicas and wait for two acknowledgments. Empty successful proposals return the saved CAS result without committing.

For SERIAL Read, v1 proposes an empty update after successful preparation, waits for quorum acceptance with the same client proposal rules, and then performs its final QUORUM read. v1nolin skips the empty proposal and performs the final QUORUM read directly after successful preparation. Both variants replay incomplete nonempty proposals and repair missing commits first. The final read is AFTER the empty proposal for v1, not before it. Failed attempts reread/recompute a CAS condition, rather than reusing an earlier attempt's value.

The supplied source has no base-table read inside `beginAndRepairPaxos` itself (SP 571-680). Its recovery commit is awaited at QUORUM so the subsequent CAS condition read or SERIAL read sees replayed updates. The port keeps that boundary. It does not use promise contents as a substitute for the base-table QUORUM read.

## Cassandra-to-port method mapping

Helper methods that only serialize bytes, report diagnostics, maintain storage caches, or implement v2 are outside the port. Constructors/value comparisons are collapsed as shown.

| Cassandra file / method (source lines) | Port method / representation |
| --- | --- |
| SP `cas` 306-329; `legacyCas` 329-437, including updateProposer 348-388 | `Node.coordinate`, CAS branch; variant dispatch in constructor |
| SP `doPaxos` 478-564 | `Node.coordinate`, shared three-attempt loop, proposal and commit |
| SP `beginAndRepairPaxos` 572-678 | `Node.beginAndRepairPaxos`; ballot generation in `coordinate` |
| SP `sendCommit` 683-688 | `beginAndRepairPaxos` sends `Learn(...,false)` to missing respondents |
| SP `preparePaxos` 690-727 | `Node.preparePaxos`, `broadcast`, `response` |
| SP `proposePaxos` 734-769 | `Node.proposePaxos`; its -1 result handled by `coordinate` |
| SP `commitPaxos` 771-822; `commitPaxosLocal` 829-861 | `Node.commitPaxos`; self sends use the same `Learn` handler |
| SP `readOne` 1820-1824, `read` 1830-1860, `readWithPaxos` 1860-1866 | `coordinate` dispatch and `Node.quorumRead` |
| SP `legacyReadWithPaxos` 1868-1964 | `coordinate`, Read branch, including variant-specific empty proposal and final read |
| SP `readRegular` 1967-2020; `fetchRows` 2068-2134 (read interface only) | `Node.quorumRead`, `Fetch`/`Data` handling and timestamp reconciliation |
| v1 `PrepareCallback` constructor 55-61, `onResponse` 63-86, `replicasMissingMostRecentCommit` 88-111 | `preparePaxos` and max/missing calculations in `beginAndRepairPaxos` |
| v1 `ProposeCallback` constructor 53-58, `onResponse` 60-74, `getAcceptCount`/`isSuccessful`/`isFullyRefused` 76-92 | `proposePaxos` counters and 1/0/-1 result |
| v1 `AbstractPaxosCallback.await` 60-76 | `response` blocking receive; attempt budget replaces timeout |
| v1 `PrepareVerbHandler.doPrepare`/`processMessage` 30-40 | `Node.handle`, Prepare branch |
| v1 `ProposeVerbHandler.doPropose`/`processMessage` 30-40 | `Node.handle`, Propose branch |
| PS `Snapshot.latestWitnessedOrLowBound` 192-201 | `Node.latestWitnessed` |
| PS `Snapshot.merge` 218-246 | `handle`, Learn branch (max committed and clear older accepted) |
| PS `legacyPrepare` 719-761 | `handle`, Prepare branch |
| PS `legacyPropose` 763-805 | `handle`, Propose branch |
| PS `commit` 664-669, `commitDirect` 671-681, `applyCommit` 683-717 | `handle`, Learn branch |
| `service/paxos/PaxosCommit.java` `RequestHandler.doVerb`/`execute` 301-322 | `handle`, Learn branch and Ack after application |
| `service/paxos/Commit.java` `newPrepare` 267-270, `emptyCommit` 272-275, `newProposal` 278-282 | `Prepare`, `NONE`, `new Commit(ballot,empty,value)` |
| `Commit.isAfter`/`isSameOrAfter`/ballot equality 284-312; `latest` 438-457 | `Ballot.compareTo`, `max`, `latest`, record equality |
| `Commit.withTimestamp` 472-475 | Every newly proposed/replayed Commit receives the current ballot as its data timestamp |
| `service/paxos/Ballot.java` `none` 50-53; `BallotGenerator.Default.next` 43-47 and Global `nextBallot` 70-71 | `ZERO`, `Node.observe`, `coordinate` fresh ballot |
| `service/paxos/PrepareResponse.java` constructor/fields 35-54 | `Promise` record |
| `service/paxos/Paxos.java` `isLinearizable` 1215-1230; `config/Config.java` PaxosVariant 889-930 | Constructor `linearizable`; Read branch only |

## Decisions, open choices, and simplifications

1. **Variant naming versus actual behavior.** Config 889-905 calls the old variant “without linearizable reads or rejected writes,” but the only v1 behavior switch is SP 1891-1894. SP 358-362 and 519-539 still propose failed-CAS empty updates unconditionally. Therefore both variants propose a rejected CAS; only SERIAL reads differ. No v2 skip-proposal optimization from Paxos.md 176-285 is ported.
2. **Budget and errors.** SP 499-503 and 584-587 use nested deadlines, and SP 613-618, 646-648 and 547-551 use random sleeps. The interface replaces all of those with a single maximum of three fresh-ballot attempts and no sleep. SP 661-669's missing-commit retry also spends an attempt. There is no arbitrary timer or nondeterministic timeout while healthy replicas can still answer. An immediate source unknown-result exception (SP 765-766) maps to Failed immediately rather than spending further attempts; it is distinct from the fully refused proposal that the source retries.
3. **Counter clock.** SP 589-600 and BallotGenerator 34-71 use time/UUID/random uniqueness; Commit 284-301 compares timestamps. The interface explicitly replaces that with lexicographic counter/id order and a fresh counter above everything observed. Accepted replies add the replica's latest witnessed ballot, whereas Cassandra's response is Boolean (ProposeVerbHandler 38-40), so a proposal rejection can advance the requested clock. This metadata does not affect the callback's success/failure rules.
4. **No expiration, purge bounds, truncate, restart, or range changes.** PS 192-201, 248-266, 685-705, 724-737, 765-766 and PrepareCallback 94-108 contain TTL, lower-bound, and truncation behavior. The interface has no time, failure, disk, truncate, or topology operations. Registers never expire, the low bound is ZERO, and all three replicas own the partition. `promisedWrite` is omitted because legacyPrepare always sets it equal to promised (PS 738); no v2 read promises are used. System table/cache atomics become process-local assignments.
5. **Base data reconciliation.** PS 699-700's mutation application and Commit 278-282/472-475 timestamp writes; Paxos.md 127-131 explains resolving by timestamps. The one integer is a last-write-wins register ordered by the full interface ballot. Delayed older commits cannot overwrite newer data. A replay uses the new ballot timestamp, matching `Commit.newProposal`, rather than retaining the old accepted timestamp. No tombstones, triggers, cells, partitions, schema validation, or concurrent ordinary writes exist.
6. **Read topology and repair.** SP 2057-2067 leaves contact selection to snitch distance, read repair configuration, and speculation; SP 2083-2127 delegates executor details unavailable in this source cut. Each `quorumRead` uses `Nondet.chooseIndex(3)` to choose the omitted replica, sends full data requests to the other two, waits for both, and resolves the maximum timestamp. This chooses any legal quorum without inventing a snitch order. Digest transport, speculation, and base-table read-repair writes are omitted; returned full values suffice to reconcile the scalar. Paxos missing-commit repair is retained separately. This is a deliberate read-executor simplification, not a port of Cassandra's entire configurable read-repair subsystem.
7. **Handler granularity and local delivery.** SP 698-717, 739-759, 788-818 schedules local tasks and remote callbacks. Every local operation here is a self message through the API; a handler's register test/update is atomic between receives/sends. A response send occurs after that state change. All replica requests are sent in index order; arrival order determines callback membership. There is no choice of leader or random retry delay left to model. The read quorum selection is the only protocol choice requiring an explicit API choice operation.
8. **Callback closure.** PrepareCallback 63-86 and ProposeCallback 60-74 synchronize/atomically release latches, while a Java coordinator may race with late callback invocations. This port closes the callback at the receive event that releases it (two promises, first rejection, or the proposal stopping test). Later replies only advance seen. It does not add nondeterministic CPU-thread races inside Cassandra callbacks. All messages are nevertheless drained by replica receive loops after clients finish.
9. **Commit acknowledgments.** SP 654-665 explicitly sends missing-commit repair without waiting and restarts; its TODO suggests a different future implementation. This port retains the actual retry, not the TODO. SP 771-822 and Paxos.md 303-306 require ordinary/replay commits at the configured level; here that is always QUORUM=2. Empty proposals skip commit entirely, exactly SP 526-534 and 625-640. No hints, failure detector, tracing, metrics, unavailable replicas, or ownership rejection paths are represented (interface exclusions; AbstractPaxosVerbHandler 48-74 and SP 807-816).
10. **Serialization and correlation.** Cassandra network serialization/version flags (PrepareResponse 61-82, Commit serializers, Ballot serializers and net/Verb.java 124-129) are replaced by tagged immutable Java records. Logical requests carry explicit round identifiers to emulate per-request network callbacks. Main uses `MpThread` directly (the thread primitive underlying `MpProcess`) so all actual Tid values are available before any protocol event. No process communicates by reading another process's mutable fields.

## Validation

Compilation, from this directory (exit status 0):

```sh
javac -d java/out -cp "$(cat cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
```

Exactly one launcher run was requested and launched from this directory:

```sh
java -Xss4m -cp java/out:$(cat cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.cassandra_paxos_v1_sol.CassandraPaxosV1Sol:impl,1,v1,fifo' must 0 1 report - 600 </dev/null
```

The single run exited with status 0. It reached the configured timeout after 600.069 seconds and explored 17,703 iterations: 17,703 distinct blocked graphs and 0 complete graphs; the exploration stack was not exhausted. Replicas intentionally never terminate, so executions end blocked on receive and are classified as blocked. The launcher reported `stop_reason: timeout`; this is bounded exploration, not exhaustive verification or a conformance proof. Both variants compile, but only the requested q=1, v1, fifo configuration was run. Startup also printed a logging-configuration diagnostic (`Console contains an invalid element or attribute "level"`).

Literal last output line:

```text
max_porf_count: 0
```
