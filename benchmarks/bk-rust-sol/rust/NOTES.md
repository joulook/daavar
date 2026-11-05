# Ledger replication with fencing

This is a port of the bounded protocol in `INTERFACE.md`, using the main source cut identified by `bk-src/README.md` (7b66b5d). Both `Program.spec` and `Program.imp` return the same implementation. There is no separate abstract specification. Only `w1` and `w2` are visible. All state belongs to individual Must threads; there is no shared protocol state, wall clock, storage service, or host-thread communication.

## Roles and protocol

The entry creates `b1`, `b2`, `b3`, `zk`, `w1h`, `w2h`, `w1`, `w2`, then `init`, in that order. The main thread only spawns and joins. `init` sends one tagged `Addresses` setup to each of the six internal processes, containing the three bookies, metadata process, handles and clients. Each service selectively receives setup before processing its inbox, so an early client request cannot bypass initialization, including under bag delivery. Clients capture the handle addresses they use. No process consults another process's memory.

`w1` uses the verbatim interface writer loop: send `Req::Add(i)` for i=1..e, wait for `Rep::Acked(i)` or `Rep::Rejected(i)`, then proceed even after rejection. `w2` uses the verbatim recoverer function: send `Req::Recover`, wait for `Rep::Closed(last)`, then end.

Before entering that function, `w2` uses `nondet()` to choose a prefix length k in 0..e (continue while true, stop on false or e). Its invisible `StartAfter(k)` exchange with `w1h` gates the start until at least k writer operations have completed. k=0 allows recovery before any write, or racing any write through message delivery; k=e also explicitly permits waiting for all writer callbacks. This is a logical start gate, not a clock or timeout. Both operations at `w2` use explicit `Visibility::Invisible`, so the gate adds no visible client messages. The writer handle receives the timing choice in its initial phase, before serving Adds, and sends Start immediately for zero or after the chosen callback count. This removes an unrelated gate receive from the add-response phase; the zero-prefix case still permits every ordering of writer and recovery requests. Once the gate replies, no gate messages remain in the client's inbox.

`w1h` initially has an open local handle and LAC=-1. An Add queues a `PendingAdd` and sends normal writes to all three bookies with its current LAC. Every operation has a distinct-bookie ack bitset. At two successes, the handle drains completed operations from the front of the pending queue, advances LAC and sends Acked in entry order. A Fenced response for the active add closes the handle locally and rejects all still-pending operations. Every subsequent Add is rejected immediately without contacting bookies. Previously sent Acked replies are never revoked. Replies for completed adds remain unreceived, including late fencing errors, following the completed-operation guard in PendingAddOp.java:300–329 and the coordinator correction. The serial public writer normally permits only one pending add, but the queue retains the source's ordered-callback rule.

Each bookie owns an initially empty entry set, a monotone fenced bit, and LAC=-1. A normal write checks fencing before storing. A recovery write always stores, including on a fenced bookie. Storing an entry advances bookie LAC to max(old LAC, LAC carried by the request), never to the entry id merely stored. Both a LAC read and an entry recovery read fence before looking up the result. Fencing is permanent and idempotent. Entries are immutable identifiers because there is exactly one writer and one value for each id.

`zk` starts with version=0, state=Open, last=-1, ensemble=[b1,b2,b3]. A metadata read returns its complete snapshot. A conditional put checks the expected version; on success it changes state/last and increments the version; on mismatch it leaves metadata unchanged and returns the current snapshot. The ensemble is never changed.

`w2h` waits for Recover, reads metadata and conditionally writes InRecovery. It then sends fencing LAC reads to all three bookies, waits for any two distinct responses, and freezes the maximum LAC among those responses. Later LAC answers do not revise the recovery starting point. It reads the next entry from one bookie at a time in entry-relative write-set order, with fencing enabled. The first positive response succeeds; after one NoSuchEntry it queries the next bookie, and a second NoSuchEntry ends recovery. There is never more than one outstanding entry-read request. Each entry read successfully is sent back to all bookies as a recovery add carrying the recovery handle's current LAC, and is considered written back after two confirmations. Only then does the next entry read start. After the first missing quorum, it conditionally writes Closed with the last entry written back, or the original recovered LAC if no entry was written back, and replies Closed to w2 only after metadata update succeeds.

Each handle receives only the response kind expected by its active phase. Add responses also select the active entry id. Late replies for completed operations remain unreceived: no draining or discard receives. All issued bookie requests still execute and send their replies. After closing, w2h blocks waiting for another untagged client request, which never arrives. Bookies, zk and both handles loop forever. Main waits on service joins, so completed client histories appear as blocked executions in Must.

## Message inventory

All messages below except the fixed Req/Rep client protocol are invisible. The explicit Invisible annotations additionally hide the two start-gate operations issued by visible w2.

| Message | Sender → receiver | Meaning and handling |
|---|---|---|
| tagged `Addresses` | init → each internal process | One setup message; tag SETUP=1 is selected before other messages. |
| `StartAfter { completed }` | w2 → w1h | Invisible pre-start gate; wait for the selected writer prefix. |
| `Start` | w1h → w2 | Gate satisfied; w2 now runs its fixed client function. |
| `Req::Add(entry)` | w1 → w1h | Fixed public add request. |
| `Rep::Acked(entry)` | w1h → w1 | Two confirmations and all preceding callbacks completed. |
| `Rep::Rejected(entry)` | w1h → w1 | Local handle closed; one failure callback per unacknowledged request. |
| `Req::Recover` | w2 → w2h | Fixed public recovery request. |
| `Rep::Closed(last)` | w2h → w2 | Final versioned Closed metadata committed. |
| `Write { entry, lac, recovery }` | w1h or w2h → every bookie | Normal or recovery add; recoveries bypass fencing. |
| tagged `AddResponse { bookie, status }` | bookie → initiating handle | Ok after storing, or Fenced for a normal add. |
| `ReadLac { fence }` | w2h → every bookie | Always fence=true here; fence then return local LAC. |
| tagged `ReadLacResponse { bookie, lac }` | bookie → w2h | Counts toward the two-response fencing coverage set. |
| `Read { entry, fence }` | w2h → next bookie | Always fence=true, one outstanding request; stop on first found or second missing. |
| tagged `ReadResponse { found }` | bookie → w2h | Entry present or NoSuchEntry; serialized requests identify the entry and bookie. |
| `GetMetadata` | w2h → zk | Read current versioned ledger metadata. |
| tagged `MetadataResponse(snapshot)` | zk → w2h | Full metadata read result. |
| `PutMetadata { expected, state, last }` | w2h → zk | Version-checked metadata update, preserving ensemble. |
| tagged `MetadataWritten { applied, metadata }` | zk → w2h | Update outcome and snapshot; conflicts trigger another read/conditional put. |

The API's generic receive type is a checked downcast, not a type selector (`api/lib.rs:1254–1256,1565–1572`). Thus each reply type has a distinct transport tag selected by `recv_tagged_msg_block`; `AddResponse` uses a vector tag `[ADD_RESPONSE, low32(entry), high32(entry)]` selected by `recv_vec_tagged_msg_block`. Setup and start-gate messages have separate tags. Client messages remain exactly Req/Rep and untagged; the writer handle selects untagged requests between adds, excluding late tagged responses. Bookies and zk receive their homogeneous request enum Wire, after setup. All receives block; none polls or times out.

## Source-to-port mapping

Paths in the source column are relative to `bk-src/main/`; line numbers refer to the supplied cut.

| BookKeeper method / source | Rust function or state |
|---|---|
| `LedgerHandle.doAsyncAddEntry`, client/LedgerHandle.java:1716–1775 | `writer_handle`, `PendingAdd`, `send_writes` |
| `LedgerHandle.sendAddSuccessCallbacks`, client/LedgerHandle.java:2225–2256 | `send_add_success_callbacks` |
| `LedgerHandle.handleUnrecoverableErrorDuringAdd`, client/LedgerHandle.java:2180–2190 | Fenced branch of `writer_handle` |
| `LedgerHandle.drainPendingAddsAndAdjustLength`, `errorOutPendingAdds`, client/LedgerHandle.java:2205–2222 | `error_out_pending_adds` (length omitted) |
| `LedgerHandle.doAsyncCloseInternal`, client/LedgerHandle.java:576–661 | Local close in `writer_handle`; final metadata close in `recovery_handle` / `update_metadata` |
| `LedgerHandle.handleBookieFailure` / `ensembleChangeLoop`, client/LedgerHandle.java:2293 onward, 2397–2420 | Fixed-ensemble failure closes `writer_handle`; no replacement or retry |
| `PendingAddOp.initiate` / `sendWriteRequest`, client/PendingAddOp.java:146–152,246–274 | `send_writes`, LAC packaged in `Wire::Write` |
| `PendingAddOp.writeComplete`, client/PendingAddOp.java:278–350,395–423 | `writer_handle`, `recovery_add`, `AckSet` |
| `RoundRobinDistributionSchedule.getWriteSetBookieIndex`, client/RoundRobinDistributionSchedule.java:58–59 | Entry-relative rotation in `send_writes` and `recovery_read` |
| `AckSetImpl.completeBookieAndCheck`, client/RoundRobinDistributionSchedule.java:307–310 | `AckSet::complete_bookie_and_check` |
| `AckSetImpl.failBookieAndCheck`, client/RoundRobinDistributionSchedule.java:314–318 | Fenced closes the fixed-ensemble handle directly; no general failure map needed |
| `RRQuorumCoverageSet.addBookie/checkCovered`, client/RoundRobinDistributionSchedule.java:373–398 | Two distinct bits in `read_last_confirmed_with_fencing` |
| `LedgerRecoveryOp.initiate`, client/LedgerRecoveryOp.java:92–146 | `recovery_handle`, `read_last_confirmed_with_fencing` |
| `LedgerRecoveryOp.doRecoveryRead`, client/LedgerRecoveryOp.java:166–171 | Recovery loop / `recovery_read` |
| `LedgerRecoveryOp.onEntryComplete`, client/LedgerRecoveryOp.java:176–221 | `recovery_read` result, then `recovery_add`, or close on missing quorum |
| `LedgerHandle.asyncRecoveryAddEntry`, client/LedgerHandle.java:1621–1627 (called by LedgerRecoveryOp.java:206) | `recovery_add`, `send_writes(..., true)` |
| `LedgerRecoveryOp.addComplete`, client/LedgerRecoveryOp.java:245–259 | `recovery_add` waits for two then advances `last` |
| `ReadLastConfirmedOp.initiateWithFencing`, client/ReadLastConfirmedOp.java:103–109 | `read_last_confirmed_with_fencing` broadcast |
| `ReadLastConfirmedOp.readEntryComplete`, client/ReadLastConfirmedOp.java:114–167 | Two-response coverage and maximum LAC in `read_last_confirmed_with_fencing` |
| `PendingReadOp.initiate/sendReadTo`, client/PendingReadOp.java:66–89,176–188 | `recovery_read`, fence flag on every request |
| `PendingReadOp.readEntryComplete`, `SingleLedgerEntryRequest.complete`, client/PendingReadOp.java:95–115,215–237 | `recovery_read` found response processing, first-success completion |
| `SequenceReadRequest.read/sendNextRead/logErrorAndReattemptRead`, client/PendingReadOp.java:357–421 | `recovery_read`: write-set order, one request at a time, stop after two misses |
| `ListenerBasedPendingReadOp.submitCallback`, client/ListenerBasedPendingReadOp.java:47–67 | One-entry callback represented by return from `recovery_read` |
| `BookieRequestProcessor` dispatch, proto/BookieRequestProcessor.java:324–328,445,513–522 | `bookie` match dispatch |
| `WriteEntryProcessorV3.getAddResponse` and callback, proto/WriteEntryProcessorV3.java:68–98,110–141 | `bookie` Write branch and AddResponse |
| `ReadEntryProcessorV3.getReadResponse`, proto/ReadEntryProcessorV3.java:194–244 | `bookie` Read/ReadLac branches, fence before lookup |
| `ReadEntryProcessorV3.readEntry`, `handleReadResultForFenceRead`, proto/ReadEntryProcessorV3.java:97–132,148–184 | Read response after synchronous in-memory fence/storage lookup |
| `RequestUtils.isFenceRequest`, proto/RequestUtils.java:27–28; BookieProtocol.java:189–190 | `fence` and `recovery` booleans in Wire requests |
| `BookieImpl.addEntry`, bookie/BookieImpl.java:1112–1125 | `Storage::add_entry` |
| `BookieImpl.recoveryAddEntry`, bookie/BookieImpl.java:1024–1034 | `Storage::recovery_add_entry` |
| `BookieImpl.addEntryInternal`, bookie/BookieImpl.java:980–984,1004–1015 | `Storage::recovery_add_entry` plus immediate AddResponse |
| `BookieImpl.fenceLedger`, bookie/BookieImpl.java:1153–1156 | `Storage::fence_ledger` |
| `BookieImpl.readEntry`, bookie/BookieImpl.java:1159–1170 | `Storage::read_entry` |
| `BookieImpl.readLastAddConfirmed`, bookie/BookieImpl.java:1187–1189 | `Storage::read_last_add_confirmed` |
| `LedgerDescriptorImpl.setFenced/isFenced/fenceAndLogInJournal`, bookie/LedgerDescriptorImpl.java:73–79,93–114 | `Storage::fenced` and `fence_ledger` |
| `LedgerDescriptorImpl.addEntry/readEntry/getLastAddConfirmed`, bookie/LedgerDescriptorImpl.java:150–167; LedgerStorage.java:140–154 | `Storage` methods, entry set and piggyback LAC |
| LedgerManager/ZooKeeper, absent from cut; versioned update use in client/LedgerHandle.java:623–655 | `metadata_store`, `read_metadata`, `update_metadata`, per interface |

## Decisions, simplifications and source boundaries

1. **Numbering and empty ledger.** Source recovery initializes from INVALID_ENTRY_ID=-1 and reads `endEntryToRead+1` (LedgerRecoveryOp.java:120–125,168). Its ordinary writer increments the entry id (LedgerHandle.java:1727). This interface explicitly uses entries 1..e with empty last=-1. Therefore the first recovery read when LAC=-1 is entry 1, skipping nonexistent entry 0. After any actual LAC, read LAC+1. There is no synthetic stored entry 0.
2. **One immutable value per id.** Digests, bodies, lengths and master keys are omitted as requested. PendingAddOp.java:261–263 packages entry id, LAC, length and payload; LedgerRecoveryOp.java:186–206 checks/copies data. A BTreeSet of ids suffices here because one writer defines exactly one value per id. A successful read therefore identifies the unique value for that id. Recovery adds are idempotent insertions; entries never disappear.
3. **LAC storage boundary.** PendingAddOp.java:261–263 supplies the sender's lastAddConfirmed; LedgerHandle.java:2251–2254 advances that sender value only after ordered completion. LedgerDescriptorImpl.java:166–167 and LedgerStorage.java:154 delegate bookie LAC to the unprovided storage implementation. I choose monotone max of carried LAC on each accepted normal or recovery write, as described in the source guide. Rejected adds do not change it. No explicit-LAC flush or watch is modeled.
4. **Quorums.** All three bookies receive each write; two distinct Ok responses acknowledge it (PendingAddOp.java:271–274; RoundRobinDistributionSchedule.java:307–310). With N=Qw=3 and Qa=2, coverage requires two responses (RoundRobinDistributionSchedule.java:378–398). Duplicate confirmations could not increase any count because ack sets are bitsets. The transport itself does not duplicate messages.
5. **Sequential entry reads and first success.** Follow the corrected INTERFACE.md and main's default `parallelRead=false` (PendingReadOp.java:48,76–79). `SequenceReadRequest.read/sendNextRead` at lines 357–380 sends to the next write-set member; lines 411–421 stop after two missing replies or send the next request. `SingleLedgerEntryRequest.complete` at lines 215–237 completes on the first valid response. No speculative reads or timers exist here. With no other error outcomes, only the first two write-set members can be queried: either a success has occurred or two misses end recovery.
6. **Recovery read fencing is retained.** PendingReadOp.java:181–184 sets FLAG_DO_FENCING for recovery reads. ReadEntryProcessorV3.java:203–219 fences before reading, even when read lookup later reports NoSuchEntry at lines 239–244. Both kinds of recovery reads in this program always set fence=true. There is no separate unfenced recovery path.
7. **Sequential recovery read/write phases.** LedgerRecoveryOp.java:206–209 can start the next batch after initiating the current recovery add, before its callback; lines 215–219 and 256–258 await the aggregate writes at completion. Per the requested one-by-one protocol I serialize each batch-size-one read, its two-confirmation write-back, and the next read. This deliberately removes that source pipeline. No extra next-read/write interleaving is claimed.
8. **Fenced responses and completed operations.** An active add's Fenced response closes the handle and rejects pending/future Adds (PendingAddOp.java:344–350; LedgerHandle.java:2180–2190). Replies for a completed add are never received, matching the completed guard and return in PendingAddOp.java:300–329 and the coordinator's explicit correction. This supersedes the earlier port's handling of late Fenced replies. The public writer is sequential, so at most one add is pending; the queue preserves entry-ordered callbacks without introducing concurrent client requests.
9. **Writer close is local; recovery owns zk closure.** Full LedgerHandle.java:623–655 also attempts metadata closure during error handling, with conflict handling at lines 627–643. The interface assigns InRecovery and final Closed updates to the recovery handle. I implement writer failure as local handle closure only, avoiding an additional writer-vs-recoverer metadata-close protocol. The writer has no metadata watcher/read loop; it discovers recovery through fenced adds. No ensemble change, retry, alternative placement or spare bookie is possible (source failure/update path LedgerHandle.java:2293 onward,2397–2420).
10. **Metadata store semantics.** The actual LedgerManager implementation is absent from the allowed source cut, as stated in bk-src/README.md. The interface defines the versioned store. I choose initial version 0, monotonically increment once per successful put, return current metadata on mismatch, and retry by read/put without a timer. Only w2h updates zk, so conflicts are unreachable in this instance but version checking is implemented. The ensemble remains in every snapshot and cannot be updated. Local bookie fencing is independent of zk state.
11. **Journal and processor scheduling.** BookieImpl.java:1119–1125 synchronizes the fence check and store; recoveryAddEntry at lines 1031–1034 synchronizes storage but bypasses fencing. LedgerDescriptorImpl.java:93–114 and ReadEntryProcessorV3.java:97–132 otherwise wait for journal persistence. With in-memory storage/no failures, I make each fence/check/store/read a single local transition followed by its response send. Storage is committed before a success reply is sent. Arrival order selects which transition runs first; no journal threads/futures or independent disk events remain.
12. **Empty bookies.** ReadLastConfirmedOp.java:141–143 counts NoSuchLedger/NoSuchEntry as valid for coverage. A preexisting empty in-memory ledger on every bookie returns LAC=-1, and entry misses return found=false (NoSuchEntry). NoSuchLedger is folded into the empty-ledger case; fence state exists even before the first add. This is consistent with fencing creating/acquiring a ledger handle (BookieImpl.java:1155).
13. **Late responses.** ReadLastConfirmedOp.java:156–167 guards its callback with completed, PendingReadOp.java:217–218 ignores success after completion, and PendingAddOp.java:300–329 returns for completed adds. Freeze each completed result and leave its remaining replies unreceived, per the coordinator. Kind tags separate phases; full entry tags separate successive adds. A sequential recovery read has no late reply because only one request is outstanding and its answer is consumed before progressing. The third LAC answer and third add confirmations can remain queued permanently. No request is canceled or suppressed after it was sent.
14. **Start timing.** The cut provides a recovery operation once invoked (LedgerRecoveryOp.java:92), but does not choose when the external client invokes it. The finite invisible nondeterministic prefix gate described above fills that boundary. It does not wait for all bookie writes, only the selected number of writer callbacks, and the zero-prefix choice imposes no ordering against writer operations. All later races follow message arrivals. Neither clients nor main poll or advance a clock.
15. **Fixed omissions.** Authorization always succeeds, no digests, read-only states, LAC watches, timers, speculative reads, batching beyond one, crashes, replacement or network loss. Source branches such as PendingAddOp.java:155–169 (timeout), PendingReadOp.java:87–89 (speculation), WriteEntryProcessorV3.java:119–156 (other errors), and BookieImpl.java:1192 onward (LAC watches) are outside this interface. No nondeterministic failures are introduced for excluded cases.
16. **Must and command arguments.** Both program entries build this implementation with positive e fitting signed entry ids. The optional positional fifo/bag token is accepted and validated; the shared `program::main` harness chooses the actual delivery model using `--model fifo` or `--model bag` (FIFO by default). There is no per-thread model override or independent transport. The harness is authoritative if both positional token and `--model` are present. The only nondeterministic local choice is the pre-start prefix; sends otherwise use the source's entry-relative round-robin order (LAC reads use ensemble order).

## Build and requested runs

The resumed port was built with:

```sh
TMPDIR=/local/home/mkhoshechin/tmp cargo build --release -j 2
```

The first smoke runs exposed that plain generic receives do not filter message types (wrong message return type: expecting StartAfter but got Req). The final implementation uses selective phase tags as documented above. Final build and requested run results follow.

Build exit status: 0. Last build output:

```text
   Compiling bench_bk_sol v0.1.0 (/local/home/mkhoshechin/Documents/github-jmc/plan/case-studies/bookkeeper/codex/rust-sol)
    Finished `release` profile [optimized] target(s) in 9.09s
```

The two requested runs were started concurrently as separate processes. Output was captured through the command tool; no additional log files are needed.

### FIFO e=1

```sh
timeout 600 target/release/bench_bk_sol count impl --model fifo -- 1
```

Exit status: 0. Exhausted in 21,141 ms. Final count and relevant measurement output:

```text
execs=0 blocked=5460
--- measurements
wall_ms: 21141
explore_ms: 21141
graphs_complete: 0
graphs_blocked: 5460
iterations: 5460
stop_reason: exhausted
peak_rss_kb: 5532
max_graph_events: 118
```

Last output lines:

```text
sweeps:
cache_tests:
cache_hits:
```

All 5,460 executions are blocked because the internal services persist. This is an exhaustive implementation exploration for this bound, not an abstract-specification conformance proof. The total need not match another implementation's count: this implementation's initial typed prefix gate, serial recovery phases and transport messages are explicit above.

### FIFO e=2

```sh
timeout 600 target/release/bench_bk_sol count impl --model fifo -- 2
```

Exit status: 124 (600-second timeout). Last output lines:

```text
Executions attempted so far: 100000 total 0 finished normally 100000 blocked (416.67/sec).
Executions attempted so far: 200000 total 0 finished normally 200000 blocked (519.48/sec).
Executions attempted so far: 300000 total 0 finished normally 300000 blocked (573.61/sec).
```

The timeout produced no additional output. No final `execs=` line or `stop_reason: exhausted` appeared for e=2; 300,000 is only the last reported lower bound, not a final total. Neither final requested run reported a panic or assertion failure. No bag exploration was requested or run. No protocol loop uses a timer: the external `timeout` only bounds the exploration command.
