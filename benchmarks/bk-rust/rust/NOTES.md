# bench_bk_port: BookKeeper ledger replication with fencing (main 7b66b5d), Must-API port

Source lines below are relative to `bk-src/main/` (paths as in `org/apache/bookkeeper`).
Instance: E = Qw = 3 (b1, b2, b3), Qa = 2, COVER = Qw - Qa + 1 = 2; one ledger; `e` entries.

## Roles and messages

Threads, in start order: `b1 b2 b3 zk w1h w2h w1 w2` and then `init`. The main thread spawns
and joins `w1`, `w2`, `init`; it sends and receives nothing. The bookies, `zk`, `w1h` and `w2h`
are daemon threads that never terminate (a handle parks on a client receive that never comes).

Visible messages (verbatim from INTERFACE.md): `Req::{Add(u64), Recover}`,
`Rep::{Acked(u64), Rejected(u64), Closed(i64)}`; `writer` and `recoverer` are verbatim.

Internal messages (`enum Net`). Every reply is a tagged send; every receive of a handle is
a tagged receive, so a handle only receives the replies its current operation is waiting for.

| message | from -> to | tag | source counterpart |
|---|---|---|---|
| `Setup{b, zk, w1h, w2h, w1, w2}` | init -> each internal | 1 | (interface) |
| `AddReq{from, id, lac, payload, recovery}` | handle -> bookie | none | AddRequest (`FLAG_RECOVERY_ADD`), PendingAddOp.sendWriteRequest |
| `AddResp{bookie, id, fenced}` | bookie -> handle | 1000+id | AddResponse EOK / EFENCED |
| `ReadReq{from, id, fence}` | w2h -> bookie | none | ReadRequest with FENCE flag; id -1 = `LAST_ADD_CONFIRMED` |
| `ReadResp{bookie, id, rc: Ok{payload, lac} / NoEntry}` | bookie -> w2h | 3 (LAC read) or 100000+id | ReadResponse EOK / ENOENTRY (ENOLEDGER) |
| `ZkRead{from}` / `ZkMeta{meta, version}` | handle <-> zk | 2 | LedgerManager.readLedgerMetadata |
| `ZkWrite{from, meta, expected}` / `ZkWriteOk{version}` / `ZkBadVersion` | handle <-> zk | 2 | LedgerManager.writeLedgerMetadata with version check |

### Bookie `bN` (`bookie`)
State: entries `id -> (payload, piggybacked LAC)`, `fenced`. Receives any request:
- `AddReq`: if not recovery and fenced -> `AddResp{fenced: true}` (BookieImpl.addEntry l.1112,
  LedgerFencedException -> EFENCED in WriteEntryProcessorV3); else store and confirm
  (recoveryAddEntry l.1024 never checks the fence; WriteEntryProcessorV3 l.112-115 dispatch).
- `ReadReq`: if `fence`, fence first (ReadEntryProcessorV3 l.216, BookieImpl.fenceLedger l.1153),
  then read (l.219): id -1 returns the highest stored entry (the LAC read), otherwise entry `id`;
  missing -> `NoEntry` (l.244). The response carries the payload and the LAC piggybacked on the entry.

### zk (`zk`)
Metadata `{state: Open|InRecovery|Closed, last}` and a version (initially Open, -1, version 0:
the ledger exists, created by w1). `ZkRead` returns both; `ZkWrite` succeeds iff `expected` equals
the version (version + 1), else `ZkBadVersion`.

### w1h (`writer_handle`)
Receives `Add(n)` from w1 only. If the handle is not writable (`isHandleWritable`, l.550-551) ->
`Rejected(n)` at once (doAsyncAddEntry l.1716 ff.). Else `id = ++lastAddPushed`, `AddReq` to
the three bookies carrying `lastAddConfirmed` (PendingAddOp.initiate l.246-261), then receives
the responses for `id`: at the 2nd confirmation the op completes, `lastAddConfirmed = id`,
`Acked(n)` (writeComplete, sendAddSuccessCallbacks l.2225); at a Fenced answer before that
(writeComplete l.344) -> handleUnrecoverableErrorDuringAdd (l.2180): cached metadata Open ->
asyncCloseInternal -> doAsyncCloseInternal (l.576 ff.): drain the pending add -> `Rejected(n)`,
handle CLOSED, `lastEntry = lastAddConfirmed`, then the metadata update loop at zk
(`close_metadata`, see D5). Every later `Add` is `Rejected` at once.

### w2h (`recovery_handle`)
1. Receives `Recover` from w2. Open with recovery: `ZkRead`; if Closed reply `Closed` at once
   (cannot happen); else `ZkWrite{InRecovery}` at the read version (retry on BadVersion).
2. ReadLastConfirmedOp.initiateWithFencing (LedgerRecoveryOp l.146): `ReadReq{id: -1, fence}` to
   all three; OK and NoEntry are both valid responses (ReadLastConfirmedOp l.129, l.143);
   completes at COVER = 2 valid responses with the maximum LAC (l.157). The third answer is ignored.
3. `lastAddPushed = lastAddConfirmed = max(LAC, -1)` (LedgerRecoveryOp l.120).
4. Recovery reads, batch size 1, starting at LAC + 1 (doRecoveryRead l.166): a SequenceReadRequest
   (PendingReadOp l.79) with the fencing flag (sendReadTo l.181-182): read from replica j = 0 of
   the write set (bookie `(id + j) % 3`, RoundRobinDistributionSchedule l.59); the FIRST positive
   answer completes the read (readEntryComplete l.109, complete l.215/424); a NoEntry counts a
   missing bookie: at 2 missing the read ends with NoSuchEntry (l.411), otherwise the next replica
   is read (l.418 `!readsOutstanding() -> sendNextRead`).
5. onEntryComplete OK (l.176-206): `readCount++`, recovery add of the entry (`++lastAddPushed`,
   checked equal to the entry id) to the three bookies, then immediately the next read: reads and
   recovery adds are pipelined as in the source. NoSuchEntry: `readDone`.
6. Recovery add responses: 2 confirmations complete an op; completion in entry order advances
   `lastAddConfirmed` and `writeCount` (sendAddSuccessCallbacks, addComplete l.246).
7. When `readDone && readCount == writeCount`: close: `close_metadata` writes
   `Closed(lastAddConfirmed)` at the cached version (doAsyncCloseInternal l.600, l.623-660), then
   `Closed(last)` to w2.

## Method map

| BookKeeper method | port |
|---|---|
| LedgerHandle.doAsyncAddEntry, isHandleWritable | `writer_handle` (head of the loop) |
| PendingAddOp.initiate / sendWriteRequest | `writer_handle`, `send_all(AddReq)`; `recovery_handle` (recovery add) |
| PendingAddOp.writeComplete (ack set, completeBookieAndCheck) | `writer_handle` inner loop; `recovery_handle` `AddResp` arm |
| LedgerHandle.sendAddSuccessCallbacks | `writer_handle` (Acked); `recovery_handle` `while let` over `pending` |
| LedgerHandle.handleUnrecoverableErrorDuringAdd, errorOutPendingAdds | `writer_handle` fenced branch |
| LedgerHandle.asyncCloseInternal / doAsyncCloseInternal (+ MetadataUpdateLoop) | `writer_handle` close, `close_metadata` |
| LedgerRecoveryOp.initiate / readLastConfirmedDataComplete | `recovery_handle` steps 2-3 |
| ReadLastConfirmedOp.initiateWithFencing / readEntryComplete | `recovery_handle` step 2 |
| LedgerRecoveryOp.doRecoveryRead / onEntryComplete / addComplete / submitCallback | `recovery_handle` loop |
| PendingReadOp.SequenceReadRequest.read / sendNextRead / sendReadTo / logErrorAndReattemptRead / complete | `send_next_read`, `ReadResp` arm |
| LedgerHandle.asyncRecoveryAddEntry | `ReadRc::Ok` arm (AddReq with `recovery: true`) |
| WriteEntryProcessorV3.getAddResponse | `bookie` `AddReq` arm |
| ReadEntryProcessorV3.getReadResponse / readEntry / sendFenceResponse | `bookie` `ReadReq` arm |
| BookieImpl.addEntry / recoveryAddEntry / fenceLedger / readEntry / readLastAddConfirmed | `bookie` |
| LedgerDescriptor setFenced / isFenced | `bookie` `fenced` |
| LedgerManager read / write (interface) | `zk`, `zk_read`, `zk_write` |

## Decisions and simplifications

- D1 Entry numbering. Internally the source's 0-based entry ids with INVALID_ENTRY_ID = -1;
  `Add(i)` is entry id i - 1 and its payload is i. `Closed{last}` reports the entry NUMBER of the
  last entry (id + 1), -1 when the ledger is empty, so it is comparable with `Acked(i)`. Open in
  the interface (`Closed{last}` "last entry id, -1 when empty" vs. entries numbered 1..e); the
  alternative (report the raw 0-based id) is a one-line change in `entry_number`.
- D2 Recovery read success. Follows the code: the first positive answer completes a read
  (PendingReadOp l.109, l.215/424); the write-back then needs Qa = 2. An earlier INTERFACE.md text
  said 2 positive answers; the first version of this port encoded that and was corrected after the
  coordinator's correction (the source has no 2-answer rule).
- D3 Recovery reads are sequential per replica (SequenceReadRequest, the default: `parallelRead`
  is false, l.79): one replica at a time, the next one after a NoEntry. The replica order is the
  write-set order `(id + j) % 3`; ReadOpBase/LedgerEntryRequest (where the write set is built and
  possibly reordered by the placement policy) is outside the cut: no reordering.
- D4 Speculative reads (l.87-88, readSpeculativeRequestPolicy) are timer-driven: not ported (a
  read is never re-sent except through the NoEntry path). Add and fence timeouts
  (`maybeTimeout`, `timeoutQuorumWait`, `fenceResult.get(1000 ms)`) are not ported: every request
  is answered.
- D5 The writer's close at zk. doAsyncCloseInternal's MetadataUpdateLoop (l.623-660; the loop
  class itself is outside the cut) writes Closed at the cached version and, on a conflict,
  re-reads and re-tests `metadata.isClosed()` (l.627). The writer's cached metadata is version 0
  and the fence implies InRecovery at zk, so the first write always fails. On the re-read: Closed
  -> stop (same lastEntry: nothing to do; different: BKMetadataVersionException, the writer's
  close fails); InRecovery -> STOP, the writer does not overwrite the recovery's state. The
  source leaves this to the loop outside the cut; the decision follows the handle's own rule that
  a ledger in recovery must not be closed by the writer (l.2181-2186 "otherwise we may lose
  entry") and l.2413-2419 (re-read InRecovery -> handleUnrecoverableErrorDuringAdd). It is
  invisible either way: Rejected is sent before the metadata loop, and zk's state is not changed.
  The writer runs this loop to completion before serving the next Add (later Adds are rejected
  from the CLOSED handle state, set before the loop, whatever its outcome).
- D6 Late responses. A completed add op ignores later responses (PendingAddOp l.300
  `if (completed) ... return`; the ensemble-change bookkeeping there is out of scope), a completed
  LAC read and a completed recovery read ignore later answers: the port never receives them
  (tagged receives); they stay in the mailbox.
- D7 Open with recovery (LedgerOpenOp, outside the cut): read the metadata and mark InRecovery by a
  versioned write, retrying on BadVersion; if already Closed, reply `Closed` at once. The ledger
  is created before the run (zk starts Open, version 0; w1h's cache is that metadata).
- D8 w2's "nondeterministic moment": no `nondet()` is added (the verbatim client has none); the
  moment is the scheduling of w2's send and of w2h's messages relative to the others, which Must
  explores. Every choice in the protocol is message-arrival order; the port has no `nondet()`.
- D9 LAC read answer: the LAC piggybacked on the bookie's highest stored entry (the source reads
  entry LAST_ADD_CONFIRMED and the client extracts the LAC from that entry's header,
  ReadLastConfirmedOp verifyDigestAndReturnLastConfirmed); NoEntry when the bookie has none.
  Lengths, digests, master keys, the journal and ledger storage are omitted (in-memory map).
- D10 Unreachable branches kept as checks: a recovery add rejected (assert), a recovery close
  conflict (assert), handleUnrecoverableErrorDuringAdd's IN_RECOVERY branch for w1h (reachable
  only from a cache updated after a close; kept as errorOutPendingAdds only), every replica
  missing (assert in `send_next_read`; NoSuchEntry ends the read at 2 missing first).
- D11 No timer-only loops exist in the ported paths; the zk retry loops end because zk's state
  only moves Open -> InRecovery -> Closed, so the exploration terminates for every e.

## Build and runs

Compile (inside this directory):
`TMPDIR=/local/home/mkhoshechin/tmp cargo build --release -j 2` -> `Finished release profile`, no warnings.

Runs (2026-11-05, after the D2 correction):

`timeout 600 target/release/bench_bk_port count impl --model fifo -- 1` -> exit 0, exhausted in ~21 s:
```
Executions attempted so far: 10000 total 10000 finished normally 0 blocked (909.09/sec).
execs=15912 blocked=0
--- measurements
...
stop_reason: exhausted
max_graph_events: 120
```

`timeout 600 target/release/bench_bk_port count impl --model fifo -- 2` -> killed by the timeout (exit 124):
```
Executions attempted so far: 300000 total 300000 finished normally 0 blocked (636.94/sec).
```
(no `execs=` line: the exploration did not finish within 600 s).
