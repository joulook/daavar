# bk-derived: the specification of what w1 and w2 see (BookKeeper master)

Sources read (only these): `plan/case-studies/bookkeeper/model/README.md`,
`model/BookKeeperProtocol.tla`, `model/MessagePassing.tla`, `INTERFACE-agents.md`, TraceForge
`lib.rs` / `program.rs`, the rust template.

## Shape

Threads: `ledger` (the spec object, internal), `w1`, `w2` (visible, client code verbatim from
INTERFACE.md), `init` (internal: sends `ledger` one tagged `Setup{w1,w2}` before anything else;
the main thread sends nothing). Main spawns ledger, w1, w2, init and joins them all. `ledger`
ends once w1's e Adds are all answered and w2 has its `Closed` (asserted). No blocked executions.

`ledger` is a loop; at each turn it lists the enabled steps and picks one with `nondet()`
(`choose`). No bookie, quorum, fencing flag, LAC or metadata version is data. The state is only
what the clients can observe or what constrains what they can still observe:
`acked` (w1 got Acked 1..acked), `pend` (w1's Add being served), `may_ack`, `handle_closed`,
`issued` (highest entry sent to bookies), `recover`, `covered`, `recovered`, `closed`.

| Step | Enabled when | Effect | Follows |
|---|---|---|---|
| Recv | w1 has an Add due (last one answered, more to come) or Recover not yet received | Add(i): if handle closed, reply `Rejected(i)` at once; else `pend=i`, `issued=i`, `may_ack = !covered`. Recover: `recover=true` | INTERFACE: "every later Add of w1 is also Rejected, replied at once"; TLA `ClientSendsAddEntryRequests`, `ClientStartsRecovery` |
| Ack | `pend` and `may_ack` | reply `Acked(pend)` | TLA `BookieSendsAddConfirmedResponse` + `ClientReceivesAddConfirmedResponse` (LAC advances when Qa reached and all prior entries reached Qa; implicit ack to caller) |
| Reject | `pend` and `recover` | reply `Rejected(pend)`, handle closed | TLA `BookieSendsAddFencedResponse` (fenced bookie rejects non-recovery add) + `ClientReceivesAddFencedResponse` (`CLIENT_WITHDRAWN`); a bookie is fenced only by a recovery message, hence after Recover |
| ReadPos | `recover`, not closed, `x <= issued` | `covered=true`, `recovered=x` (written back) | TLA `ClientSendsRecoveryReadRequests`, `ReadStatus` "generous on positive results" (one OK suffices), `ClientWritesBackEntry` + `RecoveryClientReceivesAddConfirmedResponse` (recovery adds accepted when fenced) |
| ReadNeg | `recover`, not closed | `covered=true`; if `x == pend` then `may_ack=false`; reply `Closed(x-1)` (`-1` if `x==1`) | `ReadStatus` NoSuchEntry by quorum coverage (Qw-Qa+1 = 2), `ReadNegative` (`last_recoverable_entry = id-1`), `RecoveryClientClosesLedger`; `BookieSendsReadResponse` fences at master |

where `x = max(recovered, acked) + 1`, the entry the recovery reads next.

## Why these are the visible consequences

- **Acknowledgement.** An entry is Acked once 2 bookies confirmed it and all lower entries
  are acked (TLA `MaxContiguousConfirmedEntry`, INTERFACE). w1 has at most one Add in flight
  (the client waits for each reply), so in-order acking is automatic.
- **Fencing guarantee (coverage).** Recovery proceeds to reads once 2 bookies (one of every
  ack quorum, `HasQuorumCoverage`) answered the fencing LAC read. From then on at most one
  bookie is unfenced, so an entry *sent* after coverage reaches at most one bookie and is
  never Acked (`may_ack = !covered` at issue). An entry sent before coverage may still reach
  Qa afterwards (its stores precede the fences), so it can be Acked after coverage, and even
  after `Closed` is replied.
- **Recovery reads.** Entries `<= acked` are on >= 2 bookies, so with at most one NoSuchEntry
  their reads are positive: deterministic and invisible, so the read index starts at
  `max(recovered, acked)+1`. The LAC from the fencing reads only shortens this prefix;
  it is invisible. Entry `x` can be read positive iff some bookie may hold it, i.e. iff the
  handle sent it (`x <= issued`); negative is always possible for `x > acked` (two bookies
  answer NoSuchEntry before the write reaches them).
- **Monotonic reads (the master fix).** A negative read of `x` fences the two bookies that
  answered, so `x` can never reach Qa: `may_ack=false` when `x` is pending; when `x` is not yet
  sent it will be issued after coverage and `may_ack` is false anyway. Hence at master an
  entry Acked to w1 is never absent from the closed ledger: `Closed(L)` implies every Acked
  entry `<= L` (TLA invariant `NoDivergenceBetweenClientAndMetaData`; also
  `AllCommittedEntriesReachAckQuorum`, since write-back uses recovery adds that fenced
  bookies accept).
- **Close.** The recovery closes with the last entry it wrote back: `L = x-1`. Possible values:
  `L in {K, K+1}` where `K` is w1's final acked count, `K+1` only if w1's handle sent entry
  `K+1` (it is then Rejected, or Acked when it was sent before coverage).
- **Writer after fence/close.** After one Fenced answer the handle is closed: the pending Add
  and every later Add are Rejected (later ones at once, without reaching bookies, so they are
  never recovered).
- **4.13 (not modelled, for the record).** Without fencing on recovery reads, a NoSuchEntry
  answer from the one unfenced bookie can later turn positive (README Defect 1: the entry at
  Qa-1 reaches the unfenced bookie after the recovery closed), so at 4.13 w1 can see
  `Acked(i)` with `Closed(L)`, `L < i`. In this program that is exactly the behaviour the
  `may_ack=false` line of ReadNeg forbids. Defect 2 (invalid fragments) needs ensemble
  changes, excluded by the interface.

## Decisions where the documents were silent or I abstracted

1. **Coverage moment merged into the first read.** Coverage is invisible except that entries
   sent after it cannot be acked. Moving it later (to the first read) only allows more acks
   for entries sent in between, and that later moment is itself a real schedule (fence
   responses delayed), so the visible set is unchanged. Hence no separate coverage step.
2. **`last` for an empty ledger is `-1`** (INTERFACE), while entries are 1..e; `last` takes
   values in `{-1, 1, ..., e}` and never 0. If the ports use 0 for empty (TLA LAC starts at 0),
   this is the one place to change.
3. **Rejection before coverage** is allowed as soon as Recover was received (one fenced bookie
   suffices for a Fenced answer, TLA `BookieSendsFencingReadLacResponse` fences immediately).
4. **Rejection of an entry the recovery read positive** is allowed (the other bookies fenced
   before w1's write reached them, the recovery read it from the one that has it).
5. **Excluded actions.** `ClientClosesLedgerSuccess/Fail` (w1 only adds, per INTERFACE),
   `ClientChangesEnsemble`, `ResendPendingAddOp`, `RecoveryClientChangesEnsemble` (no ensemble
   change), timeouts / lost messages of `MessagePassing.tla` (every message delivered), and
   `ReadUnknown`/`RECOVERY_ABORTED`: with all 3 read responses delivered either one is OK or at
   least 2 are NoSuchEntry, so Unknown never arises and the recovery always closes. One
   recovery client, so the metadata version check of `RecoveryClientClosesLedger` always passes.
6. **Recover timing** is the order in which `ledger` receives w2's Recover among w1's Adds (a
   receive choice); w2's own code is the verbatim client.
7. **Redundancy.** Different internal paths can give the same visible behaviour (e.g. the
   order of an invisible ReadPos and a Recv); the program is not deduplicated.
8. **Causality.** Being one object, every reply is causally after every request the object
   received before it; in particular Rejected and Closed are after Recover, as they are in the
   protocol (fencing is caused by the recovery).

## Runs (`count spec --model fifo -- e`)

| e | execs | blocked |
|---|---|---|
| 1 | 18 | 0 |
| 2 | 69 | 0 |
| 3 | 172 | 0 |
