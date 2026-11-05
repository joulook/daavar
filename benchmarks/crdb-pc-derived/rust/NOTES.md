# crdb-pc-derived: the specification derived from the parallel-commits design document

Source: `cr-src/docs/20180324_parallel_commit.md` (the RFC; called "the document" below) and
`INTERFACE-agents.md` (client code, message types, names, parameters, delivery model).
Vocabulary: the document's STAGED = STAGING, promised writes = in-flight writes,
EndTransaction = EndTxn.

## 1. Shape (revision 2, after the review derived-review/REVIEW.md)

One object thread `obj` (invisible) serves `c` and `p1..pm` (visible, client code verbatim from
the interface). An invisible thread `init` sends `obj` the clients' addresses (tagged message,
tag 1) before anything else; the main thread starts the threads and joins them, it sends and
receives nothing. Threads are started in the order `obj`, `c`, `p1..pm`, `init`.
`(n, p) = (1, 0)` is rejected (panic in the argument parser), as the interface excludes it.

The object's state is only what clients can observe: T's **fate** as reported so far
(undecided / Committed / Aborted), c's request awaiting a reply (a pending `Commit` = the
*commit window* is open: c has invoked Commit and has no answer yet), and the preventers
received and not yet answered (*pending*). No range, record, intent, timestamp or in-flight
write set is data. The object's `None` fate means "no client has been told a fate"; the hidden
internal decision is chosen existentially at the moment of the first fate-bearing reply (§4.3).

Each step the object collects every enabled action and picks one with `nondet()` (a chain of
k-1 binary choices among k actions):
- `RecvC`: receive c's next request (enabled when c is not waiting and not finished);
- `RecvP(i)`: receive pi's `Recover` (once per preventer);
- `ReplyC(rep)`: answer c's pending request with an allowed reply (§3);
- `ReplyP(i, fate)`: answer pending preventer i with an allowed fate (§4).
Receipts and replies are separate actions, so a reply (and the internal action it stands for)
can be delayed past other receipts: asynchronous delivery, and a recovery whose push/probe
happens later than its request's arrival. The loop ends when c is finished and all m preventers
are answered (no action left), so no client is left waiting and `obj` ends.

## 2. Fate: decided once, agreed by everyone

- "A transaction is committed if and only if a) there is a transaction record with status
  COMMITTED, or b) one with status STAGED and all of the intents written in the last batch of
  that transaction are present." -> T has one fate; once Committed (implicitly or explicitly)
  it stays Committed.
- "if all intents were found in place, propose a committing EndTransaction. Note that the fact
  that we discovered the transaction as committed implies that the original client, if still
  alive, can only ever issue an EndTransaction that attempts to COMMIT the transaction as well."
  -> a preventer that resolves a committed T reports Committed; c then gets Committed.
- "Status resolution only writes a single key (the transaction record) and does so
  conditionally, and intents are only resolved after this conditional write. As a result,
  having multiple status resolutions racing does not cause anomalies." and the example where
  the second resolution "fails and realizes that the transaction is now committed" -> all
  preventers report the same fate.
- "In this case our aborting EndTransaction would fail as it checks the provisional commit
  timestamp against the record's." / "the resolution process observes a transaction record with
  new activity and waits, observing the commit shortly thereafter" -> a preventer's report is the
  final fate even when its own abort attempt failed.
- "it could erroneously return a transaction aborted error even though the transaction actually
  committed" (named as the race the design avoids) -> c never sees Aborted for a committed T, and
  never Committed for an aborted T.
Every reply that names a fate (c's Committed / Aborted, pi's Recovered(f)) sets the fate if it
is undecided and must equal it otherwise.

## 3. c's replies

- `Put(k)` -> always `Written(k)` (deferrable). The interface's setting gives no failure of a
  pipelined write (blind writes, no other writers of k1..kn, no failed replication, no AbortSpan);
  its client only accepts Written. Written does not mean committed.
- `Commit` (window open):
  - `Committed` if fate != Aborted. l.93 "the coordinator can return to the client early when it
    knows that these writes have succeeded"; l.98 "on success of all writes, return to the
    client"; l.108 the commit condition. The transition stands for the staging EndTxn and every
    promised write having succeeded (T implicitly committed) before the acknowledgement; when fate
    is already Committed (a recovery found it implicitly committed, §4) it is the only reply.
    It is allowed also while a preventer is pending, including one that prevented a final-batch
    write: l.308 "the transaction may have restarted and written a new STAGED record with a higher
    provisional commit timestamp, and may now be (implicitly or explicitly) committed. In this case
    our aborting EndTransaction would fail"; l.334 "An intent at t1 is prevented, but before the
    transaction record can be aborted, the transaction restarts and manages to (implicitly)
    commit"; l.245 "the client (TxnCoordSender) restarting (includes refreshing)". In this
    interface the pushed write is absorbed by a refresh that always succeeds and a re-sent EndTxn
    that may commit before the recovery's conditional abort.
  - `Aborted` if fate == Aborted, or fate undecided and some preventer is pending. An abort needs
    an actor, and in this setting the only actors are the preventers (no coordinator rollback, no
    failure): either the push aborts a missing record before the coordinator's staging EndTxn
    reaches it (interface: a missing or PENDING record is aborted by the push; the later EndTxn
    then finds the record aborted), or the recovery prevents a final-batch write and aborts the
    still-matching STAGED record: l.296 prevention "is only possible if it isn't already there";
    l.308 "if an intent was prevented, abort the transaction"; l.346 "the abort would have beaten
    the Stage(k2) and the transaction would be aborted". The pending preventer stands for a handle
    that has already aborted T internally and whose report to its client is still delayed.
  - **No `Retry`** (removed in revision 2). The document's causes of a retry error (l.194 "their
    timestamp can be pushed, they can catch a WriteTooOldFlag, or they incur a straight-up
    TransactionRetryError"; l.249 a pushed timestamp synthesizes a retry error "up the
    TxnCoordSender's interceptor stack"; l.390 an intent "found at the wrong timestamp" is "treated
    like ... having pushed the transaction") all stay inside the coordinator here: l.245 says the
    TxnCoordSender restarts "(includes refreshing)", and in this setting the refresh always
    succeeds (blind writes, no reads) so the push becomes a re-sent EndTxn, not an error to c.
    WriteTooOld needs another writer of T's keys (none); TransactionRetryError has no source.
    A prevented pipelined write is impossible (pipelined writes are answered, hence present, before
    Commit). A prevented final-batch write is followed by the recovery's abort or by the
    re-sent commit, i.e. Aborted or Committed. The sentence of revision 1 (l.377 "If we manage to
    prevent a write, we may return to the client with a transaction retry error instead") belongs
    to "Improving Ambiguous Results", which follows an ambiguous RPC error; this setting has none.
    Consequently c runs exactly one epoch for every `a`; the `Retry` variant is kept only because
    the interface fixes the message type (it causes the one compiler warning "variant `Retry` is
    never constructed").

## 4. Preventers' replies

- `Recovered(Committed)` if fate == Committed, or fate undecided and c's commit window is open.
  l.310 "if all intents were found in place, propose a committing EndTransaction"; l.296 the
  QueryIntent checks transaction, timestamp and sequence; l.93 "If they are, the transaction must
  be marked committed". The undecided branch stands for the discovery of an already established
  implicit commit: while c's Commit is outstanding, the staging and all writes may have succeeded
  with c's acknowledgement still delayed. Outside a window it is impossible: before c's Commit
  there is no record (interface: "T's record is created only by T's EndTxn"), and
  after c's reply the fate is fixed.
- `Recovered(Aborted)` if fate != Committed.
  1. Once the object has decided Committed (by c's Committed or any preventer's Committed), no
     abort follows: the guard `fate != Committed` forbids it for every later preventer reply and
     `fate != Aborted` guards c's Committed. This is l.93 "If they are, the transaction must be
     marked committed" and l.350 agreement, and l.352 "it fails and realizes that the transaction
     is now committed": a failed abort attempt is reported as the fate it then observes.
  2. The undecided-abort branch stands only for (a) a push that beat the staging EndTxn (missing
     record aborted; possible at any time before c's Commit and during the window before the
     staging lands), or (b) a recovery that prevented a final-batch write against a still-valid
     STAGED record (l.296, l.308, l.346). It never stands for an abort of an implicitly committed
     T.
  3. Why "undecided while the window is open" needs no further restriction: no visible event
     before the first fate-bearing reply implies that T is already implicitly committed (Written
     only says a pipelined intent is present; the staging is invisible). So for any object history
     in which preventer i replies Aborted while the fate is undecided, the hidden execution can
     place i's push at (or after) the object's receipt of i's Recover and before the staging
     EndTxn lands, or its prevention before the last final-batch write lands; in that execution T
     is never implicitly committed, and every other reply of the history (all Aborted, by §2) is
     consistent with it. When n = p (no final-batch write) only case (a) applies, and it is still
     available, because the record does not exist until the staging EndTxn is evaluated. Hence the
     abstraction admits no history that requires an abort after an implicit commit.
- A preventer may stay pending across c's replies (its report deferred), as in l.334-l.344:
  prevented at t1, the abort fails, the coordinator commits, the resolution then observes the
  commit.

## 5. Decisions where the document is silent (or this setting differs)

1. The document's resolution starts only for an *abandoned* record (l.304 "wait until the
   transaction is abandoned"); the interface makes every push win (no heartbeats, no wait queue),
   so a resolution may run concurrently with a live coordinator at any time.
2. Retry: none (§3). This rests on the interface's statement that the refresh always succeeds
   here; the document itself does not say what the coordinator does after a refresh.
3. Committed after a prevented write is allowed (§3), from l.308/l.334 and the interface's
   refresh-and-resend path.
4. Written is never refused and never depends on the fate (no AbortSpan in the interface).
5. The `Commit` keys and `n` play no role in the visible behaviour beyond c's own messages; `p`
   only fixes the number of Put/Written pairs; `a` plays no role (no Retry). Hence `-- 1 1 1 1`
   and `-- 2 1 1 1` give the same count, and `-- 2 1 2 2` gives the same count as `-- 2 1 2 1`
   (checked: execs=588). The coarse abstraction does not distinguish when a final-batch write
   can be prevented (n > p) from when only a pre-staging push can abort (n = p); §4.3 explains why
   both admit the same visible outcomes.
6. `(n, p) = (1, 0)` is rejected (revision 2): the binary panics "n = 1, p = 0 is a one-phase
   commit and not an instance" (exit 101).
7. Crate name `bench_crdb_derived` (as the prompt says) instead of the interface's `crdb_pc`.
8. Delivery model: only `fifo` was run; the spec does not depend on it (the object receives
   from a chosen sender by a tagged receive, and each client has at most one message in flight
   to the object).
9. No ambiguous results, no record GC, no recreation (the interface has no failures and no GC),
   so "Transaction record recreation" (l.312-l.318) and "Improving Ambiguous Results"
   (l.375-l.379) contribute nothing beyond §2's agreement.

## 6. Runs

`cargo build --release -j 2`: success, one warning (variant `Retry` is never constructed, §3).
`timeout 600 target/release/bench_crdb_derived count spec --model fifo -- <args>`. The counts
are executions of the object's action schedules (Put and Recover receipts are separate from
their replies), not distinct visible histories; every instance has the same outcome vectors:
c Committed / all preventers Committed, and c Aborted / all preventers Aborted.

| args     | revision 1          | revision 2 (current) |
|----------|---------------------|----------------------|
| 1 1 1 1  | execs=31 blocked=0  | execs=23 blocked=0   |
| 2 1 1 1  | execs=31 blocked=0  | execs=23 blocked=0   |
| 2 2 1 1  | execs=52 blocked=0  | execs=40 blocked=0   |
| 2 0 1 1  | execs=14 blocked=0  | execs=10 blocked=0   |
| 2 1 2 1  | execs=900 blocked=0 | execs=588 blocked=0  |

All exit 0; the last printed line of each run is the measurement block's `cache_hits: `.
`-- 1 0 1 1` exits 101 with the rejection message.
