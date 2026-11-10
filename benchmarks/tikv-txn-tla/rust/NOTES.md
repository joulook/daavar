# bench_tikv_tla: specification program from PingCAP's TLA+ model

Source: `DistributedTransaction.tla` (pingcap/tla-plus 5649f86), cited below as **DT:line**, and
its `Test1`-`Test5` `.tla`/`.cfg` files. Interface: INTERFACE.md (agents' copy), cited as **IF §n**.
Files read: those, `traceforge/src/lib.rs`, `traceforge/src/program.rs`, the rust template,
and, for one trait bound, `traceforge/src/thread.rs` and `msg.rs`. These two are part of the
API but are not among the files the prompt names; see Q8. Round 1 (section 9) also read the
review `REVIEW-astra-tla.md`, the full `INTERFACE.md`, `DECISIONS.md`, and, in TraceForge,
`conformance/compact.rs` (how a summary's order counts are compared) and the `assume` /
`nondet` code in `lib.rs` and `must.rs`.

Program: `src/main.rs`. Entries: `traceforge::program::main(Program { visible, spec, imp })`,
with `spec` = `imp` = the specification (IF §9).

## 1. Structure

| Process | Name | Model part | Lines |
|---|---|---|---|
| one per key | `k1..kn` | `key_data[k]`, `key_lock[k]`, `key_write[k]` and every server action on k | DT:39-74, 360-741 |
| one | `ts` | `next_ts` | DT:91-94, 746 |
| one per client, invisible | `h1..hm` | the client actions and `client_stage/ts/key/read` | DT:76-89, 211-356 |
| one | `sup` | setup, quiescence detection (D7), end of the run, the global invariants | DT:779-878 |
| visible | `c1..cm` | client code, verbatim (IF §5.1) | — |

Start order: keys, `ts`, handlers, clients, `sup`. `sup` sends each key the key addresses and
its own address, and each handler its client's and its own address. The main thread only
spawns and returns. (Round 0 called `sup` `ctl`; renamed because IF §2 reserves `c*` names.)

**D1 Messages.** Each model request (`ReqMessages`, DT:116-143) becomes a message to the key
process. Each response (`DirectRespMessages` and `RespMessages`, DT:164-179) becomes a tagged
message to the handler that sent the request. In the model a response is a direct call into the
client's handler, atomic with the server step (DT:145-163). Here the server step and the
client's handling are two steps.

**D2 Dropped branches.** Every client response handler in the model has a first disjunct
`UNCHANGED <<req_msgs, client_vars, next_ts>>` that ignores the response (DT:229, 240, 268, 284,
323, 345). All of them are dropped, as the prompt says. Consequence used in D8: when the
remaining disjunct's guard is false (DT:275, 289), the server action that would produce that
response is not enabled, so the request has no answer in that state.

**D3 Exactly-once delivery.** In the model `req_msgs` is the set of all requests ever sent, and a
server action may evaluate the same request again and again (DT:25-35). Here every message is
delivered and evaluated exactly once (IF §1, §7). Where the model's re-evaluation matters (a
request that met a lock is evaluated again after the state changed), the handler sends the
request again (D5, D7).

## 2. Keys (`Key`, model lines followed)

- `ServerReadKey` DT:447-477: read succeeds when there is no lock, the lock is a `lock_key`
  lock, or the lock is the reader's own; otherwise the reply is `key_is_locked`. The read value
  is `find_readable_commit(k, start_ts)` (DT:403-411, 414-427).
- `ServerLockKey` DT:479-537, including the `lock_key_aborted` branch (DT:494-500), the
  write-conflict branch with its `~ \E w2 \in all_commits: w2.start_ts = req.start_ts` guard
  (DT:516-523), and the own-lock branch (DT:526-529).
- `ServerPrewriteOptimistic` DT:539-577. `ServerPrewritePessimistic` DT:579-605.
- `ServerCommit` DT:607-640. `ServerCheckTxnStatus` DT:701-715, with
  `check_txn_status_has_lock` DT:643-666 and `check_txn_status_missing_lock` DT:669-686.
- `ServerResolveCommitted` DT:717-728. `ServerResolveRollbacked` DT:730-741.
- Operators: `unlock_key` DT:361, `commit` DT:364-370, `rollback` DT:373-399 (protected flag,
  collapse of unprotected older rollbacks, no record when one with `ts = start_ts` exists),
  `keyCommitted` DT:783.

Decisions where the model is silent or cannot be taken literally:

**D4 Values.** The model's `key_data[k]` is a set of start_ts ("we don't care about the
concrete value", DT:39-41). The interface needs the written value (the writer's index). So
`data` maps start_ts to value, the prewrite request carries the value, and a read replies with
`data[w.start_ts]` of the readable commit, or 0 when there is none. The model's `value_ts` is
kept as well, for the snapshot-isolation invariants.

**D4a Push choice.** In `check_txn_status_has_lock` the two disjuncts are a TLA+ choice.
"Push min_commit_ts" (DT:659-666) is enabled only when `lock.min_commit_ts <= caller_start_ts`.
It is a `nondet()` exactly when it is enabled; otherwise the first disjunct is taken. Every
check is sent with `caller_start_ts = NoneTs` (DT:247). Every lock is created with
`min_commit_ts = NoneTs` (DT:510, 560, 599). So the push is enabled once per lock and sets
`min_commit_ts` to 1.

**D4b `Silent`.** In some states no disjunct of the server action is enabled for the request:
- `ServerLockKey` when the latest commit is newer than for_update_ts and the requester's own
  commit record exists;
- `ServerPrewriteOptimistic`'s `UNCHANGED vars` branch for a duplicated prewrite (DT:547-549).

The model would leave such a request in `req_msgs` with no answer. The key replies `Silent`
(invisible), and the handler answers `Aborted` (R2, D8 (e)).

**D4c `commit_ts_expired`.** `ClientRetryCommit` tests `client_stage[c] = "commiting"`
(DT:348), a stage that never exists (DT:198 has "committing"). So the model never handles
`commit_ts_expired`, and the commit request stays unanswered: the handler answers `Aborted`
(R2, D8 (e)). The branch is unreachable anyway: every commit_ts is >= 1 and min_commit_ts is 0 or 1.

**D6 Resolution messages.** `ServerResolveCommitted/Rollbacked` pick any key holding a
matching lock (`\E k \in KEY`, DT:723, 736). The primary's key sends the resolution message to
every other key, and each key applies it if it holds a lock with that primary and start_ts.
Otherwise the message has no effect and is consumed (exactly once, D3). Two departures:
- The primary itself is skipped: its lock of that start_ts is already gone, and it cannot come
  back because of the rollback or commit record.
- In the model a resolution request that arrives before the lock exists stays in `req_msgs`
  and could roll back a lock created later. Here it does not (Q2; approved in round 1,
  finding 8, as the departure IF §7 / G4 authorizes).

**D6a Local invariants.** After every step a key asserts the per-key invariants
`WriteConsistency` (DT:822-829), `UniqueLockOrWrite` (DT:833-837) and `UniqueWrite`
(DT:840-843).

## 3. Timestamps (`ts_process`)

A counter starting at 1 (DT:746). Each request gets the current value, and the counter is
incremented, as `next_ts' = next_ts + 1` does in DT:217, 257, 291, 336, 351.

## 4. Handlers (`Handler`, the model's client actions)

The model's client does each stage in one batch. `ClientReadOptimistic` sends all reads at
begin (DT:212-222). `ClientLockKey` sends all locks at begin (DT:252-264). The prewrites go out
together (DT:299-319), then the commit (DT:331-341). The interface's client sends one operation
at a time. The mapping:

- `Begin`: start_ts from `ts` (DT:215, 255). For a pessimistic client also
  `for_update_ts = start_ts` (DT:255). Reply `Begun`.
- `Get(k)`, optimistic: one `read_optimistic` request for k (DT:218-221). The reply
  `read_optimistic_succeed` is recorded in `client_read` (DT:227-236, `add_client_read_history`
  DT:224) and answered `Value(v)`.
- `Put(k, v)`, optimistic: buffered, reply `Done`, no message. The model's writes are just
  `CLIENT_WRITE_KEY`.
- `Put(k, v)` / `GetForUpdate(k)`, pessimistic: one `lock_key` request for k, with
  `for_update_ts = start_ts` (DT:255, 263; D15).
  - `lock_key_succeed` (DT:266-280) is taken only at the client's current for_update_ts
    (DT:275); the value is recorded in `client_read` and the reply is `Done` / `Value(v)`.
  - `lock_key_failed_write_conflict` (DT:282-297) is taken only if `latest_commit_ts >
    for_update_ts` (DT:289). It sets the client's for_update_ts from `ts` (DT:290-291) and sends
    the lock of that key again at the new for_update_ts (DT:292-296), up to `a` attempts in all
    (IF §5.4, the prompt); after the a-th conflict the reply is `Aborted`.
  - `lock_key_aborted` gives `Aborted` (RespMessages end the transaction, DT:172-175).
- `Commit`: prewrite requests to every write key, all sent first (DT:304-307 / 315-318). The
  replies are taken in any order (`ClientPrewrited` DT:321-329), each prewrite resolving its own
  locks independently (D5).
  - `prewrite_aborted` gives `Aborted`.
  - When all keys are prewritten: commit_ts from `ts` and one `commit` request **to the primary
    only** (DT:331-341). `committed` gives `Committed`; `commit_aborted` gives `Aborted`.

**D5 Lock resolution (R1, IF §8).** The model lets only a client in stage "locking" resolve a
lock (`ClientResolveLock`, DT:241-244). Here a handler resolves every lock that any of its
requests meets: read, lock_key, or prewrite. It sends the model's `check_txn_status` to the
lock's primary with `caller_start_ts = NoneTs` and `resolving_pessimistic_lock = (lock type =
lock_key)` (DT:245-249). The model's `ServerReadKey` builds its `key_is_locked` without
`lock_type` (DT:472-476). Here every `key_is_locked` carries the lock's type; a read only meets
non-`lock_key` locks, so for a read the flag is FALSE.

Each outstanding model request of a handler is a *job* (`Job`, `run_jobs`): one for a read or a
lock, one per write key for the prewrites. A job holds the request (state `RSt`: in flight,
blocked on a lock, done) and the check of the lock it last met (state `CSt`: none, in flight,
waiting, settled). Jobs progress independently, and so do a job's request and its check (round 2,
finding 2): the handler takes whichever reply, check result or notification comes first. There
is no barrier (round 0 waited for every resolution message before resending; removed in round 1,
finding 3).

The model has no reply to `check_txn_status`. Here the primary replies `CheckDone` with the
outcome (push, unlock, rollback, commit found, unchanged), the keys it sent resolution messages
to, its version after the step, and what the handler does with the check next (D7). When to send the request or the check again is D7.
The resolution messages are acknowledged to the handler (`ResolveDone`) only so that the run can
end with no message in flight (D11); nothing waits for them otherwise.

**D7 Re-evaluation without bounds (round 1, finding 2).** Round 0 bounded the resolutions per
lock (`LOCK_ROUNDS = 2`). The bound is removed. In its place, a request (or a check) is sent
again exactly when the model's re-evaluation of it can differ from its last one.

*Lemma (stutter).* The model's evaluation of a request at key k (`ServerReadKey`,
`ServerLockKey`, `ServerPrewrite*`, DT:447-605) reads only `key_lock[k]`, `key_write[k]`,
`key_data[k]` and the request, plus the client's guards (DT:231-233, 270-277, 286-289,
325-327), which do not change while the request is pending. Its choices are over singletons
(`\E l \in key_lock[k]`, `\E committed_record`), and its disjuncts are exclusive. So at an
unchanged key it gives the same response again, and `key_is_locked` of the same lock changes
nothing (DT:240-250: the check it sends is already in `req_msgs`, and `SendReq` is a set union,
DT:101). The evaluation of a check at pk (DT:701-715) reads only pk's state and the check. After
a rollback (DT:654-657, 682-685) the next evaluation finds no lock of start_ts, data already
removed and a record with `ts = start_ts` (DT:383-399: no change) and sends the same
`resolve_rollbacked` (already in `req_msgs`); after a commit found (DT:673-678) it sends the same
`resolve_committed`; after an unlock (DT:648-652) it is `missing_lock` with the resolving flag,
UNCHANGED (DT:679-680); an unchanged outcome is unchanged again. Only after the push
(DT:659-666) is the next evaluation different (`min_commit_ts = 1 > caller_start_ts = 0`
disables the push; the lock is then rolled back or unlocked).

*Rule (round 2).* Every key carries a version, incremented at each step that changes its lock,
writes or data. A watch at a key fires (`Fired`) at the first later step that changes the key.
Watches are registered by the key itself, in the step whose reply tells the handler about them,
so a watch's version is exactly the one of that evaluation and its cancellation (`Unwatch`) can
only come after it:
- Request R of a job is sent as `Tracked { wid, R }`. If the key answers `key_is_locked`, it
  registers watch `wid` in that step. When it fires, the handler sends R again, whatever the
  state of the job's check.
- A check C carries a watch id and the version vk at which R met the lock. In C's step the
  primary decides what comes next and replies it in `CheckDone`:
  - push (DT:659-666): `nondet()` between "send C again now" and "register the watch"; the
    first covers the re-evaluation at the post-push state (its delivery can be delayed to any
    later state), the second leaves C until the primary's next change or for ever, as the model
    may;
  - a resolution message sent to R's key, or C ran at R's own key and that key changed since vk:
    `Settled` (re-evaluating C is a stutter by the Lemma, except the case below; R's key changes
    for certain: when the message arrives it removes the lock R met, or that lock is already
    gone; R's own watch then fires);
  - otherwise (unlock, unchanged, or C at R's key with no change): register the watch; when it
    fires, the handler sends C again, whatever the state of R.
- If R, sent again, meets the same lock, the job keeps its check as it is (in flight, waiting or
  settled). If it meets another lock, the job stops following the old check (cancels its watch;
  a CheckDone still due is taken, and a watch it registered is cancelled) and sends a check for
  the new lock. If R is answered, the job ends the same way.

*Completeness (round 2, finding 2).* In the model, R and C are both in `req_msgs` (DT:101) and
`ServerReadKey`/`ServerLockKey`/`ServerPrewrite*` (DT:447-605) and `ServerCheckTxnStatus`
(DT:701-715) may evaluate each of them in any state, any number of times, independently of each
other and of the resolution messages (DT:717-741). By the Lemma, an evaluation of R in a state
where R's key equals the one at R's last evaluation repeats that evaluation and changes nothing,
and likewise for C at its primary (with the push as the one exception, handled by the
`nondet()`). So the evaluations that matter are those of R in the states after the first change
of R's key since its last evaluation, and those of C after the first change of its primary.
The program realizes exactly these: the watch of R fires at that first change, whatever C is
doing; the watch of C fires at its primary's first change, whatever R is doing; the message sent
again can then be delivered at any later state of the key. Resolution messages are delivered
independently and only counted (D11). Under `fifo` a handler's messages to one key keep their
order, as that delivery model requires; under `bag` they do not.
The bookkeeping adds no order between visible events that the evaluation itself lacks: the
`Fired` that triggers a resend is sent by the key at its change, and the resent request is
evaluated at the same key after that change, so the change is in the evaluation's causal past
through the key's own program order anyway; the same holds for C at its primary. The registration
of a watch happens in the evaluating step itself and sends nothing.
What remains different from the model, each argued: (i) a check is no longer followed once the
job's request has moved on (another lock, or answered): IF §7 / G4, no request is re-evaluated
for ever; this is the departure finding 8 approved for resolution messages, and the check in
flight is still evaluated once; (ii) a settled check is not re-evaluated: the stutter Lemma and
the exception below; (iii) R2 (D8).

*Exception argued, not watched.* After a rollback or commit-found decision the program does not
watch pk for further changes (only k). A later evaluation of the same check at a changed pk
makes the same decision with the same messages, except one case: the rollback record of start_ts
was collapsed meanwhile (DT:392, an unprotected record removed by a later rollback with larger
ts) and is written again. That record is read only by `ServerLockKey`'s `lock_key_aborted`
branch (DT:494) for a `lock_key` of that transaction at pk; the prewrite branches abort anyway
(DT:551, 594: the collapsing record has ts > start_ts) and `check_txn_status` decides rollback
again. Such a `lock_key` cannot come: a transaction's first written key is its primary
(IF §3, D16) and its handler locks keys one at a time in script order, so all its `lock_key`
requests at pk (including write-conflict retries) are answered before it holds any lock that a
check could be about; a pessimistic `Put` of a key already locked sends no new `lock_key` (D10).

*Quiescence.* A wait may never end: e.g. the `lock_key` lock of T on a secondary after a
resolver unlocked T's primary (DT:648-652); every later check is UNCHANGED (DT:679-680) until T
itself prewrites. When every live job of a handler is blocked with its check waiting, no check
result and no acknowledgement is due, it sends `sup` `Idle(epoch)`; when woken, `Busy(epoch)`.
When every handler is idle or finished in `sup`'s view, `sup` sends each idle one
`Quiesce(epoch)`, and the handler, if still idle in that epoch, answers `Aborted`. `sup`'s view
can be stale (round 2, note 5): a `Busy`, or a `Fired` that will wake an idle handler, can be in
transit, so a `Quiesce` does not prove that no key changes any more. The abort is justified
instead by an earlier R2 point: the abort right after the job's last check was sent (D8 (b)) has
the same effects on the keys (that check is still evaluated; afterwards the job sent only
requests answered `key_is_locked`, which change nothing, and no check) and a smaller causal past,
so its summary has the same words and pointwise smaller order counts (TraceForge `compact.rs`: a
summary covers another of the same signature with counts ≥ its own). When the view is not stale,
the request is indeed unanswered in every later state. A stale `Quiesce` (the handler was woken
meanwhile) is ignored; `sup` tells each handler after its report which `Quiesce` it still has to
take (`Release`).

*Termination.* Every run is finite: a lock is created only by a request's success, which ends
that job (at most one per job; jobs are bounded by the scripts; write-conflict resends by `a`);
each lock is pushed at most once and removed at most once; commit records only with a lock's
removal; rollback records are rewritten only after a collapse by a larger start_ts, which by
induction on start_ts happens finitely often. Every resend needs a change and every change is
one of these, except the at-once check after a push, at most once per lock. Every completed run
of section 7 ends with `stop_reason: exhausted`, `blocked=0`; the ones that did not complete
(round 2: fifo M2, `bag` X2) show bounded graph sizes.

- Write-conflict retry: `a` attempts in all, as the prompt and IF §5.4 say (the instance
  parameter; not a cut of this program).

**D8 R2 aborts (IF §8; round 1, finding 1).** `Aborted` is answered only to a request that the
model leaves unanswered in the current state. The points:
- (a) `nondet()` on every `key_is_locked` reply: in that state the key is locked by another
  transaction and the model's server answers only `key_is_locked` (DT:470-477, 530-537, 569-577).
- (b) `nondet()` right after every send of a check: the abort is causally after (a)'s state; the
  check stays in flight and is still evaluated (the model keeps it in `req_msgs`).
- (c) `Quiesce` (D7 "Quiescence"; covered by (b)).
- (d) a `lock_key` sent at a for_update_ts older than the client's current one (D15): its success
  at that timestamp is never taken (DT:275); its evaluation can only give a success that is
  dropped, or a write conflict, which is not an answer to the client but leads to a new request
  at a new for_update_ts (DT:289-296; round 2, note 6). So in the state after the send nothing
  answers it: `nondet()` right after the send; a success reply at the old timestamp, if it
  comes, gives `Aborted`.
- (e) deterministically: `Silent` (D4b), `commit_ts_expired` (D4c), a `lock_key_succeed` not at
  the current for_update_ts (DT:275), a write conflict with `latest_commit_ts <= for_update_ts`
  (DT:289); the a-th write conflict (IF §5.4).

The model's own answers stay: `lock_key_aborted`, `prewrite_aborted`, `commit_aborted`
(DT:499, 553, 604, 636, 639) give `Aborted`; `committed` gives `Committed`.

Removed in round 1: R2 right after every request send, before every receive of the prewrite
fan-out, and after the commit request. `ServerCommit` answers the commit request in every state
(DT:614-640), so it never gets R2; round 0 could answer `Aborted` and then commit the
transaction, whose writes another client could then read (review finding 1).

**D9 Read-only transactions.** A3's readers write nothing and have no primary (IF §3). DT:23
needs `CLIENT_PRIMARY[c] \in CLIENT_KEY[c]`, so a reader can be instantiated with a read key as
primary; but then `ClientCommit` (DT:331-341) sends a commit to a key the reader never locked,
and `ServerCommit` answers `commit_aborted` (DT:638-640): such a model instance has no successful
empty-write commit. The interface's rule governs (IF §3, §5.2; approved in round 1, finding 7):
`Commit` with an empty write set is answered `Committed` at once, with no request.

**D10 Interface operations the model does not have.**
- Read-your-writes: a `Get(k)` after the client's own `Put(k)` replies with the buffered value
  (IF §5.2).
- `GetForUpdate(k)`: `lock_key` of k with its value (the model's pessimistic read, DT:17-19).
- A pessimistic `Put(k)` on a key it has already locked sends no new `lock_key`.

None of these occurs in this round's scenarios.

**D15 for_update_ts of later locks (round 1, finding 4).** In the model all initial `lock_key`
requests are sent at begin with `for_update_ts = start_ts` (DT:255, 259-263). A write conflict
raises the client's for_update_ts and resends only the conflicting key (DT:290-296). Here a
key's first `lock_key` is sent when its `Put` arrives, with `for_update_ts = start_ts`, as in the
model; only the resend after a conflict carries the new for_update_ts. A success is taken only
at the client's current for_update_ts (DT:275), so after a conflict on one key the first lock of
a later key is never accepted as a success at start_ts; a write conflict whose `latest_commit_ts`
exceeds the current for_update_ts still enables the retry of DT:289-296 at a new for_update_ts,
whose success can be taken (round 2, note 6). Otherwise the answer is `Aborted` (D8 (d)). Lock resolution resends a job's request unchanged
(the model's persistent request). This matters only for a >= 2; this round runs a = 1.

**D16 Primary, read and write keys.** The primary is the first key the script writes (IF §3). It
reproduces Test4's and Test3's `ClientPrimary` for S1 and M2. `CLIENT_READ_KEY` is the
script's `Get` keys and `CLIENT_WRITE_KEY` its `Put` keys.

**D17 No secondary commit.** The model's client commits only the primary (DT:337-340).
Secondaries are committed only when another transaction's `check_txn_status` finds the commit
record and sends `resolve_committed` (DT:673-678, 717-728). The program follows this: there is
no `xi` process, and a `Committed` transaction's secondaries stay locked until some request
resolves them (approved in round 1, finding 9, Q7).

## 5. Termination (D11)

Internal threads end when every client has been served, and every message is delivered exactly
once.
1. After its last reply, a handler takes every outstanding reply and `CheckDone` of abandoned
   jobs, cancels its watches (`Unwatch`; the key answers `UnwatchAck` with whether the watch had
   already fired, and then the handler also takes that `Fired`), and takes every `ResolveDone`
   it is owed. Since round 2 there is no `Watch` message: a key registers a watch in the very
   step that answers the request or check carrying its id, and the handler learns the id only
   from that answer, so an `Unwatch` always arrives after its registration, under `bag` too. So
   `UnwatchAck` proves that every message about that watch has arrived at the key (round 2,
   finding 1: in round 1 an `Unwatch` could overtake its `Watch` under `bag`, be acknowledged at
   once, and `Stop` could then overtake the `Watch`, leaving it undelivered). It then reports to `sup`, takes `Release` and the `Quiesce` messages still owed.
2. `sup` takes the reports in handler order, and in any order while some handler is idle (so an
   idle handler is never left waiting for a report `sup` does not take). A report does not
   prove that the handler's notifications have arrived: under `bag` it can overtake the
   handler's own `Idle`/`Busy` (round 3, finding 1). So each handler counts the notifications
   it sends (`notes_sent`; the i-th carries `seq = i` and the tag `TAG_NOTE | h << 16 | i`) and
   puts the count in its report; `sup` records the `seq` of every notification it takes, and
   after the last report takes each missing one by its tag, in `seq` order (one receive per
   tag, so no choice among equal messages under `bag`). Those are stale (their handler has
   finished) and have no effect, as stale epochs already had none. A handler sends no
   notification after its report, so at that point all handler-key work is drained (item 1)
   and every notification to `sup` has been taken (the count). `Release`, outstanding
   `Quiesce` and the final client reply may still await receipt; they cannot affect keys or
   visible events, and their recipients take them before ending (item 1; the client's last
   receive). `sup` then sends `Stop` to `ts` and to every key. Each reports its
   final state, and `sup` checks the global invariants.

Every `ResolveDone` (tagged by the check's id and the acknowledging key, and carrying that tag;
round 2, finding 3: round 1 removed an arbitrary entry of the due list on a `ResolveDone` taken
before the end) and every `Quiesce` (tagged by its epoch) has its own tag, and the handler takes
and removes each by its tag. With one shared
tag, the receives at the end could read equal acknowledgements of one key in every order under
`bag` (under fifo the order is fixed), which multiplied the executions without new behaviour:
X2 under `bag` ran past 4 million executions in 600 s; with the tags it ends at 1793082.

## 6. Invariants (D12; round 1, finding 5)

The `.cfg` files' list is TypeOK, UniqueCommitOrAbort, CommitConsistency, AbortConsistency,
WriteConsistency, UniqueLockOrWrite, UniqueWrite, Optimistic/PessimisticReadSnapshotIsolation,
MsgTsConsistency (DT:880-890: in every reachable state). Checked with `traceforge::assert`:
- Per key, after every step: WriteConsistency, UniqueLockOrWrite, UniqueWrite (D6a). They
  mention only one key's variables, so this is the every-state check. At most one lock (TypeOK,
  DT:197) holds by construction (`Option`). The rest of TypeOK is type structure.
- In `sup`, on the final global state, for every transaction that began, read-only ones
  included (round 0 skipped transactions without writes, so neither A3 reader was checked; fixed):
  UniqueCommitOrAbort, CommitConsistency, AbortConsistency, Optimistic/PessimisticRead
  SnapshotIsolation, MsgTsConsistency.

Every run ends in a state with all requests evaluated, and every reachable state of a run is
followed by its final state. Why a violation in any reachable state is one in the final state:
- **UniqueCommitOrAbort** (DT:789): `resp_msgs` only grows (DT:103-104); the program's `resps`
  are all of them.
- **AbortConsistency** (DT:812): `resp_msgs` only grows, `client_ts[c].start_ts` is fixed after
  begin, and commit records are never removed (`rollback` removes only rollback records,
  DT:392; `commit`, DT:368, only adds).
- **CommitConsistency** (DT:797): the primary's commit record persists (as above). For a write
  key k, "T's lock and T's commit record" contradicts UniqueLockOrWrite (checked per step).
  "Neither" persists: T's lock at k cannot come back, because T sent every request to k before
  its `committed` (the prewrites are all answered first, DT:333) and sends nothing after; and a
  commit record of T at k is written only from T's lock (DT:364-370, 724-727).
- **OptimisticReadSnapshotIsolation** (DT:855): with `latest(s)` the largest commit ts <=
  start_ts at k in state s, a read records a value_ts that was `latest` at its evaluation and
  whose record stays. `latest` only grows (commit records are only added). So the invariant fails
  in s iff `latest(s) > value_ts`, which then holds in every later state. `client_read[c][k]` is
  the last read of k (DT:225), as here.
- **PessimisticReadSnapshotIsolation** (DT:867): its antecedent (a `committed` response) persists
  and comes after the last change of `client_ts[c].for_update_ts` (all locks are taken before
  the prewrites); then the argument above, with for_update_ts.
- **MsgTsConsistency** (DT:848): a final check against the final `next_ts` alone could miss an
  early violation, so the argument is by construction. Every timestamp in a request or a
  `committed`/`commit_aborted` response is a handler's start_ts, for_update_ts or commit_ts, or
  a lock's or record's ts copied from one; a handler gets each from `ts` before it sends it, and
  `ts` hands out t only when its counter becomes t+1; the counter only grows. So every message's
  timestamps are below `next_ts` in every state after the message exists. The final check (all
  timestamps seen at keys and held by clients <= final next_ts) is kept as a test of that
  construction.

**Reading a run.** Under `count` a failed `traceforge::assert` does not count as `blocked`. It
blocks the execution, and TraceForge prints the failing thread's name (for example `sup`) on a
line of its own. A run is clean when it prints no line other than the progress lines,
`execs=...`, `--- measurements` and the `key: value` lines (`failure_lines=0` below).

Sanity checks (mutated copies of the round-2 program built outside the tree, under the build
directory; `count spec --model fifo`, a = 1; the failing thread's lines counted):
| mutation | size | result |
|---|---|---|
| `ServerReadKey` ignores every lock | L1 | 62 `sup` failure lines |
| same | A3 | 20 `sup` failure lines (the readers are checked) |
| `assert(false)` on a `Quiesce` abort | S1 / P2 | 41 / 688 failure lines (h1, h2) |
| `assert(false)` when a check's primary watch fires | S1 / P2 | 117 / 1370 |
| assert that a request is not resent while its check is in flight | S1 / X2 | 68 / 3464 |

So the quiescence path, the re-evaluation of a check at a changed primary, and the request's
progress independent of its check (round 2, finding 2) are all reached. (Round 1's table, on the
round-1 program, had the same first two rows and reached `FullWait`, the primary watch and
quiescence in P2 only.) The unmutated program prints no failure line (section 7).

## 7. Parameters, build and runs

**D14 Parameters.**
- Arguments: `<scenario> <async> <onepc> <a> [fifo|bag]`.
- `async` and `onepc` are accepted and ignored (the prompt). `async = onepc = 1` is refused
  with a panic (IF §1).
- `a >= 1` is required.
- The optional positional model must be `fifo` or `bag` and equal `--model` (default `fifo`,
  program.rs `main`); a `--model causal` is refused. Checked in `main` on the whole command line
  before the runner starts (round 1, finding 6): e.g. `count spec --model fifo -- X2 0 0 1 bag`
  exits 2 with "the positional delivery model `bag` differs from --model fifo".
- Specifications accept no variant: a sixth argument is refused.

Compile (in OUT_DIR):

    TMPDIR=/local/home/mkhoshechin/tmp CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/claude-build/tikv-tla cargo build --release -j 2

The binary is therefore `$CARGO_TARGET_DIR/release/bench_tikv_tla`, not
`target/release/bench_tikv_tla`. The runs were made in OUT_DIR as
`timeout 600 $CARGO_TARGET_DIR/release/bench_tikv_tla count spec --model <m> -- <args>`, one at a
time, on the local machine, 2026-11-09.

Before (round 0, `src/main.rs` md5 `ca6bc5da3e1d747cbcf94070dd341a36`, LOCK_ROUNDS = 2) and after
(round 1, md5 `78467f443f0d1d3ea1fbdc95ad134364`). Every run ended `stop_reason: exhausted`,
`blocked=0`, with no assertion-failure line:

| command (after `count spec`) | execs before | wall before | execs after | wall after |
|---|---|---|---|---|
| `--model fifo -- S1 0 0 1` | 2490 | 0.33 s | 576 | 0.07 s |
| `--model fifo -- X2 0 0 1` | 51328 | 7.2 s | 64870 | 10.8 s |
| `--model fifo -- X2r 0 0 1` | 159178 | 24.9 s | 205778 | 42.9 s |
| `--model fifo -- P2 0 0 1` | 46324 | 6.6 s | 70074 | 14.2 s |
| `--model fifo -- M2 0 0 1` | 849212 | 136.8 s | 1591132 | 370.6 s |
| `--model fifo -- X2 1 0 1` | 51328 | 6.0 s | 64870 | 11.1 s |
| `--model fifo -- X2 0 1 1` | 51328 | 6.3 s | 64870 | 11.2 s |
| `--model fifo -- L1 0 0 1` | 1200 | 0.12 s | 262 | 0.04 s |
| `--model fifo -- A3 0 0 1` | 218544 | 34.5 s | 765132 | 167.2 s |
| `--model bag -- X2 0 0 1 bag` | 63380 | 8.2 s | 1793082 | 311.1 s |

Reading the change: the one-key sizes (S1, L1) shrink, because R2 now applies only where the
model leaves a request unanswered (D8). The two-key sizes grow, because the bound and the
barrier are gone (D5, D7): checks, resends and resolutions of different jobs and handlers now
interleave as the model allows, and every resolution gets a watch. A3 grows most (three
handlers). `bag` grows most of all: messages between the same two processes (a handler's
prewrite, check and watch to one key; one key's resolutions to another) are no longer kept in
order by a barrier. X2 gives the same count for `1 0 1`, `0 1 1` and `0 0 1` because async and
onepc do not change the specification.

Also run (round 1, a = 2, which exercises D15's per-key for_update_ts): `--model fifo -- S1 0 0 2`
576 executions, `P2 0 0 2` 76866; both exhausted, blocked 0, no failure line.

Last output lines of each round-1 run (the `execs=` line, the measurement lines `wall_ms` and
`stop_reason`, the `/usr/bin/time` line, and the count of failure lines):

```
== count spec --model fifo -- S1 0 0 1
rc=0
execs=576 blocked=0
wall_ms: 75
stop_reason: exhausted
time 0.07s rss 5696KB
failure_lines=0
== count spec --model fifo -- X2 0 0 1
rc=0
execs=64870 blocked=0
wall_ms: 10789
stop_reason: exhausted
time 10.79s rss 6268KB
failure_lines=0
== count spec --model fifo -- X2r 0 0 1
rc=0
execs=205778 blocked=0
wall_ms: 42893
stop_reason: exhausted
time 42.89s rss 6588KB
failure_lines=0
== count spec --model fifo -- P2 0 0 1
rc=0
execs=70074 blocked=0
wall_ms: 14178
stop_reason: exhausted
time 14.18s rss 6620KB
failure_lines=0
== count spec --model fifo -- M2 0 0 1
rc=0
execs=1591132 blocked=0
wall_ms: 370604
stop_reason: exhausted
time 370.60s rss 7736KB
failure_lines=0
== count spec --model fifo -- X2 1 0 1
rc=0
execs=64870 blocked=0
wall_ms: 11088
stop_reason: exhausted
time 11.09s rss 6160KB
failure_lines=0
== count spec --model fifo -- X2 0 1 1
rc=0
execs=64870 blocked=0
wall_ms: 11198
stop_reason: exhausted
time 11.20s rss 5920KB
failure_lines=0
== count spec --model fifo -- L1 0 0 1
rc=0
execs=262 blocked=0
wall_ms: 38
stop_reason: exhausted
time 0.04s rss 5376KB
failure_lines=0
== count spec --model fifo -- A3 0 0 1
rc=0
execs=765132 blocked=0
wall_ms: 167217
stop_reason: exhausted
time 167.21s rss 6604KB
failure_lines=0
== count spec --model bag -- X2 0 0 1 bag
rc=0
execs=1793082 blocked=0
wall_ms: 311111
stop_reason: exhausted
time 311.11s rss 6724KB
failure_lines=0
```

Round 0's last output lines are those of the table's "before" columns (round 0's NOTES.md);
they are not repeated here.

### Round 2 runs

`src/main.rs` md5 `11addedb3b778752734facf3bc8fe187`, 2026-11-09, local machine (16 cores), same
compile command. The prescribed commands (`timeout 600`, one thread) ran as two queues at once
(the nine fifo sizes one after another; `bag` X2 beside them), so wall times are approximate:

| command (after `count spec`) | round 1 execs | round 2 execs | round 2 wall |
|---|---|---|---|
| `--model fifo -- S1 0 0 1` | 576 | 2331 | 0.3 s |
| `--model fifo -- X2 0 0 1` | 64870 | 267909 | 41.1 s |
| `--model fifo -- X2r 0 0 1` | 205778 | 660573 | 124.8 s |
| `--model fifo -- P2 0 0 1` | 70074 | 328526 | 58.6 s |
| `--model fifo -- M2 0 0 1` | 1591132 | not exhausted in 600 s (> 2 M) | timeout |
| `--model fifo -- X2 1 0 1` | 64870 | 267909 | 71.2 s |
| `--model fifo -- X2 0 1 1` | 64870 | 267909 | 79.5 s |
| `--model fifo -- L1 0 0 1` | 262 | 563 | 0.1 s |
| `--model fifo -- A3 0 0 1` | 765132 | not exhausted in 600 s (> 2 M) | timeout |
| `--model bag -- X2 0 0 1 bag` | 1793082 | not exhausted in 600 s (> 4 M) | timeout |

Every completed run: `stop_reason: exhausted`, `blocked=0`, no assertion line. The timed-out
runs printed no assertion line either, and no blocked execution.

The three timeouts were run again with the partitioned engine (`--threads N`), longer:
- `count spec --threads 3 --model fifo -- A3 0 0 1`, `timeout 7200`: **exhausted, 5612572
  executions, blocked 0**, 942 s.
- `count spec --threads 3 --model fifo -- M2 0 0 1`, `timeout 7200`: not exhausted; at least
  36976624 executions explored (sum of the workers' counts), no assertion line, no blocked one.
- `count spec --threads 8 --model bag -- X2 0 0 1 bag`, `timeout 5400`: not exhausted; at least
  97881966 executions explored, no assertion line, no blocked one; the largest graph has 201
  events (fifo X2: 189), so the runs are finite and the size is breadth, not a loop.

The partitioned engine prints per-worker lines (`rtN: … execs`, `split …`, "Stopping exploration
because max_iterations was reached" for each batch). They are not assertion lines; the counter
of section 6 counts them, so those runs show large `failure_lines`. A check that no line names
a thread was done by hand.

Why round 2 is larger: a job's request and its check now progress independently (finding 2),
so the resend of a request, the re-evaluation of its check, the resolution messages and the
watch notifications interleave freely; under `bag` the messages between one handler and one
key are moreover unordered. This is the independence that DT:447-605 and 701-741 give and that
the review asked for. M2 under fifo and X2 under `bag` are not tractable locally at this cost
(Q10).

Last output lines of the round-2 prescribed runs:

```
== count spec --model fifo -- S1 0 0 1
rc=0
execs=2331 blocked=0
wall_ms: 276
stop_reason: exhausted
time 0.27s rss 5940KB
failure_lines=0
== count spec --model fifo -- X2 0 0 1
rc=0
execs=267909 blocked=0
wall_ms: 41134
stop_reason: exhausted
time 41.13s rss 6008KB
failure_lines=0
== count spec --model fifo -- X2r 0 0 1
rc=0
execs=660573 blocked=0
wall_ms: 124805
stop_reason: exhausted
time 124.80s rss 6928KB
failure_lines=0
== count spec --model fifo -- P2 0 0 1
rc=0
execs=328526 blocked=0
wall_ms: 58594
stop_reason: exhausted
time 58.59s rss 6588KB
failure_lines=0
== count spec --model fifo -- M2 0 0 1
rc=124
time 600.00s rss 6916KB
failure_lines=0
== count spec --model fifo -- X2 1 0 1
rc=0
execs=267909 blocked=0
wall_ms: 71158
stop_reason: exhausted
time 71.16s rss 6304KB
failure_lines=0
== count spec --model fifo -- X2 0 1 1
rc=0
execs=267909 blocked=0
wall_ms: 79531
stop_reason: exhausted
time 79.53s rss 6144KB
failure_lines=0
== count spec --model fifo -- L1 0 0 1
rc=0
execs=563 blocked=0
wall_ms: 105
stop_reason: exhausted
time 0.10s rss 5760KB
failure_lines=0
== count spec --model fifo -- A3 0 0 1
rc=124
time 600.00s rss 7108KB
failure_lines=0
== count spec --model bag -- X2 0 0 1 bag
rc=124
time 600.00s rss 6144KB
failure_lines=0
```

and of the longer runs:

```
== count spec --threads 3 --model fifo -- A3 0 0 1
rc=0
execs=5612572 blocked=0
wall_ms: 942097
stop_reason: exhausted
time 942.10s rss 29056KB
failure_lines=159290
== count spec --threads 3 --model fifo -- M2 0 0 1
rc=124
time 7200.00s rss 80884KB
failure_lines=1089108
== count spec --threads 8 --model bag -- X2 0 0 1 bag
rc=124
time 5400.00s rss 161880KB
failure_lines=2631467
```

## 8. Open questions

- **Q1** Read-only transactions (D9): settled in round 1 (finding 7): `Committed` at once.
- **Q2** Resolution messages that arrive before the lock exists are consumed with no effect
  (D6): settled in round 1 (finding 8): the authorized departure of IF §7 / G4.
- **Q3** for_update_ts of later locks: settled in round 1 (finding 4, D15): start_ts, as DT.
- **Q4** Global invariants on the final state: settled in round 1 (finding 5) by the
  persistence arguments of section 6.
- **Q5** Positional model against `--model`: checked in `main` since round 1 (finding 6, D14).
- **Q6** `LOCK_ROUNDS`: removed in round 1 (finding 2, D7).
- **Q7** Secondaries stay locked after `Committed` (D17): approved in round 1 (finding 9).
- **Q8** Round 0 read `traceforge/src/thread.rs` and `msg.rs`, outside the two API files the
  prompt names; round 1 read further TraceForge files (header). Recorded as a provenance
  exception (finding 9); no semantic decision rests on them, except that D7's coverage remark
  on `Quiesce` uses how summaries compare order counts.
- **Q10 (round 2)** Cost: with independent progress, fifo M2 and `bag` X2 do not end within
  hours locally (section 7, round 2 runs); A3 needs the partitioned engine (942 s, 3 threads).
  Whether these sizes stay in the round, run on more cores, or move to a later tier is the
  coordinator's decision. A reduction of the bookkeeping orders (e.g. merging a key's
  notifications to one handler in one step into one message) may help but is not proved and
  not applied.
- **Q9 (new)** The program's invisible bookkeeping (watches, `Idle`/`Busy`/`Quiesce`, `Release`)
  is not in the model. It never changes a key's state, and every message is still delivered
  exactly once. It does add executions (X2 has more executions than in round 0 although it has
  fewer R2 points): a reviewer may want to compare the visible summaries of round 0 and round 1.

## 9. Review round 1

Review: `plan/case-studies/tikv/review/REVIEW-astra-tla.md` (VERDICT: CHANGES NEEDED). Six
MUST-FIX findings, three notes. All six findings are applied; none is disputed.

| # | finding | verdict | change |
|---|---|---|---|
| 1 | R2 aborts outside its scope; Aborted then committed (K6) | agreed, applied | D8: R2 only at `key_is_locked` (a), after a check's send (b), on quiescence (c), a stale-ts lock (d); none after the commit request (DT:614-640 answers it in every state) or before a request's evaluation |
| 2 | `LOCK_ROUNDS = 2` unproved | agreed, applied | D7: bound removed; resend at the first change of the key (versions and watches), proved by the stutter lemma; quiescence ends waits that can never end. The review is also right that round 0's Q6 stated the M2 comparison (R2 off: 115 summaries at 2 rounds, 111 at 3) in the wrong direction; moot now |
| 3 | waiting for resolutions is an unproved barrier | agreed, applied | D5: no barrier; jobs progress independently; `ResolveDone` only for termination (D11). The review's point is right: a resend before a resolution lands is a model behaviour (DT:701-741 keep both enabled); D7 resends at the first change of the key from any cause |
| 4 | later keys' lock requests at the current for_update_ts | agreed, applied | D15: first `lock_key` of each key at start_ts (DT:255, 263), resend after conflict at the new one (DT:290-296), success only at the current one (DT:275) |
| 5 | invariants only on the final state; A3 readers never checked | agreed, applied | D12: read-only transactions checked (the `write_keys.is_empty()` skip was a bug; the A3 mutation now fails); a persistence argument per invariant; MsgTsConsistency by construction |
| 6 | positional model not checked against `--model`; `causal` accepted | agreed, applied | D14: `main` checks the whole command line before the runner |
| 7 | Q1 approved | noted | D9 wording corrected: DT:23 allows a read key as primary; that instance has no successful empty-write commit |
| 8 | Q2 approved under G4 | noted | unchanged; no persistent resolution messages |
| 9 | Q7 approved; Q8 provenance | noted | unchanged; Q8 kept as a provenance exception |

Also changed in round 1 (not a finding): `ctl` renamed `sup` (IF §2 keeps `c*` names out of
the invisible processes).

Counts before and after: section 7.

## 10. Review round 2

Review: `plan/case-studies/tikv/review/REVIEW-astra-tla-r2.md` (VERDICT: CHANGES NEEDED). Two
MUST-FIX, one SHOULD-FIX, six notes. All three findings are applied; none is disputed.

| # | finding | verdict | change |
|---|---|---|---|
| 1 | `Unwatch` can overtake its `Watch` under `bag`; acknowledged at once, `Stop` can then overtake the `Watch` (not exactly once, IF §7) | agreed, applied | D11: no `Watch` message any more; the key registers a watch in the step that answers the request (`Tracked { wid, .. }`) or the check carrying its id, and the handler learns the id only from that answer, so every `Unwatch` arrives after its registration; `UnwatchAck` then proves every message about the watch has arrived |
| 2 | the watch mechanism still restricted a job: no resend while its check is in flight, a check forced after a push; completeness unproved | agreed, applied | D5, D7: a job's request and check progress independently (request resent at the first change of its key whatever the check does; check resent at the first change of its primary whatever the request does; after a push a `nondet()` between "now" and "after the next change"); "Completeness (round 2)" in D7 argues that every non-stutter evaluation of the model (DT:447-605, 701-741) is realized and that the bookkeeping adds no order between visible events |
| 3 | `ResolveDone` lost its identity; the handler removed an arbitrary due tag | agreed, applied | `ResolveDone { tag }`; the handler removes exactly that tag |
| 4 | Q6's former justification stays rejected | noted | nothing left of the cutoff |
| 5 | quiescence overstated: a `Busy` or `Fired` can be in transit | agreed | D7 "Quiescence" reworded: a `Quiesce` abort is justified by the earlier R2 point (b), not by "nothing can change" |
| 6 | a stale-ts lock request can still get a write conflict that enables DT:289-296 | agreed | D8 (d) and D15 narrowed to "success at that timestamp is never taken" |
| 7 | Q4 accepted, subject to draining | noted | draining fixed by findings 1, 3 (and the two defects below) |
| 8 | Q1, Q2, Q5, Q7 resolved; Q8 provenance | noted | unchanged |
| 9 | Q9: bookkeeping must preserve delivery and observable behaviour | agreed | delivery: findings 1, 3; behaviour: D7 "Completeness", last paragraph (a resend is evaluated at the key after the change that fired its watch, so the bookkeeping adds no causal edge the evaluation lacks) |

Two defects of the round-2 rewrite, found by the runs and fixed before the recorded runs:
- A check at the request's own key was marked settled by comparing with the version sent with
  the check; when the request had meanwhile been resent and blocked again, its new watch could
  never fire (X2 fifo: `blocked=292`). Now "settled" there means that this check's step changed
  the key, which fires every watch the key holds.
- A `key_is_locked` taken while draining an abandoned request did not cancel the watch the key
  had registered for it, so the key stopped with a watcher (its `assert(s.watchers.is_empty())`
  failed). Now it is cancelled like the others.

## 11. Review round 3 (2026-11-09)

Review: `plan/case-studies/tikv/review/REVIEW-astra-tla-r3.md` (VERDICT: CHANGES NEEDED). One
MUST-FIX, seven notes. Finding 1 and the stale comments of finding 4 are applied; none is
disputed. No behaviour is cut and no cutoff is added (finding 8). Line numbers are those of this
revision.

| # | finding | verdict | change |
|---|---|---|---|
| 1 | a handler's report can overtake its own `Idle`/`Busy` under `bag`; `sup` ends with notifications in flight (not exactly once, IF §7) | agreed, applied | `main.rs`: `SupMsg::Idle`/`Busy` carry `seq` and `Handler` carries `notes_sent` (159-167); the shared `TAG_IDLE` is replaced by one tag per notification, `TAG_NOTE \| h << 16 \| seq` (`TAG_NOTE`, `TAG_CLASS`, `note_tag`, 186-192); the handler counts its notifications (`notes_sent`, 636, 710-711, 718-719, 1012) and reports the count (996); `sup` records the `seq` of each notification it takes (`notes_got`, 1040, 1056, 1064), accepts the whole `TAG_NOTE` range during the run (1048: the same set of messages as the old single tag, so the run's choices are unchanged), and after the last report takes every missing notification by its tag in `seq` order (1094-1107) before `Stop`. `NOTES.md` D11 item 2 (347-358) gives the accounting argument in place of "no message is in flight". |
| 2 | round-2 findings 1-3 implemented | noted | unchanged |
| 3 | Q6: no cutoff remains; acceptance scoped to the prescribed scripts and the G4 departure | noted | unchanged |
| 4 | R1/R2 accepted; two stale comments | agreed, applied | `main.rs` 1078-1081: a `Quiesce` abort rests on D7's summary-coverage argument (earlier R2 point), not on "nothing can change"; `main.rs` 842-844: an old-timestamp *success* cannot be accepted, a newer write conflict can still enable DT:289-296; `main.rs` 1108-1109: the end-of-run comment now cites the accounting |
| 5 | Q1, Q2, Q3 unchanged | noted | unchanged |
| 6 | Q4 accepted, subject to finding 1 | noted | delivery: finding 1 |
| 7 | interface and source checks pass; Q7, Q8, Q9 | noted | delivery (Q9): finding 1 |
| 8 | Q10: M2/fifo and X2/bag stay in the matrix; no cutoff | noted | unchanged; no runs in this revision |

The drain is necessary under `bag` and harmless under fifo: the main-loop receive always accepts
notifications, so under fifo a handler's report cannot overtake its own earlier notifications
(round 4, note 4). It receives one exact tag at a time, so it adds no choice among messages. The
build (`cargo build --release`) succeeds with the one existing warning (`Op::GetForUpdate` never
constructed); the program was not run in this revision, so the counts of section 7 predate it.

2026-11-09, round 4 (`REVIEW-astra-tla-r4.md`, ACCEPTED WITH DOCUMENTATION NOTES; comments and
NOTES only, no code change): the fifo sentence above corrected (the drain is necessary under
`bag`, harmless under fifo); D11 item 2 and `main.rs` 1113-1117 no longer claim that no message
to or from a handler is in flight (`Release`, outstanding `Quiesce` and the final client reply may
remain; they cannot affect keys or visible events); `main.rs` 192-195 gives the scenario-specific
bound behind `seq < 2^16` (note 3).
