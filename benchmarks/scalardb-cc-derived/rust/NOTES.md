# ScalarDB Consensus Commit: derived specification (bench_scalardb_derived)

Sources read: `cc-src/docs/consensus-commit.mdx` ("CC"), `cc-src/docs/design.mdx` ("D"), `INTERFACE-agents.md`
("IF"), TraceForge `lib.rs`, `program.rs`, the rust template. Nothing else.

## Shape of the program

- Visible: `c1`, `c2` (`c3` for mixedro), the IF client code verbatim, IF message types verbatim.
- One specification object, thread `spec`, holds all state and serves every request atomically.
- Ports `h1`, `h2` (`h3`), invisible. Decision: TraceForge's `recv_msg_block` does not return the sender, so the
  object cannot tell which client a `Begin`/`Read`/`Commit` came from. Each client's `h` is its port. The port relays
  the request to `spec` as `Op(i, req)` and relays the reply back. During a commit, the port sends `Step(i)` each
  time the object answers `More`. Each commit phase is then a run of separate atomic object steps, and other clients'
  requests can interleave between them.
- `init`, invisible: sends the object the ports' ids, and each port its client's id (tag 99), before any other
  message. The main thread only spawns and joins. Every thread ends: a port ends after relaying a final reply
  (`Committed`/`Aborted`, or a `Read` answered `Aborted`). The object ends once all n transactions are finished.
- Both `spec` and `imp` return this program. Arguments: `<pattern> <k> <iso> [<fifo|bag>]`. The optional 4th argument
  is ignored, because the model comes from `--model`.

## Abstract state (visible consequences only)

- `cur[k]`: the committed value of key k. Values name their writer (IF), so `cur[k]` is also the tx id of the
  committed record. Initially 0 (= t0).
- `held[k]`: the transaction whose write on k is prepared and not yet committed or rolled back. It is the abstract
  form of "the record is PREPARED by T" (CC: records carry TxID and TxState; the before image is `cur[k]`).
- `dec[T]`: Undecided / Aborted / Committed, the abstract form of "the Coordinator table determines and manages
  transaction states as a single source of truth" (CC).
- `jobs`: pending lazy-recovery roll-back jobs `(k, aborted writer, owner)`.

Records, versions, before/after images and the coordinator table are not modelled as data. The only record data
the spec keeps is the current TxID, `held[k]` or else `cur[k]`, because conditional writes and validation compare
exactly that.

## Each part and the sentences it follows

**Read(k)** (read phase: "ScalarDB copies every record that a transaction accesses ... to its local workspace", CC):
- `held[k]` empty: return `cur[k]`. CC: "it works with a single version and causes read-skew anomalies because it
  does not create global snapshots". So a read returns the latest committed value at the read, not a snapshot
  value. Read skew under SNAPSHOT follows from this.
- `held[k] = w`, w Committed: this never happens in the spec. At w's commit point all of w's held keys move into
  `cur`. CC: "Once the COMMITTED state is correctly written to the Coordinator table, the transaction is regarded
  as committed"; "If a record is in the PREPARED state and the transaction ... is committed, the record will be
  rolled forward". A reader then sees w's value, and conditional writes and validation see TxID w either way. So the
  commit-records phase has no visible window of its own.
- `held[k] = w`, w Aborted: return the before image `cur[k]` and add a roll-back job. CC: "if a record is in the
  PREPARED state, but the transaction that updated the record has expired or been aborted, the record will be
  rolled back".
- `held[k] = w`, w Undecided: a free choice (`nondet()`) of whether w has expired. CC: "A transaction expires after
  a certain amount of time (currently 15 seconds)"; IF: expiry is a free choice at each call.
  - not expired: the reply is `Aborted` and the client ends. CC (index-read section, stated for the general
    recovery): "ScalarDB throws an exception if the record hasn't expired, otherwise it recovers the transaction".
  - expired: w becomes Aborted, the read returns the before image, and a roll-back job is added. CC: "When ScalarDB
    observes a record that has been prepared by an expired transaction, ScalarDB writes the ABORTED state for the
    transaction ... If ScalarDB successfully writes the ABORTED state ..., the transaction is aborted". The other
    outcome ("Otherwise, the transaction will be committed by the original process") is the same as a read placed
    after w's commit point, which the interleavings already contain.

**Write(k, v)**: added to the write set only. CC: "stores its writes in the local workspace". Reply `Written(k)`.

**Commit** (CC "Atomic commitment protocol", "Prepare phase", "Commit phase"):
1. The handle waits for its own recovery jobs: those on written keys under SNAPSHOT, those on all read keys under
   SERIALIZABLE. Decision: this comes from IF. CC is silent on when a reader waits for the recovery it started.
2. Prepare-records phase: one object step per written key, in a free order. CC: "Consensus Commit executes each
   phase ... in parallel"; IF: parallel preparation on. A prepare succeeds iff `held[k]` is empty and `cur[k]`
   equals the value read. CC: "A transaction updates a record if the record has not been updated by another
   transaction since the transaction read it by checking if the TxID of the record has not been changed"; "ScalarDB
   runs the write validation ... by updating the statuses of the records to PREPARED". This is first-preparer-wins.
   It gives the lost-update prevention under both levels. All keys are attempted even after a failure, because the
   parallel prepare sends every key.
3. After all prepares, as its own step: "If any preparation fails, it aborts the transaction by writing an ABORTED
   state record" (CC). The abort step also rolls back the transaction's own held keys and replies `Aborted`.
4. SERIALIZABLE only, validate-records phase: one step per key read and not written, in a free order. The step aborts
   if the key's current TxID (`held[k]`, else `cur[k]`) differs from the value read. CC: "ScalarDB re-reads all the
   records in the read set to see if other transactions have written the records ... If the read set has not been
   changed, the transaction can go to the commit-state phase ...; otherwise, it aborts". A record prepared by another
   transaction that is still undecided counts as changed (its TxID changed). The first change ends validation
   (IF: "stopping at the first change"). This prevents write skew and the read-only anomaly ("Serializable: Possible
   anomalies: None", CC). The first validation step is merged with the step that collects the prepare results.
5. Commit-state phase, its own step: if the transaction is still Undecided, it becomes Committed, all its held keys
   move into `cur` at once, and the reply is `Committed`. CC: "creating a state record with a TxID if there is no
   record for the TxID" (put-if-not-exists). If a reader has already aborted it (expiry), the write fails: roll back
   own keys and reply `Aborted`. Atomicity follows: all of a transaction's writes become visible in that one step
   (CC: the atomic commitment protocol "to guarantee atomicity"), or none do.
6. Read-only transaction (no writes): SNAPSHOT replies `Committed` at once ("if a transaction contains only read
   operations, ScalarDB skips the commit-state phase", CC). SERIALIZABLE first runs the validate-records phase over
   its read set. The doc names only the commit-state phase as skipped, and IF also skips the prepare phase. If
   validation fails, the reply is `Aborted`; nothing is rolled back.

**Lazy recovery (roll-back jobs)**: a job `(k, w)` clears `held[k]` if it still holds the aborted w. A job is an
asynchronous background task, so it may have run at any time. The spec decides this lazily: whenever a prepare or a
validation meets `held[k] = w` with w Aborted and a pending job for `(k, w)`, a `nondet()` chooses whether the job
has already run. This is equivalent to "at any time" because only these observations depend on it. A read returns
`cur[k]` either way. An aborted w's own rollback clears all its held keys before it replies `Aborted`. Without a job,
an aborted w's prepared record stays until w's own rollback, and other prepares on k fail meanwhile (the TxID
differs).

## Decisions where the documents were silent

- `Unknown` is never replied. With no failures, the documents give no case where a client is left without a decision:
  "Once the COMMITTED state is correctly written ... regarded as committed". The UNKNOWN state in CC belongs to a
  third party's view of a slow transaction, not to the client's reply.
- A read that fails (not expired) ends that transaction with `Aborted`. It has written nothing, so nothing is rolled
  back. Jobs it created stay pending (background tasks).
- The 5-round limit on expiry-abort retries (IF) is not modelled. A failed ABORTED write means the writer committed,
  so the re-read is the same as a read after that commit point. With at most 3 transactions, each writing a key
  once, the limit is never reached.
- No window is modelled between a transaction's abort decision (prepare or validation failure) and its rollback.
  Anything a reader could see in that window (the before image, prepares on the key failing) it can also see in the
  window before the decision, by choosing "expired". So merging the decision and the rollback into one step loses no
  behaviour and adds none.
- The window between the last prepare (or last validation) and the commit-state write is kept as a separate step. In
  it a reader may fail or expire the transaction, and another transaction may prepare or change a validated key.
- No group commit, one-phase commit, async commit or rollback, metadata decoupling, coordinator write-set logging,
  scans or indexes (IF configuration). These options either change no client-visible behaviour for this interface,
  or are off.
- READ_COMMITTED is not an IF level and is not modelled.

## Anomalies the spec shows (by construction)

- SNAPSHOT: read skew, because each read returns the current committed value. In `mixed`, c2 can read k1 = 0 and
  then k2 = 1 after c1 commits between the two reads. Write skew: in `skew` both commit after reading 0, 0. The
  read-only anomaly appears with c3 in `mixedro`. Lost update: prevented by the prepare condition.
- SERIALIZABLE: write skew is prevented. Whichever of the two transactions validates second finds the other's key
  held or changed. The other anomalies are prevented by the same validation.
