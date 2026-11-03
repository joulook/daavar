# VR normal operation (Java, JMC message-passing API) — notes

Source: `java/bench/vrr/Vrr.java`, class `bench.vrr.Vrr` (implements `MpProgram`), argument string
`impl,<N>,<K>,<fifo|bag>`. f = (N-1)/2. `bag` -> `CommunicationModel.ASYN`, `fifo` -> `CommunicationModel.P2P`;
every send and receive uses that one model.

## Processes
- main (`run()`): creates `MpThread`s for r0..r{N-1}, then c0..c{K-1} (role = name), records each
  one's `Tid` before any is started, starts them in that order, then awaits all (`join1`). Sends/receives nothing.
- `ci` (`client`): sends `Request(from=i, op=i+1)` to r0, blocking `recv` of a `Reply`. Ends.
- `r0` (`primary`), `r1..` (`backup`).

## Messages (records, `@Tag` = name)
- `Request(int from, int op)` — REQUEST op, c, s (s = 1 implicit).
- `Reply(int result)` — REPLY v, s, x with v, s dropped (INTERFACE.md).
- `Prepare(int v, Request m, int n, int k)` — PREPARE v, m, n, k.
- `PrepareOk(int v, int n, int i)` — PREPAREOK v, n, i.
- `Commit(int v, int k)` — COMMIT v, k.

## Paper step -> code
1. Client sends REQUEST to primary: `client`, `Channel.send(model, r0, Request)`.
2. Primary compares request-number with client-table, drops if not bigger, re-sends the reply if
   it is the most recent and executed: `primary`, `Request` branch, `tableRequest/tableExecuted/tableResult`.
   (Never triggers here: one request per client, no resends.)
3. Primary advances op-number, appends to log, updates client-table, sends PREPARE(v, m, n, k) to
   the other replicas: `primary`, `Request` branch.
4. Backup accepts PREPARE n only when it has all earlier entries; increments op-number, appends,
   updates client-table, sends PREPAREOK(v, n, i): `backup`, selective `recv` whose guard accepts
   only `Prepare` with `n == opNumber + 1` (or any `Commit`), so a later PREPARE waits in the channel.
5. Primary waits for f PREPAREOKs from different backups, then (after executing all earlier ops)
   executes, increments commit-number, sends REPLY, updates client-table: `primary`, `PrepareOk`
   branch, `highestOk[]` + `preparedBy` + the execute loop.
6. Commit-number piggybacked on the next PREPARE (`Prepare.k`); COMMIT(v, k) when no new request
   comes in a timely way: `primary`, end of `PrepareOk` branch.
7. Backup learns of a commit (from `Prepare.k` or `Commit.k`), waits until the op is in its log and
   earlier ops executed, executes, increments commit-number, updates client-table, no reply:
   `backup`, `knownCommit` + execute loop. A backup ends after executing K ops; the primary after K replies.

View-number check ("Replicas only process normal protocol messages containing a view-number that
matches"): every replica message carries v = 0 and a mismatch is dropped (never happens).

## Decisions where the paper is silent
1. *"The primary waits for f PREPAREOK messages from different backups; at this point it considers
   the operation (and all earlier ones) to be committed."* — How PREPAREOKs for different ops combine
   is left open. A PREPAREOK for n means "this operation and all earlier ones have prepared locally"
   (step 4), so the primary keeps, per backup, the highest n it has acknowledged; op x is committed
   when at least f backups have highest >= x. Ops are executed in op-number order, as many as are committed.
2. *"if the primary does not receive a new client request in a timely way, it instead informs the
   backups of the latest commit by sending them a COMMIT v, k message ... (note that in this case
   commit-number = op-number)"* — "timely" is not defined. After executing, if commit-number =
   op-number and that commit-number was not yet sent in a COMMIT, the primary decides with
   `Nondet.chooseBoolean()` whether the timeout fires now (send COMMIT to all backups) or not (the
   next PREPARE carries it). If all K requests have been received (op-number = K), no new request can
   come and the timeout certainly fires: COMMIT is sent without a choice. This guarantees backups learn
   commit K and terminate. The primary sends COMMIT only from the PREPAREOK handler (that is the only
   place commit-number changes).
3. *"Backups process PREPARE messages in order: a backup won't accept a prepare with op-number n
   until it has entries for all earlier requests in its log."* — "won't accept" implemented as a
   selective receive (guard `n == opNumber + 1`), so out-of-order PREPAREs stay pending. No state
   transfer: with no failures and no loss every PREPARE is eventually delivered.
4. *"When a backup learns of a commit, it waits until it has the request in its log"* — a commit
   number larger than the log (COMMIT or a PREPARE's k overtaking an earlier PREPARE under bag) is
   remembered in `knownCommit` and applied as entries arrive.
5. *"The description ignores a number of minor details, such as re-sending protocol messages"* /
   *"If a client doesn't receive a timely response to a request, it re-sends the request to all
   replicas."* — no re-sends by anyone: INTERFACE.md guarantees every message is delivered and no
   replica fails, so the client sends once and waits.
6. Message processing order at a replica: the order in which messages arrive (blocking `recv` with
   a guard over the message kinds the role handles); the strategy chooses among pending messages.
7. Backups keep the result in their client-table (`tableResult`) but never reply (step 7).
8. Addresses: INTERFACE.md says every process knows every other's address from the start; main
   therefore constructs all `MpThread`s (minting their task ids) before starting any, and passes the
   `Tid` arrays to the bodies, instead of `MpProcess.spawn` (which constructs and starts at once).
   Same spawn order, same roles.

## Compile (from this directory)
    javac -d java/out -cp "$(cat ../../../jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
Result: no errors, no warnings.

## The one exploration run (from this directory)
    java -Xss4m -cp java/out:$(cat ../../../jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.vrr.Vrr:impl,3,2,bag' must 0 1 report - 600 </dev/null
Exit 0; `graphs: 868 distinct complete, 0 distinct blocked; stack exhausted` (868 iterations, ~10 s).
Last output line: `max_porf_count: 0`
`java/out` and `report/` were removed afterwards.
