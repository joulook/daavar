# CASPaxos register, Java (JMC Must API)

Source: `java/bench/caspaxos/CasPaxos.java`, class `bench.caspaxos.CasPaxos` (implements
`org.mpi_sws.jmc.mp.launch.MpProgram`, constructed from `impl,<A>,<K>,<fifo|bag>`).
Constants: P = 2 proposers, R = 1 retry. F = (A - 1) / 2, quorum F + 1.
`fifo` maps to `CommunicationModel.P2P`, `bag` to `CommunicationModel.ASYN`; every send and receive
uses that model. `programName()` = `caspaxos`, `size()` = `<A>,<K>`, visible roles = `c0..c{K-1}`.

## Mapping from the paper to the code

Main (`run()`): spawns `p0..p{P-1}`, then `a0..a{A-1}`, then `c0..c{K-1}` in index order (roles = the
names), records the proposer and acceptor Tids as it spawns them, then awaits every process. It sends
and receives nothing.

Client `ci` (`client`): "A client submits the f change function to a proposer" = sends
`Cas(expected = i, value = i + 1)` to `p(i mod P)`; then a blocking receive of `Done` or `Fail`.

Proposer `pj` (`proposer`), once per request assigned to it (clients i with i mod P = j):
- receives a `Cas` (blocking, any `Cas`, arrival order);
- "generates a ballot number, B": `counter++`, B = (counter, j); compared counter first, id as the
  tiebreaker (`compareBallot`);
- "sends prepare messages containing that number to the acceptors": `Prepare(counter, id)` to every
  acceptor;
- "Waits for the F + 1 confirmations": receives `Promise` / `Conflict(phase = 1)` for ballot B only
  (guard on the ballot) until F + 1 promises;
- "If they all contain the empty value, then the proposer defines the current state as ∅ (= 0)
  otherwise it picks the value of the tuple with the highest ballot number": done over the F + 1
  promises received;
- "Applies the f function to the current state": `next = current == expected ? value : current`;
- "sends the result, new state, along with B (an accept message) to the acceptors": `Accept(counter,
  id, next)` to every acceptor;
- "Waits for the F + 1 confirmations": receives `Accepted` / `Conflict(phase = 2)` for ballot B
  until F + 1 accepted;
- "Returns the new state to the client": `Done(next)` to the client.
- "When a proposer receives a conflicting message from an acceptor, it should update its counter":
  on each `Conflict`, `counter = max(counter, seenCounter)`, so the next ballot exceeds the one seen.

Acceptor `ak` (`acceptor`), state: promise (`promised, promC, promId`), accepted tuple
(`accepted, accC, accId, value`), initially no promise and nothing accepted. It receives any message
(arrival order) and:
- `Prepare(B)`: "Returns a conflict if it already saw a greater ballot number" -> `Conflict(1, B,
  seen)`; else "Persists the ballot number as a promise and returns a confirmation either with an
  empty value or with a tuple of an accepted value and its ballot number" -> `Promise(B, empty,
  accBallot, value)`.
- `Accept(B, v)`: conflict as above -> `Conflict(2, B, seen)`; else "Erases the promise, marks the
  received tuple (ballot number, value) as the accepted value and returns a confirmation" ->
  `promised = false`, accepted = (B, v), `Accepted(B)`.

Messages (all `@Tag` records, ints and booleans only): `Cas{expected,value}`, `Done{value}`,
`Fail{}` (fixed by INTERFACE.md); `Prepare{counter,id}`, `Promise{counter,id,empty,accCounter,accId,
value}`, `Accept{counter,id,value}`, `Accepted{counter,id}`, `Conflict{phase,counter,id,seenCounter,
seenId}`; `Stop{id,count}` (not in the paper, see below).

## Decisions where the paper is silent

1. When the proposer gives up a phase. Paper: "Waits for the F + 1 confirmations" (nothing on how
   many conflicts end the wait). Decision: the proposer keeps receiving replies for B until it has
   F + 1 confirmations, or until more than A - (F + 1) conflicts make F + 1 confirmations impossible;
   then the attempt failed. Every acceptor answers every request exactly once, so the wait always ends.
2. Reaction to a failed attempt. Paper: "When a proposer receives a conflicting message from an
   acceptor, it should update its counter to avoid a conflict in the future." Decision: after a
   failed phase 1 or phase 2 the proposer restarts both phases with a fresh ballot, at most R = 1
   times per request; after that it sends `Fail{}`. Retries are not chosen nondeterministically;
   no `Nondet` choice is used anywhere (no choice point remains open: message order decides).
3. Which acceptors are contacted. Paper: "sends prepare messages ... to the acceptors" / "an accept
   message to the acceptors". Decision: all A acceptors, in both phases.
4. Ballot equality. Paper: "Returns a conflict if it already saw a greater ballot number."
   Decision: strictly greater; the ballot seen is the larger of the current promise and the accepted
   ballot (the promise is erased on accept, the accepted ballot remains). The conflict reply carries
   that ballot so the proposer can update its counter.
5. Counter update. Paper: "it should update its counter". Decision: `counter = max(counter,
   seenCounter)`; the next ballot is `counter + 1`.
6. Stale replies. Paper silent on replies that arrive after the proposer has its F + 1. Decision:
   the proposer's receives are selective on the current ballot (and on the phase for conflicts);
   replies of earlier phases or attempts are never read.
7. Phase-1 empty check. Paper: "If they all contain the empty value ... otherwise it picks the
   value of the tuple with the highest ballot number". Decision: computed over exactly the F + 1
   promises that completed the quorum.
8. Termination of acceptors (paper: processes run forever; INTERFACE.md: main awaits every
   process). Decision: when a proposer has served all its requests it sends `Stop{id, count}` to
   every acceptor, `count` = number of messages it sent to that acceptor; an acceptor ends once it
   has a `Stop` from every proposer and has handled the sum of the counts. So every prepare/accept
   is handled, and no process ends blocked.
9. Who a reply goes to. `Cas` carries no sender. Decision: the proposer replies to client
   `c{expected}` (the request of `ci` has `expected = i`). Each client publishes its own Tid
   (`MpProcess.self()`) in a per-iteration table before sending its request, so the proposer can read
   it after receiving the request; proposer and acceptor Tids are recorded by main as it spawns them
   (all before any client exists). Acceptors reply to proposer `id` of the ballot.
10. Requests per proposer. Proposer `pj` serves exactly the number of clients i < K with
    i mod P = j, one at a time, in arrival order, then stops.

## Commands

Compile (from this directory, `daavar/benchmarks-cf2/caspaxos/java`):

    javac -d java/out -cp "$(cat ../../../jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')

Result: compiled without errors or warnings.

Run (once, plain exploration, smallest parameters as given in the task):

    java -Xss4m -cp java/out:$(cat ../../../jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.caspaxos.CasPaxos:impl,3,2,bag' must 0 1 report - 600 </dev/null

Result of the one run (exit 0, stopped at the 600 s timeout, stack not exhausted):

    graphs:     117848 distinct complete, 0 distinct blocked; stack not exhausted

Last output line: `max_porf_count: 0` (the end of the measurement block; full output in `run.log`).
