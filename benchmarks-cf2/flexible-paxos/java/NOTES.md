# Flexible Paxos (single decree), Java / JMC message-passing API

Source: `java/bench/fpaxos/FPaxos.java`, class `bench.fpaxos.FPaxos` (implements `MpProgram`),
constructed from `impl,<A>,<K>,<fifo|bag>`; P = K; R = 1 (constant `FPaxos.R`).
Quorums (section 4.1, A even): Q1 = A/2 + 1, Q2 = A/2 (A = 4: Q1 = 3, Q2 = 2).
Model: `bag` -> `CommunicationModel.ASYN`, `fifo` -> `CommunicationModel.P2P`; every send and receive uses it.

## Processes
- main (`run()`): spawns `a0..a{A-1}`, then `p0..p{P-1}`, then `c0..c{K-1}` (`MpProcess.spawn(role, body)`),
  then awaits every handle in spawn order. It sends and receives nothing. All protocol state is local
  to one `run()`, so each iteration starts fresh.
- Addresses: three `AtomicReferenceArray<Tid>` (acceptors, proposers, clients), filled by main from the
  spawn handles and by each process with its own `MpProcess.self()` before its first send, so a reader
  (who reads an entry only after receiving a message from that process) always finds it.

## Messages
| Paper | Record (tag) | Fields |
|---|---|---|
| client request | `Propose` ("Propose") | from, value |
| reply to client | `Decided` ("Decided") / `Fail` ("Fail") | value / - |
| prepare(p) | `Prepare` | round, proposer |
| promise(p, (p', v')) | `Promise` | acceptor, round, proposer, acceptedRound, acceptedProposer, acceptedValue (acceptedRound = 0: none) |
| propose(p, v) | `ProposeP2` (named so as not to clash with the client's `Propose`) | round, proposer, value |
| accept(p) | `Accept` | acceptor, round, proposer |
| conflict reply (not in the paper) | `Nack` | acceptor, phase (1 or 2), round, proposer, promisedRound, promisedProposer |
| termination (not in the paper) | `Done` | proposer |

Proposal number p = (round, proposer index), compared round first, then proposer index (`cmp`).
Rounds start at 1; round 0 denotes "nothing promised/accepted".

## Mapping, role by role
- Client `ci` (`client`): sends `Propose(i, i+1)` to `pi`, receives one `Decided` or `Fail`, ends.
- Proposer `pi` (`proposer`): receives the client's `Propose`; then up to 1 + R attempts:
  - Phase 1 i: sends `Prepare(round, i)` to all A acceptors.
  - Phase 1 iii: receives replies for this (round, i) and phase 1 (selective receive guard), until Q1
    promises or more than A - Q1 nacks (Q1 can no longer be reached). Remembers the promise with the
    highest accepted proposal number.
  - Phase 2 i: v = the value of the highest-numbered accepted proposal among the Q1 promises received,
    else the client's value; sends `ProposeP2(round, i, v)` to all A acceptors.
  - Phase 2 iii: receives replies for this (round, i) and phase 2 until Q2 accepts (v is decided, the
    proposer learns it) or more than A - Q2 nacks.
  - A failed phase starts the next attempt with a higher round; after R failed retries it gives up.
  - Finally replies `Decided(v)` or `Fail()` to `c{from}`, then sends `Done(i)` to every acceptor.
- Acceptor `ai` (`acceptor`): loop receiving `Prepare`, `ProposeP2` or `Done` in arrival order:
  - Phase 1 ii: `Prepare(p)` with p greater than the promised number -> promised := p, reply
    `Promise(p, last accepted proposal)`; otherwise reply `Nack(phase 1, promised)`.
  - Phase 2 ii: `ProposeP2(p, v)` with p >= promised -> promised := p, accepted := (p, v), reply
    `Accept(p)`; otherwise `Nack(phase 2, promised)`.
  - Stops after one `Done` from each of the P proposers.

## Decisions where the paper is silent
1. Rejection message. Paper: "Once the proposer receives promise from the majority of acceptors, it
   proceeds to phase two. Otherwise, it may try again with higher proposal number." It does not say how
   the proposer learns that it will not get a quorum. Decision: an acceptor that refuses replies `Nack`
   carrying its promised number; since no process fails and every message is delivered, every acceptor
   answers every request, so the proposer gives up a phase exactly when more than A - Q nacks arrive.
   No timeout and no nondeterministic choice is used: when to give up follows from the order in which
   replies arrive (the prompt's "otherwise by the order in which messages arrive").
2. Higher proposal number on retry. Paper: "it may try again with higher proposal number." Decision:
   next round = max(current round, highest promised round reported in the nacks of this attempt) + 1.
3. Retry after a failed phase 2. Paper: "Otherwise, it may try phase 1 again with a higher proposal
   number." Decision: a failed phase 2 counts as one attempt, like a failed phase 1; at most R = 1 retry
   in total, then `Fail`.
4. Promise condition. Paper: "If p is the highest proposal number promised". Decision: strictly greater
   than the current promise (equal cannot occur for a fresh prepare since numbers are unique).
5. Targets of prepare/propose. Paper: "sends prepare(p) to the acceptors" / "sends propose(p,v) to the
   acceptors". Decision: to all A acceptors, waiting for the first Q1 (resp. Q2) positive replies; late
   replies of finished phases stay unread (guards match only the current round and phase).
6. Phase-2 value. Paper: "If more than one proposal was returned in phase 1 then it must choose the value
   associated with the highest proposal number." Decision: only the promises counted towards the Q1
   quorum are considered (replies after the quorum are not read).
7. Termination of acceptors. The paper's acceptors run forever. Decision: each proposer, when finished,
   sends `Done` to every acceptor; an acceptor ends after a `Done` from each proposer, so main can await
   every process. No proposer is waiting on an acceptor at that point.
8. Learner: each proposer learns only its own decision (interface), there is no separate learner role.
9. `programName()` = "fpaxos", `size()` = "<A>,<K>".

## Commands (from this directory)
Compile:
    javac -d java/out -cp "$(cat ../../../jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')
Result: compiled without errors or warnings.

Run (once, plain exploration):
    java -Xss4m -cp java/out:$(cat ../../../jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.fpaxos.FPaxos:impl,4,2,bag' must 0 1 report - 600 </dev/null
Last output line: `max_porf_count: 0`. The run stopped on the 600 s timeout (stop_reason: timeout, stack not
exhausted) after 44436 distinct complete graphs, 0 blocked, no failure; exit status 0.
