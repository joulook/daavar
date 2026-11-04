# SwiftPaxos (paper, Java / JMC message-passing API): notes

Source: `java/bench/swiftpaxos_paper/SwiftpaxosPaper.java` (one file). Sources used: PAPER-EXCERPT.md
(§§2-4, Figures 3 and 4, Appendix A), INTERFACE.md, and the JMC API sources named in the prompt.
Scope: normal operation only. Ballot 0, leader n0, status NORMAL throughout, no recovery
(Figure 5, §4.2, §4.3 and the Liveness mechanisms of A.4 are not implemented).

## Configuration

- N = 3 replicas n0 (leader(0)), n1, n2; K = 2 clients c0, c1 with libraries l0, l1.
- Quorums (C2): FQ(0) = {{n0, n1}}; SQ(0) = {{n0, n1}, {n0, n2}}. A quorum's followers are its
  members other than the leader.
- `fast(p)`: p is in a fast quorum (n0, n1). `slow(p)` of line 25: see D3 (only n2).
- State machine: one integer register x = 0. Write(v): x := v, result unused. Read: result x.
  Rmw: x := x + 10, result the new x. Two commands conflict unless both are Reads.
- Delivery model: argument `fifo` -> `CommunicationModel.P2P`, `bag` -> `CommunicationModel.ASYN`;
  every send and receive of the program uses that model.

## Roles

### Main task
Spawns, with `MpProcess.spawn(role, body)`, n0 n1 n2 l0 l1 c0 c1 in this order, checks that each
handle's Tid equals the predicted address (D1), then awaits all handles in spawn order. Sends and
receives nothing. Since replicas and libraries never terminate, every execution ends with main
blocked on the await of n0 (Must's blocked label); the harness ends it there.

### Client ci (visible)
Exactly the Rust client of INTERFACE.md: for j = 0..q-1, kind = (i + j) mod 3: kind 0 sends
`Write{client:i, value:1+i+2j}` to li and expects `Ack`; kind 1 sends `Read{client:i}`, expects
`Value`; kind 2 sends `Rmw{client:i}`, expects `Result`. A reply of another type is an
`AssertionError`. Receives with an unrestricted guard (only li sends to ci). Ends after q replies.

### Client library li (internal), Figure 3
Loop forever:
1. Receive a request (guard: Write, Read or Rmw). Build `Cmd(Id(i, seq), kind, value)`, seq =
   0, 1, ... (command identifiers are (client, sequence number)).
2. submit(c), line 2: send `Propagate(c)` to n0, n1, n2 (in this order).
3. Receive, with a guard restricted to messages carrying the current id, `Reply`, `FastAck`,
   `SlowAck`; record the Reply, each follower's FastAck paths, the set of SlowAck senders.
   Stop as soon as line 3 or line 4 holds:
   - line 3: Reply(0, id, P, r) received, and for every follower f of the fast quorum {n0, n1}
     (i.e. n1): FastAck(0, id, _, P) from f with the same P, or SlowAck(0, id) from f;
   - line 4: Reply(0, id, _, r) received, and SlowAck(0, id) from every follower of some slow
     quorum (n1, or n2).
4. response(id, r), line 5: send `Ack{}` (Write), `Value{r}` (Read) or `Result{r}` (Rmw) to ci.
After its client is done, li stays blocked on step 1.

### Replica p in {n0, n1, n2} (internal), Figure 4
State: bal = 0, phase (default START), cmd, dep, paths, Commit (in commit order), Exec, state x,
pending_log (leader only), follower FastAck votes (id -> follower -> D), SlowAck senders
(id -> set), and a buffer of Propagate / leader-FastAck messages whose precondition is false.

Main loop: receive any message (unrestricted guard). A follower's FastAck (from != n0) or any
SlowAck is recorded as a vote (it is input to the condition of line 30). A Propagate or the
leader's FastAck is appended to the buffer. Then run handlers to a fixpoint: repeatedly fire the
first enabled of: commit (line 30, accepted ids in acceptance order), execution (line 33,
committed ids in commit order), and the first buffered message in arrival order whose
precondition holds (D6).

- Line 6, `Propagate(c)`, pre id(c) in START: phase := PREACCEPT, cmd[id] := c. If fast(p):
  D := ids of all other recorded commands conflicting with c (D2); dep[id] := D;
  P := pset(id) over the updated dep (D4); paths[id] := P.
  - p = n0 (leader): pending_log := pending_log . c; r := opt_exec (D7);
    send `Reply(0, id, P, r)` to l(client(id)); send `FastAck(0, id, D, P, from=0)` to n0, n1, n2.
  - p = n1: send `FastAck(0, id, D, P, from=1)` to n0, n1, n2 and to l(client(id)).
  - p = n2: no proposal, nothing sent.
- Line 22, `FastAck(0, id, D, P)` from n0, pre id in PREACCEPT and bal = 0 and every d in D is in
  ACCEPT or COMMIT (otherwise buffered; a duplicate for an already accepted id is dropped):
  phase := ACCEPT. If (fast(p) and dep[id] != D) or slow(p) (D3): dep[id] := D, send
  `SlowAck(0, id, from=p)` to n0, n1, n2 and to l(client(id)). Else if fast(p) and paths[id] != P:
  send `SlowAck(0, id, from=p)` to l(client(id)) only (line 29).
- Line 30, for id in ACCEPT with dep[id] in COMMIT: commit (phase := COMMIT) if, for the fast
  quorum, every follower f sent FastAck(0, id, D_f, _) with D_f = dep[id] or SlowAck(0, id); or,
  for some slow quorum, every follower sent SlowAck(0, id). The leader's FastAck is required by
  id being in ACCEPT.
- Line 33, for id in Commit \ Exec with dep[id] in Exec: apply cmd[id] to x; Exec += id; the
  leader removes it from pending_log.
- pset (lines 37-39): full paths over dep, each path a list of ids starting at id; dependencies
  of a node are visited in id order (the set does not depend on that order). A cycle reached by
  pset is an `AssertionError` (Invariant 3 says it cannot happen).

## Messages (records, all tagged with `@Tag`)

Visible (INTERFACE.md): `Write(int client, int value)`, `Read(int client)`, `Rmw(int client)`,
`Ack()`, `Value(int value)`, `Result(int value)`.
Internal: `Id(int client, int seq)`, `Cmd(Id id, int kind, int value)` (payloads only);
`Propagate(Cmd cmd)` library -> replicas; `Reply(int ballot, Id id, Set<List<Id>> paths, int result)`
leader -> library; `FastAck(int ballot, Id id, Set<Id> deps, Set<List<Id>> paths, int from)`
replica -> replicas (and n1 -> library); `SlowAck(int ballot, Id id, int from)` replica ->
replicas and/or library. Sets and lists are immutable copies.

## Decisions where the sources are silent

- D1 Addresses. INTERFACE.md: "Assume every process knows every other process's address from the
  start", and "The main thread ... sends and receives nothing". The Java entry has no Setup message.
  Decision: every process computes the addresses as the task ids minted in spawn order
  (main's id + 1 + k for the k-th spawn, after MpProgram's "the k-th process main spawns is task
  k + 1"); main checks each prediction against the handle and throws `AssertionError` on a mismatch.
- D2 Own command in D. Line 11 "let D = {id | cmd[id] ▷◁ c}" is evaluated after line 9 stored c,
  so read literally it would contain id(c) itself for a non-Read. Decision: id(c) is excluded.
- D3 slow(p, b) at line 25. §3.1: "we write fast(p, b) and slow(p, b) if p belongs to a fast and a
  slow quorum of b"; under (C2) with "slow quorums can be any majorities", n0 and n1 also belong to
  slow quorums, so a literal reading makes n1 always send SlowAck and the leader send SlowAck too.
  §3.3: "A slow quorum replica acts similarly to a fast quorum replica, except that it does not make
  proposals of its own." Decision: slow(p, b) at line 25 means "p is in a slow quorum and in no fast
  quorum" (here only n2). n0 and n1 send SlowAck only on disagreement (lines 25-27) or line 29.
- D4 Order of lines 12-14. Line 12 "let P = pset(id(c))" precedes line 13 "dep[id(c)] ← D", and
  pset reads dep[id] (line 38), which would still be ⊥. Decision: P is computed with dep[id(c)] = D.
- D5 Full paths. §4.1 "Representing dependency paths" trims paths and may hash them ("instead of
  the paths themselves one may send hashes"); this is an optimisation. Decision: the full,
  untrimmed sets of paths are sent and compared, as defined in §3.5 and by pset.
- D6 Handler order. §4: "a set of handlers, each of which executes atomically once its
  preconditions are true" does not order simultaneously enabled handlers. Decision: after each
  receive, handlers run to a fixpoint, commit first, then execution, then buffered messages in
  arrival order. No `Nondet` choice is used: the only choices left open are delivery order (made
  by the receives) and the execution order of two committed independent commands (two Reads),
  which cannot change any state or result; they are taken in commit order.
- D7 opt_exec. §4.4: "the command's response can be computed by reading the local copy while
  taking into account prior uncommitted commands in pending_log, and its side effects can be
  buffered until commit." Decision: opt_exec applies pending_log in order to a copy of the leader's
  executed state x and returns the result of the last command (c, just appended). A Write's result
  is 0 and unused.
- D8 Self-sends and send order. Line 19 "send FastAck ... to R" and line 27 "to R ∪ {client(id)}"
  include the sender itself. Decision: a replica sends to itself through the channel like to any
  other replica; sends go to n0, n1, n2 in index order, then to the library.
- D9 Paths compared at line 28. Line 28 compares "paths[id] ≠ P", i.e. the paths stored at
  Propagate time (line 14), not pset recomputed after line 26. Decision: the stored paths, as
  written; they are also what n1's FastAck carried to the client.
- D10 Library receives. Figure 3 does not say what a client does with acknowledgements of an
  earlier command. Decision: the library receives only messages carrying the current command id
  (selective receive); stale acknowledgements stay unreceived.
- D11 Buffered messages that can never fire: a duplicate Propagate (id not in START) or a leader
  FastAck for an id already in ACCEPT/COMMIT is dropped. Neither occurs in this program (each is
  sent once).
- D12 Await order. INTERFACE.md: main "waits for them". Decision: awaits in spawn order.
- D13 MpProgram metadata: `programName()` = `swiftpaxos_paper`, `size()` = q as a decimal string,
  roles n0 n1 n2 l0 l1 c0 c1, visible c0 c1.

## Compile (inside OUT_DIR)

    javac -d java/out -cp "$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')

Result: exit 0, no diagnostics.

## The one run (inside OUT_DIR)

    java -Xss4m -cp java/out:$(cat /local/home/mkhoshechin/Documents/github-jmc/daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.swiftpaxos_paper.SwiftpaxosPaper:impl,1,fifo' must 0 1 report - 600 </dev/null

Exit 0. Stop on the 600 s timeout: 31707 iterations, all blocked (as expected: replicas and
libraries never terminate), 0 complete, "graphs: 0 distinct complete, 31707 distinct blocked;
stack not exhausted", no assertion failure. Last output line: `max_porf_count: 0`.

Caveat on the record: stdout was redirected to /tmp/sp-run.log, a name another concurrent run
(bench.swiftpaxos_code.SwiftpaxosCode) also wrote to, so the file is interleaved. The lines quoted
above are attributed to this run by `observed: SwiftpaxosPaper(q=1, fifo)` and by the timing
(elapsed 600.045 s ending 22:08:45, start about 21:58:45, before the other run's 22:01:26 start).
The run's opening lines were overwritten.
