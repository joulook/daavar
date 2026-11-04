# SwiftPaxos (code port) in Rust / TraceForge Must API

Source: `plan/case-studies/swiftpaxos/code-src` (SwiftPaxos prototype cut). Interface: `INTERFACE.md`.
Port of the prototype's normal operation for ballot 0, leader n0, no recovery, with the
interface's replacements (3 replicas, fast quorum {n0, n1}, one register, Write/Read/Rmw,
no batching, no delays, no timers, no master). Line numbers below are of the Go files.

## Processes
- `main`: spawns n0, n1, n2, then l0, l1, then c0, c1 (named threads), then `init`; joins
  c0, c1, init, then l0, l1, n0, n1, n2 (the last joins never return: the execution ends
  blocked). `main` sends and receives nothing.
- `init` (invisible): sends `Setup { nodes, libs, clients }` (tag 1) to n0, n1, n2, then l0, l1.
- `c0`, `c1`: the interface's client code, verbatim.
- `l0`, `l1`: the SwiftPaxos client side (swift/client.go), one command at a time.
- `n0` (leader), `n1`, `n2`: the replica (swift/swift.go `run`).
All receives are tagged receives, so a message that arrives before Setup stays in the mailbox
(buffered) until the Setup has been received.

## Messages
Fixed (visible): `Req::{Write(i,v), Read(i), Rmw(i)}` client -> library (untagged),
`Rep::{Ack, Value(v), Result(v)}` library -> client (untagged).

Internal:
- `RMsg::Propose { cmd, op }` library -> every replica (tag 2). GPropose (replica.go:514-523);
  sent to all replicas because the client side runs with Fast = true (client.go:186-194).
- `RMsg::FastAck { replica, cmd, dep, checksum, seqnum }` replica -> every other replica (tag 2).
  MFastAck (defs.go:53-60), delivered by the batcher as a one-entry MOptAcks (batcher.go:37-47,
  135-142); the receiver handling is swift.go:321-341.
- `RMsg::LightSlowAck { replica, cmd }` replica -> every other replica (tag 2). The MOptAcks
  entry with NilDep (batcher.go:155-164), turned into an MFastAck with nil Dep at the receiver
  (swift.go:330-334).
- `LMsg::FastAckClient { replica, cmd, checksum }` follower -> library of cmd.client
  (MFastAckClient, defs.go:62-67, batcher.go:52-59, 122-125).
- `LMsg::LightSlowAck { replica, cmd }` follower -> library (batcher.go:126-129, swift.go:442-450).
- `LMsg::Reply { replica, cmd, checksum, rep }` leader -> library (MReply, replychan.go:20-28).
- Replica -> library messages carry tag 100 + seq (the command's sequence number).
- Ballot fields are dropped: always 0 (every ballot comparison in swift.go:419, 485 and
  client.go:187-193 holds).

## Replica (swift.go), per message
Single-threaded: one loop, `recv_tagged_msg_block` on tag 2, each message handled to completion.
- Propose(cmd, op) (swift.go:274-289): record the proposal; compute `dep` and `hs`
  (getDepAndHashes, swift.go:767-792, light key info key.go:470-506 on the one key:
  a Read depends on the last Write/Rmw, a Write/Rmw on the last command; `hs` = [hash-log
  append of cmd]); a non-leader applies a pending leader hash (swift.go:279-283); create or
  fetch the descriptor (proposeDep = dep, hs set once, swift.go:597-606); handlePropose
  (swift.go:355-407):
  - n2 (not in FQ): only runs a stored leader FastAck (afterPropagate.Recall) and returns.
  - n0, n1: dep = proposeDep, phase PRE_ACCEPT, Recall; if the Recall ran and took the slow
    path, or the command is delivered, no FastAck. Otherwise build FastAck{dep, checksum=hs,
    seqnum (leader: r.seqnum++)}.
    - leader: FastAck to n1, n2; then deliver (optimistic execution; the Reply is sent there).
    - n1: FastAckClient to the library, FastAck to n0, n2.
    Then handleFastAck(own FastAck).
- FastAck from a replica (swift.go:321-341): if from the leader and I am not the leader,
  recordLeaderHash(cmd, seqnum, checksum) (swift.go:794-821, dpath.go:90-143); unless delivered,
  handleFastAck.
- LightSlowAck from a replica: the same with dep = nil, checksum = [].
- handleFastAck (swift.go:409-415): from the leader -> fastAckFromLeader, else commonCaseFastAck.
- fastAckFromLeader (swift.go:417-482): waits for the proposal (afterPropagate); then phase
  ACCEPT; neq = (own dep != leader dep); sendSlowAck = not leader && (SQ contains me ||
  (FQ contains me && neq)) — SQ is Majority, which contains everyone, so every follower sends
  it; add the leader FastAck to the slow set, then (if not delivered) to the fast set; if
  sendSlowAck: adopt the leader's dep if neq and not delivered, slowPath = true, LightSlowAck to
  the library and to the other replicas, and (if not delivered) count the own LightSlowAck
  (handleLightSlowAck). Deferred part (swift.go:438-451): a LightSlowAck to the client when
  not sendSlowAck and the checksums differ (never fires here: followers always sendSlowAck,
  the leader returns).
- commonCaseFastAck (swift.go:484-497): nil dep -> slow set (and stop if delivered); then fast set.
- Message sets (mset.go:229-271): slow set over SQ = Majority(2) (contains all), fast set over
  FQ = {n0, n1}; accept = leader absent, or dep nil, or dep equals the leader's
  (swift.go:665-673); fires when |msgs| >= size, or >= size-1 with the leader's message, and
  acts only with the leader's message.
- Commit (swift.go:499-522): phase COMMIT, register cmd as successor of each undelivered dep,
  deliver.
- deliver (swift.go:533-583): needs the proposal, not delivered, phase COMMIT unless I am the
  leader (optExec), all deps delivered; executes on the register (Write: x := v; Read: x;
  Rmw: x := x+10, returns x); the leader sends Reply{checksum = hs, rep}; then delivers the
  successors.
- Hash log (dpath.go): an SHash is represented by its chain (the list of command ids folded
  into it); Append = chain of synced hash + pending + cmd; Update removes cmd from pending and
  moves the synced point to the leader's (seqnum, hash) when seqnum is higher.

## Client library (swift/client.go), per command
Waits for a Req from its client (filter: sender = client, untagged); cmd = (i, ++seq)
(client.go:198-199); Propose to n0, n1, n2 in index order. Two message sets, both
Majority(3) = 2 containing every replica (client.go:41-60, fixedMajority), accept = leader
absent or checksums equal (client.go:80-100). Receives only messages tagged with this command:
- Reply (client.go:243-260): val = rep; leader entry of the fast set (with its checksum);
  if not done, leader entry of the slow set with nil checksum.
- FastAckClient (client.go:132-139, 186-202): fast set entry with its checksum.
- LightSlowAck (client.go:204-226): fast set entry with nil checksum; if not done, slow set entry.
Done when either set fires with the leader's message (client.go:228-241); then answers the
client: Write -> Ack, Read -> Value(val), Rmw -> Result(val).
So the fast path is Reply + n1's FastAckClient with an equal checksum; the slow path is
Reply + a LightSlowAck from n1 or n2.

## Decisions (where the source is open or I simplified)
1. optExec = true, fastRead = false, slowAddrs empty, Exec = Dreply = true, Proxy irrelevant.
   The flags are constructor arguments (swift.go:106-107, 131-132, 151-153); optExec = true is the
   mode in which the leader replies (replychan.go:20-28) and followers send FastAckClient
   (swift.go:395-405), which is the interface's "leader's Reply and FastAck/SlowAck of the
   followers". fastRead = false so a Read takes the same path as a write (swift.go:392, 398).
2. Quorums: replica FQ = {n0, n1} (the quorum file's AQ for ballot 0, swift.go:164-177 with
   fixedMajority), SQ = Majority(2) (swift.go:157). Client FQ = SQ = Majority(2) as in
   client.go:41-60 ("TODO: it has to be the correct majority"); since n2 never sends a
   FastAckClient this behaves as the fast quorum {n0, n1}.
3. hook.OptCondF (afterPropagate, swift.go:364, 370, 418, 661-663; package `hook` is not in the
   cut): Call(f) runs f now if the proposal is present, else stores it; Recall() runs the stored
   f if the proposal is present and returns whether it ran. (Returning true whenever the
   condition holds would give the same result at swift.go:370, since slowPath is set only by f.)
4. No batching: every FastAck / LightSlowAck is its own message to the client and to the other
   replicas (batcher.go merges and suppresses FastAckClient when a LightSlowAck is in the same
   batch, batcher.go:113-116, 217-225; with no batching both are sent). Order: client first,
   then replicas in index order (batcher.go:122-142).
5. One thread per replica: the per-command goroutines (swift.go:613-618, 702-722), the
   deliverChan goroutines (swift.go:560-564) and the replyChan goroutine (replychan.go) are
   collapsed into synchronous calls; successors are delivered right after the delivery, the
   Reply is sent inside deliver. The history slot bookkeeping (swift.go:573-581, 745-757) is
   dropped; a delivered command's messages are dropped as getCmdDescSeq does (swift.go:591-593).
   What a replica sends does not depend on this timing (delivery at a follower needs the
   proposal, so swift.go:371 never sees it delivered).
6. Hash caches: SHA-256 chains replaced by the chain of command ids (equal iff the hashes are
   equal, assuming no collisions) (dpath.go:60-163).
7. Library: stale messages of earlier commands are never received (tag = 100 + seq); the Go
   client drops them on arrival (client.go:195-197, 205-207, 244-246), so the observable
   behaviour is the same. While idle the library only waits for its client's Req.
8. Seq numbers start at 1 (client.go:198-199); ClientId = library index.
9. Rmw is a PUT for conflicts and dependencies (it writes); every two commands conflict except
   two Reads (INTERFACE.md); Write returns nothing (state.go PUT returns nil) -> Ack.
10. The second program argument `[<fifo|bag>]` is accepted and ignored; the model comes from
   the harness's `--model`.
11. `init` is spawned after the clients (it needs every ThreadId); Setup goes to replicas then
   libraries. Tags: Setup 1, replica inbox 2, library inbox 100 + seq.
12. No `nondet()`: the code leaves no choice open in this setting other than message arrival
   order (Go map iteration in mset.go:243 and batcher.go:122-129 does not affect behaviour
   without batching).
13. Behaviours kept although odd: a follower may commit with its own dep before adopting the
   leader's (swift.go:453-463), and n2's desc.dep stays empty until the leader's FastAck
   (swift.go:363-366); both affect only the follower's local register, never a reply.

## Build and run
- Compile (in this directory): `cargo build --release -j 2` -> finished, no errors, no warnings.
- Run (in this directory): `timeout 600 target/release/bench_swiftpaxos_code count impl --model fifo -- 1`
  -> killed by the timeout (exit 124) before printing `execs=...`; last output line:
  `Executions attempted so far: 1000000 total 0 finished normally 1000000 blocked (2652.52/sec).`
  (all executions end blocked, as expected: replicas and libraries never terminate).
