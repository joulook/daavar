# Hermes-replicated key — Java (JMC message-passing API)

Source: `java/bench/hermes/Hermes.java`, class `bench.hermes.Hermes` (implements `MpProgram`),
constructed from `impl,<N>,<K>,<fifo|bag>` (`fifo` -> `CommunicationModel.P2P`, `bag` -> `ASYN`).
Program name `hermes`, size `"<N>,<K>"`, roles `r0..r{N-1}` then `c0..c{K-1}`, visible roles the clients.

## Mapping from the paper to the code

Main (`Hermes.run`): creates the replica processes (`new MpThread(body, "r<j>")`), then the client
processes (`"c<i>"`), in index order, records each one's `Tid` (from `getTaskId()`, before any process is
started), starts them all in the same order and awaits them (`join1`). It sends and receives nothing.

Clients (`client`): `Write(from=i, value=i+1)` to `r(i mod N)`, `recv(WriteAck)`; then `Read(from=i)` to
`r((i+1) mod N)`, `recv(ReadReply)`.

Messages (all `@Tag` records): client ones `Write{from,value}`, `WriteAck{}`, `Read{from}`, `ReadReply{value}`
(as INTERFACE.md with its addendum); replica ones `INV{ts,value,from}`, `ACK{ts,from}`, `VAL{ts}`, with
`TS{version,cid}` the paper's [v, cid], compared lexicographically (`Ts.higherThan`, footnote 5). The key is
implicit (one key).

Replica (`Replica`), per-key metadata: `state` (Valid/Invalid/Write/Replay/Trans; Replay never used), `ts`,
`value`; coordinator bookkeeping: `pending` (TS of its write in flight), `pendingClient`, `acks` (set of
follower ids). One event loop: blocking `recv` of any message, in arrival order.
- Reads ("returning the local value ... if it is in the Valid state. ... otherwise the request is stalled"):
  every client request is queued in `stalled`; `serveStalled` serves the queue in arrival order while the key
  is Valid; a Read is answered with `ReadReply(value)` to `c<from>`.
- Coordinator, "issues a write ... only if it is in the Valid state; otherwise the write is stalled":
  a stalled Write starts (`coordinate`) when it reaches the head of the queue in the Valid state.
  - CTS: `ts = (ts.version+1, id)`.
  - CINV: value applied locally, state Write, INV(ts, value) sent to every other replica.
  - CACK (`onAck`/`complete`): ACKs with the pending TS are counted; at N-1 the key goes Valid (Invalid if
    it was Trans), and `WriteAck` goes to `c<from>`.
  - CVAL: VAL(ts) sent to every other replica (after the WriteAck).
- Follower:
  - FINV (`onInv`): if the INV's TS is higher than the local one, state Invalid (Trans if a write of this
    replica is in flight), local TS and value replaced.
  - FACK: always `ACK(inv.ts)` to the INV's sender.
  - FVAL (`onVal`): Valid iff the VAL's TS equals the local TS, otherwise ignored.

## Decisions where the paper is silent

D1. Replays omitted (and so the Replay state and the mlt timer). Paper: "A request that finds a key in the
Invalid state for an extended period of time (determined via the mlt timer, described in §3.4) triggers a
write replay." INTERFACE.md excludes faults and losses, so every invalidation is followed by the VAL of
the highest-TS write; a replay is never needed, and no choice operation (`Nondet`) is used.

D2. A coordinator has at most one write in flight; further client requests (reads and writes) stay
stalled until its write has all ACKs. Paper: "A coordinator node issues a write to a key only if it is in
the Valid state" (silent about a second write while its own is pending but the key was validated by a VAL).

D3. A VAL may validate the key of a coordinator in Trans (its TS equals the higher write's TS that
invalidated it); its own write keeps collecting ACKs and, on completion, the key stays Valid. Paper: FVAL
"transitions the key to the Valid state if and only if the received timestamp is equal to the key's local
timestamp" and CACK "(Invalid state if the key was in Trans state)" — silent on Trans then VAL before the
last ACK. Correspondingly, FINV puts the key in Trans whenever a write of this replica is in flight (also
if the key was already Valid or Trans), not only from Write/Replay.

D4. INV and ACK carry the sender's replica index (`from`): the API's receive does not report a sender, and
"the coordinator receives ACKs from all the live replicas" needs the ACKs counted per follower. VAL needs
none.

D5. Initial key: value 0, TS (0, 0), state Valid on every replica (INTERFACE.md: "initially 0").

D6. Termination: a replica stops after the number of messages it receives in every run (no faults, no
replays): own Writes + Reads from clients + (N-1) ACKs per own write + INV and VAL for each other write
(`expectedMessages`). It then checks that nothing is stalled or pending and the key is Valid (throws
otherwise). The paper's replicas run forever.

D7. Order of the coordinator's sends at completion: `WriteAck` to the client first, then the VALs ("once
all ACKs are received, it is safe to respond to a client"; CVAL "Finally").

## Commands (from this directory, `daavar/benchmarks-cf2/hermes/java`)

Compile (no warnings, no errors):

    javac -d java/out -cp "$(cat ../../../jmc/cp.txt)" $(find java -name '*.java' -not -path '*/out/*')

One run (plain exploration, smallest parameters):

    java -Xss4m -cp java/out:$(cat ../../../jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher 'bench.hermes.Hermes:impl,3,2,bag' must 0 1 report - 600 </dev/null

Exit 0; `graphs: 4652 distinct complete, 0 distinct blocked; stack exhausted`, 16.6 s. Last output line:

    max_porf_count: 0

(Full output in `run.log`; its only ERROR line is log4j's own configuration warning.)
