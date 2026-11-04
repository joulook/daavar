# bench_zookeeper_derived: client-observable specification of Zab (fixed leader, no failure)

Source: `plan/case-studies/zookeeper/src-3.9/zookeeper-specifications/protocol-spec/Zab.tla`
(line numbers below refer to that file) and `plan/case-studies/zookeeper/INTERFACE.md`.
`spec` and `imp` of `Program` both return this program.

## 1. What a client can observe

Definitions. Let P0 = <1, 3, …, 2e-1> and P1 = <2, 4, …, 2e> be the values c0 and c1 propose
(INTERFACE.md `value(i, j)`). Let `D_s` be the sequence server `s` delivers, i.e.
`history[s][1..lastCommitted[s].index]`.

1. One broadcast order. The leader appends each request to its history when it receives it
   (`LeaderProcessRequest`, l.886-898; zxid = `IncZxid`, l.878) and broadcasts in zxid order
   (`LeaderBroadcastPROPOSE`, l.905-925, `sendCounter`). Call the leader's history T; T is a
   permutation of P0 ++ P1 in the order the requests reach the leader.
2. The leader delivers T in order: `LeaderTryToCommit` (l.970-996) commits index k only when
   `lastCommitted.index >= k-1` (`allTxnsBeforeCommitted`) and `ordered`.
3. A follower delivers T in order: `FollowerProcessPROPOSE` (l.937-968) appends only the next
   zxid (`IsNextZxid`, l.927); `FollowerProcessCOMMIT` (l.1049-1077) commits only the first
   pending entry (`match`).
4. Invariants giving the same: `PrefixConsistency` (l.1160): D_i and D_j agree on their common
   prefix; `TotalOrder` (l.1190); `LocalPrimaryOrder` (l.1206): delivery respects the leader's
   broadcast (zxid) order; `GlobalPrimaryOrder` (l.1229) and `PrimaryIntegrity` (l.1237) are
   vacuous with one epoch; `Integrity` (l.1168): only broadcast values are delivered, each once
   (zxids unique); `Agreement` (l.1178).
5. Hence every `D_s` is a prefix of the one T, and with no failure every `D_s` = T (D1).
6. Observed: c0 receives `Committed` for T from n0, c1 for T from n1, in the order the delivery
   model gives (fifo: in T's order); each client stops after its own e values.

Under fifo the arrival order at the leader preserves each client's proposal order
(c0 -> n0 direct; c1 -> n1 -> n0 by REQUEST forwarding), so T is an interleaving of P0 and P1;
this is not written into the program, it follows from the channels (under bag any permutation).

## 2. The program, role by role and packet by packet

| process | role in the specification |
|---|---|
| `c0`, `c1` | INTERFACE.md client code, verbatim. c0 sends to `n0`, c1 to `n1`. |
| `n0` (`sequencer`) | Receives its Setup; then 2e times: receives the next Propose (from c0 untagged, or from n1 tagged FWD) - this receive fixes the next element of T - and sends `Committed(v)` to c0 and DELIVER `Committed(v)` to n1. Ends. |
| `n1` (`follower`) | Receives its Setup; relays its client's e Propose to n0 (FWD) in arrival order; then receives the 2e DELIVER messages and sends each as `Committed(v)` to c1. Ends. |
| `n2` | Spawned (index order kept), does nothing, ends. |
| `init` | Sends `Setup { nodes, clients }` (tag SETUP) to n0 and n1. Ends. |
| `main` | Spawns n0, n1, n2, c0, c1, init; joins all; no message event. |

| packet | from -> to | tag | meaning |
|---|---|---|---|
| `Req::Propose(v)` | ci -> ni | none | client proposal (visible) |
| `Setup` | init -> n0, n1 | 1 SETUP | thread ids |
| `Req::Propose(v)` | n1 -> n0 | 2 FWD | REQUEST forwarding of c1's proposal |
| `Rep::Committed(v)` | n0 -> n1 | 3 DELIVER | "next element of T" (abstracts PROPOSE+ACK+COMMIT) |
| `Rep::Committed(v)` | n0 -> c0, n1 -> c1 | none | delivery (visible) |

Tags separate message types because `recv_*` does not filter by type (`recv_msg_block_with_tag`
-> `expect_msg`, lib.rs l.1565-1573); a Propose that arrives before Setup stays buffered because
the Setup receive is tag-filtered.

## 3. TLA+ definitions -> functions

| Zab.tla definition (line) | function / place in `src/main.rs` |
|---|---|
| `LeaderProcessRequest` (886), `IncZxid` (878) | `sequencer`: the Propose receive (arrival order = T) |
| `LeaderBroadcastPROPOSE` (905), `Broadcast` (232) | `sequencer`: `send_tagged_msg(n1, DELIVER, …)` |
| `FollowerProcessPROPOSE` (937), `IsNextZxid` (927), `LeaderProcessACK` (1005), `LeaderTryToCommit` (970), `DiscardAndBroadcast` (244) | abstracted: leader delivery = `send_msg(c0, Committed)` in `sequencer`, in T order |
| `FollowerProcessCOMMIT` (1049) | `follower`: DELIVER receive + `send_msg(c1, Committed)` |
| `PrefixConsistency` (1160), `TotalOrder` (1190), `LocalPrimaryOrder` (1206), `Agreement` (1178) | one T, every delivery point sends all of T in T's order |
| `Integrity` (1168) | only received proposals are delivered, each once |
| `GlobalPrimaryOrder` (1229), `PrimaryIntegrity` (1237) | vacuous (one epoch); nothing |
| Discovery/sync: `ConnectAndFollowerSendCEPOCH` (475) … `FollowerProcessCOMMITLD` (855); `UpdateLeader`/`FollowLeader` (395/404) | not modelled (fixed leader, epoch 1, empty initial history); `init`'s Setup stands for "every server knows n0 leads" |
| `Timeout` (427), `Restart` (443), `FilterNonexistentMessage` (1081) | not modelled (no failure in the instance) |
| Follower request forwarding (not in Zab.tla, l.881-885 note; INTERFACE.md REQUEST) | `follower`: FWD relay |

## 4. Decisions where the specification is silent

- D1 No loss. Zab.tla has no liveness property; under the instance (no failure, every message
  delivered, quorum {n0, n1} always available without n2) every proposal is committed and
  every server delivers all of T. So no proposal is dropped and no `nondet()` "lost" branch exists.
- D2 No `nondet()` at all. The only freedom the protocol leaves visible is the order T, which in
  Zab is the arrival order of requests at the leader (l.886); that is a message race, so it is
  left to the receive in `sequencer` (the prompt: "otherwise the order in which messages arrive").
  Delivery timing and reordering of Committed under bag are left to the model.
- D3 `late` has no client-visible effect: n2 has no client and is not needed for the quorum of
  2, and a DIFF synchronises only n2's history. The argument is parsed and ignored; n2 is a
  thread that ends at once (kept so that the spawn order of INTERFACE.md is n0, n1, n2, c0, c1).
- D4 Zab.tla lets only the leader receive requests (l.881-885). INTERFACE.md connects c1 to the
  follower n1, so the specification keeps n1 as c1's delivery point and REQUEST relay. Under fifo
  this preserves c1's proposal order at the leader, as the implementation's channels do.
- D5 `n1` relays all e proposals of c1 before it delivers anything. Not visible: c1 sends all its
  proposals before it receives anything, and n0 does not wait for n1's deliveries.
- D6 Internal threads end (the prompt), unlike the implementation's servers: n0 sequences exactly
  2e proposals, n1 delivers exactly 2e. Committed messages a client never reads (after it has
  seen its own e values) stay unreceived; a run ends complete, not blocked.
- D7 Each client is observed through its own server only; whether the leader and the follower
  deliver a given transaction before or after each other is not constrained (independent sends).
- D8 The `Setup` struct and tags are internal choices (INTERFACE.md fixes only the Setup shape).

## 5. Build and run

Compile (in this directory): `cargo build --release -j 2` -> `Finished \`release\` profile [optimized] target(s) in 1m 11s`, no warnings shown.

Run once: `timeout 600 target/release/bench_zookeeper_derived count spec --model fifo -- 1 0`
-> result line `execs=2 blocked=0` (the two orders T = <1,2> and <2,1>); exit 0. The last line
printed is the measurement block's `cache_hits: ` (empty value).
