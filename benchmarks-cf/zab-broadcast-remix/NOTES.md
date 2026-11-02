# zab-broadcast-remix: context-free Java twin

CLASS: `bench.zab_broadcast_remix.ZabBroadcastRemix` (file `java/bench/zab_broadcast_remix/ZabBroadcastRemix.java`).

Argument string: `<spec|impl>,<Q>,<fifo|bag>[,<variant>]`, variant `correct` (default) or
`issue1-propose-to-ackld`. `fifo` -> `CommunicationModel.P2P`, `bag` -> `ASYN`. As in Rust, the
variant is parsed (and an unknown one refused) on the spec side too, and ignored there.

## Mapping

Threads (role names = Rust thread names; visible roles `c0`, `c1`; main invisible, spawns all, awaits all):
- spec: spawn order `server`, `c0`, `c1` (as Rust). `server` reads 2Q messages with a blocking
  receive, expects `Write`, answers `Committed(n)` to client `c`. Terminates after 2Q.
- impl: spawn order `s0` (leader), `s1` (synced follower), `s2` (late follower, serves c1), `c0`
  (writes to s0), `c1` (writes to s2). Leader and followers loop forever on a blocking receive, as in
  Rust (so every impl execution ends blocked, both sides).
- client c: Q times `send(Write(c, 10c+j+1))` then one blocking receive of any message, failing
  unless it is `Committed`.
- Leader/follower logic transcribed line by line (history with ack bitmask, lastCommitted,
  ackeRecv/ackldRecv bitmasks, `issue1` switches PROPOSE to ackldRecv; follower log, committed
  index, synced flag, waiting writes forwarded on COMMITLD).

Channels: each Rust thread has exactly one inbox channel and only ever `recv_msg_block()`s on it
with no guard, so each maps to the Java process's own mailbox: `Channel.recv(model)` (blocking,
unguarded), then a type switch; an unexpected type throws (Rust `panic!`). Sends are
`Channel.send(model, tid, msg)` to the inbox's owner. No non-blocking receives, no Nondet (the Rust
program has no `nondet` choice).

Messages: `sealed interface Msg extends MpMessage`, one record per variant, `@Tag` = Rust variant
name, fields in Rust order, `u8` -> `int`:
`Write(c,v)`, `Committed(zxid)`, `AckEpoch()`, `NewLeader(List<Entry> hist)`, `AckLd(zxid)`,
`CommitLd(zxid)`, `Propose(zxid,c,v)`, `Ack(zxid,from)`, `Commit(zxid)`.
`Entry(u8,u8,u8)` (tuple struct) -> `@Tag("Entry") record Entry(int zxid, int c, int v)`.

MpProgram: `programName()` = `zab-broadcast-remix`; `size()` = `"<Q>"`;
`rolesInSpawnOrder()` = spec `[server, c0, c1]`, impl `[s0, s1, s2, c0, c1]`; `visibleRoles()` = `[c0, c1]`.

## Doubts / assumptions

1. Addressing. Rust creates all channels before spawning; in Java a process's address (Tid) exists
   only after its spawn, and a process can run (e.g. s2 sends AckEpoch, s0 answers to s2) before
   main has stored later Tids. I precompute the Tids from the rule in `MpProgram`'s Javadoc
   ("the k-th process main spawns is task k + 1"), i.e. first spawned = raw task id 2, and main
   checks every handle's Tid against it after spawning (throws if the numbering differs). It held
   in all runs. This relies on JMC's task numbering, which `MpMessage`'s contract discourages
   ("never a raw long"); no API-level alternative without main doing message passing.
2. Tag of the tuple struct `Entry`: assumed `"Entry"` with fields in tuple order; I do not know how
   the Rust side's encoder names/tags a tuple struct (could be an untagged array).
3. `programName()`/`size()` strings: assumed `zab-broadcast-remix` and just `Q` (variant not part of
   size). Unknown what the Rust header uses.
4. Integers: Rust `u8` (with `as u8` wrap); Java `int`. Q is limited so that `10c+j+1 <= 255`,
   no wrap occurs.
5. The Rust usage comment is `-- <Q> [variant]` with Q defaulting to 1 when absent; my launcher
   string requires Q.
6. Panics (unexpected message) are `IllegalStateException`; none fire in the runs below.

## Measured (must, plain enumeration, Q=1)

| args | complete | blocked |
|---|---|---|
| spec,1,fifo | 2 | 0 |
| impl,1,fifo | 0 | 161 |
| impl,1,fifo,issue1-propose-to-ackld | 0 | 128 |
| spec,1,bag | 2 | 0 |
| impl,1,bag | 0 | 23601 |

All ended "stack exhausted". Impl is all-blocked because servers loop forever (same as Rust).
