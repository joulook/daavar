# zab(Q) — Zab broadcast phase with a late follower (`zab-broadcast-remix`)

**Communication model:** fifo (`p2p`: `Zab.tla`'s `msgs[i][j]` are FIFO queues). **Languages:** Rust (TraceForge's
Must API), in `rust/`; Java twin in `java/` (`bench.zab_broadcast_remix.ZabBroadcast`); context-free Java port in
`java-cf/` (`ZabBroadcastRemix`). **Visible threads:** `c0`, `c1`; the specification's `server` and the
implementation's `s0` (leader), `s1` (synchronised follower), `s2` (late follower) are invisible.
**Parameters:** `1 ≤ Q ≤ 127` = the number of writes per client (default 1; the Rust program's zxids are bytes, so
the `2Q` zxids below fit only for `2Q ≤ 255`, beyond which the pseudocode and the Java twin's `int` zxid no longer
describe it); fixed: 2 clients, 3 servers, quorum 2, one
epoch (a zxid is its counter).
**Source:** the Zab protocol specification vendored in the Remix artifact (EuroSys 2025),
`zookeeper-specifications/protocol-spec/Zab.tla` and its `issues.md`.

Notation: as in `../../synthetic/naive/README.md`; `while true` is a server loop that ends blocked when nothing more
arrives (the servers' status is invisible, so every implementation execution is counted as blocked). Messages are
`Write(c, v)`, `Committed(z)`, `AckEpoch`, `NewLeader(h)`, `AckLd(z)`, `CommitLd(z)`, `Propose(z, c, v)`,
`Ack(z, f)`, `Commit(z)`; an entry of a history is `(z, c, v)`. The clients are the same on both sides except for the
server they write to (`W0`, `W1` below), which the comparison does not see:

```
Clients(Q, W0, W1) ≜ c0: for j := 0..Q−1:  send^fifo(W0, Write(0, j + 1));   m := recv^fifo()    (m = Committed(z))
                   ‖ c1: for j := 0..Q−1:  send^fifo(W1, Write(1, 11 + j));  m := recv^fifo()    (m = Committed(z))
```

Each client writes `Q` values (`v = 10c + j + 1`) one at a time and waits for its write's zxid.

The specification (both pairs), one sequencer:

```
Spec(Q) ≜ Clients(Q, server, server)
        ‖ server: for n := 1..2Q:  Write(c, v) := recv^fifo();  send^fifo(cc, Committed(n))      (cc = c0 or c1, the writer)
```

`server` reads the `2Q` writes of both clients in some order, gives the `n`-th the zxid `n`, and answers it. Every
specification execution ends with both clients done.

The implementation, the broadcast phase of `Zab.tla` with the one-shot join of `s2`:

```
Impl_v(Q) ≜ Clients(Q, s0, s2) ‖ s0 ‖ s1 ‖ s2

s0 ≜ history := [];  lc := 0;  ackeRecv := {s1};  ackldRecv := {s1}
     while true:
        m := recv^fifo()
        if m = Write(c, v):  z := |history| + 1;  history := history · (z, c, v) with acks {s0}
                             for f ∈ ⟨propose set⟩:  send^fifo(f, Propose(z, c, v))
        if m = Ack(z, f):    acks(z) := acks(z) ∪ {f}
                             if z = lc + 1 and |acks(z)| ≥ 2:
                                lc := z
                                for g ∈ ackldRecv:  send^fifo(g, Commit(z))
                                if client(z) = 0 then send^fifo(c0, Committed(z))
        if m = AckEpoch:     ackeRecv := ackeRecv ∪ {s2};  send^fifo(s2, NewLeader(history))
        if m = AckLd(z):     ackldRecv := ackldRecv ∪ {s2}
                             for every entry z' ≤ z:  acks(z') := acks(z') ∪ {s2}
                             send^fifo(s2, CommitLd(lc))

si (i = 1, 2) ≜ log := [];  k := 0;  synced := (i = 1);  waiting := []
                if i = 2 then send^fifo(s0, AckEpoch)
                while true:
                   m := recv^fifo()
                   if m = Write(c, v):     if synced then send^fifo(s0, m) else waiting := waiting · m
                   if m = NewLeader(h):    log := h;  send^fifo(s0, AckLd(last zxid of log, or 0))
                   if m = CommitLd(z):     while k < |log| and zxid(log[k]) ≤ z:  answer(log[k]);  k := k + 1
                                           synced := true
                                           for w ∈ waiting:  send^fifo(s0, w);   waiting := []
                   if m = Propose(z, c, v): if z = (last zxid of log, or 0) + 1 then
                                               log := log · (z, c, v);  send^fifo(s0, Ack(z, i))
                                           (else discard)
                   if m = Commit(z):       if k < |log| and zxid(log[k]) = z then  answer(log[k]);  k := k + 1

answer((z, c, v)) ≜ if i = 2 and c = 1 then send^fifo(c1, Committed(z))
```

`s0` is the leader, already in BROADCAST with `s1` synchronised. A write becomes the next transaction (request and
broadcast in one step) and is proposed; an acknowledgement commits the next transaction once a quorum (itself and
one follower) has it, the commit is sent to `ackldRecv`, and `c0` is answered for its own transactions. `s2`'s
`AckEpoch` is answered with the whole history and adds `s2` to `ackeRecv` (`LeaderProcessACKEPOCH`, case 1); its
`AckLd` adds `s2` to `ackldRecv` and is answered with `CommitLd(lastCommitted)` (`LeaderProcessACKLD`, case 2, the
fix of Issue 3). `s1` and `s2` append the next proposal and acknowledge it, discard any other, and commit the next
pending entry on a matching `Commit`. `s2` also joins late (`AckEpoch → NewLeader → AckLd → CommitLd`), holds `c1`'s
writes until it has processed `CommitLd` and then forwards them to the leader, and answers `c1` when it commits
`c1`'s transaction.

## The pairs

### correct — expected: **conforms**

```
⟨propose set⟩ ≜ ackeRecv
```

`s2` is in `ackeRecv` from the moment the leader sends it `NewLeader`, so every later transaction reaches it as a
`Propose` after the `NewLeader` (FIFO) and every earlier one is in the `NewLeader` history: its log is the leader's
history. A transaction committed before the leader processes `AckLd` is covered by `CommitLd(lc)`, a later one by a
`Commit` (which follows the `CommitLd` on the FIFO link). `c1`'s writes are forwarded only after `CommitLd`, so each
is proposed to and committed at `s2`, and `c1` is answered; `c0` is answered by the leader. The leader commits in
zxid order, so the answered zxids are `1..2Q` in the leader's order of writes, which is a `server` order of reads in
the specification, and each answer follows every write with a smaller zxid.

### issue1-propose-to-ackld — expected: **violation** (`Q ≥ 1`)

```
⟨propose set⟩ ≜ ackldRecv        ◀ fault (Issue 1 of issues.md): PROPOSE goes to ackldRecv, not ackeRecv
```

A follower that has received `NewLeader` but whose `AckLd` the leader has not processed is in neither the
`NewLeader` history nor the proposal set of a transaction created in between. Schedule at `Q = 1`: `s0` processes
`AckEpoch` (sends `NewLeader([])`), then `c0`'s `Write(0, 1)`: zxid 1, proposed to `ackldRecv = {s1}` only; then
`s2`'s `AckLd(0)` (`s2` joins `ackldRecv`, `CommitLd`); `s1`'s `Ack(1, 1)` commits zxid 1 and `c0` receives
`Committed(1)`. `s2`'s log lacks zxid 1 for ever. `s2` forwards `c1`'s `Write(1, 11)`, which becomes zxid 2; `s2`
discards `Propose(2, 1, 11)` (not the next zxid of its log) and ignores `Commit(2)`, so it never answers `c1`. The
uncovered behaviour is

```
c0: S(Write(0, 1)) R(Committed(1))
c1: S(Write(1, 11))                    with c1 blocked
```

In every specification execution both clients end done; the difference is in the status of `c1`.

## What it is for

A consensus-style broadcast with a real-world bug (one of the three issues of `issues.md`) against a one-thread
sequencer. The violation is a status (a client that never completes), not a wrong value, and every implementation
execution ends blocked in its invisible server loops, so the pair exercises the comparison of visible statuses
when invisible threads block.
