# fencing(K) — epoch-fenced front ends (`fencing-grove`)

**In the paper:** not in Tables 2 and 3 (reported in the appendix tables and in EVAL.pdf here only).

**Communication model:** fifo (`p2p`: per-channel order; urpc loss, timeouts and erpc's duplicate suppression are not
modelled), the default of both the Rust program (`--model` left out) and the Java twin; the campaign ran fifo only.
**Languages:** Rust (TraceForge's Must API), in `rust/`; Java twin in `java/` (`bench.fencing_grove.Fencing`);
context-free Java port in `java-cf/` (`bench.fencing_grove.FencingGrove`). **Visible threads:** `c0`, `c1` (the
clients); the specification's `server` and the implementation's `config`, `ctr` (the counter server), `fe1`, `fe2`
(the front ends) and `p0`, `p1` (the client clerks) are invisible. **Parameters:** `1 ≤ K ≤ 127` = the number of
`FetchAndIncrement(0)` calls per client (default 1; the Rust program's counter is a byte, and the `2K` values below
fit only for `2K ≤ 255`); fixed: 1 key (key `0`, one counter server), 2 front ends, 2 clients. ARGS
`<K> [fenced|stale-frontend]`, variant `fenced` (default) or `stale-frontend`; the specification ignores the
variant. **Source:** Grove (Sharma et al., SOSP 2023) / gokv, `fencing/` (the config server, the front ends, the
counter server and `client/client.go`).

Notation: as in `../../synthetic/naive/README.md`; `while true` is a server loop that ends blocked when nothing more
arrives (the statuses of `config`, `ctr`, `fe1`, `fe2` are invisible, so every implementation execution is counted
as blocked; `fe2` also ends blocked in its `Acquire` when the heartbeat never expires). Messages are `Fai(c, 0)`,
`Val(v)` (the client legs); `CfgGet(p)`, `Current(f)`, `Acquire(f)`, `Epoch(e)`, `FeFai(p, 0)`, `FeVal(v)`, `FeErr`,
`CtrGet(f, e)`, `GetRep(s, v)`, `CtrPut(f, w, e)`, `PutRep(s)` with `s ∈ {stale, ok}`. A receive written
`M(…) := recv^fifo()` takes a message of kind `M` (the Rust program's tagged receives), other kinds stay pending;
`config` takes `Acquire` and `CfgGet`, `ctr` takes `CtrGet` and `CtrPut`, and a clerk takes `FeVal` and `FeErr`, in
any order. The clients are the same on both sides except for the thread they send to (`F0`, `F1`), which the
comparison does not see:

```
Clients(K, F0, F1) ≜ ‖_{i<2} c_i: for j := 1..K:  send^fifo(F_i, Fai(c_i, 0));  m := recv^fifo()    (m = Val(v))
```

Each client calls `FetchAndIncrement(0)` `K` times, waiting for each value.

The specification (both pairs), one counter:

```
Spec(K) ≜ Clients(K, server, server)
        ‖ server: v := 0
                  for n := 1..2K:  Fai(c, 0) := recv^fifo();  send^fifo(c, Val(v));  v := v + 1
```

`server` holds the counter of key `0` and answers each call atomically: the `2K` values the clients receive are
`0, …, 2K−1`, each once, in the server's receive order. Graphs `(2K)!/(K!·K!)` (2, 6, 20, 70 at `K = 1..4`).

The implementation, `gokv/fencing/` with one key and two front ends:

```
Impl_v(K) ≜ Clients(K, p0, p1) ‖ config ‖ ctr ‖ fe1 ‖ fe2 ‖ p0 ‖ p1

config ≜ epoch := 0;  active := false;  cur := ⊥;  waiting := [];  getters := []
         while true:
            grant := ⊥
            m := recv^fifo()
            if m = Acquire(f):  if active then waiting := waiting · f  else grant := f
            if m = CfgGet(p):   if cur ≠ ⊥ then send^fifo(p, Current(cur))  else getters := getters · p
            if grant = ⊥ and active and waiting ≠ [] and nondet({true, false}):     (the holder's heartbeat expires)
               active := false;  grant := head(waiting);  waiting := tail(waiting)
            if grant = f ≠ ⊥:
               active := true;  cur := f;  epoch := epoch + 1;  send^fifo(f, Epoch(epoch))
               for p ∈ getters:  send^fifo(p, Current(f));   getters := []

ctr ≜ v := 0;  last := 0
      while true:
         m := recv^fifo()
         if m = CtrGet(f, e):     if e < last then send^fifo(f, GetRep(stale, 0))  else  last := e;  send^fifo(f, GetRep(ok, v))
         if m = CtrPut(f, w, e):  if e < last then send^fifo(f, PutRep(stale))     else  last := e;  v := w;  send^fifo(f, PutRep(ok))

fe_i (i = 1, 2) ≜ send^fifo(config, Acquire(fe_i));  Epoch(e) := recv^fifo();  exited := false
                  while true:
                     FeFai(p, 0) := recv^fifo()
                     if exited:  send^fifo(p, FeErr);  continue
                     send^fifo(ctr, CtrGet(fe_i, e));  GetRep(s, val) := recv^fifo()
                     if s = stale and ⟨fenced⟩:  exited := true;  send^fifo(p, FeErr);  continue
                     send^fifo(ctr, CtrPut(fe_i, val + 1, e));  PutRep(s) := recv^fifo()
                     if s = stale and ⟨fenced⟩:  exited := true;  send^fifo(p, FeErr);  continue
                     send^fifo(p, FeVal(val))

p_i (i = 0, 1) ≜ send^fifo(config, CfgGet(p_i));  Current(fe) := recv^fifo()                       (MakeClerk)
                 for j := 1..K:
                    Fai(c, 0) := recv^fifo()
                    for attempt := 1..2:
                       send^fifo(fe, FeFai(p_i, 0));  m := recv^fifo()
                       if m = FeVal(v):  send^fifo(c, Val(v));  break
                       if m = FeErr:     send^fifo(config, CfgGet(p_i));  Current(fe) := recv^fifo()
                    assert answered
```

`config` is the config server: an `Acquire` (`AcquireEpoch`) is granted at once when no holder is active and queued
otherwise; a `CfgGet` (`Get`) is answered with the current front end, or queued before the first grant. After each
receive, while a holder is active and a front end waits, `nondet` decides whether the holder's heartbeat has expired
now (the real-time `HeartbeatListener`), in which case the first waiting front end is granted. A grant makes the
front end current, increments the epoch, sends it, and answers the queued `CfgGet`s. `ctr` is the counter server of
key `0`: a `Get` or `Put` with `epoch < last` is answered `stale` (`EStale`); otherwise `last := epoch` and the
counter is read or written. `fe_i` acquires an epoch, then serves each `FeFai` (`RPC_FAI`) by `ctr.Get(e)` and
`ctr.Put(val + 1, e)` and answers `val`; what it does on a `stale` reply is the variant's hole `⟨fenced⟩`. `p_i` is
`c_i`'s clerk (`client.go`): it fetches the current front end once, and per call tries that front end, forwarding
`FeVal`; on `FeErr` it fetches the current front end again and retries once (a second failure does not occur, see
`fenced`).

## The pairs

### fenced — expected: **conforms**

```
⟨fenced⟩ ≜ true        (a front end that sees EStale exits: "ctr: get() stale epoch number", primitive.Exit(1))
```

`config` grants epoch `1` to the first `Acquire` it reads and epoch `2` to the other only after the expiry coin, so
the front ends hold distinct epochs and the current front end is the last granted. `ctr` orders every `CtrGet` and
`CtrPut`; once the epoch-2 front end has touched `ctr`, every operation of the epoch-1 front end is `stale` and
changes nothing. So the `Get`–`Put` pairs that both pass do not interleave at `ctr` (an epoch-2 operation between an
epoch-1 pair makes its `Put` stale; an epoch-1 operation between an epoch-2 pair is stale), and the passing calls
read `0, 1, 2, …` in `ctr` order, each value once. A front end whose `Get` or `Put` is stale exits without
answering and sends `FeErr`; its clerk then asks `config`, whose current front end is the epoch-2 one (the grant
preceded that front end's first `ctr` operation, which preceded the stale reply), and the retry there passes, since
epoch `2` is never stale. Hence every client receives one value per call, in program order. Placing the
specification `server`'s `n`-th receive at the `n`-th passing `Get` (after the client's send of `Fai`, before its
receive of `Val`) gives the specification graph with the same values in the same per-client order. Both clients end
done on both sides; the blocked loops are invisible.

### stale-frontend — expected: **violation** (`K ≥ 1`)

```
⟨fenced⟩ ≜ false        ◀ changed: a stale reply is ignored and the front end keeps serving with its old epoch
                                   (a stale Get yields val = 0, Go's zero GetReply)
```

Schedule at `K = 1`: `config` grants `fe1` epoch `1` and answers `p0`'s `CfgGet` with `fe1`; `fe2`'s `Acquire`
waits; the heartbeat expires, `fe2` gets epoch `2`, and `p1`'s `CfgGet` is answered with `fe2`. `fe1` serves `c0`'s
call: `CtrGet(fe1, 1)` reads `0` (`last := 1`). `fe2` serves `c1`'s call: `CtrGet(fe2, 2)` reads `0` (`last := 2`),
`CtrPut(fe2, 1, 2)` passes, and `p1` forwards `Val(0)` to `c1`. `fe1`'s `CtrPut(fe1, 1, 1)` is `stale`; `fe1`
ignores it and answers `FeVal(0)`, which `p0` forwards to `c0`. The uncovered behaviour is

```
c0: S(Fai(c0, 0)) R(Val(0))
c1: S(Fai(c1, 0)) R(Val(0))
```

In every specification graph the two clients' first values are `0` and `1`, one each (signature value, the joint
observation of `c0` and `c1`). The same schedule on the first call of each client exists at every `K ≥ 1`.

## What it is for

Grove's fencing example against a one-thread counter: a config server hands out epochs, two front ends serve the
clients under their epoch, and the counter server rejects stale epochs, so a front end that lost its epoch fails its
caller instead of returning a value the new front end also returns. The fault removes the front end's reaction to
`EStale`, and the violation is a value received twice, visible only jointly in the two clients. Every implementation
graph is blocked in the invisible server loops (`fenced`: 164, 660, 2540 and `stale-frontend`: 176, 1152, 12128 at
`K = 1, 2, 3`, against the specification's 2, 6, 20).
