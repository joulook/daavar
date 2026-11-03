# abd(n) — ABD register (`abd-register-civl`)

**In the paper:** not in Tables 2 and 3 (reported in the appendix tables and in EVAL.pdf here only).

**Communication model:** bag (`asyn`: the source's unordered network), the default the Rust program declares, the Java
twin's default, and the model of every campaign row. **Languages:** Rust (TraceForge's Must API), in `rust/`; Java twin
in `java/` (`bench.abd_register_civl.Spec`, args `<n>,<model>`, and `bench.abd_register_civl.Impl`, args
`<n>,<model>,<variant>`); context-free Java port in `java-cf/` (`bench.abd_register_civl.AbdRegisterCivl`, args
`<spec|impl>,<n>,<fifo|bag>[,<variant>]`). **Visible threads:** `w` (the writer), `r1`, `r2` (the readers); the
specification's `register` and the implementation's replicas `rep0 … rep(n−1)` and proxies `wp`, `rp1`, `rp2` are
invisible. **Parameters:** `n ≥ 2` = the number of replicas (default 3; majority `maj = ⌊n/2⌋ + 1`); variant
`correct` or `read-no-writeback`; fixed: one writer writing `1` once, two readers reading once each, initial value `0`
with timestamp `(0, 0)`, the writer's pid `1`. ARGS: `-- <n> [correct|read-no-writeback]` (`n` defaults to 3 and the
variant to `correct`; any other variant name is refused; `spec` ignores the variant).
**Source:** CIVL (Boogie) `samples/ABD.bpl` (Attiya, Bar-Noy, Dolev, JACM 1995), a repository sample under
`related-work-papers/sources/boogie/Test/civl/samples/`.

Notation: as in `../../synthetic/naive/README.md`. A receive written with a pattern (`QueryReply(ts, v) := recv^asyn()`)
takes a message of that type: the Rust program has one channel per receiver and message type, the Java programs use
type guards. Messages: `Write(v)`, `Read(i)` (client → register or proxy), `WriteAck`, `ReadReply(v)` (→ client),
`Done` (`r1` → `r2`), `Query(p)`, `Update(p, ts, v)` (proxy `p` → replica), `QueryReply(ts, v)`, `UpdateAck` (replica →
proxy `p`); a timestamp `ts = (t, pid)` is ordered lexicographically (the source's `lt`). The clients are the same on
both sides except for the front end they talk to (`F0`, `F1`, `F2` below), which the comparison does not see:

```
Clients(F0, F1, F2) ≜ w:  send^asyn(F0, Write(1));  WriteAck := recv^asyn()
                    ‖ r1: send^asyn(F1, Read(1));   ReadReply(v1) := recv^asyn();  send^asyn(r2, Done)
                    ‖ r2: Done := recv^asyn();  send^asyn(F2, Read(2));  ReadReply(v2) := recv^asyn()
```

`w` writes `1` once; `r1` reads, then tells `r2`; `r2` reads after that. `Done` puts `r1`'s read before `r2`'s in
happens-before (the real-time order a new-old inversion needs). The visible values are `v1` and `v2`.

The specification (both pairs), one atomic register:

```
Spec(n) ≜ Clients(register, register, register)
        ‖ register: value := 0
                    for 3 times:  m := recv^asyn()
                                  if m = Write(v):  value := v;  send^asyn(w, WriteAck)
                                  if m = Read(i):   send^asyn(r_i, ReadReply(value))
```

`register` serves the three operations one at a time in some order consistent with `Done` (`w` first, second or
third): 3 graphs for every `n`, with `(v1, v2) = (1, 1)`, `(0, 1)`, `(0, 0)`. Every specification execution ends with
all three clients done.

The implementation, the source's `WriteClient` / `ReadClient` run by one proxy per client, and `Query#0` / `Update#0`
as request/reply messages at the replicas:

```
Impl_v(n) ≜ Clients(wp, rp1, rp2) ‖ wp ‖ rp1 ‖ rp2 ‖ ‖_{i<n} rep_i            (maj = ⌊n/2⌋ + 1)

rep_i ≜ store := ((0, 0), 0)
        for ⟨requests⟩ times:  m := recv^asyn()
           if m = Query(p):          send^asyn(proxy_p, QueryReply(store))
           if m = Update(p, ts, v):  if store.ts < ts then store := (ts, v)
                                     send^asyn(proxy_p, UpdateAck)

proxy_p (p = 0: wp for w, 1: rp1 for r1, 2: rp2 for r2) ≜
        m := recv^asyn()
        if m = Write(v):  (ts, _) := QueryPhase();  UpdatePhase((ts.t + 1, 1), v);  send^asyn(w, WriteAck)
        if m = Read(i):   (ts, v) := QueryPhase();  ⟨write-back⟩;                   send^asyn(r_i, ReadReply(v))

QueryPhase()        ≜ for i := 0..n−1:  send^asyn(rep_i, Query(p))
                      best := ((0, 0), 0)
                      for maj times:  QueryReply(ts, v) := recv^asyn();  if ts > best.ts then best := (ts, v)
                      return best
UpdatePhase(ts, v)  ≜ for i := 0..n−1:  send^asyn(rep_i, Update(p, ts, v))
                      for maj times:  UpdateAck := recv^asyn()
```

A proxy receives its client's operation, queries every replica and keeps the largest-timestamped value among the
first `maj` replies (the `n − maj` others are never read); the writer then sends `((max.t + 1, 1), 1)` to every replica
and waits for `maj` acks, a reader writes back what it read (`correct`) or does not (`read-no-writeback`); the proxy
then answers its client. A replica serves every request sent to it (`⟨requests⟩` = 3 queries + 3 updates = 6 in
`correct`, 3 + 1 = 4 in `read-no-writeback`) and ends, so no implementation execution blocks. The quorum of a phase is
the set of replicas whose replies the proxy read.

## The pairs

### correct — expected: **conforms**

```
⟨write-back⟩ ≜ UpdatePhase(ts, v)        ⟨requests⟩ ≜ 6
```

Any two phases' quorums (`maj` of `n` each) share a replica, which serves both requests in its program order; if it
serves `a`'s request first then `inv(a) → resp(b)` in happens-before (its reply to `b` is one that `b`'s proxy used).
Case by case on `(v1, v2)`:

- `(1, 1)`: the value `1` flows from `w`'s update into both reads, so `inv(w) → resp(r1)` and `inv(w) → resp(r2)`:
  covered by the specification graph serving `w r1 r2`.
- `(0, 1)`: every reply of `r1`'s query quorum carried `(0, 0)`; that quorum meets `w`'s update quorum at a replica
  that answered `r1`'s query before storing `w`'s update, so `inv(r1) → resp(w)`; and `inv(w) → resp(r2)` as above:
  covered by `r1 w r2`.
- `(0, 0)`: the same argument for both readers: covered by `r1 r2 w`.
- `(1, 0)`: impossible. `r1`'s write-back stored `((1, 1), 1)` at `maj` replicas before `r1` returned; `r2`'s query
  quorum meets them at a replica that serves `r2`'s query after that update (`Done` puts `r2`'s query after `r1`'s
  return), and a stored timestamp only grows, so `r2` reads `1`.

In each covered case the specification graph's order (`inv(o_i) → resp(o_j)` for every `i ≤ j` of its serving order,
plus program order and `Done`) is implied by the implementation's happens-before, the values agree, and all three
clients end done on both sides.

### read-no-writeback — expected: **violation** (`n ≥ 3`; conforms at `n = 2`, where the fault is masked)

```
⟨write-back⟩ ≜ skip        ◀ changed: the readers skip UpdatePhase        ⟨requests⟩ ≜ 4
```

The new-old inversion. Schedule at `n = 3` (`maj = 2`): `wp`'s `Update((1, 1), 1)` has been served by `rep0` only;
`rp1` reads the replies of `rep0` and `rep1`, so `r1` reads `1`, returns and sends `Done`; `rp2` reads the replies of
`rep1` and `rep2`, which have not yet served the update, so `r2` reads `0`; the update reaches `rep1` and `rep2`
afterwards and `w`'s two acks come from any two replicas. The uncovered behaviour is

```
w:  S(Write(1)) R(WriteAck)
r1: S(Read(1)) R(ReadReply(1)) S(Done)
r2: R(Done) S(Read(2)) R(ReadReply(0))
```

No specification graph has `v1 = 1` and `v2 = 0`: `v1 = 1` needs `w` served before `r1`, and `Done` puts `r2` after
`r1`. The difference is `r2`'s value. The schedule exists for every `n ≥ 3`: the update served by one replica, `r1`'s
quorum containing it, `r2`'s quorum of `maj` replicas among the other `n − 1` (possible since `maj ≤ n − 1`). At
`n = 2` (`maj = 2`) every query reads both replicas: if `r1` read `1`, some replica answered it with `(1, 1)` and serves
`r2`'s query later (`Done`) with a timestamp at least as large, so `r2` reads `1` too; every graph is then covered as in
`correct`, the `(1, 0)` case being excluded by this argument instead of by the write-back.

## What it is for

The classic quorum register against one atomic register: the specification has 3 graphs at every `n`, the
implementation thousands (12480 at `n = 2` for `correct`), all covered. The fault removes the readers' write-back, the
step ABD's atomicity rests on; its witness is a value (`r2` reads the old `0` after `r1` read `1`) that only two
query quorums avoiding each other can produce, so it is masked at `n = 2` and appears at `n = 3`.
