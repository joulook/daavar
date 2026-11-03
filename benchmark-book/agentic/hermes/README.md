# hermes(N, K) — the Hermes-replicated key, implemented from the paper (`hermes`)

**Communication model:** bag (`asyn`), the model both ports declare (the Rust crate's default `Bag`; the Java port's
`bag` = JMC's `ASYN`). **Languages:** Rust port in `rust/` (crate `bench_hermes`, entry `imp`, TraceForge's Must
API); Java port in `java/` (`bench.hermes.Hermes`, JMC's message-passing API); the specification in
`rust/src/spec.rs` (entry `spec` of the same crate, hand-written by the coordinator, rev 2). **Visible threads:** the
clients `c0 … c{K−1}`; the replicas `r0 … r{N−1}` are invisible (in the specification: the register `reg` and one stub
`s0 … s{K−1}` per client). **Parameters:** `N` = the number of replicas, `K` = the number of clients; one integer key,
initially 0; reads and writes only (no RMW), no faults, so no leases, replays or recovery. Sizes: `(N, K)` = `(3, 2)`,
`(3, 3)`.
**Paper:** Antonios Katsarakis, Vasilis Gavrielatos, M. R. Siavash Katebzadeh, Arpit Joshi, Aleksandar Dragojevic,
Boris Grot, Vijay Nagarajan, *Hermes: a Fast, Fault-Tolerant and Linearizable Replication Protocol*, ASPLOS 2020,
§3.1–3.2 (`PAPER-EXCERPT.md`).
**Agents:** two context-free Opus 5.5 agents, one per language, each given the paper excerpt, the program interface
(`INTERFACE.md`) and the API sources only, under the prompt `PROMPT.md`; neither was told what the program is for.
Their own records of the choices the paper leaves open: `NOTES-rust.md`, `NOTES-java.md`.

Notation: as in `../../synthetic/naive/README.md`; `recv^M(P)` receives only a message matching the pattern `P`
(`_` = any field), `recv^M(from = t)` only a message sent by `t`. Messages fixed by the interface: `Write(f, v)` and
its reply `WriteAck`, `Read(f)` and its reply `ReadReply(x)`, where `f` is the client's index. A timestamp is
`t = (ver, cid)`, ordered on `ver` first. Addresses are not messages (the Rust port predicts thread ids from the spawn
order `r*`, `c*` and asserts them; the Java port mints them before starting any process).

**Interface defect, corrected before any check ran.** The first `INTERFACE.md` gave the client requests without a
sender, which neither API's receive reports; the addendum (18:10, recorded in `PROMPT.md` and `INTERFACE.md`) added
`from`, the client's index. A harness-interface issue, not a protocol divergence.

The clients are common to both ports:

```
Clients(N, K) ≜ c_i: send^asyn(r_{i mod N}, Write(i, i + 1));    a := recv^asyn()            -- a = WriteAck
                     send^asyn(r_{(i+1) mod N}, Read(i));       x := recv^asyn()            -- x = ReadReply(_)
```

Client `ci` writes `i + 1` at one replica, then reads at the next one.

The specification (rev 2; both pairs that have it):

```
Spec(N, K) ≜ c_i: send^asyn(reg, Write(i, i + 1));  a := recv^asyn()
                  send^asyn(s_i, Read(i));          x := recv^asyn()                       (i = 0 … K−1)
           ‖ reg: for i := 0..K−1:  send^asyn(s_i, Register(c_i))
                  for 1..K:
                     Write(f, v) := recv^asyn()
                     send^asyn(c_f, WriteAck);  send^asyn(s_f, Acked(v))
                     for k := 0..K−1:  send^asyn(s_k, Snap(v))
           ‖ s_i: c := ⊥;  own := ⊥;  x := 0;  seen := []
                  repeat:
                     m := recv^asyn(from = reg)
                     if m = Register(c'):  c := c'
                     if m = Acked(v):      own := v
                     if m = Snap(v):       x := v;  seen := seen · v
                  until c ≠ ⊥ ∧ own ≠ ⊥ ∧ own ∈ seen
                  Read(_) := recv^asyn(from = c)
                  while nondet({true, false}):
                     m := recv^{asyn,nb}(from = reg)
                     if m = Snap(v):  x := v  else break
                  send^asyn(c, ReadReply(x))
```

`reg` linearises the writes in arrival order: it acknowledges each, tells the writer's stub the written value, and
streams the value to every stub. Stub `si` serves its client's read from a prefix of that stream: it has consumed at
least through its own client's write (Hermes guarantees this: a write completes only after every replica was
invalidated), chooses with `nondet` how many further values to take, and replies with the last one. `reg` spawns the
stubs and the clients; spawn edges are invisible. `N` is unused.

**Why rev 2.** The first specification was the sequential register of chain replication and CASPaxos (one thread
serving writes and reads in arrival order). It over-orders Hermes: a read served by the register thread puts every
later write's acknowledgement causally after the read, while Hermes serves a read locally, with no message, so no
later write depends on it. Half of the implementation graphs were uncovered (2 310 of 4 652 at `(3, 2)`) although the
port is linearizable; the stubs remove the false order (the design note, "Specification lessons"). Against rev 2 the
Rust port conforms at `(3, 2)` (7 200 specification graphs, stateless and stateful, `EVAL.tex`).

## The pairs

### Rust implementation against the specification — expected: **conforms**

```
Rust(N, K) ≜ Clients(N, K)
           ‖ r_j: st := Valid;  ts := (0, 0);  x := 0;  pend := ⊥;  stalled := []
                  for 1..E_j:                       -- E_j = W_j + R_j + 2(K − W_j) + (N − 1)·W_j
                     m := recv^asyn()
                     if m = Write(f, v):  write(f, v)
                     if m = Read(f):      read(f)
                     if m = Inv(t, v):                                                  -- FINV, FACK
                        if t > ts:  st := (st ∈ {Write, Trans} ? Trans : Invalid);  ts := t;  x := v
                        send^asyn(r_{t.cid}, Ack(t))
                     if m = Ack(t):  if pend ≠ ⊥ ∧ pend.ts = t:  pend.acks := pend.acks + 1
                                     complete()                                         -- CACK
                     if m = Val(t):  if t = ts:  st := Valid;  retry()                  -- FVAL

             read(f)     ≜ if st = Valid:  send^asyn(c_f, ReadReply(x))  else:  stalled := stalled · Read(f)
             write(f, v) ≜ if st ≠ Valid ∨ pend ≠ ⊥:  stalled := stalled · Write(f, v);  return
                           ts := (ts.ver + 1, j);  x := v;  st := Write;  pend := (f, ts, 0)       -- CTS, CINV
                           for k ≠ j:  send^asyn(r_k, Inv(ts, v))
                           complete()
             complete()  ≜ if pend ≠ ⊥ ∧ pend.acks = N − 1:
                              st := (Write ↦ Valid, Trans ↦ Invalid, otherwise unchanged)(st)
                              for k ≠ j:  send^asyn(r_k, Val(pend.ts))                              -- CVAL
                              send^asyn(c_{pend.f}, WriteAck);  pend := ⊥;  retry()
             retry()     ≜ if st = Valid:  for q in take(stalled), in order:  read(q) or write(q)
```

`W_j` and `R_j` are the numbers of clients writing and reading at `r_j`. A replica serves a read locally iff its key is
Valid, otherwise stalls it; it coordinates a write only when Valid and with no write of its own in flight (NOTES-rust
4): new timestamp, local apply, `Inv` to every other replica, and on the last `Ack` it validates, sends `Val` to every
follower and then acknowledges the client (NOTES-rust 6). A follower adopts a higher `Inv` (Invalid, or Trans if it is
coordinating, NOTES-rust 2) and always acknowledges to the timestamp's coordinator (NOTES-rust 7); a `Val` with its
current timestamp validates the key. Stalled requests are retried in arrival order whenever the key becomes Valid
(NOTES-rust 5). Each replica stops after the number of messages it receives in every run (NOTES-rust 8). No replays,
so no `nondet` (NOTES-rust 1).

How the two sides differ: the specification has one write order and per-client read stubs; the implementation has
replicas with invalidations, acknowledgements and validations. Why it conforms: writes are ordered by their
timestamps, and a write's `WriteAck` comes only after every replica has been invalidated by it; a later read at any
replica returns that write's value or a later one, served without a message once the key is valid. That is the
stub's choice: a prefix of the write stream at least through the client's own write, with no causal order from the
read to later writes.

### Java implementation against the specification — expected: **conforms** (against rev 2)

```
Java(N, K) ≜ Clients(N, K)
           ‖ r_j: st := Valid;  ts := (0, 0);  x := 0;  pend := ⊥;  acks := ∅;  stalled := []
                  for 1..E_j:
                     m := recv^asyn()
                     if m = Write(f, v) ∨ m = Read(f):  stalled := stalled · m
                     if m = Inv(t, v, s):                                               -- FINV, FACK
                        if t > ts:  st := (pend ≠ ⊥ ? Trans : Invalid);  ts := t;  x := v
                        send^asyn(r_s, Ack(t, j))
                     if m = Ack(t, s):  if pend = t:  acks := acks ∪ {s};  if |acks| = N − 1:  complete()
                     if m = Val(t):     if t = ts:  st := Valid                         -- FVAL
                     while stalled ≠ [] ∧ st = Valid ∧ pend = ⊥:
                        q := pop(stalled)
                        if q = Write(f, v):  coordinate(f, v)
                        if q = Read(f):      send^asyn(c_f, ReadReply(x))
                  assert stalled = [] ∧ pend = ⊥ ∧ st = Valid

             coordinate(f, v) ≜ ts := (ts.ver + 1, j);  x := v;  st := Write;  pend := ts;  pf := f;  acks := ∅
                                for k ≠ j:  send^asyn(r_k, Inv(ts, v, j))
                                if N = 1:  complete()
             complete()       ≜ t := pend;  pend := ⊥;  st := (st = Trans ? Invalid : Valid)
                                send^asyn(c_pf, WriteAck)
                                for k ≠ j:  send^asyn(r_k, Val(t))
```

The same protocol with the Java agent's choices: every client request is queued and the queue is served while the key
is Valid and no own write is in flight, so a coordinator serves neither reads nor writes while its write collects
acknowledgements (NOTES-java D2); a follower goes to Trans whenever a write of its own is in flight (NOTES-java D3);
`Inv` and `Ack` carry the sender's index (D4); the client is acknowledged before the `Val`s are sent (D7).

**The recorded rows.** The offline join and the online check of this pair were run against the export of the rev-1
specification (6 graphs at `(3, 2)`, 90 at `(3, 3)`), and the rule file accordingly expects "violation" for them; they
report the over-ordering above (online at `(3, 2)`: 2 310 of 4 652 graphs uncovered, `EVAL.tex`). The same holds for
the Rust port's offline row. Against rev 2 the expected verdict is "conforms"; that pair has not been rerun.

### The two implementations against each other — expected: **conforms**

Both directions. The visible threads and the client messages are the same; the differences, from the code and the two
notes:

1. **A read at a coordinator whose write is still in flight.** Rust serves a read whenever the key is Valid; Java also
   waits until its own write has all acknowledgements (NOTES-rust 3–4, NOTES-java D2–D3). The two differ only when a
   coordinator's key is Valid while its write is pending: it was overtaken by a higher write and validated by that
   write's `Val` before its own last `Ack`. At `(3, 2)` only `r0` can be overtaken (by `r1`'s write with timestamp
   `(1, 1)`), and `r0` serves no read (the clients read at `r1` and `r2`), so the difference cannot show; at `(3, 3)`
   `r0` serves `c2`'s read, and Java delays that reply until `r0`'s write completes. Neither note declares a visible
   effect, and the twin pairs have finished only at `(3, 2)`.
2. **`Inv` while Valid with a write pending.** Rust goes to Invalid, Java to Trans; both end Invalid when the own write
   completes, so the states agree from then on.
3. **Order at completion.** Rust sends the `Val`s and then the `WriteAck`; Java the `WriteAck` and then the `Val`s
   (NOTES-rust 6, NOTES-java D7). Both sends are on the invisible coordinator, and no visible event depends on their
   order.
4. **Counting acknowledgements and addressing them.** Rust counts `Ack`s and sends each to `t.cid`; Java collects a set
   of senders and replies to `Inv`'s `from`. Without faults or replays these coincide.
5. **Serving stalled requests.** Rust retries the queue when the key becomes Valid (after the last `Ack` or a `Val`),
   Java after every message; with difference 1 set aside, the same requests are served at the same points.

## What it is for

The protocol that taught the specification lesson: a sequential register is the wrong specification for a protocol
whose reads send no message, because it adds causal order the implementation does not have. Both ports have 4 652
graphs at `(3, 2)` and agree with each other there (offline both directions and online, `EVAL.tex`); the rev-2
specification has 7 200 graphs at that size, more than the implementation, since its stubs read every prefix of the
write stream. At `(3, 3)` the offline join of the Rust port against the rev-1 export saw 90 010 608 implementation
graphs, and the online twin check reached the 4 h limit (a superseded row, rerun pending).
