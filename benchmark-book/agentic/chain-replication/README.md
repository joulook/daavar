# chainrep(N, K) — chain replication, implemented from the paper (`chain-replication`)

**Communication model:** FIFO (`fifo`: per-link order), the model both ports declare (the Rust crate's default `FIFO`;
the Java port's `fifo` = JMC's `P2P`); the paper assumes "reliable FIFO links". **Languages:** Rust port in `rust/`
(crate `bench_chainrep`, entry `imp`, TraceForge's Must API); Java port in `java/` (`bench.chainrep.ChainRep`, JMC's
message-passing API); the specification in `rust/src/spec.rs` (entry `spec` of the same crate, hand-written by the
coordinator). **Visible threads:** the clients `c0 … c{K−1}`; the servers `s0` (head) … `s{N−1}` (tail) are invisible
(in the specification: the object `obj`). **Parameters:** `N` = the chain length, `K` = the number of clients; one
integer object, initially 0; no server fails (no master, no reconfiguration). Sizes: `(N, K)` = `(3, 2)`, `(3, 3)`.
**Paper:** Robbert van Renesse, Fred B. Schneider, *Chain Replication for Supporting High Throughput and
Availability*, OSDI 2004, §2–3 (`PAPER-EXCERPT.md`).
**Agents:** two context-free Opus 5.5 agents, one per language, each given the paper excerpt, the program interface
(`INTERFACE.md`) and the API sources only, under the prompt `PROMPT.md`; neither was told what the program is for.
Their own records of the choices the paper leaves open: `NOTES-rust.md`, `NOTES-java.md`.

Notation: as in `../../synthetic/naive/README.md`; `recv^M(P)` receives only a message matching the pattern `P`
(`_` = any field). Messages fixed by the interface: `Update(f, v)` (client `c_f` writes `v`) and its reply
`UpdateReply(x)` (the value right after the update at the tail), `Query(f)` and its reply `QueryReply(x)`. Addresses
are not messages: every process knows every other's from the start (shared tables in both ports; spawn order `s*`,
`c*`).

**Interface defect, corrected before any check ran.** The first `INTERFACE.md` gave `Update(value)` and `Query()`
without a sender, but neither API's receive reports a message's sender, so the tail could not address its reply. The
addendum (18:10, recorded in `PROMPT.md` and `INTERFACE.md`) added `from`, the client's index; the Java agent first
declared the fields as `(value, from)` and reordered them to the interface's `(from, value)` on the coordinator's
request (`NOTES-java.md`, Messages; the field order is the order of the compared encoding). A harness-interface issue,
not a protocol divergence.

The specification (both pairs that have it):

```
Spec(N, K) ≜ obj: x := 0
                  for 1..2K:
                     m := recv^fifo()
                     if m = Update(f, v):  x := v;  send^fifo(c_f, UpdateReply(x))
                     if m = Query(f):      send^fifo(c_f, QueryReply(x))
           ‖ c_i: send^fifo(obj, Update(i, i + 1));  u := recv^fifo()
                  send^fifo(obj, Query(i));          q := recv^fifo()                  (i = 0 … K−1)
```

One linearizable object served in arrival order: an update is a plain write answered with the written value, a query
is answered with the current value. `obj` spawns the clients to know their ids; spawn edges are invisible. `N` is
unused.

## The pairs

### Rust implementation against the specification — expected: **conforms**

```
Rust(N, K) ≜ c_i: send^fifo(s_0, Update(i, i + 1));       u := recv^fifo()
                  send^fifo(s_{N−1}, Query(i));          q := recv^fifo()
           ‖ s_j: x := 0;  sent := [];  sn := 0
                  for 1..2K:
                     m := recv^fifo()
                     if m = Update(f, v):                                    -- head only
                        sn := sn + 1;  x := v
                        if j = N−1:  send^fifo(c_f, UpdateReply(x))
                        else:        send^fifo(s_{j+1}, Fwd(sn, x, c_f));  sent := sent · sn
                     if m = Fwd(n, v, c):
                        x := v
                        if j = N−1:  send^fifo(c, UpdateReply(x));  send^fifo(s_{j−1}, Ack(n))
                        else:        send^fifo(s_{j+1}, Fwd(n, x, c));  sent := sent · n
                     if m = Ack(n):  sent := sent − n;  if j > 0:  send^fifo(s_{j−1}, Ack(n))
                     if m = Query(f):  send^fifo(c_f, QueryReply(x))         -- tail only
```

Client `ci` writes `i + 1` at the head and waits for the tail's reply, then queries the tail. The head applies an
update (a plain write, NOTES-rust 1), numbers it (NOTES-rust 3) and forwards the new value with the client's address
(NOTES-rust 4); every server writes it and forwards it, keeping it in `Sent` until acknowledged; the tail replies to
the client and acknowledges up the chain, each server removing the update from `Sent` and passing the `Ack` on (the
head consumes it, NOTES-rust 7). The tail answers queries from its replica. Every server stops after exactly `2K`
receives, the number it gets in a failure-free run (NOTES-rust 6). No `nondet`: each server takes messages in arrival
order.

How the two sides differ: the specification has one object; the implementation has `N` replicas, forwarding and
acknowledgements, all invisible. Why it conforms: under FIFO links the updates reach every server in the head's
order, and the tail is the one point where updates become visible (`UpdateReply`) and where queries are served, so the
tail's order is the object's arrival order; the reply to an update is the written value, and a query returns the
tail's current value. The causal order matches as well, because every visible operation passes through the tail, as it
passes through `obj` (the design note, "Specification lessons": chain replication keeps the sequential register).

### Java implementation against the specification — expected: **conforms**

```
Java(N, K) ≜ c_i: send^fifo(s_0, Update(i, i + 1));       u := recv^fifo(UpdateReply(_))
                  send^fifo(s_{N−1}, Query(i));          q := recv^fifo(QueryReply(_))
           ‖ s_j: x := 0;  hist := [];  sent := [];  nextSeq := 1;  nextAck := 1;  req := 0;  cl := 0
                  while req < K ∨ cl < K:
                     m := recv^fifo(  (req < K ∧ j = 0   ∧ Update(_, _))
                                    ∨ (req < K ∧ j > 0   ∧ Forward(nextSeq, _, _))
                                    ∨ (cl < K  ∧ j = N−1 ∧ Query(_))
                                    ∨ (cl < K  ∧ j < N−1 ∧ Ack(nextAck)) )
                     if m = Update(f, v):
                        req := req + 1;  n := nextSeq;  nextSeq := nextSeq + 1;  x := v;  hist := hist · n
                        if j = N−1:  send^fifo(c_f, UpdateReply(x))
                        else:        sent := sent · n;  send^fifo(s_{j+1}, Forward(n, f, x))
                     if m = Forward(n, f, v):
                        req := req + 1;  nextSeq := nextSeq + 1;  x := v;  hist := hist · n
                        if j = N−1:  send^fifo(c_f, UpdateReply(x));  send^fifo(s_{j−1}, Ack(n))
                        else:        sent := sent · n;  send^fifo(s_{j+1}, Forward(n, f, v))
                     if m = Ack(n):
                        cl := cl + 1;  nextAck := nextAck + 1;  assert head(sent) = n;  sent := tail(sent)
                        if j > 0:  send^fifo(s_{j−1}, Ack(n))
                     if m = Query(f):  cl := cl + 1;  send^fifo(c_f, QueryReply(x))
                  assert sent = [] ∧ |hist| = K
```

The same protocol with the Java agent's choices: `Forward` carries the client's index (not its address); the receive
guard accepts only the next `Forward` and the next `Ack` by sequence number, so the chain's links are FIFO in the
program even under bag (NOTES-java 4); a server counts `K` requests and `K` closings separately (NOTES-java 5) and
checks at the end that `Sent` is empty. The argument for conformance is the one of the Rust pair.

### The two implementations against each other — expected: **conforms**

Both directions. The visible threads and the client messages are the same; the differences, from the code and the two
notes:

1. **The sequence-number guard.** Java receives `Forward` and `Ack` only in sequence order; Rust takes them as they
   arrive. Under FIFO, the model of every check, each link already delivers in order, so the guard excludes no
   deliverable message (NOTES-java 4). It would matter only under bag, which is not checked.
2. **Termination count.** Rust stops after `2K` messages of any kind, Java after `K` requests and `K` closings; in a
   failure-free run these are the same messages.
3. **Internal shapes and addresses.** Rust's `Fwd` carries the client's thread id, Java's `Forward` the client's
   index; Rust publishes server ids once all servers are spawned and each client writes its own id before its first
   send (NOTES-rust 5), Java mints every id before starting any process (NOTES-java 6). Invisible.

The code and the notes agree; no divergence is declared.

## What it is for

The smallest of the paper-to-twin protocols and the one where every check has finished (`EVAL.tex`): the
specification has 6 graphs at `(3, 2)` and 90 at `(3, 3)`, each port 18 and 1 260; the Rust port conforms in the
stateless, stateful and offline engines, the Java port conforms against the specification offline and online, and the
two ports conform to each other offline in both directions and online (JMC on the Java port against the Rust port's
export; the TraceForge-side online rows are superseded and pending a rerun). Its other use is the interface defect:
the missing sender field was corrected before any check ran and is the recurring harness lesson (neither API's
receive reports a sender).
