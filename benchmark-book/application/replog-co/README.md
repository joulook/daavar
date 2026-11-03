# replog-co(c) — replicated log over two-phase commit (`replicated-log-esop20`)

**In the paper:** Table 2 (conforming): `replog-co` = variant `correct`, model fifo, size 5. Table 3 (violating): `replog-co-er` = variant `early-reply`, model fifo, size 5. Every other variant, model and size stays in the appendix tables and in EVAL.pdf here.

**Communication models:** bag (`asyn`, Aneris's reliable unordered network) and fifo (`p2p`). **Languages:** Rust
(TraceForge's Must API), in `rust/`; Java twin in `java/` (`bench.replicated_log_esop20.RepLog`); context-free Java
port in `java-cf/` (`ReplicatedLog`). **Visible threads:** `client1 … clientc`, `auditor`; the specification's `log`
and the implementation's `coordinator`, `db1`, `db2` are invisible. **Parameters:** `c` = the number of clients
(`1 ≤ c ≤ 6`; `c = 2` is the source's `logger_runner`), client `i` logging the one event `ev(i)` = `EVENT X`,
`EVENT Y`, `EVENT Z`, … .
**Source:** Krogh-Jespersen et al., "Aneris", ESOP 2020, artifact `examples/replicated_log/rep_log.v` (with
`two_phase_commit/tpc.v`).

Notation: as in `../../synthetic/naive/README.md`. `M` is the model of the run (`bag` or `fifo`); both sides of a
pair run under the same `M`. `x := recv^M(tag ∈ S)` is a blocking receive of a message whose tag is in `S`. Messages
are `Event(i, ev)`, `Result(d)`, `Request(ev)`, `Vote(d)`, `Dec(d)`, `Ack`, `Read`, `Log(s)`, `Done`, with
`d ∈ {Commit, Abort}`; `·` on strings is concatenation. The clients and the auditor are the same code on both sides;
only the auditor's two `Read` destinations differ (`R1`, `R2` below), which the comparison does not see:

```
Clients(c) ≜ ‖_{1≤i≤c} clienti: send^M(L, Event(i, ev(i)));  r := recv^M(tag = Result);  send^M(auditor, Done)

Auditor(c, R1, R2) ≜ auditor: for c times: x := recv^M(tag = Done)
                              send^M(R1, Read);  send^M(R2, Read)
                              s1 := recv^M(tag = Log);  s2 := recv^M(tag = Log)
```

A client sends its event to the logger `L` (`log` in the specification, `coordinator` in the implementation), waits
for the reply, and tells the auditor it was answered. The auditor is added (the source has no reader of the logs):
after all `c` clients are answered it reads both replicas and receives their logs.

The specification (both pairs), the Iris specification's one atomic log:

```
Spec(c) ≜ Clients(c) ‖ Auditor(c, log, log)
        ‖ log: s := "";
               for c + 2 times:
                  m := recv^M(tag ∈ {Event, Read})
                  if m = Event(i, ev):  s := s · ev;  send^M(clienti, Result(Commit))
                  if m = Read:          send^M(auditor, Log(s))
```

`log` orders every event, appends it and answers `Commit`; a `Read` returns the whole log.

The implementation, the source's logger running `tpc_coordinate` over two databases:

```
Impl_v(c) ≜ Clients(c) ‖ Auditor(c, db1, db2)
          ‖ coordinator: for c times:
                            Event(i, ev) := recv^M(tag = Event)
                            send^M(db1, Request(ev));  send^M(db2, Request(ev))
                            ok := true
                            for 2 times:  m := recv^M(tag ∈ {Vote, Ack});  if m = Vote(Abort) then ok := false
                            d := (if ok then Commit else Abort)
                            send^M(db1, Dec(d));  send^M(db2, Dec(d))
                            ⟨reply before acks⟩
                            for 2 times:  a := recv^M(tag ∈ {Vote, Ack})
                            ⟨reply after acks⟩
          ‖ dbj (j = 1, 2): waiting := "";  s := ""
                            for 2c + 1 times:
                               m := recv^M(tag ∈ {Request, Dec, Read})
                               if m = Request(ev):  waiting := ev;  send^M(coordinator, Vote(Commit))
                               if m = Dec(d):       if d = Commit then s := s · waiting
                                                    send^M(coordinator, Ack)
                               if m = Read:         send^M(auditor, Log(s))
```

The `coordinator` serves one client event at a time: it sends the request to both databases, collects two votes,
decides `Commit` iff both voted `Commit`, sends the decision to both, collects two acknowledgements and replies to
the client. Each database stores a requested event, always votes `Commit`, appends the stored event on a `Commit`
decision and acknowledges; the added `Read` handler returns its log. (The abort path is in the code but never taken:
the databases never vote `Abort`.)

## The pairs

### correct, bag — expected: **conforms**

```
⟨reply before acks⟩ ≜ skip
⟨reply after acks⟩  ≜ send^M(clienti, Result(d))
```

A client's `Result` is sent after both `Ack`s, each sent after its database appended the event; the client's `Done`
follows its `Result`, and the auditor's `Read`s follow all `c` `Done`s. So both `Read`s are causally after every
append, and both logs are the events in the coordinator's serving order. The specification graph with the same
serving order at `log` and the same `Done` order has the same words (each client `S(Event) R(Result(Commit))
S(Done)`, the auditor `R(Done)^c S(Read) S(Read) R(Log(s)) R(Log(s))`) and the same causal order between visible
events.

### correct, fifo — expected: **conforms**

```
the same program as above, under M = fifo
```

The argument above does not use the delivery order. The specification has fewer graphs under fifo (its two `Read`s
and two `Log`s share a sender), but the implementation's extra graphs differ only in the order of equal `Log` values
and of the invisible votes and acknowledgements.

### early-reply, bag — expected: **violation** (`c ≥ 1`)

```
⟨reply before acks⟩ ≜ send^M(clienti, Result(d))     ◀ fault: the reply is sent right after the decision
⟨reply after acks⟩  ≜ skip                             ◀ fault: no reply after the acknowledgements
```

The coordinator answers the client before collecting the acknowledgements, so the reply no longer means "appended
on both databases". Schedule at `c = 1`: `client1` sends its event; the coordinator sends both requests, receives
both votes, sends `Dec(Commit)` to both databases and at once `Result(Commit)` to `client1`; `client1` sends `Done`;
the auditor sends `Read` to `db1` and `db2`; `db2` receives the `Read` before the `Dec` and answers `Log("")`. The
uncovered word is

```
auditor: R(Done) S(Read) S(Read) R(Log("EVENT X")) R(Log(""))
```

(also with the two `Log`s swapped, and with both `Log("")`). In the specification the `Read`s are served after the
event was appended, so every `Log` is `"EVENT X"`.

### early-reply, fifo — expected: **violation** (`c ≥ 1`)

```
the early-reply program above, under M = fifo
```

The race is between two senders (the coordinator's `Dec` and the auditor's `Read`, both to the same database), so
per-channel order does not prevent it; the same words are uncovered.

## What it is for

A two-phase-commit replication protocol whose replicated state is made observable by an added reader. The seeded
fault removes exactly what the acknowledgement round is for, and its violation is independent of the model, the
contrast to `../replog`, whose fault violates under bag only.
