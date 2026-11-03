# replog(W) — leader–follower replicated log (`replicated-log-aneris`)

**Communication models:** fifo (`p2p`, the default) and bag (`asyn`). **Languages:** Rust (TraceForge's Must API),
in `rust/`; Java twin in `java/` (`bench.replicated_log_aneris.RepDb`); context-free Java port in `java-cf/`
(`ReplicatedLogAneris`). **Visible threads:** `writer`, `reader`; `leader` and `follower` are invisible.
**Parameters:** `W` = the number of writes by `writer` (key `i` := `val(i)`, with `val(0) = 37`, `val(1) = 1`,
`val(i) = i` otherwise) and of reads by `reader` (keys `W−1, …, 0`, in reverse order); default 2, smallest
interesting size 2; `2 ≤ W ≤ 255`: the Rust program keeps keys, values, entry indices and the follower's `next` in
bytes (`next` overflows after 255 entries), so the pseudocode below (unbounded integers) and the Java twin (`int`
counter) describe it only within that range.
**Source:** Aneris (Krogh-Jespersen et al., ESOP 2020), the reliable-communication library's leader–followers
`repdb` (`repdb_code.ml`) driven by `causality_example_code.ml`.

Notation: as in `../../synthetic/naive/README.md`. `M` is the model of the run (`fifo` or `bag`); both sides of a
pair run under the same `M`. `x := recv^M(tag ∈ S)` is a blocking receive of a message whose tag is in `S`. Messages
are `Write(from, k, v)`, `Ack`, `Read(from, k)`, `Val(v)` (`v = 0` is `None`), `Sync(from, i)`, `Entry(k, v, j)`.
Two threads are common to both sides:

```
Clients(W) ≜ writer: for i := 0..W−1:   send^M(leader, Write(writer, i, val(i)));  a := recv^M(tag = Ack)
           ‖ reader: for i := W−1..0:   send^M(follower, Read(reader, i));        r := recv^M(tag = Val)
```

`writer` (node 0 of the example) writes keys `0..W−1` at the leader and waits for each acknowledgement; `reader`
(node 1) reads the keys at the follower in reverse order (the source's read-until loop on `y` is bounded to one
read). At `W = 2`: `x = 37`, then `y = 1`; `reader` reads `y`, then `x`.

The specification (both pairs):

```
Spec(W) ≜ Clients(W)
        ‖ leader:   for j := 0..W−1:
                       Write(f, k, v) := recv^M(tag = Write)
                       send^M(f, Ack)
                       send^M(follower, Entry(k, v, j))
        ‖ follower: db := [0]^W;  next := 0;  early := ∅
                    for 2W times:
                       m := recv^M(tag ∈ {Entry, Read})
                       if m = Entry(k, v, j):  early := early ∪ {(k, v, j)}
                                               while some (k', v', next) ∈ early:
                                                  remove it;  db[k'] := v';  next := next + 1
                       if m = Read(f, k):      send^M(f, Val(db[k]))
```

The specification `leader` acknowledges each write and pushes it to the follower as log entry `j`, unprompted. The
specification `follower` keeps entries that arrive ahead of `next` aside and applies only the contiguous prefix of
the log, so its `db` is always the first `next` entries; a read answers from that prefix. When the entries arrive
relative to the reads is the receive's nondeterminism.

The implementation (the variant chooses the follower's sync):

```
Impl_v(W) ≜ Clients(W)
          ‖ leader:   log := [];  db := [0]^W;  waiting := []
                      for 2W times:
                         m := recv^M(tag ∈ {Write, Sync})
                         if m = Write(f, k, v):  db[k] := v;  log := log · (k, v);  send^M(f, Ack)
                                                 sort waiting by index
                                                 while waiting = (g, i) · rest and i < |log|:
                                                    send^M(g, Entry(log[i], i));  waiting := rest
                         if m = Sync(g, i):      if i < |log| then send^M(g, Entry(log[i], i))
                                                 else waiting := waiting · (g, i)
          ‖ follower: db := [0]^W;  next := 0
                      ⟨sync start⟩
                      for 2W times:
                         m := recv^M(tag ∈ {Entry, Read})
                         if m = Entry(k, v, j):  ⟨check⟩;  db[k] := v;  next := next + 1;  ⟨sync next⟩
                         if m = Read(f, k):      send^M(f, Val(db[k]))
```

The implementation `leader` is `repdb`'s client handler and `follower_request_handler`: it receives `W` writes and
`W` sync requests in any order, appends each write to its log, acknowledges it, and answers every waiting request
whose index now exists, in index order (`log_wait_until` / `monitor_signal`); a request for an existing index is
answered at once. The implementation `follower` is the source's `sync_loop` and client handler in one thread: it
requests entries from the leader, applies them to its `db`, and answers reads from it. The leader's periodic
log-copy loop is not modelled.

## The pairs

### correct, fifo — expected: **conforms**

```
⟨sync start⟩ ≜ send^M(leader, Sync(follower, 0))
⟨check⟩      ≜ assert next = j
⟨sync next⟩  ≜ if next < W then send^M(leader, Sync(follower, next))
```

The source's `sync_loop`: one outstanding request; entry `i` is requested only after entry `i − 1` is applied, and
`assert (i = j)` holds. The follower's `db` is therefore always a prefix of the leader's log that grows, as in the
specification. The specification graph in which the follower receives entry `i` at the place, among its reads, where
the implementation follower applies it covers the implementation graph: in both, a read that returns write `i`'s
value is causally after `writer`'s send of `Write i` (through the leader), and the implementation's extra order (the
`Sync` requests) only removes interleavings. Values agree read by read.

### correct, bag — expected: **conforms**

```
the same program as above, under M = bag
```

A request–reply chain with one outstanding request has nothing to reorder, so the bag model gives the follower the
same entry order and the cover above applies unchanged (the implementation has the same number of graphs under both
models).

### asyn-fault, fifo (the control) — expected: **conforms**

```
⟨sync start⟩ ≜ for i := 0..W−1: send^M(leader, Sync(follower, i))    ◀ fault: every request at once (pipelined)
⟨check⟩      ≜ skip                                                    ◀ fault: no index check
⟨sync next⟩  ≜ skip                                                    ◀ fault: no further request
```

The pipelined sync of the catalogue's fault (a): the follower asks for every entry at once and applies the entries
in arrival order. Under fifo the leader receives the requests in index order, answers the waiting ones in index
order, and the follower's channel from the leader delivers them in that order, so the follower still applies a
prefix of the log and the cover of `correct` applies.

### asyn-fault, bag — expected: **violation** (`W ≥ 2`)

```
the asyn-fault program above, under M = bag
```

Under bag the follower may receive entry 1 first and apply it with no index check: either the leader sends entry 1
first (it reads both writes, then `Sync(1)` before `Sync(0)`, and answers each at once), or it sends `Entry(x, 37, 0)`
before `Entry(y, 1, 1)` and delivery reverses them. One witness, of the second kind, at `W = 2`: the follower sends `Sync(0)`, `Sync(1)`; the leader
parks both, receives `Write(x, 37)` and sends entry 0, receives `Write(y, 1)` and sends entry 1; the follower
receives entry 1, answers `Read(y)` with `1` and `Read(x)` with `0`, then receives entry 0. The uncovered word is

```
reader: S(Read(reader, y)) R(Val(1)) S(Read(reader, x)) R(Val(0))
```

In the specification the follower applies entry 1 only after entry 0, so once `reader` sees `y = 1` its later read
of `x` returns 37. This is the failure of the example's `assert (rd "x" = Some 37)`.

## What it is for

A small leader–follower replication protocol in which the specification and the implementation have different
invisible threads (an unprompted push against a request–reply sync) and the same visible clients. The pair
`asyn-fault` is the one fault of the family whose verdict depends on the model: it violates under bag and conforms
under fifo, so the two rows separate the delivery model from the program.
