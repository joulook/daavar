# lb-ll(k, q, m) and lb-cap(k, q, m, c) — least-loaded dispatch by polling, and bounded capacity where dispatch waits for space (`balancer-load-aware`)

**In the paper:** not in Tables 2 and 3 (reported in the appendix tables and in EVAL.pdf here only).

**Communication model:** fifo (`p2p`: per-channel order); the channels are plain, so the run's `--model` applies,
and the program states that the twin is meaningful under fifo only; the campaign ran fifo. **Languages:** Rust
(TraceForge's Must API), in `rust/`; Java twin in `java/` (`bench.balancer_load_aware.Balancer`, args
`<spec|impl>,<variant>,<k>,<q>,<m>[,<c>]`); context-free Java port in `java-cf/`
(`bench.balancer_load_aware.BalancerLoadAware`, documented args `<spec|impl>,<k>,<q>,<m>[,<c>],<fifo|bag>,<variant>`,
the Rust order accepted too). **Visible threads:** the clients `c0 … c(k−1)`; the fronts (`dispatcher0 …` in the
specification, `balancer0 …` in the implementation) and the servers `s0 … s(m−1)` are invisible. **Parameters:**
`k` = the number of clients, `q` = the requests per client, `m` = the number of servers, `c` = the capacity of a
server's queue (`l2`, `l2-stale` only); each in `1..=100`. `b` = the number of fronts on both sides: 1, and 2 for
`l2-stale`; client `c_i` talks to front `i mod b`. ARGS `<variant> <k> <q> <m> [<c>]` (exactly 4 arguments for
`l1`, 5 otherwise); the same ARGS build both sides, the specification ignores `c`. The paper names the program
`lb-ll` for `l1` and `lb-cap` for `l2` (variant `correct`) and `l2-stale` (variant `stale-poll`). **Source:**
written for this work (the designers' note `LB-VARIANTS.md`, 2026-10-15, variants L1, L2, L2-stale); the
specification is the relay `A4b` of `../lb`, with servers that end on `Stop` instead of looping.

Notation: as in `../../synthetic/naive/README.md`; `recv^{fifo,nb}()` is non-blocking and may return `⊥`. Every
receive reads the executing thread's one channel, except the balancer's, which has two (`recv^fifo_req()` for the
clients' requests, `recv^fifo_load()` for the servers' loads); `front_d` is `dispatcher_d` or `balancer_d`. Messages
are `Req(i, j)`, `Poll(d, again)`, `Load(x, n)`, `Reply(x, j)`, `Reject(x, j)`, `Stop`; `n_d ≜ q · |{i < k : i mod b = d}|`
is the number of requests front `d` receives.

The clients, the same in every program (spawn order: fronts, `s0..`, `c0..`):

```
C_i(q) ≜ c_i: for j := 0..q−1:  send^fifo(front_{i mod b}, Req(i, j));  _ := recv^fifo()
```

Client `c_i` sends `q` requests one at a time, each followed by a blocking read of its answer (`Reply(x, j)` or, in
the implementation only, `Reject(x, j)`); it ignores the content, so a client never blocks on a rejection.

## The pairs

The specification (the relay: a dispatcher forwards each request at once to a server of its choice):

```
Spec_b(k, q, m) ≜ ‖_{d<b} dispatcher_d ‖ ‖_{x<m} S_x ‖ ‖_{i<k} C_i(q)

dispatcher_d ≜ for t := 1..n_d:  r := recv^fifo();  x := nondet({0, …, m−1});  send^fifo(s_x, r)
               for x := 0..m−1:  send^fifo(s_x, Stop)

S_x ≜ stops := 0
      while stops < b:  r := recv^fifo()
                        if r = Req(i, j) then send^fifo(c_i, Reply(x, j))
                        if r = Stop then stops := stops + 1
```

Server `S_x` answers every request at once, naming itself, and ends after `b` `Stop`s, so every specification
graph is complete and no specification word contains a `Reject`.

The implementation (the balancer polls the servers' loads before every dispatch; a server keeps a FIFO queue):

```
Impl_v(k, q, m, c) ≜ ‖_{d<b} balancer_d ‖ ‖_{x<m} s_x ‖ ‖_{i<k} C_i(q)

balancer_d ≜ for t := 1..n_d:
                r := recv^fifo_req()
                again := false
                loop:
                   for x := 0..m−1:  send^fifo(s_x, Poll(d, again))
                   for m times:  Load(x, n) := recv^fifo_load();  load[x] := n
                   space := {x < m : SPACE(load[x])}
                   if space = ∅ then  again := true                                   (poll again)
                   else  best := {x ∈ space : load[x] = min_{y ∈ space} load[y]};  x := nondet(best);  exit loop
                send^fifo(s_x, r)
             for x := 0..m−1:  send^fifo(s_x, Stop)

s_x ≜ queue := [];  stops := 0
      loop:
         if stops = b then  for (i, j) in queue:  send^fifo(c_i, Reply(x, j));  exit
         r := recv^fifo() if queue = [] else recv^{fifo,nb}()
         if r = ⊥:               serve()
         if r = Poll(d, again):  if again and FULL then serve()
                                 send^fifo(balancer_d, Load(x, |queue|))
         if r = Req(i, j):       if FULL then send^fifo(c_i, Reject(x, j)) else queue := queue · (i, j)
         if r = Stop:            stops := stops + 1

serve() ≜ (i, j) := head(queue);  queue := tail(queue);  send^fifo(c_i, Reply(x, j))
```

`balancer_d` polls every server, reads the `m` loads in any order, keeps the servers with `SPACE`, and sends the
request to one of least load among them (ties by `nondet`; no choice for a single candidate); if none has space it
polls again with `again`. `s_x` serves the oldest queued request when its non-blocking receive reads nothing (a
server holding requests never blocks), answers a `Poll` with its queue length (a re-poll that finds it `FULL` first
serves once: the fairness bound that keeps the re-poll loop finite), queues a `Req` or rejects it when `FULL`,
counts `Stop`s, and after `b` of them drains its queue and ends. Every implementation graph is complete
(README.orig §7).

### l1 — expected: **conforms**

```
b ≜ 1        SPACE(n) ≜ true        FULL ≜ false
```

Take an implementation graph. The specification's dispatcher reads the requests in the balancer's order (both read
their one request channel in arrival order) and, for every request, chooses the server the balancer chose, a value
`nondet({0, …, m−1})` can take. The words are then equal (the same `Req`s; the same `Reply(x, j)`, since a server
replies to its queue in arrival order on both sides and the FIFO link from the one front to each server delivers in
dispatch order), no `Reject` occurs, and every client ends done. The specification's reply to a request follows the
requests the dispatcher read before it, and the implementation's follows at least those (the balancer's prefix,
through the server) plus the polls and the queueing, so the specification's order is contained.

### l2 — expected: **conforms**

```
b ≜ 1        SPACE(n) ≜ n < c        FULL ≜ |queue| ≥ c
```

As `l1`, once no request is rejected: with one balancer, a server reported with space can only lose requests between
the poll and the dispatch (nothing else sends to it), so the dispatched request finds space and is queued. The
re-polls add invisible events only. The waiting path needs every server full while the balancer holds a request,
i.e. `m·c ≤ k − 1` (each client has at most one request outstanding); at `k = q = m` with `c ≥ 1` it is never
taken and `l2` has `l1`'s graphs (README.orig §4), so the campaign also runs `(k, q, m, c) = (2, 1, 1, 1)`.

### l2-stale — expected: **violation** if `k > c ∧ (c = 1 ∨ m = 1)`; **conforms** if `k ≤ c`; no campaign verdict for `k > c ∧ c ≥ 2 ∧ m ≥ 2`

```
b ≜ 2        ◀ changed: two balancers on the same servers; SPACE and FULL as l2
```

The `l2` balancer unchanged, run twice (`c_i` talks to `balancer_{i mod 2}`): a poll's snapshot can go stale through
the other balancer. Witness at `(k, q, m, c) = (2, 1, 1, 1)`: both balancers poll `s0` while its queue is empty and
both read `Load(0, 0)`; both dispatch to `s0`, which queues the first request and answers the second with `Reject`:

```
c0: S(Req(0, 0)) R(Reply(0, 0))
c1: S(Req(1, 0)) R(Reject(0, 0))        (and its mirror image, c1 served and c0 rejected)
```

No specification word contains a `Reject` (value). With `c = 1` and `m ≥ 2` the same happens with both balancers
choosing `s0` among the tied empty servers, and `k > c = 1` gives each balancer a client. With `m = 1` and
`c ≥ 2 < k`: the balancers dispatch `c − 1` requests of distinct clients to the one server, which queues them
without serving; then each balancer polls for one more client's request, both read `Load(0, c − 1)`, both dispatch,
and the second to arrive is rejected (`k ≥ c + 1` clients, one request each, suffice). With `k ≤ c` a request
reaches a server whose queue holds at most the `k − 1` other clients' requests, fewer than `c`, so no `Reject`
occurs and the argument of `l2` applies with two fronts on both sides (the shared servers add no order the relay's
shared servers lack). For `k > c ∧ c ≥ 2 ∧ m ≥ 2` the campaign gives no expected verdict (its note: README.orig
states the violation without a size rule, and a model finds no `Reject` at `3 1 2 2`, `3 1 3 2`, `4 1 2 3`); by hand,
a balancer sends to a server reporting `c − 1` only when every other server reported at least `c − 1`, so a
rejection needs at least `m(c − 1) + 2` clients, which those three sizes lack.

## What it is for

One balancer family whose polls, loads, queueing and waiting are invisible: `l1` and `l2` conform to the relay of
`../lb` although their fronts are deterministic given the loads, and `l2-stale` is a real concurrency fault (a
stale snapshot shared by two fronts) whose symptom is a value, `Reject`, absent from every specification word,
with a verdict that depends on the size. The implementation is large against the specification (10 240 graphs
against 96 at `(2, 2, 2)`, README.orig §7), and every graph of both sides is complete.
