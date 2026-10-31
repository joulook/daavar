# Load balancer: what is modelled today, and three load-aware variants to add

Designers' note for the lead, 2026-10-15. Complete-first only, the five configurations of
`P5-CFIRST`, same limits and digest format. Nothing runs before the lead confirms the
encodings and the tester has re-derived the expected verdicts (catalogue rule).

## 0. What A4 is today (from `APPS-CFIRST.report.md` §2.4 and `grid.rs::a4_impl/a4_spec`)

`k` visible clients each send `q` requests, one at a time, to an invisible balancer; the
balancer forwards each request to one of `m` invisible servers; the server replies to the
client directly, and the reply carries the server's index. A client observes: its own
sends, and the replies with the index of the server that answered.

- Implementation policies: **hash** (client `i` always goes to server `i mod m`) and
  **round-robin** (the `p`-th request goes to server `p mod m`). Fault: **drop** (the last
  client's last request is never forwarded).
- Specifications: **A4a, affinity** (the dispatcher picks one server per client, by
  `nondet`, on the client's first request and keeps it); **A4b, relay** (the dispatcher
  picks any server, by `nondet`, for every request).
- There is **no notion of load or capacity**: servers reply immediately, the balancer never
  asks anything, and the dispatch decision depends only on the request's client or
  position. The conforming pair of Table C is `hash` against A4a.

So the owner's scenario (a balancer that reads the servers' load and sends to the least
loaded, ties broken by choice; a specification that picks a server freely) is **not** what
ran. A4b is the owner's specification; the owner's implementation is new.

## 1. Variant L1 — least-loaded dispatch by polling, unbounded servers

Servers keep a queue. A server does not answer at once: on each turn it either takes
the next message (a request to enqueue, or a poll to answer) or, by `nondet`, serves the
oldest queued request and replies to its client. The balancer, per request, polls every
server, waits for the `m` load replies, picks a server with the smallest reported load,
breaking ties by `nondet`, and forwards the request.

```
main:       spawn balancer; spawn s[0..m); spawn c[0..k)

balancer:   repeat k·q times:
              Req(i, j) := recv req
              for x in 0..m:  send srv[x], Poll
              for x in 0..m:  Load(x, n[x]) := recv load          -- any order
              min := the set of x with n[x] = min(n)
              x := choose(min)                                    -- nondet among ties
              send srv[x], Req(i, j)

s[x] (invisible):
            queue := []
            loop:
              if queue ≠ [] and choose({0,1}) = 1:               -- serve one
                  Req(i, j) := pop(queue);  send reply[i], Reply(x, j)
              else:
                  msg := recv srv[x]
                  Poll:        send load, Load(x, |queue|)
                  Req(i, j):   push(queue, Req(i, j))

c[i] (visible):  for j in 1..q:  send req, Req(i, j);  recv reply[i]
```

Specification: **A4b** as it is. Expected verdict: **conforms**. Every assignment L1
produces is one A4b can choose, the polls are invisible, and the servers' delays only
reorder invisible events. Expected uncovered behaviour: none. Value of the row: a
realistic implementation with heavy invisible traffic (`m` polls and `m` load replies per
request) against a tiny specification; it measures what the cache buys when the
implementation family is much larger than the specification's.

Sizes: `k = q = m = s`, from `s = 2`; the server loop needs the bounded-command
encoding used for A2's primary (a fixed number of turns per server, `k·q + k·q·m`
messages at most), which the lead should confirm.

## 2. Variant L2 — bounded capacity, dispatch waits for space

As L1, but each server has capacity `c` (take `c = 1` at `s = 2`, `c = s − 1` above).
A server's load reply says whether it is full. The balancer buffers the request and
re-polls until some server reports space, then dispatches to a least-loaded server with
space.

Specification **A4c, capacity oracle**: the dispatcher knows every server's true queue
length at the moment of dispatch (a shared counter per server, read atomically: in the
Must API, a server's queue length is a value the dispatcher reads by a receive from a
dedicated channel that the server refreshes; the lead chooses the encoding) and never
forwards a request to a full server; among the servers with space it picks any by
`nondet`.

Expected verdict: **conforms**. The waiting balancer never overloads, and its choice
among non-full servers is one the oracle can make.

## 3. Variant L2-stale — the realistic bug: a stale poll

As L2, but the balancer polls **once** per request and dispatches on that snapshot. In
the meantime a server can have received another request? No: the balancer is the only
sender to servers and it is sequential, so loads only *decrease* between poll and
dispatch, and a server reported non-full stays non-full. To make the snapshot matter,
let **two balancers** share the servers (`b` balancers, the clients split between them):
balancer 1 polls, sees server `x` with one free slot, and before it dispatches, balancer
2 fills that slot. Server `x` then receives a request while full and **rejects** it: it
replies `Reject(x, j)` to the client, which is a visible observation the clients of the
oracle specification never see.

Specification: A4c with `b` dispatchers sharing the atomic view, which never overload.
Expected verdict: **violates**. Expected uncovered behaviour: a complete implementation
graph in which some client receives `Reject(x, j)`; no specification graph contains a
reject. First shows at `k = 2` clients, `b = 2` balancers, `m = 1` server, `c = 1`,
`q = 1`: both balancers poll the single server (empty), both dispatch, the second request
is rejected.

## 4. What to run

For each variant: the conforming pair(s) and, for L2-stale, the violating pair, under the
five complete-first configurations (`w=0`, `w=inf`, `w=1`, `w=8`, `w=64`), sizes from
`s = 2` until the first timeout, one run per row, quiet sweeps, digest after every size
as before. Table V columns for the violating pair as in `P5-CFIRST`. Fixture names:
`apps/a4/ll/a4b/k{s}q{s}m{s}` (L1), `apps/a4/cap/a4c/k{s}q{s}m{s}c{c}` (L2),
`apps/a4/cap-stale/a4c/b2k{s}q{s}m{s}c{c}` (L2-stale).

## 5. Open points for the lead

- The server loop with a `nondet` "serve now or receive" turn needs a bound; say which
  encoding you use and its graph counts at `s = 2` (hand-derived, tester re-derived).
- Whether `Reject` should be a message to the client (visible, as above) or a blocked
  client (the request is dropped); the message is the better witness, since the
  violation then shows in the observations, not only in a status.
- If L1's implementation family is too large at `s = 3` (the polls multiply the
  interleavings), say so with the count and stop at `s = 2` for L1 and L2.
