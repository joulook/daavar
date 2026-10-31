# Load-aware load balancers (L1, L2, L2-stale) against the A4 relay

**Family:** popl-conf application  **Source:** the designers' note `plan/traceForge/LB-VARIANTS.md`
(2026-10-15, variants L1, L2, L2-stale) and the A4 balancer it extends,
`daavar/benchmarks/a4-balancer/rust` (specification `A4b`, relay)  **Expected verdict:**
`l1`: **CONFORMS** (every least-loaded choice is one the relay's `nondet` can make; polls, loads and
queueing delays are invisible); `l2`: **CONFORMS** (one balancer never sends to a server it has just
seen full, so no request is rejected, and the choice among servers with space is again one the relay
can make); `l2-stale`: **DOES NOT REFINE** (two balancers poll the same server, both see the free
slot, both dispatch to it; the second request arrives while the queue is full and its client
receives `Reject`, which no specification client ever receives).

## 1. The protocol

`k` visible clients `c0 .. c{k-1}`, each sending `q` requests one at a time (send, then wait for
the answer) to a balancer; `m` invisible servers `s0 .. s{m-1}`, each with a FIFO queue; `b`
invisible balancers `balancer0 ..` (`b = 1` for `l1` and `l2`, `b = 2` for `l2-stale`); client `c_i`
talks to balancer `i mod b`.

Messages (one enum `Msg` on both sides, the same tags and field order):

| message | from → to | visible? |
|---|---|---|
| `Req { c, j }` | client → balancer, balancer → server | the client's send |
| `Poll { b, again }` | balancer `b` → server (`again` on a re-poll) | no |
| `Load { s, n }` | server `s` → balancer: its queue length `n` | no |
| `Reply { s, j }` | server `s` → client: request `j` served | the client's receive |
| `Reject { s, j }` | server `s` → client: request `j` arrived while the queue was full | the client's receive |
| `Stop` | balancer → server: nothing more from this balancer | no |

A client observes its own `Req` sends and, per request, one `Reply { s, j }` or `Reject { s, j }`
naming the server.

Parameters: `k`, `q`, `m`, and the capacity `c` for `l2` and `l2-stale`. Sizes used: `s = 2` and
`s = 3` with `k = q = m = s`, `c = 1` at `s = 2`, `c = s - 1 = 2` at `s = 3` (LB-VARIANTS §2), and
the extra size `w`: `k = 2, q = 1, m = 1, c = 1` (LB-VARIANTS §3's "first shows" point; see §4 for
why it is needed). Model `fifo` (TraceForge `ConsType::FIFO`, JMC `p2p`) only.

## 2. The specification program

The relay `A4b` of `a4-balancer`, with `b` dispatchers. `dispatcher{d}` receives the requests of its
clients (`q` per client `i` with `i mod b = d`), forwards each to server `(0..m).nondet()` (no
`nondet` when `m = 1`), and ends with `Stop` to every server. Server `s{x}` replies
`Reply { s: x, j }` to every request at once and ends after `b` `Stop`s. For `b = 1` this is
`a4_spec(.., A4Spec::A4b)` of `a4-balancer` with one change: the servers end on `Stop` instead of
looping forever, so every graph is complete.

There is no explicit capacity oracle (LB-VARIANTS' A4c). Choice made here: since the specification's
servers answer at once, a queue never holds a request at a dispatch, so "never forward to a full
server" constrains nothing visible, and an A4c whose dispatcher reads the true queue lengths would
only add order (spec order must be contained in the implementation's, §4), never a visible value.
The one visible thing capacity changes is `Reject`, and the relay never produces one. So the same
relay serves L1 and L2 (`b = 1`) and L2-stale (`b = 2`: "A4c with `b` dispatchers sharing the
atomic view").

## 3. The implementation program

`balancer{d}`, per request: receive a `Req` (from the request channel), send `Poll { b: d, again }`
to every server, receive the `m` `Load { s, n }` replies (from the load channel, in any order), keep
the servers with space (all of them for `l1`; `n < c` for `l2`, `l2-stale`). If none has space, poll
again with `again: true`. Otherwise send the request to a server of least load among those kept,
ties broken by `nondet` (no `nondet` for a single candidate). At the end, `Stop` to every server.

`s{x}` loop: after `b` `Stop`s, serve the whole queue and end. Otherwise, with an empty queue, a
blocking receive; with a non-empty queue a **non-blocking** receive whose ⊥ means "serve the oldest
request now" (`Reply` to its client) and whose message means "take the next message". A `Poll` is
answered with `Load { s: x, n: queue length }`, except that a re-poll (`again`) that finds the queue
full first serves once. A `Req` is queued, or answered `Reject { s: x, j }` when the queue already
holds `c`. A `Stop` is counted.

Variants: `l1` (no capacity, `b = 1`), `l2` (capacity `c`, `b = 1`), `l2-stale` (capacity `c`,
`b = 2`, the same balancer code).

### Choices on LB-VARIANTS §5 (the open points)

1. **Bound on "serve now or receive".** No turn counter. The server's turns are bounded by its
   messages: each turn either serves (at most one per queued request) or consumes a message (`m`
   polls per round per request, its requests, `b` `Stop`s). The balancers' `Stop` lets a server
   end after its last message and drain. Two further choices:
   - *Non-blocking receive, not `nondet()` + blocking receive.* The first encoding (`if queue ≠ []
     && nondet() { serve } else blocking recv`) had real deadlocks: 528 blocked graphs at
     `l1 k2q2m2`, both servers holding a request and waiting to receive while the clients wait for
     replies and the balancer waits for requests (`out/runs.log`, the runs before the NOTE). With
     the non-blocking receive the complete graphs are the same (⊥ ↔ `nondet` true, a read message
     ↔ `nondet` false then that message) and the blocked ones are gone.
   - *The re-poll fairness bound.* A balancer that sees every server full re-polls. Without a bound
     the servers could keep answering "full" forever. Rule: a server that gets a *re-poll* while
     full serves once before answering. So a second round always finds space and a balancer polls
     at most twice per request with `b = 1`. With `b = 2` an extra round needs a server refilled by
     the other balancer in between, which takes a new request, so the rounds are bounded by the
     requests.
2. **Reject is a visible message** `Reject { s, j }` to the client. The client takes it as its
   answer and moves on to its next request, so a client never blocks and the violation shows up in
   the values the client observes, not in a status.
3. **L2-stale = the L2 balancer with `b = 2`.** LB-VARIANTS shows that with one balancer the
   snapshot cannot go stale (loads only go down between poll and dispatch). So the stale variant
   is not new balancer code: it is the L2 code run by two balancers on the same servers. The
   balancer still re-polls when everything is full; the bug is that it trusts the snapshot of a
   server with space.
4. **s = 3 for L1/L2:** run once, see §7. Neither tool exhausts it in 600 s; the spec alone has
   33 067 440 graphs by hand. So L1 and L2 stop at `s = 2` (LB-VARIANTS §5, third point).

## 4. Where they differ, and why the verdict is what it is

The conformance check compares, per visible client, the word (sent and received values) and its
status, and the visible order as counts. Coverage needs a specification graph with equal words and
statuses whose order is **contained** in the implementation's (`compact.rs`: spec counts ≤ impl
counts, pointwise).

- **`l1`, `l2` conform.** Take an implementation graph. The specification picks the same order of
  requests at the dispatcher and, for every request, the server the balancer chose (a value
  `(0..m).nondet()` can take). The words are then equal: the same `Req`s, the same
  `Reply { s, j }`. No `Reject` occurs. In `l1` there is no capacity. In `l2` with one balancer, a
  server reported with space can only lose requests before the dispatch reaches it. Order: in the
  spec, a reply to `r` is causally after exactly the requests the dispatcher received up to `r` and
  their causes. In the implementation it is after at least those (the balancer's prefix up to `r`,
  through the server), plus whatever polls and queueing add. So the spec's counts are ≤.
- **`l2-stale` does not refine.** Two balancers each poll a server with one free slot, both read
  `Load { n: 0 }`, both dispatch to it. The server queues the first request and answers the
  second `Reject`. Smallest size: `w` (`k = 2, q = 1, m = 1, c = 1`). The uncovered visible
  behaviour, as the cross-language join prints it:
  `c0: send Req{0,0}, recv Reply{0,0}` and `c1: send Req{1,0}, recv Reject{0,0}`, both `done`, counts
  `[0,1,0,1]` (each client's receive after the other's send), and its mirror image with the roles of
  `c0` and `c1` swapped. Caught by the **signature value**: no specification graph has a `Reject` in
  any word ("no-slot"), whatever the order.
  - At `w`, `conf` reports exactly 32 uncovered graphs out of 148. By hand (§7) the graphs with a
    `Reject` number 32, so every uncovered graph is a reject graph and no reject-free graph is
    uncovered. With `b = 2` the shared servers add no order the relay lacks (the relay's servers are
    shared too).
  - At `s = 2` `conf --stop-at-first` finds a reject witness (`Reject { s: 0, j: 1 }`) at once. The
    full enumeration at `s = 2` does not finish in 600 s.

**Why the size `w` is needed.** A balancer holding a request sees at most `k - 1` other
outstanding requests in the queues (each client has one at a time), so "every server is full" needs
`m·c ≤ k - 1`. At LB-VARIANTS' sizes (`s = 2`: `m·c = 2 > 1`; `s = 3`: `m·c = 6 > 2`) L2 never
waits. Its least-loaded server always has space, so at `s = 2, 3` `l2` behaves exactly like `l1`
(same graph counts at `s = 2`), and the waiting and re-poll path is exercised only at `w`. Size `w`
is also where LB-VARIANTS expects the stale bug to show first.

## 5. Variants and faults

| variant | ARGS (Rust) | how it differs | expected verdict | reason |
|---|---|---|---|---|
| `l1` | `l1 k q m` | least-loaded by polling, unbounded queues, `b = 1` | conforms | §4 |
| `l2` | `l2 k q m c` | capacity `c`, re-poll while all full, `b = 1` | conforms | §4; no reject is possible with one balancer |
| `l2-stale` | `l2-stale k q m c` | the `l2` balancer, `b = 2` balancers on the same servers | does not refine | stale snapshot → `Reject` to a client (§4) |

Verdicts obtained (TraceForge `conf`, Rust spec vs Rust impl, default engine):

| variant | size | verdict | uncovered |
|---|---|---|---|
| `l1` | k2q2m2 | conforms (silence, both enumerations exhausted) | 0 |
| `l2` | k2q2m2c1 | conforms (silence) | 0 |
| `l2` | w = k2q1m1c1 | conforms (silence) | 0 |
| `l2-stale` | w = k2q1m1c1 | 32 candidate violations, both enumerations exhausted | 32 graphs (2 distinct summaries in the join) |
| `l2-stale` | k2q2m2c1 | `--stop-at-first`: 1 violation found (a `Reject`) | (not enumerated) |

Cross-language joins (`conf-join`, TraceForge spec export × JMC impl export):
`l1 k2q2m2` → `verdict: conforms, uncovered: 0` (96 spec lines; JMC's 10240 graphs as 168
summaries); `l2-stale w` → `verdict: violation, uncovered: 2`, the two `Reject` summaries quoted in
§4. The files are in `out/*.jsonl`.

## 6. Files

- `source/`: none. No source program exists: the benchmark comes from the designers' note
  `plan/traceForge/LB-VARIANTS.md` and from `daavar/benchmarks/a4-balancer/rust` (its `a4_spec`
  `A4b` is the specification, its client, request and reply shape the visible interface).
- `rust/`: crate `bench_balancer_load_aware` on TraceForge's shared main; ARGS
  `<l1|l2|l2-stale> <k> <q> <m> [<c>]` (the same ARGS build both sides; the spec ignores `c`).
- `java/`: `bench.balancer_load_aware.Balancer` (`MpProgram`), args
  `<spec|impl>,<variant>,<k>,<q>,<m>[,<c>]`; `Support.java` copied from the ewd998 twin. Rust's
  channels are thread mailboxes. The balancer's two channels are its mailbox read with the type
  guards `Req.class` and `Load.class`; this is equivalent because clients send it only `Req` and
  servers only `Load`.
- `out/runs.log`: every command and its output. `out/*.jsonl`: the four exports joined above.

## 7. Plain-Must exploration counts (both tools, one run each)

Model `fifo` / `p2p`. Hand-derived counts were done before the runs. The derivation is sketched
below the table.

| side | size | hand-derived | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec (`b = 1`) | k2q2m2 | 96 / 0 | 96 / 0 | 96 / 0 | yes |
| impl `l1` | k2q2m2 | 10240 / 0 | 10240 / 0 | 10240 / 0 | yes |
| spec (`b = 1`) | k2q2m2c1 (`l2`) | 96 / 0 | 96 / 0 | 96 / 0 | yes |
| impl `l2` | k2q2m2c1 | 10240 / 0 | 10240 / 0 | 10240 / 0 | yes |
| spec (`b = 2`) | k2q2m2c1 (`l2-stale`) | 460 / 0 | 460 / 0 | 460 / 0 | yes |
| impl `l2-stale` | k2q2m2c1 | not derived | not exhausted at 600 s (> 5 000 000 complete, 0 blocked) | not exhausted at 600 s (49367 / 0) | not compared |
| spec (`b = 1`) | w = k2q1m1c1 (`l2`) | 2 / 0 | 2 / 0 | 2 / 0 | yes |
| impl `l2` | w | 12 / 0 | 12 / 0 | 12 / 0 | yes |
| spec (`b = 2`) | w (`l2-stale`) | 6 / 0 | 6 / 0 | 6 / 0 | yes |
| impl `l2-stale` | w | 148 / 0 (32 with a `Reject`) | 148 / 0 | 148 / 0 | yes |
| spec (`b = 1`) | k3q3m3 (`l1`, `l2`) | 33 067 440 / 0 | not exhausted at 600 s (> 2 000 000 / 0) | not exhausted at 600 s (28489 / 0) | not compared |
| impl `l1` | k3q3m3 | not derived | not exhausted at 600 s (> 1 000 000 / 0) | not exhausted at 600 s (8181 / 0) | not compared |
| impl `l2` | k3q3m3c2 | not derived | not exhausted at 600 s (> 1 000 000 / 0) | not exhausted at 600 s (12914 / 0) | not compared |

`l2-stale` stops at `s = 2` (over a minute; `s = 3` not run). `l1` and `l2` stop at `s = 3` (over a
minute on both tools). TraceForge's partial counts are its last progress line before the kill. The `l2` spec
at `s = 3` is the same program as the `l1` spec (`b = 1`, `c` unused), so it was run once.

**Hand derivations (sketch).**
- *spec, `b = 1`:* the dispatcher's receive order is a merge of the clients' request sequences
  (`(kq)!/(q!)^k`), times `m^{kq}` server choices. Replies and `Stop`s are forced. `s = 2`: 6·16 = 96.
  At `w`: 2·1 = 2. `s = 3`: 1680·3^9 = 33 067 440.
- *spec, `b = 2`:* each dispatcher is sequential. Per assignment of the 4 requests to servers,
  count the pairs of per-server merges of the two dispatchers' FIFO streams (requests to that
  server, then `Stop`), minus the merge pairs that close a causal cycle through the two servers.
  Assignments: all at one server 2·20·2 = 80; each client on its own server 2·(16−1) = 30; one
  client together, the other split 8 cases = 224 (30 or 26 each, the 26 losing 4 cyclic pairs);
  both split 2·36 + 2·(36−9) = 126. Total 460. At `w`: C(4,2) = 6.
- *`l1`, `s = 2`:* 2 load-receive orders per request (2^4 = 16). Then, over the 6 balancer
  receive orders (3 shapes up to the client swap), the server's serve points (⊥ before each
  receive while its queue is non-empty, or the drain after `Stop`) and the tie choices they make
  possible, subject to "a client's request is served before the poll that follows its next
  request". The three shapes give 64, 144, 112; 2·(64+144+112)·16 = 10240.
- *`l2`, `s = 2`:* the same graphs as `l1`. The least-loaded server is never full (§4), no
  re-poll, no reject. 10240.
- *`l2` at `w`:* 2 receive orders × (serve before the second poll: 2; or answer full, then the
  re-poll is preceded by a ⊥ serve or forces one: 2·2) = 2·6 = 12.
- *`l2-stale` at `w`:* the server's merges of the two balancers' streams `Poll, [Poll'], Req, Stop`
  with the ⊥ serves between receives, enumerated by hand: 74 starting with `balancer0`'s poll,
  ×2 by symmetry = 148. Of these, 32 are the graphs where both balancers read `Load { n: 0 }`
  before either request is queued and the second `Req` finds the queue full.

Commands (exact text in `out/runs.log`; `TF` is the release binary, `jrun.sh` the launcher line of
the brief):

```
TF count spec|impl --model fifo <ARGS>                 # ARGS e.g. "l1 2 2 2", "l2-stale 2 1 1 1"
TF conf --model fifo <ARGS>        TF conf --stop-at-first --model fifo l2-stale 2 2 2 1
TF export spec --out F --model fifo --program balancer-load-aware --size <S> <ARGS>
java -Xss4m -cp java/out:$(cat daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher \
  'bench.balancer_load_aware.Balancer:<spec|impl>,<variant>,<k>,<q>,<m>[,<c>]' must 0 1 <fresh report dir> <-|export file> 600 </dev/null
daavar/traceforge/target/release/conf-join <tf-spec.jsonl> <jmc-impl.jsonl>
```
