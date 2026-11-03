# ABD atomic register (CIVL) against one register server

**Family:** related-work  **Source:** CIVL (Boogie) `samples/ABD.bpl` (Attiya, Bar-Noy, Dolev,
JACM 1995), under `related-work-papers/sources/boogie/Test/civl/samples/` (CANDIDATES.md N15)
**Expected verdict:** `correct`: **CONFORMS** (quorum intersection plus the readers' write-back
give every pair of operations the causal order and the values of some serial order of the
register); `read-no-writeback`: **DOES NOT REFINE** at 3 replicas (the new-old inversion: `r1`
reads 1, then `r2`, strictly after it, reads 0. Reaching it was checked by running, §4).

## 1. The protocol

`numReplicas` replicas each hold `StampedValue(TimeStamp(t, pid), value)`. A client runs
`QueryPhase` and then `UpdatePhase`:

- `QueryPhase`: query a quorum and keep the reply with the largest timestamp (`lt` is
  lexicographic on `(t, pid)`).
- `UpdatePhase`: send `(ts, v)` to a quorum. A replica stores the pair if `ts` is larger
  (`Update#0`).

`WriteClient` writes with `ts = (max.t + 1, pid)`. `ReadClient` writes back the max it read
before returning it. In the source, `Query#0`/`Update#0` are atomic remote accesses. Here they are
request/reply messages.

Roles:

- Visible: `w` (the writer), `r1`, `r2` (two readers).
- Invisible, implementation side: replicas `rep0 .. rep{n-1}` and one proxy per client, `wp`,
  `rp1`, `rp2`. The proxy runs the source's `WriteClient`/`ReadClient`. The clients cannot talk
  to the replicas directly (rule 3).
- Invisible, specification side: `register`.

Messages:

- Client ↔ front end: `Op::Write(1)` and `Op::Read(i)` (i names the reader), answered by
  `Reply::WriteAck` and `Reply::ReadReply(v)`.
- `Done` (`r1` → `r2`).
- Proxy ↔ replica: `ToReplica::Query(p)` and `ToReplica::Update(p, Ts(t, pid), v)`, where p is the
  proxy index (0 = `wp`, 1 = `rp1`, 2 = `rp2`), answered by `QueryReply(Ts(t, pid), v)` and
  `UpdateAck`.

The clients:

- `w` sends `Write(1)` and awaits the ack.
- `r1` reads, then sends `Done` to `r2`.
- `r2` awaits `Done`, then reads.

`Done` puts `r1`'s read before `r2`'s in happens-before. That stands for real-time order, which
a new-old inversion needs: without it, the specification admits `r1 = 1, r2 = 0` by serving `r2`
first. The register starts at value 0 with timestamp (0, 0). The writer's pid is 1.

Parameter: the number of replicas n (CLI size), majority `n/2 + 1`. The brief's size is n = 3
(1 writer and 2 readers, fixed). n = 2 (majority = both replicas) is run as the smaller size the
run protocol asks for.

## 2. The specification program

One invisible `register`, as the brief asks. It performs 3 blocking receives of `Op`, serving
each atomically: `Write(v)` sets the value and acks `w`, and `Read(i)` replies with the current
value. This is the layer-4 atomic `Read`/`Write` over one `value_store`. The serving order is any
order consistent with `Done`: `w` first, second or third. That gives 3 graphs, whatever n.

## 3. The implementation program

Each proxy receives its client's `Op`. Then:

1. `QueryPhase`: `Query` to every replica. The proxy keeps the max of the first majority of
   `QueryReply`s; the remaining reply is never read.
2. `UpdatePhase`: the writer sends `Update(ts+1, 1)`; a reader sends `Update` with the max it
   read (the write-back). Each goes to every replica, and the proxy waits for a majority of
   `UpdateAck`s.
3. The proxy replies to its client.

Each replica serves every request sent to it and then ends: 6 requests in `correct`, 4 in
`read-no-writeback`. So no graph is blocked. The source's quorum is `assume IsQuorum(q)`, a free
choice. Here the quorum is the set of the first majority of repliers, which is the realistic
implementation and lets every replica know its request count.

## 4. Where they differ, and why the verdict is what it is

The specification's visible happens-before, for serving order o1 o2 o3, is
`inv(o_i) → resp(o_j)` for every i ≤ j, plus program order and `Done`. Rule 2 (prop:refstrict):
an implementation graph conforms only if some specification graph with the same values has no
more order than it has.

`correct`. Take any two operations a and b. Each phase's quorum (the replicas whose replies the
proxy used) intersects every other phase's quorum, at some replica x, which serves both requests
in its program order. Say x serves a's request first. Then `inv(a)` precedes `resp(b)` in
happens-before, since x's reply to b is one of the replies b's proxy used. Case by case on the two
readers' values:

| (r1, r2) | serving order to match | why the implementation has that order |
|---|---|---|
| (1, 1) | `w r1 r2` | the value 1 flows from `w`'s update into both reads, so `inv(w) → resp(r1)` and `inv(w) → resp(r2)` |
| (0, 1) | `r1 w r2` | some replica in `w`'s update quorum answered `r1`'s query before `w`'s update, so `inv(r1) → resp(w)`; and `inv(w) → resp(r2)` |
| (0, 0) | `r1 r2 w` | the same argument for both readers |
| (1, 0) | — | impossible: `r1`'s write-back put (ts₁, 1) on a majority before `r1` returned, and `r2`'s query majority intersects it |

So **CONFORMS**. This is an argument, not a run of a conformance checker. One caveat: if the
checker's order component compares more than the visible happens-before described here, this
pair is where a false alarm from the single-register specification would show.

`read-no-writeback` at n = 3. Schedule:

1. `wp`'s `Update((1,1), 1)` reaches `rep0` only (so far).
2. `rp1` uses replies from `rep0` and `rep1`, so `r1` reads 1 and returns, then sends `Done`.
3. `rp2` uses replies from `rep1` and `rep2`, which have not yet seen the update, so `r2` reads 0.

The writer's acks come later, from `rep1` and `rep2`. No specification graph has `r1 = 1` and then
`r2 = 0`: `r1 = 1` needs `w` served before `r1`, and `Done` puts `r2` after `r1`. The visible
difference is `r2`'s `ReadReply(0)`, a **signature value** difference.

At n = 2 the fault is masked. For `r1` to read 1, some replica answered `r1`'s query with a
timestamp ≥ (1,1). Every query reads all replicas (a majority of 2 is both), so that replica also
answers `r2`'s query, later in its own program order (`Done` puts `r2`'s query after `r1`'s
return), and its stored timestamp only grows. So `r2` reads 1 too. The n = 2 runs show 0
inversions.

**Checked by running** (TraceForge, plain Must, the crate's diagnostic). **The `witness` diagnostic
is removed from the crate and the new CLI has no such argument; what follows is its recorded
result, kept as history.** With the fifth argument `witness` of the old CLI, `r1` records its value
in a static and `r2` counts the executions in which it reads 0 after `r1` read 1. The diagnostic adds no events and the counts are unchanged. Results:

| side | n | inversion executions / execs |
|---|---|---|
| spec | 2, 3 | 0 / 3 |
| impl `correct` | 2 | 0 / 12480 |
| impl `correct` | 3 | not printed: the run was cut at 600 s before the witness line |
| impl `read-no-writeback` | 2 | 0 / 544 |
| impl `read-no-writeback` | 3 | 20736 / 960768 |

So the inversion is reached at n = 3 and absent at n = 2, as argued above.

## 5. Variants and faults

| variant | how it differs | expected verdict | reason |
|---|---|---|---|
| `correct` | — | CONFORMS (n = 2, 3) | §4 |
| `read-no-writeback` | readers skip `UpdatePhase` | n = 2: CONFORMS (masked); n = 3: DOES NOT REFINE | new-old inversion (value) |

The source ships no faults. This one is the catalogue's fault (a), the classic ABD violation. The
catalogue's (b) (quorums of 1) and (c) (the writer reuses `ts`) are not built.

## 6. Files

- `source/`: `ABD.bpl` and its `.expect`, copied verbatim, with provenance in
  `source/PROVENANCE.md`.
- `rust/`: the Must-API Rust crate `bench_abd_register_civl`. CLI, the shared main: `<bin>
  count|export|conf|online … --model M -- <replicas> [correct|read-no-writeback]`; defaults: 3
  replicas, model `bag`, variant `correct`. The old fifth argument `witness` is removed (§4). One
  channel per receiver and message type. See `_tools/run-rust.sh`.
- `java/`: package `bench.abd_register_civl`, with `Spec` (args `replicas,model`) and `Impl` (args
  `replicas,model,variant`). Shared parts are in `Abd.java`. A proxy's three Rust channels (`Op`,
  `QueryReply`, `UpdateAck`) and `r2`'s two (`Done`, `Reply`) are type guards. Spawn order: spec
  `register, w, r1, r2`; impl `rep0.., wp, rp1, rp2, w, r1, r2`. The Java side has no witness
  diagnostic. See `_tools/run-java.sh`.

## 7. Plain-Must exploration counts (both tools, one run each)

Model `bag` (asyn) throughout, the source's multiset network.

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | n=2 | bag | 3 / 0 | 3 / 0 | yes |
| spec | n=3 | bag | 3 / 0 | 3 / 0 | yes |
| impl `correct` | n=2 | bag | 12480 / 0 | 12480 / 0 | yes |
| impl `correct` | n=3 | bag | not exhausted at 600 s (more than 8000000 / 0 when cut) | not exhausted at 600 s (63819 / 0 when cut) | not comparable |
| impl `read-no-writeback` | n=2 | bag | 544 / 0 | 544 / 0 | yes |
| impl `read-no-writeback` | n=3 | bag | 960768 / 0 | not exhausted at 600 s (49007 / 0 when cut) | not comparable |

Every finished JMC run ended with "stack exhausted". The two tools agree on every row that both
finished. At n = 3 TraceForge exhausts `read-no-writeback` and JMC does not within 600 s: an
exhaustion difference, not a count disagreement. `correct` at n = 3 finishes on neither tool, so
the evaluation can run `correct` only at n = 2.
Size 4 is not run (size 3 did not finish in under a minute). The TraceForge partial count is the
last progress line of the log; the first `correct` n = 3 Rust run, made while another
benchmark's runs were going, is kept as `out/logs/rust-impl-correct-3-bag.concurrent-run.log`
and superseded by a run made alone.

Commands, run from `B=/local/home/mkhoshechin/Documents/github-jmc/plan/benchmarks`, for
`n ∈ {2, 3}`, `v ∈ {correct, read-no-writeback}`:

```
timeout 600 $B/_tools/run-rust.sh $B/abd-register-civl count spec --model bag -- <n> correct
timeout 600 $B/_tools/run-rust.sh $B/abd-register-civl count impl --model bag -- <n> <v>
$B/_tools/run-java.sh $B/abd-register-civl 'bench.abd_register_civl.Spec:<n>,bag' must 100000 600
$B/_tools/run-java.sh $B/abd-register-civl 'bench.abd_register_civl.Impl:<n>,bag,<v>' must 100000 600
```

The variant argument is ignored by `spec`. Each row's logs are in `out/logs/`, named
`rust-spec-<n>-bag.log`, `rust-impl-<v>-<n>-bag.log`, `java-spec-<n>-bag.log` and
`java-impl-<v>-<n>-bag.log` for the four commands above, in that order.
