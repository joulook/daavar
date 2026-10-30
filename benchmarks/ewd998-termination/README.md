# EWD998: Safra's termination detection against a termination oracle

**Family:** related-work  **Source:** TLA+ Examples, `related-work-papers/sources/tlaplus-examples/specifications/ewd998/`
(`AsyncTerminationDetection.tla`, `EWD998.tla`, `EWD998Chan.tla`, and the Java implementation
`impl/src/EWD998.java`) (CANDIDATES.md N14, record in CATALOGUE-tla.md)  **Expected verdict:**
`correct`: **CONFORMS** (Safra's rules announce termination only after every node is idle with
no payload in flight, so every `Trm` comes after every node's last idle report);
`no-black-on-receive`: **DOES NOT REFINE** (a detector that does not turn black on a receive lets
node 0 announce while another node is still active: an app receives `Trm` that is not ordered
after that node's last `Idle`).

## 1. The protocol

EWD 998 (Safra): N nodes in a ring, node 0 the initiator. Active nodes send payloads (`pl`); a
node may become idle (`Deactivate`) and is reactivated by a payload (`RecvMsg`). Each node keeps
`counter` (sends minus receives) and a `color`; a receive turns the node black. A token
`tok(q, color)` travels from node N-1 down to node 0: an idle node holding it passes it on with
`q + counter` and blackened if the node is black, and whitens itself (`PassToken`); node 0 starts
a round with a white token, `q = 0` (`InitiateProbe`), and detects termination when the token
comes back white with `q + counter[0] = 0` while node 0 is white and idle. The Java
implementation (`EWD998.java`) then sends `trm` to every node, which is not part of EWD998.

Roles (termination detection has no client; the split follows the catalogue):

- Visible: `app0..app{N-1}`, the application part of each node: active at the start, it may send
  its one payload (K = 1 per node) to another node, then reports that it is idle, then waits for
  a payload (reactivating it) or the announcement `Trm` (ending it).
- Invisible: `det0..det{N-1}` (detector stubs in the specification, Safra's detectors in the
  implementation); `oracle` (specification only).

Messages: `Pl { from }` (a payload; `from` is the sending node), `Idle { node, sent, got }`
(an app's idle report: `sent` the node its payload went to, if any; `got` the bitmask of the
nodes it received a payload from, cumulative), `Trm`; invisible: `Tok { q, black }` and `Stop`.
A payload travels `app<i> -> det<j> -> app<j>` on both sides, so the receiving detector knows of
a receive the moment it happens (as the Java node does: one thread owns both). Parameters: N
nodes (the size), K = 1 payload per node, R = 2 probes (fixed). Smallest interesting size:
N = 2 (the catalogue expected N = 3; §4).

## 2. The specification program

`AsyncTerminationDetection.tla` as a program. `det<j>` is a forwarding stub (a `Pl` goes on to
`app<j>`; `Stop` ends it), one per node (rule 2: the implementation delivers payloads to
different nodes independently). The apps report `Idle` to one invisible `oracle` (rule 2: the
termination decision is one decision every app observes). The oracle keeps each node's latest
report (the one with the most receives) and, after each report, tests `terminated`: every node
has reported, every receive reported was reported sent to that node by its sender (a consistent
cut), and every payload reported sent was reported received. Then it may (`nondet()`) send `Trm`
to every app and `Stop` to every stub, and either way it ends (rule 1: the implementation's
bounded rounds may never announce, and then every app ends blocked; the specification may stay
silent too). The test is exact: the reports at a consistent cut with no payload in flight and
every node idle describe a terminated global state, and termination is stable, so those reports
are every node's last ones. So every `Trm` the specification sends follows every app's last
`Idle` (through the oracle), and no `Trm` is sent while a payload is in flight.

## 3. The implementation program

`det<i>` runs `EWD998.java`'s node loop minus the application: `counter`, `color` (black or
white), the token, and `outstanding`, the payloads it forwarded to `app<i>` not yet followed by an
`Idle` (plus 1 for the initial activity): the node is active while `outstanding > 0`. On `Pl`:
`counter - 1`, black (`RecvMsg`; dropped by the fault), `outstanding + 1`, forward to `app<i>`.
On `Idle { sent }`: `counter + 1` the first time `sent` is set (`SendMsg`, counted when the app
goes idle, so before the token can leave the node), `outstanding - 1` (`Deactivate`). On `Tok`:
keep it. Then, holding the token: node i > 0 passes it to `det<i-1>` with `q + counter` and
`black || color` and whitens itself once idle (`PassToken`; the Java node re-enqueues the token
while active); node 0 evaluates the round: inconclusive, it starts a new round at once if it has
probes left (`InitiateProbe`, R = 2 in all; the first is started at the start, because the Java
initiator's first token is black); conclusive and idle, it sends `Trm` to every app and `Stop` to
`det1..`; conclusive and active, it keeps the token and evaluates again after its next message.
A detector with no more probes keeps relaying payloads; detectors that are never stopped end
blocked (invisible). Model: asyn (`EWD998Chan` receives from any inbox index).

## 4. Where they differ, and why the verdict is what it is

Notation: `S(m)` a send, `R(m)` a receive, per visible thread in program order; `Idle{n, s, g}`.

**`correct` conforms.** The apps are the same program on both sides and each payload is relayed
by one thread per node on both sides, so the words of the apps up to their last `Idle` and their
payload orders are matched by the stub graph that reads payloads in the detectors' order. The
detector of node i is Safra's node with `active = (outstanding > 0)`, which contains the app's
real activity: a node sends only while active, a receive is counted (and blackens) when it is
forwarded, a send is counted before the node goes idle. By Safra's theorem a conclusive round
implies that every node is idle and no payload is in flight, and stays so; every detector passed
the token of that round while idle, i.e. after reading its app's last `Idle`, so every `Trm` is
after every app's last `Idle` in the implementation too. If the rounds run out, no `Trm` is sent
and every app ends blocked, which the silent oracle covers. **CONFORMS.**

**`no-black-on-receive` does not refine.** Smallest size N = 2, schedule (from the fault check):
`det0` sends the token to `det1`; `app1` reports `Idle{1, None, 0}`; `det1` passes `q = 0`, white;
`app0` sends `Pl` to node 1 and reports `Idle{0, Some(1), 0}` (`counter0 = 1`); `det1` forwards
the payload, `app1` receives it and sends `Pl` to node 0; `det0` counts that receive
(`counter0 = 0`) and stays white; `app0` receives it and reports `Idle{0, Some(1), 0b10}`; the
token is white with `q + counter0 = 0`, node 0 white and idle: `det0` announces while `app1` has
not yet reported idle. Uncovered word:
`app0: S(Pl) S(Idle{0,Some(1),0}) R(Pl{1}) S(Idle{0,Some(1),0b10}) R(Trm)`,
`app1: S(Idle{1,None,0}) R(Pl{0}) S(Pl) S(Idle{1,Some(0),0b01}) R(Trm)`, in a graph where
`app1`'s last `S(Idle)` is not before `app0`'s `R(Trm)`. In the specification the oracle cannot
announce before it has read `Idle{1,Some(0),0b01}` (until then `app0`'s payload to node 1 is
reported sent and not received), so every specification graph with this word orders that `Idle`
before both `R(Trm)`. Component: **order**. The catalogue placed the smallest instance at N = 3
(nodes 1 and 2 each sending one payload); the N = 2 schedule above, which needs one payload from
each node, was found by the check below; the N = 3 instance is also reached.

Reachability was checked with `out/fault-check/` (the same crate with a counter of the true
activity, apps active plus payloads in flight, updated right after each app event, and an
assertion that it is 0 when `det0` or the oracle announces; the panic message lists the
execution's events). Executed results (2026-10-30), one run each, from
`$B/ewd998-termination/out/fault-check` with
`export CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/bench-target; export TMPDIR=/local/home/mkhoshechin/tmp`:

| claim | command | result |
|---|---|---|
| fault announces early, N=2 | `cargo run -q -j 2 --offline --release -- impl 2 bag no-black-on-receive` | panic `det0 announces termination while 1 apps/payloads are still busy; …` |
| fault announces early, N=3 | `cargo run -q -j 2 --offline --release -- impl 3 bag no-black-on-receive` | panic, same message |
| `correct` never does, N=2 / N=3 | `cargo run -q -j 2 --offline --release -- impl <N> bag correct` | exit 0, `execs=414 blocked=902` / `execs=173384 blocked=489024` |
| `spec` never does, N=2 / N=3 | `cargo run -q -j 2 --offline --release -- spec <N> bag` | exit 0, `execs=80 blocked=80` / `execs=77688 blocked=77688` |

Every claim reproduced; the holding runs explore as many executions as the plain-Must counts in
§7. Every command and its output (with the full event list of each failing execution) is in
`out/fault-check.log`.

## 5. Variants and faults

| variant | how it differs | expected verdict | reason |
|---|---|---|---|
| `correct` | Safra's rules as in `EWD998.java` | CONFORMS | §4 |
| `no-black-on-receive` | `RecvMsg` without `color' = black` (ours; the catalogue's "drop Rule 3") | DOES NOT REFINE (N ≥ 2) | `Trm` before another node's last `Idle` |

Both against the one `spec`. The source ships no switchable fault; the divergences in
`EWD998ChanTrace.md` live only in its git history and are not built.

Departures from the lead's description, stated so they can be overruled: (1) the announcement
`Trm` goes to every app, as the Java initiator sends `trm` to every node, not to the initiator's
app alone (with `Trm` to `app0` only, every other app would end blocked in every execution of
both sides); (2) payloads are relayed by the receiving node's detector on both sides, and the
apps report `Idle` to their detector (implementation) or to the oracle (specification); (3) a
send is counted when the app reports idle, not at the send; the node is active (holds the token)
in between, so the count reaches the token at the same rounds.

## 6. Files

- `source/`: `AsyncTerminationDetection.tla`, `EWD998.tla`, `EWD998Chan.tla`,
  `EWD998ChanTrace.md`, `impl/src/*.java`, copied verbatim, with provenance in
  `source/PROVENANCE.md`.
- `rust/`: crate `bench_ewd998_termination`. CLI: `<spec|impl> <N> [fifo|bag|causal] [variant]`,
  default model `bag`, default variant `correct`. Each thread has an inbox channel built by main
  before any spawn (detectors, stubs and apps address each other). See `_tools/run-rust.sh`.
- `java/`: package `bench.ewd998_termination`, class `Ewd998` (args
  `<spec|impl>,<N>,<fifo|bag>,<variant>`), `Support` (the model argument and a thread registry
  that mints every id before any thread starts, as in `ironlock/`). Spawn order: spec
  `oracle, det0.., app0..`; impl `det0.., app0..`. Visible roles `app0..app{N-1}` on both sides
  and in both languages. See `_tools/run-java.sh`.
- `out/runs.log`: every plain-Must command and its output; `out/fault-check/`: the reachability
  crate; `out/fault-check.log`: every reachability command and its output.

## 7. Plain-Must exploration counts (both tools, one run each)

Model `bag` (asyn) throughout. Size = N (nodes), K = 1, R = 2.

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | N=2 | bag | 80 / 80 | 80 / 80 | yes |
| spec | N=3 | bag | 77688 / 77688 | not exhausted: stopped at the 100000-iteration cap (50000 / 50000 so far) | not compared |
| impl `correct` | N=2 | bag | 414 / 902 | 414 / 902 | yes |
| impl `correct` | N=3 | bag | 173384 / 489024 | not exhausted: stopped at the 100000-iteration cap (16495 / 83505 so far) | not compared |
| impl `no-black-on-receive` | N=2 | bag | 750 / 376 | 750 / 376 | yes |
| impl `no-black-on-receive` | N=3 | bag | 358840 / 218184 | not exhausted at 600 s (58392 / 40927 after 99319 iterations) | not compared |

At N = 2 every JMC run ended with "stack exhausted" and the counts are equal. At N = 3 no JMC
run was exhaustive: the brief's command caps JMC at 100000 iterations, fewer than the graphs
TraceForge counts at N = 3 (155376, 662408, 577024), and the third run also reached the 600 s
limit first. The partial JMC counts are not disagreements (no N = 3 JMC run finished), and no
run was repeated with a larger cap. The spec's blocked graphs are the oracle's silent branch (every
app then waits for `Trm`); the implementation's are the executions in which the R = 2 probes run
out before termination is detected (the apps wait for `Trm`, the detectors for messages).
N = 4 was not run: N = 3 took more than a minute on both tools (`out/runs.log`).

Commands, run from `B=/local/home/mkhoshechin/Documents/github-jmc/plan/benchmarks`, for
`N ∈ {2, 3}`, `v ∈ {correct, no-black-on-receive}`:

```
$B/_tools/run-rust.sh $B/ewd998-termination spec <N> bag
$B/_tools/run-rust.sh $B/ewd998-termination impl <N> bag <v>
$B/_tools/run-java.sh $B/ewd998-termination bench.ewd998_termination.Ewd998:spec,<N>,bag must 100000 600
$B/_tools/run-java.sh $B/ewd998-termination bench.ewd998_termination.Ewd998:impl,<N>,bag,<v> must 100000 600
```

Every count command and its output is in `out/runs.log`; every reachability command and its
output is in `out/fault-check.log`.
