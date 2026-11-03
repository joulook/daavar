# ewd998(N) — Safra's termination detection against a termination oracle (`ewd998-termination`)

**Communication model:** bag (`asyn`: unordered delivery; `EWD998Chan` receives from any inbox index), the model
the crate declares. **Languages:** Rust (TraceForge's Must API), in `rust/`; Java twin, in `java/`; context-free
Java port, in `java-cf/`. **Visible threads:** `app0 … app(N−1)`; the detectors `det0 … det(N−1)` and the
specification's `oracle` are invisible. **Parameters:** `N` = the number of nodes (default 3; smallest interesting
size `N = 2`; `2 ≤ N ≤ 8`: the Rust program keeps `got` in a byte and adds node `from` as bit `1 << from`, so the set
semantics below hold only for nodes `0 … 7`; the Java twin's `int` mask has no such limit at these sizes); fixed: `K = 1` payload per node, `R = 2` probes. ARGS `<N> [variant]`, variant `correct` (default) or
`no-black-on-receive`. **Campaign status:** marked **undecidable** (`cluster/campaign/inputs/undecidable.tsv`): the
Java twin renders the absent `sent` field of `Idle` as `-1` (`java/`) while the Rust side renders `None` otherwise, so
every graph is uncovered in the cross-language and online tables (XON, XJ, XT, XTON); the verdicts below are those of
the Rust pairs. **Source:** TLA+ Examples, `specifications/ewd998/` (`AsyncTerminationDetection.tla`, `EWD998.tla`,
`EWD998Chan.tla`, `impl/src/EWD998.java`).

Notation: as in `../../synthetic/naive/README.md`. Every receive is blocking (`recv^asyn()`) and accepts any message
addressed to the executing thread; `if m = Pl(f) then …` is a case split on the message received. `loop … stop` repeats
until `stop`. `got` is a set of nodes (a bitmask in the code), `⊥` an absent value (`None`).

The application of node `i`, the same on both sides (only the thread `REP_i` it reports to differs):

```
App_i ≜ app_i: sent := ⊥; got := ∅
               PHASE_i
               for t := 1..N:
                  m := recv^asyn()
                  if m = Trm then stop
                  if m = Pl(f) then got := got ∪ {f}; PHASE_i

PHASE_i ≜ if sent = ⊥ then
             c := nondet({0, …, N−1})
             if c > 0 then j := (i + c) mod N; send^asyn(det_j, Pl(i)); sent := j
          send^asyn(REP_i, Idle(i, sent, got))
```

Each app starts active, may send its one payload to the detector of another node (`c = 0`: no payload), reports
`Idle` with where its payload went and whom it has heard from, then waits: a `Pl` reactivates it (a new phase, a new
`Idle` report), a `Trm` ends it. A payload travels `app_i → det_j → app_j` on both sides.

## The pairs

The specification of both pairs (`REP_i = oracle`); spawn order `oracle, det0.., app0..`:

```
Spec(N) ≜ oracle: view := [⊥]^N
                  loop:
                     Idle(v, s, g) := recv^asyn()
                     if view[v] = ⊥ ∨ |g| > |view[v].got| then view[v] := (s, g)
                     if TERMINATED(view) then
                        if nondet({true, false}) then
                           for a := 0..N−1: send^asyn(app_a, Trm)
                           for d := 0..N−1: send^asyn(det_d, Stop)
                        stop
        ‖ ‖_{j<N} det_j: loop: m := recv^asyn()
                               if m = Pl(f) then send^asyn(app_j, Pl(f))
                               if m = Stop then stop
        ‖ ‖_{i<N} App_i

TERMINATED(view) ≜ every view[i] ≠ ⊥
                   ∧ ∀i: view[i].sent = j ≠ ⊥ ⇒ i ∈ view[j].got        -- every payload sent was received
                   ∧ ∀i, k: k ∈ view[i].got ⇒ view[k].sent = i         -- every receive was reported sent
```

`AsyncTerminationDetection.tla` as a program. The stubs `det_j` only relay payloads. The oracle keeps each node's
latest report (the one with the most receives) and, after each report, tests whether the reports form a consistent
cut in which every node is idle and no payload is in flight; then it may announce `Trm` to every app (and `Stop` to
every stub) or stay silent, and in both cases it ends. So every `Trm` is causally after every app's last `Idle`; when
the oracle stays silent every app ends blocked on its last receive.

The implementation (`REP_i = det_i`); spawn order `det0.., app0..`; `det_i` is the only thread that differs from the
specification:

```
Impl_V(N) ≜ ‖_{i<N} det_i: D_V(i)  ‖  ‖_{i<N} App_i

D_V(i) ≜ counter := 0; black := false; out := 1; counted := false; tok := ⊥; rounds := 0
         if i = 0 then send^asyn(det_{N−1}, Tok(0, false)); rounds := 1
         loop:
            m := recv^asyn()
            if m = Pl(f) then counter := counter − 1; RECV_V; out := out + 1; send^asyn(app_i, Pl(f))
            if m = Idle(_, s, _) then
               if s ≠ ⊥ ∧ ¬counted then counter := counter + 1; counted := true
               out := out − 1
            if m = Tok(q, b) then tok := (q, b)
            if m = Stop then stop
            if tok = (q, b) then
               if i ≠ 0 then                                               -- PassToken, only when idle
                  if out = 0 then send^asyn(det_{i−1}, Tok(q + counter, b ∨ black)); black := false; tok := ⊥
               else if ¬(¬b ∧ ¬black ∧ q + counter = 0) then               -- inconclusive round
                  if rounds < 2 then send^asyn(det_{N−1}, Tok(0, false)); black := false; rounds := rounds + 1
                  tok := ⊥
               else if out = 0 then                                        -- termination detected
                  for a := 0..N−1: send^asyn(app_a, Trm)
                  for d := 1..N−1: send^asyn(det_d, Stop)
                  stop
               -- conclusive but node 0 active: keep the token, evaluate again after the next message
```

`det_i` is Safra's node of `EWD998.java` minus the application: `counter` is sends minus receives (a send is counted
when the app reports idle, once), `black` the node's colour, `out` the payloads forwarded to `app_i` not yet followed
by an `Idle` (plus 1 for the initial activity), so the node is active while `out > 0`. The token goes `i → i−1`; node
0 starts a round at once (the Java initiator's first token is black), starts a second one if the first is
inconclusive, and after `R = 2` probes only relays payloads. A conclusive round with node 0 idle sends `Trm` to every
app and `Stop` to `det1 …`; detectors never stopped end blocked (invisible).

### correct — expected: **conforms**

```
RECV_correct ≜ black := true
```

The apps are the same program on both sides and each payload is relayed by one thread per node on both sides, so
every app word up to its last `Idle` and every payload order is matched by the stub graph that reads payloads in the
detectors' order. By Safra's theorem a conclusive round implies that every node is idle and no payload is in flight,
and stays so; every detector passed that round's token while idle, i.e. after reading its app's last `Idle`, so every
`Trm` is causally after every app's last `Idle`, as in the specification. If the two probes run out, no `Trm` is sent
and every app ends blocked, which the oracle's silent branch covers.

### no-black-on-receive — expected: **violation** (`N ≥ 2`)

```
RECV_no-black-on-receive ≜ skip                                    ◀ changed: Rule 3 (RecvMsg's colour' = black) dropped
```

A detector that does not turn black on a receive lets `det0` announce while another node is active. At `N = 2`:
`det0` sends the token to `det1`; `app1` reports `Idle(1, ⊥, ∅)` and `det1` passes `Tok(0, false)`; `app0` sends
`Pl(0)` to node 1 and reports `Idle(0, 1, ∅)` (`counter0 = 1`); `det1` forwards the payload, `app1` receives it and
sends `Pl(1)` to node 0; `det0` counts that receive (`counter0 = 0`) and stays white; `app0` receives it and reports
`Idle(0, 1, {1})`; the token is white with `q + counter0 = 0` and node 0 is white and idle, so `det0` sends `Trm`.
Uncovered: `app0: S(Pl) S(Idle(0,1,∅)) R(Pl(1)) S(Idle(0,1,{1})) R(Trm)`, `app1: S(Idle(1,⊥,∅)) R(Pl(0)) S(Pl)
S(Idle(1,0,{0})) R(Trm)`, in a graph where `app1`'s last `S(Idle)` is not before `app0`'s `R(Trm)`. In the
specification the oracle cannot announce before it has read `Idle(1, 0, {0})` (until then `app0`'s payload to node 1
is reported sent and not received), so every specification graph with this word orders that `Idle` before both
`R(Trm)` (order). With the correct rule `det0` turns black on `Pl(1)` and the round is inconclusive.

## What it is for

A literature protocol with an invisible, stateful detection layer under a visible application: the implementation's
detectors and token rounds multiply its graphs (414 at `N = 2` against the specification's 80, 173 384 against
77 688 at `N = 3`, README.orig §7), many of them blocked when the probes run out, while the visible apps are the same
program on both sides. The fault is a one-line change deep in the invisible layer whose only visible effect is a
missing order between two apps. In the campaign it is also the case where a value-rendering difference between the
Java and Rust twins makes the cross-language rows undecidable.
