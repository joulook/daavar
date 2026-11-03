# Election: leader election then two-phase commit (all-to-all and ring)

**Family:** popl-conf application  **Source:** the TraceForge fixtures `2pc/leader` and
`2pc/ring` ("A5", ruling Q11), `traceforge/src/conformance/demo.rs` at `a3a7e1b`, lines 601–739,
744–872 and 977–1105 (`EIn`, `PIn`, `Setup`, `le_participant`, `le_announce`, `le_elector_with`,
`le_oracle`, `le_impl*`, `le_spec`, `RIn`, `ring_elector`, `ring_impl`), ported to `rust/`.
**Expected verdict:** `leader conf`, `ring conf`: **CONFORMS** (a real election refines a free
choice); `leader split`, `ring split`: **DOES NOT REFINE** (no participant is told it leads);
`leader silent`: **DOES NOT REFINE** (the winner is never told; everyone ends blocked).

## 1. The protocol

Roles: visible participants `p0..p{n-1}`, the same code on both sides; invisible `oracle` (spec)
or electors `e0..e{n-1}` (impl). A participant receives its role, `BeLeader(peers)` or
`BeFollower(leader)`; a follower sends `Vote(true)` to the leader (a fixed vote); the leader,
after `n-1` votes, sends `Decision(all_yes)` to every peer. Its loop is order-tolerant (a peer's
vote may overtake its role) and bounded by `n` receives. Messages: `PIn::{BeLeader(Vec<ThreadId>),
BeFollower(ThreadId), Vote(bool), Decision(bool)}` (thread ids are participants, exported as
role names); invisible `EIn::{Setup(Setup), Elect(usize)}`, `RIn::{Setup(Setup), Tok(usize),
Ann(usize)}`, `Setup { me, electors, participants }`, sent by `main` (Rust) or `init` (Java,
§8). Parameter: `n`. Smallest interesting size: `n = 2`.

## 2. The specification program

`le_spec(n)`: the `oracle` reads `Setup` and picks the leader by a ladder of boolean `nondet()`s
(`p_i` for the first `true`, else `p{n-1}`), then announces every participant's role. So exactly
one participant leads, the others follow it, and every role is causally independent of the
participants' events.

## 3. The implementation program

`leader`: `n` electors exchange `Elect(me)` all-to-all; each, having heard the `n-1` others,
takes the maximum index (its own included) and announces participant `me`'s role. `ring`: `e0`
sends `Tok(0)` to its successor; each elector forwards `Tok(max(t, me))`; when the token returns,
`e0` announces and sends `Ann(leader)` round the ring, each elector announcing its participant's
role. Participants are spawned first on both sides (F41).

## 4. Where they differ, and why the verdict is what it is

`leader conf`, `ring conf`: every elector announces leader `n-1`, which the oracle can choose;
the roles are causally after invisible events only, as in the spec. **CONFORMS**.

`leader split` (`n = 2`): an elector omits its own index, so `e0` elects 1 and `e1` elects 0:
`p0: R(BeFollower("p1")) S(Vote(true)) R(Vote(true))` and `p1: R(BeFollower("p0")) ...`. No
participant receives `BeLeader`; the spec always has exactly one leader. Component: **signature
value**; every graph is uncovered (9/9 at `n = 2`; all 882 blocked graphs at `n = 3`).

`ring split` (`n = 2`): each elector decides alone from the token value it saw (`e1` sees
`Tok(0)`, `e0` sees `Tok(1)`), so `p1` follows `p0` and `p0` follows `p1`. Component: **signature
value** (6/6 at `n = 2`).

`leader silent` (`n = 2`): the winner's elector (`e{n-1}`) never announces, so `p{n-1}` gets
votes but no role, and nobody decides: every participant ends blocked, each word a proper prefix
of a spec word. Component: **signature status** (3 graphs, 196 at `n = 3`).

## 5. Variants and faults

| variant (Rust ARGS / Java) | change | expected verdict | uncovered behaviour |
|---|---|---|---|
| `leader conf` / `leader-conf` | `le_impl(n, false)` | CONFORMS | none |
| `leader split` / `leader-split` | elector ignores its own index | DOES NOT REFINE | two followers, no leader (value) |
| `leader silent` / `leader-silent` | the winner's elector never announces | DOES NOT REFINE | all blocked (status) |
| `ring conf` / `ring-conf` | `ring_impl(n, false)` | CONFORMS | none |
| `ring split` / `ring-split` | ring elector decides from its token alone | DOES NOT REFINE | two followers (value) |

All against the one spec. Rust `conf` at `n = 2` (`out/runs.log`): `leader conf` silence, `leader
split` 9 candidate violations, `leader silent` 3, `ring conf` silence, `ring split` 6.

## 6. Files

- `source/`: none; the source is the TraceForge fixture named above (see `rust/src/main.rs`).
- `rust/`: the Must-API Rust crate on TraceForge's shared main; ARGS `leader {conf|split|silent} <n>` or `ring {conf|split} <n>`.
- `java/`: the Java twin, package `bench.election` (§8).
- `out/runs.log`: every command of the Java-twin round and its output; `out/export/`: the summary files.

## 7. Plain-Must exploration counts (both tools, one run each)

Model fifo (JMC p2p). The fixtures' `--threshold 65536` and recorded seeds were not passed
(plain `count` explores the whole space; the counts do not depend on them).

| side | n | TraceForge ARGS | JMC args | TraceForge complete / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|---|
| spec | 2 | `leader conf 2` | `spec,2,fifo` | 4 / 0 | 4 / 0 | yes |
| impl | 2 | `leader conf 2` | `impl,2,fifo,leader-conf` | 6 / 0 | 6 / 0 | yes |
| impl | 2 | `leader split 2` | `impl,2,fifo,leader-split` | 9 / 0 | 9 / 0 | yes |
| impl | 2 | `leader silent 2` | `impl,2,fifo,leader-silent` | 0 / 3 | 0 / 3 | yes |
| impl | 2 | `ring conf 2` | `impl,2,fifo,ring-conf` | 4 / 0 | 4 / 0 | yes |
| impl | 2 | `ring split 2` | `impl,2,fifo,ring-split` | 6 / 0 | 6 / 0 | yes |
| spec | 3 | `leader conf 3` | `spec,3,fifo` | 18 / 0 | 18 / 0 | yes |
| impl | 3 | `leader conf 3` | `impl,3,fifo,leader-conf` | 588 / 0 | 588 / 0 | yes |
| impl | 3 | `leader split 3` | `impl,3,fifo,leader-split` | 0 / 882 | 0 / 882 | yes |
| impl | 3 | `leader silent 3` | `impl,3,fifo,leader-silent` | 0 / 196 | 0 / 196 | yes |
| impl | 3 | `ring conf 3` | `impl,3,fifo,ring-conf` | 24 / 0 | 24 / 0 | yes |
| impl | 3 | `ring split 3` | `impl,3,fifo,ring-split` | 0 / 28 | 0 / 28 | yes |
| spec | 4 | `ring conf 4` | `spec,4,fifo` | 96 / 0 | 96 / 0 | yes |
| impl | 4 | `ring conf 4` | `impl,4,fifo,ring-conf` | 192 / 0 | 192 / 0 | yes |
| impl | 4 | `ring split 4` | `impl,4,fifo,ring-split` | 0 / 120 | 0 / 120 | yes |

`leader *` at `n = 4` not run (1 839 744 graphs for `leader conf` under plain Must, recorded in
the fixture's comments). Commands (exactly, `$R` = `/local/home/mkhoshechin/tmp/bench-target/release`):

```
$R/tf_election count spec --model fifo -- leader conf <n>
$R/tf_election count impl --model fifo -- <leader|ring> <conf|split|silent> <n>
java -Xss4m -cp $D/benchmarks/election/java/out:$(cat $D/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher \
     'bench.election.Election:spec,<n>,fifo' must 0 1 <fresh report dir> - 600 </dev/null
java ... 'bench.election.Election:impl,<n>,fifo,<variant>' must 0 1 <fresh report dir> - 600 </dev/null
```

## 8. Java twin

`java/bench/election/Election.java` (an `MpProgram`) and `Support.java`. Arguments
`Election:spec,<n>,<fifo|bag>[,<variant>]` and `Election:impl,<n>,<fifo|bag>,<variant>`, variant
`leader-conf`, `leader-split`, `leader-silent`, `ring-conf`, `ring-split` (Rust's two words
joined by `-`). Visible `p0..p{n-1}`; spawn order participants, then `e0..` or `oracle`, then
`init`. Messages are `@Tag` records with the Rust variant names and field order: `BeLeader(List<Tid>)`,
`BeFollower(Tid)`, `Vote(boolean)`, `Decision(boolean)`; invisible `Setup(int, List<Tid>, List<Tid>)`
(tag `Setup`), `EIn::Setup` and `RIn::Setup` (records `ESetup`, `RSetup`, tag `Setup`, wrapping a
`Setup`), `Elect(int)`, `Tok(int)`, `Ann(int)`. The oracle's ladder is `Nondet.chooseBoolean()`.

One departure of form: Rust's `main` sends every elector its `Setup` (or the oracle a bare
`Setup`) after spawning; a JMC main may perform no message-passing event, so the invisible
thread `init`, spawned last, sends the same messages in the same order. An elector's `Setup`
still races its peers' messages exactly as in Rust; the counts are equal (§7).

Cross-language check at `n = 2` (exports in `out/export/`, `conf-join <spec file> <impl file>`):

| variant | Rust spec vs Java spec, both ways | Rust impl vs Java impl, both ways | Rust spec ⊒ Java impl | Java spec ⊒ Rust impl | hand verdict |
|---|---|---|---|---|---|
| `leader-conf` | conforms | conforms | conforms | conforms | CONFORMS |
| `leader-split` | conforms | conforms | violation, 3 uncovered | violation, 3 uncovered | DOES NOT REFINE |
| `leader-silent` | conforms | conforms | violation, 1 uncovered | violation, 1 uncovered | DOES NOT REFINE |
| `ring-conf` | conforms | conforms | conforms | conforms | CONFORMS |
| `ring-split` | conforms | conforms | violation, 3 uncovered | violation, 3 uncovered | DOES NOT REFINE |

Not done: model `bag`; the joins at `n = 3`.
