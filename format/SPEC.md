# Summary interchange format, version 1

Normative: the summary files, join, witness files, measurements and exit codes as implemented by JMC `c1bbf657`
(`jmc/`) and TraceForge `d775f70` (`traceforge/`). Differences are per tool.

Verification comments: `J/` = `jmc/core/src/main/java/org/mpi_sws/jmc/`, `JL/` =
`jmc/core/src/test/java/org/mpi_sws/jmc/mp/launch/` (`MpLauncher`, `MpProgram`: test sources at this commit),
`T/` = `traceforge/traceforge/src/`.

## 1. Files

A summary file is UTF-8 JSON Lines: a header, then one line per distinct summary of one enumeration. Writers emit compact JSON and sort the summary lines by the bytes of their normal form (§3);
`conf-join --merge` requires that order, the plain join does not.

<!-- Verified: T/conformance/interchange.rs:1039-1046, T/conformance/export_p.rs,
T/conformance/online.rs:788-789; J/mp/summary/SummaryExporter.java:264-275, JL/MpLauncher.java:644;
T/conformance/interchange.rs:1901-1929. -->

## 2. Header

Written in this key order:

```
{"format":1,"minor":1,"tool":"jmc"|"traceforge","program":S,"size":S,"visible":[R...],"roles":[R...],
 "model":"p2p"|"asyn"|"cd","enumeration":E, ["seed":I,"iterations":I,] [online fields,] "encoding":1}
```

- `program`, `size`: free text, never compared (JMC `MpProgram`; TraceForge `--program`/`--size`, default
  `<bin>:<side>` and the ARGS joined by `,`).
- `visible`: the visible role names; `roles`: those plus the payload-nameable names (§5); both byte-sorted.
- `model`: `p2p`, `asyn`, `cd` (TraceForge `--model fifo|bag|causal`).
- `enumeration`: `complete`, `sampled`, `bounded` or `aborted`.
  - TraceForge `export` writes only `complete`. TraceForge `online --out` writes `complete` (exhausted), `bounded`
    (`--budget` reached) or `aborted` (first miss under `--stop-at-first`, failure, threshold).
  - JMC: `sampled` under `mp-random` (`aborted` on failure); under `must`, `aborted` on failure or timeout,
    else `complete` if no work was left, else `bounded`, also after a `--stop-at-first` stop.
- `seed`, `iterations`: JMC writes them for `sampled` files only.
- Online fields, before `encoding` in a file written beside an online check, ignored by readers:
  `first_miss_iteration` (1-based, 0 for no miss), `first_miss_wall_ms`, `uncovered_total`. TraceForge
  `online --threads N>1` always writes 0 (`uncovered_total` > 0 shows a miss). JMC `--split` gives the position in
  sequential segment order, or −1 when an earlier segment is missing (its measurement is then empty).

Reading. `format` and `encoding` must be 1. No `minor` means minor 0 (no `roles`; readers warn that roles were not
compared). TraceForge accepts `"minor":0` or `1`; JMC only `1` or none. Minor 1 needs `roles`; TraceForge checks them
(sorted, unique, valid names, covering `visible`) and requires string `tool`, `program`, `size`, `model`,
`enumeration`; JMC requires `enumeration` and `model`. Unknown header keys are ignored.

<!-- Verified: T/conformance/interchange.rs:754-778, 1558-1633; T/program.rs:216-223, 678-684;
T/conformance/online.rs:513-541, 783-787; T/conformance/online_p.rs:299-311; J/mp/summary/SummaryExporter.java:192-254;
JL/MpLauncher.java:609-648; J/strategies/must/MustStrategy.java:108-113; J/mp/summary/SpecificationFile.java:138-185;
T/conformance/report.rs:3072-3076. -->

## 3. Summary lines

```
{"statuses":{R:ST,...},"words":{R:[O,...],...},"counts":[C,...],"n":N}
```

- `statuses` and `words` have exactly the visible names as keys, in sorted order.
- A status is `done`, `blocked` or `err`. JMC: from the last graph event (Block `blocked`, Error `err`, else
  `done`). TraceForge: `err` if any label is a failed-assertion block, else `blocked` if the last label is a block,
  else `done`. Neither writes `err` at these commits (§6.5); both readers accept it.
- A word is the thread's observed sends and receives in program order: `{"s":V}` (value sent, no destination),
  `{"r":V}` (value read), `{"r":null}` (a non-blocking receive that read ⊥). JMC observes every send and receive of
  a visible thread; TraceForge skips events annotated invisible (§6.3).
- `counts`, event-major: for each visible name `t` (sorted), each event `e` of `t`'s word, and each other visible
  name `u` (sorted), the number of `u`'s word events causally before `e` (transitive program order, reads-from,
  spawn and join, through invisible events): Σ|w_t|·(T−1) entries, each at most |w_u|.
- `n`: execution count (JMC), never compared.

Normal form: `statuses`, `words`, `counts`, names sorted, no `n`, no whitespace, §4 values; readers re-render to it. TraceForge refuses an unknown line key and a repeated line; JMC
ignores unknown keys, skips blank lines, and holds a repeated line once.

Limits: words ≤ 2^32−1 events (TraceForge), counts ≤ 2^31−1 (JMC), counts ≤ 65535 in lines `--decide` classifies.

<!-- Verified: T/conformance/interchange.rs:534-598, 1416-1544, 1684-1689, 1222-1234, 1274-1279;
T/conformance/morphism.rs:323-365; T/conformance/obs.rs:333-378; T/conformance/compact.rs:161-200;
T/exec_graph.rs:1223; J/mp/summary/SummaryExporter.java:55-143; J/mp/summary/Summary.java:30-64;
J/mp/graph/ExecutionGraph.java:341; J/mp/summary/SpecificationFile.java:189-199, 230-298. -->

## 4. Values

Values compare by bytes.

| Kind | Encoding | JMC source | TraceForge source (derived `Debug`) |
|---|---|---|---|
| integer | JSON integer in signed 64 bits | `Byte`, `Short`, `Integer`, `Long` | integer literal |
| boolean | `true`, `false` | `Boolean` | `true`, `false` |
| string | JSON string, serde_json escaping | `String` (unpaired surrogate refused) | string literal |
| tagged value | `{"t":TAG,"f":[v1,...]}` | `record` with `@Tag(TAG)`, components in order | struct, tuple struct or enum variant; TAG = its name; field names dropped |
| unit variant | `{"t":NAME,"f":[]}` | `enum` constant (`name()`) | unit struct or unit variant |
| optional | `{"t":"None","f":[]}`, `{"t":"Some","f":[v]}` | `Optional` | `Option` |
| list | JSON array in order | `List` | `Vec`, array, slice |
| set | JSON array sorted by the bytes of the encoded elements | `Set` | `{a, b}` (`HashSet`, `BTreeSet`) |
| process identity | JSON string: the process's role, main as `"main"` (§5) | `Tid` | `ThreadId` |

Both encoders refuse floats, maps (TraceForge writes an empty map as `[]`), characters, sets with two equal
encodings and integers outside signed 64 bits. JMC also refuses `null`, arrays, untagged records and other classes;
TraceForge anonymous tuples (`()` included), ranges, `Debug` text outside the derived syntax or holding `..`, and two
summaries rendering one line. TraceForge cannot tell a hand-written `Debug` that prints accepted syntax from a derived
one: a derived (or equally faithful) `Debug` is the program author's obligation, not checked. `null` occurs only as
⊥.

Readers accept signed-64-bit integers, strings, booleans, arrays and objects with exactly `t` (string) and `f`
(array); both refuse `-0` (TraceForge parses it as a float). JMC requires the order `t`, `f` and refuses duplicate
keys; TraceForge takes either order. Arrays compare as written (a reader cannot tell a set).

<!-- Verified: J/mp/summary/MpValueEncoder.java:116-202, 216-255, 263-301, 311-322; J/mp/summary/Tag.java;
T/conformance/interchange.rs:178-301, 351-418, 1341-1381; J/mp/summary/SpecificationFile.java:300-517;
traceforge/Cargo.lock:1229-1230 (serde_json 1.0.154). -->

## 5. Identifiers and roles

- A role name is non-empty ASCII, not `main`, not starting with `~`, declared once.
- Visible roles (TraceForge `Program.visible`, JMC `MpProgram.visibleRoles()`, default all declared) have identical
  names in both programs.
- Payload-nameable roles (TraceForge `main_with_payload_names`, JMC `MpProgram.payloadNameableRoles()`, default
  none) are invisible roles whose identity may appear in a visible payload.
- An identity in a visible payload must be main, a visible or a payload-nameable role; others (daemons, unnamed
  processes) are refused. Invisible messages are not summarised and may carry any identity.
- JMC requires every non-daemon process to be spawned (`MpProcess.spawn`/`create`) under a declared role, each
  once; TraceForge only that each visible name names exactly one thread.
- Daemons are never visible.

Examples: TraceForge `invalid declared visible name: "x"`, `declared visible name is a daemon: "x"`; JMC
(prefixed `must: ` during the run) `declared role x was never spawned`, `payload names process x, an invisible role
that is not declared payload-nameable (MpProgram.payloadNameableRoles)`.

<!-- Verified: T/conformance/interchange.rs:422-446, 609-662; T/program.rs:88-109; JL/MpProgram.java;
J/strategies/must/MustStrategy.java:1677-1744, 1779-1833. -->

## 6. Constraints on the two programs

### 6.1 JMC shape rules (export and online check under `must`)

| Rule | Message (prefix `must: `; suffix `; the format requires main to spawn first, await all, and perform no message-passing event` for the first five) |
|---|---|
| main sends nothing | `main performs a send` |
| main receives nothing | `main performs a receive` |
| only main starts non-daemons | `process T was spawned by process P` |
| ... before its first completed await | `process T was spawned after main's first await` |
| only main awaits | `process T awaits another process` |
| main awaits all non-daemons (unless blocked at an await or assume) | `process(es) [..] were never awaited by main` |
| `p2p` or `asyn` | `task T uses model CD, which must does not support (p2p and asyn only)` |

Anyone may start daemons. Under `mp-random` the prefix is `mp-recording: `.

### 6.2 TraceForge rules

Export and online impose no shape rules. Only the
in-process check (`conf`) requires that no send or receive be causally before the creation of a declared visible
thread; paired programs must satisfy it. It panics with `conformance: visible thread `NAME` is created at CREATE, which is porf-after the communication event AFTER; §8
requires each declared visible thread to be spawned before its program communicates`.

### 6.3 Pairing restrictions (both programs; not checked by either tool)

- **Per-thread projection.** A paired program does not use TraceForge's per-event visibility (`send_msg_as`,
  `recv_msg_block_as`); JMC observes every send and receive of a visible thread, so hiding changes the projection.
- **Guards** are type tests only (JMC accepts any message predicate, TraceForge one over sender and tag).
- **⊥.** A non-blocking receive (JMC `Channel.tryRecv`, TraceForge `recv_msg`) may read ⊥ even with a message
  pending; both explorations cover that case.

### 6.4 Specification side

A specification is `complete` and raises no assertion; TraceForge `export` refuses assertions (`the program raised
N assertion failure(s)`), a non-exhausted run and a `--threshold` stop.

### 6.5 Failed executions

No tool writes a line for a failed execution. TraceForge `export` writes no file (exit 2); TraceForge `online --out`
and sequential JMC write `aborted` with the other executions' lines and exit 1; a failed JMC `--split` run writes no
export (exit 1), a successful one writes `complete`. Hence `err` is never written.

<!-- Verified: J/strategies/must/MustStrategy.java:541-574, 763-779, 1071-1075, 1101-1103, 1178-1180, 1606-1611,
1779-1786; J/strategies/mp/MpRecordingStrategy.java:214-220, 258-261, 284-286, 393-394, 403-417;
T/conformance/ctx.rs:451-456, 535-577, 1446-1448; T/conformance/compact.rs:323-330; T/conformance/obs.rs:355-369;
J/api/mp/Channel.java:141-163; T/lib.rs:1267-1300, 1393, 1519; T/must.rs:2491-2532;
T/conformance/interchange.rs:1006-1023; T/conformance/report.rs:3027-3062; T/conformance/online.rs:398-400, 479;
JL/MpLauncher.java:609-691, 1224-1252; J/strategies/must/split/SplitCoordinator.java:496-539. -->

## 7. The join and the online check

Coverage: an implementation summary is covered iff a specification summary has identical statuses and words and
counts componentwise ≤ the implementation's (equality covers).

`conf-join [--merge] [--decide [--decide-budget N]] spec.jsonl impl.jsonl` (TraceForge) reads either tool's files.

It refuses (exit 2) a malformed file, a repeated line, a non-`complete` specification (`the specification is `E`,
not `complete``), different models (`the two models differ: A and B`), `visible` lists (`the two files' `visible`
lists differ: …`) or minor-1 `roles` (`the two roles lists differ: …`), and under `--merge` an unsorted file. It
prints both headers, `verdict: conforms|violation|no verdict` with ` (sampled)` or ` (partial)` appended for a
`sampled` or `bounded`/`aborted` implementation (`verdict: conforms (partial)`), and each uncovered line as `<class>
<normal form>`: `no-bucket` (no specification summary of its shape), `no-slot` (shape held, not these values),
`not-found` (statuses and words held, no counts ≤ the line's).

Covered `sampled` gives `no verdict`. `--decide` adds `  decide: violation|false-alarm|
unresolved k=… k_min=… states=…` per line, and `  witness:` for a violation, without changing the verdict.

The online check (JMC `MpLauncher … must 0 … --spec F`, TraceForge `<bin> online --spec F [--out F]`) looks each
explored summary up in F; it refuses an F that is not `complete` or differs in model, `visible` or minor-1 `roles`; `--stop-at-first` stops after the first uncovered
summary. Verdicts: `conforms`, `reports`, `partial` (TraceForge also `refused`).

<!-- Verified: T/bin/conf-join.rs:16-130; T/conformance/interchange.rs:1119-1136, 1701-1845, 1983-2010;
T/conformance/report.rs:3099-3160, 3192-3240; T/conformance/online.rs:200-223, 552-581;
J/mp/summary/SpecificationFile.java:138-185; J/strategies/must/MustStrategy.java:104-113;
JL/MpLauncher.java:404-436, 576-603. -->

## 8. Witnesses

### 8.1 Text witness

`--witness <file|dir/>` (JMC with `--spec`, not `--split`; TraceForge `online`, `conf --engine stateful`, one thread)
writes the first uncovered execution, or one `witness-<n>.txt` per distinct uncovered summary.

Form: `uncovered summary (iteration <i>): <line>`, then per thread in creation order (main 0) `thread
"<name>"[tid=<t>]:` (`, daemon` before `]`) and tab-indented `(<t>, <k>): ` lines: `BEGIN`, `TCREATE(<c>:"<name>")`,
`SEND(<dest>, <value>)`, `RECV() [(<t'>, <k'>) <name>]`, `RECV() [TIMEOUT]` (⊥), `NONDET <i>` (two or more
options), `TJOIN(<c>)`, `BLK`, `END`; `k` counts printed lines. Both refuse a thread name with a control character or U+2028/U+2029;
TraceForge also any such string inside a SEND value, JMC an `ERROR` message.

### 8.2 Graph file (witness format 1)

One compact JSON line, keys in order: `witness` (1), `line` (the summary's normal form), `export` (the export
header without online fields), `run`, `join`, `nearest`, `threads`. `run` = `tool`, `tool_commit`, `entry`, `side`,
`args`, `model`, `options`, `source`, `iteration`, `producer`, `order`:

| | JMC | TraceForge |
|---|---|---|
| `entry` | program class | file name of `argv[0]` |
| `side` | `null` | `spec`/`impl` |
| `args` | the text after `:`, one element | the ARGS |
| `model` | `null` | `fifo`/`bag`/`causal` |
| `options` | `{}` | `seed`, `threshold` when given |

`tool_commit` is fixed at build (`JMC_COMMIT`/`TRACEFORGE_COMMIT`, else git, else `"unknown"`). `source` is
`{"root":…,"files":{path:md5}}` copied from a `SOURCE.md5` manifest (JMC: at the root of the program's classpath
directory; TraceForge: `<binary>.SOURCE.md5`), or `"unknown"` when that is missing, malformed or older than the
program. The manifest is written by `scripts/source-manifest.sh rust|java <dir> <dest>`: it is rooted at `<dir>`
(the `# root` line gives `<dir>` relative to the daavar tree) and its paths are relative to `<dir>` (Java class files
prefixed `classes/`). Writers emit `order` `first`; readers also accept `smallest`.

`threads[t]` = `{"name","daemon","events"}`, main first; events `{"e":"BEGIN"}`, `{"e":"TCREATE","child":c}`,
`{"e":"SEND","to":d,"v":V}`, `{"e":"RECV","mode":"block"|"try","rf":[t,k]|null}`, `{"e":"NONDET","n":n,"i":i}`
(TraceForge ranges add `"lo"`), `{"e":"TJOIN","child":c}`, `{"e":"END"}`, `{"e":"BLK"}`, `{"e":"ERROR","msg":m}`.
`(t,k)` equals the text witness's. Choice indices are raw: TraceForge `nondet()` 0 = false, JMC `chooseBoolean` 0 =
true. An invisible send whose value an encoder refuses carries `{"opaque":text}`. TraceForge writes no graph
with `Sample`, `Inbox`, symbolic labels or channel sends.

Validity. Both readers require: keys of the top level, `run`, `source`, threads and events as above, in order; thread 0 is `main`, not a daemon; each visible name is exactly one thread's; `BEGIN` occurs only
at event 0 and the terminal event only last; every thread but 0 is the child of exactly one `TCREATE` in another
thread; a `TJOIN` names a thread ending in `END`; every `rf` names a `SEND` to the receiver, none read twice,
`rf:null` only with `try`; `NONDET` has `n ≥ 2`, `0 ≤ i < n`; no opaque value is sent or read by a visible thread;
names hold no control character or U+2028/U+2029; `order` is `first` or `smallest`; porf is acyclic. Each reader adds:
- JMC: no `ERROR` event (terminal `END`/`BLK`); no `lo`; non-null `iteration` under `order: first`, online
  included; `join` any object or null; `source.files` values strings; `export` only checked for `visible`,
  `roles` (sorted, distinct, valid, roles ⊇ visible) and model `p2p|asyn|cd`.
- TraceForge: terminal `ERROR` accepted; `lo` a `u64`, TraceForge files only; null `iteration` for `smallest` or
  `producer: online`; unknown `run.options`/`join` members ignored; digests 32 lowercase hex, `files` byte-sorted;
  `export` only header keys, in order, no online field, `format` 1, string `tool`/`program`/`size`/`model`/
  `enumeration`, `minor`/`seed`/`iterations`/`encoding` unsigned decimal integers within u128 of any value (no sign, fraction, exponent,
  leading zero), `roles` if minor ≥ 1, sorted distinct
  lists, model `p2p|asyn|cd`.

Writers emit the §2 header; neither graph reader enforces all of §2.

`scripts/witness-render.py --check` also checks that `line` is the graph's projection.

### 8.3 Graph store

`--graphs G [--graphs-limit N]` (JMC under `must` with an export file; TraceForge `export`, `online --out`): line 1
`{"witness_store":1,"export":H,"run":R}` (`iteration` null), then one graph line per distinct summary at its first
occurrence, `join` and `nearest` null, at most N. Printed: `graph_store: written=W refused=R ms=T`; stderr `graphs: refused <line>: <reason>` per skipped graph. TraceForge accepts `--threads N>1` only with
`--graphs-limit 0`.

### 8.4 conf-join `--witness-dir D`

Options `[--impl-graphs P] [--spec-graphs Q] [--witness-limit N|all] [--witness-nearest N]` (defaults 20, 3,
`<file>.graphs.jsonl` if it exists). D must be absent or empty. A store is refused unless its first
line is an identity line whose `export` equals, as parsed JSON, its file's header without the online fields. In
printed order, up to the limit, each uncovered line that is not a Decide false alarm gets `D/<n>.graph.json`: the
stored graph, checked, with `join` = `tool`, `tool_commit`, `mode`, `class`, `decide`, `against`, `against_run`.
`nearest` is `[{"line":…,"graph":<file>|null}]` (Decide's effective group, at most N) only for a `not-found` line
under `--decide`, else null; `D/<n>.nearest-<j>.graph.json` exists only when the specification store holds a valid
graph of that line. `D/index.tsv`: `n class decide graph reason line`, reason `ok|limit|false-alarm|no-store|
no-graph|refused`. Stdout adds `  graph: …` per uncovered line.

### 8.5 Replay

`<bin> replay --graph G [--report R] [--allow-source-change] [--allow-tool-change] [-- ARGS]`;
`MpLauncher --replay G [--report R] [--allow-…] [<class>[:<args>]]`.

Both refuse before running an invalid or foreign graph, a `tool_commit` differing from the build's or `"unknown"`
(unless `--allow-tool-change`), and a differing or `"unknown"` source (unless `--allow-source-change`). TraceForge
compares only the `files` maps; JMC compares the whole `source`, `root` included. TraceForge refuses another
`entry`, a missing side or model, or ARGS after `--` other than the graph's, then restores side, model, seed and
threshold from the graph. JMC requires `side` null, `model` null, `options` `{}` and at most one `args` element, and
refuses an explicit program argument other than the graph's. The replay forces each `rf` and choice index and stops
at the first deviation (`event`, `dest`, `value`, `options`, `child`, `unavailable`, `early-end`, `overrun`,
`status`). Output: `replay: reproduced (<E> events, <T> threads)` or `replay: deviation at (<t>, <k>) thread
<name>: <kind>: expected …, got …`; `--report` writes
`{"replay":1,…}` with `graph`, `result`, `at`, `thread`, `kind`, `expected`, `actual`, `warnings`.

<!-- Verified: J/mp/summary/WitnessWriter.java:172, 192-289; T/conformance/execution_witness.rs:97-110, 192-238;
J/mp/summary/GraphJson.java; J/mp/summary/RunIdentity.java:44-174; T/conformance/witness_graph.rs:40-131,
196-200, 273, 355-441; T/conformance/witness_json.rs:234-709; scripts/source-manifest.sh:5-42;
T/program.rs:269-302, 355-511; T/conformance/witness_join.rs:28-72, 240-258, 380-469;
JL/MpLauncher.java:1328-1530, 1605-1770; J/strategies/mp/GraphReplayStrategy.java;
T/conformance/witness_replay.rs; J/api/mp/Nondet.java:51. -->

## 9. Measurements

The block (`--- measurements`, then `key: value` lines) is printed:
- JMC: every `must` run past argument parsing, a refused specification included (`stop_reason: error`); a `--split`
  coordinator prints its own, workers none; never under `mp-random`; replay unless refused before running.
- TraceForge: `count`; `export` unless refused before the run (bad arguments, index, model); `conf`; `online`
  unless refused before the exploration; `conf-join` always (refused: `stop_reason: error`); replay after a result, unless writing a requested `--report` failed. Shared keys, in order: `wall_ms`, `explore_ms`, `summarise_ms`, `index_ms`,
`lookup_ms`, `export_ms`, `graphs_complete`, `graphs_blocked`, `summaries_distinct`, `iterations`, `stop_reason`,
`peak_rss_kb`, `max_message_events`, `max_porf_pairs`, `first_miss_iteration`, `first_miss_wall_ms`,
`uncovered_total` (these three only with a specification), `max_graph_events`, `mean_graph_events`,
`max_visible_events`, `mean_visible_events`, `max_summary_bytes`, `max_porf_count`. Tool-specific keys follow; readers ignore unknown keys; empty means not measured.

Definitions:
- `graphs_complete`, `graphs_blocked`: distinct canonical graphs ending complete or blocked.
- per summarised execution (never per distinct summary; a refused execution counts in none):
  `max_graph_events`/`mean_graph_events` its witness event lines over all threads, `max_message_events` its sends
  and receives over all threads, `max_visible_events`/`mean_visible_events` its visible words' events.
- `max_summary_bytes`: normal-form UTF-8 bytes; `max_porf_count`: the largest count entry; `max_porf_pairs`: the
  most non-zero count entries in one summary.
- `stop_reason`: `exhausted`, `budget`, `first-miss` or `error`; JMC also `timeout`, TraceForge also `threshold`.
- `uncovered_total` in `conf-join`: Σ `n` over the uncovered lines when each carries an integer `n` in
  1 ..= 2^64−1, and 0 with no uncovered line.
  A missing or out-of-range `n` empties it without refusing the line; a refused join leaves it empty; for a
  `sampled` or partial implementation 0 does not establish conformance.

Replay prints only `wall_ms`, `peak_rss_kb`, `replay_events`.

<!-- Verified: T/conformance/measure.rs:22-71, 88-107, 176-231, 802-842; T/conformance/report.rs:3268-3314;
T/conformance/interchange.rs:1413-1427, 1546-1550; J/strategies/must/MustStrategy.java:541-554;
JL/MpLauncher.java:169-195, 407, 683-686, 1081-1144, 1501-1513, 1967-1971; T/program.rs:445-458, 487-503, 745-750. -->

## 10. Exit codes

| Command | 0 | 1 | 2 |
|---|---|---|---|
| TraceForge `export` | written | — | bad arguments, refusal, assertion, store not written |
| JMC export | completed (timeout: `aborted`) | failure, incl. shape/role/value refusals | bad arguments |
| TraceForge `online` | verdict | failure | bad arguments, refused specification or declaration |
| JMC online | verdict | failure, incl. refusals during the run | bad arguments, refused specification |
| `conf-join` | conforms, no verdict | violation | bad arguments, refused input or store, witness output not writable |
| TraceForge `conf` | conforms | reports | inconclusive, error |
| replay | reproduced | deviation, failure (JMC: unwritable report) | refused (TraceForge: unwritable report) |

<!-- Verified: T/program.rs:752-761, 812-818, 968-976, 495; T/conformance/online.rs:214-223;
T/conformance/interchange.rs:1324-1329; T/bin/conf-join.rs:106-129; T/conformance/witness_join.rs:342-372, 446, 465, 469;
JL/MpLauncher.java:169-195, 683-691, 1440-1500, 2033-2065. -->

## 11. Golden files

`golden/` holds 148 JMC exports of `A2Store` and `A3Lock` variants and sizes:
- `*.must.jsonl` (74) are `complete` Must enumerations; `*.random.jsonl` (74) are `sampled` runs (`seed` 1,
  300 iterations).
- They show the minor-0 header, model `p2p`, `n`, tagged values (`Put`, `Grant`), unit variants (`Ack`, `None`),
  empty `counts` for one visible role (`*-1-*`), `blocked` (`A2Store-silent-put-*`, `A3Lock-never-grant-*`).
- None is a TraceForge file or minor 1 or holds `{"r":null}`, `err`, a string, boolean, set or `Some`; 77 are not
  in byte order (`--merge` refuses them).

They do not validate interoperability: that needs both writers' files with minor-1 roles, ⊥, every §4 value kind,
counts through invisible events, and projection-checked graphs.
