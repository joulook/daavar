# Summary interchange format, version 1

Normative (revision 5.2): the summary files, readers, pairing check, join, witness files, measurements and exit
codes as implemented by JMC `bf742b28` (`jmc/`) and TraceForge `f9784da` (`traceforge/`). Differences are per tool.

Verification comments: `J/` = `jmc/core/src/main/java/org/mpi_sws/jmc/` (the launcher, `MpLauncher`, `MpProgram`,
`NamedCli`, is `J/mp/cli/`, main sources), `T/` = `traceforge/traceforge/src/`. A comment `Verified (rev 5):` was
checked at JMC `eb58ec75` and TraceForge `c2a2b69` (revisions 5 and 5.1), a comment `Verified (rev 5.2):` at the
pins above; phase 4b changed, among the production-source files, only the launcher, `PairingCheck`, `SpecificationFile` (JMC), `interchange.rs`,
`pairing.rs`, `program.rs` and `mod.rs` (TraceForge), so the rev-5 line numbers of other files still hold. A comment `Verified:` was checked at the revision-4 pins, JMC `c1bbf657` and TraceForge
`d775f70`, and its line numbers are those commits'; in it `JL/` = `jmc/core/src/test/java/org/mpi_sws/jmc/mp/launch/`
(the launcher's test sources at `c1bbf657`).

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
compared); otherwise `minor` is 1, never an explicit 0, and `roles` is present exactly when `minor` is. Both readers
require string `tool`, `program`, `size`, `model`, `enumeration` and refuse an unknown key and any header not in the
writers' form (§7.1). TraceForge checks `roles` itself (sorted, unique, valid names, covering `visible`); JMC compares
`visible`, `roles` and `model` with the program's.

<!-- Verified: T/conformance/interchange.rs:754-778, 1558-1633; T/program.rs:216-223, 678-684;
T/conformance/online.rs:513-541, 783-787; T/conformance/online_p.rs:299-311; J/mp/summary/SummaryExporter.java:192-254;
JL/MpLauncher.java:609-648; J/strategies/must/MustStrategy.java:108-113; J/mp/summary/SpecificationFile.java:138-185;
T/conformance/report.rs:3072-3076. -->
<!-- Verified (rev 5): Reading: T/conformance/interchange.rs:1688-1753, 1767-1825; J/mp/summary/SpecificationFile.java:156-207,
284-336; J/mp/cli/MpLauncher.java:515-519. -->

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

Normal form: `statuses`, `words`, `counts`, names sorted, no `n`, no whitespace, §4 values. Both readers accept a
line only as its normal form, optionally followed by a compact `n` as its last member, and refuse a repeated line
(§7.1).

Limits: words ≤ 2^32−1 events (TraceForge reader), counts ≤ 2^31−1 (JMC reader), counts and words ≤ 2^31−1 under
the pairing check (§7.2), counts ≤ 65535 in lines `--decide` classifies.

<!-- Verified: T/conformance/interchange.rs:534-598, 1416-1544, 1684-1689, 1222-1234, 1274-1279;
T/conformance/morphism.rs:323-365; T/conformance/obs.rs:333-378; T/conformance/compact.rs:161-200;
T/exec_graph.rs:1223; J/mp/summary/SummaryExporter.java:55-143; J/mp/summary/Summary.java:30-64;
J/mp/graph/ExecutionGraph.java:341; J/mp/summary/SpecificationFile.java:189-199, 230-298. -->
<!-- Verified (rev 5): normal form and limits: T/conformance/interchange.rs:1493-1517, 1536-1545, 1599, 1641-1664,
1873-1903; J/mp/summary/SpecificationFile.java:40-46, 210-229, 344-371; T/conformance/pairing.rs:44-51. -->

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

Both encoders refuse floats, maps (empty ones included), characters, sets with two equal encodings and integers
outside signed 64 bits. TraceForge tells an empty set from an empty map by the message type's declared shape
(`#[derive(SummaryShape)]`, `conformance::declare::<T>()`): a `{}` is an empty set (`[]`) where the shape has a set
and is refused elsewhere, in an undeclared type included; the text witness still writes an undeclared `{}` as `[]`. JMC also refuses `null`, arrays, untagged records and other classes;
TraceForge anonymous tuples (`()` included), ranges, `Debug` text outside the derived syntax or holding `..`, and two
summaries rendering one line. TraceForge cannot tell a hand-written `Debug` that prints accepted syntax from a derived
one: a derived (or equally faithful) `Debug` is the program author's obligation, not checked. `null` occurs only as
⊥.

Readers accept signed-64-bit integers, strings, booleans, arrays and objects with exactly `t` (string) and `f`
(array); both refuse `-0` (TraceForge parses it as a float), the order `f`, `t` and a duplicate key outside `n`
(TraceForge by the writer-form comparison, §7.1). Arrays compare as written (a reader cannot tell a set).

<!-- Verified: J/mp/summary/MpValueEncoder.java:116-202, 216-255, 263-301, 311-322; J/mp/summary/Tag.java;
T/conformance/interchange.rs:178-301, 351-418, 1341-1381; J/mp/summary/SpecificationFile.java:300-517;
traceforge/Cargo.lock:1229-1230 (serde_json 1.0.154). -->
<!-- Verified (rev 5): maps and shapes: T/conformance/interchange.rs:433-470, 548-575; T/conformance/shape.rs:52-66;
readers: T/conformance/interchange.rs:1641-1664; J/mp/summary/SpecificationFile.java:785-830. -->

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

### 6.3 Pairing restrictions (both programs)

Checked by the pairing check (§7.2), on the two files (`conf-join`) or on the specification file and the program
(both online checks; TraceForge `online` checks C3 as model equality only and not C13):
- **C2, names and roles.** `visible` byte-sorted with distinct names; `visible` and minor-1 `roles` equal on both
  sides.
- **C3, model.** Equal on both sides, `p2p` or `asyn`.
- **C4, enumeration.** The specification is `complete`.
- **C6, repeated lines.** Each summary once per file; under `--merge` (TraceForge) also in byte order.
- **C10, numeric bounds.** Counts ≤ 2^31−1; words ≤ 2^31−1 events. TraceForge's check sees only counts that fit
  in 64 bits (a count such as `18446744073709551616` passes it, and the reader refuses it); JMC's sees every
  integer literal.
- **C13, tag arity.** A tag used on both sides is used with compatible field arities: TraceForge, equal arity sets
  in the two files; JMC, the specification's arities a subset of those the program declares (its `@Tag` records and
  enums implementing `MpMessage` in the program class's package, and the types their components reach). A tag on
  one side only is a warning.
- **C18, identities.** A value tagged `ThreadId` or `Tid` is a warning. Otherwise a role name cannot be told from
  another string: sending identities only as role names (§5) stays the programs' obligation.

Not checked by the pairing check (other enforcement noted):
- **Per-thread projection.** A paired program does not use TraceForge's per-event visibility (`send_msg_as`,
  `recv_msg_block_as`); JMC observes every send and receive of a visible thread, so hiding changes the projection.
- **Guards** are type tests only (JMC accepts any message predicate, TraceForge one over sender and tag).
- **⊥.** A non-blocking receive (JMC `Channel.tryRecv`, TraceForge `recv_msg`) may read ⊥ even with a message
  pending; both explorations cover that case.
- **Maps, arrays, tuples.** Both encoders refuse maps (§4); JMC refuses Java arrays, TraceForge anonymous tuples,
  but TraceForge encodes a Rust fixed array as a list: a paired program uses lists.
- **Shapes.** §6.1 binds the Java program and §6.2 is checked only by TraceForge `conf`; neither tool checks the
  other program's shape.

### 6.4 Specification side

A specification is `complete` and raises no assertion; TraceForge `export` refuses assertions (`the program raised
N assertion failure(s)`), a non-exhausted run and a `--threshold` stop.

### 6.5 Failed executions

No tool writes a line for a failed execution. TraceForge `export` writes no file (exit 2); TraceForge `online --out`
and sequential JMC write `aborted` with the other executions' lines and exit 1 (JMC named form: 1 for a failure of
the program, 2 for a refusal); a failed JMC `--split` run writes no export (exit 1; named form by outcome, §10), a
successful one writes `complete`. Hence `err` is never written.

<!-- Verified: J/strategies/must/MustStrategy.java:541-574, 763-779, 1071-1075, 1101-1103, 1178-1180, 1606-1611,
1779-1786; J/strategies/mp/MpRecordingStrategy.java:214-220, 258-261, 284-286, 393-394, 403-417;
T/conformance/ctx.rs:451-456, 535-577, 1446-1448; T/conformance/compact.rs:323-330; T/conformance/obs.rs:355-369;
J/api/mp/Channel.java:141-163; T/lib.rs:1267-1300, 1393, 1519; T/must.rs:2491-2532;
T/conformance/interchange.rs:1006-1023; T/conformance/report.rs:3027-3062; T/conformance/online.rs:398-400, 479;
JL/MpLauncher.java:609-691, 1224-1252; J/strategies/must/split/SplitCoordinator.java:496-539. -->
<!-- Verified (rev 5): §6.3: T/conformance/pairing.rs:1-35, 44-51, 116-195, 257-364; J/mp/summary/PairingCheck.java:39-73,
76-79, 120-239; T/conformance/interchange.rs:433-470. -->
<!-- Verified (rev 5.2): §6.3 online: T/conformance/pairing.rs:155-171, 196, 217; T/program.rs:949-980. -->

## 7. Readers, the pairing check, the join and the online check

### 7.1 Readers

The readers (TraceForge `conf-join` and `online --spec`; JMC's online check) accept a file only in the form both
writers emit:
- header: only the §2 keys, each once, compact, in §2 order; `minor` absent or 1, `roles` exactly when `minor` is
  present; `seed`, `iterations` integers and only in a `sampled` file; the three online fields all or none,
  integers; string `tool`, `program`, `size`, `model`, `enumeration`;
- summary line: its normal form (§3), optionally followed by a compact `n` as its last member;
- line ends `\n` or `\r\n`.

Each refuses the rest, exit 2, with `line N: ` and the reason:

| Row | Input | TraceForge | JMC |
|---|---|---|---|
| R1 | a repeated summary line | `line N: the line repeats line M (each line is one distinct summary)` | `line N repeats line M` (the C6 row's text) |
| R2 | a blank line after the header | `not JSON: …` | `an empty line; every line after the header is a summary line` |
| R3 | an unknown key in a summary line | ``unknown key `x` `` | `unknown key "x" in a summary line (neither writer emits it)` |
| R4 | a line not in the normal form (whitespace, member or `statuses`/`words` key order, another spelling of a value, `n` not last) | ``the line is not in the form the writers emit (the normal form, then `n` if any; compact, each key once, in order): it differs at byte B`` | the same, `n` without backticks |
| R5 | an unknown header key | ``unknown header key `x` (neither writer emits it)`` | `unknown header key "x" (neither writer emits it)` |
| R6 | a header out of key order, with whitespace, or with another spelling of a string | `the header is not in the form the writers emit (compact, each key once, in the order format, minor, tool, program, size, visible, roles, model, enumeration, seed, iterations, the online fields, encoding): it differs at byte B` | the same |
| R7 | no `program`, `size` or `tool` (K) | ``the header has no string `K` `` | `K is not a JSON string (missing)` |
| R8 | `roles` without `minor`, or the reverse | ``the header has `roles` without `minor` or `minor` without `roles` `` | the same without backticks |
| R9 | `seed` or `iterations` (each on its own) outside a `sampled` file, or not an integer; some but not all three online fields, or a non-integer one | ``the header has `K` in a file that is not `sampled` ``; `the header has some of the online fields (first_miss_iteration, first_miss_wall_ms, uncovered_total) but not all three`; ``the header's `K` is V, not an integer`` | the same without backticks; a fraction or exponent may be refused earlier: `number V is not an integer literal (the format refuses floating-point values)` |
| R10 | bare CR line ends | the file is one line: `the header is not JSON: … trailing characters …` when CR separates records; the R6 message for a header-only file (a trailing CR is JSON whitespace) | `raw control character U+000D outside a string` |

Both also refuse an explicit `"minor":0`, `{"f":…,"t":…}`, a duplicate key outside `n`, an unpaired surrogate escape
and a non-`complete` specification. `n` is held to the same domain as every other value: both refuse, naming the
line, a fraction (`number 1.5 is not an integer literal (the format refuses floating-point values)`), an integer
outside signed 64 bits (`integer 9223372036854775808 is outside signed 64 bits`), `-0` (`number -0 is not the
canonical encoding of 0`) and a repeated key (`duplicate key "x" in an object`). Each reader accepts the other writer's canonical output (C21), within the
common domain: §3's limits and signed-64-bit integers. JMC decodes the file as a UTF-8 stream while splitting it into
lines; TraceForge's plain join and `online --spec` read it whole, `--merge` and the pairing check line by line.

Per tool, pending phase 4c:
- R1: TraceForge's reader keeps `line N: the line repeats line M (each line is one distinct summary)`; JMC's reader
  prints the C6 text.
- An exponent (`1e3`): TraceForge `number 1e3 is not an integer literal (…)`, JMC `number ending in "e" is not an
  integer literal (…)`.

Per tool, unscheduled:
- TraceForge's reader accepts an unsorted `visible` list (its pairing check refuses one, C2); JMC compares the list
  with the program's sorted list.

### 7.2 Pairing check

`conf-join --check-pairing [--merge] [--decide] spec.jsonl impl.jsonl` runs only the check (§6.3): `pairing: <row>: …`
per failure, `pairing: warning: <row>: …` per warning, then `pairing: ok` (exit 0) or `pairing: failed (K checks)`
(exit 2). Only with `--decide` is a count above 65535 a C10 warning.

The check also runs before every join and before both online checks (`<bin> online --spec F`; `MpLauncher … --spec
F`, either form), there against the program: its visible roles, header roles and model stand for the
implementation header's. In TraceForge `online` C3 compares only the models (`cd` included) and C13 is not checked;
JMC checks C3 as the join does and C13 against the program's declared tags. C18 is a warning in both. Warnings go
to stderr as `<tool>: pairing: warning: <row>: …` and the run proceeds; failures as `<tool>: pairing: <row>: …`,
then `<tool>: refused: the pairing check failed (K checks; --no-pairing-check skips it)`, exit 2, no usage text
(`<tool>` = `conf-join`, the benchmark binary or `MpLauncher`). `--no-pairing-check` skips the
check; the reader and the join, or the JMC reader, still refuse most C2, C3, C4, C6 and C10 cases on their own, in
their own wording (§7.1, §7.3); C13 and C18 are skipped. What TraceForge's join then accepts:
- C2: an unsorted `visible` list that both files share;
- C3: two equal models outside `p2p`/`asyn` (`cd`, or any string): the join requires only equality;
- C10: a word of up to 2^32−1 events, and so counts above 2^31−1 bounded by such a word.

JMC's reader requires the program's model, and Must refuses `cd` (§6.1).

A header the reader refuses stops the check: the reader's refusal is printed once, with no C13 warning before it
(TraceForge's check returns that refusal itself, `--check-pairing` included; JMC's passes it to the reader). A
JSON-object line the reader refuses contributes nothing to C13 and C18 (its tags may then appear as one-sided C13
warnings) but still counts for C6 (by its text) and C10. A line that is not a JSON object is refused by
TraceForge's check (exit 2) and skipped by JMC's. Before a join or an online check the reader's refusal follows;
`--check-pairing` returns after its report, so `pairing: ok` does not validate the lines (an `n` of `1.5` passes).

Wording (`F` the specification file, or either file in `conf-join`), identical in all three commands after the
prefix:

| Row | Failure (f) or warning (w) |
|---|---|
| C2 | f ``F: line 1: the `visible` list L is not byte-sorted with distinct names`` (`roles` cannot fail so: an unsorted `roles` list is the reader's refusal); f ``the two `visible` lists differ: A and B``; f ``the two `roles` lists differ: A and B`` |
| C3 | f `the two models differ: A and B`; f ``the specification's model `M` is not supported for a join (p2p or asyn)`` (also `implementation's`) |
| C4 | f ``the specification is `E`, not `complete` `` |
| C6 | f `F: line N repeats line M`; under `--merge` (`conf-join`) also f `F: line N precedes line M in byte order (--merge needs sorted files)` |
| C10 | f `F: line N: the count C is out of bounds (both readers accept counts up to 2^31-1)`; f ``F: line N: the word of `R` has K events (at most 2^31-1)``; w `F: line N: the count C exceeds 65535; --decide refuses the join if it classifies this line` |
| C13 | f `the tag "T" has the field arities S in the specification and S' in the implementation`; w `the tag "T" (arities S) occurs only in the specification; the implementation never sends it, which the join does not count against it`; w `the tag "T" (arities S) occurs only in the implementation; the join reports the lines that carry it as uncovered` |
| C18 | w `F: line N: a value is tagged "ThreadId", the name of an identity type; if it is a process identity, the programs must send its role name instead` (also `"Tid"`) |

Per tool, pending phase 4c: after a reader refusal TraceForge `online` prints its usage text (every reader refusal
under `--no-pairing-check`, a line refusal by default); JMC prints none.

### 7.3 Join

Coverage: an implementation summary is covered iff a specification summary has identical statuses and words and
counts componentwise ≤ the implementation's (equality covers).

`conf-join [--merge] [--decide [--decide-budget N]] [--no-pairing-check] spec.jsonl impl.jsonl` (TraceForge) reads
either tool's files.

It refuses (exit 2) a failed pairing check (§7.2), a file its reader refuses (§7.1), a non-`complete` specification (`the specification is `E`,
not `complete``), different models (`the two models differ: A and B`), `visible` lists (`the two files' `visible`
lists differ: …`) or minor-1 `roles` (`the two roles lists differ: …`), and under `--merge` an unsorted file. It
prints both headers, `verdict: conforms|violation|no verdict` with ` (sampled)` or ` (partial)` appended for a
`sampled` or `bounded`/`aborted` implementation (`verdict: conforms (partial)`), and each uncovered line as `<class>
<normal form>`: `no-bucket` (no specification summary of its shape), `no-slot` (shape held, not these values),
`not-found` (statuses and words held, no counts ≤ the line's).

Covered `sampled` gives `no verdict`. `--decide` adds `  decide: violation|false-alarm|
unresolved k=… k_min=… states=…` per line, and `  witness:` for a violation, without changing the verdict.

### 7.4 Online check

The online check (JMC `MpLauncher … must 0 … --spec F [--no-pairing-check]` or `MpLauncher online … --spec F`,
TraceForge `<bin> online --spec F [--no-pairing-check] [--out F]`) looks each explored summary up in F. Both run
the pairing check first (§7.2). Both refuse an F their reader refuses (§7.1), an F that is not `complete` or differs in model, `visible` or
minor-1 `roles`; `--stop-at-first` stops after the first uncovered summary. Verdicts: `conforms`, `reports`,
`partial` (TraceForge also `refused`). Exit on `reports`: TraceForge 0, JMC named form 1 (§10); a per-tool
difference, TraceForge to exit 1 in phase 4c.

<!-- Verified: T/bin/conf-join.rs:16-130; T/conformance/interchange.rs:1119-1136, 1701-1845, 1983-2010;
T/conformance/report.rs:3099-3160, 3192-3240; T/conformance/online.rs:200-223, 552-581;
J/mp/summary/SpecificationFile.java:138-185; J/strategies/must/MustStrategy.java:104-113;
JL/MpLauncher.java:404-436, 576-603. -->
<!-- Verified (rev 5): readers: T/conformance/interchange.rs:1493-1517, 1536-1545, 1599, 1641-1664, 1688-1825,
1873-1903; T/conformance/online.rs:560; T/conformance/online_p.rs:375; J/mp/summary/SpecificationFile.java:47-58,
156-232, 249-277, 284-371, 785-830, 898-900. Pairing check: T/bin/conf-join.rs:20-46, 56-59, 128-184;
T/conformance/pairing.rs:1-35, 44-51, 116-364; J/mp/summary/PairingCheck.java:39-73, 76-79, 120-239;
J/mp/cli/MpLauncher.java:263, 308-309, 357-358, 482-520, 2171-2199; J/mp/cli/NamedCli.java:28, 43, 207-208. -->
<!-- Verified (rev 5.1): exceptions: T/conformance/pairing.rs:224-228, 262-276; T/conformance/interchange.rs:1580-1593,
1706-1729, 1830-1838, 1936-1964, 2060-2090; T/bin/conf-join.rs:52-53, 132-147; J/mp/summary/PairingCheck.java:176-186;
J/mp/summary/SpecificationFile.java:373-425, 868-874. -->
<!-- Verified (rev 5.2): T/conformance/interchange.rs:1512, 1652, 1674-1740, 1833-1835, 1972, 2197;
T/conformance/pairing.rs:155-171, 176-251, 287, 381, 393; T/program.rs:681, 949-980, 1079; T/conformance/online.rs:214-223;
J/mp/summary/SpecificationFile.java:227, 765; J/mp/summary/PairingCheck.java:131-134; J/mp/cli/MpLauncher.java:493-516,
2188-2191. -->

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
`MpLauncher --replay G [--report R] [--allow-…] [<class>[:<args>]]` or `MpLauncher replay --graph G [--report R]
[--allow-…] [--program <class>[:<args>]]`.

Both refuse before running an invalid or foreign graph, a `tool_commit` differing from the build's or `"unknown"`
(unless `--allow-tool-change`), and a differing or `"unknown"` source (unless `--allow-source-change`). Both
compare only the `files` maps and ignore `root`; `"unknown"` on either side is refused unless `--allow-source-change`,
in both JMC forms (JMC: `--replay refused: source files differ from the program's manifest; --allow-source-change
accepts it`, `--replay refused: source unknown; …`; TraceForge: `replay: refused: the benchmark source differs from
the graph's, or is unknown (--allow-source-change)`). TraceForge refuses another
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
<!-- Verified (rev 5): §8.5 source identity: T/program.rs:475-486; J/mp/cli/MpLauncher.java:1555-1569;
J/mp/cli/NamedCli.java:132-157. -->

## 9. Measurements

The block (`--- measurements`, then `key: value` lines) is printed:
- JMC: every `must` run past argument parsing, a refused specification included (`stop_reason: error`); a `--split`
  coordinator prints its own, workers none; never under `mp-random`; replay unless refused before running.
- TraceForge: `count`; `export` unless refused before the run (bad arguments, index, model); `conf`; `online`
  unless refused before the exploration; `conf-join` always (refused: `stop_reason: error`), except `--check-pairing`, `--help` and argument errors; replay after a result, unless writing a requested `--report` failed. Shared keys, in order: `wall_ms`, `explore_ms`, `summarise_ms`, `index_ms`,
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
  1 ..= 2^63−1, and 0 with no uncovered line.
  A missing `n` or one below 1 empties it without refusing the line (the reader refuses one outside signed 64 bits); a refused join leaves it empty; for a
  `sampled` or partial implementation 0 does not establish conformance.

Replay prints only `wall_ms`, `peak_rss_kb`, `replay_events`.

<!-- Verified: T/conformance/measure.rs:22-71, 88-107, 176-231, 802-842; T/conformance/report.rs:3268-3314;
T/conformance/interchange.rs:1413-1427, 1546-1550; J/strategies/must/MustStrategy.java:541-554;
JL/MpLauncher.java:169-195, 407, 683-686, 1081-1144, 1501-1513, 1967-1971; T/program.rs:445-458, 487-503, 745-750. -->
<!-- Verified (rev 5.2): `uncovered_total`: T/conformance/interchange.rs:1512, 1833-1835. -->

## 10. Exit codes

| Command | 0 | 1 | 2 | 3 |
|---|---|---|---|---|
| TraceForge `export` | written | — | bad arguments, refusal, assertion, store not written | — |
| JMC export, positional | completed (timeout: `aborted`) | failure, incl. shape/role/value refusals | bad arguments | — |
| JMC `export`/`count`, named | exhausted | failure of the program (assertion, exception, throwing guard) | bad arguments, refusal (shape, role, value, API rule, model), output not writable | incomplete: timeout, bound, `mp-random` (sampled), unfinished split |
| TraceForge `online` | verdict (`reports` included) | failure | bad arguments, refused specification or declaration, failed pairing check | — |
| JMC online, positional | verdict | failure, incl. refusals during the run | bad arguments, refused specification, failed pairing check | — |
| JMC `online`, named | `conforms` | `reports`, failure of the program | bad arguments, refused specification, failed pairing check, refusal during the run | `partial`, incomplete |
| `conf-join` | conforms, no verdict | violation | bad arguments, refused input or store, failed pairing check, witness output not writable | — |
| `conf-join --check-pairing` | passed | — | failed, unreadable input, bad arguments | — |
| TraceForge `conf` | conforms | reports | inconclusive, error | — |
| replay | reproduced | deviation, failure (JMC: unwritable report) | refused (TraceForge: unwritable report) | — |

<!-- Verified: T/program.rs:752-761, 812-818, 968-976, 495; T/conformance/online.rs:214-223;
T/conformance/interchange.rs:1324-1329; T/bin/conf-join.rs:106-129; T/conformance/witness_join.rs:342-372, 446, 465, 469;
JL/MpLauncher.java:169-195, 683-691, 1440-1500, 2033-2065. -->
<!-- Verified (rev 5): pairing exits: T/bin/conf-join.rs:45-46, 132-184; J/mp/cli/MpLauncher.java:495-502, 2171-2199.
Named form (rev 5.1): J/mp/cli/MpLauncher.java:214-220, 774-814, 1343-1361, 1636-1648; J/strategies/mp/MpOutcome.java:6-24. -->
<!-- Verified (rev 5.2): TraceForge online: T/conformance/online.rs:214-223; T/program.rs:970-976. -->

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
