# Replicated log over two-phase commit (Aneris, ESOP 2020) against one atomic log

**Family:** related-work  **Source:** Krogh-Jespersen et al., "Aneris", ESOP 2020, artifact files
`related-work-papers/sources/aneris-esop20/examples/replicated_log/rep_log.v` (+ `two_phase_commit/tpc.v`,
`library/lib.v`; Iris spec `rep_log_proof.v`) (candidate N22, record "AN/replicated_log" in
`CATALOGUE-artifacts-2.md`)  **Expected verdict:** `correct`: **CONFORMS** under `bag` and `fifo`
(the logger answers a client only after both databases acknowledged the commit, so a reader that
starts after every answer sees both logs complete and in the logger's order); `early-reply`:
**DOES NOT REFINE** under both (the logger answers before the ACKs, so the auditor can read a
database that has not yet appended the event: `Log("")` at c = 1).

## 1. The protocol

`rep_log.v`: a `logger` (node "coordinator") with a client socket `ip:80` and a TPC socket
`ip:1200`; two databases `db1`, `db2`; clients `client1` (`"EVENT X"`), `client2` (`"EVENT Y"`).
A client sends its event to the logger and waits for the reply (`listen_wait`). The logger handles
one client event at a time (`listen "socket" handler`, the handler running to completion before the
next `listen`): `tpc_coordinate ("REQUEST_" ^ msg) socket_tpc dbs dec_handler_log`, i.e. send the
request to both databases, collect `count = 2` messages on `ip:1200` (the votes), decide
`dec_handler_log` (`COMMIT` iff every vote is `COMMIT`), send the decision to both, collect 2
messages (the ACKs), then `SendTo "socket" result sender`. A database runs `tpc_participant_setup`:
receive a request, `req_handler_log` stores the event in `waiting` and votes `"COMMIT"` (always),
receive the decision, `fin_handler_log` appends `waiting` to `log` on `"COMMIT"`, send `"ACK"`,
loop. Aneris's network (`dist_lang/lang.v`) is reliable and unordered: our `bag`. The Iris
specification: "commit appends on all, abort leaves all unchanged"; a client's reply is `COMMIT`
or `ABORT` (`log_si`).

Roles:

- Visible: `client1 .. client<c>`, `auditor` (the same code on both sides).
- Invisible: spec `log`; impl `coordinator`, `db1`, `db2`.

The source has no reader of the logs, so its replicated state is not observable. The added
`auditor` makes it observable: each client, once answered, sends `Done` to the auditor; after `c`
`Done`s the auditor sends `Read` twice (impl: to `db1` and to `db2`) and receives two `Log(s)`; each
database answers a `Read` with its `log` string. These additions are the same on both sides except
the `Read` destinations, which the comparison does not see.

Messages: `Event { from, ev }` (client → logger; `from` is the client index, Aneris's
`ReceiveFrom` sender), `Result(Commit | Abort)` (logger → client), `Request(ev)` (logger → db; the
`"REQUEST_"` prefix is the message tag), `Vote(Commit | Abort)`, `Ack` (db → logger), `Dec(Commit
| Abort)` (logger → db), `Read`, `Log(s)`, `Done` (added).

Parameter: `c` clients (the size; c = 2 is the source's `logger_runner`), client i sending one
event `EVENT X`, `EVENT Y`, `EVENT Z`, … ; two databases. Models `bag` (the source's network) and
`fifo` (the catalogue's p2p).

## 2. The specification program

One invisible `log`, the Iris spec's atomic log: `c + 2` receives of `Event | Read`; an `Event`
appends `ev` to the log string and answers `Result(Commit)` to its client; a `Read` answers
`Log(log)`. One thread orders every event (rule 2), as the logger's one-at-a-time handler does.
Graphs: bag `c! · c! · 2 · 2` (event order at `log`, `Done` order at the auditor, the two `Read`s,
the two `Log`s: 4, 16, 144); fifo `c! · c!` (the two `Read`s and two `Log`s share a sender: 1, 4,
36).

## 3. The implementation program

`coordinator`: `c` times: receive `Event` (guard: the type of `ip:80`); `Request(ev)` to `db1`,
`db2`; 2 receives of `Vote | Ack` (the types of `ip:1200`: the source's `listen socket_tpc`
accepts any message there); `Dec(Commit)` iff both votes are `Commit`; `Dec` to both; 2 receives
of `Vote | Ack` (the ACKs); `Result(dec)` to the client. `db1`, `db2`: `2c + 1` receives of
`Request | Dec | Read` on their one socket: `Request(ev)`: `waiting := ev`, `Vote(Commit)`;
`Dec(d)`: append `waiting` on `Commit`, `Ack`; `Read`: `Log(log)`. Graphs: `c! · c! · 4^c · 2`
under both models (every choice is between different senders: the two votes, the two ACKs per
round, the two `Log`s: 8, 128, 4608).

Departures: the unbounded `listen` loops are bounded (`c` events at the logger, `2c + 1` receives
at a database); the logger's two sockets are one mailbox whose receives select by message type
(the two sockets carry disjoint types); `"REQUEST_" ^ msg` and `value_of_message` are a `Request`
tag around the event; the vote is the source's constant `"COMMIT"` (no abort path is exercised;
`Abort` is kept in the type and in `dec_handler_log` as in the source); the `auditor`, `Done`,
`Read`, `Log` are added (§1).

## 4. Where they differ, and why the verdict is what it is

**`correct`, CONFORMS (bag and fifo).** A client's `Result` is sent after both ACKs, each sent
after its database appended the event; the client's `Done` follows its `Result`; the auditor's
`Read`s follow all `c` `Done`s. So both `Read`s are causally after every append, both logs are the
concatenation of the events in the logger's order, and the two `Log` values are equal. The
specification graph with the same event order at `log` (and the same `Done` order) has the same
words: clients `S(Event) R(Result(Commit)) S(Done)`, auditor `R(Done)^c S(Read) S(Read) R(Log(s))
R(Log(s))`, `s` the events in serving order. Its causal order between visible events (an event
served before another is causally before the other client's `Result`; every client event before the
auditor's `Read`s) is the implementation's, where the same edges go through the coordinator's
rounds. Under `fifo` the specification has fewer graphs, but the implementation's extra ones differ
only in the order of equal `Log` values and of the invisible votes and ACKs.

**`early-reply`, DOES NOT REFINE (bag and fifo)**, smallest size c = 1. Schedule: `client1`
sends `Event { 1, "EVENT X" }`; the coordinator sends both requests, receives both votes, sends
`Dec(Commit)` to both databases and at once `Result(Commit)` to `client1` (the fault); `client1`
sends `Done`; the auditor sends `Read` to `db1` and `db2`; `db2` receives the `Read` before the
`Dec` and answers `Log("")`. Uncovered word:

```
auditor: R(Done) S(Read) S(Read) R(Log("EVENT X")) R(Log(""))
```

(and the same with the `Log`s swapped, and with both `Log("")`: 3 distinct summaries, 24 of the 32
graphs). In the specification the `Read`s are served after the event was appended, so every
`Log` is `"EVENT X"`. Component: **signature value**. The race is between two senders (the
coordinator's `Dec` and the auditor's `Read` to the same database), so FIFO does not prevent it.

## 5. Variants and faults

| variant | model | how it differs | expected verdict | reason |
|---|---|---|---|---|
| `correct` | bag | the source's logger and `tpc_coordinate` | CONFORMS | §4 |
| `correct` | fifo | the same, p2p | CONFORMS | §4 |
| `early-reply` | bag | `SendTo socket result sender` moved before the ACK collection | DOES NOT REFINE (c ≥ 1) | auditor reads `Log("")` |
| `early-reply` | fifo | the same, p2p | DOES NOT REFINE (c ≥ 1) | the race is between two senders |

The source ships no fault. The seeded fault is the catalogue's (c): it removes exactly what the ACK
round of `tpc_coordinate` is for (the reply means "appended on all"), and it is reachable with the
source's always-`COMMIT` votes. The catalogue's faults (a) (commit if any vote commits) and (b)
(append on the request) need an aborting vote, which the source never casts; they were not built.

## 6. Files

- `source/`: `replicated_log/rep_log.v`, `rep_log_proof.v`, `two_phase_commit/tpc.v`, `tpc_proof.v`,
  `tpc_correct.v`, `library/lib.v`, copied verbatim, provenance in `source/PROVENANCE.md`.
- `rust/`: crate `bench_replicated_log_esop20` on TraceForge's shared main; ARGS `<c> [correct|early-reply]`,
  model by `--model bag|fifo`. Visible `client1 .. client<c>`, `auditor`.
- `java/`: package `bench.replicated_log_esop20`, class `RepLog` (args `<spec|impl>,<c>,<fifo|bag>[,<variant>]`),
  `Support` (model argument and thread joiner, copied from `H1Support`). Spawn order: spec `log,
  auditor, client1 ..`; impl `coordinator, db1, db2, auditor, client1 ..`; the same in Rust.
  Compiled to `java/out/`.
- `out/runs.log`: every command and its output; `out/export/`: the five summary files of §7.

## 7. Plain-Must exploration counts (both tools, one run each)

| side | size | model | TraceForge execs / blocked | JMC complete / blocked | equal? |
|---|---|---|---|---|---|
| spec | c=1 | bag | 4 / 0 | 4 / 0 | yes |
| impl `correct` | c=1 | bag | 8 / 0 | 8 / 0 | yes |
| impl `early-reply` | c=1 | bag | 32 / 0 | 32 / 0 | yes |
| spec | c=2 | bag | 16 / 0 | 16 / 0 | yes |
| impl `correct` | c=2 | bag | 128 / 0 | 128 / 0 | yes |
| impl `early-reply` | c=2 | bag | 512 / 0 | 512 / 0 | yes |
| spec | c=3 | bag | 144 / 0 | 144 / 0 | yes |
| impl `correct` | c=3 | bag | 4608 / 0 | 4608 / 0 | yes |
| impl `early-reply` | c=3 | bag | 18432 / 0 | 18432 / 0 | yes |
| spec | c=1 | fifo | 1 / 0 | 1 / 0 | yes |
| impl `correct` | c=1 | fifo | 8 / 0 | 8 / 0 | yes |
| impl `early-reply` | c=1 | fifo | 32 / 0 | 32 / 0 | yes |
| spec | c=2 | fifo | 4 / 0 | 4 / 0 | yes |
| impl `correct` | c=2 | fifo | 128 / 0 | 128 / 0 | yes |
| impl `early-reply` | c=2 | fifo | 512 / 0 | 512 / 0 | yes |
| spec | c=3 | fifo | 36 / 0 | 36 / 0 | yes |
| impl `correct` | c=3 | fifo | 4608 / 0 | 4608 / 0 | yes |
| impl `early-reply` | c=3 | fifo | 18432 / 0 | 18432 / 0 | yes |

All runs exhausted; no disagreement; every count is the hand count of §2/§3 (`early-reply`: ×4,
each database's `Read` racing the last round's `Dec`). c = 3 is the first size over a minute
(JMC's impl runs) under both models, so the sweep stops there.

Conformance and the cross-language join, c = 1:

| check | model | spec | impl | verdict |
|---|---|---|---|---|
| `conf` | bag | Rust spec | Rust `correct` | conforms (silence) |
| `conf` | bag | Rust spec | Rust `early-reply` | 24 candidate violations (exit 1) |
| `conf` | fifo | Rust spec | Rust `correct` | conforms (silence) |
| `conf` | fifo | Rust spec | Rust `early-reply` | 24 candidate violations (exit 1) |
| `conf-join` | bag | Rust spec export | Java `correct` export | conforms, 0 uncovered |
| `conf-join` | bag | Rust spec export | Java `early-reply` export | violation, 3 uncovered (`no-slot`, the words of §4) |
| `conf-join` | bag | Java spec export | Rust `correct` export | conforms, 0 uncovered |

Commands (X = `/local/home/mkhoshechin/tmp/bench-target/release/bench_replicated_log_esop20`, built with
`CARGO_TARGET_DIR=/local/home/mkhoshechin/tmp/bench-target cargo build --release -j 4` in `rust/`;
J = `java -Xss4m -cp java/out:$(cat daavar/jmc/cp.txt) org.mpi_sws.jmc.mp.launch.MpLauncher`,
Java compiled with `javac -d java/out -cp $(cat daavar/jmc/cp.txt) java/bench/replicated_log_esop20/*.java`),
for `c ∈ {1, 2, 3}`, `m ∈ {bag, fifo}`, `v ∈ {correct, early-reply}`, all under `timeout 600`:

```
X count spec --model <m> <c>
X count impl --model <m> <c> <v>
J 'bench.replicated_log_esop20.RepLog:spec,<c>,<m>' must 0 1 <fresh report dir> - 600 </dev/null
J 'bench.replicated_log_esop20.RepLog:impl,<c>,<m>,<v>' must 0 1 <fresh report dir> - 600 </dev/null
X conf --model <m> 1 <v>
X export spec --out out/export/rust-spec-c1.jsonl --model bag --program replicated-log-esop20 --size c1 1
X export impl --out out/export/rust-impl-correct-c1.jsonl --model bag --program replicated-log-esop20 --size c1 1 correct
J 'bench.replicated_log_esop20.RepLog:<side>,1,bag[,<v>]' must 0 1 <fresh report dir> out/export/java-<side>[-<v>]-c1.jsonl 600 </dev/null
daavar/traceforge/target/release/conf-join <spec file> <impl file>
```
