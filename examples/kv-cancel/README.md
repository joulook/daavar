# kv-cancel: a Put racing a Cancel of the same request

The smallest end-to-end example: one client, one request. It has a specification program (Rust), a correct
implementation in Rust and in Java, and one injected bug (`mutant1`) that the check must reject. Each export
explores at most five executions and takes well under a second.

## Protocol

1. Roles: `client` and `server` (observed), `disk` (unobserved, not a daemon). Model fifo.
2. The client sends `Put{1,"k",1}`. It then either sends `Cancel{1}` at once or sends no cancel (a nondeterministic choice).
3. On `Put` the server sends `Write{1,"k",1}` to the disk. The disk answers `Written{1}`.
4. R1: the Put is answered exactly once, with `PutOk{1}` or `Canceled{1}`. R2: `PutOk{1}` is sent only after `Written{1}`.
5. R3: a Cancel is answered exactly once, with `CancelOk{1}` or `CancelErr{1}`.
6. R4: the server MAY accept a Cancel of an unanswered Put. It then sends `Canceled{1}` and `CancelOk{1}`, and never sends `PutOk{1}`.
7. R4 (cont.): it MAY instead refuse it with `CancelErr{1}`; the Put then completes with `PutOk{1}`.
8. R5: a Cancel of an answered Put gets `CancelErr{1}`.
9. The race: the server receives `Cancel{1}` from the client and `Written{1}` from the disk in either order.
10. When the client has its answers, it sends `Shutdown`. The server then waits for an outstanding `Written`,
    sends `Shutdown` to the disk, and ends. No message is left unread.

## Programs

| Program | Where | ARGS |
|---|---|---|
| spec | `rust/src/spec.rs`, with `rust/src/msgs.rs` and `rust/src/env.rs` shared by all | `spec` |
| impl (Rust) | `rust/src/imp.rs`: a request table; it always accepts the cancel of an in-flight Put | `impl` |
| impl (Java) | `java/bench/kv_cancel/KvCancel.java`: `imp.rs`, `msgs.rs` and `env.rs`, event for event | `impl` |
| mutant1 | `imp.rs` with `args.mutant == 1`: `CancelOk` without removing the request, so a later `PutOk` follows | `mutant1` |

Bad arguments print a `usage:` line on stderr and exit 2. In Java, the constructor throws
`IllegalArgumentException` and MpLauncher exits 2.

## Expected verdicts

| checked <= reference | uncovered | verdict |
|---|---|---|
| rust <= spec, java <= spec | 0 | conforms |
| mutant1 <= spec | 1: client `Put, Cancel, CancelOk, PutOk` | violation, Decide-confirmed (mutant rejected) |
| java <= rust, rust <= java (twins) | 0 | same summaries (JMC adds a per-line `n`) |
| spec in rust, spec in java (by hand only; not a `daavar.toml` direction) | 1: `Cancel` refused while the Put is in flight (R4's MAY) | conf-join `violation`, exit 1: expected, the rewrites never refuse |

Execution counts are `count spec -- spec` 5, `count impl -- impl` 4 and `count impl -- mutant1` 3. The
export files have 5, 4 and 3 summary lines, each after the header. Java's export also has 4.

## By hand

Run from this directory, inside a daavar checkout with the `traceforge` and `jmc` submodules built.
`CJ` is TraceForge's `conf-join` (`../../traceforge/target/release/conf-join`), and `JMC_CP` is the JMC core test
classpath (DRIVER-DESIGN §3.2).

```
R=rust/target/release/tf_kv_cancel
(cd rust && cargo build --release --locked)
../../scripts/source-manifest.sh rust rust rust/target/release        # needed by replay; run after every build
mkdir -p classes && javac -d classes -cp "$JMC_CP" java/bench/kv_cancel/KvCancel.java
../../scripts/source-manifest.sh java . classes

# count
$R count spec -- spec                                               # execs=5 blocked=0
$R count impl -- impl                                               # execs=4 blocked=0
$R count impl -- mutant1                                            # execs=3 blocked=0

# export
$R export spec --out spec.jsonl --graphs spec.jsonl.graphs.jsonl --model fifo --program kv-cancel -- spec
$R export impl --out impl.jsonl --graphs impl.jsonl.graphs.jsonl --model fifo --program kv-cancel -- impl
$R export impl --out mutant1.jsonl --graphs mutant1.jsonl.graphs.jsonl --model fifo --program kv-cancel -- mutant1
mkdir -p report && java -ea -Xss4m -cp classes:"$JMC_CP" org.mpi_sws.jmc.mp.launch.MpLauncher \
     bench.kv_cancel.KvCancel:impl must 0 1 report java.jsonl 60 --graphs java.jsonl.graphs.jsonl

# join (reference first, checked second)
$CJ --decide spec.jsonl impl.jsonl                                  # exit 0, verdict: conforms
$CJ --decide spec.jsonl java.jsonl                                  # exit 0, verdict: conforms
$CJ --decide impl.jsonl java.jsonl; $CJ --decide java.jsonl impl.jsonl   # twins: exit 0 both
$CJ --decide --witness-dir w spec.jsonl mutant1.jsonl               # exit 1, violation; writes w/1.graph.json
$R replay --graph w/1.graph.json                                    # replay: reproduced, exit 0
$CJ --decide impl.jsonl spec.jsonl                                  # exit 1: the spec's refused cancel (expected)
$CJ --decide java.jsonl spec.jsonl                                  # exit 1: the same word against the Java rewrite
```

`bin/daavar check examples/kv-cancel` runs every direction of `daavar.toml` and leaves `out/<run>/VERDICT.md`
(exit 0: the two implementations conform and the mutant is rejected).
