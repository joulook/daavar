# Brief for a benchmark-building agent (2026-10-28)

You build one or more benchmark directories under `/local/home/mkhoshechin/Documents/github-jmc/plan/benchmarks/<name>/`.
You write only inside your assigned directories (and their `out/` build outputs). Everything
else is read-only: never edit `TraceForge/`, `my-gitlab/`, `rose-github/`, the private copies
under `/local/home/mkhoshechin/tmp/{tf-bench,jmc-bench}`, or any other `plan/` file.

## What a benchmark directory contains

`README.md` (follow `_tools/README-TEMPLATE.md` exactly; every section filled; the expected verdict
and its reason are mandatory), `source/` (the original program copied verbatim from
`plan/benchmarks/related-work-papers/sources/...` or from the TraceForge tree, plus a
`PROVENANCE.md` naming the path and commit), `rust/` (a crate made from `_tools/rust-template/`:
`Cargo.toml` with the crate name changed, `src/main.rs` with the `spec` and `impl` programs and
the same CLI), `java/` (the Java programs in package `bench.<name>`, each implementing
`org.mpi_sws.jmc.mp.launch.MpProgram` with a constructor taking `String... args` as the H1 ports
do, see `/local/home/mkhoshechin/tmp/jmc-bench/core/src/test/java/org/mpi_sws/jmc/mp/h1/TwoPc.java`
and `H1Support.java` for the idiom; copy what you need from `H1Support` into your own package,
do not depend on the `h1` test package).

## The Must-API idiom (both languages)

Static threads spawned by main at the start, named by role; main spawns every process first,
performs no message-passing event itself, and awaits all of them last. Communication by
`send_msg(tid, m)` / `recv_msg_block::<T>()` (blocking) / `recv_msg::<T>()` (non-blocking,
`Option`) in Rust, `Channel.send(M, tid, m)` / `Channel.recv(M, T.class)` / `Channel.tryRecv(M, T.class)`
in Java; finite choice by `nondet()` / `(0..n).nondet()` in Rust, `Nondet.chooseBoolean()` /
`Nondet.chooseIndex(n)` in Java. Communication model per run: `fifo` (= p2p), `bag` (= asyn) or
`causal` (= cd); Java `CommunicationModel.P2P / ASYN / CD`. Loop-free after unrolling the bounded
parameters; finite value domain; a blocking receive with nothing to read ends the thread
blocked. Guards (selective receives) are type tests only. Payloads carry no thread ids unless
the id is a role the format can name. Read `plan/benchmarks/RELATED-BENCHMARKS.tex` lines 42–100
(the four rules every pair follows) before writing a pair.

The specification is a program too: one server/oracle thread per independently served unit,
with the same visible roles as the implementation. The implementation is the distributed
protocol. Both must have the same visible role names.

## Running under plain Must (required for every side and size you list)

TraceForge: `plan/benchmarks/_tools/run-rust.sh <benchdir> <spec|impl> <size> [fifo|bag|causal]`
prints `execs=<complete> blocked=<blocked>`.
JMC: `plan/benchmarks/_tools/run-java.sh <benchdir> 'bench.<name>.<Class>:<arg>,<arg>' must 100000 600`
prints JMC's counting-mode lines (complete and blocked graph counts). The JMC `must` strategy
explores every graph exactly once, so its iterations equal its distinct graphs.
Run sizes 2 and 3 for every side; size 4 only if size 3 finished in under a minute on both.
One run each; no repeats; no wall times in the README (counts only). Record every command.
If the two tools disagree, record both counts and say so; do not change a program to make
them agree. If a run exceeds 600 s, record "not exhausted at 600 s".

Build notes: the Rust crate builds with a path dependency on the private TraceForge copy and a
shared target directory; the first build takes minutes, later ones seconds. `cargo` runs with
`-j 2`; run one benchmark at a time. Java compiles with `javac` against the private JMC copy's
classpath in `/local/home/mkhoshechin/tmp/jmc-bench/cp.txt`.

## Report

Your final message: the directories you built, the verdict per pair, the count table per pair
(both tools), any disagreement, and anything you could not do.
