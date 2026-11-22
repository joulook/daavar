# Scripts

Helpers for running one benchmark by hand until the `daavar` driver exists. Paths come from `env.sh`; every
value there can be overridden by exporting it first.

| script | what |
|---|---|
| `env.sh` | paths of this checkout: `DAAVAR` (the checkout), `TF_PIN` (TraceForge short sha), `EVAL_OUT` (build outputs, default `out/<pin>`), `JMC_CP_FILE`; puts `$JAVA_HOME/bin` on `PATH` when `JAVA_HOME` is set (JMC needs JDK 17); sourced by the others |
| `setup-node.sh` | build the checkout after `git submodule update --init`: TraceForge's binaries (with `conf-join`), JMC's classes and classpath file, every benchmark's Rust crate and Java sources (including `context-free/java` and `port-java/java`), each followed by its `SOURCE.md5`; `JOBS` sets the build parallelism (default `nproc`) |
| `print-classpath.gradle` | Gradle init script used by `setup-node.sh` to write JMC's test classpath |
| `source-manifest.sh rust\|java <dir> <dest>` | a benchmark's source manifest: `# root <daavar-relative dir>`, then `md5sum` lines sorted by path |
| `jmc-run.sh <bench> <Class[:a,b]> <strategy> <iterations> <seed> <export\|-> <timeoutS> [--spec <file>]` | run one Java benchmark program under JMC's launcher in the current directory; `<bench>` is a benchmark directory, or `<bench>@context-free` / `<bench>@port-java` for the Java sources in that subdirectory |
| `rust-run.sh <bench> <args...>` | run one benchmark's Rust binary (`count\|export\|conf\|online ... [--model M] -- <args>`) |
| `witness-join.sh <spec-export> <impl-export> <outdir> [conf-join args...]` | `conf-join --decide --witness-dir <outdir>/graphs`, then `witness-render.py --check` and the TikZ/DOT rendering of every witness graph, `<outdir>/WITNESS.md` and `summary.txt`; exits with conf-join's code (0 conforms, 1 violation, 2 refused) |
| `witness-render.py` | render a witness graph (`*.graph.json`) as TikZ or DOT; `--check` validates it |

Source manifests: `setup-node.sh` rewrites a benchmark's `SOURCE.md5` right after each successful build of it (Rust:
one `<bin>.SOURCE.md5` per bin beside the binary in `$EVAL_OUT/bench-target/release`; Java: in
`$EVAL_OUT/javaout/<bench>`). A binary or class file rebuilt after its manifest makes it stale, and the tools then
record `"source": "unknown"` in the witness graphs. A benchmark built any other way needs its own
`source-manifest.sh` call after the build.
