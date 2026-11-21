"""S3 fixture capture, brain03 only: runs each case through daavar.runner and keeps what the parsers and
the §4.1 classification read (DRIVER-IMPL-NOTES.md S3).

usage: python3 -I capture.py <staged daavar tree> <work dir> <out dir> [case ...]

Each case runs in a fresh directory <work>/cases/<name> (its cwd; outputs are named relative to it) and
leaves <out>/<name>/: cmd.json (argv, cwd, timeout), result.json (exit, timed_out, killed, wall_ms),
stdout, stderr, and files/ (every file the case wrote, those over 256 KiB replaced by <file>.digest with
md5, size, line count and first line).
"""

import hashlib
import json
import os
import shutil
import sys
from pathlib import Path

D, W, OUT = (Path(a).resolve() for a in sys.argv[1:4])
ONLY = set(sys.argv[4:])
sys.path.insert(0, str(D))
from daavar import runner  # noqa: E402

X = Path("/local/mkhoshechin/tmp/xcheck-pins")
HOME = Path(os.environ["HOME"])
FX = W / "fx-target/release/tf_fx"
A2A = W / "a2a-target/release/tf_a2a_task"
CJ = X / "tf/target/release/conf-join"
CP = (X / "jmc/cp.txt").read_text().strip()
JAVA = str(HOME / "jdk17/bin/java")
MAIN = "org.mpi_sws.jmc.mp.launch.MpLauncher"
JOIN = str(D / "scripts/witness-join.sh")
ENV = {
    "PATH": f"{HOME}/jdk17/bin:{HOME}/.cargo/bin:{os.environ['PATH']}",
    "TMPDIR": str(W / "tmp"),
    "WITNESS_CONF_JOIN": str(CJ),
}
CASES = W / "cases"


def jmc(classes, prog, *rest, xmx="2g"):
    return [JAVA, "-ea", "-Xss4m", f"-Xmx{xmx}", "-cp", f"{classes}:{CP}", MAIN, prog, *rest]


def fxj(prog, *rest, it="0"):
    return jmc(W / "fx-classes", prog, "must", it, "1", "report", "export.jsonl", "600",
               "--graphs", "export.jsonl.graphs.jsonl", *rest)


def tfx(sub, side, args, *rest, binary=FX, program="fx"):
    if sub == "export":
        return [binary, "export", side, "--out", "export.jsonl", "--graphs", "export.jsonl.graphs.jsonl",
                "--model", "fifo", "--program", program, *rest, "--", args]
    return [binary, "online", *rest, "--out", "export.jsonl", "--graphs", "export.jsonl.graphs.jsonl",
            "--model", "fifo", "--program", program, "--", args]


def case(name):
    return CASES / name


def exp(name):
    return str(case(name) / "export.jsonl")


JARGS = ["--decide-budget", "1000000", "--witness-limit", "20", "--witness-nearest", "3"]

# name, argv (a function, so later cases may name earlier outputs), timeout seconds
C = [
    # ---- TraceForge exports (tf_fx)
    ("tf-export-spec-ok", lambda: tfx("export", "spec", "spec,ok"), 120),
    ("tf-export-impl-ok", lambda: tfx("export", "impl", "impl,ok"), 120),
    ("tf-export-impl-bug", lambda: tfx("export", "impl", "impl,bug"), 120),
    ("tf-export-impl-bug-bag", lambda: [FX, "export", "impl", "--out", "export.jsonl", "--graphs",
                                        "export.jsonl.graphs.jsonl", "--model", "bag", "--program", "fx", "--",
                                        "impl,bug"], 120),
    ("tf-export-impl-panic", lambda: tfx("export", "impl", "impl,panic"), 120),
    ("tf-export-spec-panic", lambda: tfx("export", "spec", "spec,panic"), 120),
    ("tf-export-impl-stdassert", lambda: tfx("export", "impl", "impl,stdassert"), 120),
    ("tf-export-impl-assert", lambda: tfx("export", "impl", "impl,assert"), 120),
    ("tf-export-spec-assert", lambda: tfx("export", "spec", "spec,assert"), 120),
    ("tf-export-usage", lambda: tfx("export", "impl", "impl,nope"), 120),
    ("tf-export-badvis", lambda: tfx("export", "impl", "impl,badvis"), 120),
    ("tf-export-badopt", lambda: [FX, "export", "impl", "--out", "export.jsonl", "--frobnicate", "--", "impl,ok"], 120),
    ("tf-export-sleep", lambda: tfx("export", "impl", "impl,sleep"), 3),
    ("tf-export-threads2", lambda: tfx("export", "impl", "impl,bug", "--threads", "2", "--graphs-limit", "0"), 120),
    # ---- TraceForge online (against the tf spec export)
    ("tf-online-ok", lambda: tfx("online", "impl", "impl,ok", "--spec", exp("tf-export-spec-ok")), 120),
    ("tf-online-bug", lambda: tfx("online", "impl", "impl,bug", "--spec", exp("tf-export-spec-ok")), 120),
    ("tf-online-bug-stop", lambda: tfx("online", "impl", "impl,bug", "--spec", exp("tf-export-spec-ok"),
                                       "--stop-at-first"), 120),
    ("tf-online-bug-budget", lambda: tfx("online", "impl", "impl,bug", "--spec", exp("tf-export-spec-ok"),
                                         "--budget", "1"), 120),
    ("tf-online-panic", lambda: tfx("online", "impl", "impl,panic", "--spec", exp("tf-export-spec-ok")), 120),
    ("tf-online-assert", lambda: tfx("online", "impl", "impl,assert", "--spec", exp("tf-export-spec-ok")), 120),
    ("tf-online-stop-threads2", lambda: [FX, "online", "--spec", exp("tf-export-spec-ok"), "--threads", "2",
                                         "--stop-at-first", "--model", "fifo", "--", "impl,bug"], 120),
    ("tf-online-spec-bounded", lambda: tfx("online", "impl", "impl,ok", "--spec", exp("tf-online-bug-budget")), 120),
    # ---- JMC (fx.Fx), exports and online against the tf spec export
    ("jmc-export-impl-ok", lambda: fxj("fx.Fx:impl,ok"), 300),
    ("jmc-export-spec-ok", lambda: fxj("fx.Fx:spec,ok"), 300),
    ("jmc-export-impl-bug", lambda: fxj("fx.Fx:impl,bug"), 300),
    ("jmc-export-impl-bug-bounded", lambda: fxj("fx.Fx:impl,bug", it="1"), 300),
    ("jmc-export-assert-spawned", lambda: fxj("fx.Fx:impl,assert-spawned"), 300),
    ("jmc-export-assert-main", lambda: fxj("fx.Fx:impl,assert-main"), 300),
    ("jmc-export-mainsend", lambda: fxj("fx.Fx:impl,mainsend"), 300),
    ("jmc-export-novisible", lambda: fxj("fx.Fx:impl,novisible"), 300),
    ("jmc-export-float", lambda: fxj("fx.Fx:impl,float"), 300),
    ("jmc-export-badargs", lambda: fxj("fx.Fx:impl,nope"), 300),
    ("jmc-export-badclass", lambda: fxj("fx.Nope:impl,ok"), 300),
    ("jmc-export-badstrategy", lambda: jmc(W / "fx-classes", "fx.Fx:impl,ok", "nope", "0", "1", "report",
                                           "export.jsonl", "600"), 300),
    ("jmc-export-sleep", lambda: fxj("fx.Fx:impl,sleep"), 8),
    ("jmc-online-ok", lambda: fxj("fx.Fx:impl,ok", "--spec", exp("tf-export-spec-ok")), 300),
    ("jmc-online-bug", lambda: fxj("fx.Fx:impl,bug", "--spec", exp("tf-export-spec-ok")), 300),
    ("jmc-online-bug-stop", lambda: fxj("fx.Fx:impl,bug", "--spec", exp("tf-export-spec-ok"), "--stop-at-first"), 300),
    ("jmc-online-assert-spawned", lambda: fxj("fx.Fx:impl,assert-spawned", "--spec", exp("tf-export-spec-ok")), 300),
    ("jmc-online-spec-bounded", lambda: fxj("fx.Fx:impl,ok", "--spec", exp("jmc-export-impl-bug-bounded")), 300),
    ("jmc-help", lambda: jmc(W / "fx-classes", "--help"), 300),
    # ---- joins (witness-join.sh, as §3.3 step 6)
    ("join-tf-ok", lambda: [JOIN, exp("tf-export-spec-ok"), exp("tf-export-impl-ok"), "j", *JARGS], 300),
    ("join-tf-bug", lambda: [JOIN, exp("tf-export-spec-ok"), exp("tf-export-impl-bug"), "j", *JARGS], 300),
    ("join-tf-online-stop", lambda: [JOIN, exp("tf-export-spec-ok"), exp("tf-online-bug-stop"), "j", *JARGS], 300),
    ("join-jmc-ok", lambda: [JOIN, exp("tf-export-spec-ok"), exp("jmc-export-impl-ok"), "j", *JARGS], 300),
    ("join-jmc-bug", lambda: [JOIN, exp("tf-export-spec-ok"), exp("jmc-export-impl-bug"), "j", *JARGS], 300),
    ("join-twin-jmc-in-tf", lambda: [JOIN, exp("tf-export-impl-bug"), exp("jmc-export-impl-bug"), "j", *JARGS], 300),
    ("join-refused-model", lambda: [JOIN, exp("tf-export-spec-ok"), exp("tf-export-impl-bug-bag"), "j", *JARGS], 300),
    ("join-refused-incomplete", lambda: [JOIN, exp("tf-online-bug-budget"), exp("tf-export-impl-ok"), "j", *JARGS], 300),
    ("join-refused-bounded-jmc", lambda: [JOIN, exp("jmc-export-impl-bug-bounded"), exp("tf-export-impl-ok"), "j",
                                          *JARGS], 300),
    ("join-merge-unsorted", lambda: [JOIN, exp("tf-export-spec-ok"), exp("jmc-export-impl-bug"), "j", "--merge",
                                     *JARGS], 300),
    ("conf-join-usage", lambda: [CJ, exp("tf-export-spec-ok")], 60),
    # ---- a2a-task, one.b (the benchmark at the staged tree): spec, impl (both tools), joins
    ("a2a-tf-export-spec-b", lambda: tfx("export", "spec", "spec,b", binary=A2A, program="a2a-task"), 600),
    ("a2a-tf-export-impl-b", lambda: tfx("export", "impl", "impl,b", binary=A2A, program="a2a-task"), 600),
    ("a2a-jmc-export-impl-b", lambda: jmc(W / "a2a-classes", "bench.a2a_task.A2ATask:impl,b", "must", "0", "1",
                                          "report", "export.jsonl", "600", "--graphs", "export.jsonl.graphs.jsonl",
                                          xmx="16g"), 900),
    ("a2a-join-rust-b", lambda: [JOIN, exp("a2a-tf-export-spec-b"), exp("a2a-tf-export-impl-b"), "j", *JARGS], 900),
    ("a2a-join-java-b", lambda: [JOIN, exp("a2a-tf-export-spec-b"), exp("a2a-jmc-export-impl-b"), "j", *JARGS], 900),
    ("a2a-tf-export-usage-panic", lambda: tfx("export", "impl", "impl,q", binary=A2A, program="a2a-task"), 120),
]

LIMIT = 256 * 1024


def keep(src: Path, dst: Path):
    dst.mkdir(parents=True, exist_ok=True)
    for p in sorted(src.rglob("*")):
        if not p.is_file():
            continue
        rel = p.relative_to(src)
        t = dst / rel
        t.parent.mkdir(parents=True, exist_ok=True)
        size = p.stat().st_size
        if size > LIMIT:
            data = p.read_bytes()
            first = data.split(b"\n", 1)[0].decode("utf-8", "replace")
            t.with_name(t.name + ".digest").write_text(json.dumps({
                "md5": hashlib.md5(data).hexdigest(), "size": size, "lines": data.count(b"\n"),
                "first_line": first[:4096]}, indent=1) + "\n")
        else:
            shutil.copyfile(p, t)


def main():
    CASES.mkdir(parents=True, exist_ok=True)
    OUT.mkdir(parents=True, exist_ok=True)
    for name, argv_f, timeout in C:
        if ONLY and name not in ONLY:
            continue
        cwd = case(name)
        if cwd.exists():
            shutil.rmtree(cwd)
        cwd.mkdir(parents=True)
        argv = [str(a) for a in argv_f()]
        o = OUT / name
        if o.exists():
            shutil.rmtree(o)
        o.mkdir(parents=True)
        r = runner.run(argv, cwd, o / "stdout", o / "stderr", env_delta=ENV, timeout_s=timeout, kill_grace_s=2.0)
        (o / "cmd.json").write_text(json.dumps({"argv": argv, "cwd": str(cwd), "timeout_s": timeout,
                                                "env": ENV}, indent=1) + "\n")
        (o / "result.json").write_text(json.dumps({"exit": r.exit, "timed_out": r.timed_out, "killed": r.killed,
                                                   "wall_ms": r.wall_ms, "peak_rss_kb": r.peak_rss_kb},
                                                  indent=1) + "\n")
        keep(cwd, o / "files")
        print(f"{name}: exit {r.exit} timed_out={r.timed_out} {r.wall_ms} ms", flush=True)


main()
