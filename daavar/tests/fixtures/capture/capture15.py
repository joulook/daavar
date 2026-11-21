"""S15 fixture capture, brain03 only: the JMC cases of capture.py at JMC fc684f21 (0.3.0), each in both CLI
forms, positional and named (DRIVER-IMPL-NOTES.md S15), through the deprecated main the configs name
(org.mpi_sws.jmc.mp.launch.MpLauncher, which forwards to org.mpi_sws.jmc.mp.cli.MpLauncher).

usage: python3 -I capture15.py <staged daavar tree> <work dir> <out dir> <jmc checkout> <cp file> <fx classes>
       <tf spec export> [case ...]

Leaves <out>/<form>/<name>/ as capture.py does (cmd.json, result.json, stdout, stderr, files/).
"""

import hashlib
import json
import os
import shutil
import sys
from pathlib import Path

D, W, OUT, JMC, CPF, FXC, SPEC = (Path(a).resolve() for a in sys.argv[1:8])
ONLY = set(sys.argv[8:])
sys.path.insert(0, str(D))
from daavar import runner  # noqa: E402

HOME = Path(os.environ["HOME"])
CP = CPF.read_text().strip()
JAVA = str(HOME / "jdk17/bin/java")
MAIN = "org.mpi_sws.jmc.mp.launch.MpLauncher"
ENV = {"PATH": f"{HOME}/jdk17/bin:{os.environ['PATH']}", "TMPDIR": str(W / "tmp")}


def pre(xmx="2g"):
    return [JAVA, "-ea", "-Xss4m", f"-Xmx{xmx}", "-cp", f"{FXC}:{CP}", MAIN]


def pos(prog, it="0", timeout="600", out="export.jsonl", spec=None, stop=False, strategy="must"):
    a = pre() + [prog, strategy, it, "1", "report", out, timeout, "--graphs", f"{out}.graphs.jsonl"]
    if spec:
        a += ["--spec", str(spec)] + (["--stop-at-first"] if stop else [])
    return a


def named(prog, it="0", timeout="600", out="export.jsonl", spec=None, stop=False, strategy="must"):
    a = pre() + ["online" if spec else "export", "--program", prog, "--strategy", strategy, "--iterations", it,
                 "--seed", "1", "--report-dir", "report", "--out", out, "--timeout", timeout,
                 "--graphs", f"{out}.graphs.jsonl"]
    if spec:
        a += ["--spec", str(spec)] + (["--stop-at-first"] if stop else [])
    return a


def both(prog, **k):
    return {"pos": pos(prog, **k), "named": named(prog, **k)}


# name, {form: argv}, timeout seconds, setup (a function of the case dir, or None)
C = [
    ("export-impl-ok", both("fx.Fx:impl,ok"), 300, None),
    ("export-spec-ok", both("fx.Fx:spec,ok"), 300, None),
    ("export-impl-bug", both("fx.Fx:impl,bug"), 300, None),
    ("export-impl-bug-bounded", both("fx.Fx:impl,bug", it="1"), 300, None),
    ("export-assert-spawned", both("fx.Fx:impl,assert-spawned"), 300, None),
    ("export-assert-main", both("fx.Fx:impl,assert-main"), 300, None),
    ("export-mainsend", both("fx.Fx:impl,mainsend"), 300, None),
    ("export-novisible", both("fx.Fx:impl,novisible"), 300, None),
    ("export-float", both("fx.Fx:impl,float"), 300, None),
    ("export-badargs", both("fx.Fx:impl,nope"), 300, None),
    ("export-badclass", both("fx.Nope:impl,ok"), 300, None),
    ("export-badstrategy", both("fx.Fx:impl,ok", strategy="nope"), 300, None),
    ("export-sleep", both("fx.Fx:impl,sleep"), 8, None),
    ("export-sleep-jmctimeout", both("fx.Fx:impl,sleep", timeout="3"), 60, None),
    ("export-unwritable", both("fx.Fx:impl,ok", out="blocker/export.jsonl"), 300,
     lambda c: (c / "blocker").write_text("a file where the export's directory should be\n")),
    ("online-ok", both("fx.Fx:impl,ok", spec=SPEC), 300, None),
    ("online-bug", both("fx.Fx:impl,bug", spec=SPEC), 300, None),
    ("online-bug-stop", both("fx.Fx:impl,bug", spec=SPEC, stop=True), 300, None),
    ("online-assert-spawned", both("fx.Fx:impl,assert-spawned", spec=SPEC), 300, None),
    ("online-spec-bounded", {f: g("fx.Fx:impl,ok", spec="SPEC_BOUNDED") for f, g in (("pos", pos), ("named", named))},
     300, None),
    ("help", {"pos": pre() + ["--help"], "named": pre() + ["--help"]}, 60, None),
    ("named-badopt", {"named": pre() + ["export", "--program", "fx.Fx:impl,ok", "--frobnicate"]}, 60, None),
    ("named-noout", {"named": pre() + ["export", "--program", "fx.Fx:impl,ok"]}, 60, None),
]

LIMIT = 256 * 1024


def keep(src: Path, dst: Path):
    dst.mkdir(parents=True, exist_ok=True)
    for p in sorted(src.rglob("*")):
        if not p.is_file():
            continue
        t = dst / p.relative_to(src)
        t.parent.mkdir(parents=True, exist_ok=True)
        size = p.stat().st_size
        if size > LIMIT:
            data = p.read_bytes()
            t.with_name(t.name + ".digest").write_text(json.dumps({
                "md5": hashlib.md5(data).hexdigest(), "size": size, "lines": data.count(b"\n"),
                "first_line": data.split(b"\n", 1)[0].decode("utf-8", "replace")[:4096]}, indent=1) + "\n")
        else:
            shutil.copyfile(p, t)


def main():
    for name, forms, timeout, setup in C:
        if ONLY and name not in ONLY:
            continue
        for form, argv in forms.items():
            cwd = W / "cases15" / form / name
            if cwd.exists():
                shutil.rmtree(cwd)
            cwd.mkdir(parents=True)
            if setup:
                setup(cwd)
            bounded = str(W / "cases15" / form / "export-impl-bug-bounded" / "export.jsonl")
            argv = [bounded if a == "SPEC_BOUNDED" else str(a) for a in argv]
            o = OUT / form / name
            if o.exists():
                shutil.rmtree(o)
            o.mkdir(parents=True)
            r = runner.run(argv, cwd, o / "stdout", o / "stderr", env_delta=ENV, timeout_s=timeout,
                           kill_grace_s=2.0)
            (o / "cmd.json").write_text(json.dumps({"argv": argv, "cwd": str(cwd), "timeout_s": timeout,
                                                    "env": ENV, "jmc": str(JMC)}, indent=1) + "\n")
            (o / "result.json").write_text(json.dumps({"exit": r.exit, "timed_out": r.timed_out,
                                                       "killed": r.killed, "wall_ms": r.wall_ms,
                                                       "peak_rss_kb": r.peak_rss_kb}, indent=1) + "\n")
            keep(cwd, o / "files")
            print(f"{form}/{name}: exit {r.exit} timed_out={r.timed_out} {r.wall_ms} ms", flush=True)


main()
