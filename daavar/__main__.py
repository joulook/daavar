"""The `daavar` command line (DRIVER-DESIGN §3, §8).

stdout carries one line, `daavar <command>: exit <n> <STATUS>[ <path>]`, or with --json one envelope
{"command", "exit", "status", "paths", "result"}; diagnostics go to stderr. No prompts; stdin is never read.
Commands: new, build, check, vocab, render, replay.
"""

from __future__ import annotations

import argparse
import json
import shlex
import signal
import sys
import traceback
from pathlib import Path

from . import DaavarError, __version__, config

COMMANDS = ("new", "build", "check", "vocab", "render", "replay")
NOT_YET = {}


class _Parser(argparse.ArgumentParser):
    def error(self, message):  # argparse would print usage and exit 2 without the stdout line
        raise DaavarError(f"{self.prog}: {message}")


def _emit(args, command: str, code: int, status: str, paths=(), result=None) -> int:
    if getattr(args, "json", False):
        print(json.dumps({"command": command, "exit": code, "status": status, "paths": list(paths),
                          "result": result if result is not None else {}}, sort_keys=False))
    else:
        tail = f" {paths[0]}" if paths else ""
        print(f"daavar{' ' + command if command else ''}: exit {code} {status}{tail}")
    return code


def _parser() -> argparse.ArgumentParser:
    p = _Parser(prog="daavar", description="Conformance checks of Rust and Java Must programs.")
    p.add_argument("--version", action="version", version=f"daavar {__version__}")
    sub = p.add_subparsers(dest="command", parser_class=_Parser)

    def common(sp):
        sp.add_argument("--json", action="store_true", help="one JSON envelope on stdout")
        sp.add_argument("--dry-run", action="store_true", help="print the commands, run nothing")
        sp.add_argument("--debug", action="store_true", help="a traceback with an unexpected error")

    b = sub.add_parser("build", help="build the tools and the project's programs (§3.2)")
    b.add_argument("dir", nargs="?", default=".")
    b.add_argument("--allow-dirty-tools", action="store_true")
    common(b)
    v = sub.add_parser("vocab", help="compare the message vocabulary of the Rust and Java programs (§3.4)")
    v.add_argument("dir", nargs="?", default=".")
    v.add_argument("--values", action="store_true", help="also compare payload-enum constants")
    common(v)
    c = sub.add_parser("check", help="export, join and judge every selected direction (§3.3)")
    c.add_argument("dir", nargs="?", default=".")
    c.add_argument("--scenario", action="append", default=[], help="scenario id(s), comma-separated, repeatable")
    c.add_argument("--cell", action="append", default=[], help="cell label(s), comma-separated, repeatable")
    for f in ("variant", "reading", "split"):
        c.add_argument(f"--{f}", action="append", default=[], help=f"shorthand of --where {f}=<values>")
    c.add_argument("--where", action="append", default=[], help="<field>=<values>, repeatable")
    c.add_argument("--model", choices=("fifo", "bag", "causal"))
    c.add_argument("--mode", choices=("offline", "online"))
    c.add_argument("--stop-at-first", action="store_true", default=None)
    c.add_argument("--no-stop-at-first", dest="stop_at_first", action="store_false",
                   help="override check.stop_at_first = true")
    c.add_argument("--directions", default="", help="checked<=ref,... (a subset of the cells' directions)")
    c.add_argument("--no-build", action="store_true")
    c.add_argument("--strict", action="store_true")
    c.add_argument("--no-wait", action="store_true")
    c.add_argument("--allow-dirty-tools", action="store_true")
    c.add_argument("--from-run", help="reproduce run R (§5 Restoring)")
    c.add_argument("--into", help="the directory to restore into (default ./restore-<run>)")
    common(c)
    nw = sub.add_parser("new", help="scaffold a project: a Rust spec and a Rust and/or Java impl (§3.1)")
    nw.add_argument("name")
    nw.add_argument("--impl", default="rust", help="rust, java or rust,java")
    nw.add_argument("--spec", default="rust", help="rust (a Java spec is refused in release 1)")
    nw.add_argument("--dir")
    common(nw)
    rr = sub.add_parser("render", help="check and render witness graphs (§3.5)")
    rr.add_argument("target")
    rr.add_argument("--format", default="tikz,dot,text")
    rr.add_argument("--out-dir")
    common(rr)
    rp = sub.add_parser("replay", help="replay a witness graph against its program (§3.6)")
    rp.add_argument("graph")
    rp.add_argument("--allow-source-change", action="store_true")
    rp.add_argument("--allow-tool-change", action="store_true")
    common(rp)
    for name in NOT_YET:
        x = sub.add_parser(name, help=f"(not in this release: DRIVER-DESIGN §10 {NOT_YET[name]})")
        x.add_argument("rest", nargs=argparse.REMAINDER)
        common(x)
    return p


def cmd_build(args) -> int:
    from . import build
    cfg = config.load(args.dir)
    if args.dry_run:
        code, cmds = build.dry_run(cfg)
        if not args.json:
            for c in cmds:
                env = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in c["env"].items())
                print(f"(cd {shlex.quote(c['cwd'])} && {env + ' ' if env else ''}{shlex.join(c['argv'])})",
                      file=sys.stderr)
        if code:
            print("daavar build: the JMC CLI form is auto and no --help probe is cached; run `daavar build` once",
                  file=sys.stderr)
        return _emit(args, "build", code, "DRY-RUN" if code == 0 else "REFUSED", (), {"commands": cmds})
    try:
        res = build.build(cfg, allow_dirty_tools=args.allow_dirty_tools)
    except build.BuildError as e:
        print(f"daavar build: {e}", file=sys.stderr)
        return _emit(args, "build", 2, "REFUSED", (), {"error": str(e)})
    for s in res.steps:
        print(f"daavar build: {s.name}: {'built' if s.built else 'cached'}", file=sys.stderr)
    paths = [a.get("out") or a.get("classes") for a in res.artifacts.values()]
    return _emit(args, "build", 0, "OK", paths, res.as_dict())


def _split(xs) -> list:
    return [y for x in xs for y in x.split(",") if y]


def check_options(args):
    from .check import Options
    where = config.parse_where(args.where) + [(f, _split(getattr(args, f))) for f in ("variant", "reading", "split")
                                              if getattr(args, f)]
    return Options(scenarios=_split(args.scenario), cells=_split(args.cell), where=where, model=args.model,
                   mode=args.mode, stop_at_first=args.stop_at_first, directions=_split([args.directions]),
                   no_build=args.no_build, strict=args.strict, no_wait=args.no_wait,
                   allow_dirty_tools=args.allow_dirty_tools)


def _print_plan(args, cmds) -> None:
    if args.json:
        return
    import shlex
    for c in cmds:
        env = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in (c.get("env") or {}).items())
        print(f"(cd {shlex.quote(str(c['cwd']))} && {env + ' ' if env else ''}{shlex.join(c['argv'])})", file=sys.stderr)


def cmd_check(args) -> int:
    import os
    from . import check, provenance
    log = (lambda m: print(f"daavar check: {m}", file=sys.stderr))
    if args.from_run:
        given = [f for f in ("scenario", "cell", "variant", "reading", "split", "where") if getattr(args, f)] + \
            [f for f in ("model", "mode") if getattr(args, f)] + \
            (["stop-at-first"] if args.stop_at_first is not None else []) + \
            (["directions"] if args.directions else []) + \
            [f.replace("_", "-") for f in ("no_build", "strict", "no_wait", "allow_dirty_tools") if getattr(args, f)]
        if given or args.dir != ".":
            raise DaavarError(f"--from-run takes the run's own selection and options; refused with "
                              f"{', '.join(given) or 'a project directory'} (Opus F13)")
        R = Path(args.from_run).resolve()
        if not (R / "provenance.json").is_file():
            raise DaavarError(f"--from-run {R}: no provenance.json")
        D = Path(args.into).resolve() if args.into else Path.cwd() / f"restore-{R.name}"
        if args.dry_run:
            cmds = provenance.restore_plan(R, D)
            _print_plan(args, cmds)
            return _emit(args, "check", 0, "DRY-RUN", (), {"restore_into": str(D), "commands": cmds})
        loc = provenance.restore(R, D, log)
        prov = loc["provenance"]
        os.environ["DAAVAR_CACHE"] = str(D / ".daavar-cache")  # §5: rebuild with an empty cache
        if loc["daavar"] is not None:
            os.environ["DAAVAR"] = str(loc["daavar"])
        o = prov["options"]
        opts = check.Options(scenarios=o["scenarios"], cells=o["cells"], where=[tuple(w) for w in o["where"]],
                             model=o["model"], mode=o["mode"], stop_at_first=o["stop_at_first"],
                             directions=o["directions"], strict=o.get("strict", False), allow_dirty_tools=True)
        cfg = config.load(loc["project"])
        res = check.Check(cfg, opts, log).run()
        N = Path(res["run"])
        cmp = provenance.compare(R, N, [r["file"] for r in loc["rebased"]])
        digests = provenance.verify_artifacts(prov, json.loads((N / "provenance.json").read_text()), loc["rebased"], R, N)
        (N / "reproduction.json").write_text(json.dumps({"from_run": str(R), "into": str(D), **cmp,
                                                         "artifacts": digests}, indent=1) + "\n")
        for x in cmp["compared"]:
            if not x["equal"]:
                log(f"not reproduced: {x['file']}")
        bad = [x for x in digests if not x["equal"] and not x["allowed"]]
        for x in digests:
            if not x["equal"]:
                log(f"artifact differs: {x['name']}: {x['why'] or 'NOT ALLOWED'}")
        if not cmp["equal"] or bad:
            return _emit(args, "check", 2, "NOT-REPRODUCED", [str(N)], {**res, **cmp})
        return _emit(args, "check", res["exit"], res["status"], [str(N)], {**res, "reproduced": True, "n": cmp["n"]})
    if args.into:
        raise DaavarError("--into is the restore directory of --from-run; without --from-run it is refused")
    cfg = config.load(args.dir)
    opts = check_options(args)
    c = check.Check(cfg, opts, log)
    if args.dry_run:
        code, cmds = c.plan()
        _print_plan(args, cmds)
        if code:
            print("daavar check: the JMC CLI form is auto and no --help probe is cached; run `daavar build` once",
                  file=sys.stderr)
        return _emit(args, "check", code, "DRY-RUN" if code == 0 else "REFUSED", (), {"commands": cmds})
    res = c.run()
    return _emit(args, "check", res["exit"], res["status"], [res["run"]], res)


def cmd_new(args) -> int:
    from . import scaffold
    if args.dry_run:  # validates exactly as `new` does, writes nothing
        d, files = scaffold.new(args.name, args.impl, args.spec, Path(args.dir) if args.dir else None,
                                warn=lambda m: print(f"daavar new: warning: {m}", file=sys.stderr), dry=True)
        if not args.json:
            for f in files:
                print(f"would write {d / f}", file=sys.stderr)
        return _emit(args, "new", 0, "DRY-RUN", [str(d)], {"dir": str(d), "files": files})
    d = scaffold.new(args.name, args.impl, args.spec, Path(args.dir) if args.dir else None,
                     warn=lambda m: print(f"daavar new: warning: {m}", file=sys.stderr))
    return _emit(args, "new", 0, "OK", [str(d)], {"dir": str(d)})


def cmd_render(args) -> int:
    from . import replay
    if args.dry_run:
        cmds = replay.render_plan(Path(args.target), args.format, Path(args.out_dir) if args.out_dir else None)
        _print_plan(args, cmds)
        return _emit(args, "render", 0, "DRY-RUN", (), {"commands": cmds})
    code, out, cmds = replay.render(Path(args.target), args.format, Path(args.out_dir) if args.out_dir else None)
    status = {0: "OK", 1: "G9-MISMATCH"}.get(code, "INVALID")
    return _emit(args, "render", code if code in (0, 1) else 2, status, [str(out)] if out else [], {"commands": cmds})


def cmd_replay(args) -> int:
    from . import replay
    if args.dry_run:
        cmds = replay.plan(Path(args.graph), args.allow_source_change, args.allow_tool_change)
        _print_plan(args, cmds)
        return _emit(args, "replay", 0, "DRY-RUN", (), {"commands": cmds})
    code, d, report, argv = replay.replay(Path(args.graph), args.allow_source_change, args.allow_tool_change)
    if report:
        print(f"daavar replay: {report.get('result')} {report.get('kind') or ''} {report.get('at') or ''}".rstrip(),
              file=sys.stderr)
    status = {0: "REPRODUCED", 1: "DEVIATION"}.get(code, "REFUSED")
    return _emit(args, "replay", code if code in (0, 1) else 2, status, [str(d)], {"report": report, "argv": argv})


def cmd_vocab(args) -> int:
    from . import vocab
    cfg = config.load(args.dir)
    if args.dry_run:
        return _emit(args, "vocab", 0, "DRY-RUN", (), {"commands": []})
    r = vocab.run(cfg, values=args.values)
    for n in r.notes:
        print(f"daavar vocab: {n}", file=sys.stderr)
    for prog, kind, text in r.differences:
        print(f"daavar vocab: {prog}: {kind}: {text}", file=sys.stderr)
    return _emit(args, "vocab", r.exit, r.status, (), r.as_dict())


def _on_sigterm(signum, frame):
    from .runner import Interrupted
    raise Interrupted()


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    command = next((a for a in argv if a in COMMANDS), "")
    p = _parser()
    # SIGTERM takes the same path as Ctrl-C: the runner stops the child's process group, then main exits.
    try:
        old = signal.signal(signal.SIGTERM, _on_sigterm)
    except ValueError:  # not the main thread (tests)
        old = None
    from .runner import Interrupted
    ns = argparse.Namespace(json="--json" in argv)
    try:
        args = p.parse_args(argv)
        if not args.command:
            raise DaavarError("no command; one of " + ", ".join(COMMANDS))
        if args.command in NOT_YET:
            raise DaavarError(f"{args.command} is not in this release (DRIVER-DESIGN §10 {NOT_YET[args.command]})")
        return {"build": cmd_build, "vocab": cmd_vocab, "check": cmd_check, "render": cmd_render, "new": cmd_new,
                "replay": cmd_replay}[args.command](args)
    except DaavarError as e:
        print(f"daavar: {e}", file=sys.stderr)
        run = getattr(e, "run", None)
        return _emit(ns, command, 2, "REFUSED", [run] if run else (), {"error": str(e)})
    except (KeyboardInterrupt, Interrupted):
        print("daavar: interrupted; the running tool was stopped", file=sys.stderr)
        return _emit(ns, command, 2, "INTERRUPTED", (), {"error": "interrupted"})
    except Exception as e:  # review S1-S6 F4: never a traceback and exit 1 (1 means VIOLATION for check)
        if "--debug" in argv:
            traceback.print_exc()
        first = (str(e).splitlines() or [""])[0]
        first = "".join(ch if ch.isprintable() else "?" for ch in first)[:500]
        print(f"daavar: unexpected error: {type(e).__name__}: {first} (a driver defect; --debug shows the traceback)",
              file=sys.stderr)
        return _emit(ns, command, 2, "REFUSED", (), {"error": f"{type(e).__name__}: {e}"})
    finally:
        if old is not None:
            signal.signal(signal.SIGTERM, old)


if __name__ == "__main__":
    sys.exit(main())
