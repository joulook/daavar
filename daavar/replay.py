"""`daavar render` (DRIVER-DESIGN §3.5) and `daavar replay` (§3.6).

render: witness-render.py --check, then the formats; the renderer's exit (0 ok, 1 G9 mismatch, 2 invalid). A
graph inside a run is rendered into `out/renders/<id>/` unless --out-dir says otherwise, so the run stays as written.

replay: a graph inside a run is resolved through the run's provenance.json (the build-key and the artifact digest
recorded for the graph's tool); the cached artifact is used if its digest matches, otherwise the run is restored
from its inputs (§5) and rebuilt. A standalone graph is resolved only if exactly one cached artifact matches its
tool identity, manifest and entry. The replay runs in a fresh `out/replays/<id>/`; the run is never written.
Exit 0 reproduced, 1 deviation, 2 refused.
"""

from __future__ import annotations

import json
import os
import secrets
import sys
import time
from pathlib import Path

from . import DaavarError, build, cache, manifest, runner


def _render_script() -> Path:
    return manifest.driver_checkout() / "scripts" / "witness-render.py"


def run_root(p: Path) -> Path | None:
    """The run directory holding p (the nearest ancestor with provenance.json), or None."""
    for d in [p.parent, *p.parents]:
        if (d / "provenance.json").is_file():
            return d
    return None


def _fresh(base: Path, kind: str, stem: str) -> Path:
    d = base / kind / f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{stem}-{secrets.token_hex(3)}"
    d.mkdir(parents=True)
    return d


def render(target: Path, formats: str = "tikz,dot,text", out_dir: Path | None = None) -> tuple:
    """(exit, out_dir, commands)."""
    target = target.resolve()
    if not target.exists():
        raise DaavarError(f"render: no {target}")
    if target.is_dir() and (target / "provenance.json").is_file():
        alld = sorted(target.glob("*/joins/*/graphs"))
        gdirs = [g for g in alld if any(g.glob("*.graph.json"))]
        for g in alld:
            if g not in gdirs:  # a conforming direction has no witness graph
                print(f"daavar render: skipped {g.relative_to(target)}: no witness graph", file=sys.stderr)
        if not gdirs:
            raise DaavarError(f"render: the run {target.name} has no witness graphs")
        worst, outs, allc = 0, None, []
        for gd in gdirs:
            code, outs, cmds = render(gd, formats, out_dir)
            worst, allc = max(worst, code), allc + cmds
        return worst, outs, allc
    if target.is_dir() and not list(target.glob("*.graph.json")):
        raise DaavarError(f"render: no *.graph.json in {target}")
    run = run_root(target)
    if out_dir is None and run is not None:
        out_dir = _fresh(run.parent, "renders", target.name.split(".")[0])
    if out_dir is not None:
        Path(out_dir).mkdir(parents=True, exist_ok=True)
    py = sys.executable
    script = _render_script()
    cmds = []
    chk = [py, "-I", str(script), "--check", "-q", str(target)]
    code, o, e = runner.capture(chk, timeout_s=3600)
    cmds.append({"argv": chk, "exit": code})
    sys.stderr.write(e.decode(errors="replace"))
    if code != 0:
        return code, out_dir, cmds
    fmt = [py, "-I", str(script), "--format", formats, "-q", *(["--out-dir", str(out_dir)] if out_dir else []),
           str(target)]
    code, o, e = runner.capture(fmt, timeout_s=3600)
    sys.stderr.write(e.decode(errors="replace"))
    cmds.append({"argv": fmt, "exit": code})
    return code, out_dir, cmds


# ---------------------------------------------------------------- replay


def _graph(path: Path) -> dict:
    try:
        with open(path, "rb") as f:
            g = json.loads(f.readline())
    except (OSError, ValueError) as e:
        raise DaavarError(f"replay: {path}: not a graph file: {e}") from None
    if not isinstance(g, dict) or g.get("witness") != 1 or not isinstance(g.get("run"), dict):
        raise DaavarError(f"replay: {path}: not a witness graph (format 1)")
    return g


def _artifact_ok(a: dict, lang: str) -> bool:
    d = Path(a["dir"])
    if not (d / "build.json").is_file():
        return False
    if lang == "rust":
        return all(cache.sha256_file(d / "out" / k) == v for k, v in a["sha256"].items()
                   if (d / "out" / k).is_file()) and all((d / "out" / k).is_file() for k in a["sha256"])
    return cache.tree_digest(d / "out") == a["sha256"]


def _jmc_ok(jtool: dict | None, recorded: dict) -> bool:
    """The cached JMC tool still has the classes and jars the run recorded (final review 2)."""
    if not jtool:
        return False
    d = Path(jtool.get("dir") or cache.root() / "tools" / f"jmc-{recorded['identity']}")
    if recorded.get("classes_sha256") and cache.tree_digest(d / "tree") != recorded["classes_sha256"]:
        return False
    if recorded.get("jars_sha256"):
        jars = {e: cache.sha256_file(Path(e)) for e in jtool["classpath"] if Path(e).is_file()}
        if cache.key_of(sorted(jars.values())) != recorded["jars_sha256"]:
            return False
    return True


def resolve(graph_path: Path, g: dict, allow_source: bool = False, allow_tool: bool = False) -> dict:
    """{'lang', 'artifact', 'jmc' (tool dict or None), 'run'}: the artifact the graph was made with."""
    rid = g["run"]
    lang = {"traceforge": "rust", "jmc": "java"}.get(rid.get("tool"))
    if lang is None:
        raise DaavarError(f"replay: unknown tool {rid.get('tool')!r} in the graph")
    run = run_root(graph_path)
    if run is not None:
        prov = json.loads((run / "provenance.json").read_text())
        a = prov["artifacts"].get(lang)
        if a is None:
            raise DaavarError(f"replay: the run {run.name} has no {lang} artifact")
        jtool = None
        if lang == "java":
            jt = prov["tools"]["jmc"]
            tdir = cache.root() / "tools" / f"jmc-{jt['identity']}"
            if (tdir / "tool.json").is_file():
                jtool = {**json.loads((tdir / "tool.json").read_text()), "dir": str(tdir)}
        if _artifact_ok(a, lang) and (lang == "rust" or _jmc_ok(jtool, prov["tools"]["jmc"])):
            return {"lang": lang, "artifact": a, "jmc": jtool, "run": run, "main": _main(prov)}
        return _rebuild(run, prov, lang, allow_source, allow_tool)
    # standalone: exactly one cached artifact for this tool identity, manifest and entry
    matches = []
    adir = cache.root() / "artifacts"
    src = rid.get("source")
    for bj in sorted(adir.glob("*/build.json")) if adir.is_dir() else []:
        b = json.loads(bj.read_text())
        ki = b["key_inputs"]
        if ki["kind"] != lang or not str(ki["tool"]).startswith(str(rid.get("tool_commit"))):
            continue
        files = manifest.files_of(b["manifest"])
        if isinstance(src, dict) and files != src.get("files"):
            continue
        if lang == "rust" and rid.get("entry") not in ki.get("bins", []):
            continue
        matches.append({"key": b["key"], "dir": str(bj.parent), "sha256": b["sha256"], "key_inputs": ki})
    if not matches:
        raise DaavarError("replay: no run holds this graph and no cached artifact matches its tool identity "
                          f"({rid.get('tool')} {rid.get('tool_commit')}), manifest and entry {rid.get('entry')}")
    if len(matches) > 1:
        raise DaavarError(f"replay: {len(matches)} cached artifacts match (build-keys "
                          f"{', '.join(m['key'][:12] for m in matches)}); replay it from its run")
    a = matches[0]
    if not _artifact_ok(a, lang):  # a damaged cache entry is never used (final review 2)
        raise DaavarError(f"replay: the cached artifact {a['key'][:12]} does not match its recorded digest; "
                          f"rebuild it (daavar build) or replay from the graph's run")
    jtool = None
    if lang == "java":
        tdir = cache.root() / "tools" / f"jmc-{a['key_inputs']['tool']}"
        if not (tdir / "tool.json").is_file():
            raise DaavarError(f"replay: no cached JMC tool {a['key_inputs']['tool']}")
        jtool = {**json.loads((tdir / "tool.json").read_text()), "dir": str(tdir)}
        bad = []
        if jtool.get("classes_sha256") and cache.tree_digest(tdir / "tree") != jtool["classes_sha256"]:
            bad.append("the JMC classes differ from their recorded digest")
        for e, h in (jtool.get("jars_sha256") or {}).items():
            if not Path(e).is_file() or cache.sha256_file(Path(e)) != h:
                bad.append(f"{e} differs from its recorded digest")
        if not jtool.get("classes_sha256"):
            bad.append("the cached JMC tool records no classes digest")
        for why in bad:  # final review 2, Astra 1: every resolution path verifies the tool
            if not allow_tool:
                raise DaavarError(f"replay: {why} (--allow-tool-change accepts it)")
            print(f"daavar replay: accepted: {why} (--allow-tool-change)", file=sys.stderr)
    return {"lang": lang, "artifact": a, "jmc": jtool, "run": None, "main": "org.mpi_sws.jmc.mp.launch.MpLauncher"}


def _main(prov) -> str:
    return prov.get("jmc_main") or "org.mpi_sws.jmc.mp.launch.MpLauncher"


def _rebuild(run: Path, prov: dict, lang: str, allow_source: bool = False, allow_tool: bool = False) -> dict:
    """The recorded artifact is gone or differs: restore the run's inputs (§5) and build them."""
    from . import config, provenance
    D = _fresh(run.parent, "replays", "restore")
    loc = provenance.restore(run, D / "tree", lambda m: print(f"daavar replay: {m}", file=sys.stderr))
    saved = {k: os.environ.get(k) for k in ("DAAVAR", "DAAVAR_CACHE")}
    try:
        os.environ["DAAVAR_CACHE"] = str(D / ".daavar-cache")  # never the cache entry that failed its digest
        if loc["daavar"] is not None:
            os.environ["DAAVAR"] = str(loc["daavar"])
        cfg = config.load(loc["project"])
        res = build.build(cfg, allow_dirty_tools=True)
        nt = {}
        if "jmc" in res.tools:
            jd = Path(res.tools["jmc"]["dir"])
            tj = json.loads((jd / "tool.json").read_text())
            nt["jmc"] = {"identity": res.tools["jmc"]["identity"], "classes_sha256": cache.tree_digest(jd / "tree"),
                         "jars_sha256": cache.key_of(sorted(tj.get("jars_sha256", {}).values()))}
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    a = res.artifacts[lang]
    # §3.6: the digest is checked again; the rule of provenance.verify_artifacts (D15 narrowed): only a Rust
    # binary rebuilt elsewhere from the same build key may differ. --allow-source-change accepts a project
    # artifact difference, --allow-tool-change a tool difference; anything else is refused.
    rec = prov["artifacts"][lang]
    bad = []
    if lang == "rust":
        for k, h in rec["sha256"].items():
            if a["sha256"].get(k) != h and not (not k.endswith(".SOURCE.md5") and a["key"] == rec["key"]):
                bad.append(("source", f"{k} differs"))
    elif a["sha256"] != rec["sha256"]:
        bad.append(("source", "the Java classes differ"))
    if lang == "java":
        for k in ("identity", "classes_sha256", "jars_sha256"):
            if prov["tools"]["jmc"].get(k) != nt["jmc"].get(k):
                bad.append(("tool", f"JMC {k} differs"))
    for kind, why in bad:
        if (kind == "source" and not allow_source) or (kind == "tool" and not allow_tool):
            raise DaavarError(f"replay: the rebuilt artifact does not match the run's: {why} "
                              f"(--allow-{kind}-change accepts it)")
        print(f"daavar replay: accepted: {why} (--allow-{kind}-change)", file=sys.stderr)
    return {"lang": lang, "artifact": a, "jmc": res.tools.get("jmc"), "run": run, "main": _main(prov)}


def replay(graph_path: Path, allow_source: bool = False, allow_tool: bool = False) -> tuple:
    """(exit, replay dir, report dict or None, command)."""
    graph_path = graph_path.resolve()
    g = _graph(graph_path)
    r = resolve(graph_path, g, allow_source, allow_tool)
    rid = g["run"]
    base = (r["run"].parent if r["run"] is not None else Path.cwd() / "out")
    d = _fresh(base, "replays", Path(graph_path.name).stem.split(".")[0])
    allow = (["--allow-source-change"] if allow_source else []) + (["--allow-tool-change"] if allow_tool else [])
    rep = d / "replay.json"
    if r["lang"] == "rust":
        binary = Path(r["artifact"]["dir"]) / "out" / rid["entry"]
        argv = [str(binary), "replay", "--graph", str(graph_path), "--report", str(rep), *allow, "--",
                *rid.get("args", [])]
    else:
        java = build.java_tools()[0]
        cp = ":".join([str(Path(r["artifact"]["dir"]) / "out"), *r["jmc"]["classpath"]])
        prog = rid["entry"] + (f":{rid['args'][0]}" if rid.get("args") else "")
        argv = [java, "-ea", "-Xss4m", "-cp", cp, r["main"], "--replay", str(graph_path), "--report", str(rep),
                *allow, prog]
    res = runner.run(argv, d, d / "replay.out", d / "replay.err", timeout_s=3600)
    (d / "replay.cmd").write_text(" ".join(argv) + "\n")
    try:
        report = json.loads(rep.read_text())
    except (OSError, ValueError):
        report = None
    return res.exit, d, report, argv


def render_plan(target: Path, formats: str, out_dir: Path | None) -> list:
    """The renderer's two commands for each graph location, started by no process (--dry-run)."""
    target = target.resolve()
    if not target.exists():
        raise DaavarError(f"render: no {target}")
    tgts = ([g for g in sorted(target.glob("*/joins/*/graphs")) if any(g.glob("*.graph.json"))]
            if (target / "provenance.json").is_file() else [target])
    if not tgts:
        raise DaavarError(f"render: the run {target.name} has no witness graphs")
    out = []
    for t in tgts:
        run = run_root(t)
        od = out_dir or (run.parent / "renders" / "<id>" if run is not None else None)
        out.append({"cwd": str(Path.cwd()), "argv": [sys.executable, "-I", str(_render_script()), "--check", "-q", str(t)]})
        out.append({"cwd": str(Path.cwd()), "argv": [sys.executable, "-I", str(_render_script()), "--format", formats,
                                                     "-q", *(["--out-dir", str(od)] if od else []), str(t)]})
    return out


def plan(graph_path: Path, allow_source: bool = False, allow_tool: bool = False) -> list:
    """The replay command, from file reads only (--dry-run); a run whose artifact must be rebuilt says so."""
    graph_path = graph_path.resolve()
    g = _graph(graph_path)
    rid = g["run"]
    run = run_root(graph_path)
    lang = {"traceforge": "rust", "jmc": "java"}.get(rid.get("tool"))
    if run is not None:
        prov = json.loads((run / "provenance.json").read_text())
        a = prov["artifacts"].get(lang)
        if a is None or not _artifact_ok(a, lang):
            return [{"cwd": str(run.parent / "replays" / "<id>"), "argv": ["<restore the run's inputs, rebuild, "
                                                                           "verify the digests, then replay>"]}]
        r = {"artifact": a, "main": _main(prov),
             "jmc": {"classpath": ["<JMC_CP>"]} if lang == "java" else None}
    else:
        r = resolve(graph_path, g)
    allow = (["--allow-source-change"] if allow_source else []) + (["--allow-tool-change"] if allow_tool else [])
    rep = "out/replays/<id>/replay.json"
    if lang == "rust":
        argv = [str(Path(r["artifact"]["dir"]) / "out" / rid["entry"]), "replay", "--graph", str(graph_path),
                "--report", rep, *allow, "--", *rid.get("args", [])]
    else:
        cp = ":".join([str(Path(r["artifact"]["dir"]) / "out"), *r["jmc"]["classpath"]])
        prog = rid["entry"] + (f":{rid['args'][0]}" if rid.get("args") else "")
        argv = ["java", "-ea", "-Xss4m", "-cp", cp, r["main"], "--replay", str(graph_path), "--report", rep, *allow, prog]
    return [{"cwd": "out/replays/<id>", "argv": argv}]
