"""Provenance, inputs and reproduction (DRIVER-DESIGN §5): provenance.json, commands.sh,
config.resolved.toml, inputs/ (manifests, the project's sources by case, dirty tools), and
`check --from-run R [--into D]`.

Layout of a restore: every recorded location (the project, the TraceForge and JMC checkouts as the
configuration names them, and the daavar checkout when the project lies inside it) is placed in D at its path
relative to their common ancestor, so the project's relative path dependency and `[tools]` paths resolve
unchanged and the manifests' daavar-relative roots stay valid (DAAVAR is set to the restored checkout).
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import shlex
import shutil
import tarfile
import time
from pathlib import Path

from . import DaavarError, __version__, build, cache, manifest, runner

PROJECT_FILES_TOP = ("daavar.toml", "classes.tsv")
COMPARE_DROP_SUMMARY = ("export_ms", "join_ms", "peak_rss_kb")
ONLINE_TIMING = ("first_miss_wall_ms",)


# ---------------------------------------------------------------- helpers


def sha256(p: Path) -> str | None:
    try:
        return cache.sha256_file(p)
    except (FileNotFoundError, IsADirectoryError):
        return None


def _git(cwd, *args, check=True) -> str:
    code, out, err = runner.capture(["git", "-C", str(cwd), "-c", "core.quotepath=off", *args], timeout_s=600)
    if check and code != 0:
        raise DaavarError(f"git {' '.join(args)} in {cwd}: {err.decode(errors='replace').strip()}")
    return out.decode(errors="replace") if code == 0 else ""


def _git_bytes(cwd, *args) -> bytes:
    code, out, err = runner.capture(["git", "-C", str(cwd), "-c", "core.quotepath=off", *args], timeout_s=600)
    if code != 0:
        raise DaavarError(f"git {' '.join(args)} in {cwd}: {err.decode(errors='replace').strip()}")
    return out


def project_files(root: Path, cfg=None) -> list:
    """The project's sources, relative to its root: daavar.toml (and classes.tsv), rust/Cargo.toml,
    rust/Cargo.lock, rust/src/**/*.rs, java/**/*.java (never target/ or out/)."""
    out = [f for f in PROJECT_FILES_TOP if (root / f).is_file()]
    for f in ("rust/Cargo.toml", "rust/Cargo.lock"):
        if (root / f).is_file():
            out.append(f)
    out += ["rust/" + p for p in manifest._files(root / "rust", "src", "*.rs")] if (root / "rust").is_dir() else []
    out += manifest._files(root, "java", "*.java", exclude_out=True)
    if cfg is not None:  # the classifier's script (Astra 9)
        from .report import classifier_files
        out += [f for f in classifier_files(cfg) if f not in out]
    return out


def _tar(base: Path, rels: list, dest: Path) -> None:
    with tarfile.open(dest, "w") as t:
        for r in sorted(rels):
            t.add(base / r, arcname=r, recursive=False)


def literal(cfg, rel: str) -> Path:
    """A [tools] path as the configuration names it: joined to the project root, not resolved through links."""
    p = Path(rel)
    return Path(os.path.normpath(p if p.is_absolute() else cfg.root / p))


# ---------------------------------------------------------------- writing a run's provenance


def write(run: Path, check, cells, meta) -> None:
    cfg, b = check.cfg, check.build
    inputs = run / "inputs"
    (inputs / "manifests").mkdir(parents=True)
    for pid, p in cfg.programs.items():
        art = b.artifacts.get(p.lang)
        if art:
            (inputs / "manifests" / f"{pid}.SOURCE.md5").write_text(art["manifest"], encoding="utf-8")
    proj = project_inputs(cfg.root, inputs, cfg)
    tools = {}
    for name, t in b.tools.items():
        rec = {k: t.get(k) for k in ("identity", "commit", "dirty", "dirty_digest", "checkout")}
        lit = literal(cfg, cfg.tools["traceforge"] if name == "traceforge" else cfg.tools["jmc"]["checkout"])
        rec["literal"] = str(lit)
        rec["remotes"] = _remotes(Path(t["checkout"]))
        if t.get("dirty"):
            tool_inputs(name, Path(t["checkout"]), inputs / "tools" / name, t)
        if name == "traceforge":
            rec["conf_join_sha256"] = sha256(Path(t["conf_join"]))
            tj = Path(t["conf_join"]).parent / "tool.json"
            rec["conf_join_inputs"] = json.loads(tj.read_text()).get("build_inputs") if tj.is_file() else None
        if name == "jmc" and t.get("dir"):
            rec["classes_sha256"] = cache.tree_digest(Path(t["dir"]) / "tree")  # the executable classes (Astra 12)
            tj = json.loads((Path(t["dir"]) / "tool.json").read_text())
            rec["jars_sha256"] = cache.key_of(sorted(tj.get("jars_sha256", {}).values()))
        tools[name] = rec
    arts = {}
    for lang, a in b.artifacts.items():
        bj = json.loads((Path(a["dir"]) / "build.json").read_text())
        arts[lang] = {"key": a["key"], "dir": a["dir"], "sha256": a["sha256"], "key_inputs": bj["key_inputs"]}
    exports, joins = [], []
    for c, dirs in cells:
        for dr in dirs:
            for er in {id(x): x for x in (dr.ref, dr.chk) if x is not None}.values():
                exports.append({"cell": c.name, "program": er.program, "kind": er.kind, "ref": er.ref, "key": er.key,
                                "cached": er.cached, "class": er.outcome.kind,
                                "export": os.path.relpath(er.paths["export"], run),
                                "export_sha256": sha256(er.paths["export"]),
                                "store_sha256": sha256(er.paths["graphs"])})
            if dr.jd:
                joins.append({"cell": c.name, "direction": dr.d.name, "dir": os.path.relpath(dr.jd, run),
                              "join_out_sha256": sha256(dr.jd / "join.out"),
                              "index_sha256": sha256(dr.jd / "graphs" / "index.tsv")})
    daavar = manifest.daavar_checkout()
    prov = {
        "provenance": 1, "driver": __version__, "run": run.name, "created": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "host": meta["host"], "tmpdir": meta["tmpdir"], "model": meta["model"], "mode": meta["mode"],
        "options": check.opts.as_dict(), "project": proj, "daavar_checkout": str(daavar),
        "project_in_daavar": _inside(cfg.root, daavar), "tools": tools, "artifacts": arts,
        "exports": exports, "joins": joins, "commands": check.commands,
        "jmc_main": cfg.tools["jmc"]["main"] if cfg.tools.get("jmc") else None,
    }
    (run / "provenance.json").write_text(json.dumps(prov, indent=1, ensure_ascii=False, default=str) + "\n")
    with open(run / "commands.sh", "w", encoding="utf-8") as f:
        f.write("#!/bin/sh\n# The commands of this run, in order (DRIVER-DESIGN §5); cached ones are marked.\n")
        for c in check.commands:
            env = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in (c.get("env") or {}).items() if v is not None)
            mark = "# (cached) " if c.get("cached") else ""
            f.write(f"{mark}(cd {shlex.quote(c['cwd'])} && {env + ' ' if env else ''}"
                    f"{shlex.join(str(a) for a in c['argv'])})\n")
    (run / "config.resolved.toml").write_text(resolved_toml(cfg, check), encoding="utf-8")


def _inside(p: Path, base: Path) -> bool:
    try:
        Path(os.path.realpath(p)).relative_to(os.path.realpath(base))
        return True
    except ValueError:
        return False


def _remotes(checkout: Path) -> list:
    out = _git(checkout, "remote", "-v", check=False)
    return sorted({l.split()[1] for l in out.splitlines() if l.strip()})


def project_inputs(root: Path, inputs: Path, cfg=None) -> dict:
    files = project_files(root, cfg)
    top = _git(root, "rev-parse", "--show-toplevel", check=False).strip()
    rec = {"root": str(root), "files": {f: hashlib.md5((root / f).read_bytes()).hexdigest() for f in files}}
    if top:
        # A project file that is not tracked and not an ordinary untracked file (git-ignored, or outside what
        # git lists) cannot be restored from the commit: the snapshot is kept instead (Astra 7, Opus F3).
        rel0 = os.path.relpath(os.path.realpath(root), os.path.realpath(top))
        known = set(_git(top, "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", rel0).split("\0"))
        missing = [f for f in files if os.path.normpath(os.path.join(rel0, f)) not in known]
        if missing:
            rec["not_in_git"] = missing
            top = ""
    if not top:
        snap = inputs / "project"
        for f in files:
            (snap / f).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(root / f, snap / f)
        rec.update({"case": "snapshot"})
        return rec
    rel = os.path.relpath(os.path.realpath(root), os.path.realpath(top))
    commit = _git(top, "rev-parse", "HEAD").strip()
    paths = [os.path.normpath(os.path.join(rel, f)) for f in files]
    # the status of every preserved file (the classifier's script included, final review 4), plus the source
    # directories (a deleted file shows there)
    status = _git(top, "status", "--porcelain", "--untracked-files=all", "--", *paths, *(
        os.path.join(rel, x) for x in ("rust/src", "java") if (root / x).is_dir())).strip()
    rec.update({"repo": top, "path_in_repo": rel, "commit": commit, "remotes": _remotes(Path(top))})
    (inputs / "commit").write_text(commit + "\n")
    if not status:
        rec["case"] = "clean"
        return rec
    rec["case"] = "dirty"
    diff = _git_bytes(top, "diff", "--binary", "HEAD", "--", *paths, *(os.path.join(rel, x) for x in ("rust/src", "java")
                                                                       if (root / x).is_dir()))
    (inputs / "diff.patch").write_bytes(diff)
    rec["dirty_digest"] = hashlib.sha256(diff).hexdigest()
    untracked = [x for x in _git(top, "ls-files", "--others", "--exclude-standard", "-z", "--", rel).split("\0")
                 if x and os.path.relpath(x, rel) in files]
    _tar(Path(top), untracked, inputs / "untracked.tar")
    rec["untracked"] = untracked
    return rec


def tool_inputs(name: str, checkout: Path, dest: Path, t: dict) -> None:
    """A dirty tool build (§3.2): commit, diff.patch, untracked.tar and a source snapshot (JMC: plus its built
    classes, the cached tool directory)."""
    dest.mkdir(parents=True)
    (dest / "commit").write_text(t["commit"] + "\n")
    (dest / "diff.patch").write_bytes(_git_bytes(checkout, "diff", "--binary", "HEAD"))
    untracked = [x for x in _git(checkout, "ls-files", "--others", "--exclude-standard", "-z").split("\0") if x]
    _tar(checkout, untracked, dest / "untracked.tar")
    snap = dest / "snapshot"
    files = sorted(build.tracked_files(checkout))
    if name == "traceforge":  # the workspace's own files and the crates it builds, not the whole repository (Opus F7c)
        files = [f for f in files if "/" not in f and f.startswith(("Cargo.", "rust-toolchain"))
                 or f.startswith(("traceforge/", "traceforge-macros/"))]
    sums = {}
    for f in files:
        src = checkout / f
        if src.is_file() and not src.is_symlink():
            (snap / f).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, snap / f)
            sums[f] = cache.sha256_file(snap / f)
    (dest / "snapshot.json").write_text(json.dumps({"commit": t["commit"], "dirty_digest": t["dirty_digest"],
                                                     "files": sums}, indent=1) + "\n")


def _toml_value(v) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, str):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, list):
        return "[" + ", ".join(_toml_value(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{ " + ", ".join(f"{_key(k)} = {_toml_value(x)}" for k, x in v.items()) + " }"
    raise DaavarError(f"cannot write {v!r} as TOML")


def _key(k: str) -> str:
    return k if all(c.isalnum() or c in "_-" for c in k) else json.dumps(k)


def resolved_toml(cfg, check) -> str:
    """The configuration as resolved: defaults filled, the command's overrides applied, tool paths relative to
    the project root."""
    r = json.loads(json.dumps(cfg.raw))
    r["model"] = check.model
    r["check"] = {**cfg.check, "mode": check.mode, "stop_at_first": bool(check.stop)}
    r["budgets"] = dict(cfg.budgets)
    tools = r.get("tools", {})
    for k in ("traceforge",):
        if k in tools:
            tools[k] = os.path.relpath(literal(cfg, tools[k]), cfg.root)
    if isinstance(tools.get("jmc"), dict):
        tools["jmc"]["checkout"] = os.path.relpath(literal(cfg, tools["jmc"]["checkout"]), cfg.root)
        tools["jmc"]["cli"] = check.build.tools["jmc"]["cli"] if "jmc" in check.build.tools else tools["jmc"].get("cli", "auto")
    lines = ["# daavar.toml as resolved for this run (paths relative to the project root)"]

    def emit(prefix, d):
        # Tables become [sections] except a scenario's shared tables (fam, c2, ...), which stay inline.
        inline_ok = prefix.startswith("scenarios.") and prefix.count(".") == 1
        scal = [(k, v) for k, v in d.items() if not isinstance(v, dict) or (inline_ok and k != "matrix")]
        subs = [(k, v) for k, v in d.items() if isinstance(v, dict) and not (inline_ok and k != "matrix")]
        if prefix:
            lines.append(f"\n[{prefix}]")
        for k, v in scal:
            lines.append(f"{_key(k)} = {_toml_value(v)}")
        for k, v in subs:
            emit(f"{prefix}.{_key(k)}" if prefix else _key(k), v)

    emit("", r)
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------- --from-run


def _clone(sources: list, dest: Path, commit: str) -> str:
    errs = []
    for s in sources:
        if not s:
            continue
        if not (s.startswith(("http://", "https://", "git@", "ssh://")) or Path(s).exists()):
            errs.append(f"{s}: absent")
            continue
        code, out, err = runner.capture(["git", "clone", "-q", "--no-checkout", s, str(dest)], timeout_s=3600)
        if code == 0:
            c2, _, e2 = runner.capture(["git", "-C", str(dest), "checkout", "-q", "--detach", commit])
            if c2 == 0:
                return s
            err = e2
        if dest.exists():
            shutil.rmtree(dest)
        errs.append(f"{s}: {err.decode(errors='replace').strip()}")
    raise DaavarError(f"cannot restore commit {commit}: no source repository ({'; '.join(errs) or 'none recorded'})")


def _apply(repo: Path, patch: Path, untracked: Path | None) -> None:
    if patch.is_file() and patch.stat().st_size:
        code, out, err = runner.capture(["git", "-C", str(repo), "apply", "--binary", str(patch)])
        if code != 0:
            raise DaavarError(f"git apply {patch}: {err.decode(errors='replace').strip()}")
    if untracked and untracked.is_file():
        with tarfile.open(untracked) as t:
            t.extractall(repo, filter="data")


def restore(R: Path, D: Path, log) -> dict:
    """Steps 1-4 of §5 Restoring; returns the restored locations."""
    prov = json.loads((R / "provenance.json").read_text())
    proj = prov["project"]
    places = {"project": proj["root"]}
    for name, t in prov["tools"].items():
        places[name] = t["literal"]
    if prov["project_in_daavar"]:
        places["daavar"] = prov["daavar_checkout"]
    common = Path(os.path.commonpath(list(places.values())))

    def at(p: str) -> Path:
        return D / os.path.relpath(p, common)

    if D.exists() and any(D.iterdir()):
        raise DaavarError(f"--into {D}: not empty")
    D.mkdir(parents=True, exist_ok=True)
    work = D / ".restore"
    work.mkdir()
    # 1. the project
    pdst = at(proj["root"])
    pdst.mkdir(parents=True, exist_ok=True)
    if proj["case"] == "snapshot":
        shutil.copytree(R / "inputs" / "project", pdst, dirs_exist_ok=True)
    else:
        repo = work / "project-repo"
        src = _clone([proj.get("repo"), *proj.get("remotes", [])], repo, proj["commit"])
        log(f"from-run: project {proj['commit']} from {src}")
        if proj["case"] == "dirty":
            _apply(repo, R / "inputs" / "diff.patch", R / "inputs" / "untracked.tar")
        for f in proj["files"]:
            s = repo / proj["path_in_repo"] / f
            (pdst / f).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(s, pdst / f)
    # 2. the tools
    for name, t in prov["tools"].items():
        dst = at(t["literal"])
        if dst.exists() and any(dst.iterdir()):
            raise DaavarError(f"{dst}: the restored {name} would overlay another restored location")
        if dst.exists():
            dst.rmdir()
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            src = _clone([t.get("checkout"), *t.get("remotes", [])], dst, t["commit"])
            log(f"from-run: {name} {t['commit']} from {src}")
            if t.get("dirty"):
                td = R / "inputs" / "tools" / name
                _apply(dst, td / "diff.patch", td / "untracked.tar")
        except DaavarError:
            if not t.get("dirty"):
                raise
            td = R / "inputs" / "tools" / name
            log(f"from-run: {name}: no repository; restored from the verified snapshot")
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(td / "snapshot", dst)
            meta = json.loads((td / "snapshot.json").read_text())
            for f, h in meta["files"].items():
                if cache.sha256_file(dst / f) != h:
                    raise DaavarError(f"{td}/snapshot/{f}: sha256 differs from snapshot.json")
            if (td / "untracked.tar").is_file():
                with tarfile.open(td / "untracked.tar") as tf_:
                    tf_.extractall(dst, filter="data")
            (dst / build.IDENTITY_MARKER).write_text(json.dumps({"tool": name, "commit": meta["commit"],
                                                                 "dirty_digest": meta["dirty_digest"]}) + "\n")
    if prov["project_in_daavar"]:
        dv = at(prov["daavar_checkout"])
        dv.mkdir(parents=True, exist_ok=True)
    # 3. verify every restored file against R's manifests
    bad = []
    for f, md5 in proj["files"].items():
        got = hashlib.md5((pdst / f).read_bytes()).hexdigest() if (pdst / f).is_file() else None
        if got != md5:
            bad.append(f"{f}: md5 {got}, recorded {md5}")
    for m in sorted((R / "inputs" / "manifests").glob("*.SOURCE.md5")):
        text = m.read_text()
        root_line = text.splitlines()[0][len("# root "):]
        base = pdst / root_line[len("project:"):] if root_line.startswith("project:") else at(prov["daavar_checkout"]) / root_line
        for f, md5 in manifest.files_of(text).items():
            if f.startswith("classes/"):
                continue
            p = base / f
            got = hashlib.md5(p.read_bytes()).hexdigest() if p.is_file() else None
            if got != md5:
                bad.append(f"{m.name}: {f}: md5 {got}, recorded {md5}")
    if bad:
        raise DaavarError("the restored files differ from the run's manifests: " + "; ".join(sorted(set(bad))[:20]))
    rebased = rebase(pdst, {k: (v, at(v)) for k, v in places.items()}, log)
    return {"rebased": rebased, "project": pdst, "daavar": at(prov["daavar_checkout"]) if prov["project_in_daavar"] else None,
            "provenance": prov, "common": str(common)}


def _store_norm(p: Path, drop: set) -> list:
    """A graph store's lines with the digests of the rebased files removed from every `run.source` (a rebased
    absolute path dependency changes rust/Cargo.toml, which the store records)."""
    out = []
    for l in p.read_text(encoding="utf-8").splitlines():
        o = json.loads(l)
        src = (o.get("run") or {}).get("source")
        if isinstance(src, dict):
            for f in drop:
                src.get("files", {}).pop(f, None)
        out.append(json.dumps(o, sort_keys=True))
    return out


def compare(R: Path, N: Path, rebased_files=()) -> dict:
    """§5 Compared on reproduction: export lines (without online timing fields), graph stores, index.tsv,
    join.out without measurements, summary.tsv without time, memory and run columns."""
    out = []

    def add(rel, ok, why=""):
        out.append({"file": rel, "equal": ok, **({"why": why} if why else {})})

    for d in sorted(p for p in R.iterdir() if p.is_dir() and (p / "exports").is_dir()):
        for f in sorted((d / "exports").glob("*.jsonl")):
            rel = f.relative_to(R).as_posix()
            g = N / rel
            if f.name.endswith(".graphs.jsonl"):
                drop = {x.split("/", 1)[1] for x in rebased_files if x.startswith("rust/")}
                if drop:
                    add(rel, g.is_file() and _store_norm(f, drop) == _store_norm(g, drop),
                        f"compared without the digests of the rebased {', '.join(sorted(drop))}")
                else:
                    add(rel, g.is_file() and f.read_bytes() == g.read_bytes())
            else:
                add(rel, g.is_file() and _export_norm(f) == _export_norm(g))
        for j in sorted((d / "joins").glob("*/")) if (d / "joins").is_dir() else []:
            for name in ("graphs/index.tsv", "join.out"):
                f, g = j / name, N / j.relative_to(R) / name
                rel = f.relative_to(R).as_posix()
                if not f.exists():
                    continue
                if name == "join.out":
                    add(rel, g.is_file() and _no_meas(f) == _no_meas(g))
                else:
                    add(rel, g.is_file() and f.read_bytes() == g.read_bytes())
    add("summary.tsv", _summary_norm(R / "summary.tsv") == _summary_norm(N / "summary.tsv"))
    return {"compared": out, "equal": all(x["equal"] for x in out), "n": len(out)}


def _export_norm(p: Path) -> list:
    lines = p.read_text(encoding="utf-8").splitlines()
    h = json.loads(lines[0])
    for k in ONLINE_TIMING:
        h.pop(k, None)
    return [json.dumps(h, sort_keys=True)] + lines[1:]


def _no_meas(p: Path) -> str:
    t = p.read_text(encoding="utf-8", errors="replace")
    return t.split("--- measurements", 1)[0]


def _summary_norm(p: Path) -> list:
    if not p.is_file():
        return None
    rows = [l.split("\t") for l in p.read_text(encoding="utf-8").splitlines()]
    keep = [i for i, c in enumerate(rows[0]) if c not in COMPARE_DROP_SUMMARY]
    return [[r[i] for i in keep] for r in rows]


def artifact_digests(prov: dict, N: Path) -> list:
    """The rebuilt artifacts' digests against R's (a Rust binary rebuilt at another path embeds that path, D15)."""
    nprov = json.loads((N / "provenance.json").read_text())
    out = []
    for lang, a in prov["artifacts"].items():
        b = nprov["artifacts"].get(lang, {})
        sa, sb = a.get("sha256"), b.get("sha256")
        if isinstance(sa, dict):
            for k, v in sa.items():
                out.append({"name": f"{lang}:{k}", "equal": (sb or {}).get(k) == v})
        else:
            out.append({"name": lang, "equal": sa == sb})
    return out


def rebase(pdst: Path, places: dict, log) -> list:
    """§5 step 4: every absolute path the restored project names (daavar.toml's [tools], the Cargo path
    dependency) that lies under a recorded location is rewritten to the restored location. Relative paths
    resolve unchanged in the mirrored layout. Returns the rewrites."""
    import tomllib
    out = []
    olds = sorted(((str(old), str(new)) for old, new in places.values()), key=lambda x: -len(x[0]))

    def mapped(p: str) -> str | None:
        if not os.path.isabs(p):
            return None
        for old, new in olds:
            if p == old or p.startswith(old + os.sep):
                return new + p[len(old):]
        return None

    toml = pdst / "daavar.toml"
    t = tomllib.loads(toml.read_text())
    tools = t.get("tools", {})
    text = toml.read_text()
    for val in [tools.get("traceforge"), (tools.get("jmc") or {}).get("checkout")]:
        m = mapped(val) if isinstance(val, str) else None
        if m:
            text = text.replace(json.dumps(val), json.dumps(m))
            out.append({"file": "daavar.toml", "old": val, "new": m})
    toml.write_text(text)
    ct = pdst / "rust" / "Cargo.toml"
    if ct.is_file():
        c = tomllib.loads(ct.read_text())
        ctext = ct.read_text()
        for dep in (c.get("dependencies") or {}).values():
            m = mapped(dep["path"]) if isinstance(dep, dict) and isinstance(dep.get("path"), str) else None
            if m:
                ctext = ctext.replace(json.dumps(dep["path"]), json.dumps(m))
                out.append({"file": "rust/Cargo.toml", "old": dep["path"], "new": m})
        ct.write_text(ctext)
    for r in out:
        log(f"from-run: rebased {r['file']}: {r['old']} -> {r['new']}")
    return out


def _manifest_minus(text: str, files: set) -> list:
    return [l for l in text.splitlines()[1:] if l.split("  ", 1)[-1] not in files]


def verify_artifacts(prov: dict, nprov: dict, rebased: list, R: Path | None = None, N: Path | None = None) -> list:
    """D15 narrowed (Astra 4, Opus F12): every artifact must have its recorded digest, except a Rust binary
    (the project's or conf-join) rebuilt at another path from the same build key, which embeds that path; the
    identity then rests on the source manifest, the tool commit and the build key, and the mismatch is logged."""
    out = []
    for lang, a in prov["artifacts"].items():
        b = nprov["artifacts"].get(lang, {})
        same_key = a["key"] == b.get("key")
        for name, h in (a["sha256"].items() if isinstance(a["sha256"], dict) else [(lang, a["sha256"])]):
            hb = (b.get("sha256") or {}).get(name) if isinstance(a["sha256"], dict) else b.get("sha256")
            eq = h == hb
            path_dep = lang == "rust" and not name.endswith(".SOURCE.md5") and a.get("dir") != b.get("dir")
            cargo_rebased = any(r["file"] == "rust/Cargo.toml" for r in rebased)
            allowed = (not eq) and path_dep and (same_key or cargo_rebased)
            why = "Rust binary rebuilt at another path from the same build key (D15)" if allowed else ""
            if not eq and name.endswith(".SOURCE.md5") and cargo_rebased and R is not None and N is not None:
                # The manifest records rust/Cargo.toml, which the rebase rewrote: equal but for that line.
                prog = name[:-len(".SOURCE.md5")]
                ra = [m for m in (R / "inputs" / "manifests").glob("*.SOURCE.md5")]
                na = {m.name: m for m in (N / "inputs" / "manifests").glob("*.SOURCE.md5")}
                same = bool(ra) and all(m.name in na and _manifest_minus(m.read_text(), {"Cargo.toml"}) ==
                                        _manifest_minus(na[m.name].read_text(), {"Cargo.toml"}) for m in ra
                                        if m.read_text().startswith("# root") and "Cargo.toml" in m.read_text())
                if same:
                    allowed, why = True, "the manifest differs only in the rebased rust/Cargo.toml (D18)"
            out.append({"name": f"{lang}:{name}", "equal": eq, "allowed": allowed, "why": why})
    for name, t in prov["tools"].items():
        nt = nprov["tools"].get(name, {})
        out.append({"name": f"tool:{name}:identity", "equal": t["identity"] == nt.get("identity"), "allowed": False})
        if name == "traceforge":
            eq = t.get("conf_join_sha256") == nt.get("conf_join_sha256")
            ia, ib = t.get("conf_join_inputs") or {}, nt.get("conf_join_inputs") or {}
            same_inputs = bool(ia) and all(ia.get(k) == ib.get(k) for k in ("rustc", "cargo", "env"))
            moved = ia.get("target") != ib.get("target") or t.get("checkout") != nt.get("checkout")
            allowed = (not eq) and same_inputs and moved and t["identity"] == nt.get("identity")
            out.append({"name": "tool:conf-join", "equal": eq, "allowed": allowed,
                        "why": "conf-join rebuilt at another path from the same inputs (D15)" if allowed else ""})
        if name == "jmc":
            out.append({"name": "tool:jmc:classes", "equal": t.get("classes_sha256") == nt.get("classes_sha256"),
                        "allowed": False})
            out.append({"name": "tool:jmc:jars", "equal": t.get("jars_sha256") == nt.get("jars_sha256"),
                        "allowed": False})
    return out


def restore_plan(R: Path, D: Path) -> list:
    """What `check --from-run R --into D` would do, read from R's provenance only (--dry-run)."""
    prov = json.loads((R / "provenance.json").read_text())
    proj = prov["project"]
    out = []
    if proj["case"] == "snapshot":
        out.append({"cwd": str(D), "argv": ["<copy>", str(R / "inputs" / "project"), "<D>/<project>"]})
    else:
        src = proj.get("repo") or (proj.get("remotes") or ["<remote>"])[0]
        out.append({"cwd": str(D), "argv": ["git", "clone", "-q", "--no-checkout", src, "<D>/.restore/project-repo"]})
        out.append({"cwd": "<D>/.restore/project-repo", "argv": ["git", "checkout", "-q", "--detach", proj["commit"]]})
        if proj["case"] == "dirty":
            out.append({"cwd": "<D>/.restore/project-repo", "argv": ["git", "apply", "--binary",
                                                                    str(R / "inputs" / "diff.patch")]})
    for name, t in prov["tools"].items():
        src = t.get("checkout") or (t.get("remotes") or ["<remote>"])[0]
        out.append({"cwd": str(D), "argv": ["git", "clone", "-q", "--no-checkout", src, f"<D>/<{name}>"]})
        out.append({"cwd": f"<D>/<{name}>", "argv": ["git", "checkout", "-q", "--detach", t["commit"]]})
        if t.get("dirty"):
            out.append({"cwd": f"<D>/<{name}>", "argv": ["git", "apply", "--binary",
                                                         str(R / "inputs" / "tools" / name / "diff.patch")]})
    out.append({"cwd": "<D>/<project>", "env": {"DAAVAR_CACHE": "<D>/.daavar-cache"},
                "argv": ["daavar", "check", "<the run's selection and options>"]})
    return out
