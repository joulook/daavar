"""`daavar check` (DRIVER-DESIGN §3.3): build, vocab, export keys and the export cache, spec exports first,
preflight, checked exports (offline or online), joins, verdicts (§4), reports (§5-§6), provenance (§5).
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import time
from dataclasses import dataclass, field
from pathlib import Path

from . import (DaavarError, build, cache, config, jmc, manifest, outcome, parse, report, rundir, runner, tf,
               verdict, vocab)
from .outcome import Outcome

JMC_TIMEOUT_MARGIN_S = 60  # the driver's timeout exceeds the launcher's own, so the launcher's wins (D16)


@dataclass
class Options:
    scenarios: list = field(default_factory=list)
    cells: list = field(default_factory=list)
    where: list = field(default_factory=list)
    model: str | None = None
    mode: str | None = None
    stop_at_first: bool | None = None
    directions: list = field(default_factory=list)
    no_build: bool = False
    strict: bool = False
    no_wait: bool = False
    allow_dirty_tools: bool = False
    out: Path | None = None  # default <project>/out

    def as_dict(self) -> dict:
        d = dict(self.__dict__)
        d["out"] = str(self.out) if self.out else None
        return d


@dataclass
class ExportRun:
    key: str
    program: str
    kind: str  # offline | online
    ref: str | None
    outcome: Outcome
    paths: dict  # export, graphs, out, err, cmd (in the run)
    record: dict  # the command's provenance record
    cached: bool


class Check:
    def __init__(self, cfg: config.Config, opts: Options, log=None):
        self.cfg, self.opts = cfg, opts
        self.log = log or (lambda m: None)
        self.model = opts.model or cfg.model
        if self.model not in config.MODELS:
            raise DaavarError(f"--model {self.model}: one of {', '.join(config.MODELS)}")
        if self.model == "causal" and any(p.lang == "java" for p in cfg.programs.values()):
            raise DaavarError("--model causal is Rust-only; the project has Java programs")
        self.mode = opts.mode or cfg.check["mode"]
        self.stop = cfg.check["stop_at_first"] if opts.stop_at_first is None else opts.stop_at_first
        self.threads = cfg.check["threads"]
        if self.stop and self.mode != "online":
            raise DaavarError("--stop-at-first needs --mode online")
        if cfg.budgets["mem_cap_gb"]:
            raise DaavarError("[budgets] mem_cap_gb > 0 is not enforced in this release; set it to 0")
        if self.stop and self.threads > 1:
            raise DaavarError("--stop-at-first is refused with threads > 1 (TraceForge refuses it, program.rs:851)")
        self.commands = []
        self.exports = {}  # key -> ExportRun (one process per key and run)
        self.build = None

    # ------------------------------------------------------------ selection

    def select(self) -> list:
        sel = config.select(self.cfg, self.opts.scenarios or None, self.opts.cells or None, self.opts.where)
        if self.opts.directions:
            valid = []
            for c in sel:
                for d in c.directions:
                    if d.name not in valid:
                        valid.append(d.name)
            for d in self.opts.directions:
                if d not in valid:
                    raise DaavarError(f"unknown direction {d}; valid: {', '.join(valid)}")
            for c in sel:
                c.directions = [d for d in c.directions if d.name in self.opts.directions]
        if self.mode == "online":
            for c in sel:
                for d in c.directions:
                    p = self.cfg.programs[d.checked]
                    if d.form != "impl<=impl" and p.lang == "rust" and p.side == "spec":
                        raise DaavarError(f"--mode online: {d.checked} runs side \"spec\", and TraceForge's online "
                                          f"check runs the impl side only; check {d.name} offline")
        return sel

    # ------------------------------------------------------------ export keys and commands

    def _entry(self, prog) -> dict:
        if prog.lang == "rust":
            return {"bin": self.rust_bin, "side": prog.side}
        return {"class": prog.cls}

    def key_of(self, inv, kind: str, ref_key: str | None) -> tuple:
        """(key, argv template): §3.3 step 2, over everything that decides the export."""
        prog = inv.program
        b = self.build
        if prog.lang == "rust":
            tool = b.tools["traceforge"]["identity"]
            art = b.artifacts["rust"]
        else:
            tool = b.tools["jmc"]["identity"]
            art = b.artifacts["java"]
        tmpl = self.argv(inv, kind, "<E>", "<G>", "<S>" if kind == "online" else None, template=True)
        flags = {"threads": self.threads, "seed": self.cfg.check["seed"], "timeout_s": self.cfg.budgets["export_timeout_s"],
                 "graphs": True, "graphs_limit": 0 if self.threads > 1 else None, "mode": kind,
                 "stop_at_first": bool(self.stop and kind == "online")}
        if prog.lang == "java":
            flags["java_xmx"] = self.cfg.budgets["java_xmx"]
            flags["cli"] = b.tools["jmc"]["cli"]
        obj = {"tool": tool, "build_key": art["key"], "entry": self._entry(prog),
               "manifest": manifest.digest(art["manifest"]), "argv": tmpl,
               "header": {"program": self.cfg.name, "size": inv.joined, "model": self.model, "side": prog.side,
                          "class": prog.cls},
               "budgets": dict(self.cfg.budgets),
               "flags": flags, "reference": ref_key}
        return cache.key_of(obj), tmpl, obj

    def argv(self, inv, kind, out, graphs, spec, template=False) -> list:
        prog = inv.program
        if prog.lang == "rust":
            binary = "<ARTIFACT>" if template else self.build.artifacts["rust"]["bins"][self.rust_bin]
            if kind == "online":
                return tf.online_argv(binary, spec, out, graphs, self.model, self.cfg.name, inv.argv_text, self.stop)
            return tf.export_argv(binary, prog.side, out, graphs, self.model, self.cfg.name, inv.argv_text,
                                  self.threads)
        j = self.build.tools["jmc"]
        java = "<JAVA>" if template else build.java_tools()[0]
        cp = ["<CLASSES>", "<CP>"] if template else [self.build.artifacts["java"]["classes"], *j["classpath"]]
        return jmc.argv(j["cli"], java, self.cfg.budgets["java_xmx"], cp, j["main"], inv.argv_text,
                        self.cfg.check["seed"], out, graphs, self.cfg.budgets["export_timeout_s"],
                        spec=spec, stop_at_first=bool(self.stop and kind == "online"))

    # ------------------------------------------------------------ exports

    def export(self, cell_dir: Path, inv, kind: str = "offline", ref: ExportRun | None = None) -> ExportRun:
        prog = inv.program
        key, tmpl, keyobj = self.key_of(inv, kind, ref.key if ref else None)
        stem = prog.id if kind == "offline" else f"{prog.id}.online-{ref.program}"
        exp_dir = cell_dir / "exports"
        exp_dir.mkdir(parents=True, exist_ok=True)
        paths = {"export": exp_dir / f"{stem}.jsonl", "graphs": exp_dir / f"{stem}.jsonl.graphs.jsonl",
                 "out": exp_dir / f"{stem}.out", "err": exp_dir / f"{stem}.err", "cmd": exp_dir / f"{stem}.cmd"}
        if key in self.exports:
            prev = self.exports[key]
            _link_all(prev.paths, paths)
            return ExportRun(key, prog.id, kind, ref.program if ref else None, prev.outcome, paths,
                             {**prev.record, "reused_in_run": True}, True)
        cdir = cache.root() / "exports" / key
        if (cdir / "result.json").is_file():
            res = json.loads((cdir / "result.json").read_text())
            _link_all(_cache_paths(cdir), paths)
            o = self._interpret(prog, kind, res["exit"], False, paths)
            orig = res["record"]
            rec = {"argv": orig["argv"], "cwd": orig["cwd"], "env": orig.get("env", {}), "exit": orig["exit"],
                   "step": orig.get("step"), "cell": cell_dir.name, "program": prog.id, "class": o.kind,
                   "key": key, "cached": True, "wall_ms": None, "peak_rss_kb": None,
                   "cached_from": {"wall_ms": orig.get("wall_ms"), "peak_rss_kb": orig.get("peak_rss_kb"),
                                   "cell": orig.get("cell")}}
            er = ExportRun(key, prog.id, kind, ref.program if ref else None, o, paths, rec, True)
            self.exports[key] = er
            self.commands.append(er.record)
            return er
        work = cell_dir / f"work-{stem}"
        work.mkdir(parents=True, exist_ok=False)
        tmp = cache.tmpdir(cache.root() / "exports")
        try:
            tp = _cache_paths(tmp)
            spec = ref.paths["export"] if ref else None
            argv = self.argv(inv, kind, tp["export"], tp["graphs"], spec)
            timeout = self.cfg.budgets["export_timeout_s"] + (JMC_TIMEOUT_MARGIN_S if prog.lang == "java" else 0)
            self.log(f"export {cell_dir.name} {stem}: {' '.join(map(str, argv))}")
            r = runner.run(argv, work, tp["out"], tp["err"], timeout_s=timeout or None)
            (tp["cmd"]).write_text(" ".join(_q(a) for a in argv) + "\n")
            o = self._interpret(prog, kind, r.exit, r.timed_out, tp)
            rec = {**r.as_record(), "step": f"{kind}-export", "cell": cell_dir.name, "program": prog.id,
                   "ref": ref.program if ref else None, "class": o.kind, "key": key, "cached": False,
                   "key_inputs": keyobj}
            deterministic = cacheable(o)
            if deterministic:
                (tmp / "result.json").write_text(json.dumps({"exit": r.exit, "record": rec}, indent=1) + "\n")
                cache.make_readonly(tmp)
                try:
                    cache.install(tmp, cdir)
                except OSError:  # another check installed the same key meanwhile
                    cache.remove(tmp)
                src = _cache_paths(cdir)
                _link_all(src, paths)
            else:
                _link_all(tp, paths, copy=True)
                cache.remove(tmp)
            o = self._interpret(prog, kind, r.exit, r.timed_out, paths) if deterministic else o
        except BaseException:
            if tmp.exists():
                cache.remove(tmp)
            raise
        er = ExportRun(key, prog.id, kind, ref.program if ref else None, o, paths, rec, False)
        self.exports[key] = er
        self.commands.append(rec)
        return er

    def _interpret(self, prog, kind, exit_code, timed_out, paths) -> Outcome:
        out = _read(paths["out"])
        err = _read(paths["err"])
        if prog.lang == "rust":
            if kind == "online":
                return tf.online_outcome(exit_code, timed_out, out, err, paths["export"])
            return tf.export_outcome(exit_code, timed_out, out, err, paths["export"])
        return jmc.export_outcome(exit_code, timed_out, out, err, paths["export"], online=(kind == "online"),
                                  graphs=paths["graphs"], cli=self.build.tools["jmc"]["cli"])

    def preflight(self, er: ExportRun, scenario) -> Outcome:
        """§3.3 step 4: the header's model and visible list, before any join."""
        o = er.outcome
        if not o.ok:
            return o
        h = o.data["export"].header
        want = parse.MODEL_NAMES[self.model]
        prog = self.cfg.programs[er.program]
        if h["model"] != want:
            hint = " (the Java program's MpProgram.model() decides)" if prog.lang == "java" else ""
            return Outcome(outcome.REFUSED, f"{er.program}: the export's model is {h['model']}, not {want} "
                           f"(--model {self.model}){hint}", "model", "preflight-model", o.exit, o.data)
        vis = sorted(scenario.visible, key=lambda x: x.encode())
        if h["visible"] != vis:
            return Outcome(outcome.REFUSED, f"{er.program}: the export's visible roles are {h['visible']}, the "
                           f"scenario's {vis}", "visible", "preflight-visible", o.exit, o.data)
        return o

    # ------------------------------------------------------------ joins

    def join(self, cell_dir: Path, d, ref: ExportRun, chk: ExportRun) -> tuple:
        jd = cell_dir / "joins" / f"{d.checked}-in-{d.ref}"
        jd.parent.mkdir(parents=True, exist_ok=True)
        script = manifest.driver_checkout() / "scripts" / "witness-join.sh"
        argv = tf.join_argv(script, ref.paths["export"], chk.paths["export"], jd, self.cfg.check["decide_budget"],
                            self.cfg.check["witness_limit"], self.cfg.check["witness_nearest"], self.cfg.check["merge"])
        env = {"WITNESS_CONF_JOIN": self.build.tools["traceforge"]["conf_join"]}
        self.log(f"join {cell_dir.name} {d.name}: {' '.join(map(str, argv))}")
        r = runner.run(argv, cell_dir, cell_dir / "joins" / f"{d.checked}-in-{d.ref}.stdout",
                       cell_dir / "joins" / f"{d.checked}-in-{d.ref}.stderr", env_delta=env,
                       timeout_s=self.cfg.budgets["join_timeout_s"] or None)
        o = tf.join_outcome(r.exit, r.timed_out, r.read_stdout(), r.read_stderr(), jd)
        if o.ok and any((jd / "graphs").glob("*.graph.json")):
            # §6 links .witness.txt too: witness-join.sh renders tikz and dot only.
            import sys
            code, _, err = runner.capture([sys.executable, "-I", str(manifest.driver_checkout() / "scripts" /
                                                                     "witness-render.py"), "--format", "text", "-q",
                                           str(jd / "graphs")], timeout_s=600)
            if code != 0:
                self.log(f"render text {d.name}: exit {code}: {err.decode(errors='replace')[:300]}")
        rec = {**r.as_record(), "step": "join", "cell": cell_dir.name, "direction": d.name, "class": o.kind}
        self.commands.append(rec)
        return o, jd, rec

    # ------------------------------------------------------------ the dry run

    def plan(self) -> tuple:
        """(exit, commands) of a check without starting any process (§8 --dry-run, final reviews: Astra 6,
        Opus 4): the build commands, then per cell the exports, online checks and joins, with placeholders for what
        only a build can tell (artifact paths, the classpath, run paths)."""
        cfg = self.cfg
        code, cmds = build.dry_run(cfg)
        cmds = [{"step": "build", **c} for c in cmds]
        jc = cfg.tools.get("jmc") or {}
        cli = jc.get("cli", "positional")
        if cli == "auto":
            commit = build.git_head_nofork(cfg.tool_path(jc["checkout"])) or ""
            p = build.cli_cache_path(commit)
            cli = json.loads(p.read_text())["cli"] if p.is_file() else "positional"

        class B:
            tools = {"traceforge": {"identity": "<tf-identity>", "conf_join": "<CONF_JOIN>"},
                     "jmc": {"identity": "<jmc-identity>", "cli": cli, "classpath": ["<JMC_CP>"],
                             "main": jc.get("main", "")}}
            artifacts = {}
        if "rust" in cfg.langs:
            bins = manifest.rust_bins(cfg.root / "rust")
            if len(bins) != 1:
                raise DaavarError(f"the crate has {len(bins)} binaries ({', '.join(bins)}); one is expected")
            self.rust_bin = bins[0]
            B.artifacts["rust"] = {"key": "<rust-key>", "manifest": "", "bins": {bins[0]: f"<RUST_ARTIFACT>/{bins[0]}"}}
        if "java" in cfg.langs:
            B.artifacts["java"] = {"key": "<java-key>", "manifest": "", "classes": "<JAVA_CLASSES>"}
        self.build = B
        script = manifest.driver_checkout() / "scripts" / "witness-join.sh"
        for c in self.select():
            cd = f"out/<run>/{c.name}"
            ex = lambda p, kind="offline", ref=None: f"{cd}/exports/{p}{'' if kind == 'offline' else '.online-' + ref}.jsonl"
            done = set()
            refs = list(dict.fromkeys(d.ref for d in c.directions))
            for r in refs:
                cmds.append({"step": f"export {c.name} {r}", "cwd": f"{cd}/work-{r}",
                             "argv": self.argv(c.invocations[r], "offline", ex(r), ex(r) + ".graphs.jsonl", None)})
                done.add(("offline", r))
            for d in c.directions:
                online = self.mode == "online" and d.form != "impl<=impl" and cfg.programs[d.ref].kind == "spec"
                key = ("online", d.checked, d.ref) if online else ("offline", d.checked)
                if key not in done:
                    done.add(key)
                    kind = "online" if online else "offline"
                    out = ex(d.checked, kind, d.ref)
                    cmds.append({"step": f"{kind} {c.name} {d.checked}", "cwd": f"{cd}/work-{d.checked}",
                                 "argv": self.argv(c.invocations[d.checked], kind, out, out + ".graphs.jsonl",
                                                   ex(d.ref) if online else None)})
                chk = ex(d.checked, "online", d.ref) if online else ex(d.checked)
                cmds.append({"step": f"join {c.name} {d.name}", "cwd": cd,
                             "env": {"WITNESS_CONF_JOIN": "<CONF_JOIN>"},
                             "argv": tf.join_argv(script, ex(d.ref), chk, f"{cd}/joins/{d.checked}-in-{d.ref}",
                                                  cfg.check["decide_budget"], cfg.check["witness_limit"],
                                                  cfg.check["witness_nearest"], cfg.check["merge"])})
        for c in cmds:
            c["argv"] = [str(a) for a in c["argv"]]
        return code, cmds

    # ------------------------------------------------------------ the command

    def run(self, project_out: Path | None = None) -> dict:
        cfg = self.cfg
        out = self.opts.out or project_out or (cfg.root / "out")
        sel = self.select()
        with rundir.lock(out, wait=not self.opts.no_wait):
            cache.collect_stale_tmp()
            scen = sel[0].scenario.id if len({c.scenario.id for c in sel}) == 1 else "multi"
            run = rundir.allocate(out, scen)
            logf = open(run / "driver.log", "a", encoding="utf-8")
            outer_log = self.log

            def log(m):
                # The whole line goes to driver.log; stderr gets the step only (the command is in commands.sh).
                logf.write(time.strftime("%H:%M:%S ") + m + "\n")
                logf.flush()
                outer_log(m.split(": ", 1)[0] if m.startswith(("export ", "join ")) else m)
            self.log = log
            try:
                result = self._run(run, sel)
            except DaavarError as e:
                log(f"refused: {e}")
                report.write_refused(run, self, str(e))
                rundir.set_latest(out, run)
                e.run = str(run)
                raise
            finally:
                logf.close()
            rundir.set_latest(out, run)
        return result

    def _run(self, run: Path, sel: list) -> dict:
        cfg = self.cfg
        t0 = time.time()
        # 1. build (or the cached artifacts only), then vocab
        self.build = build.build(cfg, allow_dirty_tools=self.opts.allow_dirty_tools, cached_only=self.opts.no_build)
        for st in self.build.steps:
            self.commands.append({"step": f"build:{st.name}", "argv": [str(a) for a in st.argv], "cwd": st.cwd,
                                  "env": st.env, "cached": not st.built,
                                  **({k: st.result.get(k) for k in ("exit", "wall_ms", "peak_rss_kb")}
                                     if isinstance(st.result, dict) else {}), "class": "ok"})
        if "rust" in self.build.artifacts:
            bins = list(self.build.artifacts["rust"]["bins"])
            if len(bins) != 1:
                raise DaavarError(f"the crate has {len(bins)} binaries ({', '.join(bins)}); one is expected")
            self.rust_bin = bins[0]
        voc = vocab.run(cfg)
        self.log(f"vocab: {voc.status} {'; '.join(t for _, _, t in voc.differences)}")
        if voc.exit != 0 and self.opts.strict:
            raise DaavarError(f"vocab {voc.status} (--strict): " + "; ".join(t for _, _, t in voc.differences or
                                                                              [("", "", n) for n in voc.notes]))
        cells = []
        for c in sel:
            cd = run / c.name
            cd.mkdir()
            refs = []
            for d in c.directions:
                if d.ref not in refs:
                    refs.append(d.ref)
            ex = {}
            # 3. reference exports first
            for r in refs:
                ex[("offline", r)] = self.export(cd, c.invocations[r])
            # 5. checked exports
            online_pairs = set()
            for d in c.directions:
                ref_prog = cfg.programs[d.ref]
                if self.mode == "online" and d.form != "impl<=impl" and ref_prog.kind == "spec":
                    online_pairs.add((d.checked, d.ref))
            pre = {r: self.preflight(ex[("offline", r)], c.scenario) for r in refs}
            for d in c.directions:
                if (d.checked, d.ref) in online_pairs:
                    po = pre[d.ref]
                    if not (po.ok and po.data["export"].complete):
                        self.log(f"online {c.name} {d.name}: skipped (the reference is {po.kind}"
                                 f"{'' if not po.ok else ', ' + po.data['export'].enumeration})")
                        ex.setdefault(("online", d.checked, d.ref), None)
                    elif ("online", d.checked, d.ref) not in ex:
                        ex[("online", d.checked, d.ref)] = self.export(cd, c.invocations[d.checked], "online",
                                                                       ex[("offline", d.ref)])
                elif ("offline", d.checked) not in ex:
                    ex[("offline", d.checked)] = self.export(cd, c.invocations[d.checked])
            # 4. preflight (headers), 6. joins, verdicts
            dirs = []
            for d in c.directions:
                ref = ex[("offline", d.ref)]
                chk = ex[("online", d.checked, d.ref)] if (d.checked, d.ref) in online_pairs else ex[("offline", d.checked)]
                ro = pre[d.ref]
                co = self.preflight(chk, c.scenario) if chk is not None else None
                join = jd = jrec = None
                if ro.ok and co is not None and co.ok and ro.data["export"].complete:
                    join, jd, jrec = self.join(cd, d, ref, chk)
                v = verdict.direction(ro, co, join)
                dirs.append(report.DirResult(d, v, ref, chk, ro, co, join, jd, jrec))
            cells.append((c, dirs))
        agg = verdict.aggregate([(c.name, [(r.d, r.v) for r in dirs]) for c, dirs in cells])
        meta = {"run": run.name, "model": self.model, "mode": self.mode, "started": t0, "wall_s": time.time() - t0,
                "host": socket.gethostname(), "tmpdir": os.environ.get("TMPDIR", "")}
        report.write(run, self, cells, agg, meta, voc)
        from . import provenance
        provenance.write(run, self, cells, meta)
        return {"run": str(run), "status": agg.status, "exit": agg.exit, "gating": agg.gating, "all": agg.all}


def cacheable(o: Outcome) -> bool:
    """Only an ok export whose stop does not depend on the wall clock is cached (Opus F2): a JMC export that hit
    its own timeout (exit 0, `aborted`, stop_reason timeout) stays in the run only."""
    return o.ok and (o.data.get("measurements") or {}).get("stop_reason") != "timeout"


def _cache_paths(d: Path) -> dict:
    return {"export": d / "export.jsonl", "graphs": d / "export.jsonl.graphs.jsonl", "out": d / "stdout",
            "err": d / "stderr", "cmd": d / "cmd"}


def _link_all(src: dict, dst: dict, copy: bool = False) -> None:
    """Cached exports are read-only and hard-linked into the run (a copy across filesystems)."""
    for k, s in src.items():
        if not Path(s).exists():
            continue
        t = Path(dst[k])
        if t.exists():
            continue
        if copy:
            shutil.copyfile(s, t)
            continue
        try:
            os.link(s, t)
        except OSError:
            shutil.copyfile(s, t)
            os.chmod(t, 0o444)


def _read(p) -> str:
    try:
        return Path(p).read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        return ""


def _q(a) -> str:
    import shlex
    return shlex.quote(str(a))
