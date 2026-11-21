"""Reports of a check run (DRIVER-DESIGN §5 summary.tsv, §6 VERDICT.md): verdict.json, the classifier hook
(`[classify] command`, else `classes.tsv`), and the word-level diff of each uncovered line against its
nearest reference line (joins/<checked>-in-<ref>/nearest.diff).
"""

from __future__ import annotations

import difflib
import json
import os
import shlex
from dataclasses import dataclass
from pathlib import Path

from . import errors, runner

SUMMARY_COLUMNS = ("scenario cell model mode direction gating checked ref checked_enum ref_enum checked_lines "
                   "ref_lines uncovered decide_viol decide_fa decide_unres graphs class_T class_D class_A class_C "
                   "verdict export_ms join_ms peak_rss_kb").split()
CLASSES = ("T", "D", "A", "C")
NEAREST_SCAN_LIMIT = 200000  # reference lines read per uncovered line, at most


@dataclass
class DirResult:
    d: object  # config.Direction
    v: object  # verdict.DirVerdict
    ref: object  # check.ExportRun
    chk: object
    ro: object  # the reference outcome after preflight
    co: object
    join: object  # Outcome or None
    jd: Path | None
    jrec: dict | None


# ---------------------------------------------------------------- words


def value_text(v) -> str:
    if isinstance(v, dict) and set(v) == {"t", "f"}:
        return f"{v['t']}{{{','.join(value_text(x) for x in v['f'])}}}" if v["f"] else v["t"]
    if isinstance(v, list):
        return "[" + ",".join(value_text(x) for x in v) + "]"
    return json.dumps(v, ensure_ascii=False)


def event_text(e) -> str:
    (k, v), = e.items()
    return f"{k} {'⊥' if v is None else value_text(v)}"


def word_text(word) -> str:
    return ", ".join(event_text(e) for e in word)


def _dist(a, b) -> int:
    """The edit distance (insert, delete, substitute one event) between two event lists (Astra 11)."""
    if len(a) < len(b):
        a, b = b, a
    prev = list(range(len(b) + 1))
    for i, x in enumerate(a, 1):
        cur = [i]
        for j, y in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (x != y)))
        prev = cur
    return prev[-1]


NEAREST_TIME_S = 60.0  # per direction, all uncovered lines together (Opus F5)


class Nearest:
    """The reference read once per direction, bucketed by (statuses, thread set), at most NEAREST_SCAN_LIMIT
    lines; `find` gives the line of the smallest summed event edit distance, `gave_up` says why a search stopped
    short (the scan limit, or the direction's time bound)."""

    def __init__(self, ref_path: Path):
        self.buckets, self.truncated, self.deadline = {}, False, None
        try:
            with open(ref_path, "rb") as f:
                f.readline()
                for k, raw in enumerate(f):
                    if k >= NEAREST_SCAN_LIMIT:
                        self.truncated = True
                        break
                    o = json.loads(raw)
                    key = (json.dumps(o["statuses"], sort_keys=True), tuple(sorted(o["words"])))
                    self.buckets.setdefault(key, []).append(
                        (o, {t: [json.dumps(e, sort_keys=True) for e in w] for t, w in o["words"].items()}))
        except (OSError, ValueError, KeyError):
            self.buckets = {}

    def find(self, line: dict):
        import time
        if self.deadline is None:
            self.deadline = time.monotonic() + NEAREST_TIME_S
        key = (json.dumps(line["statuses"], sort_keys=True), tuple(sorted(line["words"])))
        want = {t: [json.dumps(e, sort_keys=True) for e in w] for t, w in line["words"].items()}
        best, best_d, why = None, None, ("scan limit: the first %d reference lines" % NEAREST_SCAN_LIMIT
                                         if self.truncated else "")
        for o, ws in self.buckets.get(key, []):
            if time.monotonic() > self.deadline:
                why = f"gave up: the direction's {NEAREST_TIME_S:.0f} s bound"
                break
            dd = sum(_dist(want[t], ws[t]) for t in want)
            if best_d is None or dd < best_d:
                best, best_d = o, dd
                if dd == 0:
                    break
        return best, why


def nearest(line: dict, ref_path: Path) -> dict | None:
    return Nearest(ref_path).find(line)[0]


def word_diff(line: dict, near: dict | None) -> dict:
    """thread -> diff lines (`  ` common, `- ` reference only, `+ ` checked only); threads that agree omitted."""
    out = {}
    for t, w in line["words"].items():
        a = [event_text(e) for e in (near["words"].get(t, []) if near else [])]
        b = [event_text(e) for e in w]
        if near is not None and a == b:
            continue
        sm = difflib.SequenceMatcher(a=a, b=b, autojunk=False)
        lines = []
        for op, i1, i2, j1, j2 in sm.get_opcodes():
            if op == "equal":
                lines += [f"  {x}" for x in a[i1:i2]]
        # (`  ` common, `- ` reference only, `+ ` checked only)
            else:
                lines += [f"- {x}" for x in a[i1:i2]] + [f"+ {x}" for x in b[j1:j2]]
        out[t] = lines
    return out


# ---------------------------------------------------------------- classification


def classify(check, dr: DirResult, run: Path) -> list:
    """One (class, note) per uncovered line: the [classify] command (JSON lines in, JSON lines out), else
    the project's classes.tsv (`direction<TAB>line<TAB>class<TAB>note`, direction `*` for any), else ("-", "")."""
    rep = dr.join.data["report"]
    n = len(rep.entries)
    if n == 0:
        return []
    cmd = check.cfg.classify.get("command")
    if cmd:
        inp = dr.jd / "classify.in.jsonl"
        with open(inp, "w", encoding="utf-8") as f:
            for e in rep.entries:
                f.write(json.dumps({"direction": dr.d.name, "checked": dr.d.checked, "ref": dr.d.ref,
                                    "class": e.cls, "line": json.loads(e.line),
                                    "decide": e.decide}, ensure_ascii=False) + "\n")
        work = dr.jd / "classify-work"  # inside the run (Astra 9)
        work.mkdir(exist_ok=True)
        r = runner.run(classifier_argv(check.cfg), work, dr.jd / "classify.out.jsonl", dr.jd / "classify.err",
                       timeout_s=600, stdin_path=inp)
        check.commands.append({**r.as_record(), "step": "classify", "direction": dr.d.name})
        res = []
        if r.exit == 0:
            for l in r.read_stdout().splitlines():
                try:
                    o = json.loads(l)
                except ValueError:
                    res.append(("-", "classifier output is not JSON"))
                    continue
                if not isinstance(o, dict) or not isinstance(o.get("class", "-"), str) \
                        or not isinstance(o.get("note", ""), (str, int, float)):
                    res.append(("-", "classifier output is not {\"class\", \"note\"}"))  # Astra 10
                    continue
                c = o.get("class", "-")
                res.append((c if c in CLASSES else "-", str(o.get("note", ""))))
        if len(res) != n:
            check.log(f"classify: {dr.d.name}: {len(res)} answers for {n} lines (exit {r.exit}); unclassified")
            return [("-", f"classifier failed (exit {r.exit}, {len(res)} answers for {n} lines)")] * n
        return res
    table = {}
    tsv = check.cfg.root / "classes.tsv"
    if tsv.is_file():
        for l in tsv.read_text(encoding="utf-8").splitlines():
            parts = l.split("\t")
            if len(parts) >= 3 and not l.startswith("#"):
                table[(parts[0], parts[1])] = (parts[2] if parts[2] in CLASSES else "-",
                                                parts[3] if len(parts) > 3 else "")
    return [table.get((dr.d.name, e.line), table.get(("*", e.line), ("-", ""))) for e in rep.entries]


def classifier_files(cfg) -> list:
    """The project files the [classify] command names (its script), relative to the project root."""
    cmd = cfg.classify.get("command")
    out = []
    for t in shlex.split(cmd) if cmd else []:
        p = cfg.root / t
        if not os.path.isabs(t) and p.is_file():
            out.append(os.path.normpath(t))
    return out


def classifier_argv(cfg) -> list:
    """The command, its project-relative files made absolute (the classifier runs in the run's directory)."""
    return [str(cfg.root / t) if not os.path.isabs(t) and (cfg.root / t).is_file() else t
            for t in shlex.split(cfg.classify["command"])]


# ---------------------------------------------------------------- writing


def _enum(o):
    return o.data["export"].enumeration if o is not None and o.ok and "export" in o.data else "--"


def _lines(o):
    return str(o.data["export"].lines) if o is not None and o.ok and "export" in o.data else "--"


def _rec(er) -> dict:
    return er.record if er is not None else {}


def write(run: Path, check, cells, agg, meta, voc) -> None:
    cfg = check.cfg
    rows, md, problems, jcells = [], [], [], []
    title = agg.status
    md.append(f"# {cfg.name}: {title}   (exit {agg.exit})")
    md.append(f"Gating checks: {agg.gating.upper() if agg.gating else 'none (REPORT ONLY)'}; all reported directions: "
              f"{agg.all.upper() if agg.all else '--'}. Run {run.name}; model {meta['model']}; mode {meta['mode']}.")
    md.append("Inputs: [configuration](config.resolved.toml), [provenance](provenance.json), [commands](commands.sh).")
    if voc.exit != 0:
        md.append(f"Vocabulary: {voc.status} ({'; '.join(t for _, _, t in voc.differences) or '; '.join(voc.notes)}).")
    for c, dirs in cells:
        md += ["", f"## {c.name}",
               "| direction | gating | checked / ref enumeration | uncovered / lines | Decide viol / fa / unres | graphs | verdict |",
               "|---|---|---|---|---|---|---|"]
        detail, jdirs = [], []
        for dr in dirs:
            d, v = dr.d, dr.v
            rep = dr.join.data["report"] if dr.join is not None and dr.join.ok else None
            summ = dr.join.data["summary"] if rep else {}
            classes = classify(check, dr, run) if rep else []
            dec = rep.decide if rep else None
            label = v.label(d.form) + (f" ({v.note})" if v.note and v.status != "conforms" or "false alarm" in v.note else "")
            md.append(f"| {d.name} | {'yes' if d.gating else 'no'} | {_enum(dr.co)} / {_enum(dr.ro)} | "
                      f"{rep.uncovered if rep else '--'} / {_lines(dr.co)} | "
                      f"{'%d / %d / %d' % (dec['violations'], dec['false_alarms'], dec['unresolved']) if dec else '-- / -- / --'} | "
                      f"{summ.get('graphs', '--')} | {label} |")
            cc = {k: sum(1 for x, _ in classes if x == k) for k in CLASSES}
            jrec = dr.jrec or {}
            rows.append([c.scenario.id, c.cell.label, meta["model"], meta["mode"], d.name, "yes" if d.gating else "no",
                         d.checked, d.ref, _enum(dr.co), _enum(dr.ro), _lines(dr.co), _lines(dr.ro),
                         str(rep.uncovered) if rep else "--", *(str(dec[k]) if dec else "--" for k in
                                                                 ("violations", "false_alarms", "unresolved")),
                         summ.get("graphs", "--"), *(str(cc[k]) if rep else "--" for k in CLASSES),
                         v.label(d.form), str(_rec(dr.chk).get("wall_ms") or "--"),
                         str(jrec.get("wall_ms", "--")),
                         str(max([x for x in (_rec(dr.chk).get("peak_rss_kb"), jrec.get("peak_rss_kb")) if x] or [0])
                             or "--")])
            ent = []
            if rep and rep.entries:
                idx = {r["n"]: r for r in dr.join.data["index"]}
                diffs = []
                near_ix = Nearest(dr.ref.paths["export"])
                limit = check.cfg.check["witness_limit"]
                detail += ["", f"### {d.checked} <= {d.ref}: uncovered words"]
                for k, e in enumerate(rep.entries, 1):
                    line = json.loads(e.line)
                    if k <= limit:
                        near, why = near_ix.find(line)
                    else:
                        near, why = None, f"not searched: beyond witness_limit {limit}"
                    wd = word_diff(line, near) if (near is not None or k <= limit) else {}
                    thread = next(iter(wd), next(iter(line["words"]), "-"))
                    cl, note = classes[k - 1] if k - 1 < len(classes) else ("-", "")
                    row = idx.get(str(k), {})
                    graph = row.get("graph", "")
                    gpath = (dr.jd / "graphs" / graph) if graph and graph != "-" else None
                    detail.append(f"{k}. `{thread}: {word_text(line['words'].get(thread, []))}` [class {cl}"
                                  f"{', ' + note if note else ''}]")
                    shown = [x[2:] if x.startswith("  ") else x for x in
                             wd.get(thread, [why or "(no reference line with these statuses)"])]
                    detail.append(f"   nearest {d.ref} word: `{' | '.join(shown)}`" + (f" ({why})" if why and near else ""))
                    dline = (f"   Decide: {e.decide['outcome']} k={e.decide['k']} k_min={e.decide['k_min']}" if e.decide
                             else "   Decide: --")
                    if gpath and gpath.exists():
                        rel = os.path.relpath(gpath, run)
                        stem = rel[:-len(".graph.json")]
                        links = " ".join(f"[{ext}]({stem}{ext})" if (run / f"{stem}{ext}").exists() else f"{ext}: none"
                                         for ext in (".tex", ".dot", ".witness.txt"))
                        hint = os.path.relpath(gpath, check.cfg.root)  # usable from the project directory
                        dline += f"; graph [{graph}]({rel}) ({links}); replay: `daavar replay {hint}`"
                    else:
                        dline += f"; graph: none ({row.get('reason', '--')})"
                    detail.append(dline)
                    diffs.append(f"## {k} {e.cls} {e.line}\n" + ("nearest: " + json.dumps(near, separators=(',', ':'))
                                                                 if near else "nearest: none") +
                                 (f" ({why})" if why else "") + "\n" +
                                 "\n".join(f"{t}:\n" + "\n".join("  " + x for x in xs) for t, xs in wd.items()) + "\n")
                    ent.append({"n": k, "class": e.cls, "line": line, "decide": e.decide, "graph": graph or None,
                                "label": cl, "note": note})
                (dr.jd / "nearest.diff").write_text("\n".join(diffs), encoding="utf-8")
            if v.reason.startswith(("ref-", "checked-")) and v.status in ("refused", "violation", "incomplete") \
                    and v.message:
                who = d.ref if v.reason.startswith("ref-") else d.checked
                o = dr.ro if v.reason.startswith("ref-") else dr.co
                er = dr.ref if v.reason.startswith("ref-") else dr.chk
                problems.append(errors.render(f"{who} ({c.name}, {d.name})", o.kind, v.message, v.category or
                                              "contributing", os.path.relpath(er.paths["err"], run),
                                              _read(er.paths["out"]) + _read(er.paths["err"])))
            elif v.reason == "join-refused" or (dr.join is not None and not dr.join.ok):
                problems.append(errors.render(f"{d.name} ({c.name})", dr.join.kind, dr.join.message,
                                              dr.join.category or "contributing",
                                              os.path.relpath(dr.jd / "join.err", run) if dr.jd else None))
            elif v.status == "incomplete":
                problems.append(f"{d.name} ({c.name}): incomplete: {v.note}. See "
                                f"{errors.CATEGORIES[v.category or 'budgets'].anchor}.")
            jdirs.append({"direction": d.name, "form": d.form, "gating": d.gating, "status": v.status,
                          "label": v.label(d.form), "aggregates_as": v.gated(d.form), "note": v.note,
                          "reason": v.reason, "checked": {"program": d.checked, "kind": dr.chk.kind if dr.chk else "skipped",
                                                          "key": dr.chk.key if dr.chk else None,
                                                          "enumeration": _enum(dr.co)},
                          "ref": {"program": d.ref, "key": dr.ref.key, "enumeration": _enum(dr.ro)},
                          "uncovered": ent if rep else None,
                          "join": os.path.relpath(dr.jd, run) if dr.jd else None})
        md += detail
        jcells.append({"cell": c.name, "status": agg.cells.get(c.name), "directions": jdirs})
    md += ["", "## Refused, incomplete, program failures"]
    md += problems or ["(none)"]
    md += ["", "Scope: the declared scenario, sizes and model only (docs/concepts.md#relation).", ""]
    (run / "VERDICT.md").write_text("\n".join(md), encoding="utf-8")
    with open(run / "summary.tsv", "w", encoding="utf-8") as f:
        f.write("\t".join(SUMMARY_COLUMNS) + "\n")
        for r in rows:
            f.write("\t".join(x.replace("\t", " ") for x in r) + "\n")
    (run / "verdict.json").write_text(json.dumps({
        "name": cfg.name, "status": agg.status, "exit": agg.exit, "gating": agg.gating, "all": agg.all,
        "run": run.name, "model": meta["model"], "mode": meta["mode"], "vocab": voc.status, "cells": jcells},
        indent=1, ensure_ascii=False) + "\n", encoding="utf-8")


def _read(p) -> str:
    try:
        return Path(p).read_text(encoding="utf-8", errors="replace")
    except (FileNotFoundError, TypeError):
        return ""


def write_refused(run: Path, check, why: str) -> None:
    """A check refused before any verdict (a build or vocabulary refusal) still leaves VERDICT.md and
    verdict.json with the reason (final review, Opus 3)."""
    first = (why.splitlines() or [""])[0]
    md = [f"# {check.cfg.name}: REFUSED   (exit 2)", f"Run {run.name}: refused before any direction was judged.", "",
          "## Refused, incomplete, program failures", f"check: refused: {first}", "",
          "See driver.log for the full message.", ""]
    (run / "VERDICT.md").write_text("\n".join(md), encoding="utf-8")
    (run / "verdict.json").write_text(json.dumps({"name": check.cfg.name, "status": "REFUSED", "exit": 2,
                                                  "run": run.name, "error": why, "cells": []}, indent=1) + "\n")
