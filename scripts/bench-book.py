#!/usr/bin/env python3
"""The benchmark book (owner's design, 2026-11-03): one directory per benchmark under benchmark-book/<group>/<paper
name>/ with the sources (links), a README and EVAL.tex/EVAL.pdf that lists every run of the campaign on that
benchmark. Colours (owner): blue = the row is at the current engine pins, a rerun would give the same result; red = the
row is superseded (an older TraceForge or JMC pin, or the in-memory join that the merge join replaces) and its rerun
is pending; a planned row without a measurement is listed in the "Not run" paragraph, never shown as a number.
Numbers come from the campaign CSVs of tables.py (the same rows the paper's tables read); statuses of the rows the CSVs
leave out (skipped, tool errors) come from brain-data. usage: bench-book.py <brain-data> <daavar> <tables-dir> <group>
[--build]   groups: synthetic | application | realsw | agentic"""
import csv, glob, os, re, sys, collections, subprocess, datetime
BD, DV, T, GROUP = sys.argv[1:5]; BUILD = "--build" in sys.argv
TF_PIN = "9a0629e"; JMC_PIN = "3c43b2d2"; JOIN_PIN = "367cdc6"
GROUPS = {"synthetic": [("naive", "s1-naive"), ("share", "s2-share"), ("commit", "s3-commit"), ("chain", "s4-chain"), ("reset", "s5-reset"), ("width", "s6-width")]}
if GROUP not in GROUPS: sys.exit(f"group {GROUP}: not defined yet (groups: {', '.join(GROUPS)})")
def esc(s): return str(s).replace("_", "\\_").replace("%", "\\%").replace("&", "\\&")
def num(x):
    try: return float(x)
    except (TypeError, ValueError): return None
def sz(s): return [int(x) for x in re.findall(r"\d+", s)]
SUF = re.compile(r"(\.c1h|\.c48h|\.j2|\.m)+$")
def base(i): return SUF.sub("", i)
# ---- records (every pin) for the statuses; latest per id ---------------------------------------------------
rec = {}
for f in [BD + "/results.tsv"] + sorted(glob.glob(BD + "/results/*.tsv")):
    for r in csv.DictReader(open(f), delimiter="\t"):
        i = r["run_id"]
        if i not in rec or (r.get("started_at") or "") > (rec[i].get("started_at") or ""): rec[i] = r
bd_commit = subprocess.run(["git", "-C", BD, "log", "-1", "--format=%h %ci"], capture_output=True, text=True).stdout.strip()
# ---- planned ids per directory (every rows file) ----------------------------------------------------------
planned = collections.defaultdict(dict)   # dir -> base id -> (kind, line)
for f in sorted(glob.glob(DV + "/cluster/campaign/**/*.rows", recursive=True)) + sorted(glob.glob(DV + "/cluster/*.rows")):
    for l in open(f):
        i = l.split(" ")[0]
        if not i or i.startswith("#"): continue
        p = i.split(".")
        if len(p) > 2: planned[p[1]].setdefault(base(i), (p[0], l.rstrip("\n")))
# ---- expected verdicts (the rule file) --------------------------------------------------------------------
EXP = collections.defaultdict(list)
for r in csv.DictReader(open(DV + "/cluster/campaign/inputs/expected-verdicts.tsv"), delimiter="\t"): EXP[r["dir"]].append(r)
# ---- the campaign CSVs: the measured rows -----------------------------------------------------------------
def load(name):
    try: return list(csv.DictReader(open(f"{T}/{name}.csv")))
    except OSError: return []
ENG = [("stateless", "campaign-stateless", "C2"), ("stateful", "campaign-stateful", "C1"), ("offline", "campaign-offline-tf", "C3J")]
def colour(r, kind):   # blue = at the current pins; red = superseded
    pin = (r.get("pin") or "")[:7]
    if pin != TF_PIN: return "red", f"TraceForge pin {pin or '?'} (current {TF_PIN})"
    rid = r.get("run id", "")
    if kind == "C3J":
        cmd = rec.get(rid, {}).get("command", "")
        if "--merge" not in cmd and not re.search(r"\.m(?=\.|$)", rid): return "red", "in-memory join (the merge join replaces it)"
    return "blue", ""
TIER = {600: "10 min", 3600: "1 h", 14400: "4 h", 86400: "24 h", 172800: "48 h"}
def tier(r):
    t = num(r.get("timeout s")); g = num(r.get("mem GB"))
    return (TIER.get(int(t), f"{int(t)} s") if t else "?") + (f", {int(g)} GB" if g else "")
def verdict_cell(r):
    v = r["verdict"]; e = r.get("expected", "")
    if v == "conforms": s = "conforms"
    elif v == "violation": s = "violation"
    elif v == "t/o": s = "\\faClock\\ (" + tier(r).split(",")[0] + ")"
    elif v == "oom": s = "oom (" + (tier(r).split(", ")[1] if ", " in tier(r) else "?") + ")"
    else: s = esc(v)
    if v in ("conforms", "violation") and e and e != v: s += "$^{\\ast}$"
    return s
def fmt(x, f="{:.1f}"):
    v = num(x); return "" if v is None else f.format(v)
def expected_text(d):
    out = []
    for e in EXP.get(d, []):
        var = e["variant"] if e["variant"] != "-" else "(the single pair)"
        out.append(f"{esc(var)}: expected {e['verdict']}" + (f" ({esc(e['note'])})" if e.get("note") else ""))
    return "; ".join(out)
for name, d in GROUPS[GROUP]:
    out_dir = f"{DV}/benchmark-book/{GROUP}/{name}"; os.makedirs(out_dir, exist_ok=True)
    L = ["\\documentclass[10pt]{article}", "\\usepackage[a4paper,margin=16mm]{geometry}", "\\usepackage{booktabs,longtable,array,xcolor,fontawesome5}", "\\usepackage[hidelinks]{hyperref}",
         "\\setlength{\\LTcapwidth}{\\textwidth}\\setlength{\\tabcolsep}{3pt}", f"\\title{{Evaluation runs: \\textsf{{{esc(name)}}} ({esc(d)})}}", "\\author{}", f"\\date{{generated {datetime.date.today().isoformat()} from brain-data {esc(bd_commit)}}}",
         "\\begin{document}\\maketitle\\small",
         f"\\noindent Every campaign run on this benchmark, one row per instance (variant, specification, communication model, size) and engine; the newest run of an instance wins. "
         f"Expected verdicts (the rule file): {expected_text(d)}. "
         f"\\textcolor{{blue}}{{Blue}} rows are at the current pins (TraceForge {TF_PIN}, merge join {JOIN_PIN}, JMC {JMC_PIN}): a rerun gives the same result. "
         f"\\textcolor{{red}}{{Red}} rows are superseded (an older pin, or the in-memory join that the merge join replaces) and their rerun is pending. "
         "A planned instance without a measurement is not a row: it is listed under ``Not run''. $G$ = graphs explored (specification, implementation); T = wall seconds; M = peak resident MB; tier = the run's time and memory limits; "
         "$^{\\ast}$ marks a verdict other than the expected one; \\faClock\\ = the time tier was reached; oom = the memory limit was reached.",
         ""]
    seen_ids = set()
    for ename, fname, kind in ENG:
        rows = [r for r in load(fname) if r.get("directory") == d]
        rows.sort(key=lambda r: (r["variant"], r["spec"], r["model"], sz(r["size"])))
        if not rows: L.append(f"\\section*{{{ename}}}\\noindent No run of the {ename} engine on this benchmark."); continue
        extra_hdr = {"stateless": " & sweeps & $G_{\\mathit{spec}}^{\\mathit{expl}}$", "stateful": "", "offline": " & export spec & export impl & join & join kind"}[ename]
        ncol = {"stateless": 2, "stateful": 0, "offline": 4}[ename]
        L += [f"\\section*{{{ename}}}", f"\\begin{{longtable}}{{@{{}}lllrrrlrrl{'r' * ncol}l@{{}}}}", "\\toprule",
              f"variant & spec & model & size & $G_{{\\mathit{{spec}}}}$ & $G_{{\\mathit{{impl}}}}$ & verdict & T & M & tier{extra_hdr} & run \\\\", "\\midrule", "\\endhead"]
        for r in rows:
            c, why = colour(r, kind); seen_ids.add(base(r["run id"]))
            if ename == "stateless": ex = f" & {esc(r.get('sweeps',''))} & {esc(r.get('spec graphs explored',''))}"
            elif ename == "offline":
                jk = "merge" if (re.search(r"\.m(?=\.|$)", r["run id"]) or "--merge" in rec.get(r["run id"], {}).get("command", "")) else "in-memory"
                ex = f" & {fmt(r.get('spec export s'))} & {fmt(r.get('impl export s'))} & {fmt(r.get('join s'))} & {jk}"
            else: ex = ""
            run = f"{esc(r.get('node',''))}, {esc((r.get('started','') or '')[:10])}"
            L.append(f"\\color{{{c}}} {esc(r['variant'] if r['variant'] not in ('', 'impl', 'correct') else 'single')} & {esc(r['spec'])} & {r['model']} & {esc(r['size'])} & {esc(r['spec graphs'])} & {esc(r['impl graphs'])} & {verdict_cell(r)} & {fmt(r.get('wall s')) if r['verdict'] in ('conforms','violation') else ''} & {fmt(r.get('peak MB'), '{:.0f}') if r['verdict'] in ('conforms','violation') else ''} & {tier(r)}{ex} & {run} \\\\")
        L += ["\\bottomrule", "\\end{longtable}"]
        # not run: planned ids of this kind without a measured row
        nr = collections.defaultdict(list)
        for b, (k, line) in planned.get(d, {}).items():
            if k != kind or b in seen_ids: continue
            st = None
            for cand in sorted([i for i in rec if base(i) == b], key=lambda i: rec[i].get("started_at") or ""):
                st = rec[cand]["stop_reason"]
            m = re.match(r"^[A-Z0-9]+\.[^.]+\.(impl|spec)_(.*?)(\.t\d+)?(\.[xgf]\d*)?(\.vs-[^.]*)?$", b)
            inst = m.group(2).replace("_", " ") if m else b
            if st is None: nr["planned but never scheduled (grid points beyond the ladder)"].append(inst)
            elif st.startswith("skipped"): nr["skipped: a smaller size had reached its limit"].append(inst)
            elif st.startswith("exit-"): nr[f"the tool ended with an error ({esc(st)})"].append(inst)
            elif st == "refused": nr["refused (an input missing at run time)"].append(inst)
            else: nr[f"left out by the table rules ({esc(st)})"].append(inst)
        if nr:
            L.append(f"\\paragraph{{Not run ({ename}).}}")
            for why, insts in nr.items():
                insts = sorted(set(insts), key=lambda s: sz(s))
                shown = ", ".join(esc(x) for x in insts[:24]) + (f", \\ldots\\ ({len(insts)} instances in all)" if len(insts) > 24 else "")
                L.append(f"{why}: {shown}.\\par")
    L.append("\\end{document}")
    open(f"{out_dir}/EVAL.tex", "w").write("\n".join(L) + "\n")
    if BUILD:
        rc = subprocess.run(["latexmk", "-pdf", "-interaction=nonstopmode", "-halt-on-error", "EVAL.tex"], cwd=out_dir, capture_output=True, text=True).returncode
        subprocess.run(["latexmk", "-c"], cwd=out_dir, capture_output=True); print(f"{name}: EVAL.pdf {'built' if rc == 0 else 'LATEX FAILED'}")
    else: print(f"{name}: EVAL.tex written")
