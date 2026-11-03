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
GROUPS = {"synthetic": [("naive", "s1-naive"), ("share", "s2-share"), ("commit", "s3-commit"), ("chain", "s4-chain"), ("reset", "s5-reset"), ("width", "s6-width")],
          "application": [("2pc", "a1-commit"), ("2pc-ab", "two-pc-abstain-kragl"), ("2pc-ds", "two-pc-disel"), ("2pc-lb", "two-pc-lbinria"), ("2pc-le", "election"), ("2pc-tr", "two-pc-trillium"),
                          ("bcast", "broadcast-consensus-civl"), ("chan", "reliable-channel-aneris"), ("ewd998", "ewd998-termination"), ("lb", "a4-balancer"), ("lock", "a3-lock"), ("pbkv", "a2-store"),
                          ("prodcons", "producer-consumer-civl"), ("replog", "replicated-log-aneris"), ("replog-co", "replicated-log-esop20"), ("ring-el", "ring-election-igloo"), ("zab", "zab-broadcast-remix")],
          "realsw": [("raft", "raft-rs"), ("omnipaxos", "omnipaxos")]}
if GROUP not in GROUPS: sys.exit(f"group {GROUP}: not defined yet (groups: {', '.join(GROUPS)})")
def esc(s): return str(s).replace("_", "\\_").replace("%", "\\%").replace("&", "\\&")
def num(x):
    try: return float(x)
    except (TypeError, ValueError): return None
def sz(s): return [int(x) for x in re.findall(r"\d+", s)]
SUF = re.compile(r"(\.c1h|\.c48h|\.j2|\.m|\.r2)+$")
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
ENG = [("stateless", "campaign-stateless", "C2"), ("stateful", "campaign-stateful", "C1"), ("offline", "campaign-offline-tf", "C3J"),
       ("cross offline, Java implementation against the Rust specification", "campaign-offline-rust-spec", "XJ1"),
       ("cross offline, Rust implementation against the Java specification", "campaign-offline-java-spec", "XJ2"),
       ("cross online, Java implementation (JMC) against the Rust specification export", "campaign-online-java-impl", "XON1"),
       ("cross online, Rust implementation (TraceForge) against the Java specification export", "campaign-online-rust-impl", "XON2"),
       ("twins offline, Rust implementation against its Java twin", "campaign-twin-offline-java-spec", "XT1"),
       ("twins offline, Java implementation against its Rust twin", "campaign-twin-offline-rust-spec", "XT2"),
       ("twins online, Java implementation (JMC) against the Rust twin's export", "campaign-twin-online-java-impl", "XTON1"),
       ("twins online, Rust implementation (TraceForge) against the Java twin's export", "campaign-twin-online-rust-impl", "XTON2"),
       ("context-free twins offline, Rust implementation against the context-free Java port", "campaign-cftwin-offline-java-spec", "XC1"),
       ("context-free twins offline, Java port against the Rust implementation", "campaign-cftwin-offline-rust-spec", "XC2"),
       ("context-free twins online, Java port (JMC) against the Rust implementation's export", "campaign-cftwin-online-java-impl", "XCON1"),
       ("context-free twins online, Rust implementation (TraceForge) against the Java port's export", "campaign-cftwin-online-rust-impl", "XCON2")]
OFFLINE = {"C3J", "XJ1", "XJ2", "XT1", "XT2", "XC1", "XC2"}; ONLINE = {"XON1", "XON2", "XTON1", "XTON2", "XCON1", "XCON2"}
JAVA = {"XJ1", "XJ2", "XON1", "XON2", "XT1", "XT2", "XTON1", "XTON2", "XC1", "XC2", "XCON1", "XCON2"}   # a Java side is involved: the JMC pin matters
def colour(r, kind):   # blue = at the current pins; red = superseded
    pin = (r.get("pin") or "")[:7]
    if pin != TF_PIN: return "red", f"TraceForge pin {pin or '?'} (current {TF_PIN})"
    rid = r.get("run id", "")
    if kind in JAVA:
        j = (rec.get(rid, {}).get("jmc_commit") or "")[:8]
        if j != JMC_PIN: return "red", f"JMC pin {j or '?'} (current {JMC_PIN})"
    if kind in OFFLINE:
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
         "A planned instance without a measurement is not a row: it is listed under ``Not run''. $G_{\\mathit{spec}}$, $G_{\\mathit{impl}}$ = the graphs of the specification and of the implementation (the stateless engine's own count of specification graphs explored, repeats included, is $G_{\\mathit{spec}}^{\\mathit{expl}}$); $G_x$ = the implementation graphs no specification graph covers (violating pairs); T = wall seconds; M = peak resident MB; tier = the run's time and memory limits; "
         "$^{\\ast}$ marks a verdict other than the expected one; \\faClock\\ = the time tier was reached; oom = the memory limit was reached.",
         ""]
    seen_ids = set()
    data = {kind: [r for r in load(fname) if r.get("directory") == d] for ename, fname, kind in ENG}
    # G_spec = the specification's own graph count: from the stateful or offline row of the instance (the stateless engine
    # reports the specification graphs it explored, repeats counted, which is G_spec^expl, not G_spec)
    gspec = {}
    for kind in ("C1", "C3J", "XJ1", "XJ2", "XT1", "XT2", "XC1", "XC2"):
        for r in data[kind]:
            if r.get("spec graphs") and (r["verdict"] in ("conforms", "violation") or r.get("spec export s")):
                gspec.setdefault((r["variant"], r["spec"], r["model"], r["size"]), r["spec graphs"])
    for ename, fname, kind in ENG:
        rows = data[kind]
        if not rows:
            if kind in ("C1", "C2", "C3J"): L.append(f"\\section*{{{ename}}}\\noindent No run of the {ename} engine on this benchmark.")
            continue
        L.append(f"\\section*{{{ename}}}")
        pairs = collections.OrderedDict()
        for r in rows: pairs.setdefault((r["variant"], r["spec"], r["model"]), []).append(r)
        def pair_side(rs):   # conforming pairs first, then violating: by the expectation, else by the decided verdicts
            e = next((r["expected"] for r in rs if r.get("expected") in ("conforms", "violation")), None)
            if e: return e
            return "violation" if any(r["verdict"] == "violation" for r in rs) else "conforms"
        for (var, spec, model), rs in sorted(pairs.items(), key=lambda kv: (pair_side(kv[1]) == "violation", kv[0])):
            side = pair_side(rs); viol = side == "violation"
            rs.sort(key=lambda r: sz(r["size"]))
            label = esc(var if var not in ("", "impl") else "single")
            L.append(f"\\subsection*{{{ename}: pair \\textsf{{{label}}}" + (f" against \\textsf{{{esc(spec)}}}" if spec and spec not in (var, "spec", "correct") else "") + f", model {model}, expected {side}}}")
            extra_hdr = " & sweeps & $G_{\\mathit{spec}}^{\\mathit{expl}}$" if kind == "C2" else (" & export spec & export impl & join & join kind" if kind in OFFLINE else (" & first miss ms" if kind in ONLINE else ""))
            ncol = 2 if kind == "C2" else (4 if kind in OFFLINE else (1 if kind in ONLINE else 0))
            gx_hdr = " & $G_x$" if viol else ""
            L += [f"\\begin{{longtable}}{{@{{}}rrr{'r' if viol else ''}lrrl{'r' * ncol}l@{{}}}}", "\\toprule",
                  f"size & $G_{{\\mathit{{spec}}}}$ & $G_{{\\mathit{{impl}}}}${gx_hdr} & verdict & T & M & tier{extra_hdr} & run \\\\", "\\midrule", "\\endhead"]
            for r in rs:
                c, why = colour(r, kind); seen_ids.add(base(r["run id"]))
                dec = r["verdict"] in ("conforms", "violation")
                gs = r["spec graphs"] if kind != "C2" else gspec.get((var, spec, model, r["size"]), "")
                if kind == "C2": ex = f" & {esc(r.get('sweeps','')) if dec else ''} & {esc(r.get('spec graphs explored','')) if dec else ''}"
                elif kind in ONLINE: ex = f" & {esc(r.get('first miss ms','')) if r['verdict'] == 'violation' else ''}"
                elif kind in OFFLINE:
                    jk = "merge" if (re.search(r"\.m(?=\.|$)", r["run id"]) or "--merge" in rec.get(r["run id"], {}).get("command", "")) else "in-memory"
                    ex = f" & {fmt(r.get('spec export s'))} & {fmt(r.get('impl export s'))} & {fmt(r.get('join s')) if dec else ''} & {jk}"
                else: ex = ""
                gx = (f" & {esc(r.get('uncovered', '')) if r['verdict'] == 'violation' else ''}") if viol else ""
                run = f"{esc(r.get('node',''))}, {esc((r.get('started','') or '')[:10])}"
                L.append(f"\\color{{{c}}} {esc(r['size'])} & {esc(gs)} & {esc(r['impl graphs'])}{gx} & {verdict_cell(r)} & {fmt(r.get('wall s')) if dec else ''} & {fmt(r.get('peak MB'), '{:.0f}') if dec else ''} & {tier(r)}{ex} & {run} \\\\")
            L += ["\\bottomrule", "\\end{longtable}"]
        # not run: planned ids of this kind without a measured row
        nr = collections.defaultdict(list)
        for b, (k, line) in planned.get(d, {}).items():
            if k != kind or b in seen_ids: continue
            st = None
            for cand in sorted([i for i in rec if base(i) == b], key=lambda i: rec[i].get("started_at") or ""):
                st = rec[cand]["stop_reason"]
            core = re.sub(r"(\.(t\d+|w\d+|[xgf]\d*|vs-[^.]*|(jmc|tf)(spec|impl)-(jmc|tf)(spec|impl)|jimpl-tfimpl|tfimpl-jimpl|tfspec-jmcimpl|jmcspec-tfimpl))+$", "", b)   # the ladder, grid, thread and spec tags, in any order
            m = re.match(r"^[A-Z0-9]+\.[^.]+\.(impl|spec)_(.*)$", core)
            inst = m.group(2).replace("_", " ") if m else core
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
