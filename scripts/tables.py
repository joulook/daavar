#!/usr/bin/env python3
"""Build the paper's tables from brain-data (R4). Attributes are joined from the row files in
daavar/cluster (never trusted from the record alone). usage: tables.py <brain-data> <daavar> <outdir>"""
import csv, glob, sys, collections, os, re, statistics
BD, DV, OUT = sys.argv[1:4]; os.makedirs(OUT, exist_ok=True)
rows = []
for f in [BD + "/results.tsv"] + sorted(glob.glob(BD + "/results/*.tsv")):
    _perpin = 1 if re.search(r"/results/[^/]+\.[0-9a-f]{7}\.tsv$", f) else 0   # results/<node>.<pin>.tsv
    for _r in csv.DictReader(open(f), delimiter="\t"): _r["_perpin"] = _perpin; rows.append(_r)
attrs = {}
for f in [DV + "/cluster/rows.tsv"] + glob.glob(DV + "/cluster/bench/*.rows.tsv") + glob.glob(DV + "/cluster/campaign/*/*.rows.tsv"):
    for r in csv.DictReader(open(f), delimiter="\t"): attrs[r["id"].replace("'", "")] = {k: (v or "").replace("'", "") for k, v in r.items()}  # early row files carried stray quotes (the runner strips them from the id)
# latest record per id wins (reruns append)
import subprocess
PIN = subprocess.run(["git", "-C", DV + "/traceforge", "rev-parse", "--short=7", "HEAD"], capture_output=True, text=True).stdout.strip()
def mispaired(r):  # an id ".vs-<spec variant>" whose command loads another specification's export
    m = re.search(r"\.vs-([a-z0-9-]+)\.", r["run_id"] + ".")
    if not m or not r["run_id"].startswith(("XON2.", "XJ2.")): return False
    ex = re.findall(r"/([A-Za-z0-9._-]+)/export\.jsonl", r.get("command", ""))
    spec = ex[0] if ex else ""
    return spec != "" and ("spec_" + m.group(1).replace("-", "_") + "_") not in spec
_n = len(rows); rows = [r for r in rows if not mispaired(r)]; print(f"mispaired rows dropped (Java spec export of another variant): {_n - len(rows)}")
# the paper's Table 1 names for the benchmark directories (cluster/campaign/inputs/paper-names.tsv)
NAMES = []
for _l in open(DV + "/cluster/campaign/inputs/paper-names.tsv"):
    if _l.startswith("#") or not _l.strip(): continue
    _d, _rx, _prog, _lab = _l.rstrip("\n").split("\t")[:4]; NAMES.append((_d, re.compile(_rx), _prog, _lab))
def paper_name(a):  # (program, variant) as the paper names them; unknown directories keep their own names
    fam, var = a.get("family", ""), a.get("variant", "")
    for d, rx, prog, lab in NAMES:
        if d == fam and rx.search(var): return prog, (var if lab == "." else lab)
    return fam, var
def base_id(i): return re.sub(r"\.c48h$", "", i)   # a 48 h tier rerun of the same row (id + .c48h) is the same case
# pins in order of landing on the frozen engine a3a7e1b; the newest pin's run wins, then the latest start
PIN_ORDER = ["a3a7e1b", "37f37fd", "af56bae", "088b6db", "2a3f8fd", "9ed1131", "9a0629e"]
def pin_rank(r): c = (r.get("traceforge_commit") or "")[:7]; return (r["run_id"].endswith(".c48h"), PIN_ORDER.index(c) if c in PIN_ORDER else (len(PIN_ORDER) if c == PIN else -1), r.get("started_at") or "", r.get("_perpin", 0))   # a per-pin results file beats the legacy per-node file
byid = {}
for r in rows:  # an id run on several pins: the 48 h tier, then the newest pin, wins
    cur = byid.get(base_id(r["run_id"]))
    if cur is None or pin_rank(r) >= pin_rank(cur): byid[base_id(r["run_id"])] = r
rows = list(byid.values())
# an id of the first phase-1 generation carried a `.vs-<spec>` segment the current rows file does not: same command, same attributes
for r in rows: r["a"] = attrs.get(base_id(r["run_id"])) or attrs.get(re.sub(r"\.vs-[A-Za-z0-9-]+", "", base_id(r["run_id"])), {})
def num(x):
    try: return float(x)
    except (TypeError, ValueError): return None
def fmt_s(ms):
    v = num(ms); return "--" if v is None else (f"{v/1000:.1f}" if v < 100000 else f"{v/1000:.0f}")
def fmt_gb(kb):  # prints MB below 1 GB, GB above
    v = num(kb); return "--" if v is None else (f"{v/1024:.0f}\\,MB" if v < 1048576 else f"{v/1048576:.1f}\\,GB")
def esc(s): return s.replace("_", "\\_")
def stats(rs):  # median and max wall (s), max peak RSS, over finished rows
    ws = [num(r["wall_ms"]) for r in rs if r["stop_reason"] == "ok" and num(r["wall_ms"]) is not None]
    ps = [num(r["peak_rss_kb"]) for r in rs if num(r["peak_rss_kb"]) is not None]
    med = f"{statistics.median(ws)/1000:.1f}" if ws else "--"; mx = fmt_s(max(ws)) if ws else "--"
    return med, mx, (fmt_gb(max(ps)) if ps else "--")
FAM = {"a1": "2PC", "a2": "PB store", "a3": "lock", "a4": "hashed store", "a5": "2PC leader", "synth": "reset"}
md = []
# ---- Table RQ1: detection / acceptance on T1 ------------------------------------------------
T1 = [r for r in rows if r["run_id"].startswith("T1.") and r["a"]]
agg = collections.defaultdict(lambda: collections.Counter()); grp = collections.defaultdict(list)
for r in T1:
    fam = r["a"]["family"]; eng = r["a"]["engine"]; exp = r["a"]["expected_verdict"]; v = (r.get("verdict") or "").lower()
    cls = "conf" if exp.startswith("conforms") else "viol"   # controls are counted with their expected verdict
    key = (fam, eng); grp[key].append(r)
    agg[key][cls + "_n"] += 1
    if "control" in exp: agg[key][cls + "_controls"] += 1
    if r["stop_reason"] != "ok": agg[key][cls + "_unfinished"] += 1
    elif (cls == "conf" and v == "conforms") or (cls == "viol" and v == "violation"): agg[key][cls + "_ok"] += 1
    else: agg[key][cls + "_wrong"] += 1
lines = ["\\begin{tabular}{@{}llrrrrrrrrr@{}}", "\\toprule",
         "family & engine & \\multicolumn{3}{c}{expected conforms} & \\multicolumn{3}{c}{expected violation} & \\multicolumn{2}{c}{wall s} & peak \\\\",
         " & & accepted & of & controls & reported & of & controls & median & max & RSS \\\\", "\\midrule"]
for (fam, eng) in sorted(agg):
    c = agg[(fam, eng)]
    def cell(cls):
        n = c[cls + "_n"]; ok = c[cls + "_ok"]; un = c[cls + "_unfinished"]; w = c[cls + "_wrong"]
        s = f"{ok}"; 
        if un: s += f"$^{{\\dagger {un}}}$"
        if w: s += f"$^{{\\ast {w}}}$"
        return s, str(n)
    cc, cn = cell("conf"); fc, fn = cell("viol")
    med, mx, pk = stats(grp[(fam, eng)])
    lines.append(f"{FAM.get(fam, fam)} & {eng} & {cc} & {cn} & {c['conf_controls']} & {fc} & {fn} & {c['viol_controls']} & {med} & {mx} & {pk} \\\\")
lines += ["\\bottomrule", "\\end{tabular}"]
open(OUT + "/rq1-t1.tex", "w").write("\n".join(lines) + "\n")
tot = collections.Counter()
for c in agg.values(): tot.update(c)
md.append(f"RQ1/T1: {len(T1)} rows; expected conforms {tot['conf_ok']}/{tot['conf_n']} accepted ({tot['conf_controls']} controls), expected violation {tot['viol_ok']}/{tot['viol_n']} reported ({tot['viol_controls']} controls), unfinished {tot['conf_unfinished']+tot['viol_unfinished']}, wrong {tot['conf_wrong']+tot['viol_wrong']}")
# ---- Table RQ2: cross-language joins (JMC specification) -------------------------------------
for r in rows:
    if r["run_id"].startswith(("T2.join","T4.join","BX.join")) and r.get("exit_code") == "1" and r.get("verdict"): r["stop_reason"] = "ok"
J = [r for r in rows if r["run_id"].startswith("T2.join") and r["stop_reason"] == "ok"]
jag = collections.defaultdict(collections.Counter); jgrp = collections.defaultdict(list)
for r in J:
    comb = r["run_id"].rsplit(".", 1)[1]; prog = r["run_id"].split(".")[2]; fam = "a3" if prog.startswith("A3") else "a2"
    jag[(fam, comb)][(r.get("verdict") or "-").lower()] += 1; jgrp[(fam, comb)].append(r)
lines = ["\\begin{tabular}{@{}llrrrrrr@{}}", "\\toprule", "family & spec / impl & conforms & violation & refused & \\multicolumn{2}{c}{join s} & peak \\\\", " & & & & & median & max & RSS \\\\", "\\midrule"]
R = [r for r in rows if r["run_id"].startswith("T2.join") and r["stop_reason"] != "ok"]
rag = collections.Counter()
for r in R: rag[("a3" if r["run_id"].split(".")[2].startswith("A3") else "a2", r["run_id"].rsplit(".", 1)[1])] += 1
for key in sorted(set(jag) | set(rag)):
    c = jag.get(key, collections.Counter()); comb = key[1].replace("jmcspec", "JMC spec").replace("tfspec", "TF spec").replace("-jmcimpl", " / JMC impl").replace("-tfimpl", " / TF impl")
    med, mx, pk = stats(jgrp.get(key, []))
    lines.append(f"{FAM[key[0]]} & {comb} & {c['conforms']} & {c['violation']} & {rag.get(key, 0)} & {med} & {mx} & {pk} \\\\")
lines += ["\\bottomrule", "\\end{tabular}"]
open(OUT + "/rq2-joins.tex", "w").write("\n".join(lines) + "\n")
md.append(f"RQ2/T2 joins: {len(J)} decided ({sum(c['conforms'] for c in jag.values())} conforms, {sum(c['violation'] for c in jag.values())} violations), {len(R)} refused (no TraceForge export yet)")
# ---- Table RQ3a: scaling (T3a) ----------------------------------------------------------------
T3 = [r for r in rows if r["run_id"].startswith("T3a.") and r["a"]]
pts = collections.defaultdict(dict)
for r in T3:
    a = r["a"]; key = (a["family"], a["size"]); col = (a["engine"], a["threads"])
    pts[key][col] = r
cols = [("Stateful", "1"), ("SverifyP", "16"), ("SverifyP", "32"), ("SverifyP", "64"), ("CompleteFirst", "1"), ("CFirstP", "16")]
lines = ["\\begin{tabular}{@{}ll" + "r" * len(cols) + "r@{}}", "\\toprule",
         "family & size & \\multicolumn{4}{c}{stateful, threads 1 / 16 / 32 / 64} & \\multicolumn{2}{c}{stateless, 1 / 16} & peak RSS \\\\", "\\midrule"]
def sz(s):
    m = re.findall(r"\d+", s); return tuple(int(x) for x in m)
for key in sorted(pts, key=lambda k: (k[0], sz(k[1]))):
    cells = []; peak = None
    for col in cols:
        r = pts[key].get(col)
        if r is None: cells.append("")
        elif r["stop_reason"] == "ok": cells.append(fmt_s(r["wall_ms"])); peak = max(peak or 0, num(r["peak_rss_kb"]) or 0)
        else: cells.append("t/o" if r["stop_reason"] == "timeout" else ("oom" if r["stop_reason"] == "memory" else "run."))
    lines.append(f"{FAM.get(key[0], key[0])} & {esc(key[1])} & " + " & ".join(cells) + f" & {fmt_gb(peak*1 if peak else None)} \\\\")
lines += ["\\bottomrule", "\\end{tabular}"]
open(OUT + "/rq3-scaling.tex", "w").write("\n".join(lines) + "\n")
md.append(f"RQ3a/T3a: {len(T3)} rows at {len(pts)} points")
# ---- Table: benchmark count agreement (Rust vs Java) ------------------------------------------
B = [r for r in rows if r["run_id"].startswith(("BR.count.", "BJ.count.")) and r["a"]]
prog = collections.defaultdict(dict)
for r in B:
    a = r["a"]; k = (a["family"], a["size"], a["model"], a["variant"]); side = "rust" if r["run_id"].startswith("BR") else "java"
    if r["run_id"].endswith(".c24h") and side in prog[k] and prog[k][side]["stop_reason"] == "ok": continue
    prog[k][side] = r
bag = collections.defaultdict(collections.Counter); bgrp = collections.defaultdict(lambda: {"rust": [], "java": []})
for k, d in prog.items():
    ru, ja = d.get("rust"), d.get("java"); c = bag[k[0]]; c["programs"] += 1
    if ru: bgrp[k[0]]["rust"].append(ru)
    if ja: bgrp[k[0]]["java"].append(ja)
    if ru and ja and ru["stop_reason"] == "ok" and ja["stop_reason"] == "ok":
        if (ru.get("impl_complete"), ru.get("impl_blocked")) == (ja.get("impl_complete"), ja.get("impl_blocked")): c["equal"] += 1
        else: c["differ"] += 1
    else: c["unfinished"] += 1
lines = ["\\begin{tabular}{@{}lrrrrrrrr@{}}", "\\toprule", "benchmark & programs & equal & differ & unfin. & \\multicolumn{2}{c}{TraceForge: max s, peak} & \\multicolumn{2}{c}{\\jmc: max s, peak} \\\\", "\\midrule"]
for k in sorted(bag):
    c = bag[k]; _, rmx, rpk = stats(bgrp[k]["rust"]); _, jmx, jpk = stats(bgrp[k]["java"])
    lines.append(f"{esc(k)} & {c['programs']} & {c['equal']} & {c['differ']} & {c['unfinished']} & {rmx} & {rpk} & {jmx} & {jpk} \\\\")
t = collections.Counter(); [t.update(c) for c in bag.values()]
lines += ["\\midrule", f"all & {t['programs']} & {t['equal']} & {t['differ']} & {t['unfinished']} & & & & \\\\", "\\bottomrule", "\\end{tabular}"]
open(OUT + "/bench-counts.tex", "w").write("\n".join(lines) + "\n")
md.append(f"Benchmarks: {t['programs']} programs with both twins run; counts equal {t['equal']}, differ {t['differ']}, unfinished {t['unfinished']}")
# ---- Campaign tables (owner's plan 2026-11-01): one per mode, same columns ----------------------
# C1 stateful parallel, C2 stateless parallel, C3J offline TF x TF; XON1 online JMC(Java impl) vs TF spec,
# XON2 online TF(Rust impl) vs JMC spec, XJ1 offline TF spec x JMC impl, XJ2 offline JMC spec x TF impl.
# expected verdicts of the program crates (lead's expected-verdicts.tsv: dir, model, variant, spec_variant,
# args template, size_rule, verdict); literature crates state theirs in their READMEs (not machine-read yet)
EXP = []
for fn in ("expected-verdicts.tsv", "expected-verdicts-literature.tsv"):
    try:
        for r in csv.DictReader(open(DV + "/cluster/campaign/inputs/" + fn), delimiter="\t"): EXP.append(r)
    except OSError: pass
def expected_for(a):
    fam, model, var, size = a.get("family", ""), a.get("model", ""), a.get("variant", ""), a.get("size", "")
    nums = [int(x) for x in re.findall(r"\d+", size)]
    # a join or online row names its specification case: its variant words (e.g. "spec hash a4b 2 2 2" -> a4b)
    # replace the implementation's own spec word when the two differ
    sc = re.sub(r"\.x\d+$", "", a.get("spec_case", "")); spec_words = [w for w in re.sub(r"^[^.]*\.spec_?", "", sc).replace("_", " ").split() if not w.isdigit() and w not in ("fifo", "bag", "p2p", "asyn", "causal")] if sc else []
    for e in EXP:
        if e["dir"] != fam or e["model"] != model: continue
        ev = e["variant"] + ("-" + e["spec_variant"] if e["spec_variant"] not in ("-", "") else "")
        if spec_words and e["spec_variant"] not in ("-", ""):
            if not (re.fullmatch(re.escape(e["variant"]) + r"-[a-z0-9]+", var) and e["spec_variant"] == spec_words[-1]): continue
        elif ev != var and e["variant"] != var: continue
        names = re.findall(r"<([a-zA-Z]+)>", e["args"].split(";")[0]); names = [n for n in names if n not in ("variant", "impl", "spec", "v")]
        env = dict(zip(names, nums))
        # the args column's domain ("<...>; k==2" or "n>=2, r>=1") must hold, then the size rule
        dom = e["args"].split(";", 1)[1] if ";" in e["args"] else ""
        try:
            indom = all(eval(c.strip().replace("&&", " and ").replace("||", " or "), {}, env) for c in re.split(r",", dom) if c.strip()) if dom.strip() else True
        except Exception: indom = False
        if not indom: continue
        rule = e["size_rule"].strip()
        try: ok = True if rule == "all" else bool(eval(rule.replace("&&", " and ").replace("||", " or "), {}, env))
        except Exception: ok = False
        if ok: return e["verdict"]
    return None
CAMP = [("C1", "campaign-stateful", "Stateful check, TraceForge, parallel"), ("C2", "campaign-stateless", "Stateless check, TraceForge, parallel"),
        ("C3J", "campaign-offline-tf", "Offline mode, TraceForge specification and implementation"),
        ("XON1", "campaign-online-java-impl", "Online: Java implementation (JMC) against Rust specification export"),
        ("XON2", "campaign-online-rust-impl", "Online: Rust implementation (TraceForge) against Java specification export"),
        ("XJ1", "campaign-offline-rust-spec", "Offline: Rust specification, Java implementation"),
        ("XJ2", "campaign-offline-java-spec", "Offline: Java specification, Rust implementation")]
def fnum(x, div=1.0, fmt="{:.0f}"):
    v = num(x); return "--" if v is None else fmt.format(v / div)
camp_rows = {}
for pre, fname, title in CAMP:
    rs = [r for r in rows if r["run_id"].startswith(pre + ".")]
    if pre in ("C3J", "XJ1", "XJ2"):  # an offline row = spec export + impl export + join
        for r in rs:
            ex = re.findall(r'/([A-Za-z0-9._-]+)/export\.jsonl', r.get("command", ""))   # the runner stores the expanded paths
            if len(ex) == 2:
                sp, im = byid.get(ex[0]), byid.get(ex[1])
                if sp: r["spec_complete"], r["spec_blocked"], r["spec_graphs_total"], r["spec_export_ms"] = sp.get("impl_complete"), sp.get("impl_blocked"), sp.get("impl_graphs_total"), sp.get("wall_ms")
                if im: r["impl_complete"], r["impl_blocked"], r["impl_graphs_total"], r["impl_export_ms"] = im.get("impl_complete"), im.get("impl_blocked"), im.get("impl_graphs_total"), im.get("wall_ms")
                r["join_ms"] = r.get("wall_ms")
                parts = [num(x) for x in (r.get("spec_export_ms"), r.get("impl_export_ms"), r.get("join_ms"))]
                if all(x is not None for x in parts): r["wall_ms"] = sum(parts)
                r["peak_rss_kb"] = max([num(x) or 0 for x in ((sp or {}).get("peak_rss_kb"), (im or {}).get("peak_rss_kb"), r.get("peak_rss_kb"))]) or r.get("peak_rss_kb")
    if pre in ("XON1", "XON2"):  # an online row's specification side is the export it loaded
        for r in rs:
            ex = re.findall(r'--spec\s+\S*/([A-Za-z0-9._-]+)/export\.jsonl', r.get("command", ""))
            sp = byid.get(ex[0]) if ex else None
            if sp: r["spec_complete"], r["spec_blocked"], r["spec_graphs_total"], r["spec_export_ms"] = sp.get("impl_complete"), sp.get("impl_blocked"), sp.get("impl_graphs_total"), sp.get("wall_ms")
    if pre in ("C1", "C2"):  # ruling Q27 (2026-10-31): five programs carry a thread identity inside visible values; the in-process
        # check compares typed values and cannot decide them, so their in-process rows are not verdict rows (offline and online only)
        Q27 = {"bank-grove", "fencing-grove", "kv-exactly-once-grove", "kv-snapshot-isolation-tla", "rpc-at-most-once-aneris", "calculator-disel"}
        _n = len(rs); rs = [r for r in rs if r["a"].get("family") not in Q27]; print(f"{pre}: {_n - len(rs)} in-process rows of the Q27 programs left out")
    if pre in ("C1", "C2"):  # earlier phase-1 ids carried a spec suffix for one and the same command: keep one row per command
        bycmd = {}
        for r in sorted(rs, key=pin_rank): bycmd[r.get("command")] = r   # the newest pin, then the latest start, wins
        rs = list(bycmd.values())
    # a skipped row (its smaller size did not finish) carries no measurement: the tables stop at the size that timed out (owner, 2026-11-02)
    rs = [r for r in rs if not (r.get("stop_reason") or "").startswith("skipped")]
    camp_rows[pre] = rs
    rs.sort(key=lambda r: (r["a"].get("family", ""), r["a"].get("variant", ""), sz(r["a"].get("size", "0"))))
    offline = pre in ("C3J", "XJ1", "XJ2")
    # one file, two longtables: the conforming pairs (no verdict, no miss, no uncovered columns) and the violating pairs
    # (first-miss time and uncovered graphs); a row goes by its verdict, a row without one (time tier reached) by its
    # expectation; a verdict against the README is marked *; the clock marks a row that reached its time tier
    conf_rows, viol_rows = [], []
    for r in rs:
        a = r["a"]; st = r["stop_reason"]
        verdict = (r.get("verdict") or "").lower() if st == "ok" else ("t/o" if st == "timeout" else ("oom" if st == "memory" else st))
        verdict = {"reports": "violation", "violation (sampled)": "violation"}.get(verdict, verdict)   # JMC says "reports"; conf-join adds "(sampled)"
        r["verdict"] = verdict if st == "ok" else r.get("verdict")
        exp = expected_for(a); r["expected"] = exp if exp else ("undefined" if a.get("family") in {e["dir"] for e in EXP} else None)
        if exp is None and a.get("family") in {e["dir"] for e in EXP}: continue   # size outside the defined rules (e.g. pair-swap at k=1): not a verdict row
        r["_mark"] = "$^{\\ast}$" if (exp and st == "ok" and verdict in ("conforms", "violation") and verdict != exp) else ""
        r["_v"] = verdict
        side = verdict if verdict in ("conforms", "violation") else (exp or "conforms")
        (viol_rows if side == "violation" else conf_rows).append(r)
    def tot(r, c, b, t, e=None):  # graphs explored = complete + blocked; else the run's own total (blocked included); else the block's explored count
        vc, vb = num(r.get(c)), num(r.get(b))
        if vc is None and vb is None: return r.get(t) or (r.get(e) if e else "") or ""
        return str(int((vc or 0) + (vb or 0)))
    def cell_time(r):
        v = r["_v"]
        if v == "t/o": return "\\faClock"
        if v == "oom": return "oom"
        if v not in ("conforms", "violation"): return esc(v)
        return fnum(r.get("wall_ms"), 1000, "{:.1f}")
    L = []
    for kind, rows_k in (("conforming", conf_rows), ("violating", viol_rows)):
        viol = kind == "violating"
        hdr = "program(size) & variant & model & $G_{\\mathit{spec}}$ & $G_{\\mathit{impl}}$ & s & MB" + ((" & miss ms" if not offline else "") + " & $G_{\\faGhost}$" if viol else "") + " \\\\"
        spec = "@{}lllrrrr" + (("r" if not offline else "") + "r" if viol else "") + "@{}"
        cap = (f"{title}: {kind} pairs. One cluster run per row; $G$ = graphs explored; s = wall time (\\faClock\\ = the time tier was reached, larger sizes of that ladder are not run); MB = peak resident memory"
               + ("; miss ms = wall time to the first uncovered graph" if (viol and not offline) else "")
               + ("; $G_{\\faGhost}$ = uncovered implementation graphs" if viol else "")
               + "; a verdict against the directory's README is marked $^{\\ast}$.")
        L += [f"\\begin{{longtable}}{{{spec}}}", f"\\caption{{{cap}}}\\label{{tab:{fname}-{'viol' if viol else 'conf'}}}\\\\", "\\toprule", hdr, "\\midrule", "\\endfirsthead", "\\toprule", hdr, "\\midrule", "\\endhead"]
        for r in rows_k:
            a = r["a"]; prog, var = paper_name(a)
            sg = tot(r, "spec_complete", "spec_blocked", "spec_graphs_total", "spec_graphs_explored"); ig = tot(r, "impl_complete", "impl_blocked", "impl_graphs_total")
            extra = ""
            if viol:
                if not offline: extra += f" & {(r.get('first_miss_wall_ms') or '') if r['_v'] == 'violation' else ''}"
                extra += f" & {(r.get('uncovered_total') or r.get('uncovered_distinct') or '') if r['_v'] == 'violation' else ''}"
            L.append(f"\\textsf{{{esc(prog)}}}$({esc(a.get('size',''))})${r['_mark']} & {esc(var)} & {a.get('model','')} & {sg} & {ig} & {cell_time(r)} & {fnum(r.get('peak_rss_kb'), 1024)}{extra} \\\\")
        L += ["\\bottomrule", "\\end{longtable}", ""]
    open(OUT + f"/{fname}.tex", "w").write("\n".join(L) + "\n")
    # the same compiled table as CSV (for the owner's Google Sheet)
    with open(OUT + f"/{fname}.csv", "w", newline="") as fh:
        offline = pre in ("C3J", "XJ1", "XJ2"); par_tf = pre in ("C1", "C2")
        cols = ["program", "variant", "size", "model", "spec graphs", "impl graphs", "wall s", "peak MB", "verdict", "expected"] + ([] if (offline or par_tf) else ["first miss iteration"]) + ([] if offline else ["first miss ms"]) + ["uncovered", "directory", "run id", "node", "pin", "started"]
        w = csv.writer(fh); w.writerow(cols)
        for r in rs:
            a = r["a"]; st = r["stop_reason"]
            v = (r.get("verdict") or "").lower() if st == "ok" else ("t/o" if st == "timeout" else ("oom" if st == "memory" else ("skipped" if st.startswith("skipped") else st)))
            def t(c, b, tt, e=None):  # complete + blocked; else the run's own total (blocked included); else the block's explored count
                vc, vb = num(r.get(c)), num(r.get(b))
                if vc is None and vb is None:
                    v = r.get(tt) or (r.get(e) if e else "") or ""; return int(v) if v.isdigit() else ""
                return int((vc or 0) + (vb or 0))
            prog, var = paper_name(a)
            row = [prog, var, a.get("size", ""), a.get("model", ""), t("spec_complete", "spec_blocked", "spec_graphs_total", "spec_graphs_explored"), t("impl_complete", "impl_blocked", "impl_graphs_total"),
                   fnum(r.get("wall_ms"), 1000, "{:.1f}"), fnum(r.get("peak_rss_kb"), 1024), v, r.get("expected") or ""]
            if not (offline or par_tf): row.append(r.get("first_miss_iteration") or "")
            if not offline: row.append((r.get("first_miss_wall_ms") or "") if v.startswith("violation") else "")
            row += [r.get("uncovered_total") or r.get("uncovered_distinct") or "", a.get("family", ""), r["run_id"], r.get("node", ""), r.get("traceforge_commit", ""), r.get("started_at", "")]
            w.writerow(row)
    ok = sum(1 for r in rs if r["stop_reason"] == "ok"); ruled = [r for r in rs if r.get("expected") and r["expected"] != "undefined"]
    mism = sum(1 for r in ruled if r["stop_reason"] == "ok" and (r.get("verdict") or "").lower() not in ("", r["expected"]))
    md.append(f"{pre} {title}: {len(rs)} rows, {ok} finished, verdicts {dict(collections.Counter((r.get('verdict') or '-').lower() for r in rs if r['stop_reason']=='ok'))}, against expectation: {mism} mismatches of {len(ruled)} with a rule, {sum(1 for r in rs if r.get('expected') == 'undefined')} outside the rules")
open(OUT + "/SUMMARY.md", "w").write("\n".join(md) + "\n"); print("\n".join(md))
# ---- Excel workbook: every table's compiled rows plus the raw records behind it -----------------
try:
    import openpyxl
    wb = openpyxl.Workbook(); wb.remove(wb.active)
    RAW = ["run_id","program","node","traceforge_commit","jmc_commit","table","family","sizes","comm_model","variant","engine","impl_threads","expected_verdict","verdict","stop_reason","spec_complete","spec_blocked","spec_graphs_total","impl_complete","impl_blocked","impl_graphs_total","spec_graphs_explored","sweeps","cache_tests","cache_hits","spec_summaries","impl_summaries","uncovered_total","uncovered_distinct","first_miss_iteration","first_miss_wall_ms","wall_ms","cpu_user_s","cpu_sys_s","peak_rss_kb","spec_phase_ms","impl_phase_ms","explore_ms","summarise_ms","index_ms","lookup_ms","export_ms","join_ms","started_at","command"]
    def sheet(name, rs):
        ws = wb.create_sheet(name[:31]); ws.append(RAW)
        for r in rs:
            a = r.get("a", {})
            ws.append([(paper_name(a)[0] if c == "program" else (r.get(c) if r.get(c) not in (None, "") else a.get(c, ""))) for c in RAW])
    sheet("T1 detection raw", T1); sheet("T2 joins raw", J + R); sheet("T3a scaling raw", T3); sheet("bench counts raw", B)
    for pre, fname, title in CAMP: sheet(fname.replace("campaign-", "camp "), camp_rows[pre])
    ws = wb.create_sheet("README"); ws.append(["Each sheet holds the raw records (one per cluster run) behind the paper table of the same name; the LaTeX tables are compiled from these rows by tables.py. Columns: see brain-data/SCHEMA.md."])
    wb.save(OUT + "/tables-raw.xlsx")
    # the compiled campaign tables, one sheet each, from the CSVs just written
    wb2 = openpyxl.Workbook(); wb2.remove(wb2.active)
    for pre, fname, title in CAMP:
        ws = wb2.create_sheet(fname.replace("campaign-", "")[:31])
        with open(OUT + f"/{fname}.csv", newline="") as fh:
            for i, row in enumerate(csv.reader(fh)):
                ws.append([(int(c) if c.isdigit() else (float(c) if re.fullmatch(r"-?\d+\.\d+", c) else c)) for c in row])
        ws.freeze_panes = "A2"
        for col in ws.columns:
            ws.column_dimensions[col[0].column_letter].width = min(40, max(8, max(len(str(c.value or "")) for c in col) + 1))
    ws = wb2.create_sheet("about"); ws.append(["Compiled campaign tables (one cluster run per row), generated by daavar/scripts/tables.py from the brain-data results; refreshed every 30 min. Raw records: tables-raw.xlsx."])
    wb2.save(OUT + "/campaign-tables.xlsx"); print("xlsx written")
except ImportError:
    print("openpyxl missing: no xlsx")
