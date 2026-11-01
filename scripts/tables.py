#!/usr/bin/env python3
"""Build the paper's tables from brain-data (R4). Attributes are joined from the row files in
daavar/cluster (never trusted from the record alone). usage: tables.py <brain-data> <daavar> <outdir>"""
import csv, glob, sys, collections, os, re, statistics
BD, DV, OUT = sys.argv[1:4]; os.makedirs(OUT, exist_ok=True)
rows = []
for f in [BD + "/results.tsv"] + sorted(glob.glob(BD + "/results/*.tsv")):
    rows += list(csv.DictReader(open(f), delimiter="\t"))
attrs = {}
for f in [DV + "/cluster/rows.tsv"] + glob.glob(DV + "/cluster/bench/*.rows.tsv") + glob.glob(DV + "/cluster/campaign/*/*.rows.tsv"):
    for r in csv.DictReader(open(f), delimiter="\t"): attrs[r["id"].replace("'", "")] = {k: (v or "").replace("'", "") for k, v in r.items()}  # early row files carried stray quotes (the runner strips them from the id)
# latest record per id wins (reruns append)
import subprocess
PIN = subprocess.run(["git", "-C", DV + "/traceforge", "rev-parse", "--short=7", "HEAD"], capture_output=True, text=True).stdout.strip()
byid = {}
for r in rows:  # an id run on several pins: the current pin wins, else the last seen
    cur = byid.get(r["run_id"])
    if cur is None or (r.get("traceforge_commit") or "")[:7] == PIN or (cur.get("traceforge_commit") or "")[:7] != PIN: byid[r["run_id"]] = r
rows = list(byid.values())
for r in rows: r["a"] = attrs.get(r["run_id"], {})
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
    sc = a.get("spec_case", ""); spec_words = [w for w in re.sub(r"^[^.]*\.spec_?", "", sc).replace("_", " ").split() if not w.isdigit() and w not in ("fifo", "bag", "p2p", "asyn", "causal")] if sc else []
    for e in EXP:
        if e["dir"] != fam or e["model"] != model: continue
        ev = e["variant"] + ("-" + e["spec_variant"] if e["spec_variant"] not in ("-", "") else "")
        if spec_words and e["spec_variant"] not in ("-", ""):
            if not (re.fullmatch(re.escape(e["variant"]) + r"-[a-z0-9]+", var) and e["spec_variant"] == spec_words[-1]): continue
        elif ev != var and e["variant"] != var: continue
        names = re.findall(r"<([a-z]+)>", e["args"].split(";")[0]); names = [n for n in names if n not in ("variant", "impl", "spec")]
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
            ex = re.findall(r'"\$EVAL_OUT"/([^/ ]+)/export\.jsonl', r.get("command", ""))
            if len(ex) == 2:
                sp, im = byid.get(ex[0]), byid.get(ex[1])
                if sp: r["spec_complete"], r["spec_blocked"], r["spec_export_ms"] = sp.get("impl_complete"), sp.get("impl_blocked"), sp.get("wall_ms")
                if im: r["impl_complete"], r["impl_blocked"], r["impl_export_ms"] = im.get("impl_complete"), im.get("impl_blocked"), im.get("wall_ms")
                r["join_ms"] = r.get("wall_ms")
                parts = [num(x) for x in (r.get("spec_export_ms"), r.get("impl_export_ms"), r.get("join_ms"))]
                if all(x is not None for x in parts): r["wall_ms"] = sum(parts)
                r["peak_rss_kb"] = max([num(x) or 0 for x in ((sp or {}).get("peak_rss_kb"), (im or {}).get("peak_rss_kb"), r.get("peak_rss_kb"))]) or r.get("peak_rss_kb")
    if pre in ("C1", "C2"):  # earlier phase-1 ids carried a spec suffix for one and the same command: keep one row per command
        bycmd = {}
        for r in rs: bycmd[r.get("command")] = r
        rs = list(bycmd.values())
    camp_rows[pre] = rs
    rs.sort(key=lambda r: (r["a"].get("family", ""), r["a"].get("variant", ""), sz(r["a"].get("size", "0"))))
    L = ["\\begin{longtable}{@{}llllrrrrlrrr@{}}", f"\\caption{{{title} (one cluster run per row; t/o = tier reached; skipped = the smaller size did not finish).}}\\label{{tab:{fname}}}\\\\", "\\toprule",
         "benchmark & variant & size & model & spec graphs & impl graphs & wall s & peak MB & verdict & 1st miss it. & 1st miss ms & uncovered \\\\", "\\midrule", "\\endfirsthead",
         "\\toprule", "benchmark & variant & size & model & spec graphs & impl graphs & wall s & peak MB & verdict & 1st miss it. & 1st miss ms & uncovered \\\\", "\\midrule", "\\endhead"]
    for r in rs:
        a = r["a"]; st = r["stop_reason"]
        verdict = (r.get("verdict") or "").lower() if st == "ok" else ("t/o" if st == "timeout" else ("oom" if st == "memory" else ("skipped" if st.startswith("skipped") else st)))
        verdict = {"reports": "violation", "violation (sampled)": "violation"}.get(verdict, verdict)   # JMC says "reports"; conf-join adds "(sampled)"
        r["verdict"] = verdict if st == "ok" else r.get("verdict")
        exp = expected_for(a); r["expected"] = exp
        if exp is None and a.get("family") in {e["dir"] for e in EXP}: continue   # size outside the defined rules (e.g. pair-swap at k=1): not a verdict row
        if exp and st == "ok" and verdict in ("conforms", "violation") and verdict != exp: verdict += "$^{\\ast}$"
        def tot(c, b):  # graphs explored = complete + blocked
            vc, vb = num(r.get(c)), num(r.get(b)); return "" if vc is None and vb is None else str(int((vc or 0) + (vb or 0)))
        sg = tot("spec_complete", "spec_blocked"); ig = tot("impl_complete", "impl_blocked")
        L.append(f"{esc(a.get('family',''))} & {esc(a.get('variant',''))} & {esc(a.get('size',''))} & {a.get('model','')} & {sg} & {ig} & {fnum(r.get('wall_ms'), 1000, '{:.1f}')} & {fnum(r.get('peak_rss_kb'), 1024)} & {verdict} & {r.get('first_miss_iteration') or ''} & {(r.get('first_miss_wall_ms') or '') if verdict.startswith('violation') else ''} & {r.get('uncovered_total') or r.get('uncovered_distinct') or ''} \\\\")
    L += ["\\bottomrule", "\\end{longtable}"]
    open(OUT + f"/{fname}.tex", "w").write("\n".join(L) + "\n")
    # the same compiled table as CSV (for the owner's Google Sheet)
    with open(OUT + f"/{fname}.csv", "w", newline="") as fh:
        w = csv.writer(fh); w.writerow(["benchmark", "variant", "size", "model", "spec graphs", "impl graphs", "wall s", "peak MB", "verdict", "expected", "first miss iteration", "first miss ms", "uncovered", "run id", "node", "pin", "started"])
        for r in rs:
            a = r["a"]; st = r["stop_reason"]
            v = (r.get("verdict") or "").lower() if st == "ok" else ("t/o" if st == "timeout" else ("oom" if st == "memory" else ("skipped" if st.startswith("skipped") else st)))
            def t(c, b):
                vc, vb = num(r.get(c)), num(r.get(b)); return "" if vc is None and vb is None else int((vc or 0) + (vb or 0))
            w.writerow([a.get("family", ""), a.get("variant", ""), a.get("size", ""), a.get("model", ""), t("spec_complete", "spec_blocked"), t("impl_complete", "impl_blocked"),
                        fnum(r.get("wall_ms"), 1000, "{:.1f}"), fnum(r.get("peak_rss_kb"), 1024), v, r.get("expected") or "", r.get("first_miss_iteration") or "",
                        (r.get("first_miss_wall_ms") or "") if v.startswith("violation") else "", r.get("uncovered_total") or r.get("uncovered_distinct") or "", r["run_id"], r.get("node", ""), r.get("traceforge_commit", ""), r.get("started_at", "")])
    ok = sum(1 for r in rs if r["stop_reason"] == "ok"); mism = sum(1 for r in rs if r.get("expected") and r["stop_reason"] == "ok" and (r.get("verdict") or "").lower() != r["expected"])
    md.append(f"{pre} {title}: {len(rs)} rows, {ok} finished, verdicts {dict(collections.Counter((r.get('verdict') or '-').lower() for r in rs if r['stop_reason']=='ok'))}, against expectation: {mism} mismatches of {sum(1 for r in rs if r.get('expected'))} with a rule")
open(OUT + "/SUMMARY.md", "w").write("\n".join(md) + "\n"); print("\n".join(md))
# ---- Excel workbook: every table's compiled rows plus the raw records behind it -----------------
try:
    import openpyxl
    wb = openpyxl.Workbook(); wb.remove(wb.active)
    RAW = ["run_id","node","traceforge_commit","jmc_commit","table","family","sizes","comm_model","variant","engine","impl_threads","expected_verdict","verdict","stop_reason","spec_complete","spec_blocked","impl_complete","impl_blocked","spec_summaries","impl_summaries","uncovered_total","uncovered_distinct","first_miss_iteration","first_miss_wall_ms","wall_ms","cpu_user_s","cpu_sys_s","peak_rss_kb","spec_phase_ms","impl_phase_ms","explore_ms","summarise_ms","index_ms","lookup_ms","export_ms","join_ms","started_at","command"]
    def sheet(name, rs):
        ws = wb.create_sheet(name[:31]); ws.append(RAW)
        for r in rs:
            a = r.get("a", {})
            ws.append([r.get(c) if r.get(c) not in (None, "") else a.get(c, "") for c in RAW])
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
