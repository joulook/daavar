#!/usr/bin/env python3
"""Build the paper's tables from brain-data (R4). Attributes are joined from the row files in
daavar/cluster (never trusted from the record alone). usage: tables.py <brain-data> <daavar> <outdir>"""
import csv, glob, sys, collections, os, re, statistics
BD, DV, OUT = sys.argv[1:4]; os.makedirs(OUT, exist_ok=True)
rows = []
for f in [BD + "/results.tsv"] + sorted(glob.glob(BD + "/results/*.tsv")):
    rows += list(csv.DictReader(open(f), delimiter="\t"))
attrs = {}
for f in [DV + "/cluster/rows.tsv"] + glob.glob(DV + "/cluster/bench/*.rows.tsv"):
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
    camp_rows[pre] = rs
    rs.sort(key=lambda r: (r["a"].get("family", ""), r["a"].get("variant", ""), sz(r["a"].get("size", "0"))))
    L = ["\\begin{tabular}{@{}llllrrrrlrrr@{}}", "\\toprule",
         "benchmark & variant & size & model & spec graphs & impl graphs & wall s & peak MB & verdict & 1st miss it. & 1st miss s & uncovered \\\\", "\\midrule"]
    for r in rs:
        a = r["a"]; st = r["stop_reason"]
        verdict = (r.get("verdict") or "").lower() if st == "ok" else ("t/o" if st == "timeout" else ("oom" if st == "memory" else ("skipped" if st.startswith("skipped") else st)))
        sg = r.get("spec_complete") or ""; ig = r.get("impl_complete") or ""
        L.append(f"{esc(a.get('family',''))} & {esc(a.get('variant',''))} & {esc(a.get('size',''))} & {a.get('model','')} & {sg} & {ig} & {fnum(r.get('wall_ms'), 1000, '{:.1f}')} & {fnum(r.get('peak_rss_kb'), 1024)} & {verdict} & {r.get('first_miss_iteration') or ''} & {fnum(r.get('first_miss_wall_ms'), 1000, '{:.1f}') if r.get('first_miss_wall_ms') else ''} & {r.get('uncovered_total') or r.get('uncovered_distinct') or ''} \\\\")
    L += ["\\bottomrule", "\\end{tabular}"]
    open(OUT + f"/{fname}.tex", "w").write("\n".join(L) + "\n")
    ok = sum(1 for r in rs if r["stop_reason"] == "ok"); md.append(f"{pre} {title}: {len(rs)} rows, {ok} finished, verdicts {dict(collections.Counter((r.get('verdict') or '-').lower() for r in rs if r['stop_reason']=='ok'))}")
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
    wb.save(OUT + "/tables-raw.xlsx"); print("xlsx written")
except ImportError:
    print("openpyxl missing: no xlsx")
