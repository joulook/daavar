#!/usr/bin/env python3
"""TraceForge standalone conformance table (owner's design, 2026-11-02): one block per instance (program, variant,
model), one row per size; the implementation's and specification's graph counts; then per mode the measurements:
stateless (sweeps kept, specification graphs explored in total, time, peak memory), stateful (time, peak memory),
offline (time of both exports plus the join, memory = the two summary files kept). Conforming pairs and the synthetic
families only. Reads the campaign CSVs written by tables.py; checks that the graph counts agree across the modes.
usage: standalone-table.py <tables-dir>"""
import sys, csv, re, collections
T = sys.argv[1]
def load(name):
    return list(csv.DictReader(open(f"{T}/{name}.csv")))
C1, C2, C3 = load("campaign-stateful"), load("campaign-stateless"), load("campaign-offline-tf")
SYN = {"naive", "share", "commit", "chain", "reset", "width"}
def key(r): return (r["program"], r["variant"], r["spec"], r["model"], r["size"])
def num(x):
    try: return float(x)
    except (TypeError, ValueError): return None
def graphs(r): return r.get("impl graphs") or "", r.get("spec graphs") or ""
inst = collections.OrderedDict(); notes = []
for mode, rs in (("stateful", C1), ("stateless", C2), ("offline", C3)):
    for r in rs:
        conf = (r["expected"] == "conforms" and r["verdict"] in ("conforms", "t/o", "oom")) or (r["program"] in SYN and r["verdict"] in ("conforms", "t/o", "oom"))
        if not conf: continue
        inst.setdefault(key(r), {})[mode] = r
def sz(s): return [int(x) for x in re.findall(r"\d+", s)]
order = sorted(inst.keys(), key=lambda k: (k[0] not in SYN, k[0], k[1], k[2], k[3], sz(k[4])))
# consistency of the graph counts across the modes
incons = []
for k in order:
    m = inst[k]; gi = {md: m[md]["impl graphs"] for md in m if m[md]["verdict"] == "conforms" and m[md]["impl graphs"]}
    gs = {md: m[md]["spec graphs"] for md in m if m[md]["verdict"] == "conforms" and m[md]["spec graphs"]}
    if len(set(gi.values())) > 1: incons.append((k, "impl", gi))
    gs2 = {md: v for md, v in gs.items() if md != "stateless"}   # the stateless engine reports spec graphs explored, not the spec's own count
    if len(set(gs2.values())) > 1: incons.append((k, "spec", gs2))
def cell(r, col, div=1.0, fmt="{:.1f}"):
    if r is None: return ""
    if r["verdict"] == "t/o": return "\\faClock" if col in ("wall s",) else ""
    v = num(r.get(col)); return "" if v is None else fmt.format(v / div)
def name(k):
    prog, var, spec, model, _ = k
    sub = var if var not in ("", "spec", "impl") else ""
    if spec and spec not in var: sub = (sub + "/" if sub else "") + spec
    return f"\\textsf{{{prog}}}" + (f"$_{{\\mathit{{{sub.replace('_','-')}}}}}^{{\\mathit{{{model}}}}}$" if sub else f"$^{{\\mathit{{{model}}}}}$")
L = ["\\begin{longtable}{@{}lrrrrrrrrrrr@{}}",
     "\\caption{TraceForge standalone conformance checking: conforming pairs and the synthetic families. Instance = program$_{\\mathit{variant}}^{\\mathit{model}}$; $G_{\\mathit{impl}}$, $G_{\\mathit{spec}}$ = the graphs of the implementation and of the specification (plain Must counts, equal across the modes). Stateless: sweeps kept in the cache, specification graphs explored in total (repeats counted), time, peak memory. Stateful: time, peak memory. Offline: time of both exports and the join, memory = the size of the two summary files kept. 16 threads; \\faClock\\ = the 1 h tier reached.}\\label{tab:standalone}\\\\",
     "\\toprule", "instance & size & $G_{\\mathit{impl}}$ & $G_{\\mathit{spec}}$ & \\multicolumn{4}{c}{stateless} & \\multicolumn{2}{c}{stateful} & \\multicolumn{2}{c}{offline} \\\\",
     "\\cmidrule(lr){5-8}\\cmidrule(lr){9-10}\\cmidrule(lr){11-12}",
     " & & & & sweeps & $G_{\\mathit{spec}}^{\\mathit{expl}}$ & s & MB & s & MB & s & MB \\\\", "\\midrule", "\\endfirsthead",
     "\\toprule", "instance & size & $G_{\\mathit{impl}}$ & $G_{\\mathit{spec}}$ & sweeps & $G_{\\mathit{spec}}^{\\mathit{expl}}$ & s & MB & s & MB & s & MB \\\\", "\\midrule", "\\endhead"]
csvrows = [["program", "variant", "spec", "model", "size", "G impl", "G spec", "stateless sweeps", "stateless spec graphs explored", "stateless s", "stateless MB", "stateful s", "stateful MB", "offline s", "offline MB (both export files)"]]
prev = None
for k in order:
    m = inst[k]; c1, c2, c3 = m.get("stateful"), m.get("stateless"), m.get("offline")
    gi = next((x[0] for x in (graphs(r) for r in (c1, c3, c2) if r) if x[0]), ""); gs = next((x[1] for x in (graphs(r) for r in (c1, c3) if r) if x[1]), "")
    off_mb = ""
    if c3 and c3["verdict"] == "conforms":
        b = [num(c3.get("spec export bytes")), num(c3.get("impl export bytes"))]
        if all(x is not None for x in b): off_mb = "{:.1f}".format(sum(b) / 1048576)
    nm = name(k) if k[:4] != prev else ""; prev = k[:4]
    if nm and L[-1] != "\\endhead" and not L[-1].startswith("\\midrule"): L.append("\\midrule")
    L.append(f"{nm} & {k[4]} & {gi} & {gs} & {(c2 or {}).get('sweeps','') if c2 and c2['verdict']=='conforms' else ''} & {(c2 or {}).get('spec graphs explored','') if c2 and c2['verdict']=='conforms' else ''} & {cell(c2,'wall s')} & {cell(c2,'peak MB',1,'{:.0f}')} & {cell(c1,'wall s')} & {cell(c1,'peak MB',1,'{:.0f}')} & {cell(c3,'wall s')} & {off_mb} \\\\")
    csvrows.append([k[0], k[1], k[2], k[3], k[4], gi, gs, (c2 or {}).get("sweeps", ""), (c2 or {}).get("spec graphs explored", ""), (c2 or {}).get("wall s", "") if c2 else "", (c2 or {}).get("peak MB", "") if c2 else "", (c1 or {}).get("wall s", "") if c1 else "", (c1 or {}).get("peak MB", "") if c1 else "", (c3 or {}).get("wall s", "") if c3 else "", off_mb])
L += ["\\bottomrule", "\\end{longtable}"]
open(f"{T}/standalone-conformance.tex", "w").write("\n".join(L) + "\n")
with open(f"{T}/standalone-conformance.csv", "w", newline="") as fh: csv.writer(fh).writerows(csvrows)
try:
    import openpyxl
    wb = openpyxl.Workbook(); ws = wb.active; ws.title = "standalone"
    for row in csvrows: ws.append([(int(c) if isinstance(c, str) and c.isdigit() else (float(c) if isinstance(c, str) and re.fullmatch(r"-?\d+\.\d+", c) else c)) for c in row])
    ws.freeze_panes = "A2"; wb.save(f"{T}/standalone-conformance.xlsx")
except ImportError: pass
print(f"instances {len({k[:4] for k in order})}, rows {len(order)}; graph-count inconsistencies: {len(incons)}")
for k, what, d in incons[:12]: print("  ", k, what, d)
