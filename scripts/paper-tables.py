#!/usr/bin/env python3
"""Paper tables in the standalone table's style (owner's design, 2026-11-02): one block per instance (program,
variant, spec, model), one row per size, the implementation's and specification's graph counts, for violating
pairs the uncovered graphs G_x, then per engine the time and the peak memory.
Three families of tables x three benchmark groups x {conforming, violating}:
  standalone     TraceForge, Rust spec / Rust impl: stateless (sweeps, G_spec^expl, T, M), stateful (T, M), offline (T, M)
  cross-validity a specification in one language, an implementation in the other: offline Java spec / Rust impl (XJ2),
                 offline Rust spec / Java impl (XJ1), online Rust impl against the Java spec export (XON2), online Java impl (JMC) against the Rust spec export (XON1)
  cross-twins    the same implementation in both languages, one twin as the specification: offline (XT1: Java as spec, XT2: Rust as spec),
                 online (XTON1: JMC, Rust twin export; XTON2: TraceForge, Java twin export)
Groups: synthetic (naive, share, commit, chain, reset, width), realsw (raft, omnipaxos), application (the rest).
Paper mode keeps per instance the two largest sizes the primary engine finished and every size that reached a limit;
--full keeps every size. Reads the campaign CSVs written by tables.py. usage: paper-tables.py <tables-dir> [--full]"""
import sys, csv, re, collections, os
T = sys.argv[1]; FULL = "--full" in sys.argv; SUF = "-full" if FULL else ""
# --keep <file>: only the listed "program" or "program variant" lines (one per line, # comments); --out <dir>: write there instead of the tables dir
KEEP = None; OUT = T
if "--keep" in sys.argv:
    KEEP = {tuple(l.split()) for l in open(sys.argv[sys.argv.index("--keep") + 1]) if l.strip() and not l.startswith("#")}
if "--out" in sys.argv: OUT = sys.argv[sys.argv.index("--out") + 1]
def kept(k): return KEEP is None or (k[0],) in KEEP or (k[0], k[1]) in KEEP
def load(name):
    try: return list(csv.DictReader(open(f"{T}/{name}.csv")))
    except OSError: return []
SYN = {"naive", "share", "commit", "chain", "reset", "width"}; REAL = {"raft", "omnipaxos"}
def group(p): return "synthetic" if p in SYN else ("realsw" if p in REAL else "application")
GROUP_TXT = {"synthetic": "the synthetic families", "application": "the application benchmarks", "realsw": "the real-software case studies"}
FAM = {  # family -> (title, [(mode key, csv, column header pair)], primary mode)
 "standalone": ("TraceForge standalone", [("stateless", "campaign-stateless"), ("stateful", "campaign-stateful"), ("offline", "campaign-offline-tf")], "stateful"),
 "cross-validity": ("Cross-language refinement, specification and implementation in different languages",
                    [("offJR", "campaign-offline-java-spec"), ("offRJ", "campaign-offline-rust-spec"), ("onR", "campaign-online-rust-impl"), ("onJ", "campaign-online-java-impl")], None),
 "cross-twins": ("Cross-language refinement between twin implementations",
                 [("offJR", "campaign-twin-offline-java-spec"), ("offRJ", "campaign-twin-offline-rust-spec"), ("onJ", "campaign-twin-online-java-impl"), ("onR", "campaign-twin-online-rust-impl")], None),
 "cross-cftwins": ("Cross-language refinement between a Rust implementation and its context-free Java port",
                 [("offJR", "campaign-cftwin-offline-java-spec"), ("offRJ", "campaign-cftwin-offline-rust-spec"), ("onJ", "campaign-cftwin-online-java-impl"), ("onR", "campaign-cftwin-online-rust-impl")], None)}
def num(x):
    try: return float(x)
    except (TypeError, ValueError): return None
def sz(s): return [int(x) for x in re.findall(r"\d+", s)]
def esc(s): return s.replace("_", "\\_")
def name(k):
    prog, var, spec, model, _ = k
    sub = var if var not in ("", "spec", "impl", "correct") else ""
    if spec and spec not in var and spec not in ("spec", "correct"): sub = (sub + "/" if sub else "") + spec
    return f"\\textsf{{{esc(prog)}}}" + (f"$_{{\\mathit{{{esc(sub)}}}}}^{{\\mathit{{{model}}}}}$" if sub else f"$^{{\\mathit{{{model}}}}}$")
def decided(r): return r["verdict"] in ("conforms", "violation")
def limit(r): return r["verdict"] in ("t/o", "oom")
def cell(r, col, div=1.0, fmt="{:.1f}"):
    if r is None: return ""
    if r["verdict"] == "t/o": return "\\faClock" if col == "wall s" else ""
    if r["verdict"] == "oom": return "oom" if col == "wall s" else ""
    if not decided(r): return ""
    v = num(r.get(col)); return "" if v is None else fmt.format(v / div)
def off_mb(r):
    if r is None or not decided(r): return ""
    b = [num(r.get("spec export bytes")), num(r.get("impl export bytes"))]
    return "{:.1f}".format(sum(b) / 1048576) if all(x is not None for x in b) else ""
import openpyxl
wb = openpyxl.Workbook(); wb.remove(wb.active)
summary = []
for fam, (title, modes, primary) in FAM.items():
    data = {m: load(c) for m, c in modes}
    inst = collections.OrderedDict()
    for m, rs in data.items():
        for r in rs:
            inst.setdefault((r["program"], r["variant"], r["spec"], r["model"], r["size"]), {})[m] = r
    # a row's side: its verdict; a row without one (limit reached): its expectation
    def side(m):
        for r in m.values():
            if decided(r): return r["verdict"]
        return "violation" if any(r["expected"] == "violation" for r in m.values()) else "conforms"
    # consistency: the implementation's count and the specification's count must agree across the modes (stateless reports the spec graphs explored, not the count)
    incons = []
    for k, m in inst.items():
        gi = {md: r["impl graphs"] for md, r in m.items() if decided(r) and r["impl graphs"]}
        gs = {md: r["spec graphs"] for md, r in m.items() if decided(r) and r["spec graphs"] and md != "stateless"}
        if len(set(gi.values())) > 1: incons.append((k, "impl", gi))
        if len(set(gs.values())) > 1: incons.append((k, "spec", gs))
    for g in ("synthetic", "application", "realsw"):
        for kind in ("conf", "viol"):
            keys = [k for k, m in inst.items() if kept(k) and group(k[0]) == g and side(m) == ("violation" if kind == "viol" else "conforms")]
            keys.sort(key=lambda k: (k[0], k[1], k[2], k[3], sz(k[4])))
            if not FULL:
                keep = []; byinst = collections.OrderedDict()
                for k in keys: byinst.setdefault(k[:4], []).append(k)
                for ik, ks in byinst.items():
                    tout = [k for k in ks if any(limit(r) for r in inst[k].values())]
                    fin = [k for k in ks if k not in tout and (decided(inst[k][primary]) if primary and primary in inst[k] else any(decided(r) for r in inst[k].values()))]
                    keep += [k for k in ks if k in fin[-2:] or k in tout]
                keys = keep
            if not keys: continue
            viol = kind == "viol"
            # header
            if fam == "standalone":
                eng = "\\multicolumn{4}{c}{stateless} & \\multicolumn{2}{c}{stateful} & \\multicolumn{2}{c}{offline}"; sub = "sweeps & $G_{\\mathit{spec}}^{\\mathit{expl}}$ & T & M & T & M & T & M"; ncol = 8
                engcsv = ["stateless sweeps", "stateless spec graphs explored", "stateless s", "stateless MB", "stateful s", "stateful MB", "offline s", "offline MB (both export files)"]
            elif fam == "cross-validity":
                eng = "\\multicolumn{2}{c}{offline J$\\to$R} & \\multicolumn{2}{c}{offline R$\\to$J} & \\multicolumn{2}{c}{online R} & \\multicolumn{2}{c}{online J}"; sub = "T & M & T & M & T & M & T & M"; ncol = 8
                engcsv = ["offline Java-spec Rust-impl s", "offline Java-spec Rust-impl MB", "offline Rust-spec Java-impl s", "offline Rust-spec Java-impl MB", "online Rust-impl s", "online Rust-impl MB", "online Java-impl s", "online Java-impl MB"]
            else:
                eng = "\\multicolumn{2}{c}{offline J$\\to$R} & \\multicolumn{2}{c}{offline R$\\to$J} & \\multicolumn{2}{c}{online J} & \\multicolumn{2}{c}{online R}"; sub = "T & M & T & M & T & M & T & M"; ncol = 8
                engcsv = ["offline Java-twin-spec Rust-impl s", "offline Java-twin-spec Rust-impl MB", "offline Rust-twin-spec Java-impl s", "offline Rust-twin-spec Java-impl MB", "online Java-impl s", "online Java-impl MB", "online Rust-impl s", "online Rust-impl MB"]
            gx = " & $G_x$" if viol else ""; gxc = "r" if viol else ""
            base = 4 + (1 if viol else 0)
            cm = f"\\cmidrule(lr){{{base+1}-{base+4}}}\\cmidrule(lr){{{base+5}-{base+6}}}\\cmidrule(lr){{{base+7}-{base+8}}}" if fam == "standalone" else "".join(f"\\cmidrule(lr){{{base+1+2*i}-{base+2+2*i}}}" for i in range(4))
            label = f"tab:{fam}-{g}-{kind}{SUF}"
            if fam == "standalone": engtxt = "Stateless: sweeps kept in the witness cache, $G_{\\mathit{spec}}^{\\mathit{expl}}$ = specification graphs explored in total (repeats counted). Offline: T = the two exports and the join, M = the size of the two summary files kept."
            elif fam == "cross-validity": engtxt = "Offline J$\\to$R: Java specification export, Rust implementation export, join (T = all three, M = the two summary files); R$\\to$J: the reverse. Online R: TraceForge explores the Rust implementation against the Java specification export; online J: JMC explores the Java implementation against the Rust specification export."
            elif fam == "cross-cftwins": engtxt = "The Rust implementation and a Java port of it written by an agent given only the Rust program and the Java API (no review), checked against each other; the spec subscript names the twin used as the specification when it is another variant. Columns as in the twin table."
            else: engtxt = "The two implementations of one program, one in Java and one in Rust, checked against each other; the spec subscript names the twin used as the specification when it is another variant. Offline J$\\to$R: the Java twin's export as the specification, the Rust twin's export as the implementation (T = the two exports and the join, M = the two summary files); R$\\to$J: the reverse. Online J: JMC explores the Java twin against the Rust twin's export; online R: TraceForge explores the Rust twin against the Java twin's export."
            cap = (f"{title}: the {'violating' if viol else 'conforming'} pairs of {GROUP_TXT[g]}" + (", every size run" if FULL else ", the two largest sizes that finished and the sizes that reached a limit") +
                   ". Instance = program$_{\\mathit{variant}}^{\\mathit{model}}$ (no subscript for the family's correct program). $G_{\\mathit{impl}}$, $G_{\\mathit{spec}}$ = the graphs of the implementation and of the specification (plain Must counts, equal across the engines)" +
                   (", $G_x$ = the implementation graphs no specification graph covers" if viol else "") + ". " + engtxt + " T = wall time in seconds, M = peak resident memory in MB. Every run at 16 threads with a time limit of 3600 s (\\faClock\\ = reached; a blank cell = not run, because a smaller size had reached the limit) and a memory limit of 250 GB (oom = reached).")
            hdr1 = f"instance & size & $G_{{\\mathit{{impl}}}}$ & $G_{{\\mathit{{spec}}}}${gx} & {eng} \\\\"; hdr2 = f" & & & {'& ' if viol else ''}& {sub} \\\\"
            L = [f"\\begin{{longtable}}{{@{{}}lrrr{gxc}{'r'*ncol}@{{}}}}", f"\\caption{{{cap}}}\\label{{{label}}}\\\\", "\\toprule", hdr1, cm, hdr2, "\\midrule", "\\endfirsthead", "\\toprule", hdr1, cm, hdr2, "\\midrule", "\\endhead"]
            csvrows = [["program", "variant", "spec", "model", "size", "G impl", "G spec"] + (["G x"] if viol else []) + engcsv]
            prev = None
            for k in keys:
                m = inst[k]; rs = [m.get(md) for md, _ in modes]
                dec = [r for r in rs if r and decided(r)]
                gi = next((r["impl graphs"] for r in dec if r["impl graphs"]), ""); gs = next((r["spec graphs"] for r in dec if r["spec graphs"] and r is not m.get("stateless")), "")
                gxv = next((r.get("uncovered", "") for r in dec if r.get("uncovered") not in ("", None)), "") if viol else None
                nm = name(k) if k[:4] != prev else ""; prev = k[:4]
                if nm and L[-1] != "\\endhead" and not L[-1].startswith("\\midrule"): L.append("\\midrule")
                if fam == "standalone":
                    c2, c1, c3 = rs
                    cells = [(c2 or {}).get("sweeps", "") if c2 and decided(c2) else "", (c2 or {}).get("spec graphs explored", "") if c2 and decided(c2) else "", cell(c2, "wall s"), cell(c2, "peak MB", 1, "{:.0f}"), cell(c1, "wall s"), cell(c1, "peak MB", 1, "{:.0f}"), cell(c3, "wall s"), off_mb(c3)]
                else:
                    a, b, c, d = rs
                    cells = [cell(a, "wall s"), off_mb(a), cell(b, "wall s"), off_mb(b), cell(c, "wall s"), cell(c, "peak MB", 1, "{:.0f}"), cell(d, "wall s"), cell(d, "peak MB", 1, "{:.0f}")]
                L.append(f"{nm} & {k[4]} & {gi} & {gs}" + (f" & {gxv}" if viol else "") + " & " + " & ".join(cells) + " \\\\")
                csvrows.append(list(k[:4]) + [k[4], gi, gs] + ([gxv] if viol else []) + [c.replace("\\faClock", "t/o") for c in cells])
            L += ["\\bottomrule", "\\end{longtable}"]
            fn = f"{fam}-{g}-{kind}{SUF}"
            open(f"{OUT}/{fn}.tex", "w").write("\n".join(L) + "\n")
            with open(f"{OUT}/{fn}.csv", "w", newline="") as fh: csv.writer(fh).writerows(csvrows)
            ws = wb.create_sheet(fn[:31])
            for row in csvrows: ws.append([(int(c) if isinstance(c, str) and c.isdigit() else (float(c) if isinstance(c, str) and re.fullmatch(r"-?\d+\.\d+", c) else c)) for c in row])
            ws.freeze_panes = "A2"
            summary.append(f"{fn}: {len({k[:4] for k in keys})} instances, {len(keys)} rows")
    if incons:
        print(f"{fam}: {len(incons)} graph-count inconsistencies across the engines:")
        for k, what, d in incons[:20]: print("  ", k, what, d)
wb.save(f"{OUT}/paper-tables{SUF}.xlsx")
print("\n".join(summary))
