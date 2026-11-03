#!/usr/bin/env python3
"""The paper's evaluation tables (owner's design, 2026-11-02 evening): every table has the same shape, one block
per instance (program, variant, model), one row per size, the implementation's and the specification's graph
counts, for violating pairs the uncovered graphs G_x, then per engine the time T and the memory M.

  standalone-<group>-<conf|viol>   TraceForge, Rust specification against Rust implementation:
                                   stateless (T, M), stateful (T, M), offline (T, M)
  cross-<group>-<conf|viol>        the specification in one language, the implementation in the other, one row per
                                   implementation language: offline (T, M), online (T, M)
  twins-<set>-<conf|viol>          the same program in both languages, one twin explored as the implementation, the
                                   other as the specification, one row per twin explored: offline (T, M), online (T, M)
                                   sets: reviewed (the hand-written twins), contextfree (Java ports written by a
                                   context-free agent from the Rust program), paper (both ports written by context-free
                                   agents from the paper)
Groups: synthetic, application, realsw. Paper mode keeps per instance the two largest sizes that finished and every
size that reached a limit; --full keeps every size. --keep F restricts to the programs (or program + variant) listed
in F; --drop F leaves them out (the dropped-benchmarks document is --keep, the paper --drop). Reads the campaign CSVs
written by tables.py. usage: paper-tables.py <tables-dir> [--full] [--keep F | --drop F] [--out D]"""
import sys, csv, re, collections, os
T = sys.argv[1]; FULL = "--full" in sys.argv; SUF = "-full" if FULL else ""
OUT = sys.argv[sys.argv.index("--out") + 1] if "--out" in sys.argv else T
def listed(flag):
    if flag not in sys.argv: return None
    return {tuple(l.split()) for l in open(sys.argv[sys.argv.index(flag) + 1]) if l.strip() and not l.startswith("#")}
KEEP, DROP = listed("--keep"), listed("--drop")
def kept(prog, var):
    hit = (prog,) in (KEEP or DROP or set()) or (prog, var) in (KEEP or DROP or set())
    return hit if KEEP is not None else (not hit if DROP is not None else True)
DV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
def load(name):
    try: return list(csv.DictReader(open(f"{T}/{name}.csv")))
    except OSError: return []
SYN = {"naive", "share", "commit", "chain", "reset", "width"}; REAL = {"raft", "omnipaxos"}
P2T = {"caspaxos", "chain-replication", "hermes", "flexible-paxos", "ricart-agrawala", "vrr", "ben-or"}
def group(p): return "synthetic" if p in SYN else ("realsw" if p in REAL else "application")
GROUP_TXT = {"synthetic": "the synthetic families", "application": "the application benchmarks", "realsw": "the real-software case studies"}
SET_TXT = {"reviewed": "the hand-written twins", "contextfree": "the context-free Java ports (an agent given the Rust program and the Java API)",
           "paper": "the paper-to-twin protocols (both ports written by context-free agents from the paper)"}
# the variant word of each program's correct program (paper-names.tsv: label "correct"), so that a specification case
# named after it is the program's own specification and needs no suffix
CORRECT = collections.defaultdict(set)
try:
    for l in open(os.path.join(DV, "cluster", "campaign", "inputs", "paper-names.tsv")):
        if l.startswith("#") or not l.strip(): continue
        d, rx, prog, lab = l.rstrip("\n").split("\t")[:4]
        if lab == "correct": CORRECT[prog].add(rx.strip("^$"))
except OSError: pass
def num(x):
    try: return float(x)
    except (TypeError, ValueError): return None
def sz(s): return [int(x) for x in re.findall(r"\d+", s)]
def esc(s): return s.replace("_", "\\_")
def spec_norm(prog, var, spec):   # the specification word, empty when it is the program's own specification
    s = (spec or "").strip()
    if s in ("", "spec", "correct", var) or s in CORRECT.get(prog, ()): return ""
    return s
def key_of(r): return (r["program"], r["variant"], spec_norm(r["program"], r["variant"], r.get("spec", "")), r["model"], r["size"])
def name(k):
    prog, var, spec, model, _ = k
    sub = var if var not in ("", "spec", "impl", "correct") else ""
    if spec: sub = (sub + "/" if sub else "") + spec
    return f"\\textsf{{{esc(prog)}}}" + (f"$_{{\\mathit{{{esc(sub)}}}}}^{{\\mathit{{{model}}}}}$" if sub else f"$^{{\\mathit{{{model}}}}}$")
def decided(r): return r is not None and r["verdict"] in ("conforms", "violation")
def limit(r): return r is not None and r["verdict"] in ("t/o", "oom")
TIER = {600: "10m", 3600: "1h", 14400: "4h", 86400: "24h", 172800: "48h"}
def tier(r):
    t = num(r.get("timeout s")); return TIER.get(int(t), f"{int(t)}s") if t else ""
def cell_T(r):
    if r is None or not (decided(r) or limit(r)): return ""
    if r["verdict"] == "t/o": return "\\faClock" + (f"$_{{\\mathrm{{{tier(r)}}}}}$" if tier(r) else "")
    if r["verdict"] == "oom":
        g = num(r.get("mem GB")); return "oom" + (f"$_{{\\mathrm{{{int(g)}G}}}}$" if g and g < 250 else "")
    v = num(r.get("wall s")); return "" if v is None else f"{v:.1f}"
def cell_M(r):
    if not decided(r): return ""
    v = num(r.get("peak MB")); return "" if v is None else f"{v:.0f}"
def cell_M_off(r): return cell_M(r)   # offline: the peak of the two exports and the join (tables.py composes it); the summary files' sizes are in the CSV
def files_mb(r):
    if r is None: return ""
    b = [num(r.get("spec export bytes")), num(r.get("impl export bytes"))]
    return f"{sum(b) / 1048576:.1f}" if all(x is not None for x in b) else ""
def exported(r): return r is not None and bool(r.get("spec export s")) and bool(r.get("impl export s"))   # both exports finished: the counts are complete even if the join did not finish
# ---- the table families -------------------------------------------------------------------------------------
# standalone: three engines, one row per (instance, size)
# cross/twins: two engines, one row per (instance, size, side); side = the implementation language (cross) or the twin explored (twins)
FAMS = []
FAMS.append(("standalone", None, [("stateless", "campaign-stateless"), ("stateful", "campaign-stateful"), ("offline", "campaign-offline-tf")]))
FAMS.append(("cross", None, [("offline", "Java", "campaign-offline-rust-spec"), ("online", "Java", "campaign-online-java-impl"),
                             ("offline", "Rust", "campaign-offline-java-spec"), ("online", "Rust", "campaign-online-rust-impl")]))
FAMS.append(("twins", "reviewed", [("offline", "Rust", "campaign-twin-offline-java-spec"), ("online", "Rust", "campaign-twin-online-rust-impl"),
                                    ("offline", "Java", "campaign-twin-offline-rust-spec"), ("online", "Java", "campaign-twin-online-java-impl")]))
for st in ("contextfree", "paper"):
    FAMS.append(("twins", st, [("offline", "Rust", "campaign-cftwin-offline-java-spec"), ("online", "Rust", "campaign-cftwin-online-rust-impl"),
                               ("offline", "Java", "campaign-cftwin-offline-rust-spec"), ("online", "Java", "campaign-cftwin-online-java-impl")]))
try:
    import openpyxl
    wb = openpyxl.Workbook(); wb.remove(wb.active)
except ImportError: wb = None
summary = []; incons = []
def select(keys, inst, primary):   # paper mode: per instance the two largest sizes that finished and every size that reached a limit
    if FULL: return keys
    out = []; byinst = collections.OrderedDict()
    for k in keys: byinst.setdefault(k[:4], []).append(k)
    for ik, ks in byinst.items():
        tout = [k for k in ks if any(limit(r) for r in inst[k].values())]
        fin = [k for k in ks if k not in tout and (decided(inst[k].get(primary)) if primary else any(decided(r) for r in inst[k].values()))]
        out += [k for k in ks if k in fin[-2:] or k in tout]
    return out
def side_of(m):   # a block's side: the decided verdict, else the expectation
    for r in m.values():
        if decided(r): return r["verdict"]
    return "violation" if any(r["expected"] == "violation" for r in m.values()) else "conforms"
def emit(fn, label, cap, colspec, hdr1, cm, hdr2, body, csvhdr, csvrows):
    L = [f"\\begin{{longtable}}{{{colspec}}}", f"\\caption{{{cap}}}\\label{{{label}}}\\\\", "\\toprule", hdr1, cm, hdr2, "\\midrule", "\\endfirsthead",
         "\\toprule", hdr1, cm, hdr2, "\\midrule", "\\endhead"] + body + ["\\bottomrule", "\\end{longtable}"]
    open(f"{OUT}/{fn}.tex", "w").write("\n".join(L) + "\n")
    with open(f"{OUT}/{fn}.csv", "w", newline="") as fh: csv.writer(fh).writerows([csvhdr] + csvrows)
    if wb is not None:
        ws = wb.create_sheet(fn[:31])
        for row in [csvhdr] + csvrows: ws.append([(int(c) if isinstance(c, str) and c.isdigit() else (float(c) if isinstance(c, str) and re.fullmatch(r"-?\d+\.\d+", c) else c)) for c in row])
        ws.freeze_panes = "A2"
COMMON = ("Instance = program$_{\\mathit{variant}}^{\\mathit{model}}$ (no subscript for the program's correct variant; a second word names the specification variant when the program has several). "
          "$G_{\\mathit{impl}}$, $G_{\\mathit{spec}}$ = the graphs of the implementation and of the specification (the plain exploration counts; they agree across the engines)")
GX = ", $G_x$ = the implementation graphs that no specification graph covers"
LIMITS = (" T = wall time in seconds, M = peak resident memory in MB; an offline row counts the two exports and the join together (T summed, M the largest of the three). "
          "Every run at 16 threads. \\faClock\\ = the row's time tier was reached (subscript: the tier), oom = the row's memory limit was reached (subscript: the limit when below 250 GB); "
          "a blank cell = not run, because a smaller size had reached a limit.")
for fam, tset, modes in FAMS:
    if fam == "standalone":
        data = {m: load(c) for m, c in modes}
        inst = collections.OrderedDict()
        for m, rs in data.items():
            for r in rs: inst.setdefault(key_of(r), {})[m] = r
        for k, m in inst.items():   # the graph counts must agree across the engines (stateless reports the spec graphs explored, not the count)
            gi = {md: r["impl graphs"] for md, r in m.items() if decided(r) and r["impl graphs"]}
            gs = {md: r["spec graphs"] for md, r in m.items() if decided(r) and r["spec graphs"] and md != "stateless"}
            if len(set(gi.values())) > 1: incons.append((fam, k, "impl", gi))
            if len(set(gs.values())) > 1: incons.append((fam, k, "spec", gs))
        for g in ("synthetic", "application", "realsw"):
            for kind in ("conf", "viol"):
                viol = kind == "viol"
                keys = [k for k, m in inst.items() if kept(k[0], k[1]) and group(k[0]) == g and side_of(m) == ("violation" if viol else "conforms")]
                keys.sort(key=lambda k: (k[0], k[1], k[2], k[3], sz(k[4])))
                keys = select(keys, inst, "stateful")
                if not keys: continue
                body = []; csvrows = []; prev = None
                for k in keys:
                    m = inst[k]; c2, c1, c3 = m.get("stateless"), m.get("stateful"), m.get("offline")
                    dec = [r for r in (c1, c3, c2) if decided(r)] + ([c3] if exported(c3) and not decided(c3) else [])
                    gi = next((r["impl graphs"] for r in dec if r["impl graphs"]), ""); gs = next((r["spec graphs"] for r in (c1, c3) if (decided(r) or exported(r)) and r["spec graphs"]), "")
                    gx = next((r.get("uncovered", "") for r in dec if r.get("uncovered") not in ("", None)), "") if viol else None
                    nm = name(k) if k[:4] != prev else ""; prev = k[:4]
                    if nm and body and not body[-1].startswith("\\midrule"): body.append("\\midrule")
                    cells = [cell_T(c2), cell_M(c2), cell_T(c1), cell_M(c1), cell_T(c3), cell_M_off(c3)]
                    body.append(f"{nm} & {k[4]} & {gi} & {gs}" + (f" & {gx}" if viol else "") + " & " + " & ".join(cells) + " \\\\")
                    csvrows.append(list(k[:4]) + [k[4], gi, gs] + ([gx] if viol else []) + [c.replace("\\faClock", "t/o") for c in cells] + [files_mb(c3)])
                base = 5 if viol else 4
                cap = (f"TraceForge standalone refinement checking, Rust specification against Rust implementation: the {'violating' if viol else 'conforming'} pairs of {GROUP_TXT[g]}"
                       + (", every size run" if FULL else ", the two largest sizes that finished and the sizes that reached a limit") + ". " + COMMON + (GX if viol else "") + "." + LIMITS)
                hdr1 = f"instance & size & $G_{{\\mathit{{impl}}}}$ & $G_{{\\mathit{{spec}}}}${' & $G_x$' if viol else ''} & \\multicolumn{{2}}{{c}}{{stateless}} & \\multicolumn{{2}}{{c}}{{stateful}} & \\multicolumn{{2}}{{c}}{{offline}} \\\\"
                cm = "".join(f"\\cmidrule(lr){{{base + 1 + 2 * i}-{base + 2 + 2 * i}}}" for i in range(3))
                hdr2 = " & " * (base - 1) + " & T & M & T & M & T & M \\\\"
                fn = f"standalone-{g}-{kind}{SUF}"
                emit(fn, f"tab:{fn}", cap, "@{}lrrr" + ("r" if viol else "") + "rrrrrr@{}", hdr1, cm, hdr2, body,
                     ["program", "variant", "spec", "model", "size", "G impl", "G spec"] + (["G x"] if viol else []) + ["stateless s", "stateless MB", "stateful s", "stateful MB", "offline s", "offline MB", "summary files MB"], csvrows)
                summary.append(f"{fn}: {len({k[:4] for k in keys})} instances, {len(keys)} rows")
        continue
    # cross and twins: rows keyed by (instance, size, side)
    data = {(m, sd): load(c) for m, sd, c in modes}
    inst = collections.OrderedDict()   # (instance, size) -> side -> mode -> row
    for (m, sd), rs in data.items():
        for r in rs:
            if fam == "twins" and tset in ("contextfree", "paper") and (r["program"] in P2T) != (tset == "paper"): continue
            inst.setdefault(key_of(r), {}).setdefault(sd, {})[m] = r
    for k, sides in inst.items():
        for sd, m in sides.items():
            gi = {md: r["impl graphs"] for md, r in m.items() if decided(r) and r["impl graphs"]}
            gs = {md: r["spec graphs"] for md, r in m.items() if decided(r) and r["spec graphs"]}
            if len(set(gi.values())) > 1: incons.append((fam, k + (sd,), "impl", gi))
            if len(set(gs.values())) > 1: incons.append((fam, k + (sd,), "spec", gs))
    flat = {k: {f"{sd}/{md}": r for sd, m in sides.items() for md, r in m.items()} for k, sides in inst.items()}
    groups = [("synthetic",), ("application",), ("realsw",)] if fam == "cross" else [(None,)]
    for (g,) in groups:
        for kind in ("conf", "viol"):
            viol = kind == "viol"
            keys = [k for k, m in flat.items() if kept(k[0], k[1]) and (g is None or group(k[0]) == g) and side_of(m) == ("violation" if viol else "conforms")]
            keys.sort(key=lambda k: (k[0], k[1], k[2], k[3], sz(k[4])))
            keys = select(keys, flat, None)
            if not keys: continue
            body = []; csvrows = []; prev = None
            for k in keys:
                sides = inst[k]; first = True
                for sd in ("Java", "Rust"):
                    m = sides.get(sd)
                    if not m: continue
                    a, b = m.get("offline"), m.get("online")
                    dec = [r for r in (a, b) if decided(r)] + ([a] if exported(a) and not decided(a) else [])
                    gi = next((r["impl graphs"] for r in dec if r["impl graphs"]), ""); gs = next((r["spec graphs"] for r in dec if r["spec graphs"]), "")
                    gx = next((r.get("uncovered", "") for r in dec if r.get("uncovered") not in ("", None)), "") if viol else None
                    nm = name(k) if k[:4] != prev else ""; prev = k[:4]
                    if nm and body and not body[-1].startswith("\\midrule"): body.append("\\midrule")
                    cells = [cell_T(a), cell_M_off(a), cell_T(b), cell_M(b)]
                    body.append(f"{nm} & {k[4] if first else ''} & {sd} & {gi} & {gs}" + (f" & {gx}" if viol else "") + " & " + " & ".join(cells) + " \\\\")
                    csvrows.append(list(k[:4]) + [k[4], sd, gi, gs] + ([gx] if viol else []) + [c.replace("\\faClock", "t/o") for c in cells] + [files_mb(a)]); first = False
            base = 6 if viol else 5
            if fam == "cross":
                what = f"Cross-language refinement checking, the specification in one language and the implementation in the other: the {'violating' if viol else 'conforming'} pairs of {GROUP_TXT[g]}"
                sidetxt = "impl = the implementation's language (Java: JMC explores the Java implementation, the specification is TraceForge's Rust export; Rust: the reverse). Offline: the two exports and the join; online: the implementation explored against the other language's specification export."
                fn = f"cross-{g}-{kind}{SUF}"
            else:
                what = f"Cross-language refinement checking between twin implementations, {SET_TXT[tset]}: the {'violating' if viol else 'conforming'} pairs"
                sidetxt = "impl = the twin explored as the implementation (the other twin is the specification; a second word in the instance names the twin variant used as the specification when it is another variant). Offline: the two exports and the join; online: the twin explored against the other twin's export."
                fn = f"twins-{tset}-{kind}{SUF}"
            cap = what + (", every size run" if FULL else ", the two largest sizes that finished and the sizes that reached a limit") + ". " + COMMON + (GX if viol else "") + ". " + sidetxt + LIMITS
            hdr1 = f"instance & size & impl & $G_{{\\mathit{{impl}}}}$ & $G_{{\\mathit{{spec}}}}${' & $G_x$' if viol else ''} & \\multicolumn{{2}}{{c}}{{offline}} & \\multicolumn{{2}}{{c}}{{online}} \\\\"
            cm = "".join(f"\\cmidrule(lr){{{base + 1 + 2 * i}-{base + 2 + 2 * i}}}" for i in range(2))
            hdr2 = " & " * (base - 1) + " & T & M & T & M \\\\"
            emit(fn, f"tab:{fn}", cap, "@{}lrlrr" + ("r" if viol else "") + "rrrr@{}", hdr1, cm, hdr2, body,
                 ["program", "variant", "spec", "model", "size", "impl", "G impl", "G spec"] + (["G x"] if viol else []) + ["offline s", "offline MB", "online s", "online MB", "summary files MB"], csvrows)
            summary.append(f"{fn}: {len({k[:4] for k in keys})} instances, {len(csvrows)} rows")

# ---- Table 2 of the paper: the rows of inputs/table2.tsv, standalone columns, no model superscript (owner, 2026-11-03) ----
T2 = os.path.join(DV, "cluster", "campaign", "inputs", "table2.tsv")
if os.path.exists(T2) and not FULL and KEEP is None:
    want = [l.rstrip("\n").split("\t") for l in open(T2) if l.strip() and not l.startswith("#")]
    data = {m: load(c) for m, c in [("stateless", "campaign-stateless"), ("stateful", "campaign-stateful"), ("offline", "campaign-offline-tf")]}
    inst = collections.OrderedDict()
    for m, rs in data.items():
        for r in rs: inst.setdefault(key_of(r), {})[m] = r
    body = []; csvrows = []; prev = None
    for prog, var, model, size in want:
        k = next((kk for kk in inst if kk[0] == prog and kk[1] == var and kk[3] == model and kk[4] == size), None)
        m = inst.get(k, {}) if k else {}
        c2, c1, c3 = m.get("stateless"), m.get("stateful"), m.get("offline")
        dec = [r for r in (c1, c3, c2) if decided(r)] + ([c3] if exported(c3) and not decided(c3) else [])
        gi = next((r["impl graphs"] for r in dec if r["impl graphs"]), ""); gs = next((r["spec graphs"] for r in (c1, c3) if (decided(r) or exported(r)) and r["spec graphs"]), "")
        nm = f"\\textsf{{{esc(prog)}}}" + (f"$_{{\\mathit{{{esc(var)}}}}}$" if var not in ("", "impl", "correct") else "")
        nm = nm if (prog, var) != prev else ""; prev = (prog, var)
        if nm and body: body.append("\\midrule")
        cells = [cell_T(c2), cell_M(c2), cell_T(c1), cell_M(c1), cell_T(c3), cell_M_off(c3)]
        body.append(f"{nm} & {size} & {gi} & {gs} & " + " & ".join(cells) + " \\\\")
        csvrows.append([prog, var, model, size, gi, gs] + [c.replace("\\faClock", "t/o") for c in cells])
    cap = ("TraceForge standalone refinement checking on three synthetic families, Rust specification against Rust implementation: the stateless, stateful and offline engines side by side. " + COMMON + "." + LIMITS)
    hdr1 = "instance & size & $G_{\\mathit{impl}}$ & $G_{\\mathit{spec}}$ & \\multicolumn{2}{c}{stateless} & \\multicolumn{2}{c}{stateful} & \\multicolumn{2}{c}{offline} \\\\"
    emit("table2", "tab:table2", cap, "@{}lrrrrrrrrr@{}", hdr1, "\\cmidrule(lr){5-6}\\cmidrule(lr){7-8}\\cmidrule(lr){9-10}", " & & & & T & M & T & M & T & M \\\\", body,
         ["program", "variant", "model", "size", "G impl", "G spec", "stateless s", "stateless MB", "stateful s", "stateful MB", "offline s", "offline MB"], csvrows)
    summary.append(f"table2: {len(csvrows)} rows")
if wb is not None: wb.save(f"{OUT}/paper-tables{SUF}.xlsx")
if incons:
    print(f"{len(incons)} graph-count inconsistencies across the engines:")
    for fam, k, what, d in incons[:30]: print("  ", fam, k, what, d)
print("\n".join(summary))
