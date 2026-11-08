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
FALLBACK = "--fallback" in sys.argv   # owner 2026-11-05 09:40: an empty curated cell shows the newest older-pin run (orange/red), a never-run size shows the next smaller size that ran (dagger)
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
    if s in ("", "spec", var): return ""   # Opus audit 11-06: "correct" is a specification of its own (a faulty variant against the correct program), never merged with the twin check
    return s
def key_of(r): return (r["program"], r["variant"] + ("@spec" if ".spec_" in r.get("run id", "") else ""), spec_norm(r["program"], r["variant"], r.get("spec", "")), r["model"], r["size"])   # a check of the specification side (".spec_" in the id) is its own instance (Opus audit 11-06)
def pref(r):   # among runs of one cell: the reserved machine (.p), then an uncontended run, then the latest start (Opus audit 11-06: 2pc-ds(4))
    import re as _re
    return (bool(_re.search(r"\.p(?=\.|$)", r.get("run id", ""))), r.get("contended") != "yes", r.get("started") or "")
def put(inst, k, m, r):
    cur = inst.setdefault(k, {}).get(m)
    if cur is None or pref(r) >= pref(cur): inst[k][m] = r
def name(k):
    prog, var, spec, model, _ = k
    side = var.endswith("@spec"); var = var[:-5] if side else var
    sub = var if var not in ("", "spec", "impl", "correct") else ""
    if side: sub = (sub + "/" if sub else "") + "spec side"
    if spec: sub = (sub + "/" if sub else "") + spec
    return f"\\textsf{{{esc(prog)}}}" + (f"$_{{\\mathit{{{esc(sub)}}}}}^{{\\mathit{{{model}}}}}$" if sub else f"$^{{\\mathit{{{model}}}}}$")
def decided(r): return r is not None and r["verdict"] in ("conforms", "violation")
def limit(r): return r is not None and r["verdict"] in ("t/o", "oom")
TIER = {600: "10m", 3600: "1h", 14400: "4h", 86400: "24h", 172800: "48h"}
def tier(r):
    t = num(r.get("timeout s")); return TIER.get(int(t), f"{int(t)}s") if t else ""
def rnd(v, nd):   # decimal rounding (half up) from the value's text, so 5.55 s prints as 5.6 and not as 5.5 (binary 5.55 is below the half; audit 1108-1648)
    from decimal import Decimal, ROUND_HALF_UP
    return str(Decimal(repr(v)).quantize(Decimal(1).scaleb(-nd), rounding=ROUND_HALF_UP))
def cell_T(r):
    if r is None or not (decided(r) or limit(r)): return ""
    if r["verdict"] == "t/o": return "\\faClock" + (f"$_{{\\mathrm{{{tier(r)}}}}}$" if tier(r) else "")
    if r["verdict"] == "oom":
        g = num(r.get("mem GB")); return "oom" + (f"$_{{\\mathrm{{{int(g)}G}}}}$" if g and g < 250 else "")
    v = num(r.get("wall s")); return "" if v is None else rnd(v, 1)
def cell_T1(r, online):   # the first violation (owner 2026-11-06): Nested's first report counts from the run's start; Online's from the
    # implementation phase, so the specification phase, which must finish before any violation can be known, is added (sverify_p.rs impl_started)
    if r is None or r.get("verdict") != "violation": return ""
    fm = num(r.get("first miss ms"))
    if fm is None: return ""
    if online:
        sp = num(r.get("spec phase s"))
        if sp is None: return ""   # a measured zero is a valid short phase (Astra, 11-06)
        v = sp + fm / 1000
    else: v = fm / 1000
    return rnd(v, 3) if v < 0.1 else (rnd(v, 2) if v < 10 else rnd(v, 1))   # small first-violation times at ms precision, so a short specification phase is visible
def cell_Tp(r):   # T below 0.1 s to the millisecond (Table 3, beside T_1)
    c = cell_T(r); ms = num((r or {}).get("wall ms")); v = ms / 1000 if ms is not None else num((r or {}).get("wall s"))   # the exact wall, not the CSV's 0.1 s rounding (Opus audit 11-06)
    return f"{v:.3f}" if (c and v is not None and not c.startswith("\\") and not c.startswith("oom") and v < 0.1) else c
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
          "$G_{\\mathit{cand}}$, $G_{\\mathit{ref}}$ = the graphs of the candidate and of the reference (the plain exploration counts; they agree across the engines)")
GX = ", $G_x$ = the implementation graphs that no specification graph covers"
LIMITS = (" T = wall time in seconds, M = peak resident memory in MiB; an offline row runs its two exports as independent jobs side by side: T = the longer export plus the join, M = the sum of the two exports' peaks (both are resident at once), or the join's peak if larger; an online row against the other language's export: T = that export plus the online run (the specification must be enumerated for the check), M = the larger of the export's peak and the run's; an in-process online row: T = the run, which enumerates the specification itself. "
          "Every run at 16 threads. \\faClock\\ = the row's time tier was reached (subscript: the tier), oom = the row's memory limit was reached (subscript: the limit when below 250 GB); "
          "a blank cell = not run, because a smaller size had reached a limit.")
for fam, tset, modes in FAMS:
    if fam == "standalone":
        data = {m: load(c) for m, c in modes}
        inst = collections.OrderedDict()
        for m, rs in data.items():
            for r in rs: put(inst, key_of(r), m, r)
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
                body = []; csvrows = []; prev = None; EXTRA = []; DONE_EXTRA = set()
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
                hdr1 = f"instance & size & $G_{{\\mathit{{cand}}}}$ & $G_{{\\mathit{{ref}}}}${' & $G_x$' if viol else ''} & \\multicolumn{{2}}{{c}}{{\\nested}} & \\multicolumn{{2}}{{c}}{{\\online}} & \\multicolumn{{2}}{{c}}{{\\offline}} \\\\"
                cm = "".join(f"\\cmidrule(lr){{{base + 1 + 2 * i}-{base + 2 + 2 * i}}}" for i in range(3))
                hdr2 = " & " * (base - 1) + " & T & M & T & M & T & M \\\\"
                fn = f"standalone-{g}-{kind}{SUF}"
                emit(fn, f"tab:{fn}", cap, "@{}lrrr" + ("r" if viol else "") + "rrrrrr@{}", hdr1, cm, hdr2, body,
                     ["program", "variant", "spec", "model", "size", "G impl", "G spec"] + (["G x"] if viol else []) + ["nested s", "nested MB", "online s", "online MB", "offline s", "offline MB", "summary files MB"], csvrows)
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
            body = []; csvrows = []; prev = None; EXTRA = []; DONE_EXTRA = set()
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
            hdr1 = f"instance & size & impl & $G_{{\\mathit{{cand}}}}$ & $G_{{\\mathit{{ref}}}}${' & $G_x$' if viol else ''} & \\multicolumn{{2}}{{c}}{{\\offline}} & \\multicolumn{{2}}{{c}}{{\\online}} \\\\"
            cm = "".join(f"\\cmidrule(lr){{{base + 1 + 2 * i}-{base + 2 + 2 * i}}}" for i in range(2))
            hdr2 = " & " * (base - 1) + " & T & M & T & M \\\\"
            emit(fn, f"tab:{fn}", cap, "@{}lrlrr" + ("r" if viol else "") + "rrrr@{}", hdr1, cm, hdr2, body,
                 ["program", "variant", "spec", "model", "size", "impl", "G impl", "G spec"] + (["G x"] if viol else []) + ["offline s", "offline MB", "online s", "online MB", "summary files MB"], csvrows)
            summary.append(f"{fn}: {len({k[:4] for k in keys})} instances, {len(csvrows)} rows")

# ---- the paper's curated tables (owner, 2026-11-03): inputs/table<N>.tsv lists the rows in order (program, variant, model,
# size); standalone columns, no model superscript; a violating table adds G_x; a row without results shows empty cells ----
CURATED = [("table2", "tab:table2", False, "Results of the refinement checking over conforming pairs, Rust implementation against Rust specification, under \\nested, \\online and \\offline engines. We use \\faClock~ to show timeout and oom for memory limit was reached."),   # the owner's wording from Overleaf (2026-11-07 17:50); the generator reproduces it so that regeneration does not revert it
           ("table3", "tab:table3", True, "Results of the refinement checking over violating pairs, Rust implementation against Rust specification, under \\nested, \\online and \\offline engines, with $G_x$ and T$_1$, the time to the first potential violation; \\offline has no T$_1$. We use \\faClock~ to show timeout and oom for memory limit was reached.")]
if not FULL and KEEP is None:
    data = {m: load(c) for m, c in [("stateless", "campaign-stateless"), ("stateful", "campaign-stateful"), ("offline", "campaign-offline-tf")]}
    inst = collections.OrderedDict()
    for m, rs in data.items():
        for r in rs: put(inst, key_of(r), m, r)
    BRK = []
    for tname, tlabel, viol, what in CURATED:
        TF_ = os.path.join(DV, "cluster", "campaign", "inputs", tname + ".tsv")
        if not os.path.exists(TF_): continue
        want = [l.rstrip("\n").split("\t") for l in open(TF_) if l.strip() and not l.startswith("#")]
        body = []; csvrows = []; prev = None; EXTRA = []; DONE_EXTRA = set()
        def fresh(r, offline=False):   # the owner's rule for the curated tables (2026-11-03 18:30): a cell only from runs at the current pins;
            # a limit marker only from a run at the 48 h tier (an older tier's clock is superseded by the queued rerun)
            if r is None: return None
            # owner 2026-11-03 21:10: a cell from the reserved machine (suffix .p, brain04, at most four rows at a time) is blue; an earlier campaign cell stays, in red, until its .p rerun lands
            if (r.get("pin") or "")[:7] not in ("9a0629e", "367cdc6"): return None   # 367cdc6 is equivalent to 9a0629e except for joins (which must be merge joins)
            if offline and not (r.get("exports current") == "yes" and r.get("join mode") == "merge"): return None
            if limit(r) and num(r.get("timeout s")) != 172800: return None
            r = dict(r); r["colour"] = "blue" if re.search(r"\.p(?=\.|$)", r.get("run id", "")) else ("orange" if r.get("contended") == "yes" else "red")   # orange (owner 2026-11-04 12:40): the run shared its NUMA node with another run; its time is not trusted
            return r
        def paint(r, cell): return f"\\textcolor{{{r['colour']}}}{{{cell}}}" if r is not None and cell != "" else cell
        for prog, var, model, size in want:
            k = next((kk for kk in inst if kk[0] == prog and kk[1] == var and kk[3] == model and kk[4] == size), None)
            m = inst.get(k, {}) if k else {}
            c2, c1, c3 = fresh(m.get("stateless")), fresh(m.get("stateful")), fresh(m.get("offline"), True)
            fb_size = ""; fb_note = []
            if FALLBACK:
                def older(r, offline=False):   # level A: the newest run of this cell at any pin; never blue; orange when it shared its node or ran before the current pins, else red
                    if r is None or not (decided(r) or limit(r)): return None
                    # the owner's eligibility rules hold on this path too (data audit 3, 2026-11-05): a limit marker only at the 48 h tier, an offline cell only with current exports and a merge join
                    if limit(r) and num(r.get("timeout s")) != 172800: return None
                    if offline and not (r.get("exports current") == "yes" and r.get("join mode") == "merge"): return None
                    r = dict(r); cur = (r.get("pin") or "")[:7] in ("9a0629e", "367cdc6")
                    r["colour"] = "orange" if (r.get("contended") == "yes" or not cur) else "red"; return r
                if c2 is None and older(m.get("stateless")) is not None: c2 = older(m.get("stateless")); fb_note.append("nested:older")
                if c1 is None and older(m.get("stateful")) is not None: c1 = older(m.get("stateful")); fb_note.append("online:older")
                if c3 is None and older(m.get("offline"), True) is not None: c3 = older(m.get("offline"), True); fb_note.append("offline:older")
                def best(r, offline=False):   # a cell with its true colour: the current-pin rule first (blue/red/orange), else the older-run rule
                    f = fresh(r, offline); return f if f is not None else older(r, offline)
                listed_sizes = {w[3] for w in want if w[0] == prog and w[1] == var and w[2] == model}
                never_ran = (c1 is None and c2 is None and c3 is None)   # owner 2026-11-05 09:50: such a row stays empty; the largest size that ran follows it
                short = [r for r in (c1, c2, c3) if r is not None and limit(r)]   # any limit (data audit 3: the 48 h tier too): the largest completed smaller size follows
                if short or never_ran:   # the largest size of the instance at which every engine completed (no limit), below this row's size
                    shown = sz(size)
                    import math
                    cands = [kk for kk in inst if kk[0] == prog and kk[1] == var and kk[3] == model and len(sz(kk[4])) == len(shown) and all(a <= b for a, b in zip(sz(kk[4]), shown)) and sz(kk[4]) != shown
                             and any(decided(inst[kk].get(e)) for e in ("stateful", "offline", "stateless"))
                             and not any(limit(inst[kk].get(e)) for e in ("stateful", "offline", "stateless"))]   # completed = every engine that ran at that size finished, none hit a limit
                    if cands:
                        import math
                        kk = max(cands, key=lambda x: (math.prod(sz(x[4])), sz(x[4])))   # every parameter at most the listed one, the largest by their product; when it is already a row of the table, the information is there: no extra row
                        if kk[4] not in listed_sizes: EXTRA.append((prog, var, model, kk[4], "completed"))
            dec = [r for r in (c1, c3, c2) if decided(r)] + ([c3] if exported(c3) and not decided(c3) else [])
            gi_r = next((r for r in dec if r["impl graphs"]), None); gi = gi_r["impl graphs"] if gi_r else ""
            gs_r = next((r for r in (c1, c3) if (decided(r) or exported(r)) and r["spec graphs"]), None); gs = gs_r["spec graphs"] if gs_r else ""
            gx_r = next((r for r in dec if r.get("uncovered") not in ("", None)), None) if viol else None; gx = gx_r.get("uncovered", "") if gx_r else ("" if viol else None)
            CURATED_NAMES = {("lb", "hash-a4a"): "lb-hash", ("2pc", "early-abort"): "2pc-ea", ("2pc-ab", "off-by-one"): "2pc-ab-off1", ("2pc-ds", "skip-ack-loop"): "2pc-ds-sal", ("2pc-lb", "counting-bug"): "2pc-lb-cb", ("2pc-le", "ring-conf"): "2pc-le", ("2pc-le", "ring-split"): "2pc-le-split", ("2pc-tr", "ignore-abort-vote"): "2pc-tr-iav", ("bcast", "decide-early"): "bcast-de", ("chan", "no-seqno"): "chan-ns", ("ewd998", "no-black-on-receive"): "ewd998-nb", ("lb", "rr-a4b"): "lb", ("lb", "rr-a4a"): "lb-strict", ("lock", "fifo"): "lock", ("lock", "wrong-round"): "lock-wr", ("pbkv", "pb"): "pbkv", ("replog-co", "early-reply"): "replog-co-er", ("ring-el", "no-compare"): "ring-el-nc", ("zab", "issue 1"): "zab-issue1"}   # the owner's names for the curated tables (2026-11-03)
            nm = f"\\textsf{{{esc(CURATED_NAMES[(prog, var)])}}}" if (prog, var) in CURATED_NAMES else f"\\textsf{{{esc(prog)}}}" + (f"$_{{\\mathit{{{esc(var)}}}}}$" if var not in ("", "impl", "correct", prog) else "")   # a variant named as the program is the program (owner: "only call it reset")
            nm_full = nm; nm = nm if (prog, var) != prev else ""; prev = (prog, var)
            if decided(c1) and decided(c3): BRK.append((tname, nm_full, size, c1, c3))   # Table 4 (owner 2026-11-08): the time components of the pairs both algorithms finished
            if nm and body: body.append("\\midrule")
            expl = esc(c2.get("spec graphs explored", "")) if c2 is not None and decided(c2) else ""   # the stateless engine's own counts: sweeps kept in the witness cache, specification graphs explored (repeats counted)
            sweeps = esc(c2.get("sweeps", "")) if c2 is not None and decided(c2) else ""
            cT = cell_Tp if viol else cell_T
            cells = [sweeps, expl] + ([cell_T1(c2, False)] if viol else []) + [cT(c2), cell_M(c2)] + ([cell_T1(c1, True)] if viol else []) + [cT(c1), cell_M(c1), cT(c3), cell_M_off(c3)]
            owners = [c2, c2] + ([c2] if viol else []) + [c2, c2] + ([c1] if viol else []) + [c1, c1, c3, c3]
            size_cell = size
            body.append(f"{nm} & {size_cell} & {paint(gi_r, gi)} & {paint(gs_r, gs)}" + (f" & {paint(gx_r, gx)}" if viol else "") + " & " + " & ".join(paint(o, c) for o, c in zip(owners, cells)) + " \\\\")
            csvrows.append([prog, var, model, size, gi, gs] + ([gx] if viol else []) + [c.replace("\\faClock", "t/o") for c in cells] + [",".join(f"{e}={r['colour']}" for e, r in (("nested", c2), ("online", c1), ("offline", c3)) if r is not None)] + ([";".join(fb_note)] if FALLBACK else []))
            for ex in [e for e in EXTRA if e[:3] == (prog, var, model) and e not in DONE_EXTRA]:
                DONE_EXTRA.add(ex); kx = next((kk for kk in inst if kk[0] == prog and kk[1] == var and kk[3] == model and kk[4] == ex[3]), None); mx = inst.get(kx, {}) if kx else {}
                x2, x1, x3 = best(mx.get("stateless")), best(mx.get("stateful")), best(mx.get("offline"), True)
                dx = [r for r in (x1, x3, x2) if decided(r)]; gxi_r = next((r for r in dx if r["impl graphs"]), None); gxi = gxi_r["impl graphs"] if gxi_r else ""; gxs_r = next((r for r in (x1, x3) if decided(r) and r["spec graphs"]), None); gxs = gxs_r["spec graphs"] if gxs_r else ""
                gxx_r = next((r for r in dx if r.get("uncovered") not in ("", None)), None) if viol else None; gxx = gxx_r.get("uncovered", "") if gxx_r else ("" if viol else None)   # the counts of a dagger row carry the colour of the run that supplies them, as in the other rows (reviews r9)
                xcells = [esc(x2.get("sweeps", "")) if x2 is not None and decided(x2) else "", esc(x2.get("spec graphs explored", "")) if x2 is not None and decided(x2) else ""] + ([cell_T1(x2, False)] if viol else []) + [(cell_Tp if viol else cell_T)(x2), cell_M(x2)] + ([cell_T1(x1, True)] if viol else []) + [(cell_Tp if viol else cell_T)(x1), cell_M(x1), (cell_Tp if viol else cell_T)(x3), cell_M_off(x3)]
                xown = [x2, x2] + ([x2] if viol else []) + [x2, x2] + ([x1] if viol else []) + [x1, x1, x3, x3]
                if decided(x1) and decided(x3): BRK.append((tname, nm_full, ex[3], x1, x3))   # the dagger row's pair belongs to Table 4 too (its colours are its own)
                body.append(f" & \\textcolor{{orange}}{{{ex[3]}$^\\ddagger$}} & {paint(gxi_r, gxi)} & {paint(gxs_r, gxs)}" + (f" & {paint(gxx_r, gxx)}" if viol else "") + " & " + " & ".join(paint(o, c) for o, c in zip(xown, xcells)) + " \\\\")
                csvrows.append([prog, var, model, ex[3], gxi, gxs] + ([gxx] if viol else []) + [c.replace("\\faClock", "t/o") for c in xcells] + [",".join(f"{e}={r['colour']}" for e, r in (("nested", x2), ("online", x1), ("offline", x3)) if r is not None), "completed-size-for:" + size])
        cap = what   # the curated caption carries the column definitions (owner, 2026-11-05: short captions)
        base = 5 if viol else 4
        nN, nO = (5, 3) if viol else (4, 2)
        hdr1 = f"instance & size & $G_{{\\mathit{{cand}}}}$ & $G_{{\\mathit{{ref}}}}${' & $G_x$' if viol else ''} & \\multicolumn{{{nN}}}{{c}}{{\\nested}} & \\multicolumn{{{nO}}}{{c}}{{\\online}} & \\multicolumn{{2}}{{c}}{{\\offline}} \\\\"
        cm = f"\\cmidrule(lr){{{base + 1}-{base + nN}}}\\cmidrule(lr){{{base + nN + 1}-{base + nN + nO}}}\\cmidrule(lr){{{base + nN + nO + 1}-{base + nN + nO + 2}}}"
        T1 = "T$_1$ & " if viol else ""
        emit(tname, tlabel, cap, "@{}lrrr" + ("r" if viol else "") + "r" * (nN + nO + 2) + "@{}", hdr1, cm, " & " * (base - 1) + f" & sweeps & $G_{{\\mathit{{ref}}}}^{{\\mathit{{expl}}}}$ & {T1}T & M & {T1}T & M & T & M \\\\", body,
             ["program", "variant", "model", "size", "G impl", "G spec"] + (["G x"] if viol else []) + ["nested sweeps", "nested spec graphs explored"] + (["nested first violation s"] if viol else []) + ["nested s", "nested MB"] + (["online first violation s"] if viol else []) + ["online s", "online MB", "offline s", "offline MB", "colour (blue = paper run (.p) on a brain node with one row per NUMA node, orange = shared node, earlier engine version or shared-node type, red = earlier campaign at the current pins)"] + (["fallback"] if FALLBACK else []), csvrows)
        summary.append(f"{tname}: {len(csvrows)} rows")
    # ---- Table 4 (owner 2026-11-08): where the time of an online and of an offline check goes, on the pairs of Tables 1-2 that both finished.
    # Online (in-process): T_ref = exploring the reference and building the index (spec phase), T_cand = exploring the candidate and querying it (impl phase).
    # Offline: per export E = exploring and summarising, W = writing the summary file; J = the merge join; T as in Tables 1-2 (the longer export plus the join).
    def comp(r, key):   # a component from its record field: never a printed zero (owner 2026-11-06); "--" when the record lacks it
        v = num((r or {}).get(key))
        return "--" if v is None else ("$<$0.1" if v < 0.05 else rnd(v, 1))
    body = []; csvrows = []; prev_t = None; prev_nm = None
    for tname, nm, size, c1, c3 in BRK:
        if tname != prev_t and body: body.append("\\midrule")
        shown = nm if (tname, nm) != prev_nm else ""; prev_t, prev_nm = tname, (tname, nm)
        pair = "C" if tname == "table2" else "V"
        on = [comp(c1, "spec phase s"), comp(c1, "impl phase s"), cell_T(c1)]
        off = [comp(c3, "spec explore s"), comp(c3, "spec write s"), comp(c3, "impl explore s"), comp(c3, "impl write s"), comp(c3, "join s"), cell_T(c3)]
        # the export's extra cost per candidate graph (owner 2026-11-08): (E_cand - T_cand) / G_cand in microseconds, signed
        _e, _t, _g = num(c3.get("impl explore s")), num(c1.get("impl phase s")), num(c1.get("impl graphs"))
        dg = f"{(_e - _t) / _g * 1e6:+.1f}" if None not in (_e, _t, _g) and _g >= 10000 else ""   # blank below 10 000 candidate graphs: the fixed costs of a run dominate
        body.append(f"{shown} & {size} & {pair} & " + " & ".join([paint(c1, x) for x in on] + [paint(c3, x) for x in off]) + f" & {dg}" + " \\\\")
        csvrows.append([nm.replace("\\textsf{", "").rstrip("}"), size, pair] + on + off + [dg, c1.get("run id", ""), c3.get("run id", "")])
    if body:
        cap = ("Time components in seconds of \\online and \\offline on the pairs of the two preceding tables that both finished (C = conforming, V = violating). "
               "\\online: T$_{\\mathit{ref}}$ = exploring the reference and building the index, T$_{\\mathit{cand}}$ = exploring the candidate and querying it, T = the run. "
               "\\offline: for the reference and the candidate export, E = exploring and summarising, W = writing the summary file; J = the merge join; T = the longer export plus the join. "
               "$\\Delta/G$ = (E$_{\\mathit{cand}}$ $-$ T$_{\\mathit{cand}}$) divided by the number of candidate graphs, in microseconds of wall time with 16 workers, for pairs of at least 10\\,000 candidate graphs. "
               "A component below 0.05\\,s is printed as $<$0.1.")
        hdr1 = "instance & size & pair & \\multicolumn{3}{c}{\\online} & \\multicolumn{6}{c}{\\offline} & \\\\"
        cm = "\\cmidrule(lr){4-6}\\cmidrule(lr){7-12}"
        hdr2 = " & & & T$_{\\mathit{ref}}$ & T$_{\\mathit{cand}}$ & T & E$_{\\mathit{ref}}$ & W$_{\\mathit{ref}}$ & E$_{\\mathit{cand}}$ & W$_{\\mathit{cand}}$ & J & T & $\\Delta/G$ \\\\"
        emit("table4", "tab:table4", cap, "@{}lrlrrrrrrrrrr@{}", hdr1, cm, hdr2, body,
             ["instance", "size", "pair", "online T_ref s", "online T_cand s", "online T s", "offline E_ref s", "offline W_ref s", "offline E_cand s", "offline W_cand s", "offline join s", "offline T s", "delta per candidate graph us", "online run id", "offline run id"], csvrows)
        summary.append(f"table4: {len(csvrows)} rows")
if wb is not None: wb.save(f"{OUT}/paper-tables{SUF}.xlsx")
if incons:
    print(f"{len(incons)} graph-count inconsistencies across the engines:")
    for fam, k, what, d in incons[:30]: print("  ", fam, k, what, d)
print("\n".join(summary))
