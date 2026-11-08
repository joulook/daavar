#!/usr/bin/env python3
"""Twins of the paper's Phase B tables (owner, 2026-11-09 00:00): every Rust-against-specification check of the case
studies that brain-data holds or the queues will run, and that the two Phase B tables of eval2 (spec-conf, spec-viol)
do not show. Same shape as those tables: pair (candidate $\\sqsubseteq$ reference), size, G_cand, G_ref, [G_x], Offline
T and M, Online T and M. The pair names come from study-tables.py's role functions (its definitions are loaded from the
file), so a run id is named exactly as in the study documents, with the agent "sol" written Codex as in the paper. Java
candidates and Java references belong to Phases C and D and are left out. A third table lists the rows queued or
running on the cluster whose pair is not yet in any table.
usage: phaseb-extra.py <repo-root> <out-dir> <pending.txt>   (pending.txt: lines "node queue state id" from
       cluster-pending.sh; writes table3-extra.tex, table4-extra.tex, table5-extra.tex and a -note.tex)"""
import sys, os, csv, glob, re, collections
R, OUT, PENDING = sys.argv[1], sys.argv[2], sys.argv[3]
src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "study-tables.py")).read()
ns = {}; argv = sys.argv; sys.argv = [argv[0], R]
exec(compile(src[:src.index("for study in sys.argv[2:]:")], "study-tables.py", "exec"), ns); sys.argv = argv
STUDIES, esc, fnum, mib, status, parse = (ns[k] for k in ("STUDIES", "esc", "fnum", "mib", "status", "parse"))
SR = "S$_{\\mathrm{raft}}$"
TITLE = {"raft": "Raft and OmniPaxos libraries", "omnipaxos": "Omni-Paxos", "zookeeper": "ZooKeeper (Zab)", "chainpaxos": "ChainPaxos", "swiftpaxos": "SwiftPaxos", "pineapple": "Pineapple",
         "cassandra": "Cassandra, lightweight transactions", "cassandra-rr": "Cassandra, read repair", "craq": "3FS (CRAQ)", "bk": "BookKeeper", "crdb": "CockroachDB, parallel commits", "scalardb": "ScalarDB, consensus commit"}
ORDER = ["raft", "omnipaxos", "zookeeper", "chainpaxos", "swiftpaxos", "pineapple", "cassandra", "cassandra-rr", "craq", "bk", "crdb", "scalardb"]
PREFIX = {"zookeeper": "ZooKeeper ", "chainpaxos": "ChainPaxos ", "swiftpaxos": "SwiftPaxos ", "pineapple": "Pineapple ", "cassandra": "Cassandra ", "cassandra-rr": "Cassandra ", "craq": "3FS ", "bk": "BookKeeper ", "crdb": "CockroachDB ", "scalardb": "ScalarDB "}
def lab(x): return esc(x).replace("S$\\_{", "S$_{")   # escape underscores outside the S$_{...}$ names
def paper_name(study, x):   # a study label in the paper's words
    x = x.replace("sol", "Codex").replace("Rust OmniPaxos", "OmniPaxos")
    if re.match(r"^(port|code|paper) \(", x): x = PREFIX.get(study, "") + x
    return x
def norm(s):   # a comparable form of a pair side, from the paper's macros or the study labels
    s = re.sub(r"\\s(raft|paper|omni|zab|der|lin|cas|pmd|chain|swift|pine|rr|bk|pc|cc)\b", lambda m: "S_" + m.group(1), s)
    s = s.replace("S_omni", "S_paper").replace("S_pmd", "S_der").replace("S_chain", "S_der").replace("S_swift", "S_der").replace("S_pine", "S_der")   # the study labels keep the old names
    s = re.sub(r"^(ZooKeeper|ChainPaxos|SwiftPaxos|Pineapple|Cassandra|3FS|BookKeeper|CockroachDB|ScalarDB) (port|code|paper)", r"\2", s)   # the paper's tables group by system and drop the prefix
    s = re.sub(r"S\$\\?_\{\\mathrm\{(\w+)\}\}\$", r"S_\1", s)
    s = s.replace("$^{\\ast}$", "").replace("\\_", "_")
    return re.sub(r"\s+", " ", s).strip()
def row_key(study, cand, ref, size): return (norm(paper_name(study, cand)), norm(paper_name(study, ref)), size)
# ---- the rows the paper's tables already show ----
shown = set(); unmatched = []
for f in ("realsw", "agents-rust"):   # 2026-11-09: the paper's Phase C and D tables (the former spec-conf/spec-viol)
    pair = None
    for line in open(f"{R}/PLDI/tex/tables/eval2/{f}.tex"):
        if "&" not in line or "multicolumn" in line or line.lstrip().startswith(("%", "pair")) or re.match(r"\s*\\(toprule|midrule|bottomrule|cmidrule|begin|end|caption|label|scriptsize)", line): continue
        cells = [c.strip() for c in line.split("&")]
        if cells[0]: pair = cells[0]
        if len(cells) > 3 and cells[2] and ("\\tf" in cells[2] or "\\jmc" in cells[2]): del cells[2]   # the real-software table has a tools column
        if cells[0] and "\\jmc" in cells[0]: continue
        size = re.sub(r"\\pendg\{([^}]*)\}", r"\1", cells[1]).strip()
        if not pair or not size: continue
        p = pair; m = re.search(r" \((v1nolin|v1)\)", p)
        if m: p = p.replace(m.group(0), ""); size = size + (" v1nolin" if m.group(1) == "v1nolin" else "")
        a, b = [norm(x) for x in p.split("$\\sqsubseteq$")]
        for bb in b.split(", "):   # a row against two references at once (ZooKeeper port against S_raft, S_zab) stands for both pairs
            shown.add((a, bb, size))
            if re.fullmatch(r"\d+", size): shown.add((a, bb, size + ",0"))   # the paper writes a ZooKeeper size (e, 0) as e
# ---- the runs: per study, Rust candidates against specifications or Rust programs ----
OMNI_RUST = {"Rust OmniPaxos", "S$_{\\mathrm{raft}}$", "S$_{\\mathrm{paper}}$"}   # the Omni-Paxos study's other candidates are Java programs (Phase D)
def is_spec(x): return x.startswith("S$")
def rust_only(ro, study=None):   # a Rust candidate against a specification: Java sides and program-against-program pairs belong to Phases C and D
    if not all(isinstance(x, str) for x in ro[:2]) or any("Java" in x for x in ro[:2]): return False
    if study == "omnipaxos" and not all(x in OMNI_RUST for x in ro[:2]): return False
    return is_spec(ro[1])
CROSS_DER = {"pineapple-chainderived-local": "S$_{\\mathrm{der}}$ (ChainPaxos, local)", "pineapple-chainderived-mixed": "S$_{\\mathrm{der}}$ (ChainPaxos, mixed)", "pineapple-swiftderived": "S$_{\\mathrm{der}}$ (SwiftPaxos)"}
rows = {}   # (study, cand, ref, size, model) -> {"offline": r, "online": r}
def consider(study, ro, r):
    if ro: ro = (CROSS_DER.get(ro[0], ro[0]), CROSS_DER.get(ro[1], ro[1]), ro[2])
    if not ro or not rust_only(ro, study): return
    if r["verdict"] in ("t/o", "oom") and (r.get("timeout s") or "") != "172800": return
    if r["verdict"] not in ("conforms", "violation", "t/o", "oom"): return
    kind = "online" if ro[2] in ("online", "in-process") else "offline"
    v = r.get("variant") or ""; size = r["size"] + ((" " + v) if v and v not in ("impl", "spec", "correct", "v1") and v not in r["size"] else "")
    k = (study, ro[0], ro[1], size, r["model"]); cur = rows.setdefault(k, {}).get(kind)
    if cur is None or (r.get("started") or "") > (cur.get("started") or ""): rows[k][kind] = r
for f in glob.glob(f"{R}/PLDI/tex/tables/campaign-*.csv"):
    for r in csv.DictReader(open(f)):
        if r["program"] == "raft" and r["variant"] == "correct":
            pre = r["run id"].split(".")[0]
            if pre in ("C1", "C3J"): consider("raft", ("raft-rs", SR, "online" if pre == "C1" else "offline"), r)
            continue
        for study, S in STUDIES.items():
            if S["sel"](r):
                consider(study, S["roles"](r["run id"]), r); break
NEW = {s: S["new"] for s, S in STUDIES.items()}; NEW["raft"] = "2026-11-03T23:30"
def paint(study, r, cells):
    c = "blue" if (r.get("started") or "") >= NEW[study] else ("orange" if r.get("contended") == "yes" else "red")
    return [f"\\textcolor{{{c}}}{{{x}}}" if x not in ("", "--") else x for x in cells]
def T(r): st = status(r); return st if st else fnum(r["wall s"])
def M(r): return mib(r["peak MB"]) if r["verdict"] in ("conforms", "violation") else "--"
def sz(s): return [int(x) for x in re.findall(r"\d+", s)]
conf, viol = [], []
for k in sorted(rows, key=lambda k: (ORDER.index(k[0]), k[1], k[2], sz(k[3]), k[3])):
    study, cand, ref, size, model = k
    if row_key(study, cand, ref, size) in shown: continue
    ro, rn = rows[k].get("offline"), rows[k].get("online"); r0 = ro or rn
    label = f"{lab(paper_name(study, cand))} $\\sqsubseteq$ {lab(paper_name(study, ref))}" + ("" if model == "fifo" else f"$^{{\\mathrm{{{model}}}}}$")
    v = r0["verdict"] == "violation"
    cells = [r0["impl graphs"] or "--", r0["spec graphs"] or "--"] + ([(ro or {}).get("uncovered") or (rn or {}).get("uncovered") or "--"] if v else [])
    off = paint(study, ro, [T(ro), M(ro)]) if ro else ["", ""]; on = paint(study, rn, [T(rn), M(rn)]) if rn else ["", ""]
    (viol if v else conf).append((study, label, size, paint(study, r0, cells) + off + on))
# ---- rows queued or running whose pair is not yet in any table ----
MODELS = {"bag", "fifo", "p2p", "asyn", "causal"}
have = {row_key(k[0], k[1], k[2], k[3]) for k in rows} | shown
pend = {}
for line in open(PENDING):
    parts = line.split()
    if len(parts) < 4: continue
    node, queue, state, rid = parts[:4]; pre = rid.split(".")[0]
    if pre not in ("C1", "C3J", "XJ1"): continue   # a Rust check or a join; exports alone are not a pair
    d = rid.split(".")[1]
    if d == "raft": ro, study = ("raft-rs", SR, "online" if pre == "C1" else "offline"), "raft"
    else:
        ro = study = None
        for s, S in STUDIES.items():
            try: x = S["roles"](rid)
            except Exception: x = None
            if x: ro, study = x, s; break
    if ro: ro = (CROSS_DER.get(ro[0], ro[0]), CROSS_DER.get(ro[1], ro[1]), ro[2])
    if not ro or not rust_only(ro, study): continue
    desc = rid.split(".")[2]; toks = desc.split("_")[1:]; nums = [t for t in toks if t.isdigit()]; words = [t for t in toks if not t.isdigit() and t not in MODELS]
    model = next((t for t in toks if t in MODELS), "fifo")
    size = ",".join(nums) + ((" " + "-".join(words)) if words and "-".join(words) not in ("impl", "spec", "correct", "v1") else "")
    key = row_key(study, ro[0], ro[1], size)
    if key in have: continue
    pend.setdefault((study, ro[0], ro[1], size, model), []).append((state, node, "online" if ro[2] == "online" else "offline"))
sched = []
for k in sorted(pend, key=lambda k: (ORDER.index(k[0]), k[1], k[2], sz(k[3]), k[3])):
    study, cand, ref, size, model = k
    label = f"{lab(paper_name(study, cand))} $\\sqsubseteq$ {lab(paper_name(study, ref))}" + ("" if model == "fifo" else f"$^{{\\mathrm{{{model}}}}}$")
    st = {kind: f"\\textit{{{state}}} ({node})" for state, node, kind in pend[k]}
    sched.append((study, label, size, ["", ""] + [st.get("offline", ""), ""] + [st.get("online", ""), ""]))
# ---- emit ----
def emit(fn, cap, items, viol):
    cols = "@{}llrr" + ("r" if viol else "") + "rrrr@{}"
    hdr1 = " & & & " + ("& " if viol else "") + "& \\multicolumn{2}{c}{\\offline} & \\multicolumn{2}{c}{\\online} \\\\"
    b = 6 if viol else 5
    cm = f"\\cmidrule(lr){{{b}-{b + 1}}}\\cmidrule(lr){{{b + 2}-{b + 3}}}"
    hdr2 = "pair & size & $G_{\\mathit{cand}}$ & $G_{\\mathit{ref}}$ " + ("& $G_x$ " if viol else "") + "& T & M & T & M \\\\"
    L = [f"\\begin{{longtable}}{{{cols}}}", f"\\caption{{{cap}}}\\label{{tab:{fn}}}\\\\", "\\toprule", hdr1, cm, hdr2, "\\midrule", "\\endfirsthead", "\\toprule", hdr1, cm, hdr2, "\\midrule", "\\endhead"]
    prev_study = prev_label = None
    for study, label, size, cells in items:
        if study != prev_study:
            if prev_study is not None: L.append("\\midrule")
            L.append(f"\\multicolumn{{{b + 3}}}{{@{{}}l}}{{\\emph{{{TITLE[study]}}}}} \\\\")
            prev_study, prev_label = study, None
        L.append(f"{label if label != prev_label else ''} & {size} & " + " & ".join(cells) + " \\\\"); prev_label = label
    L += ["\\bottomrule", "\\end{longtable}"]
    open(f"{OUT}/{fn}.tex", "w").write("\n".join(L) + "\n")
os.makedirs(OUT, exist_ok=True)
emit("table3-extra", "Specifications and real software in Rust, conforming pairs not in the paper's table: candidate $\\sqsubseteq$ reference, with $G_{\\mathit{cand}}$, $G_{\\mathit{ref}}$, and T and M of \\offline and of \\online where it was run.", conf, False)
emit("table4-extra", "Specifications and real software in Rust, violating pairs not in the paper's table, with $G_x$.", viol, True)
emit("table5-extra", "Phase B pairs queued or running on the cluster with no result yet, with the queue state and node in the column of the algorithm.", sched, False)
open(f"{OUT}/table3-extra-note.tex", "w").write(f"\\noindent {len(conf)} conforming rows, {len(viol)} violating rows and {len(sched)} rows queued or running, from {len(rows)} checks of the case studies; {len(shown)} rows are in the paper's tables and are left out here.\n")
print(f"conf {len(conf)}, viol {len(viol)}, scheduled {len(sched)}; shown in paper {len(shown)}")
matched = {k for k in shown if k in {row_key(x[0], x[1], x[2], x[3]) for x in rows}}
for k in sorted(shown - matched): print("paper row not matched to a run:", k)
