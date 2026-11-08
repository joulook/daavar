#!/usr/bin/env python3
"""A twin of the paper's Table 1 (owner, 2026-11-08 evening) for the conforming pairs it leaves out: Rust implementation
against its own Rust specification, under Nested, Online and Offline, over the instances (program, variant, model) that
inputs/table2.tsv does not list and brain-data has decided. Same columns, same caption, same cell rules and colours as
paper-tables.py's curated Table 1 (current-pin cells blue/red/orange, older-pin cells orange/red, limit markers only at
the 48 h tier, offline cells only with current exports and a merge join). Per instance the two largest sizes with a
result from any of the three algorithms and the smallest size that reached a limit; no dagger rows. Scope: the benchmark
collection (paper-names.tsv) and the agent-written classical protocols; the real-software case studies and the
specification-side checks are left out. Violating pairs and pairs with a violating verdict at any size are left out.
usage: table1-extra.py <tables-dir> <out-dir>   (reads campaign-{stateless,stateful,offline-tf}.csv, writes
       table1-extra.tex, table1-extra.csv, table1-extra-note.tex)"""
import sys, csv, re, os, math, collections
from decimal import Decimal, ROUND_HALF_UP
T, OUT = sys.argv[1], sys.argv[2]
DV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
INP = os.path.join(DV, "cluster", "campaign", "inputs")
def rows(name):
    try: return list(csv.DictReader(open(f"{T}/{name}.csv")))
    except OSError: return []
def tsv(name): return [l.rstrip("\n").split("\t") for l in open(os.path.join(INP, name)) if l.strip() and not l.startswith("#")]
T1 = {(p, v, m) for p, v, m, s in tsv("table2.tsv")}
T2 = {(p, v, m) for p, v, m, s in tsv("table3.tsv")}
BENCH = {l[2] for l in tsv("paper-names.tsv")}            # the benchmark collection's paper names
REAL = {"raft", "omnipaxos"} | {p for p in BENCH if p.startswith("omnipaxos")}
AGENT = {"caspaxos", "chain-replication", "hermes", "flexible-paxos", "ricart-agrawala", "vrr"}   # classical protocols written by agents (paper-tables.py P2T)
SYN = {"naive", "share", "commit", "chain", "reset", "width"}
DROPPED = {tuple(l.split()) for l in open(os.path.join(DV, "..", "plan", "dropped-tables", "dropped.keep")) if l.strip() and not l.startswith("#")} if os.path.exists(os.path.join(DV, "..", "plan", "dropped-tables", "dropped.keep")) else set()
def dropped(p, v): return (p,) in DROPPED or (p, v) in DROPPED
def num(x):
    try: return float(x)
    except (TypeError, ValueError): return None
def sz(s): return [int(x) for x in re.findall(r"\d+", s)]
def esc(s): return s.replace("_", "\\_")
def own_spec(r):   # the program's own specification: an empty spec word, "spec", or the variant itself
    s = (r.get("spec") or "").strip(); return s in ("", "spec", r["variant"])
def decided(r): return r is not None and r["verdict"] in ("conforms", "violation")
def limit(r): return r is not None and r["verdict"] in ("t/o", "oom")
TIER = {600: "10m", 3600: "1h", 14400: "4h", 86400: "24h", 172800: "48h"}
def tier(r):
    t = num(r.get("timeout s")); return TIER.get(int(t), f"{int(t)}s") if t else ""
def rnd(v, nd): return str(Decimal(repr(v)).quantize(Decimal(1).scaleb(-nd), rounding=ROUND_HALF_UP))
def cell_T(r):
    if r is None or not (decided(r) or limit(r)): return ""
    if r["verdict"] == "t/o": return "\\faClock" + (f"$_{{\\mathrm{{{tier(r)}}}}}$" if tier(r) else "")
    if r["verdict"] == "oom":
        g = num(r.get("mem GB")); return "oom" + (f"$_{{\\mathrm{{{int(g)}G}}}}$" if g and g < 250 else "")
    v = num(r.get("wall s")); return "" if v is None else ("$<$0.1" if v < 0.05 else rnd(v, 1))   # never a printed zero (owner 2026-11-06)
def cell_M(r):
    if not decided(r): return ""
    v = num(r.get("peak MB")); return "" if v is None else f"{v:.0f}"
def pref(r): return (bool(re.search(r"\.p(?=\.|$)", r.get("run id", ""))), r.get("contended") != "yes", r.get("started") or "")
CUR = ("9a0629e", "367cdc6")
def fresh(r, offline=False):   # paper-tables.py: a cell from a run at the current pins; blue on the reserved machine (.p), orange when contended, else red
    if r is None: return None
    if (r.get("pin") or "")[:7] not in CUR: return None
    if offline and not (r.get("exports current") == "yes" and r.get("join mode") == "merge"): return None
    if limit(r) and num(r.get("timeout s")) != 172800: return None
    r = dict(r); r["colour"] = "blue" if re.search(r"\.p(?=\.|$)", r.get("run id", "")) else ("orange" if r.get("contended") == "yes" else "red"); return r
def older(r, offline=False):   # paper-tables.py --fallback: the newest run at any pin, never blue
    if r is None or not (decided(r) or limit(r)): return None
    if limit(r) and num(r.get("timeout s")) != 172800: return None
    if offline and not (r.get("exports current") == "yes" and r.get("join mode") == "merge"): return None
    r = dict(r); r["colour"] = "orange" if (r.get("contended") == "yes" or (r.get("pin") or "")[:7] not in CUR) else "red"; return r
def best(r, offline=False):
    f = fresh(r, offline); return f if f is not None else older(r, offline)
def paint(r, cell): return f"\\textcolor{{{r['colour']}}}{{{cell}}}" if r is not None and cell != "" else cell
CURATED_NAMES = {("lb", "hash-a4a"): "lb-hash", ("2pc", "early-abort"): "2pc-ea", ("2pc-ab", "off-by-one"): "2pc-ab-off1", ("2pc-ds", "skip-ack-loop"): "2pc-ds-sal", ("2pc-lb", "counting-bug"): "2pc-lb-cb", ("2pc-le", "ring-conf"): "2pc-le", ("2pc-le", "ring-split"): "2pc-le-split", ("2pc-tr", "ignore-abort-vote"): "2pc-tr-iav", ("bcast", "decide-early"): "bcast-de", ("chan", "no-seqno"): "chan-ns", ("ewd998", "no-black-on-receive"): "ewd998-nb", ("lb", "rr-a4b"): "lb", ("lb", "rr-a4a"): "lb-strict", ("lock", "fifo"): "lock", ("lock", "wrong-round"): "lock-wr", ("pbkv", "pb"): "pbkv", ("replog-co", "early-reply"): "replog-co-er", ("ring-el", "no-compare"): "ring-el-nc", ("zab", "issue 1"): "zab-issue1"}
# ---- collect: instance (program, variant, model) -> size -> engine -> preferred record ----
inst = collections.defaultdict(lambda: collections.defaultdict(dict))
for eng, f in (("nested", "campaign-stateless"), ("online", "campaign-stateful"), ("offline", "campaign-offline-tf")):
    for r in rows(f):
        if ".spec_" in r.get("run id", "") or not own_spec(r): continue
        k = (r["program"], r["variant"], r["model"])
        cur = inst[k][r["size"]].get(eng)
        if cur is None or pref(r) >= pref(cur): inst[k][r["size"]][eng] = r
left = collections.Counter()
keep = []
for k in sorted(inst, key=lambda k: (k[0] not in SYN, k)):
    p, v, m = k
    if k in T1: left["in Table 1"] += 1; continue
    if k in T2: left["in Table 2"] += 1; continue
    if p in REAL or (p not in BENCH and p not in AGENT): left["case study / real software"] += 1; continue
    verd = {r["verdict"] for sm in inst[k].values() for r in sm.values() if decided(r)}
    if not verd: left["no decided run"] += 1; continue
    if "violation" in verd: left["violating at some size"] += 1; continue
    keep.append(k)
# ---- select sizes: the two largest with a result from any algorithm, plus the smallest size that reached a 48 h limit ----
body, csvrows, note_rows = [], [], []
multi = collections.Counter((p, v) for p, v, m in keep)   # the same program and variant under two models: the model is shown
for k in keep:
    p, v, m = k
    cells_by_size = {}
    for size, sm in inst[k].items():
        c2, c1, c3 = best(sm.get("nested")), best(sm.get("online")), best(sm.get("offline"), True)
        if any(decided(r) or limit(r) for r in (c1, c2, c3)): cells_by_size[size] = (c2, c1, c3)
    order = sorted(cells_by_size, key=lambda s: (math.prod(sz(s)) if sz(s) else 0, sz(s)))
    fin = [s for s in order if any(decided(r) for r in cells_by_size[s])]
    lim = [s for s in order if s not in fin and any(limit(r) for r in cells_by_size[s])]
    sel = [s for s in order if s in fin[-2:] or s in lim[:1]]   # the smallest size that reached a limit stands for the larger ones
    if not sel: continue
    nm = f"\\textsf{{{esc(CURATED_NAMES[(p, v)])}}}" if (p, v) in CURATED_NAMES else f"\\textsf{{{esc(p)}}}" + (f"$_{{\\mathit{{{esc(v)}}}}}$" if v not in ("", "impl", "correct", p) else "")
    if multi[(p, v)] > 1: nm += f"$^{{\\mathit{{{m}}}}}$"
    if body: body.append("\\midrule")
    first = True
    for size in sel:
        c2, c1, c3 = cells_by_size[size]
        dec = [r for r in (c1, c3, c2) if decided(r)]
        gi_r = next((r for r in dec if r["impl graphs"]), None); gi = gi_r["impl graphs"] if gi_r else ""
        gs_r = next((r for r in (c1, c3) if decided(r) and r["spec graphs"]), None); gs = gs_r["spec graphs"] if gs_r else ""
        sweeps = esc(c2.get("sweeps", "")) if c2 is not None and decided(c2) else ""
        expl = esc(c2.get("spec graphs explored", "")) if c2 is not None and decided(c2) else ""
        cells = [sweeps, expl, cell_T(c2), cell_M(c2), cell_T(c1), cell_M(c1), cell_T(c3), cell_M(c3)]
        owners = [c2, c2, c2, c2, c1, c1, c3, c3]
        body.append(f"{nm if first else ''} & {size} & {paint(gi_r, gi)} & {paint(gs_r, gs)} & " + " & ".join(paint(o, c) for o, c in zip(owners, cells)) + " \\\\")
        csvrows.append([p, v, m, size, gi, gs] + [c.replace("\\faClock", "t/o") for c in cells] + [",".join(f"{e}={r['colour']}" for e, r in (("nested", c2), ("online", c1), ("offline", c3)) if r is not None), "dropped" if dropped(p, v) else ""])
        first = False
    note_rows.append((nm, dropped(p, v)))
cap = "Results of the refinement checking over conforming pairs, Rust implementation against Rust specification, under \\nested, \\online and \\offline engines. We use \\faClock~ to show timeout and oom for memory limit was reached."
hdr1 = "instance & size & $G_{\\mathit{cand}}$ & $G_{\\mathit{ref}}$ & \\multicolumn{4}{c}{\\nested} & \\multicolumn{2}{c}{\\online} & \\multicolumn{2}{c}{\\offline} \\\\"
cm = "\\cmidrule(lr){5-8}\\cmidrule(lr){9-10}\\cmidrule(lr){11-12}"
hdr2 = " &  &  &  & sweeps & $G_{\\mathit{ref}}^{\\mathit{expl}}$ & T & M & T & M & T & M \\\\"
L = ["\\begin{longtable}{@{}lrrrrrrrrrrr@{}}", f"\\caption{{{cap}}}\\label{{tab:table1-extra}}\\\\", "\\toprule", hdr1, cm, hdr2, "\\midrule", "\\endfirsthead",
     "\\toprule", hdr1, cm, hdr2, "\\midrule", "\\endhead"] + body + ["\\bottomrule", "\\end{longtable}"]
os.makedirs(OUT, exist_ok=True)
open(f"{OUT}/table1-extra.tex", "w").write("\n".join(L) + "\n")
with open(f"{OUT}/table1-extra.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["program", "variant", "model", "size", "G impl", "G spec", "nested sweeps", "nested spec graphs explored", "nested s", "nested MB", "online s", "online MB", "offline s", "offline MB", "colour (blue = paper run (.p) on a brain node with one row per NUMA node, orange = shared node or earlier engine version, red = earlier campaign at the current pins)", "left out of the paper (dropped.keep)"]); w.writerows(csvrows)
dr = [n for n, d in note_rows if d]
open(f"{OUT}/table1-extra-note.tex", "w").write(
    f"\\noindent {len(note_rows)} instances, {len(csvrows)} rows. " +
    (f"The reviews of 2026-11-02 left {len(dr)} of these instances out of the paper's tables (\\texttt{{dropped.keep}}): " + ", ".join(dr) + ". " if dr else "") +
    "Instances left out of this table: " + "; ".join(f"{n} {w}" for w, n in sorted(left.items())) + ".\n")
print(f"{len(note_rows)} instances, {len(csvrows)} rows; left out:", dict(left))
