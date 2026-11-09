#!/usr/bin/env python3
"""Twins of the paper's Tables 3, 5 and 6 (eval2: realsw, agents-rust, agents; owner 2026-11-09): every finished offline
check (a join with verdict conforms or violation) that could sit in one of those tables and that the table does not show,
one row per pair (candidate $\\sqsubseteq$ reference) at the pair's biggest finished size, and only when the
paper's table shows the pair at no size or only at smaller sizes (owner 2026-11-09: rows that extend the paper).
The sides of a join are named from the two export ids in its command (conf-join takes the reference export first, the
candidate second), in the paper's words and macros, so the direction of a row never depends on the id's prefix.
Values: brain-data records only, composed as tables.py composes the campaign CSVs (export record = the exact id's latest
record that started before the join, on the join's node if there is one; G = complete + blocked graphs of the side's
export; T = the longer export + the join; M = the larger of the two exports' peaks summed and the join's peak); a row
whose join id is also in a campaign CSV is checked against that CSV row and every mismatch is printed.
Left out: Codex programs (an export directory ending in -sol), the seeded variants (reorder), joins without both export
records, and joins where a side's export has no graph (vacuous); raft-rs and MicroRaft at sizes with 0 election timeouts
are listed in the notes (known-problems 2026-11-09 10:20).
usage: paper-extra.py <repo-root> <out-dir>   (writes realsw-extra.tex, agents-rust-extra.tex, agents-extra.tex, notes.tex)"""
import sys, os, csv, glob, re, collections
from decimal import Decimal, ROUND_HALF_UP
R, OUT = sys.argv[1], sys.argv[2]
BD, TB = R + "/brain-data/results", R + "/PLDI/tex/tables"
# ---- records -------------------------------------------------------------------------------------------------------
recs = []
for f in sorted(glob.glob(BD + "/*.tsv")):
    with open(f) as fh:
        hdr = fh.readline().rstrip("\n").split("\t")
        for line in fh:
            v = line.rstrip("\n").split("\t")
            if len(v) == len(hdr): recs.append(dict(zip(hdr, v)))   # a line against another header is no record (tables.py)
INVALID, INVALID_STARTS = set(), set()
for l in open(R + "/daavar/cluster/campaign/inputs/invalid-rows.tsv"):
    if l.strip() and not l.startswith("#"):
        x = l.split(); (INVALID_STARTS.add((x[0], x[1])) if len(x) >= 2 else INVALID.add(x[0]))
def base_id(i): return re.sub(r"(\.c(1|48)h|\.cinf|\.j2|\.m|\.r\d+|\.p)+$", "", i)
def invalid(r): return (r["run_id"] in INVALID or base_id(r["run_id"]) in INVALID or (r["run_id"], r.get("started_at") or "") in INVALID_STARTS
                        or (r.get("valid") or "yes") == "no" or (r.get("stop_reason") or "") != "ok")
byexact = collections.defaultdict(list)
for r in recs: byexact[r["run_id"]].append(r)
for k in byexact: byexact[k].sort(key=lambda r: r.get("started_at") or "")
IMPORTS = {"sraft-e1": "C3E.omnipaxos.spec_correct_1_1_1_fifo.t16.c48h", "sraft-e2": "C3E.omnipaxos.spec_correct_2_1_1_fifo.t16.c48h"}   # tables.py
def export_record(ex, reader):   # tables.py's rule
    cands = [r for r in byexact.get(ex, []) if (r.get("started_at") or "") <= (reader.get("started_at") or "~")]
    same = [r for r in cands if r.get("node") == reader.get("node")]
    return same[-1] if same else (cands[-1] if cands else None)
import datetime as _dt
POLICY_FROM = "2026-11-03T19:29"   # tables.py: before the one-row-per-NUMA-node rule every run shared its machine
def _iv(r):
    try:
        t0 = _dt.datetime.fromisoformat(r["started_at"]); n = str(r.get("numa_node") or "").split("+")[0]
        return None if not r.get("node") or n == "" else (r["node"], n, t0, t0 + _dt.timedelta(seconds=float(r.get("wall_ms") or 0) / 1000.0))
    except Exception: return None
_slots = collections.defaultdict(list)
for _r in recs:
    _v = _iv(_r)
    if _v: _slots[_v[:2]].append((_v[2], _v[3], _r["run_id"]))
def contended(r):   # tables.py's rule: another run on the same host and NUMA node for more than 30 s of this run's wall time
    if (r.get("started_at") or "") < POLICY_FROM and not re.search(r"\.p(?=\.|$)", r["run_id"]): return True
    v = _iv(r)
    return bool(v) and any(i != r["run_id"] and (min(v[3], e) - max(v[2], s)).total_seconds() > 30 for s, e, i in _slots[v[:2]])
def num(x):
    try: return float(x)
    except Exception: return None
def graphs(x):   # tables.py: complete + blocked of the export; a JMC export records its count as iterations
    c = x.get("impl_complete") or x.get("iterations") or x.get("graphs_complete"); b = x.get("impl_blocked")
    if num(c) is None and num(b) is None: return num(x.get("impl_graphs_total"))
    return (num(c) or 0) + (num(b) or 0)
# ---- naming: an export id -> (block, label, tool) ------------------------------------------------------------------
TF, JMC = "\\tf", "\\jmc"
CLASSICAL = {"hermes": "\\textsc{hermes}", "vrr": "\\textsc{vrr}", "chain-replication": "\\textsc{chain-replication}", "ricart-agrawala": "\\textsc{ricart-agrawala}"}
def side(ex):
    """(block, label, tool, words) of an export id, or (None, reason) when the side is left out."""
    ex = IMPORTS.get(ex, ex); p = ex.split(".")
    if len(p) < 3: return None, "unnamed export " + ex
    pre, d, case = p[0], p[1], p[2]; tool = TF if pre == "C3E" else JMC
    kind = case.split("_")[0]; words = [t for t in case.split("_")[1:] if not t.isdigit() and t not in ("fifo", "bag", "p2p", "asyn", "causal")]
    if d.endswith("-sol") or "-sol-" in d or "sol" in words: return None, "Codex"
    if "reorder" in words: return None, "seeded variant (reorder)"
    J = pre in ("XCE", "XJE")
    if d == "raft-rs": return ("realsw", "\\sraft" if kind == "spec" else ("MicroRaft" if J else "raft-rs"), tool, words)
    if d == "omnipaxos": return ("realsw", "\\sraft" if kind == "spec" else "OmniPaxos", tool, words)
    if d == "omnipaxos-derived": return ("realsw", "\\somni", tool, words)
    if d.startswith("omnipaxos-"):
        n = {"omnipaxos-paper": "paper (Java)", "omnipaxos-spec": "\\sraft{} port (Java)", "omnipaxos-derived-impl": "\\somni{} port (Java)"}.get(d)
        return ("omni", n, tool, words) if n else (None, "unnamed Omni-Paxos program " + d)
    if d == "zookeeper-derived": return ("zab", "\\szab", tool, words)
    if d == "zookeeper-zab-rust": return ("zab", "port (Rust)", tool, words)
    if d == "zookeeper-zab-opus": return ("zab", "port (Java)", tool, words)
    if d == "zookeeper-zab": return ("zab", "transliteration", tool, words)
    if d == "cassandra-cas": return ("cas", "\\scas", tool, words)
    if d == "cassandra-derived": return ("cas", "\\spaxos", tool, words)
    if d == "cassandra-paxos-v1-rust": return ("cas", "port (Rust)", tool, words)
    if d == "cassandra-paxos-v1-opus": return ("cas", "port (Java)", tool, words)
    if d == "cassandra-paxos-v1": return ("cas", "transliteration", tool, words)
    if d == "pineapple-lin": return ("lin", "\\slin", tool, words)   # the register specification of three studies: the block is the other side's
    for s, m in (("chainpaxos", "\\schain"), ("swiftpaxos", "\\sswift"), ("pineapple", "\\spine")):
        if d == s + "-derived": return (s, m, tool, words)
        mm = re.fullmatch(s + r"-(paper|code)(-rust)?", d)
        if mm: return (s, f"{mm.group(1)} ({'Rust' if mm.group(2) else 'Java'})", tool, words)
    if d in CLASSICAL: return ("classical:" + d, "S" if kind == "spec" else ("Java" if J else "Rust"), tool, words)
    return None, "outside Tables 3, 5, 6: " + d
TITLE = {"zab": "ZooKeeper Zab (code only)", "cas": "Cassandra lightweight transactions (code only)", "chainpaxos": "ChainPaxos (paper only and code only)",
         "swiftpaxos": "SwiftPaxos (paper only and code only)", "pineapple": "Pineapple (paper only and code only)", "omni": "Omni-Paxos (four Java programs)",
         "classical": "Paper only, classical protocols (specification $S$ of the paper's property)"}
ORDER = {"realsw": ["realsw"], "agents-rust": ["zab", "cas", "chainpaxos", "swiftpaxos", "pineapple"], "agents": ["classical", "omni", "zab", "chainpaxos", "swiftpaxos", "pineapple", "cas"]}
def nrm(s): return re.sub(r"\s+", " ", s.replace("\\quad", "").replace("{}", "")).strip()
def is_spec(l): return l.startswith("\\s") or l == "S"
# ---- the joins -----------------------------------------------------------------------------------------------------
JOIN = ("C3J", "XC1", "XC2", "XJ1", "XJ2", "XT1", "XT2")
left = collections.Counter(); vacuous = []; zero_to = []; unnamed = collections.Counter(); found = {}
for r in recs:
    pre = r["run_id"].split(".")[0]
    if pre not in JOIN or invalid(r): continue
    v = (r.get("verdict") or "").lower(); v = {"reports": "violation", "violation (sampled)": "violation"}.get(v, v)
    if v not in ("conforms", "violation"): continue
    ex = [IMPORTS.get(e, e) for e in re.findall(r"/([A-Za-z0-9._-]+)/export\.jsonl", r.get("command", ""))]
    if len(ex) != 2: left["no two export paths in the command"] += 1; continue
    ref, cand = side(ex[0]), side(ex[1])
    bad = [x[1] for x in (ref, cand) if x[0] is None]
    if bad:
        for b in bad: (unnamed.__setitem__(b, unnamed[b] + 1) if b.startswith("unnamed") else None)
        left[bad[0].split(":")[0]] += 1; continue
    (rb, rl, rt, rw), (cb, cl, ct, cw) = ref, cand
    blocks = {rb, cb} - {"lin"}
    if blocks == {"realsw", "zab"}: blk = "zab"   # S_raft against S_zab belongs to the ZooKeeper study
    elif blocks == {"realsw", "omni"}: blk = "omni"   # a Java Omni-Paxos program against S_raft, S_omni or the library
    elif len(blocks) == 1: blk = blocks.pop()
    elif not blocks: blk = "lin"
    else: left["sides from two studies"] += 1; continue
    if blk == "lin": left["S_lin against itself"] += 1; continue
    nums = [int(t) for t in r["run_id"].split(".")[2].split("_") if t.isdigit()]
    if blk == "zab" and is_spec(cl) and is_spec(rl): nums = nums[:1]   # the paper's S_zab/S_raft rows: size e
    size = ",".join(map(str, nums))
    variant = [w for w in (cw or rw) if w not in ("correct", "impl", "spec")]
    if blk == "chainpaxos" and variant: size += " " + variant[0]
    sp, im = export_record(ex[0], r), export_record(ex[1], r)
    if not (sp and im): left["export record missing"] += 1; continue
    gr, gc = graphs(sp), graphs(im)
    if blk == "realsw" and len(nums) == 3 and nums[1] == 0 and ({"raft-rs", "MicroRaft"} & {rl, cl}):
        zero_to.append((cl, rl, size, r["run_id"])); continue
    if not gr or not gc: vacuous.append((cl, rl, size, r["run_id"])); continue
    if blk.startswith("classical"): tab = "agents"
    elif blk == "realsw": tab = "realsw"
    elif JMC in (rt, ct) or not (is_spec(rl) or is_spec(cl)): tab = "agents"   # a Java side, or two programs: Table 6
    else: tab = "agents-rust"
    label_c, label_r = cl, rl
    if blk == "cas" and variant:   # the paper: the variant follows S_paxos, \scas $\sqsubseteq$ \spaxos (v1nolin); a pair without S_paxos carries it in the size
        if cl == "\\spaxos": label_c = f"{cl} ({variant[0]})"
        elif rl == "\\spaxos": label_r = f"{rl} ({variant[0]})"
        elif variant[0] != "v1": size += " " + variant[0]
    t = [num(sp.get("wall_ms")), num(im.get("wall_ms")), num(r.get("wall_ms"))]
    m = [num(sp.get("peak_rss_kb")), num(im.get("peak_rss_kb")), num(r.get("peak_rss_kb"))]
    T = (max(t[0], t[1]) + t[2]) / 1000 if None not in t else None
    M = max(m[0] + m[1], m[2]) / 1024 if None not in m else None
    x = dict(tab=tab, blk=blk, cand=label_c, ref=label_r, ctool=ct, rtool=rt, size=size, nums=nums, gc=gc, gr=gr, gx=(num(r.get("uncovered_distinct") or r.get("uncovered_total")) or 0) if v == "violation" else 0,
             T=T, M=M, t=t, cont=contended(r) or contended(sp) or contended(im), verdict=v, id=r["run_id"], started=r.get("started_at") or "", node=r.get("node"), model=next((t for t in r["run_id"].split(".")[2].split("_") if t in ("fifo", "bag", "p2p", "asyn", "causal")), ""))
    k = (tab, blk, nrm(label_c), nrm(label_r), ct, rt, size, x["model"])
    if k not in found or x["started"] > found[k]["started"]: found[k] = x   # the latest finished run of the pair at the size
# ---- check against the campaign CSVs -------------------------------------------------------------------------------
csvrows = {}
for f in glob.glob(TB + "/campaign-*.csv"):
    for c in csv.DictReader(open(f)): csvrows[c["run id"]] = c
mism = []; incsv = 0
for x in found.values():
    c = csvrows.get(x["id"])
    if not c: continue
    incsv += 1
    def close(a, b, tol): return a is not None and num(b) is not None and abs(a - num(b)) <= tol
    if (c.get("contended") == "yes") != x["cont"]: mism.append(f"{x['id']}: contended {x['cont']}, CSV {c.get('contended')}")
    if not (close(x["gc"], c["impl graphs"], 0) and close(x["gr"], c["spec graphs"], 0) and close(x["T"], c["wall s"], max(0.051, 0.0006 * (x["T"] or 0))) and close(x["M"], c["peak MB"], 1)):
        mism.append(f"{x['id']}: record G {x['gc']:.0f}/{x['gr']:.0f} T {x['T']} M {x['M']}; CSV G {c['impl graphs']}/{c['spec graphs']} T {c['wall s']} M {c['peak MB']}")
# ---- the rows the paper's tables show ------------------------------------------------------------------------------
PBLK = {"ZooKeeper": "zab", "Cassandra": "cas", "ChainPaxos": "chainpaxos", "SwiftPaxos": "swiftpaxos", "Pineapple": "pineapple", "Omni-Paxos": "omni", "Paper only, classical": "classical"}
shown = collections.defaultdict(dict)   # (tab, blk, cand, ref, tools) -> {size: (G_cand, G_ref) as printed}
for tab in ("realsw", "agents-rust", "agents"):
    blk = "realsw" if tab == "realsw" else None; pair = None; prog = None
    for line in open(f"{TB}/eval2/{tab}.tex"):
        if "\\emph{" in line:
            blk = next((b for w, b in PBLK.items() if w in line), "?"); pair = None; continue
        if "&" not in line or "multicolumn" in line or line.lstrip().startswith(("%", "pair ")) or re.match(r"\s*\\(toprule|midrule|bottomrule|cmidrule|begin|end|caption|label|scriptsize)", line): continue
        cells = [c.strip() for c in line.split("&")]
        if cells[0]: pair = cells[0].replace("$\\sqsubseteq S$", "$\\sqsubseteq$ $S$")
        if not pair: continue
        tools = cells[2].replace("\\ ", " ").split(" / ") if tab == "realsw" else None
        size = cells[1]; g = [c.replace("\\,", "") for c in cells[(3 if tab == "realsw" else 2):][:2]]
        for pr in re.split(r",\s*(?=[^,]*\$\\sqsubseteq\$)", pair):
            a, b = [nrm(y) for y in pr.split("$\\sqsubseteq$")]
            k_blk = blk
            if blk == "classical":
                mm = re.match(r"(\\textsc\{[^}]*\})\s*(.*)", a)
                if mm: prog, a = mm.group(1), mm.group(2)
                k_blk = "classical:" + next(d for d, n in CLASSICAL.items() if n == prog)
                b = {"$S$": "S", "Java twin": "Java", "Rust twin": "Rust"}.get(b.replace("S$", "$S$") if b == "S$" else b, b)
                if b.startswith("$S"): b = "S"
            for bb in [b]:
                shown[(tab, k_blk, a, bb, tuple(t.strip() for t in tools) if tools else None)][size] = tuple(g)
def fkey(x): return (x["tab"], x["blk"], nrm(x["cand"]), nrm(x["ref"]), (x["ctool"], x["rtool"]) if x["tab"] == "realsw" else None)
for k, d in shown.items():   # a ChainPaxos size printed without its variant (\slin $\sqsubseteq$ \schain, 3): the record with the printed graph counts names it
    if k[1] != "chainpaxos": continue
    for sz in [z for z in d if not re.search(r"[a-z]", z)]:
        hit = [x for x in found.values() if fkey(x) == k and x["size"].split(" ")[0] == sz and (str(int(x["gc"])), str(int(x["gr"]))) == d[sz]]
        if len(hit) == 1: d[hit[0]["size"]] = d.pop(sz)
paper_check = []   # a paper row whose graph counts differ from the record of its pair and size
for k, d in shown.items():
    for sz, g in d.items():
        xs = [x for x in found.values() if fkey(x) == k and x["size"] == sz]
        if xs and not any((str(int(x["gc"])), str(int(x["gr"]))) == g for x in xs): paper_check.append(f"{k[1]} {k[2]} <= {k[3]} {sz}: paper G {g[0]}/{g[1]}, record " + ", ".join(f"{int(x['gc'])}/{int(x['gr'])} ({x['id']})" for x in xs))
        if not xs: paper_check.append(f"{k[1]} {k[2]} <= {k[3]} {sz}: paper row without a finished record in brain-data")
def shown_sizes(x):
    tools = (x["ctool"], x["rtool"]) if x["tab"] == "realsw" else None
    return set(shown.get((x["tab"], x["blk"], nrm(x["cand"]), nrm(x["ref"]), tools), {}))
def ssum(s): return (sum(int(n) for n in re.findall(r"\d+", s)), [int(n) for n in re.findall(r"\d+", s)])
# ---- twin rows: per pair, the biggest size not shown -----------------------------------------------------------------
pairs = collections.defaultdict(list)
for k, x in found.items():
    vkey = x["size"].split(" ")[1] if " " in x["size"] else ""   # ChainPaxos: mixed and local are different pairs
    pairs[(x["tab"], x["blk"], x["cand"], x["ref"], x["ctool"], x["rtool"], vkey, x["model"])].append(x)
dropped_smaller = []; twins = collections.defaultdict(list); bigger = collections.defaultdict(list); shown_used = set()
for pk, xs in pairs.items():
    ss = shown_sizes(xs[0])
    rest = [x for x in xs if x["size"] not in ss]
    if not rest: continue
    top = max(ssum(x["size"])[0] for x in rest)
    bests = [x for x in rest if ssum(x["size"])[0] == top]   # the biggest size by the sum of its parameters; sizes tied on the sum (1,2,1 and 1,1,2) are all kept
    ssv = [s for s in ss if (s.split(" ")[1] if " " in s else "") == pk[6]]   # the paper's sizes of the same pair (ChainPaxos: of the same variant)
    if ssv and not all(top > ssum(s)[0] for s in ssv): dropped_smaller += bests; continue   # owner 2026-11-09: only rows that extend the paper; a pair the paper shows at an equal or larger size (sum) has no twin row
    for best in sorted(bests, key=lambda x: ssum(x["size"])):
        twins[best["tab"]].append(best)
        if ssv: bigger[best["tab"]].append((best, ssv))
# ---- emit ----------------------------------------------------------------------------------------------------------
def dsec(v, nd): return str(Decimal(repr(v)).quantize(Decimal(1).scaleb(-nd), rounding=ROUND_HALF_UP))
def grp(s):   # 13\,530 as in the paper: a thin space from five digits
    i, _, f = s.partition("."); i = i if len(i) < 5 else re.sub(r"(\d)(?=(\d{3})+$)", r"\1\\,", i); return i + ("." + f if f else "")
def fT(x):
    if x["T"] is None: return "--"
    if x["T"] == 0: return "$<$0.01"
    return grp(dsec(x["T"], 1) if x["T"] >= 0.1 else dsec(x["T"], 2)) + ("$^{\\dagger}$" if x["cont"] else "")
def fM(x): return "--" if x["M"] is None else grp(str(int(Decimal(repr(x["M"])).quantize(Decimal(1), rounding=ROUND_HALF_UP))))
def fG(v): return grp(str(int(v)))
def emit(tab, items):
    rs = tab == "realsw"
    cols = "@{}lll" + "r" * 7 + "@{}" if rs else "@{}ll" + "r" * 5 + "@{}"
    nc = 10 if rs else 7
    L = [f"\\begin{{longtable}}{{{cols}}}", "\\toprule"]
    if rs: L += [" & & & & & & \\multicolumn{2}{c}{\\offline} & \\multicolumn{2}{c}{\\online} \\\\", "\\cmidrule(lr){7-8}\\cmidrule(lr){9-10}",
                 "pair & size & tools & $G_{\\mathit{cand}}$ & $G_{\\mathit{ref}}$ & $G_x$ & T & M & T & M \\\\"]
    else: L += ["pair & size & $G_{\\mathit{cand}}$ & $G_{\\mathit{ref}}$ & $G_x$ & T & M \\\\"]
    L += ["\\midrule"]
    order = ORDER[tab]
    def bkey(x):
        b = x["blk"].split(":")[0]; return (order.index(b) if b in order else 99, x["blk"], x["cand"], x["ref"], ssum(x["size"]))
    prev_b = prev_p = None
    for x in sorted(items, key=bkey):
        b = x["blk"].split(":")[0]
        if not rs and b != prev_b:
            if prev_b is not None: L.append("\\midrule")
            L.append(f"\\multicolumn{{{nc}}}{{@{{}}l}}{{\\emph{{{TITLE[b]}}}}} \\\\"); prev_b = b; prev_p = None
        if x["blk"].startswith("classical:"):
            n = CLASSICAL[x["blk"].split(":")[1]]; refl = "$S$" if x["ref"] == "S" else x["ref"] + " twin"
            label = f"{n} {x['cand']} $\\sqsubseteq$ {refl}" + (" (Java $S$)" if x["ref"] == "S" and x["rtool"] == JMC else "")
        else: label = f"{x['cand']} $\\sqsubseteq$ {x['ref']}"
        if x["model"] not in ("fifo", "") and not x["blk"].startswith("classical"): label += f"$^{{\\mathrm{{{x['model']}}}}}$"
        cells = [label if label != prev_p else "", x["size"]] + ([f"{x['ctool']}\\ / {x['rtool']}"] if rs else []) + [fG(x["gc"]), fG(x["gr"]), fG(x["gx"]), fT(x), fM(x)] + (["", ""] if rs else [])
        L.append(" & ".join(cells) + " \\\\"); prev_p = label
    if not items: L.append(f"\\multicolumn{{{nc}}}{{@{{}}l}}{{no row}} \\\\")
    L += ["\\bottomrule", "\\end{longtable}"]
    open(f"{OUT}/{tab}-extra.tex", "w").write("\n".join(L) + "\n")
os.makedirs(OUT, exist_ok=True)
for tab in ("realsw", "agents-rust", "agents"): emit(tab, twins[tab])
def plabel(x): return f"{x['cand']} $\\sqsubseteq$ {x['ref']}" if not x["blk"].startswith("classical:") else f"{CLASSICAL[x['blk'].split(':')[1]]} {x['cand']} $\\sqsubseteq$ {'$S$' if x['ref'] == 'S' else x['ref'] + ' twin'}"
N = []
for tab, name in (("realsw", "realsw"), ("agents-rust", "agentsrust"), ("agents", "agents")):
    bs = sorted(bigger[tab], key=lambda y: (y[0]["blk"], y[0]["cand"], y[0]["ref"]))
    g = {}   # one entry per pair: sizes tied on the sum are listed together
    for x, ssv in bs: g.setdefault(plabel(x), (ssv, []))[1].append(x["size"])
    s = "; ".join(f"{p} ({'; '.join(sorted(ssv, key=ssum))} in the paper, {'; '.join(sz)} here)" for p, (ssv, sz) in g.items()) or "none"
    extra = ""
    if tab == "realsw":
        z = sorted({(c, rr, sz) for c, rr, sz, _ in zero_to}, key=lambda y: (y[0], y[1], ssum(y[2])))
        extra = (" Not results, left out: the joins of raft-rs or MicroRaft at a size with 0 election timeouts (their exports explore no complete graph, the conformance is vacuous): "
                 + ("; ".join(f"{c} $\\sqsubseteq$ {rr} ({sz})" for c, rr, sz in z) if z else "none in brain-data yet") + ".")
    N.append(f"\\newcommand{{\\note{name}}}{{\\noindent {len(twins[tab])} rows. Pairs the paper shows at a smaller size than the row here: {s}.{extra}}}")
open(f"{OUT}/notes.tex", "w").write("\n".join(N) + "\n")
print(f"joins named: {len(found)} (pair, size) cells; twin rows: " + ", ".join(f"{t} {len(twins[t])}" for t in ("realsw", "agents-rust", "agents")))
print(f"checked against the campaign CSVs: {incsv} rows, {len(mism)} mismatches"); [print("  MISMATCH", m) for m in mism]
print("left out:", dict(left)); print("unnamed sides:", dict(unnamed))
print(f"pairs left out because the paper shows them at an equal or larger size: {len(dropped_smaller)}");
print("vacuous (a side with no graph):", vacuous); print("0 election timeouts:", zero_to)
print(f"paper rows: {sum(len(d) for d in shown.values())} (pair, size) cells; checked against the records: {len(paper_check)} differences"); [print("PAPER CHECK", m) for m in paper_check]
for t in ("realsw", "agents-rust", "agents"):
    for x in sorted(twins[t], key=lambda x: (x["blk"], x["cand"], x["ref"])): print(t, x["blk"], x["cand"], "<=", x["ref"], x["ctool"], x["rtool"], x["size"], x["verdict"], int(x["gc"]), int(x["gr"]), x["gx"], fT(x), fM(x), x["id"], x["node"], x["started"][:16])
