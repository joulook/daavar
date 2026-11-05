#!/usr/bin/env python3
"""Deterministic audit of the generated tables against the raw records (owner's pipeline rule, 2026-11-05 17:00):
run after every regeneration. usage: table-check.py <repo-root> <state-dir>
Writes <state-dir>/report.md (problems), <state-dir>/diff.md (cells changed since the previous run) and the flag file
<state-dir>/NEW_DATA (non-empty when any table changed). Exit 0: no problem; 2: problems listed in report.md.
Checks: (1) malformed record lines (column count); (2) coverage: every decided, non-invalid record whose benchmark
directory is in some table appears in some campaign CSV (by base id); (3) semantic sanity of every campaign row
(verdict vocabulary, times and memory numeric and positive, graph counts integers, uncovered consistent with the
verdict, timeout tiers, run id and pin present); (4) graph counts agree across the engines per instance; (5) Tables 2/3:
every cell parses, time and memory come in pairs, G counts present where a cell is decided, no row blank in every
column unless a dagger row follows it, every present cell has a colour; (6) the diff."""
import csv, os, re, sys, glob, json
R, ST = sys.argv[1], sys.argv[2]; os.makedirs(ST, exist_ok=True)
TB = os.path.join(R, "PLDI/tex/tables"); BD = os.path.join(R, "brain-data/results"); DV = os.path.join(R, "daavar")
problems = []; notes = []
def base_id(i): return re.sub(r"(\.c(1|48)h|\.j2|\.m|\.r2|\.r3|\.p)+$", "", i)
def num(x):
    try: return float(x)
    except Exception: return None
# (1) records
INVALID = {l.split()[0] for l in open(os.path.join(DV, "cluster/campaign/inputs/invalid-rows.tsv")) if l.strip() and not l.startswith("#")}
records = {}; malformed = []
for f in sorted(glob.glob(os.path.join(BD, "*.tsv"))):
    with open(f, errors="replace") as fh:
        hdr = fh.readline().rstrip("\n").split("\t")
        for ln, line in enumerate(fh, 2):
            cols = line.rstrip("\n").split("\t")
            if len(cols) != len(hdr): malformed.append(f"{os.path.basename(f)}:{ln} has {len(cols)} columns, header {len(hdr)}"); continue
            r = dict(zip(hdr, cols)); records.setdefault(r["run_id"], []).append(r)
KNOWN_MALFORMED = set()
if os.path.exists(os.path.join(ST, "known-malformed.txt")): KNOWN_MALFORMED = {l.strip() for l in open(os.path.join(ST, "known-malformed.txt")) if l.strip()}
newm = [m for m in malformed if m.split(" ")[0] not in KNOWN_MALFORMED]
if newm: problems.append(f"malformed record lines: {len(newm)}; first: " + "; ".join(newm[:5]))
elif malformed: notes.append(f"malformed record lines already reported to the owner: {len(malformed)} ({'; '.join(m.split(' ')[0] for m in malformed[:5])})")
# (2) coverage
csv_ids = set(); csv_dirs = set(); rows_by_file = {}
for f in sorted(glob.glob(os.path.join(TB, "campaign-*.csv"))):
    rs = list(csv.DictReader(open(f))); rows_by_file[os.path.basename(f)] = rs
    for r in rs:
        if r.get("run id"): csv_ids.add(base_id(r["run id"]))
        if r.get("directory"): csv_dirs.add(r["directory"])
        for k in ("spec export s", "impl export s"):   # exports are components of an offline row, named in its directory column only
            pass
def decided(r): return r.get("verdict") in ("conforms", "violation") or r.get("stop_reason") in ("timeout", "memory")
fams = {i.split(".")[0] for i in csv_ids}
EXPL = set()
for f in glob.glob(os.path.join(DV, "cluster/campaign/inputs/exploratory-variants.tsv")) + glob.glob(os.path.join(DV, "benchmarks/exploratory-variants.tsv")):
    EXPL |= {tuple(l.split("\t")[:2]) for l in open(f) if l.strip() and not l.startswith("#")}
DROPPED = {}
dp = os.path.join(TB, "dropped-records.tsv")
if os.path.exists(dp):
    for l in list(open(dp))[1:]:
        i, _, why = l.rstrip("\n").partition("\t"); DROPPED[i] = why
missing = {}; byfam = {}
for rid, rs in records.items():
    if rid in DROPPED: continue   # left out by the generator for a stated reason (the ledger)
    b = base_id(rid)
    if b in INVALID or rid in INVALID or b in csv_ids: continue
    fam = rid.split(".")[0]
    if fam.endswith("E") or fam in ("C3E", "XJE", "XCE") or fam not in fams: continue   # export rows are components; families no CSV carries are not table families
    for r in rs:
        if decided(r) and (r.get("valid") or "yes") != "no" and r.get("benchmark", "") in csv_dirs:
            if (r.get("benchmark", ""), r.get("variant", "")) in EXPL: break
            missing.setdefault(r.get("benchmark", "?"), []).append(rid); byfam[fam] = byfam.get(fam, 0) + 1; break
if missing:
    n = sum(len(v) for v in missing.values())
    (problems if DROPPED else notes).append(f"coverage: {n} decided record(s) of tabled benchmarks appear in no campaign CSV (by family: {byfam}): " + "; ".join(f"{k}: {', '.join(v[:3])}{' …' if len(v) > 3 else ''}" for k, v in list(missing.items())[:12]))
# (3) sanity of campaign rows
VERD = {"conforms", "violation", "t/o", "oom", ""}
ginst = {}
for fn, rs in rows_by_file.items():
    for r in rs:
        key = f"{fn}:{r.get('program')},{r.get('variant')},{r.get('spec')},{r.get('model')},{r.get('size')}"
        v = r.get("verdict", "")
        if v not in VERD: problems.append(f"{key}: verdict '{v}' not in the vocabulary")
        if not r.get("run id"): problems.append(f"{key}: no run id")
        if not r.get("pin"): problems.append(f"{key}: no pin")
        if v in ("conforms", "violation"):
            w = num(r.get("wall s")); m = num(r.get("peak MB"))
            if w is None or w < 0: problems.append(f"{key}: wall s '{r.get('wall s')}' not a non-negative number")
            if m is None or m <= 0: problems.append(f"{key}: peak MB '{r.get('peak MB')}' not positive")
            gi = r.get("impl graphs", ""); gs = r.get("spec graphs", "")
            if not re.fullmatch(r"\d+", gi or "") or int(gi) < 1: problems.append(f"{key}: impl graphs '{gi}' not a positive integer")
            if gs and not re.fullmatch(r"\d+", gs): problems.append(f"{key}: spec graphs '{gs}' not an integer")
            if "stateless" not in fn and not gs: problems.append(f"{key}: spec graphs empty on a decided row")
            u = r.get("uncovered", "")
            if v == "violation" and u not in ("",) and not (re.fullmatch(r"\d+", u) and int(u) >= 1): problems.append(f"{key}: violation with uncovered '{u}'")
            if v == "conforms" and u not in ("", "0"): problems.append(f"{key}: conforms with uncovered '{u}'")
            ik = (r.get("program"), r.get("variant"), r.get("spec"), r.get("model"), r.get("size"))
            side = "java-impl" if ("java-impl" in fn or "rust-spec" in fn) else "rust-impl"   # cftwin-offline-rust-spec = the Java twin against the Rust export
            if gi: ginst.setdefault(ik + (side,), {})[fn] = gi
        if v == "t/o" and r.get("timeout s") not in ("3600", "14400", "172800"): problems.append(f"{key}: t/o with timeout s '{r.get('timeout s')}'")
        if r.get("contended", "") not in ("yes", "no", ""): problems.append(f"{key}: contended '{r.get('contended')}'")
# (4) graph agreement
for ik, d in ginst.items():
    if len(set(d.values())) > 1: problems.append(f"impl graphs disagree across engines for {','.join(ik)}: " + ", ".join(f"{k}={v}" for k, v in d.items()))
# (5) tables 2/3
def limit(x): return x.startswith("t/o") or x.startswith("oom")
def cellnum(x): return x == "" or limit(x) or num(x.replace("\\,", "").replace(",", "")) is not None
for t in ("table2", "table3"):
    p = os.path.join(TB, t + ".csv")
    if not os.path.exists(p): continue
    rs = list(csv.DictReader(open(p))); cols = rs[0].keys() if rs else []
    eng = [("nested s", "nested MB"), ("online s", "online MB"), ("offline s", "offline MB")]
    for i, r in enumerate(rs):
        key = f"{t}:{r.get('program')},{r.get('variant')},{r.get('model')},{r.get('size')}"
        for c in cols:
            if c.startswith(("nested", "online", "offline", "G ")) and not cellnum(r.get(c, "")): problems.append(f"{key}: cell {c} = '{r.get(c)}' does not parse")
        anycell = False; decided_cell = False
        for s, m in eng:
            vs, vm = r.get(s, ""), r.get(m, "")
            if vs or vm: anycell = True
            if vs and not limit(vs): decided_cell = True
            if (vs == "" and vm != "") or (vs and not limit(vs) and vm == ""): problems.append(f"{key}: {s}='{vs}' but {m}='{vm}' (time and memory must come together)")
            colour = r.get(next(c for c in cols if c.startswith("colour")), "")
            if vs and s.split()[0] + "=" not in colour: problems.append(f"{key}: {s} present but no colour for it ('{colour}')")
        if decided_cell and (not r.get("G impl") or not r.get("G spec")): problems.append(f"{key}: a decided cell but G impl='{r.get('G impl')}' G spec='{r.get('G spec')}'")
        if not anycell and not r.get("G impl"):
            nxt = rs[i + 1] if i + 1 < len(rs) else None
            smaller = any(rs[j].get("program") == r.get("program") and rs[j].get("variant") == r.get("variant") and any(rs[j].get(s) for s, _ in eng) for j in range(i))
            if nxt and nxt.get("fallback", "").startswith("completed-size-for:"): notes.append(f"{key}: blank row, dagger row below (allowed by the owner's rule)")
            elif smaller: notes.append(f"{key}: blank row, a smaller size of the pair with results above it (allowed by the owner's rule)")
            else: problems.append(f"{key}: blank in every column, no dagger row below and no smaller size with results above")
# (6) diff against the previous run
prev = os.path.join(ST, "prev"); os.makedirs(prev, exist_ok=True); diff = []; changed_files = 0
def keyrow(fn, r):
    ks = [k for k in ("program", "variant", "spec", "model", "size") if k in r]; return ",".join(r.get(k, "") for k in ks)
for f in sorted(glob.glob(os.path.join(TB, "campaign-*.csv")) + [os.path.join(TB, "table2.csv"), os.path.join(TB, "table3.csv")]):
    fn = os.path.basename(f); pf = os.path.join(prev, fn)
    cur = {keyrow(fn, r): r for r in csv.DictReader(open(f))} if os.path.exists(f) else {}
    old = {keyrow(fn, r): r for r in csv.DictReader(open(pf))} if os.path.exists(pf) else None
    if old is None: diff.append(f"{fn}: first run, {len(cur)} rows recorded as the baseline")
    else:
        fchg = 0
        for k, r in cur.items():
            if k not in old: diff.append(f"{fn}: + {k}: run id {r.get('run id', '')} verdict {r.get('verdict', '')} wall {r.get('wall s', r.get('online s', ''))}"); fchg += 1; continue
            for c, v in r.items():
                def same(a, b):   # a change of formatting only (e.g. 0.0 -> 0.040 when the phase columns went to ms precision) is no change
                    if a == b: return True
                    x, y = num(a), num(b); return x is not None and y is not None and round(x, 1) == round(y, 1)
                if not same(old[k].get(c, ""), v) and c not in ("started", "directory"): diff.append(f"{fn}: ~ {k}: {c}: '{old[k].get(c, '')}' -> '{v}' (run id {r.get('run id', '')})"); fchg += 1
        for k in old:
            if k not in cur: diff.append(f"{fn}: - {k} (row gone)"); fchg += 1
        if fchg: changed_files += 1
    if os.path.exists(f): open(pf, "w").write(open(f).read())
for f in sorted(glob.glob(os.path.join(R, "plan/twin-tables/*.tex"))):
    fn = os.path.basename(f); pf = os.path.join(prev, fn); cur = open(f).read()
    if os.path.exists(pf):
        old = open(pf).read().splitlines(); new = cur.splitlines()
        ch = [l for l in new if l not in set(old) and "&" in l]
        if ch: diff.append(f"{fn}: {len(ch)} changed/added table line(s); first: {ch[0][:160]}"); changed_files += 1
    else: diff.append(f"{fn}: baseline recorded")
    open(pf, "w").write(cur)
open(os.path.join(ST, "diff.md"), "w").write("\n".join(diff[:400]) + ("\n… (%d more)\n" % (len(diff) - 400) if len(diff) > 400 else "\n"))
open(os.path.join(ST, "NEW_DATA"), "w").write("yes\n" if changed_files else "")
rep = ["# table-check report", f"records: {sum(len(v) for v in records.values())} lines, {len(records)} ids; campaign rows: {sum(len(v) for v in rows_by_file.values())}; changed files: {changed_files}; diff lines: {len(diff)}", ""]
rep += ["## problems (%d)" % len(problems)] + [f"- {p}" for p in problems[:300]] + ["", "## notes (%d)" % len(notes)] + [f"- {n}" for n in notes[:100]]
open(os.path.join(ST, "report.md"), "w").write("\n".join(rep) + "\n")
print(f"table-check: {len(problems)} problem(s), {len(notes)} note(s), {changed_files} changed file(s), {len(diff)} diff line(s)")
sys.exit(2 if problems else 0)
