#!/usr/bin/env python3
"""Push every size ladder of the campaign to its time tier (owner, 2026-11-02): for each (family, variant, model)
whose largest finished size ended ok and has no larger size that timed out, emit the next size on the diagonal
(every size axis +1, or a larger step while the top row is fast), as C1/C2 conf rows, C3E export rows (spec and impl)
and the C3J join, with needok on the top row. Honours cluster/campaign/inputs/grid-axes.tsv (axes and the program's
own bounds). Reads the results repository and the committed rows files; writes <out>/<name>.rows(.tsv).
usage: extend-frontier.py <brain-data> <daavar> <out-dir> [--tag fN]"""
import sys, re, csv, glob, os, collections, argparse
ap = argparse.ArgumentParser(); ap.add_argument("bd"); ap.add_argument("dv"); ap.add_argument("out"); ap.add_argument("--tag", default="f")
A = ap.parse_args()
PIN_ORDER = ["a3a7e1b", "37f37fd", "af56bae", "088b6db", "2a3f8fd", "9ed1131", "9a0629e"]
def rank(r): c = (r.get("traceforge_commit") or "")[:7]; return (PIN_ORDER.index(c) if c in PIN_ORDER else -1, r.get("started_at") or "")
res = {}
for f in glob.glob(A.bd + "/results/*.tsv"):
    for r in csv.DictReader(open(f), delimiter="\t"):
        i = r["run_id"]
        if i not in res or rank(r) >= rank(res[i]): res[i] = r
rows = {}  # id -> (mem, to, dep, cmd)
for f in glob.glob(A.dv + "/cluster/campaign/*/*.rows"):
    for line in open(f):
        p = line.rstrip("\n").split(" ", 3)
        if len(p) < 4 or p[0] in rows: continue
        dep = None; cmd = p[3]
        if cmd.startswith("needok:"): dep, cmd = cmd.split(" ", 1)[0][7:], cmd.split(" ", 1)[1]
        rows[p[0]] = (p[1], p[2], dep, cmd)
axes_of, axes_max = {}, {}
for l in open(A.dv + "/cluster/campaign/inputs/grid-axes.tsv"):
    if l.startswith("#") or not l.strip(): continue
    f = l.rstrip("\n").split("\t"); axes_of[f[0]] = [x == "1" for x in f[1].split()]
    if len(f) > 2 and f[2].strip(): axes_max[f[0]] = [int(x) if x.isdigit() else None for x in f[2].split()]
EXPL = set(tuple(l.rstrip("\n").split("\t")[:2]) for l in open(A.dv + "/cluster/campaign/inputs/exploratory-variants.tsv") if not l.startswith("#") and l.strip())
Q27 = {"bank-grove", "fencing-grove", "kv-exactly-once-grove", "kv-snapshot-isolation-tla", "rpc-at-most-once-aneris", "calculator-disel"}
RX = re.compile(r"^(C1|C2|C3E)\.([A-Za-z0-9-]+)\.(impl|spec)_(.*)_(fifo|bag|p2p|asyn|causal)(\.[xg]\d*|\.f\d*)*\.t16$")
def parse(i):
    m = RX.match(i)
    if not m: return None
    pre, fam, side, mid, model = m.groups()[:5]
    toks = mid.split("_"); nums = tuple(int(t) for t in toks if t.isdigit()); words = [t for t in toks if not t.isdigit()]
    return pre, fam, side, "-".join(words) if words else side, model, nums
groups = collections.defaultdict(list)
for i, r in res.items():
    p = parse(i)
    if not p: continue
    groups[p[:5]].append((p[5], i, r))
HDR = "id\ttable\tfamily\tsize\tmodel\tvariant\tengine\tthreads\texpected_verdict\tnote\ttier\tkind\tspec_tool\timpl_tool\tspec_lang\timpl_lang\tmode\texploration\tbenchmark\tspec_case\timpl_case"
out = collections.defaultdict(lambda: ([], [HDR])); stopped = []
def step_for(nums, wall_ms):
    w = float(wall_ms or 0) / 1000
    if w < 10: return lambda n: max(n + 1, 2 * n)
    if w < 600: return lambda n: max(n + 1, int(n * 1.25))
    return lambda n: n + 1
def emit(name, rid, mem, to, dep, cmd, attrs):
    o = out[name]; o[0].append(f"{rid} {mem} {to} {'needok:' + dep + ' ' if dep else ''}{cmd}"); o[1].append("\t".join(attrs))
def attrs(i, fam, size, model, var, eng, thr, tier, kind, mode, sc, ic):
    return [i, "C", fam, size, model, var, eng, str(thr), "readme", "frontier", tier, kind, "traceforge", "traceforge", "rust", "rust", mode, "full", fam, sc, ic]
done_specs = set()
for (pre, fam, side, var, model), items in sorted(groups.items()):
    if pre == "C3E" and side == "spec": continue            # specs follow their implementation ladders
    if fam in Q27 and pre in ("C1", "C2"): continue
    if (fam, var) in EXPL: continue
    items.sort()
    if any(r.get("stop_reason") in ("timeout", "memory") for _, _, r in items): continue   # this ladder has reached its tier
    top_nums, top_id, top = items[-1]
    if top.get("stop_reason") != "ok" or top_id not in rows: continue
    k = len(top_nums); ax = (axes_of.get(fam, [True] * k) + [True] * k)[:k]; mx = (axes_max.get(fam, []) + [None] * k)[:k]
    if not any(ax): continue
    st = step_for(top_nums, top.get("wall_ms"))
    nxt = tuple(st(n) if ax[i] else n for i, n in enumerate(top_nums))
    if any(mx[i] is not None and nxt[i] > mx[i] for i in range(k)):
        nxt = tuple(min(nxt[i], mx[i]) if mx[i] is not None else nxt[i] for i in range(k))
        if nxt == top_nums: stopped.append((pre, fam, var, model, top_nums, "program bound")); continue
    mem, to, _, cmd = rows[top_id]
    # substitute the numbers after "-- " in the command, in order
    head, args = cmd.rsplit(" -- ", 1); it = iter(nxt)
    nargs = " ".join(str(next(it)) if t.isdigit() else t for t in args.split())
    sz = "_".join(nargs.split()); size = ",".join(str(n) for n in nxt)
    tag = f".{A.tag}"
    if pre in ("C1", "C2"):
        rid = f"{pre}.{fam}.impl_{sz}_{model}{tag}.t16"
        emit(pre, rid, mem, to, top_id, f"{head} -- {nargs}", attrs(rid, fam, size, model, var, ("stateful" if pre == "C1" else "stateless") + "-parallel", 16, "c1h", "conf-traceforge", "in-process", "", f"{fam}.impl_{sz}_{model}"))
    else:  # C3E impl: also the spec export at the new size (once) and the join
        rid = f"C3E.{fam}.impl_{sz}_{model}{tag}.t16"
        emit("C3", rid, mem, to, top_id, f"{head} -- {nargs}", attrs(rid, fam, size, model, var, "must-partitioned", 16, "c1h", "export", "offline-batch", f"{fam}.impl_{sz}_{model}", f"{fam}.impl_{sz}_{model}"))
        # the spec row: from the join row that used this implementation's export, take the spec export's command
        spec_id = None
        for jid, (jm, jt, jd, jc) in rows.items():
            if jid.startswith("C3J.") and jd == top_id:
                m2 = re.search(r'/(C3E\.[^/]+)/export\.jsonl "\$EVAL_OUT"/' , jc); spec_id = m2.group(1) if m2 else None; break
        if spec_id and spec_id in rows:
            smem, sto, sdep, scmd = rows[spec_id]; shead, sargs = scmd.rsplit(" -- ", 1); it2 = iter(nxt)
            snargs = " ".join(str(next(it2)) if t.isdigit() else t for t in sargs.split()); ssz = "_".join(snargs.split())
            sp = parse(spec_id); svar = sp[3] if sp else "spec"
            sid = f"C3E.{fam}.spec_{ssz}_{model}{tag}.t16"
            if sid not in done_specs:
                done_specs.add(sid); emit("C3", sid, smem, sto, spec_id, f"{shead} -- {snargs}", attrs(sid, fam, size, model, "spec-" + svar, "must-partitioned", 16, "c1h", "export", "offline-batch", f"{fam}.spec_{ssz}_{model}", f"{fam}.spec_{ssz}_{model}"))
            jid = f"C3J.{fam}.impl_{sz}_{model}{tag}"
            emit("C3", jid, 16, 600, rid, f'"$DAAVAR"/traceforge/target/release/conf-join "$EVAL_OUT"/{sid}/export.jsonl "$EVAL_OUT"/{rid}/export.jsonl', attrs(jid, fam, size, model, var, "join", 1, "c600", "cross-language", "offline-batch", f"{fam}.spec_{ssz}_{model}", f"{fam}.impl_{sz}_{model}"))
os.makedirs(A.out, exist_ok=True)
for name, (rs, tsv) in out.items():
    open(f"{A.out}/{name}.rows", "w").write("\n".join(rs) + "\n"); open(f"{A.out}/{name}.rows.tsv", "w").write("\n".join(tsv) + "\n"); print(f"{name}: {len(rs)} rows")
for s in stopped: print("bounded:", s)
