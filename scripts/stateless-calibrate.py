#!/usr/bin/env python3
"""Calibrated stateless rows for the paper tables (owner, 2026-11-05): the stateless engine runs each instance of
Tables 2/3 only as long as the slower of the stateful and the offline engine took on it (rounded up to whole minutes,
at least 600 s), so that a stateless time-out costs no more than the other two engines' time. Reads the campaign CSVs
(tables.py output) and the set-aside stateless rows; emits the rows whose stateful and offline runs have both finished
(preferring the reserved-machine .p runs) and lists the others as not yet calibrated.
usage: stateless-calibrate.py <tables-dir> <stateless-rows.txt> <out.rows>"""
import csv, math, re, sys
tdir, src, out = sys.argv[1:4]
def load(f): return list(csv.DictReader(open(f"{tdir}/{f}")))
def key(r): return (r["program"], r["variant"], r["size"], r["model"])
def best(rows):
    d = {}
    for r in rows:
        if r["verdict"] not in ("conforms", "violation"): continue
        k = key(r); cur = d.get(k)
        rank = (1 if r["run id"].endswith(".p") or ".p." in r["run id"] else 0, r["started"])
        if cur is None or rank > cur[0]: d[k] = (rank, float(r["wall s"]))
    return {k: v[1] for k, v in d.items()}
sf, off = best(load("campaign-stateful.csv")), best(load("campaign-offline-tf.csv"))
# the row id names program, size and variant: C2.<dir>.impl_<size>_<model>_<variant>.t16.p (synthetic: impl_<size>_<model>)
attrs = {}
for f in ("campaign-stateful.csv", "campaign-offline-tf.csv"):
    for r in load(f):
        if r["run id"].endswith(".p") or ".p." in r["run id"]:
            base = re.sub(r"^C\d\.|^C3J\.", "", r["run id"]).replace(".t16.p", "").replace(".p.m", "")
            attrs[base] = key(r)
emitted, waiting = [], []
for l in open(src).read().splitlines():
    if not l.strip() or l.startswith("#"): continue
    rid = l.split()[0]; base = re.sub(r"^C2\.", "", rid).replace(".t16.p", "")
    k = attrs.get(base)
    if k is None or k not in sf or k not in off:
        waiting.append((rid, "stateful" if k is None or k not in sf else "", "offline" if k is None or k not in off else "")); continue
    limit = max(600, 60 * math.ceil(max(sf[k], off[k]) / 60))
    parts = l.split(" "); parts[2] = str(limit)
    emitted.append(" ".join(parts))
with open(out, "w") as fh:
    fh.write("# calibrated stateless rows (scripts/stateless-calibrate.py): time limit = the slower of the stateful and offline runs of the instance, rounded up to minutes, at least 600 s\n")
    fh.write("\n".join(emitted) + ("\n" if emitted else ""))
print(f"{len(emitted)} rows calibrated -> {out}; {len(waiting)} waiting for their stateful/offline run:")
for rid, a, b in waiting: print("  ", rid, a, b)
for l in emitted: print("  ", l.split()[0], "limit", l.split()[2])
