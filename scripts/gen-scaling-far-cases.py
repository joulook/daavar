#!/usr/bin/env python3
"""Scaling-far cases (filler for idle nodes, owner 2026-11-03 00:20: every core busy): the scaling-next plan moved by an offset; owner 2026-11-02 evening: as far as we can go, 2026-11-02): clone the campaign's Rust count rows and Java cases at the
plan's sizes, so gen-rows-campaign.py can emit the scaling rows (C1, C3 exports + joins, XJE, XON1/2, twins).
usage: gen-scaling-far-cases.py <inputs dir> <out rust rows> <out java cases> <offset>   (offset: how many sizes beyond the scaling-next plan; the filler rows of 2026-11-03)
Template = the existing case of the same (dir, side, model, variant); the size numbers are replaced in
the id, the command and the Java class arguments. A spec case is cloned per listed size with the given
spec variant word ("" = the single spec)."""
import sys, re, glob, csv
inp, out_r, out_j = sys.argv[1:4]; OFF = int(sys.argv[4])
# (dir, model, impl variants -> sizes, spec variant word, spec sizes)
PLAN = [
 ("reliable-channel-aneris", "bag", {"correct": ["7"], "no-seqno": ["7"]}, "", ["7"]),
 ("replicated-log-aneris", "fifo", {"correct": ["10"], "asyn-fault": ["10"]}, "", ["10"]),
 ("replicated-log-esop20", "bag", {"correct": ["7"], "early-reply": ["6"]}, "", ["6", "7"]),
 ("two-pc-abstain-kragl", "bag", {"correct": ["10"], "off-by-one": ["10"]}, "", ["10"]),
 ("two-pc-disel", "bag", {"correct": ["5"], "skip-ack-loop": ["5"]}, "", ["5"]),
 ("two-pc-trillium", "bag", {"correct": ["9"], "ignore-abort-vote": ["9"]}, "", ["9"]),
 ("ring-election-igloo", "bag", {"correct": ["7"], "no-compare": ["7"]}, "", ["7"]),
 ("election", "fifo", {"ring-conf": ["11"], "ring-split": ["11"]}, "ring-conf", ["11"]),
 ("two-pc-lbinria", "bag", {"correct": ["7"], "counting-bug": ["8"]}, "", ["7", "8"]),
 ("zab-broadcast-remix", "fifo", {"correct": ["5"], "issue1-propose-to-ackld": ["5"]}, "", ["5"]),
 ("broadcast-consensus-civl", "bag", {"correct": ["6"], "decide-early": ["6"]}, "", ["6"]),
# a2-store (pbkv) and a3-lock (lock) left out on 2026-11-02 (owner: the eleven families with twins)
]
# templates: Rust count rows keyed by (dir, side, model, variant)
rt = {}
for f in sorted(glob.glob(inp + "/B-count-rust.rows.phase*")):
    for l in open(f):
        if not l.startswith("BR.count."): continue
        rid, mem, to, cmd = l.rstrip("\n").split(" ", 3)
        d, rest = rid.replace("BR.count.", "").split(".", 1); side = rest.split("_")[0]
        m = re.search(r"--model (\S+)", cmd); mdl = m.group(1)
        args = cmd.split(" -- ", 1)[1]; words = [t for t in args.split() if not t.isdigit()]
        var = "-".join(words) if side == "impl" else ("-".join(words) if words else "")
        rt.setdefault((d, side, mdl, var), (rid, cmd))
jt = {}
for r in csv.reader(open(inp + "/B-java-cases.tsv"), delimiter="\t"):
    if len(r) < 7 or r[0].startswith("#"): continue
    jt.setdefault((r[0], r[2], r[4], r[5].replace("spec-", "") if r[2] == "spec" else r[5]), r)
def resize(s, nums):  # replace the numeric tokens of s (in order) by nums; s may hold the size in the id with "_" or in args with " "
    it = iter(nums); return re.sub(r"(?<![0-9a-f])\d+(?![0-9a-f])", lambda m: next(it, m.group(0)), s)
R, J = [], []
def clone(d, side, mdl, var, size):
    k = (d, side, mdl, var)
    if k not in rt: print("no Rust template", k); return
    rid, cmd = rt[k]; nums = size.split()
    head, args = cmd.split(" -- ", 1)
    new_args = " ".join(str(next(it)) if t.isdigit() else t for it in [iter(nums)] for t in args.split())
    new_id = rid.replace(".", "\x00", 2)  # BR.count.<dir>.<rest>: only the rest carries the size
    pre, rest = rid.rsplit(".", 1)
    rest2 = re.sub(r"(?<=_)\d+(?=_)|(?<=_)\d+$", lambda m, it=iter(nums): next(it, m.group(0)), rest)
    R.append(f"{pre}.{rest2} 250 3600 {head} -- {new_args}")
    jk = (d, side, mdl, var if side == "impl" else (var or "spec"))
    j = jt.get(jk) or jt.get((d, side, mdl, "spec")) if side == "spec" else jt.get(jk)
    if j:
        cid = re.sub(r"(?<=_)\d+(?=_)|(?<=_)\d+$", lambda m, it=iter(nums): next(it, m.group(0)), j[1])
        cls = re.sub(r"(?<=[:,])\d+(?=,|$)", lambda m, it=iter(nums): next(it, m.group(0)), j[6])
        J.append([d, cid, side, ",".join(nums), mdl, j[5], cls, j[7] if len(j) > 7 else "readme"])
def far(s): return " ".join(str(int(t) + OFF) if t.isdigit() else t for t in s.split())   # every size number moved by the offset (one-parameter families)
for d, mdl, impls, sv, ssizes in PLAN:
    for s in ssizes: clone(d, "spec", mdl, sv, far(s))
    for v, sizes in impls.items():
        for s in sizes: clone(d, "impl", mdl, v, far(s))
open(out_r, "w").write("\n".join(R) + "\n"); open(out_j, "w").write("\n".join("\t".join(j) for j in J) + "\n")
print(f"rust cases {len(R)}, java cases {len(J)}")
