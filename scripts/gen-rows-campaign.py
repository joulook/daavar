#!/usr/bin/env python3
"""Campaign rows (owner's plan of 2026-11-01), from the delivered Rust count rows and the Java case list.

Part 1, every Rust implementation case against its specification case (same directory, model, size):
  C1  conf --engine stateful  --threads T      C2  conf --engine stateless --threads T
  C3  export spec + export impl (+ conf-join)  -> files C3-export.rows, C3-join.rows
Part 2, every Rust/Java twin pair (same directory, side, model, size, variant):
  XJE Java exports (JMC, split W)              XON1 JMC online: Java impl vs Rust spec export
  XON2 TF online: Rust impl vs Java spec export  XJ1/XJ2 joins: TF spec x JMC impl, JMC spec x TF impl
Size ladder: within (dir, model, variant, mode) rows are ordered by size and each carries
needok:<previous-size row>, so a size runs only if the smaller one finished within its tier.
usage: gen-rows-campaign.py <B-count-rust.rows> <B-java-cases.tsv> <outdir> [--threads 16] [--timeout 3600]
       [--mem 250] [--export-threads 0] [--online-threads 0] [--jmc-split 16] [--jmc-online-split 0]"""
import sys, re, os, csv, collections, argparse
ap = argparse.ArgumentParser(); ap.add_argument("rust_rows"); ap.add_argument("java_cases"); ap.add_argument("out")
ap.add_argument("--threads", type=int, default=16); ap.add_argument("--timeout", type=int, default=3600); ap.add_argument("--mem", type=int, default=250)
ap.add_argument("--export-threads", type=int, default=0, help="TraceForge export threads (0 = flag not passed; the pin's export is sequential)")
ap.add_argument("--online-threads", type=int, default=0); ap.add_argument("--jmc-split", type=int, default=16); ap.add_argument("--jmc-online-split", type=int, default=0)
ap.add_argument("--extend", type=int, default=0, help="add N further sizes beyond each ladder's largest listed size (every numeric argument +1 per step); they run only while the previous size finished within its tier")
A = ap.parse_args(); os.makedirs(A.out, exist_ok=True)
HDR = "id\ttable\tfamily\tsize\tmodel\tvariant\tengine\tthreads\texpected_verdict\tnote\ttier\tkind\tspec_tool\timpl_tool\tspec_lang\timpl_lang\tmode\texploration\tbenchmark\tspec_case\timpl_case"
# ---- Rust cases from the count rows: id, dir, side, model, args ---------------------------------
Case = collections.namedtuple("Case", "id dir side model args nums variant")
rust = []
for line in open(A.rust_rows):
    if not line.strip() or line.startswith("#"): continue
    rid, mem, to, cmd = line.rstrip("\n").split(" ", 3)
    m = re.search(r"rust-run\.sh (\S+) count (spec|impl) (.*?)-- (.*)$", cmd)
    if not m: continue
    d, side, opts, args = m.groups(); mdl = re.search(r"--model (\S+)", opts); mdl = mdl.group(1) if mdl else "fifo"
    toks = args.split(); nums = tuple(int(t) for t in toks if t.isdigit()); words = [t for t in toks if not t.isdigit()]
    variant = "-".join(words) if words else ("spec" if side == "spec" else "impl")
    rust.append(Case(rid.replace("BR.count.", ""), d, side, mdl, args, nums, variant))
# ladder extension: beyond the largest listed size of each (dir, side, model, variant), add sizes with
# every numeric argument raised by 1 per step (ids carry .x<step>); the runner's needok chain stops
# the ladder at the first size that does not finish
if A.extend:
    groups = collections.defaultdict(list)
    for c in rust: groups[(c.dir, c.side, c.model, c.variant)].append(c)
    extra = []
    for key, cs in groups.items():
        top = max(cs, key=lambda c: c.nums)
        if not top.nums: continue
        for step in range(1, A.extend + 1):
            nums = tuple(n + step for n in top.nums); it = iter(nums)
            args = " ".join(str(next(it)) if tok.isdigit() else tok for tok in top.args.split())
            extra.append(Case(f"{top.dir}.{top.side}_{'_'.join(args.split())}_{top.model}.x{step}", top.dir, top.side, top.model, args, nums, top.variant))
    rust += extra
specs = collections.defaultdict(list)
for c in rust:
    if c.side == "spec": specs[(c.dir, c.model, c.nums)].append(c)
pairs = []  # (impl case, spec case)
for c in rust:
    if c.side != "impl": continue
    ss = specs.get((c.dir, c.model, c.nums), [])
    if not ss: ss = specs.get((c.dir, c.model, c.nums[:1]), [])  # fallback: spec keyed by the first number
    for s in ss: pairs.append((c, s))
# ---- Java cases: dir, case id, side, size, model, variant, class:args --------------------------
JCase = collections.namedtuple("JCase", "dir cid side size model variant cls nums")
java = []
for r in csv.reader(open(A.java_cases), delimiter="\t"):
    if len(r) < 7 or r[0].startswith("#"): continue
    d, cid, side, size, mdl, var, cls = r[:7]
    nums = tuple(int(x) for x in re.findall(r"\d+", size))
    java.append(JCase(d, cid, side, size, mdl, var.replace("spec-", ""), cls, nums))
def jkey(j): return (j.dir, j.side, j.model, j.nums, j.variant if j.side == "impl" else "spec")
def rkey(c): return (c.dir, c.side, c.model, c.nums, c.variant if c.side == "impl" else "spec")
jmap = {jkey(j): j for j in java}; rmap = {rkey(c): c for c in rust}
# ---- emit --------------------------------------------------------------------------------------
files = {}
def emit(name, rid, cmd, attrs, dep=None, mem=None, to=None):
    f = files.setdefault(name, [[], [HDR]])
    f[0].append(f"{rid} {mem or A.mem} {to or A.timeout} {('needok:'+dep+' ') if dep else ''}{cmd}")
    f[1].append("\t".join([rid] + [str(attrs.get(k, "")) for k in HDR.split("\t")[1:]]))
def size_of(nums): return ",".join(map(str, nums))
def ladder(items, keyf):  # yield (item, previous item of the same ladder) ordered by size
    groups = collections.defaultdict(list)
    for it in items: groups[keyf(it)].append(it)
    for g in groups.values():
        g.sort(key=lambda it: it.nums if hasattr(it, "nums") else it[0].nums); prev = None
        for it in g: yield it, prev; prev = it
T = A.threads
# in-process conf decides its specification from the implementation's arguments: one row per impl
# case, whichever spec cases share its numbers (joins keep one row per spec case below)
seen_impl = set(); conf_pairs = []
for c, s in pairs:
    if c.id in seen_impl: continue
    seen_impl.add(c.id); conf_pairs.append((c, s))
for (c, s), prev in ladder(conf_pairs, lambda p: (p[0].dir, p[0].model, p[0].variant)):
    base = dict(table="C", family=c.dir, size=size_of(c.nums), model=c.model, variant=c.variant, threads=T, expected_verdict="readme",
                kind="conf-traceforge", spec_tool="traceforge", impl_tool="traceforge", spec_lang="rust", impl_lang="rust", exploration="full", benchmark=c.dir, spec_case=s.id, impl_case=c.id, tier="c1h")
    cid = f"{c.dir}.{c.id.split('.',1)[1]}"
    pid = f"{prev[0].dir}.{prev[0].id.split('.',1)[1]}" if prev else None
    for mode, eng in (("C1", "stateful"), ("C2", "stateless")):
        emit(mode, f"{mode}.{cid}.t{T}", f'"$DAAVAR"/scripts/rust-run.sh {c.dir} conf --engine {eng} --threads {T} --model {c.model} -- {c.args}',
             dict(base, engine=f"{eng}-parallel", mode="in-process"), dep=f"{mode}.{pid}.t{T}" if pid else None)
# C3: exports once per Rust case, joins per pair
xt = f" --threads {A.export_threads}" if A.export_threads else ""
exp_id = {}
for c, prev in ladder(rust, lambda c: (c.dir, c.side, c.model, c.variant)):
    rid = f"C3E.{c.dir}.{c.id.split('.',1)[1]}" + (f".t{A.export_threads}" if A.export_threads else ".t1"); exp_id[c.id] = rid
    prid = exp_id.get(prev.id) if prev else None
    emit("C3-export", rid, f'"$DAAVAR"/scripts/rust-run.sh {c.dir} export {c.side} --out export.jsonl{xt} --model {c.model} -- {c.args}',
         dict(table="C", family=c.dir, size=size_of(c.nums), model=c.model, variant=c.variant, engine="export", threads=A.export_threads or 1, kind="export", spec_tool="traceforge", impl_tool="traceforge", spec_lang="rust", impl_lang="rust", mode="offline-batch", exploration="full", benchmark=c.dir, tier="c1h"), dep=prid)
for c, s in pairs:
    rid = f"C3J.{c.dir}.{c.id.split('.',1)[1]}" + (f".vs-{s.variant}" if s.variant not in ("spec", "correct") else "")
    emit("C3-join", rid, f'"$DAAVAR"/traceforge/target/release/conf-join "$EVAL_OUT"/{exp_id[s.id]}/export.jsonl "$EVAL_OUT"/{exp_id[c.id]}/export.jsonl',
         dict(table="C", family=c.dir, size=size_of(c.nums), model=c.model, variant=c.variant, engine="join", threads=1, kind="cross-language", spec_tool="traceforge", impl_tool="traceforge", spec_lang="rust", impl_lang="rust", mode="offline-batch", exploration="full", benchmark=c.dir, spec_case=s.id, impl_case=c.id, tier="c600"), dep=exp_id[c.id], mem=16, to=600)
# Part 2: Java exports for every Java case that has a Rust twin; online and joins per twin pair
split = f" --split {A.jmc_split} --k 1" if A.jmc_split else ""
jexp = {}
for j, prev in ladder([j for j in java if jkey(j) in rmap], lambda j: (j.dir, j.side, j.model, j.variant)):
    rid = f"XJE.{j.dir}.{j.cid}.w{A.jmc_split or 1}"; jexp[jkey(j)] = rid
    emit("X-java-export", rid, f'"$DAAVAR"/scripts/jmc-run.sh {j.dir} \'{j.cls}\' must 0 1 export.jsonl {A.timeout}{split}',
         dict(table="X", family=j.dir, size=size_of(j.nums), model=j.model, variant=j.variant, engine="must-split" if A.jmc_split else "must", threads=A.jmc_split or 1, kind="export", spec_tool="jmc", impl_tool="jmc", spec_lang="java", impl_lang="java", mode="offline-batch", exploration="full", benchmark=j.dir, tier="c1h"),
         dep=jexp.get(jkey(prev)) if prev else None)
osplit = f" --split {A.jmc_online_split} --k 1" if A.jmc_online_split else ""
ot = f" --threads {A.online_threads}" if A.online_threads else ""
for c, s in pairs:
    jc, js = jmap.get(rkey(c)), jmap.get(rkey(s))
    if not (jc and js): continue
    common = dict(table="X", family=c.dir, size=size_of(c.nums), model=c.model, variant=c.variant, expected_verdict="readme", exploration="full", benchmark=c.dir, spec_case=s.id, impl_case=c.id)
    tag = f".vs-{s.variant}" if s.variant not in ("spec", "correct") else ""
    emit("X-online-jmc", f"XON1.{c.dir}.{jc.cid}{tag}.w{A.jmc_online_split or 1}", f'"$DAAVAR"/scripts/jmc-run.sh {c.dir} \'{jc.cls}\' must 0 1 - {A.timeout}{osplit} --spec "$EVAL_OUT"/{exp_id[s.id]}/export.jsonl',
         dict(common, engine="stateful-online", threads=A.jmc_online_split or 1, kind="cross-language", spec_tool="traceforge", impl_tool="jmc", spec_lang="rust", impl_lang="java", mode="online", tier="c1h"), dep=exp_id[s.id])
    emit("X-online-tf", f"XON2.{c.dir}.{c.id.split('.',1)[1]}{tag}.t{A.online_threads or 1}", f'"$DAAVAR"/scripts/rust-run.sh {c.dir} online{ot} --model {c.model} --spec "$EVAL_OUT"/{jexp[jkey(js)]}/export.jsonl -- {c.args}',
         dict(common, engine="stateful-online", threads=A.online_threads or 1, kind="cross-language", spec_tool="jmc", impl_tool="traceforge", spec_lang="java", impl_lang="rust", mode="online", tier="c1h"), dep=jexp[jkey(js)])
    emit("X-join", f"XJ1.{c.dir}.{c.id.split('.',1)[1]}{tag}.tfspec-jmcimpl", f'"$DAAVAR"/traceforge/target/release/conf-join "$EVAL_OUT"/{exp_id[s.id]}/export.jsonl "$EVAL_OUT"/{jexp[jkey(jc)]}/export.jsonl',
         dict(common, engine="join", threads=1, kind="cross-language", spec_tool="traceforge", impl_tool="jmc", spec_lang="rust", impl_lang="java", mode="offline-batch", tier="c600"), dep=jexp[jkey(jc)], mem=16, to=600)
    emit("X-join", f"XJ2.{c.dir}.{c.id.split('.',1)[1]}{tag}.jmcspec-tfimpl", f'"$DAAVAR"/traceforge/target/release/conf-join "$EVAL_OUT"/{jexp[jkey(js)]}/export.jsonl "$EVAL_OUT"/{exp_id[c.id]}/export.jsonl',
         dict(common, engine="join", threads=1, kind="cross-language", spec_tool="jmc", impl_tool="traceforge", spec_lang="java", impl_lang="rust", mode="offline-batch", tier="c600"), dep=exp_id[c.id], mem=16, to=600)
for name, (rows, tsv) in files.items():
    open(f"{A.out}/{name}.rows", "w").write("\n".join(rows) + "\n"); open(f"{A.out}/{name}.rows.tsv", "w").write("\n".join(tsv) + "\n")
    print(f"{name}: {len(rows)} rows")
print(f"rust cases {len(rust)}, pairs {len(pairs)}, java cases {len(java)}, java cases with a Rust twin {sum(1 for j in java if jkey(j) in rmap)}, twin pairs {sum(1 for c,s in pairs if jmap.get(rkey(c)) and jmap.get(rkey(s)))}")
unp = [c for c in rust if c.side == 'impl' and not specs.get((c.dir, c.model, c.nums)) and not specs.get((c.dir, c.model, c.nums[:1]))]
if unp: print("impl cases without a spec:", len(unp), sorted(set(c.dir for c in unp)))
