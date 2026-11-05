#!/usr/bin/env python3
"""Rows and companion attributes of a case study (2026-11-05, after study-cassandra-rr.rows): every pair of the study at the given
sizes, as the read-repair study laid them out. usage: gen-study-rows.py <study> <sizes> <out-prefix>
  study = craq | ... (the STUDIES table below: hand spec dir, derived spec dir, Rust ports, Java programs with class and jmc name)
Row shapes: C3E export (TraceForge, 16 threads, 48 h), C3J merge join of two exports, XCE JMC export (split 16), XJ1 Java impl vs
TraceForge spec export, XC1/XC2 Java port vs Rust port in both directions. Ids: <fam>.<dir>.<side>_<size>_<model>...; args `-- <size>`."""
import sys
STUDIES = {
    "craq": dict(hand="craq-lin", derived="craq-derived", rust=["craq-rust", "craq-rust-sol"],
                 java=[("craq", "bench.craq.ChainReplication", "transliteration"), ("craq-sol", "bench.craq_sol.ChainReplication", "")],
                 model="fifo", expected="readme", bench="craq"),
    "bk": dict(hand="bk-ledger", derived="bk-derived", rust=["bk-rust", "bk-rust-sol"],
               java=[("bk", "bench.bookkeeper.BookKeeper", "transliteration"), ("bk-413", "bench.bookkeeper413.BookKeeper413", "transliteration-4.13"),
                     ("bk-sol", "bench.bookkeeper_sol.BookKeeper", "")],
               model="fifo", expected="readme", bench="bk"),
    "crdb": dict(hand="crdb-pc", derived="crdb-pc-derived", rust=["crdb-pc-rust", "crdb-pc-rust-sol"],
                 java=[("crdb-pc", "bench.cockroach.ParallelCommits", "transliteration"), ("crdb-pc-sol", "bench.cockroach_sol.ParallelCommits", "")],
                 model="fifo", expected="readme", bench="crdb-pc"),   # sizes n_p_m_a (underscores in the id, commas in the Java argument string and the size column, spaces in the Rust args)
}
study, sizes, out = sys.argv[1], sys.argv[2].split(","), sys.argv[3]   # a multi-number size (crdb: n,p,m,a) is written with underscores (1_1_1_1), as in every run id
S = STUDIES[study]; M = S["model"]; rows = []; tsv = []
H = "id\ttable\tfamily\tsize\tmodel\tvariant\tengine\tthreads\texpected_verdict\tnote\ttier\tkind\tspec_tool\timpl_tool\tspec_lang\timpl_lang\tmode\texploration\tbenchmark\tspec_case\timpl_case"
def exp(d, side, q): return f"C3E.{d}.{side}_{q}_{M}.t16.c48h"
def join(d, side, q, vs, a, b): return f"C3J.{d}.{side}_{q}_{M}.vs-{vs}.c48h.m", f'needok:{a},{b} "$EVAL_OUT"/tf-merge-target/release/conf-join --merge "$EVAL_OUT"/{a}/export.jsonl "$EVAL_OUT"/{b}/export.jsonl'
def attr(id_, table, fam, q, variant, engine, threads, expected, note, kind, st, it, sl, il, spec_case, impl_case):
    base = id_.replace(".c48h.m", "").replace(".c48h", "").replace(".t16.c48h", ".t16")
    tsv.append("\t".join([base, table, fam, q.replace("_", ","), M, variant, engine, str(threads), expected, note, "c48h", kind, st, it, sl, il, "offline-batch", "full", fam, spec_case, impl_case]))
for q in sizes:
    hand, der = exp(S["hand"], "spec", q), exp(S["derived"], "spec", q)
    for d, e in ((S["hand"], hand), (S["derived"], der)):
        rows.append(f'{e} 250 172800 "$DAAVAR"/scripts/rust-run.sh {d} export spec --out export.jsonl --threads 16 --model {M} -- {q.replace("_", " ")}')
        attr(e.replace(".c48h", ""), "C", d, q, "spec", "must-partitioned", 16, S["expected"], "", "export", "traceforge", "traceforge", "rust", "rust", f"{d}.spec_{q}_{M}", f"{d}.spec_{q}_{M}")
    for a, b, d, vs in ((hand, der, S["derived"], "hand"), (der, hand, S["hand"], "derived")):
        i, c = join(d, "spec", q, vs, a, b); rows.append(f"{i} 250 172800 {c}")
        attr(i, "C", d, q, "spec", "join", 1, S["expected"], "", "cross-language", "traceforge", "traceforge", "rust", "rust", a.split(".")[1] + f".spec_{q}_{M}", f"{d}.spec_{q}_{M}")
    rexp = {}
    for r in S["rust"]:
        e = exp(r, "impl", q); rexp[r] = e
        rows.append(f'{e} 250 172800 "$DAAVAR"/scripts/rust-run.sh {r} export impl --out export.jsonl --threads 16 --model {M} -- {q.replace("_", " ")}')
        attr(e.replace(".c48h", ""), "C", r, q, "impl", "must-partitioned", 16, S["expected"], "", "export", "traceforge", "traceforge", "rust", "rust", f"{r}.impl_{q}_{M}", f"{r}.impl_{q}_{M}")
    for r in S["rust"]:
        for vs, sp in (("hand", hand), ("derived", der)):
            i, c = join(r, "impl", q, vs, sp, rexp[r]); rows.append(f"{i} 250 172800 {c}")
            attr(i, "C", r, q, "impl", "join", 1, S["expected"], "", "cross-language", "traceforge", "traceforge", "rust", "rust", sp.split(".")[1] + f".spec_{q}_{M}", f"{r}.impl_{q}_{M}")
    if len(S["rust"]) == 2:
        r1, r2 = S["rust"]
        for a, b in ((r1, r2), (r2, r1)):
            i, c = join(a, "impl", q, b.replace(study + "-", ""), rexp[b], rexp[a]); rows.append(f"{i} 250 172800 {c}")
            attr(i, "C", a, q, "impl", "join", 1, S["expected"], "", "cross-language", "traceforge", "traceforge", "rust", "rust", f"{b}.impl_{q}_{M}", f"{a}.impl_{q}_{M}")
    for jd, cls, note in S["java"]:
        qj = q.replace("_", ",")   # the Java argument string takes the size with commas
        xe = f"XCE.{jd}.impl_{q}_{M}.w16.c48h"
        rows.append(f"{xe} 250 172800 \"$DAAVAR\"/scripts/jmc-run.sh cf-{jd} '{cls}:impl,{qj},{M}' must 0 1 export.jsonl 172800 --split 16 --k 1")
        attr(xe.replace(".c48h", ""), "X", jd, q, "impl", "must-split", 16, "", note, "export", "jmc", "jmc", "java", "java", "", "")
        for vs, sp in (("hand", hand), ("derived", der)):
            i = f"XJ1.{jd}.impl_{q}_{M}.vs-{vs}.tfspec-jmcimpl.c48h.m"
            rows.append(f'{i} 250 172800 needok:{sp},{xe} "$EVAL_OUT"/tf-merge-target/release/conf-join --merge "$EVAL_OUT"/{sp}/export.jsonl "$EVAL_OUT"/{xe}/export.jsonl')
            attr(i, "X", jd, q, "impl", "join", 1, S["expected"], note, "cross-language", "traceforge", "jmc", "rust", "java", sp.split(".")[1] + f".spec_{q}_{M}", f"{jd}.impl_{q}_{M}")
        for r in S["rust"]:
            tag = "" if (jd.endswith("-sol") and r.endswith("-sol")) else ".vs-" + r.replace(study + "-", "")
            if jd.endswith("-sol") and not r.endswith("-sol"): continue   # the Codex Java port is twinned with the Codex Rust port only
            i1 = f"XC1.{jd}.impl_{q}_{M}{tag}.jimpl-tfimpl.c48h.m"; i2 = f"XC2.{jd}.impl_{q}_{M}{tag}.tfimpl-jimpl.c48h.m"
            rows.append(f'{i1} 250 172800 needok:{xe},{rexp[r]} "$EVAL_OUT"/tf-merge-target/release/conf-join --merge "$EVAL_OUT"/{xe}/export.jsonl "$EVAL_OUT"/{rexp[r]}/export.jsonl')
            attr(i1, "X", jd, q, "impl", "join", 1, "twin", (note + " twin-spec=" + r).strip(), "twin-cf", "jmc", "traceforge", "java", "rust", f"{jd}.impl_{q}_{M}", f"{jd}.impl_{q}_{M}")
            rows.append(f'{i2} 250 172800 needok:{rexp[r]},{xe} "$EVAL_OUT"/tf-merge-target/release/conf-join --merge "$EVAL_OUT"/{rexp[r]}/export.jsonl "$EVAL_OUT"/{xe}/export.jsonl')
            attr(i2, "X", jd, q, "impl", "join", 1, "twin", (note + " twin-spec=" + r).strip(), "twin-cf", "traceforge", "jmc", "rust", "java", f"{r}.impl_{q}_{M}", f"{jd}.impl_{q}_{M}")
hdr = f"# {study} study rows (generated by scripts/gen-study-rows.py, sizes {','.join(sizes)}): hand spec {S['hand']}, derived {S['derived']}, Rust ports {S['rust']}, Java {[j[0] for j in S['java']]}\n"
open(out + ".rows", "w").write(hdr + "\n".join(rows) + "\n"); open(out + ".rows.tsv", "w").write(H + "\n" + "\n".join(tsv) + "\n")
print(f"{len(rows)} rows, {len(tsv)} attribute lines -> {out}.rows(.tsv)")
