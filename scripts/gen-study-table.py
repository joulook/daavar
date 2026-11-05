#!/usr/bin/env python3
"""Study tables of the gen-study-rows.py studies (craq, bk; 2026-11-06), in the shape of chainpaxos-study.py:
usage: gen-study-table.py <study> <repo-root>  ->  plan/twin-tables/<STUDY>-STUDY.tex (conforming / violating / no-verdict tables)."""
import csv, sys, re, glob
study, R = sys.argv[1], sys.argv[2]
T = {
  "craq": dict(prefix="craq", hand=("craq-lin", r"S$_{\mathrm{lin}}$"), derived=("craq-derived", r"S$_{\mathrm{der}}$"),
    rust={"craq-rust": "code (Opus, Rust)", "craq-rust-sol": "code (sol, Rust)"},
    java={"craq": "transliteration (Java)", "craq-sol": "code (sol, Java)"}, sizes="q",
    title=r"The 3FS study (DeepSeek 3FS, chain replication of one chunk, CRAQ): two specifications, three context-free candidates and the transliteration",
    text=r"S$_{\mathrm{lin}}$ = the hand-written atomic-register specification; S$_{\mathrm{der}}$ = the specification derived from 3FS's design notes by a context-free agent (Opus), reviewed by Astra (faithful). The candidates were written by context-free agents from the 3FS sources (the storage client, the storage service's chain update and read paths, the chunk store), in Rust (Opus, Codex sol) and in Java (Codex sol); the transliteration (Opus, Java) is the unscored reference under the logical-task and shared-memory contracts. A row is one check at $q$ operations per client (two clients, a chain of three targets, no failure, fifo, the not-committed read retry cut per target), offline (exports and merge join)."),
  "bk": dict(prefix="bk", hand=("bk-ledger", r"S$_{\mathrm{bk}}$"), derived=("bk-derived", r"S$_{\mathrm{der}}$"),
    rust={"bk-rust": "main (Opus, Rust)", "bk-rust-sol": "main (sol, Rust)"},
    java={"bk": "transliteration main (Java)", "bk-413": "transliteration 4.13 (Java)", "bk-sol": "main (sol, Java)"}, sizes="e",
    title=r"The BookKeeper study (Apache BookKeeper, ledger replication with fencing): two specifications, three context-free candidates of main, the transliterations of main and of release 4.13",
    text=r"S$_{\mathrm{bk}}$ = the hand-written specification from the protocol's TLA+ model (acknowledged entries form a prefix; the recovery closes at or above every acknowledged entry; nothing is acknowledged after the close); S$_{\mathrm{der}}$ = the specification derived from the model's README and master TLA+ by a context-free agent (Opus), reviewed by Astra (faithful). The candidates were written by context-free agents from the main sources at 7b66b5d (the client's add path, the recovery, the bookie's request processors), in Rust (Opus, Codex sol) and in Java (Codex sol); the transliterations (Opus, Java) of main and of release-4.13.0 are unscored references under the logical-task contract (4.13: the recovery reads do not fence, the model's first documented finding). A row is one check at $e$ entries of the writer (one writer, one recovery client, three bookies, write quorum 3, ack quorum 2, no failure, fifo), offline (exports and merge join)."),
  "crdb": dict(prefix="crdb-pc", hand=("crdb-pc", r"S$_{\mathrm{pc}}$"), derived=("crdb-pc-derived", r"S$_{\mathrm{der}}$"),
    rust={"crdb-pc-rust": "code (Opus, Rust)", "crdb-pc-rust-sol": "code (sol, Rust)"},
    java={"crdb-pc": "transliteration (Java)", "crdb-pc-sol": "code (sol, Java)"}, sizes="(n,p,m,a)",
    title=r"The CockroachDB study (parallel commits with transaction recovery, d30c905): two specifications, three context-free candidates and the transliteration",
    text=r"S$_{\mathrm{pc}}$ = the hand-written specification from the authors' TLA$^+$ model (one fate for the committer and every preventer; an acknowledged commit is never aborted; no retry in this failure-free setting); S$_{\mathrm{der}}$ = the specification derived from the parallel-commits RFC by a context-free agent (Opus), reviewed by Astra (faithful, round 2). The candidates were written by context-free agents from the Go sources of the cut (the transaction coordinator's pipeliner and committer, the DistSender's parallel commit, the batch evaluation of EndTxn, QueryIntent, RecoverTxn and PushTxn, the recovery manager), in Rust (Opus, Codex sol) and in Java (Codex sol); the transliteration (Opus, Java) is the unscored reference under the logical-task contract. A row is one check at $(n,p,m,a)$ = keys, pipelined writes, preventers, epochs (no failure, logical timestamps, fifo), offline (exports and merge join)."),
}[study]
P = T["prefix"]; names = {T["hand"][0]: T["hand"][1], T["derived"][0]: T["derived"][1], **T["rust"], **T["java"]}
def spec_of(vs):  # the .vs- word of a row: hand | derived | <rust-port suffix>
    return {"hand": T["hand"][1], "derived": T["derived"][1]}.get(vs) or names.get(P + "-" + vs) or vs
def roles(i):
    p = i.split("."); pre, d, rest = p[0], p[1], ".".join(p[2:]); m = re.search(r"\.vs-([a-z0-9-]+)", rest); vs = m.group(1) if m else None
    if d not in names: return None
    if pre in ("C3J", "XJ1") and vs: return (names[d], spec_of(vs), "offline")
    if pre in ("XC1", "XC2"):
        r = (P + "-" + vs) if vs else (P + "-rust-sol")
        return (names[d], names[r], "offline") if pre == "XC1" else (names[r], names[d], "offline")
    return None
def fnum(x):
    try: v = float(x); return str(int(v)) if v == int(v) else "%.1f" % v
    except Exception: return "--"
def mib(x):
    try: return str(round(float(x)))
    except Exception: return "--"
rows = []
for f in glob.glob(R + "/PLDI/tex/tables/campaign-*.csv"):
    for r in csv.DictReader(open(f)):
        if not (r["program"] == P or r["program"].startswith(P + "-")): continue
        ro = roles(r["run id"])
        if not ro: continue
        if r["verdict"] in ("t/o", "oom") and (r.get("timeout s") or "") != "172800": continue
        rows.append((ro, r))
best = {}
for ro, r in rows:
    k = (ro[0], ro[1], r["size"], ro[2])
    if k not in best or (r.get("started") or "") > (best[k].get("started") or ""): best[k] = r
def tcell(r):
    v = r["verdict"]
    if v == "t/o": return r"\faClock"
    if v == "oom": return "oom"
    if v == "": return "--"
    return fnum(r["wall s"])
def paint(r, cells): c = "orange" if r.get("contended") == "yes" else "black"; return [r"\textcolor{" + c + "}{" + x + "}" for x in cells]
L = [r"\documentclass[9pt]{article}", r"\usepackage[a4paper,landscape,margin=12mm]{geometry}", r"\usepackage{booktabs,longtable,amsmath,xcolor,fontawesome5}", r"\usepackage[T1]{fontenc}", r"\setlength{\tabcolsep}{4pt}", r"\begin{document}", r"\section*{" + T["title"] + "}", T["text"] + r" $G$ as in the paper, $G_x$ = uncovered graphs; T = critical path, M = peak memory (MiB).", r"\medskip"]
for viol, title in ((False, "Conforming pairs"), (True, "Violating pairs")):
    sel = [(k, r) for k, r in best.items() if r["verdict"] in ("conforms", "violation") and (r["verdict"] == "violation") == viol]
    sel.sort(key=lambda kr: (kr[0][0], kr[0][1], kr[0][2]))
    hdr = r"implementation side & specification side & " + T["sizes"] + r" & kind & $G_{\mathit{impl}}$ & $G_{\mathit{spec}}$" + (r" & $G_x$" if viol else "") + r" & T & M \\"
    L += [r"\begin{longtable}{@{}lllr" + ("rrr" if viol else "rr") + r"rr@{}}", r"\caption{" + title + " (" + str(len(sel)) + r" checks).}\\", r"\toprule", hdr, r"\midrule", r"\endfirsthead", r"\toprule", hdr, r"\midrule", r"\endhead", r"\bottomrule", r"\endfoot"]
    for k, r in sel:
        cells = [k[0], k[1], k[2], k[3], r["impl graphs"] or "--", r["spec graphs"] or "--"] + ([r.get("uncovered") or "--"] if viol else []) + [tcell(r), mib(r["peak MB"])]
        L.append(" & ".join(paint(r, cells)) + r" \\")
    L += [r"\end{longtable}"]
und = [(k, r) for k, r in best.items() if r["verdict"] not in ("conforms", "violation")]
if und:
    hdr = r"implementation side & specification side & " + T["sizes"] + r" & kind & outcome \\"
    L += [r"\begin{longtable}{@{}lllrl@{}}", r"\caption{Checks without a verdict (" + str(len(und)) + r": time or memory limit, or no verdict).}\\", r"\toprule", hdr, r"\midrule", r"\endfirsthead", r"\toprule", hdr, r"\midrule", r"\endhead", r"\bottomrule", r"\endfoot"]
    for k, r in sorted(und, key=lambda kr: kr[0]): L.append(" & ".join(paint(r, [k[0], k[1], k[2], k[3], tcell(r)])) + r" \\")
    L += [r"\end{longtable}"]
L.append(r"\end{document}")
open(R + "/plan/twin-tables/" + study.upper() + "-STUDY.tex", "w").write("\n".join(L) + "\n"); print(study, "study written:", len(best), "checks")
