#!/usr/bin/env python3
"""The verdict summary of the evaluation (owner 2026-11-06: every count in the text must come from a table). For each configuration, mode
and direction: checks decided with a hand-derived verdict (agreeing / disagreeing), decided without one, at the time limit, at the memory
limit; for Phase A's Offline also the joins under one second. Reads PLDI/tex/tables/campaign-*.csv (one row per instance, as tables.py
writes them) and writes PLDI/tex/tables/verdict-summary.tex and .csv. usage: summary-table.py <PLDI/tex/tables>"""
import csv, sys, re
T = sys.argv[1]
NOT_A = re.compile(r"^(raft|omnipaxos|zookeeper|cassandra|chainpaxos|swiftpaxos|pineapple|craq|bk|crdb|scalardb)")
def load(f):
    try: return list(csv.DictReader(open(f"{T}/{f}.csv")))
    except OSError: return []
ROWS = [("A", "Nested", "Rust / Rust", "campaign-stateless", True),
        ("A", "Online", "Rust / Rust", "campaign-stateful", True),
        ("A", "Offline", "Rust / Rust", "campaign-offline-tf", True),
        ("C", "Offline", "Java impl / Rust spec", "campaign-offline-rust-spec", True),
        ("C", "Offline", "Rust impl / Java spec", "campaign-offline-java-spec", True),
        ("C", "Online", "Java impl / Rust spec", "campaign-online-java-impl", True),
        ("C", "Online", "Rust impl / Java spec", "campaign-online-rust-impl", True),
        ("C", "Offline", "twins, Java as spec", "campaign-twin-offline-java-spec", True),
        ("C", "Offline", "twins, Rust as spec", "campaign-twin-offline-rust-spec", True),
        ("D", "Offline", "ports, Java as spec", "campaign-cftwin-offline-java-spec", True),
        ("D", "Offline", "ports, Rust as spec", "campaign-cftwin-offline-rust-spec", True),
        ("D", "Online", "Rust impl / Java port", "campaign-cftwin-online-rust-impl", True),
        ("D", "Online", "Java port / Rust impl", "campaign-cftwin-online-java-impl", True)]
out = []; csvrows = []
for ph, mode, side, f, phase_a_only in ROWS:
    rs = [r for r in load(f) if not NOT_A.match(r["program"])]
    dec = [r for r in rs if r["verdict"] in ("conforms", "violation")]
    der = [r for r in dec if r.get("expected") in ("conforms", "violation")]
    agree = sum(1 for r in der if r["verdict"] == r["expected"]); dis = len(der) - agree
    noder = len(dec) - len(der)
    to = sum(1 for r in rs if r["verdict"] == "t/o"); oom = sum(1 for r in rs if r["verdict"] == "oom")
    j1 = jl = ""
    if f == "campaign-offline-tf":
        js = [(float(r["join s"]), float(r.get("spec export s") or 0), float(r.get("impl export s") or 0)) for r in dec if (r.get("join s") or "") != ""]
        j1 = f"{sum(1 for x in js if x[0] < 1.0)} of {len(js)}"; jl = str(sum(1 for j, a, b in js if j > max(a, b)))
    cells = [ph, mode, side, str(agree), str(dis), str(noder), str(to), str(oom), j1, jl]
    out.append(" & ".join(cells) + r" \\"); csvrows.append(cells)
L = [r"\begin{table}[tp]",
     r"\caption{Verdict summary of every finished check, per configuration, mode and direction (the classical, handcrafted, literature and synthetic benchmarks; real software and the case studies are in their own tables): checks whose verdict agrees with the hand-derived one, that disagree, decided checks of pairs without a derived verdict, and checks at the time or the memory limit; for \offline\ in \ref{phase:engines}, the joins that took under one second among the decided checks, and those that took longer than both exports.}",
     r"\label{tab:eval2:summary}", r"\scriptsize\setlength{\tabcolsep}{3pt}\centering", r"\begin{tabular}{@{}lllrrrrrrr@{}}", r"\toprule",
     r"phase & mode & sides & agree & disagree & no derived verdict & time limit & memory limit & joins $<$1\,s & join $>$ exports \\", r"\midrule"] + out + [r"\bottomrule", r"\end{tabular}", r"\end{table}"]
open(f"{T}/verdict-summary.tex", "w").write("\n".join(L) + "\n")
with open(f"{T}/verdict-summary.csv", "w", newline="") as fh:
    w = csv.writer(fh); w.writerow(["phase", "mode", "sides", "agree", "disagree", "no derived verdict", "time limit", "memory limit", "joins under 1 s", "join longer than both exports"]); w.writerows(csvrows)
print("\n".join(" | ".join(c) for c in csvrows))
