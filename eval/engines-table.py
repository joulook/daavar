#!/usr/bin/env python3
"""Phase A table of the evaluation (one merged table).

usage: engines-table.py TABLE2.tex TABLE3.tex ROWS.tsv LIMIT_HOURS OUT.tex RESULTS_DIR CHECK.tsv

A row comes from the generated tex/tables/table2.tex (conforming) or table3.tex (seeded fault), or, when it is not
printed there, from the records themselves (flags rnested, ronline, join). One table is written: black cells, whole
seconds, one time limit, no memory.
ROWS.tsv: first line "#caption<TAB>text"; then block<TAB>instance<TAB>size<TAB>flags ; block is conf or fault ;
flags is "-" or a comma list of
  nested=ran:<hours>   the Nested run at this size was stopped after <hours> without finishing (known-limit cell)
  nested=pending       no Nested run at this size reached the limit: the clock prints in red (pending)
  nested=mark:<text>   no Nested run at this size reached the limit: the clock carries the footnote mark <text>
  nested=ruled         no Nested run at this size reached the limit: a plain clock, by the owner's ruling of
                       2026-11-11 that such a run is certain not to finish within the limit
  nested=notrun        prints -- (no run)
  join=<run id>        the Offline join. Seeded-fault rows need it: its number of distinct uncovered summaries is
                       printed as the number of potential violations.
  rnested=<run id>, ronline=<run id>   (with join=) a row taken from the records: the counts are the graph counts of
                       the two exports named in the join's command, Offline T = the longer export plus the join (the
                       rule of the generated tables), Online T and T1 and the Nested cells from the two records.
  to=<run id>         (a row of the generated tables) the Online run: its T is taken from the record's wall_ms and
                       checked against the generated cell. With join=, the Offline T of such a row is composed from the
                       records in the same way. A time is rounded to whole seconds from the record, not from the
                       one-decimal cell: a cell that ends in .5 without its record is an error.
A known-limit cell (\\faClock{} in the generated file) without a flag is an error, and so is nested=ran:<h> with h
below the limit. A finished time above the limit prints as a clock and is reported on stderr.
CHECK.tsv lists the records used for the potential-violation counts and for the rows taken from the records.
"""
import glob, os, re, sys

t2, t3, rowsf, limit_h, out, resdir, checkf = sys.argv[1:8]
LIMIT = float(limit_h) * 3600.0
KNOWN, CLOCK = "\\faClock{}", "\\faClock"
RAW = "ms:"   # a time taken from a record, in milliseconds

REC = {}
for f in sorted(glob.glob(os.path.join(resdir, "*.tsv"))):
    for line in open(f):
        if not line.startswith(("C1.", "C2.", "C3E.", "C3J.")): continue
        p = line.rstrip("\n").split("\t")
        if len(p) >= 93: REC.setdefault(p[0], []).append(p)
F = lambda p, n: p[n - 1]
check = []

def join_rec(jid, who):
    c = [p for p in REC.get(jid, []) if F(p, 39) == "ok" and F(p, 38) in ("conforms", "violation") and F(p, 74) == "yes"]
    assert c, f"{who}: no valid join record {jid}"
    assert len({(F(p, 38), F(p, 89)) for p in c}) == 1, f"{who}: join records disagree: {jid}"
    return max(c, key=lambda p: F(p, 3))

def potential(jid, who):
    j = join_rec(jid, who)
    assert F(j, 38) == "violation" and re.fullmatch(r"[1-9]\d*", F(j, 89)), f"{who}: join {jid}: {F(j, 38)} {F(j, 89)}"
    check.append([who, "potential violations", jid, F(j, 4), F(j, 3), F(j, 89)])
    return F(j, 89)

def export_rec(eid, node, before, who):
    c = [p for p in REC.get(eid, []) if F(p, 39) == "ok" and F(p, 74) == "yes" and F(p, 47) not in ("", "0")
         and F(p, 4) == node and F(p, 3) <= before]
    assert c and len({F(p, 47) for p in c}) == 1, f"{who}: export {eid} on {node} before {before}: {len(c)} records"
    return max(c, key=lambda p: F(p, 3))

def from_records(block, flags, who):
    """the cells of a row that the generated tables do not print, in their raw form"""
    j = join_rec(flags["join"], who)
    assert (F(j, 38) == "conforms") == (block == "conf"), f"{who}: join verdict {F(j, 38)} in block {block}"
    m = re.search(r"--merge\s+(\S+)/export\.jsonl\s+(\S+)/export\.jsonl", F(j, 31))
    assert m, f"{who}: cannot parse the join command"
    rid, cid = os.path.basename(m.group(1)), os.path.basename(m.group(2))
    er, ec = export_rec(rid, F(j, 4), F(j, 3), who), export_rec(cid, F(j, 4), F(j, 3), who)
    ft = max(int(F(er, 51)), int(F(ec, 51))) + int(F(j, 51))
    check.append([who, "offline join", F(j, 1), F(j, 4), F(j, 3), f"verdict {F(j, 38)}, wall_ms {F(j, 51)}"])
    check.append([who, "reference export", rid, F(er, 4), F(er, 3), f"graphs {F(er, 47)}, wall_ms {F(er, 51)}"])
    check.append([who, "candidate export", cid, F(ec, 4), F(ec, 3), f"graphs {F(ec, 47)}, wall_ms {F(ec, 51)}"])
    def run(rid_, kind):
        c = [p for p in REC.get(rid_, []) if F(p, 74) == "yes" and F(p, 39) in ("ok", "timeout")]
        assert len(c) == 1, f"{who}: {kind} {rid_}: {len(c)} valid records"
        p = c[0]
        check.append([who, kind, rid_, F(p, 4), F(p, 3), f"stop {F(p, 39)}, verdict {F(p, 38)}, wall_ms {F(p, 51)}, "
                      f"first_ms {F(p, 50)}, sweeps {F(p, 93)}, explored {F(p, 92)}"])
        if F(p, 39) == "timeout":
            assert int(F(p, 51)) >= LIMIT * 1000, f"{who}: {kind} timeout below the limit"
            return p, None
        assert (F(p, 38) == "conforms") == (block == "conf"), f"{who}: {kind} verdict {F(p, 38)}"
        return p, int(F(p, 51))
    def t1(p):   # Online's first potential violation: specification phase + first miss (paper-tables.py, cell_T1)
        assert F(p, 50) != "" and F(p, 87) != "", f"{who}: Online record without first-miss or specification-phase time"
        check.append([who, "online T1", F(p, 1), F(p, 4), F(p, 3), f"spec_phase_ms {F(p, 87)} + first_miss_wall_ms {F(p, 50)}"])
        return RAW + str(int(F(p, 87)) + int(F(p, 50)))
    o, ot = run(flags["ronline"], "online")
    n, nt = run(flags["rnested"], "nested")
    return dict(gc=F(ec, 47), gr=F(er, 47),
                nsw=F(n, 93) if nt is not None else "", nex=F(n, 92) if nt is not None else "",
                nt=RAW + str(nt) if nt is not None else CLOCK,
                ot1=("--" if block == "conf" else t1(o) if ot is not None else ""),
                ot=RAW + str(ot) if ot is not None else CLOCK, ft=RAW + str(ft))

def near(raw, cell, who):
    assert re.fullmatch(r"\d+(\.\d+)?", cell), f"{who}: the generated cell is {cell!r}, the record says {raw}"
    assert abs(raw - float(cell)) <= 0.0501, f"{who}: record {raw} s against the generated cell {cell}"

def offline_raw(jid, cell, who):
    j = join_rec(jid, who)
    m = re.search(r"--merge\s+(\S+)/export\.jsonl\s+(\S+)/export\.jsonl", F(j, 31))
    assert m, f"{who}: cannot parse the join command"
    rid, cid = os.path.basename(m.group(1)), os.path.basename(m.group(2))
    er, ec = export_rec(rid, F(j, 4), F(j, 3), who), export_rec(cid, F(j, 4), F(j, 3), who)
    ms = max(int(F(er, 51)), int(F(ec, 51))) + int(F(j, 51))
    near(ms / 1000.0, cell, who + " Offline")
    check.append([who, "offline T", F(j, 1), F(j, 4), F(j, 3), f"join wall_ms {F(j, 51)}; exports {rid} wall_ms {F(er, 51)}, {cid} wall_ms {F(ec, 51)}; T = longer export + join = {ms} ms"])
    return RAW + str(ms)

def online_raw(rid_, cell, who):
    c = [p for p in REC.get(rid_, []) if F(p, 74) == "yes" and F(p, 39) == "ok" and F(p, 38) in ("conforms", "violation")]
    assert c and len({F(p, 51) for p in c}) == 1, f"{who}: Online {rid_}: {len(c)} valid finished records"
    ms = int(F(c[0], 51))
    near(ms / 1000.0, cell, who + " Online")
    check.append([who, "online T", rid_, F(c[0], 4), F(c[0], 3), f"wall_ms {ms}, verdict {F(c[0], 38)}"])
    return RAW + str(ms)

def rows_of(path):
    d, last, inbody = {}, None, False
    for line in open(path):
        if line.startswith("\\endhead"): inbody = True; continue
        if not inbody or "&" not in line: continue
        line = re.sub(r"\\textcolor\{[a-z]+\}\{((?:[^{}]|\{\})*)\}", r"\1", line.rstrip("\n"))
        assert "textcolor" not in line, line
        cells = [c.strip() for c in line.rstrip("\\").rstrip().rstrip("\\").split("&")]
        if cells[0] == "":            # a further size of the instance of the row above
            assert last, line
            name = last
        else:
            name = re.match(r"\\textsf\{([^}]*)\}", cells[0]).group(1); last = name
        assert (name, cells[1]) not in d, (name, cells[1])
        d[(name, cells[1])] = cells
    return d

conf, fault = rows_of(t2), rows_of(t3)
warn = []

def count(c):
    if c == "": return ""
    assert re.fullmatch(r"\d+", c), c
    if len(c) <= 4: return c
    s = ""
    while len(c) > 3: s = "\\," + c[-3:] + s; c = c[:-3]
    return c + s

def secs(c, who):
    """a time cell: whole seconds; '<1' below one second; a clock stays a clock"""
    if c in (CLOCK, KNOWN, "oom", "", "--"): return c
    if c.startswith(RAW): v = int(c[len(RAW):]) / 1000.0
    else:
        assert not re.fullmatch(r"\d+\.50*", c), f"{who}: the cell {c} ends in a half; name its record in the row file (to=, join=)"
        v = float(c)
    if v > LIMIT:
        warn.append(f"{who}: finished time {v:.1f} s is above the limit, printed as a clock"); return CLOCK
    if v < 1.0: return "$<$1"
    from decimal import Decimal, ROUND_HALF_UP
    return count(str(int(Decimal(repr(v)).quantize(Decimal(1), rounding=ROUND_HALF_UP))))

caption = None
lines, n = {"conf": [], "fault": []}, 0
for raw in open(rowsf):
    if raw.startswith("#caption\t"): caption = raw.rstrip("\n").split("\t", 1)[1]; continue
    if not raw.strip() or raw.startswith("#"): continue
    block, name, size, flags = (raw.rstrip("\n").split("\t") + ["-"])[:4]
    assert block in ("conf", "fault"), raw
    flags = dict(f.split("=", 1) for f in flags.split(",")) if flags not in ("-", "") else {}
    src = conf if block == "conf" else fault
    who = f"{name}({size})"
    if "ronline" in flags or "rnested" in flags:
        assert (name, size) not in src, f"{who}: the row is in the generated table, take it from there"
        r = from_records(block, flags, who)
    else:
        assert (name, size) in src, f"row {name} {size} is not in the generated {block} table"
        c = src[(name, size)]
        if block == "conf":   # instance size Gc Gr | sweeps Gexpl T M | T M | T M
            r = dict(gc=c[2], gr=c[3], nsw=c[4], nex=c[5], nt=c[6], ot1="--", ot=c[8], ft=c[10])
        else:                 # instance size Gc Gr Gx | sweeps Gexpl T1 T M | T1 T M | T M
            r = dict(gc=c[2], gr=c[3], nsw=c[5], nex=c[6], nt=c[8], ot1=c[10], ot=c[11], ft=c[13])
        if "to" in flags: r["ot"] = online_raw(flags["to"], r["ot"], who)
        if "join" in flags: r["ft"] = offline_raw(flags["join"], r["ft"], who)
    gx = "0" if block == "conf" else potential(flags.get("join", ""), who)
    assert r["gc"] and r["gr"] and r["ft"] != "", f"{who}: a row without counts or without an Offline result is not printed"
    nsw, nex = r["nsw"], r["nex"]
    if r["nt"] == KNOWN:
        st = flags.get("nested")
        assert st, f"{who}: the Nested cell is a known limit, not a measured one; flag it in the row file"
        if st == "pending": ntc = "\\pendr{\\faClock}"
        elif st == "notrun": ntc = "--"
        elif st.startswith("mark:"): ntc = "\\faClock$^{" + st[5:] + "}$"
        elif st == "ruled": ntc = CLOCK
        else:
            h = float(st.split(":")[1]); assert h * 3600 >= LIMIT, f"{who}: the Nested run lasted {h} h, below the limit"
            ntc = CLOCK
        nsw = nex = ""
    else:
        assert "nested" not in flags, f"{who}: a Nested flag on a measured cell"
        ntc = secs(r["nt"], who + " Nested")
        if ntc == CLOCK: nsw = nex = ""
    dash = lambda x: x if x != "" else "--"
    otc = secs(r["ot"], who + " Online")
    ot1c = secs(r["ot1"], who + " Online T1")
    if ot1c in ("", CLOCK): ot1c = "--"       # no first potential violation within the limit
    ftc = secs(r["ft"], who + " Offline")
    cells = [f"\\textsc{{{name}}}({size})", count(r["gc"]), count(r["gr"]), count(gx),
             dash(count(nsw)), dash(count(nex)), ntc, ot1c, otc, ftc]
    lines[block].append(" & ".join(cells) + " \\\\")
    n += 1

assert caption, "the row file needs a first line: #caption<TAB>text"
head = r"""\begin{table}[t]
\caption{%s}\label{tab:eval2:engines}
\centering\scriptsize\setlength{\tabcolsep}{3.4pt}
\begin{tabular}{@{}lrrrrrrrrr@{}}
\toprule
 &  &  &  & \multicolumn{3}{c}{\nested} & \multicolumn{2}{c}{\online} & \offline \\
\cmidrule(lr){5-7}\cmidrule(lr){8-9}\cmidrule(l){10-10}
pair & $G_{\mathit{cand}}$ & $G_{\mathit{ref}}$ & pot. & inner & $G_{\mathit{ref}}^{\mathit{expl}}$ & T & T$_1$ & T & T \\
\midrule
""" % caption
body = "\n".join(lines["conf"]) + "\n\\midrule\n" + "\n".join(lines["fault"]) + "\n"
tail = "\\bottomrule\n\\end{tabular}\n\\end{table}\n"
with open(out + ".new", "w") as fh:
    fh.write("% generated by engines-table.py from tex/tables/table2.tex, table3.tex and the records; do not edit by hand\n" + head + body + tail)
os.replace(out + ".new", out)
with open(checkf + ".new", "w") as fh:
    fh.write("row\tuse\trecord\tnode\tfinished\tvalues\n")
    for c in check: fh.write("\t".join(c) + "\n")
os.replace(checkf + ".new", checkf)
for w in warn: print("WARNING", w, file=sys.stderr)
print(f"{n} rows ({len(lines['conf'])} conforming, {len(lines['fault'])} violating), limit {limit_h} h -> {out}")
