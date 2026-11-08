#!/usr/bin/env python3
"""The time-components figure of the evaluation (owner, 2026-11-08 23:00: Table 3 was too fine-grained for a paper;
show the shape instead). Reads paper-tables.py's table4.csv and writes a TikZ picture: one row per pair that Online
and Offline both finished, two horizontal bars per row, both scaled by Online's T. The Online bar is T_ref then T_cand
(then the remainder of T, if any). The Offline bar is the longer export's E and W, then the join J (then the remainder):
the two exports run side by side, so the longer one is Offline's critical path, as in the tables' T. Rows are sorted by
the share of the reference in Online's T, so the reference-dominated pairs are at the top and the candidate-dominated
ones at the bottom. A bar longer than the axis is cut and its ratio printed; ratios far from 1 are printed as well.
usage: fig-components.py <table4.csv> <out.tex>"""
import sys, csv
from decimal import Decimal, ROUND_HALF_UP
SRC, OUT = sys.argv[1], sys.argv[2]
def num(x):
    x = (x or "").strip()
    if x in ("", "--"): return None
    if x.startswith("$<$"): return 0.0   # below the timer's resolution: drawn as nothing
    try: return float(x)
    except ValueError: return None
rows = []
for r in csv.DictReader(open(SRC)):
    T, To = num(r["online T s"]), num(r["offline T s"])
    if not T or not To: continue
    on = [num(r["online T_ref s"]) or 0.0, num(r["online T_cand s"]) or 0.0]
    e_r, w_r, e_c, w_c, j = (num(r[k]) or 0.0 for k in ("offline E_ref s", "offline W_ref s", "offline E_cand s", "offline W_cand s", "offline join s"))
    side = "ref" if e_r + w_r >= e_c + w_c else "cand"
    off = [(e_r if side == "ref" else e_c), (w_r if side == "ref" else w_c), j]
    rows.append(dict(name=r["instance"], size=r["size"], pair=r["pair"], T=T, To=To, on=on, off=off, side=side, ratio=To / T, refshare=on[0] / T))
XMAX = 2.0                      # bars are scaled by Online's T; the axis ends at twice Online's time
W, H = 1.55, 0.062             # panel width in inches (bars), row pitch per bar in inches; two panels fit acmsmall's 5 in text width
GAP = 0.03                     # gap between the two bars of a pair... in inches
LEFT = 0.72                    # label column
PANEL_GAP = 0.3
COL = {"ref": "figRef", "cand": "figCand", "refx": "figRef!45!white", "candx": "figCand!45!white", "w": "black!45", "j": "black!75", "rest": "black!15"}
def rnd(v, nd=1): return str(Decimal(repr(v)).quantize(Decimal(1).scaleb(-nd), rounding=ROUND_HALF_UP))
def esc(s): return s.replace("_", "\\_")
def panel(rs, x0, title):
    rs = sorted(rs, key=lambda r: -r["refshare"])
    L = [f"\\node[anchor=south west,font=\\scriptsize\\bfseries,inner sep=1pt] at ({x0 + LEFT:.3f}in,{0.03:.3f}in) {{{title}}};"]
    y = 0.0
    def seg(x, segs, y, h):
        out = []; cur = x
        for frac, col in segs:
            if frac <= 0: continue
            w = min(frac, XMAX - (cur - x) / W * 1.0) * W / XMAX if False else frac * W / XMAX
            end = min(cur + w, x + W)
            if end <= cur: break
            out.append(f"\\fill[{col}] ({cur:.3f}in,{y:.3f}in) rectangle ({end:.3f}in,{y + h:.3f}in);")
            cur = end
        return out
    for r in rs:
        y -= 2 * H + GAP
        T = r["T"]; x = x0 + LEFT
        on_fr = [r["on"][0] / T, r["on"][1] / T]; rest_on = max(0.0, 1.0 - sum(on_fr))
        L.append(f"\\node[anchor=east,font=\\tiny,inner sep=1pt] at ({x - 0.04:.3f}in,{y + H + GAP / 2:.3f}in) {{\\textsf{{{esc(r['name'])}}}({r['size']})}};")
        L += seg(x, [(on_fr[0], COL["ref"]), (on_fr[1], COL["cand"]), (rest_on, COL["rest"])], y + H + GAP, H * 0.92)
        off_fr = [v / T for v in r["off"]]; rest_off = max(0.0, r["ratio"] - sum(off_fr))
        ex = COL["refx"] if r["side"] == "ref" else COL["candx"]
        L += seg(x, [(off_fr[0], ex), (off_fr[1], COL["w"]), (off_fr[2], COL["j"]), (rest_off, COL["rest"])], y, H * 0.92)
        if r["ratio"] > XMAX:   # cut bar: a break mark and the ratio
            L.append(f"\\draw[white,line width=1.2pt] ({x + W - 0.05:.3f}in,{y - 0.01:.3f}in) -- ({x + W - 0.02:.3f}in,{y + H:.3f}in);")
            L.append(f"\\node[anchor=west,font=\\tiny,inner sep=1pt] at ({x + W + 0.02:.3f}in,{y + H / 2:.3f}in) {{{rnd(r['ratio'])}}};")
        elif r["ratio"] < 0.8 or r["ratio"] > 1.25:
            L.append(f"\\node[anchor=west,font=\\tiny,inner sep=1pt] at ({x + r['ratio'] * W / XMAX + 0.02:.3f}in,{y + H / 2:.3f}in) {{{rnd(r['ratio'], 2)}}};")
    # axis: the line at 1 (Online's T) and ticks
    x = x0 + LEFT; yb = y - 0.05
    L.append(f"\\draw[black!60,dashed,line width=0.4pt] ({x + W / XMAX:.3f}in,{yb:.3f}in) -- ({x + W / XMAX:.3f}in,0in);")
    L.append(f"\\draw[black!60,line width=0.4pt] ({x:.3f}in,{yb:.3f}in) -- ({x + W:.3f}in,{yb:.3f}in);")
    for t in (0, 0.5, 1, 1.5, 2):
        xt = x + t * W / XMAX
        L.append(f"\\draw[black!60,line width=0.4pt] ({xt:.3f}in,{yb:.3f}in) -- ({xt:.3f}in,{yb - 0.03:.3f}in);")
        L.append(f"\\node[anchor=north,font=\\tiny,inner sep=1pt] at ({xt:.3f}in,{yb - 0.03:.3f}in) {{{t:g}}};")
    return L, yb - 0.16
C = [r for r in rows if r["pair"] == "C"]; V = [r for r in rows if r["pair"] == "V"]
L = ["% generated by daavar/scripts/fig-components.py from tex/tables/table4.csv; do not edit",
     "\\definecolor{figRef}{HTML}{0072B2}\\definecolor{figCand}{HTML}{E69F00}",
     "\\begin{tikzpicture}"]
l1, yb1 = panel(C, 0.0, "conforming pairs"); l2, yb2 = panel(V, LEFT + W + PANEL_GAP, "violating pairs")
L += l1 + l2
yb = min(yb1, yb2)
TOTAL = 2 * LEFT + 2 * W + PANEL_GAP
L.append(f"\\node[anchor=north,font=\\tiny] at ({LEFT + W / 2:.3f}in,{yb1 - 0.12:.3f}in) {{time, as a multiple of \\online's T}};")
L.append(f"\\node[anchor=north,font=\\tiny] at ({LEFT + W + PANEL_GAP + LEFT + W / 2:.3f}in,{yb2 - 0.12:.3f}in) {{time, as a multiple of \\online's T}};")
# legend: two rows at fixed positions
ly = yb - 0.34; lx = LEFT
rows_ = [[("figRef", "\\online: T$_{\\mathit{ref}}$", 0.0), ("figCand", "\\online: T$_{\\mathit{cand}}$", 0.95), ("black!15", "remainder of T", 1.95)],
         [("figRef!45!white", "\\offline: E, reference export", 0.0), ("figCand!45!white", "E, candidate export", 1.55), ("black!45", "W", 2.75), ("black!75", "J", 3.05)]]
for row in rows_:
    for col, lab, dx in row:
        x = lx + dx
        L.append(f"\\fill[{col}] ({x:.3f}in,{ly:.3f}in) rectangle ({x + 0.12:.3f}in,{ly + 0.08:.3f}in);")
        L.append(f"\\node[anchor=west,font=\\tiny,inner sep=1pt] at ({x + 0.14:.3f}in,{ly + 0.04:.3f}in) {{{lab}}};")
    ly -= 0.14
L.append("\\end{tikzpicture}")
open(OUT, "w").write("\n".join(L) + "\n")
print(f"{len(C)} conforming and {len(V)} violating pairs")
