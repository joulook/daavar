#!/usr/bin/env python3
"""Companion attributes for row files whose ids tables.py cannot name (2026-11-04, after the Raft twin rows showed blank
instance cells). usage: rows-attrs.py <daavar> <rows file>...  For every id of a rows file that has no entry in any
*.rows.tsv (by base id), an entry is derived from a template entry of the same engine prefix (same directory when one
exists) and from the id and command: size, model, variant, tier, threads, spec and impl cases. Appended to <rows>.tsv."""
import csv, glob, re, sys, os
DV = sys.argv[1]; FILES = sys.argv[2:]
HDR = "id table family size model variant engine threads expected_verdict note tier kind spec_tool impl_tool spec_lang impl_lang mode exploration benchmark spec_case impl_case".split()
MODELS = {"bag", "fifo", "p2p", "asyn", "causal"}
SUF = r"(\.(t\d+|w\d+|c1h|c48h|c4h|m|p|g|r2|j2|x\d+|f\d+|jimpl-tfimpl|tfimpl-jimpl|tfspec-jmcimpl|jmcspec-tfimpl))+$"
def base_id(i): return re.sub(r"(\.c(1|48)h|\.j2|\.m|\.r2|\.p)+$", "", i)
attrs = {}
for f in [DV + "/cluster/rows.tsv"] + glob.glob(DV + "/cluster/bench/*.rows.tsv") + glob.glob(DV + "/cluster/campaign/*/*.rows.tsv"):
    for r in csv.DictReader(open(f), delimiter="\t"): attrs[r["id"].replace("'", "")] = {k: (v or "").replace("'", "") for k, v in r.items()}
def known(i): return base_id(i) in attrs or re.sub(r"\.vs-[A-Za-z0-9-]+", "", base_id(i)) in attrs
def case_of(path):  # "$EVAL_OUT"/<id>/export.jsonl -> <dir>.<descriptor>
    i = path.split("/")[-2]; i = re.sub(r"\.vs-[A-Za-z0-9-]+", "", re.sub(SUF, "", i)); return i.split(".", 1)[1] if "." in i else i
def parse(i):
    pre, rest = i.split(".", 1); d, desc = rest.split(".", 1); desc = re.sub(SUF, "", desc)
    vs = re.search(r"\.vs-([A-Za-z0-9-]+)", desc); desc = re.sub(r"\.vs-[A-Za-z0-9-]+", "", desc).split(".")[0]
    toks = desc.split("_"); side = toks[0]; toks = toks[1:]
    nums = [t for t in toks if t.isdigit()]; model = next((t for t in reversed(toks) if t in MODELS), "")
    words = [t for t in toks if not t.isdigit() and t not in MODELS]
    return pre, d, desc, side, ",".join(nums), model, ("-".join(words) if words else "impl"), (vs.group(1) if vs else None)
def template(pre, d):
    for k, v in attrs.items():
        if k.startswith(pre + "." + d + "."): return dict(v)
    for k, v in attrs.items():
        if k.startswith(pre + "."): t = dict(v); t["family"] = t["benchmark"] = d; return t
    return None
for F in FILES:
    rows = [l.rstrip("\n") for l in open(F) if l.strip() and not l.startswith("#")]
    out = []; seen = set()
    for l in rows:
        f = l.split(); i, mem, to = f[0], f[1], f[2]; cmd = " ".join(f[3:]); cmd = re.sub(r"^needok:\S+ ", "", cmd)
        if known(i) or base_id(i) in seen: continue
        pre, d, desc, side, size, model, var, vs = parse(i); t = template(pre, d)
        if t is None: print("no template for", i); continue
        e = dict(t); e["id"] = base_id(i)   # tables.py looks attributes up by the base id (tier/rerun/join suffixes stripped)
        e["family"] = e["benchmark"] = d; e["size"] = size; e["model"] = model or t.get("model", "")
        e["variant"] = ("spec-" + var if side == "spec" and str(t.get("variant", "")).startswith("spec-") else var)
        e["tier"] = "c48h" if to == "172800" else ("c4h" if to == "14400" else ("c1h" if to == "3600" else t.get("tier", "")))
        m16 = re.search(r"\.(t|w)(\d+)", i); e["threads"] = m16.group(2) if m16 else "1"
        paths = re.findall(r'"\$EVAL_OUT"/[^ ]+/export\.jsonl', cmd)
        if pre in ("XT1", "XT2", "XC1", "XC2", "XJ1", "XJ2", "C3J") and len(paths) == 2:
            e["spec_case"], e["impl_case"] = case_of(paths[0]), case_of(paths[1])
        elif pre in ("XTON2", "XTON1", "XCON1", "XCON2", "XON1", "XON2") and paths:
            e["spec_case"], e["impl_case"] = case_of(paths[0]), d + "." + desc
        elif pre in ("C1", "C2"):
            e["impl_case"] = d + "." + desc
            cands = [x.split()[0] for x in rows if x.startswith("C3E." + d + ".spec_") and re.search(r"(^|_)" + size.replace(",", "_") + "_" + model, x.split()[0])]
            toks = set(desc.split("_")); sp = max(cands, key=lambda c: len(toks & set(re.sub(SUF, "", c).split(".")[2].split("_")))) if cands else None   # the spec export sharing most words with the implementation (lb: rr_a4b -> spec_rr_a4b)
            e["spec_case"] = (re.sub(SUF, "", sp).split(".", 1)[1] if sp else t.get("spec_case", ""))
        elif pre in ("C3E", "XJE", "XCE"):
            e["spec_case"] = e["impl_case"] = ""
            if side == "spec" and str(t.get("spec_case", "")): e["spec_case"] = e["impl_case"] = d + "." + desc
        if pre in ("XT1", "XT2", "XC1", "XC2"):
            e["expected_verdict"] = "twin"; e["note"] = "twin-spec=" + (vs or var)
        elif pre in ("XTON2", "XTON1", "XCON1", "XCON2"):
            e["expected_verdict"] = "twin"; e["note"] = "twin-spec=" + (vs or var)
        out.append(e); seen.add(base_id(i))
    if out:
        comp = F + ".tsv"; new = not os.path.exists(comp)
        with open(comp, "a") as fh:
            w = csv.DictWriter(fh, fieldnames=HDR, delimiter="\t", extrasaction="ignore")
            if new: w.writeheader()
            for e in out: w.writerow({k: e.get(k, "") for k in HDR})
        print(f"{os.path.basename(F)}: {len(out)} entries appended to {os.path.basename(comp)}")
