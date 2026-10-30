#!/usr/bin/env python3
"""Build record.json for one finished row and print its results.tsv line (EVAL-RULES R2).
usage: mkrecord.py <rowdir>   (env: BRAIN_DATA for the column header, ROWS_TSV for row attributes,
DAAVAR for the schema8 header). Parses: status, time-v.txt, load.log, stdout.log (TraceForge EVALROW
record, JMC launcher lines + `--- measurements` block, conf-join lines, benchmark `execs= blocked=`)."""
import csv, io, json, os, re, sys, subprocess, pathlib, socket

d = pathlib.Path(sys.argv[1]); rid = d.name
env = os.environ
def read(p):
    try: return pathlib.Path(p).read_text(errors="replace")
    except OSError: return ""
def kv(text):
    out = {}
    for line in text.splitlines():
        if "=" in line: k, v = line.split("=", 1); out[k.strip()] = v.strip()
    return out
status = kv(read(d / "status"))
rec = {"run_id": rid, "node": status.get("host", socket.gethostname()), "queue": status.get("queue"),
       "started_at": status.get("start"), "finished_at": status.get("end"), "cores": status.get("cores"),
       "numa_node": status.get("numa"), "mem_limit_gb": status.get("mem_gb"), "timeout_s": status.get("timeout_s"),
       "exit_code": status.get("exit"), "stop_reason": status.get("reason"), "load_before": status.get("load_before"),
       "command": read(d / "cmd.txt").strip(), "operator": env.get("EVAL_OPERATOR", "runner")}
rec["valid"] = "no" if str(rec["stop_reason"]).startswith("invalid") else "yes"
# commits
def git(path, *a):
    try: return subprocess.run(["git", "-C", path, *a], capture_output=True, text=True, timeout=20).stdout.strip()
    except Exception: return ""
dv = env.get("DAAVAR", "")
if dv:
    rec["daavar_commit"] = git(dv, "rev-parse", "--short", "HEAD")
    # the pin the row RAN on: recorded at launch; rows launched before that field existed ran on a3a7e1b
    rec["traceforge_commit"] = status.get("pin") or "a3a7e1b"
    rec["jmc_commit"] = git(dv + "/jmc", "rev-parse", "--short", "HEAD")
    rec["benchmarks_commit"] = rec["daavar_commit"]; rec["scripts_commit"] = rec["daavar_commit"]
# /usr/bin/time -v
tv = read(d / "time-v.txt")
def tvf(label):
    m = re.search(re.escape(label) + r".*?:\s*([0-9:.]+)", tv); return m.group(1) if m else None
def hms(s):
    if not s: return None
    p = s.split(":"); p = [float(x) for x in p]
    return int(1000 * (p[0]*3600 + p[1]*60 + p[2] if len(p) == 3 else p[0]*60 + p[1] if len(p) == 2 else p[0]))
rec["wall_ms"] = hms(tvf("Elapsed (wall clock) time"))
rec["cpu_user_s"] = tvf("User time (seconds)"); rec["cpu_sys_s"] = tvf("System time (seconds)")
rec["peak_rss_kb"] = tvf("Maximum resident set size")
# load samples
loads = [float(l.split()[1]) for l in read(d / "load.log").splitlines() if len(l.split()) > 1]
rec["load_max_during"] = max(loads) if loads else None
# row attributes (sidecar tsv: id table family size model variant engine threads expected_verdict note tier [kind spec_tool impl_tool spec_lang impl_lang mode exploration benchmark])
attrs = {}
rt = env.get("ROWS_TSV", "")
if rt and os.path.exists(rt):
    with open(rt, newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            if row.get("id") == rid: attrs = row; break
for k in ("table", "family", "size", "model", "variant", "engine", "threads", "expected_verdict", "tier", "note",
          "kind", "spec_tool", "impl_tool", "spec_lang", "impl_lang", "mode", "exploration", "benchmark", "direction"):
    if attrs.get(k): rec[k] = attrs[k]
rec.setdefault("sizes", rec.get("size")); rec.setdefault("comm_model", rec.get("model"))
if "threads" in rec: rec.setdefault("impl_threads", rec["threads"]); rec.setdefault("spec_threads", rec["threads"])
out = read(d / "stdout.log")
# TraceForge EVALROW record
m = re.search(r"EVALROW\t(.*)", out)
if m and dv:
    hdr = next(csv.reader([read(dv + "/scripts/schema8-header.csv").strip()]))
    cells = next(csv.reader([m.group(1)]))
    tr = dict(zip(hdr, cells)); rec["tool_record"] = tr
    def g(k): v = tr.get(k, ""); return v if v not in ("", None) else None
    rec.update({"verdict": g("verdict"), "spec_complete": g("spec_complete"), "spec_blocked": g("spec_blocked"),
        "impl_complete": g("impl_complete"), "impl_blocked": g("impl_blocked"), "impl_summaries": g("impl_distinct_summaries"),
        "spec_summaries": g("signatures"), "uncovered_total": g("uncovered_distinct"), "first_miss_wall_ms": g("wall_ms_at_first_report"),
        "spec_phase_ms": g("spec_wall_time_ms"), "impl_phase_ms": g("impl_wall_time_ms"), "precheck_ms": g("precheck_wall_time_ms"),
        "iterations": g("executions"), "max_graph_events": g("max_paper_events"), "max_summary_bytes": g("summary_bytes"),
        "tool_stop": g("end"), "tool_wall_ms": g("wall_ms"), "tool_peak_kb": g("vmhwm_kb_max")})
    if g("threads"): rec["impl_threads"] = g("threads")
    # engines whose record has no verdict/split cells (frozen engine): derive from reports and end states
    ends = [x for x in (g("end"), g("impl_end"), g("spec_end")) if x]
    if not rec.get("verdict") and g("reports") is not None:
        rec["verdict"] = "violation" if g("reports") != "0" else ("conforms" if ends and all(e == "StateSpaceExhausted" for e in ends) else "partial")
    rec["reports"] = g("reports")
    if not rec.get("spec_complete") and g("spec_graphs"): rec["spec_complete"] = g("spec_graphs")
    if not rec.get("impl_complete") and g("impl_graphs"): rec["impl_complete"] = g("impl_graphs")
    if not rec.get("impl_complete") and g("outer_graphs(execs+block)"):
        mm = re.search(r"\d+", g("outer_graphs(execs+block)")); rec["impl_complete"] = mm.group(0) if mm else None
    if not rec.get("tool_stop"): rec["tool_stop"] = "/".join(dict.fromkeys(ends)) or g("end_class")
    rec.setdefault("kind", "conf-traceforge"); rec.setdefault("spec_tool", "traceforge"); rec.setdefault("impl_tool", "traceforge")
    rec.setdefault("spec_lang", "rust"); rec.setdefault("impl_lang", "rust"); rec.setdefault("mode", "in-process")
    if g("verdict") and g("reports") and g("reports") != "0" and rec.get("stop_reason") == "ok": pass
# JMC launcher
if "org.mpi_sws.jmc" in rec["command"] or re.search(r"^graphs:\s", out, re.M):
    rec.setdefault("impl_tool", "jmc"); rec.setdefault("impl_lang", "java")
    g = re.search(r"graphs:\s*(\d+) distinct complete, (\d+) distinct blocked; stack (exhausted|not exhausted)", out)
    if g: rec["impl_complete"], rec["impl_blocked"], rec["tool_stop"] = g.group(1), g.group(2), g.group(3)
    for key, pat in (("iterations", r"^ran:\s*(\d+)"), ("verdict", r"^verdict:\s*(\S+)"), ("lookups", r"^lookups:\s*(\d+)"),
                     ("impl_summaries", r"exported: \d+ iterations as (\d+) distinct summaries")):
        mm = re.search(pat, out, re.M); 
        if mm: rec[key] = mm.group(1)
    mm = re.search(r"^at: iteration (\d+)", out, re.M)
    if mm: rec["first_miss_iteration"] = mm.group(1)
    if "--- measurements" in out:
        meas = {}
        for line in out.split("--- measurements", 1)[1].splitlines():
            mm = re.match(r"^\s*([a-z_]+):\s*(.*)$", line)
            if mm: meas[mm.group(1)] = mm.group(2).strip()
        rec["measurements"] = meas
        for k, v in meas.items():
            tgt = {"explore_ms": "explore_impl_ms", "graphs_complete": "impl_complete", "graphs_blocked": "impl_blocked",
                   "summaries_distinct": "impl_summaries", "stop_reason": "tool_stop", "peak_rss_kb": "tool_peak_kb", "wall_ms": "tool_wall_ms"}.get(k, k)
            if v != "": rec[tgt] = v
    rec.setdefault("kind", "cross-language" if "--spec" in rec["command"] else ("export" if ".jsonl" in rec["command"] else "must-standalone"))
    rec.setdefault("mode", "online" if "--spec" in rec["command"] else "n/a")
    rec.setdefault("engine", "stateful-online" if "--spec" in rec["command"] else "must")
    rec.setdefault("exploration", "stop-at-first" if "--spec" in rec["command"] and "full" not in rec["command"] else "full")
# conf-join
if "conf-join" in rec["command"] or re.search(r"^spec: tool=\S+ program=", out, re.M):
    rec.setdefault("kind", "cross-language"); rec.setdefault("mode", "offline-batch"); rec.setdefault("engine", "join")
    for side in ("spec", "impl"):
        mm = re.search(rf"^{side}: tool=(\S+) program=(\S+) size=(\S+) model=(\S+) enumeration=(\S+)", out, re.M)
        if mm:
            rec[f"{side}_tool"] = mm.group(1); rec[f"{side}_lang"] = "java" if mm.group(1) == "jmc" else "rust"
            rec[f"{side}_program"] = mm.group(2); rec.setdefault("sizes", mm.group(3)); rec.setdefault("comm_model", mm.group(4)); rec[f"{side}_enumeration"] = mm.group(5)
    mm = re.search(r"^verdict:\s*(.+)$", out, re.M); rec["verdict"] = mm.group(1).strip() if mm else None
    mm = re.search(r"^uncovered:\s*(\d+)", out, re.M); rec["uncovered_distinct"] = mm.group(1) if mm else None  # conf-join counts distinct lines, not graphs
    mm = re.search(r"refused", out); 
    if mm: rec["verdict"] = "refused"
    rec["join_ms"] = rec.get("wall_ms")
    # conf-join exits 1 on a violation and 2 when it refuses an input: a verdict means the row ran fine
    if rec.get("exit_code") == "1" and rec.get("verdict") and rec.get("stop_reason") == "exit-1": rec["stop_reason"] = "ok"
    if rec.get("exit_code") == "2" and rec.get("stop_reason") == "exit-2": rec["stop_reason"] = "refused"
# plain benchmark Rust count
mm = re.search(r"^execs=(\d+) blocked=(\d+)", out, re.M)
if mm and "impl_complete" not in rec:
    rec["impl_complete"], rec["impl_blocked"] = mm.group(1), mm.group(2)
    rec.setdefault("kind", "must-standalone"); rec.setdefault("impl_tool", "traceforge"); rec.setdefault("impl_lang", "rust"); rec.setdefault("engine", "must")
# durability
summ = [str(p.relative_to(d)) for p in d.rglob("*.jsonl*")]
rec["summaries_kept"] = "yes" if summ else "no"; rec["summaries_paths"] = ";".join(summ)
rec["stdout_path"] = "stdout.log"; rec["stderr_path"] = "stderr.log"; rec["timev_path"] = "time-v.txt"
rec["replay_path"] = ";".join(str(p.relative_to(d)) for p in d.glob("*replay*")) or None
rec["direction"] = rec.get("direction") or (f"{rec['spec_lang']}-spec/{rec['impl_lang']}-impl" if rec.get("spec_lang") and rec.get("impl_lang") else None)
rec["slot"] = rec.get("numa_node")
if isinstance(rec.get("verdict"), str): rec["verdict"] = rec["verdict"].lower()
(d / "record.json").write_text(json.dumps(rec, indent=1, sort_keys=True, default=str) + "\n")
# tsv line in brain-data's column order
hdr_path = pathlib.Path(env.get("BRAIN_DATA", "")) / "results.tsv"
cols = read(hdr_path).splitlines()[0].split("\t") if hdr_path.exists() else sorted(k for k in rec if not isinstance(rec[k], dict))
def cell(k):
    v = rec.get(k); 
    if v is None or isinstance(v, (dict, list)): return ""
    return str(v).replace("\t", " ").replace("\n", " ")
print("\t".join(cell(c) for c in cols))
