"""Parsers of the tools' outputs (format/SPEC.md §2, §7-§9): measurement blocks, labelled lines, export
headers, conf-join reports, index.tsv and witness-join's summary.txt.

A parser returns data or raises ParseError; the caller classes a ParseError after exit 0 as refused
(the output was not what the contract promises), never as a verdict.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

from . import DaavarError

MEASUREMENTS_MARKER = "--- measurements"
ENUMERATIONS = ("complete", "sampled", "bounded", "aborted")
ONLINE_FIELDS = ("first_miss_iteration", "first_miss_wall_ms", "uncovered_total")


class ParseError(DaavarError):
    pass


# ---------------------------------------------------------------- measurement block and labelled lines

_KEY = re.compile(r"([a-z][a-z0-9_]*): ?(.*)\Z")


def split_measurements(text: str) -> tuple:
    """(the text before the last measurement block, the block as a dict or None, the text after it)."""
    lines = text.splitlines()
    idx = None
    for i, l in enumerate(lines):
        if l == MEASUREMENTS_MARKER:
            idx = i
    if idx is None:
        return text, None, ""
    block = {}
    j = idx + 1
    while j < len(lines):
        l = lines[j]
        if l[:1].isdigit():  # an interleaved log line (a timestamp): skipped
            j += 1
            continue
        m = _KEY.match(l)
        if not m:
            break
        block[m.group(1)] = m.group(2)
        j += 1
    return "\n".join(lines[:idx]), block, "\n".join(lines[j:])


# The labels of the tools' human-readable lines (TraceForge pads them to 12 columns, JMC too).
LABELS = ("spec", "lookups", "verdict", "mode", "first miss", "uncovered", "witness", "at", "FAILED",
          "exported", "graph_store", "program", "strategy", "iterations", "seed", "reportPath", "exportFile",
          "ran", "blocked", "completed", "rate", "elapsed", "observed", "graphs", "witnesses", "error")
_LABEL = re.compile(r"(" + "|".join(re.escape(x) for x in LABELS) + r"):(?: +|\Z)(.*)\Z")


def labelled(text: str) -> dict:
    """The labelled lines (the first of each label) before the measurement block, and `FAILED` anywhere
    (JMC prints it after the block)."""
    before, _, after = split_measurements(text)
    out = {}
    for l in before.splitlines():
        m = _LABEL.match(l)
        if m and m.group(1) not in out:
            out[m.group(1)] = m.group(2)
    for l in after.splitlines():
        if l.startswith("FAILED:") and "FAILED" not in out:
            out["FAILED"] = l[len("FAILED:"):].strip()
    return out


def failed_line(text: str) -> str | None:
    for l in text.splitlines():
        if l.startswith("FAILED:"):
            return l[len("FAILED:"):].strip()
    return None


_STORE = re.compile(r"graph_store: written=(\d+) refused=(\d+) ms=(\d*)\Z")


def graph_store_line(text: str) -> dict | None:
    for l in text.splitlines():
        m = _STORE.match(l)
        if m:
            return {"written": int(m.group(1)), "refused": int(m.group(2)),
                    "ms": int(m.group(3)) if m.group(3) else None}
    return None


_TF_EXPORTED = re.compile(r"exported: (\d+) lines from (\d+) graphs to (.*)\Z")
_JMC_EXPORTED = re.compile(r"exported: +(\d+) iterations as (\d+) distinct summaries \((\w+), (\d+) not exported\)"
                           r" to (.*)\Z")


def tf_exported_line(text: str) -> dict | None:
    for l in text.splitlines():
        m = _TF_EXPORTED.match(l)
        if m:
            return {"lines": int(m.group(1)), "graphs": int(m.group(2)), "path": m.group(3)}
    return None


def jmc_exported_line(text: str) -> dict | None:
    for l in text.splitlines():
        m = _JMC_EXPORTED.match(l)
        if m:
            return {"iterations": int(m.group(1)), "lines": int(m.group(2)), "enumeration": m.group(3),
                    "not_exported": int(m.group(4)), "path": m.group(5)}
    return None


_UNCOVERED = re.compile(r"(\d+) graphs \((\d+) distinct summaries\)\Z")
_FIRST_MISS = re.compile(r"(?:iteration (\d+), )?(\d+) ms\Z")


def online_lines(text: str) -> dict | None:
    """The online check's lines (SPEC §7; TraceForge report.rs, JMC MpLauncher), or None without a verdict."""
    lab = labelled(text)
    if "verdict" not in lab:
        return None
    out = {"verdict": lab["verdict"], "mode": lab.get("mode"), "bug_finder": "bug-finder" in (lab.get("mode") or ""),
           "first_miss_iteration": None, "uncovered_total": None, "uncovered_distinct": None,
           "witness": lab.get("witness"), "failed": lab.get("FAILED")}
    if "first miss" in lab:
        m = _FIRST_MISS.match(lab["first miss"])
        if m and m.group(1):
            out["first_miss_iteration"] = int(m.group(1))
    if "uncovered" in lab:
        m = _UNCOVERED.match(lab["uncovered"])
        if not m:
            raise ParseError(f"online: bad uncovered line {lab['uncovered']!r}")
        out["uncovered_total"], out["uncovered_distinct"] = int(m.group(1)), int(m.group(2))
    if out["verdict"] not in ("conforms", "reports", "partial", "refused"):
        raise ParseError(f"online: unknown verdict {out['verdict']!r}")
    return out


# ---------------------------------------------------------------- export files


@dataclass
class ExportFile:
    path: str
    header: dict
    lines: int  # summary lines (the header excluded)

    @property
    def enumeration(self) -> str:
        return self.header["enumeration"]

    @property
    def complete(self) -> bool:
        return self.header["enumeration"] == "complete"

    def header_without_online(self) -> dict:
        return {k: v for k, v in self.header.items() if k not in ONLINE_FIELDS}


def export_file(path) -> ExportFile:
    """Reads an export's header (SPEC §2) and counts its summary lines; a malformed file is a ParseError."""
    p = Path(path)
    try:
        with open(p, "rb") as f:
            first = f.readline()
            n = 0
            last = b"\n"
            for line in f:
                if line.strip():
                    n += 1
                last = line
    except FileNotFoundError:
        raise ParseError(f"{p}: no export file") from None
    if not first:
        raise ParseError(f"{p}: empty export file")
    if not last.endswith(b"\n") and n:
        raise ParseError(f"{p}: the last line is truncated")
    try:
        h = json.loads(first)
    except ValueError as e:
        raise ParseError(f"{p}: the header is not JSON: {e}") from None
    if not isinstance(h, dict):
        raise ParseError(f"{p}: the header is not an object")
    if type(h.get("format")) is not int or type(h.get("encoding")) is not int or \
            h.get("format") != 1 or h.get("encoding") != 1:
        raise ParseError(f"{p}: format {h.get('format')!r}, encoding {h.get('encoding')!r}: only 1 and 1 are read")
    for k in ("tool", "program", "size", "model", "enumeration", "visible"):
        if k not in h:
            raise ParseError(f"{p}: the header has no {k}")
    if h["enumeration"] not in ENUMERATIONS:
        raise ParseError(f"{p}: unknown enumeration {h['enumeration']!r}")
    if h["model"] not in ("p2p", "asyn", "cd"):
        raise ParseError(f"{p}: unknown model {h['model']!r}")
    for k in ("tool", "program", "size", "model", "enumeration"):
        if not isinstance(h[k], str) or not _scalar_str(h[k]):
            raise ParseError(f"{p}: header {k} is not a string of Unicode scalar values")
    vis = h["visible"]
    if not isinstance(vis, list) or not all(isinstance(x, str) and _scalar_str(x) for x in vis):
        raise ParseError(f"{p}: header visible is not a list of names")
    minor = h.get("minor", 0)
    if type(minor) is not int or minor not in (0, 1):  # TraceForge reads minor 0 and 1 only
        raise ParseError(f"{p}: header minor {minor!r}")
    if vis != sorted(set(vis), key=lambda x: x.encode()) or not all(_role_ok(x) for x in vis):
        raise ParseError(f"{p}: header visible is not a byte-sorted list of distinct valid names")
    if minor >= 1:
        roles = h.get("roles")
        if not isinstance(roles, list) or not all(isinstance(x, str) and _scalar_str(x) and _role_ok(x) for x in roles) \
                or roles != sorted(set(roles), key=lambda x: x.encode()) or not set(vis) <= set(roles):
            raise ParseError(f"{p}: a minor-{minor} header needs roles: sorted, distinct, valid, covering visible")
    check_body(p, sorted(vis))
    return ExportFile(str(p), h, n)


def _role_ok(n: str) -> bool:
    """SPEC §5: non-empty ASCII, not main, not starting with ~, no control character."""
    return bool(n) and n.isascii() and n != "main" and not n.startswith("~") and all(32 <= ord(c) < 127 for c in n)


_I64 = (-(1 << 63), (1 << 63) - 1)


def _scalar_str(s: str) -> bool:
    """A string of Unicode scalar values (serde_json refuses an unpaired surrogate)."""
    return not any(0xD800 <= ord(c) <= 0xDFFF for c in s)


def _value_ok(v) -> bool:
    """SPEC §4 readers: signed-64 integers, strings, booleans, arrays, objects of exactly t (string), f (array)."""
    if isinstance(v, bool):
        return True
    if isinstance(v, str):
        return _scalar_str(v)
    if isinstance(v, int):
        return _I64[0] <= v <= _I64[1]
    if isinstance(v, list):
        return all(_value_ok(x) for x in v)
    if isinstance(v, dict):
        return set(v) == {"t", "f"} and isinstance(v["t"], str) and _scalar_str(v["t"]) and isinstance(v["f"], list) and \
            all(_value_ok(x) for x in v["f"])
    return False  # floats, null outside a receive


def _event_ok(e) -> bool:
    if not isinstance(e, dict) or len(e) != 1:
        return False
    (k, v), = e.items()
    if k == "s":
        return _value_ok(v)
    if k == "r":
        return v is None or _value_ok(v)
    return False


def _no_float(s: str):
    raise ValueError(f"a float {s}")


def _int(s: str) -> int:
    if s == "-0":
        raise ValueError("-0 (both readers refuse it, SPEC §4)")
    return int(s)


def check_body(p: Path, visible: list) -> None:
    """Every summary line (SPEC §3): a JSON object with statuses and words keyed by the visible names, events
    `{"s":V}`/`{"r":V|null}` with §4 values, Σ|w_t|·(T-1) counts each at most |w_u|, n (if present) a
    positive integer, no other key, and no summary twice."""
    seen = set()
    with open(p, "rb") as f:
        f.readline()
        for k, raw in enumerate(f, 2):
            if not raw.strip():
                raise ParseError(f"{p}:{k}: a blank line (TraceForge's reader refuses it)")
            try:
                o = json.loads(raw, parse_float=_no_float, parse_constant=_no_float, parse_int=_int)
            except ValueError as e:
                raise ParseError(f"{p}:{k}: not JSON (or a float): {e}") from None
            if not isinstance(o, dict) or set(o) - {"statuses", "words", "counts", "n"} \
                    or not {"statuses", "words", "counts"} <= set(o):
                raise ParseError(f"{p}:{k}: not a summary line (statuses, words, counts[, n])")
            st, wo, co = o["statuses"], o["words"], o["counts"]
            if not isinstance(st, dict) or sorted(st) != visible or \
                    not all(v in ("done", "blocked", "err") for v in st.values()):
                raise ParseError(f"{p}:{k}: statuses are not one of done/blocked/err per visible name")
            if not isinstance(wo, dict) or sorted(wo) != visible or not all(isinstance(v, list) for v in wo.values()):
                raise ParseError(f"{p}:{k}: words are not one list per visible name")
            if not isinstance(co, list) or not all(isinstance(c, int) and not isinstance(c, bool) and c >= 0 for c in co):
                raise ParseError(f"{p}:{k}: counts are not non-negative integers")
            for t, w in wo.items():
                if not all(_event_ok(e) for e in w):
                    raise ParseError(f"{p}:{k}: a word event of {t} is not {{\"s\":V}} or {{\"r\":V|null}} with a §4 value")
            want = sum(len(w) for w in wo.values()) * (len(visible) - 1)
            if len(co) != want:
                raise ParseError(f"{p}:{k}: {len(co)} counts, the words need {want}")
            i = 0
            for t in visible:
                for _ in wo[t]:
                    for u in visible:
                        if u == t:
                            continue
                        if co[i] > len(wo[u]):
                            raise ParseError(f"{p}:{k}: a count {co[i]} exceeds |w_{u}| = {len(wo[u])}")
                        i += 1
            if "n" in o and (isinstance(o["n"], bool) or not isinstance(o["n"], int) or o["n"] < 1):
                raise ParseError(f"{p}:{k}: n is not a positive integer")
            nf = json.dumps([st, wo, co], sort_keys=True, separators=(",", ":"))
            if nf in seen:
                raise ParseError(f"{p}:{k}: a repeated summary")
            seen.add(nf)


def check_counts(ex: "ExportFile", meas: dict | None, enumeration_of_stop: dict, who: str) -> None:
    """The export against the run's measurement block: summaries_distinct = the file's lines, and the
    enumeration the stop reason implies (a truncated or relabelled export is refused)."""
    if not meas:
        raise ParseError(f"{who}: no measurement block")
    sd = meas.get("summaries_distinct", "")
    if not sd.isdigit() or int(sd) != ex.lines:
        raise ParseError(f"{who}: summaries_distinct {sd!r}, the export holds {ex.lines} lines")
    sr = meas.get("stop_reason", "")
    if sr not in enumeration_of_stop:
        raise ParseError(f"{who}: stop_reason {sr!r} after exit 0")
    if ex.enumeration not in enumeration_of_stop[sr]:
        raise ParseError(f"{who}: stop_reason {sr} with an export that is {ex.enumeration}")


def store_head(path) -> dict:
    """A graph store's identity line (SPEC §8.3: `{"witness_store":1,"export":H,"run":R}`)."""
    p = Path(path)
    try:
        with open(p, "rb") as f:
            first = f.readline()
    except FileNotFoundError:
        raise ParseError(f"{p}: no graph store") from None
    try:
        h = json.loads(first)
    except ValueError:
        raise ParseError(f"{p}: the identity line is not JSON") from None
    if not isinstance(h, dict) or h.get("witness_store") != 1 or not first.endswith(b"\n"):
        raise ParseError(f"{p}: not a graph store identity line")
    return h


MODEL_NAMES = {"fifo": "p2p", "bag": "asyn", "causal": "cd"}


# ---------------------------------------------------------------- conf-join


@dataclass
class JoinEntry:
    cls: str  # no-bucket | no-slot | not-found
    line: str
    decide: dict | None = None  # outcome, k, k_min, states
    witness: str | None = None
    graph: str | None = None


@dataclass
class JoinReport:
    spec: dict
    imp: dict
    verdict: str  # conforms | violation | no verdict
    label: str  # "", "sampled" or "partial"
    uncovered: int
    merge: dict | None
    decide: dict | None  # the counters
    entries: list = field(default_factory=list)
    measurements: dict | None = None

    def decide_counts(self) -> dict:
        return {k: self.decide[k] for k in ("violations", "false_alarms", "unresolved")} if self.decide else {}


_HEAD = re.compile(r"tool=(?P<tool>\S*) program=(?P<program>.*?) size=(?P<size>.*) model=(?P<model>\S+) "
                   r"enumeration=(?P<enumeration>\S+)\Z")
_VERDICT = re.compile(r"verdict: (conforms|violation|no verdict)(?: \((sampled|partial)\))?\Z")
_DECIDE = re.compile(r"decide: uncovered=(\d+) violations=(\d+) false_alarms=(\d+) unresolved=(\d+) "
                     r"max_group=(\d+) states=(\d+) budget=(\d+) decide_ms=(\d+)\Z")
_ENTRY_DECIDE = re.compile(r"  decide: (violation|false-alarm|unresolved) k=(\d+) k_min=(\d+) states=(\d+)\Z")
_ENTRY = re.compile(r"(no-bucket|no-slot|not-found) (\{.*\})\Z")
_KV = re.compile(r"(\w+)=(\S*)")


def conf_join(text: str, exit_code: int | None = None) -> JoinReport:
    """conf-join's stdout (report.rs JoinReport::render), with every count checked against the entries:
    violations + false alarms + unresolved = uncovered, one entry per uncovered line, the verdict and the
    exit code agreeing. Anything missing or inconsistent is a ParseError (refused, never 0)."""
    before, block, _ = split_measurements(text)
    lines = before.splitlines()
    i = 0

    def need(prefix: str) -> str:
        nonlocal i
        if i >= len(lines) or not lines[i].startswith(prefix):
            got = lines[i] if i < len(lines) else "(end of output)"
            raise ParseError(f"conf-join: expected `{prefix}`, got {got!r}")
        i += 1
        return lines[i - 1][len(prefix):]

    heads = []
    for side in ("spec: ", "impl: "):
        m = _HEAD.match(need(side))
        if not m:
            raise ParseError(f"conf-join: bad {side.strip()} line")
        heads.append(m.groupdict())
    m = _VERDICT.match("verdict: " + need("verdict: "))
    if not m:
        raise ParseError(f"conf-join: bad verdict line {lines[i - 1]!r}")
    verdict, label = m.group(1), m.group(2) or ""
    merge = None
    if i < len(lines) and lines[i].startswith("merge: "):
        merge = dict(_KV.findall(need("merge: ")))
    unc_text = need("uncovered: ")
    if not unc_text.isdigit():
        raise ParseError(f"conf-join: bad uncovered count {unc_text!r}")
    uncovered = int(unc_text)
    decide = None
    if i < len(lines) and lines[i].startswith("decide: "):
        m = _DECIDE.match(lines[i])
        if not m:
            raise ParseError(f"conf-join: bad decide line {lines[i]!r}")
        keys = ("uncovered", "violations", "false_alarms", "unresolved", "max_group", "states", "budget", "decide_ms")
        decide = dict(zip(keys, (int(x) for x in m.groups())))
        i += 1
    entries = []
    while i < len(lines):
        l = lines[i]
        if not l:
            i += 1
            continue
        m = _ENTRY.match(l)
        if not m:
            raise ParseError(f"conf-join: unexpected line {l!r}")
        e = JoinEntry(m.group(1), m.group(2))
        i += 1
        while i < len(lines) and lines[i].startswith("  "):
            sub = lines[i]
            md = _ENTRY_DECIDE.match(sub)
            if md:
                e.decide = {"outcome": md.group(1), "k": int(md.group(2)), "k_min": int(md.group(3)),
                            "states": int(md.group(4))}
            elif sub.startswith("  witness: "):
                e.witness = sub[len("  witness: "):]
            elif sub.startswith("  graph: "):
                e.graph = sub[len("  graph: "):]
            else:
                raise ParseError(f"conf-join: unexpected entry line {sub!r}")
            i += 1
        entries.append(e)
    r = JoinReport(heads[0], heads[1], verdict, label, uncovered, merge, decide, entries, block)
    check_join(r, exit_code)
    return r


def check_join(r: JoinReport, exit_code: int | None) -> None:
    if len(r.entries) != r.uncovered:
        raise ParseError(f"conf-join: uncovered: {r.uncovered} but {len(r.entries)} uncovered lines printed (truncated)")
    if r.decide is not None:
        d = r.decide
        if d["uncovered"] != r.uncovered:
            raise ParseError(f"conf-join: decide counts {d['uncovered']} uncovered lines, the join {r.uncovered}")
        if d["violations"] + d["false_alarms"] + d["unresolved"] != r.uncovered:
            raise ParseError("conf-join: violations + false alarms + unresolved != uncovered "
                             f"({d['violations']} + {d['false_alarms']} + {d['unresolved']} != {r.uncovered})")
        got = {"violation": 0, "false-alarm": 0, "unresolved": 0}
        for e in r.entries:
            if e.decide is None:
                raise ParseError(f"conf-join: an uncovered line has no decide line: {e.line[:200]}")
            got[e.decide["outcome"]] += 1
        if (got["violation"], got["false-alarm"], got["unresolved"]) != (d["violations"], d["false_alarms"], d["unresolved"]):
            raise ParseError(f"conf-join: the per-line decide outcomes {got} disagree with the counters")
    if r.uncovered == 0 and r.verdict == "violation":
        raise ParseError("conf-join: violation with no uncovered line")
    if r.uncovered > 0 and r.verdict != "violation":
        raise ParseError(f"conf-join: {r.uncovered} uncovered lines but verdict {r.verdict}")
    if exit_code is not None:
        want = 1 if r.verdict == "violation" else 0
        if exit_code != want:
            raise ParseError(f"conf-join: verdict {r.verdict} with exit {exit_code}")


INDEX_HEADER = ["n", "class", "decide", "graph", "reason", "line"]
INDEX_REASONS = ("ok", "limit", "false-alarm", "no-store", "no-graph", "refused")


def index_tsv(path, report: JoinReport | None = None) -> list:
    """`graphs/index.tsv` (SPEC §8.4): one row per uncovered line, in printed order, checked against the report."""
    p = Path(path)
    try:
        text = p.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ParseError(f"{p}: no index.tsv") from None
    if text and not text.endswith("\n"):
        raise ParseError(f"{p}: the last row is truncated")
    rows = [l.split("\t") for l in text.splitlines() if l]
    if not rows or rows[0] != INDEX_HEADER:
        raise ParseError(f"{p}: the header is not {' '.join(INDEX_HEADER)}")
    out = []
    for k, r in enumerate(rows[1:], 1):
        if len(r) != 6:
            raise ParseError(f"{p}: row {k} has {len(r)} columns")
        if r[0] != str(k):
            raise ParseError(f"{p}: row {k} is numbered {r[0]}")
        if r[4] not in INDEX_REASONS:
            raise ParseError(f"{p}: row {k}: unknown reason {r[4]!r}")
        out.append(dict(zip(INDEX_HEADER, r)))
    if report is not None:
        if len(out) != report.uncovered:
            raise ParseError(f"{p}: {len(out)} rows for {report.uncovered} uncovered lines")
        for row, e in zip(out, report.entries):
            if row["line"] != e.line or row["class"] != e.cls:
                raise ParseError(f"{p}: row {row['n']} is not the report's uncovered line {row['n']}")
            if e.decide and row["decide"] != e.decide["outcome"]:
                raise ParseError(f"{p}: row {row['n']}: decide {row['decide']} vs {e.decide['outcome']}")
    return out


def summary_txt(path) -> dict:
    """witness-join.sh's summary.txt: key=value lines (exit, uncovered, graphs, check_failures, render_failures)."""
    p = Path(path)
    try:
        text = p.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ParseError(f"{p}: no summary.txt") from None
    out = {}
    for l in text.splitlines():
        if "=" in l:
            k, v = l.split("=", 1)
            out[k] = v
    for k in ("exit", "uncovered", "graphs", "check_failures", "render_failures"):
        if k not in out:
            raise ParseError(f"{p}: no {k}=")
    return out
